"""generate_html.py v4 — thêm new_releases panel + buzz insight"""
import os, sys, re, logging, json
from pathlib import Path
from datetime import datetime, timezone
from jinja2 import Environment, FileSystemLoader
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

ROOT      = Path(__file__).parent.parent
TEMPLATES = ROOT / "templates"
OUTPUT    = ROOT / "docs"
OUTPUT.mkdir(exist_ok=True)

SUPABASE_URL = os.environ.get("SUPABASE_URL","")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY","")
if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Thiếu env vars"); sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def md_to_html(text: str) -> str:
    if not text: return ""
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    def repl_ol(m):
        items = re.findall(r'^\d+\.\s+(.+)$', m.group(0), re.MULTILINE)
        return '<ol>' + ''.join(f'<li>{i}</li>' for i in items) + '</ol>'
    text = re.sub(r'(?:^\d+\. .+\n?)+', repl_ol, text, flags=re.MULTILINE)
    def repl_ul(m):
        items = re.findall(r'^[-•]\s+(.+)$', m.group(0), re.MULTILINE)
        return '<ul>' + ''.join(f'<li>{i}</li>' for i in items) + '</ul>'
    text = re.sub(r'(?:^[-•] .+\n?)+', repl_ul, text, flags=re.MULTILINE)
    text = re.sub(r'\n{2,}', '</p><p>', text)
    text = f'<p>{text}</p>'
    text = re.sub(r'<p>\s*</p>', '', text)
    return text


def q(table, select="*", limit=50, order=None, filters=None):
    try:
        req = sb.table(table).select(select)
        for f in (filters or []):
            col, op, val = f
            if op == "eq":  req = req.eq(col, val)
            elif op == "gte": req = req.gte(col, val)
        if order:
            req = req.order(order.lstrip("-"), desc=order.startswith("-"))
        return req.limit(limit).execute().data or []
    except Exception as e:
        log.error("q(%s): %s", table, e); return []


def q1(table, select="*"):
    try:
        r = sb.table(table).select(select).limit(1).execute()
        return r.data[0] if r.data else {}
    except Exception as e:
        log.error("q1(%s): %s", table, e); return {}


def insight(t):
    try:
        r = sb.table("ai_insights").select("content").eq("insight_type", t)\
              .order("generated_at", desc=True).limit(1).execute()
        return r.data[0]["content"] if r.data else ""
    except: return ""


def history(ids):
    if not ids: return {}
    try:
        r = sb.table("v_history_7days")\
              .select("app_id,name,snapshot_date,concurrent_peak")\
              .in_("app_id", ids[:8]).execute()
        out = {}
        for row in (r.data or []):
            aid = row["app_id"]
            if aid not in out:
                out[aid] = {"name": row["name"], "points": []}
            if row.get("concurrent_peak"):
                out[aid]["points"].append({
                    "date": str(row["snapshot_date"])[:10],
                    "peak": row["concurrent_peak"]
                })
        return out
    except Exception as e:
        log.error("history: %s", e); return {}


def get_new_releases(limit: int = 20) -> list[dict]:
    """Lấy game mới từ bảng new_releases, sort theo launch_score desc."""
    try:
        r = sb.table("new_releases")\
              .select("app_id, name, developer, genres, review_pct, review_count, "
                      "positive_reviews, negative_reviews, concurrent_peak, "
                      "owners_text, price_usd, is_free, img_header, img_capsule, "
                      "steam_url, release_date, days_since_release, launch_score")\
              .order("launch_score", desc=True)\
              .limit(limit).execute()
        return r.data or []
    except Exception as e:
        log.error("new_releases: %s", e); return []


def render():
    log.info("Fetching data...")
    trending_raw  = q("v_trending_today", limit=80)
    surge         = q("v_surge_today",       limit=12)
    deals         = q("v_deals_today",        limit=16)
    genre_stat    = q("v_genre_stats_today",  limit=10)
    stats         = q1("v_stats_today")
    new_releases  = get_new_releases(limit=20)

    # Dedupe trending
    seen, trending = set(), []
    for g in trending_raw:
        aid = g.get("app_id")
        if aid and aid not in seen:
            seen.add(aid); trending.append(g)
        if len(trending) >= 24: break

    top_ids = [g["app_id"] for g in trending[:8] if g.get("app_id")]
    hist    = history(top_ids)

    COLORS = ["#5b6af8","#22c984","#f5a623","#f05252","#a78bfa","#38bdf8","#fb923c","#34d399"]
    all_dates = sorted({pt["date"] for info in hist.values() for pt in info["points"]})
    chart_line = [
        {
            "label": info["name"][:22],
            "data":  [{pt["date"]: pt["peak"] for pt in info["points"]}.get(d) for d in all_dates],
            "color": COLORS[i % len(COLORS)],
        }
        for i, (_, info) in enumerate(hist.items())
    ]

    now = datetime.now(timezone.utc)
    ctx = dict(
        trending=trending, surge=surge, deals=deals,
        genre_stat=genre_stat, stats=stats,
        new_releases=new_releases,
        total_new=len(new_releases),
        summary=md_to_html(insight("weekly_summary")),
        trend_analysis=md_to_html(insight("trend_analysis")),
        deal_picks=md_to_html(insight("deal_picks")),
        hidden_gems=md_to_html(insight("hidden_gems")),
        new_releases_buzz=md_to_html(insight("new_releases_buzz")),
        chart_labels=all_dates, chart_line=chart_line,
        has_history=bool(all_dates and chart_line),
        updated_at=now.strftime("%d/%m/%Y %H:%M UTC"),
        updated_iso=now.isoformat(),
        total_games=len(trending),
        total_deals=len(deals),
        total_surge=len(surge),
        top_game=trending[0] if trending else None,
    )

    env = Environment(loader=FileSystemLoader(str(TEMPLATES)), autoescape=True)
    env.filters["commas"] = lambda v: f"{int(v):,}" if v else "N/A"
    env.filters["usd"]    = lambda v: f"${float(v):.2f}" if v else "Free"
    env.filters["abbr"]   = lambda v: (
        f"{v/1_000_000:.1f}M" if v and v >= 1_000_000 else
        f"{v/1_000:.0f}K"     if v and v >= 1_000     else
        str(int(v))           if v else "N/A"
    )
    env.filters["sign"]   = lambda v: f"+{v:.1f}%" if v and v > 0 else (f"{v:.1f}%" if v else "—")

    html = env.get_template("index.html.j2").render(**ctx)
    out  = OUTPUT / "index.html"
    out.write_text(html, encoding="utf-8")
    log.info("Render OK → %s (%d bytes)", out, len(html))


def main():
    log.info("="*55); log.info("generate_html.py v4"); log.info("="*55)
    render(); log.info("Done!")


if __name__ == "__main__":
    main()
