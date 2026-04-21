"""
generate_html.py  v2
====================
- Dùng view mới v2 (dedupe, filtered)
- Query thêm historical data, genre stats, tổng stats
- Render markdown từ Groq thành HTML
- Fallback rõ ràng khi field NULL/thiếu
"""

import os
import sys
import re
import logging
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

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Thiếu SUPABASE_URL hoặc SUPABASE_KEY")
    sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Markdown → HTML ──────────────────────────────────────────
def md_to_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    text = re.sub(r'(?<!\n)\*(?!\*|[ \t])(.+?)(?<!\*)(?<! )\*', r'<em>\1</em>', text)
    def replace_ol(m):
        items = re.findall(r'^\d+\.\s+(.+)$', m.group(0), re.MULTILINE)
        return '<ol>' + ''.join(f'<li>{i}</li>' for i in items) + '</ol>'
    text = re.sub(r'(?:^\d+\. .+\n?)+', replace_ol, text, flags=re.MULTILINE)
    def replace_ul(m):
        items = re.findall(r'^[-•]\s+(.+)$', m.group(0), re.MULTILINE)
        return '<ul>' + ''.join(f'<li>{i}</li>' for i in items) + '</ul>'
    text = re.sub(r'(?:^[-•] .+\n?)+', replace_ul, text, flags=re.MULTILINE)
    text = re.sub(r'\n{2,}', '</p><p>', text)
    text = f'<p>{text}</p>'
    text = re.sub(r'<p>\s*</p>', '', text)
    return text


# ── Fetch helpers ─────────────────────────────────────────────
def fetch(table, select="*", limit=50, filters=None, order=None):
    try:
        q = sb.table(table).select(select)
        if filters:
            for col, op, val in filters:
                if op == "gte": q = q.gte(col, val)
                elif op == "eq": q = q.eq(col, val)
        if order:
            desc = order.startswith("-")
            q = q.order(order.lstrip("-"), desc=desc)
        return q.limit(limit).execute().data or []
    except Exception as e:
        log.error("fetch(%s): %s", table, e)
        return []

def fetch_one(table, select="*"):
    try:
        r = sb.table(table).select(select).limit(1).execute()
        return r.data[0] if r.data else {}
    except Exception as e:
        log.error("fetch_one(%s): %s", table, e)
        return {}

def fetch_insight(insight_type):
    try:
        r = (sb.table("ai_insights")
             .select("content")
             .eq("insight_type", insight_type)
             .order("generated_at", desc=True)
             .limit(1).execute())
        return r.data[0]["content"] if r.data else ""
    except Exception as e:
        log.error("fetch_insight(%s): %s", insight_type, e)
        return ""

def fetch_history(top_app_ids):
    if not top_app_ids:
        return {}
    try:
        r = (sb.table("v_history_7days")
             .select("app_id, name, snapshot_date, concurrent_peak")
             .in_("app_id", top_app_ids[:8])
             .execute())
        result = {}
        for row in (r.data or []):
            aid = row["app_id"]
            if aid not in result:
                result[aid] = {"name": row["name"], "points": []}
            if row.get("concurrent_peak"):
                result[aid]["points"].append({
                    "date": str(row["snapshot_date"])[:10],
                    "peak": row["concurrent_peak"],
                })
        return result
    except Exception as e:
        log.error("fetch_history: %s", e)
        return {}


# ── Main render ───────────────────────────────────────────────
def render():
    log.info("Fetching data...")

    trending_raw = fetch("v_trending_today", limit=60)

    # Dedupe by app_id ở Python
    seen: set = set()
    trending = []
    for g in trending_raw:
        aid = g.get("app_id")
        if aid and aid not in seen:
            seen.add(aid)
            trending.append(g)
        if len(trending) >= 24:
            break

    deals       = fetch("v_deals_today",       limit=16)
    genre_stats = fetch("v_genre_stats_today",  limit=10)
    stats       = fetch_one("v_stats_today")

    top_ids = [g["app_id"] for g in trending[:6] if g.get("app_id")]
    history = fetch_history(top_ids)

    summary        = md_to_html(fetch_insight("weekly_summary"))
    trend_analysis = md_to_html(fetch_insight("trend_analysis"))
    deal_picks     = md_to_html(fetch_insight("deal_picks"))
    hidden_gems    = md_to_html(fetch_insight("hidden_gems"))

    now = datetime.now(timezone.utc)

    # Chart data
    all_dates = sorted({
        pt["date"]
        for info in history.values()
        for pt in info["points"]
    })
    COLORS = ["#5b6af8","#22c984","#f5a623","#f05252","#a78bfa","#38bdf8","#fb923c","#34d399"]
    chart_datasets = [
        {
            "label": info["name"][:22],
            "data":  [{pt["date"]: pt["peak"] for pt in info["points"]}.get(d) for d in all_dates],
            "color": COLORS[i % len(COLORS)],
        }
        for i, (_, info) in enumerate(history.items())
    ]

    context = dict(
        trending=trending, deals=deals, genre_stats=genre_stats, stats=stats,
        summary=summary, trend_analysis=trend_analysis,
        deal_picks=deal_picks, hidden_gems=hidden_gems,
        chart_labels=all_dates, chart_datasets=chart_datasets,
        has_history=bool(all_dates and chart_datasets),
        updated_at=now.strftime("%d/%m/%Y %H:%M UTC"),
        updated_iso=now.isoformat(),
        total_games=len(trending), total_deals=len(deals),
        top_game=trending[0] if trending else None,
    )

    env = Environment(loader=FileSystemLoader(str(TEMPLATES)), autoescape=True)
    env.filters["commas"] = lambda v: f"{int(v):,}"      if v else "N/A"
    env.filters["usd"]    = lambda v: f"${float(v):.2f}" if v else "Free"
    env.filters["pct"]    = lambda v: f"{int(v)}%"        if v else "—"
    env.filters["abbr"]   = lambda v: (
        f"{v/1_000_000:.1f}M" if v and v >= 1_000_000 else
        f"{v/1_000:.0f}K"     if v and v >= 1_000     else
        str(int(v))           if v else "N/A"
    )

    html = env.get_template("index.html.j2").render(**context)
    out  = OUTPUT / "index.html"
    out.write_text(html, encoding="utf-8")
    log.info("Render OK → %s (%d bytes)", out, len(html))


def main():
    log.info("=" * 55)
    log.info("Steam Tracker — generate_html.py v2")
    log.info("=" * 55)
    render()
    log.info("Done!")

if __name__ == "__main__":
    main()
