"""
generate_html.py
================
Bước 3 của pipeline: Query dữ liệu từ Supabase (games + insights),
render Jinja2 template ra docs/index.html.

Biến môi trường cần có:
  SUPABASE_URL  — URL project Supabase
  SUPABASE_KEY  — Service role key
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone

from jinja2 import Environment, FileSystemLoader
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).parent.parent
TEMPLATES = ROOT / "templates"
OUTPUT    = ROOT / "docs"
OUTPUT.mkdir(exist_ok=True)

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Thiếu SUPABASE_URL hoặc SUPABASE_KEY")
    sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Fetch data ────────────────────────────────────────────────────────────────

def fetch_trending(limit: int = 20) -> list[dict]:
    try:
        r = sb.table("v_trending_today").select("*").limit(limit).execute()
        return r.data or []
    except Exception as e:
        log.error("fetch_trending: %s", e)
        return []

def fetch_deals(limit: int = 12) -> list[dict]:
    try:
        r = sb.table("v_deals_today").select("*").limit(limit).execute()
        return r.data or []
    except Exception as e:
        log.error("fetch_deals: %s", e)
        return []

def fetch_insight(insight_type: str) -> str:
    """Lấy insight mới nhất theo type."""
    try:
        r = (
            sb.table("ai_insights")
            .select("content")
            .eq("insight_type", insight_type)
            .order("generated_at", desc=True)
            .limit(1)
            .execute()
        )
        return r.data[0]["content"] if r.data else ""
    except Exception as e:
        log.error("fetch_insight(%s): %s", insight_type, e)
        return ""

def fetch_genre_stats(limit: int = 8) -> list[dict]:
    """Tổng hợp genre phổ biến từ top trending hôm nay."""
    try:
        r = sb.table("v_trending_today").select("genres, concurrent_peak").limit(50).execute()
        games = r.data or []
        genre_totals: dict[str, int] = {}
        for g in games:
            for genre in (g.get("genres") or []):
                genre_totals[genre] = genre_totals.get(genre, 0) + (g.get("concurrent_peak") or 0)
        sorted_genres = sorted(genre_totals.items(), key=lambda x: x[1], reverse=True)[:limit]
        return [{"genre": k, "players": v} for k, v in sorted_genres]
    except Exception as e:
        log.error("fetch_genre_stats: %s", e)
        return []


# ── Render ────────────────────────────────────────────────────────────────────

def render():
    log.info("Fetching data from Supabase...")
    trending   = fetch_trending(20)
    deals      = fetch_deals(12)
    genre_stats = fetch_genre_stats(8)

    log.info("Fetching AI insights...")
    summary      = fetch_insight("weekly_summary")
    trend_analysis = fetch_insight("trend_analysis")
    deal_picks   = fetch_insight("deal_picks")
    hidden_gems  = fetch_insight("hidden_gems")

    now = datetime.now(timezone.utc)

    context = {
        "trending":       trending,
        "deals":          deals,
        "genre_stats":    genre_stats,
        "summary":        summary,
        "trend_analysis": trend_analysis,
        "deal_picks":     deal_picks,
        "hidden_gems":    hidden_gems,
        "updated_at":     now.strftime("%d/%m/%Y %H:%M UTC"),
        "updated_iso":    now.isoformat(),
        "total_games":    len(trending),
        "total_deals":    len(deals),
        "top_game":       trending[0] if trending else None,
    }

    env = Environment(loader=FileSystemLoader(str(TEMPLATES)), autoescape=True)
    # Custom filter: format số có dấu phẩy
    env.filters["commas"] = lambda v: f"{int(v):,}" if v else "N/A"
    env.filters["usd"]    = lambda v: f"${float(v):.2f}" if v else "Free"

    template = env.get_template("index.html.j2")
    html     = template.render(**context)

    out_path = OUTPUT / "index.html"
    out_path.write_text(html, encoding="utf-8")
    log.info("Đã render → %s (%d bytes)", out_path, len(html))


def main():
    log.info("=" * 60)
    log.info("Steam Tracker — generate_html.py bắt đầu")
    log.info("=" * 60)
    render()
    log.info("Hoàn thành!")


if __name__ == "__main__":
    main()
