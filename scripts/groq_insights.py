"""
groq_insights.py  v2
====================
Thêm insight mới: new_releases_buzz
  — Phân tích game mới ra: tốc độ tăng trưởng, khả năng giữ player, điểm mạnh/yếu

5 loại insight mỗi ngày:
  1. trend_analysis    — Genre / game nào đang nổi
  2. deal_picks        — Top 5 game đáng mua
  3. hidden_gems       — Game ít người biết nhưng rating cao
  4. weekly_summary    — Bản tin tổng hợp ngắn
  5. new_releases_buzz — Phân tích game mới ra trong 14 ngày ★ NEW
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone

from groq import Groq
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Thiếu SUPABASE_URL hoặc SUPABASE_KEY"); sys.exit(1)
if not GROQ_API_KEY:
    log.error("Thiếu GROQ_API_KEY"); sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
groq_client = Groq(api_key=GROQ_API_KEY)

MODEL         = "llama-3.3-70b-versatile"
TEMPERATURE   = 0.7
MAX_TOKENS    = 1024
DELAY_BETWEEN = 2.0


def call_groq(system_prompt: str, user_prompt: str) -> tuple[str, int]:
    response = groq_client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
    )
    content     = response.choices[0].message.content
    token_count = response.usage.total_tokens if response.usage else 0
    return content, token_count


def save_insight(insight_type: str, content: str, token_count: int) -> None:
    sb.table("ai_insights").insert({
        "insight_type": insight_type,
        "content":      content,
        "model_used":   MODEL,
        "token_count":  token_count,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }).execute()
    log.info("Đã lưu insight '%s' (%d tokens)", insight_type, token_count)


# ── Data fetchers ─────────────────────────────────────────────────────────────

def get_trending_games(limit: int = 25) -> list[dict]:
    try:
        r = sb.table("v_trending_today") \
              .select("name, developer, genres, concurrent_peak, review_pct, review_count, rank_trending, discount_pct, price_current, price_usd") \
              .limit(limit).execute()
        return r.data or []
    except Exception as e:
        log.error("trending: %s", e); return []


def get_deal_games(limit: int = 20) -> list[dict]:
    try:
        r = sb.table("v_deals_today") \
              .select("name, developer, genres, review_pct, review_count, price_usd, discount_pct, price_current, savings_usd, concurrent_peak") \
              .limit(limit).execute()
        return r.data or []
    except Exception as e:
        log.error("deals: %s", e); return []


def get_all_today_games(limit: int = 50) -> list[dict]:
    try:
        r = sb.table("v_trending_today") \
              .select("name, genres, concurrent_peak, review_pct, discount_pct, rank_trending") \
              .limit(limit).execute()
        return r.data or []
    except Exception as e:
        log.error("all today: %s", e); return []


def get_new_releases(limit: int = 20) -> list[dict]:
    """Lấy game mới ra từ bảng new_releases, sort theo launch_score."""
    try:
        r = sb.table("new_releases") \
              .select("name, developer, genres, review_pct, review_count, "
                      "positive_reviews, negative_reviews, concurrent_peak, "
                      "owners_text, price_usd, is_free, "
                      "release_date, days_since_release, launch_score, short_desc") \
              .order("launch_score", desc=True) \
              .limit(limit).execute()
        return r.data or []
    except Exception as e:
        log.error("new_releases: %s", e); return []


# ── Formatters ────────────────────────────────────────────────────────────────

def format_trending_for_prompt(games: list[dict]) -> str:
    lines = []
    for g in games:
        genres_str  = ", ".join(g.get("genres") or []) or "N/A"
        players     = g.get("concurrent_peak")
        players_str = f"{players:,}" if players else "N/A"
        review      = g.get("review_pct")
        review_str  = f"{review}%" if review else "N/A"
        rank        = g.get("rank_trending", "?")
        lines.append(
            f"#{rank}. {g['name']} | Genre: {genres_str} | "
            f"Players online: {players_str} | Review: {review_str}"
        )
    return "\n".join(lines)


def format_deals_for_prompt(games: list[dict]) -> str:
    lines = []
    for g in games:
        genres_str = ", ".join(g.get("genres") or []) or "N/A"
        original   = g.get("price_usd", 0)
        current    = g.get("price_current", 0)
        discount   = g.get("discount_pct", 0)
        review     = g.get("review_pct")
        review_str = f"{review}%" if review else "N/A"
        lines.append(
            f"- {g['name']} ({genres_str}) | "
            f"Giá gốc: ${original:.2f} → còn ${current:.2f} (-{discount}%) | "
            f"Review: {review_str} ({g.get('review_count', 0):,} đánh giá)"
        )
    return "\n".join(lines)


def format_all_for_summary(games: list[dict]) -> str:
    genre_counts: dict[str, int] = {}
    top_players = sorted(
        [g for g in games if g.get("concurrent_peak")],
        key=lambda x: x["concurrent_peak"], reverse=True,
    )[:5]
    for g in games:
        for genre in (g.get("genres") or []):
            genre_counts[genre] = genre_counts.get(genre, 0) + 1
    top_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    lines = ["Top game theo lượt chơi:"]
    for g in top_players:
        lines.append(f"  - {g['name']}: {g['concurrent_peak']:,} players")
    lines.append("\nGenre phổ biến nhất hôm nay:")
    for genre, count in top_genres:
        lines.append(f"  - {genre}: {count} game trong top")
    return "\n".join(lines)


def format_new_releases_for_prompt(games: list[dict]) -> str:
    """Format data game mới để đưa vào prompt phân tích buzz."""
    lines = []
    for i, g in enumerate(games, 1):
        name         = g.get("name", "Unknown")
        days         = g.get("days_since_release", "?")
        review_pct   = g.get("review_pct")
        review_count = g.get("review_count", 0) or 0
        pos          = g.get("positive_reviews", 0) or 0
        neg          = g.get("negative_reviews", 0) or 0
        players      = g.get("concurrent_peak", 0) or 0
        owners       = g.get("owners_text", "N/A") or "N/A"
        genres_str   = ", ".join(g.get("genres") or []) or "N/A"
        price        = "Free" if g.get("is_free") else f"${g.get('price_usd', 0):.2f}"
        launch_score = g.get("launch_score", 0) or 0
        short_desc   = (g.get("short_desc") or "")[:120]

        # Tính velocity: reviews/ngày
        if days and days > 0 and review_count > 0:
            velocity = round(review_count / days)
        else:
            velocity = 0

        lines.append(
            f"{i}. [{name}] — Ra {days} ngày trước | Genre: {genres_str}\n"
            f"   Giá: {price} | Players: {players:,} | Owners: {owners}\n"
            f"   Review: {review_pct}% ({review_count:,} total, +{pos:,}/-{neg:,})\n"
            f"   Velocity: ~{velocity:,} review/ngày | Launch Score: {launch_score:.3f}\n"
            f"   Mô tả: {short_desc}"
        )
    return "\n\n".join(lines)


# ── 5 insight generators ──────────────────────────────────────────────────────

def generate_trend_analysis(trending: list[dict]) -> None:
    log.info("Đang tạo trend_analysis...")
    system = (
        "Bạn là chuyên gia phân tích thị trường game với 10 năm kinh nghiệm. "
        "Viết bằng tiếng Việt, giọng điệu thân thiện như đang kể chuyện."
    )
    data_text = format_trending_for_prompt(trending)
    user = f"""Top game đang trending trên Steam hôm nay:

{data_text}

Phân tích 3 điểm (mỗi điểm 2-3 câu):
1. **Xu hướng nổi bật**: Genre hoặc loại game nào đang chiếm ưu thế?
2. **Điểm thú vị**: Có game nào bất ngờ hoặc pattern lạ không?
3. **Dự báo ngắn**: Tuần tới có thể kỳ vọng gì?

Không giải thích nguồn dữ liệu."""

    content, tokens = call_groq(system, user)
    save_insight("trend_analysis", content, tokens)


def generate_deal_picks(deals: list[dict]) -> None:
    log.info("Đang tạo deal_picks...")
    system = (
        "Bạn là người bạn am hiểu game, luôn tìm game hay với ngân sách hợp lý. "
        "Viết bằng tiếng Việt, ngắn gọn và thực tế."
    )
    data_text = format_deals_for_prompt(deals)
    user = f"""Game đang sale trên Steam hôm nay (review >= 70%):

{data_text}

Chọn đúng 5 game đáng mua nhất, format:
1. **[Tên game]** — [1-2 câu lý do, nhấn mạnh giá trị]
2. ..."""

    content, tokens = call_groq(system, user)
    save_insight("deal_picks", content, tokens)


def generate_hidden_gems(all_games: list[dict]) -> None:
    log.info("Đang tạo hidden_gems...")
    system = (
        "Bạn là game thủ kỳ cựu chuyên tìm game indie ít nổi nhưng cực chất. "
        "Viết bằng tiếng Việt với giọng nhiệt huyết."
    )
    potential_gems = [
        g for g in all_games
        if (g.get("review_pct") or 0) >= 80
        and (g.get("rank_trending") or 999) > 15
        and (g.get("concurrent_peak") or 0) < 50000
    ]
    if len(potential_gems) < 3:
        potential_gems = [
            g for g in all_games
            if (g.get("review_pct") or 0) >= 75 and (g.get("rank_trending") or 999) > 10
        ]
    data_text = format_trending_for_prompt(potential_gems[:15])
    user = f"""Game có review tốt nhưng không quá viral:

{data_text}

Giới thiệu 3 "hidden gem" đáng khám phá:
- Tên game và genre
- Tại sao xứng đáng được chú ý hơn
- Phù hợp với ai

Giọng như recommend cho người bạn thân."""

    content, tokens = call_groq(system, user)
    save_insight("hidden_gems", content, tokens)


def generate_weekly_summary(all_games: list[dict], deals: list[dict]) -> None:
    log.info("Đang tạo weekly_summary...")
    system = (
        "Bạn là biên tập viên trang tin gaming Việt Nam. "
        "Viết bản tin ngắn, súc tích, hấp dẫn. Phong cách chuyên nghiệp nhưng có cá tính."
    )
    summary_data = format_all_for_summary(all_games)
    deal_count   = len(deals)
    top_deal     = deals[0] if deals else None
    top_deal_str = f"{top_deal['name']} (-{top_deal['discount_pct']}%)" if top_deal else "không có"
    today_str    = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    user = f"""Dữ liệu Steam ngày {today_str}:

{summary_data}

{deal_count} game đang sale, nổi bật: {top_deal_str}.

Viết bản tin tổng hợp TỐI ĐA 4 câu:
- Câu 1: Bức tranh tổng thể hôm nay
- Câu 2: Điểm nhấn nổi bật
- Câu 3: Thông tin deal/sale
- Câu 4: Kết thú vị hoặc gợi ý hành động

Bắt đầu thẳng vào nội dung."""

    content, tokens = call_groq(system, user)
    save_insight("weekly_summary", content, tokens)


def generate_new_releases_buzz(new_games: list[dict]) -> None:
    """
    ★ NEW: Phân tích game mới ra — tốc độ tăng trưởng, khả năng giữ player, điểm mạnh/yếu.
    Mục tiêu: cho người đọc biết game mới nào ĐÁNG CHÚ Ý và TẠI SAO.
    """
    log.info("Đang tạo new_releases_buzz...")

    if not new_games:
        log.warning("Không có game mới, bỏ qua new_releases_buzz")
        return

    system = (
        "Bạn là một game journalist chuyên theo dõi launch mới trên Steam. "
        "Bạn giỏi đọc dữ liệu và dự đoán game nào sẽ 'bùng nổ' hay 'chết yểu' dựa trên "
        "tốc độ review, lượng player, và phản hồi cộng đồng trong tuần đầu. "
        "Viết bằng tiếng Việt, thẳng thắn, có số liệu cụ thể, không nói chung chung."
    )

    data_text    = format_new_releases_for_prompt(new_games)
    game_count   = len(new_games)
    top_game     = new_games[0] if new_games else {}
    top_name     = top_game.get("name", "N/A")
    top_score    = top_game.get("launch_score", 0)
    today_str    = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    user = f"""Dữ liệu {game_count} game mới ra trên Steam (trong 14 ngày, đã qua bộ lọc chất lượng):

{data_text}

Ngày phân tích: {today_str}
Game có launch_score cao nhất: {top_name} (score: {top_score:.3f})

Hãy viết phân tích theo 3 phần:

**🚀 Ngôi sao mới nổi** (1-2 game):
Chỉ ra game nào đang có trajectory tốt nhất. Dẫn số liệu cụ thể: velocity review/ngày, 
tỷ lệ positive, số player. Giải thích tại sao đây là dấu hiệu tốt.

**⚡ Tốc độ tăng trưởng** (nhận xét chung):
So sánh velocity (review/ngày) giữa các game. Game nào đang "nóng" và game nào 
"chậm sưởi ấm"? Điều này nói lên điều gì về marketing hoặc word-of-mouth?

**🎯 Dự báo 2 tuần tới**:
Dựa trên dữ liệu hiện tại, game nào có khả năng:
- Giữ được momentum (và lý do)
- Fade dần (và lý do)

Ngắn gọn, có số liệu, không viết lan man."""

    content, tokens = call_groq(system, user)
    save_insight("new_releases_buzz", content, tokens)
    log.info("new_releases_buzz: %d tokens", tokens)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Steam Tracker — groq_insights.py v2")
    log.info("Model: %s", MODEL)
    log.info("Thời gian: %s UTC", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
    log.info("=" * 60)

    log.info("Đang lấy dữ liệu từ Supabase...")
    trending     = get_trending_games(limit=25)
    deals        = get_deal_games(limit=20)
    all_games    = get_all_today_games(limit=50)
    new_releases = get_new_releases(limit=15)

    if not trending and not all_games:
        log.error("Không có dữ liệu. Hãy chạy fetch_data.py trước!")
        sys.exit(1)

    log.info("trending=%d, deals=%d, all=%d, new_releases=%d",
             len(trending), len(deals), len(all_games), len(new_releases))

    trending_data = trending or all_games
    deals_data    = deals or []

    results = []

    tasks = [
        ("trend_analysis",    lambda: generate_trend_analysis(trending_data)),
        ("deal_picks",        lambda: generate_deal_picks(deals_data) if deals_data else None),
        ("hidden_gems",       lambda: generate_hidden_gems(all_games or trending_data)),
        ("weekly_summary",    lambda: generate_weekly_summary(all_games or trending_data, deals_data)),
        ("new_releases_buzz", lambda: generate_new_releases_buzz(new_releases)),
    ]

    for name, fn in tasks:
        try:
            fn()
            results.append(f"{name}: OK")
        except Exception as exc:
            log.error("%s thất bại: %s", name, exc)
            results.append(f"{name}: FAILED ({exc})")
        time.sleep(DELAY_BETWEEN)

    log.info("=" * 60)
    for r in results:
        icon = "v" if "OK" in r else ("S" if "SKIP" in r else "x")
        log.info("  [%s] %s", icon, r)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
