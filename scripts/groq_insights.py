"""
groq_insights.py
================
Bước 2 của pipeline: Lấy dữ liệu từ Supabase, gọi Groq AI phân tích,
rồi lưu kết quả vào bảng ai_insights.

4 loại insight sẽ được tạo ra mỗi ngày:
  1. trend_analysis   — Genre / game nào đang nổi và tại sao
  2. deal_picks       — Top 5 game đáng mua nhất hôm nay
  3. hidden_gems      — Game ít người biết nhưng rating cao, giá rẻ
  4. weekly_summary   — Bản tin tổng hợp dạng ngắn gọn cho dashboard

Biến môi trường cần có:
  SUPABASE_URL  — URL project Supabase
  SUPABASE_KEY  — Service role key
  GROQ_API_KEY  — Key tại console.groq.com (free tier đủ dùng)
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone

from groq import Groq
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Khởi tạo clients ──────────────────────────────────────────────────────────
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")
GROQ_API_KEY  = os.environ.get("GROQ_API_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Thiếu SUPABASE_URL hoặc SUPABASE_KEY")
    sys.exit(1)

if not GROQ_API_KEY:
    log.error("Thiếu GROQ_API_KEY")
    sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Dùng đúng pattern đã test thành công
groq_client = Groq(api_key=GROQ_API_KEY)

MODEL         = "llama-3.3-70b-versatile"
TEMPERATURE   = 0.7
MAX_TOKENS    = 1024
DELAY_BETWEEN = 2.0   # giây giữa các lần gọi Groq (tránh rate limit)


# ── Helper: gọi Groq ──────────────────────────────────────────────────────────

def call_groq(system_prompt: str, user_prompt: str) -> tuple[str, int]:
    """
    Gọi Groq API với đúng pattern đã test thành công.
    Trả về (content_text, token_count).
    """
    response = groq_client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
        ],
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
    )

    content     = response.choices[0].message.content
    token_count = response.usage.total_tokens if response.usage else 0
    return content, token_count


def save_insight(insight_type: str, content: str, token_count: int) -> None:
    """Lưu kết quả phân tích vào bảng ai_insights trong Supabase."""
    sb.table("ai_insights").insert({
        "insight_type": insight_type,
        "content":      content,
        "model_used":   MODEL,
        "token_count":  token_count,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }).execute()
    log.info("Đã lưu insight '%s' (%d tokens)", insight_type, token_count)


# ── Lấy dữ liệu từ Supabase ───────────────────────────────────────────────────

def get_trending_games(limit: int = 25) -> list[dict]:
    """Lấy top game trending hôm nay từ view v_trending_today."""
    try:
        result = (
            sb.table("v_trending_today")
            .select("name, developer, genres, concurrent_peak, review_pct, review_count, rank_trending, discount_pct, price_current, price_usd")
            .limit(limit)
            .execute()
        )
        return result.data or []
    except Exception as exc:
        log.error("Không lấy được trending games: %s", exc)
        return []


def get_deal_games(limit: int = 20) -> list[dict]:
    """Lấy game đang giảm giá chất lượng cao từ view v_deals_today."""
    try:
        result = (
            sb.table("v_deals_today")
            .select("name, developer, genres, review_pct, review_count, price_usd, discount_pct, price_current, savings_usd, concurrent_peak")
            .limit(limit)
            .execute()
        )
        return result.data or []
    except Exception as exc:
        log.error("Không lấy được deal games: %s", exc)
        return []


def get_all_today_games(limit: int = 50) -> list[dict]:
    """Lấy toàn bộ game có snapshot hôm nay để tổng hợp."""
    try:
        result = (
            sb.table("v_trending_today")
            .select("name, genres, concurrent_peak, review_pct, discount_pct, rank_trending")
            .limit(limit)
            .execute()
        )
        return result.data or []
    except Exception as exc:
        log.error("Không lấy được all today games: %s", exc)
        return []


# ── Định dạng data thành text để đưa vào prompt ───────────────────────────────

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
    """Tóm tắt gọn để đưa vào weekly summary prompt."""
    genre_counts: dict[str, int] = {}
    top_players = sorted(
        [g for g in games if g.get("concurrent_peak")],
        key=lambda x: x["concurrent_peak"],
        reverse=True,
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


# ── 4 insight functions ───────────────────────────────────────────────────────

def generate_trend_analysis(trending: list[dict]) -> None:
    """Phân tích xu hướng: genre nào đang nổi, pattern gì đáng chú ý."""
    log.info("Đang tạo trend_analysis...")

    system = (
        "Bạn là chuyên gia phân tích thị trường game với 10 năm kinh nghiệm. "
        "Nhiệm vụ của bạn là đọc dữ liệu Steam và rút ra insight sắc bén, "
        "cụ thể và thú vị cho cộng đồng game thủ Việt Nam. "
        "Viết bằng tiếng Việt, giọng điệu thân thiện, dễ hiểu như đang kể chuyện."
    )

    data_text = format_trending_for_prompt(trending)
    user = f"""Đây là top game đang trending trên Steam hôm nay:

{data_text}

Hãy phân tích và trả lời 3 điểm sau (mỗi điểm 2-3 câu):
1. **Xu hướng nổi bật**: Genre hoặc loại game nào đang chiếm ưu thế và tại sao có thể như vậy?
2. **Điểm thú vị**: Có game nào bất ngờ, hoặc pattern lạ nào trong dữ liệu không?
3. **Dự báo ngắn**: Dựa trên trend này, tuần tới có thể kỳ vọng gì?

Không cần giải thích về nguồn dữ liệu. Đi thẳng vào phân tích."""

    content, tokens = call_groq(system, user)
    save_insight("trend_analysis", content, tokens)


def generate_deal_picks(deals: list[dict]) -> None:
    """Chọn ra top 5 deal đáng mua nhất, giải thích lý do cụ thể."""
    log.info("Đang tạo deal_picks...")

    system = (
        "Bạn là người bạn am hiểu game, luôn giúp mọi người tìm được game hay "
        "với ngân sách hợp lý. Bạn biết rõ loại game nào xứng đáng với từng mức giá. "
        "Viết bằng tiếng Việt, ngắn gọn và thực tế."
    )

    data_text = format_deals_for_prompt(deals)
    user = f"""Đây là danh sách game đang sale trên Steam hôm nay (đã lọc review >= 70%):

{data_text}

Hãy chọn ra đúng 5 game đáng mua nhất và giải thích ngắn gọn lý do cho từng game.
Format trả về:
1. **[Tên game]** — [1-2 câu tại sao nên mua, nhấn mạnh giá trị so với giá tiền]
2. ...
(và tiếp tục cho đủ 5 game)

Ưu tiên game có review cao, giảm giá sâu, và genre có nhiều người chơi."""

    content, tokens = call_groq(system, user)
    save_insight("deal_picks", content, tokens)


def generate_hidden_gems(all_games: list[dict]) -> None:
    """Tìm hidden gems — game ít người biết nhưng chất lượng cao."""
    log.info("Đang tạo hidden_gems...")

    system = (
        "Bạn là một game thủ kỳ cựu, chuyên tìm ra những game indie hoặc ít nổi "
        "nhưng cực kỳ chất lượng mà đám đông hay bỏ qua. "
        "Bạn viết bằng tiếng Việt với giọng nhiệt huyết của người yêu game thật sự."
    )

    # Lọc game có review cao nhưng không ở top 10 trending (ít được chú ý)
    potential_gems = [
        g for g in all_games
        if (g.get("review_pct") or 0) >= 80
        and (g.get("rank_trending") or 999) > 15
        and (g.get("concurrent_peak") or 0) < 50000
    ]

    if len(potential_gems) < 3:
        # Nới lỏng điều kiện nếu không đủ
        potential_gems = [
            g for g in all_games
            if (g.get("review_pct") or 0) >= 75
            and (g.get("rank_trending") or 999) > 10
        ]

    data_text = format_trending_for_prompt(potential_gems[:15])

    user = f"""Từ dữ liệu Steam hôm nay, đây là các game có review tốt nhưng không quá viral:

{data_text}

Hãy giới thiệu 3 "hidden gem" đáng để khám phá. Với mỗi game:
- Tên game và genre
- Tại sao nó xứng đáng được chú ý hơn
- Phù hợp với ai (kiểu game thủ nào sẽ thích)

Giọng điệu như đang recommend cho người bạn thân, không phải viết review chính thức."""

    content, tokens = call_groq(system, user)
    save_insight("hidden_gems", content, tokens)


def generate_weekly_summary(all_games: list[dict], deals: list[dict]) -> None:
    """Tổng hợp toàn bộ thành bản tin ngắn gọn cho phần đầu dashboard."""
    log.info("Đang tạo weekly_summary...")

    system = (
        "Bạn là biên tập viên của một trang tin tức gaming Việt Nam. "
        "Bạn viết những bản tin ngắn, súc tích, hấp dẫn để người đọc "
        "nắm bắt được tình hình gaming trong 30 giây. "
        "Phong cách: chuyên nghiệp nhưng không khô khan, có cá tính."
    )

    summary_data = format_all_for_summary(all_games)
    deal_count   = len(deals)
    top_deal     = deals[0] if deals else None
    top_deal_str = (
        f"{top_deal['name']} (-{top_deal['discount_pct']}%)"
        if top_deal else "không có deal nổi bật"
    )
    today_str = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    user = f"""Dữ liệu Steam ngày {today_str}:

{summary_data}

Hôm nay có {deal_count} game đang sale đáng chú ý, nổi bật nhất là {top_deal_str}.

Viết một bản tin tổng hợp NGẮN GỌN (tối đa 4 câu) theo cấu trúc:
- Câu 1: Bức tranh tổng thể hôm nay trên Steam
- Câu 2: Điểm nhấn nổi bật nhất (game hoặc genre)
- Câu 3: Thông tin về deal / sale
- Câu 4: Một câu kết thú vị hoặc gợi ý hành động

Bắt đầu thẳng vào nội dung, không cần viết "Bản tin ngày..." hay tiêu đề."""

    content, tokens = call_groq(system, user)
    save_insight("weekly_summary", content, tokens)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Steam Tracker — groq_insights.py bắt đầu")
    log.info("Model: %s", MODEL)
    log.info("Thời gian: %s UTC", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
    log.info("=" * 60)

    # ── Lấy dữ liệu từ Supabase ──────────────────────────────
    log.info("Đang lấy dữ liệu từ Supabase...")
    trending  = get_trending_games(limit=25)
    deals     = get_deal_games(limit=20)
    all_games = get_all_today_games(limit=50)

    if not trending and not all_games:
        log.error("Không có dữ liệu trong Supabase. Hãy chạy fetch_data.py trước!")
        sys.exit(1)

    log.info("Có %d trending, %d deals, %d total games", len(trending), len(deals), len(all_games))

    # ── Gọi Groq cho từng loại insight ───────────────────────
    # Dùng data nào có sẵn (fallback nếu một source bị trống)
    trending_data  = trending  if trending  else all_games
    deals_data     = deals     if deals     else []

    results = []

    try:
        generate_trend_analysis(trending_data)
        results.append("trend_analysis: OK")
    except Exception as exc:
        log.error("trend_analysis thất bại: %s", exc)
        results.append(f"trend_analysis: FAILED ({exc})")

    time.sleep(DELAY_BETWEEN)

    try:
        if deals_data:
            generate_deal_picks(deals_data)
            results.append("deal_picks: OK")
        else:
            log.warning("Không có deal data, bỏ qua deal_picks")
            results.append("deal_picks: SKIPPED (no deals)")
    except Exception as exc:
        log.error("deal_picks thất bại: %s", exc)
        results.append(f"deal_picks: FAILED ({exc})")

    time.sleep(DELAY_BETWEEN)

    try:
        generate_hidden_gems(all_games if all_games else trending_data)
        results.append("hidden_gems: OK")
    except Exception as exc:
        log.error("hidden_gems thất bại: %s", exc)
        results.append(f"hidden_gems: FAILED ({exc})")

    time.sleep(DELAY_BETWEEN)

    try:
        generate_weekly_summary(all_games if all_games else trending_data, deals_data)
        results.append("weekly_summary: OK")
    except Exception as exc:
        log.error("weekly_summary thất bại: %s", exc)
        results.append(f"weekly_summary: FAILED ({exc})")

    # ── Summary ───────────────────────────────────────────────
    log.info("=" * 60)
    log.info("Hoàn thành! Kết quả:")
    for r in results:
        status = "OK" if "OK" in r else ("SKIP" if "SKIP" in r else "FAIL")
        icon   = {"OK": "v", "SKIP": "-", "FAIL": "x"}[status]
        log.info("  [%s] %s", icon, r)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
