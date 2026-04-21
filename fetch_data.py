"""
fetch_data.py
=============
Bước 1 của pipeline: Thu thập dữ liệu từ Steam APIs và lưu vào Supabase.

Luồng chạy:
  1. SteamSpy  → top 100 game trending + metadata cơ bản
  2. Storefront API → featured / top sellers chính thức từ Steam
  3. Steam Web API → concurrent players real-time
  4. Upsert vào bảng `games` (thông tin game)
  5. Insert vào bảng `snapshots` (metrics ngày hôm nay)

Biến môi trường cần có:
  SUPABASE_URL  — URL project Supabase
  SUPABASE_KEY  — Service role key (không phải anon key)
  STEAM_API_KEY — Key miễn phí tại steamcommunity.com/dev/apikey
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone

import requests
from supabase import create_client, Client

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Khởi tạo Supabase client ─────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
STEAM_API_KEY = os.environ.get("STEAM_API_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Thiếu SUPABASE_URL hoặc SUPABASE_KEY")
    sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Constants ─────────────────────────────────────────────────────────────────
STEAMSPY_TOP100   = "https://steamspy.com/api.php?request=top100in2weeks"
STEAMSPY_APPDATA  = "https://steamspy.com/api.php?request=appdetails&appid={app_id}"
STOREFRONT_FEAT   = "https://store.steampowered.com/api/featuredcategories?cc=us&l=en"
STORE_APPDETAILS  = "https://store.steampowered.com/api/appdetails?appids={app_id}&cc=us&l=en"
STEAM_CONCURRENT  = (
    "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    "?appid={app_id}&key={key}"
)
STEAM_CDN_HEADER  = "https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/header.jpg"
STEAM_CDN_CAPSULE = "https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/capsule_231x87.jpg"
STEAM_STORE_URL   = "https://store.steampowered.com/app/{app_id}"

REQUEST_TIMEOUT   = 15   # giây
STEAMSPY_DELAY    = 0.7  # giây giữa các request tránh rate limit
BATCH_SIZE        = 50   # số game xử lý mỗi lần


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_get(url: str, retries: int = 3, delay: float = 2.0) -> dict | list | None:
    """GET request với retry tự động."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            log.warning("HTTP %s cho %s", resp.status_code, url[:80])
        except requests.RequestException as exc:
            log.warning("Attempt %d/%d thất bại: %s", attempt + 1, retries, exc)
        time.sleep(delay * (attempt + 1))
    return None


def chunk(lst: list, size: int):
    """Chia list thành các batch nhỏ."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


# ── Phase 1: SteamSpy top 100 ────────────────────────────────────────────────

def fetch_steamspy_top100() -> dict[int, dict]:
    """
    Trả về dict {app_id: data} cho 100 game trending nhất 2 tuần qua.
    SteamSpy cung cấp: owners, average_forever, positive, negative, price.
    """
    log.info("Đang lấy SteamSpy top 100...")
    data = safe_get(STEAMSPY_TOP100)
    if not data:
        log.error("Không lấy được SteamSpy top 100")
        return {}

    result = {}
    for rank, (app_id_str, info) in enumerate(data.items(), start=1):
        try:
            app_id = int(app_id_str)
            result[app_id] = {
                **info,
                "rank_trending": rank,
            }
        except (ValueError, TypeError):
            continue

    log.info("SteamSpy: %d games", len(result))
    return result


# ── Phase 2: Storefront API — featured & top sellers ─────────────────────────

def fetch_storefront_featured() -> dict[int, dict]:
    """
    Lấy danh sách game từ Steam Storefront (top sellers, specials/deals).
    Trả về dict {app_id: {rank_sellers, discount_pct, price_current}}.
    """
    log.info("Đang lấy Storefront featured categories...")
    data = safe_get(STOREFRONT_FEAT)
    if not data:
        return {}

    result: dict[int, dict] = {}

    # Top sellers
    top_sellers = data.get("top_sellers", {}).get("items", [])
    for rank, item in enumerate(top_sellers, start=1):
        app_id = item.get("id")
        if not app_id:
            continue
        result[app_id] = {
            "rank_sellers": rank,
            "discount_pct": item.get("discount_percent", 0),
            "price_current": item.get("final_price", 0) / 100,  # cents → USD
        }

    # Specials (deals) — chỉ lấy discount info nếu game chưa có
    specials = data.get("specials", {}).get("items", [])
    for item in specials:
        app_id = item.get("id")
        if not app_id:
            continue
        if app_id not in result:
            result[app_id] = {}
        result[app_id].setdefault("discount_pct", item.get("discount_percent", 0))
        result[app_id].setdefault("price_current", item.get("final_price", 0) / 100)

    log.info("Storefront: %d games (top sellers + deals)", len(result))
    return result


# ── Phase 3: Store App Details (metadata đầy đủ) ─────────────────────────────

def fetch_app_details(app_id: int) -> dict | None:
    """
    Lấy chi tiết đầy đủ của 1 game từ Steam Store API.
    Bao gồm: genres, developer, publisher, price, screenshots.
    """
    url = STORE_APPDETAILS.format(app_id=app_id)
    data = safe_get(url)
    if not data:
        return None

    app_data = data.get(str(app_id), {})
    if not app_data.get("success"):
        return None

    details = app_data.get("data", {})
    if details.get("type") not in ("game", "dlc"):
        return None  # bỏ qua DLC, phim, v.v.

    # Lấy genres
    genres = [g["description"] for g in details.get("genres", [])]

    # Lấy tags (categories trong Steam)
    tags = [c["description"] for c in details.get("categories", [])][:10]

    # Price
    price_overview = details.get("price_overview", {})
    is_free = details.get("is_free", False)
    price_usd = price_overview.get("initial", 0) / 100 if price_overview else 0.0

    return {
        "name":         details.get("name", ""),
        "developer":    ", ".join(details.get("developers", [])),
        "publisher":    ", ".join(details.get("publishers", [])),
        "genres":       genres,
        "tags":         tags,
        "price_usd":    price_usd,
        "is_free":      is_free,
        "img_header":   STEAM_CDN_HEADER.format(app_id=app_id),
        "img_capsule":  STEAM_CDN_CAPSULE.format(app_id=app_id),
        "steam_url":    STEAM_STORE_URL.format(app_id=app_id),
    }


# ── Phase 4: Concurrent players real-time ────────────────────────────────────

def fetch_concurrent_players(app_ids: list[int]) -> dict[int, int]:
    """
    Lấy số người đang chơi đồng thời cho danh sách app_ids.
    Yêu cầu STEAM_API_KEY.
    """
    if not STEAM_API_KEY:
        log.warning("Không có STEAM_API_KEY, bỏ qua concurrent players")
        return {}

    result: dict[int, int] = {}
    for app_id in app_ids:
        url = STEAM_CONCURRENT.format(app_id=app_id, key=STEAM_API_KEY)
        data = safe_get(url)
        if data and "response" in data:
            count = data["response"].get("player_count", 0)
            result[app_id] = count
        time.sleep(0.2)

    log.info("Concurrent players: %d games", len(result))
    return result


# ── Phase 5: Upsert vào Supabase ─────────────────────────────────────────────

def upsert_games(games_data: list[dict]) -> int:
    """Upsert danh sách game vào bảng `games`. Trả về số record upserted."""
    if not games_data:
        return 0
    try:
        sb.table("games").upsert(
            games_data,
            on_conflict="app_id",
        ).execute()
        return len(games_data)
    except Exception as exc:
        log.error("Upsert games thất bại: %s", exc)
        return 0


def insert_snapshots(snapshots: list[dict]) -> int:
    """Insert snapshots của ngày hôm nay. Trả về số record inserted."""
    if not snapshots:
        return 0
    try:
        sb.table("snapshots").insert(snapshots).execute()
        return len(snapshots)
    except Exception as exc:
        log.error("Insert snapshots thất bại: %s", exc)
        return 0


# ── Main pipeline ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Steam Tracker — fetch_data.py bắt đầu")
    log.info("Thời gian: %s UTC", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
    log.info("=" * 60)

    # ── 1. Thu thập từ các nguồn ──────────────────────────────
    steamspy_data   = fetch_steamspy_top100()        # {app_id: {...}}
    storefront_data = fetch_storefront_featured()    # {app_id: {...}}

    # Hợp nhất danh sách app_ids (ưu tiên SteamSpy top 100 + thêm từ storefront)
    all_app_ids = list(set(list(steamspy_data.keys()) + list(storefront_data.keys())))
    log.info("Tổng %d app_ids cần xử lý", len(all_app_ids))

    # Lấy concurrent players cho top 50 (tránh quá nhiều request)
    top50_ids = list(steamspy_data.keys())[:50]
    concurrent_data = fetch_concurrent_players(top50_ids)

    # ── 2. Xử lý từng game, lấy app details ──────────────────
    games_to_upsert   = []
    snapshots_to_insert = []
    now_utc = datetime.now(timezone.utc).isoformat()

    for i, app_id in enumerate(all_app_ids[:BATCH_SIZE], start=1):
        log.info("[%d/%d] Xử lý app_id=%d", i, min(len(all_app_ids), BATCH_SIZE), app_id)

        # Lấy app details từ Steam Store
        details = fetch_app_details(app_id)
        time.sleep(0.5)  # tránh rate limit Store API

        if not details:
            # Fallback: dùng tên từ SteamSpy nếu Store API không trả về
            spy = steamspy_data.get(app_id, {})
            if not spy.get("name"):
                log.debug("Bỏ qua app_id=%d (không có data)", app_id)
                continue
            details = {
                "name":        spy.get("name", f"App {app_id}"),
                "developer":   spy.get("developer", ""),
                "publisher":   spy.get("publisher", ""),
                "genres":      [],
                "tags":        [],
                "price_usd":   spy.get("price", 0) / 100,
                "is_free":     spy.get("price", 0) == 0,
                "img_header":  STEAM_CDN_HEADER.format(app_id=app_id),
                "img_capsule": STEAM_CDN_CAPSULE.format(app_id=app_id),
                "steam_url":   STEAM_STORE_URL.format(app_id=app_id),
            }

        # Review score từ SteamSpy
        spy = steamspy_data.get(app_id, {})
        positive = spy.get("positive", 0)
        negative = spy.get("negative", 0)
        total = positive + negative
        review_pct   = round(positive / total * 100) if total > 0 else None
        review_count = total if total > 0 else None

        # Game record (upsert)
        games_to_upsert.append({
            "app_id":       app_id,
            "name":         details["name"],
            "developer":    details.get("developer"),
            "publisher":    details.get("publisher"),
            "genres":       details.get("genres", []),
            "tags":         details.get("tags", []),
            "price_usd":    details.get("price_usd"),
            "review_pct":   review_pct,
            "review_count": review_count,
            "img_header":   details["img_header"],
            "img_capsule":  details["img_capsule"],
            "steam_url":    details["steam_url"],
            "is_free":      details.get("is_free", False),
            "updated_at":   now_utc,
        })

        # Snapshot record (insert lịch sử)
        store_info = storefront_data.get(app_id, {})
        snapshots_to_insert.append({
            "app_id":          app_id,
            "concurrent_peak": concurrent_data.get(app_id),
            "owners_estimate": spy.get("owners_count"),  # SteamSpy trả về range string, parse bên dưới
            "discount_pct":    store_info.get("discount_pct", 0),
            "price_current":   store_info.get("price_current", details.get("price_usd")),
            "rank_trending":   spy.get("rank_trending"),
            "rank_sellers":    store_info.get("rank_sellers"),
            "captured_at":     now_utc,
        })

        time.sleep(STEAMSPY_DELAY)

    # ── 3. Lưu vào Supabase ───────────────────────────────────
    log.info("-" * 40)
    log.info("Đang upsert %d games vào Supabase...", len(games_to_upsert))

    # Upsert games theo batch nhỏ để tránh payload quá lớn
    total_upserted = 0
    for batch in chunk(games_to_upsert, 20):
        total_upserted += upsert_games(batch)
        time.sleep(0.3)

    log.info("Đang insert %d snapshots...", len(snapshots_to_insert))
    total_inserted = 0
    for batch in chunk(snapshots_to_insert, 20):
        total_inserted += insert_snapshots(batch)
        time.sleep(0.3)

    # ── 4. Summary ────────────────────────────────────────────
    log.info("=" * 60)
    log.info("Hoàn thành!")
    log.info("  Games upserted : %d", total_upserted)
    log.info("  Snapshots saved: %d", total_inserted)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
