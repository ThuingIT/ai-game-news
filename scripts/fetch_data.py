"""
fetch_data.py  v4
=================
Thêm: new_releases — game mới ra trong 14 ngày, lọc theo chất lượng
New: positive_reviews, negative_reviews, owners_text từ SteamSpy
New: release_date, days_since_release cho bảng games
"""

import os, sys, time, logging
from datetime import datetime, timezone, timedelta
import requests
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

SUPABASE_URL  = os.environ.get("SUPABASE_URL","")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY","")
STEAM_API_KEY = os.environ.get("STEAM_API_KEY","")

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Thiếu SUPABASE_URL hoặc SUPABASE_KEY"); sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

STEAMSPY_TOP100   = "https://steamspy.com/api.php?request=top100in2weeks"
# SteamSpy endpoint lấy game mới release trong khoảng thời gian
STEAMSPY_NEW      = "https://steamspy.com/api.php?request=all&page=0"
STORE_APPDETAILS  = "https://store.steampowered.com/api/appdetails?appids={app_id}&cc=us&l=en"
STOREFRONT_FEAT   = "https://store.steampowered.com/api/featuredcategories?cc=us&l=en"
# Steam API tìm game mới theo release date
STEAM_NEWSRELEASE = "https://store.steampowered.com/api/featuredcategories?cc=us&l=en"
STEAM_CONCURRENT  = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}&key={key}"
CDN_HEADER        = "https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/header.jpg"
CDN_CAPSULE       = "https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/capsule_231x87.jpg"
STORE_URL         = "https://store.steampowered.com/app/{app_id}"
BATCH_SIZE        = 60

# ── Ngưỡng lọc game mới ────────────────────────────────────────────────────────
NEW_RELEASE_DAYS     = 14     # game ra trong vòng 14 ngày
NEW_MIN_REVIEW_PCT   = 70     # review tích cực tối thiểu 70%
NEW_MIN_REVIEW_COUNT = 100    # ít nhất 100 reviews
NEW_MIN_PLAYERS      = 500    # ít nhất 500 players online
NEW_MAX_GAMES        = 20     # lưu tối đa 20 game mới mỗi lần chạy


def safe_get(url, retries=3, delay=2.0):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                return r.json()
            log.warning("HTTP %s: %s", r.status_code, url[:70])
        except requests.RequestException as e:
            log.warning("Attempt %d/%d: %s", i+1, retries, e)
        time.sleep(delay * (i+1))
    return None


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def parse_owners_text(owners_str: str) -> tuple[str, int | None]:
    if not owners_str:
        return "", None
    try:
        parts = [p.strip().replace(",","") for p in owners_str.split("..")]
        low  = int(parts[0])
        high = int(parts[1]) if len(parts) > 1 else low
        mid  = (low + high) // 2
        def fmt(n):
            if n >= 1_000_000: return f"{n/1_000_000:.0f}M"
            if n >= 1_000:     return f"{n/1_000:.0f}K"
            return str(n)
        return f"~{fmt(low)}–{fmt(high)}", mid
    except Exception:
        return owners_str, None


def fetch_steamspy_top100() -> dict[int, dict]:
    log.info("SteamSpy top100...")
    data = safe_get(STEAMSPY_TOP100)
    if not data:
        return {}
    result = {}
    for rank, (aid_str, info) in enumerate(data.items(), 1):
        try:
            result[int(aid_str)] = {**info, "rank_trending": rank}
        except (ValueError, TypeError):
            continue
    log.info("SteamSpy: %d games", len(result))
    return result


def fetch_storefront() -> dict[int, dict]:
    log.info("Storefront API...")
    data = safe_get(STOREFRONT_FEAT)
    if not data:
        return {}
    result = {}
    for rank, item in enumerate(data.get("top_sellers",{}).get("items",[]), 1):
        aid = item.get("id")
        if aid:
            result[aid] = {
                "rank_sellers":  rank,
                "discount_pct":  item.get("discount_percent", 0),
                "price_current": item.get("final_price", 0) / 100,
            }
    for item in data.get("specials",{}).get("items",[]):
        aid = item.get("id")
        if aid and aid not in result:
            result[aid] = {
                "discount_pct":  item.get("discount_percent", 0),
                "price_current": item.get("final_price", 0) / 100,
            }
    log.info("Storefront: %d games", len(result))
    return result


def fetch_new_release_candidates() -> list[int]:
    """
    Lấy danh sách app_id của game mới ra từ Steam storefront.
    Dùng nhiều nguồn: new_releases từ featuredcategories + coming_soon + top_sellers gần đây.
    """
    log.info("Fetching new release candidates...")
    candidates = set()

    data = safe_get(STOREFRONT_FEAT)
    if data:
        # new_releases section
        for item in data.get("new_releases", {}).get("items", []):
            aid = item.get("id")
            if aid:
                candidates.add(aid)
        # coming_soon (vừa ra)
        for item in data.get("coming_soon", {}).get("items", []):
            aid = item.get("id")
            if aid:
                candidates.add(aid)
        # specials thường có nhiều game mới
        for item in data.get("specials", {}).get("items", []):
            aid = item.get("id")
            if aid:
                candidates.add(aid)
        # top_sellers cũng bắt được game mới hot
        for item in data.get("top_sellers", {}).get("items", []):
            aid = item.get("id")
            if aid:
                candidates.add(aid)

    log.info("New release candidates: %d app_ids", len(candidates))
    return list(candidates)


def fetch_app_details(app_id: int) -> dict | None:
    data = safe_get(STORE_APPDETAILS.format(app_id=app_id))
    if not data:
        return None
    app_data = data.get(str(app_id), {})
    if not app_data.get("success"):
        return None
    d = app_data.get("data", {})
    if d.get("type") not in ("game",):
        return None
    price = d.get("price_overview", {})

    # Parse release date
    release_date_raw = d.get("release_date", {})
    release_date_str = release_date_raw.get("date", "") if release_date_raw else ""
    release_date = None
    days_since   = None
    if release_date_str:
        for fmt in ("%b %d, %Y", "%d %b, %Y", "%B %d, %Y"):
            try:
                release_date = datetime.strptime(release_date_str, fmt).date()
                days_since   = (datetime.now(timezone.utc).date() - release_date).days
                break
            except ValueError:
                continue

    return {
        "name":          d.get("name",""),
        "developer":     ", ".join(d.get("developers",[])),
        "publisher":     ", ".join(d.get("publishers",[])),
        "genres":        [g["description"] for g in d.get("genres",[])],
        "tags":          [c["description"] for c in d.get("categories",[])][:10],
        "price_usd":     price.get("initial",0)/100 if price else 0.0,
        "is_free":       d.get("is_free", False),
        "img_header":    CDN_HEADER.format(app_id=app_id),
        "img_capsule":   CDN_CAPSULE.format(app_id=app_id),
        "steam_url":     STORE_URL.format(app_id=app_id),
        "release_date":  str(release_date) if release_date else None,
        "days_since_release": days_since,
        "short_desc":    d.get("short_description","")[:300],
    }


def fetch_concurrent(app_ids: list[int]) -> dict[int, int]:
    if not STEAM_API_KEY:
        return {}
    result = {}
    for aid in app_ids:
        data = safe_get(STEAM_CONCURRENT.format(app_id=aid, key=STEAM_API_KEY))
        if data and "response" in data:
            result[aid] = data["response"].get("player_count", 0)
        time.sleep(0.2)
    return result


def process_new_releases(spy_data: dict, concurrent: dict) -> list[dict]:
    """
    Lọc và xử lý game mới từ candidates.
    Điều kiện:
      - release_date trong 14 ngày gần đây (từ Steam appdetails)
      - review_pct >= 70%
      - review_count >= 100
      - concurrent_peak >= 500 (nếu có STEAM_API_KEY) hoặc owners_mid >= 10K
    """
    log.info("Processing new releases...")
    candidates_ids = fetch_new_release_candidates()

    # Gộp thêm từ spy_data nếu game có vẻ mới (ít reviews nhưng ratio cao)
    for aid, info in spy_data.items():
        pos = info.get("positive", 0)
        neg = info.get("negative", 0)
        tot = pos + neg
        if tot > 0 and tot < 5000 and pos/tot >= 0.80:
            candidates_ids.append(aid)

    candidates_ids = list(set(candidates_ids))
    log.info("Total candidates to check: %d", len(candidates_ids))

    new_games = []
    checked   = 0

    for app_id in candidates_ids:
        if len(new_games) >= NEW_MAX_GAMES * 3:  # lấy dư để sau filter còn đủ
            break
        checked += 1
        details = fetch_app_details(app_id)
        time.sleep(0.6)

        if not details:
            continue

        days = details.get("days_since_release")
        if days is None or days > NEW_RELEASE_DAYS:
            continue  # không phải game mới

        spy = spy_data.get(app_id, {})
        pos = spy.get("positive", 0)
        neg = spy.get("negative", 0)
        tot = pos + neg
        review_pct   = round(pos/tot*100) if tot > 0 else None
        review_count = tot

        # Filter chất lượng
        if review_pct is not None and review_pct < NEW_MIN_REVIEW_PCT:
            log.info("  Skip %s: review %d%% < %d%%", details["name"], review_pct, NEW_MIN_REVIEW_PCT)
            continue
        if review_count < NEW_MIN_REVIEW_COUNT:
            log.info("  Skip %s: only %d reviews", details["name"], review_count)
            continue

        players = concurrent.get(app_id, 0)
        owners_text, owners_mid = parse_owners_text(spy.get("owners",""))

        # Nếu không có API key, dùng owners_mid làm proxy
        if STEAM_API_KEY:
            if players < NEW_MIN_PLAYERS:
                log.info("  Skip %s: only %d players", details["name"], players)
                continue
        else:
            if (owners_mid or 0) < 10_000:
                log.info("  Skip %s: owners too low", details["name"])
                continue

        # Tính launch_score: composite để rank
        # Trọng số: review_pct (40%) + velocity players (30%) + review_count_norm (30%)
        rev_score   = (review_pct or 0) / 100
        player_norm = min(players / 50_000, 1.0) if players else min((owners_mid or 0) / 500_000, 1.0)
        count_norm  = min(review_count / 5_000, 1.0)
        launch_score = round(rev_score * 0.4 + player_norm * 0.3 + count_norm * 0.3, 4)

        new_games.append({
            "app_id":              app_id,
            "name":                details["name"],
            "developer":           details.get("developer"),
            "publisher":           details.get("publisher"),
            "genres":              details.get("genres",[]),
            "tags":                details.get("tags",[]),
            "price_usd":           details.get("price_usd"),
            "review_pct":          review_pct,
            "review_count":        review_count if review_count > 0 else None,
            "positive_reviews":    pos if pos > 0 else None,
            "negative_reviews":    neg if neg > 0 else None,
            "owners_text":         owners_text or None,
            "img_header":          details["img_header"],
            "img_capsule":         details["img_capsule"],
            "steam_url":           details["steam_url"],
            "is_free":             details.get("is_free", False),
            "release_date":        details.get("release_date"),
            "days_since_release":  days,
            "short_desc":          details.get("short_desc",""),
            "concurrent_peak":     players,
            "owners_mid":          owners_mid,
            "launch_score":        launch_score,
            "updated_at":          datetime.now(timezone.utc).isoformat(),
        })
        log.info("  ✓ New game: %s (day %d, review %s%%, score %.3f)",
                 details["name"], days, review_pct, launch_score)

    # Sort theo launch_score giảm dần
    new_games.sort(key=lambda x: x["launch_score"], reverse=True)
    result = new_games[:NEW_MAX_GAMES]
    log.info("New releases qualified: %d/%d checked", len(result), checked)
    return result


def main():
    log.info("="*55)
    log.info("fetch_data.py v4 — %s UTC", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
    log.info("="*55)

    spy_data   = fetch_steamspy_top100()
    store_data = fetch_storefront()
    all_ids    = list(dict.fromkeys(list(spy_data.keys()) + list(store_data.keys())))
    concurrent = fetch_concurrent(list(spy_data.keys())[:50])

    now_utc = datetime.now(timezone.utc).isoformat()
    games_batch, snap_batch = [], []

    for i, app_id in enumerate(all_ids[:BATCH_SIZE], 1):
        log.info("[%d/%d] app_id=%d", i, min(len(all_ids), BATCH_SIZE), app_id)
        details = fetch_app_details(app_id)
        time.sleep(0.5)

        spy = spy_data.get(app_id, {})
        if not details:
            if not spy.get("name"):
                continue
            details = {
                "name":       spy.get("name", f"App {app_id}"),
                "developer":  spy.get("developer",""),
                "publisher":  spy.get("publisher",""),
                "genres":[], "tags":[],
                "price_usd":  spy.get("price",0)/100,
                "is_free":    spy.get("price",0) == 0,
                "img_header":  CDN_HEADER.format(app_id=app_id),
                "img_capsule": CDN_CAPSULE.format(app_id=app_id),
                "steam_url":   STORE_URL.format(app_id=app_id),
                "release_date": None,
                "days_since_release": None,
                "short_desc": "",
            }

        pos = spy.get("positive", 0)
        neg = spy.get("negative", 0)
        tot = pos + neg
        owners_text, owners_mid = parse_owners_text(spy.get("owners",""))

        games_batch.append({
            "app_id":             app_id,
            "name":               details["name"],
            "developer":          details.get("developer"),
            "publisher":          details.get("publisher"),
            "genres":             details.get("genres",[]),
            "tags":               details.get("tags",[]),
            "price_usd":          details.get("price_usd"),
            "review_pct":         round(pos/tot*100) if tot > 0 else None,
            "review_count":       tot if tot > 0 else None,
            "positive_reviews":   pos if pos > 0 else None,
            "negative_reviews":   neg if neg > 0 else None,
            "owners_text":        owners_text or None,
            "img_header":         details["img_header"],
            "img_capsule":        details["img_capsule"],
            "steam_url":          details["steam_url"],
            "is_free":            details.get("is_free", False),
            "release_date":       details.get("release_date"),
            "days_since_release": details.get("days_since_release"),
            "updated_at":         now_utc,
        })

        store = store_data.get(app_id, {})
        snap_batch.append({
            "app_id":          app_id,
            "concurrent_peak": concurrent.get(app_id),
            "owners_estimate": owners_mid,
            "discount_pct":    store.get("discount_pct", 0),
            "price_current":   store.get("price_current", details.get("price_usd")),
            "rank_trending":   spy.get("rank_trending"),
            "rank_sellers":    store.get("rank_sellers"),
            "captured_at":     now_utc,
        })
        time.sleep(0.4)

    # ── Process new releases ───────────────────────────────────
    log.info("\n── New Releases ──")
    new_releases = process_new_releases(spy_data, concurrent)

    # Upsert new_releases vào bảng riêng
    if new_releases:
        log.info("Upserting %d new releases...", len(new_releases))
        for batch in chunk(new_releases, 10):
            try:
                sb.table("new_releases").upsert(batch, on_conflict="app_id").execute()
            except Exception as e:
                log.error("upsert new_releases: %s", e)
            time.sleep(0.3)

        # Cũng upsert vào games table để có đầy đủ data
        nr_games = [{k: v for k, v in g.items()
                     if k not in ("concurrent_peak","owners_mid","launch_score","short_desc","days_since_release")}
                    for g in new_releases]
        for batch in chunk(nr_games, 10):
            try:
                sb.table("games").upsert(batch, on_conflict="app_id").execute()
            except Exception as e:
                log.error("upsert games (new): %s", e)

    # ── Main games upsert ──────────────────────────────────────
    log.info("Upserting %d games...", len(games_batch))
    for batch in chunk(games_batch, 20):
        try:
            sb.table("games").upsert(batch, on_conflict="app_id").execute()
        except Exception as e:
            log.error("upsert: %s", e)
        time.sleep(0.3)

    log.info("Inserting %d snapshots...", len(snap_batch))
    for batch in chunk(snap_batch, 20):
        try:
            sb.table("snapshots").insert(batch).execute()
        except Exception as e:
            log.error("insert: %s", e)
        time.sleep(0.3)

    log.info("Done! games=%d snapshots=%d new_releases=%d",
             len(games_batch), len(snap_batch), len(new_releases))


if __name__ == "__main__":
    main()
