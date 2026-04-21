"""
fetch_data.py  v3
=================
Thêm: positive_reviews, negative_reviews, owners_text từ SteamSpy
"""

import os, sys, time, logging
from datetime import datetime, timezone
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

STEAMSPY_TOP100  = "https://steamspy.com/api.php?request=top100in2weeks"
STORE_APPDETAILS = "https://store.steampowered.com/api/appdetails?appids={app_id}&cc=us&l=en"
STOREFRONT_FEAT  = "https://store.steampowered.com/api/featuredcategories?cc=us&l=en"
STEAM_CONCURRENT = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}&key={key}"
CDN_HEADER       = "https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/header.jpg"
CDN_CAPSULE      = "https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/capsule_231x87.jpg"
STORE_URL        = "https://store.steampowered.com/app/{app_id}"
BATCH_SIZE       = 60


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
    """SteamSpy 'owners' field: '2,000,000 .. 5,000,000' → ('~2M–5M', 3500000)"""
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
    return {
        "name":        d.get("name",""),
        "developer":   ", ".join(d.get("developers",[])),
        "publisher":   ", ".join(d.get("publishers",[])),
        "genres":      [g["description"] for g in d.get("genres",[])],
        "tags":        [c["description"] for c in d.get("categories",[])][:10],
        "price_usd":   price.get("initial",0)/100 if price else 0.0,
        "is_free":     d.get("is_free", False),
        "img_header":  CDN_HEADER.format(app_id=app_id),
        "img_capsule": CDN_CAPSULE.format(app_id=app_id),
        "steam_url":   STORE_URL.format(app_id=app_id),
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


def main():
    log.info("="*55)
    log.info("fetch_data.py v3 — %s UTC", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
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
            }

        pos = spy.get("positive", 0)
        neg = spy.get("negative", 0)
        tot = pos + neg
        owners_text, owners_mid = parse_owners_text(spy.get("owners",""))

        games_batch.append({
            "app_id":           app_id,
            "name":             details["name"],
            "developer":        details.get("developer"),
            "publisher":        details.get("publisher"),
            "genres":           details.get("genres",[]),
            "tags":             details.get("tags",[]),
            "price_usd":        details.get("price_usd"),
            "review_pct":       round(pos/tot*100) if tot > 0 else None,
            "review_count":     tot if tot > 0 else None,
            "positive_reviews": pos if pos > 0 else None,
            "negative_reviews": neg if neg > 0 else None,
            "owners_text":      owners_text or None,
            "img_header":       details["img_header"],
            "img_capsule":      details["img_capsule"],
            "steam_url":        details["steam_url"],
            "is_free":          details.get("is_free", False),
            "updated_at":       now_utc,
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

    log.info("Done! games=%d snapshots=%d", len(games_batch), len(snap_batch))


if __name__ == "__main__":
    main()
