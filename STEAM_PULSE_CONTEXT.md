# STEAM_PULSE_CONTEXT.md — Steam Pulse Tracker
> **Dành cho AI (Claude).** File tổng quan dự án Steam Pulse để debug và phát triển mà không cần xem lại toàn bộ repo.
> Cập nhật file này khi có thay đổi lớn về schema, logic, hoặc thêm tính năng mới.

---

## 1. MỤC ĐÍCH DỰ ÁN

Pipeline tự động thu thập dữ liệu Steam hàng ngày, phân tích bằng Groq AI, và publish dashboard tĩnh lên GitHub Pages. Chạy 1 lần/ngày lúc 06:00 ICT.

Dashboard hiển thị: trending games, surge detection, deals, **new releases**, charts, AI insights.

---

## 2. KIẾN TRÚC TỔNG QUAN

```
GitHub Actions  (update.yml — cron 0 23 * * * UTC = 06:00 ICT)
│
├── Step 1: scripts/fetch_data.py        →  crawl Steam API + SteamSpy → Supabase
├── Step 2: scripts/groq_insights.py     →  Groq AI generate 5 insight types → Supabase
├── Step 3: scripts/generate_html.py     →  query Supabase → render Jinja2 → docs/index.html
└── Deploy:  peaceiris/actions-gh-pages  →  docs/ → gh-pages branch
```

**Không có job dependencies (artifact)** — tất cả chạy tuần tự trong 1 job duy nhất. Env vars truyền trực tiếp qua `env:` trong từng step.

**Không có hourly run** — chỉ 1 pipeline duy nhất mỗi ngày.

---

## 3. FILES TRONG REPO

| File | Vai trò | Ghi chú |
|------|---------|---------|
| `scripts/fetch_data.py` | Crawl Steam + SteamSpy → upsert Supabase | v4, có new_releases |
| `scripts/groq_insights.py` | Groq AI → 5 insight types → ai_insights table | v2, có new_releases_buzz |
| `scripts/generate_html.py` | Query Supabase → render Jinja2 → docs/index.html | v4 |
| `templates/index.html.j2` | Jinja2 template chính | Không có new_releases panel trong template này (chỉ có trong docs/index.html tĩnh) |
| `docs/index.html` | HTML tĩnh được generate — đây là file GitHub Pages serve | **KHÔNG sửa tay** — bị overwrite mỗi ngày |
| `.github/workflows/update.yml` | CI pipeline | 1 job, 4 steps |
| `supabase_setup.sql` | Schema gốc | Chạy đầu tiên |
| `supabase_v3.sql` | Migration v3 — thêm surge, review breakdown, positive/negative cols | Chạy sau setup |
| `supabase_fix_v2.sql` | Fix views — dedupe + DISTINCT ON + nới lỏng điều kiện | Chạy sau v3 |
| `supabase_v4.sql` | Migration v4 — tạo bảng `new_releases` + thêm `release_date`/`days_since_release` vào `games` | Chạy sau fix_v2 |
| `requirements.txt` | supabase, groq, requests, jinja2, python-dotenv | Python 3.12 |

### ✅ Template đã đầy đủ
`templates/index.html.j2` **đã có** panel "Game Mới" với đầy đủ CSS (`.nr-*`, `.feat-*`, `.score-meter`, `.nrc`, `.days-badge`, `.vel-badge`, `.buzz-card`), tab `new-tab`, và JS animate launch score bars via `data-target` attribute.

---

## 4. SUPABASE SCHEMA

### Bảng chính

```
games            app_id (PK INT), name, developer, publisher, genres TEXT[],
                 tags TEXT[], price_usd, review_pct, review_count,
                 positive_reviews, negative_reviews, owners_text,
                 img_header, img_capsule, steam_url, is_free,
                 release_date, days_since_release, updated_at

snapshots        id BIGSERIAL PK, app_id→games, concurrent_peak,
                 owners_estimate, discount_pct, price_current,
                 rank_trending, rank_sellers, captured_at TIMESTAMPTZ
                 (append-only — KHÔNG upsert, mỗi lần chạy INSERT mới)

ai_insights      id BIGSERIAL PK, insight_type TEXT, content TEXT,
                 model_used, token_count, generated_at TIMESTAMPTZ

new_releases     app_id (PK INT), name, developer, genres TEXT[],
                 review_pct, review_count, positive_reviews, negative_reviews,
                 concurrent_peak, owners_text, owners_mid, price_usd, is_free,
                 img_header, img_capsule, steam_url,
                 release_date, days_since_release,
                 launch_score NUMERIC, short_desc,
                 updated_at TIMESTAMPTZ
                 (upsert on app_id — chỉ giữ game trong 14 ngày)
```

### Views (đều là regular views, KHÔNG phải materialized)

```
v_trending_today     DISTINCT ON(app_id) snapshot mới nhất 26h, composite display_score,
                     có player_change_pct so với hôm qua
v_surge_today        Game tăng >10% so hôm qua (cần ≥2 ngày snapshot)
v_deals_today        sale≥20%, review≥65%, review_count≥50, is_free=FALSE
v_history_7days      DISTINCT ON(app_id, date) mỗi ngày → dùng cho line chart
v_genre_stats_today  UNNEST(genres) → SUM(concurrent_peak) group by genre
v_stats_today        Tổng hợp: total_games, total_players, total_deals, avg_review_pct, top_game_name
```

### Thứ tự chạy SQL migrations
1. `supabase_setup.sql` — tạo games, snapshots, ai_insights + views gốc + RLS
2. `supabase_v3.sql` — DROP+recreate views, thêm columns positive/negative/owners_text
3. `supabase_fix_v2.sql` — DROP+recreate v_trending/v_deals với DISTINCT ON, nới lỏng thresholds
4. `supabase_v4.sql` — tạo bảng `new_releases` + thêm `release_date`, `days_since_release` vào `games`

---

## 5. DATA FLOW CHI TIẾT

### fetch_data.py (Step 1)
```
SteamSpy top100     → top 100 game trending 2 tuần
Steam Storefront    → top sellers, specials, discount info
Steam AppDetails    → metadata: genres, price, release_date, description
Steam Concurrent    → player count real-time (cần STEAM_API_KEY)

→ upsert games (on_conflict=app_id)
→ INSERT snapshots (append)
→ process_new_releases() → upsert new_releases (on_conflict=app_id)
```

**Nguồn dữ liệu:**
- `SteamSpy API` (không cần key): `steamspy.com/api.php?request=top100in2weeks`
- `Steam Store API` (không cần key): `store.steampowered.com/api/appdetails`, `featuredcategories`
- `Steam Web API` (cần key): `ISteamUserStats/GetNumberOfCurrentPlayers` — nếu không có key thì `concurrent_peak = 0`

**New Release filtering:**
```
days_since_release ≤ 14
review_pct ≥ 70%
review_count ≥ 100
concurrent_peak ≥ 500 (nếu có STEAM_API_KEY) hoặc owners_mid ≥ 10K
→ Launch Score = review_pct×0.4 + player_norm×0.3 + count_norm×0.3
→ sort by launch_score desc, giữ top 20
```

### groq_insights.py (Step 2)
```
5 insight types, mỗi loại 1 Groq call:
  1. trend_analysis     — top 25 trending
  2. deal_picks         — deals với điều kiện tốt
  3. hidden_gems        — game review cao nhưng rank >15 và player <50K
  4. weekly_summary     — tổng hợp ngắn 4 câu
  5. new_releases_buzz  — phân tích new_releases theo launch score + velocity

Model: llama-3.3-70b-versatile, temp=0.7, max_tokens=1024
Delay: 2s giữa các calls (tránh rate limit)
→ INSERT ai_insights (không upsert — mỗi ngày thêm rows mới)
```

**Đọc insight:** `generate_html.py` query `ai_insights` lấy row mới nhất per type:
```python
sb.table("ai_insights").select("content").eq("insight_type", t)
  .order("generated_at", desc=True).limit(1).execute()
```

### generate_html.py (Step 3)
```
Query: v_trending_today (limit 80 → dedupe → 24)
       v_surge_today (limit 12)
       v_deals_today (limit 16)
       v_genre_stats_today (limit 10)
       v_stats_today
       new_releases (sort launch_score, limit 20)
       v_history_7days (top 8 game → line chart)
       ai_insights (5 types)

→ Jinja2 render templates/index.html.j2
→ docs/index.html
```

**Jinja2 filters custom:**
- `commas`: format số có dấu phẩy (1,234)
- `usd`: format giá USD ($12.34)
- `abbr`: rút gọn số (1.2M, 340K)
- `sign`: format % với dấu (+3.5%, -2.1%)

**`md_to_html()`**: convert Groq markdown output → HTML (bold, ol, ul, p). Dùng cho tất cả insight content.

---

## 6. GITHUB ACTIONS — LƯU Ý

### Secrets cần thiết
```
SUPABASE_URL      — https://xxxx.supabase.co
SUPABASE_KEY      — service_role key (bypass RLS), KHÔNG dùng anon key
GROQ_API_KEY      — console.groq.com
STEAM_API_KEY     — steamcommunity.com/dev/apikey (optional — chỉ dùng concurrent players)
GITHUB_TOKEN      — tự động có sẵn
```

### Permissions
```yaml
permissions:
  contents: write   # ← BẮT BUỘC để push gh-pages branch
```
Thiếu dòng này → lỗi 403 khi deploy.

### Timeout
20 phút — đủ cho pipeline bình thường. fetch_data.py có thể chậm nếu crawl nhiều game mới (rate limit Steam API 0.5-0.6s/call).

---

## 7. TEMPLATE — NEW RELEASES PANEL (chi tiết kỹ thuật)

Panel Game Mới trong `templates/index.html.j2` dùng pattern sau:

**JS animate bars:** Không dùng JS inline `style.width` hardcode nữa — dùng `data-target` attribute:
```html
<div class="bar-fill" data-target="{{ ((score) * 100) | int }}%" style="width:0%"></div>
```
Sau đó JS `window.addEventListener('load')` sẽ set `el.style.width = el.dataset.target` cho tất cả `[data-target]` elements — cả featured bar lẫn tất cả `.nrc-bar-fill`.

**Velocity tính trong template:**
```jinja2
{% set velocity = (g.review_count / g.days_since_release) | int if g.review_count and g.days_since_release and g.days_since_release > 0 else 0 %}
```

**Days badge "fresh"** (màu xanh): `days_since_release <= 3`

**Hot velocity badge**: `velocity > 200`

**Empty state**: hiển thị khi `new_releases` list rỗng — có icon + mô tả điều kiện lọc.

---

## 8. DASHBOARD HTML — CẤU TRÚC

### Template: `templates/index.html.j2` (Jinja2)

Fonts: Bebas Neue (headings), DM Sans (body), DM Mono (mono/stats)
Charts: Chart.js 4.4.0 từ jsdelivr CDN

**Sections theo thứ tự trong template:**
1. Header (logo, updated_at, counts — thêm "X game mới" nếu `total_new > 0`)
2. Banner (weekly_summary)
3. Stats row (thêm ô "Game mới nổi" với class `new-c` nếu `total_new > 0`)
4. Tabs: Trending / Đột biến / Deals / **✦ Game Mới** / Charts / AI Insights
5. Panel Trending
6. Panel Surge
7. Panel Deals
8. **Panel Game Mới** (featured card + list + buzz card)
9. Panel Charts
10. Panel AI Insights (có thêm card `new_releases_buzz` với màu purple)
11. Footer

### Game card badges logic
```
is_free → pill.free "FREE"
discount_pct > 0 → pill.sale "-XX%"
player_change_pct > 20 → pill.surge "+XX%"
player_change_pct < -15 → pill.drop "XX%"
else → pill.change "±XX%"
```

### Template variables quan trọng
```python
trending        # list[dict] từ v_trending_today (dedupe, max 24)
surge           # list[dict] từ v_surge_today
deals           # list[dict] từ v_deals_today
genre_stat      # list[dict] từ v_genre_stats_today
stats           # dict từ v_stats_today
new_releases    # list[dict] từ new_releases table (sort launch_score)
total_new       # len(new_releases)
summary         # HTML từ md_to_html(insight("weekly_summary"))
trend_analysis  # HTML từ md_to_html(insight("trend_analysis"))
deal_picks      # HTML từ md_to_html(insight("deal_picks"))
hidden_gems     # HTML từ md_to_html(insight("hidden_gems"))
new_releases_buzz  # HTML từ md_to_html(insight("new_releases_buzz"))
chart_labels    # list[str] dates
chart_line      # list[dict] {label, data, color}
has_history     # bool
updated_at      # "dd/mm/yyyy HH:MM UTC"
top_game        # first item of trending (or None)
```

---

## 9. DEBUG CHECKLIST

### Step 1 (fetch_data) fail
- Lỗi `SUPABASE_URL/KEY` → kiểm tra secrets
- Lỗi upsert `new_releases` → bảng chưa được tạo (xem Section 7)
- SteamSpy trả về empty → API rate limit hoặc down → safe_get retry 3 lần, không fail hard
- Steam AppDetails 429 → quá nhiều request → tăng `time.sleep()` trong vòng lặp
- `days_since_release = None` cho nhiều game → release_date format lạ, thêm format pattern vào `fetch_app_details()`

### Step 2 (groq_insights) fail
- `new_releases` table empty → Step 1 chưa upsert được new_releases (xem trên)
- Groq rate limit → thêm `time.sleep()`, giảm `max_tokens`
- `md_to_html` render sai → Groq trả về format markdown không chuẩn → kiểm tra regex trong `generate_html.py`

### Step 3 (generate_html) fail
- `TemplateNotFound: index.html.j2` → kiểm tra `templates/` folder tồn tại, `TEMPLATES = ROOT / "templates"`
- View `v_surge_today` trả về empty → bình thường nếu chỉ có 1 ngày data (cần ≥2 ngày snapshot)
- `v_trending_today` trả về duplicate → chạy lại `supabase_fix_v2.sql`
- `chart_line` empty mặc dù có data → `v_history_7days` cần ≥2 ngày, xem `history()` function

### Dashboard trống / thiếu data
- Stats row có ô trống → `v_stats_today` có thể NULL nếu snapshot quá cũ (>26h)
- Surge panel "chưa đủ dữ liệu" → cần chạy ít nhất 2 ngày liên tiếp
- Charts không hiện → `has_history = False` → xem `v_history_7days`

### Deploy fail (403)
- Thiếu `permissions: contents: write` trong workflow → thêm vào (xem Section 6)

### New Releases panel không cập nhật
- `templates/index.html.j2` chưa có panel → generate_html.py ghi đè docs/index.html bằng version cũ
- Fix: thêm new_releases panel vào template (xem Section 7 + docs/index.html để tham khảo UI)

---

## 10. PHÁT TRIỂN THÊM — HƯỚNG DẪN

### Thêm new releases panel vào template
✅ **Đã hoàn thành** — panel `p-new` đã có trong `templates/index.html.j2`.

### Thêm insight type mới
1. `groq_insights.py` → thêm function `generate_xxx()`, gọi trong `tasks` list
2. `generate_html.py` → thêm `xxx=md_to_html(insight("xxx"))` vào `ctx`
3. `templates/index.html.j2` → thêm card trong panel AI Insights
4. `ai_insights` table → không cần migration (chỉ cần insert row mới với insight_type mới)

### Thêm nguồn data mới
1. Thêm function fetch trong `fetch_data.py`
2. Nếu cần bảng mới → viết CREATE TABLE SQL và thêm vào một file migration mới
3. Thêm view mới nếu cần → `supabase_vX.sql`

### Thay đổi threshold lọc
- New releases: `NEW_RELEASE_DAYS`, `NEW_MIN_REVIEW_PCT`, `NEW_MIN_REVIEW_COUNT`, `NEW_MIN_PLAYERS` trong `fetch_data.py`
- Deals: sửa trực tiếp trong `v_deals_today` view (discount_pct, review_pct, review_count)
- Surge: sửa `> 0.10` threshold trong `v_surge_today` view

### Thêm chart mới
1. `generate_html.py` → thêm data vào `ctx`
2. `templates/index.html.j2` → thêm `<canvas>` trong panel Charts
3. JS section ở cuối template → thêm `new Chart(...)` call

---

## 11. FILES CẦN XEM KHI DEBUG (yêu cầu gửi lại)

| Vấn đề | File cần xem |
|--------|-------------|
| Dashboard layout / CSS / UI | `templates/index.html.j2` |
| New releases panel / launch score | `templates/index.html.j2` (panel `p-new`) |
| Logic crawl / data thiếu | `scripts/fetch_data.py` |
| Insight content sai / format | `scripts/groq_insights.py` |
| Jinja2 render lỗi / variable | `scripts/generate_html.py` |
| View SQL logic sai | `supabase_v3.sql` + `supabase_fix_v2.sql` |
| Schema / table missing | `supabase_setup.sql` + `supabase_v4.sql` |
| CI fail / secret / permission | `.github/workflows/update.yml` |

---

## 12. TRẠNG THÁI HIỆN TẠI & TODO

### Đã hoạt động
- [x] Daily crawl Steam + SteamSpy
- [x] Groq AI 5 insight types (incl. new_releases_buzz)
- [x] Trending panel với composite display_score (free game penalty)
- [x] Surge detection (cần ≥2 ngày data)
- [x] Deals panel với điều kiện linh hoạt
- [x] Charts: line history, bar genre, horizontal bar surge
- [x] New releases data pipeline (`fetch_data.py` + `groq_insights.py`)
- [x] New releases UI — panel đầy đủ trong `templates/index.html.j2`
- [x] `supabase_v4.sql` — bảng `new_releases` + columns `release_date`/`days_since_release` trong `games`

### Biết trước sẽ empty khi mới deploy
- Surge panel: cần ≥2 ngày snapshot
- Line chart: cần ≥2 ngày snapshot
- All panels: cần chạy Step 1 thành công ít nhất 1 lần

### Cần làm (known gaps)
- [ ] Không có cleanup logic cho `new_releases` cũ hơn 14 ngày (SQL comment gợi ý: `DELETE FROM new_releases WHERE days_since_release > 21`)
- [ ] Không có cleanup logic cho `ai_insights` cũ (table sẽ phình theo thời gian)
- [ ] `days_since_release` trong `new_releases` không tự cập nhật — mỗi ngày fetch_data.py upsert lại thì mới refresh

---

*File này được tạo tự động — cập nhật khi có thay đổi lớn.*
