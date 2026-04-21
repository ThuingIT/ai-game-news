-- ============================================================
-- STEAM TRACKER — SQL Fixes v2
-- Chạy file này trong Supabase SQL Editor để replace các view cũ
-- ============================================================

-- ── Drop views cũ ────────────────────────────────────────────
DROP VIEW IF EXISTS v_trending_today;
DROP VIEW IF EXISTS v_deals_today;

-- ── View 1: Trending hôm nay — dedupe + lấy snapshot mới nhất ──
-- DISTINCT ON (app_id) lấy 1 dòng duy nhất mỗi game
-- Lọc bỏ free-to-play cực lớn (CS2, PUBG, CoD...) có thể override bằng
-- tham số min_price nếu muốn, hiện tại giữ free game nhưng giới hạn top 3 each
CREATE OR REPLACE VIEW v_trending_today AS
WITH latest_snapshot AS (
  -- Lấy snapshot MỚI NHẤT của mỗi game trong 26 giờ qua
  SELECT DISTINCT ON (app_id)
    app_id,
    concurrent_peak,
    owners_estimate,
    discount_pct,
    price_current,
    rank_trending,
    rank_sellers,
    captured_at
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '26 hours'
  ORDER BY app_id, captured_at DESC  -- DISTINCT ON lấy dòng đầu tiên theo captured_at DESC
),
ranked AS (
  SELECT
    g.app_id,
    g.name,
    g.developer,
    g.publisher,
    g.genres,
    g.tags,
    g.price_usd,
    g.review_pct,
    g.review_count,
    g.img_header,
    g.img_capsule,
    g.steam_url,
    g.is_free,
    s.concurrent_peak,
    s.owners_estimate,
    s.discount_pct,
    s.price_current,
    s.rank_trending,
    s.rank_sellers,
    s.captured_at,
    -- Tính score đa chiều: không chỉ dựa vào concurrent_peak
    -- Game free khổng lồ (CS2, PUBG) bị penalty để game trả tiền có cơ hội hiển thị
    CASE
      WHEN g.is_free AND s.concurrent_peak > 200000 THEN s.concurrent_peak * 0.3
      WHEN g.is_free AND s.concurrent_peak > 50000  THEN s.concurrent_peak * 0.5
      ELSE s.concurrent_peak
    END AS display_score
  FROM games g
  JOIN latest_snapshot s ON s.app_id = g.app_id
  WHERE g.name IS NOT NULL
)
SELECT *
FROM ranked
ORDER BY
  CASE WHEN rank_trending IS NOT NULL THEN rank_trending ELSE 9999 END ASC,
  display_score DESC NULLS LAST
LIMIT 60;


-- ── View 2: Deals — dedupe + điều kiện nới lỏng hơn ─────────
CREATE OR REPLACE VIEW v_deals_today AS
WITH latest_snapshot AS (
  SELECT DISTINCT ON (app_id)
    app_id,
    discount_pct,
    price_current,
    concurrent_peak,
    captured_at
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '26 hours'
  ORDER BY app_id, captured_at DESC
)
SELECT
  g.app_id,
  g.name,
  g.developer,
  g.genres,
  g.price_usd,
  g.review_pct,
  g.review_count,
  g.img_header,
  g.img_capsule,
  g.steam_url,
  g.is_free,
  s.discount_pct,
  s.price_current,
  s.concurrent_peak,
  s.captured_at,
  ROUND((COALESCE(g.price_usd, 0) - COALESCE(s.price_current, 0))::NUMERIC, 2) AS savings_usd
FROM games g
JOIN latest_snapshot s ON s.app_id = g.app_id
WHERE
  s.discount_pct >= 20           -- nới lỏng: 20% thay vì 30%
  AND g.review_pct >= 65         -- nới lỏng: 65% thay vì 70%
  AND g.review_count >= 50       -- nới lỏng: 50 thay vì 100
  AND g.is_free = FALSE          -- chỉ game có giá gốc mới có deal thật
  AND s.price_current IS NOT NULL
  AND s.price_current > 0
ORDER BY s.discount_pct DESC, g.review_pct DESC NULLS LAST
LIMIT 30;


-- ── View 3: Historical data cho chart theo ngày ──────────────
-- Dùng để vẽ line chart "player count 7 ngày qua" trên frontend
CREATE OR REPLACE VIEW v_history_7days AS
WITH daily AS (
  SELECT DISTINCT ON (app_id, date_trunc('day', captured_at))
    app_id,
    date_trunc('day', captured_at)::DATE AS snapshot_date,
    concurrent_peak,
    discount_pct,
    price_current,
    rank_trending
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '8 days'
  ORDER BY app_id, date_trunc('day', captured_at), captured_at DESC
)
SELECT
  d.app_id,
  g.name,
  g.is_free,
  d.snapshot_date,
  d.concurrent_peak,
  d.discount_pct,
  d.price_current,
  d.rank_trending
FROM daily d
JOIN games g ON g.app_id = d.app_id
ORDER BY d.app_id, d.snapshot_date;


-- ── View 4: Genre stats hôm nay ──────────────────────────────
CREATE OR REPLACE VIEW v_genre_stats_today AS
WITH latest_snapshot AS (
  SELECT DISTINCT ON (app_id)
    app_id, concurrent_peak
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '26 hours'
  ORDER BY app_id, captured_at DESC
),
genre_expanded AS (
  SELECT
    UNNEST(g.genres) AS genre,
    COALESCE(s.concurrent_peak, 0) AS concurrent_peak,
    1 AS game_count
  FROM games g
  JOIN latest_snapshot s ON s.app_id = g.app_id
  WHERE g.genres IS NOT NULL AND array_length(g.genres, 1) > 0
)
SELECT
  genre,
  SUM(concurrent_peak) AS total_players,
  COUNT(*) AS game_count
FROM genre_expanded
GROUP BY genre
ORDER BY total_players DESC
LIMIT 12;


-- ── View 5: Stats tổng hợp hôm nay ──────────────────────────
CREATE OR REPLACE VIEW v_stats_today AS
WITH latest_snapshot AS (
  SELECT DISTINCT ON (app_id)
    app_id, concurrent_peak, discount_pct, price_current
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '26 hours'
  ORDER BY app_id, captured_at DESC
)
SELECT
  COUNT(DISTINCT s.app_id)                                    AS total_games,
  SUM(s.concurrent_peak)                                      AS total_players,
  COUNT(*) FILTER (WHERE s.discount_pct >= 20 AND g.is_free = FALSE) AS total_deals,
  MAX(s.concurrent_peak)                                      AS peak_players,
  AVG(g.review_pct) FILTER (WHERE g.review_pct IS NOT NULL)  AS avg_review_pct,
  (SELECT g2.name FROM games g2
   JOIN latest_snapshot s2 ON s2.app_id = g2.app_id
   ORDER BY s2.concurrent_peak DESC NULLS LAST LIMIT 1)      AS top_game_name
FROM latest_snapshot s
JOIN games g ON g.app_id = s.app_id;
