-- ============================================================
-- STEAM TRACKER — SQL v3
-- Thêm: surge detection, player change %, review breakdown
-- Chạy trong Supabase SQL Editor
-- ============================================================

-- Thêm cột review breakdown vào bảng games nếu chưa có
ALTER TABLE games
  ADD COLUMN IF NOT EXISTS positive_reviews INTEGER,
  ADD COLUMN IF NOT EXISTS negative_reviews INTEGER,
  ADD COLUMN IF NOT EXISTS owners_text      TEXT;   -- "2,000,000 .. 5,000,000" từ SteamSpy

-- ── Drop & recreate views ─────────────────────────────────────

DROP VIEW IF EXISTS v_trending_today;
DROP VIEW IF EXISTS v_deals_today;
DROP VIEW IF EXISTS v_surge_today;
DROP VIEW IF EXISTS v_history_7days;
DROP VIEW IF EXISTS v_genre_stats_today;
DROP VIEW IF EXISTS v_stats_today;

-- ── View 1: Trending hôm nay — dedupe + composite score ──────
CREATE OR REPLACE VIEW v_trending_today AS
WITH today AS (
  SELECT DISTINCT ON (app_id)
    app_id, concurrent_peak, owners_estimate,
    discount_pct, price_current, rank_trending, rank_sellers, captured_at
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '26 hours'
  ORDER BY app_id, captured_at DESC
),
yesterday AS (
  SELECT DISTINCT ON (app_id)
    app_id, concurrent_peak AS peak_yesterday
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '50 hours'
    AND captured_at <  NOW() - INTERVAL '22 hours'
  ORDER BY app_id, captured_at DESC
),
combined AS (
  SELECT
    g.*,
    t.concurrent_peak,
    t.owners_estimate,
    t.discount_pct,
    t.price_current,
    t.rank_trending,
    t.rank_sellers,
    t.captured_at,
    y.peak_yesterday,
    -- % thay đổi so với hôm qua
    CASE
      WHEN y.peak_yesterday > 0 AND t.concurrent_peak IS NOT NULL
      THEN ROUND(((t.concurrent_peak - y.peak_yesterday)::NUMERIC / y.peak_yesterday) * 100, 1)
      ELSE NULL
    END AS player_change_pct,
    -- Display score: penalize free giants, boost paid games với review cao
    CASE
      WHEN g.is_free AND t.concurrent_peak > 200000 THEN t.concurrent_peak * 0.25
      WHEN g.is_free AND t.concurrent_peak > 50000  THEN t.concurrent_peak * 0.45
      ELSE COALESCE(t.concurrent_peak, 0) * (1 + COALESCE(g.review_pct, 70) / 200.0)
    END AS display_score
  FROM games g
  JOIN today t ON t.app_id = g.app_id
  LEFT JOIN yesterday y ON y.app_id = g.app_id
  WHERE g.name IS NOT NULL
)
SELECT * FROM combined
ORDER BY
  CASE WHEN rank_trending IS NOT NULL THEN rank_trending ELSE 9999 END,
  display_score DESC NULLS LAST
LIMIT 60;


-- ── View 2: Surge — game tăng đột biến so với hôm qua ───────
CREATE OR REPLACE VIEW v_surge_today AS
WITH today AS (
  SELECT DISTINCT ON (app_id)
    app_id, concurrent_peak, rank_trending, discount_pct, captured_at
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '26 hours'
  ORDER BY app_id, captured_at DESC
),
yesterday AS (
  SELECT DISTINCT ON (app_id)
    app_id, concurrent_peak AS peak_yesterday
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '50 hours'
    AND captured_at <  NOW() - INTERVAL '22 hours'
  ORDER BY app_id, captured_at DESC
)
SELECT
  g.app_id,
  g.name,
  g.developer,
  g.genres,
  g.review_pct,
  g.review_count,
  g.positive_reviews,
  g.negative_reviews,
  g.img_header,
  g.img_capsule,
  g.steam_url,
  g.is_free,
  g.price_usd,
  g.owners_text,
  t.concurrent_peak,
  y.peak_yesterday,
  t.discount_pct,
  ROUND(((t.concurrent_peak - y.peak_yesterday)::NUMERIC / y.peak_yesterday) * 100, 1) AS surge_pct,
  t.concurrent_peak - y.peak_yesterday AS surge_abs
FROM games g
JOIN today t     ON t.app_id = g.app_id
JOIN yesterday y ON y.app_id = g.app_id
WHERE
  y.peak_yesterday > 500           -- chỉ game đã có người chơi hôm qua
  AND t.concurrent_peak IS NOT NULL
  AND t.concurrent_peak > y.peak_yesterday  -- chỉ game tăng
  AND ((t.concurrent_peak - y.peak_yesterday)::NUMERIC / y.peak_yesterday) > 0.10  -- tăng > 10%
ORDER BY surge_pct DESC
LIMIT 20;


-- ── View 3: Deals ──────────────────────────────────────────
CREATE OR REPLACE VIEW v_deals_today AS
WITH latest AS (
  SELECT DISTINCT ON (app_id)
    app_id, discount_pct, price_current, concurrent_peak, captured_at
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '26 hours'
  ORDER BY app_id, captured_at DESC
)
SELECT
  g.app_id, g.name, g.developer, g.genres,
  g.price_usd, g.review_pct, g.review_count,
  g.positive_reviews, g.negative_reviews,
  g.img_header, g.img_capsule, g.steam_url, g.is_free,
  g.owners_text,
  s.discount_pct, s.price_current, s.concurrent_peak, s.captured_at,
  ROUND((COALESCE(g.price_usd,0) - COALESCE(s.price_current,0))::NUMERIC, 2) AS savings_usd
FROM games g
JOIN latest s ON s.app_id = g.app_id
WHERE s.discount_pct >= 20
  AND g.review_pct   >= 65
  AND g.review_count >= 50
  AND g.is_free = FALSE
  AND s.price_current IS NOT NULL
  AND s.price_current > 0
ORDER BY s.discount_pct DESC, g.review_pct DESC NULLS LAST
LIMIT 30;


-- ── View 4: History 7 ngày ──────────────────────────────────
CREATE OR REPLACE VIEW v_history_7days AS
WITH daily AS (
  SELECT DISTINCT ON (app_id, date_trunc('day', captured_at))
    app_id,
    date_trunc('day', captured_at)::DATE AS snapshot_date,
    concurrent_peak, discount_pct, price_current, rank_trending
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '8 days'
  ORDER BY app_id, date_trunc('day', captured_at), captured_at DESC
)
SELECT d.app_id, g.name, g.is_free, d.snapshot_date,
       d.concurrent_peak, d.discount_pct, d.price_current, d.rank_trending
FROM daily d
JOIN games g ON g.app_id = d.app_id
ORDER BY d.app_id, d.snapshot_date;


-- ── View 5: Genre stats ─────────────────────────────────────
CREATE OR REPLACE VIEW v_genre_stats_today AS
WITH latest AS (
  SELECT DISTINCT ON (app_id) app_id, concurrent_peak
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '26 hours'
  ORDER BY app_id, captured_at DESC
)
SELECT
  UNNEST(g.genres)                      AS genre,
  SUM(COALESCE(s.concurrent_peak,0))   AS total_players,
  COUNT(*)                              AS game_count
FROM games g
JOIN latest s ON s.app_id = g.app_id
WHERE g.genres IS NOT NULL AND array_length(g.genres,1) > 0
GROUP BY genre
ORDER BY total_players DESC
LIMIT 12;


-- ── View 6: Stats tổng hợp ─────────────────────────────────
CREATE OR REPLACE VIEW v_stats_today AS
WITH latest AS (
  SELECT DISTINCT ON (app_id) app_id, concurrent_peak, discount_pct, price_current
  FROM snapshots
  WHERE captured_at >= NOW() - INTERVAL '26 hours'
  ORDER BY app_id, captured_at DESC
)
SELECT
  COUNT(DISTINCT s.app_id)                                    AS total_games,
  SUM(s.concurrent_peak)                                      AS total_players,
  COUNT(*) FILTER (WHERE s.discount_pct>=20 AND g.is_free=FALSE) AS total_deals,
  MAX(s.concurrent_peak)                                      AS peak_players,
  ROUND(AVG(g.review_pct) FILTER (WHERE g.review_pct IS NOT NULL), 1) AS avg_review_pct,
  COUNT(*) FILTER (WHERE g.is_free=TRUE)                      AS free_game_count,
  (SELECT g2.name FROM games g2 JOIN latest s2 ON s2.app_id=g2.app_id
   ORDER BY s2.concurrent_peak DESC NULLS LAST LIMIT 1)       AS top_game_name
FROM latest s JOIN games g ON g.app_id = s.app_id;
