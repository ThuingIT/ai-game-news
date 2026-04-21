-- ============================================================
-- STEAM TRACKER — Supabase Schema Setup
-- Chạy toàn bộ file này trong Supabase SQL Editor
-- ============================================================

-- 1. Bảng games: thông tin cơ bản của từng game (upsert hàng ngày)
CREATE TABLE IF NOT EXISTS games (
  app_id        INTEGER PRIMARY KEY,
  name          TEXT NOT NULL,
  developer     TEXT,
  publisher     TEXT,
  genres        TEXT[],
  tags          TEXT[],
  price_usd     NUMERIC(8,2),
  review_pct    INTEGER,        -- % positive reviews (0-100)
  review_count  INTEGER,
  img_header    TEXT,           -- URL ảnh header từ Steam CDN
  img_capsule   TEXT,           -- URL ảnh capsule nhỏ
  steam_url     TEXT,
  is_free       BOOLEAN DEFAULT FALSE,
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Bảng snapshots: lịch sử metrics theo ngày (append-only)
--    Đây là thứ cho phép vẽ chart trend theo thời gian
CREATE TABLE IF NOT EXISTS snapshots (
  id               BIGSERIAL PRIMARY KEY,
  app_id           INTEGER NOT NULL REFERENCES games(app_id) ON DELETE CASCADE,
  concurrent_peak  INTEGER,     -- số người chơi đồng thời cao nhất ngày đó
  owners_estimate  INTEGER,     -- ước tính số người sở hữu (SteamSpy)
  discount_pct     INTEGER DEFAULT 0,  -- % giảm giá (0 nếu không sale)
  price_current    NUMERIC(8,2),       -- giá sau giảm
  rank_trending    INTEGER,     -- thứ hạng trending hôm nay
  rank_sellers     INTEGER,     -- thứ hạng top sellers
  captured_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Index để query nhanh snapshots theo game và thời gian
CREATE INDEX IF NOT EXISTS idx_snapshots_app_id    ON snapshots(app_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_captured  ON snapshots(captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_app_date  ON snapshots(app_id, captured_at DESC);

-- 3. Bảng ai_insights: cache kết quả phân tích từ Groq AI
CREATE TABLE IF NOT EXISTS ai_insights (
  id            BIGSERIAL PRIMARY KEY,
  insight_type  TEXT NOT NULL,  -- 'trend', 'deal_picks', 'weekly_summary', 'genre_analysis'
  content       TEXT NOT NULL,
  model_used    TEXT DEFAULT 'llama-3.3-70b-versatile',
  token_count   INTEGER,
  generated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_insights_type_date ON ai_insights(insight_type, generated_at DESC);

-- ============================================================
-- VIEWS tiện lợi để query trong generate_html.py
-- ============================================================

-- View: top 50 game trending hôm nay kèm snapshot mới nhất
CREATE OR REPLACE VIEW v_trending_today AS
SELECT
  g.app_id,
  g.name,
  g.developer,
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
  s.captured_at
FROM games g
JOIN snapshots s ON s.app_id = g.app_id
WHERE s.captured_at >= NOW() - INTERVAL '26 hours'
ORDER BY s.rank_trending ASC NULLS LAST, s.concurrent_peak DESC NULLS LAST
LIMIT 50;

-- View: game đang giảm giá sâu, lọc quality >= 70%
CREATE OR REPLACE VIEW v_deals_today AS
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
  s.discount_pct,
  s.price_current,
  s.concurrent_peak,
  s.captured_at,
  ROUND((g.price_usd - s.price_current)::NUMERIC, 2) AS savings_usd
FROM games g
JOIN snapshots s ON s.app_id = g.app_id
WHERE s.captured_at >= NOW() - INTERVAL '26 hours'
  AND s.discount_pct >= 30
  AND g.review_pct >= 70
  AND g.review_count >= 100
ORDER BY s.discount_pct DESC, g.review_pct DESC
LIMIT 30;

-- ============================================================
-- ROW LEVEL SECURITY (bảo vệ dữ liệu nếu dùng anon key)
-- Service key trong GitHub Actions bypass RLS hoàn toàn
-- ============================================================
ALTER TABLE games        ENABLE ROW LEVEL SECURITY;
ALTER TABLE snapshots    ENABLE ROW LEVEL SECURITY;
ALTER TABLE ai_insights  ENABLE ROW LEVEL SECURITY;

-- Cho phép đọc công khai (cần cho phần fetch từ browser nếu muốn)
CREATE POLICY "public read games"       ON games       FOR SELECT USING (true);
CREATE POLICY "public read snapshots"   ON snapshots   FOR SELECT USING (true);
CREATE POLICY "public read ai_insights" ON ai_insights FOR SELECT USING (true);
