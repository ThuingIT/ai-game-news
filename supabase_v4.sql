-- ============================================================
-- STEAM TRACKER — SQL v4
-- Thêm: bảng new_releases + view v_surge_today update
-- Chạy THÊM file này sau khi đã chạy supabase_v3.sql
-- ============================================================

-- ── Bảng new_releases ─────────────────────────────────────────
-- Lưu game mới ra trong 14 ngày qua đã qua bộ lọc chất lượng
-- Upsert theo app_id mỗi ngày từ fetch_data.py
CREATE TABLE IF NOT EXISTS new_releases (
  app_id              INTEGER PRIMARY KEY,
  name                TEXT NOT NULL,
  developer           TEXT,
  publisher           TEXT,
  genres              TEXT[],
  tags                TEXT[],
  price_usd           NUMERIC(8,2),
  is_free             BOOLEAN DEFAULT FALSE,
  review_pct          INTEGER,
  review_count        INTEGER,
  positive_reviews    INTEGER,
  negative_reviews    INTEGER,
  owners_text         TEXT,
  img_header          TEXT,
  img_capsule         TEXT,
  steam_url           TEXT,
  release_date        DATE,
  days_since_release  INTEGER,
  concurrent_peak     INTEGER,
  owners_mid          INTEGER,
  launch_score        NUMERIC(6,4),   -- composite score: review 40% + player 30% + count 30%
  short_desc          TEXT,
  updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Index để sort nhanh theo launch_score
CREATE INDEX IF NOT EXISTS idx_new_releases_score      ON new_releases(launch_score DESC);
CREATE INDEX IF NOT EXISTS idx_new_releases_release    ON new_releases(release_date DESC);
CREATE INDEX IF NOT EXISTS idx_new_releases_updated    ON new_releases(updated_at DESC);

-- RLS
ALTER TABLE new_releases ENABLE ROW LEVEL SECURITY;
CREATE POLICY "public read new_releases" ON new_releases FOR SELECT USING (true);

-- ── Dọn dẹp game cũ hơn 21 ngày ra khỏi new_releases ─────────
-- (Chạy thủ công hoặc đặt trong cron nếu cần)
-- DELETE FROM new_releases WHERE days_since_release > 21;
