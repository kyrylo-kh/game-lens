-- ==============================================================================
-- FACT TABLE: Daily snapshot per game
-- ==============================================================================
CREATE TABLE IF NOT EXISTS fact_game_daily
(
  snapshot_date         Date,
  appid                 UInt32,

  price                 Nullable(Float64),
  price_usd             Nullable(Float64),
  currency              FixedString(3),
  discount_percent      UInt8,
  recommendations_total Int64,
  metacritic_score      Int16,
  achievements_total    Int16,
  is_free               UInt8,

  fetched_at            DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (snapshot_date, appid);

-- ==============================================================================
-- DIMENSION TABLE: Latest game attributes
-- ==============================================================================
CREATE TABLE IF NOT EXISTS dim_game
(
  appid         UInt32,
  name          String,
  type          LowCardinality(String),
  release_date  Nullable(Date),
  required_age  UInt8,
  coming_soon   UInt8,

  genres        Array(String),
  categories    Array(String),
  developers    Array(String),
  publishers    Array(String),

  updated_at    DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY appid;

-- ==============================================================================
-- VIEWS: Dynamics & Analytics
-- ==============================================================================

-- View: Game Growth (The Hype Train)
-- Calculates daily, weekly, monthly growth using window functions.
CREATE VIEW IF NOT EXISTS view_game_growth AS
SELECT
    snapshot_date,
    appid,
    recommendations_total,
    -- 1 Day Growth
    recommendations_total - lagInFrame(recommendations_total, 1) OVER (PARTITION BY appid ORDER BY snapshot_date) as rec_growth_1d,
    -- 7 Day Growth
    recommendations_total - lagInFrame(recommendations_total, 7) OVER (PARTITION BY appid ORDER BY snapshot_date) as rec_growth_7d,
    -- 30 Day Growth
    recommendations_total - lagInFrame(recommendations_total, 30) OVER (PARTITION BY appid ORDER BY snapshot_date) as rec_growth_30d
FROM gamelens.fact_game_daily;

-- View: Genre Growth (Market Trends)
-- Flattens the array to allow "Group By Genre"
CREATE VIEW IF NOT EXISTS view_genre_growth AS
SELECT
    f.snapshot_date as snapshot_date,
    genre as genre,
    sum(f.rec_growth_1d) as genre_growth_1d,
    sum(f.rec_growth_7d) as genre_growth_7d,
    avg(f.recommendations_total) as avg_recommendations
FROM gamelens.view_game_growth f
JOIN gamelens.dim_game d ON f.appid = d.appid
ARRAY JOIN d.genres as genre
GROUP BY snapshot_date, genre;

-- View: Publisher Growth
CREATE VIEW IF NOT EXISTS view_publisher_growth AS
SELECT
    f.snapshot_date as snapshot_date,
    publisher as publisher,
    sum(f.rec_growth_1d) as pub_growth_1d,
    sum(f.rec_growth_7d) as pub_growth_7d
FROM gamelens.view_game_growth f
JOIN gamelens.dim_game d ON f.appid = d.appid
ARRAY JOIN d.publishers as publisher
GROUP BY snapshot_date, publisher;
