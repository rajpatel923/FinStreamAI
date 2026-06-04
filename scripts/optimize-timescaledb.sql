-- ============================================================
-- TimescaleDB Optimization: Continuous Aggregates, Compression,
-- Retention Policies, and Indexes
-- Run against: finstreami-timescaledb (port 5433)
-- ============================================================

-- ─── Continuous Aggregates ───────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS market_bars_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time)  AS bucket,
    symbol,
    first(price, time)           AS open,
    max(price)                   AS high,
    min(price)                   AS low,
    last(price, time)            AS close,
    sum(volume)                  AS volume,
    count(*)                     AS tick_count,
    avg(price)                   AS vwap
FROM market_ticks
GROUP BY bucket, symbol
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS market_bars_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time)   AS bucket,
    symbol,
    first(price, time)           AS open,
    max(price)                   AS high,
    min(price)                   AS low,
    last(price, time)            AS close,
    sum(volume)                  AS volume,
    count(*)                     AS tick_count,
    avg(price)                   AS vwap
FROM market_ticks
GROUP BY bucket, symbol
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS market_bars_1w
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 week', time)  AS bucket,
    symbol,
    first(price, time)           AS open,
    max(price)                   AS high,
    min(price)                   AS low,
    last(price, time)            AS close,
    sum(volume)                  AS volume,
    count(*)                     AS tick_count,
    avg(price)                   AS vwap
FROM market_ticks
GROUP BY bucket, symbol
WITH NO DATA;

-- Refresh policies: keep aggregates up-to-date automatically
SELECT add_continuous_aggregate_policy('market_bars_1h',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

SELECT add_continuous_aggregate_policy('market_bars_1d',
    start_offset => INTERVAL '3 days',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day'
);

SELECT add_continuous_aggregate_policy('market_bars_1w',
    start_offset => INTERVAL '3 weeks',
    end_offset   => INTERVAL '1 week',
    schedule_interval => INTERVAL '1 week'
);

-- ─── Compression ─────────────────────────────────────────────

ALTER TABLE market_ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy('market_ticks', INTERVAL '7 days');

ALTER TABLE market_bars SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'time DESC'
) WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'market_bars');

-- ─── Retention Policies ──────────────────────────────────────

SELECT add_retention_policy('market_ticks', INTERVAL '7 days');
SELECT add_retention_policy('market_bars',  INTERVAL '30 days')
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'market_bars');

-- ─── Indexes ─────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol_time
    ON market_ticks (symbol, time DESC);

CREATE INDEX IF NOT EXISTS idx_market_ticks_time
    ON market_ticks (time DESC);

CREATE INDEX IF NOT EXISTS idx_market_bars_1h_symbol
    ON market_bars_1h (symbol, bucket DESC);

CREATE INDEX IF NOT EXISTS idx_market_bars_1d_symbol
    ON market_bars_1d (symbol, bucket DESC);
