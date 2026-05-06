-- ============================================================
-- FinStreamAI — TimescaleDB Initialization
-- Time-series market data schema
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─── Market Ticks ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_ticks (
    time        TIMESTAMPTZ NOT NULL,
    symbol      VARCHAR(20) NOT NULL,
    price       DECIMAL(15,6) NOT NULL,
    volume      BIGINT,
    bid_price   DECIMAL(15,6),
    ask_price   DECIMAL(15,6),
    bid_size    INTEGER,
    ask_size    INTEGER,
    exchange    VARCHAR(20)
);

SELECT create_hypertable('market_ticks', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol_time ON market_ticks(symbol, time DESC);

-- ─── Market Bars ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_bars (
    time        TIMESTAMPTZ NOT NULL,
    symbol      VARCHAR(20) NOT NULL,
    timeframe   VARCHAR(10) NOT NULL,
    open_price  DECIMAL(15,6) NOT NULL,
    high_price  DECIMAL(15,6) NOT NULL,
    low_price   DECIMAL(15,6) NOT NULL,
    close_price DECIMAL(15,6) NOT NULL,
    volume      BIGINT,
    vwap        DECIMAL(15,6),
    trade_count INTEGER
);

SELECT create_hypertable('market_bars', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_market_bars_symbol_time ON market_bars(symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_market_bars_symbol_timeframe ON market_bars(symbol, timeframe, time DESC);

-- ─── Technical Indicators ────────────────────────────────────
CREATE TABLE IF NOT EXISTS technical_indicators (
    time           TIMESTAMPTZ NOT NULL,
    symbol         VARCHAR(20) NOT NULL,
    indicator_name VARCHAR(50) NOT NULL,
    timeframe      VARCHAR(10) NOT NULL,
    value          DECIMAL(20,8) NOT NULL,
    metadata       JSONB NOT NULL DEFAULT '{}'
);

SELECT create_hypertable('technical_indicators', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_tech_indicators_symbol_time ON technical_indicators(symbol, time DESC);

-- ─── Sentiment Scores ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sentiment_scores (
    time             TIMESTAMPTZ NOT NULL,
    symbol           VARCHAR(20) NOT NULL,
    source           VARCHAR(50) NOT NULL,
    sentiment_score  DECIMAL(5,4) NOT NULL,
    confidence_score DECIMAL(5,4),
    article_count    INTEGER NOT NULL DEFAULT 1
);

SELECT create_hypertable('sentiment_scores', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_sentiment_symbol_time ON sentiment_scores(symbol, time DESC);

-- ─── Trading Signals ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trading_signals (
    time            TIMESTAMPTZ NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    signal_type     VARCHAR(50) NOT NULL,
    signal_strength DECIMAL(5,4) NOT NULL,
    direction       VARCHAR(10) NOT NULL CHECK (direction IN ('LONG', 'SHORT', 'NEUTRAL')),
    confidence      DECIMAL(5,4) NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}'
);

SELECT create_hypertable('trading_signals', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_signals_symbol_time ON trading_signals(symbol, time DESC);

-- ─── Risk Metrics ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS risk_metrics (
    time              TIMESTAMPTZ NOT NULL,
    symbol            VARCHAR(20) NOT NULL,
    var_1d            DECIMAL(10,6),
    var_5d            DECIMAL(10,6),
    expected_shortfall DECIMAL(10,6),
    beta              DECIMAL(8,4),
    sharpe_ratio      DECIMAL(8,4),
    max_drawdown      DECIMAL(8,4)
);

SELECT create_hypertable('risk_metrics', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_risk_symbol_time ON risk_metrics(symbol, time DESC);

-- ─── System Metrics ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS system_metrics (
    time         TIMESTAMPTZ NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    metric_name  VARCHAR(100) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    tags         JSONB NOT NULL DEFAULT '{}'
);

SELECT create_hypertable('system_metrics', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_system_metrics_service_time ON system_metrics(service_name, time DESC);

-- ─── Continuous Aggregates ───────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS market_bars_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    symbol,
    first(open_price, time)     AS open_price,
    max(high_price)             AS high_price,
    min(low_price)              AS low_price,
    last(close_price, time)     AS close_price,
    sum(volume)                 AS volume,
    sum(volume * vwap) / NULLIF(sum(volume), 0) AS vwap
FROM market_bars
WHERE timeframe = '1min'
GROUP BY bucket, symbol
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS sentiment_daily_avg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    symbol,
    source,
    avg(sentiment_score)       AS avg_sentiment,
    avg(confidence_score)      AS avg_confidence,
    sum(article_count)         AS total_articles
FROM sentiment_scores
GROUP BY bucket, symbol, source
WITH NO DATA;

-- ─── Retention Policies ──────────────────────────────────────
SELECT add_retention_policy('market_ticks', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('system_metrics', INTERVAL '30 days', if_not_exists => TRUE);
