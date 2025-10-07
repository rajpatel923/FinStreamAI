-- scripts/init-timescaledb.sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Market Data Tables
CREATE TABLE market_ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price DECIMAL(15,6) NOT NULL,
    volume INTEGER NOT NULL,
    bid_price DECIMAL(15,6),
    ask_price DECIMAL(15,6),
    bid_size INTEGER,
    ask_size INTEGER,
    exchange VARCHAR(10)
);

CREATE TABLE market_bars (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    timeframe VARCHAR(10) NOT NULL, -- 1min, 5min, 1hour, etc.
    open_price DECIMAL(15,6) NOT NULL,
    high_price DECIMAL(15,6) NOT NULL,
    low_price DECIMAL(15,6) NOT NULL,
    close_price DECIMAL(15,6) NOT NULL,
    volume BIGINT NOT NULL,
    vwap DECIMAL(15,6),
    trade_count INTEGER
);

-- Technical Indicators
CREATE TABLE technical_indicators (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    indicator_name VARCHAR(50) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    value DECIMAL(15,6) NOT NULL,
    metadata JSONB
);

-- Sentiment Scores
CREATE TABLE sentiment_scores (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    source VARCHAR(50) NOT NULL,
    sentiment_score DECIMAL(5,4) NOT NULL, -- -1 to 1
    confidence_score DECIMAL(5,4),
    article_count INTEGER DEFAULT 1
);

-- Trading Signals
CREATE TABLE trading_signals (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    signal_type VARCHAR(50) NOT NULL,
    signal_strength DECIMAL(5,4) NOT NULL, -- 0 to 1
    direction VARCHAR(10) NOT NULL, -- buy/sell/hold
    confidence DECIMAL(5,4),
    metadata JSONB
);

-- Risk Metrics
CREATE TABLE risk_metrics (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    var_1d DECIMAL(15,6),
    var_5d DECIMAL(15,6),
    expected_shortfall DECIMAL(15,6),
    beta DECIMAL(10,6),
    sharpe_ratio DECIMAL(10,6),
    max_drawdown DECIMAL(10,6)
);

-- System Metrics
CREATE TABLE system_metrics (
    time TIMESTAMPTZ NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,6) NOT NULL,
    tags JSONB
);

-- Create hypertables
SELECT create_hypertable('market_ticks', 'time');
SELECT create_hypertable('market_bars', 'time');
SELECT create_hypertable('technical_indicators', 'time');
SELECT create_hypertable('sentiment_scores', 'time');
SELECT create_hypertable('trading_signals', 'time');
SELECT create_hypertable('risk_metrics', 'time');
SELECT create_hypertable('system_metrics', 'time');

-- Create indexes
CREATE INDEX idx_market_ticks_symbol_time ON market_ticks (symbol, time DESC);
CREATE INDEX idx_market_bars_symbol_timeframe_time ON market_bars (symbol, timeframe, time DESC);
CREATE INDEX idx_technical_indicators_symbol_indicator ON technical_indicators (symbol, indicator_name, time DESC);
CREATE INDEX idx_sentiment_scores_symbol_source ON sentiment_scores (symbol, source, time DESC);
CREATE INDEX idx_trading_signals_symbol_type ON trading_signals (symbol, signal_type, time DESC);
CREATE INDEX idx_risk_metrics_symbol_time ON risk_metrics (symbol, time DESC);
CREATE INDEX idx_system_metrics_service_metric ON system_metrics (service_name, metric_name, time DESC);

-- Create continuous aggregates for common queries
CREATE MATERIALIZED VIEW market_bars_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', time) AS hour,
    symbol,
    first(open_price, time) AS open_price,
    max(high_price) AS high_price,
    min(low_price) AS low_price,
    last(close_price, time) AS close_price,
    sum(volume) AS volume,
    avg(vwap) AS vwap,
    sum(trade_count) AS trade_count
FROM market_bars 
WHERE timeframe = '1min'
GROUP BY hour, symbol;

CREATE MATERIALIZED VIEW sentiment_daily_avg
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', time) AS day,
    symbol,
    source,
    avg(sentiment_score) AS avg_sentiment,
    avg(confidence_score) AS avg_confidence,
    sum(article_count) AS total_articles
FROM sentiment_scores
GROUP BY day, symbol, source;

-- Retention policies
SELECT add_retention_policy('market_ticks', INTERVAL '7 days');
SELECT add_retention_policy('system_metrics', INTERVAL '30 days');