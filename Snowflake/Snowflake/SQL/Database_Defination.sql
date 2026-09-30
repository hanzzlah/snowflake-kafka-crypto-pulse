-- ============================================================================
-- 0. SETUP
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS CRYPTO_PULSE_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
USE WAREHOUSE CRYPTO_PULSE_WH;

CREATE DATABASE IF NOT EXISTS CRYPTO_PULSE_DB;
USE DATABASE CRYPTO_PULSE_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;


-- ============================================================================
-- 1. SPEED LAYER TABLES
-- ============================================================================
CREATE TABLE IF NOT EXISTS TICKS_LIVE (
    RECORD_CONTENT VARIANT,
    RECORD_METADATA VARIANT
);


-- ============================================================================
-- 2. BATCH LAYER TABLES
-- ============================================================================
CREATE TABLE IF NOT EXISTS DAILY_CANDLES (
    symbol VARCHAR(20) NOT NULL,
    candle_date DATE NOT NULL,
    open_price NUMBER(18, 8),
    high_price NUMBER(18, 8),
    low_price NUMBER(18, 8),
    close_price NUMBER(18, 8),
    volume NUMBER(24, 8),
    PRIMARY KEY (symbol, candle_date)
);

CREATE TABLE IF NOT EXISTS NEWS_RAW (
    headline_id STRING PRIMARY KEY,
    published_at TIMESTAMP_NTZ,
    source_name STRING,
    title STRING,
    sentiment_score NUMBER(5, 4),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DAILY_BASELINE (
    symbol VARCHAR(20) NOT NULL,
    target_date DATE NOT NULL,
    predicted_low NUMBER(18, 8) NOT NULL,
    predicted_high NUMBER(18, 8) NOT NULL,
    avg_sentiment NUMBER(5, 4),
    generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (symbol, target_date)
);


-- ============================================================================
-- 3. SERVING & ALERTS TABLES + VIEW
-- ============================================================================
CREATE TABLE IF NOT EXISTS PRICE_ALERTS (
    alert_id VARCHAR(36) DEFAULT UUID_STRING(),
    symbol VARCHAR(20),
    trade_id NUMBER,
    trade_time TIMESTAMP_NTZ,
    live_price NUMBER(18, 8),
    predicted_low NUMBER(18, 8),
    predicted_high NUMBER(18, 8),
    deviation_direction VARCHAR(10),
    alert_triggered_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 4. STREAM & AUTOMATED 1-MIN AGGREGATION TASK
-- ============================================================================
CREATE TABLE IF NOT EXISTS TICKS_1MIN_AGG (
    symbol VARCHAR(20),
    window_start TIMESTAMP_NTZ,
    open_price NUMBER(18, 8),
    high_price NUMBER(18, 8),
    low_price NUMBER(18, 8),
    close_price NUMBER(18, 8),
    volume NUMBER(24, 8),
    trade_count INT,
    PRIMARY KEY (symbol, window_start)
);

CREATE OR REPLACE STREAM TICKS_LIVE_STREAM ON TABLE TICKS_LIVE;

CREATE OR REPLACE TASK TASK_AGGREGATE_1MIN_TICKS
  WAREHOUSE = CRYPTO_PULSE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('TICKS_LIVE_STREAM')
AS
MERGE INTO TICKS_1MIN_AGG target
USING (
  WITH parsed_stream AS (
    SELECT 
      RECORD_CONTENT:s::STRING AS symbol,
      RECORD_CONTENT:p::NUMBER(18, 8) AS price,
      RECORD_CONTENT:q::NUMBER(18, 8) AS quantity,
      TO_TIMESTAMP_NTZ(RECORD_CONTENT:T::NUMBER / 1000) AS trade_time
    FROM TICKS_LIVE_STREAM
    WHERE METADATA$ACTION = 'INSERT'
  ),
  windowed AS (
    SELECT 
      symbol,
      DATE_TRUNC('MINUTE', trade_time) AS window_start,
      ARRAY_AGG(price) WITHIN GROUP (ORDER BY trade_time ASC)[0]::NUMBER(18,8) AS open_price,
      MAX(price) AS high_price,
      MIN(price) AS low_price,
      ARRAY_AGG(price) WITHIN GROUP (ORDER BY trade_time DESC)[0]::NUMBER(18,8) AS close_price,
      SUM(quantity) AS volume,
      COUNT(*) AS trade_count
    FROM parsed_stream
    GROUP BY symbol, DATE_TRUNC('MINUTE', trade_time)
  )
  SELECT * FROM windowed
) src
ON target.symbol = src.symbol AND target.window_start = src.window_start
WHEN MATCHED THEN UPDATE SET
  high_price = GREATEST(target.high_price, src.high_price),
  low_price = LEAST(target.low_price, src.low_price),
  close_price = src.close_price,
  volume = target.volume + src.volume,
  trade_count = target.trade_count + src.trade_count
WHEN NOT MATCHED THEN INSERT 
  (symbol, window_start, open_price, high_price, low_price, close_price, volume, trade_count)
VALUES 
  (src.symbol, src.window_start, src.open_price, src.high_price, src.low_price, src.close_price, src.volume, src.trade_count);

ALTER TASK TASK_AGGREGATE_1MIN_TICKS RESUME;


-- ============================================================================
-- 5. SANITY CHECKS & DIAGNOSTICS
-- ============================================================================
SELECT 'TICKS_LIVE' AS obj, COUNT(*) AS row_count FROM TICKS_LIVE
UNION ALL
SELECT 'V_TICKS_LIVE_PARSED', COUNT(*) FROM V_TICKS_LIVE_PARSED
UNION ALL
SELECT 'DAILY_CANDLES', COUNT(*) FROM DAILY_CANDLES
UNION ALL
SELECT 'NEWS_RAW', COUNT(*) FROM NEWS_RAW
UNION ALL
SELECT 'DAILY_BASELINE', COUNT(*) FROM DAILY_BASELINE
UNION ALL
SELECT 'PRICE_ALERTS', COUNT(*) FROM PRICE_ALERTS
UNION ALL
SELECT 'TICKS_1MIN_AGG', COUNT(*) FROM TICKS_1MIN_AGG;

-- View current task graph status
SELECT * FROM TABLE(information_schema.current_task_graphs());

TRUNCATE TABLE TICKS_LIVE;

-- 2. Clear out any old aggregated history or alerts tied to bad data
TRUNCATE vie V_TICKS_LIVE_PARSED;
TRUNCATE TABLE daily_baseline;

select * from ti