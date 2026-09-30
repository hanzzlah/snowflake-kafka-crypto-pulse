-- ============================================================================
-- CRYPTO PULSE TRACKER — TRANSFORMATION WORKSHEET (PKT TIMEZONE)
-- Run top to bottom. Each step is labeled.
-- ============================================================================


-- ============================================================================
-- STEP 1: PARSE RAW TICKS (TICKS_LIVE -> V_TICKS_LIVE_PARSED)
-- Parses RECORD_CONTENT JSON into structured columns. TRY_CAST drops bad rows
-- instead of erroring. trade_time / ingested_at converted from UTC to PKT.
-- ============================================================================
CREATE OR REPLACE VIEW V_TICKS_LIVE_PARSED AS
SELECT
    TRY_CAST(RECORD_CONTENT:s::STRING AS STRING)              AS symbol,
    TRY_CAST(RECORD_CONTENT:p::STRING AS NUMBER(18,8))        AS trade_price,
    TRY_CAST(RECORD_CONTENT:q::STRING AS NUMBER(18,8))        AS trade_quantity,
    CONVERT_TIMEZONE('UTC', 'Asia/Karachi', DATEADD(millisecond, TRY_CAST(RECORD_CONTENT:T::STRING AS NUMBER), '1970-01-01'::TIMESTAMP_NTZ)) AS trade_time,
    TRY_CAST(RECORD_CONTENT:t::STRING AS NUMBER)              AS trade_id,
    CONVERT_TIMEZONE('UTC', 'Asia/Karachi', DATEADD(millisecond, TRY_CAST(RECORD_METADATA:CreateTime::STRING AS NUMBER), '1970-01-01'::TIMESTAMP_NTZ)) AS ingested_at
FROM TICKS_LIVE
WHERE RECORD_CONTENT:s IS NOT NULL AND RECORD_CONTENT:p IS NOT NULL;

-- verify
SELECT * FROM V_TICKS_LIVE_PARSED
ORDER BY ingested_at DESC LIMIT 10;


-- ============================================================================
-- STEP 2: DEDUPE TICKS (V_TICKS_LIVE_PARSED -> V_TICKS_LIVE_DEDUP)
-- Kafka can double-deliver; keep only the latest row per trade_id.
-- No change needed — works on whatever timezone the parsed view provides.
-- ============================================================================
CREATE OR REPLACE VIEW V_TICKS_LIVE_DEDUP AS
SELECT symbol, trade_price, trade_quantity, trade_id, trade_time, ingested_at
FROM V_TICKS_LIVE_PARSED
QUALIFY ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY ingested_at DESC) = 1;


-- ============================================================================
-- STEP 3: CHECK NEWS_RAW HAS DATA
-- ============================================================================
SELECT COUNT(*) FROM NEWS_RAW;


-- ============================================================================
-- STEP 4: DEDUPE NEWS (NEWS_RAW -> V_NEWS_DEDUP)
-- Same headline can arrive twice; keep one per day so sentiment avg isn't skewed.
-- published_at stays UTC (source data, untouched) — fine as-is.
-- ============================================================================
CREATE OR REPLACE VIEW V_NEWS_DEDUP AS
SELECT headline_id, published_at, source_name, title, sentiment_score
FROM NEWS_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY title, published_at::DATE ORDER BY ingested_at DESC) = 1;


-- ============================================================================
-- STEP 5: GENERATE DAILY BASELINE (DAILY_CANDLES + V_NEWS_DEDUP -> DAILY_BASELINE)
-- ARIMA forecast on historical closes (mean +/- stdev fallback if <10 days
-- history). Pulls today's avg sentiment. Merges into DAILY_BASELINE.
-- CHANGED: "today" now computed in PKT, not UTC (date.today() defaults UTC
-- inside Snowflake and would misalign with trade_time's PKT day boundary).
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_GENERATE_BASELINE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','statsmodels','numpy')
HANDLER = 'generate_baseline'
AS
$$
from datetime import datetime
from zoneinfo import ZoneInfo
from statsmodels.tsa.arima.model import ARIMA
import numpy as np

def generate_baseline(session):
    today = datetime.now(ZoneInfo("Asia/Karachi")).date()
    symbols = session.table("DAILY_CANDLES").select("symbol").distinct().collect()

    for row in symbols:
        symbol = row["SYMBOL"]
        rows = session.table("DAILY_CANDLES").filter(f"symbol='{symbol}'") \
            .select("candle_date", "close_price").sort("candle_date").collect()

        prices = np.array([r["CLOSE_PRICE"] for r in rows], dtype=float)

        if len(prices) < 10:
            mean = prices.mean()
            std = prices.std() if len(prices) > 1 else mean * 0.02
            low, high = mean - std, mean + std
        else:
            model = ARIMA(prices, order=(1, 1, 1)).fit()
            fc = model.get_forecast(steps=1)
            mean = fc.predicted_mean[0]
            ci = fc.conf_int(alpha=0.05)[0]
            low, high = ci[0], ci[1]

        avg_s_row = session.sql(f"""
            SELECT AVG(sentiment_score) a FROM V_NEWS_DEDUP
            WHERE published_at::DATE = '{today}'
        """).collect()
        avg_sentiment = avg_s_row[0]["A"] or 0

        session.sql(f"""
            MERGE INTO DAILY_BASELINE t
            USING (SELECT '{symbol}' AS symbol, '{today}' AS target_date) s
            ON t.symbol = s.symbol AND t.target_date = s.target_date
            WHEN MATCHED THEN UPDATE SET
                predicted_low = {low}, predicted_high = {high},
                avg_sentiment = {avg_sentiment}, generated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (symbol, target_date, predicted_low, predicted_high, avg_sentiment)
                VALUES ('{symbol}', '{today}', {low}, {high}, {avg_sentiment})
        """).collect()

    return "Baseline generated"
$$;

-- run it (bootstrap — must run at least once before alerts can work)
CALL SP_GENERATE_BASELINE();
SELECT * FROM DAILY_BASELINE;


-- ============================================================================
-- STEP 5b: DAILY BASELINE TASK (automation)
-- CHANGED: schedule now runs at PKT midnight instead of UTC midnight.
-- ============================================================================
CREATE OR REPLACE TASK TASK_DAILY_BASELINE
  WAREHOUSE = CRYPTO_PULSE_WH
  SCHEDULE = 'USING CRON 0 0 * * * Asia/Karachi'
AS
CALL SP_GENERATE_BASELINE();

ALTER TASK TASK_DAILY_BASELINE RESUME;
ALTER TASK TASK_DAILY_BASELINE SUSPEND;



-- ============================================================================
-- STEP 6: CREATE ALERT STREAM (TICKS_LIVE -> STRM_ALERTS_TICKS)
-- Dedicated stream, separate from teammate's TICKS_LIVE_STREAM (used by their
-- 1-min OHLC task) so the two consumers don't corrupt each other's offsets.
-- No change needed.
-- ============================================================================
CREATE OR REPLACE STREAM STRM_ALERTS_TICKS ON TABLE TICKS_LIVE;


-- ============================================================================
-- STEP 7: DETECT PRICE BREACHES (STRM_ALERTS_TICKS + DAILY_BASELINE -> PRICE_ALERTS)
-- Compares new ticks against today's predicted range, logs breaches.
-- CHANGED: join now compares against today's date in PKT, not CURRENT_DATE()
-- (which is UTC-based) — otherwise this task and DAILY_BASELINE's date could
-- mismatch around the UTC/PKT day-boundary gap (~5 hours).
-- trade_time in the INSERT also converted to PKT to match everything else.
-- ============================================================================
CREATE OR REPLACE TASK TASK_PRICE_ALERTS
  WAREHOUSE = CRYPTO_PULSE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('STRM_ALERTS_TICKS')
AS
INSERT INTO PRICE_ALERTS
    (symbol, trade_id, trade_time, live_price, predicted_low, predicted_high, deviation_direction)
SELECT
    RECORD_CONTENT:s::STRING,
    RECORD_CONTENT:t::NUMBER,
    CONVERT_TIMEZONE('UTC', 'Asia/Karachi', DATEADD(millisecond, RECORD_CONTENT:T::NUMBER, '1970-01-01'::TIMESTAMP_NTZ)),
    RECORD_CONTENT:p::NUMBER(18,8),
    b.predicted_low, b.predicted_high,
    CASE WHEN RECORD_CONTENT:p::NUMBER > b.predicted_high THEN 'ABOVE_RANGE' ELSE 'BELOW_RANGE' END
FROM STRM_ALERTS_TICKS s
JOIN DAILY_BASELINE b
    ON RECORD_CONTENT:s::STRING = b.symbol
    AND b.target_date = CONVERT_TIMEZONE('UTC', 'Asia/Karachi', CURRENT_TIMESTAMP())::DATE
WHERE METADATA$ACTION = 'INSERT'
  AND (RECORD_CONTENT:p::NUMBER > b.predicted_high OR RECORD_CONTENT:p::NUMBER < b.predicted_low);

ALTER TASK TASK_PRICE_ALERTS RESUME;
ALTER TASK TASK_PRICE_ALERTS SUSPEND;


-- ============================================================================
-- STEP 8: BUILD DASHBOARD VIEW (DAILY_BASELINE + V_TICKS_LIVE_DEDUP -> V_DASHBOARD_LIVE)
-- Final serving view: live ticks joined with today's baseline, labeled by status.
-- No change needed — t.trade_time is already PKT (from step 1), and
-- CAST(t.trade_time AS DATE) will naturally align with b.target_date since
-- DAILY_BASELINE is now generated using PKT dates (step 5).
-- ============================================================================
CREATE OR REPLACE VIEW V_DASHBOARD_LIVE AS
SELECT
    t.symbol, t.trade_time, t.trade_price,
    b.predicted_low, b.predicted_high, b.avg_sentiment,
    CASE
        WHEN t.trade_price > b.predicted_high THEN 'HIGH_ALERT'
        WHEN t.trade_price < b.predicted_low THEN 'LOW_ALERT'
        ELSE 'NORMAL'
    END AS price_status
FROM V_TICKS_LIVE_DEDUP t
LEFT JOIN DAILY_BASELINE b
    ON t.symbol = b.symbol AND CAST(t.trade_time AS DATE) = b.target_date;

-- verify
SELECT * FROM V_DASHBOARD_LIVE LIMIT 20;
SELECT * FROM PRICE_ALERTS;


-- ============================================================================
-- STEP 9: TEST ALERT FIRING (manual — inject a fake breach tick, run task, verify)
-- No change needed — test insert logic is UTC epoch ms, gets converted
-- downstream automatically by the updated views/task.
-- ============================================================================
INSERT INTO TICKS_LIVE (RECORD_CONTENT, RECORD_METADATA)
SELECT
    PARSE_JSON('{"E": ' || (DATE_PART(epoch_millisecond, CURRENT_TIMESTAMP())) ||
               ', "M": true, "T": ' || (DATE_PART(epoch_millisecond, CURRENT_TIMESTAMP())) ||
               ', "e": "trade", "m": false, "p": "82000.00000000", "q": "0.01000000", "s": "BTCUSDT", "t": 9999999999}'),
    PARSE_JSON('{"CreateTime": ' || (DATE_PART(epoch_millisecond, CURRENT_TIMESTAMP())) ||
               ', "key": "BTCUSDT", "offset": 0, "partition": 0, "topic": "price_ticks"}');

EXECUTE TASK TASK_PRICE_ALERTS;
SELECT * FROM PRICE_ALERTS;


-- ============================================================================
-- STEP 10: FULL END-TO-END SANITY CHECK
-- ============================================================================
SELECT COUNT(*) FROM V_TICKS_LIVE_DEDUP;
SELECT COUNT(*) FROM DAILY_BASELINE;
SELECT COUNT(*) FROM PRICE_ALERTS;
SELECT * FROM V_DASHBOARD_LIVE ORDER BY trade_time DESC LIMIT 10;


-- ============================================================================
-- STEP 11: CLEANUP TEST DATA (remove fake $82,000 tick before demo)
-- ============================================================================
DELETE FROM TICKS_LIVE WHERE RECORD_CONTENT:t::NUMBER = 9999999999;
-- optionally also clear the test alert it generated:
DELETE FROM PRICE_ALERTS WHERE trade_id = 9999999999;



SELECT * FROM V_DASHBOARD_LIVE ORDER BY trade_time DESC LIMIT 20;

SELECT * FROM CRYPTO_PULSE_DB.PUBLIC.V_DASHBOARD_LIVE LIMIT 5;

SELECT RECORD_CONTENT 
FROM TICKS_LIVE 
LIMIT 5;

SELECT * FROM CRYPTO_PULSE_DB.PUBLIC.V_DASHBOARD_LIVE LIMIT 1;

SELECT symbol, COUNT(*) FROM DAILY_CANDLES GROUP BY symbol;
SELECT DISTINCT RECORD_CONTENT:s::STRING AS symbol FROM TICKS_LIVE;
SELECT symbol, target_date FROM DAILY_BASELINE;