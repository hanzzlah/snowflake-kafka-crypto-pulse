import os
import requests
from datetime import datetime
from dotenv import load_dotenv # type:ignore
import snowflake.connector # type:ignore
from cryptography.hazmat.primitives import serialization # type:ignore
from cryptography.hazmat.backends import default_backend # type:ignore
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer # type:ignore

load_dotenv()

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
analyzer = SentimentIntensityAnalyzer()

def get_private_key_bytes():
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "./rsa_key.p8")
    raw_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip().strip("'\"")
    passphrase = raw_passphrase.encode("utf-8") if raw_passphrase else None

    with open(key_path, "rb") as key_file:
        key_data = key_file.read()

    try:
        p_key = serialization.load_pem_private_key(key_data, password=passphrase, backend=default_backend())
    except TypeError:
        p_key = serialization.load_pem_private_key(key_data, password=None, backend=default_backend())

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=get_private_key_bytes(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

def ingest_daily_candles(cursor):
    query = """
    MERGE INTO DAILY_CANDLES target
    USING (SELECT %s AS symbol, %s::DATE AS candle_date, %s AS open_price, %s AS high_price, %s AS low_price, %s AS close_price, %s AS volume) src
    ON target.symbol = src.symbol AND target.candle_date = src.candle_date
    WHEN MATCHED THEN UPDATE SET 
        open_price = src.open_price, high_price = src.high_price, low_price = src.low_price, close_price = src.close_price, volume = src.volume
    WHEN NOT MATCHED THEN INSERT (symbol, candle_date, open_price, high_price, low_price, close_price, volume)
    VALUES (src.symbol, src.candle_date, src.open_price, src.high_price, src.low_price, src.close_price, src.volume)
    """
    for symbol in SYMBOLS:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1d&limit=30"
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f"[Candles] Failed API request for {symbol}: {resp.status_code}")
            continue

        candles = resp.json()
        for c in candles:
            candle_date = datetime.utcfromtimestamp(c[0] / 1000).strftime('%Y-%m-%d')
            cursor.execute(query, (symbol, candle_date, float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])))
        print(f"[Candles] Ingested {len(candles)} records for {symbol}.")

def ingest_news_headlines(cursor):
    if not NEWS_API_KEY:
        print("[News] Skipping. NEWS_API_KEY missing.")
        return

    # Updated query to capture BTC, ETH, SOL, and general crypto news
    url = f"https://newsapi.org/v2/everything?q=crypto+OR+bitcoin+OR+ethereum+OR+solana&sortBy=publishedAt&pageSize=30&apiKey={NEWS_API_KEY}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"[News] Failed API request: {resp.status_code}")
        return

    articles = resp.json().get("articles", [])
    query = """
    MERGE INTO NEWS_RAW target
    USING (SELECT %s::STRING AS headline_id, %s::TIMESTAMP_NTZ AS published_at, %s::STRING AS source_name, %s::STRING AS title, %s::NUMBER(5, 4) AS sentiment_score) src
    ON target.headline_id = src.headline_id
    WHEN NOT MATCHED THEN INSERT (headline_id, published_at, source_name, title, sentiment_score)
    VALUES (src.headline_id, src.published_at, src.source_name, src.title, src.sentiment_score)
    """
    for item in articles:
        title = item.get("title", "")
        if not title:
            continue
        sentiment_score = analyzer.polarity_scores(title)["compound"]
        cursor.execute(query, (
            item.get("url"), item.get("publishedAt"), item.get("source", {}).get("name"), title, sentiment_score
        ))
    print(f"[News] Ingested {len(articles)} headlines directly to Snowflake.")

def main():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        ingest_daily_candles(cursor)
        ingest_news_headlines(cursor)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()