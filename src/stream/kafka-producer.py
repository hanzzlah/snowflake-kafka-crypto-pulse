import json
import asyncio
import websockets # type:ignore
from kafka import KafkaProducer # type:ignore

KAFKA_BROKER = "localhost:9092"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
TOPIC_LIVE_TICKS = "price_ticks"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8") if k else b""
)

async def produce_live_trades_for_symbol(symbol: str):
    ws_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                print(f"[Live Ticks] Connected: {symbol} ({ws_url})")
                while True:
                    msg = await ws.recv()
                    payload = json.loads(msg)
                    # Sends payload to Kafka with the respective symbol key
                    producer.send(TOPIC_LIVE_TICKS, key=symbol, value=payload)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[Live Ticks] {symbol} error ({e}). Reconnecting in 5s...")
            await asyncio.sleep(5)

async def main():
    # Stream BTC, ETH, and SOL concurrently
    await asyncio.gather(*(produce_live_trades_for_symbol(symbol) for symbol in SYMBOLS))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        producer.flush()
        producer.close()