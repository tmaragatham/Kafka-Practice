import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "stock-ticks"
SYMBOLS = ["TCS", "INFY", "RELIANCE", "HDFCBANK", "ICICIBANK"]

# Starting prices used for simulated stock market ticks
prices = {
    "TCS": 3850.0,
    "INFY": 1750.0,
    "RELIANCE": 2950.0,
    "HDFCBANK": 1680.0,
    "ICICIBANK": 1250.0,
}

producer = KafkaProducer(
    bootstrap_servers="kafka:29092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Streaming simulated stock ticks to topic '{TOPIC}'... Press Ctrl+C to stop.")

try:
    while True:
        symbol = random.choice(SYMBOLS)

        # Simulate a small market price movement
        change = random.uniform(-0.02, 0.02)
        prices[symbol] = round(prices[symbol] * (1 + change), 2)

        # Occasionally generate a larger movement for alert demonstration
        if random.random() < 0.05:
            prices[symbol] = round(prices[symbol] * random.choice([0.94, 1.06]), 2)

        tick = {
            "symbol": symbol,
            "price": prices[symbol],
            "volume": random.randint(1, 10000),
            "change_percent": round(change * 100, 2),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }

        meta = producer.send(TOPIC, key=symbol, value=tick).get(timeout=10)
        print(
            f"[SENT] partition={meta.partition} offset={meta.offset} "
            f"symbol={symbol} price={tick['price']} volume={tick['volume']}"
        )
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopped producer.")
finally:
    producer.close()
