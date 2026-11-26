import json
import sys
from kafka import KafkaConsumer

group_id = sys.argv[1] if len(sys.argv) > 1 else "stock-alert-team"
TOPIC = "stock-ticks"


def safe_json_deserialize(v):
    if v is None:
        return None
    text = v.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw_text": text}


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="kafka:29092",
    group_id=group_id,
    auto_offset_reset="earliest",
    value_deserializer=safe_json_deserialize,
)

print(f"Consumer started in group='{group_id}' on topic='{TOPIC}'. Waiting for ticks...")

try:
    for msg in consumer:
        key = msg.key.decode("utf-8") if msg.key else None
        print(
            f"[READ group={group_id}] partition={msg.partition} offset={msg.offset} "
            f"key={key} value={msg.value}"
        )

except KeyboardInterrupt:
    print("\nStopped consumer.")
finally:
    consumer.close()
