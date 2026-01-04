import json
import time
import random
from kafka import KafkaProducer
from configs.kafka_config import KAFKA_BOOTSTRAP_SERVERS, ORDER_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_order_event():
    return {
        "order_id": random.randint(50000, 99999),
        "user_id": random.randint(1000, 9999),
        "amount": round(random.uniform(100, 5000), 2),
        "timestamp": int(time.time())
    }

if __name__ == "__main__":
    while True:
        event = generate_order_event()
        producer.send(ORDER_TOPIC, event)
        print(f"Sent order event → {event}")
        time.sleep(3)
