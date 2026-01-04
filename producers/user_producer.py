import json
import time
import random
from kafka import KafkaProducer
from configs.kafka_config import KAFKA_BOOTSTRAP_SERVERS, USER_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_user_event():
    return {
        "user_id": random.randint(1000, 9999),
        "event": random.choice(["login", "logout", "purchase"]),
        "timestamp": int(time.time())
    }

if __name__ == "__main__":
    while True:
        event = generate_user_event()
        producer.send(USER_TOPIC, event)
        print(f"Sent user event → {event}")
        time.sleep(2)
