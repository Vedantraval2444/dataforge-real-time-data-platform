import json
import time
import random
from kafka import KafkaProducer
from pymongo import MongoClient
from datetime import datetime

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# MongoDB Connection
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["dataforge"]
collection = db["raw_user_events"]

users = [101, 102, 103, 104]
events = ["login", "click", "logout"]

while True:
    event = {
        "user_id": random.choice(users),
        "event_type": random.choice(events),
        "timestamp": datetime.utcnow().isoformat()
    }

    # Send to Kafka
    producer.send("user_events", event)

    # Store raw in MongoDB
    collection.insert_one(event)

    print("Sent:", event)

    time.sleep(2)
