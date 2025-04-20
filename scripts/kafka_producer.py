# scripts/kafka_producer.py

import pandas as pd
from kafka import KafkaProducer
import json
import time

def stream_user_product_pairs(prior_csv, orders_csv, topic="reorder-stream"):
    print("📦 Loading prior and order data...")
    prior = pd.read_csv(prior_csv)
    orders = pd.read_csv(orders_csv)

    # ✅ Merge to attach user_id to each product in prior
    merged = pd.merge(prior, orders[['order_id', 'user_id']], on='order_id', how='left')

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    print(f"📡 Streaming to Kafka topic: {topic}...")

    for _, row in merged.iterrows():
        message = {
            "user_id": int(row["user_id"]),
            "product_id": int(row["product_id"])
        }
        producer.send(topic, value=message)
        time.sleep(0.001)  # Simulate real-time delay

    print("✅ Finished streaming 32M+ user-product pairs.")
