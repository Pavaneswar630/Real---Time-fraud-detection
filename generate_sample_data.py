#!/usr/bin/env python3
"""
Synthetic Transaction Stream Generator
======================================
Produces high-frequency JSON transaction events to Kafka topic `raw-transactions`.
Can be used to populate Kafka KRaft for PySpark Structured Streaming validation.
"""

import os
import sys
import json
import time
import random
import uuid
from datetime import datetime, timezone

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "raw-transactions")


def create_producer():
    """Attempt to initialize kafka producer if kafka-python or confluent-kafka is present."""
    try:
        from kafka import KafkaProducer
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except ImportError:
        print("Note: 'kafka-python' not installed in current environment. Producer will print sample messages.")
        return None


def generate_transaction(user_pool_size=100):
    """Generate a realistic synthetic transaction."""
    user_id = f"user_{random.randint(1, user_pool_size)}"
    amount = round(random.expovariate(1 / 150.0) + 5.0, 2)  # Skewed towards typical purchase sizes
    
    # 2% chance of an anomalous transaction spike
    if random.random() < 0.02:
        amount = round(random.uniform(2500.0, 9500.0), 2)

    return {
        "transaction_id": f"txn_{uuid.uuid4()}",
        "user_id": user_id,
        "amount": amount,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def stream_transactions(rate_per_sec=10, max_messages=100):
    producer = create_producer()
    print(f"Starting synthetic transaction stream to {TOPIC_NAME} at ~{rate_per_sec} msg/sec...")
    count = 0

    try:
        while max_messages is None or count < max_messages:
            txn = generate_transaction()
            if producer:
                producer.send(TOPIC_NAME, value=txn)
            print(f"[{count+1}] Produced: {txn['user_id']} | ${txn['amount']} | {txn['transaction_id']}")
            count += 1
            time.sleep(1.0 / rate_per_sec)
        if producer:
            producer.flush()
        print(f"Successfully finished streaming {count} transactions.")
    except KeyboardInterrupt:
        print("\nStreaming paused by user.")


if __name__ == "__main__":
    stream_transactions(rate_per_sec=5, max_messages=25)
