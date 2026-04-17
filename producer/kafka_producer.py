"""Publishes synthetic transaction events to a Kafka topic."""

import json, random, time, uuid
from datetime import datetime
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC  = "transactions"

CATEGORIES = ["electronics", "clothing", "food", "travel"]
REGIONS    = ["us-east", "us-west", "eu-central", "ap-south"]


def make_event():
    amount = round(random.gauss(150, 50), 2)
    if random.random() < 0.02:          # ~2% anomalies
        amount = round(random.uniform(5000, 20000), 2)
    return {
        "id":         str(uuid.uuid4()),
        "user_id":    f"user_{random.randint(1, 1000)}",
        "category":   random.choice(CATEGORIES),
        "region":     random.choice(REGIONS),
        "amount":     amount,
        "timestamp":  datetime.utcnow().isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",
    )
    print(f"Publishing to '{TOPIC}' on {BROKER} — Ctrl+C to stop")
    count = 0
    while True:
        producer.send(TOPIC, make_event())
        count += 1
        if count % 100 == 0:
            print(f"  Published {count} events")
        time.sleep(0.05)


if __name__ == "__main__":
    main()
