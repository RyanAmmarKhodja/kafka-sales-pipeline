import os
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'customer-orders'

print(f"Connecting Order Producer to Kafka at {bootstrap_servers}...")

producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Order Producer successfully connected to Kafka!")
    except Exception as e:
        print(f"Kafka not ready yet. Retrying in 3 seconds... (Error: {e})")
        time.sleep(3)

print(f"Starting Order Producer. Sending messages to topic '{TOPIC_NAME}'...")

try:
    while True:
        order = {
            "order_id": random.randint(10000, 99999),
            "user_id": f"user_{random.randint(1, 500)}",
            "items": [
                {"item_id": "item_abc", "qty": random.randint(1, 3)},
                {"item_id": "item_xyz", "qty": random.randint(1, 2)}
            ],
            "total_amount": round(random.uniform(20.0, 1500.0), 2),
            "timestamp": datetime.utcnow().isoformat()
        }
        producer.send(TOPIC_NAME, value=order)
        print(f"Sent Order: {order['order_id']} | Amount: ${order['total_amount']}")
        producer.flush()
        time.sleep(2)
except KeyboardInterrupt:
    print("Producer stopped.")
    producer.close()
