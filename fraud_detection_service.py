import os
import time
import json
from kafka import KafkaConsumer, KafkaProducer

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

TOPIC_INPUT = 'customer-orders'
TOPIC_FRAUD = 'fraud-alerts'
TOPIC_PROCESSED = 'processed-orders'

print(f"Connecting to Kafka at {bootstrap_servers}...")

# Retry loop for Consumer (consuming raw bytes, no value_deserializer warning)
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            TOPIC_INPUT,
            bootstrap_servers=[bootstrap_servers],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='fraud-detection-group'
        )
        print("Fraud Detection Service successfully connected to Kafka Consumer!")
    except Exception as e:
        print(f"Kafka not ready yet. Retrying in 3 seconds... (Error: {e})")
        time.sleep(3)

# Retry loop for Producer
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Fraud Detection Service successfully connected to Kafka Producer!")
    except Exception as e:
        print(f"Kafka producer not ready yet. Retrying in 3 seconds... (Error: {e})")
        time.sleep(3)

print(f"Fraud Detection Service listening on '{TOPIC_INPUT}'...")

try:
    for message in consumer:
        try:
            # Manually decode and parse JSON payload
            order = json.loads(message.value.decode('utf-8'))
        except json.JSONDecodeError:
            print(f"[ERROR] Failed to decode message bytes: {message.value}")
            continue

        order_id = order.get('order_id')
        amount = order.get('total_amount', 0.0)

        if amount > 1000.0:
            print(f"[FRAUD ALERT] Order {order_id} flagged! Amount: ${amount}")
            producer.send(TOPIC_FRAUD, value=order)
        else:
            print(f"[APPROVED] Order {order_id} passed. Amount: ${amount}")
            producer.send(TOPIC_PROCESSED, value=order)

        producer.flush()
except KeyboardInterrupt:
    consumer.close()
    producer.close()
