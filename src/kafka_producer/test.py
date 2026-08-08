from confluent_kafka import Producer
from datetime import datetime
from faker import Faker
from random import randint
import uuid
import json
import time
import os


config = {
    'bootstrap.servers': 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
    'client.id': 'transaction-producer',
    'acks': 'all',
    'retries': 5,
    'batch.size': 16384,
    'linger.ms': 5,
    'compression.type': 'gzip'
}

fake = Faker()

# Initialize the Kafka producer
producer = Producer(config)

# Define the topic to send data to
topic = 'transactions'


# Callback to handle delivery reports (called once for each message)
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [Partition: {msg.partition()}] at Offset: {msg.offset()}")

# Function to produce messages to Kafka
def produce_messages():
    for i in range(10):
        key = f"key-{randint(500, 1000)}"
        value = json.dumps({"id": randint(1,100), "message": f"sample message {i}"})

        print(f"Producing message : Key = {key} Value = {value}")
        # Send message with key-value and delivery report callback
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=delivery_report

        )

        # Poll to trigger the delivery report callback
        producer.poll(0)

        # Optional: Add delay for demonstration purposes
        time.sleep(1)

    # Flush the producer to ensure all messages are sent
    producer.flush()


# Run the producer function
if __name__ == "__main__":
    produce_messages()