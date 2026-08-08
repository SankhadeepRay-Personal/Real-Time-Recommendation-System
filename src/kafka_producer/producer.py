from confluent_kafka import Producer
from datetime import datetime
from faker import Faker
from random import randint
from dotenv import load_dotenv
from pathlib import Path
import certifi
import uuid
import json
import time
import os


load_dotenv(dotenv_path=Path(__file__).resolve().with_name('.env'))

required_env_vars = ["BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET"]
missing_env_vars = [name for name in required_env_vars if not os.getenv(name)]

if missing_env_vars:
    raise RuntimeError(
        f"Missing required environment variables: {', '.join(missing_env_vars)}"
    )




config = {
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
#    'ssl.ca.location': ssl_ca_location,
#    'client.id': 'transaction-producer',
    'acks': 'all',
    'retries': 3,
    'batch.size': 16384,
    'linger.ms': 5,
    'compression.type': 'gzip'
}



#fake = Faker()

# Initialize the Kafka producer
producer = Producer(config)

# Define the topic to send data to
topic = 'toll-crossings'

vehicles = ['WB06AB1234', 'WB06CD5678', 'WB06EF9012', 'WB06GH3456', 'WB06IJ7890',
            'WB06KL4321', 'WB06MN8765', 'WB06OP8569', 'WB06QR5890', 'WB06ST3759']

toll_dict = {
    1: "Golden Gate Toll",
    2: "Brooklyn Bridge Toll",
    3: "Lincoln Tunnel Toll",
    4: "Holland Tunnel Toll",
    5: "George Washington Bridge Toll",
    6: "Verrazzano-Narrows Toll",
    7: "Mackinac Bridge Toll",
    8: "Tacoma Narrows Toll",
    9: "Chesapeake Bay Toll",
    10: "Tappan Zee Toll"
}

# Callback to handle delivery reports (called once for each message)
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [Partition: {msg.partition()}] at Offset: {msg.offset()}")

# Function to produce messages to Kafka
def produce_messages():
    for i in range(10):
        key = vehicles[i]
        value = json.dumps({"event_id": str(uuid.uuid4()), 
                            "vehicle_number": key, 
                            "toll_id": i, 
                            "toll_name": toll_dict[i+1],
                            "crossing_time": datetime.now().isoformat()})

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