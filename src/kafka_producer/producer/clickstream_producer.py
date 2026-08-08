!pip install -r ../requirements.txt authlib fastavro
import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
load_dotenv(".env")
from config import KAFKA_CONFIG, TOPIC_NAME, SCHEMA_REGISTRY_CONFIG, DEADLETTER_TOPIC

#--------------Schema Registry Client-----------------------
schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

#---------------Reading Avro Schema------------------------
with open ("clickstream_schema.avsc") as f:
    schema_str = f.read()

#----------------Defining how to convert obj to dict for Avro Serializer------------------
def to_dict(event, ctx):
    return event          

#----------------Avro Serialization-----------------------
avro_serializer = AvroSerializer(
    schema_registry_client = schema_registry_client,
    schema_str = schema_str,
    to_dict = to_dict,
    conf = {"auto.register.schemas": True}
)

# ---------- Load Dimension Data (keeps clickstream referentially consistent) ----------
#customers_df = pd.read_csv("C:\\Users\\POC\\Data_Engineering_Kafka\\customers.csv")
#products_df = pd.read_csv("C:\\Users\\POC\\Data_Engineering_Kafka\\products.csv")
#BASE_DIR = Path(__file__).resolve().parents[1]
#customers_df = pd.read_csv(BASE_DIR / "customers.csv")
#products_df = pd.read_csv(BASE_DIR / "products.csv")
customers_df = pd.read_csv("customers.csv")
products_df = pd.read_csv("products.csv")

customer_ids = customers_df["customerId"].tolist()
products_by_category = products_df.groupby("category")["productId"].apply(list).to_dict()
product_lookup = products_df.set_index("productId").to_dict("index")
customer_lookup = customers_df.set_index("customerId").to_dict("index")

DEVICE_TYPES = ["Mobile", "Desktop", "Tablet"]
EVENT_FUNNEL_WEIGHTS = {
    "view": 45,
    "click": 20,
    "search": 10,
    "add_to_cart": 12,
    "wishlist_add": 5,
    "remove_from_cart": 3,
    "purchase": 4,
    "review": 1
}

SEARCH_TERMS = {
    "Electronics": ["best smartphone 2026", "wireless earbuds", "gaming laptop", "smartwatch under 5000"],
    "Fashion": ["summer dresses", "running shoes", "formal shirts", "designer bags"],
    "Home & Kitchen": ["non stick cookware", "sofa set", "kitchen organizer"],
    "Books": ["bestseller fiction", "self help books", "kids story books"],
    "Sports & Fitness": ["yoga mat", "dumbbells", "cycling gear"],
    "Beauty & Personal Care": ["anti aging cream", "matte lipstick", "hair serum"],
    "Grocery": ["organic snacks", "cold pressed oil"],
    "Toys & Games": ["lego sets", "board games for kids"]
}

def delivery_report(err, msg):
    if err is not None:
        print(f"message Delivery failed: {err}")
    else:
        print(f"message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def pick_event_type():
    events, weights = zip(*EVENT_FUNNEL_WEIGHTS.items())
    return random.choices(events, weights=weights, k=1)[0]


def pick_product_for_customer(customer):
    """80% chance user browses their preferred category (realistic affinity)."""
    preferred_cat = customer["preferredCategory"]
    if random.random() < 0.8 and preferred_cat in products_by_category:
        category = preferred_cat
    else:
        category = random.choice(list(products_by_category.keys()))

    product_id = random.choice(products_by_category[category])
    return product_id, category


def build_event(session_id, customer_id, event_sequence_num):
    customer = customer_lookup[customer_id]
    product_id, category = pick_product_for_customer(customer)
    product = product_lookup[product_id]
    event_type = pick_event_type()

    event = {
        "eventId": str(uuid.uuid4()),
        "sessionId": session_id,
        "userId": customer_id,
        "productId": product_id,
        "eventType": event_type,
        #"category": category,
        #"subCategory": product["subCategory"],
        #"brand": product["brand"],
        #"price": product["finalPrice"],
        "deviceType": customer["preferredDevice"] if random.random() < 0.7 else random.choice(DEVICE_TYPES),
        "eventSequence": event_sequence_num,
        "searchQuery": None,  # to be filled for search events
        "quantity": None,  # to be filled for add_to_cart and purchase events
        "rating": None,  # to be filled for review events
        "totalAmount": None,  # to be filled for purchase events
        "paymentMode": None,  # to be filled for purchase events
        "event_timestamp": datetime.now(timezone.utc).isoformat()
        #"event_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000)
    }

    # enrich certain event types
    if event_type == "search":
        event["searchQuery"] = random.choice(SEARCH_TERMS.get(category, ["best deals"]))

    if event_type in ("add_to_cart", "purchase"):
        event["quantity"] = random.randint(1, 3)

    if event_type == "purchase":
        event["totalAmount"] = round(product["finalPrice"] * event["quantity"], 2)
        event["paymentMode"] = random.choice(["UPI", "CreditCard", "DebitCard", "COD", "Wallet"])

    if event_type == "review":
        event["rating"] = random.randint(1, 5)

    return event

def send_to_dlt(dlt_producer, event, error):
    dlt_message ={
        "error": str(error),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event
    }

    dlt_producer.produce(
        topic = DEADLETTER_TOPIC,
        key=str(event["userId"]),
        value=json.dumps(dlt_message)
    )
    dlt_producer.flush()
    print("Message sent to DLT")

def simulate_user_session(producer, dlt_producer, customer_id):
    """Simulate a realistic multi-event browsing session per user."""
    session_id = str(uuid.uuid4())
    num_events = random.randint(2, 8)  # a session has multiple clickstream events

    for seq in range(1, num_events + 1):
        event = build_event(session_id, customer_id, seq)
        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=str(customer_id), 
                value=event,
                on_delivery=delivery_report
            )
        except Exception as e:
            print(f"Serialization failed: {e}")
            send_to_dlt(dlt_producer, event, e)

        producer.poll(0)
        time.sleep(random.uniform(0.2, 1.5))  # simulate human browsing delay


def main():
    producer_conf = {
        **KAFKA_CONFIG,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer
    }
    producer = SerializingProducer(producer_conf)
    print(f" Producing clickstream events to topic: {TOPIC_NAME}")

    dlt_producer = Producer({
    "bootstrap.servers" : KAFKA_CONFIG["bootstrap.servers"],
    "security.protocol" : KAFKA_CONFIG["security.protocol"],
    "sasl.mechanisms" : KAFKA_CONFIG["sasl.mechanisms"],
    "sasl.username" : KAFKA_CONFIG["sasl.username"],
    "sasl.password" : KAFKA_CONFIG["sasl.password"],
})

    try:
        while True:
            customer_id = random.choice(customer_ids)
            simulate_user_session(producer, dlt_producer, customer_id)
            producer.flush()
    except KeyboardInterrupt:
        print("\n Stopped by user.")
    finally:
        producer.flush()
        dlt_producer.flush()


if __name__ == "__main__":
    main()