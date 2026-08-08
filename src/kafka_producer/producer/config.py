# producer/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1]/".env")

required_env_vars = ["BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET", "SCHEMA_REGISTRY_URL", "SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET"]
missing_env_vars = [name for name in required_env_vars if not os.getenv(name)]

if missing_env_vars:
    raise RuntimeError(
        f"Missing required environment variables: {', '.join(missing_env_vars)}"
    )

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
#    'ssl.ca.location': ssl_ca_location,
    'client.id': 'clickstream-producer',
    'acks': 'all',
    'enable.idempotence': True,
    'retries': 5,
    'batch.size': 16384,
    'linger.ms': 5,
    'compression.type': 'gzip'
}

SCHEMA_REGISTRY_CONFIG = {
    'url': os.getenv("SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': (
        f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:"
        f"{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    )
}

TOPIC_NAME = "clickstream_avro"
DEADLETTER_TOPIC = "clickstream_deadletter"