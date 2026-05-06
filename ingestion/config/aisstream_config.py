# ingestion/config/aisstream_config.py

import os
from dotenv import load_dotenv

load_dotenv()


# ----------------------------------------------------------------------
# AISStream
# ----------------------------------------------------------------------
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
AISSTREAM_WEBSOCKET_URL = "wss://stream.aisstream.io/v0/stream"


# ----------------------------------------------------------------------
# Kafka (Producer + Consumer)
# ----------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Use ONE canonical topic name
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "vessels.raw")

KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ais-consumer-group")

KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": os.getenv("KAFKA_CLIENT_ID", "ais-producer-01"),

    # Durability
    "acks": "all",
    "enable.idempotence": True,

    # Retry
    "retries": 5,
    "retry.backoff.ms": 1000,

    # Batching (light tuning)
    "linger.ms": 5,
    "batch.num.messages": 1000,
}


# ----------------------------------------------------------------------
# Pipeline Control
# ----------------------------------------------------------------------
MESSAGE_CAP = int(os.getenv("MESSAGE_CAP", 1_000_000))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", 5))  # seconds


# ----------------------------------------------------------------------
# Sink Configuration
# ----------------------------------------------------------------------
SINK_TYPE = os.getenv("SINK_TYPE", "duckdb")  # duckdb | gcs

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/ais.duckdb")

GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_PREFIX = os.getenv("GCS_PREFIX", "ais")


# ----------------------------------------------------------------------
# Region Filter
# ----------------------------------------------------------------------
REGION_FILTER = {
    "min_lat": 0.0,
    "max_lat": 45.0,
    "min_lon": 68.0,
    "max_lon": 148.0,
}


# ----------------------------------------------------------------------
# Debugging
# ----------------------------------------------------------------------
DEBUG_UNKNOWN_MESSAGES = os.getenv("DEBUG_UNKNOWN_MESSAGES", "false").lower() == "true"


# ----------------------------------------------------------------------
# Validation (lightweight, mode-aware)
# ----------------------------------------------------------------------
def validate_config():
    if not AISSTREAM_API_KEY:
        raise ValueError("AISSTREAM_API_KEY is required")

    if SINK_TYPE not in ("duckdb", "gcs"):
        raise ValueError(f"Invalid SINK_TYPE: {SINK_TYPE}")

    if SINK_TYPE == "gcs":
        if not GCS_BUCKET:
            raise ValueError("GCS_BUCKET must be set when SINK_TYPE=gcs")

    if BATCH_SIZE <= 0:
        raise ValueError("BATCH_SIZE must be > 0")

    if FLUSH_INTERVAL <= 0:
        raise ValueError("FLUSH_INTERVAL must be > 0")


# Optional: call explicitly in entrypoints (recommended)
# validate_config()