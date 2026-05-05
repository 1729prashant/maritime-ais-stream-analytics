# configs/aisstream_config.py
import os
from dotenv import load_dotenv

load_dotenv()

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
AISSTREAM_WEBSOCKET_URL = "wss://stream.aisstream.io/v0/stream"

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW_AIS = "vessels.raw"

KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'ais-producer-01',
    # Ensure messages are durable
    'acks': 'all' 
}

# Control Logic
MESSAGE_CAP = int(os.getenv("MESSAGE_CAP", 1000000))


# Define the region filter (bounding box: min_lat, min_lon, max_lat, max_lon)
# This bounding box is designed to cover:
# - Left: Entire coast of West India
# - Right: East-most coast of Japan
# - Lower: Just above Australia (approximately the Equator)
# - Upper: Northern parts of Japan
REGION_FILTER = {
    "min_lat": 0.0,   # Approximately the Equator, just above Australia
    "max_lat": 45.0,  # Covers northern parts of Japan
    "min_lon": 68.0,  # West coast of India
    "max_lon": 148.0  # East-most coast of Japan
}


DEBUG_UNKNOWN_MESSAGES = False   # Set to True only during development/troubleshooting