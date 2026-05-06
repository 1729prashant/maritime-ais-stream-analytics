"""
Kafka Consumer for AIS Stream Pipeline

Responsibilities:
- Consume raw AIS JSON messages from Kafka
- Call parser (pure function) to extract structured data
- Buffer PositionReport and ShipStaticData separately
- Flush buffers based on:
    1. Batch size
    2. Time interval
- Delegate persistence to sink layer

This module MUST NOT:
- Contain parsing logic
- Contain WebSocket logic
- Contain sink-specific implementation details
"""

import json
import logging
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer

# Config
from ingestion.config.aisstream_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    BATCH_SIZE,
    FLUSH_INTERVAL,
)

# Parser (pure function)
from processing.utils.ais_parser import extract_ais_data

# Sink abstraction
from storage.utils.sink import get_sink


# -------------------------------
# Logging
# -------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# -------------------------------
# Buffers (global, controlled)
# -------------------------------
position_buffer = []
static_buffer = []

last_flush_time = datetime.now(timezone.utc)


# -------------------------------
# Sink Initialization
# -------------------------------
sink = get_sink()


# -------------------------------
# Flush Logic
# -------------------------------
def flush_buffers():
    """
    Flush buffered records to sink.

    Delegates actual storage to sink implementation.
    Keeps this layer storage-agnostic.
    """
    global position_buffer, static_buffer, last_flush_time

    if not position_buffer and not static_buffer:
        return

    try:
        logger.info(
            f"Flushing buffers | Position: {len(position_buffer)}, Static: {len(static_buffer)}"
        )

        sink.flush(position_buffer, static_buffer)

        # Clear buffers after successful flush
        position_buffer.clear()
        static_buffer.clear()

        last_flush_time = datetime.now(timezone.utc)

    except Exception as e:
        logger.error(f"Error flushing buffers: {e}", exc_info=True)


# -------------------------------
# Message Handling
# -------------------------------
def handle_message(extracted_data: dict, message_type: str):
    """
    Route parsed AIS data into appropriate buffers.

    Args:
        extracted_data: Parsed AIS record (dict)
        message_type: AIS message type
    """
    global position_buffer, static_buffer

    if message_type in ("PositionReport", "StandardClassBPositionReport"):
        position_buffer.append({
            "timestamp": extracted_data.get("timestamp"),
            "mmsi": extracted_data.get("mmsi"),
            "latitude": extracted_data.get("latitude"),
            "longitude": extracted_data.get("longitude"),
            "sog": extracted_data.get("sog"),
            "cog": extracted_data.get("cog"),
            "true_heading": extracted_data.get("true_heading"),
            "nav_status": extracted_data.get("nav_status"),
            "rot": extracted_data.get("rot"),
        })

    elif message_type == "ShipStaticData":
        static_buffer.append({
            "timestamp": extracted_data.get("timestamp"),
            "mmsi": extracted_data.get("mmsi"),
            "imo_number": extracted_data.get("imo_number"),
            "call_sign": extracted_data.get("call_sign"),
            "vessel_name": extracted_data.get("vessel_name"),
            "vessel_type": extracted_data.get("vessel_type"),
            "length": extracted_data.get("length"),
            "width": extracted_data.get("width"),
            "draught": extracted_data.get("draught"),
            "destination": extracted_data.get("destination"),
            "eta_month": extracted_data.get("eta_month"),
            "eta_day": extracted_data.get("eta_day"),
            "eta_hour": extracted_data.get("eta_hour"),
            "eta_minute": extracted_data.get("eta_minute"),
        })


# -------------------------------
# Flush Policy
# -------------------------------
def maybe_flush():
    """
    Decide whether buffers should be flushed based on:
    - Batch size
    - Time interval
    """
    global last_flush_time

    now = datetime.now(timezone.utc)

    if (
        len(position_buffer) >= BATCH_SIZE
        or len(static_buffer) >= BATCH_SIZE
    ):
        flush_buffers()

    elif (now - last_flush_time).total_seconds() >= FLUSH_INTERVAL:
        flush_buffers()


# -------------------------------
# Kafka Consumer Loop
# -------------------------------
def run_consumer():
    """
    Main Kafka consumer loop.

    - Reads raw JSON messages
    - Parses via ais_parser
    - Buffers and flushes
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    logger.info("Kafka consumer started...")

    message_count = 0

    try:
        for msg in consumer:
            raw_message = msg.value

            try:
                extracted_data, message_type = extract_ais_data(raw_message)

                if extracted_data and message_type:
                    handle_message(extracted_data, message_type)
                    maybe_flush()

                    message_count += 1

                    if message_count % 10000 == 0:
                        logger.info(f"Processed {message_count} messages")

                else:
                    logger.debug("Parser returned empty result")

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")

    finally:
        logger.info("Final flush before shutdown")
        flush_buffers()
        consumer.close()


# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    run_consumer()