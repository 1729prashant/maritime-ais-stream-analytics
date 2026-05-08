import asyncio
import websockets
import json
import os
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv

# Import existing configurations
from ingestion.config.aisstream_config import (
    REGION_FILTER, 
    AISSTREAM_API_KEY,
    AISSTREAM_WEBSOCKET_URL,
    KAFKA_CONFIG,
    KAFKA_TOPIC,
    MESSAGE_CAP,
    validate_config
)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AISStreamProducer")

# --- Kafka Delivery Callback ---
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        # Avoid high-volume logging in production; use debug for offset tracking
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

async def produce_ais_stream():
    """
    Connects to AISStream WebSocket and streams raw JSON directly to Kafka.
    Implements:
    - Reconnection logic
    - Backpressure handling
    - Partition key (MMSI-based)
    - Message cap
    """
    validate_config()
    
    producer = Producer(KAFKA_CONFIG)

    connection_attempts = 0
    total_message_count = 0

    try:
        while total_message_count < MESSAGE_CAP:
            connection_attempts += 1
            logger.info(f"Connection attempt {connection_attempts} to {AISSTREAM_WEBSOCKET_URL}")

            try:
                async with websockets.connect(AISSTREAM_WEBSOCKET_URL) as websocket:
                    logger.info("Successfully connected to AISStream WebSocket.")

                    subscribe_message = {
                        "APIKey": AISSTREAM_API_KEY,
                        "BoundingBoxes": [[
                            [REGION_FILTER["min_lat"], REGION_FILTER["min_lon"]],
                            [REGION_FILTER["max_lat"], REGION_FILTER["max_lon"]]
                        ]],
                        "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                    }

                    await websocket.send(json.dumps(subscribe_message))
                    logger.info(f"Subscription sent for region: {REGION_FILTER}")

                    while total_message_count < MESSAGE_CAP:
                        try:
                            raw_message = await websocket.recv()

                            # --- Extract partition key (MMSI) ---
                            key = None
                            try:
                                msg_json = json.loads(raw_message)

                                # Try PositionReport
                                mmsi = (
                                    msg_json.get("Message", {})
                                    .get("PositionReport", {})
                                    .get("UserID")
                                )

                                # Fallback to ShipStaticData
                                if not mmsi:
                                    mmsi = (
                                        msg_json.get("Message", {})
                                        .get("ShipStaticData", {})
                                        .get("UserID")
                                    )

                                if mmsi:
                                    key = str(mmsi)

                            except Exception:
                                # Do not fail pipeline due to malformed JSON
                                key = None

                            # --- Backpressure-safe produce ---
                            while True:
                                try:
                                    # Fix: Determine if we have bytes or str to avoid AttributeError
                                    if isinstance(raw_message, str):
                                        kafka_value = raw_message.encode('utf-8')
                                    else:
                                        kafka_value = raw_message # Already bytes

                                    producer.produce(
                                        topic=KAFKA_TOPIC,
                                        key=key,
                                        value=kafka_value,
                                        callback=delivery_report
                                    )
                                    break
                                except BufferError:
                                    # Queue full → wait for delivery reports
                                    producer.poll(1)

                            # Serve delivery callbacks
                            producer.poll(0)

                            total_message_count += 1

                            if total_message_count % 1000 == 0:
                                logger.info(f"Streamed {total_message_count} messages to Kafka...")

                        except asyncio.CancelledError:
                            logger.info("Producer task cancelled.")
                            raise

                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("WebSocket connection closed by server. Retrying...")
                            break

                        except Exception as e:
                            logger.error(f"Error in recv loop: {e}", exc_info=True)
                            break

            except Exception as e:
                logger.error(f"Failed to connect: {e}. Retrying in 10s...")
                await asyncio.sleep(10)

            # Flush before reconnect
            producer.flush()

    finally:
        # Critical: ensure no data loss on shutdown
        logger.info("Flushing producer before shutdown...")
        producer.flush()

    logger.info(f"Message cap of {MESSAGE_CAP} reached. Producer shutting down.")

# if __name__ == "__main__":
#     try:
#         asyncio.run(produce_ais_stream())
#     except KeyboardInterrupt:
#         logger.info("Producer stopped by user.")
#     except Exception as e:
#         logger.critical(f"Fatal error: {e}")