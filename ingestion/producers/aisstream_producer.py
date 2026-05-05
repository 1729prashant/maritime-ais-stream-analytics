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
    KAFKA_TOPIC_RAW_AIS,
    MESSAGE_CAP
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
    Implements reconnection logic and a message cap for controlled execution.
    """
    # Initialize Kafka Producer
    # Local: 'bootstrap.servers': 'localhost:9092'
    producer = Producer(KAFKA_CONFIG)
    
    connection_attempts = 0
    total_message_count = 0

    while total_message_count < MESSAGE_CAP:
        connection_attempts += 1
        logger.info(f"Connection attempt {connection_attempts} to {AISSTREAM_WEBSOCKET_URL}")

        try:
            async with websockets.connect(AISSTREAM_WEBSOCKET_URL) as websocket:
                logger.info("WebSocket Connection Established.")

                # Subscription payload per AISStream API specs
                subscribe_message = {
                    "APIKey": AISSTREAM_API_KEY,
                    "BoundingBoxes": [
                        [
                            [REGION_FILTER["min_lat"], REGION_FILTER["min_lon"]],
                            [REGION_FILTER["max_lat"], REGION_FILTER["max_lon"]]
                        ]
                    ],
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }
                
                await websocket.send(json.dumps(subscribe_message))
                logger.info(f"Subscription sent for region: {REGION_FILTER}")

                while total_message_count < MESSAGE_CAP:
                    try:
                        # Receive raw JSON string from WebSocket
                        raw_message = await websocket.recv()
                        
                        # Forward to Kafka immediately - NO PARSING
                        # We use encode('utf-8') as Kafka expects bytes
                        producer.produce(
                            topic=KAFKA_TOPIC_RAW_AIS, 
                            value=raw_message.encode('utf-8'),
                            callback=delivery_report
                        )
                        
                        # Serve delivery callbacks from previous asynchronous produces
                        producer.poll(0)
                        
                        total_message_count += 1

                        if total_message_count % 1000 == 0:
                            logger.info(f"Streamed {total_message_count} messages to Kafka...")

                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("WebSocket connection closed by server. Retrying...")
                        break 
                    except Exception as e:
                        logger.error(f"Error in recv loop: {e}")
                        break

        except Exception as e:
            logger.error(f"Failed to connect: {e}. Retrying in 10s...")
            await asyncio.sleep(10)

        # Ensure all messages are sent before a potential reconnect
        producer.flush()

    logger.info(f"Message cap of {MESSAGE_CAP} reached. Producer shutting down.")

if __name__ == "__main__":
    try:
        asyncio.run(produce_ais_stream())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")