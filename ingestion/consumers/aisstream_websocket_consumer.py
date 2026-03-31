import asyncio
import websockets
import json
import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timezone # Ensure this import is present and correct

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
if not AISSTREAM_API_KEY:
    raise ValueError("AISSTREAM_API_KEY not found in .env file or environment variables.")

AISSTREAM_WEBSOCKET_URL = "wss://stream.aisstream.io/v0/stream"

from ingestion.config.aisstream_config import REGION_FILTER

# --- Logging Setup ---
log_file_path = os.path.join(os.path.dirname(__file__), 'debug.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file_path) # Add file handler
    ]
)
logger = logging.getLogger(__name__)

# --- Data Extraction Helper ---
def extract_ais_data(message):
    """
    Extracts comprehensive AIS data fields from a raw AISStream JSON message.
    This function makes assumptions about the AISStream JSON structure.
    Adjust field paths based on actual AISStream API documentation.
    """
    extracted_data = {}
    try:
        # Prioritize 'Time' from message, fall back to generating timestamp
        extracted_data['timestamp'] = message.get('Time', datetime.now(timezone.utc).isoformat())
        message_type = message.get('MessageType')
        if message_type:
            logger.debug(f"Received message of type: {message_type}") # Log message type

        # PositionReport
        # An PositionReport AIS message is used to report the vessel's current position, heading, speed, and 
        # other relevant information to other vessels and coastal authorities. This message includes the 
        # vessel's unique MMSI (Maritime Mobile Service Identity) number, the latitude and longitude of its 
        # current position, the vessel's course over ground (COG) and speed over ground (SOG), the type of 
        # navigation status the vessel is in (e.g. underway using engine, anchored, etc.), and the vessel's 
        # dimensional information (length, width, and type). This information is used to help identify and 
        # track vessels in order to improve safety, efficiency, and compliance in the maritime industry.
        # Attributes
        # MessageID Integer
        # RepeatIndicator Integer
        # UserID Integer
        # Valid Boolean
        # NavigationalStatus Integer
        # RateOfTurn Integer
        # Sog Double
        # PositionAccuracy Boolean
        # Longitude Double
        # Latitude Double
        # Cog Double
        # TrueHeading Integer
        # Timestamp Integer
        # SpecialManoeuvreIndicator Integer
        # Spare Integer
        # Raim Boolean
        # CommunicationState Integer 
        if message_type in ['PositionReport', 'StandardClassBPositionReport']:
            report = message['Message'][message_type]

            extracted_data['mmsi'] = report.get('UserID')
            extracted_data['latitude'] = report.get('Latitude')
            extracted_data['longitude'] = report.get('Longitude')
            extracted_data['sog'] = report.get('Sog') # Corrected field name
            extracted_data['cog'] = report.get('Cog') # Corrected field name
            extracted_data['true_heading'] = report.get('TrueHeading') # Corrected field name
            extracted_data['nav_status'] = report.get('NavigationalStatus') # Corrected field name
            extracted_data['rot'] = report.get('RateOfTurn') # Corrected field name

        # ShipStaticData
        # An ShipStaticData AIS message contains static data about the vessel, such as its name, call sign, 
        # length, width, and type of vessel. It also includes information about the vessel's owner or operator, 
        # as well as its place of build and its gross tonnage. This message is transmitted at regular intervals, 
        # usually every few minutes, and is used by other vessels and coastal authorities to identify and track 
        # the vessel. It is an important safety feature that helps to prevent collisions and improve navigation 
        # in crowded waterways.
        # MessageID Integer
        # RepeatIndicator Integer
        # UserID Integer
        # Valid Boolean
        # AisVersion Integer
        # ImoNumber Integer
        # CallSign String
        # Name String
        # Type Integer
        # Dimension ShipStaticData_Dimension
        # FixType Integer
        # Eta ShipStaticData_Eta
        # MaximumStaticDraught Double
        # Destination String
        # Dte Boolean
        # Spare Boolean 
        elif message_type == 'ShipStaticData':
            report = message['Message'][message_type]

            extracted_data['mmsi'] = report.get('UserID')
            extracted_data['imo_number'] = report.get('ImoNumber')
            extracted_data['call_sign'] = report.get('CallSign')
            extracted_data['vessel_name'] = report.get('Name')
            extracted_data['vessel_type'] = report.get('Type')
            # extracted_data['length'] = report.get('Length')
            # extracted_data['width'] = report.get('Width')
            # extracted_data['draught'] = report.get('Draught')
            extracted_data['destination'] = report.get('Destination')
            extracted_data['eta'] = report.get('ETA')

        else:
            logger.debug(f"Unhandled message type: {message_type}. Raw message: {json.dumps(message)}") # Log raw for unhandled

    except KeyError as e:
        logger.warning(f"KeyError during data extraction: {e} in message: {message}")
    except Exception as e:
        logger.error(f"Error extracting AIS data: {e}", exc_info=True)

    # Log raw message if no significant data was extracted, to help debug unknown message types
    if not extracted_data or all(value is None for key, value in extracted_data.items() if key != 'timestamp'):
        logger.debug(f"No significant data extracted. Raw message: {json.dumps(message)}")

    return extracted_data

# --- WebSocket Consumer ---
async def consume_ais_stream():
    """
    Connects to the AISStream WebSocket, filters by region,
    receives and processes JSON events, and handles reconnections.
    """
    connection_attempts = 0
    while True:
        connection_attempts += 1
        logger.info(f"Attempting to connect to AISStream WebSocket (attempt {connection_attempts})...")
        try:
            async with websockets.connect(AISSTREAM_WEBSOCKET_URL) as websocket:
                logger.info("Successfully connected to AISStream WebSocket.")

                # Send subscription message with API key, region filter, and all message types
                subscribe_message = {
                    "APIKey": AISSTREAM_API_KEY,
                    "BoundingBoxes": [
                        [
                            [REGION_FILTER["min_lat"], REGION_FILTER["min_lon"]],
                            [REGION_FILTER["max_lat"], REGION_FILTER["max_lon"]]
                        ]
                    ],
                    "MessageTypes": list(range(1, 28)) # Request all standard AIS message types
                }
                await websocket.send(json.dumps(subscribe_message))
                logger.info(f"Sent subscription message with region filter: {REGION_FILTER} and all message types.")

                message_count = 0 # Initialize counter
                while True:
                    try:
                        message_json = await websocket.recv()
                        logger.debug(f"Received raw message: {message_json}") # Log raw message
                        message = json.loads(message_json)

                        # Process the message
                        extracted_data = extract_ais_data(message)
                        if extracted_data:
                            logger.info(f"Received and extracted AIS data: {extracted_data}")
                            message_count += 1 # Increment counter
                            if message_count >= 100000: # Check limit
                                logger.info(f"Reached {message_count} messages limit. Stopping consumer.")
                                return # Exit the async function
                        else:
                            logger.debug(f"No relevant data extracted from message: {message_json}")

                    except websockets.exceptions.ConnectionClosedOK:
                        logger.info("WebSocket connection closed gracefully.")
                        break # Exit inner loop to trigger reconnection
                    except websockets.exceptions.ConnectionClosedError as e:
                        logger.error(f"WebSocket connection closed with error: {e}. Reconnecting...")
                        break # Exit inner loop to trigger reconnection
                    except json.JSONDecodeError:
                        logger.warning(f"Received non-JSON message or malformed JSON: {message_json}")
                    except asyncio.CancelledError:
                        logger.info("Consumer task cancelled.")
                        raise # Propagate cancellation
                    except Exception as e:
                        logger.error(f"Error receiving or processing message: {e}", exc_info=True)
                        # Consider if this error should break the inner loop or just log and continue
                        # For now, we'll continue to try receiving messages
                        await asyncio.sleep(1) # Small delay to prevent tight loop on persistent errors

        except websockets.exceptions.InvalidURI as e:
            logger.critical(f"Invalid WebSocket URI: {e}. Please check AISSTREAM_WEBSOCKET_URL.")
            return # Cannot recover from invalid URI
        except ConnectionRefusedError:
            logger.error("Connection refused. AISStream server might be down or unreachable. Retrying...")
        except Exception as e:
            logger.error(f"Unhandled connection error: {e}. Retrying in 5 seconds...", exc_info=True)

        await asyncio.sleep(5) # Wait before retrying connection

if __name__ == "__main__":
    try:
        asyncio.run(consume_ais_stream())
    except KeyboardInterrupt:
        logger.info("AISStream consumer stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error in main consumer loop: {e}", exc_info=True)
