import asyncio
import websockets
import json
import os
import logging

import os, time, json
import duckdb
import pandas as pd

from google.cloud import bigquery

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

SINK_TYPE = os.getenv("SINK_TYPE", "duckdb")  # duckdb | bigquery
if not SINK_TYPE:
    raise ValueError("SINK_TYPE not found in .env file or environment variables.")

DUCKDB_PATH = os.getenv("SINK_TYPE", "data/ais.db")
if not SINK_TYPE:
    raise ValueError("SINK_TYPE not found in .env file or environment variables.")

BQ_PROJECT = os.getenv("BQ_PROJECT")
if not BQ_PROJECT:
    raise ValueError("BQ_PROJECT not found in .env file or environment variables.")

BQ_DATASET = os.getenv("BQ_DATASET")
if not BQ_DATASET:
    raise ValueError("BQ_DATASET not found in .env file or environment variables.")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
if not BATCH_SIZE:
    raise ValueError("BATCH_SIZE not found in .env file or environment variables.")


# --- Database connections ---
# TODO: Check logic in flush_buffers
def get_duckdb_conn():
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS PositionReport (
      MMSI BIGINT, Latitude DOUBLE, Longitude DOUBLE, Timestamp TIMESTAMP, Speed DOUBLE, Course DOUBLE
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS ShipStaticData (
      MMSI BIGINT, Name VARCHAR, IMO BIGINT, CallSign VARCHAR, VesselType VARCHAR, Length DOUBLE, Width DOUBLE
    );
    """)
    return conn

def get_bq_client():
    client = bigquery.Client(project=BQ_PROJECT)
    return client

def get_sink():
    if SINK_TYPE == "duckdb":
        return get_duckdb_conn()
    else:
        return get_bq_client()

# TODO: use these or remove
position_buffer = []
static_buffer = []
sink = get_sink()

# TODO: use these or remove
def flush_buffers():
    global position_buffer, static_buffer
    if not position_buffer and not static_buffer:
        return
    if SINK_TYPE == "duckdb":
        if position_buffer:
            df = pd.DataFrame(position_buffer)
            sink.register("tmp_pos", df)
            sink.execute("INSERT INTO PositionReport SELECT * FROM tmp_pos")
        if static_buffer:
            df = pd.DataFrame(static_buffer)
            sink.register("tmp_static", df)
            sink.execute("INSERT INTO ShipStaticData SELECT * FROM tmp_static")
    else:
        # BigQuery: use insert_rows_json or load job (implement per BQ best practice)
        # Example: client.insert_rows_json(table_ref, position_buffer)
        pass
    position_buffer, static_buffer = [], []

# TODO: use these or remove
async def handle_message(msg):
    data = json.loads(msg)
    if data["type"] == "PositionReport":
        position_buffer.append({
          "MMSI": data["mmsi"], "Latitude": data["lat"], "Longitude": data["lon"],
          "Timestamp": data["ts"], "Speed": data["speed"], "Course": data["course"]
        })
    else:
        static_buffer.append({...})
    if len(position_buffer) >= BATCH_SIZE or len(static_buffer) >= BATCH_SIZE:
        flush_buffers()


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
        # navigation status the vessel is in (e.g. underway using engine, anchored, etc.).
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
            extracted_data['sog'] = report.get('Sog') 
            extracted_data['cog'] = report.get('Cog') 
            extracted_data['true_heading'] = report.get('TrueHeading') 
            extracted_data['nav_status'] = report.get('NavigationalStatus') 
            extracted_data['rot'] = report.get('RateOfTurn')

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
            extracted_data['length'] = int(report.get('Dimension')["A"]) + int(report.get('Dimension')["B"])
            extracted_data['width'] = int(report.get('Dimension')["C"]) + int(report.get('Dimension')["D"])
            extracted_data['draught'] = report.get('MaximumStaticDraught')
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
                    # "FilterMessageTypes":["PositionReport","ShipStaticData"]
                }
                await websocket.send(json.dumps(subscribe_message))
                logger.info(f"Sent subscription message with region filter: {REGION_FILTER} and all message types.")
                
                #ensure at least 1 second delay before any further sends
                # await asyncio.sleep(1)

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
                            # await asyncio.sleep(1)  # throttle processing/logging 1 second
                            message_count += 1 # Increment counter
                            if message_count >= 10000: # Check limit
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



# consumer state in position report
# Example usage
# json_data = {
#     "Cog": 0,
#     "CommunicationState": 59916,
#     "Latitude": 51.44458833333333,
#     "Longitude": 3.590816666666667,
#     "MessageID": 1,
#     "NavigationalStatus": 7,
#     "PositionAccuracy": True,
#     "Raim": True,
#     "RateOfTurn": 0,
#     "RepeatIndicator":0,
#     "Sog": 0,
#     "Spare": 0,
#     "SpecialManoeuvreIndicator":0,
#     "Timestamp": 12,
#     "TrueHeading": 17,
#     "UserID": 245473000,
#     "Valid": True
# }

# decoded = decode_communication_state(json_data["CommunicationState"])
# print(decoded)

def decode_communication_state(comm_state):
    """
    Decode AIS CommunicationState (19-bit field) into human-readable SOTDMA/ITDMA info.

    Args:
        comm_state (int): CommunicationState integer from AIS JSON

    Returns:
        dict: Decoded communication state
    """
    # Mask to get 19 bits
    comm_state_19bit = comm_state & 0x7FFFF  # 19 bits

    # Selector flag (MSB)
    selector_flag = (comm_state_19bit >> 18) & 0x1

    if selector_flag == 0:
        # SOTDMA
        sync_state = (comm_state_19bit >> 16) & 0x3      # 2 bits
        slot_timeout = (comm_state_19bit >> 13) & 0x7    # 3 bits
        sub_message = comm_state_19bit & 0x1FFF          # 13 bits
        return {
            "Type": "SOTDMA",
            "SelectorFlag": selector_flag,
            "SyncState": sync_state,
            "SlotTimeout": slot_timeout,
            "SubMessage": sub_message
        }
    else:
        # ITDMA
        sync_state = (comm_state_19bit >> 16) & 0x3       # 2 bits
        slot_increment = (comm_state_19bit >> 3) & 0x1FFF # 13 bits
        num_slots = (comm_state_19bit >> 0) & 0x7         # 3 bits
        keep_flag = comm_state_19bit & 0x1                # 1 bit
        return {
            "Type": "ITDMA",
            "SelectorFlag": selector_flag,
            "SyncState": sync_state,
            "SlotIncrement": slot_increment,
            "NumSlots": num_slots,
            "KeepFlag": keep_flag
        }

