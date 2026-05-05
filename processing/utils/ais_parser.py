"""
AIS Message Parser - Pure functions for parsing AISStream WebSocket messages.

This module contains pure, stateless functions responsible for:
- Extracting structured data from raw AISStream JSON messages
- Field mapping and type coercion for PositionReport and ShipStaticData
- Decoding low-level AIS fields (e.g., CommunicationState)

These functions have **no side effects, no I/O, and no external dependencies**
other than the standard library. They are designed to be used in both local
and cloud (GCP) environments, and are unit-testable.

"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def extract_ais_data(
    message: Dict[str, Any], 
    debug_unknown: bool = False
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Extracts structured AIS data from raw AISStream JSON.

    Args:
        message: Raw message from AISStream WebSocket
        debug_unknown: If True, logs full raw JSON for unhandled/empty messages.
                       Controlled via config in higher layers (consumer/producer).

    Returns:
        (extracted_data, message_type)
    """
    extracted_data: Dict[str, Any] = {}
    message_type: Optional[str] = None

    try:
        # --- Timestamp Normalization ---
        ts = message.get('Time')
        if ts:
            if isinstance(ts, str):
                # AISStream returns Z-suffixed ISO strings
                extracted_data['timestamp'] = datetime.fromisoformat(
                    ts.replace('Z', '+00:00')
                )
            else:
                extracted_data['timestamp'] = ts
        else:
            extracted_data['timestamp'] = datetime.now(timezone.utc)

        message_type = message.get('MessageType')

        # --- Position Reports ---
        if message_type in ['PositionReport', 'StandardClassBPositionReport']:
            report = message.get('Message', {}).get(message_type, {})
            extracted_data.update({
                'mmsi': report.get('UserID'),
                'latitude': report.get('Latitude'),
                'longitude': report.get('Longitude'),
                'sog': report.get('Sog'),
                'cog': report.get('Cog'),
                'true_heading': report.get('TrueHeading'),
                'nav_status': report.get('NavigationalStatus'),
                'rot': report.get('RateOfTurn'),
                'position_accuracy': report.get('PositionAccuracy'),
                'raim': report.get('Raim'),
            })

        # --- Ship Static Data ---
        elif message_type == 'ShipStaticData':
            report = message.get('Message', {}).get(message_type, {})
            dim = report.get('Dimension', {})
            
            length = width = None
            try:
                length = int(dim.get('A', 0)) + int(dim.get('B', 0))
                width = int(dim.get('C', 0)) + int(dim.get('D', 0))
            except (TypeError, ValueError):
                pass

            eta = report.get('Eta', {})
            extracted_data.update({
                'mmsi': report.get('UserID'),
                'imo_number': report.get('ImoNumber'),
                'call_sign': report.get('CallSign'),
                'vessel_name': report.get('Name'),
                'vessel_type': report.get('Type'),
                'length': length,
                'width': width,
                'draught': report.get('MaximumStaticDraught'),
                'destination': report.get('Destination'),
                'eta_month': eta.get('Month'),
                'eta_day': eta.get('Day'),
                'eta_hour': eta.get('Hour'),
                'eta_minute': eta.get('Minute'),
            })

        else:
            logger.debug(f"Unhandled message type: {message_type}")

    except KeyError as e:
        logger.warning(f"KeyError extracting AIS data: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in extract_ais_data: {e}", exc_info=True)

    # === Debug logging for unknown / empty messages ===
    if (not extracted_data or len(extracted_data) <= 1) and message_type:
        if debug_unknown or logger.isEnabledFor(logging.DEBUG):
            try:
                raw_preview = json.dumps(message)[:1500]  # prevent massive logs
                logger.debug(f"No significant data extracted. Type: {message_type} | Raw: {raw_preview}")
            except Exception:
                logger.debug(f"No significant data extracted for type: {message_type}")

    return extracted_data, message_type


def decode_communication_state(comm_state: Optional[int]) -> Dict[str, Any]:
    """... (unchanged from previous version - clean and robust) ..."""
    if comm_state is None:
        return {"Type": "Unknown", "Reason": "No communication state provided"}
    
    # [same implementation as before]
    try:
        comm_state_19bit = int(comm_state) & 0x7FFFF
        selector_flag = (comm_state_19bit >> 18) & 0x1

        if selector_flag == 0:
            return {
                "Type": "SOTDMA",
                "SelectorFlag": selector_flag,
                "SyncState": (comm_state_19bit >> 16) & 0x3,
                "SlotTimeout": (comm_state_19bit >> 13) & 0x7,
                "SubMessage": comm_state_19bit & 0x1FFF
            }
        else:
            return {
                "Type": "ITDMA",
                "SelectorFlag": selector_flag,
                "SyncState": (comm_state_19bit >> 16) & 0x3,
                "SlotIncrement": (comm_state_19bit >> 3) & 0x1FFF,
                "NumSlots": (comm_state_19bit) & 0x7,
                "KeepFlag": comm_state_19bit & 0x1
            }
    except Exception as e:
        logger.warning(f"Failed to decode communication state {comm_state}: {e}")
        return {"Type": "Unknown", "Error": str(e)}


def parse_ais_messages_batch(messages: list[Dict[str, Any]], debug_unknown: bool = False) -> list[Dict[str, Any]]:
    """Batch parsing helper."""
    return [
        data for msg in messages 
        if (data := extract_ais_data(msg, debug_unknown=debug_unknown)[0]) and len(data) > 1
    ]