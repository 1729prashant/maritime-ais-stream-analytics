import asyncio
import logging
from ingestion.consumers.aisstream_websocket_consumer import consume_ais_stream

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting AISStream consumer...")
    try:
        asyncio.run(consume_ais_stream())
    except KeyboardInterrupt:
        logger.info("AISStream consumer stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error in main application loop: {e}", exc_info=True)

if __name__ == "__main__":
    main()
