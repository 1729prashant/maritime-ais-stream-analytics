import asyncio
import logging
from math import prod
from ingestion.producers.aisstream_producer import produce_ais_stream

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


async def main_async():
    logger.info("Starting AISStream producer...")
    try:
        await produce_ais_stream()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error in main application loop: {e}", exc_info=True)
    finally:
        logger.info("Producer shut down cleanly.")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()