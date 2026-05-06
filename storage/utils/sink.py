"""
storage/utils/sink.py

Sink Abstraction Layer for the AIS Pipeline.

This module provides a clean, extensible abstraction for persisting parsed AIS data.
It supports multiple backends (DuckDB for local/dev, GCS Parquet for production/cloud)
via a common interface.

Responsibilities:
- Environment-aware sink instantiation (based on SINK_TYPE)
- Efficient batch writing (no row-by-row inserts)
- Schema enforcement (via CREATE TABLE IF NOT EXISTS)
- Zero parsing logic — accepts only already-structured data from the parser/consumer

Usage:
    sink = get_sink()
    sink.flush(position_rows, static_rows)
"""

import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

import pandas as pd
import duckdb

from google.cloud import storage

# Centralized configuration
					  
									
from ingestion.config.aisstream_config import (
    SINK_TYPE,
    DUCKDB_PATH,
    GCS_BUCKET,
    GCS_PREFIX,
)

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# DuckDB Helpers
# ----------------------------------------------------------------------
def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    """
    Create and configure a DuckDB connection with required tables.
    Safe to call multiple times (idempotent).
    """
    conn = duckdb.connect(DUCKDB_PATH)

    # Position Report Table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS PositionReport (
            timestamp        TIMESTAMP,
            mmsi             BIGINT,
            latitude         DOUBLE,
            longitude        DOUBLE,
            sog              DOUBLE,
            cog              DOUBLE,
            true_heading     INTEGER,
            nav_status       INTEGER,
            rot              INTEGER,
            position_accuracy BOOLEAN,
            raim             BOOLEAN
        );
    """)

    # Ship Static Data Table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ShipStaticData (
            timestamp     TIMESTAMP,
            mmsi          BIGINT,
            imo_number    INTEGER,
            call_sign     VARCHAR,
            vessel_name   VARCHAR,
            vessel_type   INTEGER,
            length        DOUBLE,
            width         DOUBLE,
            draught       DOUBLE,
            destination   VARCHAR,
            eta_month     INTEGER,
            eta_day       INTEGER,
            eta_hour      INTEGER,
            eta_minute    INTEGER
        );
    """)

    return conn


# ----------------------------------------------------------------------
# GCS Helpers
# ----------------------------------------------------------------------
def get_gcs_client() -> storage.Client:
	   
						  
    """Initialize GCS client. Relies on GOOGLE_APPLICATION_CREDENTIALS or ADC."""
	   
    return storage.Client()


# ----------------------------------------------------------------------
# Sink Interface
# ----------------------------------------------------------------------
class BaseSink:
    """Abstract base class defining the sink contract."""
								 

    def flush(self, position_rows: List[Dict[str, Any]], static_rows: List[Dict[str, Any]]) -> None:
        """Flush batches of parsed rows to the underlying storage."""
        raise NotImplementedError("Subclasses must implement flush()")


# ----------------------------------------------------------------------
# DuckDB Sink (Local Development / Testing)
# ----------------------------------------------------------------------
class DuckDBSink(BaseSink):
    """Persistent DuckDB sink for local use."""

    def __init__(self):
        self.conn = get_duckdb_conn()
        logger.info(f"DuckDB sink initialized with database: {DUCKDB_PATH}")

    def flush(self, position_rows: List[Dict[str, Any]], static_rows: List[Dict[str, Any]]) -> None:
        """
        Efficiently insert batches using pandas DataFrame + temporary view.
        This is significantly faster than row-by-row INSERTs.
        """
        try:
            if position_rows:
                df = pd.DataFrame(position_rows)
                self.conn.register("tmp_pos", df)
									 
                self.conn.execute("INSERT INTO PositionReport SELECT * FROM tmp_pos")
										 
					
                self.conn.unregister("tmp_pos")

            if static_rows:
                df = pd.DataFrame(static_rows)
                self.conn.register("tmp_static", df)
									 
                self.conn.execute("INSERT INTO ShipStaticData SELECT * FROM tmp_static")
											
					
                self.conn.unregister("tmp_static")

            logger.debug(f"Flushed {len(position_rows)} position + {len(static_rows)} static rows to DuckDB")

        except Exception as e:
            logger.error(f"DuckDB flush failed: {e}", exc_info=True)
            raise


# ----------------------------------------------------------------------
# GCS Parquet Sink (Cloud / Production)
# ----------------------------------------------------------------------
class GCSSink(BaseSink):
    """GCS sink that writes partitioned Parquet files."""

    def __init__(self):
        self.client = get_gcs_client()
        self.bucket = self.client.bucket(GCS_BUCKET)
        logger.info(f"GCS sink initialized for bucket: {GCS_BUCKET}")

    def _upload_parquet(self, df: pd.DataFrame, gcs_path: str) -> None:
		   
        """Upload DataFrame as compressed Parquet to GCS."""
        if df.empty:
            return

        blob = self.bucket.blob(gcs_path)
        # Use snappy compression (fast + good ratio) - excellent default for AIS data
        parquet_bytes = df.to_parquet(index=False, compression='snappy')

        blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")
        logger.debug(f"Uploaded {len(df)} rows to gs://{GCS_BUCKET}/{gcs_path}")

    def flush(self, position_rows: List[Dict[str, Any]], static_rows: List[Dict[str, Any]]) -> None:
        """
        Write batches as timestamp-partitioned Parquet files to GCS.

        Partitioning: year/month/day/hour (good balance for AIS query patterns)
											 
									
        """
        try:
            now = datetime.now(timezone.utc)
            partition = now.strftime("%Y/%m/%d/%H")

            if position_rows:
                df_pos = pd.DataFrame(position_rows)
                path = f"{GCS_PREFIX}/PositionReport/{partition}/pos_{now.timestamp():.0f}.parquet"
                self._upload_parquet(df_pos, path)

            if static_rows:
                df_static = pd.DataFrame(static_rows)
                path = f"{GCS_PREFIX}/ShipStaticData/{partition}/static_{now.timestamp():.0f}.parquet"
                self._upload_parquet(df_static, path)

        except Exception as e:
            logger.error(f"GCS flush failed: {e}", exc_info=True)
            raise


# ----------------------------------------------------------------------
# Sink Factory
# ----------------------------------------------------------------------
def get_sink() -> BaseSink:
    """
    Factory that returns the appropriate sink based on SINK_TYPE environment variable.
    """
    if not SINK_TYPE:
        raise ValueError("SINK_TYPE environment variable is not set.")

    logger.info(f"Creating sink with type: {SINK_TYPE}")

    if SINK_TYPE == "duckdb":
        return DuckDBSink()

    elif SINK_TYPE == "gcs":
        if not GCS_BUCKET:
            raise ValueError("GCS_BUCKET must be configured when using gcs sink.")
        return GCSSink()

    else:
        raise ValueError(f"Unsupported SINK_TYPE: {SINK_TYPE}. Use 'duckdb' or 'gcs'.")