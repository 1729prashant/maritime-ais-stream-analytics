"""
storage/utils/sink.py

Sink Abstraction Layer for the AIS Data Pipeline.

Provides a clean, backend-agnostic interface for persisting parsed AIS messages.
Supports:
    - DuckDB (local/dev)
    - GCS Parquet (production/cloud)

Key Design Principles:
    - No parsing logic here — only accepts already extracted rows
    - Efficient batch writes
    - Resilient to schema drift
    - Unique file naming to prevent collisions
    - Explicit column contracts
"""

import io
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any

import pandas as pd
import duckdb
from google.cloud import storage

# Centralized config
from ingestion.config.aisstream_config import (
    SINK_TYPE,
    DUCKDB_PATH,
    GCS_BUCKET,
    GCS_PREFIX,
)

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Column Contracts (Critical for schema stability)
# ----------------------------------------------------------------------
POSITION_COLUMNS = [
    "timestamp", "mmsi", "latitude", "longitude", "sog", "cog",
    "true_heading", "nav_status", "rot", "position_accuracy", "raim"
]

STATIC_COLUMNS = [
    "timestamp", "mmsi", "imo_number", "call_sign", "vessel_name",
    "vessel_type", "length", "width", "draught", "destination",
    "eta_month", "eta_day", "eta_hour", "eta_minute"
]


# ----------------------------------------------------------------------
# DuckDB Helpers
# ----------------------------------------------------------------------
def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection and ensure schema exists."""
    conn = duckdb.connect(DUCKDB_PATH)

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
    """Initialize GCS client using Application Default Credentials."""
    return storage.Client()


# ----------------------------------------------------------------------
# Sink Interface
# ----------------------------------------------------------------------
class BaseSink:
    """Common interface for all sinks."""

    def flush(self, position_rows: List[Dict[str, Any]], static_rows: List[Dict[str, Any]]) -> None:
        raise NotImplementedError


# ----------------------------------------------------------------------
# DuckDB Sink
# ----------------------------------------------------------------------
class DuckDBSink(BaseSink):
    """Local DuckDB sink. Single writer recommended (DuckDB limitation)."""

    def __init__(self):
        self.conn = get_duckdb_conn()
        logger.info(f"DuckDB sink initialized: {DUCKDB_PATH}")

    def flush(self, position_rows: List[Dict[str, Any]], static_rows: List[Dict[str, Any]]) -> None:
        """Flush with explicit column alignment to prevent schema mismatches."""
        try:
            if position_rows:
                df = pd.DataFrame(position_rows)
                df = df.reindex(columns=POSITION_COLUMNS)          # Critical: schema safety
                self.conn.register("tmp_pos", df)
                self.conn.execute("INSERT INTO PositionReport SELECT * FROM tmp_pos")
                self.conn.unregister("tmp_pos")

            if static_rows:
                df = pd.DataFrame(static_rows)
                df = df.reindex(columns=STATIC_COLUMNS)
                self.conn.register("tmp_static", df)
                self.conn.execute("INSERT INTO ShipStaticData SELECT * FROM tmp_static")
                self.conn.unregister("tmp_static")

            logger.info(f"Flushed {len(position_rows)} position + {len(static_rows)} static rows to DuckDB")

        except Exception as e:
            logger.error("DuckDB flush failed", exc_info=True)
            raise


# ----------------------------------------------------------------------
# GCS Parquet Sink
# ----------------------------------------------------------------------
class GCSSink(BaseSink):
    """GCS sink writing partitioned, compressed Parquet files."""

    def __init__(self):
        self.client = get_gcs_client()
        self.bucket = self.client.bucket(GCS_BUCKET)
        logger.info(f"GCS sink initialized for bucket: {GCS_BUCKET}")

    def _upload_parquet(self, df: pd.DataFrame, gcs_path: str) -> None:
        """Safely upload DataFrame as Parquet using in-memory buffer."""
        if df.empty:
            return

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, compression='snappy')
        buffer.seek(0)

        blob = self.bucket.blob(gcs_path)
        blob.upload_from_file(buffer, content_type="application/octet-stream")
        logger.debug(f"Uploaded {len(df)} rows → gs://{GCS_BUCKET}/{gcs_path}")

    def _get_partition_path(self, rows: List[Dict[str, Any]], table_name: str) -> str:
        """
        Partition by event time (AIS timestamp) when possible.
        Falls back to ingestion time.
        """
        if not rows:
            now = datetime.now(timezone.utc)
            partition = now.strftime("%Y/%m/%d/%H")
        else:
            # Prefer event time from first row
            ts = rows[0].get("timestamp")
            if isinstance(ts, datetime):
                partition = ts.strftime("%Y/%m/%d/%H")
            else:
                now = datetime.now(timezone.utc)
                partition = now.strftime("%Y/%m/%d/%H")

        filename = f"{table_name.lower()}_{uuid.uuid4().hex}.parquet"
        return f"{GCS_PREFIX}/{table_name}/{partition}/{filename}"

    def flush(self, position_rows: List[Dict[str, Any]], static_rows: List[Dict[str, Any]]) -> None:
        """Flush with retry logic and safe Parquet handling."""
        try:
            if position_rows:
                df = pd.DataFrame(position_rows)
                df = df.reindex(columns=POSITION_COLUMNS)
                path = self._get_partition_path(position_rows, "PositionReport")
                self._upload_with_retry(df, path)

            if static_rows:
                df = pd.DataFrame(static_rows)
                df = df.reindex(columns=STATIC_COLUMNS)
                path = self._get_partition_path(static_rows, "ShipStaticData")
                self._upload_with_retry(df, path)

        except Exception as e:
            logger.error("GCS flush failed", exc_info=True)
            raise

    def _upload_with_retry(self, df: pd.DataFrame, gcs_path: str, max_retries: int = 3) -> None:
        """Upload with exponential backoff."""
        for attempt in range(max_retries):
            try:
                self._upload_parquet(df, gcs_path)
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"GCS upload failed after {max_retries} attempts", exc_info=True)
                    raise
                wait = (2 ** attempt) + 0.5
                logger.warning(f"GCS upload failed (attempt {attempt+1}), retrying in {wait:.1f}s: {e}")
                time.sleep(wait)


# ----------------------------------------------------------------------
# Factory
# ----------------------------------------------------------------------
def get_sink() -> BaseSink:
    """Return configured sink based on SINK_TYPE."""
    if SINK_TYPE == "duckdb":
        return DuckDBSink()
    elif SINK_TYPE == "gcs":
        if not GCS_BUCKET:
            raise ValueError("GCS_BUCKET must be set when SINK_TYPE=gcs")
        return GCSSink()
    else:
        raise ValueError(f"Unsupported SINK_TYPE: {SINK_TYPE}. Supported: duckdb, gcs")