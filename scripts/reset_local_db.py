"""
Utility script to reset local DuckDB tables.

⚠️ NEVER run in production.
"""

import duckdb
from ingestion.config.aisstream_config import DUCKDB_PATH


def reset_db():
    conn = duckdb.connect(DUCKDB_PATH)

    conn.execute("DROP TABLE IF EXISTS PositionReport;")
    conn.execute("DROP TABLE IF EXISTS ShipStaticData;")

    print("Dropped tables: PositionReport, ShipStaticData")


if __name__ == "__main__":
    confirm = input("This will DELETE local tables. Continue? (yes/no): ")
    if confirm.lower() == "yes":
        reset_db()
    else:
        print("Aborted.")