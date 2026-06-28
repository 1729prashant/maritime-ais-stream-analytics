"""
dashboard/app.py

AIS ship position viewer — two modes:
  1. Live   — auto-refreshing snapshot of the live DuckDB file, shows current positions
  2. Replay — a single fixed snapshot taken once per session, scrub through its
              full time range without the underlying data shifting mid-session

Why snapshots at all: DuckDB holds an exclusive lock on the .duckdb file while
the consumer is writing to it (https://duckdb.org/docs/stable/connect/concurrency).
This script never opens the live file directly — it always reads from a copy.

Resource hygiene notes:
  - _cleanup_orphaned_snapshots() sweeps /tmp once per SERVER PROCESS (not per
    session, not per rerun) via st.cache_resource. This catches snapshots left
    behind by crashed processes or abandoned tabs from a PREVIOUS run. It does
    NOT catch a tab abandoned during the CURRENT run — Streamlit has no
    reliable session-end hook, so that leak is only cleaned on next restart.
  - Replay queries are cached for 30s via st.cache_data so repeated/nearby
    slider positions reuse results instead of re-reading disk every rerun.
    Live mode is deliberately NOT cached this way — its snapshot path changes
    every refresh, so caching there would only accumulate unused cache entries.
"""

import glob
import os
import shutil
import tempfile
import time
from datetime import datetime

import duckdb
import pandas as pd
import streamlit as st

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/ais.duckdb")
LIVE_REFRESH_SECONDS = int(os.getenv("MAP_REFRESH_SECONDS", "5"))
LIVE_STALE_MINUTES = int(os.getenv("MAP_STALE_MINUTES", "30"))
REPLAY_CACHE_TTL_SECONDS = int(os.getenv("REPLAY_CACHE_TTL_SECONDS", "30"))

st.set_page_config(page_title="AIS Ship Tracker", layout="wide")


# ----------------------------------------------------------------------
# Startup cleanup — runs once per server process, not per session
# ----------------------------------------------------------------------
@st.cache_resource
def _cleanup_orphaned_snapshots() -> int:
    """
    Sweep /tmp for AIS snapshot directories left behind by previous
    Streamlit server runs (crashed sessions, closed tabs, etc).

    st.cache_resource guarantees this function body runs exactly once
    per server process, regardless of how many browser sessions connect
    or how many times the script reruns. Placed before any snapshot is
    taken in this run, so it never touches a snapshot created during
    the current process's lifetime.
    """
    pattern = os.path.join(tempfile.gettempdir(), "ais_snapshot_*")
    removed = 0
    for path in glob.glob(pattern):
        try:
            shutil.rmtree(path, ignore_errors=True)
            removed += 1
        except Exception:
            pass
    return removed


# ----------------------------------------------------------------------
# Snapshot handling
# ----------------------------------------------------------------------
def take_snapshot(source_path: str) -> str:
    """
    Copy the live DuckDB file to a fresh temp path.

    Best-effort, not transactionally safe — copying a file mid-write can
    occasionally catch an inconsistent state. Caller must handle failure.
    """
    if not os.path.exists(source_path):
        raise FileNotFoundError(source_path)

    tmp_dir = tempfile.mkdtemp(prefix="ais_snapshot_")
    tmp_path = os.path.join(tmp_dir, "ais_snapshot.duckdb")
    shutil.copy2(source_path, tmp_path)

    wal_path = source_path + ".wal"
    if os.path.exists(wal_path):
        shutil.copy2(wal_path, tmp_path + ".wal")

    return tmp_path


def cleanup_snapshot(path: str) -> None:
    if path and os.path.exists(path):
        shutil.rmtree(os.path.dirname(path), ignore_errors=True)


# ----------------------------------------------------------------------
# Query layer — shared by both modes
# ----------------------------------------------------------------------
def query_latest_positions(snapshot_path: str, as_of: datetime, stale_minutes: int = None) -> pd.DataFrame:
    """
    Latest known position per vessel, as of `as_of`.

    If stale_minutes is given, also drops vessels whose latest position
    is older than that window (used by Live mode to hide stale ships;
    Replay mode passes stale_minutes=None to show full history state).
    """
    conn = duckdb.connect(snapshot_path, read_only=True)
    try:
        staleness_clause = ""
        params = [as_of]
        if stale_minutes is not None:
            staleness_clause = f"AND timestamp >= ? - INTERVAL '{stale_minutes} minutes'"
            params.append(as_of)

        df = conn.execute(f"""
            SELECT mmsi, latitude, longitude, timestamp, sog, cog
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp DESC) AS rn
                FROM PositionReport
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND timestamp <= ?
                  {staleness_clause}
            )
            WHERE rn = 1
        """, params).fetchdf()
    finally:
        conn.close()
    return df


@st.cache_data(ttl=REPLAY_CACHE_TTL_SECONDS, show_spinner=False)
def _cached_replay_query(snapshot_path: str, as_of: datetime) -> pd.DataFrame:
    """
    Cached wrapper around query_latest_positions, for Replay mode only.

    Cache key is (snapshot_path, as_of). snapshot_path only changes when
    the user explicitly takes a new snapshot, so repeated or nearby slider
    positions within the TTL window reuse the cached result instead of
    re-querying disk on every Streamlit rerun.
    """
    return query_latest_positions(snapshot_path, as_of=as_of, stale_minutes=None)


def query_time_bounds(snapshot_path: str):
    conn = duckdb.connect(snapshot_path, read_only=True)
    try:
        min_ts, max_ts = conn.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM PositionReport"
        ).fetchone()
    finally:
        conn.close()
    return min_ts, max_ts


# ----------------------------------------------------------------------
# Rendering — shared by both modes
# ----------------------------------------------------------------------
def render_map(df: pd.DataFrame, caption: str) -> None:
    col1, col2 = st.columns([1, 4])
    with col1:
        st.metric("Vessels shown", len(df))
        st.caption(caption)
    with col2:
        if df.empty:
            st.info("No vessel positions to display.")
        else:
            map_df = df.rename(columns={"latitude": "lat", "longitude": "lon"})
            st.map(map_df, latitude="lat", longitude="lon", size=20)

    with st.expander("Raw data"):
        st.dataframe(df.sort_values("timestamp", ascending=False))


# ----------------------------------------------------------------------
# Live mode
# ----------------------------------------------------------------------
def run_live_mode():
    try:
        snapshot_path = take_snapshot(DUCKDB_PATH)
    except FileNotFoundError:
        st.error(f"Database not found at {DUCKDB_PATH}. Is the consumer running?")
        return

    try:
        df = query_latest_positions(
            snapshot_path,
            as_of=datetime.now(),
            stale_minutes=LIVE_STALE_MINUTES,
        )
    except Exception as e:
        st.warning(f"Snapshot read failed (likely caught mid-write): {e}")
        df = pd.DataFrame()
    finally:
        cleanup_snapshot(snapshot_path)

    render_map(df, f"Positions within last {LIVE_STALE_MINUTES} min · refreshing every {LIVE_REFRESH_SECONDS}s")

    time.sleep(LIVE_REFRESH_SECONDS)
    st.rerun()


# ----------------------------------------------------------------------
# Replay mode
# ----------------------------------------------------------------------
def run_replay_mode():
    if "replay_snapshot_path" not in st.session_state:
        st.session_state.replay_snapshot_path = None
        st.session_state.replay_snapshot_taken_at = None

    take_new = st.sidebar.button("📸 Take new snapshot")
    if take_new or st.session_state.replay_snapshot_path is None:
        cleanup_snapshot(st.session_state.replay_snapshot_path)
        try:
            st.session_state.replay_snapshot_path = take_snapshot(DUCKDB_PATH)
            st.session_state.replay_snapshot_taken_at = datetime.now()
        except FileNotFoundError:
            st.error(f"Database not found at {DUCKDB_PATH}.")
            return

    snapshot_path = st.session_state.replay_snapshot_path
    st.sidebar.caption(f"Snapshot taken: {st.session_state.replay_snapshot_taken_at:%H:%M:%S}")
    st.sidebar.caption("This snapshot will NOT change while you scrub — take a new one to refresh it.")

    try:
        min_ts, max_ts = query_time_bounds(snapshot_path)
    except Exception as e:
        st.error(f"Snapshot appears invalid: {e}. Try taking a new snapshot.")
        return

    if min_ts is None:
        st.info("No data in snapshot yet.")
        return

    as_of = st.sidebar.slider(
        "Replay time",
        min_value=min_ts,
        max_value=max_ts,
        value=max_ts,
        format="YYYY-MM-DD HH:mm:ss",
    )

    df = _cached_replay_query(snapshot_path, as_of)
    render_map(df, f"Positions as of {as_of:%Y-%m-%d %H:%M:%S}")


# ----------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------
def main():
    removed = _cleanup_orphaned_snapshots()
    if removed:
        st.sidebar.caption(f"🧹 Cleaned up {removed} orphaned snapshot(s) from a previous run.")

    st.title("🚢 AIS Ship Positions")
    mode = st.sidebar.radio("Mode", ["Live", "Replay"])

    if mode == "Live":
        run_live_mode()
    else:
        run_replay_mode()


if __name__ == "__main__":
    main()