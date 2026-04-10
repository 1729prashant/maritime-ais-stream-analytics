# monitor_tables.py
import duckdb
import time
import shutil
import tempfile
import os
from datetime import datetime
from rich.console import Console
from rich.live import Live
from rich.text import Text
from rich.table import Table
from rich.console import Group
from rich.padding import Padding
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH")
if not DUCKDB_PATH:
    raise ValueError("DUCKDB_PATH not found in .env file or environment variables.")

DB_PATH = DUCKDB_PATH   
TABLE_1  = "PositionReport"
TABLE_2  = "ShipStaticData"
REFRESH_INTERVAL = 1 # seconds

console = Console()

console = Console()

def force_copy(src, dst):
    """Copy a file even if it's open/locked by another process on Windows."""
    import ctypes
    if os.name == 'nt':
        GENERIC_READ        = 0x80000000
        FILE_SHARE_READ     = 0x00000001
        FILE_SHARE_WRITE    = 0x00000002
        FILE_SHARE_DELETE   = 0x00000004
        OPEN_EXISTING       = 3
        FILE_ATTRIBUTE_NORMAL = 0x80

        CreateFile = ctypes.windll.kernel32.CreateFileW
        ReadFile   = ctypes.windll.kernel32.ReadFile
        CloseHandle= ctypes.windll.kernel32.CloseHandle

        handle = CreateFile(
            src,
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None
        )
        if handle == ctypes.c_void_p(-1).value:
            raise OSError(f"Cannot open {src} with shared read")

        chunks = []
        buf    = ctypes.create_string_buffer(1024 * 1024)  # 1MB chunks
        bytes_read = ctypes.c_ulong(0)
        while True:
            ok = ReadFile(handle, buf, len(buf), ctypes.byref(bytes_read), None)
            if not ok or bytes_read.value == 0:
                break
            chunks.append(buf.raw[:bytes_read.value])
        CloseHandle(handle)

        with open(dst, 'wb') as f:
            for chunk in chunks:
                f.write(chunk)
    else:
        import shutil
        shutil.copy2(src, dst)

def get_counts():
    tmp_db  = tempfile.mktemp(suffix=".db")
    tmp_wal = tmp_db + ".wal"
    try:
        force_copy(DB_PATH, tmp_db)
        wal = DB_PATH + ".wal"
        if os.path.exists(wal):
            try:
                force_copy(wal, tmp_wal)
            except:
                pass

        con = duckdb.connect(tmp_db, read_only=True)
        counts = {}
        for table in (TABLE_1, TABLE_2):
            try:
                result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = result[0] if result else 0
            except Exception as e:
                counts[table] = f"ERR: {e}"
        con.close()
        return counts
    except Exception as e:
        return {TABLE_1: f"ERR: {e}", TABLE_2: f"ERR: {e}"}
    finally:
        for f in (tmp_db, tmp_wal):
            try:
                if os.path.exists(f):
                    os.remove(f)
            except:
                pass

def main():
    prev_counts = {TABLE_1: 0, TABLE_2: 0}
    start_time  = time.time()

    with Live(console=console, refresh_per_second=1, screen=False) as live:
        try:
            while True:
                curr_counts = get_counts()
                elapsed = int(time.time() - start_time)
                now = datetime.now().strftime("%H:%M:%S")

                table = Table(
                    title=f"[bold cyan]DuckDB Table Monitor[/]  |  [dim]{DB_PATH}[/]  |  [dim]{now}[/]",
                    border_style="cyan",
                    header_style="bold cyan"
                )
                table.add_column("Table", style="white", min_width=25)
                table.add_column("Row Count", justify="right", min_width=12)
                table.add_column("Delta / sec", justify="right", min_width=12)

                for tbl in (TABLE_1, TABLE_2):
                    curr_val = curr_counts.get(tbl, 0)
                    prev_val = prev_counts.get(tbl, 0)
                    if isinstance(curr_val, int) and isinstance(prev_val, int):
                        delta     = curr_val - prev_val
                        delta_str = f"+{delta}" if delta >= 0 else str(delta)
                        delta_text = Text(delta_str, style="green bold" if delta > 0 else "white")
                        table.add_row(tbl, f"{curr_val:,}", delta_text)
                    else:
                        table.add_row(tbl, Text(str(curr_val), style="yellow"), "-")

                footer = Text(
                    f"Elapsed: {elapsed}s   Refresh: {REFRESH_INTERVAL}s   Press Ctrl+C to quit",
                    style="dim"
                )
                live.update(Group(table, footer))
                prev_counts = dict(curr_counts)
                time.sleep(REFRESH_INTERVAL)

        except KeyboardInterrupt:
            pass

    console.print("[green]Monitor stopped.[/]")

if __name__ == "__main__":
    main()
