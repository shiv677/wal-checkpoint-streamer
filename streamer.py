import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Sequence


class SimulatedCrashException(Exception):
    """Raised to simulate an unexpected worker crash mid-stream."""
    pass


class WALStreamProcessor:
    """High-throughput stream processing worker with SQLite WAL journal persistence."""

    def __init__(self, db_path: str | Path = ":memory:"):
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path, timeout=10.0)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        # Enable WAL mode for high-throughput concurrent reads and atomic writes
        cursor = self.conn.cursor()
        if self.db_path != ":memory:":
            cursor.execute("PRAGMA journal_mode = WAL;")
        cursor.execute("PRAGMA synchronous = NORMAL;")

        # Create schema for checkpoints and processed records
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stream_checkpoints (
                stream_id TEXT PRIMARY KEY,
                last_offset INTEGER NOT NULL,
                total_items_processed INTEGER NOT NULL DEFAULT 0,
                last_checkpoint_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_records (
                record_id TEXT PRIMARY KEY,
                stream_id TEXT NOT NULL,
                offset_index INTEGER NOT NULL,
                payload TEXT NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.conn.commit()

    def get_journal_mode(self) -> str:
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA journal_mode;")
        return cursor.fetchone()[0].lower()

    def get_checkpoint(self, stream_id: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute("SELECT last_offset FROM stream_checkpoints WHERE stream_id = ?;", (stream_id,))
        row = cursor.fetchone()
        return row["last_offset"] if row else 0

    def process_stream(
        self,
        stream_id: str,
        items: Sequence[dict[str, Any]],
        commit_interval: int = 50,
        crash_at: int | None = None,
    ) -> dict[str, Any]:
        """Processes items from the stream starting from the last saved checkpoint."""
        current_offset = self.get_checkpoint(stream_id)
        if current_offset >= len(items):
            return {
                "stream_id": stream_id,
                "resumed_from": current_offset,
                "processed_in_run": 0,
                "current_offset": current_offset,
                "total_stream_items": len(items),
            }

        processed_in_run = 0
        total_items = len(items)

        for batch_start in range(current_offset, total_items, commit_interval):
            batch_end = min(batch_start + commit_interval, total_items)
            batch = items[batch_start:batch_end]

            # Atomic transaction per checkpoint interval
            with self.conn:
                cursor = self.conn.cursor()
                for idx, item in enumerate(batch):
                    global_offset = batch_start + idx

                    if crash_at is not None and global_offset >= crash_at:
                        raise SimulatedCrashException(
                            f"Worker crashed at offset {global_offset} prior to checkpoint commit!"
                        )

                    record_id = f"{stream_id}_{item.get('id', global_offset)}"
                    payload_json = json.dumps(item)

                    cursor.execute("""
                        INSERT OR REPLACE INTO processed_records (record_id, stream_id, offset_index, payload)
                        VALUES (?, ?, ?, ?);
                    """, (record_id, stream_id, global_offset, payload_json))

                new_offset = batch_end
                cursor.execute("""
                    INSERT INTO stream_checkpoints (stream_id, last_offset, total_items_processed)
                    VALUES (?, ?, ?)
                    ON CONFLICT(stream_id) DO UPDATE SET
                        last_offset = excluded.last_offset,
                        total_items_processed = total_items_processed + ?,
                        last_checkpoint_at = CURRENT_TIMESTAMP;
                """, (stream_id, new_offset, len(batch), len(batch)))

                processed_in_run += len(batch)

        final_offset = self.get_checkpoint(stream_id)
        return {
            "stream_id": stream_id,
            "resumed_from": current_offset,
            "processed_in_run": processed_in_run,
            "current_offset": final_offset,
            "total_stream_items": total_items,
        }

    def get_processed_records_count(self, stream_id: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed_records WHERE stream_id = ?;", (stream_id,))
        return cursor.fetchone()[0]

    def close(self):
        self.conn.close()
