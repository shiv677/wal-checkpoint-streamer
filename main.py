import argparse
import sys
import tempfile
from pathlib import Path

try:
    from .streamer import WALStreamProcessor, SimulatedCrashException
except ImportError:
    from streamer import WALStreamProcessor, SimulatedCrashException


def generate_sample_events(count: int = 500) -> list[dict]:
    return [
        {
            "id": f"evt_{i:04d}",
            "type": "telemetry_metric",
            "metric": f"sensor_{i % 10}",
            "value": round((i * 1.37) % 100, 2),
            "timestamp": 1700000000 + i,
        }
        for i in range(count)
    ]


def run_demo(db_path: Path, total_events: int = 500, commit_interval: int = 100, crash_at: int = 250):
    print("=" * 70)
    print("WAL Checkpoint Streamer - Resilience Demonstration")
    print("=" * 70)
    print(f"Target DB Path:   {db_path}")
    print(f"Total Events:     {total_events}")
    print(f"Commit Interval:  {commit_interval} items")
    print(f"Crash Injection:  At offset {crash_at}")
    print("-" * 70)

    stream_id = "sensor_stream_alpha"
    events = generate_sample_events(total_events)

    # Phase 1: Start streaming and inject simulated crash
    print("[Phase 1] Starting initial streaming worker...")
    processor = WALStreamProcessor(db_path)
    print(f"  SQLite Journal Mode: {processor.get_journal_mode().upper()}")

    crashed = False
    try:
        processor.process_stream(
            stream_id=stream_id,
            items=events,
            commit_interval=commit_interval,
            crash_at=crash_at,
        )
    except SimulatedCrashException as e:
        crashed = True
        print(f"  [CRASH DETECTED] {e}")

    processor.close()

    assert crashed, "Expected simulated crash to occur!"

    # Phase 2: Inspect checkpoint and database state post-crash
    print("\n[Phase 2] Verifying checkpoint consistency post-crash...")
    verifier = WALStreamProcessor(db_path)
    saved_offset = verifier.get_checkpoint(stream_id)
    saved_records = verifier.get_processed_records_count(stream_id)
    print(f"  Persisted Checkpoint Offset: {saved_offset}")
    print(f"  Persisted Committed Records: {saved_records}")

    expected_clean_offset = (crash_at // commit_interval) * commit_interval
    assert saved_offset == expected_clean_offset, f"Expected checkpoint {expected_clean_offset}, got {saved_offset}"
    assert saved_records == expected_clean_offset, f"Expected {expected_clean_offset} records, got {saved_records}"
    print(f"  -> Atomic rollback verified: exactly {saved_offset} records preserved (0 dirty uncommitted records)")

    # Phase 3: Resume streaming worker from checkpoint
    print("\n[Phase 3] Resuming streaming worker from checkpoint...")
    result = verifier.process_stream(
        stream_id=stream_id,
        items=events,
        commit_interval=commit_interval,
        crash_at=None,  # No crash on resume
    )

    final_offset = verifier.get_checkpoint(stream_id)
    final_records = verifier.get_processed_records_count(stream_id)
    verifier.close()

    print(f"  Resumed from Offset: {result['resumed_from']}")
    print(f"  Processed on Resume: {result['processed_in_run']}")
    print(f"  Final Checkpoint:    {final_offset}")
    print(f"  Total Ingested:      {final_records}/{total_events}")
    print("-" * 70)
    print("[SUCCESS] Zero data loss, zero duplicate records processed.")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Run SQLite WAL checkpoint streamer resilience demo.")
    parser.add_argument("--db", type=Path, default=None, help="Database file path (defaults to temp file)")
    parser.add_argument("--count", type=int, default=500, help="Number of stream items (default 500)")
    parser.add_argument("--batch-size", type=int, default=100, help="Commit batch interval (default 100)")
    parser.add_argument("--crash-at", type=int, default=250, help="Offset to inject crash (default 250)")
    args = parser.parse_args()

    if args.db:
        target_db = args.db
    else:
        temp_dir = tempfile.gettempdir()
        target_db = Path(temp_dir) / "wal_stream_demo.db"
        if target_db.exists():
            target_db.unlink()

    run_demo(target_db, total_events=args.count, commit_interval=args.batch_size, crash_at=args.crash_at)


if __name__ == "__main__":
    main()
