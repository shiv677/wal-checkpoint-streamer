import sys
import tempfile
from pathlib import Path
import pytest

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

try:
    from projects.wal_checkpoint_streamer.streamer import WALStreamProcessor, SimulatedCrashException
    from projects.wal_checkpoint_streamer.main import generate_sample_events, run_demo
except ImportError:
    from streamer import WALStreamProcessor, SimulatedCrashException
    from main import generate_sample_events, run_demo


@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    db_file = tmp_path / "test_stream.db"
    return db_file


def test_wal_journal_mode(temp_db: Path):
    processor = WALStreamProcessor(temp_db)
    mode = processor.get_journal_mode()
    assert mode == "wal"
    processor.close()


def test_clean_stream_processing(temp_db: Path):
    processor = WALStreamProcessor(temp_db)
    items = generate_sample_events(120)

    res = processor.process_stream("stream_test", items, commit_interval=50)
    assert res["resumed_from"] == 0
    assert res["processed_in_run"] == 120
    assert res["current_offset"] == 120

    checkpoint = processor.get_checkpoint("stream_test")
    assert checkpoint == 120

    count = processor.get_processed_records_count("stream_test")
    assert count == 120
    processor.close()


def test_crash_and_rollback(temp_db: Path):
    processor = WALStreamProcessor(temp_db)
    items = generate_sample_events(150)

    # Commit interval is 50. Crash at 80 (in middle of batch 2: items 50-99).
    with pytest.raises(SimulatedCrashException):
        processor.process_stream("stream_crash", items, commit_interval=50, crash_at=80)

    # Checkpoint must be exactly 50 (from the first committed batch)
    checkpoint = processor.get_checkpoint("stream_crash")
    assert checkpoint == 50

    # Records in database must be exactly 50, not 80
    records_count = processor.get_processed_records_count("stream_crash")
    assert records_count == 50
    processor.close()


def test_resume_after_crash(temp_db: Path):
    processor = WALStreamProcessor(temp_db)
    items = generate_sample_events(150)

    # Phase 1: Crash at 80
    with pytest.raises(SimulatedCrashException):
        processor.process_stream("stream_resume", items, commit_interval=50, crash_at=80)

    assert processor.get_checkpoint("stream_resume") == 50
    assert processor.get_processed_records_count("stream_resume") == 50

    # Phase 2: Resume
    res = processor.process_stream("stream_resume", items, commit_interval=50, crash_at=None)
    assert res["resumed_from"] == 50
    assert res["processed_in_run"] == 100
    assert res["current_offset"] == 150

    assert processor.get_checkpoint("stream_resume") == 150
    assert processor.get_processed_records_count("stream_resume") == 150

    # Phase 3: Idempotent call on already complete stream
    res_noop = processor.process_stream("stream_resume", items, commit_interval=50)
    assert res_noop["processed_in_run"] == 0
    assert processor.get_processed_records_count("stream_resume") == 150
    processor.close()


def test_demo_runner_integration(temp_db: Path):
    run_demo(temp_db, total_events=150, commit_interval=50, crash_at=85)
