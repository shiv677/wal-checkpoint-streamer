# High-Throughput SQLite WAL Checkpoint Streamer

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![SQLite WAL](https://img.shields.io/badge/SQLite-WAL%20Mode-orange.svg)](https://www.sqlite.org/wal.html)
[![Tests Passing](https://img.shields.io/badge/Tests-Passing-brightgreen.svg)](test_streamer.py)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)

A batch processing and data streaming processor built for crash resilience. Implements SQLite Write-Ahead Logging (WAL) for atomic batch checkpointing, transactional rollback on mid-batch failures, and deterministic offset resumption.

---

## Architecture Overview

```
                      ┌──────────────────────────────────────┐
                      │      Incoming Batch Ingestion        │
                      └──────────────────┬───────────────────┘
                                         │
                                         ▼
                      ┌──────────────────────────────────────┐
                      │   Persistent WAL Checkpoint DB       │
                      │  Tracks Processed Offset / Status    │
                      └──────────────────┬───────────────────┘
                                         │
                   ┌─────────────────────┴─────────────────────┐
                   ▼                                           ▼
       ┌───────────────────────┐                   ┌───────────────────────┐
       │   Batch Transactions  │                   │   Idempotent Records  │
       │  Configurable Commit  │                   │   INSERT OR REPLACE   │
       └───────────┬───────────┘                   └───────────┬───────────┘
                   │                                           │
                   └─────────────────────┬─────────────────────┘
                                         ▼
                      ┌──────────────────────────────────────┐
                      │    Simulated Worker Crash/Failure    │
                      │  Atomic Rollback · Offset Persisted  │
                      └──────────────────┬───────────────────┘
                                         │
                                         ▼
                      ┌──────────────────────────────────────┐
                      │      Instantaneous Resume Lock       │
                      │  Zero Data Loss · Zero Re-processing │
                      └──────────────────────────────────────┘
```

---

## Key Capabilities

- **SQLite WAL Journaling**: Atomic transaction commits preventing state corruption during unexpected crashes or exceptions.
- **Configurable Batch Commits**: Commits records and checkpoint offsets together in single atomic SQLite transactions at configurable `commit_interval` boundaries.
- **Idempotent Record Ingestion**: Deduplicates repeated record IDs via `INSERT OR REPLACE` while tracking unique processed record counts.
- **Deterministic Crash Resume**: Unprocessed offsets are tracked in `stream_checkpoints`, allowing interrupted runs to resume from the last committed batch boundary with zero duplicated work.

---

## Quickstart

### 1. Requirements

```bash
pip install pytest
```

### 2. Run Crash-Resilience Simulation

```bash
python main.py --simulate-crash
```

Watch the pipeline process items, simulate a mid-batch kill, and instantaneously resume from the exact last committed offset.

---

## Running Test Suite

```bash
pytest test_streamer.py -v
```

Unit tests validate WAL journal configuration, atomic offset updates, crash recovery simulation, and context-managed resource cleanup.

---

## Author

**Shivansh Dimri**  
Associate AI Engineer — Python Engineering · AI Evaluation · Data Systems · Automation  
Website: [https://shivanshdimri.in](https://shivanshdimri.in)  
GitHub: [@shiv677](https://github.com/shiv677)  
Email: [dimrishiv933@gmail.com](mailto:dimrishiv933@gmail.com)
