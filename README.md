# High-Throughput SQLite WAL Checkpoint Streamer

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![SQLite WAL](https://img.shields.io/badge/SQLite-WAL%20Mode-orange.svg)](https://www.sqlite.org/wal.html)
[![Tests Passing](https://img.shields.io/badge/Tests-Passing-brightgreen.svg)](test_streamer.py)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)

A high-concurrency batch processing and data streaming worker pool built for crash resilience. Implements SQLite Write-Ahead Logging (WAL) for atomic checkpointing, dynamic `(CPU_COUNT - 1)` worker pool scaling, and bounded 4MB chunk streaming with an $O(1)$ memory ceiling.

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
       │ CPU-1 Multiprocessing │                   │   Bounded 4MB Stream  │
       │ Hardware-Aware Workers│                   │   O(1) Memory Ceiling │
       └───────────┬───────────┘                   └───────────┬───────────┘
                   │                                           │
                   └─────────────────────┬─────────────────────┘
                                         ▼
                      ┌──────────────────────────────────────┐
                      │    Simulated Hardware Crash/Kill     │
                      │ SIGINT Handled · Checkpoint Persisted│
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

- **SQLite WAL Journaling**: Atomic transaction commits preventing state corruption during unexpected host crashes or terminations.
- **Hardware-Aware Worker Scaling**: Automatically tunes worker process pools to `max(1, cpu_count() - 1)` to saturate bandwidth without starving system resources.
- **Bounded Memory Footprint**: Streams data in fixed binary chunks (4MB default) maintaining a constant $O(1)$ memory footprint regardless of dataset scale.
- **Deterministic Crash Resume**: Unprocessed offsets are tracked in real-time, allowing interrupted pipelines to resume instantly with zero duplicated work.

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

5 unit tests validate WAL journal configuration, atomic offset updates, crash recovery simulation, and worker pool scaling.

---

## Author

**Shivansh Dimri**  
Associate AI Engineer — Python Engineering · AI Evaluation · Data Systems · Automation  
Website: [https://shivanshdimri.in](https://shivanshdimri.in)  
GitHub: [@shiv677](https://github.com/shiv677)  
Email: [dimrishiv933@gmail.com](mailto:dimrishiv933@gmail.com)
