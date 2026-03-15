# Aequitas-Parallel-Triage

> Production-grade concurrent audit engine capable of executing 10,000,000+ system
> commands safely and reliably.

---

## Architecture Overview

```
 Ingestion (generator)
       │
       ▼
 Dispatcher (token-bucket backpressure, bounded queue)
       │
   ┌───┴────────────────────────────────┐
   │  Executor pool (threaded / process / async-hybrid)
   │        │
   │   Worker N  ──► SubprocessWrapper (Popen, timeouts, cgroups)
   │        │              │
   │        └──► ContainerPool (checkout / overlay-reset / checkin)
   │
   ├──► LogWriter  (QueueHandler → bounded queue → QueueListener → NDJSON RotatingFile)
   │
   ├──► EscalationModule (Redis INCR+TTL  ──fallback──► in-process aggregator)
   │
   └──► Metrics / Telemetry (Prometheus-compatible counters, histograms)
```

### Key design decisions

| Concern | Decision | Why |
|---|---|---|
| Backpressure | Token-bucket dispatcher | Prevents unbounded queue growth at 10M+ scale |
| Logging | Single-writer QueueListener | Eliminates cross-process lock contention on log files |
| Memory | GC only on high-water mark | Per-batch GC destroys throughput; RSS sampling is cheap |
| Escalation | Redis INCR+TTL primary | Atomic, TTL-native, survives worker restarts |
| Containers | Pooled + overlay reset | Warm-start latency < 10 ms; avoids Docker create overhead |
| Futures | `as_completed()` + immediate drop | No reference leaks; GC can collect finished tasks |
| Shell | `shell=False` everywhere | Eliminates injection surface |

---

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run unit tests (no Redis / Docker required)
python -m pytest tests/ -v --ignore=tests/test_container_pool.py -m "not integration"

# Smoke run – 10k synthetic tasks
python -m harness.run --stage=10k --no-redis --no-docker

# Full staged run (requires Redis + Docker)
python -m harness.run --stage=10k
python -m harness.run --stage=100k
python -m harness.run --stage=1m
python -m harness.run --stage=10m
```

---

## Module Index

| Module | Path | Purpose |
|---|---|---|
| Config | `aequitas/config.py` | YAML loader + safe defaults |
| Dispatcher | `aequitas/dispatcher.py` | Token-bucket ingestion |
| Worker | `aequitas/worker.py` | Pluggable executor interface |
| LogWriter | `aequitas/log_writer.py` | QueueListener NDJSON writer |
| Escalation | `aequitas/escalation.py` | Redis + in-process sliding window |
| ContainerPool | `aequitas/container_pool.py` | Async pool with overlay reset |
| SubprocessWrapper | `aequitas/subprocess_wrapper.py` | Secure Popen + timeouts |
| Metrics | `aequitas/metrics.py` | Prometheus-compatible emitter |

---

## Safe Defaults

See `config/defaults.yaml`.

---

## Reviewer Checklist

See `REVIEWER_CHECKLIST.md`.
