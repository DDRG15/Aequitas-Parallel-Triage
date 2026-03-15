# REVIEWER CHECKLIST — Aequitas-Parallel-Triage

> **Status Gate**: Mark each artifact **Ready-for-Review** only after all
> gating commands in that section return exit code 0 and produce the expected
> sample output.

---

## Gating Rules (must ALL pass before marking Ready-for-Review)

| Gate | Command | Expected |
|---|---|---|
| Unit tests green | `pytest tests/ -m "not redis and not docker" -v` | `N passed` |
| 10k smoke within RSS | `python -m harness.run --stage=10k --no-redis --no-docker` | `[PASS] rss_mb < HWM` |
| 10k smoke log queue | same run | `[PASS] log_queue < 80%` |
| NDJSON validity | `jq . /tmp/aequitas-*/aequitas.ndjson \| head -n 5` | valid JSON objects |
| No shell=True | `grep -r "shell=True" aequitas/` | *no output* (grep returns 1) |

---

## Artifact 1 — `aequitas/config.py`

### Acceptance Criteria
- [ ] All safe defaults match spec values
- [ ] YAML load merges with defaults (partial override doesn't break remaining fields)
- [ ] `sanitized_env_template()` never includes secret variables

### Verification Commands
```bash
# 1. Run config unit tests
pytest tests/test_config.py -v
# Expected: 12 passed

# 2. Assert all safe defaults programmatically
python3 -c "
from aequitas.config import EngineConfig
cfg = EngineConfig()
assert cfg.memory.memory_high_water_mb == 2048, cfg.memory.memory_high_water_mb
assert cfg.logging.log_queue_size == 100_000, cfg.logging.log_queue_size
assert cfg.engine.max_workers == 200, cfg.engine.max_workers
assert cfg.engine.batch_size == 10_000, cfg.engine.batch_size
assert cfg.logging.flush_interval_ms == 500, cfg.logging.flush_interval_ms
assert cfg.logging.max_batch_bytes == 4_194_304, cfg.logging.max_batch_bytes
assert cfg.subprocess.soft_kill_timeout_s == 30, cfg.subprocess.soft_kill_timeout_s
assert cfg.subprocess.hard_kill_timeout_s == 60, cfg.subprocess.hard_kill_timeout_s
print('ALL DEFAULTS OK')
"
# Expected output: ALL DEFAULTS OK

# 3. Verify env template excludes secrets
python3 -c "
import os
os.environ['AWS_SECRET_ACCESS_KEY'] = 'secret123'
from aequitas.config import EngineConfig
cfg = EngineConfig()
env = cfg.sanitized_env_template()
assert 'AWS_SECRET_ACCESS_KEY' not in env, 'SECRET LEAKED'
print('ENV TEMPLATE SAFE:', list(env.keys()))
"
```

---

## Artifact 2 — `aequitas/dispatcher.py`

### Acceptance Criteria
- [ ] Token bucket rate-limits ingestion correctly
- [ ] Future references dropped immediately after `as_completed()`
- [ ] No `shell=True` anywhere in dispatcher code
- [ ] Bounded semaphore prevents unbounded in-flight tasks

### Verification Commands
```bash
# 1. Run dispatcher tests
pytest tests/test_dispatcher.py -v
# Expected: all passed

# 2. Verify no shell=True
grep -n "shell=True" aequitas/dispatcher.py
# Expected: no output (exit code 1)

# 3. Verify future-drop test explicitly
pytest tests/test_dispatcher.py::TestDispatcher::test_future_refs_dropped -v
# Expected: PASSED

# 4. Token bucket thread-safety
pytest tests/test_dispatcher.py::TestTokenBucket::test_thread_safety -v
# Expected: PASSED — exactly 1000 tokens granted to 1000 threads
```

---

## Artifact 3 — `aequitas/log_writer.py`

### Acceptance Criteria
- [ ] Workers use `QueueHandler`; single `LogWriterThread` writes to disk
- [ ] NDJSON output contains `ts_ms`, `level`, `pid`, `thread`, `logger`, `msg`, `stream`
- [ ] Backpressure: DEBUG/INFO sampled/dropped when queue > 80% full
- [ ] WARNING+ records never dropped
- [ ] Writer batches and fsyncs based on `max_batch_bytes` OR `flush_interval_ms`

### Verification Commands
```bash
# 1. Run log writer tests
pytest tests/test_logging_backpressure.py -v
# Expected: all passed

# 2. Produce sample NDJSON and validate fields
python3 -c "
import tempfile, logging, time, queue
from aequitas.log_writer import build_log_writer
from aequitas.config import EngineConfig

cfg = EngineConfig()
cfg.logging.log_dir = tempfile.mkdtemp()
cfg.logging.flush_interval_ms = 50

log_queue, writer = build_log_writer(cfg.logging)
writer.start()

logger = logging.getLogger('test')
logger.info('hello from test')
logger.warning('warning from test')

time.sleep(0.2)
writer.stop()
writer.join(timeout=2)

import pathlib, orjson
log_file = pathlib.Path(cfg.logging.log_dir) / 'aequitas.ndjson'
if log_file.exists():
    for line in log_file.read_text().strip().splitlines():
        obj = orjson.loads(line)
        print(obj)
"
# Expected output: NDJSON objects with ts_ms, level, pid, thread, logger, msg, stream

# 3. Verify single-writer (no interleaving)
pytest tests/test_logging_backpressure.py::TestSingleWriter::test_single_writer_no_interleaving -v
```

### Sample NDJSON Lines
```json
{"ts_ms":1718000000123,"level":"INFO","pid":12345,"thread":140234,"logger":"aequitas.dispatcher","msg":"Dispatcher started max_workers=200","stream":"log"}
{"ts_ms":1718000000456,"level":"WARNING","pid":12345,"thread":140234,"logger":"aequitas.escalation","msg":"ESCALATION THRESHOLD BREACHED: window=512","stream":"log"}
```

---

## Artifact 4 — `aequitas/escalation.py`

### Acceptance Criteria
- [ ] In-process aggregator: sliding window sums correct across bucket boundaries
- [ ] Redis backend: uses `INCR` + `EXPIRE`, key format `aequitas:escalation:{metric}:{bucket_id}`
- [ ] Lock-sharded backend: no data races under 200 concurrent threads
- [ ] Circuit breaker: opens after 3 failures, transitions HALF_OPEN after backoff, closes on success
- [ ] `query_window()` returns -1 (not exception) when Redis circuit is open

### Verification Commands
```bash
# 1. Run all escalation tests (no Redis)
pytest tests/test_escalation.py -m "not redis" -v
# Expected: all passed

# 2. Run with Redis (requires: docker run -d -p 6379:6379 redis:7)
pytest tests/test_escalation.py -m redis -v
# Expected: all passed, including sliding-window verification printout

# 3. Verify Redis key format manually
python3 -c "
import time
bucket_width = 5  # 10s / 2 buckets
bucket_id = int(time.time()) // bucket_width
print(f'Current Redis key: aequitas:escalation:task_errors:{bucket_id}')
print(f'Window keys: aequitas:escalation:task_errors:{bucket_id-1} through :{bucket_id}')
"

# 4. Assert sliding window sums (simulate)
python3 -c "
import time
from aequitas.escalation import InProcessEscalationBackend

b = InProcessEscalationBackend(window_seconds=10, bucket_count=2)
time.sleep(0.05)
b.increment('x', 5)
b.increment('x', 3)
time.sleep(0.2)
total = b.query_window('x')
print(f'Window sum: {total}')   # Expected: 8
assert total == 8, f'Got {total}'
b.stop()
print('SLIDING WINDOW OK')
"
# Expected: Window sum: 8 / SLIDING WINDOW OK

# 5. Circuit breaker state machine
pytest tests/test_escalation.py::TestCircuitBreaker -v
# Expected: 5 passed
```

### Sample Redis Keys (TTL demonstration)
```
127.0.0.1:6379> KEYS aequitas:escalation:*
1) "aequitas:escalation:task_errors:344556"
2) "aequitas:escalation:task_errors:344555"

127.0.0.1:6379> GET aequitas:escalation:task_errors:344556
"42"

127.0.0.1:6379> TTL aequitas:escalation:task_errors:344556
(integer) 97   # 97 seconds remaining of 120s TTL
```

---

## Artifact 5 — `aequitas/container_pool.py`

### Acceptance Criteria
- [ ] `checkout()` / `checkin()` semantics: pool returns to full size after checkin
- [ ] Overlay reset increments `stats.overlay_resets`
- [ ] Pool exhaustion blocks until checkin; `checkout_timeout_s` raises `asyncio.TimeoutError`
- [ ] Worker threads can call `checkout_sync()` / `checkin_sync()` from non-async code
- [ ] Container lifecycle (create/destroy) is NEVER called synchronously in worker threads

### Verification Commands
```bash
# 1. Run container pool tests (mocked Docker)
pytest tests/test_container_pool.py -m "not docker" -v
# Expected: all passed

# 2. Verify async controller assertion
pytest tests/test_container_pool.py::TestPoolSemantics -v
# Expected: all passed

# 3. Thread-safe sync wrapper
pytest tests/test_container_pool.py::TestCheckoutSync -v
# Expected: PASSED

# 4. Checkout timeout test
pytest tests/test_container_pool.py::TestPoolSemantics::test_checkout_timeout_raises -v
# Expected: PASSED — asyncio.TimeoutError raised after 50ms
```

---

## Artifact 6 — `aequitas/subprocess_wrapper.py`

### Acceptance Criteria
- [ ] `shell=False` everywhere; `shell=True` causes `AssertionError` if detected
- [ ] `cmd` must be `list[str]`; string raises `AssertionError`
- [ ] Soft kill (SIGTERM) at `soft_kill_timeout_s`; SIGKILL at `hard_kill_timeout_s`
- [ ] Output > `stream_threshold_bytes` written to tempfile, not held in memory
- [ ] `resource.setrlimit` applied via `preexec_fn` (CPU, AS limits)
- [ ] Environment is template-copy + patch; NOT `os.environ.copy()` per task

### Verification Commands
```bash
# 1. Run subprocess tests
pytest tests/test_subprocess_wrapper.py -m "not slow" -v
# Expected: all passed

# 2. Run timeout tests (requires ~3s each)
pytest tests/test_subprocess_wrapper.py -m slow -v
# Expected: PASSED — process killed within hard_kill_timeout + 2s margin

# 3. Verify no shell=True in codebase
grep -rn "shell=True" aequitas/
# Expected: no output

# 4. Verify env template reuse (not os.environ.copy per task)
grep -n "os.environ.copy" aequitas/subprocess_wrapper.py
# Expected: no output (copy is in config.py sanitized_env_template, called ONCE)
```

---

## Artifact 7 — `aequitas/metrics.py`

### Acceptance Criteria
- [ ] `maybe_collect_gc()` calls `gc.collect()` ONLY when RSS > `memory_high_water_mb`
- [ ] GC NOT called when RSS < HWM
- [ ] All metric names match the spec (check `_init_metrics`)
- [ ] Push loop runs in background thread and does not block workers

### Verification Commands
```bash
# 1. Run memory / GC tests
pytest tests/test_memory_gc.py -v
# Expected: all passed

# 2. GC policy unit test
pytest tests/test_memory_gc.py::TestGCPolicy::test_gc_not_triggered_below_hwm -v
pytest tests/test_memory_gc.py::TestGCPolicy::test_gc_triggered_at_hwm -v
# Both: PASSED

# 3. Verify metric names
python3 -c "
import inspect
from aequitas import metrics
src = inspect.getsource(metrics)
required = [
    'aequitas_tasks_total',
    'aequitas_task_duration_seconds',
    'aequitas_queue_depth',
    'aequitas_log_queue_depth',
    'aequitas_rss_mb',
    'aequitas_gc_pause_seconds',
    'aequitas_docker_create_seconds',
    'aequitas_error_rate_window',
    'aequitas_escalation_threshold_breaches',
]
for m in required:
    assert m in src, f'Missing metric: {m}'
    print(f'  OK: {m}')
print('ALL METRIC NAMES PRESENT')
"
```

---

## Artifact 8 — Load Harness (`harness/run.py`)

### Acceptance Criteria
- [ ] 10k smoke run completes with `rss_mb < 2048`, `log_queue < 80_000`, `throughput > 1000 tps`
- [ ] Escalation counter increments tracked correctly
- [ ] GC triggered only when RSS > HWM (not every batch)
- [ ] All four stages (10k, 100k, 1M, 10M) have the same entrypoint

### Verification Commands
```bash
# 1. Smoke run (CI-safe, no Redis, no Docker)
python -m harness.run --stage=10k --no-redis --no-docker --workers=8
# Expected: ✅ READY-FOR-REVIEW: All thresholds passed.

# 2. Run CI runner end-to-end
python ci_runner.py --no-slow
# Expected: ✅ CI PASSED — all checks green

# 3. Verify staged thresholds
python3 -c "
from harness.run import STAGES
for stage, n in STAGES.items():
    print(f'{stage}: {n:,} tasks')
"
# Expected:
# 10k: 10,000 tasks
# 100k: 100,000 tasks
# 1m: 1,000,000 tasks
# 10m: 10,000,000 tasks
```

---

## Reviewer Metric Thresholds Reference

| Metric | Threshold | Source |
|---|---|---|
| `rss_mb` | < 2048 MB | `memory_high_water_mb` |
| `log_queue_len` | < 80,000 | 80% × `log_queue_size=100_000` |
| `docker_create_ms` | < 200 ms | `docker_create_timeout_ms` |
| `throughput_tps` | > 1,000 | minimum viable for 10M workload |
| `escalation_window` | < 500 | `error_threshold` |
| `gc_triggered_per_1M` | < 10 | GC should not fire continuously |

---

## CI Integration Notes

```bash
# Minimal CI (GitHub Actions free tier — 2 cores, 7 GB RAM)
python ci_runner.py --no-slow --smoke-stage=10k --workers=4

# Standard CI (4 cores, 16 GB RAM)
python ci_runner.py --smoke-stage=10k --workers=16

# Full integration CI (Redis + Docker available)
docker run -d -p 6379:6379 redis:7
python ci_runner.py --redis --docker --smoke-stage=100k --workers=32

# Performance environment only (dedicated server, 32+ cores, 64+ GB RAM)
python -m harness.run --stage=10m --workers=200
```

---

## Ready-for-Review Sign-off

Reviewer must confirm:

```
[ ] pytest tests/ -m "not redis and not docker" — ALL PASSED
[ ] python -m harness.run --stage=10k --no-redis --no-docker — READY-FOR-REVIEW
[ ] grep -r "shell=True" aequitas/ — NO OUTPUT
[ ] NDJSON sample validated with jq
[ ] Sliding window assertion script passed
[ ] All TODO markers reviewed and production gaps documented
[ ] Safe defaults unchanged from spec
```

Signed: _____________ Date: _____________ Artifact Version: 0.1.0
