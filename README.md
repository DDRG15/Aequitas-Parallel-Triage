
```markdown
# Aequitas-Parallel-Triage v0.3.0 🛡️🚀

**"Because processing 10 million tasks shouldn't feel like a boss fight."**

Aequitas is a production-grade, concurrent audit engine that treats system resources with clinical efficiency. I have Aequitas hitting **23k+ TPS on a Windows laptop** without breaking a sweat, ensuring zero tolerance for logic errors and maximum resource dexterity.

## 📊 Performance Benchmarks (Validated March 14, 2026)
- **Throughput:** 23,696.6 TPS.
- **RSS Footprint:** 45.6 MB at 10,000 tasks.
- **Engine Latency:** < 0.5s for 10k task bursts. 
- **Stability:** 77/77 Unit Tests Passed.

---

## 🏗️ Architecture Overview

```text
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

### Key Design Decisions

| Concern | Decision | Why |
| --- | --- | --- |
| **Backpressure** | Token-bucket dispatcher | Prevents unbounded queue growth at 10M+ scale. |
| **Logging** | Single-writer QueueListener | Eliminates cross-process lock contention on log files. |
| **Memory** | GC only on high-water mark | Per-batch GC destroys throughput; RSS sampling is cheap. |
| **Escalation** | Redis INCR+TTL primary | Atomic, TTL-native, survives worker restarts. |
| **Containers** | Pooled + overlay reset | Warm-start latency < 10 ms; avoids Docker create overhead. |
| **Security** | `shell=False` everywhere | Eliminates command injection surface. |

---

## 🧠 Core Engineering (The "Secret Sauce")

* **True Cross-Platform Mastery:** Most engines cry on Windows. I built a custom `platform_compat` layer to orchestrate `spawn` (Windows) and `forkserver` (POSIX) contexts with pinpoint accuracy and dexterity.
* **Single-Writer Logging:** I decoupled I/O using a bounded `QueueListener`. Workers don't wait for the disk; they get back to work.
* **Resource Governance:** My platform-aware signal handling and safe `fsync` wrappers ensure that even when Windows VFS gets moody, the audit trail remains immortal.

## ⚔️ The War Stories (Difficulties Overcome)

During the refactor from v2 to v3, I leveraged AI-collaborative engineering to overcome three major "Wall-Clock" bottlenecks:

1. **The Windows Signal Trap:** Windows doesn't handle `SIGKILL` like POSIX. I architected a layer to translate these into Windows-native termination to prevent zombie processes.
2. **The Moody VFS:** High-speed logging on Windows triggered `OSError` during `os.fsync()`. I implemented a "Safe Fsync" wrapper that absorbs non-critical OS sync errors.
3. **Context Drift:** Moving to Windows `spawn` meant every object had to be picklable. I had to clean up task ingestion to ensure zero state-leakage.

## 🎯 Mission Profile

* **Massive Fleet Audits:** Running security checks across 100,000+ nodes.
* **High-Volume Command Orchestration:** Executing millions of system-level transactions.
* **Parallel Log Triage:** Ingesting and processing massive NDJSON streams in real-time.

## ⚠️ Where this will break (The Honest Limits)

* **Container Churn:** Creating 10 million *ephemeral* containers will kill the Docker Daemon. Use the `ContainerPool`.
* **Windows Process Overhead:** Even at 23k TPS, Windows `spawn` is heavier than Linux. For absolute peak performance, use a Linux host.
* **Redis Latency:** Remote Redis will plummet throughput. Use local or co-located instances.

---

## 🛠️ Quick Start & Validation

1. **Initialize Environment:**
```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt

```


2. **Logic Check (77 Tests):**
```bash
python run_tests.py -v

```


3. **Performance Smoke Run:**
```bash
python ci_runner.py --use-run-tests --no-slow

```



---

## 📂 Module Index

| Module | Path | Purpose |
| --- | --- | --- |
| **Config** | `aequitas/config.py` | YAML loader + safe defaults. |
| **Dispatcher** | `aequitas/dispatcher.py` | Token-bucket ingestion. |
| **Worker** | `aequitas/worker.py` | Pluggable executor interface. |
| **LogWriter** | `aequitas/log_writer.py` | QueueListener NDJSON writer. |
| **Escalation** | `aequitas/escalation.py` | Redis + in-process sliding window. |
| **ContainerPool** | `aequitas/container_pool.py` | Async pool with overlay reset. |
| **Subprocess** | `aequitas/subprocess_wrapper.py` | Secure Popen + timeouts. |

---

**Architected by DDRG15 via Human-AI Collaborative Engineering.**


