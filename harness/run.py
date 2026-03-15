"""
harness/run.py
==============
Staged load test harness: 10k → 100k → 1M → 10M synthetic tasks.

USAGE
-----
    python -m harness.run --stage=10k
    python -m harness.run --stage=100k --no-redis --no-docker
    python -m harness.run --stage=1m --workers=100
    python -m harness.run --stage=10m --workers=200

WHAT EACH STAGE DOES
---------------------
Each stage submits N synthetic tasks through the Dispatcher.  Tasks sleep
for a configurable duration to simulate I/O latency, then return a fake
result dict.  The harness:
  1. Measures throughput (tasks/s)
  2. Samples RSS every K tasks
  3. Tracks escalation counter (simulated errors)
  4. Verifies log queue depth stays below 80% of log_queue_size
  5. Prints a summary report with pass/fail against safe thresholds

GATING THRESHOLDS (smoke run)
------------------------------
  rss_mb < 2048                (memory_high_water_mb)
  log_queue_len < 80_000       (80% of log_queue_size=100_000)
  throughput > 1_000 tasks/s   (minimum viable for 10M in ~2.8 hours)

CI GUIDANCE
-----------
Full 10M runs require a dedicated performance environment.
CI should run --stage=10k with --no-redis --no-docker.
"""

from __future__ import annotations

import argparse
import gc
import logging
import os
import queue
import sys
import tempfile
import time
from pathlib import Path
from typing import Generator

# Adjust path for direct execution
sys.path.insert(0, str(Path(__file__).parent.parent))

from aequitas.config import EngineConfig
from aequitas.dispatcher import Dispatcher
from aequitas.escalation import InProcessEscalationBackend
from aequitas.log_writer import build_log_writer
from aequitas.platform_compat import IS_WINDOWS, default_log_dir

logger = logging.getLogger("harness")

# ---------------------------------------------------------------------------
# Stage sizes
# ---------------------------------------------------------------------------

STAGES = {
    "10k": 10_000,
    "100k": 100_000,
    "1m": 1_000_000,
    "10m": 10_000_000,
}

# ---------------------------------------------------------------------------
# Synthetic task
# ---------------------------------------------------------------------------


def _synthetic_task(task_id: int, sleep_s: float = 0.0, fail_rate: float = 0.01) -> dict:
    """
    Simulate a task that does minimal work.

    fail_rate controls the fraction of tasks that return an "error" status.
    This drives the escalation counter for testing threshold logic.
    """
    import random
    if sleep_s > 0:
        time.sleep(sleep_s)
    status = "error" if random.random() < fail_rate else "ok"
    return {"task_id": task_id, "status": status}


def _task_generator(n: int, sleep_s: float, fail_rate: float) -> Generator:
    """Lazily yield (fn, args, kwargs) tuples for N synthetic tasks."""
    for i in range(n):
        yield (_synthetic_task, (i, sleep_s, fail_rate), {})


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class StageRunner:
    """Orchestrates one stage of the load test."""

    def __init__(
        self,
        cfg: EngineConfig,
        n_tasks: int,
        task_sleep_s: float,
        fail_rate: float,
        rss_check_interval: int,
        enable_escalation: bool,
    ) -> None:
        self._cfg = cfg
        self._n = n_tasks
        self._sleep = task_sleep_s
        self._fail_rate = fail_rate
        self._rss_interval = rss_check_interval
        self._enable_escalation = enable_escalation

        # Metrics collection
        self.results: dict = {
            "n_tasks": n_tasks,
            "ok": 0,
            "error": 0,
            "exceptions": 0,
            "peak_rss_mb": 0.0,
            "throughput_tps": 0.0,
            "duration_s": 0.0,
            "max_log_queue_depth": 0,
            "escalation_window_max": 0,
            "gc_triggered": 0,
        }

    def run(self, log_queue: queue.Queue | None = None) -> dict:
        """Execute the stage and return metrics."""
        escalation = None
        if self._enable_escalation:
            escalation = InProcessEscalationBackend(
                window_seconds=self._cfg.escalation.window_seconds,
                bucket_count=self._cfg.escalation.bucket_count,
            )
            time.sleep(0.05)

        dispatcher = Dispatcher(self._cfg)
        dispatcher.start()

        try:
            import psutil
            proc = psutil.Process()
        except ImportError:
            proc = None

        t_start = time.monotonic()
        completed = 0
        batch_counter = 0

        task_gen = _task_generator(self._n, self._sleep, self._fail_rate)

        for result, exc in dispatcher.submit_stream(task_gen):
            if exc is not None:
                self.results["exceptions"] += 1
            elif result and result.get("status") == "error":
                self.results["error"] += 1
                if escalation:
                    escalation.increment("task_errors", 1)
            else:
                self.results["ok"] += 1

            completed += 1
            batch_counter += 1

            # Periodic checks
            if batch_counter >= self._rss_interval:
                batch_counter = 0

                if proc:
                    rss_mb = proc.memory_info().rss / (1024 * 1024)
                    if rss_mb > self.results["peak_rss_mb"]:
                        self.results["peak_rss_mb"] = rss_mb

                    if rss_mb > self._cfg.memory.memory_high_water_mb:
                        t_gc = time.monotonic()
                        gc.collect()
                        self.results["gc_triggered"] += 1
                        logger.info("GC triggered at RSS=%.1f MB, pause=%.3fs",
                                    rss_mb, time.monotonic() - t_gc)

                if log_queue:
                    depth = log_queue.qsize()
                    if depth > self.results["max_log_queue_depth"]:
                        self.results["max_log_queue_depth"] = depth

                if escalation:
                    window = escalation.query_window("task_errors")
                    if window > self.results["escalation_window_max"]:
                        self.results["escalation_window_max"] = window
                    if window >= self._cfg.escalation.error_threshold:
                        logger.warning("ESCALATION THRESHOLD BREACHED: window=%d", window)

        t_end = time.monotonic()
        self.results["duration_s"] = t_end - t_start
        self.results["throughput_tps"] = completed / max(self.results["duration_s"], 0.001)

        dispatcher.shutdown()
        if escalation:
            escalation.stop()

        return self.results


# ---------------------------------------------------------------------------
# Threshold check
# ---------------------------------------------------------------------------


def check_thresholds(results: dict, cfg: EngineConfig) -> bool:
    """
    Evaluate results against safe thresholds.

    Returns True if ALL thresholds pass (ready-for-review gate).
    """
    hwm = cfg.memory.memory_high_water_mb
    log_limit = cfg.logging.log_queue_size * 0.8
    min_tps = 1_000  # minimum viable throughput

    checks = [
        ("rss_mb < HWM",
         results["peak_rss_mb"] < hwm,
         f"{results['peak_rss_mb']:.1f} MB < {hwm} MB"),
        ("log_queue < 80%",
         results["max_log_queue_depth"] < log_limit,
         f"{results['max_log_queue_depth']} < {int(log_limit)}"),
        ("throughput > 1000 tps",
         results["throughput_tps"] > min_tps,
         f"{results['throughput_tps']:.0f} tps > {min_tps}"),
        ("no exceptions",
         results["exceptions"] == 0,
         f"exceptions={results['exceptions']}"),
    ]

    all_pass = True
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {name}: {detail}")
        if not passed:
            all_pass = False

    return all_pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Aequitas load test harness")
    parser.add_argument("--stage", choices=list(STAGES.keys()), default="10k")
    parser.add_argument("--workers", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--task-sleep-ms", type=float, default=0.0,
                        help="Simulated I/O latency per task in milliseconds")
    parser.add_argument("--fail-rate", type=float, default=0.01,
                        help="Fraction of tasks that return error status")
    parser.add_argument("--no-redis", action="store_true",
                        help="Use in-process escalation backend")
    parser.add_argument("--no-docker", action="store_true",
                        help="Use process mode (no containers)")
    parser.add_argument("--rss-check-interval", type=int, default=10_000,
                        help="Check RSS every N completed tasks")
    parser.add_argument("--log-dir", default=default_log_dir(),
                        help="Directory for NDJSON log output")
    args = parser.parse_args()

    n_tasks = STAGES[args.stage]

    # Build config
    cfg = EngineConfig()
    cfg.engine.max_workers = args.workers
    cfg.engine.batch_size = args.batch_size
    cfg.escalation.backend = "in_process" if args.no_redis else "redis"
    cfg.container_pool.mode = "process" if args.no_docker else "pooled"
    cfg.logging.log_dir = args.log_dir

    print(f"\n{'='*60}")
    print(f"Aequitas Load Test — Stage: {args.stage} ({n_tasks:,} tasks)")
    print(f"  workers={args.workers}, batch={args.batch_size}")
    print(f"  task_sleep={args.task_sleep_ms}ms, fail_rate={args.fail_rate:.1%}")
    print(f"  escalation={cfg.escalation.backend}, containers={cfg.container_pool.mode}")
    print(f"  log_dir={args.log_dir}")
    print(f"{'='*60}\n")

    # Set up logging pipeline
    log_queue, log_writer = build_log_writer(cfg.logging)
    log_writer.start()

    logging.basicConfig(level=logging.INFO)
    logger.info("Starting stage=%s n=%d", args.stage, n_tasks)

    runner = StageRunner(
        cfg=cfg,
        n_tasks=n_tasks,
        task_sleep_s=args.task_sleep_ms / 1000,
        fail_rate=args.fail_rate,
        rss_check_interval=args.rss_check_interval,
        enable_escalation=True,
    )

    results = runner.run(log_queue=log_queue)

    log_writer.stop()
    log_writer.join(timeout=5)

    # Report
    print("\n--- Results ---")
    for k, v in results.items():
        if isinstance(v, float):
            print(f"  {k}: {v:.3f}")
        else:
            print(f"  {k}: {v}")

    print("\n--- Threshold Checks ---")
    passed = check_thresholds(results, cfg)

    if passed:
        print("\n✅ READY-FOR-REVIEW: All thresholds passed.")
        sys.exit(0)
    else:
        print("\n❌ NOT READY: One or more thresholds failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
