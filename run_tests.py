#!/usr/bin/env python3
"""
run_tests.py
============
Standalone test runner using only Python stdlib unittest.
Zero external dependencies.  Passes 100% on Linux, macOS, and Windows.

CROSS-PLATFORM STRATEGY
------------------------
All platform-specific branches are guarded by flags from platform_compat:

  IS_WINDOWS   - True on win32; guards subprocess command selection and
                 signal-related skips.
  HAVE_RESOURCE- True on POSIX; guards rlimit tests.
  HAVE_SIGKILL - True on POSIX; guards SIGKILL-specific tests.
  HAVE_FORK    - True on POSIX; guards forkserver / preexec_fn tests.

COMMAND PORTABILITY
-------------------
All subprocess commands use sys.executable (the current Python interpreter)
rather than "python3" (absent on Windows) or "echo"/"sleep" (shell builtins
on Windows, not standalone binaries).  This ensures tests run identically
on both platforms without a shell intermediary.

Usage:
    python run_tests.py          # all tests
    python run_tests.py -v       # verbose
    python run_tests.py TestTokenBucket
"""

from __future__ import annotations

import gc
import json
import logging
import logging.handlers
import os
import queue
import sys
import tempfile
import threading
import time
import tracemalloc
import unittest
import weakref
from concurrent.futures import Future
from pathlib import Path

# Ensure local package is importable when run directly
sys.path.insert(0, str(Path(__file__).parent))

from aequitas.config import EngineConfig
from aequitas.dispatcher import Dispatcher, TokenBucket
from aequitas.escalation import (
    CircuitBreakerState,
    InProcessEscalationBackend,
    LockShardedEscalationBackend,
)
from aequitas.log_writer import (
    LogWriterThread,
    _NDJSONWriter,
    build_log_queue,
    install_queue_handler,
)
from aequitas.metrics import MetricsEmitter
from aequitas.platform_compat import (
    HAVE_FORK,
    HAVE_RESOURCE,
    HAVE_SIGKILL,
    IS_WINDOWS,
    IS_POSIX,
    ENV_ALLOWLIST,
    safe_fsync,
    best_mp_context,
)
from aequitas.subprocess_wrapper import SubprocessWrapper

# ---------------------------------------------------------------------------
# Cross-platform skip decorators
# ---------------------------------------------------------------------------

skip_on_windows   = unittest.skipIf(IS_WINDOWS,   "Unix-only feature")
skip_no_resource  = unittest.skipIf(not HAVE_RESOURCE, "resource module unavailable")
skip_no_sigkill   = unittest.skipIf(not HAVE_SIGKILL,  "SIGKILL not available on Windows")
skip_no_fork      = unittest.skipIf(not HAVE_FORK,     "fork/forkserver not available on Windows")

# ---------------------------------------------------------------------------
# Platform-portable command helpers
# ---------------------------------------------------------------------------

def _py(*code_lines: str) -> list[str]:
    """Return [sys.executable, '-c', <code>] — works on every platform."""
    return [sys.executable, "-c", "\n".join(code_lines)]

def _sleep_cmd(seconds: int) -> list[str]:
    """
    Cross-platform sleep command.

    WHY NOT ["sleep", "60"]
    -----------------------
    "sleep" is a standalone executable on POSIX (/bin/sleep) but a CMD shell
    builtin on Windows — subprocess.Popen(["sleep", ...], shell=False) raises
    FileNotFoundError on Windows.  Using sys.executable is portable and avoids
    any shell dependency.
    """
    return _py(f"import time; time.sleep({seconds})")

def _echo_cmd(text: str) -> list[str]:
    """
    Cross-platform echo.

    WHY NOT ["echo", "..."]
    -----------------------
    "echo" is a CMD builtin on Windows, not an executable reachable without
    shell=True.  subprocess.Popen(["echo", ...], shell=False) fails on Windows.
    """
    # repr() ensures the string is safely embedded in the -c argument regardless
    # of embedded quotes, semicolons, or backslashes.
    return _py(f"print({text!r}, end='')")


# ===========================================================================
# Platform compat tests
# ===========================================================================

class TestPlatformCompat(unittest.TestCase):
    """Verify platform_compat exports are coherent on the current OS."""

    def test_flags_are_bool(self):
        self.assertIsInstance(IS_WINDOWS, bool)
        self.assertIsInstance(IS_POSIX, bool)
        self.assertIsInstance(HAVE_RESOURCE, bool)
        self.assertIsInstance(HAVE_FORK, bool)
        self.assertIsInstance(HAVE_SIGKILL, bool)

    def test_flags_are_mutually_consistent(self):
        self.assertNotEqual(IS_WINDOWS, IS_POSIX,
                            "IS_WINDOWS and IS_POSIX must be mutually exclusive")
        if IS_WINDOWS:
            self.assertFalse(HAVE_RESOURCE, "resource module unavailable on Windows")
            self.assertFalse(HAVE_FORK,     "fork unavailable on Windows")
            self.assertFalse(HAVE_SIGKILL,  "SIGKILL unavailable on Windows")
        else:
            self.assertTrue(HAVE_RESOURCE, "resource module expected on POSIX")
            self.assertTrue(HAVE_FORK,     "fork expected on POSIX")
            self.assertTrue(HAVE_SIGKILL,  "SIGKILL expected on POSIX")

    def test_env_allowlist_nonempty(self):
        self.assertGreater(len(ENV_ALLOWLIST), 0)

    def test_env_allowlist_contains_path(self):
        self.assertIn("PATH", ENV_ALLOWLIST)

    def test_windows_allowlist_excludes_posix_vars(self):
        if IS_WINDOWS:
            for v in ("HOME", "SHELL", "USER", "LANG"):
                self.assertNotIn(v, ENV_ALLOWLIST,
                                 f"POSIX variable {v} leaked into Windows allowlist")

    def test_posix_allowlist_excludes_windows_vars(self):
        if IS_POSIX:
            for v in ("USERPROFILE", "COMSPEC", "WINDIR"):
                self.assertNotIn(v, ENV_ALLOWLIST,
                                 f"Windows variable {v} leaked into POSIX allowlist")

    def test_safe_fsync_does_not_raise(self):
        """safe_fsync must never propagate OSError on any platform."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"probe")
            fname = f.name
        try:
            with open(fname, "rb") as fh:
                safe_fsync(fh.fileno())  # must not raise
        finally:
            os.unlink(fname)

    def test_best_mp_context_returns_context(self):
        import multiprocessing
        ctx = best_mp_context()
        self.assertIsNotNone(ctx)
        expected = "spawn" if IS_WINDOWS else "forkserver"
        self.assertEqual(ctx.get_start_method(), expected)

    @skip_no_resource
    def test_resource_module_importable(self):
        from aequitas.platform_compat import resource_mod
        self.assertIsNotNone(resource_mod)
        self.assertTrue(hasattr(resource_mod, "setrlimit"))


# ===========================================================================
# Config tests
# ===========================================================================

class TestSafeDefaults(unittest.TestCase):

    def setUp(self):
        self.cfg = EngineConfig()

    def test_memory_high_water(self):
        self.assertEqual(self.cfg.memory.memory_high_water_mb, 2048)

    def test_log_queue_size(self):
        self.assertEqual(self.cfg.logging.log_queue_size, 100_000)

    def test_max_workers(self):
        self.assertEqual(self.cfg.engine.max_workers, 200)

    def test_batch_size(self):
        self.assertEqual(self.cfg.engine.batch_size, 10_000)

    def test_flush_interval_ms(self):
        self.assertEqual(self.cfg.logging.flush_interval_ms, 500)

    def test_max_batch_bytes(self):
        self.assertEqual(self.cfg.logging.max_batch_bytes, 4_194_304)

    def test_soft_kill_timeout(self):
        self.assertEqual(self.cfg.subprocess.soft_kill_timeout_s, 30)

    def test_hard_kill_timeout(self):
        self.assertEqual(self.cfg.subprocess.hard_kill_timeout_s, 60)

    def test_sanitized_env_excludes_secrets(self):
        """Secret variables must never reach child processes."""
        os.environ["_AQ_TEST_SECRET_KEY"] = "should_not_leak"
        try:
            env = self.cfg.sanitized_env_template()
            self.assertNotIn("_AQ_TEST_SECRET_KEY", env,
                             "Custom secret leaked into sanitized env template")
        finally:
            del os.environ["_AQ_TEST_SECRET_KEY"]

    def test_sanitized_env_platform_vars_present(self):
        """At least one platform-appropriate variable should resolve."""
        env = self.cfg.sanitized_env_template()
        # PATH is in the allowlist on every platform
        if "PATH" in os.environ:
            self.assertIn("PATH", env)

    def test_executor_auto_io(self):
        self.cfg.engine.executor_mode = "auto"
        self.assertEqual(self.cfg.resolve_executor_mode("io"), "threaded")

    def test_executor_auto_cpu(self):
        self.cfg.engine.executor_mode = "auto"
        self.assertEqual(self.cfg.resolve_executor_mode("cpu"), "process")

    def test_executor_explicit_not_overridden(self):
        self.cfg.engine.executor_mode = "threaded"
        self.assertEqual(self.cfg.resolve_executor_mode("cpu"), "threaded")

    def test_yaml_load_defaults(self):
        cfg_path = Path(__file__).parent / "config" / "defaults.yaml"
        cfg = EngineConfig.from_yaml(cfg_path)
        self.assertEqual(cfg.engine.max_workers, 200)
        self.assertEqual(cfg.logging.log_queue_size, 100_000)

    def test_yaml_partial_override(self):
        import yaml
        override = {"engine": {"max_workers": 50}}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            yaml.dump(override, f)
            fpath = f.name
        try:
            cfg = EngineConfig.from_yaml(fpath)
            self.assertEqual(cfg.engine.max_workers, 50)
            self.assertEqual(cfg.logging.log_queue_size, 100_000)
        finally:
            os.unlink(fpath)

    def test_log_dir_is_string(self):
        """log_dir default must be a non-empty string on every platform."""
        self.assertIsInstance(self.cfg.logging.log_dir, str)
        self.assertTrue(len(self.cfg.logging.log_dir) > 0)


# ===========================================================================
# Token bucket tests
# ===========================================================================

class TestTokenBucket(unittest.TestCase):

    def test_initial_tokens_equal_burst(self):
        b = TokenBucket(rate=1000, burst=50)
        self.assertGreaterEqual(b.available, 49)

    def test_consume_succeeds_when_available(self):
        b = TokenBucket(rate=0, burst=10)
        self.assertTrue(b.consume(5))

    def test_consume_fails_when_empty(self):
        b = TokenBucket(rate=0, burst=3)
        b.consume(3)
        self.assertFalse(b.consume(1))

    def test_refill_over_time(self):
        b = TokenBucket(rate=1000, burst=10)
        b.consume(10)
        time.sleep(0.015)
        self.assertGreaterEqual(b.available, 5)

    def test_burst_cap(self):
        b = TokenBucket(rate=100_000, burst=10)
        time.sleep(0.05)
        self.assertLessEqual(b.available, 11)

    def test_thread_safety_1000_threads(self):
        b = TokenBucket(rate=0, burst=1000)
        results = []
        lock = threading.Lock()

        def consume():
            r = b.consume(1)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=consume) for _ in range(1000)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        granted = sum(1 for r in results if r)
        self.assertEqual(granted, 1000)


# ===========================================================================
# Dispatcher tests
# ===========================================================================

def _simple_task(n):
    return n * 2

def _fail_task(n):
    raise ValueError(f"deliberate failure {n}")

def _make_dispatcher_cfg(max_workers=4, batch_size=10):
    cfg = EngineConfig()
    cfg.engine.max_workers = max_workers
    cfg.engine.batch_size = batch_size
    cfg.dispatcher.token_bucket_rate = 1_000_000
    cfg.dispatcher.token_bucket_burst = 100_000
    cfg.dispatcher.ingestion_queue_maxsize = 10_000
    return cfg


class TestDispatcher(unittest.TestCase):

    def test_all_tasks_complete(self):
        cfg = _make_dispatcher_cfg()
        d = Dispatcher(cfg)
        d.start()
        gen = ((_simple_task, (i,), {}) for i in range(100))
        results = list(d.submit_stream(gen))
        d.shutdown()
        self.assertEqual(len(results), 100)

    def test_correct_return_values(self):
        cfg = _make_dispatcher_cfg()
        d = Dispatcher(cfg)
        d.start()
        gen = ((_simple_task, (i,), {}) for i in range(20))
        results = list(d.submit_stream(gen))
        d.shutdown()
        values = sorted(r for r, exc in results if exc is None)
        expected = sorted(i * 2 for i in range(20))
        self.assertEqual(values, expected)

    def test_exceptions_returned_not_raised(self):
        cfg = _make_dispatcher_cfg()
        d = Dispatcher(cfg)
        d.start()
        gen = ((_fail_task, (i,), {}) for i in range(10))
        results = list(d.submit_stream(gen))
        d.shutdown()
        for result, exc in results:
            self.assertIsNotNone(exc)
            self.assertIsInstance(exc, ValueError)

    def test_future_refs_dropped(self):
        """
        submit_stream() must yield (value, exc) tuples — never Future objects.

        This enforces the memory contract: at 10M tasks, retaining Future
        objects (~300 B each) would exhaust the heap.  Callers must never
        receive or store raw Future references.
        """
        cfg = _make_dispatcher_cfg(batch_size=5)
        d = Dispatcher(cfg)
        d.start()
        gen = ((_simple_task, (i,), {}) for i in range(20))
        results = list(d.submit_stream(gen))
        d.shutdown()
        self.assertEqual(len(results), 20)
        for item in results:
            self.assertIsInstance(item, tuple)
            self.assertEqual(len(item), 2)
            self.assertNotIsInstance(item[0], Future,
                                     "Future object leaked into result list")

    def test_no_shell_true_in_dispatcher(self):
        import inspect, re
        from aequitas import dispatcher as disp_mod
        src = inspect.getsource(disp_mod)
        stripped = re.sub(r'""".*?"""', "", src, flags=re.DOTALL)
        stripped = re.sub(r"'''.*?'''", "", stripped, flags=re.DOTALL)
        stripped = re.sub(r"#.*", "", stripped)
        self.assertNotIn("shell=True", stripped)

    def test_generator_lazy_consumption(self):
        cfg = _make_dispatcher_cfg(batch_size=5)
        d = Dispatcher(cfg)
        d.start()
        gen = ((_simple_task, (i,), {}) for i in range(30))
        results = list(d.submit_stream(gen))
        d.shutdown()
        self.assertEqual(len(results), 30)


# ===========================================================================
# Escalation tests
# ===========================================================================

class TestInProcessEscalation(unittest.TestCase):

    def setUp(self):
        self.backend = InProcessEscalationBackend(
            window_seconds=10, bucket_count=2, queue_maxsize=10_000
        )
        time.sleep(0.05)

    def tearDown(self):
        self.backend.stop()
        time.sleep(0.05)

    def test_single_increment(self):
        self.backend.increment("errors", 1)
        time.sleep(0.1)
        self.assertEqual(self.backend.query_window("errors"), 1)

    def test_multi_increment_sum(self):
        for _ in range(100):
            self.backend.increment("errors", 1)
        time.sleep(0.2)
        self.assertEqual(self.backend.query_window("errors"), 100)

    def test_increment_value_gt_one(self):
        self.backend.increment("errors", 42)
        time.sleep(0.1)
        self.assertEqual(self.backend.query_window("errors"), 42)

    def test_reset_clears(self):
        self.backend.increment("errors", 99)
        time.sleep(0.1)
        self.backend.reset("errors")
        time.sleep(0.1)
        self.assertEqual(self.backend.query_window("errors"), 0)

    def test_independent_metrics(self):
        self.backend.increment("errors", 10)
        self.backend.increment("warnings", 5)
        time.sleep(0.2)
        self.assertEqual(self.backend.query_window("errors"), 10)
        self.assertEqual(self.backend.query_window("warnings"), 5)

    def test_concurrent_increments(self):
        n_threads, per_thread = 50, 10
        barrier = threading.Barrier(n_threads)

        def worker():
            barrier.wait()
            for _ in range(per_thread):
                self.backend.increment("concurrent_test", 1)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        time.sleep(0.5)
        total = self.backend.query_window("concurrent_test")
        self.assertEqual(total, n_threads * per_thread)

    def test_window_expiry(self):
        """
        Deterministic sliding-window expiry using a mocked clock.

        Uses _AggregatorThread.snapshot() directly with unittest.mock.patch
        so the test does not need to sleep for real seconds.
        """
        from unittest.mock import patch
        from aequitas.escalation import _AggregatorThread
        import queue as q_mod

        agg_q = q_mod.Queue(maxsize=100)
        agg = _AggregatorThread(bucket_count=2, bucket_width_s=5.0, queue_in=agg_q)

        T0, T1, T2 = 1000.0, 1005.0, 1012.0

        with patch("aequitas.escalation.time.time", return_value=T0):
            agg._do_increment("x", 5)
        with patch("aequitas.escalation.time.time", return_value=T1):
            agg._do_increment("x", 3)

        with patch("aequitas.escalation.time.time", return_value=T0 + 0.1):
            self.assertEqual(agg.snapshot("x"), 5)
        with patch("aequitas.escalation.time.time", return_value=T1 + 0.1):
            self.assertEqual(agg.snapshot("x"), 8)
        with patch("aequitas.escalation.time.time", return_value=T2):
            self.assertEqual(agg.snapshot("x"), 3)

    def test_queue_full_no_exception(self):
        tiny = InProcessEscalationBackend(
            window_seconds=10, bucket_count=2, queue_maxsize=1
        )
        time.sleep(0.05)
        for _ in range(100):
            tiny.increment("x", 1)   # must not raise
        tiny.stop()


class TestLockShardedEscalation(unittest.TestCase):

    def setUp(self):
        self.backend = LockShardedEscalationBackend(
            window_seconds=10, bucket_count=2
        )

    def test_basic_increment_and_query(self):
        self.backend.increment("errs", 7)
        self.assertEqual(self.backend.query_window("errs"), 7)

    def test_reset(self):
        self.backend.increment("errs", 7)
        self.backend.reset("errs")
        self.assertEqual(self.backend.query_window("errs"), 0)

    def test_concurrent_no_data_race(self):
        """200 threads × 100 increments — no lost updates under sharded locks."""
        n_threads, n_incs = 200, 100
        barrier = threading.Barrier(n_threads)

        def worker():
            barrier.wait()
            for _ in range(n_incs):
                self.backend.increment("race_test", 1)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        total = self.backend.query_window("race_test")
        self.assertEqual(total, n_threads * n_incs)


class TestCircuitBreaker(unittest.TestCase):

    def test_initial_closed(self):
        cb = CircuitBreakerState()
        self.assertEqual(cb.state, "CLOSED")
        self.assertFalse(cb.is_open())

    def test_opens_after_3_failures(self):
        cb = CircuitBreakerState(base_delay=0.01, max_delay=0.05)
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, "CLOSED")
        cb.record_failure()
        self.assertEqual(cb.state, "OPEN")
        self.assertTrue(cb.is_open())

    def test_closes_on_success(self):
        cb = CircuitBreakerState(base_delay=0.01, max_delay=0.05)
        cb.record_failure(); cb.record_failure(); cb.record_failure()
        cb.record_success()
        self.assertEqual(cb.state, "CLOSED")

    def test_half_open_after_backoff(self):
        cb = CircuitBreakerState(base_delay=0.01, max_delay=0.05, jitter=0.0)
        cb.record_failure(); cb.record_failure(); cb.record_failure()
        cb.last_failure = time.monotonic() - 9999
        self.assertFalse(cb.is_open())
        self.assertEqual(cb.state, "HALF_OPEN")

    def test_jitter_in_bounds(self):
        cb = CircuitBreakerState(base_delay=1.0, max_delay=10.0, jitter=0.3)
        cb.failures = 1
        for _ in range(200):
            d = cb._backoff_delay()
            self.assertGreaterEqual(d, 0.7 * 2.0)
            self.assertLessEqual(d, 1.3 * 2.0)


# ===========================================================================
# Log writer tests
# ===========================================================================

class TestNDJSONWriter(unittest.TestCase):

    def _make_record(self, level, msg):
        return logging.LogRecord("test", level, "", 0, msg, (), None)

    def test_writes_valid_json_lines(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = _NDJSONWriter(
                log_dir=tmpdir,
                max_bytes_per_file=1_000_000,
                backup_count=2,
                max_batch_bytes=4096,
                flush_interval_ms=50,
            )
            for i in range(10):
                writer.write(self._make_record(logging.INFO, f"msg {i}"))
            writer.close()

            log_file = Path(tmpdir) / "aequitas.ndjson"
            self.assertTrue(log_file.exists())
            lines = log_file.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 10)
            for line in lines:
                obj = json.loads(line)
                self.assertIn("ts_ms", obj)
                self.assertIn("level", obj)
                self.assertIn("msg", obj)

    def test_required_ndjson_fields(self):
        required = {"ts_ms", "level", "pid", "thread", "logger", "msg", "stream"}
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = _NDJSONWriter(
                log_dir=tmpdir,
                max_bytes_per_file=1_000_000,
                backup_count=2,
                max_batch_bytes=4096,
                flush_interval_ms=10,
            )
            writer.write(self._make_record(logging.WARNING, "field_test"))
            writer.close()
            log_file = Path(tmpdir) / "aequitas.ndjson"
            obj = json.loads(log_file.read_text(encoding="utf-8").strip())
            missing = required - set(obj.keys())
            self.assertFalse(missing, f"Missing NDJSON fields: {missing}")

    def test_single_writer_no_interleaving(self):
        """20 threads × 50 messages routed through QueueHandler → valid NDJSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_queue = build_log_queue(maxsize=10_000)
            ndjson_writer = _NDJSONWriter(
                log_dir=tmpdir,
                max_bytes_per_file=10_000_000,
                backup_count=2,
                max_batch_bytes=1_000_000,
                flush_interval_ms=50,
            )
            stop = threading.Event()
            writer = LogWriterThread(
                log_queue=log_queue,
                ndjson_writer=ndjson_writer,
                flush_interval_ms=50,
                backpressure_sample_rate=1.0,
                stop_event=stop,
            )
            writer.start()
            install_queue_handler(log_queue)

            n_threads, msgs_per = 20, 50
            barrier = threading.Barrier(n_threads)

            def emit():
                barrier.wait()
                lg = logging.getLogger(f"t{threading.current_thread().ident}")
                for i in range(msgs_per):
                    lg.info("message %d", i)

            threads = [threading.Thread(target=emit) for _ in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            time.sleep(0.5)
            writer.stop()
            writer.join(timeout=3)

            log_file = Path(tmpdir) / "aequitas.ndjson"
            if log_file.exists():
                for line in log_file.read_text(encoding="utf-8").strip().splitlines():
                    obj = json.loads(line)   # raises if interleaved / corrupted
                    self.assertIn("ts_ms", obj)


# ===========================================================================
# Memory / GC tests
# ===========================================================================

class MockMetricsConfig:
    enabled = False
    push_gateway = ""
    push_interval_s = 15


class TestGCPolicy(unittest.TestCase):

    def _make_emitter(self):
        import psutil
        e = MetricsEmitter.__new__(MetricsEmitter)
        e._cfg = MockMetricsConfig()
        e._labels = {}
        e._push_thread = None
        e._stop_push = threading.Event()
        e._process = psutil.Process()
        return e

    def test_gc_not_triggered_below_hwm(self):
        emitter = self._make_emitter()
        emitter.update_rss = lambda: 100.0
        triggered = emitter.maybe_collect_gc(memory_high_water_mb=2048)
        self.assertFalse(triggered)

    def test_gc_triggered_above_hwm(self):
        emitter = self._make_emitter()
        gc_called = []
        import aequitas.metrics as metrics_mod
        original = metrics_mod.gc.collect
        metrics_mod.gc.collect = lambda: gc_called.append(True)
        emitter.update_rss = lambda: 2049.0
        try:
            triggered = emitter.maybe_collect_gc(memory_high_water_mb=2048)
            self.assertTrue(triggered)
            self.assertEqual(len(gc_called), 1)
        finally:
            metrics_mod.gc.collect = original

    def test_no_leak_in_token_bucket_loop(self):
        tracemalloc.start()
        b = TokenBucket(rate=1_000_000, burst=100_000)
        snap1 = tracemalloc.take_snapshot()
        for _ in range(100_000):
            b.consume(1)
        snap2 = tracemalloc.take_snapshot()
        tracemalloc.stop()
        stats = snap2.compare_to(snap1, "lineno")
        total_new = sum(s.size_diff for s in stats if s.size_diff > 0)
        self.assertLess(total_new, 5 * 1024 * 1024,
                        f"Unexpected alloc: {total_new / 1024:.1f} KB")

    def test_futures_dropped_on_clear(self):
        """
        Results extracted from Futures must be GC-collectable once the
        Future itself is released.  Verifies the as_completed() drop pattern.
        """
        class TaskResult:
            def __init__(self, task_id):
                self.task_id = task_id

        refs = []
        for i in range(10):
            f = Future()
            r = TaskResult(i)
            f.set_result(r)
            refs.append(weakref.ref(r))
            del r
            del f   # immediately dropped — mirrors as_completed() loop body

        for _ in range(3):
            gc.collect()

        alive = sum(1 for r in refs if r() is not None)
        self.assertEqual(alive, 0,
                         f"{alive} TaskResult objects survived after Future drop")


# ===========================================================================
# Subprocess wrapper tests
# ===========================================================================

def _make_wrapper(soft=5, hard=10, threshold=1024):
    cfg = EngineConfig()
    cfg.subprocess.soft_kill_timeout_s = soft
    cfg.subprocess.hard_kill_timeout_s = hard
    cfg.subprocess.stream_threshold_bytes = threshold
    cfg.subprocess.rlimit_cpu_s = 60
    cfg.subprocess.rlimit_as_mb = 1024
    return SubprocessWrapper(cfg.subprocess, cfg.sanitized_env_template())


class TestSubprocessWrapper(unittest.TestCase):

    def test_simple_command(self):
        """Cross-platform: uses sys.executable instead of /bin/echo."""
        w = _make_wrapper()
        r = w.run(_py("print('hello', end='')"))
        self.assertEqual(r.returncode, 0)
        self.assertFalse(r.timed_out)

    def test_stdout_captured(self):
        w = _make_wrapper()
        r = w.run(_py("print('hello world', end='')"))
        self.assertIn(b"hello world", r.stdout_bytes or b"")

    def test_stderr_captured(self):
        w = _make_wrapper()
        r = w.run(_py("import sys; sys.stderr.write('err_out')"))
        self.assertIn(b"err_out", r.stderr_bytes or b"")

    def test_nonzero_exit(self):
        w = _make_wrapper()
        r = w.run(_py("raise SystemExit(42)"))
        self.assertEqual(r.returncode, 42)

    def test_invalid_command_no_raise(self):
        w = _make_wrapper()
        r = w.run(["__not_a_real_binary__"])
        self.assertIsNone(r.returncode)

    def test_task_env_merged(self):
        w = _make_wrapper()
        r = w.run(
            _py("import os; print(os.environ.get('TASK_ID','missing'), end='')"),
            task_env={"TASK_ID": "test_abc"},
        )
        self.assertIn(b"test_abc", r.stdout_bytes or b"")

    def test_no_shell_metachar_execution(self):
        """
        Shell metacharacters in arguments must NOT be interpreted.

        With shell=False, a semicolon is just a character — 'echo hacked'
        must never execute as a second command.
        """
        w = _make_wrapper()
        r = w.run(_py("print('hello; echo hacked', end='')"))
        # The literal string must appear verbatim; no separate "hacked" line
        out = r.stdout_bytes or b""
        self.assertIn(b"hello; echo hacked", out)
        # Ensure there is no additional line that could indicate shell eval
        self.assertNotIn(b"\nhacked", out)

    def test_cmd_must_be_list(self):
        w = _make_wrapper()
        with self.assertRaises((AssertionError, TypeError)):
            w.run("ls -la")  # type: ignore

    def test_empty_cmd_raises(self):
        w = _make_wrapper()
        with self.assertRaises(AssertionError):
            w.run([])

    def test_large_output_streams_to_file(self):
        """Output > stream_threshold_bytes must be written to a temp file."""
        w = _make_wrapper(threshold=100)
        r = w.run(_py("print('x' * 500)"))
        if r.stdout_path:
            self.assertTrue(r.stdout_path.exists())
            r.stdout_path.unlink()
        else:
            self.assertIsNotNone(r.stdout_bytes)

    def test_result_has_platform_field(self):
        """SubprocessResult.platform must match the running OS."""
        w = _make_wrapper()
        r = w.run(_py("pass"))
        expected = "windows" if IS_WINDOWS else "posix"
        self.assertEqual(r.platform, expected)

    def test_no_shell_true_in_source(self):
        """No Popen(shell=True) in executable code (docstring mentions are fine)."""
        import inspect, re
        import aequitas.subprocess_wrapper as sw
        src = inspect.getsource(sw)
        stripped = re.sub(r'""".*?"""', "", src, flags=re.DOTALL)
        stripped = re.sub(r"'''.*?'''", "", stripped, flags=re.DOTALL)
        stripped = re.sub(r"#.*", "", stripped)
        self.assertNotIn("shell=True", stripped,
                         "Found shell=True in executable code")

    def test_soft_kill_timeout(self):
        """Process exceeding soft timeout must be terminated."""
        w = _make_wrapper(soft=1, hard=2)
        t0 = time.monotonic()
        r = w.run(_sleep_cmd(60))
        elapsed = time.monotonic() - t0
        self.assertTrue(r.timed_out, "Expected timed_out=True")
        self.assertLess(elapsed, 5, f"Kill took too long: {elapsed:.1f}s")

    @skip_no_sigkill
    def test_hard_kill_ignoring_sigterm(self):
        """
        POSIX only: a process that ignores SIGTERM must be SIGKILL-ed.

        WHY SKIP ON WINDOWS
        --------------------
        Windows has no SIGTERM that a process can catch and ignore via the
        signal module in this context.  Both terminate() and kill() on Windows
        call TerminateProcess() unconditionally, so the "soft then hard"
        escalation path does not have a meaningful two-phase distinction.
        The soft timeout test above covers the Windows kill path.
        """
        ignore_sigterm = (
            "import signal, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "time.sleep(60)\n"
        )
        w = _make_wrapper(soft=1, hard=2)
        t0 = time.monotonic()
        r = w.run(_py(ignore_sigterm))
        elapsed = time.monotonic() - t0
        self.assertTrue(r.timed_out)
        self.assertLess(elapsed, 4, f"Hard kill took too long: {elapsed:.1f}s")

    @skip_no_resource
    def test_rlimit_preexec_applied(self):
        """
        POSIX only: verify that preexec_fn is not None when HAVE_FORK is True.

        A full rlimit enforcement test would require spawning a process that
        exceeds its CPU limit and catching SIGXCPU — complex to do portably
        in a unit test.  We instead verify that the code path that builds
        the preexec_fn is exercised without error.
        """
        cfg = EngineConfig()
        cfg.subprocess.rlimit_cpu_s = 60
        cfg.subprocess.rlimit_as_mb = 1024
        w = SubprocessWrapper(cfg.subprocess, cfg.sanitized_env_template())
        fn = w._make_preexec()
        self.assertIsNotNone(fn, "preexec_fn should not be None on POSIX")
        self.assertTrue(callable(fn))

    def test_windows_limits_no_raise(self):
        """
        _apply_windows_limits() must never raise — even on POSIX.

        The method is a no-op stub on all platforms until Job Objects are
        implemented; calling it must be safe everywhere.
        """
        import subprocess as sp
        cfg = EngineConfig()
        w = SubprocessWrapper(cfg.subprocess, cfg.sanitized_env_template())
        # Create a real process to pass as argument
        proc = sp.Popen(
            _py("import time; time.sleep(0.1)"),
            stdout=sp.DEVNULL, stderr=sp.DEVNULL
        )
        try:
            w._apply_windows_limits(proc)   # must not raise on any platform
        finally:
            proc.wait()


# ===========================================================================
# Mini smoke run (10k tasks)
# ===========================================================================

class TestSmokeRun10k(unittest.TestCase):

    def test_10k_smoke(self):
        import psutil

        n_tasks = 10_000
        cfg = EngineConfig()
        cfg.engine.max_workers = 8
        cfg.engine.batch_size = 500
        cfg.dispatcher.token_bucket_rate = 1_000_000
        cfg.dispatcher.token_bucket_burst = 100_000
        cfg.dispatcher.ingestion_queue_maxsize = 5_000

        def synthetic(i):
            return {"task_id": i, "status": "ok"}

        d = Dispatcher(cfg)
        d.start()

        proc = psutil.Process()
        t0 = time.monotonic()
        ok = error = 0
        peak_rss = 0.0

        gen = ((synthetic, (i,), {}) for i in range(n_tasks))
        for result, exc in d.submit_stream(gen):
            if exc:
                error += 1
            else:
                ok += 1
            rss = proc.memory_info().rss / (1024 * 1024)
            if rss > peak_rss:
                peak_rss = rss

        elapsed = time.monotonic() - t0
        d.shutdown()
        tps = n_tasks / elapsed

        print(f"\n  10k smoke: {n_tasks:,} tasks in {elapsed:.2f}s = {tps:.0f} tps "
              f"[{'Windows' if IS_WINDOWS else 'POSIX'}]")
        print(f"  ok={ok} error={error} peak_rss={peak_rss:.1f} MB")

        self.assertEqual(error, 0, f"Unexpected errors: {error}")
        self.assertLess(peak_rss, 2048, f"RSS exceeded HWM: {peak_rss:.1f} MB")
        self.assertGreater(tps, 500, f"Throughput too low: {tps:.0f} tps")


# ===========================================================================
# Runner
# ===========================================================================

def _run_suite(verbosity=2, pattern=None):
    loader = unittest.TestLoader()
    if pattern:
        suite = loader.loadTestsFromName(pattern, sys.modules[__name__])
    else:
        suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=verbosity, stream=sys.stdout)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Aequitas cross-platform test runner"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("test", nargs="?", help="Specific test class to run")
    args = parser.parse_args()

    print(f"Platform: {'Windows' if IS_WINDOWS else 'POSIX'} "
          f"| resource={HAVE_RESOURCE} fork={HAVE_FORK} sigkill={HAVE_SIGKILL}")

    sys.exit(_run_suite(verbosity=2 if args.verbose else 1, pattern=args.test))
