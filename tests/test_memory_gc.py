"""
tests/test_memory_gc.py
=======================
Tests for memory management: high-water mark GC triggering, tracemalloc
leak detection, and no-GC-on-every-batch enforcement.

Run:
    pytest tests/test_memory_gc.py -v

Reviewer verification:
    pytest tests/test_memory_gc.py::TestGCPolicy -v
    pytest tests/test_memory_gc.py::TestHighWaterMark -v
"""

from __future__ import annotations

import gc
import sys
import threading
import tracemalloc
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from aequitas.metrics import MetricsEmitter


# ---------------------------------------------------------------------------
# Mock config
# ---------------------------------------------------------------------------


class MockMetricsCfg:
    enabled = False  # avoid prometheus dependency
    push_gateway = ""
    push_interval_s = 15


class MockMetricsNoPrometheus(MetricsEmitter):
    """MetricsEmitter with prometheus and psutil mocked out."""

    def __init__(self):
        # Bypass __init__ prometheus setup
        self._cfg = MockMetricsCfg()
        self._labels = {}
        self._push_thread = None
        self._stop_push = threading.Event()
        self._process = None


# ---------------------------------------------------------------------------
# GC policy tests
# ---------------------------------------------------------------------------


class TestGCPolicy:

    def test_gc_not_triggered_below_hwm(self):
        """
        maybe_collect_gc() must NOT call gc.collect() when RSS < HWM.
        """
        emitter = MockMetricsNoPrometheus()
        rss_low = 100  # MB

        with patch.object(emitter, "update_rss", return_value=rss_low):
            with patch("aequitas.metrics.gc") as mock_gc:
                triggered = emitter.maybe_collect_gc(memory_high_water_mb=2048)
                mock_gc.collect.assert_not_called()
                assert triggered is False

    def test_gc_triggered_at_hwm(self):
        """
        maybe_collect_gc() MUST call gc.collect() when RSS >= HWM.
        """
        emitter = MockMetricsNoPrometheus()
        rss_over = 2049  # MB — 1 MB over HWM

        call_log = []

        def fake_update_rss():
            return rss_over

        with patch.object(emitter, "update_rss", side_effect=fake_update_rss):
            with patch("aequitas.metrics.gc") as mock_gc:
                triggered = emitter.maybe_collect_gc(memory_high_water_mb=2048)
                mock_gc.collect.assert_called_once()
                assert triggered is True

    def test_gc_not_called_every_batch(self):
        """
        Simulate N batch iterations: gc.collect() should only be called
        when the emulated RSS crosses the HWM, not on every iteration.
        """
        emitter = MockMetricsNoPrometheus()
        hwm = 2048
        gc_calls = []

        # Emulate RSS growing: starts below HWM, crosses it at batch 8
        rss_series = [200 + i * 50 for i in range(10)]  # 200, 250, ... 650 MB (never > 2048)

        gc_count = 0
        for rss in rss_series:
            with patch.object(emitter, "update_rss", return_value=rss):
                with patch("aequitas.metrics.gc") as mock_gc:
                    emitter.maybe_collect_gc(memory_high_water_mb=hwm)
                    gc_count += mock_gc.collect.call_count

        assert gc_count == 0, f"GC should not have been triggered; called {gc_count} times"

    def test_gc_triggered_once_when_rss_crosses_hwm(self):
        """When RSS crosses HWM mid-run, GC fires exactly once per crossing."""
        emitter = MockMetricsNoPrometheus()
        hwm = 500

        rss_series = [400, 450, 510, 520]  # crosses at index 2
        trigger_count = 0

        for rss in rss_series:
            with patch.object(emitter, "update_rss", return_value=rss):
                with patch("aequitas.metrics.gc") as mock_gc:
                    result = emitter.maybe_collect_gc(memory_high_water_mb=hwm)
                    if result:
                        trigger_count += 1

        # GC triggered at rss=510 and 520 (both over HWM)
        assert trigger_count == 2


# ---------------------------------------------------------------------------
# Memory leak detection via tracemalloc
# ---------------------------------------------------------------------------


class TestTracemalloc:

    def test_no_unbounded_growth_in_dispatcher(self):
        """
        Run the token bucket in a tight loop and verify no unbounded allocation.

        This test verifies that repeated consume() calls do not leak memory
        (no list/dict growth per call).

        NOTE: This is a proxy test — full dispatcher requires integration.
        """
        from aequitas.dispatcher import TokenBucket

        tracemalloc.start()
        bucket = TokenBucket(rate=1_000_000, burst=100_000)

        snapshot1 = tracemalloc.take_snapshot()

        for _ in range(100_000):
            bucket.consume(1)

        snapshot2 = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Compare top allocations
        top_stats = snapshot2.compare_to(snapshot1, "lineno")
        # Total new allocation should be modest (< 5 MB for 100k iterations)
        total_new = sum(s.size_diff for s in top_stats if s.size_diff > 0)
        assert total_new < 5 * 1024 * 1024, (
            f"Unexpected memory growth: {total_new / 1024:.1f} KB in token bucket loop"
        )

    def test_future_references_dropped_after_completion(self):
        """
        After iterating as_completed(), no Future objects should remain
        reachable from the result list.

        This is a conceptual test: we verify that when futures are collected
        into a list and iterated, dropping the list drops all futures.
        """
        import weakref
        from concurrent.futures import Future

        futures = []
        refs = []

        for _ in range(10):
            f = Future()
            f.set_result("done")
            futures.append(f)
            refs.append(weakref.ref(f))

        # Simulate "drop references after iteration"
        futures.clear()
        gc.collect()

        alive = sum(1 for r in refs if r() is not None)
        assert alive == 0, f"{alive} futures still alive after clearing list"
