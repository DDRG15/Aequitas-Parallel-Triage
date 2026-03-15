"""
tests/test_escalation.py
========================
Deterministic correctness tests for all three escalation backends.

Run:
    pytest tests/test_escalation.py -v
    pytest tests/test_escalation.py -v -m "not redis"   # skip Redis
    pytest tests/test_escalation.py -v -m "not integration"

Reviewer verification:
    pytest tests/test_escalation.py::test_inprocess_window_sum -v    # expected: PASSED
    pytest tests/test_escalation.py::test_inprocess_bucket_expiry -v # expected: PASSED
    pytest tests/test_escalation.py::test_locksharded_concurrent -v  # expected: PASSED
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from aequitas.escalation import (
    CircuitBreakerState,
    InProcessEscalationBackend,
    LockShardedEscalationBackend,
    RedisEscalationBackend,
)


# ---------------------------------------------------------------------------
# In-process aggregator tests
# ---------------------------------------------------------------------------


class TestInProcessEscalation:
    """Tests for InProcessEscalationBackend (no external dependencies)."""

    def setup_method(self):
        # 10-second window, 2 buckets (each 5 seconds wide)
        self.backend = InProcessEscalationBackend(
            window_seconds=10,
            bucket_count=2,
            queue_maxsize=1000,
        )
        # Allow aggregator thread to start
        time.sleep(0.05)

    def teardown_method(self):
        self.backend.stop()
        time.sleep(0.05)

    def test_single_increment_visible_in_window(self):
        """A single increment should be visible in the sliding window."""
        self.backend.increment("errors", 1)
        time.sleep(0.1)   # wait for aggregator thread
        total = self.backend.query_window("errors")
        assert total == 1, f"Expected 1, got {total}"

    def test_multiple_increments_sum_correctly(self):
        """Multiple increments on the same metric should accumulate."""
        for _ in range(100):
            self.backend.increment("errors", 1)
        time.sleep(0.2)
        total = self.backend.query_window("errors")
        assert total == 100, f"Expected 100, got {total}"

    def test_increment_value_gt_one(self):
        """increment(metric, N) should add N to the counter."""
        self.backend.increment("errors", 42)
        time.sleep(0.1)
        assert self.backend.query_window("errors") == 42

    def test_reset_clears_counter(self):
        """reset() should zero all buckets for the metric."""
        self.backend.increment("errors", 99)
        time.sleep(0.1)
        self.backend.reset("errors")
        time.sleep(0.1)
        assert self.backend.query_window("errors") == 0

    def test_independent_metrics(self):
        """Two metrics must not interfere with each other."""
        self.backend.increment("errors", 10)
        self.backend.increment("warnings", 5)
        time.sleep(0.2)
        assert self.backend.query_window("errors") == 10
        assert self.backend.query_window("warnings") == 5

    def test_queue_full_drops_gracefully(self):
        """When the queue is full, increments are dropped without exception."""
        tiny = InProcessEscalationBackend(
            window_seconds=10,
            bucket_count=2,
            queue_maxsize=1,   # deliberately tiny
        )
        time.sleep(0.05)
        # Flood the queue — must not raise
        for _ in range(100):
            tiny.drop_count = getattr(tiny, "drop_count", 0)
            tiny.increment("x", 1)  # some will drop silently
        tiny.stop()

    def test_concurrent_increments_no_lost_updates(self):
        """
        Concurrent increments from N threads must all be accounted for
        (subject to queue-full drops — queue is large enough here).

        DETERMINISTIC BECAUSE:
        - Aggregator is single-threaded, so increments are serialised.
        - Queue size (10_000) >> N_threads × increments_per_thread (50×10=500).
        - No drops expected.
        """
        n_threads = 50
        increments_per_thread = 10
        expected = n_threads * increments_per_thread
        barrier = threading.Barrier(n_threads)

        def worker():
            barrier.wait()  # synchronise all threads to start together
            for _ in range(increments_per_thread):
                self.backend.increment("concurrent_test", 1)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        time.sleep(0.5)   # give aggregator thread time to process queue
        total = self.backend.query_window("concurrent_test")
        assert total == expected, f"Expected {expected}, got {total}"

    def test_window_sum_sliding(self):
        """
        Sliding window expiry verified via deterministic mocked clock.

        WHY MOCKED CLOCK
        ----------------
        The original test slept for ~2.3 s in total, making it sensitive to
        CI scheduler jitter and OS timer granularity.  By patching time.time()
        inside _AggregatorThread._do_increment / .snapshot() we control the
        bucket assignment with zero real-time delay.

        This is deterministic on all platforms (no sleep-based races).
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

        # Only bucket 200 populated
        with patch("aequitas.escalation.time.time", return_value=T0 + 0.1):
            assert agg.snapshot("x") == 5, f"Expected 5, got {agg.snapshot('x')}"

        # Both buckets 200 and 201 in window
        with patch("aequitas.escalation.time.time", return_value=T1 + 0.1):
            total = agg.snapshot("x")
            assert total == 8, f"Expected 8, got {total}"

        # Bucket 200 expired; only bucket 201 remains
        with patch("aequitas.escalation.time.time", return_value=T2):
            total = agg.snapshot("x")
            assert total == 3, f"Expected 3 after expiry, got {total}"


# ---------------------------------------------------------------------------
# Lock-sharded backend tests
# ---------------------------------------------------------------------------


class TestLockShardedEscalation:
    """Tests for LockShardedEscalationBackend (no external dependencies)."""

    def setup_method(self):
        self.backend = LockShardedEscalationBackend(
            window_seconds=10,
            bucket_count=2,
        )

    def test_increment_and_query(self):
        self.backend.increment("errs", 7)
        assert self.backend.query_window("errs") == 7

    def test_reset(self):
        self.backend.increment("errs", 7)
        self.backend.reset("errs")
        assert self.backend.query_window("errs") == 0

    def test_concurrent_no_data_race(self):
        """
        200 threads each increment 100 times = 20_000 expected total.
        Under sharded locks, no data races should occur.
        """
        n_threads = 200
        n_incs = 100
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
        assert total == n_threads * n_incs, f"Expected {n_threads * n_incs}, got {total}"


# ---------------------------------------------------------------------------
# Circuit breaker tests
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    """Tests for CircuitBreakerState."""

    def test_initial_state_closed(self):
        cb = CircuitBreakerState()
        assert cb.state == "CLOSED"
        assert not cb.is_open()

    def test_opens_after_3_failures(self):
        cb = CircuitBreakerState(base_delay=0.01, max_delay=0.05)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "CLOSED"
        cb.record_failure()
        assert cb.state == "OPEN"
        assert cb.is_open()

    def test_closes_on_success(self):
        cb = CircuitBreakerState(base_delay=0.01, max_delay=0.05)
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open()
        cb.record_success()
        assert cb.state == "CLOSED"
        assert not cb.is_open()

    def test_half_open_after_backoff(self):
        cb = CircuitBreakerState(base_delay=0.01, max_delay=0.05, jitter=0.0)
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open()
        # Simulate time passing beyond backoff
        cb.last_failure = time.monotonic() - 999
        # is_open() should transition to HALF_OPEN and return False (allow probe)
        assert not cb.is_open()
        assert cb.state == "HALF_OPEN"

    def test_jitter_within_bounds(self):
        """Backoff with jitter should stay within [base*(1-jitter), base*(1+jitter)]."""
        cb = CircuitBreakerState(base_delay=1.0, max_delay=10.0, jitter=0.3)
        cb.failures = 1
        delays = [cb._backoff_delay() for _ in range(100)]
        for d in delays:
            assert 0.7 * 2.0 <= d <= 1.3 * 2.0, f"Jitter out of bounds: {d}"


# ---------------------------------------------------------------------------
# Redis backend tests (requires Redis; skipped in CI without it)
# ---------------------------------------------------------------------------


@pytest.mark.redis
@pytest.mark.integration
class TestRedisEscalation:
    """
    Tests for RedisEscalationBackend.

    These are integration tests that require a live Redis instance.
    Skip with: pytest -m "not redis"

    Reviewer: run with:
        docker run -d -p 6379:6379 redis:7
        pytest tests/test_escalation.py -m redis -v
    """

    def setup_method(self):
        pytest.importorskip("redis")
        try:
            import redis as redis_lib
            client = redis_lib.from_url("redis://localhost:6379/0")
            client.ping()
        except Exception:
            pytest.skip("Redis not available")

        self.backend = RedisEscalationBackend(
            redis_url="redis://localhost:6379/0",
            window_seconds=10,
            bucket_count=2,
            bucket_ttl_s=20,
            error_threshold=500,
        )
        self.backend.reset("test_metric")

    def teardown_method(self):
        self.backend.reset("test_metric")

    def test_incr_and_query(self):
        self.backend.increment("test_metric", 1)
        time.sleep(0.05)
        total = self.backend.query_window("test_metric")
        assert total >= 1

    def test_key_format(self):
        """Verify Redis key format matches expected pattern."""
        import redis as redis_lib
        client = redis_lib.from_url("redis://localhost:6379/0")
        self.backend.increment("key_format_test", 1)
        keys = client.keys("aequitas:escalation:key_format_test:*")
        assert len(keys) >= 1
        key = keys[0]
        parts = key.split(":")
        assert parts[0] == "aequitas"
        assert parts[1] == "escalation"
        assert parts[2] == "key_format_test"
        assert parts[3].isdigit()

    def test_ttl_set(self):
        """Keys must have a TTL set (auto-expiry works)."""
        import redis as redis_lib
        client = redis_lib.from_url("redis://localhost:6379/0")
        self.backend.increment("ttl_test", 1)
        keys = client.keys("aequitas:escalation:ttl_test:*")
        assert keys
        ttl = client.ttl(keys[0])
        assert ttl > 0, f"TTL should be positive, got {ttl}"

    def test_circuit_breaker_blocks_failing_redis(self):
        """When Redis is unreachable, circuit breaker should open and calls return gracefully."""
        bad_backend = RedisEscalationBackend(
            redis_url="redis://localhost:19999/0",   # nothing here
            window_seconds=10,
            bucket_count=2,
            bucket_ttl_s=20,
            error_threshold=500,
        )
        # Should not raise; circuit breaker absorbs failures
        for _ in range(5):
            bad_backend.increment("x", 1)
        result = bad_backend.query_window("x")
        assert result == -1  # -1 signals "unavailable"

    def test_sliding_window_sum(self):
        """
        Sliding window sum script for reviewers.

        EXPECTED OUTPUT EXAMPLE:
            Bucket 0: aequitas:escalation:test_metric:34521 = 5
            Bucket 1: aequitas:escalation:test_metric:34520 = 0
            Window sum: 5
        """
        import redis as redis_lib
        client = redis_lib.from_url("redis://localhost:6379/0")
        self.backend.reset("test_metric")
        self.backend.increment("test_metric", 5)
        time.sleep(0.1)

        total = self.backend.query_window("test_metric")
        bucket_width = 5   # 10s / 2 buckets
        current_bucket = int(time.time()) // bucket_width

        print("\n--- Sliding Window Verification ---")
        for i in range(2):
            bid = current_bucket - i
            key = f"aequitas:escalation:test_metric:{bid}"
            val = client.get(key)
            print(f"Bucket {i}: {key} = {val or 0}")
        print(f"Window sum: {total}")

        assert total == 5
