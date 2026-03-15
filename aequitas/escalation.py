"""
aequitas/escalation.py
======================
Sliding-window error escalation counter with three backends.

BACKEND SELECTION HIERARCHY
----------------------------
1. Redis INCR+TTL (primary, recommended for 10M+ workloads, multi-process safe)
2. In-process aggregator (single-threaded, bounded queue, lock-free reads)
3. Lock-sharded per-bucket counters (last resort, all-in-process, high lock contention)

WHY THREE BACKENDS
------------------
Redis is unavailable in some CI/test environments.  The in-process aggregator
covers the common single-host case.  The lock-sharded fallback is provided for
completeness but should be avoided — see lock contention notes below.

SLIDING WINDOW ALGORITHM
------------------------
We maintain N time buckets, each covering window_seconds / bucket_count seconds.
Incrementing a counter in bucket B also sets a TTL on the Redis key.
To query the window, sum the last N bucket keys.

Redis key format:  aequitas:escalation:{metric}:{bucket_id}
Example:           aequitas:escalation:task_errors:34521  (bucket = epoch_s // bucket_width)

CIRCUIT BREAKER
---------------
Per-backend circuit breaker prevents hammering a failing Redis host.
State transitions: CLOSED → OPEN (on consecutive failures) → HALF_OPEN (after backoff) → CLOSED
Backoff uses exponential delay with random jitter to avoid thundering herd.
"""

from __future__ import annotations

import abc
import logging
import math
import queue
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Protocol

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


@dataclass
class CircuitBreakerState:
    """
    Per-backend circuit breaker.

    WHY EXPONENTIAL BACKOFF WITH JITTER
    ------------------------------------
    Multiple workers retrying simultaneously after a Redis failure creates a
    thundering herd.  Jitter spreads retries across the backoff window, reducing
    peak load on the recovering service.
    """
    failures: int = 0
    last_failure: float = 0.0
    state: str = "CLOSED"   # CLOSED | OPEN | HALF_OPEN
    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter: float = 0.3
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_failure(self) -> None:
        with self._lock:
            self.failures += 1
            self.last_failure = time.monotonic()
            if self.failures >= 3:
                self.state = "OPEN"
                logger.warning("CircuitBreaker OPEN after %d failures", self.failures)

    def record_success(self) -> None:
        with self._lock:
            self.failures = 0
            self.state = "CLOSED"

    def is_open(self) -> bool:
        with self._lock:
            if self.state == "CLOSED":
                return False
            if self.state == "OPEN":
                delay = self._backoff_delay()
                if time.monotonic() - self.last_failure > delay:
                    self.state = "HALF_OPEN"
                    return False   # Allow one probe
                return True
            return False  # HALF_OPEN: allow probe

    def _backoff_delay(self) -> float:
        exp = min(self.failures, 10)
        base = min(self.base_delay * (2 ** exp), self.max_delay)
        return base * (1 + random.uniform(-self.jitter, self.jitter))


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------


class EscalationBackend(abc.ABC):
    """Abstract interface all backends must satisfy."""

    @abc.abstractmethod
    def increment(self, metric: str, value: int = 1) -> None:
        """Atomically increment the counter for `metric` in the current time bucket."""

    @abc.abstractmethod
    def query_window(self, metric: str) -> int:
        """Return the sum of `metric` across the entire sliding window."""

    @abc.abstractmethod
    def reset(self, metric: str) -> None:
        """Reset all buckets for `metric`."""


# ---------------------------------------------------------------------------
# Redis backend
# ---------------------------------------------------------------------------


class RedisEscalationBackend(EscalationBackend):
    """
    Redis INCR+TTL sliding window.

    WHY REDIS FOR PRODUCTION
    ------------------------
    - INCR is atomic: no lost increments under concurrent workers.
    - TTL handles bucket expiry automatically (no cleanup threads).
    - Survives worker process restarts (state is external).
    - Multi-host deployments share the same counter without coordination.

    KEY LAYOUT
    ----------
    aequitas:escalation:{metric}:{bucket_id}
    bucket_id = int(epoch_seconds) // bucket_width_s

    TTL = bucket_ttl_s (default 2× window — keeps overlap for late arrivals)
    """

    _KEY_PREFIX = "aequitas:escalation"

    def __init__(
        self,
        redis_url: str,
        window_seconds: int,
        bucket_count: int,
        bucket_ttl_s: int,
        error_threshold: int,
        circuit_breaker: CircuitBreakerState | None = None,
    ) -> None:
        import redis as redis_lib
        self._client = redis_lib.from_url(redis_url, decode_responses=True)
        self._window_s = window_seconds
        self._bucket_count = bucket_count
        self._bucket_width_s = window_seconds // bucket_count
        self._ttl_s = bucket_ttl_s
        self._threshold = error_threshold
        self._cb = circuit_breaker or CircuitBreakerState()

    def _bucket_id(self) -> int:
        return int(time.time()) // self._bucket_width_s

    def _key(self, metric: str, bucket_id: int) -> str:
        return f"{self._KEY_PREFIX}:{metric}:{bucket_id}"

    def increment(self, metric: str, value: int = 1) -> None:
        if self._cb.is_open():
            logger.debug("RedisEscalation: circuit open, skipping increment")
            return
        try:
            key = self._key(metric, self._bucket_id())
            pipe = self._client.pipeline()
            pipe.incrby(key, value)
            pipe.expire(key, self._ttl_s)
            pipe.execute()
            self._cb.record_success()
        except Exception as exc:  # noqa: BLE001
            logger.warning("RedisEscalation.increment failed: %s", exc)
            self._cb.record_failure()

    def query_window(self, metric: str) -> int:
        if self._cb.is_open():
            return -1   # signal: data unavailable
        current_bucket = self._bucket_id()
        keys = [
            self._key(metric, current_bucket - i)
            for i in range(self._bucket_count)
        ]
        try:
            values = self._client.mget(*keys)
            total = sum(int(v) for v in values if v is not None)
            self._cb.record_success()
            return total
        except Exception as exc:  # noqa: BLE001
            logger.warning("RedisEscalation.query_window failed: %s", exc)
            self._cb.record_failure()
            return -1

    def reset(self, metric: str) -> None:
        current_bucket = self._bucket_id()
        keys = [
            self._key(metric, current_bucket - i)
            for i in range(self._bucket_count)
        ]
        try:
            self._client.delete(*keys)
        except Exception as exc:  # noqa: BLE001
            logger.warning("RedisEscalation.reset failed: %s", exc)


# ---------------------------------------------------------------------------
# In-process aggregator backend
# ---------------------------------------------------------------------------


class _AggregatorThread(threading.Thread):
    """
    Single-threaded aggregator that serialises all increments.

    WHY SINGLE-THREADED
    -------------------
    A single thread eliminates lock contention for writes.  Reads are served
    from a snapshot array updated on every increment — no lock required for
    reads (Python GIL provides read atomicity on lists of ints).
    """

    def __init__(
        self,
        bucket_count: int,
        bucket_width_s: float,
        queue_in: queue.Queue,
    ) -> None:
        super().__init__(name="aequitas-escalation-aggregator", daemon=True)
        self._bucket_count = bucket_count
        self._bucket_width = bucket_width_s
        self._q = queue_in
        # buckets: dict[metric] -> list[int] indexed by bucket_id % bucket_count
        self._buckets: dict[str, list[int]] = {}
        self._bucket_ids: dict[str, list[int]] = {}  # tracks which epoch each slot holds
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                op, metric, value = self._q.get(timeout=0.05)
            except queue.Empty:
                continue
            if op == "incr":
                self._do_increment(metric, value)
            elif op == "reset":
                self._buckets.pop(metric, None)
                self._bucket_ids.pop(metric, None)

    def _do_increment(self, metric: str, value: int) -> None:
        if metric not in self._buckets:
            self._buckets[metric] = [0] * self._bucket_count
            self._bucket_ids[metric] = [-1] * self._bucket_count

        current_id = int(time.time() / self._bucket_width)
        slot = current_id % self._bucket_count
        buckets = self._buckets[metric]
        ids = self._bucket_ids[metric]

        # If this slot held a stale bucket, clear it first
        if ids[slot] != current_id:
            buckets[slot] = 0
            ids[slot] = current_id

        buckets[slot] += value

    def snapshot(self, metric: str) -> int:
        """
        Return sliding window sum for `metric`.

        Thread-safe read: Python list access is atomic under the GIL.
        We only sum buckets whose epoch matches the current window.
        """
        buckets = self._buckets.get(metric)
        ids = self._bucket_ids.get(metric)
        if not buckets or not ids:
            return 0

        current_id = int(time.time() / self._bucket_width)
        window_start = current_id - self._bucket_count + 1
        return sum(
            buckets[i % self._bucket_count]
            for i in range(window_start, current_id + 1)
            if ids[i % self._bucket_count] == i
        )

    def stop(self) -> None:
        self._stop.set()


class InProcessEscalationBackend(EscalationBackend):
    """
    In-process aggregator with a bounded queue.

    WHY BOUNDED QUEUE
    -----------------
    If the aggregator thread falls behind, the bounded queue prevents the
    calling threads from leaking memory.  When full, increments are dropped
    with a warning — acceptable for escalation counters where precision matters
    less than availability.
    """

    def __init__(
        self,
        window_seconds: int,
        bucket_count: int,
        queue_maxsize: int = 10_000,
    ) -> None:
        self._bucket_count = bucket_count
        self._bucket_width = window_seconds / bucket_count
        self._q: queue.Queue = queue.Queue(maxsize=queue_maxsize)
        self._aggregator = _AggregatorThread(bucket_count, self._bucket_width, self._q)
        self._aggregator.start()

    def increment(self, metric: str, value: int = 1) -> None:
        try:
            self._q.put_nowait(("incr", metric, value))
        except queue.Full:
            logger.debug("EscalationQueue full; dropping increment for %s", metric)

    def query_window(self, metric: str) -> int:
        return self._aggregator.snapshot(metric)

    def reset(self, metric: str) -> None:
        try:
            self._q.put_nowait(("reset", metric, 0))
        except queue.Full:
            pass

    def stop(self) -> None:
        self._aggregator.stop()


# ---------------------------------------------------------------------------
# Lock-sharded fallback backend (last resort)
# ---------------------------------------------------------------------------


class LockShardedEscalationBackend(EscalationBackend):
    """
    Lock-sharded per-bucket counters.  LAST RESORT ONLY.

    WHY THIS IS INFERIOR
    --------------------
    - Each increment acquires a lock → contention under 200 threads.
    - No external state → lost on process restart.
    - Sharding reduces contention but does not eliminate it.

    Prefer Redis or in-process aggregator.  This exists only when both are
    unavailable and you still need *some* escalation counting.

    SHARDING
    ---------
    N_SHARDS lock shards are allocated.  metric+bucket_id is hashed to a
    shard, so two metrics never share a lock unless their hash collides.
    """

    N_SHARDS = 16

    def __init__(self, window_seconds: int, bucket_count: int) -> None:
        self._bucket_count = bucket_count
        self._bucket_width = window_seconds / bucket_count
        # shards: list of (lock, dict[metric, list[int]])
        self._shards: list[tuple[threading.Lock, dict]] = [
            (threading.Lock(), {}) for _ in range(self.N_SHARDS)
        ]
        self._id_shards: list[tuple[threading.Lock, dict]] = [
            (threading.Lock(), {}) for _ in range(self.N_SHARDS)
        ]

    def _shard(self, metric: str, bucket_id: int) -> int:
        return hash((metric, bucket_id)) % self.N_SHARDS

    def increment(self, metric: str, value: int = 1) -> None:
        current_id = int(time.time() / self._bucket_width)
        shard_idx = self._shard(metric, current_id)
        lock, buckets = self._shards[shard_idx]
        _, ids = self._id_shards[shard_idx]

        with lock:
            if metric not in buckets:
                buckets[metric] = [0] * self._bucket_count
                ids[metric] = [-1] * self._bucket_count
            slot = current_id % self._bucket_count
            if ids[metric][slot] != current_id:
                buckets[metric][slot] = 0
                ids[metric][slot] = current_id
            buckets[metric][slot] += value

    def query_window(self, metric: str) -> int:
        current_id = int(time.time() / self._bucket_width)
        window_start = current_id - self._bucket_count + 1
        total = 0
        for i in range(window_start, current_id + 1):
            shard_idx = self._shard(metric, i)
            lock, buckets = self._shards[shard_idx]
            _, ids = self._id_shards[shard_idx]
            with lock:
                if metric in buckets:
                    slot = i % self._bucket_count
                    if ids[metric][slot] == i:
                        total += buckets[metric][slot]
        return total

    def reset(self, metric: str) -> None:
        for lock, buckets in self._shards:
            with lock:
                buckets.pop(metric, None)
        for lock, ids in self._id_shards:
            with lock:
                ids.pop(metric, None)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_escalation_backend(cfg_escalation) -> EscalationBackend:
    """
    Build the appropriate escalation backend from configuration.

    Switching guide:
    - Set cfg.escalation.backend = "redis" for production.
    - Set cfg.escalation.backend = "in_process" for single-host or CI.
    - Set cfg.escalation.backend = "lock_sharded" only if both above fail.
    """
    backend = cfg_escalation.backend
    if backend == "redis":
        return RedisEscalationBackend(
            redis_url=cfg_escalation.redis_url,
            window_seconds=cfg_escalation.window_seconds,
            bucket_count=cfg_escalation.bucket_count,
            bucket_ttl_s=cfg_escalation.bucket_ttl_s,
            error_threshold=cfg_escalation.error_threshold,
        )
    elif backend == "in_process":
        return InProcessEscalationBackend(
            window_seconds=cfg_escalation.window_seconds,
            bucket_count=cfg_escalation.bucket_count,
        )
    elif backend == "lock_sharded":
        logger.warning("Using lock-sharded escalation backend — high contention expected")
        return LockShardedEscalationBackend(
            window_seconds=cfg_escalation.window_seconds,
            bucket_count=cfg_escalation.bucket_count,
        )
    raise ValueError(f"Unknown escalation backend: {backend}")
