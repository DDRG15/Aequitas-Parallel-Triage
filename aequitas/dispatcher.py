"""
aequitas/dispatcher.py
======================
Non-blocking, token-bucket dispatcher with bounded ingestion.

WHY TOKEN BUCKET
----------------
A token bucket limits the *average* rate while allowing short bursts.  At
10M tasks we cannot let ingestion run ahead of workers: an unbounded queue
would exhaust heap memory.  The bucket refills at `rate` tokens/s and holds
at most `burst` tokens.  When the bucket is empty the caller either blocks
(blocking=True) or receives BackpressureError immediately.

WHY BOUNDED QUEUE
-----------------
concurrent.futures.ThreadPoolExecutor / ProcessPoolExecutor internal queues
are unbounded by default.  We wrap submission in a semaphore-gated method
so the number of *in-flight* tasks is always ≤ max_workers + ingestion_queue_maxsize.

USAGE
-----
    dispatcher = Dispatcher(cfg)
    dispatcher.start()
    for result in dispatcher.submit_stream(task_generator()):
        process(result)
    dispatcher.shutdown()
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Generator, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, Callable

from .config import EngineConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class BackpressureError(Exception):
    """Raised when ingestion queue is full and caller must slow down."""


# ---------------------------------------------------------------------------
# Token bucket
# ---------------------------------------------------------------------------


class TokenBucket:
    """
    Thread-safe token bucket for rate limiting.

    WHY NOT time.sleep() IN THE HOT PATH
    ------------------------------------
    We record *debt* instead of sleeping.  The caller checks `consume()` and
    decides whether to back off.  This keeps the dispatcher thread alive for
    bookkeeping while sleeping elsewhere.
    """

    def __init__(self, rate: float, burst: int) -> None:
        self._rate = rate          # tokens added per second
        self._burst = burst        # maximum token capacity
        self._tokens: float = burst
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def consume(self, n: int = 1) -> bool:
        """
        Attempt to consume `n` tokens.

        Returns True if tokens were available, False if backpressure applies.
        Thread-safe.
        """
        with self._lock:
            self._refill()
            if self._tokens >= n:
                self._tokens -= n
                return True
            return False

    def _refill(self) -> None:
        """Add tokens proportional to elapsed time (called under lock)."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        self._last_refill = now

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class Dispatcher:
    """
    Non-blocking dispatcher: accepts a stream of tasks, rate-limits with a
    token bucket, submits to a bounded worker pool, and yields results via
    concurrent.futures.as_completed().

    LIFECYCLE
    ---------
    1. dispatcher.start()                  — allocate executor
    2. for result in dispatcher.submit_stream(gen):  — ingest + consume
    3. dispatcher.shutdown(wait=True)      — drain + clean up

    MEMORY CONTRACT
    ---------------
    Future objects are dropped *immediately* after their result is yielded
    from as_completed().  The caller must not hold references to yielded
    futures beyond the loop body.
    """

    def __init__(self, cfg: EngineConfig) -> None:
        self._cfg = cfg
        self._bucket = TokenBucket(
            rate=cfg.dispatcher.token_bucket_rate,
            burst=cfg.dispatcher.token_bucket_burst,
        )
        # Semaphore bounds the number of live futures (in-flight tasks)
        self._semaphore = threading.Semaphore(cfg.dispatcher.ingestion_queue_maxsize)
        self._executor: ThreadPoolExecutor | None = None
        self._shutdown_event = threading.Event()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Allocate the thread-pool executor."""
        self._executor = ThreadPoolExecutor(
            max_workers=self._cfg.engine.max_workers,
            thread_name_prefix="aequitas-worker",
        )
        logger.info(
            "Dispatcher started max_workers=%d bucket_rate=%.0f burst=%d",
            self._cfg.engine.max_workers,
            self._cfg.dispatcher.token_bucket_rate,
            self._cfg.dispatcher.token_bucket_burst,
        )

    def shutdown(self, wait: bool = True) -> None:
        """Drain in-flight tasks and release resources."""
        self._shutdown_event.set()
        if self._executor:
            self._executor.shutdown(wait=wait)
        logger.info("Dispatcher shut down wait=%s", wait)

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    def submit_stream(
        self,
        tasks: Iterable[tuple[Callable, tuple, dict]],
        *,
        blocking_backpressure: bool = True,
        backpressure_sleep_s: float = 0.01,
    ) -> Generator[tuple[Any, Exception | None], None, None]:
        """
        Consume a generator of (fn, args, kwargs) tuples, submit to the pool,
        and yield (result, error) pairs as tasks complete.

        WHY GENERATOR-BASED INGESTION
        ------------------------------
        The task source may be infinite or very large (10M records from a DB
        cursor).  Pulling lazily means only `batch_size` tasks are in-flight
        at any time, bounding memory regardless of total dataset size.

        WHY as_completed() IMMEDIATELY DROPS REFERENCES
        ------------------------------------------------
        We collect futures into a list of at most `batch_size` and pass the
        *entire list* to as_completed().  As each future completes we yield its
        result and the list element falls out of scope — Python GC can reclaim
        the Future and its associated task state.
        """
        assert self._executor is not None, "Call start() before submit_stream()"

        batch_size = self._cfg.engine.batch_size
        pending: list[Future] = []

        task_iter = iter(tasks)
        exhausted = False

        while not exhausted or pending:
            # ---- Fill batch ------------------------------------------------
            while not exhausted and len(pending) < batch_size:
                try:
                    fn, args, kwargs = next(task_iter)
                except StopIteration:
                    exhausted = True
                    break

                # Token-bucket: wait for a token
                while not self._bucket.consume():
                    if blocking_backpressure:
                        time.sleep(backpressure_sleep_s)
                    else:
                        raise BackpressureError("Token bucket empty; slow down ingestion")

                # Semaphore: bound in-flight futures
                acquired = self._semaphore.acquire(
                    blocking=blocking_backpressure,
                    timeout=backpressure_sleep_s * 10 if blocking_backpressure else 0,
                )
                if not acquired:
                    raise BackpressureError("In-flight limit reached; semaphore not acquired")

                future = self._executor.submit(self._run_task, fn, args, kwargs)
                future.add_done_callback(lambda _f: self._semaphore.release())
                pending.append(future)

            # ---- Drain completed futures -----------------------------------
            if pending:
                done_iter = as_completed(pending, timeout=1.0)
                still_pending: list[Future] = []
                completed_count = 0

                for future in done_iter:
                    # Extract result immediately; drop future reference
                    exc = future.exception()
                    result = None if exc else future.result()
                    yield result, exc
                    completed_count += 1

                # Rebuild pending list: futures not yet completed
                # NOTE: as_completed() does not drain the list; rebuild it.
                still_pending = [f for f in pending if not f.done()]
                pending = still_pending

                if completed_count == 0:
                    # as_completed timed out — short sleep to avoid busy-wait
                    time.sleep(0.005)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _run_task(fn: Callable, args: tuple, kwargs: dict) -> Any:
        """
        Trampoline executed inside the thread pool.

        WHY A TRAMPOLINE
        ----------------
        Keeps Future-creation logic in the dispatcher while allowing workers
        to be plain callables — no coupling to Future internals.
        """
        # TODO: integrate WorkerInterface.run_task() here
        return fn(*args, **kwargs)
