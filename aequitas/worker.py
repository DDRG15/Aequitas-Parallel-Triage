"""
aequitas/worker.py
==================
Pluggable worker interface with threaded, process, and async-hybrid implementations.

WHY A COMMON INTERFACE
----------------------
All three execution backends (threads, processes, asyncio) expose identical
lifecycle hooks (setup / run_task / teardown).  The dispatcher calls only the
interface, not the concrete class — this allows runtime backend switching
based on task profile without changing dispatcher code.

EXECUTOR AUTO-SELECTION
------------------------
- Threaded: I/O-bound tasks (subprocess + network).  GIL is released during
  syscalls so threads are effective.  Lower overhead than processes.
- Process: CPU-bound tasks (parsing, crypto, heavy computation).  Bypasses GIL.
- Async-hybrid: Mixed I/O and CPU; an asyncio event loop drives I/O while a
  process pool handles CPU bursts.

LIFECYCLE HOOKS
---------------
setup()    — called once per worker thread/process on start
run_task() — called for every task; must be idempotent
teardown() — called once on graceful shutdown
"""

from __future__ import annotations

import abc
import asyncio
import logging
import multiprocessing as mp
import os
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Callable

from .config import EngineConfig
from .platform_compat import best_mp_context

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class WorkerInterface(abc.ABC):
    """
    Abstract worker.  Concrete subclasses implement setup/run_task/teardown
    and optionally override executor_factory() to provide a custom pool.
    """

    def __init__(self, cfg: EngineConfig) -> None:
        self._cfg = cfg

    @abc.abstractmethod
    def setup(self) -> None:
        """
        One-time initialisation per worker process/thread.

        Examples: pre-warm container checkout, open DB connections, load models.
        Must be idempotent (called exactly once per worker lifetime).
        """

    @abc.abstractmethod
    def run_task(self, task: dict) -> dict:
        """
        Execute a single task dict and return a result dict.

        Contract:
        - Must not raise unhandled exceptions (catch and return error payload).
        - Must not block indefinitely (SubprocessWrapper enforces timeouts).
        - Must not write directly to log files (use logging module only).
        - Must not call gc.collect().

        TODO: implement per-task logic here.
        """

    @abc.abstractmethod
    def teardown(self) -> None:
        """
        Cleanup on graceful shutdown.  Release resources acquired in setup().
        """

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _make_result(
        self,
        task_id: str,
        status: str,
        payload: Any = None,
        error: str | None = None,
    ) -> dict:
        """Standardised result envelope."""
        return {
            "task_id": task_id,
            "status": status,       # "ok" | "error" | "timeout"
            "pid": os.getpid(),
            "payload": payload,
            "error": error,
        }


# ---------------------------------------------------------------------------
# Threaded worker
# ---------------------------------------------------------------------------


class ThreadedWorker(WorkerInterface):
    """
    Worker suitable for I/O-bound tasks.

    Each thread calls setup() on first use via a threading.local flag.
    Threads share the same process heap — keep per-task state in local vars.

    WHY THREADS FOR I/O
    -------------------
    CPython releases the GIL during blocking syscalls (Popen, socket, file I/O).
    A 200-thread pool can have 200 subprocesses running concurrently without
    multiprocessing overhead (~1 ms process fork vs ~1 µs thread create).
    """

    _local = threading.local()

    def setup(self) -> None:
        if getattr(self._local, "initialised", False):
            return
        # TODO: per-thread initialisation (e.g. pre-warm container checkout)
        logger.debug("ThreadedWorker setup thread=%s pid=%d", threading.current_thread().name, os.getpid())
        self._local.initialised = True

    def run_task(self, task: dict) -> dict:
        self.setup()
        task_id = task.get("id", "unknown")
        try:
            # TODO: call SubprocessWrapper here
            result_payload = self._execute(task)
            return self._make_result(task_id, "ok", result_payload)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Task %s failed: %s", task_id, exc)
            return self._make_result(task_id, "error", error=str(exc))

    def teardown(self) -> None:
        # TODO: release thread-local resources
        logger.debug("ThreadedWorker teardown thread=%s", threading.current_thread().name)

    def _execute(self, task: dict) -> Any:
        """TODO: invoke SubprocessWrapper.run() with task parameters."""
        raise NotImplementedError("TODO: integrate SubprocessWrapper")


# ---------------------------------------------------------------------------
# Process worker
# ---------------------------------------------------------------------------

def _process_worker_init(cfg_dict: dict) -> None:
    """
    Initialiser called once in each worker process by ProcessPoolExecutor.

    WHY A MODULE-LEVEL FUNCTION
    ---------------------------
    multiprocessing requires picklable initialisers.  A module-level function
    with a plain dict argument avoids pickling the entire EngineConfig object.
    """
    import logging
    logging.basicConfig(level=logging.WARNING)  # workers log via QueueHandler
    # TODO: install QueueHandler using cfg_dict["log_queue_address"]
    # TODO: per-process setup (memory limits, signal handlers, etc.)
    logger.debug("ProcessWorker init pid=%d", os.getpid())


class ProcessWorker(WorkerInterface):
    """
    Worker using a ProcessPoolExecutor for CPU-bound tasks.

    WHY PROCESS ISOLATION
    ----------------------
    Subprocesses executing untrusted commands are isolated by the OS.  A
    crash in a worker process does not corrupt shared heap.  The GIL does not
    limit CPU parallelism.

    NOTE: ProcessPoolExecutor futures are submitted from the main process.
    The actual task execution happens inside worker processes via the
    _process_worker_init + a module-level task function.
    """

    def setup(self) -> None:
        # Called in the *main* process before pool creation
        logger.info("ProcessWorker pool setup max_workers=%d", self._cfg.engine.max_workers)

    def run_task(self, task: dict) -> dict:
        # This is called inside a worker process (via ProcessPoolExecutor)
        task_id = task.get("id", "unknown")
        try:
            result_payload = self._execute(task)
            return self._make_result(task_id, "ok", result_payload)
        except Exception as exc:  # noqa: BLE001
            return self._make_result(task_id, "error", error=str(exc))

    def teardown(self) -> None:
        logger.info("ProcessWorker teardown pid=%d", os.getpid())

    def _execute(self, task: dict) -> Any:
        """TODO: invoke SubprocessWrapper.run() with task parameters."""
        raise NotImplementedError("TODO: integrate SubprocessWrapper")

    def make_executor(self) -> ProcessPoolExecutor:
        """
        Factory for the process pool.

        Uses best_mp_context() from platform_compat:
          - POSIX: "forkserver" avoids lock corruption from bare fork() in
            multi-threaded processes.
          - Windows: "spawn" is the only available context.
        """
        return ProcessPoolExecutor(
            max_workers=self._cfg.engine.max_workers,
            mp_context=best_mp_context(),
            initializer=_process_worker_init,
            initargs=({"log_queue_size": self._cfg.logging.log_queue_size},),
        )


# ---------------------------------------------------------------------------
# Async-hybrid worker
# ---------------------------------------------------------------------------


class AsyncHybridWorker(WorkerInterface):
    """
    Async-hybrid worker: asyncio event loop for I/O, process pool for CPU.

    WHY HYBRID
    ----------
    Some tasks are I/O-bound (container API, file streaming) and others are
    CPU-bound (log parsing, hash computation).  Forcing all tasks through
    threads wastes CPU; forcing all through processes wastes IPC overhead.
    The hybrid approach dispatches per-task type.

    IMPLEMENTATION SKETCH
    ---------------------
    - asyncio.run() drives the top-level coroutine.
    - asyncio.get_event_loop().run_in_executor(process_pool, cpu_fn) offloads
      CPU work without blocking the event loop.
    - asyncio.create_subprocess_exec() for subprocesses (no Popen needed).

    TODO: full implementation requires asyncio.Semaphore for rate limiting,
    asyncio.Queue as the ingestion channel, and structured task cancellation.
    """

    def setup(self) -> None:
        logger.info("AsyncHybridWorker setup")

    def run_task(self, task: dict) -> dict:
        task_id = task.get("id", "unknown")
        try:
            result = asyncio.run(self._async_run(task))
            return self._make_result(task_id, "ok", result)
        except Exception as exc:  # noqa: BLE001
            return self._make_result(task_id, "error", error=str(exc))

    def teardown(self) -> None:
        logger.info("AsyncHybridWorker teardown")

    async def _async_run(self, task: dict) -> Any:
        """
        TODO: implement coroutine-based task execution.

        Sketch:
            proc = await asyncio.create_subprocess_exec(
                *task["cmd"],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=cfg.task_timeout_s)
            return {"stdout": stdout, "stderr": stderr, "returncode": proc.returncode}
        """
        raise NotImplementedError("TODO: implement async subprocess execution")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_worker(cfg: EngineConfig, task_profile: str = "io") -> WorkerInterface:
    """
    Instantiate the correct worker type based on resolved executor mode.

    This is the single construction point — callers never import concrete
    worker classes directly.
    """
    mode = cfg.resolve_executor_mode(task_profile)
    mapping: dict[str, type[WorkerInterface]] = {
        "threaded": ThreadedWorker,
        "process": ProcessWorker,
        "async-hybrid": AsyncHybridWorker,
    }
    worker_cls = mapping.get(mode, ThreadedWorker)
    logger.info("Building worker type=%s for profile=%s", worker_cls.__name__, task_profile)
    return worker_cls(cfg)
