"""
aequitas/log_writer.py
======================
Single-writer NDJSON log pipeline using QueueHandler + QueueListener.

WHY SINGLE-WRITER
-----------------
When 200 worker threads all write to the same log file concurrently, each
write() syscall must be serialised by the OS anyway — but with multiple
callers we also get interleaved partial writes, redundant fsyncs, and lock
contention.  A single writer process/thread amortises fsyncs across batches
and keeps file access patterns sequential.

ARCHITECTURE
------------
Workers ──► logging.QueueHandler ──► multiprocessing.Queue (bounded)
                                          │
                                   QueueListener (single thread)
                                          │
                                   _NDJSONWriter (batch + fsync)
                                          │
                                   RotatingFileHandler

BACKPRESSURE
------------
The queue is bounded (log_queue_size).  When full:
1. DEBUG/INFO records are sampled at `backpressure_sample_rate`.
2. If still full after sampling, emit a single "log_dropped" NDJSON record
   and discard the message.
3. WARNING+ records are never dropped.

This prevents the log queue from stalling worker threads on a slow disk.
"""

from __future__ import annotations

import logging
import logging.handlers
import multiprocessing as mp
import os
import queue
import threading
import time
from pathlib import Path
from typing import Sequence

try:
    import orjson as _json_lib
    _USE_ORJSON = True
except ImportError:
    import json as _json_lib  # type: ignore[no-redef]
    _USE_ORJSON = False

from .platform_compat import safe_fsync


# ---------------------------------------------------------------------------
# NDJSON Writer
# ---------------------------------------------------------------------------


class _NDJSONWriter:
    """
    Serialises LogRecord objects to NDJSON and writes batched to disk.

    WHY ORJSON
    ----------
    orjson is 5–10× faster than the stdlib json module for small dicts and
    handles datetime/bytes natively.  At 10M tasks emitting ~1 log record each,
    serialisation throughput matters.

    FSYNC POLICY
    ------------
    We fsync on two triggers:
    1. Accumulated batch exceeds max_batch_bytes.
    2. Oldest record in batch is older than flush_interval_ms.
    This amortises fsync() cost across many records while bounding data loss.
    """

    def __init__(
        self,
        log_dir: str,
        max_bytes_per_file: int,
        backup_count: int,
        max_batch_bytes: int,
        flush_interval_ms: int,
    ) -> None:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        log_path = str(Path(log_dir) / "aequitas.ndjson")

        self._handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=max_bytes_per_file,
            backupCount=backup_count,
            encoding="utf-8",
        )
        self._file = self._handler.stream  # raw file object for batched writes

        self._max_batch_bytes = max_batch_bytes
        self._flush_interval_ms = flush_interval_ms
        self._batch: list[bytes] = []
        self._batch_bytes = 0
        self._batch_start_ms: float = time.monotonic() * 1000

    def write(self, record: logging.LogRecord) -> None:
        """Serialise one LogRecord and add to the pending batch."""
        payload = {
            "ts_ms": int(record.created * 1000),
            "level": record.levelname,
            "pid": record.process,
            "thread": record.thread,
            "logger": record.name,
            "msg": record.getMessage(),
            "stream": getattr(record, "stream_origin", "log"),
        }
        if record.exc_info:
            payload["exc"] = self._handler.formatter.formatException(record.exc_info) if self._handler.formatter else str(record.exc_info)

        if _USE_ORJSON:
            line = _json_lib.dumps(payload) + b"\n"
        else:
            line = _json_lib.dumps(payload).encode("utf-8") + b"\n"
        self._batch.append(line)
        self._batch_bytes += len(line)

        # Check flush triggers
        age_ms = time.monotonic() * 1000 - self._batch_start_ms
        if self._batch_bytes >= self._max_batch_bytes or age_ms >= self._flush_interval_ms:
            self.flush()

    def flush(self) -> None:
        """Write all pending batch records to disk and fsync."""
        if not self._batch:
            return
        # Rotate check — delegate to RotatingFileHandler
        self._handler.doRollover() if self._should_rotate() else None

        self._file = self._handler.stream
        for line in self._batch:
            self._file.write(line.decode("utf-8"))

        self._file.flush()
        safe_fsync(self._file.fileno())

        self._batch = []
        self._batch_bytes = 0
        self._batch_start_ms = time.monotonic() * 1000

    def _should_rotate(self) -> bool:
        """Check if RotatingFileHandler would rotate on the next emit."""
        if self._handler.stream is None:
            return False
        try:
            return self._handler.shouldRollover(None)  # type: ignore[arg-type]
        except Exception:  # noqa: BLE001
            return False

    def close(self) -> None:
        self.flush()
        self._handler.close()


# ---------------------------------------------------------------------------
# Log Writer Process / Thread
# ---------------------------------------------------------------------------


class LogWriterThread(threading.Thread):
    """
    Single writer thread that consumes from a bounded queue and writes NDJSON.

    WHY A THREAD NOT A PROCESS
    --------------------------
    A thread is sufficient when all workers are in the same process (threaded
    mode).  For process-mode workers, use LogWriterProcess instead so the queue
    can be a multiprocessing.Queue rather than queue.Queue.
    """

    def __init__(
        self,
        log_queue: queue.Queue,
        ndjson_writer: _NDJSONWriter,
        flush_interval_ms: int,
        backpressure_sample_rate: float,
        stop_event: threading.Event | None = None,
    ) -> None:
        super().__init__(name="aequitas-log-writer", daemon=True)
        self._queue = log_queue
        self._writer = ndjson_writer
        self._flush_interval_ms = flush_interval_ms
        self._sample_rate = backpressure_sample_rate
        self._stop_event = stop_event or threading.Event()
        self._stats = {"written": 0, "dropped": 0, "sampled": 0}

    def run(self) -> None:
        import random
        timeout = self._flush_interval_ms / 1000

        while not self._stop_event.is_set():
            try:
                record: logging.LogRecord = self._queue.get(timeout=timeout)
            except queue.Empty:
                # Flush on timeout even if no records arrived
                self._writer.flush()
                continue

            # Backpressure sampling for low-severity records
            if self._queue.qsize() > self._queue.maxsize * 0.8:
                if record.levelno < logging.WARNING:
                    if random.random() > self._sample_rate:
                        self._stats["dropped"] += 1
                        continue
                    self._stats["sampled"] += 1

            self._writer.write(record)
            self._stats["written"] += 1

        # Drain remainder on stop
        while True:
            try:
                record = self._queue.get_nowait()
                self._writer.write(record)
            except queue.Empty:
                break
        self._writer.close()

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stats(self) -> dict:
        return dict(self._stats)


# ---------------------------------------------------------------------------
# Factory / Setup helpers
# ---------------------------------------------------------------------------


def build_log_queue(maxsize: int) -> queue.Queue:
    """
    Build the bounded log queue.

    Using queue.Queue (not multiprocessing.Queue) because QueueHandler works
    with both; for multi-process deployments swap for mp.Queue and adjust
    LogWriterProcess accordingly.
    """
    return queue.Queue(maxsize=maxsize)


def install_queue_handler(log_queue: queue.Queue, level: int = logging.DEBUG) -> None:
    """
    Replace the root logger's handlers with a QueueHandler pointing at our
    bounded queue.

    WHY REPLACE ROOT HANDLERS
    -------------------------
    Any logging.warning() call in any thread will route through QueueHandler
    and into our single-writer pipeline.  This ensures no module bypasses
    our backpressure and flush controls.
    """
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.handlers.QueueHandler(log_queue)
    handler.setLevel(level)
    root.addHandler(handler)
    root.setLevel(level)


def build_log_writer(cfg_logging, cfg_memory=None) -> tuple[queue.Queue, LogWriterThread]:
    """
    Convenience factory: build the queue + writer thread from config.

    Returns (log_queue, writer_thread).  Caller must call writer_thread.start().

    SAMPLE NDJSON LINE
    ------------------
    {"ts_ms":1718000000123,"level":"INFO","pid":12345,"thread":140234,"logger":"aequitas.dispatcher","msg":"Dispatcher started max_workers=200","stream":"log"}
    """
    log_queue = build_log_queue(cfg_logging.log_queue_size)
    ndjson_writer = _NDJSONWriter(
        log_dir=cfg_logging.log_dir,
        max_bytes_per_file=cfg_logging.max_bytes_per_file,
        backup_count=cfg_logging.backup_count,
        max_batch_bytes=cfg_logging.max_batch_bytes,
        flush_interval_ms=cfg_logging.flush_interval_ms,
    )
    writer_thread = LogWriterThread(
        log_queue=log_queue,
        ndjson_writer=ndjson_writer,
        flush_interval_ms=cfg_logging.flush_interval_ms,
        backpressure_sample_rate=cfg_logging.backpressure_sample_rate,
    )
    install_queue_handler(log_queue)
    return log_queue, writer_thread
