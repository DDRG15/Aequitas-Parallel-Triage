"""
aequitas/metrics.py
===================
Lightweight Prometheus-compatible metrics emitter with tracing stubs.

WHY PROMETHEUS-COMPATIBLE
--------------------------
Prometheus pull-based scraping is the de-facto standard for container
workloads.  We emit counters, gauges, and histograms using the
prometheus_client library (no heavy SDK required).

METRICS EMITTED
---------------
aequitas_tasks_total{status}           Counter   Tasks completed (ok/error/timeout)
aequitas_task_duration_seconds         Histogram Task wall-clock latency
aequitas_queue_depth                   Gauge     Dispatcher ingestion queue depth
aequitas_log_queue_depth               Gauge     Log writer queue depth
aequitas_rss_mb                        Gauge     Process RSS in MB
aequitas_gc_pause_seconds              Histogram GC pause duration
aequitas_docker_create_seconds         Histogram Container create+start latency
aequitas_error_rate_window             Gauge     Sliding-window error count
aequitas_escalation_threshold_breaches Counter   Times error_threshold exceeded

PUSH GATEWAY
------------
For batch workloads (no HTTP server), metrics are pushed to a Prometheus
Push Gateway on an interval timer.  See MetricsEmitter.start_push_loop().
"""

from __future__ import annotations

import gc
import logging
import threading
import time
from contextlib import contextmanager
from typing import Callable

try:
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        push_to_gateway,
    )
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

try:
    import psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metrics Emitter
# ---------------------------------------------------------------------------


class MetricsEmitter:
    """
    Central metrics registry and emission point.

    USAGE
    -----
        emitter = MetricsEmitter(cfg.metrics, labels={"env": "prod"})
        emitter.start_push_loop()

        with emitter.task_timer():
            result = worker.run_task(task)

        emitter.record_task(status="ok")
        emitter.update_queue_depth(dispatcher_q_size, log_q_size)
    """

    def __init__(self, cfg_metrics, labels: dict | None = None) -> None:
        self._cfg = cfg_metrics
        self._labels = labels or {}
        self._registry = CollectorRegistry() if _PROMETHEUS_AVAILABLE else None
        self._push_thread: threading.Thread | None = None
        self._stop_push = threading.Event()
        self._process = psutil.Process() if _PSUTIL_AVAILABLE else None

        if _PROMETHEUS_AVAILABLE and cfg_metrics.enabled:
            self._init_metrics()
        else:
            self._metrics: dict = {}
            if not _PROMETHEUS_AVAILABLE:
                logger.warning("prometheus_client not installed; metrics disabled")

    def _init_metrics(self) -> None:
        lnames = list(self._labels.keys())

        self._tasks_total = Counter(
            "aequitas_tasks_total",
            "Tasks completed by status",
            ["status"] + lnames,
            registry=self._registry,
        )
        self._task_duration = Histogram(
            "aequitas_task_duration_seconds",
            "Task wall-clock latency",
            lnames,
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
            registry=self._registry,
        )
        self._queue_depth = Gauge(
            "aequitas_queue_depth",
            "Dispatcher ingestion queue depth",
            lnames,
            registry=self._registry,
        )
        self._log_queue_depth = Gauge(
            "aequitas_log_queue_depth",
            "Log writer queue depth",
            lnames,
            registry=self._registry,
        )
        self._rss_mb = Gauge(
            "aequitas_rss_mb",
            "Process RSS memory in MB",
            lnames,
            registry=self._registry,
        )
        self._gc_pause = Histogram(
            "aequitas_gc_pause_seconds",
            "GC pause duration",
            lnames,
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5],
            registry=self._registry,
        )
        self._docker_create = Histogram(
            "aequitas_docker_create_seconds",
            "Container create+start latency",
            lnames,
            buckets=[0.05, 0.1, 0.2, 0.5, 1.0, 2.0],
            registry=self._registry,
        )
        self._error_rate = Gauge(
            "aequitas_error_rate_window",
            "Sliding-window error count",
            lnames,
            registry=self._registry,
        )
        self._threshold_breaches = Counter(
            "aequitas_escalation_threshold_breaches",
            "Times error threshold exceeded in window",
            lnames,
            registry=self._registry,
        )

    # ------------------------------------------------------------------
    # Recording methods
    # ------------------------------------------------------------------

    def record_task(self, status: str) -> None:
        """Increment task counter for the given status (ok/error/timeout)."""
        if not _PROMETHEUS_AVAILABLE or not self._cfg.enabled:
            return
        self._tasks_total.labels(status=status, **self._labels).inc()

    @contextmanager
    def task_timer(self):
        """Context manager: time a task and record the duration histogram."""
        if not _PROMETHEUS_AVAILABLE or not self._cfg.enabled:
            yield
            return
        start = time.monotonic()
        try:
            yield
        finally:
            elapsed = time.monotonic() - start
            self._task_duration.labels(**self._labels).observe(elapsed)

    def update_queue_depth(self, dispatcher_depth: int, log_depth: int) -> None:
        if not _PROMETHEUS_AVAILABLE or not self._cfg.enabled:
            return
        self._queue_depth.labels(**self._labels).set(dispatcher_depth)
        self._log_queue_depth.labels(**self._labels).set(log_depth)

    def update_rss(self) -> float:
        """Sample current RSS and update gauge. Returns RSS in MB."""
        if not _PSUTIL_AVAILABLE:
            return 0.0
        rss_mb = self._process.memory_info().rss / (1024 * 1024)
        if _PROMETHEUS_AVAILABLE and self._cfg.enabled:
            self._rss_mb.labels(**self._labels).set(rss_mb)
        return rss_mb

    def record_gc_pause(self, duration_s: float) -> None:
        if not _PROMETHEUS_AVAILABLE or not self._cfg.enabled:
            return
        self._gc_pause.labels(**self._labels).observe(duration_s)

    def record_docker_create(self, duration_s: float) -> None:
        if not _PROMETHEUS_AVAILABLE or not self._cfg.enabled:
            return
        self._docker_create.labels(**self._labels).observe(duration_s)

    def update_error_rate(self, window_count: int) -> None:
        if not _PROMETHEUS_AVAILABLE or not self._cfg.enabled:
            return
        self._error_rate.labels(**self._labels).set(window_count)

    def record_threshold_breach(self) -> None:
        if not _PROMETHEUS_AVAILABLE or not self._cfg.enabled:
            return
        self._threshold_breaches.labels(**self._labels).inc()

    # ------------------------------------------------------------------
    # Memory / GC helpers
    # ------------------------------------------------------------------

    def maybe_collect_gc(self, memory_high_water_mb: int) -> bool:
        """
        Conditionally run gc.collect() ONLY if RSS exceeds the high-water mark.

        WHY NOT CALL gc.collect() AFTER EVERY BATCH
        ---------------------------------------------
        gc.collect() triggers a stop-the-world pause for all threads.  At
        10M tasks this accumulates to minutes of wasted time.  RSS-based
        triggering runs GC at most once per ~100 MB growth.

        Returns True if GC was triggered.
        """
        rss_mb = self.update_rss()
        if rss_mb > memory_high_water_mb:
            logger.info("RSS %.1f MB exceeds HWM %d MB — triggering GC", rss_mb, memory_high_water_mb)
            t0 = time.monotonic()
            gc.collect()
            pause = time.monotonic() - t0
            self.record_gc_pause(pause)
            logger.info("GC complete pause=%.3fs rss_after=%.1f MB", pause, self.update_rss())
            return True
        return False

    # ------------------------------------------------------------------
    # Push loop
    # ------------------------------------------------------------------

    def start_push_loop(self) -> None:
        """Start a background thread that pushes metrics to Push Gateway."""
        if not _PROMETHEUS_AVAILABLE or not self._cfg.enabled:
            return
        self._push_thread = threading.Thread(
            target=self._push_loop,
            name="aequitas-metrics-push",
            daemon=True,
        )
        self._push_thread.start()
        logger.info("Metrics push loop started gateway=%s interval=%ds",
                    self._cfg.push_gateway, self._cfg.push_interval_s)

    def _push_loop(self) -> None:
        while not self._stop_push.wait(timeout=self._cfg.push_interval_s):
            try:
                push_to_gateway(
                    self._cfg.push_gateway,
                    job="aequitas",
                    registry=self._registry,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Metrics push failed: %s", exc)

    def stop_push_loop(self) -> None:
        self._stop_push.set()
        if self._push_thread:
            self._push_thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Tracing stubs
    # ------------------------------------------------------------------

    @contextmanager
    def trace_span(self, name: str, attributes: dict | None = None):
        """
        OpenTelemetry-compatible tracing stub.

        TODO: replace with opentelemetry-sdk span when tracing is enabled.
        Currently a no-op context manager so workers can instrument without
        coupling to a specific tracing backend.
        """
        # TODO: create OTEL span here
        yield
        # TODO: end span here
