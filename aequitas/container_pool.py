"""
aequitas/container_pool.py
==========================
Async controller for pooled Docker containers with overlay reset.

WHY POOLED CONTAINERS
---------------------
Creating a fresh Docker container per task is too slow (100–500 ms per
docker create/start).  A warm pool of pre-started containers cuts task
startup to < 10 ms (just overlay reset).  The pool is managed by a
dedicated async controller so worker threads never block on Docker SDK calls.

OVERLAY RESET
-------------
Between tasks we reset the container's writable overlay layer by running
`docker exec <id> sh -c 'find /task -delete'` (or a configurable reset
command).  This is faster than stop+start and preserves the warm process
inside the container.

LIFECYCLE (SEPARATE CONTROLLER)
---------------------------------
The async controller is the ONLY place that calls the Docker SDK.  Workers
communicate via asyncio.Queue (checkout_queue / checkin_queue).  Worker
threads use run_coroutine_threadsafe() to submit requests to the controller's
event loop.

MODES
-----
- process: no containerisation; subprocess runs in host process namespace
- pooled:  warm pool, overlay reset between tasks (DEFAULT)
- ephemeral: create/destroy per task (slow; use for isolation-critical tasks)
- microvm:  TODO stub for Firecracker/gVisor integration
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ContainerHandle:
    """
    Opaque handle to a checked-out container.

    Workers receive this from checkout() and return it via checkin().
    Never store ContainerHandle beyond the scope of a single task.
    """
    container_id: str
    lease_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    checked_out_at: float = field(default_factory=time.monotonic)
    _overlay_reset_done: bool = False

    def mark_overlay_reset(self) -> None:
        self._overlay_reset_done = True


@dataclass
class PoolStats:
    total: int = 0
    available: int = 0
    checked_out: int = 0
    overlay_resets: int = 0
    create_errors: int = 0
    checkout_timeouts: int = 0


# ---------------------------------------------------------------------------
# Container Pool Controller (async)
# ---------------------------------------------------------------------------


class ContainerPoolController:
    """
    Async controller for container lifecycle.

    HOW IT WORKS
    ------------
    1. On startup: pre-warm `pool_size` containers (pull image, create, start).
    2. checkout(): pull from available queue; if empty, wait up to timeout.
    3. checkin(handle): reset overlay, then return to available queue.
    4. On shutdown: stop + remove all managed containers.

    WHY ASYNC
    ---------
    Docker SDK calls (create, start, exec) are I/O-bound.  asyncio allows
    managing 20+ containers concurrently without spawning 20 threads.

    WORKER INTERFACE
    ----------------
    Worker threads are NOT async.  They call checkout_sync() / checkin_sync()
    which submit coroutines to the controller's event loop via
    asyncio.run_coroutine_threadsafe().
    """

    def __init__(
        self,
        image: str,
        pool_size: int,
        overlay_reset: bool,
        checkout_timeout_s: int,
        docker_create_timeout_ms: int,
        mode: str = "pooled",
    ) -> None:
        self._image = image
        self._pool_size = pool_size
        self._overlay_reset = overlay_reset
        self._checkout_timeout = checkout_timeout_s
        self._docker_timeout_ms = docker_create_timeout_ms
        self._mode = mode

        self._loop: asyncio.AbstractEventLoop | None = None
        self._available: asyncio.Queue[ContainerHandle] = asyncio.Queue()
        self._stats = PoolStats()
        self._running = False

        # TODO: initialise docker client (e.g. aiodocker or run_in_executor wrapping docker-py)
        self._docker = None  # placeholder

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Pre-warm all containers in the pool."""
        self._loop = asyncio.get_running_loop()
        self._running = True
        logger.info("ContainerPool starting mode=%s pool_size=%d image=%s",
                    self._mode, self._pool_size, self._image)

        if self._mode == "process":
            # No containers; pool is no-op
            return

        tasks = [self._create_and_warm() for _ in range(self._pool_size)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.error("Container warmup failed: %s", r)
                self._stats.create_errors += 1
            else:
                await self._available.put(r)
                self._stats.total += 1
                self._stats.available += 1

        logger.info("ContainerPool ready available=%d errors=%d",
                    self._stats.available, self._stats.create_errors)

    async def stop(self) -> None:
        """Drain pool and remove all containers."""
        self._running = False
        containers = []
        while not self._available.empty():
            try:
                handle = self._available.get_nowait()
                containers.append(handle)
            except asyncio.QueueEmpty:
                break
        # TODO: call docker stop + rm for each handle
        for handle in containers:
            logger.debug("Removing container %s", handle.container_id)
            # await self._docker.containers.get(handle.container_id).delete(force=True)
        logger.info("ContainerPool stopped removed=%d", len(containers))

    # ------------------------------------------------------------------
    # Checkout / Checkin
    # ------------------------------------------------------------------

    async def checkout(self) -> ContainerHandle:
        """
        Acquire a container from the pool.

        Raises asyncio.TimeoutError if no container available within timeout.
        """
        if self._mode == "process":
            # Return a dummy handle; no real container
            return ContainerHandle(container_id="host-process")

        if self._mode == "ephemeral":
            return await self._create_ephemeral()

        try:
            handle = await asyncio.wait_for(
                self._available.get(),
                timeout=self._checkout_timeout,
            )
            self._stats.available -= 1
            self._stats.checked_out += 1
            return handle
        except asyncio.TimeoutError:
            self._stats.checkout_timeouts += 1
            raise

    async def checkin(self, handle: ContainerHandle) -> None:
        """
        Return a container to the pool after resetting its overlay.

        WHY OVERLAY RESET BEFORE CHECKIN
        ---------------------------------
        We reset overlay BEFORE re-queuing so the next checkout() always
        receives a clean filesystem.  If reset fails, discard the container
        and create a replacement asynchronously.
        """
        if self._mode == "process":
            return
        if self._mode == "ephemeral":
            await self._destroy_container(handle)
            return

        try:
            if self._overlay_reset:
                await self._reset_overlay(handle)
                handle.mark_overlay_reset()
                self._stats.overlay_resets += 1
            await self._available.put(handle)
            self._stats.checked_out -= 1
            self._stats.available += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("Overlay reset failed for %s: %s; replacing container",
                           handle.container_id, exc)
            asyncio.create_task(self._replace_container())

    # ------------------------------------------------------------------
    # Thread-safe wrappers (called from non-async worker threads)
    # ------------------------------------------------------------------

    def checkout_sync(self) -> ContainerHandle:
        """
        Blocking wrapper for worker threads.

        Uses run_coroutine_threadsafe to submit to the controller's event loop.
        """
        assert self._loop is not None, "Controller not started"
        fut = asyncio.run_coroutine_threadsafe(self.checkout(), self._loop)
        return fut.result(timeout=self._checkout_timeout + 1)

    def checkin_sync(self, handle: ContainerHandle) -> None:
        """Blocking wrapper for worker threads."""
        assert self._loop is not None, "Controller not started"
        fut = asyncio.run_coroutine_threadsafe(self.checkin(handle), self._loop)
        fut.result(timeout=10)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _create_and_warm(self) -> ContainerHandle:
        """
        TODO: Create and start a Docker container.

        Sketch using docker-py via run_in_executor:
            loop = asyncio.get_running_loop()
            container = await loop.run_in_executor(
                None,
                lambda: self._docker.containers.run(
                    self._image,
                    detach=True,
                    command="sleep infinity",
                    ...
                )
            )
            return ContainerHandle(container_id=container.id)
        """
        raise NotImplementedError("TODO: implement Docker container creation")

    async def _reset_overlay(self, handle: ContainerHandle) -> None:
        """
        TODO: Reset the container's writable layer.

        Sketch:
            await loop.run_in_executor(
                None,
                lambda: self._docker.containers.get(handle.container_id)
                    .exec_run("sh -c 'rm -rf /task/* /tmp/*'")
            )
        """
        raise NotImplementedError("TODO: implement overlay reset")

    async def _create_ephemeral(self) -> ContainerHandle:
        """TODO: Create a single-use ephemeral container."""
        raise NotImplementedError("TODO: implement ephemeral container creation")

    async def _destroy_container(self, handle: ContainerHandle) -> None:
        """TODO: Stop and remove a container."""
        raise NotImplementedError("TODO: implement container destruction")

    async def _replace_container(self) -> None:
        """Background task: create a replacement container after a failed reset."""
        try:
            handle = await self._create_and_warm()
            await self._available.put(handle)
            self._stats.total += 1
            self._stats.available += 1
            logger.info("Replacement container ready: %s", handle.container_id)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to create replacement container: %s", exc)
            self._stats.create_errors += 1

    @property
    def stats(self) -> PoolStats:
        return self._stats
