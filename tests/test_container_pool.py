"""
tests/test_container_pool.py
=============================
Container pool checkout/checkin correctness and stress tests.

Most tests use mocked Docker SDK calls.  Full integration tests are marked
@pytest.mark.docker and require a running Docker daemon.

Run:
    pytest tests/test_container_pool.py -v -m "not docker"

Reviewer verification:
    pytest tests/test_container_pool.py::TestPoolSemantics -v
    pytest tests/test_container_pool.py::TestCheckoutTimeout -v
"""

from __future__ import annotations

import asyncio
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aequitas.container_pool import ContainerHandle, ContainerPoolController, PoolStats


# ---------------------------------------------------------------------------
# Helper: build a pool with _create_and_warm mocked
# ---------------------------------------------------------------------------


def _make_controller(pool_size: int = 3, mode: str = "pooled") -> ContainerPoolController:
    ctrl = ContainerPoolController(
        image="test:latest",
        pool_size=pool_size,
        overlay_reset=True,
        checkout_timeout_s=2,
        docker_create_timeout_ms=200,
        mode=mode,
    )
    return ctrl


async def _start_with_mocks(ctrl: ContainerPoolController) -> None:
    """Start controller with _create_and_warm returning fake handles."""
    ids = iter(range(ctrl._pool_size))

    async def fake_create():
        return ContainerHandle(container_id=f"fake_{next(ids)}")

    async def fake_reset(handle):
        return None  # overlay reset is a no-op in tests

    ctrl._create_and_warm = fake_create
    ctrl._reset_overlay = fake_reset
    await ctrl.start()


# ---------------------------------------------------------------------------
# Semantics tests
# ---------------------------------------------------------------------------


class TestPoolSemantics:

    @pytest.mark.asyncio
    async def test_checkout_returns_handle(self):
        ctrl = _make_controller(pool_size=2)
        await _start_with_mocks(ctrl)

        handle = await ctrl.checkout()
        assert isinstance(handle, ContainerHandle)
        assert handle.container_id.startswith("fake_")
        await ctrl.checkin(handle)
        await ctrl.stop()

    @pytest.mark.asyncio
    async def test_checkin_returns_to_pool(self):
        ctrl = _make_controller(pool_size=2)
        await _start_with_mocks(ctrl)

        assert ctrl._available.qsize() == 2

        handle = await ctrl.checkout()
        assert ctrl._available.qsize() == 1

        await ctrl.checkin(handle)
        # After overlay reset, handle returns to pool
        assert ctrl._available.qsize() == 2
        await ctrl.stop()

    @pytest.mark.asyncio
    async def test_pool_exhaustion_blocks_then_releases(self):
        """Checkout blocks when pool is empty; checkin unblocks it."""
        ctrl = _make_controller(pool_size=1)
        await _start_with_mocks(ctrl)

        handle1 = await ctrl.checkout()
        assert ctrl._available.qsize() == 0

        # Schedule a checkin after 0.1s from a concurrent task
        async def delayed_checkin():
            await asyncio.sleep(0.1)
            await ctrl.checkin(handle1)

        asyncio.create_task(delayed_checkin())

        # This checkout should block until delayed_checkin fires
        t0 = time.monotonic()
        handle2 = await ctrl.checkout()
        elapsed = time.monotonic() - t0

        assert elapsed >= 0.09, f"Should have waited for checkin; waited {elapsed:.3f}s"
        assert handle2.container_id == handle1.container_id  # same container reused
        await ctrl.checkin(handle2)
        await ctrl.stop()

    @pytest.mark.asyncio
    async def test_checkout_timeout_raises(self):
        ctrl = ContainerPoolController(
            image="test:latest",
            pool_size=1,
            overlay_reset=False,
            checkout_timeout_s=0.05,   # very short timeout
            docker_create_timeout_ms=200,
            mode="pooled",
        )
        await _start_with_mocks(ctrl)

        handle = await ctrl.checkout()  # deplete the pool

        with pytest.raises(asyncio.TimeoutError):
            await ctrl.checkout()

        await ctrl.checkin(handle)
        await ctrl.stop()

    @pytest.mark.asyncio
    async def test_process_mode_returns_dummy_handle(self):
        ctrl = _make_controller(mode="process")
        await ctrl.start()  # no real containers

        handle = await ctrl.checkout()
        assert handle.container_id == "host-process"
        await ctrl.checkin(handle)
        await ctrl.stop()

    @pytest.mark.asyncio
    async def test_overlay_reset_increments_stat(self):
        ctrl = _make_controller(pool_size=2)
        await _start_with_mocks(ctrl)

        handle = await ctrl.checkout()
        assert ctrl.stats.overlay_resets == 0

        await ctrl.checkin(handle)
        assert ctrl.stats.overlay_resets == 1

        await ctrl.stop()

    @pytest.mark.asyncio
    async def test_concurrent_checkouts_all_served(self):
        """
        10 concurrent tasks each checking out and returning containers
        from a pool of 3 must all complete without deadlock.
        """
        ctrl = _make_controller(pool_size=3)
        await _start_with_mocks(ctrl)

        results = []
        lock = asyncio.Lock()

        async def task():
            handle = await ctrl.checkout()
            await asyncio.sleep(0.01)   # simulate task work
            await ctrl.checkin(handle)
            async with lock:
                results.append("done")

        await asyncio.gather(*[task() for _ in range(10)])
        assert len(results) == 10
        await ctrl.stop()

    @pytest.mark.asyncio
    async def test_stats_tracked(self):
        ctrl = _make_controller(pool_size=2)
        await _start_with_mocks(ctrl)

        assert ctrl.stats.total == 2
        assert ctrl.stats.available == 2
        assert ctrl.stats.checked_out == 0

        handle = await ctrl.checkout()
        assert ctrl.stats.checked_out == 1
        assert ctrl.stats.available == 1

        await ctrl.checkin(handle)
        assert ctrl.stats.checked_out == 0
        assert ctrl.stats.available == 2
        await ctrl.stop()


# ---------------------------------------------------------------------------
# Thread-safe sync wrapper tests
# ---------------------------------------------------------------------------


class TestCheckoutSync:
    """checkout_sync() / checkin_sync() are called from non-async worker threads."""

    def test_checkout_sync_from_thread(self):
        """Worker threads must be able to checkout/checkin without asyncio."""

        loop = asyncio.new_event_loop()
        ctrl = _make_controller(pool_size=2)

        async def _setup():
            await _start_with_mocks(ctrl)
            ctrl._loop = loop

        loop.run_until_complete(_setup())

        # Run event loop in background thread
        loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
        loop_thread.start()

        # Checkout from a non-async thread
        handle = ctrl.checkout_sync()
        assert isinstance(handle, ContainerHandle)
        ctrl.checkin_sync(handle)

        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=2)
