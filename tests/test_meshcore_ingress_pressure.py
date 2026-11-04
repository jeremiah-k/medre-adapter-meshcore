"""Measure MeshCore callback task pressure with a held canonical publisher.

This is a deterministic SDK-callback shape test, not companion or RF evidence.
There is intentionally no implicit lossy cap: if a bounded admission policy is
added later it must explicitly classify overflow and preserve dedup correctness.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterContext


def _packet(index: int) -> dict:
    """Make a relayable unique channel packet without SDK/hardware state."""
    return {
        "text": f"pressure {index}",
        "pubkey_prefix": "pressure-source",
        "sender_timestamp": index + 1,
        "type": "CHAN",
        "txt_type": 0,
        "channel_idx": 0,
    }


def _report(burst: int, pending: int, delivered: int, elapsed: float) -> None:
    """Emit only counters/timing; never store synthetic message content."""
    root = os.environ.get("MEDRE_PRESSURE_REPORT_DIR")
    if not root:
        return
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"meshcore-{burst}.json").write_text(
        json.dumps(
            {
                "transport": "meshcore",
                "tier": "synthetic_sdk_callback",
                "burst": burst,
                "peak_pending_callbacks": pending,
                "delivered": delivered,
                "drained": delivered == burst,
                "elapsed_ms": round(elapsed * 1000, 2),
                "overflow_policy": "not_configured",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


@pytest.mark.parametrize("burst", [32, 256])
async def test_held_publish_burst_measures_pre_admission_tasks(burst: int) -> None:
    """The sync SDK callback schedules work before the awaited publish boundary."""
    gate = asyncio.Event()
    all_entered = asyncio.Event()
    seen: set[str] = set()
    published: set[str] = set()

    async def publish(event) -> None:
        seen.add(event.event_id)
        if len(seen) == burst:
            all_entered.set()
        await gate.wait()
        published.add(event.event_id)

    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="meshcore-pressure"))
    await adapter.start(
        AdapterContext(
            adapter_id=adapter.adapter_id,
            publish_inbound=publish,
            logger=logging.getLogger("test.meshcore.ingress.pressure"),
            clock=lambda: datetime.now(UTC),
            shutdown_event=asyncio.Event(),
        )
    )
    try:
        start = time.perf_counter()
        for index in range(burst):
            adapter._on_message(_packet(index))
        pending = len(adapter._background_tasks)
        assert pending == burst
        await asyncio.wait_for(all_entered.wait(), 10)
        assert adapter.diagnostics()["inbound_published"] == 0
        gate.set()
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 10)
        assert adapter.diagnostics()["inbound_published"] == burst
        assert len(seen) == burst
        assert published == seen
        assert not adapter._background_tasks
        _report(burst, pending, len(published), time.perf_counter() - start)
    finally:
        gate.set()
        await adapter.stop()


async def test_stop_cancels_pending_sdk_callback_tasks() -> None:
    """A held callback is cancelled by lifecycle stop, not silently retained."""
    gate = asyncio.Event()
    entered = asyncio.Event()
    exited = asyncio.Event()
    cancelled = asyncio.Event()
    publishers: list[asyncio.Task] = []
    published = []

    async def publish(event) -> None:
        task = asyncio.current_task()
        assert task is not None
        publishers.append(task)
        entered.set()
        try:
            await gate.wait()
            published.append(event.event_id)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        finally:
            exited.set()

    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="meshcore-pressure-stop"))
    await adapter.start(
        AdapterContext(
            adapter_id=adapter.adapter_id,
            publish_inbound=publish,
            logger=logging.getLogger("test.meshcore.ingress.shutdown"),
            clock=lambda: datetime.now(UTC),
            shutdown_event=asyncio.Event(),
        )
    )
    try:
        adapter._on_message(_packet(1))
        await asyncio.wait_for(entered.wait(), 5)
        assert adapter._background_tasks
        await adapter.stop()
        await asyncio.wait_for(exited.wait(), 5)
        await asyncio.gather(*publishers, return_exceptions=True)
        assert cancelled.is_set()
        assert all(task.cancelled() for task in publishers)
        assert not adapter._background_tasks
        gate.set()
        adapter._on_message(_packet(2))
        assert not adapter._background_tasks
        assert not published
        assert adapter.diagnostics()["inbound_published"] == 0
    finally:
        gate.set()
        for task in publishers:
            task.cancel()
        await asyncio.gather(*publishers, return_exceptions=True)
        await adapter.stop()


async def test_scheduled_callback_rechecks_stop_before_publication(monkeypatch) -> None:
    """A queued callback cannot publish after stop, even before drain cancels it."""
    published = []

    async def publish(event) -> None:
        published.append(event.event_id)

    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="meshcore-scheduled-stop"))
    await adapter.start(
        AdapterContext(
            adapter_id=adapter.adapter_id,
            publish_inbound=publish,
            logger=logging.getLogger("test.meshcore.ingress.scheduled_stop"),
            clock=lambda: datetime.now(UTC),
            shutdown_event=asyncio.Event(),
        )
    )
    tasks = ()
    try:
        adapter._on_message(_packet(1))
        tasks = tuple(adapter._background_tasks)
        assert len(tasks) == 1 and not tasks[0].done()

        async def drain_without_cancellation(timeout=5.0):
            # Isolate the coroutine's stop fence from the drain's cancellation.
            await asyncio.gather(*tasks)

        monkeypatch.setattr(
            adapter, "_drain_background_tasks", drain_without_cancellation
        )
        await adapter.stop()
        assert all(task.done() and not task.cancelled() for task in tasks)
        assert not published
        assert adapter.diagnostics()["inbound_published"] == 0
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await adapter.stop()
