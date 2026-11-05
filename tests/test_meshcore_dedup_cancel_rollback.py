"""MeshCore cancelled-ingress dedup rollback regression tests.

Covers the active-session cancellation window in the SDK callback
publish path: ``_on_message`` commits a provisional dedup key before
scheduling ``_on_message_async``, and a publication task cancelled
before admission must release that key so a retransmission of the same
message identity is re-admitted instead of being silently suppressed.

Ownership rule: each commit stores an opaque ownership token and a
cancelled handler may only release the entry it still owns, so a late
stale handler can never evict a reservation re-committed by a newer
delivery.  A cancellation that lands after ``publish_inbound`` has
returned never releases the key — the event is durably admitted by
then and its identity must keep suppressing redelivery.

Evidence level: ``fake_adapter_callback`` (tier 1–2).  Fake mode only;
no network, no hardware, no Docker, no fixed sleeps.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Literal

import pytest
from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.identity import derive_message_identity
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterContext
from medre.core.events import CanonicalEvent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    *,
    adapter_id: str = "mc-cancel-dedup",
    connection_type: Literal["fake", "tcp", "serial", "ble"] = "fake",
) -> MeshCoreConfig:
    return MeshCoreConfig(adapter_id=adapter_id, connection_type=connection_type)


def _meshcore_text_packet(
    text: str = "retransmit-me",
    pubkey_prefix: str = "cxl1",
    sender_timestamp: int = 4242,
    channel_idx: int = 0,
    msg_type: str = "CHAN",
) -> dict:
    """Build a minimal MeshCore text packet that classifies as ``relay``."""
    return {
        "text": text,
        "pubkey_prefix": pubkey_prefix,
        "sender_timestamp": sender_timestamp,
        "type": msg_type,
        "txt_type": 0,
        "channel_idx": channel_idx,
    }


def _dedup_key(packet: dict) -> str | None:
    """Derive the expected dedup identity for a txt_type=0 channel packet."""
    return derive_message_identity(
        sender_id=packet["pubkey_prefix"],
        channel_index=packet["channel_idx"],
        sender_timestamp=packet["sender_timestamp"],
        txt_type=packet["txt_type"],
        text=packet["text"],
        is_direct_message=False,
    )


class _HeldPublisher:
    """Deterministic gated publisher.

    Each ``publish_inbound`` call marks its per-call entry event (the
    caller is then suspended on the shared gate) and, once the gate
    opens, records the published event id.  Entry events make
    "publication is pending" observable without sleeps.
    """

    def __init__(self, entry_count: int) -> None:
        self.gate = asyncio.Event()
        self.entry_events = [asyncio.Event() for _ in range(entry_count)]
        self._entered = 0
        self.published: list[str] = []

    async def __call__(self, event: CanonicalEvent) -> None:
        index = min(self._entered, len(self.entry_events) - 1)
        self._entered += 1
        self.entry_events[index].set()
        await self.gate.wait()
        self.published.append(event.event_id)


async def _start(adapter: MeshCoreAdapter, publisher: _HeldPublisher) -> AdapterContext:
    ctx = AdapterContext(
        adapter_id=adapter.adapter_id,
        publish_inbound=publisher,
        logger=logging.getLogger(f"test.{adapter.adapter_id}"),
        clock=lambda: datetime.now(UTC),
        shutdown_event=asyncio.Event(),
    )
    await adapter.start(ctx)
    return ctx


def _single_pending_task(adapter: MeshCoreAdapter) -> asyncio.Task:
    tasks = tuple(adapter._background_tasks)
    assert len(tasks) == 1, f"expected one pending task, saw {len(tasks)}"
    return tasks[0]


# ===================================================================
# Defect: cancelling a pending publication while started left the
# provisional dedup key cached, so an identical retransmission
# scheduled zero tasks and was silently suppressed.
# ===================================================================


async def test_cancelled_before_first_step_releases_provisional_key() -> None:
    """The task's done callback releases a reservation even if never stepped."""
    publisher = _HeldPublisher(entry_count=1)
    adapter = MeshCoreAdapter(_make_config(adapter_id="mc-cancel-unstepped"))
    await _start(adapter, publisher)
    try:
        packet = _meshcore_text_packet()
        key = _dedup_key(packet)
        assert key is not None
        adapter._on_message(packet)
        task = _single_pending_task(adapter)
        assert key in adapter._inbound_dedup
        task.cancel()  # No await: the coroutine has not begun execution.
        with pytest.raises(asyncio.CancelledError):
            await task
        assert key not in adapter._inbound_dedup
        assert publisher._entered == 0
        adapter._on_message(packet)
        assert len(adapter._background_tasks) == 1
        publisher.gate.set()
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 5)
        assert len(publisher.published) == 1
        assert key in adapter._inbound_dedup
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_cancelled_pending_publication_releases_provisional_key() -> None:
    """A publication task cancelled before admission must release its key."""
    publisher = _HeldPublisher(entry_count=2)
    adapter = MeshCoreAdapter(_make_config(adapter_id="mc-cancel-release"))
    await _start(adapter, publisher)
    try:
        packet = _meshcore_text_packet()
        key = _dedup_key(packet)
        assert key is not None

        adapter._on_message(packet)
        task = _single_pending_task(adapter)
        await asyncio.wait_for(publisher.entry_events[0].wait(), 5)
        assert key in adapter._inbound_dedup
        assert adapter.diagnostics()["inbound_published"] == 0

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()
        assert adapter.diagnostics()["inbound_published"] == 0

        # The provisional key must not outlive its cancelled publication.
        assert key not in adapter._inbound_dedup
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_cancelled_publication_redelivery_is_readmitted() -> None:
    """After cancellation the identical packet must be admitted again.

    On the unpatched adapter the leaked key suppressed the
    retransmission: no task was scheduled and the packet never reached
    the pipeline.
    """
    publisher = _HeldPublisher(entry_count=2)
    adapter = MeshCoreAdapter(_make_config(adapter_id="mc-cancel-readmit"))
    await _start(adapter, publisher)
    try:
        packet = _meshcore_text_packet()
        key = _dedup_key(packet)
        assert key is not None

        adapter._on_message(packet)
        task = _single_pending_task(adapter)
        await asyncio.wait_for(publisher.entry_events[0].wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        before = frozenset(adapter._background_tasks)
        adapter._on_message(packet)
        scheduled = frozenset(adapter._background_tasks) - before
        assert len(scheduled) == 1, (
            "identical retransmission after cancelled publication "
            "must be admitted, not silently suppressed"
        )

        publisher.gate.set()
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 5)
        assert len(publisher.published) == 1
        assert adapter.diagnostics()["inbound_published"] == 1
        assert key in adapter._inbound_dedup
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Post-admission cancellation must not roll back the durable key.
# ===================================================================


async def test_post_admission_cancellation_keeps_dedup_key() -> None:
    """After a successful publish the key stays; cancelling is a no-op."""
    publisher = _HeldPublisher(entry_count=1)
    adapter = MeshCoreAdapter(_make_config(adapter_id="mc-cancel-admitted"))
    await _start(adapter, publisher)
    try:
        packet = _meshcore_text_packet()
        key = _dedup_key(packet)
        assert key is not None

        adapter._on_message(packet)
        task = _single_pending_task(adapter)
        await asyncio.wait_for(publisher.entry_events[0].wait(), 5)
        publisher.gate.set()
        await asyncio.wait_for(asyncio.gather(task), 5)
        assert not task.cancelled()
        assert adapter.diagnostics()["inbound_published"] == 1
        assert key in adapter._inbound_dedup

        # The task is already done and durably admitted: cancellation
        # cannot retroactively release the admitted identity.
        assert task.cancel() is False
        assert key in adapter._inbound_dedup

        before = frozenset(adapter._background_tasks)
        adapter._on_message(packet)
        assert not (frozenset(adapter._background_tasks) - before)
        assert len(publisher.published) == 1
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Ownership: a cancelled stale handler must not evict a key
# re-committed by a newer delivery.
# ===================================================================


async def test_cancelled_stale_owner_keeps_recommitted_key(monkeypatch) -> None:
    """A stale cancellation token must not release a newer reservation.

    Reproduces the audit interleaving through production paths: the
    first publication task is cancelled and releases its own
    reservation, a redelivery re-commits the same identity through
    ``_on_message``, and a stale handler (the production
    ``_on_message_async`` re-run with the first task's ownership token)
    is cancelled afterwards — its release must be ignored because the
    entry is owned by the newer commit.
    """
    publisher = _HeldPublisher(entry_count=3)
    adapter = MeshCoreAdapter(_make_config(adapter_id="mc-cancel-ownership"))
    await _start(adapter, publisher)

    calls: list[tuple[CanonicalEvent, str | None, object | None]] = []
    original_async = adapter._on_message_async

    async def capturing_async(canonical, dedup_key=None, dedup_token=None):
        calls.append((canonical, dedup_key, dedup_token))
        await original_async(canonical, dedup_key, dedup_token)

    monkeypatch.setattr(adapter, "_on_message_async", capturing_async)

    try:
        packet = _meshcore_text_packet()
        key = _dedup_key(packet)
        assert key is not None

        # First delivery: provisional commit, held pending.
        adapter._on_message(packet)
        first_task = _single_pending_task(adapter)
        await asyncio.wait_for(publisher.entry_events[0].wait(), 5)
        stale_canonical, _, stale_token = calls[0]

        first_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_task
        # The stale owner released its own reservation.
        assert key not in adapter._inbound_dedup

        # Retransmission re-commits through the production path.
        before = frozenset(adapter._background_tasks)
        adapter._on_message(packet)
        scheduled = frozenset(adapter._background_tasks) - before
        assert len(scheduled) == 1
        newer_task = next(iter(scheduled))
        await asyncio.wait_for(publisher.entry_events[1].wait(), 5)
        newer_token = calls[1][2]

        # A stale handler running after the re-commit (production path,
        # first task's token) must leave the newer reservation untouched.
        rerun = asyncio.create_task(
            adapter._on_message_async(stale_canonical, key, stale_token)
        )
        await asyncio.wait_for(publisher.entry_events[2].wait(), 5)
        rerun.cancel()
        with pytest.raises(asyncio.CancelledError):
            await rerun
        assert adapter._inbound_dedup.get(key) is newer_token

        # The newer delivery still publishes exactly once.
        publisher.gate.set()
        await asyncio.wait_for(asyncio.gather(newer_task), 5)
        assert len(publisher.published) == 1
        assert adapter.diagnostics()["inbound_published"] == 1
        assert adapter._inbound_dedup.get(key) is newer_token
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Stop/shutdown under the cancellation path still clears everything.
# ===================================================================


async def test_stop_with_pending_publication_clears_dedup_and_blocks_publish() -> None:
    """stop() drains cancelled publications, clears dedup, blocks post-stop."""
    publisher = _HeldPublisher(entry_count=1)
    adapter = MeshCoreAdapter(_make_config(adapter_id="mc-cancel-stop"))
    await _start(adapter, publisher)
    try:
        packet = _meshcore_text_packet()
        key = _dedup_key(packet)
        assert key is not None

        adapter._on_message(packet)
        await asyncio.wait_for(publisher.entry_events[0].wait(), 5)
        assert key in adapter._inbound_dedup

        await adapter.stop()

        assert not adapter._background_tasks
        assert not adapter._inbound_dedup
        assert publisher.published == []
        assert adapter.diagnostics()["inbound_published"] == 0

        # Post-stop callbacks are ignored: no task, no publication.
        adapter._on_message(packet)
        assert not adapter._background_tasks
        assert publisher.published == []
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Existing behavior preserved: duplicates still dedup in both the
# provisional window and after durable admission.
# ===================================================================


async def test_duplicate_arrivals_still_dedup() -> None:
    """Pending and admitted duplicates are suppressed without new tasks."""
    publisher = _HeldPublisher(entry_count=1)
    adapter = MeshCoreAdapter(_make_config(adapter_id="mc-cancel-dup"))
    await _start(adapter, publisher)
    try:
        packet = _meshcore_text_packet()
        key = _dedup_key(packet)
        assert key is not None

        adapter._on_message(packet)
        task = _single_pending_task(adapter)
        await asyncio.wait_for(publisher.entry_events[0].wait(), 5)

        # Provisional window: identical arrival while publication pending.
        before = frozenset(adapter._background_tasks)
        adapter._on_message(packet)
        assert not (frozenset(adapter._background_tasks) - before)
        assert len(publisher.published) == 0

        publisher.gate.set()
        await asyncio.wait_for(asyncio.gather(task), 5)
        assert len(publisher.published) == 1
        assert key in adapter._inbound_dedup

        # Admitted window: redelivery after successful publication.
        before = frozenset(adapter._background_tasks)
        adapter._on_message(packet)
        assert not (frozenset(adapter._background_tasks) - before)
        assert len(publisher.published) == 1
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# LRU cap semantics unchanged with ownership tokens as values.
# ===================================================================


async def test_lru_cap_eviction_unchanged_with_tokens() -> None:
    """The bounded cache still evicts least-recently-seen past the cap."""
    from medre.adapters.meshcore.adapter import _DEDUP_MAX_SIZE

    publisher = _HeldPublisher(entry_count=1)
    adapter = MeshCoreAdapter(_make_config(adapter_id="mc-cancel-lru"))
    await _start(adapter, publisher)
    try:
        keys = [f"mc1-{index:064x}" for index in range(_DEDUP_MAX_SIZE + 1)]
        for key in keys:
            adapter._commit_dedup_key(key)

        assert len(adapter._inbound_dedup) == _DEDUP_MAX_SIZE
        assert keys[0] not in adapter._inbound_dedup
        assert keys[1] in adapter._inbound_dedup
        assert keys[-1] in adapter._inbound_dedup

        # Releasing an evicted key is a no-op.
        assert adapter._release_dedup_key(keys[0], object()) is False
        assert len(adapter._inbound_dedup) == _DEDUP_MAX_SIZE
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_cancellation_as_publish_returns_keeps_admitted_key(monkeypatch) -> None:
    """Task cancellation at completion cannot undo a successful publication."""
    publisher = _HeldPublisher(entry_count=1)
    adapter = MeshCoreAdapter(_make_config())
    await _start(adapter, publisher)

    async def admitted_then_cancelled(event):
        publisher.published.append(event.event_id)
        asyncio.current_task().cancel()

    monkeypatch.setattr(adapter, "publish_inbound", admitted_then_cancelled)
    try:
        packet = _meshcore_text_packet()
        key = _dedup_key(packet)
        adapter._on_message(packet)
        task = _single_pending_task(adapter)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert adapter.diagnostics()["inbound_published"] == 1
        assert key in adapter._inbound_dedup
        adapter._on_message(packet)
        assert not adapter._background_tasks
        assert len(publisher.published) == 1
    finally:
        await adapter.stop()


async def test_stale_failed_handler_keeps_recommitted_key(monkeypatch) -> None:
    """An ordinary publish error obeys the same reservation ownership fence."""
    publisher = _HeldPublisher(entry_count=1)
    adapter = MeshCoreAdapter(_make_config())
    await _start(adapter, publisher)
    try:
        packet = _meshcore_text_packet()
        key = _dedup_key(packet)
        assert key is not None
        stale = adapter._commit_dedup_key(key)
        replacement = adapter._commit_dedup_key(key)

        async def fail(_event):
            raise RuntimeError("publication failed")

        monkeypatch.setattr(adapter, "publish_inbound", fail)
        await adapter._on_message_async(adapter.get_codec().decode(packet), key, stale)
        assert adapter._inbound_dedup[key] is replacement
    finally:
        await adapter.stop()
