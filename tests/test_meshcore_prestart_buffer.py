"""Pre-start inbound buffering tests for MeshCoreAdapter.

The pinned MeshCore SDK's ``start_auto_message_fetching`` issues one
``get_msg()`` while ``MeshCoreSession.start()`` is still awaiting, so a
frame buffered on the device can reach ``MeshCoreAdapter._on_message``
before the adapter sets ``_started``.  Because ``GET_NEXT_MSG`` pops the
frame from the device queue, the pre-fix silent drop lost that message.

These tests pin the queue-then-flush policy:

- payloads delivered while ``start()`` is in flight are buffered (bounded
  FIFO, cap 64) and flushed through the normal inbound path after start
  completes, in arrival order, passing every existing guard (dedup,
  classification, bounded admission);
- a full buffer drops with a cumulative counter and a rate-limited,
  content-free warning;
- ``stop()`` and failed starts discard the buffer and never replay it
  into a later session;
- callbacks arriving outside the start window (never started, post-stop)
  keep the pre-existing silent drop.

Deterministic SDK-callback shape tests (patched session, no network, no
hardware, no sleeps).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, Mock, patch

import pytest
from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterContext

_CONFIG_ID = "mc-prestart"

MidStartFn = Callable[[Callable[[dict], None]], Awaitable[None]]


def _make_config(**overrides: object) -> MeshCoreConfig:
    """Build the adapter config, overriding any default field."""
    defaults: dict = {"adapter_id": _CONFIG_ID}
    defaults.update(overrides)
    return MeshCoreConfig(**defaults)


def _chan_packet(index: int) -> dict:
    """Make a relayable, unique channel packet without SDK/hardware state."""
    return {
        "text": f"prestart {index}",
        "pubkey_prefix": "prestart-source",
        "sender_timestamp": index + 1,
        "type": "CHAN",
        "txt_type": 0,
        "channel_idx": 0,
    }


def _dm_packet(text: str = "early drain dm") -> dict:
    """Make a relayable direct-message packet (the startup-drain case)."""
    return {
        "text": text,
        "pubkey_prefix": "cafe00",
        "sender_timestamp": 77,
        "type": "PRIV",
        "txt_type": 0,
    }


def _published_bodies(collector: object) -> list[str]:
    """Return the collected publication bodies in arrival order."""
    return [str(event.payload.get("body")) for event in collector.events]  # type: ignore[attr-defined]


async def _drain(adapter: MeshCoreAdapter, timeout: float = 10.0) -> None:
    """Await every publication task created by the adapter so far."""
    tasks = tuple(adapter._background_tasks)
    if tasks:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout)


def _racing_session_mock(mid_start: MidStartFn | None = None) -> AsyncMock:
    """Build a patched MeshCoreSession whose start() runs *mid_start*.

    *mid_start* receives the adapter's ``_on_message`` callback and runs
    while ``session.start()`` is awaiting — i.e. before the adapter sets
    ``_started``, exactly where the pinned SDK's startup drain dispatches
    a device-buffered frame.  Without *mid_start*, start() completes
    cleanly.  Sync session surfaces touched by the inbound path are
    pinned to plain values.
    """
    mock_inst = AsyncMock()
    mock_inst.diagnostics = Mock(return_value={"connected": True})
    mock_inst.resolve_contact_label = Mock(return_value=None)
    mock_inst.stop = AsyncMock()
    if mid_start is None:
        mock_inst.start = AsyncMock()
    else:

        async def _start(
            message_callback: Callable[[dict], None], **_kwargs: object
        ) -> None:
            await mid_start(message_callback)

        mock_inst.start = AsyncMock(side_effect=_start)
    return mock_inst


def _patch_session(mock_inst: AsyncMock) -> Mock:
    """Patch the adapter's session constructor to yield *mock_inst*."""
    return patch(
        "medre.adapters.meshcore.adapter.MeshCoreSession", return_value=mock_inst
    )


async def _cancel_start_retaining_owner(
    adapter: MeshCoreAdapter, ctx: AdapterContext
) -> AsyncMock:
    """Cancel an in-flight start, leaving its partial session retained.

    Drives ``start()`` to the point where the session is installed and
    ``session.start()`` is still awaiting, then cancels the caller task.
    Returns the retained partial session mock — the owner that core's
    bounded startup rollback would later ``stop()``.
    """
    entered = asyncio.Event()

    async def hanging_start(_message_callback: Callable[[dict], None]) -> None:
        entered.set()
        await asyncio.Event().wait()  # parks until the caller cancels

    owner = _racing_session_mock(hanging_start)
    with _patch_session(owner):
        first = asyncio.create_task(adapter.start(ctx))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        finally:
            if not first.done():
                first.cancel()
                await asyncio.gather(first, return_exceptions=True)
    return owner


# ===================================================================
# Core behavior: mid-start payloads are flushed, not lost
# ===================================================================


async def test_midstart_dm_flushed_and_published_after_start(
    inbound_collector, make_adapter_context
) -> None:
    """A device-buffered DM delivered mid-start is published after start.

    Regression for the startup drain race: ``start_auto_message_fetching``
    pops one ``get_msg()`` frame inside ``session.start()`` and dispatches
    it to ``_on_message`` before ``_started`` is set.  The frame must be
    buffered and flushed, not silently dropped.
    """
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)

    dm = _dm_packet()
    observed: dict[str, object] = {}

    async def mid_start(message_callback: Callable[[dict], None]) -> None:
        observed["started"] = adapter._started
        observed["starting"] = adapter._starting
        message_callback(dm)
        observed["buffered"] = len(adapter._prestart_buffer)

    with _patch_session(_racing_session_mock(mid_start)):
        await adapter.start(ctx)

    # The frame arrived before _started and was buffered, not dropped.
    assert observed == {"started": False, "starting": True, "buffered": 1}

    # Start completed: the buffer was flushed through the normal path.
    assert adapter._started is True
    assert not adapter._prestart_buffer
    assert adapter._prestart_flushed_count == 1

    await _drain(adapter)
    assert _published_bodies(inbound_collector) == ["early drain dm"]

    diag = adapter.diagnostics()
    assert diag["prestart_buffered"] == 1
    assert diag["prestart_flushed"] == 1
    assert diag["prestart_overflow_dropped"] == 0
    assert diag["inbound_published"] == 1

    await adapter.stop()


async def test_flush_preserves_arrival_order(
    inbound_collector, make_adapter_context
) -> None:
    """Buffered frames flush in arrival order through the normal path."""
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)

    async def mid_start(message_callback: Callable[[dict], None]) -> None:
        for index in range(3):
            message_callback(_chan_packet(index))

    with _patch_session(_racing_session_mock(mid_start)):
        await adapter.start(ctx)

    assert adapter._prestart_flushed_count == 3
    await _drain(adapter)
    assert _published_bodies(inbound_collector) == [
        "prestart 0",
        "prestart 1",
        "prestart 2",
    ]

    await adapter.stop()


async def test_flushed_packets_pass_dedup_guard(
    inbound_collector, make_adapter_context
) -> None:
    """Flushed frames re-enter the full handler, including dedup.

    Two identical frames buffered mid-start (e.g. a device-queue
    redelivery) flush in order: the first publishes, the second is
    suppressed by the adapter-level dedup — exactly as a live duplicate
    would be.
    """
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)

    duplicate = _chan_packet(0)

    async def mid_start(message_callback: Callable[[dict], None]) -> None:
        message_callback(duplicate)
        message_callback(dict(duplicate))

    with _patch_session(_racing_session_mock(mid_start)):
        await adapter.start(ctx)

    # Both frames were buffered and flushed (flush counts hand-offs).
    assert adapter._prestart_buffered_count == 2
    assert adapter._prestart_flushed_count == 2

    await _drain(adapter)
    # But only the first delivery was published.
    assert _published_bodies(inbound_collector) == ["prestart 0"]
    assert adapter.diagnostics()["inbound_published"] == 1

    await adapter.stop()


async def test_simulate_inbound_during_start_buffers_and_flushes(
    inbound_collector, make_adapter_context
) -> None:
    """simulate_inbound during the start window buffers like the callback."""
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)

    dm = _dm_packet("simulated mid-start")

    async def mid_start(_message_callback: Callable[[dict], None]) -> None:
        await adapter.simulate_inbound(dm)
        assert len(adapter._prestart_buffer) == 1

    with _patch_session(_racing_session_mock(mid_start)):
        await adapter.start(ctx)

    assert adapter._prestart_flushed_count == 1
    await _drain(adapter)
    assert _published_bodies(inbound_collector) == ["simulated mid-start"]

    await adapter.stop()


# ===================================================================
# Bounded buffer: overflow drops with counter and warning
# ===================================================================


async def test_buffer_overflow_drops_with_counter_and_warning(
    inbound_collector, make_adapter_context, caplog: pytest.LogCaptureFixture
) -> None:
    """Packets beyond the 64-item cap drop with a counter and warning.

    The dropped frame is the newest one: frames 0..63 stay buffered and
    flush; frame 64 is refused with ``prestart_overflow_dropped == 1``
    and one content-free warning (no message text, no sender identity).
    """
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context("mc-prestart-overflow")

    burst = 65  # one more than _PRESTART_BUFFER_MAX (64)

    async def mid_start(message_callback: Callable[[dict], None]) -> None:
        for index in range(burst):
            message_callback(_chan_packet(index))

    with caplog.at_level(logging.WARNING):
        with _patch_session(_racing_session_mock(mid_start)):
            await adapter.start(ctx)

    assert adapter._prestart_buffered_count == 64
    assert adapter._prestart_overflow_dropped_count == 1
    assert not adapter._prestart_buffer
    assert adapter._prestart_flushed_count == 64

    await _drain(adapter)
    bodies = _published_bodies(inbound_collector)
    assert bodies == [f"prestart {index}" for index in range(64)]
    assert "prestart 64" not in bodies

    overflow_warnings = [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING
        and "pre-start buffer full" in record.getMessage()
    ]
    assert len(overflow_warnings) == 1
    message = overflow_warnings[0].getMessage()
    assert "cap=64" in message
    assert "prestart 64" not in message
    assert "prestart-source" not in message

    diag = adapter.diagnostics()
    assert diag["prestart_buffered"] == 64
    assert diag["prestart_flushed"] == 64
    assert diag["prestart_overflow_dropped"] == 1

    await adapter.stop()


# ===================================================================
# Lifecycle: stop and failed start never replay the buffer
# ===================================================================


async def test_stop_discards_buffered_packets_and_never_replays(
    inbound_collector, make_adapter_context, caplog: pytest.LogCaptureFixture
) -> None:
    """stop() inside the start window discards the buffer; no replay later.

    A stopped adapter never replays pre-start buffered frames — not on
    the stopped lifecycle and not into a subsequent session.
    """
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)

    async def mid_start(message_callback: Callable[[dict], None]) -> None:
        message_callback(_chan_packet(0))
        assert len(adapter._prestart_buffer) == 1
        # Stop while start() is still in flight: the buffered frame must
        # be discarded here, before start() reaches its flush.
        await adapter.stop()
        # A late callback after stop must not reopen the buffer window.
        message_callback(_chan_packet(1))
        assert not adapter._prestart_buffer

    with caplog.at_level(logging.WARNING):
        with _patch_session(_racing_session_mock(mid_start)):
            with pytest.raises(asyncio.CancelledError, match="stopped or superseded"):
                await adapter.start(ctx)

    discarded_warnings = [
        record
        for record in caplog.records
        if "discarded 1 pre-start buffered packet" in record.getMessage()
    ]
    assert len(discarded_warnings) == 1

    assert adapter._prestart_buffered_count == 1
    assert adapter._prestart_flushed_count == 0
    assert adapter._started is False
    assert adapter._starting is False
    assert not adapter._prestart_buffer

    # Give the loop a few turns: nothing may publish.
    for _ in range(3):
        await asyncio.sleep(0)
    assert inbound_collector.events == []

    # A real later start must create a new session, not hit the idempotent
    # _started guard from the aborted attempt.
    with _patch_session(_racing_session_mock()):
        await adapter.start(ctx)
    await _drain(adapter)
    assert inbound_collector.events == []
    assert adapter._prestart_flushed_count == 0

    await adapter.stop()


async def test_stop_while_session_start_is_pending_does_not_resurrect(
    inbound_collector, make_adapter_context
) -> None:
    """Stop retires an in-flight start; even a late SDK callback cannot replay."""
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)
    entered, release = asyncio.Event(), asyncio.Event()

    async def mid_start(message_callback: Callable[[dict], None]) -> None:
        message_callback(_chan_packet(0))
        entered.set()
        await release.wait()
        message_callback(_chan_packet(1))

    with _patch_session(_racing_session_mock(mid_start)):
        starting = asyncio.create_task(adapter.start(ctx))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            assert adapter._starting
            assert len(adapter._prestart_buffer) == 1
            await adapter.stop()
            assert adapter._starting is False
            assert not adapter._prestart_buffer
            release.set()
            with pytest.raises(asyncio.CancelledError, match="stopped or superseded"):
                await starting
            assert adapter._started is False
            assert adapter._prestart_flushed_count == 0
            assert adapter._prestart_buffered_count == 1
            assert not adapter._prestart_buffer
            assert inbound_collector.events == []
        finally:
            release.set()
            if not starting.done():
                starting.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await starting

    # A subsequent start is a genuine new session and inherits no buffer.
    with _patch_session(_racing_session_mock()):
        await adapter.start(ctx)
        assert adapter._started is True
        await _drain(adapter)
        assert inbound_collector.events == []
        await adapter.stop()


async def test_session_constructor_failure_closes_buffer_window(
    inbound_collector, make_adapter_context
) -> None:
    """An SDK constructor error must not leave a callback-buffering window."""
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)
    with patch(
        "medre.adapters.meshcore.adapter.MeshCoreSession",
        side_effect=ConnectionError("construction failed"),
    ):
        with pytest.raises(ConnectionError, match="construction failed"):
            await adapter.start(ctx)
    assert adapter._started is False
    assert adapter._starting is False
    assert adapter._session is None
    assert adapter.ctx is None
    adapter._on_message(_chan_packet(9))
    assert not adapter._prestart_buffer
    assert inbound_collector.events == []


async def test_constructor_failure_after_cancelled_start_closes_buffer_window(
    inbound_collector, make_adapter_context
) -> None:
    """A constructor failure must not wedge the adapter after a cancelled start.

    A cancelled start retains its partial session on purpose (core's
    bounded startup rollback calls ``stop()`` on it).  A follow-up
    ``start()`` whose ``MeshCoreSession`` constructor raises must still
    close the buffering window even though the identity guard sees the
    retained session instead of the session that was never constructed —
    otherwise ``_starting`` stays set and every later start is refused as
    already in progress until an intervening ``stop()``.
    """
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)
    owner = await _cancel_start_retaining_owner(adapter, ctx)
    assert adapter._session is owner
    assert adapter._starting is False

    with patch(
        "medre.adapters.meshcore.adapter.MeshCoreSession",
        side_effect=ConnectionError("construction failed"),
    ):
        with pytest.raises(ConnectionError, match="construction failed"):
            await adapter.start(ctx)

    # The failure closed the window instead of wedging _starting on.
    assert adapter._starting is False
    adapter._on_message(_chan_packet(7))
    assert not adapter._prestart_buffer
    assert inbound_collector.events == []

    # A later start without an intervening stop is not refused.
    with _patch_session(_racing_session_mock()):
        await adapter.start(ctx)
    assert adapter._started is True
    await _drain(adapter)
    assert inbound_collector.events == []
    await adapter.stop()


async def test_stop_after_constructor_failure_retires_retained_session(
    make_adapter_context,
) -> None:
    """stop() after a failed construction still retires the retained owner.

    The constructor-failure cleanup must keep the cancelled start's
    partial session in ``self._session`` so ``stop()`` can retire it.
    """
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)
    owner = await _cancel_start_retaining_owner(adapter, ctx)

    with patch(
        "medre.adapters.meshcore.adapter.MeshCoreSession",
        side_effect=ConnectionError("construction failed"),
    ):
        with pytest.raises(ConnectionError, match="construction failed"):
            await adapter.start(ctx)
    assert adapter._session is owner

    await adapter.stop()
    owner.stop.assert_awaited_once()
    assert adapter._session is None


async def test_failed_start_discards_buffered_packets(
    inbound_collector, make_adapter_context, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed start discards buffered frames and returns to the drop path."""
    adapter = MeshCoreAdapter(_make_config())
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)

    async def mid_start(message_callback: Callable[[dict], None]) -> None:
        message_callback(_chan_packet(0))
        raise ConnectionError("boom mid-drain")

    with caplog.at_level(logging.WARNING):
        with _patch_session(_racing_session_mock(mid_start)):
            with pytest.raises(ConnectionError, match="boom mid-drain"):
                await adapter.start(ctx)

    assert adapter._starting is False
    assert adapter._prestart_buffered_count == 1
    assert adapter._prestart_flushed_count == 0
    assert not adapter._prestart_buffer

    discarded_warnings = [
        record
        for record in caplog.records
        if "discarded 1 pre-start buffered packet" in record.getMessage()
    ]
    assert len(discarded_warnings) == 1

    # After a failed start the adapter is not starting: callbacks drop
    # again instead of buffering into a session that never happened.
    adapter._on_message(_chan_packet(99))
    assert not adapter._prestart_buffer
    assert adapter._prestart_buffered_count == 1
    assert inbound_collector.events == []

    # stop() after failed start stays idempotent.
    await adapter.stop()


# ===================================================================
# Outside the start window: the pre-existing silent drop is preserved
# ===================================================================


async def test_callback_before_first_start_is_dropped_without_buffering(
    inbound_collector,
) -> None:
    """A never-started adapter keeps the silent drop — no buffer, no counters."""
    adapter = MeshCoreAdapter(_make_config(connection_type="fake"))

    adapter._on_message(_chan_packet(0))
    adapter._on_message(_chan_packet(1))

    assert adapter._starting is False
    assert not adapter._prestart_buffer
    assert adapter._prestart_buffered_count == 0
    assert adapter._prestart_flushed_count == 0
    assert adapter._prestart_overflow_dropped_count == 0

    for _ in range(3):
        await asyncio.sleep(0)
    assert inbound_collector.events == []
    diag = adapter.diagnostics()
    assert diag["prestart_buffered"] == 0
    assert diag["prestart_flushed"] == 0
    assert diag["prestart_overflow_dropped"] == 0


async def test_post_stop_callback_is_dropped_without_buffering(
    inbound_collector, make_adapter_context
) -> None:
    """After stop(), late callbacks drop as today instead of buffering.

    The post-stop race guard (between drain completing and the session
    unsubscribing) must not start buffering: nothing would ever flush
    those frames.
    """
    adapter = MeshCoreAdapter(_make_config(connection_type="fake"))
    await adapter.start(make_adapter_context(_CONFIG_ID))
    await adapter.stop()

    adapter._on_message(_chan_packet(0))

    assert adapter._starting is False
    assert not adapter._prestart_buffer
    assert adapter._prestart_buffered_count == 0

    for _ in range(3):
        await asyncio.sleep(0)
    assert inbound_collector.events == []


# ===================================================================
# Restart hygiene: counters reset per lifecycle, flush after re-start
# ===================================================================


async def test_restart_resets_prestart_counters_and_flushes_again(
    inbound_collector, make_adapter_context
) -> None:
    """Every nonzero counter resets at the next actual session startup."""
    adapter = MeshCoreAdapter(_make_config())
    ctx = make_adapter_context(_CONFIG_ID)

    async def first_start(callback):
        for index in range(65):
            callback(_chan_packet(index))

    with _patch_session(_racing_session_mock(first_start)):
        await adapter.start(ctx)
    await _drain(adapter)
    first_bodies = _published_bodies(inbound_collector)
    assert first_bodies == [f"prestart {i}" for i in range(64)]
    diag = adapter.diagnostics()
    assert (
        diag["prestart_buffered"],
        diag["prestart_flushed"],
        diag["prestart_overflow_dropped"],
    ) == (64, 64, 1)
    await adapter.stop()

    async def second_start(callback):
        callback(_dm_packet("second lifecycle"))

    with _patch_session(_racing_session_mock(second_start)):
        await adapter.start(ctx)
    await _drain(adapter)
    diag = adapter.diagnostics()
    assert (
        diag["prestart_buffered"],
        diag["prestart_flushed"],
        diag["prestart_overflow_dropped"],
    ) == (1, 1, 0)
    assert _published_bodies(inbound_collector)[len(first_bodies) :] == [
        "second lifecycle"
    ]
    await adapter.stop()


async def test_buffer_snapshots_reused_packet_mapping(
    inbound_collector, make_adapter_context
):
    """Each buffered entry snapshots its mapping at receipt.

    A caller reusing and mutating one dictionary between deliveries must
    not rewrite earlier buffered entries; replay publishes one message
    per distinct snapshot.
    """
    adapter = MeshCoreAdapter(_make_config())
    ctx = make_adapter_context(_CONFIG_ID)

    async def mid_start(_callback):
        packet = _chan_packet(0)
        await adapter.simulate_inbound(packet)
        packet.update(_chan_packet(1))
        await adapter.simulate_inbound(packet)
        packet.update(_chan_packet(2))

    with _patch_session(_racing_session_mock(mid_start)):
        await adapter.start(ctx)
    try:
        await _drain(adapter)
        assert _published_bodies(inbound_collector) == ["prestart 0", "prestart 1"]
    finally:
        await adapter.stop()


async def test_flush_respects_callback_budget_and_allows_refused_redelivery(
    inbound_collector, make_adapter_context
):
    """Flushed frames face bounded admission; refused ones stay retryable.

    With a callback budget of one, the second flushed frame is refused
    with a producer-overflow report and no publication; once capacity
    frees, a live redelivery of the same identity is re-admitted.
    """
    adapter = MeshCoreAdapter(_make_config(max_pending_ingress_callbacks=1))
    ctx = make_adapter_context(_CONFIG_ID)
    release, entered = asyncio.Event(), asyncio.Event()
    original = ctx.publish_inbound

    async def blocked(event):
        entered.set()
        await release.wait()
        await original(event)

    ctx.publish_inbound = blocked

    async def mid_start(callback):
        callback(_chan_packet(0))
        callback(_chan_packet(1))

    with _patch_session(_racing_session_mock(mid_start)):
        await adapter.start(ctx)
    try:
        await entered.wait()
        diag = adapter.diagnostics()
        assert diag["pending_ingress_callbacks"] == 1
        assert diag["prestart_flushed"] == 2
        assert diag["prestart_overflow_dropped"] == 0
        assert adapter._producer_overflow_count == 1
        release.set()
        await _drain(adapter)
        assert _published_bodies(inbound_collector) == ["prestart 0"]
        adapter._on_message(_chan_packet(1))
        await _drain(adapter)
        assert _published_bodies(inbound_collector) == ["prestart 0", "prestart 1"]
    finally:
        release.set()
        await adapter.stop()


async def test_prestart_overflow_warning_throttles_and_rearms(
    make_adapter_context, caplog, monkeypatch
):
    """Overflow warnings are rate-limited, content-free, and re-arm.

    Drops within the throttle window stay counter-only; after the window
    the next drop warns again.  Warning text carries the cap and the
    cumulative total, never message content.
    """
    adapter = MeshCoreAdapter(_make_config())
    now = [100.0]
    monkeypatch.setattr("medre.adapters.meshcore.adapter.monotonic", lambda: now[0])

    async def mid_start(callback):
        for index in range(65):
            callback(_chan_packet(index))
        now[0] = 104.0
        callback(_chan_packet(65))
        now[0] = 106.0
        callback(_chan_packet(66))

    with caplog.at_level(logging.WARNING):
        with _patch_session(_racing_session_mock(mid_start)):
            await adapter.start(make_adapter_context(_CONFIG_ID))
    try:
        warnings = [
            r.getMessage()
            for r in caplog.records
            if "pre-start buffer full" in r.getMessage()
        ]
        assert len(warnings) == 2
        assert "total_dropped=1" in warnings[0]
        assert "total_dropped=3" in warnings[1]
        assert not any("prestart 6" in warning for warning in warnings)
        assert adapter.diagnostics()["prestart_overflow_dropped"] == 3
    finally:
        await adapter.stop()


async def test_retired_start_callback_cannot_enter_replacement_lifetime(
    inbound_collector, make_adapter_context
):
    """A retired startup's callback must not feed a replacement lifecycle.

    The old session's callback fires while the replacement start is in
    flight; the lifecycle fence must reject it so only the replacement
    session's own frames enter the new buffer window.
    """
    adapter = MeshCoreAdapter(_make_config())
    ctx = make_adapter_context(_CONFIG_ID)
    entered, release = asyncio.Event(), asyncio.Event()
    old_callbacks = []

    async def old_start(callback):
        old_callbacks.append(callback)
        callback(_chan_packet(0))
        entered.set()
        await release.wait()
        callback(_chan_packet(2))

    async def new_start(callback):
        old_callbacks[0](_chan_packet(9))
        callback(_chan_packet(1))

    with _patch_session(_racing_session_mock(old_start)):
        first = asyncio.create_task(adapter.start(ctx))
        await entered.wait()
    try:
        await adapter.stop()
        with _patch_session(_racing_session_mock(new_start)):
            await adapter.start(ctx)
        release.set()
        with pytest.raises(asyncio.CancelledError, match="stopped or superseded"):
            await first
        await _drain(adapter)
        assert _published_bodies(inbound_collector) == ["prestart 1"]
        assert adapter.diagnostics()["prestart_buffered"] == 1
    finally:
        release.set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        await adapter.stop()


async def test_concurrent_start_does_not_replace_inflight_owner(make_adapter_context):
    """A start while another is in flight is refused, leaving the owner.

    The re-entrancy guard raises before any counter reset, epoch bump or
    session replacement, so the in-flight startup owner is untouched.
    """
    adapter = MeshCoreAdapter(_make_config())
    ctx = make_adapter_context(_CONFIG_ID)
    entered, release = asyncio.Event(), asyncio.Event()

    async def mid_start(callback):
        entered.set()
        await release.wait()

    owner = _racing_session_mock(mid_start)
    with _patch_session(owner):
        first = asyncio.create_task(adapter.start(ctx))
        await entered.wait()
        try:
            with pytest.raises(RuntimeError, match="already in progress"):
                await adapter.start(ctx)
            assert adapter._session is owner
            owner.start.assert_awaited_once()
        finally:
            release.set()
            await first
            await adapter.stop()
