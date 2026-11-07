"""Session teardown fencing tests for the MeshCore adapter.

Two teardown defects and their deterministic guards:

- ``MeshCoreSession.stop()`` must clear ``_message_callback`` in its
  synchronous prologue — before any await — so inbound delivery ends
  when stop() begins: an SDK event dispatched during the teardown
  awaits (or after a stop cancelled mid-await) can never reach a
  retired callback.
- ``MeshCoreAdapter.start()`` must refuse while a cancelled startup's
  partial session is still retained: constructing a replacement would
  orphan the retained owner without ever running its ``stop()`` — for
  BLE skipping the BlueZ held-link release documented in
  ``session.stop()`` — and the retired session must still be freed by
  reference counting, without a GC pass.

Deterministic: fake mode and mocked SDK, no hardware, no sleeps.
"""

from __future__ import annotations

import asyncio
import gc
import sys
import weakref
from collections.abc import Callable
from unittest.mock import AsyncMock, Mock, patch

import pytest
from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterContext
from medre_meshcore_tests.helpers.meshcore_session import (
    MockEvent,
    MockEventType,
    build_mock_meshcore_module,
)

_CONFIG_ID = "mc-teardown"
_BLE_ADDRESS = "AA:BB:CC:DD:EE:FF"


def _make_config(**overrides: object) -> MeshCoreConfig:
    """Build the adapter config, overriding any default field."""
    defaults: dict = {"adapter_id": _CONFIG_ID}
    defaults.update(overrides)
    return MeshCoreConfig(**defaults)


def _sdk_dm_event(text: str) -> MockEvent:
    """Make a relayable direct-message event in the SDK dispatch shape."""
    return MockEvent(
        event_type=MockEventType.CONTACT_MSG_RECV,
        payload={
            "text": text,
            "pubkey_prefix": "cafe00",
            "sender_timestamp": 77,
            "type": "PRIV",
            "txt_type": 0,
        },
    )


def _clean_session_mock() -> AsyncMock:
    """Build a patched MeshCoreSession whose start() completes cleanly."""
    mock_inst = AsyncMock()
    mock_inst.diagnostics = Mock(return_value={"connected": True})
    mock_inst.resolve_contact_label = Mock(return_value=None)
    mock_inst.stop = AsyncMock()
    mock_inst.start = AsyncMock()
    return mock_inst


class _ParkedSession(MeshCoreSession):
    """A real MeshCoreSession whose start() parks mid-connect.

    Mimics a startup cancelled while the SDK client is already held and
    the inbound callback is live: the adapter's cancellation handler
    retains this partial owner for core's bounded startup rollback.
    """

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.parked = asyncio.Event()

    async def start(
        self,
        message_callback: Callable[[dict], None] | None = None,
        recover_on_failure: bool = True,
    ) -> None:
        self._message_callback = message_callback
        self._meshcore = AsyncMock()  # the held SDK client (BLE link hazard)
        self._set_connected(True)
        self.parked.set()
        await asyncio.Event().wait()


async def _cancel_start_retaining_owner(
    adapter: MeshCoreAdapter, ctx: AdapterContext
) -> _ParkedSession:
    """Cancel an in-flight start, leaving its partial session retained.

    Drives ``start()`` to the point where the real session is installed,
    holds an SDK client, and ``session.start()`` is still parking, then
    cancels the caller task.  Returns the retained partial owner — the
    session core's bounded startup rollback would later ``stop()``.
    """
    created: list[_ParkedSession] = []
    constructed = asyncio.Event()

    def _factory(**kwargs: object) -> _ParkedSession:
        session = _ParkedSession(**kwargs)
        created.append(session)
        constructed.set()
        return session

    with patch("medre.adapters.meshcore.adapter.MeshCoreSession", side_effect=_factory):
        first = asyncio.create_task(adapter.start(ctx))
        try:
            await asyncio.wait_for(constructed.wait(), 5)
            owner = created[0]
            await asyncio.wait_for(owner.parked.wait(), 5)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        finally:
            if not first.done():
                first.cancel()
                await asyncio.gather(first, return_exceptions=True)
    assert adapter._session is owner
    assert adapter._starting is False
    return owner


# ===================================================================
# Session: stop() fences inbound delivery from its synchronous prologue
# ===================================================================


async def test_stop_clears_message_callback_and_fences_late_sdk_events() -> None:
    """After stop(), _on_sdk_event never reaches the retired callback.

    stop() must clear ``_message_callback`` in its synchronous prologue
    so the session's own contract ("disconnect from MeshCore and release
    all resources") holds: inbound delivery ends when stop begins, not
    when the SDK unsubscribe eventually completes.
    """
    mock_mc, _mock_inst = build_mock_meshcore_module()
    session = MeshCoreSession(
        _make_config(connection_type="tcp", host="localhost"), "teardown-clear"
    )
    received: list[dict] = []

    async def callback(pkt: dict) -> None:
        received.append(pkt)

    with (
        patch("medre.adapters.meshcore.session.HAS_MESHCORE", True),
        patch.dict(sys.modules, {"meshcore": mock_mc}),
    ):
        await session.start(callback)
        assert session._message_callback is not None
        await session.stop()

    assert session._message_callback is None
    await session._on_sdk_event(_sdk_dm_event("after stop"))
    assert received == []


async def test_stop_cancelled_mid_await_still_fences_delivery() -> None:
    """A stop() cancelled mid-await still leaves the session inert.

    The callback clear is synchronous, before any await: cancelling the
    stop task while it is parked in the SDK's ``stop_auto_message_
    fetching`` seam cannot undo the fence, and later SDK events stay
    undelivered.
    """
    mock_mc, mock_inst = build_mock_meshcore_module()
    session = MeshCoreSession(
        _make_config(connection_type="tcp", host="localhost"), "teardown-cancelled"
    )
    received: list[dict] = []
    entered, release = asyncio.Event(), asyncio.Event()

    async def blocking_stop_fetch() -> None:
        entered.set()
        await release.wait()

    mock_inst.stop_auto_message_fetching = AsyncMock(side_effect=blocking_stop_fetch)

    async def callback(pkt: dict) -> None:
        received.append(pkt)

    with (
        patch("medre.adapters.meshcore.session.HAS_MESHCORE", True),
        patch.dict(sys.modules, {"meshcore": mock_mc}),
    ):
        await session.start(callback)
        stop_task = asyncio.create_task(session.stop())
        try:
            await asyncio.wait_for(entered.wait(), 5)
            stop_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await stop_task
        finally:
            release.set()

    assert session._message_callback is None
    await session._on_sdk_event(_sdk_dm_event("after cancelled stop"))
    assert received == []


# ===================================================================
# Adapter: start() refuses while a cancelled startup's owner is retained
# ===================================================================


async def test_start_after_cancelled_start_refuses_until_owner_retired(
    make_adapter_context,
) -> None:
    """A start over an unretired cancelled startup refuses before any mutation.

    Constructing a replacement session would overwrite ``self._session``
    and orphan the retained owner without ever running its ``stop()`` —
    for BLE skipping the BlueZ held-link release that feeds the
    historical intermittent "Failed to connect" on the next start.  The
    refusal must happen before the epoch bump, counter reset, buffer
    discard and context assignment, and ``stop()`` must then retire the
    owner exactly once before a fresh start can proceed.
    """
    adapter = MeshCoreAdapter(
        _make_config(connection_type="ble", ble_address=_BLE_ADDRESS)
    )
    ctx: AdapterContext = make_adapter_context(_CONFIG_ID)
    with patch.object(
        MeshCoreSession, "_disconnect_stale_ble_client", new_callable=AsyncMock
    ) as stale_drop:
        owner = await _cancel_start_retaining_owner(adapter, ctx)

        epoch_before = adapter._health_lifecycle_epoch
        buffered_before = adapter._prestart_buffered_count
        flushed_before = adapter._prestart_flushed_count
        published_before = adapter._inbound_published
        ctx_before = adapter.ctx

        with patch(
            "medre.adapters.meshcore.adapter.MeshCoreSession",
            side_effect=ConnectionError("replacement construction"),
        ) as replacement_factory:
            with pytest.raises(RuntimeError, match="session from an earlier start"):
                await adapter.start(ctx)

        # The refusal happened before any mutation: the replacement
        # session was never constructed and nothing was reset.
        replacement_factory.assert_not_called()
        assert adapter._health_lifecycle_epoch == epoch_before
        assert adapter._prestart_buffered_count == buffered_before
        assert adapter._prestart_flushed_count == flushed_before
        assert adapter._inbound_published == published_before
        assert not adapter._prestart_buffer
        assert adapter.ctx is ctx_before
        assert adapter._session is owner

        # stop() retires the retained owner exactly once, running the
        # BLE stale-link cleanup the orphaned-owner path would skip.
        await adapter.stop()
        stale_drop.assert_awaited_once_with(_BLE_ADDRESS)
        assert adapter._session is None
        assert owner._message_callback is None

        # A fresh start proceeds after the owner is retired.
        with patch(
            "medre.adapters.meshcore.adapter.MeshCoreSession",
            return_value=_clean_session_mock(),
        ):
            await adapter.start(ctx)
        assert adapter._started is True
        await adapter.stop()


# ===================================================================
# Adapter: the retired session is freed by reference counting alone
# ===================================================================


async def test_retired_session_freed_by_reference_count_without_gc(
    make_adapter_context,
) -> None:
    """The adapter drops the retired session without a GC pass.

    With the collector disabled, the retired session and its inbound
    callback must still be freed immediately when stop() ends: the
    callback cleared from the session and the session dropped from the
    adapter break every reference by plain refcounting.  A callback
    stored on the object it fences must not close over it — a captured
    session would form a reference cycle only a GC pass could collect.
    """
    gc.disable()
    try:
        adapter = MeshCoreAdapter(_make_config(connection_type="fake"))
        ctx: AdapterContext = make_adapter_context(_CONFIG_ID)
        await adapter.start(ctx)
        session = adapter._session
        assert session is not None
        callback_ref = weakref.ref(session._message_callback)
        session_ref = weakref.ref(session)
        assert session_ref() is not None

        await adapter.stop()

        # stop() cleared the callback: the retired callback object is
        # released deterministically instead of being pinned by the
        # surviving session object.
        assert callback_ref() is None
        del session
        assert session_ref() is None
        assert adapter._session is None
    finally:
        gc.enable()
