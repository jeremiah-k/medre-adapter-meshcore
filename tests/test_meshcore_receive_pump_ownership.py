"""Deterministic receive-pump ownership and lifecycle tests.

The three tests that shipped with the receive-pump branch
(``test_meshcore_receive_pump.py``) drive the pump through a
hand-built session double. They therefore never execute
``_connect_real`` and never observe which component owns the
native ``GET_NEXT_MSG`` request. The shared mock SDK helper
(``medre_meshcore_tests.helpers.meshcore_session``) also models an
SDK whose ``EventType`` has no ``MESSAGES_WAITING``/``NO_MORE_MSGS``,
so every mock-driven session test takes the *compatibility* branch.

These tests close that gap against both SDK shapes:

- the explicit shape (``MESSAGES_WAITING`` + ``NO_MORE_MSGS`` present),
  which is the shape the pinned SDK 2.3.15 actually has, and
- the compatibility shape, which has neither.

They pin the ownership invariants the branch claims:

- the explicit path starts no SDK auto-fetch worker and starts exactly
  one MEDRE receive pump;
- the compatibility path starts the SDK auto-fetch worker and starts no
  MEDRE receive pump;
- the SDK stays the only publisher: the pump never calls the message
  callback;
- the pump drains to ``NO_MORE_MSGS`` and then idles without spinning;
- each ``MESSAGES_WAITING`` notification drives one more drain;
- a client replaced mid-flight cannot fetch again;
- ``stop()`` cancels the pump before it disconnects the SDK client;
- a reconnect leaves exactly one live pump, bound to the new client;
- the capacity gate never holds the send lock while it waits.

Deterministic: mocked SDK, no radio, no Docker, no sleeps beyond
bounded event-loop yields.
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from medre_meshcore_tests.helpers.meshcore_session import (
    install_mock_module,
    remove_mock_module,
)

from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig


class ExplicitEventType(Enum):
    """The pinned SDK's event shape: explicit notification + empty-queue."""

    CONTACT_MSG_RECV = "contact_message"
    CHANNEL_MSG_RECV = "channel_message"
    CHANNEL_DATA_RECV = "channel_data"
    DISCONNECTED = "disconnected"
    MESSAGES_WAITING = "messages_waiting"
    NO_MORE_MSGS = "no_more_messages"
    ERROR = "command_error"
    CONTACTS = "contacts"
    SELF_INFO = "self_info"


class CompatibilityEventType(Enum):
    """An SDK without the explicit notification API."""

    CONTACT_MSG_RECV = "contact_message"
    CHANNEL_MSG_RECV = "channel_message"
    DISCONNECTED = "disconnected"
    ERROR = "command_error"
    CONTACTS = "contacts"
    SELF_INFO = "self_info"


def _mock_module(event_type: type[Enum]) -> tuple[MagicMock, AsyncMock]:
    """Build a mock SDK module exposing *event_type* and a client instance."""
    mock_mc = MagicMock()
    mock_mc.EventType = event_type
    instance = AsyncMock()
    instance.disconnect = AsyncMock()
    instance.subscribe = MagicMock(return_value=MagicMock())
    instance.unsubscribe = MagicMock()
    commands = AsyncMock()
    commands.get_msg = AsyncMock(
        return_value=SimpleNamespace(type=ExplicitEventType.NO_MORE_MSGS)
    )
    instance.commands = commands
    instance.self_info = {}
    instance.start_auto_message_fetching = AsyncMock()
    instance.stop_auto_message_fetching = AsyncMock()
    mock_mc.MeshCore = MagicMock(return_value=instance)
    mock_mc.MeshCore.create_tcp = AsyncMock(return_value=instance)
    return mock_mc, instance


def _session() -> MeshCoreSession:
    return MeshCoreSession(
        MeshCoreConfig(
            adapter_id="pump-ownership",
            connection_type="tcp",
            host="127.0.0.1",
            port=1,
        ),
        "pump-ownership",
    )


def _subscribed_types(instance: AsyncMock) -> list[Any]:
    return [call.args[0] for call in instance.subscribe.call_args_list if call.args]


def _notification_callback(instance: AsyncMock, event_type: type[Enum]) -> Any:
    """Return the callback registered for the explicit wait notification."""
    for call in instance.subscribe.call_args_list:
        if call.args and call.args[0] is event_type.MESSAGES_WAITING:
            return call.args[1]
    raise AssertionError("no MESSAGES_WAITING subscription was registered")


async def _started_session(
    event_type: type[Enum],
) -> tuple[MeshCoreSession, MagicMock, AsyncMock]:
    """Start a session against a mock SDK with *event_type*."""
    mock_mc, instance = _mock_module(event_type)
    install_mock_module(mock_mc)
    session = _session()
    with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
        await session.start(lambda _payload: None)
    # Let the pump task take its first steps.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    return session, mock_mc, instance


@pytest.mark.asyncio
async def test_explicit_path_is_the_sole_native_fetch_owner() -> None:
    """The explicit path starts no SDK auto-fetch worker and one MEDRE pump."""
    session, _mock_mc, instance = await _started_session(ExplicitEventType)
    try:
        assert instance.start_auto_message_fetching.await_count == 0
        assert ExplicitEventType.MESSAGES_WAITING in _subscribed_types(instance)
        pump = session._fetch_task
        assert pump is not None
        assert not pump.done()
        # The MEDRE pump is the only component issuing GET_NEXT_MSG.
        assert instance.commands.get_msg.await_count >= 1
    finally:
        await session.stop()
        remove_mock_module()


@pytest.mark.asyncio
async def test_compatibility_path_starts_no_medre_receive_pump() -> None:
    """The compatibility path leaves fetching to the SDK and runs no pump."""
    session, _mock_mc, instance = await _started_session(CompatibilityEventType)
    try:
        assert instance.start_auto_message_fetching.await_count == 1
        assert session._fetch_task is None
        assert session._fetch_wake is None
        assert instance.commands.get_msg.await_count == 0
    finally:
        await session.stop()
        remove_mock_module()


@pytest.mark.asyncio
async def test_pump_never_publishes_fetch_results() -> None:
    """SDK events are the only publication path; the pump does not publish."""
    published: list[dict[str, Any]] = []
    mock_mc, instance = _mock_module(ExplicitEventType)
    install_mock_module(mock_mc)
    session = _session()
    try:
        with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
            await session.start(published.append)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert instance.commands.get_msg.await_count >= 1
        assert published == []
    finally:
        await session.stop()
        remove_mock_module()


@pytest.mark.asyncio
async def test_pump_idles_after_empty_queue_without_spinning() -> None:
    """After NO_MORE_MSGS the pump stops fetching and waits for a wakeup."""
    mock_mc, instance = _mock_module(ExplicitEventType)
    install_mock_module(mock_mc)
    session = _session()
    try:
        with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
            await session.start(lambda _payload: None)
        pump = session._fetch_task
        assert pump is not None
        await asyncio.sleep(0.02)
        calls = instance.commands.get_msg.await_count
        assert calls == 1
        # The pump is alive and idle, not spinning on GET_NEXT_MSG.
        assert not pump.done()
        await asyncio.sleep(0.02)
        assert instance.commands.get_msg.await_count == calls
    finally:
        await session.stop()
        remove_mock_module()


@pytest.mark.asyncio
async def test_each_notification_drives_exactly_one_more_drain() -> None:
    """Repeated MESSAGES_WAITING notifications each drive one further drain."""
    mock_mc, instance = _mock_module(ExplicitEventType)
    install_mock_module(mock_mc)
    session = _session()
    try:
        with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
            await session.start(lambda _payload: None)
        await asyncio.sleep(0.02)
        calls = instance.commands.get_msg.await_count
        notify = _notification_callback(instance, ExplicitEventType)
        for _ in range(3):
            await notify(SimpleNamespace())
            await asyncio.sleep(0.02)
        assert instance.commands.get_msg.await_count == calls + 3
    finally:
        await session.stop()
        remove_mock_module()


@pytest.mark.asyncio
async def test_replaced_client_cannot_fetch_again() -> None:
    """A client replaced mid-flight cannot issue another native fetch."""
    mock_mc, instance = _mock_module(ExplicitEventType)
    install_mock_module(mock_mc)
    session = _session()
    try:
        with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
            await session.start(lambda _payload: None)
        pump = session._fetch_task
        assert pump is not None
        await asyncio.sleep(0.02)
        calls = instance.commands.get_msg.await_count

        # Replace the client underneath the running pump.
        _replacement_module, replacement_instance = _mock_module(ExplicitEventType)
        session._meshcore = replacement_instance
        await asyncio.sleep(0.02)

        assert instance.commands.get_msg.await_count == calls
        assert replacement_instance.commands.get_msg.await_count == 0
        # The pump parks on the retired client's wake event, which the
        # stale client can no longer set; only _stop_receive_pump()
        # retires it.  Either way no further native fetch is issued.
        assert not pump.cancelled()
    finally:
        await session.stop()
        remove_mock_module()


@pytest.mark.asyncio
async def test_stop_cancels_the_pump_before_disconnecting_the_sdk() -> None:
    """The pump is cancelled and finished before the SDK client is torn down."""
    order: list[str] = []
    mock_mc, instance = _mock_module(ExplicitEventType)
    instance.disconnect = AsyncMock(side_effect=lambda: order.append("disconnect"))
    install_mock_module(mock_mc)
    session = _session()
    try:
        with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
            await session.start(lambda _payload: None)
        pump = session._fetch_task
        assert pump is not None

        original_cancel = pump.cancel

        def record_cancel() -> bool:
            order.append("pump-cancel")
            return original_cancel()

        pump.cancel = record_cancel  # type: ignore[method-assign]
        await session.stop()
        assert order == ["pump-cancel", "disconnect"]
        assert pump.done()
        assert session._fetch_task is None
        assert session._fetch_wake is None
    finally:
        remove_mock_module()


@pytest.mark.asyncio
async def test_reconnect_leaves_exactly_one_live_pump_on_the_new_client() -> None:
    """A reconnect replaces the pump instead of adding a second one."""
    mock_mc, instance = _mock_module(ExplicitEventType)
    install_mock_module(mock_mc)
    session = _session()
    try:
        with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
            await session.start(lambda _payload: None)
        first = session._fetch_task
        assert first is not None
        await asyncio.sleep(0.02)

        # Replace the client the way a reconnect does.
        await session._cleanup_stale_sdk()
        assert first.done()
        assert session._fetch_task is None

        _replacement_module, replacement_instance = _mock_module(ExplicitEventType)
        mock_mc.MeshCore.create_tcp = AsyncMock(return_value=replacement_instance)
        with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
            await session._connect_real()
        session._start_receive_pump()
        second = session._fetch_task
        assert second is not None
        assert second is not first
        assert not second.done()
        assert first.done()
        # The replacement client owns the native request from now on.
        await asyncio.sleep(0.02)
        assert replacement_instance.commands.get_msg.await_count >= 1
    finally:
        await session.stop()
        remove_mock_module()


@pytest.mark.asyncio
async def test_capacity_gate_waits_without_holding_the_send_lock() -> None:
    """A saturated ingress stalls the pump but never holds the send lock."""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def capacity() -> None:
        entered.set()
        await release.wait()

    session = MeshCoreSession.__new__(MeshCoreSession)
    commands = AsyncMock()
    commands.get_msg = AsyncMock(
        return_value=SimpleNamespace(type=ExplicitEventType.NO_MORE_MSGS)
    )
    client = SimpleNamespace(commands=commands)
    session._meshcore = client
    session._sdk_module = SimpleNamespace(EventType=ExplicitEventType)
    session._diag = SimpleNamespace(connected=True)
    session._stop_requested = False
    session._send_lock = asyncio.Lock()
    session._wait_for_receive_capacity = capacity
    session._release_receive_permit = None
    session._fetch_inbound_seen = 0
    session._fetch_wake = asyncio.Event()
    session._fetch_wake.set()
    session._fetch_task = None
    session._adapter_id = "pump-ownership"
    session._logger = logging.getLogger(__name__)
    session._set_connected = lambda value: setattr(session._diag, "connected", value)
    session._ensure_recovery_task = Mock()

    session._start_receive_pump()
    await asyncio.wait_for(entered.wait(), 2)
    await asyncio.sleep(0.02)
    assert commands.get_msg.await_count == 0
    assert not session._send_lock.locked()
    release.set()
    await asyncio.sleep(0.02)
    assert commands.get_msg.await_count == 1
    await session._stop_receive_pump()
