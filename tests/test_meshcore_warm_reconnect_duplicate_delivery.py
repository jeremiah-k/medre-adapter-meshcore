"""Deterministic warm-reconnect duplicate-delivery tests.

Real-radio evidence (BLE, LilyGo T-Beam companion v1.17.1) showed
that after a warm reconnect every inbound frame is dispatched twice at
the SDK layer (two identical ``CHANNEL_MSG_RECV`` events ~1 ms apart;
fresh connects never double). The raw logs attribute the doubling to
the host BLE stack, not to two fetchers: BlueZ emits each notification
twice, and ``meshcore/ble_cx.py``'s ``_cleanup_stale_client`` docstring
describes exactly this (a stale GATT notify registration surviving at
the BlueZ D-Bus level). The pinned SDK 2.3.15 has no connect-time
pending-messages fetch — ``MeshCore.connect()`` only starts the
dispatcher, connects, and sends APP_START — so the receive pump is
already the only fetcher, and the ``_send_lock`` already keeps only
one ``GET_NEXT_MSG`` in flight.

These tests pin the invariants that make the doubled delivery harmless:

- across a warm reconnect whose fetch rounds resolve from duplicated
  frames, at most one ``get_msg`` command is ever in flight, the old
  client never fetches again, and the whole backlog still drains to
  ``NO_MORE_MSGS`` with exactly one extra round per duplicate;
- every duplicate that reaches ingress is an exact payload duplicate
  (same derived message identity), which the adapter's packet-id dedup
  collapses to one stored event — asserted here through the real
  ``MeshCoreAdapter._on_message`` ingress path.

Evidence level: mocked SDK, no radio, no Docker, no fixed sleeps.
"""

from __future__ import annotations

import asyncio
from enum import Enum
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from medre_meshcore_tests.helpers.async_utils import wait_until
from medre_meshcore_tests.helpers.meshcore_session import (
    install_mock_module,
    remove_mock_module,
)

from medre.adapters.meshcore.adapter import MeshCoreAdapter
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


_BACKLOG = [
    ("W6-DUP one", 1795645545),
    ("W6-DUP two", 1795645546),
    ("W6-DUP three", 1795645547),
]


def _channel_event(text: str, timestamp: int) -> SimpleNamespace:
    """One inbound channel-message frame as the SDK dispatches it."""
    return SimpleNamespace(
        type=ExplicitEventType.CHANNEL_MSG_RECV,
        payload={
            "type": "CHAN",
            "SNR": 9.25,
            "channel_idx": 1,
            "path_hash_mode": 0,
            "path_len": 0,
            "txt_type": 0,
            "sender_timestamp": timestamp,
            "text": f"MEDRE-MC-B: {text}",
        },
        attributes={"channel_idx": 1, "txt_type": 0},
    )


def _no_more_event() -> SimpleNamespace:
    return SimpleNamespace(
        type=ExplicitEventType.NO_MORE_MSGS,
        payload={"messages_available": False},
        attributes={},
    )


def _doubled_delivery_client(
    backlog: list[tuple[str, int]],
) -> tuple[AsyncMock, dict[str, int]]:
    """Build a mock SDK client that replays the observed warm-reconnect
    behaviour: every message frame is delivered twice (the second delivery
    resolving the next waiting fetch round) before ``NO_MORE_MSGS`` ends
    the drain. Tracks fetch in-flight depth for the ownership assertion."""
    instance = AsyncMock()
    instance.disconnect = AsyncMock()
    instance.unsubscribe = MagicMock()
    instance.self_info = {}
    instance.start_auto_message_fetching = AsyncMock()
    instance.stop_auto_message_fetching = AsyncMock()

    script: list[SimpleNamespace] = []
    for text, timestamp in backlog:
        script.append(_channel_event(text, timestamp))
        script.append(_channel_event(text, timestamp))
    script.append(_no_more_event())

    state = {"index": 0, "inflight": 0, "max_inflight": 0}
    channel_callbacks: list[Any] = []

    async def get_msg(timeout: Any = None) -> SimpleNamespace:
        state["inflight"] += 1
        state["max_inflight"] = max(state["max_inflight"], state["inflight"])
        try:
            # One loop tick: any second fetch issued before this one
            # returns would register as in-flight depth 2.
            await asyncio.sleep(0)
            event = script[state["index"]]
            state["index"] += 1
            if event.type is ExplicitEventType.CHANNEL_MSG_RECV:
                # The SDK dispatches every fetched frame to the session's
                # ingress subscription, duplicates included.
                for callback in list(channel_callbacks):
                    await callback(event)
            return event
        finally:
            state["inflight"] -= 1

    commands = AsyncMock()
    commands.get_msg = AsyncMock(side_effect=get_msg)
    instance.commands = commands

    def subscribe(event_type: Any, callback: Any, *args: Any, **kwargs: Any) -> Any:
        if event_type is ExplicitEventType.CHANNEL_MSG_RECV:
            channel_callbacks.append(callback)
        return MagicMock()

    instance.subscribe = MagicMock(side_effect=subscribe)
    return instance, state


def _empty_queue_client() -> tuple[AsyncMock, AsyncMock]:
    """A mock SDK client whose queue is empty (fresh-connect shape)."""
    instance = AsyncMock()
    instance.disconnect = AsyncMock()
    instance.subscribe = MagicMock(return_value=MagicMock())
    instance.unsubscribe = MagicMock()
    instance.self_info = {}
    instance.start_auto_message_fetching = AsyncMock()
    instance.stop_auto_message_fetching = AsyncMock()
    commands = AsyncMock()
    commands.get_msg = AsyncMock(return_value=_no_more_event())
    instance.commands = commands
    return instance, commands


def _mock_module(instance: AsyncMock) -> MagicMock:
    mock_mc = MagicMock()
    mock_mc.EventType = ExplicitEventType
    mock_mc.MeshCore = MagicMock(return_value=instance)
    mock_mc.MeshCore.create_tcp = AsyncMock(return_value=instance)
    return mock_mc


def _session() -> MeshCoreSession:
    return MeshCoreSession(
        MeshCoreConfig(
            adapter_id="pump-warm-dup",
            connection_type="tcp",
            host="127.0.0.1",
            port=1,
        ),
        "pump-warm-dup",
    )


async def test_warm_reconnect_doubled_delivery_stays_a_single_fetcher() -> None:
    """A warm reconnect with duplicated frame delivery never overlaps two
    ``get_msg`` commands, never revives the old client, and still drains
    the whole backlog to ``NO_MORE_MSGS``."""
    published: list[dict[str, Any]] = []

    fresh_instance, fresh_commands = _empty_queue_client()
    mock_mc = _mock_module(fresh_instance)
    install_mock_module(mock_mc)
    session = _session()
    try:
        with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
            await session.start(published.append)
        assert await wait_until(lambda: fresh_commands.get_msg.await_count == 1)
        assert await wait_until(lambda: session._fetch_task is not None)
        first_pump = session._fetch_task
        assert first_pump is not None

        # Warm reconnect: retire the old client and connect a new one the
        # way the recovery loop does.
        await session._cleanup_stale_sdk()
        assert first_pump.done()

        replacement, state = _doubled_delivery_client(_BACKLOG)
        mock_mc.MeshCore.create_tcp = AsyncMock(return_value=replacement)
        with patch("medre.adapters.meshcore.session.HAS_MESHCORE", True):
            await session._connect_real()
        session._start_receive_pump()

        expected_rounds = 2 * len(_BACKLOG) + 1
        assert await wait_until(
            lambda: state["index"] == expected_rounds
        ), f"drain stalled at round {state['index']} of {expected_rounds}"

        # The ownership invariant: one GET_NEXT_MSG in flight, ever.
        assert state["max_inflight"] == 1
        # The drain ended on the node's empty-queue answer.
        assert replacement.commands.get_msg.await_count == expected_rounds
        # The replaced client issued no further native fetch.
        assert fresh_commands.get_msg.await_count == 1
        # The doubled delivery did not retire the connection.
        assert session._diag.connected
        assert session._reconnect_task is None

        # Every queued message reached ingress exactly twice — the two
        # deliveries being exact payload duplicates — and nothing else.
        expected_payloads = [
            _channel_event(text, timestamp).payload for text, timestamp in _BACKLOG
        ]
        assert published == [p for p in expected_payloads for _ in range(2)]
    finally:
        await session.stop()
        remove_mock_module()


async def test_doubled_dispatch_pair_stores_exactly_once(
    make_adapter_context: Any,
    inbound_collector: Any,
) -> None:
    """The duplicate dispatch pair collapses to one stored event through
    the adapter's real ingress path (packet-id keyed dedup)."""
    config = MeshCoreConfig(adapter_id="mc-warm-dup", connection_type="fake")
    adapter = MeshCoreAdapter(config)
    ctx = make_adapter_context("mc-warm-dup")
    await adapter.start(ctx)

    packet = _channel_event("W6-DUP one", 1795645545).payload

    adapter._on_message(dict(packet))
    pending = tuple(adapter._background_tasks)
    assert len(pending) == 1
    await asyncio.gather(*pending)
    assert len(inbound_collector.events) == 1

    # The duplicate delivery ~1 ms later: identical payload.
    adapter._on_message(dict(packet))
    assert not adapter._background_tasks
    assert len(inbound_collector.events) == 1

    await adapter.stop()
