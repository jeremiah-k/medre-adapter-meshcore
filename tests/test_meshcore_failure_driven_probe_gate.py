"""A failure-driven retirement gates the next connection's first fetch.

The node's fetch is a destructive pop with no acknowledgement in the
protocol. A connection that follows a failure-driven retirement — a fetch or
probe failure, or a transport loss while a fetch round was in flight — may
be reconnecting into a link whose server-to-client direction is broken. If
such a connection fetches immediately, every round pops one queued message
into that broken direction and the message is gone. So that connection must
earn the right to fetch: before its first fetch it sends one liveness probe
(the same SDK device query the parked pump uses) and waits for the answer.
Answered — the link delivers — the backlog drains normally from then on.
Unanswered — the pump issues no fetch at all and retires through the
existing path, so a still-broken link loses nothing.

A fresh start and a reconnect after a clean disconnect (the peer closed, the
host dropped the link, the adapter was stopped; no fetch was in flight, so
nothing was popped into the dead link) keep fetching immediately.

Deterministic SDK-shape tests: no network, no hardware, no fixed sleeps —
condition-based waiting only. The gate tests pin that no fetch precedes
an answered probe on a gated connection, and that clean starts and clean
reconnects fetch immediately.
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum, auto
from types import SimpleNamespace
from unittest.mock import Mock

from medre.adapters.meshcore.session import MeshCoreSession
from tests.medre_meshcore_tests.helpers.async_utils import wait_until


class EventType(Enum):
    CHANNEL_MSG_RECV = auto()
    CONTACT_MSG_RECV = auto()
    DEVICE_INFO = auto()
    NO_MORE_MSGS = auto()
    ERROR = auto()


def event(event_type: Enum, payload: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(type=event_type, payload=payload or {})


def session_with_fetch(get_msg, device_query=None, release=None):
    session = MeshCoreSession.__new__(MeshCoreSession)
    client = SimpleNamespace(
        commands=SimpleNamespace(get_msg=get_msg, send_device_query=device_query)
    )
    session._meshcore = client
    session._sdk_module = SimpleNamespace(EventType=EventType)
    session._diag = SimpleNamespace(connected=True)
    # The retirement path sanitizes through the session config.
    session._config = SimpleNamespace(ble_pin=None)
    session._stop_requested = False
    session._started = True
    session._send_lock = asyncio.Lock()
    session._wait_for_receive_capacity = None
    session._release_receive_permit = release
    session._fetch_inbound_seen = 0
    session._fetch_wake = asyncio.Event()
    session._fetch_wake.set()
    session._fetch_task = None
    session._fetch_round_in_flight = False
    session._pump_silent_rounds = 0
    session._adapter_id = "test"
    session._logger = logging.getLogger(__name__)
    session._set_connected = lambda value: setattr(session._diag, "connected", value)
    session._ensure_recovery_task = Mock()
    # A reconnect that follows a failure-driven retirement starts gated;
    # tests flip this to model the retirement they came from.
    session._receive_failure_pending = False
    return session


def fast_gate(monkeypatch) -> None:
    """Make the park path and probe cadence fast and observable."""
    monkeypatch.setattr(
        "medre.adapters.meshcore.session._FETCH_IDLE_REISSUE_DELAY", 0.01
    )
    monkeypatch.setattr("medre.adapters.meshcore.session._FETCH_ROUND_TIMEOUT", 0.2)
    monkeypatch.setattr("medre.adapters.meshcore.session._FETCH_IDLE_SILENT_ROUNDS", 2)
    monkeypatch.setattr(
        "medre.adapters.meshcore.session._PROBE_IDLE_INTERVAL", 0.05, raising=False
    )


async def test_unanswered_probe_gates_every_fetch_out_of_a_failed_reconnect(
    monkeypatch,
) -> None:
    """A reconnect into a swallowing link fetches nothing and retires instead.

    The link answers the connect handshake but swallows replies (the
    device query is never answered). The gated pump must issue zero
    fetches — each one would pop a queued message into the broken
    direction — and retire through the existing path, leaving the node's
    queue unchanged.
    """
    fast_gate(monkeypatch)
    get_msg_calls = 0
    probe_calls = 0

    async def get_msg(*, timeout):
        nonlocal get_msg_calls
        get_msg_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        # The stalled direction never answers the probe either.
        return event(EventType.ERROR, {"reason": "no_event_received"})

    session = session_with_fetch(get_msg, send_device_query)
    # The previous connection retired through a probe failure.
    session._receive_failure_pending = True
    session._start_receive_pump()
    try:
        assert await wait_until(
            lambda: session._fetch_task.done(), timeout=2
        ), "the gated pump must retire on the unanswered probe"
        assert (
            get_msg_calls == 0
        ), "no destructive pop may be issued before the probe is answered"
        assert probe_calls >= 1, "the gate must probe before fetching"
        assert session._diag.connected is False
        session._ensure_recovery_task.assert_called_once()
        # The retirement stays failure-driven: the next connection is
        # gated too until one of its probes is answered.
        assert session._receive_failure_pending is True
    finally:
        await session._stop_receive_pump()


async def test_answered_probe_lets_the_failed_reconnect_drain_once_in_order(
    monkeypatch,
) -> None:
    """After one answered probe the backlog drains once, in order."""
    fast_gate(monkeypatch)
    probe_calls = 0
    first_fetch_after_probe = False
    backlog = [
        event(EventType.CHANNEL_MSG_RECV, {"text": "m1"}),
        event(EventType.CHANNEL_MSG_RECV, {"text": "m2"}),
        event(EventType.CHANNEL_MSG_RECV, {"text": "m3"}),
        event(EventType.NO_MORE_MSGS, {}),
    ]
    fetch_order: list[str | None] = []

    async def get_msg(*, timeout):
        nonlocal first_fetch_after_probe
        first_fetch_after_probe = probe_calls == 1
        assert backlog, "the queue must not be fetched more than once"
        item = backlog.pop(0)
        fetch_order.append(item.payload.get("text"))
        return item

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    session._receive_failure_pending = True
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: not backlog, timeout=2)
        assert session._diag.connected is True
        assert not session._ensure_recovery_task.called
        # Exactly one probe earned the right to fetch, before the first
        # destructive pop; then the whole backlog drained once, in order.
        assert probe_calls == 1, "one answered probe must precede the first fetch"
        assert first_fetch_after_probe
        assert fetch_order == ["m1", "m2", "m3", None]
        # The connection is proven: it is no longer failure-driven.
        assert session._receive_failure_pending is False
    finally:
        await session._stop_receive_pump()


async def test_clean_reconnect_fetches_immediately_without_a_probe(
    monkeypatch,
) -> None:
    """A reconnect after a clean disconnect never delays on a probe."""
    fast_gate(monkeypatch)
    probe_calls = 0
    backlog = [
        event(EventType.CHANNEL_MSG_RECV, {"text": "c1"}),
        event(EventType.NO_MORE_MSGS, {}),
    ]

    async def get_msg(*, timeout):
        assert backlog, "the queue must not be fetched more than once"
        item = backlog.pop(0)
        return item

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    # The peer closed the link while the pump was parked: nothing was in
    # flight, so this reconnect is clean and fetches straight away.
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: not backlog, timeout=2)
        assert probe_calls == 0, "a clean reconnect must not probe"
        assert session._diag.connected is True
        assert not session._ensure_recovery_task.called
    finally:
        await session._stop_receive_pump()


async def test_transport_loss_during_a_fetch_gates_the_next_connection(
    monkeypatch,
) -> None:
    """A disconnect while a fetch round is in flight is unclean."""
    fast_gate(monkeypatch)
    get_msg_calls = 0
    probe_calls = 0

    async def get_msg(*, timeout):
        nonlocal get_msg_calls
        get_msg_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    session = session_with_fetch(get_msg, send_device_query)
    session._receive_failure_pending = False
    # A fetch round is outstanding when the transport dies.
    session._fetch_round_in_flight = True
    await session._on_disconnect_event(event(EventType.ERROR, {}))
    assert (
        session._receive_failure_pending is True
    ), "a transport loss during an in-flight fetch must gate the next connection"
    session._ensure_recovery_task.assert_called_once()
    # The reconnect lands in a link that answers the handshake but swallows
    # replies: the gate must pop nothing.
    session._diag.connected = True
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: session._fetch_task.done(), timeout=2)
        assert get_msg_calls == 0
        assert probe_calls >= 1
        assert session._ensure_recovery_task.call_count == 2
    finally:
        await session._stop_receive_pump()


async def test_transport_loss_after_silent_fetch_rounds_gates_the_next_connection(
    monkeypatch,
) -> None:
    """A disconnect with unanswered fetch rounds behind it is unclean.

    The pinned SDK declares a link dead after several frames went out with
    nothing coming back; the transport loss can land while the pump is
    parked on those silent rounds — no fetch round in flight — yet the
    rounds already popped messages whose frames never arrived. The next
    connection must prove the link delivers before it pops anything.
    """
    fast_gate(monkeypatch)
    get_msg_calls = 0
    probe_calls = 0

    async def get_msg(*, timeout):
        nonlocal get_msg_calls
        get_msg_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    session = session_with_fetch(get_msg, send_device_query)
    session._receive_failure_pending = False
    # The drain already went silent — destructive pops whose frames never
    # arrived — and the pump is parked when the transport dies. No fetch
    # round is in flight.
    session._pump_silent_rounds = 4
    session._fetch_round_in_flight = False
    await session._on_disconnect_event(event(EventType.ERROR, {}))
    assert (
        session._receive_failure_pending is True
    ), "a transport loss after silent fetch rounds must gate the next connection"
    # The reconnect lands in a link that answers the handshake but swallows
    # replies: the gate must pop nothing.
    session._diag.connected = True
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: session._fetch_task.done(), timeout=2)
        assert get_msg_calls == 0
        assert probe_calls >= 1
    finally:
        await session._stop_receive_pump()


async def test_transport_loss_on_a_proven_connection_stays_clean(monkeypatch) -> None:
    """A disconnect with nothing unanswered behind it fetches immediately."""
    fast_gate(monkeypatch)
    probe_calls = 0
    backlog = [
        event(EventType.CHANNEL_MSG_RECV, {"text": "after-clean-loss"}),
        event(EventType.NO_MORE_MSGS, {}),
    ]

    async def get_msg(*, timeout):
        assert backlog, "the queue must not be fetched more than once"
        item = backlog.pop(0)
        return item

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    session._receive_failure_pending = False
    session._pump_silent_rounds = 0
    session._fetch_round_in_flight = False
    # The peer closed a connection whose every fetch was answered.
    await session._on_disconnect_event(event(EventType.ERROR, {}))
    assert session._receive_failure_pending is False
    session._diag.connected = True
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: not backlog, timeout=2)
        assert probe_calls == 0, "a clean loss must not gate the next connection"
    finally:
        await session._stop_receive_pump()


async def test_fetch_failure_retirement_marks_the_next_connection(monkeypatch) -> None:
    """A fetch failure that retires leaves the next connection gated."""
    monkeypatch.setattr("medre.adapters.meshcore.session._FETCH_ROUND_TIMEOUT", 0.2)
    probe_calls = 0

    async def get_msg(*, timeout):
        # A node ERROR reply — not an elapsed window — retires.
        return event(EventType.ERROR, {"error": "bad request"})

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    session._receive_failure_pending = False
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: session._fetch_task.done(), timeout=2)
        assert session._diag.connected is False
        assert (
            session._receive_failure_pending is True
        ), "a fetch-failure retirement must mark the next connection"
    finally:
        await session._stop_receive_pump()
