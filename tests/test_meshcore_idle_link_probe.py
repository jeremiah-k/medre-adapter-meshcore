"""Idle-link liveness probes for the parked receive pump.

A silently dead link — a peer that vanishes without FIN/RST — never fails a
fetch: the pump parks after its silent idle rounds and the session looks
connected forever. While the pump would otherwise sit parked, it MUST
periodically confirm the node is still reachable with a cheap local query
(the SDK device query, whose DEVICE_INFO reply cannot be confused with a
message event). A probe that goes unanswered is a transport failure:
the connection retires through the pump's ordinary retire path, which runs
the existing recovery loop exactly once. Only an elapsed-wait report or a
local failure to send is a silent-dead link; a reply the node itself sent —
a DEVICE_INFO answer, the wire's numeric error frame, or bytes that arrived
but failed to parse — proves the link is alive and never retires.

One probe runs at a time, never concurrent with a fetch, serialized with
sends on the session's send lock. Stop, retire and reconnect cancel the
probe with the pump; a late probe reply matches no subscription.

Deterministic SDK-shape tests: no network, no hardware, no fixed sleeps —
condition-based waiting only. The probe tests fail against the previous
park-forever behaviour, which never sent any probe.
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from enum import Enum, auto
from types import SimpleNamespace
from unittest.mock import Mock

from medre.adapters.meshcore.session import MeshCoreSession
from tests.medre_meshcore_tests.helpers.async_utils import wait_until


class EventType(Enum):
    CHANNEL_MSG_RECV = auto()
    CONTACT_MSG_RECV = auto()
    CHANNEL_DATA_RECV = auto()
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
    session._stop_requested = False
    session._send_lock = asyncio.Lock()
    session._fetch_round_in_flight = False
    session._pump_silent_rounds = 0
    session._receive_failure_pending = False
    session._wait_for_receive_capacity = None
    session._release_receive_permit = release
    session._fetch_inbound_seen = 0
    session._fetch_wake = asyncio.Event()
    session._fetch_wake.set()
    session._fetch_task = None
    session._adapter_id = "test"
    session._logger = logging.getLogger(__name__)
    session._set_connected = lambda value: setattr(session._diag, "connected", value)
    session._ensure_recovery_task = Mock()
    return session


def park_quietly(monkeypatch) -> None:
    """Make the silent-round park and the probe cadence fast and observable."""
    monkeypatch.setattr(
        "medre.adapters.meshcore.session._FETCH_IDLE_REISSUE_DELAY", 0.02
    )
    monkeypatch.setattr("medre.adapters.meshcore.session._FETCH_ROUND_TIMEOUT", 0.2)
    # raising=False lets this same suite run against a revision that does
    # not have the probe yet, so the tests fail on behaviour, not setup.
    monkeypatch.setattr(
        "medre.adapters.meshcore.session._PROBE_IDLE_INTERVAL", 0.05, raising=False
    )


async def test_silent_link_probe_fails_and_retires_with_exactly_one_recovery(
    monkeypatch,
) -> None:
    """A silent dead link is probed, then retired through the existing path."""
    park_quietly(monkeypatch)
    get_msg_calls = 0
    probe_calls = 0

    async def get_msg(*, timeout):
        nonlocal get_msg_calls
        get_msg_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        # The dead link never answers: the SDK reports an elapsed wait.
        return event(EventType.ERROR, {"reason": "no_event_received"})

    session = session_with_fetch(get_msg, send_device_query)
    session._start_receive_pump()
    try:
        # The pump parks after its silent rounds, then probes the idle link.
        assert await wait_until(
            lambda: probe_calls >= 1, timeout=2
        ), "a parked pump must probe the idle link instead of waiting forever"
        assert get_msg_calls >= 1
        # The failed probe is a transport failure: the ordinary retire path
        # runs, and recovery is armed exactly once.
        assert await wait_until(lambda: session._fetch_task.done(), timeout=2)
        assert session._diag.connected is False
        session._ensure_recovery_task.assert_called_once()
    finally:
        await session._stop_receive_pump()


async def test_slow_but_alive_link_probe_replies_and_stays_connected(
    monkeypatch,
) -> None:
    """A probe that is answered keeps the connection and repeats on cadence."""
    park_quietly(monkeypatch)
    get_msg_calls = 0
    probe_calls = 0

    async def get_msg(*, timeout):
        nonlocal get_msg_calls
        get_msg_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        # A slow companion still answers the local device query.
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    session._start_receive_pump()
    try:
        assert await wait_until(
            lambda: probe_calls >= 1, timeout=2
        ), "a parked pump must probe the idle link instead of waiting forever"
        # An answered probe never retires: the link stays up, no recovery.
        for _ in range(5):
            await asyncio.sleep(0.02)
        assert session._diag.connected is True
        assert not session._ensure_recovery_task.called
        # Silence continues, so the probe repeats on its cadence.
        assert await wait_until(lambda: probe_calls >= 2, timeout=2)
        assert session._diag.connected is True
        # A fresh notification still resumes the drain on the same connection.
        session._fetch_wake.set()
        rounds_before = get_msg_calls
        assert await wait_until(lambda: get_msg_calls > rounds_before, timeout=2)
        assert session._diag.connected is True
    finally:
        await session._stop_receive_pump()


async def test_node_error_reply_to_the_probe_keeps_the_link(monkeypatch) -> None:
    """A companion that rejects the query still proves the link is alive."""
    park_quietly(monkeypatch)
    probe_calls = 0

    async def get_msg(*, timeout):
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        # The node answers, but does not implement this query: the wire's
        # ERROR frame carries a numeric code, which the SDK reports under
        # "error_code" — never an elapsed-wait reason, never the string
        # the SDK itself uses for a local send failure.
        return event(EventType.ERROR, {"error_code": 2, "code_string": "err"})

    session = session_with_fetch(get_msg, send_device_query)
    session._start_receive_pump()
    try:
        # Several probes come and go; none of them retires the connection.
        assert await wait_until(lambda: probe_calls >= 3, timeout=3)
        assert session._diag.connected is True
        assert not session._fetch_task.done()
        session._ensure_recovery_task.assert_not_called()
    finally:
        await session._stop_receive_pump()


async def test_stop_during_a_probe_cancels_cleanly_with_no_warnings(
    monkeypatch, caplog
) -> None:
    """Stop cancels the in-flight probe with the pump; nothing is left over."""
    park_quietly(monkeypatch)
    probe_entered = asyncio.Event()
    release_probe = asyncio.Event()

    async def get_msg(*, timeout):
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        probe_entered.set()
        await release_probe.wait()

    session = session_with_fetch(get_msg, send_device_query)
    session._start_receive_pump()
    try:
        assert await wait_until(
            probe_entered.is_set, timeout=2
        ), "the probe must be in flight before stop for this test to bind"
        task = session._fetch_task
        await session._stop_receive_pump()
        # No pump task and no probe remain; the cancelled probe raised no
        # warnings or stray exceptions anywhere.
        assert session._fetch_task is None
        assert task.done()
        warnings = [
            record for record in caplog.records if record.levelno >= logging.WARNING
        ]
        assert warnings == [], [record.getMessage() for record in warnings]
    finally:
        release_probe.set()
        await session._stop_receive_pump()


async def test_probe_never_overlaps_a_fetch(monkeypatch) -> None:
    """While a probe is in flight no fetch is issued, and both serialize."""
    park_quietly(monkeypatch)
    get_msg_calls = 0
    probe_entered = asyncio.Event()
    release_probe = asyncio.Event()

    async def get_msg(*, timeout):
        nonlocal get_msg_calls
        get_msg_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        probe_entered.set()
        # The SDK correlates replies by event type, so a probe and a fetch
        # must never be outstanding together.
        assert session._send_lock.locked()
        await release_probe.wait()
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    session._start_receive_pump()
    try:
        assert await wait_until(
            probe_entered.is_set, timeout=2
        ), "the probe must be in flight for this test to bind"
        rounds_at_probe = get_msg_calls
        for _ in range(5):
            await asyncio.sleep(0.02)
        assert (
            get_msg_calls == rounds_at_probe
        ), "a fetch must not be issued while a probe is in flight"
        release_probe.set()
        # After the answered probe the pump stays parked (no notification);
        # the next fetch happens only after a fresh wake.
        for _ in range(5):
            await asyncio.sleep(0.02)
        assert get_msg_calls == rounds_at_probe
        assert session._diag.connected is True
    finally:
        release_probe.set()
        await session._stop_receive_pump()


async def test_a_parse_fault_reply_to_the_probe_keeps_the_link(monkeypatch) -> None:
    """Bytes that arrived but failed to parse are a reply from the node."""
    park_quietly(monkeypatch)
    probe_calls = 0

    async def get_msg(*, timeout):
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        nonlocal probe_calls
        probe_calls += 1
        # The node's bytes arrived but the SDK failed to parse them: the
        # SDK dispatches a non-elapsed parse-fault reason. This is a
        # genuine reply, not an unanswered query.
        return event(EventType.ERROR, {"reason": "binary_parse_error: bad frame"})

    session = session_with_fetch(get_msg, send_device_query)
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: probe_calls >= 3, timeout=3)
        assert session._diag.connected is True
        assert not session._fetch_task.done()
        session._ensure_recovery_task.assert_not_called()
    finally:
        await session._stop_receive_pump()


async def test_local_probe_write_error_does_not_clear_reconnect_gate():
    """The SDK reports a local send failure as a string under "error":
    no reply came from the node, so the failure-driven gate stays."""
    async def get_msg(*, timeout):
        pytest.fail("a failed gate must not fetch")

    async def send_device_query():
        return event(EventType.ERROR, {"error": "broken pipe"})

    session = session_with_fetch(get_msg, send_device_query)
    session._receive_failure_pending = True
    with pytest.raises(RuntimeError, match="unanswered"):
        await session._probe_idle_link(session._meshcore)
    assert session._receive_failure_pending is True
