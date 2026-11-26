"""Idle-link liveness probes for the parked receive pump.

A silently dead link — a peer that vanishes without FIN/RST — never fails a
fetch: the pump parks after an empty fetch window and the session looks
connected forever. While the pump would otherwise sit parked, it MUST
periodically confirm the node is still reachable with a cheap local query
(the SDK device query, whose DEVICE_INFO reply cannot be confused with a
message event). A probe that goes unanswered is a transport failure:
the connection retires through the pump's ordinary retire path, which runs
the existing recovery loop exactly once. A reply of any kind — including an
error the node itself sends — proves the link is alive and never retires.

An answered probe also re-arms exactly one fetch for a drain that an
elapsed window interrupted: the node may still hold messages that drain
never saw, so a link that recovers resumes within one probe interval, and
a link that answers probes while swallowing message frames consumes at
most one message per probe interval. A drain that ended with
NO_MORE_MSGS is complete; an answered probe never fetches for it.

One probe runs at a time, never concurrent with a fetch, serialized with
sends on the session's send lock. Stop, retire and reconnect cancel the
probe with the pump; a late probe reply matches no subscription.

Deterministic SDK-shape tests: no network, no hardware, no fixed sleeps —
condition-based waiting only. The probe tests pin the probe cadence, the
retirement of an unanswered link, the one-fetch re-arm of an answered
probe, and the never-counted device reply.
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum, auto
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

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
    # The retirement path sanitizes through the session config.
    session._config = SimpleNamespace(ble_pin=None)
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
        # The node answers, but does not implement this query: the SDK
        # reports the node's own error code, not an elapsed-wait reason.
        return event(EventType.ERROR, {"error": 2})

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


async def test_probe_never_overlaps_a_fetch_and_rearms_exactly_one(monkeypatch) -> None:
    """A probe is never concurrent with a fetch, and it re-arms one fetch."""
    park_quietly(monkeypatch)
    get_msg_calls = 0
    probes_answered = 0
    probe_entered = asyncio.Event()
    release_first_probe = asyncio.Event()
    block_later_probes = asyncio.Event()

    async def get_msg(*, timeout):
        nonlocal get_msg_calls
        get_msg_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        nonlocal probes_answered
        probe_entered.set()
        # The SDK correlates replies by event type, so a probe and a fetch
        # must never be outstanding together.
        assert session._send_lock.locked()
        if probes_answered == 0:
            await release_first_probe.wait()
            probes_answered += 1
            return event(EventType.DEVICE_INFO, {"fw ver": 13})
        # Every later probe stays in flight, unanswered.
        await block_later_probes.wait()
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
        release_first_probe.set()
        # The drain this probe interrupted ended on an elapsed window, so
        # the answered probe re-arms exactly one fetch.
        assert await wait_until(
            lambda: get_msg_calls == rounds_at_probe + 1, timeout=2
        ), "one answered probe must re-arm exactly one fetch after an interrupted drain"
        # The re-armed round's window elapses too: the pump parks again, and
        # with later probes left unanswered nothing further may fetch.
        for _ in range(5):
            await asyncio.sleep(0.02)
        assert (
            get_msg_calls == rounds_at_probe + 1
        ), "an answered probe re-arms one fetch, not a re-issue streak"
        assert session._diag.connected is True
    finally:
        block_later_probes.set()
        await session._stop_receive_pump()


async def test_an_answered_probe_rearms_an_interrupted_drain_and_not_a_complete_one(
    monkeypatch,
) -> None:
    """One answered probe issues one fetch after an interrupted drain;
    a drain that ended with NO_MORE_MSGS stays parked."""
    park_quietly(monkeypatch)

    # An interrupted drain: the elapsed window parked the pump, and each
    # answered probe re-arms exactly one fetch until traffic answers.
    fetch_calls = 0
    probe_gate = asyncio.Event()

    async def get_msg(*, timeout):
        nonlocal fetch_calls
        fetch_calls += 1
        # Every fetch round ends with no inbound traffic: the pump parks.
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        # In flight (unanswered) until the test opens the gate.
        await probe_gate.wait()
        probe_gate.clear()
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: fetch_calls >= 1, timeout=2)
        for _ in range(4):
            await asyncio.sleep(0.05)
        assert (
            fetch_calls == 1
        ), "the interrupted drain stays parked while no probe is answered"
        probe_gate.set()
        assert await wait_until(
            lambda: fetch_calls == 2, timeout=2
        ), "one answered probe must re-arm exactly one fetch"
        # The re-armed round's window elapses too: the pump parks again, and
        # the NEXT answered probe re-arms it again — one fetch per answer.
        probe_gate.clear()
        for _ in range(4):
            await asyncio.sleep(0.05)
        assert (
            fetch_calls == 2
        ), "the re-armed round parked again; an unanswered probe re-arms nothing"
        probe_gate.set()
        assert await wait_until(lambda: fetch_calls == 3, timeout=2)
        assert session._diag.connected is True
        assert not session._ensure_recovery_task.called
    finally:
        probe_gate.set()
        await session._stop_receive_pump()

    # A complete drain: it ended with the node's NO_MORE_MSGS, so no
    # answered probe may fetch for it — the notification does.
    complete_calls = 0

    async def drained_queue(*, timeout):
        nonlocal complete_calls
        complete_calls += 1
        return event(EventType.NO_MORE_MSGS)

    async def always_answer():
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    complete_session = session_with_fetch(drained_queue, always_answer)
    complete_session._start_receive_pump()
    try:
        assert await wait_until(lambda: complete_calls == 1, timeout=2)
        for _ in range(6):
            await asyncio.sleep(0.05)
        assert (
            complete_calls == 1
        ), "an answered probe must not fetch for a NO_MORE_MSGS drain"
        assert complete_session._diag.connected is True
        assert not complete_session._ensure_recovery_task.called
    finally:
        await complete_session._stop_receive_pump()


async def test_a_link_that_answers_probes_and_swallows_frames_consumes_one_message_per_probe_interval(
    monkeypatch,
) -> None:
    """The loss bound: one popped message per answered probe interval."""
    park_quietly(monkeypatch)
    fetch_calls = 0
    probe_gate = asyncio.Event()

    async def get_msg(*, timeout):
        nonlocal fetch_calls
        fetch_calls += 1
        # The node pops a message and sends it; the link swallows the
        # frame. From the SDK's side the fetch window simply elapses with
        # no inbound traffic.
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        # In flight (unanswered) until the test answers this one probe.
        await probe_gate.wait()
        probe_gate.clear()
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    session._start_receive_pump()
    try:
        # The first fetch runs on the initial wake; its window elapses
        # with no traffic and the pump parks.
        assert await wait_until(lambda: fetch_calls >= 1, timeout=2)
        for _ in range(4):
            await asyncio.sleep(0.05)
        assert fetch_calls == 1
        # Each answered probe re-arms exactly one fetch — one swallowed
        # message per probe interval, never a re-issue streak.
        for expected in (2, 3, 4):
            probe_gate.set()
            assert await wait_until(
                lambda expected=expected: fetch_calls == expected, timeout=2
            )
            probe_gate.clear()
            for _ in range(4):
                await asyncio.sleep(0.05)
            assert (
                fetch_calls == expected
            ), "an answered probe re-arms one fetch, not a re-issue streak"
        assert session._diag.connected is True
        assert not session._ensure_recovery_task.called
    finally:
        probe_gate.set()
        await session._stop_receive_pump()


async def test_an_answered_probe_on_a_connection_that_died_issues_no_fetch(
    monkeypatch,
) -> None:
    """An answered probe cannot re-arm a fetch on a dead connection.

    The probe's reply and a transport loss can land in either order; a
    connection that is already down when the answer arrives must end the
    pump without issuing any fetch — the recovery loop owns the retry.
    """
    park_quietly(monkeypatch)
    get_msg_calls = 0
    probe_entered = asyncio.Event()

    async def get_msg(*, timeout):
        nonlocal get_msg_calls
        get_msg_calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    async def send_device_query():
        probe_entered.set()
        # The transport dies as the reply arrives — the disconnect
        # handler has already marked the connection down.
        session._diag.connected = False
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: get_msg_calls >= 1, timeout=2)
        assert await wait_until(
            probe_entered.is_set, timeout=2
        ), "the parked pump must probe the idle link"
        assert await wait_until(lambda: session._fetch_task.done(), timeout=2)
        # The answered probe never fetched: the pump ended on the
        # connection check, not on a fetch round.
        assert get_msg_calls == 1, (
            f"an answered probe on a dead connection must issue no fetch "
            f"(saw {get_msg_calls})"
        )
        assert session._diag.connected is False
        assert not session._ensure_recovery_task.called, (
            "the pump's own exit must not start recovery: the disconnect "
            "handler owns it"
        )
    finally:
        await session._stop_receive_pump()


async def test_probe_replies_never_count_as_inbound_traffic(monkeypatch) -> None:
    """Only published message types count as inbound traffic; a device
    reply to a probe is never mistaken for a fetched frame."""
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
        return event(EventType.DEVICE_INFO, {"fw ver": 13})

    session = session_with_fetch(get_msg, send_device_query)
    session._message_callback = None
    session._start_receive_pump()
    try:
        assert await wait_until(
            lambda: probe_calls >= 2, timeout=2
        ), "the parked pump must repeat its probes"
        assert (
            session._fetch_inbound_seen == 0
        ), "a DEVICE_INFO probe reply must never bump the inbound counter"
        # A message frame delivered through the event callback is what
        # counts — published or not.
        await session._on_sdk_event(event(EventType.CONTACT_MSG_RECV))
        assert session._fetch_inbound_seen == 1
        assert session._diag.connected is True
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
