"""Receive-pump idle fetches and the pump's inbound-capacity permit.

An SDK fetch window that elapses with no event is idle, not a failure: the
pump keeps the connection and re-issues the fetch at a bounded rate, because
publication happens in the SDK event callback whenever the frame actually
arrives. Only a real transport or protocol failure retires the connection.

The pump holds at most one inbound-capacity permit, acquired before it
issues a fetch; the fetched message's publication consumes the permit
instead of re-reserving, so a message the node has already popped is never
refused at admission. A drain that ends without a fetched message releases
the permit.

Deterministic SDK-shape tests: no network, no hardware, no fixed sleeps —
condition-based waiting only. The idle/permit tests fail against the
previous retire-on-timeout, reserve-at-publication behaviour.
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
    NO_MORE_MSGS = auto()
    ERROR = auto()


def event(event_type: Enum, payload: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(type=event_type, payload=payload or {})


def session_with_fetch(get_msg, capacity=None, release=None):
    session = MeshCoreSession.__new__(MeshCoreSession)
    client = SimpleNamespace(commands=SimpleNamespace(get_msg=get_msg))
    session._meshcore = client
    session._sdk_module = SimpleNamespace(EventType=EventType)
    session._diag = SimpleNamespace(connected=True)
    session._stop_requested = False
    session._send_lock = asyncio.Lock()
    session._wait_for_receive_capacity = capacity
    session._release_receive_permit = release
    session._fetch_wake = asyncio.Event()
    session._fetch_wake.set()
    session._fetch_task = None
    session._adapter_id = "test"
    session._logger = logging.getLogger(__name__)
    session._set_connected = lambda value: setattr(session._diag, "connected", value)
    session._ensure_recovery_task = Mock()
    return session


# ===================================================================
# D1: an idle fetch window keeps the connection and re-issues.
# ===================================================================


async def test_fetch_timeout_reissues_on_the_same_connection() -> None:
    """An elapsed fetch window re-issues the fetch without retiring."""
    seen: list[Enum] = []
    drained = asyncio.Event()
    responses = iter(
        [
            event(EventType.ERROR, {"reason": "no_event_received"}),
            event(EventType.CONTACT_MSG_RECV),
            event(EventType.NO_MORE_MSGS),
        ]
    )

    async def get_msg(*, timeout):
        assert timeout > 0
        response = next(responses)
        seen.append(response.type)
        if response.type is EventType.NO_MORE_MSGS:
            drained.set()
        return response

    session = session_with_fetch(get_msg)
    session._start_receive_pump()
    try:
        assert await wait_until(drained.is_set, timeout=2)
        assert seen == [
            EventType.ERROR,
            EventType.CONTACT_MSG_RECV,
            EventType.NO_MORE_MSGS,
        ]
        # The same connection still owns the pump; no recovery was started.
        assert session._diag.connected is True
        assert not session._ensure_recovery_task.called
    finally:
        await session._stop_receive_pump()


async def test_repeated_idle_fetches_stay_bounded_and_connected() -> None:
    """Idle rounds re-issue, but only two run immediately; the rest pace."""
    calls = 0

    async def get_msg(*, timeout):
        nonlocal calls
        calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    session = session_with_fetch(get_msg)
    session._start_receive_pump()
    try:
        # The initial round plus the immediate re-issues run without waiting.
        assert await wait_until(lambda: calls >= 3, timeout=2)
        # The next round is paced: a few loop turns do not run it early.
        for _ in range(5):
            await asyncio.sleep(0.01)
        assert calls == 3, "pacing must not let idle rounds spin"
        # Pacing is a floor, not a stall: the pump still re-issues after it.
        assert await wait_until(lambda: calls >= 4, timeout=4)
        # Idleness never retires the connection.
        assert session._diag.connected is True
        assert not session._ensure_recovery_task.called
    finally:
        await session._stop_receive_pump()


async def test_node_error_reply_still_retires_the_connection() -> None:
    """A non-idle ERROR payload remains a protocol failure that retires."""
    calls = 0

    async def get_msg(*, timeout):
        nonlocal calls
        calls += 1
        return event(EventType.ERROR, {"error": "bad request"})

    session = session_with_fetch(get_msg)
    session._start_receive_pump()
    await asyncio.wait_for(session._fetch_task, 2)
    assert calls == 1
    assert session._diag.connected is False
    session._ensure_recovery_task.assert_called_once()
    await session._stop_receive_pump()


# ===================================================================
# D2: one capacity permit per fetched message.
# ===================================================================


async def test_empty_queue_round_releases_the_fetch_permit() -> None:
    """A drain that ends on NO_MORE_MSGS releases the pump's permit."""
    release = Mock()

    async def get_msg(*, timeout):
        return event(EventType.NO_MORE_MSGS)

    session = session_with_fetch(get_msg, release=release)
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: release.call_count >= 1, timeout=2)
        assert session._diag.connected is True
    finally:
        await session._stop_receive_pump()


async def test_pump_exit_releases_an_unconsumed_permit() -> None:
    """Stopping the pump mid-drain releases the permit it still holds."""
    release = Mock()
    gate_open = asyncio.Event()

    async def capacity() -> None:
        await gate_open.wait()

    async def get_msg(*, timeout):  # pragma: no cover - never reached
        return event(EventType.NO_MORE_MSGS)

    session = session_with_fetch(get_msg, capacity=capacity, release=release)
    session._start_receive_pump()
    for _ in range(5):
        await asyncio.sleep(0.01)
    await session._stop_receive_pump()
    assert session._fetch_task is None
    release.assert_called_once()


async def test_ambiguous_fetch_response_releases_its_permit_before_retiring() -> None:
    """A non-message fetch result releases the permit, then retires."""
    release = Mock()

    async def get_msg(*, timeout):
        return event(EventType.ERROR, {"error": "bad request"})

    session = session_with_fetch(get_msg, release=release)
    session._start_receive_pump()
    await asyncio.wait_for(session._fetch_task, 2)
    release.assert_called_once()
    assert session._diag.connected is False
    await session._stop_receive_pump()
