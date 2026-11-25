"""Connection-scoped receive ownership; no radio, no SDK transport needed."""

import asyncio
import logging
from enum import Enum, auto
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from medre.adapters.meshcore.session import MeshCoreSession


class EventType(Enum):
    CHANNEL_MSG_RECV = auto()
    CONTACT_MSG_RECV = auto()
    NO_MORE_MSGS = auto()
    ERROR = auto()


def session_with_fetch(get_msg, capacity=None):
    session = MeshCoreSession.__new__(MeshCoreSession)
    client = SimpleNamespace(commands=SimpleNamespace(get_msg=get_msg))
    session._meshcore = client
    session._sdk_module = SimpleNamespace(EventType=EventType)
    session._diag = SimpleNamespace(connected=True)
    session._stop_requested = False
    session._send_lock = asyncio.Lock()
    session._wait_for_receive_capacity = capacity
    session._release_receive_permit = None
    session._fetch_wake = asyncio.Event()
    session._fetch_wake.set()
    session._fetch_task = None
    session._adapter_id = "test"
    session._logger = logging.getLogger(__name__)
    session._set_connected = lambda value: setattr(session._diag, "connected", value)
    session._ensure_recovery_task = Mock()
    return session


@pytest.mark.asyncio
async def test_pump_drains_initial_and_notification_backlogs_without_second_owner():
    seen = []
    drained = asyncio.Event()
    responses = iter(
        [
            EventType.CHANNEL_MSG_RECV,
            EventType.NO_MORE_MSGS,
            EventType.CONTACT_MSG_RECV,
            EventType.NO_MORE_MSGS,
        ]
    )

    async def get_msg(*, timeout):
        assert timeout > 0
        event_type = next(responses)
        seen.append(event_type)
        if len(seen) == 2:
            # A notification arrives during the empty-queue response.
            session._fetch_wake.set()
        elif len(seen) == 4:
            drained.set()
        return SimpleNamespace(type=event_type)

    session = session_with_fetch(get_msg)
    session._start_receive_pump()
    owner = session._fetch_task
    session._start_receive_pump()
    assert session._fetch_task is owner
    assert seen == []  # startup does not pop messages in the caller's turn
    try:
        await asyncio.wait_for(drained.wait(), 2)
        assert seen == [
            EventType.CHANNEL_MSG_RECV,
            EventType.NO_MORE_MSGS,
            EventType.CONTACT_MSG_RECV,
            EventType.NO_MORE_MSGS,
        ]
        assert not session._ensure_recovery_task.called
    finally:
        await session._stop_receive_pump()
    assert owner.done() and session._fetch_task is None


@pytest.mark.asyncio
async def test_pump_respects_capacity_and_serializes_native_command():
    entered = asyncio.Event()
    capacity = asyncio.Event()
    fetched = asyncio.Event()

    async def wait_capacity():
        entered.set()
        await capacity.wait()

    async def get_msg(*, timeout):
        assert session._send_lock.locked()
        fetched.set()
        return SimpleNamespace(type=EventType.NO_MORE_MSGS)

    session = session_with_fetch(get_msg, wait_capacity)
    async with session._send_lock:
        session._start_receive_pump()
        await asyncio.wait_for(entered.wait(), 2)
        assert not fetched.is_set()
        capacity.set()
        await asyncio.sleep(0)
        assert not fetched.is_set()
    try:
        await asyncio.wait_for(fetched.wait(), 2)
    finally:
        await session._stop_receive_pump()


@pytest.mark.asyncio
async def test_ambiguous_fetch_response_retires_connection_without_retry():
    calls = 0

    async def get_msg(*, timeout):
        nonlocal calls
        calls += 1
        return SimpleNamespace(type=EventType.ERROR)

    session = session_with_fetch(get_msg)
    session._start_receive_pump()
    await asyncio.wait_for(session._fetch_task, 2)
    assert calls == 1
    assert session._diag.connected is False
    session._ensure_recovery_task.assert_called_once()
    await session._stop_receive_pump()
