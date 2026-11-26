"""Receive-pump idle fetches and the pump's inbound-capacity permit.

The node's fetch is a destructive pop, so a fetch window that elapses with
no inbound traffic parks the pump at once: the window may have ended
because the node popped a message whose frame was lost, and re-issuing
would pop another message into a link that is not returning what it pops.
A window that elapses while traffic is still arriving — a late frame, or a
notification — means the link is slow but delivering: the pump keeps the
connection and re-issues at a bounded rate. A frame that pops after its
window publishes through the SDK event callback, which wakes the pump, so
a slow link resumes its interrupted drain on the same connection. Only a
real transport or protocol failure retires the connection.

The pump holds at most one inbound-capacity permit, acquired before it
issues a fetch; the fetched message's publication consumes the permit
instead of re-reserving, so a message the node has already popped is never
refused at admission. A drain that ends without a fetched message releases
the permit.

Deterministic SDK-shape tests: no network, no hardware, no fixed sleeps —
condition-based waiting only. The idle/permit tests pin the empty-window
park rule and the one-permit-per-fetched-message accounting, including the
popped-but-not-relayed channel-data round.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from enum import Enum, auto
from types import SimpleNamespace
from unittest.mock import Mock

from medre.core.contracts.adapter import AdapterContext

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig
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
    # The retirement path sanitizes through the session config.
    session._config = SimpleNamespace(ble_pin=None)
    session._stop_requested = False
    session._send_lock = asyncio.Lock()
    session._fetch_round_in_flight = False
    session._pump_silent_rounds = 0
    session._pump_drain_incomplete = False
    session._receive_failure_pending = False
    session._message_callback = None
    session._wait_for_receive_capacity = capacity
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


# ===================================================================
# D1: an elapsed window with no traffic parks at once; a late frame
# (or a notification) resumes the drain on the same connection.
# ===================================================================


async def test_late_frame_after_an_elapsed_window_wakes_the_pump(monkeypatch) -> None:
    """A frame that pops after its window wakes the parked pump."""
    monkeypatch.setattr(
        "medre.adapters.meshcore.session._PROBE_IDLE_INTERVAL", 0.05, raising=False
    )
    seen: list[Enum] = []
    drained = asyncio.Event()
    late_frame_published = asyncio.Event()

    async def get_msg(*, timeout):
        assert timeout > 0
        if not seen:
            # The node popped a message and sent it; the frame arrives
            # after this window closes, so the round ends empty.
            response = event(EventType.ERROR, {"reason": "no_event_received"})
        elif len(seen) == 1:
            # The next destructive pop must wait until the late frame was
            # actually published: fetching earlier would pop again into a
            # link that has not returned anything yet.
            assert (
                late_frame_published.is_set()
            ), "fetched again before the late frame arrived"
            response = event(EventType.CONTACT_MSG_RECV)
        else:
            response = event(EventType.NO_MORE_MSGS)
            drained.set()
        seen.append(response.type)
        return response

    session = session_with_fetch(get_msg)
    session._start_receive_pump()
    try:
        # The elapsed window parks the pump: no further fetch follows it.
        assert await wait_until(lambda: len(seen) == 1, timeout=2)
        for _ in range(5):
            await asyncio.sleep(0.02)
        assert (
            len(seen) == 1
        ), "an elapsed window with no inbound traffic must park, not re-issue"
        # The frame pops after its window and publishes through the event
        # callback — which wakes the pump.
        late_frame_published.set()
        await session._on_sdk_event(event(EventType.CONTACT_MSG_RECV, {"text": "late"}))
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


async def test_elapsed_window_without_traffic_parks_until_traffic_or_a_wake(
    monkeypatch,
) -> None:
    """One empty window parks the pump: no fetch follows until traffic comes."""
    monkeypatch.setattr(
        "medre.adapters.meshcore.session._PROBE_IDLE_INTERVAL", 0.05, raising=False
    )
    calls = 0

    async def get_msg(*, timeout):
        nonlocal calls
        calls += 1
        return event(EventType.ERROR, {"reason": "no_event_received"})

    # No device query: nothing can answer a probe, so nothing may fetch.
    session = session_with_fetch(get_msg)
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: calls >= 1, timeout=2)
        for _ in range(6):
            await asyncio.sleep(0.05)
        assert (
            calls == 1
        ), "an elapsed window with no inbound traffic must park the pump at once"
        # Idleness never retires the connection.
        assert session._diag.connected is True
        assert not session._ensure_recovery_task.called
        # A fresh notification starts a new drain on the same connection:
        # one more fetch, whose empty window parks the pump again.
        session._fetch_wake.set()
        assert await wait_until(lambda: calls == 2, timeout=2)
        for _ in range(4):
            await asyncio.sleep(0.05)
        assert calls == 2, "the next empty window parks again"
        assert session._diag.connected is True
    finally:
        await session._stop_receive_pump()


async def test_idle_rounds_with_inbound_traffic_keep_draining(monkeypatch) -> None:
    """Late frames arriving between idle rounds prove the link delivers."""
    monkeypatch.setattr(
        "medre.adapters.meshcore.session._FETCH_IDLE_REISSUE_DELAY", 0.02
    )
    monkeypatch.setattr("medre.adapters.meshcore.session._FETCH_ROUND_TIMEOUT", 0.2)
    calls = 0

    async def get_msg(*, timeout):
        nonlocal calls
        calls += 1
        # A late response frame arrives while this round is outstanding.
        session._fetch_inbound_seen += 1
        await asyncio.sleep(0)
        return event(EventType.ERROR, {"reason": "no_event_received"})

    session = session_with_fetch(get_msg)
    session._start_receive_pump()
    try:
        # Inbound activity resets the silent streak every round, so the pump
        # keeps re-issuing well past the silent park bound.
        assert await wait_until(lambda: calls >= 12, timeout=5)
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


async def test_a_notification_during_a_drain_drives_one_extra_round_after_no_more(
    monkeypatch,
) -> None:
    """A wake that lands mid-drain survives the drain's NO_MORE round as
    exactly one extra, bounded round against the emptied queue."""
    monkeypatch.setattr(
        "medre.adapters.meshcore.session._PROBE_IDLE_INTERVAL", 0.05, raising=False
    )
    calls = 0

    async def get_msg(*, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            # A fetched message: the drain keeps going.
            return event(EventType.CONTACT_MSG_RECV)
        if calls == 2:
            # The notification arrives while this round is outstanding;
            # the wake survives the round's NO_MORE_MSGS answer.
            session._fetch_wake.set()
        return event(EventType.NO_MORE_MSGS)

    session = session_with_fetch(get_msg)
    session._start_receive_pump()
    try:
        assert await wait_until(lambda: calls >= 3, timeout=2)
        for _ in range(5):
            await asyncio.sleep(0.02)
        assert calls == 3, (
            "a mid-drain notification costs exactly one extra empty round, "
            f"not an unbounded sequence (saw {calls})"
        )
        # The extra round ended on NO_MORE_MSGS: the pump is parked, the
        # connection is intact, and nothing was treated as a failure.
        assert session._pump_parked is True
        assert session._pump_drain_incomplete is False
        assert session._diag.connected is True
        assert not session._ensure_recovery_task.called
    finally:
        await session._stop_receive_pump()


# ===================================================================
# D2 at the adapter boundary: the real gate reserves, publication consumes.
# ===================================================================


def _packet(index: int) -> dict:
    """Make a relayable unique channel packet without SDK/hardware state."""
    return {
        "text": f"pump-permit {index}",
        "pubkey_prefix": "pump-permit-source",
        "sender_timestamp": index + 1,
        "type": "CHAN",
        "txt_type": 0,
        "channel_idx": 0,
    }


class _HeldPublisher:
    """Publication stays pending until the gate opens."""

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.entered = asyncio.Event()
        self.published: list[str] = []

    async def __call__(self, canonical) -> None:
        self.entered.set()
        await self.gate.wait()
        self.published.append(str(canonical.payload.get("body")))


class _PressureSink:
    def __init__(self) -> None:
        self.reports: list[object] = []

    def __call__(self, report) -> None:
        self.reports.append(report)

    def outcome_count(self, outcome: str) -> int:
        return sum(
            getattr(report, "outcome", None) == outcome for report in self.reports
        )


async def _start_adapter(
    publisher: _HeldPublisher, sink: _PressureSink, *, max_pending: int = 1
):
    context: dict = {
        "adapter_id": "pump-permit",
        "publish_inbound": publisher,
        "logger": logging.getLogger("test.pump-permit"),
        "clock": lambda: datetime.now(UTC),
        "shutdown_event": asyncio.Event(),
    }
    ctx = AdapterContext(**context)
    ctx.report_producer_pressure = sink
    adapter = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="pump-permit", max_pending_ingress_callbacks=max_pending
        )
    )
    await adapter.start(ctx)
    return adapter


async def test_gate_reserves_the_permit_before_fetching() -> None:
    """The capacity gate acquires the pump's admission slot, not just a peek."""
    publisher = _HeldPublisher()
    sink = _PressureSink()
    adapter = await _start_adapter(publisher, sink)
    try:
        gate = adapter._session._wait_for_receive_capacity
        await asyncio.wait_for(gate(), 2)
        # The slot is reserved before any fetch is issued.
        assert adapter._pump_permit_pending is True
        assert adapter._pending_callback_admissions == 1
        assert not publisher.entered.is_set()
        # One fetch per permit: while the permit is pending the pump reuses
        # it instead of reserving a second slot.
        await asyncio.wait_for(gate(), 2)
        assert adapter._pump_permit_pending is True
        assert adapter._pending_callback_admissions == 1
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_publication_consumes_the_permit_instead_of_re_reserving() -> None:
    """A fetched message publishes at a full pool; the permit is not re-taken."""
    publisher = _HeldPublisher()
    sink = _PressureSink()
    adapter = await _start_adapter(publisher, sink)
    try:
        gate = adapter._session._wait_for_receive_capacity
        await asyncio.wait_for(gate(), 2)
        assert adapter._pump_permit_pending is True
        assert adapter._pending_callback_admissions == 1

        # The fetched frame arrives while the pool is full with the pump's
        # own reservation. Publication consumes the permit: no re-reserve,
        # no refusal.
        adapter._on_message(_packet(0))
        assert await asyncio.wait_for(publisher.entered.wait(), 2)
        assert adapter._pump_permit_pending is False
        assert adapter._pending_callback_admissions == 1
        assert sink.outcome_count("overflow") == 0

        # The slot now belongs to the pending publication; it frees on
        # completion and a fresh acquisition reserves again.
        publisher.gate.set()
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 2)
        assert adapter._pending_callback_admissions == 0
        await asyncio.wait_for(gate(), 2)
        assert adapter._pump_permit_pending is True
        assert adapter._pending_callback_admissions == 1
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_a_full_inbound_queue_blocks_the_gate_until_capacity_frees() -> None:
    """A full pool holds the gate — and with it the fetch — until a slot frees."""
    publisher = _HeldPublisher()
    sink = _PressureSink()
    adapter = await _start_adapter(publisher, sink)
    try:
        gate = adapter._session._wait_for_receive_capacity
        await asyncio.wait_for(gate(), 2)
        # A fetched publication now occupies the pool.
        adapter._on_message(_packet(0))
        assert await asyncio.wait_for(publisher.entered.wait(), 2)
        # The next acquisition must wait: the pool is at its bound.
        gate_task = asyncio.create_task(gate())
        for _ in range(5):
            await asyncio.sleep(0.01)
        assert not gate_task.done()
        publisher.gate.set()
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 2)
        await asyncio.wait_for(gate_task, 2)
        assert adapter._pump_permit_pending is True
        assert adapter._pending_callback_admissions == 1
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_duplicate_fetched_frame_releases_the_permit() -> None:
    """A fetched frame suppressed by dedup frees its admission permit."""
    publisher = _HeldPublisher()
    sink = _PressureSink()
    adapter = await _start_adapter(publisher, sink, max_pending=2)
    try:
        gate = adapter._session._wait_for_receive_capacity
        # A first copy publishes normally.
        adapter._on_message(_packet(0))
        assert await asyncio.wait_for(publisher.entered.wait(), 2)
        # The pump's permit for the (redelivered) frame is pending.
        await asyncio.wait_for(gate(), 2)
        assert adapter._pump_permit_pending is True
        assert adapter._pending_callback_admissions == 2
        # The duplicate is fetched, then suppressed: fetched but not
        # published, so the permit must be released, not held.
        adapter._on_message(_packet(0))
        assert adapter._pump_permit_pending is False
        assert adapter._pending_callback_admissions == 1
        assert adapter._receive_capacity.is_set()
        assert sink.outcome_count("overflow") == 0
        # The freed slot is acquirable again.
        publisher.gate.set()
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 2)
        await asyncio.wait_for(gate(), 2)
        assert adapter._pump_permit_pending is True
    finally:
        publisher.gate.set()
        await adapter.stop()
