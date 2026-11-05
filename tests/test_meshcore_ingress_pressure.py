"""Bounded MeshCore callback admission boundary tests.

The synchronous SDK callback reserves one admission slot per distinct
packet before committing the dedup key and scheduling the publication
task.  When ``max_pending_ingress_callbacks`` tasks are already pending,
a distinct packet is refused without admission: no task, no dedup key,
one ``"overflow"`` producer-pressure report with the
``"callback_capacity"`` reason, and rate-limited content-free warnings. A refused
identity stays retryable — its next delivery is re-admitted once
capacity frees.  Accepted packets report ``"scheduled"``; tasks
cancelled before admission report ``"cancelled_before_admission"``.

These are deterministic SDK-callback shape tests (fake mode only, no
network, no hardware, no sleeps), not companion or RF evidence.  The
pre-cap measurement history in ``docs/dev/ingress-pressure.md`` was
recorded while admission was unbounded; these tests pin the bounded
policy that replaced it.
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from datetime import UTC, datetime
from typing import Any

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.identity import derive_message_identity
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterContext

try:
    from medre.core.contracts.adapter import ProducerPressureReport
except ImportError:
    # Older compatible core releases lack producer-pressure reporting.
    ProducerPressureReport = None
from medre.core.events import CanonicalEvent


def _packet(index: int) -> dict:
    """Make a relayable unique channel packet without SDK/hardware state."""
    return {
        "text": f"pressure {index}",
        "pubkey_prefix": "pressure-source",
        "sender_timestamp": index + 1,
        "type": "CHAN",
        "txt_type": 0,
        "channel_idx": 0,
    }


def _dedup_key(packet: dict) -> str | None:
    """Derive the expected dedup identity for a txt_type=0 channel packet."""
    return derive_message_identity(
        sender_id=packet["pubkey_prefix"],
        channel_index=packet["channel_idx"],
        sender_timestamp=packet["sender_timestamp"],
        txt_type=packet["txt_type"],
        text=packet["text"],
        is_direct_message=False,
    )


class _HeldPublisher:
    """Deterministic gated publisher.

    Each ``publish_inbound`` call marks its per-call entry event (the
    caller is then suspended on the shared gate) and, once the gate
    opens, records the published payload body.  Entry events make
    "publication is pending" observable without sleeps.
    """

    def __init__(self, entry_count: int) -> None:
        self.gate = asyncio.Event()
        self.entry_events = [asyncio.Event() for _ in range(entry_count)]
        self._entered = 0
        self.published: list[str] = []

    async def __call__(self, event: CanonicalEvent) -> None:
        index = min(self._entered, len(self.entry_events) - 1)
        self._entered += 1
        self.entry_events[index].set()
        await self.gate.wait()
        self.published.append(str(event.payload.get("body")))


class _PressureSink:
    """Record every producer-pressure report the adapter emits."""

    def __init__(self) -> None:
        self.reports: list[ProducerPressureReport] = []

    def __call__(self, report: ProducerPressureReport) -> None:
        self.reports.append(report)

    def outcome_count(self, outcome: str) -> int:
        return sum(report.outcome == outcome for report in self.reports)


async def _start(
    adapter: MeshCoreAdapter,
    publisher: _HeldPublisher,
    sink: _PressureSink | None = None,
) -> AdapterContext:
    context: dict[str, Any] = {
        "adapter_id": adapter.adapter_id,
        "publish_inbound": publisher,
        "logger": logging.getLogger(f"test.{adapter.adapter_id}"),
        "clock": lambda: datetime.now(UTC),
        "shutdown_event": asyncio.Event(),
    }
    ctx = AdapterContext(**context)
    if sink is not None:
        # Older compatible AdapterContext versions lack this constructor
        # field; the adapter must tolerate the optional seam being absent.
        ctx.report_producer_pressure = sink
    await adapter.start(ctx)
    return ctx


def _pending_tasks(adapter: MeshCoreAdapter) -> tuple[asyncio.Task, ...]:
    return tuple(adapter._background_tasks)


async def _drain(adapter: MeshCoreAdapter, timeout: float = 10.0) -> None:
    tasks = _pending_tasks(adapter)
    if tasks:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout)


def _assert_reports_wellformed(sink: _PressureSink, adapter: MeshCoreAdapter) -> None:
    for report in sink.reports:
        assert report.adapter_id == adapter.adapter_id
        assert report.count == 1
        assert report.outcome in (
            "scheduled",
            "overflow",
            "cancelled_before_admission",
        )
        if report.outcome == "overflow":
            assert report.reason == "callback_capacity"
        else:
            assert report.reason is None


# ===================================================================
# Exact boundary: the default cap admits exactly 100 of a 256 burst.
# ===================================================================


async def test_default_capacity_admits_exactly_the_boundary_burst() -> None:
    """256 distinct callbacks under held publication: 100 admitted, 156 refused."""
    burst = 256
    publisher = _HeldPublisher(entry_count=100)
    sink = _PressureSink()
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc-boundary-default"))
    await _start(adapter, publisher, sink)
    try:
        keys = [_dedup_key(_packet(index)) for index in range(burst)]
        for index in range(burst):
            adapter._on_message(_packet(index))

        # Exactly the default bound is pending; the rest were refused.
        assert len(_pending_tasks(adapter)) == 100
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 100
        assert adapter.diagnostics()["max_pending_ingress_callbacks"] == 100
        assert sink.outcome_count("scheduled") == 100
        assert sink.outcome_count("overflow") == 156
        assert adapter.diagnostics()["producer_scheduled"] == 100
        assert adapter.diagnostics()["producer_overflow"] == 156

        # Reservation precedes the dedup commit: only admitted
        # identities hold provisional keys.
        assert set(adapter._inbound_dedup) == set(keys[:100])

        await asyncio.wait_for(publisher.entry_events[99].wait(), 10)
        assert adapter.diagnostics()["inbound_published"] == 0

        # Full drain after release: every admitted packet publishes, in
        # admission order; refused packets publish nothing.
        publisher.gate.set()
        await _drain(adapter)
        assert not _pending_tasks(adapter)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        assert publisher.published == [f"pressure {i}" for i in range(100)]
        assert adapter.diagnostics()["inbound_published"] == 100

        # The overflow accounting is stable after the drain.
        assert sink.outcome_count("overflow") == 156
        assert adapter.diagnostics()["producer_overflow"] == 156
        _assert_reports_wellformed(sink, adapter)
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_below_capacity_burst_unchanged() -> None:
    """A 32-packet burst under the cap behaves as before: all delivered."""
    publisher = _HeldPublisher(entry_count=32)
    sink = _PressureSink()
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc-boundary-32"))
    await _start(adapter, publisher, sink)
    try:
        for index in range(32):
            adapter._on_message(_packet(index))
        assert len(_pending_tasks(adapter)) == 32
        assert sink.outcome_count("scheduled") == 32
        assert sink.outcome_count("overflow") == 0
        assert adapter.diagnostics()["producer_overflow"] == 0

        await asyncio.wait_for(publisher.entry_events[31].wait(), 10)
        publisher.gate.set()
        await _drain(adapter)
        assert not _pending_tasks(adapter)
        assert publisher.published == [f"pressure {i}" for i in range(32)]
        assert adapter.diagnostics()["inbound_published"] == 32
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        _assert_reports_wellformed(sink, adapter)
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Refusal never poisons dedup: a refused identity is re-admitted when
# capacity frees.
# ===================================================================


async def test_refused_packet_identity_is_readmitted_after_capacity_frees() -> None:
    """A capacity-refused packet publishes on a later delivery."""
    publisher = _HeldPublisher(entry_count=2)
    sink = _PressureSink()
    adapter = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="mc-boundary-readmit", max_pending_ingress_callbacks=1
        )
    )
    await _start(adapter, publisher, sink)
    try:
        admitted = _packet(0)
        refused = _packet(1)
        refused_key = _dedup_key(refused)
        assert refused_key is not None

        adapter._on_message(admitted)
        await asyncio.wait_for(publisher.entry_events[0].wait(), 5)

        adapter._on_message(refused)
        assert len(_pending_tasks(adapter)) == 1
        assert sink.outcome_count("overflow") == 1
        assert refused_key not in adapter._inbound_dedup

        # Capacity frees; the refused identity is re-admitted.
        publisher.gate.set()
        await _drain(adapter)
        assert not _pending_tasks(adapter)
        assert publisher.published == ["pressure 0"]
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0

        before = frozenset(_pending_tasks(adapter))
        adapter._on_message(refused)
        scheduled = frozenset(_pending_tasks(adapter)) - before
        assert len(scheduled) == 1, (
            "redelivery of a refused packet must be scheduled once " "capacity frees"
        )
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 1
        await _drain(adapter)
        assert publisher.published == ["pressure 0", "pressure 1"]
        assert adapter.diagnostics()["inbound_published"] == 2
        assert adapter.diagnostics()["producer_overflow"] == 1
        assert refused_key in adapter._inbound_dedup
        _assert_reports_wellformed(sink, adapter)
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_duplicate_bursts_dedup_within_bounded_capacity() -> None:
    """Duplicates stay suppressed and refusals stay retryable while full."""
    publisher = _HeldPublisher(entry_count=4)
    sink = _PressureSink()
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc-boundary-dedup", max_pending_ingress_callbacks=4)
    )
    await _start(adapter, publisher, sink)
    try:
        head = _packet(0)
        head_key = _dedup_key(head)
        assert head_key is not None

        adapter._on_message(head)
        await asyncio.wait_for(publisher.entry_events[0].wait(), 5)

        # Duplicates of a pending admission are suppressed before the
        # capacity check: no tasks, no overflow.
        for _ in range(3):
            adapter._on_message(head)
        assert len(_pending_tasks(adapter)) == 1
        assert sink.outcome_count("overflow") == 0

        # Fill the remaining capacity with distinct packets.
        for index in range(1, 4):
            adapter._on_message(_packet(index))
        assert len(_pending_tasks(adapter)) == 4
        assert sink.outcome_count("overflow") == 0

        # A duplicate of an admitted packet while full is still a dedup
        # hit, not an overflow.
        adapter._on_message(head)
        assert len(_pending_tasks(adapter)) == 4
        assert sink.outcome_count("overflow") == 0

        # A distinct packet while full is refused; its identity stays
        # uncommitted, so repeated redeliveries are refused again
        # instead of being suppressed by a poisoned key.
        fresh = _packet(4)
        fresh_key = _dedup_key(fresh)
        assert fresh_key is not None
        adapter._on_message(fresh)
        assert len(_pending_tasks(adapter)) == 4
        assert sink.outcome_count("overflow") == 1
        assert fresh_key not in adapter._inbound_dedup
        adapter._on_message(fresh)
        assert sink.outcome_count("overflow") == 2
        assert fresh_key not in adapter._inbound_dedup

        # Release: exactly the four admitted packets publish, then the
        # refused identity is re-admitted.
        publisher.gate.set()
        await _drain(adapter)
        assert sorted(publisher.published) == [f"pressure {i}" for i in range(4)]
        assert head_key in adapter._inbound_dedup

        adapter._on_message(fresh)
        await _drain(adapter)
        assert sorted(publisher.published) == [f"pressure {i}" for i in range(5)]
        assert adapter.diagnostics()["producer_overflow"] == 2
        assert adapter.diagnostics()["inbound_published"] == 5
        assert fresh_key in adapter._inbound_dedup
        _assert_reports_wellformed(sink, adapter)
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Shutdown under load: stop cancels pending work, accounts it, and a
# new session inherits no stale reservations.
# ===================================================================


async def test_stop_under_load_cancels_pending_and_accounts_cancellation() -> None:
    """Running publications cancel without claiming that admission never began."""
    publisher = _HeldPublisher(entry_count=8)
    sink = _PressureSink()
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc-boundary-stop", max_pending_ingress_callbacks=8)
    )
    await _start(adapter, publisher, sink)
    try:
        for index in range(8):
            adapter._on_message(_packet(index))
        await asyncio.wait_for(publisher.entry_events[7].wait(), 5)
        assert len(_pending_tasks(adapter)) == 8

        await adapter.stop()
        assert not _pending_tasks(adapter)
        assert publisher.published == []
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        assert adapter.diagnostics()["producer_cancelled_before_admission"] == 0
        assert sink.outcome_count("cancelled_before_admission") == 0
        assert sink.outcome_count("overflow") == 0
        assert not adapter._inbound_dedup

        # Post-stop callbacks stay lifecycle-ignored, not overflow.
        before = adapter.diagnostics()["producer_overflow"]
        adapter._on_message(_packet(50))
        assert not _pending_tasks(adapter)
        assert adapter.diagnostics()["producer_overflow"] == before
        assert sink.outcome_count("overflow") == 0
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_restart_inherits_no_stale_reservations() -> None:
    """After stop/start the pending counter is clean and ingress works."""
    publisher = _HeldPublisher(entry_count=1)
    restarted = publisher
    sink = _PressureSink()
    adapter = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="mc-boundary-restart", max_pending_ingress_callbacks=1
        )
    )
    await _start(adapter, publisher, sink)
    try:
        adapter._on_message(_packet(0))
        await asyncio.wait_for(publisher.entry_events[0].wait(), 5)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 1
        adapter._on_message(_packet(1))
        assert adapter.diagnostics()["producer_overflow"] == 1

        await adapter.stop()
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0

        # A new session starts clean: no stale reservations, no stale
        # overflow accounting.
        restarted = _HeldPublisher(entry_count=1)
        await _start(adapter, restarted)
        adapter._on_message(_packet(2))
        assert len(_pending_tasks(adapter)) == 1
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 1
        assert adapter.diagnostics()["producer_overflow"] == 0
        assert adapter.diagnostics()["producer_cancelled_before_admission"] == 0

        restarted.gate.set()
        await _drain(adapter)
        assert restarted.published == ["pressure 2"]
    finally:
        publisher.gate.set()
        restarted.gate.set()
        await adapter.stop()


async def test_stop_cancels_tasks_that_never_entered_their_coroutine() -> None:
    """Slots of burst tasks cancelled before their first step are released.

    Done callbacks run even for tasks whose coroutine never stepped, so
    the reservation accounting and the cancelled_before_admission
    reports cover the whole burst without leaks.
    """
    publisher = _HeldPublisher(entry_count=1)
    sink = _PressureSink()
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc-boundary-unstepped"))
    await _start(adapter, publisher, sink)
    try:
        for index in range(100):
            adapter._on_message(_packet(index))
        assert len(_pending_tasks(adapter)) == 100

        # stop() cancels before any publisher entry event could fire.
        await adapter.stop()
        assert not _pending_tasks(adapter)
        assert publisher.published == []
        assert not publisher.entry_events[0].is_set()
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        assert adapter.diagnostics()["producer_cancelled_before_admission"] == 100
        assert sink.outcome_count("cancelled_before_admission") == 100
        assert sink.outcome_count("scheduled") == 100
        assert sink.outcome_count("overflow") == 0
        assert not adapter._inbound_dedup
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# The pressure seam is optional and best-effort.
# ===================================================================


async def test_adapter_bounds_and_refuses_without_pressure_sink() -> None:
    """A context without report_producer_pressure still bounds admission."""
    publisher = _HeldPublisher(entry_count=2)
    adapter = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="mc-boundary-no-seam", max_pending_ingress_callbacks=2
        )
    )
    await _start(adapter, publisher, sink=None)
    try:
        for index in range(4):
            adapter._on_message(_packet(index))
        assert len(_pending_tasks(adapter)) == 2
        assert adapter.diagnostics()["producer_overflow"] == 2
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 2

        publisher.gate.set()
        await _drain(adapter)
        assert publisher.published == ["pressure 0", "pressure 1"]
        assert adapter.diagnostics()["inbound_published"] == 2
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_unavailable_pressure_report_type_is_skipped(monkeypatch) -> None:
    """Older cores without the report type degrade to local accounting."""
    import medre.adapters.meshcore.adapter as adapter_module

    monkeypatch.setattr(adapter_module, "ProducerPressureReport", None)
    publisher = _HeldPublisher(entry_count=1)
    sink = _PressureSink()
    adapter = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="mc-boundary-old-core", max_pending_ingress_callbacks=1
        )
    )
    await _start(adapter, publisher, sink)
    try:
        adapter._on_message(_packet(0))
        adapter._on_message(_packet(1))
        assert len(_pending_tasks(adapter)) == 1
        assert sink.reports == []
        assert adapter.diagnostics()["producer_overflow"] == 1

        publisher.gate.set()
        await _drain(adapter)
        assert publisher.published == ["pressure 0"]
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_raising_pressure_sink_is_contained() -> None:
    """A sink that raises cannot break admission or refusal accounting."""

    class _RaisingSink:
        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, report: ProducerPressureReport) -> None:
            self.calls += 1
            raise ValueError("sink unavailable")

    publisher = _HeldPublisher(entry_count=1)
    sink = _RaisingSink()
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc-boundary-sink", max_pending_ingress_callbacks=1)
    )
    await _start(adapter, publisher, sink)  # type: ignore[arg-type]
    try:
        adapter._on_message(_packet(0))
        adapter._on_message(_packet(1))
        assert sink.calls == 2
        assert len(_pending_tasks(adapter)) == 1
        assert adapter.diagnostics()["producer_overflow"] == 1

        publisher.gate.set()
        await _drain(adapter)
        assert publisher.published == ["pressure 0"]
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Warnings are content-free and rate-limited; refusal evidence is exact.
# ===================================================================


async def test_refusal_warning_is_rate_limited_but_counts_remain_exact(caplog) -> None:
    """A burst emits one warning, while each refusal is counted and reported."""
    publisher = _HeldPublisher(entry_count=1)
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc-boundary-log", max_pending_ingress_callbacks=1)
    )
    sink = _PressureSink()
    await _start(adapter, publisher, sink)
    try:
        with caplog.at_level(logging.WARNING, logger="test.mc-boundary-log"):
            adapter._on_message(_packet(0))
            await asyncio.wait_for(publisher.entry_events[0].wait(), 5)
            for index in range(1, 65):
                adapter._on_message(_packet(index))

            assert adapter.diagnostics()["producer_overflow"] == 64
            assert sink.outcome_count("overflow") == 64
            adapter._last_overflow_warning_at -= 6.0
            adapter._on_message(_packet(65))

        refusals = [
            record
            for record in caplog.records
            if record.levelno == logging.WARNING
            and "packet refused without admission" in record.getMessage()
        ]
        assert len(refusals) == 2
        assert "total_refused=1" in refusals[0].getMessage()
        assert "total_refused=65" in refusals[1].getMessage()
        assert adapter.diagnostics()["producer_overflow"] == 65
        assert sink.outcome_count("overflow") == 65
        for record in refusals:
            message = record.getMessage()
            assert "pressure 0" not in message
            assert "pressure 1" not in message
            assert "pressure 2" not in message
            assert "pressure-source" not in message
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Lifecycle fences preserved from the pre-cap harness.
# ===================================================================


async def test_stop_cancels_pending_sdk_callback_tasks() -> None:
    """A held callback is cancelled by lifecycle stop, not silently retained."""
    gate = asyncio.Event()
    entered = asyncio.Event()
    exited = asyncio.Event()
    cancelled = asyncio.Event()
    publishers: list[asyncio.Task] = []
    published = []

    async def publish(event) -> None:
        task = asyncio.current_task()
        assert task is not None
        publishers.append(task)
        entered.set()
        try:
            await gate.wait()
            published.append(event.event_id)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        finally:
            exited.set()

    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="meshcore-pressure-stop"))
    await adapter.start(
        AdapterContext(
            adapter_id=adapter.adapter_id,
            publish_inbound=publish,
            logger=logging.getLogger("test.meshcore.ingress.shutdown"),
            clock=lambda: datetime.now(UTC),
            shutdown_event=asyncio.Event(),
        )
    )
    try:
        adapter._on_message(_packet(1))
        await asyncio.wait_for(entered.wait(), 5)
        assert adapter._background_tasks
        await adapter.stop()
        await asyncio.wait_for(exited.wait(), 5)
        await asyncio.gather(*publishers, return_exceptions=True)
        assert cancelled.is_set()
        assert all(task.cancelled() for task in publishers)
        assert not adapter._background_tasks
        gate.set()
        adapter._on_message(_packet(2))
        assert not adapter._background_tasks
        assert not published
        assert adapter.diagnostics()["inbound_published"] == 0
    finally:
        gate.set()
        for task in publishers:
            task.cancel()
        await asyncio.gather(*publishers, return_exceptions=True)
        await adapter.stop()


async def test_scheduled_callback_rechecks_stop_before_publication(monkeypatch) -> None:
    """A queued callback cannot publish after stop, even before drain cancels it."""
    published = []

    async def publish(event) -> None:
        published.append(event.event_id)

    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="meshcore-scheduled-stop"))
    await adapter.start(
        AdapterContext(
            adapter_id=adapter.adapter_id,
            publish_inbound=publish,
            logger=logging.getLogger("test.meshcore.ingress.scheduled_stop"),
            clock=lambda: datetime.now(UTC),
            shutdown_event=asyncio.Event(),
        )
    )
    tasks = ()
    try:
        adapter._on_message(_packet(1))
        tasks = tuple(adapter._background_tasks)
        assert len(tasks) == 1 and not tasks[0].done()

        async def drain_without_cancellation(timeout=5.0):
            # Isolate the coroutine's stop fence from the drain's cancellation.
            await asyncio.gather(*tasks)

        monkeypatch.setattr(
            adapter, "_drain_background_tasks", drain_without_cancellation
        )
        await adapter.stop()
        assert all(task.done() and not task.cancelled() for task in tasks)
        assert not published
        assert adapter.diagnostics()["inbound_published"] == 0
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await adapter.stop()


@pytest.mark.parametrize("retired_outcome", ["late_success", "late_cancel"])
async def test_retired_publication_does_not_release_new_session_capacity(
    retired_outcome: str,
) -> None:
    """A timed-out stop cannot free a slot in the next session's budget."""
    old_entered = asyncio.Event()
    old_cancel_seen = asyncio.Event()
    old_release = asyncio.Event()
    old_finished = asyncio.Event()

    async def old_publisher(_event: CanonicalEvent) -> None:
        old_entered.set()
        try:
            await old_release.wait()
        except asyncio.CancelledError:
            old_cancel_seen.set()
            await old_release.wait()  # Deliberately ignores first cancellation.

    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc-retired-epoch", max_pending_ingress_callbacks=1)
    )
    retired_sink = _PressureSink()
    await adapter.start(
        AdapterContext(
            adapter_id=adapter.adapter_id,
            publish_inbound=old_publisher,
            logger=logging.getLogger("test.meshcore.retired.epoch"),
            clock=lambda: datetime.now(UTC),
            shutdown_event=asyncio.Event(),
            report_producer_pressure=retired_sink,
        )
    )
    new_publisher = _HeldPublisher(entry_count=1)
    retired_task: asyncio.Task | None = None
    try:
        adapter._on_message(_packet(0))
        retired_task = _pending_tasks(adapter)[0]
        retired_task.add_done_callback(lambda _: old_finished.set())
        await asyncio.wait_for(old_entered.wait(), 5)
        await asyncio.wait_for(adapter.stop(timeout=0.02), 2)
        assert old_cancel_seen.is_set()
        assert not retired_task.done()
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0

        sink = _PressureSink()
        await _start(adapter, new_publisher, sink)
        adapter._on_message(_packet(1))
        await asyncio.wait_for(new_publisher.entry_events[0].wait(), 5)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 1

        if retired_outcome == "late_success":
            old_release.set()
        else:
            retired_task.cancel()  # A second cancellation propagates.
        await asyncio.wait_for(old_finished.wait(), 5)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 1
        assert adapter.diagnostics()["producer_cancelled_before_admission"] == 0
        assert adapter.diagnostics()["inbound_published"] == 0
        assert retired_sink.outcome_count("scheduled") == 1
        assert retired_sink.outcome_count("cancelled_before_admission") == 0
        assert sink.outcome_count("cancelled_before_admission") == 0
        adapter._on_message(_packet(2))
        assert adapter.diagnostics()["producer_overflow"] == 1
        assert len(_pending_tasks(adapter)) == 1

        new_publisher.gate.set()
        await _drain(adapter)
        assert adapter.diagnostics()["inbound_published"] == 1
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
    finally:
        old_release.set()
        new_publisher.gate.set()
        if retired_task is not None:
            await asyncio.gather(retired_task, return_exceptions=True)
        await adapter.stop()


async def test_successful_self_cancellation_is_not_reported_as_unadmitted():
    """A completed publication cannot become a producer cancellation loss."""
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc-completed-cancel"))
    sink = _PressureSink()

    async def publish(_event):
        asyncio.current_task().cancel()

    await _start(adapter, publish, sink)
    try:
        adapter._on_message(_packet(0))
        task = _pending_tasks(adapter)[0]
        with pytest.raises(asyncio.CancelledError):
            await task
        assert adapter.diagnostics()["inbound_published"] == 1
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        assert sink.outcome_count("cancelled_before_admission") == 0
        assert adapter.diagnostics()["producer_cancelled_before_admission"] == 0
    finally:
        await adapter.stop()


async def test_failed_publisher_releases_slot_and_allows_another_packet():
    """An ordinary publication error retires its slot as well as its dedup key."""
    adapter = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="mc-publish-failure",
            max_pending_ingress_callbacks=1,
        )
    )
    seen = []

    async def publish(event):
        seen.append(event.event_id)
        if len(seen) == 1:
            raise RuntimeError("publish failed")

    await _start(adapter, publish)
    try:
        adapter._on_message(_packet(0))
        await _drain(adapter)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        adapter._on_message(_packet(1))
        await _drain(adapter)
        assert len(seen) == 2
        assert adapter.diagnostics()["inbound_published"] == 1
        assert adapter.diagnostics()["producer_overflow"] == 0
    finally:
        await adapter.stop()


async def test_retired_simulation_does_not_commit_new_session_state():
    """A simulation's late completion cannot pollute replacement diagnostics."""
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc-simulation-epoch"))
    old = _HeldPublisher(entry_count=1)
    await _start(adapter, old)
    running = asyncio.create_task(adapter.simulate_inbound(_packet(0)))
    try:
        await asyncio.wait_for(old.entry_events[0].wait(), 5)
        await adapter.stop()
        fresh = _HeldPublisher(entry_count=1)
        await _start(adapter, fresh)
        old.gate.set()
        await running
        assert adapter.diagnostics()["inbound_published"] == 0
        assert not adapter._inbound_dedup
    finally:
        old.gate.set()
        await asyncio.gather(running, return_exceptions=True)
        await adapter.stop()


async def test_rejected_task_creation_closes_coroutine_and_returns_capacity(
    monkeypatch,
):
    """A failed scheduler retains neither a coroutine nor a reservation."""
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc-scheduling-failure"))
    await _start(adapter, _HeldPublisher(entry_count=1))
    rejected = []

    def reject(coroutine):
        rejected.append(coroutine)
        raise RuntimeError("scheduler closed")

    try:
        monkeypatch.setattr(asyncio, "create_task", reject)
        adapter._on_message(_packet(0))
        assert len(rejected) == 1 and rejected[0].cr_frame is None
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        assert not adapter._inbound_dedup
    finally:
        await adapter.stop()
