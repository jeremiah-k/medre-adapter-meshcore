"""Sustained-ingress qualification at and above the MeshCore admission cap.

Where ``test_meshcore_ingress_pressure.py`` pins the exact admission
boundary, these tests qualify the *sustained* shape that the provisional
default of ``max_pending_ingress_callbacks=100`` was reserved for:

* above-cap bursts at 1x, 4x and 16x the bound, with exact
  counter/report reconciliation and dedup-safe re-admission of every
  refused identity once capacity frees;
* a bounded sustained-imbalance cycle in which arrivals stay above
  capacity for thousands of callbacks while the publisher releases in
  small batches;
* a cancellation-resistant publisher under full saturation whose late
  completion cannot touch the restarted session's capacity or counters;
* content-free loop-lag and tracemalloc evidence for the measured-budget
  record (env-gated through ``MEDRE_PRESSURE_REPORT_DIR``).

These are deterministic SDK-callback shape tests (fake mode only, no
network, no hardware, no sleeps).  They prove bounded admission, dedup
safe refusal re-admission, epoch isolation under load and the absence of
loop starvation at burst scale.  They do not prove throughput, real-SDK,
hardware or RF behavior, and the loop-lag bound is a generous CI guard,
not an SLA.
"""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
import time
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

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

# Generous CI-tolerant ceiling for the loop-lag sentinel.  The measured
# value is recorded, not tuned; this guard only proves that a fully
# saturated burst window cannot starve the owner loop for whole seconds.
SENTINEL_LAG_BOUND_SECONDS = 5.0

# Generous sublinearity bound: a 1600-burst is 16x a 100-burst, so a
# retained pending-window footprint within 4x demonstrates that the
# pending budget is governed by the cap, not by the burst size.
SUBLINEAR_RETAINED_RATIO_BOUND = 4.0


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

    def reset_window(self, entry_count: int) -> None:
        """Re-arm a closed gate and fresh entry events for another window."""
        self.gate = asyncio.Event()
        self.entry_events = [asyncio.Event() for _ in range(entry_count)]
        self._entered = 0

    async def __call__(self, event: CanonicalEvent) -> None:
        index = min(self._entered, len(self.entry_events) - 1)
        self._entered += 1
        self.entry_events[index].set()
        await self.gate.wait()
        self.published.append(str(event.payload.get("body")))


class _BatchedPublisher:
    """Publisher whose in-flight publications are released in small batches.

    Every entry creates its own gate and records its task, so a test can
    release the oldest unreleased entries as a batch and observe the
    resulting capacity refill deterministically.
    """

    def __init__(self) -> None:
        self._gates: list[asyncio.Event] = []
        self.tasks: list[asyncio.Task] = []
        self.entered = 0
        self._entered_changed = asyncio.Event()
        self.published: list[str] = []

    async def __call__(self, event: CanonicalEvent) -> None:
        gate = asyncio.Event()
        self._gates.append(gate)
        task = asyncio.current_task()
        assert task is not None
        self.tasks.append(task)
        self.entered += 1
        self._entered_changed.set()
        await gate.wait()
        self.published.append(str(event.payload.get("body")))

    async def wait_entered(self, target: int) -> None:
        while self.entered < target:
            self._entered_changed.clear()
            await asyncio.wait_for(self._entered_changed.wait(), 10)

    def release(self, count: int) -> list[asyncio.Task]:
        released: list[asyncio.Task] = []
        # Gates and tasks are appended in the same synchronous entry
        # block, so the lists are always the same length.
        for gate, task in zip(self._gates, self.tasks, strict=False):
            if len(released) == count:
                break
            if not gate.is_set():
                gate.set()
                released.append(task)
        return released

    def release_all(self) -> list[asyncio.Task]:
        return self.release(len(self._gates))


class _PressureSink:
    """Record every producer-pressure report the adapter emits."""

    def __init__(self) -> None:
        self.reports: list[ProducerPressureReport] = []

    def __call__(self, report: ProducerPressureReport) -> None:
        self.reports.append(report)

    def outcome_count(self, outcome: str) -> int:
        return sum(report.outcome == outcome for report in self.reports)


def _write_report(name: str, payload: dict[str, Any]) -> None:
    """Export a content-free qualification record for the measured-budget round.

    Writes only when ``MEDRE_PRESSURE_REPORT_DIR`` is set; the JSON
    carries counters and local timings, never message bodies, sender
    identity or key material.  ``ru_maxrss`` is recorded as context and
    is never asserted.
    """
    root = os.environ.get("MEDRE_PRESSURE_REPORT_DIR")
    if not root:
        return
    try:
        import resource

        payload["ru_maxrss_kb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except ImportError:  # pragma: no cover - non-Unix contexts
        payload["ru_maxrss_kb"] = None
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(
        json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


async def _sentinel_lag_seconds() -> float:
    """Measure one event-loop pass behind a fully saturated burst window.

    The sentinel is a plain coroutine scheduled on the owner loop; its
    lag covers whatever the loop must step first (the whole pending
    publication window).  This is starvation evidence for the measured
    record, not an SLA and not a latency claim.
    """
    started = time.perf_counter()
    await asyncio.sleep(0)
    return time.perf_counter() - started


async def _stage_held_burst(
    adapter: MeshCoreAdapter,
    publisher: _HeldPublisher,
    burst: int,
    offset: int = 0,
) -> tuple[int, int, float]:
    """Fire a distinct burst under held publication and return evidence.

    Returns ``(retained_bytes, raw_peak_bytes, sentinel_lag_seconds)``.
    ``retained_bytes`` is the live tracemalloc footprint of the
    saturated window after refused-decode churn has been collected: the
    pending publication tasks, their canonical events and the publisher
    bookkeeping, all bounded by the cap.  ``raw_peak_bytes`` is the
    unretouched high-water including transient decode allocations for
    refused packets; it is recorded as context only.  The sentinel lag
    is measured immediately after the synchronous burst returns, so it
    covers the loop stepping the whole pending window.
    """
    gc.collect()
    tracemalloc.start()
    try:
        for index in range(burst):
            adapter._on_message(_packet(offset + index))
        lag = await _sentinel_lag_seconds()
        assert lag < SENTINEL_LAG_BOUND_SECONDS, (
            "saturated burst window starved the event loop beyond the "
            "generous CI guard"
        )
        entries = min(burst, adapter._config.max_pending_ingress_callbacks)
        await asyncio.wait_for(publisher.entry_events[entries - 1].wait(), 30)
        gc.collect()
        retained, raw_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return retained, raw_peak, lag


async def _start(
    adapter: MeshCoreAdapter,
    publisher: Any,
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


async def _drain(adapter: MeshCoreAdapter, timeout: float = 30.0) -> None:
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
# Above-cap bursts at 1x / 4x / 16x of the default bound: exact
# reconciliation, ordered admitted window, dedup-safe re-admission.
# ===================================================================


@pytest.mark.parametrize("burst", [100, 400, 1600])
async def test_burst_above_capacity_reconciles_and_readmits(burst: int) -> None:
    """N distinct callbacks under held publication: 100 admitted, N-100 refused.

    The refused identities stay retryable: redelivered after the
    release, each schedules and publishes with no new overflow,
    proving refusal never poisons dedup.  Counters and reports
    reconcile exactly.
    """
    cap = 100
    publisher = _HeldPublisher(entry_count=cap)
    sink = _PressureSink()
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id=f"mc-burst-{burst}"))
    await _start(adapter, publisher, sink)
    started = time.perf_counter()
    try:
        keys = [_dedup_key(_packet(index)) for index in range(burst)]
        retained, raw_peak, lag = await _stage_held_burst(adapter, publisher, burst)

        # Exactly the default bound is pending; the rest were refused.
        assert len(_pending_tasks(adapter)) == cap
        diagnostics = adapter.diagnostics()
        assert diagnostics["pending_ingress_callbacks"] == cap
        assert diagnostics["max_pending_ingress_callbacks"] == cap
        assert diagnostics["producer_scheduled"] == cap
        assert diagnostics["producer_overflow"] == burst - cap
        assert sink.outcome_count("scheduled") == cap
        assert sink.outcome_count("overflow") == burst - cap

        # Reservation precedes the dedup commit: only admitted
        # identities hold provisional keys.
        assert set(adapter._inbound_dedup) == set(keys[:cap])
        assert adapter.diagnostics()["inbound_published"] == 0

        # Full drain on release: the admitted window publishes in
        # admission order; refused packets publish nothing.
        publisher.gate.set()
        await _drain(adapter)
        assert not _pending_tasks(adapter)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        assert publisher.published == [f"pressure {i}" for i in range(cap)]
        assert adapter.diagnostics()["inbound_published"] == cap

        # Re-admission wave: every previously refused identity is
        # redelivered in bound-sized slices.  Each slice schedules in
        # full with no new overflow, refuting dedup poisoning.
        refused_ids = list(range(cap, burst))
        while refused_ids:
            wave_ids, refused_ids = refused_ids[:cap], refused_ids[cap:]
            before = frozenset(_pending_tasks(adapter))
            for index in wave_ids:
                adapter._on_message(_packet(index))
            scheduled = frozenset(_pending_tasks(adapter)) - before
            assert len(scheduled) == len(wave_ids)
            assert adapter.diagnostics()["pending_ingress_callbacks"] == len(wave_ids)
            assert sink.outcome_count("overflow") == burst - cap
            await _drain(adapter)
            assert adapter.diagnostics()["pending_ingress_callbacks"] == 0

        assert publisher.published == [f"pressure {i}" for i in range(burst)]
        diagnostics = adapter.diagnostics()
        assert diagnostics["inbound_published"] == burst
        assert diagnostics["producer_scheduled"] == burst
        assert diagnostics["producer_overflow"] == burst - cap
        assert sink.outcome_count("scheduled") == burst
        assert sink.outcome_count("overflow") == burst - cap

        # Admitted identities keep suppressing; the LRU evicts the
        # oldest beyond the dedup cache bound.
        assert set(adapter._inbound_dedup) == set(keys[-1024:])
        _assert_reports_wellformed(sink, adapter)
        _write_report(
            f"meshcore-{burst}.json",
            {
                "transport": "meshcore",
                "tier": "synthetic_sdk_callback",
                "burst": burst,
                "pending": cap,
                "admitted": cap,
                "refused": burst - cap,
                "readmitted": burst - cap,
                "published": burst,
                "drained": True,
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
                "retained_bytes": retained,
                "raw_peak_bytes": raw_peak,
                "sentinel_lag_ms": round(lag * 1000, 3),
                "overflow_policy": "bounded_callback_capacity",
            },
        )
    finally:
        publisher.gate.set()
        await adapter.stop()


async def test_repeated_bursts_reuse_full_capacity_within_one_session() -> None:
    """Consecutive held bursts refill the whole bound; no slot leaks."""
    cap = 100
    publisher = _HeldPublisher(entry_count=cap)
    sink = _PressureSink()
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc-burst-reuse"))
    await _start(adapter, publisher, sink)
    try:
        for round_index in range(3):
            offset = round_index * cap
            for index in range(offset, offset + cap):
                adapter._on_message(_packet(index))
            assert len(_pending_tasks(adapter)) == cap
            assert adapter.diagnostics()["pending_ingress_callbacks"] == cap
            assert adapter.diagnostics()["producer_overflow"] == 0
            assert sink.outcome_count("overflow") == 0
            assert sink.outcome_count("scheduled") == offset + cap

            publisher.gate.set()
            await _drain(adapter)
            assert not _pending_tasks(adapter)
            assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
            assert adapter.diagnostics()["inbound_published"] == offset + cap
            # Re-arm the publisher for the next burst in this session.
            publisher.reset_window(cap)
        assert publisher.published == [f"pressure {i}" for i in range(3 * cap)]
        _assert_reports_wellformed(sink, adapter)
    finally:
        publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Sustained imbalance: arrivals stay above capacity for thousands of
# callbacks while the publisher releases in small batches.
# ===================================================================


async def test_sustained_arrivals_above_capacity_hold_per_cycle() -> None:
    """Thousands of arrivals above a small bound keep every invariant exact."""
    cap = 8
    cycles = 200
    arrivals = 12
    released_per_cycle = 4
    publisher = _BatchedPublisher()
    sink = _PressureSink()
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc-sustained", max_pending_ingress_callbacks=cap)
    )
    await _start(adapter, publisher, sink)
    started = time.perf_counter()
    try:
        scheduled_total = 0
        overflow_total = 0
        distinct_total = 0
        dedup_suppressed = 0
        expected_published: list[str] = []
        pending_before = 0
        for cycle in range(cycles):
            base = cycle * arrivals
            for index in range(base, base + arrivals):
                adapter._on_message(_packet(index))
            distinct_total += arrivals

            # Exact admission arithmetic for this cycle.
            admitted = min(arrivals, cap - pending_before)
            diagnostics = adapter.diagnostics()
            pending_after_fire = diagnostics["pending_ingress_callbacks"]
            assert pending_after_fire <= cap
            assert pending_after_fire == pending_before + admitted
            cycle_scheduled = diagnostics["producer_scheduled"] - scheduled_total
            cycle_overflow = diagnostics["producer_overflow"] - overflow_total
            assert cycle_scheduled == admitted
            assert cycle_overflow == arrivals - admitted
            assert cycle_scheduled + cycle_overflow == arrivals
            scheduled_total += cycle_scheduled
            overflow_total += cycle_overflow

            # A duplicate of an identity inside the pending window is a
            # dedup hit before the capacity check: no task, no overflow.
            adapter._on_message(_packet(base))
            dedup_suppressed += 1
            diagnostics = adapter.diagnostics()
            assert diagnostics["pending_ingress_callbacks"] == pending_after_fire
            assert diagnostics["producer_scheduled"] == scheduled_total
            assert diagnostics["producer_overflow"] == overflow_total

            # Every scheduled publication has entered its coroutine.
            await publisher.wait_entered(scheduled_total)
            assert publisher.entered == scheduled_total
            expected_published.extend(
                f"pressure {i}" for i in range(base, base + admitted)
            )

            # Release a small batch; capacity refills by exactly that.
            released = publisher.release(released_per_cycle)
            assert len(released) == min(released_per_cycle, pending_after_fire)
            await asyncio.wait(released)
            diagnostics = adapter.diagnostics()
            pending_before = diagnostics["pending_ingress_callbacks"]
            assert pending_before == pending_after_fire - len(released)
            assert pending_before <= cap

        assert distinct_total == cycles * arrivals
        assert scheduled_total + overflow_total == distinct_total
        assert dedup_suppressed == cycles

        # Full drain: every admitted identity published exactly once, in
        # admission order; refused identities published nothing.
        while True:
            released = publisher.release_all()
            if not released:
                break
            await asyncio.wait(released)
        await _drain(adapter)
        diagnostics = adapter.diagnostics()
        assert diagnostics["pending_ingress_callbacks"] == 0
        assert not _pending_tasks(adapter)
        assert publisher.published == expected_published
        assert diagnostics["inbound_published"] == scheduled_total
        assert diagnostics["producer_scheduled"] == scheduled_total
        assert diagnostics["producer_overflow"] == overflow_total
        assert sink.outcome_count("scheduled") == scheduled_total
        assert sink.outcome_count("overflow") == overflow_total
        assert sink.outcome_count("cancelled_before_admission") == 0
        _assert_reports_wellformed(sink, adapter)
        _write_report(
            "meshcore-sustained-imbalance.json",
            {
                "transport": "meshcore",
                "tier": "synthetic_sdk_callback",
                "cycles": cycles,
                "arrivals_per_cycle": arrivals,
                "released_per_cycle": released_per_cycle,
                "capacity": cap,
                "distinct_deliveries": distinct_total,
                "dedup_suppressed": dedup_suppressed,
                "scheduled": scheduled_total,
                "refused": overflow_total,
                "published": len(publisher.published),
                "drained": True,
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
                "overflow_policy": "bounded_callback_capacity",
            },
        )
    finally:
        for gate in publisher._gates:
            gate.set()
        await adapter.stop()


# ===================================================================
# Cancellation-resistant publisher under full saturation: the timed-out
# drain retires the stubborn task, and its late completion cannot touch
# the restarted session.
# ===================================================================


@pytest.mark.parametrize("late_outcome", ["late_success", "late_cancel"])
async def test_saturated_stubborn_publisher_retires_and_restart_refills(
    late_outcome: str, caplog
) -> None:
    """Stop under 100 held publishers survives one cancellation-suppressing task."""
    cap = 100
    entered: list[asyncio.Task] = []
    all_entered = asyncio.Event()
    stubborn_first_cancel = asyncio.Event()
    stubborn_finished = asyncio.Event()
    stubborn_release = asyncio.Event()
    cooperative_release = asyncio.Event()
    stubborn_task: asyncio.Task | None = None

    async def publish(event: CanonicalEvent) -> None:
        task = asyncio.current_task()
        assert task is not None
        entered.append(task)
        if str(event.payload.get("body")) == f"pressure {cap - 1}":
            all_entered.set()
            try:
                await stubborn_release.wait()
            except asyncio.CancelledError:
                stubborn_first_cancel.set()
                await stubborn_release.wait()  # Suppresses cancellation.
        else:
            await cooperative_release.wait()

    original_sink = _PressureSink()
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc-sat-stubborn"))
    ctx = AdapterContext(
        adapter_id=adapter.adapter_id,
        publish_inbound=publish,
        logger=logging.getLogger("test.mc-sat-stubborn"),
        clock=lambda: datetime.now(UTC),
        shutdown_event=asyncio.Event(),
        report_producer_pressure=original_sink,
    )
    await adapter.start(ctx)
    epoch_before_stop = adapter._callback_admission_epoch
    try:
        with caplog.at_level(logging.WARNING, logger="test.mc-sat-stubborn"):
            for index in range(cap):
                adapter._on_message(_packet(index))
            assert len(_pending_tasks(adapter)) == cap
            await asyncio.wait_for(all_entered.wait(), 10)
            assert len(entered) == cap
            stubborn_task = entered[-1]
            stubborn_task.add_done_callback(lambda _task: stubborn_finished.set())

            # Bounded drain: cooperative publishers acknowledge
            # cancellation promptly; the stubborn task does not.  The
            # budget is a real deadline and is always fully consumed
            # here, which is the retire-from-tracking shape.
            await adapter.stop(timeout=1.0)

        assert stubborn_first_cancel.is_set()
        assert not stubborn_task.done()
        assert not _pending_tasks(adapter), "stubborn task must be retired"
        assert adapter._callback_admission_epoch == epoch_before_stop + 1
        abandoned_warnings = [
            record
            for record in caplog.records
            if record.levelno == logging.WARNING
            and "did not stop within the drain budget" in record.getMessage()
        ]
        assert len(abandoned_warnings) == 1
        assert "1 inbound publications" in abandoned_warnings[0].getMessage()
        assert "pressure" not in abandoned_warnings[0].getMessage()
        # Every publisher had started; running cancellation cannot claim
        # that admission never began.
        assert original_sink.outcome_count("cancelled_before_admission") == 0
        assert original_sink.outcome_count("scheduled") == cap

        # Restart: the new session starts from full, private capacity.
        new_publisher = _HeldPublisher(entry_count=cap)
        new_sink = _PressureSink()
        await _start(adapter, new_publisher, new_sink)
        for index in range(cap, cap * 2):
            adapter._on_message(_packet(index))
        await asyncio.wait_for(new_publisher.entry_events[cap - 1].wait(), 10)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == cap
        assert adapter.diagnostics()["producer_overflow"] == 0

        # The bound still refuses in the new session.
        adapter._on_message(_packet(cap * 2))
        assert adapter.diagnostics()["pending_ingress_callbacks"] == cap
        assert adapter.diagnostics()["producer_overflow"] == 1

        # The stubborn task finally settles after the restart.
        if late_outcome == "late_success":
            stubborn_release.set()
        else:
            stubborn_task.cancel()  # A second cancellation propagates.
        await asyncio.wait_for(stubborn_finished.wait(), 10)
        await asyncio.gather(stubborn_task, return_exceptions=True)
        if late_outcome == "late_cancel":
            assert stubborn_task.cancelled()
        else:
            assert not stubborn_task.cancelled()

        # The late completion touches nothing in the new session: its
        # epoch is retired, so no slot frees and no counter moves.
        diagnostics = adapter.diagnostics()
        assert diagnostics["pending_ingress_callbacks"] == cap
        assert diagnostics["producer_cancelled_before_admission"] == 0
        assert diagnostics["inbound_published"] == 0
        adapter._on_message(_packet(cap * 2 + 1))
        assert adapter.diagnostics()["pending_ingress_callbacks"] == cap
        assert adapter.diagnostics()["producer_overflow"] == 2
        assert new_sink.outcome_count("cancelled_before_admission") == 0
        assert new_sink.outcome_count("scheduled") == cap
        assert original_sink.outcome_count("cancelled_before_admission") == 0

        # The new session still drains its own window in full.
        new_publisher.gate.set()
        await _drain(adapter)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        assert new_publisher.published == [f"pressure {i}" for i in range(cap, cap * 2)]
        assert adapter.diagnostics()["inbound_published"] == cap
        _assert_reports_wellformed(original_sink, adapter)
        _assert_reports_wellformed(new_sink, adapter)
    finally:
        stubborn_release.set()
        cooperative_release.set()
        if stubborn_task is not None and not stubborn_task.done():
            stubborn_task.cancel()
            await asyncio.gather(stubborn_task, return_exceptions=True)
        await adapter.stop()


async def test_suppressing_publisher_yielding_within_budget_drains_cleanly(
    caplog,
) -> None:
    """A stubborn publisher that completes inside the budget leaves no warning."""
    cap = 4
    entered = asyncio.Event()
    release = asyncio.Event()
    suppressed = asyncio.Event()

    async def publish(_event: CanonicalEvent) -> None:
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            suppressed.set()
            await release.wait()  # Suppresses the first cancellation.

    async def release_soon() -> None:
        # Two loop passes: deterministic, no wall-clock sleep.
        for _ in range(2):
            await asyncio.sleep(0)
        release.set()

    sink = _PressureSink()
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc-sat-drains", max_pending_ingress_callbacks=cap)
    )
    await _start(adapter, publish, sink)
    waker: asyncio.Task | None = None
    try:
        with caplog.at_level(logging.WARNING, logger="test.mc-sat-drains"):
            for index in range(cap):
                adapter._on_message(_packet(index))
            # The burst tasks step FIFO, so by the time this wait
            # returns every publication has entered and is suspended on
            # the gate: stop will cancel *running* publishers.
            await asyncio.wait_for(entered.wait(), 5)
            waker = asyncio.create_task(release_soon())
            await adapter.stop(timeout=30.0)
        assert suppressed.is_set()
        assert not _pending_tasks(adapter)
        abandoned_warnings = [
            record
            for record in caplog.records
            if record.levelno == logging.WARNING
            and "did not stop within the drain budget" in record.getMessage()
        ]
        assert abandoned_warnings == []
        if waker is not None:
            await asyncio.gather(waker, return_exceptions=True)
    finally:
        release.set()
        if waker is not None:
            await asyncio.gather(waker, return_exceptions=True)
        await adapter.stop()


# ===================================================================
# Retired-epoch report routing: a late pre-start cancellation reports
# through the ORIGINAL session's pressure sink, never the new one.
#
# A task cancelled before its coroutine first steps settles on the
# first loop pass, so this shape cannot survive a real drain; the test
# drives the adapter's own done-callback seam with a real
# cancelled-unstarted task, matching the established seam-level
# precedent for timing shapes no public flow can reach.
# ===================================================================


async def test_retired_pre_start_cancellation_reports_to_original_sink() -> None:
    """The original context keeps its cancellation; the new session stays clean."""
    original_sink = _PressureSink()
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc-late-pre-start", max_pending_ingress_callbacks=1)
    )
    await _start(adapter, _HeldPublisher(entry_count=1), original_sink)
    retired_epoch = adapter._callback_admission_epoch
    retired_context = adapter.ctx
    assert retired_context is not None
    try:
        await adapter.stop()

        # New session: full capacity and a fresh pressure sink.
        new_publisher = _HeldPublisher(entry_count=1)
        new_sink = _PressureSink()
        await _start(adapter, new_publisher, new_sink)
        adapter._on_message(_packet(0))
        await asyncio.wait_for(new_publisher.entry_events[0].wait(), 5)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 1
        assert adapter.diagnostics()["producer_cancelled_before_admission"] == 0

        # A real task cancelled before its coroutine first stepped,
        # completing only after the epoch advanced.
        late = asyncio.create_task(asyncio.sleep(0))
        late.cancel()
        await asyncio.gather(late, return_exceptions=True)
        assert late.cancelled()
        adapter._on_background_task_done(
            retired_epoch,
            retired_context,
            late,
            publication_started=False,
        )
        assert original_sink.outcome_count("cancelled_before_admission") == 1
        assert new_sink.outcome_count("cancelled_before_admission") == 0
        assert adapter.diagnostics()["producer_cancelled_before_admission"] == 0

        report = original_sink.reports[0]
        assert report.adapter_id == adapter.adapter_id
        assert report.outcome == "cancelled_before_admission"
        assert report.reason is None

        # The new session's capacity is untouched by the late report.
        new_publisher.gate.set()
        await _drain(adapter)
        assert adapter.diagnostics()["pending_ingress_callbacks"] == 0
        assert new_publisher.published == ["pressure 0"]
        _assert_reports_wellformed(original_sink, adapter)
        _assert_reports_wellformed(new_sink, adapter)
    finally:
        new_publisher.gate.set()
        await adapter.stop()


# ===================================================================
# Measured-budget evidence: the retained saturated window grows
# sublinearly with the burst because pending work is bounded by the
# cap, and the loop keeps servicing sentinels while saturated.
# ===================================================================


async def test_retained_pending_window_grows_sublinearly_with_burst() -> None:
    """A 1600-burst retains near the same pending footprint as a 100-burst.

    The pressure sink is deliberately absent here so the measurement
    isolates the pending window itself; report bookkeeping is covered
    by the parametrized burst records.
    """
    retained_peaks: dict[int, int] = {}
    raw_peaks: dict[int, int] = {}
    lags: dict[int, float] = {}
    for burst, offset in ((100, 0), (1600, 100_000)):
        publisher = _HeldPublisher(entry_count=100)
        adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id=f"mc-peak-{burst}"))
        await _start(adapter, publisher, sink=None)
        try:
            retained, raw_peak, lag = await _stage_held_burst(
                adapter, publisher, burst, offset
            )
            retained_peaks[burst] = retained
            raw_peaks[burst] = raw_peak
            lags[burst] = lag
            assert retained > 0
            assert len(_pending_tasks(adapter)) == 100
            publisher.gate.set()
            await _drain(adapter)
            assert adapter.diagnostics()["inbound_published"] == min(burst, 100)
        finally:
            publisher.gate.set()
            await adapter.stop()

    small, large = retained_peaks[100], retained_peaks[1600]
    assert large < SUBLINEAR_RETAINED_RATIO_BOUND * small, (
        "retained saturated footprint must be governed by the cap, not "
        f"the burst size (100B={small}B, 1600B={large}B)"
    )
    assert lags[1600] < SENTINEL_LAG_BOUND_SECONDS
    _write_report(
        "meshcore-memory-sublinearity.json",
        {
            "transport": "meshcore",
            "tier": "synthetic_sdk_callback",
            "burst_small": 100,
            "burst_large": 1600,
            "retained_small_bytes": small,
            "retained_large_bytes": large,
            "retained_ratio": round(large / small, 3) if small else None,
            "retained_ratio_bound": SUBLINEAR_RETAINED_RATIO_BOUND,
            "burst_growth_ratio": 16,
            "raw_peak_small_bytes": raw_peaks[100],
            "raw_peak_large_bytes": raw_peaks[1600],
            "sentinel_lag_small_ms": round(lags[100] * 1000, 3),
            "sentinel_lag_large_ms": round(lags[1600] * 1000, 3),
            "overflow_policy": "bounded_callback_capacity",
        },
    )
