"""Process-lifetime restart-cycle qualification for bounded MeshCore admission.

Where ``test_meshcore_sustained_admission.py`` proves single-restart epoch
isolation, this module qualifies the process lifetime: repeated restart
cycles in one process, each stranding one cancellation-resistant publisher,
while every newer session still starts from full, private capacity.

Proven per cycle and cumulatively:

* the tracked publication-task registry stays empty after every bounded
  stop, and a process-wide census of adapter-created pending tasks grows
  by exactly one stranded publisher per cycle — retired tasks are
  epoch-fenced from the active session's capacity, counters and dedup
  state;
* every session admits a full window while all prior cycles' stranded
  publishers are still alive, so per-session diagnostics stay honest
  under accumulation pressure;
* retired sessions keep their own pressure sink: a late pre-start
  cancellation driven through the adapter's done-callback seam reports to
  the original cycle's sink, never to the restarted session, and late
  completions and late running-cancellations report nothing anywhere;
* dedup state cannot cross a session boundary: stop and start clear the
  map, so a stranded reservation never suppresses a new-session
  re-admission of the same identity, and a stale ownership token cannot
  evict a reservation a newer session re-committed for the same key;
* process-wide retained memory stays bounded across the whole run: the
  first restart cycles establish a warmed steady-state baseline with
  their strands still pending, the remaining cycles grow the census to
  every strand alive at once, the peak growth stays under the absolute
  extra-bytes bound, and after every stranded publisher settles the
  retained footprint returns below twice that baseline — nothing stays
  pinned across the process lifetime.

These are deterministic SDK-callback shape tests (fake mode only, no
network, no hardware, no fixed sleeps; waiting uses the repo's
condition-based helpers).  The cancellation-resistant publisher
is a test double, not a claim about real MeshCore SDK behavior.  These
tests do not prove throughput, real-SDK, hardware or RF behavior; the
memory bounds are generous CI guards, not an SLA.
"""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
import sys
import time
import tracemalloc
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.identity import derive_message_identity
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterContext
from medre.core.events import CanonicalEvent
from medre_meshcore_tests.helpers.async_utils import (
    bounded_cancel_and_reap,
    wait_until,
)

try:
    from medre.core.contracts.adapter import ProducerPressureReport
except ImportError:
    # Older compatible core releases lack producer-pressure reporting.
    ProducerPressureReport = None

# Restart cycles in one process.  Every cycle strands exactly one
# cancellation-resistant publisher; the strands of the baseline cycles
# stay pending while the remaining cycles run, so every strand that the
# process accumulated is alive at the peak measurement.
CYCLES = 25
BASELINE_STRANDS = 10

# Per-cycle bound and burst: a full admitted window plus refused extras.
CAP = 12
REFUSED_PER_CYCLE = 4
ARRIVALS_PER_CYCLE = CAP + REFUSED_PER_CYCLE

# The stop budget is a real deadline each cycle fully consumes: the
# stranded publisher suppresses cancellation, so the drain always times
# out with exactly one abandoned task.
DRAIN_BUDGET_SECONDS = 0.5

# Warm-up first so the baseline carries the warmed adapter, codec, task
# and publisher machinery at the cycle-scale steady state; then
# tracemalloc guards the growth of the remaining cycles and the residue
# left once every strand settles.
RETAINED_RATIO_BOUND = 2.0
RETAINED_EXTRA_BYTES_BOUND = 256_000

ADAPTER_ID = "mc-process-lifetime"
LOGGER_NAME = f"test.{ADAPTER_ID}"


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


class _DrainWarningCounter(logging.Handler):
    """Count bounded-drain timeout warnings without retaining records.

    Retaining one log record per cycle across many cycles would let the
    test's own capture bookkeeping inflate the process memory guard, so
    this handler keeps only the count.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        if "did not stop within the drain budget" in record.getMessage():
            self.count += 1


class _CycleSink:
    """Per-session pressure sink that keeps only outcome counts.

    Reports are validated on arrival against the content-free report
    shape and never retained.  One sink instance per session is what
    makes retired-epoch report routing observable.
    """

    def __init__(self, adapter_id: str) -> None:
        self.adapter_id = adapter_id
        self.counts: Counter[str] = Counter()

    def __call__(self, report: ProducerPressureReport) -> None:
        assert report.adapter_id == self.adapter_id
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
        self.counts[report.outcome] += 1


class _CyclePublisher:
    """Per-session publisher that strands one cancellation-resistant call.

    Cooperative calls park on the session gate until stop cancels them.
    The call whose payload body equals ``stubborn_body`` records its task,
    suppresses its first cancellation and stays pending until the test
    releases it — the exact retire-from-tracking shape a timed-out drain
    leaves behind.  Entry bookkeeping retains only a counter and the
    stubborn task reference, never per-entry task lists, so the test's
    own records cannot contaminate the process memory guard.
    """

    def __init__(self, cap: int, stubborn_body: str) -> None:
        self.cap = cap
        self.stubborn_body = stubborn_body
        self.gate = asyncio.Event()
        self.stubborn_release = asyncio.Event()
        self.stubborn_task: asyncio.Task[None] | None = None
        self.stubborn_first_cancel = False
        self.entered = 0
        self.published: list[str] = []

    async def __call__(self, event: CanonicalEvent) -> None:
        body = str(event.payload.get("body"))
        self.entered += 1
        if body == self.stubborn_body:
            task = asyncio.current_task()
            assert task is not None
            self.stubborn_task = task
            try:
                await self.stubborn_release.wait()
            except asyncio.CancelledError:
                # First cancellation is suppressed; a second one
                # propagates and ends the task as cancelled.
                self.stubborn_first_cancel = True
                await self.stubborn_release.wait()
            self.published.append(body)
            return
        await self.gate.wait()
        self.published.append(body)

    async def wait_entered(self, timeout: float = 10.0) -> asyncio.Task[None] | None:
        """Wait until every admitted publication entered, FIFO order.

        Publication tasks are created synchronously in fire order, so the
        stubborn call — the last admitted packet — enters last; its entry
        proves the whole window already stepped.
        """
        await wait_until(lambda: self.entered >= self.cap, timeout=timeout)
        assert self.entered == self.cap
        return self.stubborn_task


async def _start(
    adapter: MeshCoreAdapter,
    publisher: Any,
    sink: _CycleSink | None = None,
) -> AdapterContext:
    """Start a session with its own publisher and pressure sink."""
    context: dict[str, Any] = {
        "adapter_id": adapter.adapter_id,
        "publish_inbound": publisher,
        "logger": logging.getLogger(LOGGER_NAME),
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


def _pending_stranded(stranded: list[asyncio.Task[None]]) -> list[asyncio.Task[None]]:
    """Return the recorded stranded publishers that are still pending."""
    return [task for task in stranded if not task.done()]


def _assert_census(stranded: list[asyncio.Task[None]], expected: int) -> None:
    """Exact process-wide census of adapter-created pending tasks.

    Every stranded publisher is a real publication task the adapter
    created; each one must still be pending in the running loop's own
    task census while untracked by the adapter registry.
    """
    pending = _pending_stranded(stranded)
    assert len(pending) == expected
    loop_pending = asyncio.all_tasks()
    assert all(task in loop_pending for task in pending)


def _write_report(name: str, payload: dict[str, Any]) -> None:
    """Export a content-free qualification record for the measured budget.

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

        rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        payload["ru_maxrss_kb"] = rss // 1024 if sys.platform == "darwin" else rss
    except ImportError:  # pragma: no cover - non-Unix contexts
        payload["ru_maxrss_kb"] = None
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(
        json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


async def _run_restart_cycle(
    adapter: MeshCoreAdapter,
    cycle: int,
    publishers: list[_CyclePublisher],
    sinks: list[_CycleSink],
    stranded: list[asyncio.Task[None]],
    stranded_indices: list[int],
    warnings: _DrainWarningCounter,
    all_publishers: list[_CyclePublisher],
) -> AdapterContext:
    """Run one restart cycle: saturate, strand, bounded stop, seam check.

    Every cycle fires a full admitted window plus refused extras at one
    fresh session, strands one cancellation-resistant publisher behind a
    timed-out bounded drain, and drives the retired epoch's done-callback
    seam with a real cancelled-unstarted task.  From the second cycle on,
    the first packet re-delivers the prior cycle's stranded identity so
    its retired reservation must not suppress re-admission.
    """
    base = cycle * ARRIVALS_PER_CYCLE
    fresh = list(range(base, base + ARRIVALS_PER_CYCLE))
    if cycle > 0:
        prior_index = stranded_indices[cycle - 1]
        admitted_order = [prior_index, *fresh[: CAP - 1]]
        refused_order = fresh[CAP:]
    else:
        admitted_order = fresh[:CAP]
        refused_order = fresh[CAP:]
    stubborn_body = f"pressure {admitted_order[-1]}"
    publisher = _CyclePublisher(CAP, stubborn_body)
    publishers.append(publisher)
    all_publishers.append(publisher)
    sink = _CycleSink(adapter.adapter_id)
    sinks.append(sink)

    _assert_census(stranded, cycle)
    ctx = await _start(adapter, publisher, sink)
    retired_epoch = adapter._callback_admission_epoch

    # Session-open honesty: full private capacity and a cleared dedup
    # map while every prior stranded publisher is still alive and
    # untracked.
    diagnostics = adapter.diagnostics()
    assert diagnostics["started"] is True
    assert diagnostics["pending_ingress_callbacks"] == 0
    assert diagnostics["max_pending_ingress_callbacks"] == CAP
    assert diagnostics["producer_scheduled"] == 0
    assert diagnostics["producer_overflow"] == 0
    assert diagnostics["producer_cancelled_before_admission"] == 0
    assert diagnostics["inbound_published"] == 0
    assert not adapter._inbound_dedup
    assert not adapter._background_tasks

    for index in [*admitted_order, *refused_order]:
        adapter._on_message(_packet(index))

    # Exact admission arithmetic for this cycle.
    diagnostics = adapter.diagnostics()
    assert diagnostics["pending_ingress_callbacks"] == CAP
    assert diagnostics["producer_scheduled"] == CAP
    assert diagnostics["producer_overflow"] == REFUSED_PER_CYCLE
    committed = set(adapter._inbound_dedup)
    assert committed == {_dedup_key(_packet(index)) for index in admitted_order}

    # A duplicate inside the pending window is a dedup hit before the
    # capacity check: no task, no overflow.
    adapter._on_message(_packet(admitted_order[0]))
    diagnostics = adapter.diagnostics()
    assert diagnostics["pending_ingress_callbacks"] == CAP
    assert diagnostics["producer_scheduled"] == CAP
    assert diagnostics["producer_overflow"] == REFUSED_PER_CYCLE

    stubborn = await publisher.wait_entered()
    assert stubborn is not None
    assert len(adapter._background_tasks) == CAP

    # Bounded stop: the budget is a real deadline that the suppressing
    # publisher always forces to completion.
    warnings_before = warnings.count
    await adapter.stop(timeout=DRAIN_BUDGET_SECONDS)
    assert warnings.count == warnings_before + 1

    assert publisher.stubborn_first_cancel
    assert not stubborn.done()
    assert stubborn not in adapter._background_tasks
    assert not adapter._background_tasks
    diagnostics = adapter.diagnostics()
    assert diagnostics["pending_ingress_callbacks"] == 0
    assert diagnostics["producer_cancelled_before_admission"] == 0
    assert diagnostics["inbound_published"] == 0
    assert adapter._callback_admission_epoch == retired_epoch + 1
    assert not adapter._inbound_dedup
    assert sink.counts == Counter({"scheduled": CAP, "overflow": REFUSED_PER_CYCLE})

    # Retired-epoch seam: a real task cancelled before its coroutine
    # first stepped, settling only after this session retired, reports
    # through the ORIGINAL context's sink and never into the new
    # session's counters.
    late = asyncio.create_task(asyncio.sleep(0))
    late.cancel()
    await asyncio.gather(late, return_exceptions=True)
    assert late.cancelled()
    adapter._on_background_task_done(
        retired_epoch, ctx, late, publication_started=False
    )
    assert sink.counts["cancelled_before_admission"] == 1
    assert adapter.diagnostics()["producer_cancelled_before_admission"] == 0
    assert not adapter._background_tasks

    stranded.append(stubborn)
    stranded_indices.append(admitted_order[-1])
    _assert_census(stranded, cycle + 1)
    return ctx


async def test_restart_cycles_leave_no_process_lifetime_accumulation() -> None:
    """Repeated restart cycles strand publishers without accumulating state.

    Each cycle saturates admission beyond the bound, strands exactly one
    cancellation-resistant publisher behind a timed-out bounded drain,
    and restarts.  Per cycle the tracked registry must come back empty
    while the stranded task stays pending, the restarted session must
    report full private capacity, and the retired session's late
    pre-start cancellation must report to its own sink.  Across cycles
    the pending-task census must grow by exactly one stranded publisher
    per cycle, dedup reservations from retired sessions must never
    suppress a new-session re-admission, a stale ownership token must not
    evict a newer session's re-committed reservation, and process memory
    must stay inside the dual bounds.
    """
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id=ADAPTER_ID, max_pending_ingress_callbacks=CAP)
    )
    owns_tracing = not tracemalloc.is_tracing()
    if owns_tracing:
        tracemalloc.start()
    started = time.perf_counter()
    stranded: list[asyncio.Task[None]] = []
    publishers: list[_CyclePublisher] = []
    sinks: list[_CycleSink] = []
    stranded_indices: list[int] = []
    all_publishers: list[_CyclePublisher] = []
    warnings = _DrainWarningCounter()
    logger = logging.getLogger(LOGGER_NAME)
    prev_level = logger.level
    prev_propagate = logger.propagate
    logger.addHandler(warnings)
    logger.setLevel(logging.WARNING)
    logger.propagate = False
    try:
        # Baseline cycles: full restart cycles whose strands stay pending,
        # so the post-baseline snapshot captures the process at a
        # cycle-scale steady state rather than an empty first-use state.
        for cycle in range(BASELINE_STRANDS):
            await _run_restart_cycle(
                adapter,
                cycle,
                publishers,
                sinks,
                stranded,
                stranded_indices,
                warnings,
                all_publishers,
            )
        gc.collect()
        baseline, raw_peak_absolute = tracemalloc.get_traced_memory()
        assert baseline > 0
        _assert_census(stranded, BASELINE_STRANDS)
        assert not adapter._background_tasks

        # Measured cycles: each restart grows the live-strand census by
        # exactly one while every prior strand stays alive and untracked.
        for cycle in range(BASELINE_STRANDS, CYCLES):
            await _run_restart_cycle(
                adapter,
                cycle,
                publishers,
                sinks,
                stranded,
                stranded_indices,
                warnings,
                all_publishers,
            )

        # --------------------------------------------------------------
        # Peak accumulation: every stranded publisher of every cycle
        # still alive, untracked, and fenced from the active session.
        # --------------------------------------------------------------
        _assert_census(stranded, CYCLES)
        assert all(task not in adapter._background_tasks for task in stranded)
        assert sum(sink.counts["scheduled"] for sink in sinks) == CYCLES * CAP
        assert (
            sum(sink.counts["overflow"] for sink in sinks) == CYCLES * REFUSED_PER_CYCLE
        )
        assert (
            sum(sink.counts["cancelled_before_admission"] for sink in sinks) == CYCLES
        )

        # --------------------------------------------------------------
        # Final session: full capacity under peak accumulation, and the
        # token-ownership boundary for a re-committed identity.
        # --------------------------------------------------------------
        final_base = CYCLES * ARRIVALS_PER_CYCLE
        final_fresh = list(range(final_base, final_base + ARRIVALS_PER_CYCLE))
        final_admitted = [stranded_indices[0], *final_fresh[: CAP - 1]]
        final_refused = final_fresh[CAP : CAP + 2]
        # The final window must drain cleanly, so no body in it can
        # match the stubborn marker: every admitted publication is
        # cooperative and parks on the gate alone.
        final_publisher = _CyclePublisher(
            CAP, stubborn_body=f"final-drain {final_base}"
        )
        all_publishers.append(final_publisher)
        final_sink = _CycleSink(adapter.adapter_id)
        await _start(adapter, final_publisher, final_sink)

        diagnostics = adapter.diagnostics()
        assert diagnostics["pending_ingress_callbacks"] == 0
        assert not adapter._inbound_dedup
        _assert_census(stranded, CYCLES)

        for index in [*final_admitted, *final_refused]:
            adapter._on_message(_packet(index))
        diagnostics = adapter.diagnostics()
        assert diagnostics["pending_ingress_callbacks"] == CAP
        assert diagnostics["producer_scheduled"] == CAP
        assert diagnostics["producer_overflow"] == len(final_refused)
        key_zero = _dedup_key(_packet(stranded_indices[0]))
        assert key_zero is not None
        assert key_zero in adapter._inbound_dedup
        current_token = adapter._inbound_dedup[key_zero]

        # A duplicate of the re-committed identity stays suppressed.
        adapter._on_message(_packet(final_admitted[0]))
        diagnostics = adapter.diagnostics()
        assert diagnostics["pending_ingress_callbacks"] == CAP
        assert diagnostics["producer_overflow"] == len(final_refused)

        # Late cancellation of the first stranded publisher: its stale
        # token targets the same key this session re-committed.  The
        # ownership check must keep the newer reservation.
        stranded_zero = stranded[0]
        stranded_zero.cancel()
        await asyncio.wait_for(
            asyncio.gather(stranded_zero, return_exceptions=True), 10
        )
        assert stranded_zero.done()
        assert stranded_zero.cancelled()
        assert adapter._inbound_dedup.get(key_zero) is current_token
        adapter._on_message(_packet(final_admitted[0]))
        diagnostics = adapter.diagnostics()
        assert diagnostics["pending_ingress_callbacks"] == CAP
        assert diagnostics["producer_overflow"] == len(final_refused)
        assert diagnostics["producer_cancelled_before_admission"] == 0
        assert sinks[0].counts["cancelled_before_admission"] == 1
        _assert_census(stranded, CYCLES - 1)

        # The final window publishes in admission order; the late
        # cancellation reported nothing anywhere.
        final_publisher.gate.set()
        await asyncio.wait_for(
            asyncio.gather(*adapter._background_tasks, return_exceptions=True), 30
        )
        assert not adapter._background_tasks
        diagnostics = adapter.diagnostics()
        assert diagnostics["pending_ingress_callbacks"] == 0
        assert diagnostics["inbound_published"] == CAP
        assert final_sink.counts == Counter(
            {"scheduled": CAP, "overflow": len(final_refused)}
        )
        await adapter.stop(timeout=DRAIN_BUDGET_SECONDS)

        gc.collect()
        peak_retained, raw_peak_absolute = tracemalloc.get_traced_memory()
        peak_extra = max(0, peak_retained - baseline)

        # --------------------------------------------------------------
        # Endgame: settle every remaining stranded publisher — late
        # success on even cycles, late running-cancellation on odd
        # cycles.  Every settle belongs to a retired epoch.
        # --------------------------------------------------------------
        for cycle in range(1, CYCLES, 2):
            stranded[cycle].cancel()
        for cycle in range(2, CYCLES, 2):
            publishers[cycle].stubborn_release.set()
        outstanding = _pending_stranded(stranded)
        if outstanding:
            _, still_pending = await asyncio.wait(outstanding, timeout=30)
            assert not still_pending
        assert all(task.done() for task in stranded)
        for cycle in range(1, CYCLES, 2):
            assert stranded[cycle].cancelled()
        for cycle in range(2, CYCLES, 2):
            assert not stranded[cycle].cancelled()

        # The census is empty, nothing stays tracked or reserved, and
        # the late settles moved no counter and reported nothing.
        _assert_census(stranded, 0)
        assert not adapter._background_tasks
        assert not adapter._inbound_dedup
        diagnostics = adapter.diagnostics()
        assert diagnostics["pending_ingress_callbacks"] == 0
        assert diagnostics["inbound_published"] == CAP
        assert diagnostics["producer_cancelled_before_admission"] == 0
        assert (
            sum(sink.counts["cancelled_before_admission"] for sink in sinks) == CYCLES
        )
        assert warnings.count == CYCLES
        assert adapter._callback_admission_epoch == 2 * (CYCLES + 1)

        # Drop the test's own bookkeeping before the residue measurement
        # so the guard observes the process, not the test's lists.
        publishers.clear()
        sinks.clear()
        stranded.clear()
        all_publishers.clear()
        gc.collect()
        final_retained, raw_peak_absolute = tracemalloc.get_traced_memory()
        final_extra = max(0, final_retained - baseline)

        # Dual process-lifetime bounds: the growth that 25 deliberately
        # stranded publishers forced beyond the warmed steady state
        # stays under the absolute extra-bytes bound, and after every
        # stranded publisher settles the retained footprint returns
        # below twice that baseline — nothing stays pinned across the
        # process lifetime.
        assert peak_extra < RETAINED_EXTRA_BYTES_BOUND, (
            "cycling-phase accumulation must stay bounded "
            f"(baseline={baseline}B, peak_extra={peak_extra}B)"
        )
        assert final_retained < RETAINED_RATIO_BOUND * baseline, (
            "retained footprint must return near the post-warm-up baseline "
            f"(baseline={baseline}B, final={final_retained}B)"
        )
        assert final_extra < RETAINED_EXTRA_BYTES_BOUND, (
            "settled process residue must stay bounded "
            f"(baseline={baseline}B, final_extra={final_extra}B)"
        )

        _write_report(
            "meshcore-process-lifetime.json",
            {
                "transport": "meshcore",
                "tier": "synthetic_sdk_callback",
                "restart_cycles": CYCLES,
                "baseline_strands": BASELINE_STRANDS,
                "peak_strands": CYCLES,
                "capacity": CAP,
                "arrivals_per_cycle": ARRIVALS_PER_CYCLE,
                "refused_per_cycle": REFUSED_PER_CYCLE,
                "drain_budget_seconds": DRAIN_BUDGET_SECONDS,
                "scheduled_total": CYCLES * CAP,
                "refused_total": CYCLES * REFUSED_PER_CYCLE,
                "late_pre_start_cancellations": CYCLES,
                "late_success_settles": CYCLES // 2,
                "late_cancel_settles": CYCLES - CYCLES // 2,
                "drain_timeout_warnings": CYCLES,
                "retained_baseline_bytes": baseline,
                "retained_peak_bytes": peak_retained,
                "retained_peak_extra_bytes": peak_extra,
                "retained_final_bytes": final_retained,
                "retained_final_extra_bytes": final_extra,
                "retained_ratio_bound": RETAINED_RATIO_BOUND,
                "retained_extra_bytes_bound": RETAINED_EXTRA_BYTES_BOUND,
                "raw_peak_bytes": raw_peak_absolute,
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
                "overflow_policy": "bounded_callback_capacity",
            },
        )
    finally:
        logger.removeHandler(warnings)
        logger.setLevel(prev_level)
        logger.propagate = prev_propagate
        for publisher in all_publishers:
            publisher.gate.set()
            publisher.stubborn_release.set()
        outstanding = tuple(_pending_stranded(stranded))
        if outstanding:
            await bounded_cancel_and_reap(outstanding, timeout=1.0)
        if owns_tracing:
            tracemalloc.stop()
        await adapter.stop(timeout=DRAIN_BUDGET_SECONDS)
