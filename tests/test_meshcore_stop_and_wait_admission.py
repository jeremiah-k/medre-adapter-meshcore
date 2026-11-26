"""Stop-and-wait drain: at most one popped message is ever in flight.

The node's fetch is a destructive pop with no acknowledgement in the
protocol: a message the node has popped is gone from the queue, so anything
this process holds in memory dies with the process. Core's live-adapter
ingress commits durably before ``publish_inbound`` returns, and the
publication task's completion is the existing seam that signals exactly
that. So the receive pump waits until the fetched message's publication has
committed before it issues the next fetch: a crash can lose at most the one
message that was in flight.

Deterministic adapter-boundary tests: no SDK, no network, no fixed sleeps.
The bound tests pin that the next fetch starts only from the fetched
message's durable commit, and that traffic arriving during a gate wait
publishes on its own reservation.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from medre.core.contracts.adapter import AdapterContext
from medre.core.storage.backend import DuplicateEventError

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.config.adapters.meshcore import MeshCoreConfig
from tests.medre_meshcore_tests.helpers.async_utils import wait_until


def _packet(index: int) -> dict:
    """Make a relayable unique channel packet without SDK/hardware state."""
    return {
        "text": f"stop-and-wait {index}",
        "pubkey_prefix": "stop-and-wait-source",
        "sender_timestamp": index + 1,
        "type": "CHAN",
        "txt_type": 0,
        "channel_idx": 0,
    }


class _TrackedPublisher:
    """Record when each publication enters and finishes, with per-run holds."""

    def __init__(self, holds: list[asyncio.Event] | None = None) -> None:
        self.events: list[tuple[str, int]] = []
        self.entered = asyncio.Event()
        self.holds = holds if holds is not None else []
        self._runs = 0

    async def __call__(self, canonical) -> None:
        self._runs += 1
        index = self._runs
        self.events.append(("enter", index))
        self.entered.set()
        if self.holds:
            await self.holds[min(index, len(self.holds)) - 1].wait()
        self.events.append(("commit", index))

    def commit_indexes(self) -> list[int]:
        return [i for kind, i in self.events if kind == "commit"]


async def _start_adapter(publisher: Any) -> MeshCoreAdapter:
    """Start the real adapter the same way the pump-permit suite does."""
    context: dict[str, Any] = {
        "adapter_id": "stop-and-wait",
        "publish_inbound": publisher,
        "logger": logging.getLogger("test.stop-and-wait"),
        "clock": lambda: datetime.now(UTC),
        "shutdown_event": asyncio.Event(),
    }
    ctx = AdapterContext(**context)
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="stop-and-wait", max_pending_ingress_callbacks=4)
    )
    await adapter.start(ctx)
    return adapter


async def test_the_next_fetch_waits_until_the_fetched_message_is_durable() -> None:
    """While a fetched publication is pending, no further fetch may start."""
    hold = asyncio.Event()
    publisher = _TrackedPublisher(holds=[hold])
    adapter = await _start_adapter(publisher)
    try:
        gate = adapter._session._wait_for_receive_capacity
        # The pump reserves before its fetch and the fetched frame arrives.
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(0))
        assert await asyncio.wait_for(publisher.entered.wait(), 2)
        # The next fetch must not start while publication 1 is in flight.
        next_gate = asyncio.create_task(gate())
        for _ in range(5):
            await asyncio.sleep(0.01)
        assert (
            not next_gate.done()
        ), "the pump must wait for the fetched message's durable commit"
        assert publisher.events == [("enter", 1)]
        assert adapter._pump_publication_outstanding is True
        # The publication commits (publish_inbound returned): only now may
        # the next fetch start.
        hold.set()
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 2)
        await asyncio.wait_for(next_gate, 2)
        assert adapter._pump_publication_outstanding is False
        assert adapter._pump_permit_pending is True
        assert adapter._pending_callback_admissions == 1
    finally:
        hold.set()
        await adapter.stop()


async def test_a_multi_message_drain_is_one_message_in_flight_in_order() -> None:
    """Three fetched messages publish strictly one at a time, in order."""
    holds = [asyncio.Event() for _ in range(3)]
    publisher = _TrackedPublisher(holds=holds)
    adapter = await _start_adapter(publisher)
    try:
        gate = adapter._session._wait_for_receive_capacity
        gate_call = asyncio.create_task(gate())
        for index in range(3):
            await asyncio.wait_for(gate_call, 2)
            adapter._on_message(_packet(index))
            # Publication index+1 is the only one in flight: every earlier
            # publication has already committed.
            assert publisher.commit_indexes() == list(
                range(1, index + 1)
            ), "publications must commit strictly one at a time, in order"
            if index < 2:
                # The next fetch waits until this publication commits.
                gate_call = asyncio.create_task(gate())
                for _ in range(5):
                    await asyncio.sleep(0.01)
                assert not gate_call.done(), (
                    "the pump must wait for the durable commit before the " "next fetch"
                )
            holds[index].set()
            assert await wait_until(
                lambda index=index: len(publisher.commit_indexes()) == index + 1,
                timeout=2,
            )
        assert adapter._pump_publication_outstanding is False
    finally:
        for hold in holds:
            hold.set()
        await adapter.stop()


async def test_a_frame_arriving_while_the_gate_waits_publishes_and_is_counted() -> None:
    """Traffic that lands while the pump waits for capacity still
    publishes; the pump resumes once its slot frees and holds no stale
    state from the interruption."""
    holds = [asyncio.Event(), asyncio.Event()]
    # The second publication commits immediately; only the fetched one
    # is held on the test gate.
    holds[1].set()
    publisher = _TrackedPublisher(holds=holds)
    adapter = await _start_adapter(publisher)
    try:
        gate = adapter._session._wait_for_receive_capacity
        # The pump reserves, its fetched frame arrives, and its
        # publication holds the drain.
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(0))
        assert await asyncio.wait_for(publisher.entered.wait(), 2)
        # While the gate is blocked, another frame arrives through the
        # SDK event callback and publishes on its own reservation.
        gate_task = asyncio.create_task(gate())
        for _ in range(5):
            await asyncio.sleep(0.01)
        assert not gate_task.done(), "the gate must wait for the commit"
        adapter._on_message(_packet(1))
        assert await wait_until(lambda: publisher.commit_indexes() == [2], timeout=2), (
            "the frame that arrived during the wait must commit on its "
            "own reservation while the fetched one is still in flight"
        )
        assert adapter._pump_publication_outstanding is True
        # The fetched publication commits: the pump's slot frees and the
        # gate resumes with a fresh reservation, holding no stale state.
        holds[0].set()
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 2)
        await asyncio.wait_for(gate_task, 2)
        assert adapter._pump_permit_pending is True
        assert adapter._pending_callback_admissions == 1
        assert adapter._pump_publication_outstanding is False
    finally:
        holds[0].set()
        await adapter.stop()


async def test_failed_fetched_admission_retains_identity_and_blocks_next_fetch():
    entered = asyncio.Event()
    recover = asyncio.Event()
    attempts = []

    async def publisher(canonical):
        attempts.append(canonical)
        if not recover.is_set():
            entered.set()
            raise RuntimeError("injected admission failure")

    adapter = await _start_adapter(publisher)
    next_gate = None
    try:
        gate = adapter._session._wait_for_receive_capacity
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(0))
        await asyncio.wait_for(entered.wait(), 2)
        next_gate = asyncio.create_task(gate())
        assert adapter._pump_publication_outstanding
        assert adapter._pending_callback_admissions == 1
        adapter._on_message(_packet(0))
        assert len(adapter._background_tasks) == 1
        recover.set()
        await asyncio.wait_for(next_gate, 3)
        assert len(attempts) == 2
        assert attempts[0] is attempts[1]
        assert not adapter._pump_publication_outstanding
        assert adapter._pump_permit_pending
    finally:
        if next_gate is not None:
            next_gate.cancel()
            await asyncio.gather(next_gate, return_exceptions=True)
        await adapter.stop()


async def test_fetched_duplicate_promotes_pending_unsolicited_publication():
    entered = asyncio.Event()
    release = asyncio.Event()
    attempts = []

    async def publisher(canonical):
        attempts.append(canonical)
        if len(attempts) == 1:
            entered.set()
            await release.wait()
            raise RuntimeError("first admission failed")

    adapter = await _start_adapter(publisher)
    gate_task = None
    try:
        adapter._on_message(_packet(0))
        await asyncio.wait_for(entered.wait(), 2)
        gate = adapter._session._wait_for_receive_capacity
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(0))
        assert adapter._pump_publication_outstanding
        assert adapter._pending_callback_admissions == 1
        gate_task = asyncio.create_task(gate())
        release.set()
        await asyncio.wait_for(gate_task, 3)
        assert len(attempts) == 2
        assert attempts[0] is attempts[1]
    finally:
        release.set()
        if gate_task is not None:
            gate_task.cancel()
            await asyncio.gather(gate_task, return_exceptions=True)
        await adapter.stop()


async def test_a_never_admissible_event_releases_the_drain():
    """A failure that is deterministic for the event (core's validation
    raises ValueError or TypeError only for an event that can never be
    admitted) is not retried: the identity and the reservation are
    released, and the next message publishes."""
    attempts = []

    async def publisher(canonical):
        attempts.append(canonical)
        if len(attempts) == 1:
            raise ValueError("injected validation refusal")

    adapter = await _start_adapter(publisher)
    try:
        gate = adapter._session._wait_for_receive_capacity
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(0))
        # The refused event released its identity and reservation: the
        # pump re-reserves and the drain continues with the next message,
        # not a retry of the refused one.
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(1))
        await asyncio.wait_for(
            asyncio.gather(*tuple(adapter._background_tasks)), 2
        )
        assert len(attempts) == 2
        assert attempts[0] is not attempts[1]
        assert not adapter._pump_publication_outstanding
        assert not adapter._pending_dedup_publications
    finally:
        await adapter.stop()


async def test_retention_and_the_gate_survive_a_session_reconnect():
    """A warm reconnect inside one adapter lifecycle replaces the pump
    task and the connection state but bumps no admission epoch: the
    retained event and the stop-and-wait gate carry across it unchanged,
    and recovery admits the same event exactly once."""
    entered = asyncio.Event()
    recover = asyncio.Event()
    attempts = []

    async def publisher(canonical):
        attempts.append(canonical)
        if not recover.is_set():
            entered.set()
            raise RuntimeError("injected admission failure")

    adapter = await _start_adapter(publisher)
    next_gate = None
    try:
        gate = adapter._session._wait_for_receive_capacity
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(0))
        await asyncio.wait_for(entered.wait(), 2)
        epoch_before = adapter._callback_admission_epoch
        # The churn a warm reconnect drives on the session: the pump task
        # is replaced and the connection drops and restores. Adapter
        # epochs move only at adapter start and stop.
        session = adapter._session
        await session._stop_receive_pump()
        session._set_connected(False)
        session._set_connected(True)
        session._start_receive_pump()
        assert adapter._callback_admission_epoch == epoch_before
        assert adapter._pump_publication_outstanding is True
        assert adapter._pending_callback_admissions == 1
        assert len(adapter._pending_dedup_publications) == 1
        next_gate = asyncio.create_task(gate())
        for _ in range(5):
            await asyncio.sleep(0.01)
        assert not next_gate.done(), "the gate must hold across the reconnect"
        recover.set()
        await asyncio.wait_for(next_gate, 3)
        assert len(attempts) == 2
        assert attempts[0] is attempts[1]
        assert not adapter._pump_publication_outstanding
    finally:
        if next_gate is not None:
            next_gate.cancel()
            await asyncio.gather(next_gate, return_exceptions=True)
        await adapter.stop()


async def test_stale_publication_completion_keeps_a_newer_owners_entry():
    """Pending drain ownership is token-fenced: a stale publication's
    completion cannot evict a newer owner's dedup key or pending entry,
    and the pending map never holds more than outstanding publications."""
    hold = asyncio.Event()
    publisher = _TrackedPublisher(holds=[hold])
    adapter = await _start_adapter(publisher)
    try:
        gate = adapter._session._wait_for_receive_capacity
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(0))
        key = next(iter(adapter._pending_dedup_publications))
        stale_token = adapter._pending_dedup_publications[key][0]
        # A redelivery re-commits the identity with a new token after the
        # LRU evicted the stale entry, as _commit_dedup_key allows.
        adapter._inbound_dedup.pop(key)
        new_token = adapter._commit_dedup_key(key)
        adapter._pending_dedup_publications[key] = (new_token, lambda: None)
        # The stale publication's completion path must not evict the
        # newer owner's entries.
        adapter._retire_pending_publication(key, stale_token)
        assert adapter._pending_dedup_publications[key][0] is new_token
        hold.set()
        await asyncio.wait_for(
            asyncio.gather(*tuple(adapter._background_tasks)), 2
        )
        assert adapter._pending_dedup_publications[key][0] is new_token
        assert len(adapter._pending_dedup_publications) == 1
    finally:
        hold.set()
        await adapter.stop()


async def test_an_evicted_identity_still_promotes_from_the_pending_map():
    """A fetched duplicate whose dedup key the LRU evicted still matches
    the pending map: it transfers drain ownership to the in-flight
    publication instead of publishing a second copy."""
    hold = asyncio.Event()
    entered = asyncio.Event()
    attempts = []

    async def publisher(canonical):
        attempts.append(canonical)
        entered.set()
        await hold.wait()

    adapter = await _start_adapter(publisher)
    try:
        gate = adapter._session._wait_for_receive_capacity
        adapter._on_message(_packet(0))
        await asyncio.wait_for(entered.wait(), 2)
        # LRU pressure evicts the in-flight identity from the dedup map.
        for index in range(1030):
            adapter._commit_dedup_key(f"pressure-{index}")
        key = next(iter(adapter._pending_dedup_publications))
        assert key not in adapter._inbound_dedup
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(0))
        assert len(adapter._background_tasks) == 1
        assert adapter._pump_publication_outstanding is True
        assert len(attempts) == 1
        hold.set()
        await asyncio.wait_for(
            asyncio.gather(*tuple(adapter._background_tasks)), 2
        )
        assert key not in adapter._pending_dedup_publications
        assert len(adapter._pending_dedup_publications) == 0
    finally:
        hold.set()
        await adapter.stop()


async def test_a_stored_identity_ends_the_publication_without_retry(caplog) -> None:
    """A message whose identity core reports as stored ends the
    publication: no retry, the dedup key stays committed, the gate is
    released, the next fetched message publishes, and the published
    count is unchanged by the stored one."""
    attempts: list[Any] = []

    async def publisher(canonical):
        attempts.append(canonical)
        if len(attempts) == 1:
            raise DuplicateEventError("injected stored identity")

    caplog.set_level(logging.INFO, logger="test.stop-and-wait")
    adapter = await _start_adapter(publisher)
    try:
        gate = adapter._session._wait_for_receive_capacity
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(0))
        key = next(iter(adapter._inbound_dedup))
        # The stored identity ends the publication: the drain continues
        # with the next fetch instead of a retry of the same message.
        await asyncio.wait_for(gate(), 2)
        adapter._on_message(_packet(1))
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 2)
        assert len(attempts) == 2
        assert attempts[0] is not attempts[1]
        assert key in adapter._inbound_dedup
        assert adapter._inbound_published == 1
        infos = [
            r
            for r in caplog.records
            if r.name == "test.stop-and-wait"
            and r.levelno == logging.INFO
            and "stored" in r.getMessage()
        ]
        assert len(infos) == 1, [r.getMessage() for r in infos]
        assert not [
            r.getMessage()
            for r in caplog.records
            if r.name == "test.stop-and-wait"
            and r.levelno >= logging.WARNING
            and "native fetches are paused" in r.getMessage()
        ]
    finally:
        await adapter.stop()


async def test_an_unsolicited_stored_identity_keeps_the_key(caplog) -> None:
    """An unsolicited message whose identity is stored ends the
    publication too: the key stays committed, a redelivered copy of the
    same frame stays suppressed, and nothing is retried or logged with
    a traceback."""
    attempts: list[Any] = []

    async def publisher(canonical):
        attempts.append(canonical)
        raise DuplicateEventError("injected stored identity")

    caplog.set_level(logging.INFO, logger="test.stop-and-wait")
    adapter = await _start_adapter(publisher)
    try:
        adapter._on_message(_packet(0))
        key = next(iter(adapter._inbound_dedup))
        await asyncio.wait_for(asyncio.gather(*tuple(adapter._background_tasks)), 2)
        assert key in adapter._inbound_dedup
        adapter._on_message(_packet(0))
        assert not adapter._background_tasks
        assert len(attempts) == 1
        infos = [
            r
            for r in caplog.records
            if r.name == "test.stop-and-wait"
            and r.levelno == logging.INFO
            and "stored" in r.getMessage()
        ]
        assert len(infos) == 1, [r.getMessage() for r in infos]
        assert not [
            r.getMessage()
            for r in caplog.records
            if r.name == "test.stop-and-wait"
            and (r.levelno >= logging.ERROR or r.exc_info)
        ]
    finally:
        await adapter.stop()
