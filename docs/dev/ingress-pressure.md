# MeshCore synchronous-callback ingress pressure

`tests/test_meshcore_ingress_pressure.py` drives distinct relayable channel
callbacks with publication held by an event, including a 256-packet burst
against the default bound and a 32-packet burst below it. These callbacks
exercise the real classifier, native dedup identity, codec and the synchronous
adapter callback on the asyncio owner loop, without a physical companion or
SDK socket.

## Bounded callback admission

The SDK callback reserves one admission slot per distinct packet before it
commits the dedup key and schedules the publication task. The reservation is a
non-blocking compare-and-increment on the owner loop: the callback never waits
for an async slot and no semaphore is held across an await, so the loop cannot
block or deadlock.

`MeshCoreConfig.max_pending_ingress_callbacks` bounds the publication tasks
pending per adapter. The default `100` is provisional pending a measured-budget
round; validation rejects non-positive values, so the bound is always finite.

Ordering rule: the slot is reserved before the dedup key is committed. A packet
refused for capacity is therefore never recorded in dedup — no task is
scheduled and no identity is cached — so its next delivery is re-admitted once
capacity frees. Duplicates of an admitted packet keep suppressing as before,
both across the pending publish window and after durable admission; the
redelivery of a refused packet while capacity is still full is refused again.

## Loss-honest disposition

Every distinct packet reaching the callback is accounted exactly once: it is
scheduled toward admission, refused with explicit overflow accounting, or
cancelled before admission by lifecycle stop. Refusal and cancellation never
claim durable admission, never grant a receipt, and never imply a retry;
redelivery remains the radio's decision. Refusals log one content-free warning
line (adapter id, pending count, bound) — never message content, sender
identity, or key material.

Reservations are released by each publication task's done callback — covering
completion, publish failure, cancellation, and tasks cancelled before their
coroutine first stepped — and reset at session boundaries, so stop/start and
reconnect leave no stale reservations. Shutdown under load cancels pending
tasks; every cancelled task is reported and counted as cancelled before
admission. The queued-but-unstarted fence is unchanged: a callback that has
not yet run cannot publish after stop, and releasing the sink cannot complete
a publication after stop.

A publication task cancelled while the adapter remains started also releases
its provisional dedup reservation, so a retransmission of the same identity is
re-admitted instead of being silently suppressed by a reservation whose
publication never completed. The release is ownership-checked — a stale
handler cannot evict a reservation a newer delivery re-committed, on the
cancellation path or the publish-failure path — and a cancellation after a
successful publish keeps the admitted key.

## Producer pressure reports

Pre-admission outcomes are reported through the optional
`report_producer_pressure` seam with the closed vocabulary: `"scheduled"` when
a publication task is created, `"overflow"` with the `"callback_capacity"`
reason when bounded admission refuses a packet, and
`"cancelled_before_admission"` when staged work is dropped before any admission
attempt. Each report carries `count=1`, the adapter's own id, and no content.
The seam is best-effort: reports are skipped when the runtime context predates
it (older core) or the report type is unavailable, and a failing sink is
contained without affecting admission. Diagnostics expose the accounting
alongside the classifier counters: `pending_ingress_callbacks`,
`max_pending_ingress_callbacks`, `producer_scheduled`, `producer_overflow`,
and `producer_cancelled_before_admission`.

## Pre-cap measurement history

The held-publication measurements recorded below were taken while admission
was unbounded, before `max_pending_ingress_callbacks` existed. The harness
then retained exactly one asyncio task per distinct packet: 32 and 256
outstanding tasks, respectively, with all events publishing after release and
the task set draining. Those numbers document linear pre-admission task growth
and are not evidence of loss or of a sustainable capacity threshold; they are
kept here as the baseline the bounded policy superseded. The bound's default
remains provisional until a measured-budget round selects a durable value.
