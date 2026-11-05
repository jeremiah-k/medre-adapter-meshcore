# MeshCore synchronous-callback ingress pressure

`tests/test_meshcore_ingress_pressure.py` drives distinct relayable channel
callbacks with publication held by an event, including a 256-packet burst
against the default bound and a 32-packet burst below it.
`tests/test_meshcore_sustained_admission.py` qualifies the sustained shape:
bursts at 1x, 4x and 16x of the bound, a sustained-imbalance cycle,
cancellation-resistant publishers under saturation, and loop-lag plus memory
evidence. `tests/test_meshcore_process_lifetime_admission.py` extends the
record across restart cycles in one process: tracked-registry and task-census
bounds, per-session capacity honesty while prior strands stay alive, dedup
boundaries across sessions, and whole-run memory guards. These callbacks
exercise the real classifier, native dedup identity,
codec and the synchronous adapter callback on the asyncio owner loop, without
a physical companion or SDK socket.

## Bounded callback admission

The SDK callback reserves one admission slot per distinct packet before it
commits the dedup key and schedules the publication task. The reservation is a
non-blocking compare-and-increment on the owner loop: the callback never waits
for an async slot and no semaphore is held across an await, so the loop cannot
block or deadlock.

`MeshCoreConfig.max_pending_ingress_callbacks` bounds the publication tasks
pending per adapter. The default `100` has completed synthetic sustained-budget
qualification for the shapes recorded below; it is not a throughput or hardware
guarantee.
Validation rejects non-positive values, so the bound is always finite.

Ordering rule: the slot is reserved before the dedup key is committed. A packet
refused for capacity is therefore never recorded in dedup — no task is
scheduled and no identity is cached — so its next delivery is re-admitted once
capacity frees. Duplicates of an admitted packet keep suppressing as before,
both across the pending publish window and after durable admission; the
redelivery of a refused packet while capacity is still full is refused again.

## Loss-honest disposition

Each distinct relayable packet is either scheduled toward admission or
refused with explicit overflow accounting. A scheduled publication cancelled
before its publication begins additionally reports cancellation; these are different stages,
not mutually exclusive outcomes. Refusal and cancellation never
claim durable admission, never grant a receipt, and never imply a retry;
redelivery remains the radio's decision. Warnings are rate-limited: the first
refusal of a session and at most one per five seconds afterward, including the
cumulative refusal count, pending count and bound. Counters and producer-pressure
reports still include every refusal. Logs never include message content, sender
identity or key material.

Reservations are released by each publication task's done callback — covering
completion, publish failure, cancellation, and tasks cancelled before their
coroutine first stepped — and reset at session boundaries, so stop/start and
reconnect leave no stale reservations. Shutdown under load cancels pending
tasks. Pre-start cancellations are counted; a late pre-start cancellation from a
retired session still reports to the original context, without modifying
new-session counters. The queued-but-unstarted fence is unchanged: callbacks
that never started cannot publish after stop. A publisher already running and
ignoring cancellation can finish after a timed-out drain; this does not confer
new admission or send authority.

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
kept here as the baseline the bounded policy superseded. The default `100` is
qualified for the synthetic bounded-admission shapes below; real-SDK and
hardware throughput remain unqualified.

Running cancellation is not evidence that admission never began and is not
reported as `cancelled_before_admission`. A publication that successfully
returns while requesting task cancellation retains its admitted identity.
The simulation path also fences counters and dedup commits across epochs.

The configured budget is per session. Publishers that ignore cancellation may
survive a timed-out stop; repeated restarts can accumulate retired tasks outside
that budget. `tests/test_meshcore_process_lifetime_admission.py` pins that
accumulation at cycle scale: the tracked registry returns to empty after every
stop, the process-wide census of adapter-created tasks grows by exactly one
stranded publisher per cycle and settles to empty, and whole-run memory stays
inside the recorded bounds. The census covers adapter-created tasks only; it is
not a bound on tasks the SDK runtime owns.

## Measured sustained qualification (2026-11-04)

`tests/test_meshcore_sustained_admission.py` recorded the measured-budget
round for the default of `100`, qualifying the synthetic shapes described below.
Content-free JSON records export to `MEDRE_PRESSURE_REPORT_DIR`; the numbers
below are from one local run and
are reproducible per machine. They prove bounded admission above the cap,
dedup-safe re-admission of refused identities, epoch isolation under
saturation, and post-burst pending-window loop stepping. Synchronous burst
callback blocking is recorded separately; the tests do not prove its absence.
They do not prove throughput, real-SDK behavior, hardware or RF performance;
the post-burst sentinel bound is a generous CI guard, not an SLA.

Above-cap bursts with publication held (default bound 100):

| Burst | Pending | Refused | Re-admitted | Published | Sentinel lag |
|------:|--------:|--------:|------------:|----------:|-------------:|
| 100   | 100     | 0       | 0           | 100       | 1.32 ms      |
| 400   | 100     | 300     | 300         | 400       | 1.31 ms      |
| 1600  | 100     | 1500    | 1500        | 1600      | 1.37 ms      |

Every refused identity was redelivered after the release and re-admitted in
bound-sized slices with no new overflow, and counters and producer-pressure
reports reconciled exactly (`scheduled` equals the burst size, `overflow`
equals the refusal count). The admitted window published in admission order
and drained in full.

The initial 2026-11-04 `tracemalloc` record showed retained saturated-window
footprints near 534 KB and 538 KB for 100 and 1600 callbacks (about 1.008x),
with end-to-end footprints near 556 KB and 716 KB (about 1.29x). The revised
test warms up the adapter, measures retained bytes relative to each pre-burst
baseline, and requires both less than 2x growth and less than 256 KB extra
retained memory at 16x arrivals. It records synchronous callback burst time
separately from the post-burst sentinel. JSON records from each run supersede
the historical numbers when evaluating a new checkout. Absolute tracing
high-water and process RSS are context only, never memory-budget guarantees.
An already-running `tracemalloc` session is neither reset nor stopped.

Sustained imbalance used a small bound (8) over 200 cycles of 12 arrivals
each — 2,400 distinct deliveries plus 200 duplicates suppressed by dedup —
while the publisher released 4 publications per cycle. Every cycle kept
pending at or below the bound, `scheduled + refused` equaled distinct
deliveries (804 + 1,596), duplicates never consumed capacity or overflow, and
the full admitted sequence published exactly once in admission order. The
cycle completed in about 0.22 s with no wall-clock sleeps.

Saturation stop with a cancellation-resistant publisher: with 100 held
publications, `stop` cancelled the 99 cooperative publishers within a 1 s
drain budget, warned once about the single cancellation-suppressing
publication, retired it from tracking, and advanced the admission epoch. The
restarted session admitted a full 100-packet window and refused the 101st;
when the stubborn task finally completed (or was cancelled) after the
restart, the new session's pending count, capacity and counters were
untouched, and its cancellation was never misreported as
`cancelled_before_admission`. A retired-epoch seam check confirmed a late
pre-start cancellation reports through the original session's pressure sink
and never the restarted session's.
