# Bound MeshCore callback admission with dedup-safe overflow reporting

The MeshCore SDK callback schedules one publication task per distinct inbound
packet. `MeshCoreConfig.max_pending_ingress_callbacks` bounds the publication
tasks pending per adapter; the default `100` is provisional pending a
measured-budget round and validation rejects non-positive values. The callback
reserves a slot with a non-blocking compare-and-increment on the owner loop
before it commits the dedup key, so the loop never blocks on an async slot and
no semaphore is held across an await.

Because the reservation precedes the dedup commit, a capacity refusal never
poisons dedup: a refused packet schedules no task and caches no identity, so
its next delivery is re-admitted once capacity frees. Content-free overflow
warnings are rate-limited to the first refusal and at most one per five seconds,
with a cumulative count; every refusal still increments diagnostics and reports
producer pressure. Accepted packets report `scheduled` through the optional
`report_producer_pressure` seam; refusals report `overflow` with the
`callback_capacity` reason; publication tasks cancelled before their first step report
`cancelled_before_admission`. Each report carries `count=1` and the adapter's
own id and never carries message content, sender identity, or key material.
Reports are pre-admission accounting only: they never claim durable admission
and never authorize retry or resend.

The seam is best-effort. Reporting is skipped when the runtime context predates
the seam or the report type is unavailable on the installed core, and a failing
sink is contained without affecting admission. Diagnostics expose
`pending_ingress_callbacks`, `max_pending_ingress_callbacks`,
`producer_scheduled`, `producer_overflow`, and
`producer_cancelled_before_admission`.

Reservations are released by each publication task's done callback — covering
completion, publish failure, cancellation, and tasks cancelled before their
coroutine first stepped — and reset at session boundaries, so stop/start and
reconnect leave no stale reservations. Callbacks bind their capacity release
to a session epoch: a late task from a timed-out stop cannot release a new
session slot or inflate its cancellation and publication counters.
Background-task drains use a bounded wait that does not depend on a
publisher acknowledging cancellation.

Running cancellation is phase-ambiguous and does not supply a pre-admission
cancellation fact. Simulation completions obey the same epoch fence. Rejected
task creation closes its coroutine and releases the slot and provisional key.
