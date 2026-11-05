# Bounded callback admission

> **Authority:** Authoritative specification for bounded, loss-honest
> admission of MeshCore inbound callbacks between the synchronous SDK
> callback and the canonical inbound pipeline.

MeshCore delivers decoded packets through a synchronous SDK callback that
runs on the owner event loop. Creating one publication task per distinct
packet outside core's bounded admission gate accumulates producer-side
pressure the admission gate cannot see: a held canonical publisher leaves
every scheduled task pending. This page defines the admission contract for
that boundary.

## Admission bound

The adapter MUST bound pending inbound publication tasks by
`max_pending_ingress_callbacks`. The setting MUST be a positive integer;
zero, negative, boolean and non-integer values MUST be rejected by
configuration validation. There MUST NOT be an unbounded mode. The default
MUST be a finite value aligned with core's admission default; the shipped
default (`100`) is qualified for the synthetic bounded-admission shapes
recorded in `docs/dev/ingress-pressure.md` — burst, sustained-imbalance,
cancellation-resistance, post-burst loop stepping and memory evidence. It
MUST NOT be described as a throughput or hardware guarantee; real-SDK and
physical-device qualification remains separate.

The reservation MUST be a non-blocking check executed on the owner event
loop. The callback MUST NOT wait synchronously for an asynchronous slot,
MUST NOT acquire a semaphore across an `await`, and the event loop MUST
NOT block while capacity is exhausted.

## Dedup ordering

The slot MUST be reserved before the dedup key is committed. A
producer-refused packet MUST NOT consume dedup state or native identity,
so a refused identity stays retryable by the radio: its next delivery MUST
be admitted once capacity frees. A reservation MUST NOT be held by a
packet that was never scheduled.

## Overflow semantics

A refused packet MUST NOT be scheduled, MUST NOT be admitted and MUST NOT
create a publication task. The refusal MUST be explicit, loss-honest
accounting: the adapter MUST increment a `producer_overflow` diagnostics
counter, report an `overflow` producer-pressure fact with reason
`callback_capacity` through the optional sink
(`AdapterContext.report_producer_pressure`) when present. The adapter MUST
warn on the first refusal of a session and SHOULD rate-limit subsequent
warnings to at most one per five seconds; a warning MUST carry the cumulative
refusal count and MUST NOT contain message text, sender identity or key
material. Every refusal MUST still be counted and reported independently of
warning suppression. Refusal MUST NOT imply admission, authorize retry or
resend, or route the packet toward any recovery authority. Producer staging is explicitly not
lossless: overflow loss may be irreversible for a cursor-less radio
source.

## Reservation lifecycle

A reservation MUST be released exactly once per acquisition, on every
terminal state of the publication task — completion, failure and
cancellation alike, including tasks cancelled before their coroutine first
stepped. Stop MUST release or reset every outstanding reservation after
draining, and a timed-out drain MUST NOT strand reservations into the next
session. A new session MUST start from full, private capacity; a release
from a retired session MUST NOT grant capacity to the current one.
Every reservation MUST bind its done callback to the creating session epoch;
late completions MUST NOT modify pending slots, cancellation counts, or
`inbound_published` diagnostics belonging to a newer session. A late
pre-start cancellation MUST report to the original session's pressure sink when
available; it MUST NOT be attributed to the restarted session. The stop
budget MUST be a real deadline even when a callback's publisher does not
cooperate with cancellation. A timed-out drain MAY leave a retired task
alive, but MUST retire its capacity epoch before a new session starts.

## Producer pressure accounting

Accepted staging MUST report a `scheduled` producer-pressure fact. A
publication task cancelled before admission MUST report
`cancelled_before_admission` and be counted in diagnostics. Reports MUST
be content-free pre-admission facts through the optional sink; they never
claim durable admission, native acceptance or replay authority, and their
loss when the sink is absent or the store fails is operational-accounting
loss, never message durability.

Cancellation of an already-started publication MUST NOT be reported as proof
that admission never began. Only cancellation before the publication starts
supports `cancelled_before_admission`; a successful publication that requests
cancellation while returning MUST retain its admitted identity and MUST NOT
increment this outcome. The simulation path MUST also fence post-await counters
and dedup commits against a retired epoch.

## Process lifetime across restart cycles

Repeated restart cycles in one process MUST NOT accumulate abandoned
cancellation-resistant publications, tracked task registry entries,
admission reservations or dedup state. After every bounded stop the tracked
publication-task registry MUST be empty even while a retired task is still
pending, and each restarted session MUST admit a full window while every
prior cycle's stranded publisher is still alive. The process-wide census of
adapter-created pending publication tasks MUST grow by exactly one stranded
publisher per cycle, and MUST return to empty once every stranded publisher
settles. Late completions and late running cancellations MUST NOT change any
newer session's capacity or counters, and a late pre-start cancellation MUST
report through its own retired session's pressure sink at cycle scale, never
through the restarted session's.

Dedup state MUST NOT cross a session boundary: stop and start clear the map,
so a retired session's reservation MUST NOT suppress a new-session
re-admission of the same identity. Token ownership governs the remaining
within-session race: a stale ownership token from a retired session MUST NOT
evict a reservation the active session re-committed for the same key, and a
duplicate of that identity stays suppressed after the stale handler
settles. The boundary clear is what lets a stranded identity be
re-admitted; ownership decides nothing across a cleared boundary.

Process memory MUST stay bounded across the whole cycling run. With the
first restart cycles retained as a warmed steady-state baseline, the peak
growth of the remaining cycles MUST stay under the recorded absolute
extra-bytes bound, and once every stranded publisher settles the retained
footprint MUST return below twice that baseline.

Qualification scope: these shapes are proven by synthetic SDK-callback tests
in `tests/test_meshcore_process_lifetime_admission.py` (fake mode, no
network, no hardware) using a test double for the cancellation-resistant
publisher. They are not a claim about real MeshCore SDK callback behavior,
throughput, hardware or RF performance.
