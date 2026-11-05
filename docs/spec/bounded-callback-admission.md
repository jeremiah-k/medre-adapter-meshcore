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
default (`100`) is provisional pending a measured-budget qualification
round and MUST NOT be documented as a throughput guarantee.

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
