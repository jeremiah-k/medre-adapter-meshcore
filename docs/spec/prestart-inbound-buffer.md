# Pre-start inbound buffer

> **Authority:** Authoritative specification for queue-then-flush handling of
> inbound MeshCore payloads that arrive between session startup and the
> adapter's started transition.

The pinned MeshCore SDK's `start_auto_message_fetching()` issues one
`get_msg()` while `MeshCoreSession.start()` is still awaiting inside
`MeshCoreAdapter.start()`. The SDK dispatcher fans the fetched frame out to
the session's inbound subscription, so `MeshCoreAdapter._on_message()` can run
before the adapter has set `_started`. Because `GET_NEXT_MSG` pops the frame
from the device queue, a silent drop at that boundary loses the message unless
the mesh re-delivers it. This page defines the queue-then-flush contract for
that window.

## Buffering window

The adapter MUST treat an inbound payload delivered while `_started` is false
and `start()` is in flight (`_starting` set) as bufferable, not droppable.
This applies to the synchronous SDK callback path and to any other inbound
entry point that carries the same not-started early return, such as
`simulate_inbound()`. The buffer MUST be bounded: it MUST hold at most 64
packets and MUST NOT grow without bound. The adapter MUST snapshot each payload
mapping when buffering it so later caller mutations cannot replace a queued
message. Session callbacks received outside an active start or started lifetime
MUST silently drop payloads without buffering. `simulate_inbound()` MUST raise
`RuntimeError` when no context exists; otherwise inactive calls silently drop.
Callbacks from a retired session MUST NOT enter a replacement lifecycle's buffer
or live admission path. A concurrent start without an intervening stop MUST be
rejected before changing the current startup owner, and a start while an
earlier start's session remains installed MUST be refused before any mutation,
as `connection-recovery.md` specifies.

## Overflow

A buffer at capacity MUST refuse a new payload with a cumulative diagnostics
counter and a content-free warning. The warning MUST be rate-limited the same
way as bounded-admission overflow warnings and MUST NOT carry message content,
sender identity, or key material. Overflow MUST NOT evict already-buffered
frames and MUST NOT be silent.

## Flush

After `start()` sets `_started`, the adapter MUST flush buffered payloads
through the same normal inbound path used for live callbacks — re-entering
the synchronous handler — in arrival order, and then clear the buffer.
Flushed payloads MUST pass every existing guard (classification, dedup,
bounded callback admission) as if they had just been received; the flush MUST
NOT bypass or shortcut any guard. The flush re-entry MUST be synchronous so
that no live callback can interleave between buffer hand-offs.

## Lifecycle boundaries

`stop()` MUST close the buffering window and discard the buffer before its
first await; callbacks arriving after stop starts MUST NOT reopen the buffer.
The session's own stop clears its inbound callback in the same synchronous
prologue, as `connection-recovery.md` specifies.
A `start()` still awaiting an SDK session that was stopped MUST NOT mark the
adapter started or flush any prior frames when that await completes. A stopped
adapter MUST NOT replay buffered frames into a later session. A failed or
cancelled start MUST also discard the buffer before cleanup completes. Discarding a non-empty buffer MUST be
observed by a content-free warning: the loss is real (the device queue was
already drained) even though no publication is attempted.

## Diagnostics

Diagnostics MUST expose cumulative per-lifecycle counters for pre-start
buffering: `prestart_buffered` (frames accepted into the buffer),
`prestart_flushed` (frames handed to the normal path), and
`prestart_overflow_dropped` (frames refused on a full buffer). Counters MUST
reset at each start boundary. The counters are process-local observations and
MUST NOT be treated as durable evidence.
