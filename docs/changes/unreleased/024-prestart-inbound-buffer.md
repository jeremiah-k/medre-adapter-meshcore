# Buffer inbound payloads that arrive before startup completes

`MeshCoreAdapter.start()` awaits `MeshCoreSession.start()`, and the pinned
SDK's `start_auto_message_fetching()` issues one `get_msg()` inside that call.
The SDK dispatcher fans the fetched frame out to the session's inbound
subscription, so `MeshCoreAdapter._on_message()` could run before the adapter
set `_started`; the not-started guard silently dropped the frame, and because
`GET_NEXT_MSG` pops it from the device queue, the message was lost unless the
mesh re-delivered it.

While `start()` is in flight (`_starting`), inbound payloads delivered by the
SDK callback — and by `simulate_inbound()` — are now held in a bounded FIFO
(64 items) and flushed through the normal inbound path in arrival order after
`_started` is set, passing classification, dedup, and bounded callback
admission as if just received. A full buffer drops the newest payload with a
cumulative counter and a rate-limited, content-free warning. `stop()`, failed
starts, and cancelled starts discard the buffer and never replay it; callbacks
outside the start window (never started, post-stop) keep the pre-existing
silent drop.

Diagnostics expose `prestart_buffered`, `prestart_flushed`, and
`prestart_overflow_dropped`, reset at each start boundary. The behavior is
specified in `docs/spec/prestart-inbound-buffer.md` and pinned by
`tests/test_meshcore_prestart_buffer.py`.

Concurrent shutdown closes the buffer before awaiting native cleanup, and
an in-flight session start cannot mark the adapter ready after its lifecycle
was retired. Regression tests exercise a suspended start, stop, late callback,
and a genuinely new session (not an idempotent second `start()` call).

Buffered payload mappings are snapshotted at receipt, and callbacks are fenced
to the lifecycle epoch that installed them. Concurrent starts cannot replace an
in-flight owner; stop followed by a replacement start preserves the epoch fence.
