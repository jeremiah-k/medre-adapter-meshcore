# Retire MeshCore sessions before delivering or restarting

`MeshCoreSession.stop()` cleared its inbound message callback only on the
never-started and failed-start paths, and `_on_sdk_event()` gated delivery
solely on that callback, so an SDK event dispatched during a started session's
teardown awaits could still reach a retired callback. Stop now clears the
callback in its synchronous prologue, before any await: inbound delivery ends
when stop begins, even if the stop is cancelled mid-teardown. The
connection-change notification still publishes the final disconnected
transition, and reconnect-path stale-SDK cleanup still preserves the callback.

`MeshCoreAdapter.start()` retains a cancelled startup's partial session so the
runtime's bounded rollback can stop it, but a second start without an
intervening stop overwrote that session without stopping it, which for BLE
skipped the BlueZ stale-link release. Start now refuses with a content-free
`RuntimeError`, before any mutation, while an earlier start's session remains
installed; `stop()` retires it, and a fresh start then proceeds. Retired
sessions stay freed by reference counting, without a garbage-collection pass.

The contract is specified in `docs/spec/connection-recovery.md` and pinned by
`tests/test_meshcore_session_teardown.py` and
`tests/test_meshcore_prestart_buffer.py`.
