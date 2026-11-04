# Release provisional MeshCore dedup keys after cancelled ingress publications

The MeshCore SDK callback path commits a provisional inbound dedup key before
it schedules the asynchronous publication task, so concurrent duplicates of
one message identity stay suppressed across the publish boundary. A
publication task cancelled before admission now releases that provisional key
and propagates the cancellation: while the adapter remains started, a
retransmission of the same message identity is re-admitted instead of being
silently suppressed by a reservation whose publication never completed.

Each commit stores an opaque ownership token, and a cancelled handler releases
an entry only while the cached entry still belongs to its own token. A stale
handler that runs after a newer delivery re-committed the same identity
therefore leaves the newer reservation intact. Cancellation after
`publish_inbound` returns never releases the key: the event is durably
admitted at that point and its identity must keep suppressing redelivery.
Shutdown behavior is unchanged — stop() still drains pending publication
tasks and clears the cache at the session boundary.
