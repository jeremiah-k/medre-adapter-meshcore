# Drain the MeshCore backlog without competing fetch owners

After startup and reconnect, a single connection-scoped session receive
pump — the session's own loop that asks the node for waiting messages and
hands each one to the adapter — uses the pinned SDK's public notification
and fetch APIs to drain existing messages and handle later notifications.
It starts after adapter startup, serializes native fetches with sends, and
respects callback-capacity pressure. At connect time the pinned SDK itself
fetches nothing — its handshake only sends APP_START — so the pump's
initial wakeup is the only connect-time fetch.

An elapsed fetch or a known duplicate leaves the destructive pop
unresolved. The pump parks and retains its admission slot for a late
reply. A distinct text frame resumes the drain after publication. An
answered idle probe can re-arm one fetch. Queue notifications wake a
completed drain and cannot re-arm an unresolved one.

Because a parked pump cannot tell a truly idle node from a link that died
silently — the peer vanished without a FIN or RST, so nothing ever fails
— a parked wait expires into a probe at a fixed internal cadence: one
cheap device query the companion answers over the control connection
itself, with no radio traffic, whose reply type cannot be mistaken for a
message event. One probe runs at a time, never while a fetch is
outstanding, and it is cancelled with the pump on stop, retirement, and
reconnect. A probe that goes unanswered retires the connection through
the same path as any other pump failure, so an idle MEDRE reconnects on
its own instead of waiting for an outbound send to notice. A reply of any
kind, including an error the node itself sends, proves the link is alive
— and one answered probe re-arms exactly one fetch for a drain the
elapsed window interrupted, so a link that answers probes and swallows
message frames consumes at most one queued message per probe interval. A
drain that ended with the node's NO_MORE_MSGS is complete; an answered
probe never fetches for it. Detection takes roughly the probe interval
plus one probe budget after the silence begins; that is a timing bound,
not a guarantee.

The probe also earns a failed reconnect the right to fetch. A connection
that follows a failure-driven retirement — a fetch or probe failure, or a
transport loss with unanswered destructive pops behind it — sends one
probe before its first fetch and fetches only if that probe is answered;
otherwise it retires having popped nothing. A fresh start and a reconnect
after a clean disconnect fetch immediately.

The pump reserves one bounded admission slot before a destructive fetch
and waits for durable admission before continuing. Known duplicate
identities remain deduplicated. Binary channel-data frames release the
slot and advance the queue without publication. The SDK does not carry
request identity, so a distinct unsolicited frame or a late reply that
overlaps a new request can still be mistaken for that request's reply.
