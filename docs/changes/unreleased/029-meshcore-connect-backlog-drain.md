# Drain the MeshCore backlog without competing fetch owners

After startup and reconnect, a single connection-scoped session receive
pump — the session's own loop that asks the node for waiting messages and
hands each one to the adapter — uses the pinned SDK's public notification
and fetch APIs to drain existing messages and handle later notifications.
It starts after adapter startup, serializes native fetches with sends, and
respects callback-capacity pressure. At connect time the pinned SDK itself
fetches nothing — its handshake only sends APP_START — so the pump's
initial wakeup is the only connect-time fetch.

A fetch window that elapses with no inbound traffic parks the pump at
once: the node's fetch is a destructive pop, and a window that ends
without returning anything may have popped a message whose frame was
lost, so every further fetch would pop another message into a link that
is not returning what it pops. The reserved admission permit is released
and the silent round stays recorded, so a transport loss behind it is
recognised as unclean. A window that elapses while inbound traffic
continues arriving — a late frame, or a notification — means the link is
delivering, however slowly: the pump keeps the connection and re-issues
at a bounded rate. A frame that pops after its window publishes through
the SDK event callback, which wakes the pump: a slow-but-delivering link
resumes its interrupted drain on the same connection. A reply that is
neither a message, nor an empty queue, nor an elapsed window retires the
connection for ordinary recovery.

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

The drain is stop-and-wait: the pump reserves one bounded-admission slot
before it fetches, the fetched message's publication consumes that slot
instead of reserving again, and the pump waits for the publication's
completion — the seam that signals core's durable admission commit —
before the next fetch. At most one popped message is ever in flight, and
a crash loses at most that one. Messages the client process had taken in
before it died remain lost — that is the protocol's no-acknowledgement
boundary, not a guarantee. A drain that ends without a fetched message
gives the slot back. Binary channel-data frames are popped and dropped:
the adapter relays text messages only, the node's queue is FIFO and
destructive, so the pump pops each such frame, releases its admission
permit, logs one DEBUG line, and keeps draining — the pop is accounted
for and never counts as lost traffic. Over BLE, a warm reconnect after an
unclean drop can make the host Bluetooth stack deliver every notification
twice; each duplicate resolves one fetch round against a queue that pops
in node order, and the adapter's ingress deduplication collapses each
duplicated pair to one stored event. Absorbing that host-stack artefact
is queue retrieval, not an exactly-once promise. The SDK event decoder
remains the only publication path.
