# Drain the MeshCore backlog without competing fetch owners

After startup and reconnect, a single connection-scoped session receive pump
— the session's own loop that asks the node for waiting messages and hands
each one to the adapter — uses the pinned SDK's public notification and fetch
APIs to drain existing messages and handle later notifications. It starts
after adapter startup, serializes native fetches with sends, and respects
callback-capacity pressure. A fetch window that elapses with no inbound
traffic parks the pump at once: the node's fetch is a destructive pop, so
the window may have ended because the node popped a message whose frame
was lost, and re-issuing used to pop several more queued messages into a
link that was not returning what it popped. A frame that pops after its
window still publishes through the SDK event callback and now wakes the
pump, so a slow-but-delivering link resumes its interrupted drain on the
same connection, and a window that elapses while traffic is still
arriving re-issues at a bounded rate as before. A non-message reply other
than an elapsed window still retires the connection for ordinary
recovery. Because a parked pump can no longer
tell a truly idle node from a link that died silently — the peer vanished
without a FIN or RST, so nothing ever fails — a parked wait now expires
into a probe at a fixed internal cadence: one cheap device query the
companion answers over the control connection itself, with no radio
traffic, whose reply type cannot be mistaken for a message event. One
probe runs at a time, never while a fetch is outstanding, and it is
cancelled with the pump on stop, retirement, and reconnect. A probe that
goes unanswered retires the connection through the same path as any other
pump failure, so an idle MEDRE reconnects on its own instead of waiting for
an outbound send to notice. A reply of any kind, including an error the
node itself sends, proves the link is alive — and one answered probe now
re-arms exactly one fetch for a drain the elapsed window interrupted, so a
link that recovers resumes within one probe interval, while a link that
answers probes and swallows message frames consumes at most one queued
message per probe interval. A drain that ended with the node's
NO_MORE_MSGS is complete; an answered probe never fetches for it.
Detection takes roughly
the probe interval plus one probe budget after the silence begins; that is
a timing bound, not a guarantee.
The probe now also earns a failed reconnect the right to fetch. The node's
fetch is a destructive pop with no acknowledgement, so a reconnect into a
link whose server-to-client direction is still broken used to swallow one
queued message per retire-reconnect cycle: the handshake was answered, the
first fetch popped a message into the broken direction, the round went
idle, the probe failed, and the next cycle popped the next message. A
connection that follows a failure-driven retirement — a fetch or probe
failure, or a transport loss with unanswered destructive pops behind it (a
fetch round still in flight, or fetch rounds that came back with no event
at all) — now sends one probe before its first fetch and fetches only if
that probe is
answered; otherwise it retires having popped nothing. A fresh start and a
reconnect after a clean disconnect (the peer closed, the host dropped the
link, the adapter was stopped) still fetch immediately.
The drain is now stop-and-wait. The pump used to fetch the next message
while earlier fetched messages were still queued for publication, so a
process crash inside a drain burst could lose every popped-but-uncommitted
message at once. Core commits live adapter ingress durably before its
publish callback returns, and the publication's completion already signals
exactly that, so the pump now waits for it: at most one popped message is
ever in flight, and a crash loses at most that one. Messages already
handed to the client process at the moment of a crash are still lost —
that is the protocol's no-acknowledgement boundary, not a guarantee.
The pump reserves one
bounded-admission slot before it fetches; the fetched message's publication
consumes that slot instead of reserving again, so a message the node has
already popped is not refused at admission, and a drain that ends without a
fetched message gives the slot back. No fixed queue-depth assumption is
needed. The SDK event decoder remains the only publication path. This is
queue retrieval, not an exactly-once or durable delivery guarantee.
At connect time the pinned SDK fetches nothing — its handshake only sends
APP_START — so the pump's initial wakeup is the only connect-time fetch, and
no SDK option is needed to keep it that way. Over BLE, a warm reconnect
after an unclean drop can make the host Bluetooth stack deliver every
notification twice; each duplicate resolves one fetch round against a queue
that pops in node order, costs no extra pop, and is collapsed to one stored
event by the adapter's ingress deduplication. Absorbing that host-stack
artefact is queue retrieval, not an exactly-once promise.
