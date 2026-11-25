# Drain the MeshCore backlog without competing fetch owners

After startup and reconnect, a single connection-scoped session receive pump
— the session's own loop that asks the node for waiting messages and hands
each one to the adapter — uses the pinned SDK's public notification and fetch
APIs to drain existing messages and handle later notifications. It starts
after adapter startup, serializes native fetches with sends, and respects
callback-capacity pressure. A fetch window that elapses with no event is an
empty window, not a failure: the connection stays and the fetch is re-issued
at a bounded rate, while a non-message reply other than an elapsed window
still retires the connection for ordinary recovery. Idle rounds that bring
no inbound traffic at all park the pump instead of popping further messages
into a link that is not answering. Because a parked pump can no longer
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
node itself sends, proves the link is alive. Detection takes roughly
the probe interval plus one probe budget after the silence begins; that is
a timing bound, not a guarantee. The pump reserves one
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
