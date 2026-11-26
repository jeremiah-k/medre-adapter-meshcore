# Connect backlog drain

> **Authority:** Session ownership of MeshCore companion queue retrieval.

The pinned SDK's auto-fetch startup retrieves only one queued message; the
rest can wait indefinitely for a new MESSAGES_WAITING notification. Starting
an independent drain alongside that SDK worker is unsafe because native
responses are correlated by event type, not unique request identity.

## Receive ownership

The receive pump is the session's own loop that asks the node for waiting
messages and hands each one to the adapter. For the pinned SDK the session
MUST own a single connection-scoped receive pump, using public subscribe and
get_msg APIs. It MUST NOT start the SDK's
auto-fetch worker at the same time. A compatibility SDK without the explicit
notification API uses only its original SDK worker.

The pump MUST start after connection setup returns, outside the adapter's
pre-start buffer window. Every new client starts with an initial wakeup;
MESSAGES_WAITING sets the same wakeup. Clear before draining so a notification
arriving during a fetch is not lost. Fetch until NO_MORE_MSGS, yielding between
requests. There is no guessed backlog depth or startup drain deadline. Each
request has its own timeout and is serialized with sends; only one fetch runs
at a time, and the next is issued only after the previous one returned.

At connect time the pinned SDK itself fetches nothing: its connect handshake
only starts the event dispatcher, opens the transport, and sends APP_START.
The pump's initial wakeup is therefore the only connect-time fetch, and the
SDK auto-fetch worker is never started on SDKs that expose the explicit
notification API. Over BLE, a warm reconnect after an unclean drop can make
the host Bluetooth stack deliver every notification twice (observed on
BlueZ: two identical deliveries about a millisecond apart). The dropped
client's notify session survives in the host stack and is re-armed on the
next connection; the pinned SDK clears such a stale registration only when
the same connection object reconnects, which this adapter's
fresh-client-per-connect policy never does, and no public SDK API reaches a
dropped client's session. A duplicated frame resolves one fetch round, so
each duplicate costs one extra fetch; every round still pops at most one
queued message in node order, the extra rounds run against the emptied
queue and pop nothing, and the drain still ends on the node's NO_MORE_MSGS.
The adapter's ingress deduplication collapses each duplicated pair to one
stored event. Absorbing this host-stack artefact is not a delivery promise:
the destructive pop protocol cannot promise exactly-once across reconnects.

A fetch window that elapses with no event is an empty window: the node had
nothing ready within it. The SDK reports that state as an ERROR event whose
payload reason says no event was received (or timed out). The pump MUST
treat such a round as idle — keep the connection and re-issue the fetch —
because publication happens in the SDK event callback, which sees every
inbound frame even when its frame pops after the window. The re-issue rate
MUST stay bounded: a bounded number of consecutive immediate re-issues,
after which each further round waits for a notification or a fixed floor
delay, so a slow link neither spins nor strands its backlog. Each idle
round still costs the node a popped message, so a streak of idle rounds
that brings no inbound traffic at all — a link that is not answering —
MUST park the pump on its wakeup instead of popping further messages the
link cannot return; the next notification starts a new drain on the same
connection. Repeated idle rounds are logged at DEBUG, not as warnings. A
reply that is neither a message, nor an empty queue, nor an elapsed window
— a node ERROR reply, a malformed result, a transport-level failure —
retires the connection for ordinary recovery. Stop and client replacement
MUST cancel the old pump before disconnecting the SDK.

A parked wait is bounded. A link can die silently — the peer vanishes
without a FIN or RST reaching us — so no fetch ever fails and no
notification ever arrives, and the parked pump would make the session look
connected forever. While the pump would otherwise sit parked, it MUST
periodically confirm the node is still reachable with a probe: one cheap
request the companion answers over the control connection itself, without
any radio traffic. The session uses the SDK's device query; its
`DEVICE_INFO` reply type cannot be confused with a message event, so a
reply can never be mistaken for fetched traffic. The first probe follows
the park after a fixed internal interval, and the probe repeats at that
cadence while the quiet continues; the interval equals the pinned SDK's
own default command timeout, so a slow-but-alive node still answers inside
one probe budget and is not probed to death. Exactly one probe runs at a
time, under the same lock as fetches and sends — the SDK correlates
responses by event type, not request identity — and never while a fetch is
outstanding. A probe that goes unanswered (the SDK's own elapsed-wait
report) is a transport failure: it retires the connection through the same
retire path as any other pump failure, which runs ordinary recovery. A
reply of any other kind, including an error the node itself sends, proves
the link is alive and leaves the connection alone. Stop, retirement, and
reconnect cancel the probe with the pump; a probe reply that arrives after
cancellation matches no subscription. Detection takes roughly the probe interval plus one probe budget after
the silence begins; that is a timing bound, not a delivery or recovery
guarantee.

A connection that follows a failure-driven retirement must earn the right
to fetch. The node's fetch is a destructive pop with no acknowledgement in
the protocol, so a link whose server-to-client direction is still broken
consumes one queued message per fetch round: the reconnect's handshake is
answered, the first fetch reaches the node, the node pops a message and
sends it, and the frame is swallowed. Before its first fetch on such a
connection the pump therefore sends one liveness probe — the same device
query the parked pump uses, under the same lock — and waits for the
answer. A real reply, including an error the node itself sends, proves the
link delivers; the backlog then drains normally, and from that point the
connection fetches without a gate. An unanswered probe issues no fetch at
all: the connection retires through the ordinary path and the backoff
reconnect runs again, so a still-broken link loses nothing. A retirement
is failure-driven when a fetch or probe failed on the connection, or when
the transport died with destructive pops unanswered behind it — a fetch
round still in flight, or completed fetch rounds that came back with no
event at all, so their popped messages' frames may have been lost with the
link. A fresh start and a reconnect
after a clean disconnect — the peer closed, the host dropped the link, the
adapter was stopped; no fetch was in flight and no fetch round had gone
unanswered, so nothing was popped into the dead link — fetch immediately,
with no probe. An SDK without the
device query cannot be gated and keeps the immediate-fetch behaviour.

The gate bounds loss on a link that delivers nothing. It cannot bound loss
on a link that answers the probe and then swallows a message frame: such a
link consumes at most one queued message per reconnect attempt, and
recovery stays active, so the loss continues for as long as that link
persists. No exchange short of popping a message can tell that link from a
healthy one.

## Ingress and evidence

The adapter exposes a prefetch capacity gate using its existing bounded
callback-admission count. The pump reserves one admission slot through that
gate before it issues a fetch — the companion pops each fetched message
destructively, so the slot the message will publish into must already
exist. The drain is stop-and-wait: while the fetched message's publication
is still on its way to durable admission, the pump issues no further
fetch. Core's live-adapter ingress commits the event durably before its
publish callback returns, and the publication task's completion is the
existing seam that signals exactly that, so the pump resumes only from the
point where the message has left the adapter's hand for good. At most one
popped message is ever in flight: if the process dies, committed messages
survive in storage, the uncommitted remainder is still queued at the node,
and the only possible loss is the single message that was in flight. The
protocol has no delivery receipt, so even that bound is not a guarantee —
a message already handed to the client process at the moment of a crash is
lost, and that is at most one message per crash. A drain that ends without
a fetched message (empty queue, retirement, stop, or a fetched frame
suppressed by dedup or decode) releases the slot. Unsolicited packets
still reserve on publication and can still be refused under pressure as
before. The pump waits while no slot is free, rather than deliberately
popping more queued messages into an already-full adapter. SDK event
callbacks remain the only publication path. No new event kind, delivery
receipt, or replay authority is introduced. The destructive companion
protocol cannot promise exactly-once or transactional handoff across
disconnects, crashes or native queue overflow.

## Qualification

The pinned SDK verifies the drain over local TCP without hardware: a
backlog queued before the connect drains completely, in node order, once;
a backlog deeper than 64 messages drains completely; a drain never blocks
startup; a reconnect drains what queued while the transport was down; and
a disconnect after an inbound delivery reconnects and delivers what
follows. The local companion answers the device query the probe uses, so
a gated reconnect can prove its link; the gate itself — no fetch before
an answered probe — verifies at the session level against a scripted
transport.

The hardware lab's openHop qualification exercises the drain over TCP
against independent openHop peers: notification and fetch overlap, ingress
saturation, transport timeouts and reconnect, and the fault cases the
bounds name — a peer that stalls replies, a link closed mid-fetch,
saturation, a silently dead idle link, a peer that answers probes and
swallows message frames, and a stall armed during an active drain.

Bluetooth qualified on real companion radios for two-way delivery,
offline backlog replay, and clean-disconnect recovery. A Bluetooth link
that dies silently is not simulated on hardware; the probe's retirement
of a silent link verifies at the session level against a fake transport.
Serial is non-functional on the bench radios and unqualified.
