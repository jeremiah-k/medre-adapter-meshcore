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

## Ingress and evidence

The adapter exposes a prefetch capacity gate using its existing bounded
callback-admission count. The pump reserves one admission slot through that
gate before it issues a fetch — the companion pops each fetched message
destructively, so the slot the message will publish into must already
exist. One slot covers one fetch: while the fetched message has not
published yet, the pending slot is reused for the next round rather than
reserved twice. The fetched message's publication consumes the reserved
slot instead of reserving again; a drain that ends without a fetched
message (empty queue, retirement, stop, or a fetched frame suppressed by
dedup or decode) releases it. Unsolicited packets still reserve on
publication and can still be refused under pressure as before. A callback
can consume the reserved slot while the pump waits behind the native
command lock, so the pump rechecks its reservation after taking that lock
and, when the slot is gone, returns to the capacity wait instead of
issuing a destructive fetch with no reservation. The pump waits while no
slot is free, rather than deliberately popping more queued messages into
an already-full adapter. SDK event callbacks remain the only
publication path. The reservation covers the message a fetch popped; it is
not a durable admission reservation for a frame that arrives after the pump
stopped draining, and the protocol has no delivery receipt to close that
gap. No new event kind, delivery receipt, or replay
authority is introduced. The destructive companion protocol cannot promise
exactly-once or transactional handoff across disconnects, crashes or native
queue overflow.

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
