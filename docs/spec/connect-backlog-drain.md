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
request has its own timeout, is serialized with sends, and a non-message/non-
empty result retires the ambiguous connection for ordinary recovery. Stop and
client replacement MUST cancel the old pump before disconnecting the SDK.

## Ingress and evidence

The adapter exposes a prefetch capacity gate using its existing bounded
callback-admission count. The pump waits while that count is full, rather
than deliberately popping more queued messages into an already-full adapter.
SDK event callbacks remain the only publication path. The gate is advisory
with respect to unsolicited native callbacks; it is not a durable admission
reservation. No new event kind, delivery receipt, or replay authority is
introduced. The destructive companion protocol cannot promise exactly-once
or transactional handoff across disconnects, crashes or native queue overflow.

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
