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
nothing ready within it, or it popped a message whose frame did not arrive
in time. The SDK reports that state as an ERROR event whose payload reason
says no event was received (or timed out). A frame that pops after the
window still publishes through the SDK event callback, which sees every
direct-message and channel-text frame the connection delivers — counted
whether or not the frame publishes; frames the callback is not subscribed
to, such as binary channel data or device replies, count as nothing — and
that callback wakes the pump, so a slow-but-delivering link resumes its
interrupted drain on the same connection, with no loss and no reconnect.
A window
that elapses while inbound traffic is still arriving — a late frame, or a
notification — means the link is delivering, however slowly: the pump
keeps the connection and re-issues the fetch, and the re-issue rate MUST
stay bounded — a bounded number of consecutive immediate re-issues, after
which each further round waits for a notification or a fixed floor delay —
so a slow link neither spins nor strands its backlog. Each
MESSAGES_WAITING notification is liveness evidence for at most one
elapsed round: it is consumed for pacing once, and a notification that
races the node's NO_MORE_MSGS answer triggers exactly one extra drain
before the pump parks again. A window that
elapses with no inbound traffic at all MUST park the pump at once: the
node's fetch is a destructive pop, so the window may have ended because
the node popped a message whose frame was lost, and every further fetch
would pop another message into a link that is not returning what it pops.
The reserved capacity permit is released, as any interrupted drain's is,
and the silent round stays recorded, so a transport loss behind it is
recognised as unclean. Repeated elapsed windows are logged at DEBUG, not
as warnings. A reply that is neither a message, nor an empty queue, nor an
elapsed window — a node ERROR reply, a malformed result, a transport-level
failure — retires the connection for ordinary recovery. Stop and client
replacement MUST cancel the old pump before disconnecting the SDK.

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
one probe budget and is not probed to death. An answered probe also
re-arms an interrupted drain: while the pump stays parked because the last
drain ended on an elapsed window rather than the node's NO_MORE_MSGS, one
answered probe issues exactly one fetch; if that fetch's window elapses
with no inbound traffic the pump parks again, and the next answered probe
re-arms it again. A drain that ended with NO_MORE_MSGS is complete: an
answered probe issues nothing for it — the notification does. Exactly one
probe runs at a
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

The gate bounds loss on a link that delivers nothing, and the elapsed-
window rule bounds loss on a link that answers the probe and then swallows
message frames: such a link consumes at most one queued message per probe
interval (about fifteen seconds) for as long as it persists, and recovery
stays active. The bound assumes the link returns no direct-message or
channel-text frame: replies of any other kind — device answers, binary
channel data, node errors — never count as inbound traffic, so a link
that swallows message frames while answering anything else stays silent
to the park rule and re-arms at most one fetch per answered probe. A
silence the connection survives resumes an interrupted drain within one
probe interval of the park, plus one probe budget for the answer. A
longer silence retires the connection; the reconnect backoff then runs
from its base doubling to its cap under bounded jitter — one to thirty
seconds on TCP and serial, ten seconds to five minutes on BLE — each
attempt bounded by its connect timeout, and a link that recovers resumes
at the first successful redial, so recovery timing is dominated by that
backoff when the silence covers the disconnect. No exchange short of
popping a message can tell such a link from a healthy one at the moment
the fetch is issued; that is a loss bound, not a delivery promise.

## Ingress and evidence

An identity that core reports as stored ends the publication: the dedup key stays committed, nothing is retried or counted as published, and the gate is released so the drain continues.

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

A fetched event whose durable admission raises stays in its original
bounded callback slot with its original canonical identity and its
reservation. Retry uses capped backoff between 1 and 30 seconds, and the
pump issues no further destructive fetch until that message is admitted or
this adapter lifecycle ends; a warm reconnect inside the lifecycle moves
neither the retained event nor the gate. A failure that is deterministic
for the event — core's validation raises ``ValueError`` or ``TypeError``
only for an event that can never be admitted — is never retried: one
warning names the failure class without payload, the identity and the
reservation are released, and the drain continues with the next message.
Every other failure retains the event and retries. The first failed
attempt logs one warning naming the exception class without payload and
stating that native fetches are paused, recovery logs one info line, and
no counters, health states, or configuration fields are added for this.
Core's admission-pressure accounting counts every refused attempt, so a
retained retry is counted per attempt; refused attempts must not be read
as unique messages permanently lost. The retained message lives only in
memory: stopping the adapter releases it unadmitted, and the documented
loss bound stays at most one popped message per stop or crash. A fetched
duplicate of an unsolicited publication whose publication has not
completed promotes that publication to pump ownership instead of
publishing a second copy; pending ownership is token-fenced, bounded by
the outstanding publications, and a stale completion cannot evict a
newer owner's reservation.

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
