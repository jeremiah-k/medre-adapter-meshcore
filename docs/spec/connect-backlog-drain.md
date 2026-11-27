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
dropped client's session. The adapter recognizes known duplicate text by
its ingress identity. If a duplicate arrives during another fetch, it may
belong to an earlier request. The receive pump MUST park if the SDK returns
only that duplicate, retaining the admission permit until distinct text
arrives or an answered probe re-arms the fetch. Duplicate delivery does
not publish a second copy of a known identity.

A fetch window may elapse before a reply arrives. The SDK reports that
state as ERROR with a reason of no_event_received or timeout. If distinct
direct-message or channel text arrived during that window, the pump can
retry on the same connection, with bounded immediate retries and pacing
afterward. Otherwise the pump MUST park and retain its one admission permit
for the late frame. A late distinct text frame consumes that permit, holds
the drain until its admission is settled, and wakes the pump. A
MESSAGES_WAITING notification alone MUST NOT re-arm a parked fetch with an
unresolved pop. It still wakes a completed drain, and one that races
NO_MORE_MSGS requests one additional drain. An answered idle probe re-arms
one unresolved fetch at the normal probe cadence. Repeated elapsed windows
are logged at DEBUG. Other fetch failures retire the connection. Stop and
client replacement MUST cancel the old pump before disconnecting the SDK.

A binary channel-data frame is popped but never relayed: the adapter
publishes text messages only. The node's queue is FIFO and destructive, so
the pump pops such a frame anyway, releases that round's reserved admission
permit — nothing will publish from it — logs one DEBUG line, and continues
the drain immediately. The pop is accounted for, so it is never mistaken
for lost traffic, and it neither stalls the drain nor retires the
connection.

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
and on an orderly queue with one distinguishable reply per fetch, the only
possible uncommitted popped message is the one in flight. The protocol
has no delivery receipt. The SDK uses the same event types for fetched
and unsolicited text and has no request identity. A distinct unsolicited
frame arriving inside a fetch can therefore be mistaken for its reply.
That overlap can consume the reservation before the fetched frame arrives,
and capacity pressure can refuse the fetched frame. The one-in-flight
claim assumes the companion returns queued text only in response to a
fetch and one matching reply resolves each fetch before the next one. A
late reply that overlaps a retry can also be ambiguous. A drain that ends without
a fetched message (empty queue, retirement, stop, or a fetched frame
suppressed by dedup or decode) releases the slot. Unsolicited packets
still reserve on publication and can still be refused under pressure as
before. A callback can consume the reserved slot while the pump
waits behind the native command lock, so the pump rechecks its reservation
after taking that lock and, when the slot is gone, returns to the capacity
wait instead of issuing a destructive fetch with no reservation. The pump
waits while no slot is free, rather than deliberately
popping more queued messages into an already-full adapter. SDK event
callbacks remain the only publication path.
A retired connection's scheduled callbacks MUST NOT wake, publish through, or
retire the replacement connection. No new event kind, delivery
receipt, or replay authority is introduced. The destructive companion
protocol cannot promise exactly-once or transactional handoff across
disconnects, crashes or native queue overflow.

A fetched event whose durable admission raises stays in its original
bounded callback slot with its original canonical identity and its
reservation. Retry uses capped backoff between 1 and 30 seconds, and the
pump issues no further destructive fetch until that message is admitted or
this adapter lifecycle ends; a warm reconnect inside the lifecycle moves
neither the retained event nor the gate. A failure that is deterministic
for the event — core's validation raises `ValueError` or `TypeError`
only for an event that can never be admitted — is never retried: one
warning names the failure class without payload, the identity and the
reservation are released, and the drain continues with the next message.
Every other failure retains the event and retries. The first failed
attempt logs one warning naming the exception class without payload and
stating that native fetches are paused, recovery logs one info line, and
no counters, health states, or configuration fields are added for this.
Core's admission-pressure accounting counts every refused attempt, so a
retained retry is counted per attempt; refused attempts must not be read
as unique messages permanently lost. Retained publications live in memory and stop releases them. A duplicate
of a pending unsolicited publication promotes that publication to pump
ownership without taking another callback slot. The permit stays reserved
for the outstanding reply. The pump waits for both publications if a
distinct reply arrives before the promoted publication settles. Pending
ownership is bounded by the callback pool and token-fenced.

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
