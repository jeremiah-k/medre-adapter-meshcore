# Qualify sustained MeshCore admission at and above the bound

`tests/test_meshcore_sustained_admission.py` records the measured-budget
qualification round behind the provisional
`max_pending_ingress_callbacks` default of `100`. Above-cap bursts at 100,
400 and 1600 distinct packets with publication held admit exactly the bound,
refuse the remainder with exact counter and producer-pressure
reconciliation, and re-admit every refused identity on redelivery once
capacity frees — refusal never poisons dedup. Repeated bursts in one session
refill the whole bound with no slot leak.

A bounded sustained-imbalance cycle drives thousands of arrivals above a
small bound while the publisher releases in small batches: pending stays at
or below the bound, scheduled plus refused equals distinct deliveries,
duplicates inside the pending window stay dedup-suppressed without consuming
capacity or overflow, and the admitted sequence publishes exactly once in
admission order.

Saturation stop against a cancellation-resistant publisher is now
qualified: the bounded drain times out, warns once about the stubborn
publication, retires it from tracking and advances the admission epoch, and
the restarted session admits from full capacity while the stubborn task's
late completion — or late cancellation — touches nothing in the new
session. A retired-epoch seam check confirms a late pre-start cancellation
reports through the original session's pressure sink.

The tests also export content-free loop-lag and memory evidence through
`MEDRE_PRESSURE_REPORT_DIR`: a loop sentinel scheduled behind a saturated
window runs in low single-digit milliseconds, and the retained saturated
window grows sublinearly from a 100-burst to a 1600-burst (about 1.008
isolated, about 1.29 end-to-end against a 16x burst growth), confirming
pending work is governed by the cap. `ru_maxrss` is recorded as context and
never asserted. `docs/dev/ingress-pressure.md` carries the dated measured
record; these results do not prove throughput, real-SDK, hardware or RF
behavior, and the loop-lag bound is a generous CI guard, not an SLA.
