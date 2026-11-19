# Startup-info provider: held node identity at ready

`MeshCoreAdapter` now exposes an optional, duck-typed `startup_info()`
callable that core discovers with `getattr` at the adapter's ready
transition and calls under a bounded, supervised wait. The adapter does
not import core symbols for the seam, so it stays installable against
older cores that never look for it. Failure, expiry, or absence of held
facts only means no startup block; it never affects readiness, routing,
or delivery.

The provider is a pure read of announce/identity state the adapter
already holds — no device or radio I/O, no resource acquisition, no
session mutation. Row 0 is the node identity: the device name (session
self-info snapshot, falling back to the configured identity) and the
public-key prefix from the same snapshot.
The prefix is a published mesh identifier that appears in packet
headers and diagnostics; core renders it in full under its public-key
name rule. Row 1 carries the radio frequency when the self-info
snapshot captured one. Core runs synchronous hooks on a worker thread;
the hook binds the session once and obtains the three held scalars
from a single locked publication. A concurrent refresh cannot combine
facts from different announcements. A partial announcement replaces
missing fields with absent values.

Firmware build and battery/charge state are not held at ready and are
deliberately omitted rather than probed. Contact or peer-table state
the session holds is never surfaced — startup info is the operator's
own device identity, not a directory. No secret-named, pairing, or
BLE-PIN material is offered. Redaction remains core's render-boundary
responsibility; the provider simply does not offer sensitive fields.

Pinned by `tests/test_meshcore_startup_info.py` (fake connection mode
only, no device I/O). The core-side contract is specified in core's
`docs/spec/adapter-startup-info.md`.
