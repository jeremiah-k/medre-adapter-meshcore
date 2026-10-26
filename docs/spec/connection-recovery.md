# Connection recovery

Long-lived adapters retry transport reachability failures after degraded startup.
TCP and serial attempts have a 15-second setup allowance; BLE has 45 seconds,
including discovery. SDK cleanup remains separately bounded and cooperative.

Known configuration and SDK contract errors (ValueError, TypeError,
AttributeError, ImportError and NotImplementedError) MUST propagate during
initial setup and MUST stop background recovery. Wrapped connection errors MUST
preserve their transient classification. Other transport failures retain retry
behavior; classification MUST NOT parse exception messages or pairing secrets.

BLE discovery selectors may be advertised names, addresses or platform UUIDs.
A selector absent from discovery is a reachability failure; a MAC-only validator
would incorrectly reject supported selectors. Missing SDK support fails startup.

## Recovery cadence and host availability

Background BLE recovery MUST wait ten seconds before its first retry, then use
exponential backoff capped at 300 seconds. TCP and serial MUST retain their existing
one-to-thirty-second policy. BLE jitter MUST stay within the initial
delay and cap. Retry ownership MUST remain with the session, with one BLE SDK
factory call per background attempt. Initial strict setup may retain its bounded
three-call probe sequence; background recovery MUST NOT repeat that inner burst.

A typed Bleak Bluetooth-unavailable error during discovery or connection MUST
remain a transient host-availability failure. It MUST bypass address-only
fallback and remaining immediate setup attempts. The operator receives a concise
instruction to enable the host controller; the session remains degraded and
recovers automatically when Bluetooth returns. No retry may power on the
controller or alter pairing state. Cancellation MUST still interrupt backoff.

The configured pairing PIN MAY be used during explicit initial setup. Background
recovery MUST omit it from SDK factory calls and reuse only existing bonds.
Typed host-controller errors MUST retain fixed, secret-free recovery guidance
even when a PIN is configured; arbitrary SDK exception text remains redacted.

## Native health observations

A successful native handshake and event subscription MUST make connected health
healthy, including after degraded startup. Disconnects and completed recovery
MUST update cached diagnostics without requiring a health probe. The adapter MAY
publish the same `AdapterInfo` through the runtime's optional `report_health`
context sink. The session MUST publish connection transitions only after updating
its owned connection state, and MUST suppress duplicate connection notifications.
Retired session callbacks MUST NOT publish a replacement lifetime's health.
A diagnostic observer failure MUST NOT fail native startup, cleanup or delivery.

Native health reports do not change packet acceptance or confirmation levels.
The core runtime preserves startup history and publishes current health
separately. A runtime without a report sink continues to use health probes.
