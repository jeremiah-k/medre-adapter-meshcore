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

Background recovery MUST wait ten seconds before its first retry, then use
exponential backoff capped at 300 seconds. Jitter MUST stay within the initial
delay and cap. Retry ownership MUST remain with the session, with one BLE SDK
factory call per background attempt. Initial strict setup may retain its bounded
three-call probe sequence; background recovery MUST NOT repeat that inner burst.

A typed Bleak Bluetooth-unavailable error during discovery or connection MUST
remain a transient host-availability failure. It MUST bypass address-only
fallback and remaining immediate setup attempts. The operator receives a concise
instruction to enable the host controller; the session remains degraded and
recovers automatically when Bluetooth returns. No retry may power on the
controller or alter pairing state. Cancellation MUST still interrupt backoff.
