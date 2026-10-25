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
