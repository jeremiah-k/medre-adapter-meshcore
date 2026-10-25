# Pace BLE recovery during outages

Background reconnects use one SDK factory call per attempt and back off from ten
seconds to five minutes, with jitter bounded by those limits. Typed host Bluetooth
unavailability no longer becomes an address-only connection fallback or a burst
of repeated setup calls. Recovery remains automatic and cancellation remains prompt.
