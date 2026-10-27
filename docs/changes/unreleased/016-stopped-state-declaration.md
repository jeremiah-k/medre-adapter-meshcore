# Declare MeshCore stopped state as device-attached firmware

Ship a provider-owned stopped-state declaration module and reference it from
the adapter spec through the optional `stopped_state_declaration` symbol. The
declaration reports every configured instance as device-attached: the adapter
keeps no local files to capture, node identity and firmware state live on the
attached radio, and device ownership cannot be proven from files, so local
exclusion is reported as unsupported instead of claimed. Fake connectivity
reports as stateless. Consumers and the trust-boundary contract live in
`medre.maintenance.stopped_declare` on the core side; a distribution without a
declaration is reported as unsupported there rather than guessed.

TCP ownership checks refer to the configured endpoint, which can be remote.
Serial and BLE checks refer to the selected physical radio. Neither case
provides a host-file lease or a backup of firmware identity.
