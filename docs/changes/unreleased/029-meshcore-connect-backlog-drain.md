# Drain the MeshCore backlog without competing fetch owners

After startup and reconnect, a single connection-scoped session receive pump
uses the pinned SDK's public notification and fetch APIs to drain existing
messages and handle later notifications. It starts after adapter startup,
serializes native fetches with sends, respects callback-capacity pressure and
retires the connection after an ambiguous fetch response. No fixed queue-depth
assumption is needed. The SDK event decoder remains the only publication path.
This is queue retrieval, not an exactly-once or durable delivery guarantee.
