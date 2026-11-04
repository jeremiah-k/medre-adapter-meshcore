# Fence core-bound MeshCore native channel identity

Core-bound guarded channel packets now verify their selected native channel
against the durable target before invoking the SDK or consuming the one-use
handoff grant. Prebuilt results that would redirect a core attempt to a
separate channel or direct message are rejected. Ordinary renderer output
already carries the matching channel; direct unbound session calls are
unchanged. This narrows a pre-release trust-boundary gap without creating new
send authority, schema fields or recipient-delivery claims.
