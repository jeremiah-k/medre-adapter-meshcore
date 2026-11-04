# Fence core-bound MeshCore native channel identity

Core-bound packets now verify the receiving adapter instance and selected native
channel against the durable target before invoking the SDK or consuming the one-use
handoff grant. Prebuilt results that would redirect a core attempt to a
separate channel or direct message are rejected. Ordinary renderer output
already carries the matching channel; direct unbound session calls are
unchanged. This narrows a pre-release trust-boundary gap without creating new
send authority, schema fields or recipient-delivery claims.

The same identity fence runs before fake-mode grant consumption or synthetic
handoff. Channel resolution shares the renderer's integer parsing and configured
default, including signed numeric forms and absent or symbolic targets; these
forms cannot bypass validation or redirect a channel attempt into a DM. Durable
target strings retain their exact identity.
