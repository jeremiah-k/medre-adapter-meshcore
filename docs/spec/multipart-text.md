# Guarded multipart text

`max_text_parts` MUST be an integer from 1 through 128, with bool rejected. The
value 1 retains established single-packet clipping. A larger value opts ordinary
`message.created` text into the shared exact radio planner when needed. Short
messages and reaction fallbacks MUST retain their established wire form.

The renderer MUST select canonical text before planning, degrade any relation
fallback before splitting, and repeat the applied relay attribution prefix on
every numbered packet. UTF-8, marker and firmware sender-name overhead MUST fit
the effective per-packet budget. Exceeding the part cap MUST reject the entire
plan before native handoff. A multipart result MUST report no truncation and
retain the typed immutable plan; payload text is its first precomputed packet.

The adapter MUST declare synchronous guarded-radio support and MUST refuse a
complete plan delivered directly, before any native work. Core materializes
individual packet results under the exact durable outbox claim. The adapter
MUST forward their `before_native_send` callback to the session unchanged.
The callback MUST run once inside the existing send lock after pacing and
immediately before the SDK command. Each guarded invocation MUST make at most
one native call; ordinary unguarded sends retain their existing retry policy.

Shutdown/connection state MUST be checked again inside the send owner before
the grant. A refused or expired grant MUST prevent native use. Native timeout
or cancellation after a grant MUST stop the plan and retain uncertainty in
core; no internal retry may consume that grant again. The default path and
other concurrent sends MUST share the same lock and pacing.

Core MUST retain each part's actual local handoff evidence using its owned
`radio-local-handoff.schema.json` shape. Channel acceptance
without an SDK-assigned packet ID MUST remain explicitly ID-less, not a made-up
native relation target. DM IDs MUST retain native channel scope and ordinal.
Every part's acceptance must commit before core reports aggregate local handoff.
A multipart aggregate MUST NOT select one native ID as a whole-message reply
mapping, and local transport acceptance MUST NOT imply RF recipient delivery.

This path requires the coordinated core radio handoff contract. The package
version remains 0.1.0; production source installation MUST pair qualified core
and provider commits. Deterministic tests exercise the actual adapter/session
owner without SDK sockets. Installed-SDK and bounded physical qualification
remain separate gates. Meshtastic's deferred queue is outside this contract.
