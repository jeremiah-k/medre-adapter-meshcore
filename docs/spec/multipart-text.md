# Guarded native text

`max_text_parts` MUST be an integer from 1 through 128, with bool rejected. The
value 1 retains established single-packet clipping. A larger value opts ordinary
`message.created` text into the shared exact radio planner when needed. Short
messages and reaction fallbacks MUST retain their established wire form.

For TCP, serial and BLE configurations, every nonempty rendered packet MUST
carry the shared immutable radio plan, including short messages, clipped text
and relation fallbacks. A single-packet plan MUST preserve the final wire text
without adding numbering or repeating attribution. Empty final native text,
including a zero-byte effective budget, MUST fail before dispatch. Synthetic
fake-mode single-packet rendering retains its established behavior.

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
Core-bound native results without that callback MUST be refused before SDK use,
including results constructed outside the standard renderer.
For a core-bound numeric channel target, the adapter MUST verify the packet's
native channel index and target identity against the durable attempt before
consuming the grant; mismatch MUST NOT reach the SDK. This also applies to
prebuilt packet results that bypass standard rendering.
The callback MUST run once inside the existing send lock after pacing and
immediately before the SDK command. Each guarded invocation MUST make at most
one native call. Direct session calls and unbound legacy results retain their
existing retry policy and are outside the durable core-delivery guarantee.

Shutdown/connection state and native SDK client identity MUST be checked inside
the send owner before the grant and rechecked after the grant returns. If the
session stops, disconnects or replaces its SDK client while the grant awaits
storage, the consumed grant MUST NOT reach a native command and its packet
MUST remain uncertain rather than be automatically retried. A refused or
expired grant MUST prevent native use. Native timeout
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
The existing core radio-plan and local-handoff schema shapes already represent
one packet; no new fields or provider metadata schema version are required.
