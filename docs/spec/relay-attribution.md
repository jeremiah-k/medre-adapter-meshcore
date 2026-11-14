# Relay attribution

## Group sender claims and bridge mentions

The sender_raw and sender_short_raw template variables MUST expose unannotated
wire sender claims. The sender_verification variable MUST remain unverified for
ordinary channel sender names, independently of the selected visible prefix.
Default sender labels retain their unverified annotation.

A leading MeshCore UI mention of this adapter's own SDK-advertised node name MAY
be removed from normalized group text. The original wire text and derived
message identity MUST remain unchanged as evidence. Other mentions MUST remain
intact. A sender mention alone MUST NOT create a reply relation or select the
latest previously forwarded message.

## Relay prefix

The `relay_prefix` key (string, default `""`) configures the relay prefix
template prepended to outbound text. The empty default renders no prefix;
no automatic prefix template is defined for this transport. A non-empty
value is applied by the shared relay-prefix formatter
(`format_relay_prefix`) against the source `RelayAttribution`, and the
rendered prefix counts toward the UTF-8 text budget.

MeshCore's companion display is width-constrained, so this transport
declares `sender`, `sender_short`, and `origin_label` truncatable: a
custom template MAY suffix each with a count (`{sender5}` renders the
first five Unicode code points of the value; N ≥ 1, counting code points
never bytes). Numeric suffixes on undeclared stems — such as the
fixed-width `sender_id` pubkey prefix — and on roomy transports, which
declare no truncatable variables, are unsupported placeholders rejected by configuration validation. The shared formatter semantics are specified in the
MEDRE core `docs/spec/routing-delivery.md` §17.5.5.
