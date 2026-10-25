
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
