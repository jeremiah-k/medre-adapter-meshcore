# MeshCore channel text normalization

MeshCore firmware carries the sending node's advertised name in channel text as
`name: body`; direct messages identify their sender separately. The codec MUST
separate a recognized channel sender header from the canonical body so outbound
attribution does not duplicate the sender. It MUST retain the original text in
`metadata.native.data.meshcore.wire_text` when normalization occurs.

Only the recognized firmware header and its single separator space are removed.
Whitespace and colon-prefixed text authored inside the body are preserved.
Messages without a recognized header and direct messages retain their bodies.
An explicitly resolved contact label takes precedence for attribution.

The derived native message identity MUST continue to use the original wire text.
Normalization does not change duplicate detection or receipt correlation. The
optional wire_text field is additive to native metadata schema version 1.
