# Separate channel sender headers from message bodies

Canonical channel text excludes the recognized firmware sender header, preventing
duplicate sender names when another renderer adds relay attribution. Original
wire text remains in optional native wire_text metadata, and derived message IDs
retain their wire inputs. Direct-message bodies and explicit contact-label
precedence are preserved.

Only exact firmware colon-space sender headers are normalized; ambiguous
colon/whitespace text remains authored body. Unicode names are supported,
blank contact labels fall back to wire claims, and all channel sender claims
are visibly marked unverified. The native metadata shape advances to version 2
without changing canonical event versioning. Missing-SDK tests fail before
attempting transport connections.
