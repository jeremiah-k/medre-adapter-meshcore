# Separate channel sender headers from message bodies

Canonical channel text excludes the recognized firmware sender header, preventing
duplicate sender names when another renderer adds relay attribution. Original
wire text remains in optional native wire_text metadata, and derived message IDs
retain their wire inputs. Direct-message bodies and explicit contact-label
precedence are preserved.
