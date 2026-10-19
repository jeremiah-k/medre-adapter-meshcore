# MeshCore channel sender contract

MeshCore channel messages do not carry authenticated sender identity. Sender
names decoded from the exact `name: body` firmware wire convention MUST be
treated as **unverified claims**. All channel-contact labels, even local contact
lookups, MUST show an explicit `(unverified)` marker in projected long and
short sender labels, because channel sender identity is unauthenticated. The original wire text MUST
remain available in the transport-native metadata. Its shape uses native metadata
version 2; canonical event schema versions are independent.

The codec MUST only strip an actual firmware-style colon-and-one-space header.
Other colon/whitespace text MUST remain user-authored body, and direct messages
MUST NOT be subject to channel-header normalization.
