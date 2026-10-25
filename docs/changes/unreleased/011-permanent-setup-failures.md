# Stop retrying permanent setup failures

MeshCore preserves explicit connection recoverability through SDK wrappers.
Known configuration and SDK contract errors fail startup or stop recovery rather
than retrying indefinitely. Offline transport failures continue retrying.
The recovery regression also exercises inbound delivery after reconnect.
