"""Companion firmware text limits, including its group sender-name prefix.

BaseChatMesh limits the complete group text to 160 UTF-8 bytes. It prepends
its advertised name and ': ' and otherwise silently clips the message. The
companion frame itself is limited to 176 bytes. Keep clipping in the renderer,
where delivery evidence can report it, and reject bypasses at the session.
"""

GROUP_TEXT_BYTES = 160
MAX_NODE_NAME_BYTES = 31


def group_text_budget(configured_bytes: int, device_name: str | None = None) -> int:
    """Return the safe application-text budget for the current device name."""
    name_bytes = (
        len(device_name.encode("utf-8"))
        if device_name is not None
        else MAX_NODE_NAME_BYTES
    )
    return min(configured_bytes, max(0, GROUP_TEXT_BYTES - name_bytes - 2))
