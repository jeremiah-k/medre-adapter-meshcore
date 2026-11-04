"""Native channel resolution shared by rendering and guarded delivery."""

from __future__ import annotations


def resolve_channel_index(target_channel: str | None, default_channel: int) -> int:
    """Preserve numeric target parsing and the configured fallback destination."""
    if target_channel is None:
        return default_channel
    try:
        return int(target_channel)
    except (ValueError, TypeError):
        return default_channel
