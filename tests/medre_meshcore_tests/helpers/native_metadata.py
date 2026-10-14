"""Canonical native-metadata fixtures for built-in transport tests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _native_data(
    namespace: str,
    schema_version: int,
    defaults: Mapping[str, object],
    field_overrides: Mapping[str, object] | None,
    keyword_overrides: Mapping[str, object],
) -> dict[str, object]:
    """Build one fixture with deterministic override precedence.

    ``field_overrides`` is the optional positional mapping supplied by callers;
    ``keyword_overrides`` is applied last so explicit keyword fields win.
    """
    native: dict[str, object] = {"schema_version": schema_version, **defaults}
    if field_overrides is not None:
        native.update(field_overrides)
    native.update(keyword_overrides)
    return {namespace: native}


def meshcore_native_data(
    field_overrides: Mapping[str, object] | None = None,
    /,
    **overrides: Any,
) -> dict[str, object]:
    """Return a schema-complete MeshCore v1 native-metadata fixture."""
    from medre.adapters.meshcore.event_shape import MESHCORE_NATIVE_SCHEMA_VERSION

    return _native_data(
        "meshcore",
        MESHCORE_NATIVE_SCHEMA_VERSION,
        {
            "packet_id": None,
            "sender_id": "",
            "channel": None,
            "pubkey_prefix": "",
            "txt_type": None,
            "is_direct_message": False,
            "contact_label": None,
            "contact_short_label": None,
            "classification": {
                "action": "relay",
                "category": "text",
                "reason": "test_fixture",
                "is_direct_message": False,
                "routeable": True,
            },
        },
        field_overrides,
        overrides,
    )
