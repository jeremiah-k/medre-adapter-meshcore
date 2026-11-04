"""Canonical input shared by guarded radio rendering tests."""

from medre.core.events import CanonicalEvent, EventMetadata


def radio_event(text: str) -> CanonicalEvent:
    """Build a canonical message with a fixed native scope and Unicode source text."""
    from datetime import UTC, datetime

    return CanonicalEvent(
        event_id="radio-event",
        event_kind="message.created",
        schema_version=1,
        timestamp=datetime.now(UTC),
        source_adapter="source",
        source_transport_id="peer",
        source_channel_id="0",
        parent_event_id=None,
        lineage=(),
        relations=(),
        payload={"text": text},
        metadata=EventMetadata(),
    )
