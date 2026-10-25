"""Firmware-added sender names cannot silently truncate delivered text."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.errors import MeshCoreSendError
from medre.adapters.meshcore.limits import group_text_budget
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig


@pytest.mark.parametrize(
    "name,expected", [(None, 127), ("", 158), ("MEDRE-MC-A", 148), ("é" * 15, 128)]
)
def test_group_budget_counts_utf8_firmware_prefix(name, expected):
    assert group_text_budget(512, name) == expected
    assert group_text_budget(10, name) == 10


async def test_current_device_name_controls_planning_budget():
    config = MeshCoreConfig(adapter_id="mc", message_delay_seconds=0)
    adapter = MeshCoreAdapter(config)
    assert adapter._capabilities.max_text_bytes == 127
    session = MeshCoreSession(config, adapter_id="mc")
    adapter._session = session
    session._capture_self_info({"name": "MEDRE-MC-A"})
    assert adapter._capabilities.max_text_bytes == 148
    session._capture_self_info({"name": "X" * 31})
    assert adapter._capabilities.max_text_bytes == 127


async def test_oversize_group_send_fails_permanently_before_sdk_or_retry():
    session = MeshCoreSession(
        MeshCoreConfig(adapter_id="mc", message_delay_seconds=0), adapter_id="mc"
    )
    session._capture_self_info({"name": "MEDRE-MC-A"})
    commands = SimpleNamespace(send_chan_msg=AsyncMock())
    session._meshcore = SimpleNamespace(commands=commands)
    with pytest.raises(MeshCoreSendError) as raised:
        await session._send_real("", "é" * 75, channel_index=1)
    assert raised.value.transient is False
    commands.send_chan_msg.assert_not_awaited()


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"body": "current", "text": "legacy"}, "legacy"),
        (
            {
                "body": "current",
                "text": "legacy",
                "formatting": {"version": 1, "spans": []},
            },
            "current",
        ),
    ],
)
async def test_meshcore_renderer_uses_shared_text_authority(payload, expected):
    """The radio must not choose different text from the other destinations."""
    from datetime import UTC, datetime

    from medre.adapters.meshcore.renderer import MeshCoreRenderer
    from medre.core.events import CanonicalEvent, EventMetadata
    from medre.core.rendering.renderer import RenderingContext

    config = MeshCoreConfig(adapter_id="mc", meshcore_relay_prefix="")
    event = CanonicalEvent(
        event_id="text-source", event_kind="message.created", schema_version=1,
        timestamp=datetime.now(UTC), source_adapter="source",
        source_transport_id="node", source_channel_id="0", parent_event_id=None,
        lineage=(), relations=(), payload=payload, metadata=EventMetadata(),
    )
    result = await MeshCoreRenderer(configs={"mc": config}).render(
        event,
        RenderingContext(
            target_adapter="mc", target_channel="0",
            target_platform="meshcore", delivery_strategy="direct",
        ),
    )
    assert result.payload["text"] == expected
