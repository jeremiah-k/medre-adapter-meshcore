"""Core-bound MeshCore packets cannot silently change their native destination."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterPermanentError
from medre.core.events import DeliveryAttemptProvenance
from medre.core.rendering.renderer import RenderingResult


def _result(channel_index: object) -> RenderingResult:
    return RenderingResult(
        event_id="evt",
        target_adapter="mc",
        target_channel="2",
        payload={"text": "packet", "channel_index": channel_index},
        attempt_provenance=DeliveryAttemptProvenance(
            event_id="evt",
            delivery_plan_id="plan",
            target_adapter="mc",
            target_channel="2",
            outbox_id="outbox",
            attempt_number=1,
            source="live",
        ),
        before_native_send=AsyncMock(),
    )


@pytest.mark.parametrize(
    "channel",
    [3, "2", True, None],
)
async def test_core_bound_mismatch_is_refused_before_sdk(channel: object) -> None:
    """A wrong native target cannot consume a grant or invoke the SDK."""
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc", connection_type="tcp", host="localhost")
    )
    session = MagicMock()
    session.send_text = AsyncMock(return_value=None)
    adapter._session = session
    result = _result(channel)

    with pytest.raises(AdapterPermanentError, match="differs from durable target"):
        await adapter.deliver(result)
    grant = result.before_native_send
    assert isinstance(grant, AsyncMock)
    grant.assert_not_awaited()
    session.send_text.assert_not_awaited()


async def test_matching_core_bound_channel_reaches_owned_send() -> None:
    """Matching channel packets still forward the exact one-use callback."""
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc", connection_type="tcp", host="localhost")
    )
    session = MagicMock()
    session.send_text = AsyncMock(return_value=None)
    adapter._session = session
    result = _result(2)

    await adapter.deliver(result)
    session.send_text.assert_awaited_once_with(
        contact_id="",
        text="packet",
        channel_index=2,
        before_native_send=result.before_native_send,
    )
