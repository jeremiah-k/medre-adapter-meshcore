"""Core-bound MeshCore packets cannot silently change their native destination."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import pytest

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.renderer import MeshCoreRenderer
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterPermanentError
from medre.core.events import DeliveryAttemptProvenance
from medre.core.rendering.renderer import RenderingContext, RenderingResult
from medre_meshcore_tests.helpers.radio_event import radio_event


def _result(
    channel_index: object, *, target: str | None = "2", adapter_id: str = "mc"
) -> RenderingResult:
    return RenderingResult(
        event_id="evt",
        target_adapter=adapter_id,
        target_channel=target,
        payload={"text": "packet", "channel_index": channel_index},
        attempt_provenance=DeliveryAttemptProvenance(
            event_id="evt",
            delivery_plan_id="plan",
            target_adapter=adapter_id,
            target_channel=target,
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


@pytest.mark.parametrize("mode", ["tcp", "fake"])
@pytest.mark.parametrize("target", ["+2", " 2 ", "0_2", "٢", None, "default", ""])
async def test_all_renderable_targets_reject_redirection(mode, target):
    """Alternate numeric forms and fallback destinations cannot skip the fence."""
    adapter = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="mc",
            connection_type=mode,
            host="localhost",
            default_channel=2,
        )
    )
    session = MagicMock()
    session.send_text = AsyncMock(return_value=None)
    adapter._session = session
    result = _result(3, target=target)
    with pytest.raises(AdapterPermanentError, match="differs from durable target"):
        await adapter.deliver(result)
    result.before_native_send.assert_not_awaited()
    session.send_text.assert_not_awaited()


@pytest.mark.parametrize("mode", ["tcp", "fake"])
@pytest.mark.parametrize("target", ["+2", " 2 ", "0_2", "٢", None, "default", ""])
async def test_renderer_destination_is_accepted_without_changing_target_identity(
    mode, target
):
    """Native destination parsing agrees with rendering while durable text stays exact."""
    config = MeshCoreConfig(
        adapter_id="mc",
        connection_type=mode,
        host="localhost",
        default_channel=2,
    )
    rendered = await MeshCoreRenderer(configs={"mc": config}).render(
        radio_event("packet"),
        RenderingContext(
            target_adapter="mc",
            target_channel=target,
            target_platform="meshcore",
            delivery_strategy="direct",
        ),
    )
    assert rendered.payload["channel_index"] == 2
    packet = replace(
        rendered,
        radio_plan=None,
        attempt_provenance=replace(
            _result(2, target=target).attempt_provenance, event_id=rendered.event_id
        ),
        before_native_send=AsyncMock(),
    )
    assert packet.target_channel == target
    assert packet.attempt_provenance.target_channel == (target or None)
    adapter = MeshCoreAdapter(config)
    session = MagicMock()
    session.send_text = AsyncMock(return_value=None)
    adapter._session = session
    await adapter.deliver(packet)
    if mode == "fake":
        packet.before_native_send.assert_awaited_once_with()
        session.send_text.assert_not_awaited()
    else:
        session.send_text.assert_awaited_once_with(
            contact_id="",
            text="packet",
            channel_index=2,
            before_native_send=packet.before_native_send,
        )


@pytest.mark.parametrize("mode", ["tcp", "fake"])
async def test_grant_for_other_adapter_cannot_be_consumed(mode):
    """A fully consistent result for a second adapter cannot use this send owner."""
    current = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="mc",
            connection_type=mode,
            host="localhost",
        )
    )
    other = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="other",
            connection_type=mode,
            host="localhost",
        )
    )
    session = MagicMock()
    session.send_text = AsyncMock(return_value=None)
    current._session = session
    result = _result(2, adapter_id=other.adapter_id)
    with pytest.raises(
        AdapterPermanentError, match="adapter differs from durable target"
    ):
        await current.deliver(result)
    result.before_native_send.assert_not_awaited()
    session.send_text.assert_not_awaited()


@pytest.mark.parametrize("mode", ["tcp", "fake"])
@pytest.mark.parametrize("channel", [True, "2", None])
async def test_default_target_cannot_be_redirected_to_dm(mode, channel):
    adapter = MeshCoreAdapter(
        MeshCoreConfig(
            adapter_id="mc",
            connection_type=mode,
            host="localhost",
            default_channel=2,
        )
    )
    result = _result(channel, target=None)
    result.payload["contact_id"] = "001122334455"
    with pytest.raises(AdapterPermanentError, match="differs from durable target"):
        await adapter.deliver(result)
    result.before_native_send.assert_not_awaited()
