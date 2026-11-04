"""Ordinary native messages use the existing durable packet authority."""

from dataclasses import replace

import msgspec
import pytest

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.renderer import MeshCoreRenderer
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterPermanentError
from medre.core.events import DeliveryAttemptProvenance
from medre.core.rendering.renderer import RenderingContext, RenderingResult
from medre_meshcore_tests.helpers.radio_event import radio_event as event


@pytest.mark.parametrize("connection", ["tcp", "serial", "ble"])
@pytest.mark.parametrize("body", ["short", "α café " * 30])
async def test_native_single_plan_preserves_clipped_wire_text(connection, body):
    config = MeshCoreConfig(
        adapter_id="mc",
        connection_type=connection,
        max_text_bytes=40,
        meshcore_relay_prefix="[North] ",
    )
    context = RenderingContext(
        target_adapter="mc",
        target_channel="2",
        target_platform="meshcore",
        delivery_strategy="direct",
    )
    native = await MeshCoreRenderer(configs={"mc": config}).render(event(body), context)
    synthetic = await MeshCoreRenderer(
        configs={"mc": replace(config, connection_type="fake")}
    ).render(event(body), context)
    assert native.payload == synthetic.payload
    assert native.truncated == synthetic.truncated
    for key in ("original_text_bytes", "rendered_text_bytes", "truncated"):
        assert native.metadata[key] == synthetic.metadata[key]
    assert native.radio_plan is not None
    assert len(native.radio_plan.parts) == 1
    assert native.radio_plan.parts[0].wire_text == native.payload["text"]
    assert native.payload["text"].count("[North] ") == 1
    assert synthetic.radio_plan is None


async def test_reaction_fallback_also_requires_one_native_grant():
    config = MeshCoreConfig(adapter_id="mc", connection_type="tcp", max_text_parts=16)
    reaction = msgspec.structs.replace(event("reacted 👍"), event_kind="reaction.added")
    result = await MeshCoreRenderer(configs={"mc": config}).render(
        reaction,
        RenderingContext(
            target_adapter="mc",
            target_channel="2",
            target_platform="meshcore",
            delivery_strategy="fallback_text",
        ),
    )
    assert result.fallback_applied == "strategy_fallback_text"
    assert result.radio_plan is not None
    assert len(result.radio_plan.parts) == 1
    assert result.radio_plan.parts[0].wire_text == result.payload["text"]


@pytest.mark.parametrize("body,budget", [("", 40), ("body", 0), ("α", 1)])
async def test_empty_native_wire_text_is_refused_before_dispatch(body, budget):
    config = MeshCoreConfig(
        adapter_id="mc", connection_type="tcp", max_text_bytes=budget
    )
    with pytest.raises(ValueError):
        await MeshCoreRenderer(configs={"mc": config}).render(
            event(body),
            RenderingContext(
                target_adapter="mc",
                target_channel="2",
                target_platform="meshcore",
                delivery_strategy="direct",
            ),
        )


async def test_core_bound_prebuilt_packet_cannot_bypass_native_grant():
    adapter = MeshCoreAdapter(
        MeshCoreConfig(adapter_id="mc", connection_type="tcp", host="localhost")
    )
    result = RenderingResult(
        event_id="event",
        target_adapter="mc",
        target_channel="2",
        payload={"text": "packet", "channel_index": 2},
        attempt_provenance=DeliveryAttemptProvenance(
            event_id="event",
            delivery_plan_id="plan",
            target_adapter="mc",
            target_channel="2",
            outbox_id="outbox",
            attempt_number=1,
            source="live",
        ),
    )
    # No session is needed: a bypass is rejected before touching transport state.
    with pytest.raises(AdapterPermanentError, match="requires a native handoff grant"):
        await adapter.deliver(result)
