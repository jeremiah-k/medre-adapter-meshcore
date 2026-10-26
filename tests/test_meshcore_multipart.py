"""Opt-in radio plans use the existing paced owner and one guarded native call."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.errors import MeshCoreSendError
from medre.adapters.meshcore.renderer import MeshCoreRenderer
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig, MeshCoreConfigError
from medre.core.contracts.adapter import AdapterPermanentError
from medre.core.contracts.delivery import AdapterHandoffResult
from medre.core.engine.pipeline.radio_delivery import deliver_radio_plan
from medre.core.events import CanonicalEvent, DeliveryAttemptProvenance, EventMetadata
from medre.core.rendering.renderer import RenderingContext
from medre_meshcore_tests.helpers.storage_outbox import (
    create_outbox_item_with_parent,
    make_outbox_item,
)


def event(text):
    """Build a canonical message with a fixed native scope and Unicode source text."""
    from datetime import UTC, datetime

    return CanonicalEvent(
        event_id="multipart-event",
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


async def render(text, *, max_parts=16):
    """Render a small real byte budget with an attribution prefix on every packet."""
    config = MeshCoreConfig(
        adapter_id="mc",
        max_text_bytes=40,
        max_text_parts=max_parts,
        meshcore_relay_prefix="[North] ",
    )
    result = await MeshCoreRenderer(configs={"mc": config}).render(
        event(text),
        RenderingContext(
            target_adapter="mc",
            target_channel="0",
            target_platform="meshcore",
            delivery_strategy="direct",
        ),
    )
    return result


async def test_renderer_preserves_all_text_and_repeats_attribution_within_budget():
    """Long words, whitespace and Unicode survive an exact shared fragment plan."""
    body = "α café\n" * 12
    result = await render(body)
    assert result.truncated is False
    assert result.radio_plan is not None
    assert "".join(part.body for part in result.radio_plan.parts) == body
    assert all(
        part.wire_text.startswith("[North] (") for part in result.radio_plan.parts
    )
    assert all(
        len(part.wire_text.encode("utf-8")) <= 40 for part in result.radio_plan.parts
    )
    assert result.payload["text"] == result.radio_plan.parts[0].wire_text


async def test_single_packet_defaults_remain_bounded_and_opt_in_rejects_overflow():
    """The opt-in never silently clips a plan that exceeds the configured part cap."""
    result = await render("word " * 100, max_parts=1)
    assert result.radio_plan is None and result.truncated
    assert len(result.payload["text"].encode("utf-8")) <= 40
    with pytest.raises(ValueError, match="part limit"):
        await render("word " * 100, max_parts=2)
    short = await render("short")
    assert short.radio_plan is None
    assert short.payload["text"] == "[North] short"


async def test_zero_byte_budget_stays_on_bounded_single_packet_path():
    """No multi-part planner may turn a zero-byte budget into a send attempt."""
    config = MeshCoreConfig(
        adapter_id="mc",
        max_text_bytes=0,
        max_text_parts=16,
        meshcore_relay_prefix="[North] ",
    )
    result = await MeshCoreRenderer(configs={"mc": config}).render(
        event("radio body"),
        RenderingContext(
            target_adapter="mc",
            target_channel="0",
            target_platform="meshcore",
            delivery_strategy="direct",
        ),
    )
    assert result.radio_plan is None
    assert result.payload["text"] == ""
    assert result.truncated is True


def test_part_limit_appends_after_existing_positional_fields():
    """Adding the opt-in must not change preexisting positional config values."""
    from dataclasses import fields

    defaults = MeshCoreConfig(adapter_id="mc")
    names = [field.name for field in fields(defaults)]
    assert names.index("max_text_parts") > names.index("meshcore_relay_prefix")
    args = [getattr(defaults, name) for name in names[: names.index("max_text_parts")]]
    args[names.index("max_text_bytes")] = 40
    args[names.index("meshcore_relay_prefix")] = "[North] "
    config = MeshCoreConfig(*args)
    assert config.max_text_bytes == 40
    assert config.meshcore_relay_prefix == "[North] "
    assert config.max_text_parts == 1


@pytest.mark.parametrize("value", [True, False, 0, 129, 2.5, "2", None])
def test_part_limit_rejects_invalid_config(value):
    """The explicit opt-in is bounded and does not interpret bool as an integer."""
    with pytest.raises(MeshCoreConfigError, match="max_text_parts"):
        MeshCoreConfig(adapter_id="mc", max_text_parts=value).validate()


@pytest.mark.parametrize("fails", [False, True])
async def test_guard_runs_inside_lock_after_pacing_and_native_is_never_retried(
    monkeypatch, fails
):
    """A timeout after grant is ambiguous; no SDK retry may consume the same grant."""
    config = MeshCoreConfig(
        adapter_id="mc",
        connection_type="tcp",
        host="localhost",
        message_delay_seconds=0.5,
    )
    session = MeshCoreSession(config, "mc")
    session._diag.connected = True
    phases = []

    async def pause(delay):
        assert delay == 0.5
        assert session._send_lock.locked()
        phases.append("paced")

    async def grant():
        assert session._send_lock.locked()
        assert phases[-1] == "paced"
        phases.append("grant")

    async def send(channel, text):
        assert phases[-1] == "grant"
        phases.append("native")
        if fails:
            raise TimeoutError("ambiguous")
        return {}

    sdk_send = AsyncMock(side_effect=send)
    session._meshcore = SimpleNamespace(
        commands=SimpleNamespace(send_chan_msg=sdk_send)
    )
    monkeypatch.setattr("medre.adapters.meshcore.session.asyncio.sleep", pause)
    if fails:
        with pytest.raises(MeshCoreSendError):
            await session.send_text(
                "", "packet", channel_index=0, before_native_send=grant
            )
    else:
        assert (
            await session.send_text(
                "", "packet", channel_index=0, before_native_send=grant
            )
            is None
        )
    assert phases == ["paced", "grant", "native"]
    assert sdk_send.await_count == 1


async def test_expired_grant_cannot_reach_native_sdk():
    """A refused storage checkpoint propagates before the actual native command."""
    config = MeshCoreConfig(
        adapter_id="mc",
        connection_type="tcp",
        host="localhost",
        message_delay_seconds=0,
    )
    session = MeshCoreSession(config, "mc")
    session._diag.connected = True
    native = AsyncMock()
    session._meshcore = SimpleNamespace(commands=SimpleNamespace(send_chan_msg=native))

    async def refuse():
        raise AdapterPermanentError("claim expired")

    with pytest.raises(AdapterPermanentError, match="claim expired"):
        await session.send_text(
            "", "packet", channel_index=0, before_native_send=refuse
        )
    native.assert_not_awaited()


async def test_stop_while_waiting_for_send_owner_blocks_grant_and_native():
    """Shutdown is checked again after queued pacing, not only on entry."""
    config = MeshCoreConfig(
        adapter_id="mc",
        connection_type="tcp",
        host="localhost",
        message_delay_seconds=0,
    )
    session = MeshCoreSession(config, "mc")
    session._diag.connected = True
    native, grant = AsyncMock(), AsyncMock()
    session._meshcore = SimpleNamespace(commands=SimpleNamespace(send_chan_msg=native))
    await session._send_lock.acquire()
    started = asyncio.Event()

    async def submit():
        started.set()
        await session.send_text("", "packet", channel_index=0, before_native_send=grant)

    task = asyncio.create_task(submit())
    await started.wait()
    session._stop_requested = True
    session._send_lock.release()
    with pytest.raises(MeshCoreSendError):
        await task
    grant.assert_not_awaited()
    native.assert_not_awaited()


@pytest.mark.parametrize("interruption", ["stop", "disconnect", "replace_sdk"])
async def test_session_changes_during_guard_never_reach_native(interruption):
    """A committed grant cannot send through a stopped or replaced SDK client."""
    config = MeshCoreConfig(
        adapter_id="mc",
        connection_type="tcp",
        host="localhost",
        message_delay_seconds=0,
    )
    session = MeshCoreSession(config, "mc")
    session._diag.connected = True
    original_native = AsyncMock()
    replacement_native = AsyncMock()
    original_client = SimpleNamespace(
        commands=SimpleNamespace(send_chan_msg=original_native)
    )
    session._meshcore = original_client
    grant_entered = asyncio.Event()
    release_grant = asyncio.Event()
    grants = []

    async def grant():
        assert session._send_lock.locked()
        grants.append(True)
        grant_entered.set()
        await release_grant.wait()  # storage I/O yields to stop or reconnect

    task = asyncio.create_task(
        session.send_text("", "packet", channel_index=0, before_native_send=grant)
    )
    await grant_entered.wait()
    if interruption == "stop":
        session._stop_requested = True
    elif interruption == "disconnect":
        session._diag.connected = False
    else:
        session._meshcore = SimpleNamespace(
            commands=SimpleNamespace(send_chan_msg=replacement_native)
        )
    release_grant.set()
    with pytest.raises(MeshCoreSendError, match="changed after native handoff grant"):
        await task
    assert grants == [True]
    original_native.assert_not_awaited()
    replacement_native.assert_not_awaited()


async def test_actual_adapter_and_session_persist_each_native_handoff(temp_storage):
    """The production adapter path runs the core plan through the real send owner."""
    from dataclasses import replace

    result = await render("café α " * 14)
    item = replace(
        make_outbox_item(
            status="in_progress",
            event_id=result.event_id,
            target_adapter="mc",
            target_channel="0",
        ),
        worker_id="owner",
    )
    item = await create_outbox_item_with_parent(temp_storage, item)
    claim = DeliveryAttemptProvenance(
        event_id=item.event_id,
        delivery_plan_id=item.delivery_plan_id,
        target_adapter="mc",
        target_channel="0",
        outbox_id=item.outbox_id,
        attempt_number=1,
        source="live",
    )
    config = MeshCoreConfig(
        adapter_id="mc",
        connection_type="tcp",
        host="localhost",
        message_delay_seconds=0,
    )
    adapter = MeshCoreAdapter(config)
    session = MeshCoreSession(config, "mc")
    session._diag.connected = True
    seen = []

    async def native(channel, text):
        snapshot = await temp_storage.get_radio_attempt_snapshot(item.outbox_id, 1)
        assert snapshot["parts"][len(seen)]["state"] == "submitting"
        seen.append(text)
        return {}

    session._meshcore = SimpleNamespace(
        commands=SimpleNamespace(send_chan_msg=AsyncMock(side_effect=native))
    )
    adapter._session = session
    handoff = await deliver_radio_plan(
        temp_storage,
        replace(result, attempt_provenance=claim),
        "owner",
        adapter.deliver,
    )
    assert isinstance(handoff, AdapterHandoffResult)
    assert seen == [part.wire_text for part in result.radio_plan.parts]
    assert handoff.native_message_id is None
    assert handoff.confirmation_level == "local_transport"
    snapshot = await temp_storage.get_radio_attempt_snapshot(item.outbox_id, 1)
    assert all(part["state"] == "accepted" for part in snapshot["parts"])


async def test_complete_plan_cannot_bypass_core_with_direct_adapter_delivery():
    """A direct caller cannot accidentally send only a plan's first packet."""
    result = await render("long body " * 20)
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc", connection_type="fake"))
    with pytest.raises(AdapterPermanentError, match="core-owned packet dispatch"):
        await adapter.deliver(result)
