"""Real SDK single-packet uncertainty remains durable across a storage restart."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.renderer import MeshCoreRenderer
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterPermanentError
from medre.core.engine.pipeline.radio_delivery import deliver_radio_plan
from medre.core.events import DeliveryAttemptProvenance
from medre.core.rendering.renderer import RenderingContext
from medre.core.storage.sqlite.storage import SQLiteStorage
from medre_meshcore_tests.helpers.meshcore_local_node import LocalMeshCoreNode
from medre_meshcore_tests.helpers.storage_outbox import (
    create_outbox_item_with_parent,
    make_outbox_item,
)
from medre_meshcore_tests.helpers.radio_event import radio_event as event

pytestmark = [pytest.mark.local_integration, pytest.mark.meshcore_sdk]


@pytest.mark.parametrize("outcome", ["accepted", "timeout", "disconnect", "cancel"])
async def test_native_single_packet_checkpoint_survives_restart(tmp_path, outcome):
    async with LocalMeshCoreNode(
        stall_sends=outcome in {"timeout", "cancel"},
        disconnect_on_next_send=outcome == "disconnect",
    ) as node:
        config = MeshCoreConfig(
            adapter_id="mc",
            connection_type="tcp",
            host=node.host,
            port=node.port,
            message_delay_seconds=0,
        )
        result = await MeshCoreRenderer(configs={"mc": config}).render(
            event("ordinary packet"),
            RenderingContext(
                target_adapter="mc",
                target_channel="2",
                target_platform="meshcore",
                delivery_strategy="direct",
            ),
        )
        assert result.radio_plan is not None and len(result.radio_plan.parts) == 1
        path = str(tmp_path / "ledger.sqlite")
        storage = SQLiteStorage(path)
        await storage.initialize()
        session = MeshCoreSession(config, "mc")
        adapter = MeshCoreAdapter(config)
        adapter._session = session
        task = None
        try:
            item = await create_outbox_item_with_parent(
                storage,
                make_outbox_item(
                    event_id=result.event_id,
                    target_adapter="mc",
                    target_channel="2",
                    attempt_number=0,
                ),
            )
            claims = await storage.claim_due_outbox_items(
                datetime.now(UTC).isoformat(), "owner"
            )
            assert any(row.outbox_id == item.outbox_id for row in claims)
            assert await storage.reserve_outbox_attempt(item.outbox_id, "owner", 0) == 1
            result = replace(
                result,
                attempt_provenance=DeliveryAttemptProvenance(
                    event_id=item.event_id,
                    delivery_plan_id=item.delivery_plan_id,
                    target_adapter="mc",
                    target_channel="2",
                    outbox_id=item.outbox_id,
                    attempt_number=1,
                    source="live",
                ),
            )
            await session.start(lambda _payload: None)
            session._meshcore.default_timeout = 0.1
            task = asyncio.create_task(
                deliver_radio_plan(storage, result, "owner", adapter.deliver)
            )
            await asyncio.wait_for(node.send_seen.wait(), timeout=2)
            submitting = await storage.get_radio_attempt_snapshot(item.outbox_id, 1)
            assert submitting["parts"][0]["state"] in {
                "submitting",
                "accepted",
                "unknown",
            }
            if outcome == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            elif outcome != "accepted":
                with pytest.raises(
                    AdapterPermanentError, match="retained native progress"
                ):
                    await task
            else:
                handoff = await task
                assert handoff.confirmation_level == "local_transport"
                assert handoff.native_message_id is None
            assert len(node.send_commands) == 1
            snapshot = await storage.get_radio_attempt_snapshot(item.outbox_id, 1)
            expected = "accepted" if outcome == "accepted" else "unknown"
            assert snapshot["parts"][0]["state"] == expected
            await storage.close()
            storage = SQLiteStorage(path)
            await storage.initialize()
            retained = await storage.get_radio_attempt_snapshot(item.outbox_id, 1)
            assert retained["parts"][0]["state"] == expected
            # Neither positive acceptance nor uncertain submission is a new grant.
            with pytest.raises(AdapterPermanentError, match="already bound"):
                await deliver_radio_plan(storage, result, "owner", adapter.deliver)
            assert len(node.send_commands) == 1
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await session.stop()
            await storage.close()
