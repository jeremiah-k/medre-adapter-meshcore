"""Local real-SDK integration tests for the MeshCore session boundary."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from typing import Any

import pytest
from medre_meshcore_tests.helpers.async_utils import wait_until
from medre_meshcore_tests.helpers.meshcore_local_node import (
    LocalMeshCoreNode,
    channel_message_payload,
)

from medre.adapters.meshcore import session as session_mod
from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.errors import MeshCoreConnectionError
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig

pytestmark = [pytest.mark.local_integration, pytest.mark.meshcore_sdk]


def _config(node: LocalMeshCoreNode) -> MeshCoreConfig:
    return MeshCoreConfig(
        adapter_id="meshcore-local-integration",
        connection_type="tcp",
        host=node.host,
        port=node.port,
        message_delay_seconds=0,
    )


def _session(node: LocalMeshCoreNode) -> MeshCoreSession:
    return MeshCoreSession(
        adapter_id="meshcore-local-integration",
        config=_config(node),
        logger=logging.getLogger("test.meshcore.local"),
    )


async def test_real_sdk_start_inbound_send_and_stop() -> None:
    received: list[dict[str, Any]] = []
    async with LocalMeshCoreNode() as node:
        session = _session(node)
        await session.start(received.append)
        assert session.connected is True
        assert session.diagnostics()["device_name"] == "MEDRE local node"

        await node.inject_channel_message("local inbound", channel_index=2)
        assert await wait_until(lambda: len(received) == 1)
        assert received[0]["text"] == "local inbound"
        assert received[0]["channel_idx"] == 2

        native_id = await session.send_text("001122334455", "local outbound")
        assert native_id == "10203040"
        assert len(node.send_commands) == 1

        await session.stop()
        assert session.connected is False


async def test_real_sdk_repeated_start_stop_reuses_local_endpoint() -> None:
    async with LocalMeshCoreNode() as node:
        session = _session(node)
        for _ in range(3):
            await session.start(lambda _payload: None)
            assert session.connected is True
            await session.stop()
            assert session.connected is False
        assert node.connection_count == 3


async def test_real_sdk_startup_error_cleans_partial_client() -> None:
    async with LocalMeshCoreNode(appstart_error=True) as node:
        session = _session(node)
        with pytest.raises(MeshCoreConnectionError, match="No response"):
            await session.start(lambda _payload: None)
        assert session.connected is False
        assert session.diagnostics()["reconnecting"] is False
        assert session._meshcore is None


async def test_real_sdk_connect_drains_messages_queued_before_connect() -> None:
    """Every message the node buffered before the connect is delivered.

    The node hands a queued message to the client only in answer to
    ``GET_MSG`` and never emits a ``MESSAGES_WAITING`` (0x83) notification,
    so a backlog that the SDK's auto-fetch loop does not fetch explicitly
    starves until a *new* message arrives.
    """
    received: list[dict[str, Any]] = []
    async with LocalMeshCoreNode() as node:
        node.queue_channel_message("queued one")
        node.queue_channel_message("queued two")
        node.queue_channel_message("queued three")

        session = _session(node)
        await session.start(received.append)

        assert await wait_until(lambda: len(received) == 3)
        assert [item["text"] for item in received] == [
            "queued one",
            "queued two",
            "queued three",
        ]
        assert node.queued_messages == []
        await session.stop()


async def test_real_sdk_connect_drains_backlog_larger_than_the_old_round_cap() -> None:
    """A backlog deeper than the previous 64-round cap drains completely.

    openHop's repeater companion queues up to 512 messages by default
    (``offline_queue_size``), so a drain bound below that leaves the tail
    of the backlog stranded in the node.  This queues 100 messages before
    the connect — more than the old cap of 64 rounds — and requires every
    one of them to be delivered exactly once, in node order, with the
    node's queue empty afterwards.
    """
    received: list[dict[str, Any]] = []
    queued = [f"queued {index}" for index in range(100)]
    async with LocalMeshCoreNode() as node:
        for text in queued:
            node.queue_channel_message(text)

        session = _session(node)
        await session.start(received.append)

        assert await wait_until(lambda: len(received) == len(queued), timeout=10.0)
        texts = [item["text"] for item in received]
        assert texts == queued
        # Exactly once: no message is delivered twice by the drain.
        assert len(set(texts)) == len(queued)
        assert node.queued_messages == []
        await session.stop()


async def test_real_sdk_backlog_does_not_block_startup() -> None:
    received: list[dict[str, Any]] = []
    async with LocalMeshCoreNode() as node:
        for index in range(100):
            node.queue_channel_message(f"queued {index}")
        session = _session(node)
        await session.start(received.append)
        # The pump has not run in the caller's startup turn.
        assert received == []
        assert await wait_until(lambda: len(received) == 100, timeout=10.0)
        assert node.queued_messages == []
        await session.stop()


async def test_real_sdk_reconnect_drains_messages_queued_while_disconnected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backlog that piled up while the transport was down survives reconnect."""
    received: list[dict[str, Any]] = []
    async with LocalMeshCoreNode() as node:
        # Keep the recovery window wide enough to queue the backlog while
        # the client is provably disconnected, and free of jitter so the
        # reconnect ordering stays deterministic.
        monkeypatch.setattr(session_mod, "_RECONNECT_BASE_DELAY", 1.0)
        monkeypatch.setattr(session_mod, "_RECONNECT_JITTER_FRACTION", 0.0)

        session = _session(node)
        await session.start(received.append)

        await node.disconnect_clients()
        node.queue_channel_message("offline one")
        node.queue_channel_message("offline two")

        assert await wait_until(lambda: node.connection_count >= 2, timeout=10.0)
        assert await wait_until(lambda: session.connected, timeout=10.0)
        assert await wait_until(lambda: len(received) == 2, timeout=10.0)
        assert [item["text"] for item in received] == [
            "offline one",
            "offline two",
        ]
        assert node.queued_messages == []
        await session.stop()


async def test_real_sdk_disconnect_during_inbound_reconnects() -> None:
    received: list[dict[str, Any]] = []
    async with LocalMeshCoreNode() as node:
        session = _session(node)
        await session.start(received.append)
        await node.inject_channel_message("before disconnect")
        assert await wait_until(lambda: len(received) == 1)

        await node.disconnect_clients()
        assert await wait_until(lambda: node.connection_count >= 2, timeout=5.0)
        assert await wait_until(lambda: session.connected, timeout=5.0)

        await node.inject_channel_message("after reconnect")
        assert await wait_until(lambda: len(received) == 2)
        assert [item["text"] for item in received] == [
            "before disconnect",
            "after reconnect",
        ]
        await session.stop()


async def test_real_sdk_malformed_frame_resynchronizes() -> None:
    received: list[dict[str, Any]] = []
    async with LocalMeshCoreNode() as node:
        session = _session(node)
        await session.start(received.append)

        await node.inject_malformed_frame_then_channel("after malformed")
        assert await wait_until(lambda: len(received) == 1)
        assert received[0]["text"] == "after malformed"
        assert session.connected is True
        await session.stop()


async def test_real_sdk_disconnect_during_outbound_recovers_on_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with LocalMeshCoreNode(disconnect_on_next_send=True) as node:
        session = _session(node)
        # Make the ownership transition deterministic without weakening the
        # real SDK command/response path exercised by this test.
        monkeypatch.setattr(session_mod, "_RECONNECT_BASE_DELAY", 0.0)
        monkeypatch.setattr(session_mod, "_RECONNECT_JITTER_FRACTION", 0.0)
        await session.start(lambda _payload: None)
        assert session._meshcore is not None
        session._meshcore.default_timeout = 0.2

        native_id = await session.send_text("001122334455", "disconnect once")
        assert native_id == "10203040"
        assert await wait_until(lambda: node.connection_count >= 2)
        assert session.connected is True
        assert session.transient_delivery_failures == 1
        await session.stop()


async def test_real_sdk_stalled_send_is_cancellable() -> None:
    async with LocalMeshCoreNode(stall_sends=True) as node:
        session = _session(node)
        await session.start(lambda _payload: None)
        task = asyncio.create_task(session.send_text("001122334455", "stalled"))
        await asyncio.wait_for(node.send_seen.wait(), timeout=2.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await session.stop()


async def test_real_sdk_send_lock_applies_backpressure() -> None:
    async with LocalMeshCoreNode(hold_sends=True) as node:
        session = _session(node)
        await session.start(lambda _payload: None)

        first = asyncio.create_task(session.send_text("001122334455", "first"))
        first_receipt = await asyncio.wait_for(node.next_send_receipt(), timeout=2.0)
        assert first_receipt in node.send_commands

        second_entered = asyncio.Event()

        async def send_second() -> str | None:
            second_entered.set()
            return await session.send_text("001122334455", "second")

        second = asyncio.create_task(send_second())
        await asyncio.wait_for(second_entered.wait(), timeout=2.0)

        # The local endpoint keeps reading while the first response is blocked.
        # Without the MEDRE session lock, the second command would reach this
        # receipt queue before the first response is released.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(node.next_send_receipt(), timeout=0.1)

        node.release_sends.set()
        assert await first == "10203040"
        second_receipt = await asyncio.wait_for(node.next_send_receipt(), timeout=2.0)
        assert second_receipt in node.send_commands
        assert await second == "10203040"
        assert len(node.send_commands) == 2
        await session.stop()


@pytest.mark.soak
async def test_real_sdk_local_soak_repeated_lifecycle_and_send() -> None:
    async with LocalMeshCoreNode() as node:
        session = _session(node)
        for index in range(10):
            await session.start(lambda _payload: None)
            native_id = await session.send_text(
                "001122334455",
                f"local soak {index}",
            )
            assert native_id == "10203040"
            await session.stop()
        assert node.connection_count == 10
        assert len(node.send_commands) == 10


async def test_real_sdk_delayed_duplicate_cannot_advance_the_native_queue(
    monkeypatch: pytest.MonkeyPatch, make_adapter_context: Any
) -> None:
    """Separate the node's destructive pop from the SDK's response waiter."""
    send_second = asyncio.Event()
    second_admitting = asyncio.Event()
    commit_second = asyncio.Event()
    committed: list[str] = []

    async def publish(canonical):
        body = str(canonical.payload["body"])
        if body == "second":
            second_admitting.set()
            await commit_second.wait()
        committed.append(body)

    async with LocalMeshCoreNode() as node:
        for text in ("first", "second", "third"):
            node.queue_channel_message(text)
        handle = node._handle_command

        async def handle_fetch(payload, writer):
            if payload != b"\x0a" or node.commands.count(b"\x0a") != 2:
                await handle(payload, writer)
                return
            second, channel_index = node.queued_messages.pop(0)
            assert second == "second"
            await node._respond(writer, channel_message_payload("first"))
            await send_second.wait()
            await node._respond(
                writer, channel_message_payload(second, channel_index=channel_index)
            )

        monkeypatch.setattr(node, "_handle_command", handle_fetch)
        adapter = MeshCoreAdapter(
            replace(_config(node), max_pending_ingress_callbacks=1)
        )
        context = make_adapter_context("meshcore-local-integration")
        context.publish_inbound = publish
        await adapter.start(context)
        try:
            session = adapter._session
            assert session is not None
            assert await wait_until(
                lambda: session._pump_parked and session._pump_silent_rounds == 1,
                timeout=2,
            )
            assert node.commands.count(b"\x0a") == 2
            assert node.queued_messages == [("third", 0)]
            send_second.set()
            await asyncio.wait_for(second_admitting.wait(), 2)
            assert session._receive_permit_ready is not None
            assert not session._receive_permit_ready()
            assert node.commands.count(b"\x0a") == 2
            assert committed == ["first"]
            commit_second.set()
            assert await wait_until(
                lambda: committed == ["first", "second", "third"]
                and session._pump_parked,
                timeout=2,
            )
            assert node.queued_messages == []
        finally:
            send_second.set()
            commit_second.set()
            await adapter.stop()
