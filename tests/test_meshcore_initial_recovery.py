"""Long-lived adapters recover initial native failure without inventing handoff."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

import medre.adapters.meshcore.session as session_module
from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.errors import MeshCoreConnectionError, MeshCoreSendError
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig
from medre_meshcore_tests.helpers.async_utils import wait_until


@pytest.fixture
def recovering_session(monkeypatch):
    """Keep native attempts deterministic and isolated from all BLE/TCP sockets."""
    monkeypatch.setattr(session_module, "HAS_MESHCORE", True)
    monkeypatch.setattr(session_module, "_RECONNECT_BASE_DELAY", 0.0)
    monkeypatch.setattr(session_module, "_RECONNECT_MAX_DELAY", 0.0)
    monkeypatch.setattr(session_module, "_BLE_RECONNECT_BASE_DELAY", 0.0)
    monkeypatch.setattr(session_module, "_BLE_RECONNECT_MAX_DELAY", 0.0)
    monkeypatch.setattr(session_module, "_RECONNECT_JITTER_FRACTION", 0.0)
    return MeshCoreSession(
        MeshCoreConfig(
            adapter_id="recovering", connection_type="tcp", host="offline.invalid"
        ),
        "recovering",
    )


async def test_initial_offline_adapter_becomes_healthy_and_keeps_inbound_callback(
    recovering_session, monkeypatch, make_adapter_context, inbound_collector
):
    """The production adapter opts into recovery and preserves its session owner."""
    session = recovering_session
    release, retrying = asyncio.Event(), asyncio.Event()
    attempts = 0

    async def connect():
        """Model an offline board becoming reachable after startup has returned."""
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise MeshCoreConnectionError("node powered off")
        retrying.set()
        await release.wait()
        session._diag.connected = True

    monkeypatch.setattr(session, "_connect_real", connect)
    monkeypatch.setattr(
        "medre.adapters.meshcore.adapter.MeshCoreSession", lambda **kwargs: session
    )
    adapter = MeshCoreAdapter(session._config)
    try:
        await adapter.start(make_adapter_context("recovering"))
        assert (await adapter.health_check()).health == "degraded"
        assert session.reconnecting
        assert session._message_callback is not None
        with pytest.raises(MeshCoreSendError) as unavailable:
            await session.send_text("", "pending", channel_index=0)
        assert unavailable.value.transient is True
        await retrying.wait()
        release.set()
        assert await wait_until(lambda: session.connected)
        assert (await adapter.health_check()).health == "healthy"
        assert attempts == 2
        assert session._message_callback is not None
        result = session._message_callback(
            {
                "type": "CHAN",
                "channel_idx": 0,
                "sender_timestamp": 42,
                "text": "Peer: received after recovery",
                "txt_type": 0,
            }
        )
        if result is not None:
            await result
        assert await wait_until(lambda: len(inbound_collector.events) == 1)
        assert inbound_collector.events[0].payload["body"] == "received after recovery"
    finally:
        await adapter.stop()
    assert not session.connected
    assert not session.reconnecting
    assert session._reconnect_task is None


async def test_recovery_does_not_give_up_after_ten_failures(
    recovering_session, monkeypatch
):
    """A board returning after a longer outage still gets a new native attempt."""
    session = recovering_session
    attempts = 0

    async def connect():
        """Succeed only after the previous hard-coded attempt cap is exceeded."""
        nonlocal attempts
        attempts += 1
        if attempts <= 12:
            raise MeshCoreConnectionError("still powered off")
        session._diag.connected = True

    monkeypatch.setattr(session, "_connect_real", connect)
    assert session_module._RECONNECT_MAX_ATTEMPTS == 0
    await session.start(lambda _: None, recover_on_failure=True)
    try:
        assert await wait_until(lambda: session.connected)
        assert attempts == 13
    finally:
        await session.stop()


async def test_connection_deadline_cleans_partial_sdk_before_recovery(
    recovering_session, monkeypatch
):
    """A stalled initial call cancels before any retry can own its partial client."""
    session = recovering_session
    monkeypatch.setattr(session_module, "_CONNECT_ATTEMPT_TIMEOUT", 0.01)
    retired = asyncio.Event()
    calls = 0
    active = 0
    cleanup = AsyncMock()

    async def connect():
        """Own a cancellable native setup operation and expose overlap errors."""
        nonlocal calls, active
        calls += 1
        active += 1
        assert active == 1
        try:
            await asyncio.Event().wait()
        finally:
            active -= 1
            retired.set()

    monkeypatch.setattr(session, "_connect_real", connect)
    monkeypatch.setattr(session, "_cleanup_stale_sdk", cleanup)
    await session.start(lambda _: None, recover_on_failure=True)
    try:
        assert retired.is_set()
        assert "exceeded" in session.last_error
        assert not session.connected
    finally:
        await session.stop()
    assert active == 0
    assert cleanup.await_count >= 1


async def test_stop_cancels_an_initial_recovery_attempt(
    recovering_session, monkeypatch
):
    """Stopping during recovery cannot leave a pending native connection task."""
    session = recovering_session
    retrying, retired = asyncio.Event(), asyncio.Event()
    calls = 0

    async def connect():
        """Keep the retry pending until the session cancels its owned loop."""
        nonlocal calls
        calls += 1
        if calls == 1:
            raise MeshCoreConnectionError("offline")
        retrying.set()
        try:
            await asyncio.Event().wait()
        finally:
            retired.set()

    monkeypatch.setattr(session, "_connect_real", connect)
    await session.start(lambda _: None, recover_on_failure=True)
    await retrying.wait()
    await session.stop()
    assert retired.is_set()
    assert calls == 2
    assert not any(
        task.get_name() == "meshcore-recovery:recovering" and not task.done()
        for task in asyncio.all_tasks()
    )


async def test_missing_sdk_never_enters_background_recovery(
    recovering_session, monkeypatch
):
    """Missing code is a configuration failure, rather than an offline board."""
    session = recovering_session
    monkeypatch.setattr(session_module, "HAS_MESHCORE", False)
    with pytest.raises(MeshCoreConnectionError, match="not installed"):
        await session.start(lambda _: None, recover_on_failure=True)
    assert session._reconnect_task is None


async def test_stop_retires_initial_setup_before_it_can_connect(
    recovering_session, monkeypatch
):
    """stop() must cancel a separately running initial SDK setup owner."""
    session = recovering_session
    entered, retired = asyncio.Event(), asyncio.Event()

    async def connect():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            retired.set()

    monkeypatch.setattr(session, "_connect_real", connect)
    startup = asyncio.create_task(
        session.start(lambda _: None, recover_on_failure=True)
    )
    await entered.wait()
    await session.stop()
    assert retired.is_set()
    assert startup.done()
    with pytest.raises(asyncio.CancelledError):
        await startup
    assert session._initial_start_task is None
    assert not session.connected
    assert not session._started
    assert session._reconnect_task is None


async def test_stalled_stale_disconnect_is_bounded(recovering_session, monkeypatch):
    """A wedged SDK disconnect cannot trap recovery or shutdown forever."""
    from types import SimpleNamespace

    session = recovering_session
    monkeypatch.setattr(session_module, "_SDK_LIFECYCLE_TIMEOUT", 0.01)
    cancelled = asyncio.Event()

    async def disconnect():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    session._meshcore = SimpleNamespace(disconnect=disconnect)
    await asyncio.wait_for(session._cleanup_stale_sdk(), timeout=0.2)
    assert cancelled.is_set()
    assert session._meshcore is None


async def test_deadline_does_not_disable_background_recovery(
    recovering_session, monkeypatch
):
    """An expired setup deadline is a retryable failure, not a stop request."""
    session = recovering_session
    monkeypatch.setattr(session_module, "_CONNECT_ATTEMPT_TIMEOUT", 0.01)
    attempted = 0

    async def connect():
        nonlocal attempted
        attempted += 1
        if attempted == 1:
            await asyncio.Event().wait()
        session._diag.connected = True

    monkeypatch.setattr(session, "_connect_real", connect)
    await session.start(lambda _: None, recover_on_failure=True)
    try:
        assert not session._stop_requested
        assert await wait_until(lambda: session.connected)
        assert attempted >= 2
    finally:
        await session.stop()


async def test_ble_subscribe_error_never_logs_pin(
    recovering_session, monkeypatch, caplog
):
    """A post-connect subscription error must redact BLE secrets on retry."""
    from dataclasses import replace

    session = recovering_session
    session._config = replace(
        session._config,
        connection_type="ble",
        ble_address="test-device",
        ble_pin="123456",
    )

    async def fail():
        raise MeshCoreConnectionError("private pin 123456")

    monkeypatch.setattr(session, "_connect_real", fail)
    await session.start(lambda _: None, recover_on_failure=True)
    try:
        assert await wait_until(lambda: session.reconnect_attempts >= 1)
        assert "123456" not in caplog.text
        assert "123456" not in (session.last_error or "")
    finally:
        await session.stop()
