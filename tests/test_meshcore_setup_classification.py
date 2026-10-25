"""Configuration and SDK contract failures cannot self-heal through reconnect."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from medre.adapters.meshcore.errors import MeshCoreConnectionError
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig


@pytest.fixture
def session(monkeypatch):
    """Disable native SDK access and backoff for classification-only cases."""
    import medre.adapters.meshcore.session as module

    monkeypatch.setattr(module, "HAS_MESHCORE", True)
    monkeypatch.setattr(module, "_RECONNECT_BASE_DELAY", 0.0)
    monkeypatch.setattr(module, "_RECONNECT_JITTER_FRACTION", 0.0)
    return MeshCoreSession(
        MeshCoreConfig(
            adapter_id="contract", connection_type="tcp", host="offline.invalid"
        ),
        "contract",
    )


@pytest.mark.parametrize(
    "error", [ValueError, TypeError, AttributeError, ImportError, NotImplementedError]
)
async def test_initial_contract_error_never_launches_recovery(
    session, monkeypatch, error
):
    """An invalid native call propagates instead of reporting a recoverable start."""
    attempt = AsyncMock(side_effect=error("contract failure"))
    monkeypatch.setattr(session, "_connect_real", attempt)
    with pytest.raises(error):
        await session.start(lambda _: None, recover_on_failure=True)
    assert attempt.await_count == 1
    assert session._reconnect_task is None
    assert not session._started
    assert session._message_callback is None


async def test_wrapped_permanent_failure_stops_reconnect(session, monkeypatch):
    """Preserve a wrapper's classification through the background recovery owner."""
    attempt = AsyncMock(
        side_effect=MeshCoreConnectionError("invalid SDK contract", transient=False)
    )
    monkeypatch.setattr(session, "_connect_real", attempt)
    await session._reconnect_loop()
    assert attempt.await_count == 1
    assert not session.reconnecting
    assert "invalid SDK contract" in session.last_error


async def test_ble_factory_contract_failure_is_not_retried(session):
    """Unsupported factory arguments fail immediately without discovery or sleeps."""
    create = AsyncMock(side_effect=TypeError("unsupported keyword"))
    module = SimpleNamespace(MeshCore=SimpleNamespace(create_ble=create))
    with pytest.raises(MeshCoreConnectionError) as failure:
        await session._create_ble_with_retries(module, "node-name", None)
    assert failure.value.transient is False
    assert create.await_count == 1


async def test_tcp_factory_preserves_contract_failure(session, monkeypatch):
    """The native constructor wrapper must retain permanent failure evidence."""
    import sys

    create = AsyncMock(side_effect=TypeError("unsupported keyword"))
    monkeypatch.setitem(
        sys.modules,
        "meshcore",
        SimpleNamespace(MeshCore=SimpleNamespace(create_tcp=create)),
    )
    with pytest.raises(MeshCoreConnectionError) as failure:
        await session.start(lambda _: None, recover_on_failure=True)
    assert failure.value.transient is False
    assert create.await_count == 1
    assert session._reconnect_task is None
