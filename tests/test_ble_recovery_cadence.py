"""Recovery attempts remain paced and distinguish a host outage from a missing node."""

import sys
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from medre.adapters.meshcore.errors import MeshCoreConnectionError
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig
from medre_meshcore_tests.helpers.meshcore_session import build_mock_meshcore_module


def session():
    """Build a BLE owner without opening a scanner or native connection."""
    return MeshCoreSession(
        MeshCoreConfig(
            adapter_id="recovery",
            connection_type="ble",
            ble_address="AA:BB:CC:DD:EE:FF",
        ),
        "recovery",
    )


async def test_background_recovery_does_not_multiply_native_attempts(monkeypatch):
    """One backoff interval permits one SDK call, rather than a burst of three."""
    owner = session()
    owner._diag.reconnecting = True
    factory = AsyncMock(side_effect=OSError("device is out of range"))
    sdk = SimpleNamespace(MeshCore=SimpleNamespace(create_ble=factory))
    monkeypatch.setattr("medre.adapters.meshcore.session.asyncio.sleep", AsyncMock())
    monkeypatch.setattr(owner, "_find_ble_device", AsyncMock(return_value=None))
    monkeypatch.setattr(owner, "_disconnect_stale_ble_client", AsyncMock())
    with pytest.raises(MeshCoreConnectionError):
        await owner._create_ble_with_retries(sdk, owner._config.ble_address, None)
    assert factory.await_count == 1


async def test_powered_off_controller_is_not_an_address_fallback(monkeypatch):
    """A typed host-unavailable scan error reaches recovery without trying the node."""

    class BleakBluetoothNotAvailableError(Exception):
        """Model Bleak's SDK error without importing its hardware backend."""

    scan = AsyncMock(side_effect=BleakBluetoothNotAvailableError("powered off"))
    monkeypatch.setitem(
        sys.modules,
        "bleak",
        SimpleNamespace(
            BleakScanner=SimpleNamespace(find_device_by_filter=scan),
        ),
    )
    owner = session()
    owner._config = replace(owner._config, ble_pin="123456")
    sdk, _ = build_mock_meshcore_module()
    monkeypatch.setattr("medre.adapters.meshcore.session.HAS_MESHCORE", True)
    monkeypatch.setitem(sys.modules, "meshcore", sdk)
    monkeypatch.setattr(owner, "_disconnect_stale_ble_client", AsyncMock())
    monkeypatch.setattr("medre.adapters.meshcore.session.asyncio.sleep", AsyncMock())
    with pytest.raises(MeshCoreConnectionError, match="Bluetooth unavailable"):
        await owner._connect_real()
    scan.assert_awaited_once()
    sdk.MeshCore.create_ble.assert_not_awaited()
    assert "enable the controller" in owner.last_error
    assert "123456" not in owner.last_error


@pytest.mark.parametrize("recovering", [False, True])
async def test_pairing_pin_is_used_only_for_initial_setup(monkeypatch, recovering):
    """Automatic retries reuse existing bonds without initiating PIN-based pairing."""
    owner = session()
    owner._diag.reconnecting = recovering
    sdk, _ = build_mock_meshcore_module()
    await owner._create_ble_with_retries(
        sdk, owner._config.ble_address, None, pin="123456"
    )
    kwargs = sdk.MeshCore.create_ble.await_args.kwargs
    assert ("pin" in kwargs) is (not recovering)


async def test_factory_controller_failure_keeps_safe_guidance_with_a_pin(monkeypatch):
    """A late host outage must retain actionable text without exposing SDK error args."""

    class BleakBluetoothNotAvailableError(Exception):
        """Model a typed backend outage whose details may contain private values."""

    owner = session()
    owner._config = replace(owner._config, ble_pin="123456")
    owner._diag.reconnecting = True
    sdk, _ = build_mock_meshcore_module()
    sdk.MeshCore.create_ble.side_effect = BleakBluetoothNotAvailableError(
        "private 123456"
    )
    with pytest.raises(MeshCoreConnectionError) as failure:
        await owner._create_ble_with_retries(
            sdk, owner._config.ble_address, None, pin="123456"
        )
    public = owner._sanitize_ble_exc(failure.value)
    assert "enable the controller" in public
    assert "123456" not in public
    sdk.MeshCore.create_ble.assert_awaited_once()


async def test_outage_backoff_has_a_floor_and_grows_to_five_minutes(monkeypatch):
    """Simulate a long outage without sleeping or opening any native transports."""
    owner = session()
    delays = []

    async def wait(delay):
        """Capture the actual recovery-loop delay instead of waiting in real time."""
        delays.append(delay)

    monkeypatch.setattr("medre.adapters.meshcore.session.asyncio.sleep", wait)
    monkeypatch.setattr(
        "medre.adapters.meshcore.session.random.uniform", lambda _a, _b: 0
    )
    monkeypatch.setattr("medre.adapters.meshcore.session._RECONNECT_MAX_ATTEMPTS", 10)
    monkeypatch.setattr(
        owner,
        "_connection_attempt",
        AsyncMock(side_effect=MeshCoreConnectionError("offline")),
    )
    await owner._reconnect_loop()
    assert delays == [10, 20, 40, 80, 160, 300, 300, 300, 300, 300]
