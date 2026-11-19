"""Startup-info provider: duck-typed emission of held node-identity facts.

Covers:
- ``startup_info`` is discovered duck-typed on the adapter object.
- Facts come only from already-held state (self-info snapshot and the
  configured identity fallback): device name, public-key prefix, radio
  frequency.
- Facts not held at ready (firmware build, battery) are omitted, never
  probed.
- The public-key prefix renders in full through core's redaction
  boundary (published mesh identifier, not key material).
- Pure read: calling the hook mutates no adapter or session state and
  is safe on a worker thread, matching core's off-loop invocation.
- No contact or peer-table material is reported.

Core-side collection/rendering symbols are imported in tests only; the
adapter source must not import them.  Fake connection mode only — no
device I/O.  See core ``docs/spec/adapter-startup-info.md``.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

from medre.core.contracts.adapter import AdapterContext
from medre.core.startup_info import (
    collect_startup_info,
    render_startup_info_block,
    startup_info_callable,
)

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.config.adapters.meshcore import MeshCoreConfig

_EMITTED_AT = "2026-11-19T00:00:00+00:00"


# -- Helpers ---------------------------------------------------------------


def _make_config(**overrides: object) -> MeshCoreConfig:
    defaults: dict[str, Any] = {"adapter_id": "mc-startup-info"}
    defaults.update(overrides)
    return MeshCoreConfig(**defaults)


def _make_ctx(adapter_id: str = "mc-startup-info") -> AdapterContext:
    return AdapterContext(
        adapter_id=adapter_id,
        publish_inbound=AsyncMock(),
        logger=__import__("logging").getLogger("test.startup_info"),
        clock=lambda: datetime.now(timezone.utc),
        shutdown_event=asyncio.Event(),
    )


async def _started_adapter(config: MeshCoreConfig) -> MeshCoreAdapter:
    adapter = MeshCoreAdapter(config)
    await adapter.start(_make_ctx(config.adapter_id))
    return adapter


# -- Duck-typed discovery ---------------------------------------------------


def test_startup_info_symbol_is_discoverable() -> None:
    """Core's getattr discovery finds a callable on the adapter object."""
    adapter = MeshCoreAdapter(_make_config())
    discovered = startup_info_callable(adapter)
    assert discovered is not None
    assert callable(discovered)
    assert discovered.__name__ == "startup_info"


# -- Held-fact formatting ---------------------------------------------------


async def test_reports_held_identity_prefix_and_frequency() -> None:
    """Announce state held by the session formats into two ordered rows."""
    adapter = await _started_adapter(_make_config())
    try:
        assert adapter._session is not None
        adapter._session._capture_self_info(
            {
                "name": "Ridge Relay",
                "public_key": "aabbccddeeff00112233445566778899",
                "freq": 868.0,
            }
        )
        assert adapter.startup_info() == {
            "rows": [
                [
                    {"name": "device_name", "value": "Ridge Relay"},
                    {
                        "name": "public_key_prefix",
                        "value": "aabbccddeeff",
                        "label": "pubkey",
                    },
                ],
                [
                    {
                        "name": "radio_freq_mhz",
                        "value": 868.0,
                        "label": "radio",
                    }
                ],
            ]
        }
    finally:
        await adapter.stop()


async def test_collect_keeps_rows_and_public_key_in_full() -> None:
    """Core's collection preserves row order and renders the published
    public-key prefix in full (no generic long-key redaction)."""
    adapter = await _started_adapter(_make_config())
    try:
        assert adapter._session is not None
        adapter._session._capture_self_info(
            {
                "name": "Ridge Relay",
                "public_key": "aabbccddeeff00112233445566778899",
                "freq": 868.0,
            }
        )
        info = await collect_startup_info(
            "mc-startup-info", "meshcore", adapter, emitted_at=_EMITTED_AT
        )
        assert info is not None
        assert [(f.name, f.value, f.row, f.label) for f in info.fields] == [
            ("device_name", "Ridge Relay", 0, None),
            ("public_key_prefix", "aabbccddeeff", 0, "pubkey"),
            ("radio_freq_mhz", "868.0", 1, "radio"),
        ]
        assert render_startup_info_block("meshcore.mc-startup-info  ready", info) == (
            "meshcore.mc-startup-info  ready\n"
            "  Ridge Relay · pubkey aabbccddeeff\n"
            "  radio 868.0"
        )
    finally:
        await adapter.stop()


async def test_configured_identity_used_without_self_info() -> None:
    """Fake mode holds no announce snapshot; the configured identity is
    the only name fact and no prefix or frequency fact is offered."""
    adapter = await _started_adapter(
        _make_config(connection_type="fake", identity="Configured Node")
    )
    try:
        assert adapter.startup_info() == {
            "rows": [[{"name": "device_name", "value": "Configured Node"}]]
        }
    finally:
        await adapter.stop()


async def test_omits_facts_not_held_at_ready() -> None:
    """Without announce state or a configured identity nothing is
    offered — and core collects no record.  Firmware build and battery
    state are not held and are never probed."""
    adapter = await _started_adapter(_make_config())
    try:
        assert adapter.startup_info() == {"rows": []}
        info = await collect_startup_info(
            "mc-startup-info", "meshcore", adapter, emitted_at=_EMITTED_AT
        )
        assert info is None
    finally:
        await adapter.stop()


async def test_session_device_name_wins_over_configured_identity() -> None:
    """The self-info device name is the primary node identity; the
    configured identity is only the fallback."""
    adapter = await _started_adapter(_make_config(identity="Configured Node"))
    try:
        assert adapter._session is not None
        adapter._session._capture_self_info({"name": "Device Name"})
        assert adapter.startup_info() == {
            "rows": [[{"name": "device_name", "value": "Device Name"}]]
        }
    finally:
        await adapter.stop()


async def test_never_started_adapter_reports_configured_identity_only() -> None:
    """Before start no session exists; only configured held state is
    reported and the call never raises."""
    adapter = MeshCoreAdapter(_make_config(identity="Configured Node"))
    assert adapter.startup_info() == {
        "rows": [[{"name": "device_name", "value": "Configured Node"}]]
    }


# -- Pure-read and invocation model -----------------------------------------


async def test_hook_is_pure_read() -> None:
    """Calling startup_info mutates no adapter or session state."""
    adapter = await _started_adapter(_make_config())
    try:
        assert adapter._session is not None
        adapter._session._capture_self_info(
            {"name": "node", "public_key": "aabbccddeeff00112233445566778899"}
        )
        before_adapter = adapter.diagnostics()
        before_session = adapter._session.diagnostics()
        adapter.startup_info()
        assert adapter.diagnostics() == before_adapter
        assert adapter._session.diagnostics() == before_session
    finally:
        await adapter.stop()


async def test_hook_runs_on_a_worker_thread_like_core_invocation() -> None:
    """Core invokes sync hooks off-loop; the hook must be safe there."""
    adapter = await _started_adapter(_make_config(identity="Configured Node"))
    try:
        off_loop = await asyncio.to_thread(adapter.startup_info)
        assert off_loop == adapter.startup_info()
    finally:
        await adapter.stop()


async def test_startup_info_reports_no_contact_material() -> None:
    """Startup info carries the operator's own identity only; contact
    or peer-table state the session holds is never surfaced."""
    adapter = await _started_adapter(_make_config())
    try:
        assert adapter._session is not None
        adapter._session._capture_self_info(
            {"name": "node", "public_key": "aabbccddeeff00112233445566778899"}
        )
        adapter._session._diag.known_contact_count = 3
        assert adapter.startup_info() == {
            "rows": [
                [
                    {"name": "device_name", "value": "node"},
                    {
                        "name": "public_key_prefix",
                        "value": "aabbccddeeff",
                        "label": "pubkey",
                    },
                ]
            ]
        }
    finally:
        await adapter.stop()


async def test_refresh_publishes_one_complete_identity_snapshot() -> None:
    adapter = await _started_adapter(_make_config())
    session = adapter._session
    assert session is not None
    session._capture_self_info(
        {"name": "Old", "public_key": "aabbccddeeff0123", "freq": 868.0}
    )
    previous = adapter.startup_info()
    paused = threading.Event()
    resume = threading.Event()

    class PausedInfo(dict):
        def get(self, key, default=None):
            if key == "public_key":
                paused.set()
                assert resume.wait(5), "refresh release not signalled"
            return super().get(key, default)

    refresh = asyncio.create_task(
        asyncio.to_thread(
            session._capture_self_info,
            PausedInfo(name="New", public_key="112233445566abcd", freq=915.0),
        )
    )
    try:
        assert await asyncio.to_thread(paused.wait, 3)
        # Parsing is outside the publication lock; the old snapshot remains coherent.
        assert await asyncio.to_thread(adapter.startup_info) == previous
        resume.set()
        await refresh
        assert await asyncio.to_thread(adapter.startup_info) == {
            "rows": [
                [
                    {"name": "device_name", "value": "New"},
                    {
                        "name": "public_key_prefix",
                        "value": "112233445566",
                        "label": "pubkey",
                    },
                ],
                [{"name": "radio_freq_mhz", "value": 915.0, "label": "radio"}],
            ]
        }
        session._capture_self_info({"name": "Partial"})
        assert session.self_info_snapshot() == ("Partial", None, None)
    finally:
        resume.set()
        await asyncio.gather(refresh, return_exceptions=True)
        await adapter.stop()
    assert session.self_info_snapshot() == (None, None, None)


async def test_startup_info_does_not_read_a_replacement_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = await _started_adapter(_make_config(identity="Fallback"))
    session = adapter._session
    assert session is not None
    session._capture_self_info(
        {"name": "Old", "public_key": "aabbccddeeff0123", "freq": 868.0}
    )
    expected = adapter.startup_info()
    original = session.self_info_snapshot

    def snapshot():
        result = original()
        adapter._session = None
        return result

    monkeypatch.setattr(session, "self_info_snapshot", snapshot)
    try:
        assert adapter.startup_info() == expected
    finally:
        adapter._session = session
        monkeypatch.setattr(session, "self_info_snapshot", original)
        await adapter.stop()


@pytest.mark.parametrize("malformed", ["not-a-key-token", "abcd", "gggggggggggg"])
async def test_invalid_key_and_frequency_are_not_published(malformed: str) -> None:
    adapter = await _started_adapter(_make_config())
    try:
        session = adapter._session
        assert session is not None
        session._capture_self_info(
            {"name": "Node", "public_key": malformed, "freq": 10**400}
        )
        assert session.self_info_snapshot() == ("Node", None, None)
        assert adapter.startup_info() == {
            "rows": [[{"name": "device_name", "value": "Node"}]]
        }
    finally:
        await adapter.stop()
