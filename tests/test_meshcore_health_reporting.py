"""Native readiness reports reflect connections without stale lifetime callbacks."""

import asyncio
import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.contracts.adapter import AdapterContext


def context(reports):
    """Attach a diagnostic sink without changing native send authority."""
    ctx = AdapterContext(
        "mc",
        AsyncMock(),
        logging.getLogger("health-report"),
        lambda: datetime.now(timezone.utc),
        asyncio.Event(),
    )
    # Older core contexts remain usable; their runtimes may omit this sink.
    ctx.report_health = reports.append
    return ctx


async def test_connections_update_reports_and_diagnostics_without_probe():
    """A recovered link clears degraded health before another health_check call."""
    reports = []
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc", connection_type="fake"))
    try:
        await adapter.start(context(reports))
        assert reports[-1].health == "healthy"
        session = adapter._session
        session._set_connected(False)
        assert reports[-1].health == "degraded"
        assert adapter.diagnostics()["health"] == "degraded"
        session._set_connected(True)
        assert reports[-1].health == "healthy"
        assert adapter.diagnostics()["health"] == "healthy"
        before = len(reports)
        session._set_connected(True)
        assert len(reports) == before
    finally:
        await adapter.stop()


async def test_retired_session_callback_cannot_report_for_restarted_adapter():
    """Native callbacks from a stopped session cannot publish a newer lifetime's health."""
    reports = []
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc", connection_type="fake"))
    await adapter.start(context(reports))
    old_callback = adapter._session._on_connection_change
    await adapter.stop()
    try:
        await adapter.start(context(reports))
        adapter._session._set_connected(False)
        before = len(reports)
        old_callback(True)
        assert len(reports) == before
        assert adapter.diagnostics()["health"] == "degraded"
    finally:
        await adapter.stop()


async def test_failed_diagnostic_sink_does_not_fail_native_start():
    """An observer error cannot turn a healthy connection into a startup failure."""
    ctx = context([])

    def refused(info):
        """Model an unavailable diagnostic consumer."""
        raise RuntimeError("consumer failed")

    ctx.report_health = refused
    adapter = MeshCoreAdapter(MeshCoreConfig(adapter_id="mc", connection_type="fake"))
    try:
        await adapter.start(ctx)
        assert (await adapter.health_check()).health == "healthy"
        adapter._session._set_connected(False)
        assert (await adapter.health_check()).health == "degraded"
    finally:
        await adapter.stop()
