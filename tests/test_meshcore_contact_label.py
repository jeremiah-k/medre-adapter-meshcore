"""Tests for MeshCore known-contact label enrichment.

Covers the enrichment pipeline that resolves a known contact's
advertised name from the session's local contacts store and injects it
into native metadata so the projection maps it into
``source_sender_label`` / ``source_sender_short_label``.

Enrichment chain:

    session.resolve_contact_label(pubkey_prefix)
        -> adapter._resolve_contact_label(sender_id)
        -> codec.decode(packet, contact_label=...)
        -> native_meta["meshcore.contact_label"]
        -> project_meshcore_attribution -> source_sender_label

All tests are unit-level; no network calls, no SDK dependency required.

Evidence tier: ``fake_pipeline`` — no real radio or SDK connection.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from medre.adapters.meshcore.adapter import MeshCoreAdapter
from medre.adapters.meshcore.attribution import project_meshcore_attribution
from medre.adapters.meshcore.codec import MeshCoreCodec
from medre.adapters.meshcore.renderer import MeshCoreRenderer
from medre.adapters.meshcore.session import MeshCoreSession
from medre.config.adapters.meshcore import MeshCoreConfig
from medre.core.events import CanonicalEvent, EventMetadata, NativeMetadata
from medre.core.rendering.attribution import (
    RelayAttribution,
    format_relay_prefix,
)
from medre.core.rendering.renderer import RenderingContext, RenderingResult
from medre_meshcore_tests.helpers.native_metadata import meshcore_native_data

# ===================================================================
# Helpers
# ===================================================================


def _make_fake_config(adapter_id: str = "test_meshcore") -> MeshCoreConfig:
    """Return a minimal fake-mode MeshCoreConfig."""
    return MeshCoreConfig(adapter_id=adapter_id, connection_type="fake")


def _make_text_packet(
    sender: str = "a1b2c3",
    channel: int | None = 0,
    text: str = "hello",
    packet_id: int = 12345,
) -> dict[str, Any]:
    """Build a minimal MeshCore text packet dict."""
    pkt: dict[str, Any] = {
        "text": text,
        "pubkey_prefix": sender,
        "sender_timestamp": packet_id,
        "type": "CHAN",
        "txt_type": 0,
    }
    if channel is not None:
        pkt["channel_idx"] = channel
    return pkt


def _make_fake_meshcore_with_contacts(
    contacts_by_prefix: dict[str, dict[str, Any]] | None = None,
) -> MagicMock:
    """Build a fake SDK client whose get_contact_by_key_prefix returns contacts.

    Parameters
    ----------
    contacts_by_prefix:
        Maps a pubkey-prefix substring to the contact dict that
        ``get_contact_by_key_prefix`` returns for that prefix.
    """
    mc = MagicMock()
    contacts_by_prefix = contacts_by_prefix or {}

    def _get_by_prefix(prefix: str) -> dict[str, Any] | None:
        for key_prefix, contact in contacts_by_prefix.items():
            if key_prefix.lower().startswith(prefix.lower()):
                return contact
        return None

    mc.get_contact_by_key_prefix = _get_by_prefix
    return mc


# ===================================================================
# Session.resolve_contact_label
# ===================================================================


def test_resolve_contact_label_returns_adv_name() -> None:
    """A known contact's adv_name is returned for a matching prefix."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    session._meshcore = _make_fake_meshcore_with_contacts(
        {"deadbeef": {"public_key": "deadbeefcafe", "adv_name": "EA1ABC"}}
    )
    assert session.resolve_contact_label("dead") == "EA1ABC"


def test_resolve_contact_label_strips_whitespace() -> None:
    """Surrounding whitespace in adv_name is stripped."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    session._meshcore = _make_fake_meshcore_with_contacts(
        {"aabbcc": {"public_key": "aabbccdd", "adv_name": "  Node1  "}}
    )
    assert session.resolve_contact_label("aabbcc") == "Node1"


def test_resolve_contact_label_none_when_no_sdk() -> None:
    """Returns None when the SDK client is not initialised (fake mode)."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    # _meshcore stays None in fake mode.
    assert session.resolve_contact_label("deadbeef") is None


def test_resolve_contact_label_none_for_empty_prefix() -> None:
    """An empty prefix yields None without touching the SDK."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    session._meshcore = _make_fake_meshcore_with_contacts({"aa": {"adv_name": "Alice"}})
    assert session.resolve_contact_label("") is None


def test_resolve_contact_label_none_when_contact_unknown() -> None:
    """Returns None when the prefix does not match any known contact."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    session._meshcore = _make_fake_meshcore_with_contacts(
        {"known": {"adv_name": "Known"}}
    )
    assert session.resolve_contact_label("unknown") is None


def test_resolve_contact_label_none_when_adv_name_empty() -> None:
    """An empty adv_name yields None, not an empty string."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    session._meshcore = _make_fake_meshcore_with_contacts({"aa": {"adv_name": ""}})
    assert session.resolve_contact_label("aa") is None


def test_resolve_contact_label_none_when_adv_name_whitespace_only() -> None:
    """A whitespace-only adv_name yields None after stripping."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    session._meshcore = _make_fake_meshcore_with_contacts({"aa": {"adv_name": "   "}})
    assert session.resolve_contact_label("aa") is None


def test_resolve_contact_label_none_when_adv_name_non_string() -> None:
    """A non-string adv_name yields None without raising."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    session._meshcore = _make_fake_meshcore_with_contacts({"aa": {"adv_name": 12345}})
    assert session.resolve_contact_label("aa") is None


def test_resolve_contact_label_none_when_getter_raises() -> None:
    """An exception from the SDK getter yields None, never raises."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    mc = MagicMock()

    def _boom(prefix: str) -> dict[str, Any]:
        raise RuntimeError("SDK internal error")

    mc.get_contact_by_key_prefix = _boom
    session._meshcore = mc
    assert session.resolve_contact_label("aa") is None


def test_resolve_contact_label_none_when_no_getter_method() -> None:
    """Returns None when the SDK client lacks get_contact_by_key_prefix."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    mc = MagicMock(spec=[])  # No methods.
    session._meshcore = mc
    assert session.resolve_contact_label("aa") is None


def test_resolve_contact_label_none_when_result_not_dict() -> None:
    """Returns None when get_contact_by_key_prefix returns a non-dict."""
    session = MeshCoreSession(
        config=_make_fake_config(),
        adapter_id="test",
    )
    mc = MagicMock()
    mc.get_contact_by_key_prefix = MagicMock(return_value="not a dict")
    session._meshcore = mc
    assert session.resolve_contact_label("aa") is None


# ===================================================================
# Codec decode with contact_label
# ===================================================================


def test_codec_decode_includes_contact_label_in_native_meta() -> None:
    """decode() stores contact_label in native metadata."""
    codec = MeshCoreCodec("test_meshcore", _make_fake_config())
    packet = _make_text_packet(sender="deadbeef")
    event = codec.decode(packet, contact_label="EA1ABC")
    assert event.metadata.native is not None
    assert event.metadata.native.data["meshcore"]["contact_label"] == "EA1ABC"
    assert event.metadata.native.data["meshcore"]["contact_short_label"] is None


def test_codec_decode_contact_label_defaults_none() -> None:
    """decode() without contact_label stores None."""
    codec = MeshCoreCodec("test_meshcore", _make_fake_config())
    packet = _make_text_packet(sender="deadbeef")
    event = codec.decode(packet)
    assert event.metadata.native is not None
    assert event.metadata.native.data["meshcore"]["contact_label"] is None
    assert event.metadata.native.data["meshcore"]["contact_short_label"] is None


def test_codec_decode_with_explicit_short_label() -> None:
    """decode() stores both contact_label and contact_short_label."""
    codec = MeshCoreCodec("test_meshcore", _make_fake_config())
    packet = _make_text_packet(sender="deadbeef")
    event = codec.decode(
        packet,
        contact_label="Base Station",
        contact_short_label="BASE",
    )
    assert event.metadata.native is not None
    assert event.metadata.native.data["meshcore"]["contact_label"] == "Base Station"
    assert event.metadata.native.data["meshcore"]["contact_short_label"] == "BASE"


def test_codec_decode_preserves_sender_id_with_contact_label() -> None:
    """source_transport_id stays the pubkey prefix even with a contact label."""
    codec = MeshCoreCodec("test_meshcore", _make_fake_config())
    packet = _make_text_packet(sender="a1b2c3")
    event = codec.decode(packet, contact_label="Alice")
    assert event.source_transport_id == "a1b2c3"


# ===================================================================
# Adapter._resolve_contact_label
# ===================================================================


def test_adapter_resolve_contact_label_delegates_to_session() -> None:
    """The adapter delegates contact resolution to the session."""
    config = _make_fake_config()
    adapter = MeshCoreAdapter(config)
    session = MagicMock()
    session.resolve_contact_label = MagicMock(return_value="EA1ABC")
    adapter._session = session  # type: ignore[assignment]
    assert adapter._resolve_contact_label("deadbeef") == "EA1ABC"
    session.resolve_contact_label.assert_called_once_with("deadbeef")


def test_adapter_resolve_contact_label_none_without_session() -> None:
    """Returns None when the session is not set (fake mode)."""
    config = _make_fake_config()
    adapter = MeshCoreAdapter(config)
    adapter._session = None  # type: ignore[assignment]
    assert adapter._resolve_contact_label("deadbeef") is None


def test_adapter_resolve_contact_label_none_for_empty_sender() -> None:
    """Returns None when sender_id is empty or None."""
    config = _make_fake_config()
    adapter = MeshCoreAdapter(config)
    session = MagicMock()
    session.resolve_contact_label = MagicMock(return_value="ShouldNotBeCalled")
    adapter._session = session  # type: ignore[assignment]
    assert adapter._resolve_contact_label("") is None
    assert adapter._resolve_contact_label(None) is None
    session.resolve_contact_label.assert_not_called()


# ===================================================================
# Renderer prefix end-to-end (projection -> RelayAttribution -> format)
# ===================================================================


def test_renderer_sender_shows_contact_label() -> None:
    """{sender} renders the contact label when enriched."""
    projected = project_meshcore_attribution(
        {
            "meshcore": {
                "schema_version": 2,
                "pubkey_prefix": "a1b2c3",
                "channel": 0,
                "packet_id": 42,
                "contact_label": "EA1ABC",
            },
        }
    )
    attr = RelayAttribution(
        source_adapter_id="meshcore-node",
        source_platform="meshcore",
        **projected,
    )
    result = format_relay_prefix("[{sender}] ", attr)
    assert result.rendered_prefix == "[EA1ABC] "
    assert result.formatting_error is None


def test_renderer_sender_empty_without_contact() -> None:
    """{sender} renders empty when no contact label is available."""
    projected = project_meshcore_attribution(
        {
            "meshcore": {
                "schema_version": 2,
                "pubkey_prefix": "a1b2c3",
                "channel": 0,
            },
        }
    )
    attr = RelayAttribution(
        source_platform="meshcore",
        **projected,
    )
    result = format_relay_prefix("[{sender}] ", attr)
    assert result.rendered_prefix == "[] "
    assert "sender" in result.missing_variables


def test_renderer_sender_id_shows_pubkey_prefix() -> None:
    """{sender_id} exposes the pubkey prefix regardless of contact label."""
    projected = project_meshcore_attribution(
        {
            "meshcore": {
                "schema_version": 2,
                "pubkey_prefix": "deadbeef",
                "channel": 0,
                "contact_label": "Alice",
            },
        }
    )
    attr = RelayAttribution(source_platform="meshcore", **projected)
    result = format_relay_prefix("{sender_id}: ", attr)
    assert result.rendered_prefix == "deadbeef: "


def test_renderer_sender_short_shows_first_token() -> None:
    """{sender_short} renders the first token of the contact label."""
    projected = project_meshcore_attribution(
        {
            "meshcore": {
                "schema_version": 2,
                "pubkey_prefix": "pk",
                "channel": 0,
                "contact_label": "Base Station Alpha",
            },
        }
    )
    attr = RelayAttribution(source_platform="meshcore", **projected)
    result = format_relay_prefix("[{sender_short}] ", attr)
    assert result.rendered_prefix == "[Base] "


def test_renderer_combined_sender_and_sender_id() -> None:
    """{sender} and {sender_id} render independently in one template."""
    projected = project_meshcore_attribution(
        {
            "meshcore": {
                "schema_version": 2,
                "pubkey_prefix": "aabbcc",
                "channel": 1,
                "contact_label": "Node1",
            },
        }
    )
    attr = RelayAttribution(source_platform="meshcore", **projected)
    result = format_relay_prefix("{sender} ({sender_id}): ", attr)
    assert result.rendered_prefix == "Node1 (aabbcc): "


# ===================================================================
# Renderer prefix truncation (declared constrained-display affordance)
#
# The renderer declares sender / sender_short / origin_label truncatable
# so custom relay_prefix templates accept {nameN} suffixes (first N
# Unicode code points, N >= 1).  The default template is "" — no
# automatic prefix — so the declaration only widens custom templates.
# ===================================================================


def _truncation_event(contact_label: str | None) -> CanonicalEvent:
    """Build a canonical message carrying MeshCore contact metadata."""
    return CanonicalEvent(
        event_id="truncation-event",
        event_kind="message.created",
        schema_version=1,
        timestamp=datetime.now(UTC),
        source_adapter="source",
        source_transport_id="peer",
        source_channel_id="0",
        parent_event_id=None,
        lineage=(),
        relations=(),
        payload={"text": "body"},
        metadata=EventMetadata(
            native=NativeMetadata(
                data=meshcore_native_data(
                    pubkey_prefix="a1b2c3",
                    channel=0,
                    contact_label=contact_label,
                )
            )
        ),
    )


async def _render_truncation(
    config: MeshCoreConfig,
    event: CanonicalEvent,
    *,
    source_attribution: dict[str, Any] | None = None,
) -> RenderingResult:
    return await MeshCoreRenderer(
        configs={"mc": config}, source_attribution=source_attribution or {}
    ).render(
        event,
        RenderingContext(
            target_adapter="mc",
            target_channel="0",
            target_platform="meshcore",
            delivery_strategy="direct",
        ),
    )


async def test_relay_prefix_truncation_renders_first_n_code_points() -> None:
    """{sender5} resolves to the first five code points of the sender label."""
    config = MeshCoreConfig(adapter_id="mc", relay_prefix="{sender5}[MC] ")
    result = await _render_truncation(config, _truncation_event("TadChesapeake"))
    assert result.payload["text"] == "TadCh[MC] body"
    assert result.metadata["relay_prefix_variables_used"] == ("sender5",)
    assert result.metadata["relay_prefix_formatting_error"] is None


async def test_relay_prefix_truncation_counts_code_points_not_bytes() -> None:
    """The suffix counts Unicode code points, never UTF-8 bytes."""
    config = MeshCoreConfig(adapter_id="mc", relay_prefix="{sender2}: ")
    result = await _render_truncation(config, _truncation_event("\U0001f47eIngrid"))
    assert result.payload["text"] == "\U0001f47eI: body"


async def test_relay_prefix_truncation_zero_suffix_stays_literal() -> None:
    """{sender0} is unknown (N >= 1) and passes through unchanged."""
    config = MeshCoreConfig(adapter_id="mc", relay_prefix="{sender0}[MC] ")
    result = await _render_truncation(config, _truncation_event("TadChesapeake"))
    assert result.payload["text"].startswith("{sender0}[MC] ")
    assert result.metadata["relay_prefix_unknown_variables"] == ("sender0",)
    assert result.metadata["relay_prefix_formatting_error"] is not None


async def test_relay_prefix_undeclared_truncation_stem_stays_literal() -> None:
    """A numeric suffix on an undeclared stem passes through as a literal.

    ``sender_id`` is the fixed-width pubkey prefix — it is not declared
    truncatable for this constrained-display transport.
    """
    config = MeshCoreConfig(adapter_id="mc", relay_prefix="{sender_id5}[MC] ")
    result = await _render_truncation(config, _truncation_event("TadChesapeake"))
    assert result.payload["text"].startswith("{sender_id5}[MC] ")
    assert result.metadata["relay_prefix_unknown_variables"] == ("sender_id5",)


async def test_relay_prefix_sender_short_truncation() -> None:
    """{sender_short3} bounds the projected short label."""
    config = MeshCoreConfig(adapter_id="mc", relay_prefix="{sender_short3}> ")
    result = await _render_truncation(config, _truncation_event("Base Station Alpha"))
    assert result.payload["text"] == "Bas> body"


async def test_relay_prefix_origin_label_truncation() -> None:
    """{origin_label5} bounds the adapter-registered origin label."""
    config = MeshCoreConfig(adapter_id="mc", relay_prefix="{origin_label5}> ")
    result = await _render_truncation(
        config,
        _truncation_event(None),
        source_attribution={"source": SimpleNamespace(origin_label="East Ridge")},
    )
    assert result.payload["text"] == "East > body"


async def test_relay_prefix_empty_template_disables_prefix() -> None:
    """The "" default renders no prefix and no prefix diagnostic metadata."""
    config = MeshCoreConfig(adapter_id="mc", relay_prefix="")
    result = await _render_truncation(config, _truncation_event("TadChesapeake"))
    assert result.payload["text"] == "body"
    assert "relay_prefix_template" not in result.metadata


# ===================================================================
# Adapter simulate_inbound enrichment integration
# ===================================================================


async def test_simulate_inbound_enriches_with_contact_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """simulate_inbound resolves and injects a contact label.

    Constructs a real MeshCoreAdapter with a manually-injected mock
    session so that _resolve_contact_label returns a known label.
    The codec then stores meshcore.contact_label in native metadata.
    """

    config = _make_fake_config()
    adapter = MeshCoreAdapter(config)

    # Inject a mock session that returns a known contact label.
    session = MagicMock()
    session.resolve_contact_label = MagicMock(return_value="EA1ABC")
    adapter._session = session  # type: ignore[assignment]

    # Capture the decoded event by patching publish_inbound.
    captured: list[Any] = []

    async def _capture(event: Any) -> None:
        captured.append(event)

    monkeypatch.setattr(adapter, "publish_inbound", _capture)

    # Simulate a started adapter.
    adapter._started = True
    adapter.ctx = MagicMock()  # type: ignore[assignment]

    packet = _make_text_packet(sender="deadbeef", text="hello mesh")
    await adapter.simulate_inbound(packet)

    assert len(captured) == 1
    event = captured[0]
    assert event.metadata.native is not None
    assert event.metadata.native.data["meshcore"]["contact_label"] == "EA1ABC"

    # The session was queried with the pubkey prefix.
    session.resolve_contact_label.assert_called_once_with("deadbeef")


async def test_simulate_inbound_no_contact_when_session_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """simulate_inbound stores None contact label without a session."""

    config = _make_fake_config()
    adapter = MeshCoreAdapter(config)
    adapter._session = None  # type: ignore[assignment]

    captured: list[Any] = []

    async def _capture(event: Any) -> None:
        captured.append(event)

    monkeypatch.setattr(adapter, "publish_inbound", _capture)

    adapter._started = True
    adapter.ctx = MagicMock()  # type: ignore[assignment]

    packet = _make_text_packet(sender="deadbeef")
    await adapter.simulate_inbound(packet)

    assert len(captured) == 1
    event = captured[0]
    assert event.metadata.native is not None
    assert event.metadata.native.data["meshcore"]["contact_label"] is None
    # Pubkey prefix still flows to sender_id.
    assert event.source_transport_id == "deadbeef"
