"""MeshCore renderer for target-specific event rendering.

The :class:`MeshCoreRenderer` converts canonical events into
MeshCore-ready content payloads (dicts with ``text`` and
``channel_index``).

The renderer is initialised with a **required** mapping of adapter IDs to
:class:`~medre.config.adapters.meshcore.MeshCoreConfig` instances.
At render time the config for *target_adapter* is resolved from this
mapping — there is no fallback or default.  An empty mapping raises
:class:`ValueError` at construction; an unknown *target_adapter* raises
:class:`KeyError` at render time.

This renderer is owned by the MeshCore adapter package and is registered
with the rendering pipeline.

Selection is via the rendering pipeline's platform registry: when the
pipeline populates the adapter's platform as ``"meshcore"``, the renderer
matches on that platform string directly.  This decouples renderer
selection from adapter naming conventions.

**Strict RenderingContext protocol**

Both ``can_render`` and ``render`` accept a frozen
:class:`~medre.core.rendering.renderer.RenderingContext` carrying all
dispatch metadata — delivery strategy, target identity, capability
constraints, and text budgets.  One strict renderer signature.

**fallback_text strategy**

When ``delivery_strategy == "fallback_text"``, relation semantics are
degraded into inline text within the MeshCore content body while
preserving MeshCore payload ownership (``text``, ``channel_index``).
Contact/channel/destination semantics and target
addressing are retained in the native MeshCore structure.  The result
carries ``fallback_applied="strategy_fallback_text"``.

Text messages use bounded single-packet rendering by default. Explicit
``max_text_parts > 1`` opts long ordinary messages into the shared immutable
radio planner; all native packets use core-owned durable handoffs, including
short and clipped single packets. Attribution, numbering and
UTF-8 code points respect every effective per-packet budget. Plans exceeding
the part cap fail as a whole rather than clipping an incomplete message.

**Important**: local send acceptance is not delivery confirmation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from medre.adapters.metadata import project_source_fields
from medre.core.events import CanonicalEvent
from medre.core.events.text import select_message_body
from medre.core.rendering.attribution import (
    TRUNCATABLE_RELAY_LABELS,
    resolve_relay_attribution,
    format_relay_prefix,
)
from medre.core.rendering.radio_text import plan_radio_text
from medre.core.rendering.relations import compose_relation_text
from medre.core.rendering.text_helpers import truncate_text_bytes
from medre.core.rendering.renderer import RenderingContext, RenderingResult

from medre.adapters.meshcore.destination import resolve_channel_index
from medre.adapters.meshcore.limits import GROUP_TEXT_BYTES, group_text_budget

"""Relay-prefix label variables that accept a ``{nameN}`` truncation suffix.

MeshCore's companion-app display is width-constrained, so custom prefix
templates may bound these free-text labels to the first N Unicode code
points (N >= 1): ``sender`` and ``sender_short`` project the wire contact
labels and ``origin_label`` resolves from the rendering context or the
adapter's ``origin_label`` config.  The default prefix is empty — this
transport defines no automatic prefix template — so the declaration only
widens what custom templates can express.  Undeclared stems (for example
the fixed-width ``sender_id`` pubkey prefix) treat numeric suffixes as
unknown placeholders.  Formatter rules are specified by the shared
formatter (MEDRE ``docs/spec/routing-delivery.md`` §17.5.5).
"""

if TYPE_CHECKING:
    from medre.config.adapters.meshcore import MeshCoreConfig


class MeshCoreRenderer:
    """Renderer for MeshCore transport targets.

    Produces content dicts with ``text`` and ``channel_index``.

    **Target-aware rendering.** The renderer is initialised with a
    mapping of adapter IDs to :class:`~medre.config.adapters.meshcore.MeshCoreConfig`
    instances.  At render time the config for *target_adapter* is resolved
    from this mapping.  This allows multi-node setups where each adapter
    has different ``max_text_bytes`` values.

    An empty *configs* mapping raises :class:`ValueError`.  An unknown
    *target_adapter* at render time raises :class:`KeyError`.

    Selection is via the pipeline's platform registry.
    """

    name: str = "meshcore"
    """Platform name this renderer handles (used by the rendering pipeline
    when platform registry is available)."""

    _PLATFORM: str = "meshcore"
    """Internal platform identifier for matching via ``target_platform``."""

    def __init__(
        self,
        *,
        configs: Mapping[str, MeshCoreConfig],
        source_attribution: dict[str, Any] | None = None,
    ) -> None:
        if not configs:
            raise ValueError(
                "MeshCoreRenderer requires at least one adapter config. "
                "Pass a non-empty configs mapping."
            )
        self._configs: dict[str, MeshCoreConfig] = dict(configs)
        self._source_attribution: dict[str, Any] = dict(source_attribution or {})

    # ------------------------------------------------------------------
    # Capability check
    # ------------------------------------------------------------------

    def can_render(
        self,
        event: CanonicalEvent,
        ctx: RenderingContext,
    ) -> bool:
        """Return ``True`` when the context's target platform is ``"meshcore"``
        and *target_adapter* has a registered config.

        Parameters
        ----------
        event:
            The canonical event to check.
        ctx:
            Frozen rendering context with target identity, delivery
            strategy, and capability metadata.

        Returns
        -------
        bool
            Whether this renderer handles events for the given context.
        """
        return (
            ctx.target_platform == self._PLATFORM
            and ctx.target_adapter in self._configs
        )

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    async def render(
        self,
        event: CanonicalEvent,
        ctx: RenderingContext,
    ) -> RenderingResult:
        """Render a canonical event into a MeshCore content payload.

        The rendered payload includes:

        * ``text``: the bounded single packet, or the first exact packet
          of an opted-in ``radio_plan`` containing the complete message.
        * ``channel_index``: parsed from ``ctx.target_channel`` or
          ``config.default_channel``.

        Under ``ctx.delivery_strategy == "fallback_text"``, relation
        semantics are degraded into inline text while the payload retains
        MeshCore-native structure (``text``, ``channel_index``).
        Contact/channel/destination semantics and
        target addressing are preserved.

        **Target-aware config resolution.** The renderer resolves the
        config for ``ctx.target_adapter`` from the ``configs`` mapping
        supplied at construction.  If ``ctx.target_adapter`` is not
        found, a :class:`KeyError` is raised — there is no fallback.

        Parameters
        ----------
        event:
            The canonical event to render.
        ctx:
            Frozen rendering context with target identity, delivery
            strategy, capability metadata, and text budgets.

        Returns
        -------
        RenderingResult
            The rendered MeshCore content dict wrapped in a result.
        """
        # Resolve target-adapter-specific config.
        target_adapter = ctx.target_adapter
        try:
            adapter_config = self._configs[target_adapter]
        except KeyError:
            raise KeyError(
                f"No MeshCoreConfig registered for target_adapter "
                f"{target_adapter!r}. Known adapters: "
                f"{sorted(self._configs.keys())}"
            ) from None

        # Use context budget if provided, else adapter config budget.
        # Clamp to non-negative to guard against misconfiguration.
        selected_max_text_bytes = (
            ctx.max_text_bytes
            if ctx.max_text_bytes is not None
            else group_text_budget(adapter_config.max_text_bytes)
        )
        max_text_bytes = max(
            0,
            min(
                selected_max_text_bytes, adapter_config.max_text_bytes, GROUP_TEXT_BYTES
            ),
        )

        text = select_message_body(event.payload)

        channel_index = resolve_channel_index(
            ctx.target_channel, adapter_config.default_channel
        )

        # Determine fallback behaviour
        is_fallback = ctx.delivery_strategy == "fallback_text"

        if is_fallback:
            text = compose_relation_text(event, text)

        body = text
        wire_prefix = ""

        # -- Optional relay prefix (prepended before truncation) ---------
        prefix_template = adapter_config.relay_prefix
        rendered_prefix = ""
        prefix_formatting_error: str | None = None
        prefix_vars_used: tuple[str, ...] = ()
        prefix_vars_missing: tuple[str, ...] = ()
        prefix_vars_unknown: tuple[str, ...] = ()

        if prefix_template:
            # Resolve origin_label precedence:
            # ctx.source_origin_label (route/context) > adapter registry > None.
            # Use 'is not None' to preserve explicit empty string labels.
            src_attr_cfg = self._source_attribution.get(event.source_adapter)
            # Project source identity from native metadata via dispatch.
            native_data: dict[str, object] = {}
            if event.metadata and event.metadata.native:
                native_data = dict(event.metadata.native.data)

            platform_hint = (
                getattr(src_attr_cfg, "platform", None) if src_attr_cfg else None
            )
            projected = project_source_fields(
                native_data,
                source_adapter=event.source_adapter,
                source_transport_id=event.source_transport_id,
                platform_hint=platform_hint,
            )

            attr = resolve_relay_attribution(
                event,
                source_origin_label=ctx.source_origin_label,
                configured_origin_label=getattr(src_attr_cfg, "origin_label", None),
                source_attribution=ctx.source_attribution,
                projected_fields=projected,
            )
            prefix_result = format_relay_prefix(
                prefix_template, attr, truncatable=TRUNCATABLE_RELAY_LABELS
            )
            rendered_prefix = prefix_result.rendered_prefix
            prefix_formatting_error = prefix_result.formatting_error
            prefix_vars_used = prefix_result.variables_used
            prefix_vars_missing = prefix_result.missing_variables
            prefix_vars_unknown = prefix_result.unknown_variables
            if prefix_formatting_error:
                raise ValueError(f"Invalid relay_prefix: {prefix_formatting_error}")
            wire_prefix = rendered_prefix
            text = rendered_prefix + text

        # Long ordinary messages use the exact shared wire plan only when the
        # operator opts in. Reactions retain the established single-packet path.
        radio_plan = None
        if (
            adapter_config.max_text_parts > 1
            and max_text_bytes > 0
            and event.event_kind == "message.created"
            and len(text.encode("utf-8")) > max_text_bytes
        ):
            radio_plan = plan_radio_text(
                body,
                prefix=wire_prefix,
                max_text_bytes=max_text_bytes,
                max_parts=adapter_config.max_text_parts,
            )
            truncated_text = radio_plan.parts[0].wire_text
            was_truncated = False
            original_bytes = len(text.encode("utf-8"))
            rendered_bytes = sum(part.wire_bytes for part in radio_plan.parts)
        else:
            truncated_text, was_truncated, original_bytes, rendered_bytes = (
                truncate_text_bytes(text, max_text_bytes)
            )

        # Native single packets need the same durable one-use grant as multipart
        # packets. Plan the final wire text so clipping and attribution stay exact.
        if radio_plan is None and adapter_config.connection_type != "fake":
            radio_plan = plan_radio_text(
                truncated_text,
                prefix="",
                max_text_bytes=max_text_bytes,
                max_parts=1,
            )

        content: dict[str, object] = {
            "text": truncated_text,
            "channel_index": channel_index,
        }

        metadata: dict[str, object] = {
            "renderer": self.name,
            "original_length": len(text),
            "rendered_length": (
                sum(len(part.wire_text) for part in radio_plan.parts)
                if radio_plan is not None
                else len(truncated_text)
            ),
            "original_text_bytes": original_bytes,
            "rendered_text_bytes": rendered_bytes,
            "max_text_bytes": max_text_bytes,
            "truncated": was_truncated,
        }

        if radio_plan is not None:
            metadata["rendered_text_chars"] = sum(
                len(part.wire_text) for part in radio_plan.parts
            )
            metadata["radio_part_count"] = len(radio_plan.parts)
            metadata["radio_plan_fingerprint"] = radio_plan.fingerprint

        # Prefix-specific metadata (only when prefix is configured).
        if prefix_template:
            metadata["relay_prefix_template"] = prefix_template
            metadata["relay_prefix_rendered"] = rendered_prefix
            metadata["relay_prefix_variables_used"] = prefix_vars_used
            metadata["relay_prefix_missing_variables"] = prefix_vars_missing
            metadata["relay_prefix_unknown_variables"] = prefix_vars_unknown
            metadata["relay_prefix_formatting_error"] = prefix_formatting_error

        return RenderingResult(
            event_id=event.event_id,
            target_adapter=target_adapter,
            target_channel=ctx.target_channel,
            payload=content,
            metadata=metadata,
            truncated=was_truncated,
            radio_plan=radio_plan,
            fallback_applied="strategy_fallback_text" if is_fallback else None,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------


def build_meshcore_renderer(
    *,
    runtime_configs: Mapping[str, Any],
    all_runtime_configs: Mapping[str, Mapping[str, Any]],
    source_attribution: Mapping[str, Any],
) -> MeshCoreRenderer | None:
    """Build the registered MeshCore renderer for runtime assembly."""
    del all_runtime_configs
    from medre.config.adapters.meshcore import MeshCoreConfig

    configs: dict[str, MeshCoreConfig] = {}
    for rtc in runtime_configs.values():
        if not getattr(rtc, "enabled", False):
            continue
        adapter_id = rtc.adapter_id
        config = rtc.config or MeshCoreConfig(adapter_id=adapter_id)
        configs[adapter_id] = config
    if not configs:
        return None
    return MeshCoreRenderer(
        configs=configs,
        source_attribution=dict(source_attribution),
    )
