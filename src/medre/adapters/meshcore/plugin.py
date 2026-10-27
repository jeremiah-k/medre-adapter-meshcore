"""Declarative registration for the separately distributed adapter."""

from typing import Any

from medre import adapter_spec as registration
from medre.adapter_spec import AdapterSpec, DestinationSpec, SymbolRef


def adapter_spec() -> AdapterSpec:
    # Prompt metadata is optional; discovery works with released core versions
    # that do not expose the authoring form contract.
    native_setup: dict[str, Any] = {}
    NativeSetupField = getattr(registration, "NativeSetupField", None)
    if NativeSetupField is not None:
        native_setup["native_setup_fields"] = (
            NativeSetupField(
                "connection_type",
                "Radio connection type",
                choices=("tcp", "serial", "ble"),
            ),
            NativeSetupField(
                "host", "TCP hostname", active_when=("connection_type", "tcp")
            ),
            NativeSetupField(
                "port",
                "TCP port (blank uses the protocol default)",
                required=False,
                active_when=("connection_type", "tcp"),
                numeric=True,
            ),
            NativeSetupField(
                "serial_port",
                "Serial device",
                active_when=("connection_type", "serial"),
            ),
            NativeSetupField(
                "ble_address",
                "BLE device address",
                active_when=("connection_type", "ble"),
            ),
            NativeSetupField(
                "ble_pin",
                "BLE PIN (blank uses the device default)",
                required=False,
                secret=True,
                active_when=("connection_type", "ble"),
            ),
        )
    return AdapterSpec(
        **native_setup,
        sample_config=SymbolRef("medre.adapters.meshcore.sample", "sample_config"),
        stopped_state_declaration=SymbolRef(
            "medre.adapters.meshcore.stopped_declaration", "declaration"
        ),
        destinations=(
            DestinationSpec(
                "meshcore_contact",
                SymbolRef(
                    "medre.config.adapters.meshcore_destinations",
                    "validate_destination",
                ),
            ),
        ),
        transport="meshcore",
        config=SymbolRef("medre.config.adapters.meshcore", "MeshCoreConfig"),
        runtime_config=SymbolRef(
            "medre.config.adapters.meshcore_runtime", "MeshCoreRuntimeConfig"
        ),
        adapter=SymbolRef("medre.adapters.meshcore.adapter", "MeshCoreAdapter"),
        fake_adapter=SymbolRef("medre.adapters.fakes.meshcore", "FakeMeshCoreAdapter"),
        renderer_factory=SymbolRef(
            "medre.adapters.meshcore.renderer", "build_meshcore_renderer"
        ),
        dependency_probe=SymbolRef("medre.adapters.meshcore.compat", "HAS_MESHCORE"),
        distribution="meshcore",
        import_names=("meshcore",),
        sdk_import_roots=("meshcore", "bleak", "serial", "serial_asyncio"),
        native_namespace_reader=SymbolRef(
            "medre.adapters.meshcore.event_shape", "meshcore_namespace"
        ),
        versioned_namespace_reader=SymbolRef(
            "medre.adapters.meshcore.event_shape", "meshcore_versioned_namespace"
        ),
        attribution_projector=SymbolRef(
            "medre.adapters.meshcore.attribution", "project_meshcore_attribution"
        ),
        support_endpoint_fields=(
            "host",
            "port",
            "serial_port",
            "ble_address",
            "serial_baudrate",
        ),
        support_secret_fields=("ble_pin",),
        native_detection_priority=10,
    )
