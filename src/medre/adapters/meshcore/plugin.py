"""Declarative registration for the separately distributed adapter."""

from medre.adapter_registry import AdapterSpec, DestinationSpec, SymbolRef


def adapter_spec() -> AdapterSpec:
    return AdapterSpec(
        sample_config=SymbolRef("medre.adapters.meshcore.sample", "sample_config"),
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
