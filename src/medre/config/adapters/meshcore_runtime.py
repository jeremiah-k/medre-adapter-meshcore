"""Adapter-owned runtime configuration wrapper for meshcore."""

from dataclasses import dataclass
from typing import Self

from medre.config.adapters.meshcore import MeshCoreConfig
from medre.config.model import GenericAdapterRuntimeConfig


@dataclass(frozen=True)
class MeshCoreRuntimeConfig(GenericAdapterRuntimeConfig):
    """Validated runtime configuration for one meshcore instance."""

    config: MeshCoreConfig | None = None

    @classmethod
    def from_dict(cls, instance_name: str, data: dict) -> Self:
        return cls.from_transport_dict(
            instance_name, data, transport="meshcore", config_cls=MeshCoreConfig
        )
