"""Fake MeshCore setup enforces the native configuration contract."""

import pytest

from medre.adapters.fakes.meshcore import FakeMeshCoreAdapter
from medre.config.adapters.meshcore import MeshCoreConfig, MeshCoreConfigError


def test_fake_adapter_rejects_invalid_prefix_at_setup():
    with pytest.raises(MeshCoreConfigError):
        FakeMeshCoreAdapter(
            MeshCoreConfig(adapter_id="test", relay_prefix="{unsupported}")
        )
