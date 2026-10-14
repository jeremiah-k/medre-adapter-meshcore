"""Discovery contract for a separately installed MEDRE transport."""
from importlib.metadata import entry_points

from medre.adapter_registry import AdapterSpec, get_adapter_spec


def test_adapter_entry_point() -> None:
    matches = [ep for ep in entry_points(group="medre.adapters") if ep.name == "meshcore"]
    assert len(matches) == 1
    spec = matches[0].load()()
    assert isinstance(spec, AdapterSpec)
    assert spec.transport == "meshcore"
    assert spec.adapter.module == "medre.adapters.meshcore.adapter"
    assert spec.config.module == "medre.config.adapters.meshcore"
    assert get_adapter_spec("meshcore") == spec
