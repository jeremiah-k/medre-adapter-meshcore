"""Public fake and native-schema seams owned by this provider."""

import copy
import json
from pathlib import Path

import jsonschema
from medre.adapters.meshcore.plugin import adapter_spec
from medre.testing.fakes import assert_fake_contract


def test_fake_conforms_to_explicit_core_calls():
    assert_fake_contract(adapter_spec())


def test_native_schema_example_and_reader_agree():
    spec = adapter_spec()
    schemas = Path(__file__).resolve().parents[1] / "docs/schemas"
    schema = json.loads(
        (schemas / "meshcore-native-metadata.schema.json").read_text(encoding="utf-8")
    )
    example = json.loads(
        (schemas / "examples/meshcore-native-metadata-example.json").read_text(
            encoding="utf-8"
        )
    )
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(example, schema)
    reader = spec.native_namespace_reader.load()
    assert reader(example) == example["meshcore"]
    future = copy.deepcopy(example)
    future["meshcore"]["schema_version"] += 1
    assert reader(future) == {}
    assert spec.versioned_namespace_reader.load()(future) == future["meshcore"]


def test_direct_factory_import_does_not_quarantine_provider(tmp_path):
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "from medre.adapters.meshcore.plugin import adapter_spec; from medre.adapter_registry import registered_transports, adapter_discovery_failures; assert 'meshcore' in registered_transports(), adapter_discovery_failures()",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
