"""MeshCore stopped-state declaration truthfulness.

The radio carries the identity; the dedup digest is in-memory. The
declaration must say so with zero files instead of inventing capturable
state. No device or SDK import is involved.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from medre.adapters.meshcore.plugin import adapter_spec
from medre.adapters.meshcore.stopped_declaration import declaration
from medre.maintenance.stopped_declare import declaration_from_spec


def test_spec_references_the_declaration_module():
    reference = adapter_spec().stopped_state_declaration
    assert reference is not None
    assert reference.module == "medre.adapters.meshcore.stopped_declaration"
    assert reference.name == "declaration"
    assert declaration().provider == "meshcore"


def test_radio_providers_are_device_attached_with_zero_files():
    coverage = declaration().coverage(
        SimpleNamespace(connection_type="tcp"), adapter_id="radio"
    )
    assert coverage.state_class == "device_attached"
    assert coverage.files == ()
    assert any("device" in note for note in coverage.notes)
    assert any("cannot be proven from files" in note for note in coverage.notes)


def test_fake_connectivity_is_stateless():
    coverage = declaration().coverage(
        SimpleNamespace(connection_type="fake"), adapter_id="radio"
    )
    assert coverage.state_class == "stateless_local"
    assert coverage.files == ()


@pytest.mark.parametrize("mode", ["tcp", "serial", "ble"])
def test_device_ownership_is_reported_unsupported(mode):
    exclusion = declaration().local_exclusion(
        SimpleNamespace(connection_type=mode), adapter_id="radio"
    )
    assert exclusion.scope == "unsupported"
    assert exclusion.method == "none"
    assert "exclusive" in exclusion.note
    assert ("TCP endpoint" if mode == "tcp" else "physical radio") in exclusion.note


def test_core_resolution_resolves_the_referenced_factory():
    resolved = declaration_from_spec(adapter_spec())
    assert resolved.provider == "meshcore"
    coverage = resolved.coverage(
        SimpleNamespace(connection_type="tcp"), adapter_id="radio"
    )
    assert coverage.state_class == "device_attached"
    assert coverage.files == ()
