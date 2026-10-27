"""MeshCore declaration: device-attached firmware state, no local files.

Layout contract read from this distribution: the adapter keeps no local
files; its dedup identity is an in-memory digest of wire fields, and node
identity lives in the attached radio. Truthful coverage means saying so, not
inventing files to capture. This module follows the seam contract in
``medre.maintenance.stopped_declare``.
"""

from __future__ import annotations

from medre.maintenance.stopped_declare import (
    LocalExclusion,
    ProviderCoverage,
    StoppedStateDeclaration,
    declared,
)

_NOTES = (
    "node identity and firmware state live on the attached device and are "
    "outside any file capture",
    "radio ownership cannot be proven from files; verify the configured "
    "TCP endpoint (including remote radios), or the selected serial/Bluetooth "
    "device and its exclusive use",
)


class MeshCoreStoppedState:
    """Provider-owned declaration of one configured radio connection."""

    provider = "meshcore"

    def coverage(self, config: object, *, adapter_id: str) -> ProviderCoverage:
        """Declare the honest device-attached representation with zero files."""
        if getattr(config, "connection_type", "") == "fake":
            return declared(
                provider=self.provider,
                adapter_id=adapter_id,
                files=(),
                state_class="stateless_local",
                notes=("fake connectivity journals in memory only",),
            )
        return declared(
            provider=self.provider,
            adapter_id=adapter_id,
            files=(),
            state_class="device_attached",
            notes=_NOTES,
        )

    def local_exclusion(self, config: object, *, adapter_id: str) -> LocalExclusion:
        """Report that device ownership cannot be established from files."""
        instruction = (
            "verify the configured TCP endpoint and exclusive radio use"
            if getattr(config, "connection_type", "") == "tcp"
            else "verify the selected physical radio and exclusive device use"
        )
        return LocalExclusion(
            self.provider,
            note=f"no local ownership primitive exists; {instruction}",
        )


def declaration() -> StoppedStateDeclaration:
    """Return the provider-owned declaration for stopped-state planning."""
    return MeshCoreStoppedState()
