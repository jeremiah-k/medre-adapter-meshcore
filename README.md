# medre-adapter-meshcore

The MeshCore adapter for MEDRE.

## Install

Use [uv](https://docs.astral.sh/uv/getting-started/installation/) to install
MEDRE and this adapter in the same persistent tool environment:

```bash
uv tool install medre
medre adapters install meshcore
```

On Windows, the package-changing command prints a uv command to run **after**
`medre.exe` exits, rather than replacing an executable still in use.
Adapter installation defaults to package-index requirements; existing saved
Git requests and catalogs are preserved. For fake smoke, guided route
creation, locked uv deployment projects, private Git sources, local checkouts,
and the same-environment requirement, see the
[MEDRE Getting Started guide](https://github.com/jeremiah-k/medre/blob/main/docs/ops/getting-started.md)
and the
[adapter-source guide](https://github.com/jeremiah-k/medre/blob/main/docs/ops/adapter-installation.md).
Neither MEDRE nor the adapter installs packages during runtime startup.

## Configure

Save a private `connections.yaml` with one real TCP instance. The common case
needs the connection type and the node host; the default TCP port is shown in
the setup guide's options:

```yaml
adapters:
  meshcore:
    meshcore-alpha:
      adapter_kind: real
      connection_type: tcp
      host: 192.168.1.100
routes: {}
```

For BLE, set `ble_address` to the MAC address you recorded during pairing.
Add another adapter instance, then build routes with guided configuration.
For every option, environment overrides, and troubleshooting, see the
[MeshCore setup guide](https://github.com/jeremiah-k/medre/blob/main/docs/ops/transport-setup/meshcore.md).

## Verify

Confirm the installed adapter and the saved configuration resolve:

```bash
medre adapters check --config connections.yaml
```

The setup guide's
[Verify](https://github.com/jeremiah-k/medre/blob/main/docs/ops/transport-setup/meshcore.md#verify)
section shows the config check, a healthy start, health states, and
reconnecting behavior.

## Text budgets and delivery behavior

MeshCore group messages have a 160-byte firmware text limit including the
firmware's `<device name>: ` prefix. MEDRE reserves that space, uses a conservative
127-byte budget before the device name is known, and refreshes the effective
budget from the live handshake. `max_text_bytes` (default 160) is an operator
upper bound. Rendering truncates safely at UTF-8 boundaries and records that
fact; native sends that bypass this budget fail permanently before SDK I/O.

All core-rendered native messages use the durable packet grant, including short
and clipped single packets. A timeout, disconnect or cancellation after submission
retains uncertainty and does not trigger an internal resend. Empty native wire
text is rejected before dispatch. Direct session calls keep their legacy retry
policy and do not provide this guarantee. See [guarded native text](docs/spec/multipart-text.md).

Replies and threads relay as inline fallback text. They do not emit native
reply/thread operations. Delivery evidence identifies `fallback_text`;
reactions, edits, and deletes remain unsupported and are suppressed.
Fake adapters expose the same planning capabilities as their health reports.

## Offline startup and recovery

A long-lived MEDRE adapter returns from initial setup in degraded health when
its configured radio is unavailable. Each complete native connection attempt
has a 15-second TCP/serial budget or a 45-second BLE budget (including BlueZ
cleanup and discovery). One session-owned recovery loop then retries with
exponential backoff and jitter, capped at 30 seconds between attempts, until the
radio connects or the adapter stops. The SDK's own auto-reconnect stays disabled.
Initial unavailability and later disconnects use the same serialized setup path.
Missing SDKs and invalid configuration still fail explicitly.

A disconnected started session reports transient send failure before any native
handoff. MEDRE owns delivery retries and their configured limits; connection
recovery does not guarantee unlimited message retention or native delivery.
Stopping the adapter cancels recovery before native teardown. Session stop
clears the inbound callback before its first await, so inbound delivery ends
when stop begins, even if the stop is cancelled mid-teardown. Native
cancellation still depends on the pinned SDK and driver cooperating; this is
not a process isolation boundary.

Direct one-shot `MeshCoreSession.start()` callers retain strict connection
failure by default; the production adapter explicitly requests degraded startup
with `recover_on_failure=True`.

## Development

See [contributing](CONTRIBUTING.md) for the shared Trunk quality gate and
uv-based editable development workflow. Maintainer material — packaging and
provenance, ownership and imports, offline samples and diagnostics, tests,
fake smoke sources, coordinated CI, and the adapter contract — lives in
[adapter maintenance](docs/dev/adapter-maintenance.md).
