"""Transport-owned session boundary for the MeshCore adapter.

:class:`MeshCoreSession` encapsulates the MeshCore SDK client lifecycle —
connection creation, event subscription, reconnection, and clean shutdown —
so that :class:`~medre.adapters.meshcore.adapter.MeshCoreAdapter` can
delegate all SDK interaction to this single owned object.

The session is the **sole owner** of the SDK ``MeshCore`` instance.  No
other module in the adapter package imports or touches the SDK directly.

Metadata Normalization Audit
----------------------------
MeshCore native message payloads (from the SDK reader) carry these fields:

* ``type`` — ``"PRIV"`` (direct/DM) or ``"CHAN"`` (channel broadcast)
* ``pubkey_prefix`` — 6-byte hex prefix of the sender's public key
* ``sender_timestamp`` — 4-byte little-endian Unix timestamp
* ``text`` — UTF-8 decoded payload text
* ``txt_type`` — message sub-type code (0=plain, 2=signed, etc.)
* ``channel_idx`` — channel index (CHAN messages only)
* ``path_len``, ``path_hash_mode`` — routing metadata (optional)
* ``SNR``, ``RSSI`` — radio metadata (V3 messages only)

Comparison with other transports:

* **Matrix**: uses ``event_id``, ``room_id``, ``sender`` (MXID), ``content``,
  ``origin_server_ts``, ``type`` (event type string).  Metadata is
  JSON-structured with ``m.relates_to`` for threading.
* **Meshtastic**: uses ``messageId`` (uint32), ``fromId``, ``toId``,
  ``channel``, ``portnum``, ``rxTime``, ``rxSnr``, ``rxRssi``, ``hopLimit``.
  Rich metadata in protobuf-structured ``decoded`` payloads.

MeshCore differs in that:

1. Sender identity is a **pubkey prefix** (6 hex bytes), not a human-readable
   MXID or numeric node ID.
2. Channel is an **index** (0–255), not a room ID or name.
3. No built-in reply/threading — ``txt_type`` carries sub-type info only.
4. Radio metadata (SNR/RSSI) is only available in V3 protocol messages.

The codec normalises all of these into
:class:`~medre.core.events.metadata.NativeMetadata` under a ``meshcore``
namespace, stripping SDK-specific structures before emitting canonical events.

Connection Modes
----------------
The session supports four connection types via
:class:`~medre.config.adapters.meshcore.MeshCoreConfig`:

``"fake"``
    No real SDK client.  Used for unit tests without hardware.

``"tcp"``
    Connects via TCP using ``MeshCore.create_tcp(host, port)`` factory.

``"serial"``
    Connects via serial by constructing ``SerialConnection(port, baudrate,
    cx_dly=0.1, rts=False, dtr=False)`` and handing it to the ``MeshCore``
    constructor — not the ``create_serial`` factory, whose no-response
    retry would re-open the port with DTR asserted.

``"ble"``
    Pre-scans with ``BleakScanner.find_device_by_filter()`` for a
    ``BLEDevice``, then calls ``MeshCore.create_ble(address=...,
    device=..., pin=...)``.  When the scan returns ``None``,
    ``device=None`` is passed for address-only fallback.  ``pin`` is
    forwarded only when ``ble_pin`` is configured.  Between retry
    attempts, stale BlueZ state is cleaned before re-scanning.

Inbound Backlog Drain
----------------------
One connection-scoped receive pump owns GET_NEXT_MSG on the pinned SDK.
It starts after connection setup returns and wakes on MESSAGES_WAITING, draining
until NO_MORE_MSGS. The SDK still decodes and dispatches messages; MEDRE does
not publish the command result a second time. An unanswered command is not
proof of an empty queue, and the companion protocol cannot guarantee receipt.
A parked pump probes the idle link at a fixed cadence with one cheap local
device query, so a link that died silently retires through ordinary
recovery instead of looking connected forever. A connection that follows a
failure-driven retirement answers one probe before its first fetch, so a
link whose server-to-client direction is still broken loses nothing: the
destructive pop never runs against an unproven link.

Reconnect Policy
----------------
On unexpected disconnect the session attempts bounded exponential backoff:

* BLE delays: 10 s, 20 s, 40 s, … capped at 300 s, with a 10-second floor.
* TCP/serial retain their 1 s → 30 s policy. Both policies apply ±25 % jitter.
* Background BLE recovery makes one native factory call per backoff interval.
* Continue until connected or stopped, with 15-second TCP/serial or 45-second
  BLE setup budgets per attempt.
* Known configuration and SDK contract errors stop recovery; transient transport
  failures continue retrying. Error text is never used to classify failures.
* On ``stop()`` a ``_stop_requested`` guard prevents further reconnects.

.. note::

   **Duplicate-send risk for unbound calls.**  Without a core packet grant,
   :meth:`send_text` may retry transient failures up to 3 times. A lost ACK
   can then cause duplicate native commands. Guarded core-bound calls use a
   one-use handoff grant and make at most one SDK command; any post-grant
   timeout or cancellation remains uncertain rather than being retried.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import math
import random
import threading
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol, cast

from medre.adapters.meshcore.compat import HAS_MESHCORE
from medre.adapters.meshcore.errors import (
    MeshCoreBluetoothUnavailableError,
    MeshCoreConnectionError,
    MeshCoreSendError,
)
from medre.adapters.meshcore.limits import GROUP_TEXT_BYTES, group_text_budget
from medre.config.adapters.meshcore import MeshCoreConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Reconnect backoff parameters.
_RECONNECT_BASE_DELAY: float = 1.0  # TCP/serial seconds
_RECONNECT_MAX_DELAY: float = 30.0  # TCP/serial seconds
_BLE_RECONNECT_BASE_DELAY: float = 10.0
_BLE_RECONNECT_MAX_DELAY: float = 300.0
_RECONNECT_MAX_ATTEMPTS: int = 0  # zero keeps long-lived recovery active until stop
_RECONNECT_JITTER_FRACTION: float = 0.25  # ±25 %

# Outbound delivery retry.
_SEND_MAX_RETRIES: int = 3

# ``meshcore`` reports command-response timeouts as EventType.ERROR payloads
# instead of raising. These reasons describe transport uncertainty, not a
# permanent remote rejection, so MEDRE keeps them retryable.
_TRANSIENT_SDK_SEND_REASONS: frozenset[str] = frozenset(
    {"no_event_received", "timeout"}
)

# Timeout for external SDK lifecycle calls (auto-fetch start/stop).
_SDK_LIFECYCLE_TIMEOUT: float = 5.0  # seconds
_CONNECT_ATTEMPT_TIMEOUT: float = 15.0  # TCP / serial setup attempt
_BLE_CONNECT_ATTEMPT_TIMEOUT: float = 45.0  # includes BlueZ cleanup and discovery

# One bounded native request at a time. A successful backlog can be arbitrarily
# deep; it does not extend the startup deadline or require a guessed queue cap.
_FETCH_ROUND_TIMEOUT: float = 2.0

# A fetch window that elapses with no inbound traffic at all parks the pump
# at once: the node's fetch is a destructive pop, so the window may have
# ended because it popped a message whose frame was lost, and re-issuing
# would pop another message into a link that is not returning what it pops.
# A window that elapses while inbound traffic is still arriving — a late
# frame, or a notification — means the link is slow but delivering: the
# pump keeps the connection and re-issues, because publication happens in
# the SDK event callback whenever the frame actually arrives, even if it
# pops after the window. That re-issue rate stays bounded: at most
# _FETCH_IDLE_IMMEDIATE_REISSUES consecutive immediate re-issues, after
# which each further re-issue waits for a notification or
# _FETCH_IDLE_REISSUE_DELAY, whichever comes first.
_FETCH_IDLE_IMMEDIATE_REISSUES: int = 2
_FETCH_IDLE_REISSUE_DELAY: float = 2.0

# A parked wait never expires on its own, so a link that died silently —
# the peer vanished without FIN/RST — would hold the pump parked and the
# session looking connected forever. While the pump would otherwise sit
# parked, it waits for the wakeup at most this long before sending one
# liveness probe, and repeats at this cadence while the quiet continues.
# The value equals the pinned SDK's own default command timeout (15 s): a
# slow-but-alive companion answers inside it (the lab's 4 s per-direction
# latency fault answers in ~8 s), while each probe also gets this same
# budget before it is judged failed, so detection completes at roughly
# this interval plus one probe budget after the silence begins. On a
# healthy node the query answers in milliseconds; the budget is a
# worst-case bound.
_PROBE_IDLE_INTERVAL: float = 15.0

# The SDK reports its own elapsed command wait as an ERROR event carrying one
# of these reasons; nothing was received from the node.
_PROBE_UNANSWERED_REASONS = frozenset({"timeout", "no_event_received"})

# suggested_timeout clamping bounds (seconds).
# The firmware returns milliseconds; we convert to seconds and clamp.
# Floor: never sleep less than 0.5 s even if firmware suggests very short.
# Ceil: never sleep more than 30 s (matches reconnect max delay).
_SUGGESTED_TIMEOUT_FLOOR: float = 0.5  # seconds
_SUGGESTED_TIMEOUT_CEIL: float = 30.0  # seconds


def _retry_delay_contact_key(contact_id: str) -> str:
    """Normalize a contact_id for retry-delay cache lookups.

    Strips leading/trailing whitespace and lowercases.
    This prevents cached timeout hints from fragmenting when
    equivalent contact IDs are passed with case or whitespace
    differences (e.g. pubkey hex prefixes).
    """
    return contact_id.strip().lower()


# Type alias for the inbound message callback.
# The callback receives a plain dict (native payload), NOT an SDK Event object.
# Both sync and async callables are accepted.
MessageCallback = Callable[[dict[str, Any]], Any]


@dataclass
class _SessionDiagnostics:
    """Mutable diagnostics snapshot owned by the session."""

    connected: bool = False
    reconnecting: bool = False
    reconnect_attempts: int = 0
    last_message_time: datetime | None = None
    last_error: str | None = None
    transient_delivery_failures: int = 0
    permanent_delivery_failures: int = 0
    device_name: str | None = None
    public_key_prefix: str | None = None
    radio_freq: float | None = None
    # suggested_timeout observability.
    sdk_suggested_timeouts_used: int = 0
    # Contact/self-info observability (diagnostics only).
    known_contact_count: int = 0
    last_contact_update_time: datetime | None = None


class _MeshCoreFactory(Protocol):
    """The SDK class constructor and TCP factory used by this session."""

    def __call__(self, connection: Any, *, auto_reconnect: bool) -> Any: ...

    async def create_tcp(
        self, host: str, port: int, *, auto_reconnect: bool
    ) -> Any: ...


class _MeshCoreModule(Protocol):
    """Structural type for the optional ``meshcore`` SDK package.

    Defines the subset of the SDK's public API used by
    :meth:`MeshCoreSession._connect_real`.  Because ``meshcore`` is an
    optional dependency whose package may be absent at type-check time,
    this Protocol gives Pyright a concrete shape without requiring the
    SDK's type stubs to be installed.

    The SDK exposes async factory methods on ``MeshCore`` —
    ``create_tcp`` and ``create_ble`` — which handle connection
    construction and initial handshake internally.  Serial clients are
    constructed from ``SerialConnection`` directly (see
    :class:`MeshCoreSession._connect_real`) to keep the DTR/RTS lines
    deasserted; the serial factory is deliberately not consumed.
    """

    MeshCore: _MeshCoreFactory
    SerialConnection: type
    EventType: Any


def _extract_expected_ack(raw: Any) -> str | None:
    """Extract deterministic hex string from expected_ack bytes.

    Per meshcore_py, expected_ack is a 4-byte hash. Returns lowercase
    hex string, or None if raw is not 4 bytes (which would indicate
    an SDK API change).
    """
    if isinstance(raw, bytes):
        if len(raw) != 4:
            # Possible SDK API change (e.g., extended to 8 bytes in a
            # future meshcore_py version).  Surface a warning so this
            # doesn't silently break native_id extraction.
            logger = logging.getLogger(__name__)
            logger.warning(
                "MeshCore expected_ack length %d != 4 (SDK API change?); "
                "falling back to message_id or None",
                len(raw),
            )
            return None
        return raw.hex()
    return None


def _extract_suggested_timeout(source: Any) -> float | None:
    """Extract and clamp suggested_timeout from SDK send result.

    Per meshcore_py, ``suggested_timeout`` is a 4-byte little-endian
    unsigned int in the MSG_SENT payload (units: milliseconds).

    Returns the value in **seconds**, clamped to
    ``[_SUGGESTED_TIMEOUT_FLOOR, _SUGGESTED_TIMEOUT_CEIL]``.  Returns
    ``None`` when the value is absent or not a valid positive number.

    Parameters
    ----------
    source:
        A dict-like object that may contain ``"suggested_timeout"``.
    """
    if not isinstance(source, dict):
        return None
    raw = source.get("suggested_timeout")
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if not isinstance(raw, (int, float)):
        return None
    # The SDK returns milliseconds; convert to seconds.
    val = float(raw) / 1000.0
    if not math.isfinite(val) or val <= 0.0:
        return None
    return max(_SUGGESTED_TIMEOUT_FLOOR, min(val, _SUGGESTED_TIMEOUT_CEIL))


class MeshCoreSession:
    """Transport-owned session boundary wrapping a MeshCore SDK client.

    Parameters
    ----------
    config:
        Validated :class:`~medre.config.adapters.meshcore.MeshCoreConfig`.
    adapter_id:
        Identifier of the owning adapter (for logging).
    platform:
        Platform string (``"meshcore"``).
    logger:
        Optional logger; defaults to ``logging.getLogger(...)``.
    """

    def __init__(
        self,
        config: MeshCoreConfig,
        adapter_id: str,
        platform: str = "meshcore",
        *,
        logger: logging.Logger | None = None,
        on_connection_change: Callable[[bool], None] | None = None,
        wait_for_receive_capacity: Callable[[], Awaitable[None]] | None = None,
        release_receive_permit: Callable[[], None] | None = None,
        receive_permit_ready: Callable[[], bool] | None = None,
    ) -> None:
        self._config = config
        self._adapter_id = adapter_id
        self._platform = platform
        self._logger = logger or logging.getLogger(
            f"medre.adapters.meshcore.session.{adapter_id}"
        )

        # SDK objects — only populated for real connection modes.
        self._meshcore: Any = None  # meshcore.MeshCore instance
        # The imported SDK module of the live client.  Kept so the backlog
        # drain can read the SDK event types without a second import.
        self._sdk_module: Any = None
        self._subscriptions: list[Any] = []  # Subscription handles

        # Inbound callback set via start().
        self._message_callback: MessageCallback | None = None
        self._on_connection_change = on_connection_change
        self._wait_for_receive_capacity = wait_for_receive_capacity
        # Adapter-side release of the pump's inbound-capacity permit. The
        # pump holds at most one permit from before a fetch until the
        # fetched message's publication consumes it; drains that end
        # without a fetched message release it through this callable.
        self._release_receive_permit = release_receive_permit
        self._receive_permit_ready = receive_permit_ready
        self._fetch_wake: asyncio.Event | None = None
        self._fetch_task: asyncio.Task[None] | None = None
        # True while a fetch round's destructive pop is outstanding, so a
        # disconnect that lands inside the round is recognised as unclean:
        # the node may have popped a message whose frame was lost with the
        # link.
        self._fetch_round_in_flight: bool = False
        # Mirror of the pump's current streak of fetch rounds that came
        # back with no event at all (not even an empty queue): each such
        # round popped a message whose frame never arrived. A transport
        # loss on a connection with a nonzero streak is unclean even when
        # no fetch round is in flight at that moment.
        self._pump_silent_rounds: int = 0
        # True when the last drain on this connection was interrupted by an
        # elapsed fetch window instead of ending on the node's NO_MORE_MSGS:
        # the node may still hold queued messages this connection never saw.
        # An answered idle probe re-arms exactly one fetch for such a drain;
        # a drain that ended with NO_MORE_MSGS is complete and an answered
        # probe never fetches for it.
        self._pump_drain_incomplete: bool = False
        # True while the pump is parked — waiting for traffic, a
        # notification, or an answered probe — so a frame publishing
        # through the event callback wakes it. A frame published while a
        # drain is running is that drain's business; waking would only
        # leave a stale wake behind after the drain's final empty-queue
        # round.
        self._pump_parked: bool = False
        # A connection that follows a failure-driven retirement — a fetch or
        # probe failure, or an unclean transport loss while a fetch was in
        # flight — must earn the right to fetch: its first probe must be
        # answered before the pump issues its first fetch. Cleared when a
        # probe is answered, or when the session starts clean (a fresh
        # start or a clean-disconnect reconnect never set it).
        self._receive_failure_pending: bool = False

        # Lifecycle guards.
        self._started: bool = False
        self._stop_requested: bool = False
        self._reconnect_task: asyncio.Task[None] | None = None
        self._initial_start_task: asyncio.Task[Any] | None = None
        # Bumped for every event the SDK dispatcher delivers, so the
        # receive pump can tell an idle-but-delivering link (late responses
        # keep arriving) from a silent one that returns nothing at all.
        self._fetch_inbound_seen: int = 0

        # Send serialization: ensures pacing sleeps are not bypassed when
        # multiple send_text() calls overlap via pipeline fan-out.
        # Created in __init__ (not start()) because MeshCoreSession does not
        # capture a running loop reference — asyncio.Lock() lazy-binds on
        # first use in Python 3.10+.
        self._send_lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()

        # Per-contact cached SDK suggested_timeout for DM retry delays.
        # Keyed by contact_id so that each contact's timeout is tracked
        # independently.  Persisted across send_text() calls so that a
        # successful DM that captures a timeout can inform the retry delay
        # of a subsequent failing DM to the same contact.  Cleared on stop().
        self._contact_retry_delays: dict[str, float] = {}

        # Diagnostics.
        self._diag = _SessionDiagnostics()
        # Only the held identity scalars cross the startup-info thread boundary.
        self._self_info_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    def _set_connected(self, connected: bool) -> None:
        """Publish actual native connection transitions after updating diagnostics.

        Recovery and disconnects share this path. Callback failures cannot
        change native ownership or interrupt session cleanup.
        """
        changed = self._diag.connected != connected
        self._diag.connected = connected
        if changed and self._on_connection_change is not None:
            try:
                self._on_connection_change(connected)
            except Exception as exc:
                self._logger.warning(
                    "MeshCoreSession %s: connection notification failed (%s)",
                    self._adapter_id,
                    type(exc).__name__,
                )

    @property
    def connected(self) -> bool:
        """Whether the session has an active SDK connection."""
        return self._diag.connected

    @property
    def reconnecting(self) -> bool:
        """Whether a reconnect loop is in progress."""
        return self._diag.reconnecting

    @property
    def reconnect_attempts(self) -> int:
        """Number of consecutive reconnect attempts since last disconnect."""
        return self._diag.reconnect_attempts

    @property
    def last_message_time(self) -> datetime | None:
        """UTC datetime of the last successfully processed inbound message."""
        return self._diag.last_message_time

    @property
    def last_error(self) -> str | None:
        """Human-readable description of the last session-level error."""
        return self._diag.last_error

    @property
    def transient_delivery_failures(self) -> int:
        """Count of transient outbound delivery failures."""
        return self._diag.transient_delivery_failures

    @property
    def permanent_delivery_failures(self) -> int:
        """Count of permanent outbound delivery failures."""
        return self._diag.permanent_delivery_failures

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    def _permanent_setup_error(exc: Exception) -> bool:
        """Recognize explicit contract failures without parsing SDK error text.

        BLE names, MAC addresses and platform UUIDs are all supported discovery
        selectors. A missing discovery result is reachability, not invalid syntax.
        """
        if isinstance(exc, MeshCoreConnectionError):
            return not exc.transient
        return isinstance(
            exc,
            (ValueError, TypeError, AttributeError, ImportError, NotImplementedError),
        )

    async def start(
        self, message_callback: MessageCallback, *, recover_on_failure: bool = False
    ) -> None:
        """Connect to MeshCore and begin receiving events.

        Parameters
        ----------
        message_callback:
            Sync or async callable invoked with a plain dict for each
            inbound message.  The dict contains native MeshCore payload
            fields (``text``, ``pubkey_prefix``, ``sender_timestamp``,
            ``type``, ``channel_idx``, etc.) — **not** the SDK ``Event``
            object.

        ``recover_on_failure`` is enabled by the long-lived adapter. A native
        reachability failure then starts degraded with one owned recovery loop;
        strict one-shot session/probe callers retain failure propagation.

        Raises
        ------
        MeshCoreConnectionError
            If the SDK is unavailable, or a strict initial connection fails.
        """
        if self._started:
            return

        self._message_callback = message_callback
        self._stop_requested = False
        # A fresh session start is clean: no earlier connection of this
        # session popped anything into a broken link, so the first pump
        # fetches immediately without a gate probe.
        self._receive_failure_pending = False

        if self._config.connection_type == "fake":
            # Fake mode: no real SDK client needed.
            self._set_connected(True)
        else:
            if not HAS_MESHCORE:
                await self._cleanup_failed_start()
                raise MeshCoreConnectionError(
                    "meshcore SDK not installed; install medre-adapter-meshcore and restart"
                )
            self._initial_start_task = asyncio.current_task()
            try:
                try:
                    await self._connection_attempt()
                except Exception as exc:
                    if not recover_on_failure or self._permanent_setup_error(exc):
                        await self._cleanup_failed_start()
                        raise
                    self._set_connected(False)
                    self._diag.last_error = self._sanitize_ble_exc(exc)
                    self._logger.warning(
                        "MeshCoreSession %s initial connection unavailable; "
                        "starting degraded with recovery active: %s",
                        self._adapter_id,
                        self._diag.last_error,
                    )
                    self._ensure_recovery_task()
            finally:
                self._initial_start_task = None

        if self._stop_requested:
            return
        self._started = True
        self._start_receive_pump()
        self._logger.info(
            "MeshCoreSession %s started (mode=%s, connected=%s)",
            self._adapter_id,
            self._config.connection_type,
            self._diag.connected,
        )

    async def stop(self) -> None:
        """Disconnect from MeshCore and release all resources.

        Sets ``_stop_requested`` to prevent reconnect loops and clears
        the inbound message callback synchronously — before any await —
        so inbound delivery ends when stop() begins: SDK events
        dispatched during the unsubscribe, fetch-stop, or disconnect
        awaits cannot reach the retired callback, and a stop() cancelled
        mid-await still leaves the session inert.
        Idempotent — safe to call multiple times.
        """
        self._stop_requested = True
        self._message_callback = None
        # Initial setup runs in the caller task, not the recovery task. Retire
        # it before cleanup so it cannot reconnect after stop() returns.
        initial = self._initial_start_task
        if (
            initial is not None
            and initial is not asyncio.current_task()
            and not initial.done()
        ):
            initial.cancel()
            try:
                await initial
            except asyncio.CancelledError:
                pass
        if (
            not self._started
            and self._meshcore is None
            and self._reconnect_task is None
        ):
            self._message_callback = None
            self._set_connected(False)
            self._diag.reconnecting = False
            return

        # Cancel reconnect task if running.
        if self._reconnect_task is not None and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        self._reconnect_task = None

        await self._stop_receive_pump()

        # Unsubscribe all callbacks.
        await self._unsubscribe_all()

        # Disconnect SDK client.
        if self._meshcore is not None:
            # Stop auto message fetching if available.
            if hasattr(self._meshcore, "stop_auto_message_fetching"):
                try:
                    await asyncio.wait_for(
                        self._meshcore.stop_auto_message_fetching(),
                        timeout=_SDK_LIFECYCLE_TIMEOUT,
                    )
                except TimeoutError:
                    self._logger.warning(
                        "MeshCoreSession %s: timed out stopping auto_message_fetching",
                        self._adapter_id,
                    )
                except Exception as exc:
                    self._logger.debug(
                        "MeshCoreSession %s: error stopping auto_message_fetching: %s",
                        self._adapter_id,
                        self._sanitize_ble_exc(exc),
                    )
            try:
                await asyncio.wait_for(
                    self._meshcore.disconnect(), timeout=_SDK_LIFECYCLE_TIMEOUT
                )
            except Exception as exc:
                self._logger.warning(
                    "MeshCoreSession %s: error during disconnect: %s",
                    self._adapter_id,
                    self._sanitize_ble_exc(exc),
                )
            self._meshcore = None

        # BlueZ central-link teardown: the pinned SDK's disconnect() can
        # return while BlueZ still holds the central link (observed twice
        # live: clean runtime stop left the board ``Connected`` ~35s+
        # until an explicit disconnect, plausibly feeding the historical
        # intermittent "Failed to connect" on the next start).  Drop the
        # link deterministically — bounded, best-effort, BLE only, and
        # never allowed to fail the stop path itself.
        if self._config.connection_type == "ble" and self._config.ble_address:
            cleanup_task = asyncio.create_task(
                self._disconnect_stale_ble_client(self._config.ble_address)
            )
            try:
                done, _pending = await asyncio.wait(
                    {cleanup_task}, timeout=_SDK_LIFECYCLE_TIMEOUT
                )
            except asyncio.CancelledError:
                cleanup_task.cancel()
                cleanup_task.add_done_callback(self._consume_cleanup_task_result)
                raise
            if cleanup_task in done:
                self._consume_cleanup_task_result(cleanup_task)
            else:
                cleanup_task.cancel()
                cleanup_task.add_done_callback(self._consume_cleanup_task_result)
                self._logger.warning(
                    "MeshCoreSession %s: stale BlueZ cleanup exceeded %.1fs; "
                    "continuing shutdown",
                    self._adapter_id,
                    _SDK_LIFECYCLE_TIMEOUT,
                )

        self._set_connected(False)
        self._diag.reconnecting = False
        # Reset observability counters on stop so they don't leak across
        # lifecycles.  Contact/self-info data is per-session.
        self._diag.sdk_suggested_timeouts_used = 0
        self._diag.known_contact_count = 0
        self._diag.last_contact_update_time = None
        # Reset self-info diagnostics so stale values don't persist
        # across lifecycle boundaries.
        self._set_self_info(None, None, None)
        self._contact_retry_delays.clear()
        self._started = False
        self._logger.info("MeshCoreSession %s stopped", self._adapter_id)

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    async def send_text(
        self,
        contact_id: str,
        text: str,
        *,
        channel_index: int | None = None,
        before_native_send: Callable[[], Awaitable[None]] | None = None,
    ) -> str | None:
        """Send a text message via the MeshCore SDK.

        Parameters
        ----------
        contact_id:
            Destination identifier.  For DMs this is a pubkey prefix
            (hex string).  Ignored when *channel_index* is provided
            (channel message).
        text:
            Message body.
        channel_index:
            If provided, sends a channel message on this index instead
            of a DM.
        before_native_send:
            Optional one-use core grant for a planned packet. Called under the
            send lock after pacing, immediately before one SDK invocation.
            Guarded packets disable internal retries so ambiguous native
            acceptance cannot be replayed without durable evidence.

        Returns
        -------
        str | None
            A native message ID if available, else ``None``.

        Raises
        ------
        MeshCoreSendError
            On permanent failure or after exhausting retries.
        """
        if not self._diag.connected:
            recovering = self._started and not self._stop_requested
            raise MeshCoreSendError(
                "Session is not connected; native handoff has not occurred",
                transient=recovering,
            )

        if self._config.connection_type == "fake":
            # Fake mode — no real send.
            return None

        if before_native_send is None:
            return await self._send_real(contact_id, text, channel_index=channel_index)
        return await self._send_real(
            contact_id,
            text,
            channel_index=channel_index,
            before_native_send=before_native_send,
        )

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def self_info_snapshot(self) -> tuple[str | None, str | None, float | None]:
        """Read the three held identity scalars from one publication."""
        with self._self_info_lock:
            return (
                self._diag.device_name,
                self._diag.public_key_prefix,
                self._diag.radio_freq,
            )

    def _set_self_info(
        self, name: str | None, prefix: str | None, frequency: float | None
    ) -> None:
        # No SDK accesses or awaits under this lock.
        with self._self_info_lock:
            self._diag.device_name = name
            self._diag.public_key_prefix = prefix
            self._diag.radio_freq = frequency

    def diagnostics(self) -> dict[str, Any]:
        """Return a safe diagnostics snapshot.

        No secrets, private keys, or raw SDK internals are exposed.
        """
        name, prefix, frequency = self.self_info_snapshot()
        return {
            "connected": self._diag.connected,
            "reconnecting": self._diag.reconnecting,
            "reconnect_attempts": self._diag.reconnect_attempts,
            "last_message_time": (
                self._diag.last_message_time.isoformat()
                if self._diag.last_message_time
                else None
            ),
            "last_error": self._diag.last_error,
            "transient_delivery_failures": self._diag.transient_delivery_failures,
            "permanent_delivery_failures": self._diag.permanent_delivery_failures,
            "device_name": name,
            "public_key_prefix": prefix,
            "radio_freq": frequency,
            "mode": self._config.connection_type,
            "sdk_suggested_timeouts_used": self._diag.sdk_suggested_timeouts_used,
            "sdk_contact_timeout_count": len(self._contact_retry_delays),
            "known_contact_count": self._diag.known_contact_count,
            "last_contact_update_time": (
                self._diag.last_contact_update_time.isoformat()
                if self._diag.last_contact_update_time
                else None
            ),
        }

    # ------------------------------------------------------------------
    # Contact-label resolution (local, synchronous)
    # ------------------------------------------------------------------

    def resolve_contact_label(self, pubkey_prefix: str) -> str | None:
        """Resolve a known contact's advertised name by public-key prefix.

        Performs a synchronous lookup against the SDK's in-memory
        contacts store (populated by ``CONTACTS`` events).  No network
        calls are issued.  The MeshCore SDK exposes
        ``MeshCore.get_contact_by_key_prefix(prefix)`` which iterates
        the locally-cached ``contacts`` dict; each contact carries an
        ``adv_name`` field (human-readable advertised name).

        This method is the sole bridge between the SDK contact store and
        adapter-native enrichment.  It keeps SDK object access inside the
        session boundary and returns only a plain ``str | None``.

        Parameters
        ----------
        pubkey_prefix:
            Sender public-key prefix (hex string) from the inbound
            packet.  May be empty or ``None``; both are treated as
            "no lookup possible".

        Returns
        -------
        str | None
            The contact's ``adv_name`` (stripped), or ``None`` when the
            SDK client is unavailable, the prefix is empty, the sender
            is not a known contact, or the contact's ``adv_name`` is
            empty or non-string.  Never raises.
        """
        if not pubkey_prefix or self._meshcore is None:
            return None
        getter = getattr(self._meshcore, "get_contact_by_key_prefix", None)
        if getter is None:
            return None
        try:
            contact = getter(pubkey_prefix)
        except Exception:
            return None
        if not isinstance(contact, dict):
            return None
        name = contact.get("adv_name")
        if isinstance(name, str):
            name = name.strip()
            return name or None
        return None

    # ==================================================================
    # Private — real connection
    # ==================================================================

    async def _connection_attempt(self) -> None:
        """Own one bounded setup attempt and retire partial state before retry.

        Serializing setup prevents SDK disconnect callbacks from overlapping
        another connection attempt. The pinned SDK closes factory-created
        transports when its connect coroutine is cancelled.
        """
        async with self._connection_lock:
            if self._stop_requested:
                raise asyncio.CancelledError
            attempt_timeout = (
                _BLE_CONNECT_ATTEMPT_TIMEOUT
                if self._config.connection_type == "ble"
                else _CONNECT_ATTEMPT_TIMEOUT
            )
            deadline = asyncio.timeout(attempt_timeout)
            try:
                async with deadline:
                    await self._connect_real()
                if deadline.expired():
                    raise TimeoutError("native setup exceeded its deadline")
            except asyncio.CancelledError:
                self._stop_requested = True
                await self._cleanup_stale_sdk()
                self._set_connected(False)
                raise
            except Exception as exc:
                await self._cleanup_stale_sdk()
                self._set_connected(False)
                if deadline.expired():
                    raise MeshCoreConnectionError(
                        f"Native connection attempt exceeded {attempt_timeout:g}s"
                    ) from None
                raise exc

    def _ensure_recovery_task(self) -> None:
        """Keep a single cancellable recovery owner for the session."""
        if self._stop_requested:
            return
        if self._reconnect_task is None or self._reconnect_task.done():
            self._diag.reconnecting = True
            self._reconnect_task = asyncio.create_task(
                self._reconnect_loop(), name=f"meshcore-recovery:{self._adapter_id}"
            )
            self._reconnect_task.add_done_callback(self._log_task_exception)

    async def _cleanup_failed_start(self) -> None:
        """Full cleanup after a failed start().

        Clears all partial state so diagnostics and late SDK events
        cannot reference stale SDK objects or flags.  Preserves
        ``last_error`` for diagnostics.
        """
        self._message_callback = None
        await self._stop_receive_pump()
        self._set_connected(False)
        self._diag.reconnecting = False
        self._subscriptions.clear()
        self._contact_retry_delays.clear()
        if self._meshcore is not None:
            try:
                await asyncio.wait_for(
                    self._meshcore.disconnect(), timeout=_SDK_LIFECYCLE_TIMEOUT
                )
            except Exception as exc:
                self._logger.debug(
                    "MeshCoreSession %s: error during cleanup disconnect: %s",
                    self._adapter_id,
                    self._sanitize_ble_exc(exc),
                )
            self._meshcore = None

    async def _cleanup_stale_sdk(self) -> None:
        """Best-effort cleanup of a stale SDK client before reconnect.

        Unsubscribes existing event subscriptions, disconnects the old
        client, and clears internal handles.  Failures are logged but
        do NOT prevent the subsequent reconnect from proceeding.

        Safe to call when ``_meshcore`` is ``None`` (no-op).
        Does NOT clear ``_message_callback`` or set ``_stop_requested``.
        """
        if self._meshcore is None:
            return

        await self._stop_receive_pump()

        # Unsubscribe all registered callbacks.
        for sub in list(self._subscriptions):
            try:
                self._meshcore.unsubscribe(sub)
            except Exception as exc:
                self._logger.debug(
                    "MeshCoreSession %s: error unsubscribing during "
                    "stale cleanup: %s",
                    self._adapter_id,
                    exc,
                )
        self._subscriptions.clear()

        # Stop auto message fetching if the stale client supports it.
        # Uses the same SDK lifecycle timeout policy as stop().
        if hasattr(self._meshcore, "stop_auto_message_fetching"):
            try:
                await asyncio.wait_for(
                    self._meshcore.stop_auto_message_fetching(),
                    timeout=_SDK_LIFECYCLE_TIMEOUT,
                )
            except TimeoutError:
                self._logger.warning(
                    "MeshCoreSession %s: timed out stopping "
                    "auto_message_fetching during stale cleanup",
                    self._adapter_id,
                )
            except Exception as exc:
                self._logger.debug(
                    "MeshCoreSession %s: error stopping "
                    "auto_message_fetching during stale cleanup: %s",
                    self._adapter_id,
                    exc,
                )

        # Disconnect the old client.
        try:
            await asyncio.wait_for(
                self._meshcore.disconnect(), timeout=_SDK_LIFECYCLE_TIMEOUT
            )
        except Exception as exc:
            self._logger.debug(
                "MeshCoreSession %s: error disconnecting stale client: %s",
                self._adapter_id,
                self._sanitize_ble_exc(exc),
            )
        self._meshcore = None

    async def _disconnect_stale_ble_client(self, address: str) -> None:
        """Best-effort stale BlueZ disconnect for *address*.

        After a BLE disconnect, BlueZ may retain a cached "connected"
        entry.  Subsequent ``BleakClient(address)`` calls then hit
        ``le-connection-abort-by-local`` because BlueZ tries to reuse
        the stale entry.

        Unconditionally constructs a ``BleakClient`` for *address* and
        calls ``disconnect()`` without consulting a stale-client
        connection-state flag.  That flag is unreliable for a client that
        was never ``.connect()``-ed (it always reports disconnected), so
        gating on it would make this method a no-op in the exact scenario
        it is meant to handle.  Instead we call ``disconnect()`` directly
        — on a truly disconnected client it is a no-op or raises a
        harmless error, both of which are suppressed.

        IMPORTANT: we do NOT use ``async with BleakClient`` here
        because that would connect (and then disconnect), creating
        exactly the churn we want to avoid.
        Pattern adapted from mmrelay's ``_disconnect_ble_by_address()``.

        After the per-client cleanup, a bounded system-level
        ``bluetoothctl disconnect`` releases a link the board's firmware
        still holds from a previous session — a connection this process
        never owned, which no per-client call can terminate.  Absent
        binary and failures are best-effort no-ops.
        """
        try:
            from bleak import BleakClient

            stale = BleakClient(address, timeout=3.0)
            self._logger.debug(
                "MeshCoreSession %s: best-effort stale BlueZ " "disconnect for %s",
                self._adapter_id,
                address,
            )
            # Unconditional disconnect — see docstring rationale.  Keep this
            # hard-bounded: a wedged D-Bus/Bleak disconnect must never stall
            # adapter stop or the next connection attempt indefinitely.
            task = asyncio.create_task(stale.disconnect())
            try:
                done, _pending = await asyncio.wait(
                    {task}, timeout=_SDK_LIFECYCLE_TIMEOUT
                )
            except asyncio.CancelledError:
                task.cancel()
                task.add_done_callback(self._consume_cleanup_task_result)
                raise
            if done:
                await asyncio.gather(task, return_exceptions=True)
            else:
                task.cancel()
                task.add_done_callback(self._consume_cleanup_task_result)
                self._logger.debug(
                    "MeshCoreSession %s: stale BlueZ disconnect for %s "
                    "did not finish within %.1fs",
                    self._adapter_id,
                    address,
                    _SDK_LIFECYCLE_TIMEOUT,
                )
        except Exception:
            pass  # best-effort — proceed even if cleanup fails

        # System-level release: a board's companion firmware can hold its
        # single BLE connection slot after a previous session until the
        # host terminates the link.  The per-client cleanup above cannot
        # release a connection this process never owned; a BlueZ-level
        # disconnect can (same remedy as the lab pair harness, pattern
        # from mmrelay).  Absent binary, nonzero exit, and timeout are
        # all best-effort no-ops — a missing bluetoothctl must not break
        # hosts that connect fine without it.
        try:
            proc = await asyncio.create_subprocess_exec(
                "bluetoothctl",
                "disconnect",
                address,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except FileNotFoundError:
            self._logger.debug(
                "MeshCoreSession %s: bluetoothctl unavailable; skipping "
                "system-level stale disconnect for %s",
                self._adapter_id,
                address,
            )
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.CancelledError:
            await self._kill_subprocess(proc)
            raise
        except asyncio.TimeoutError:
            await self._kill_subprocess(proc)
            self._logger.debug(
                "MeshCoreSession %s: system-level disconnect for %s timed "
                "out; killed",
                self._adapter_id,
                address,
            )

    @staticmethod
    async def _kill_subprocess(proc: Any) -> None:
        """Kill a subprocess and reap it, tolerating an already-dead pid."""
        try:
            proc.kill()
        except Exception:
            pass  # already exited — nothing to signal
        try:
            await proc.wait()
        except Exception:
            pass  # best-effort reap

    @staticmethod
    def _consume_cleanup_task_result(task: asyncio.Future[Any]) -> None:
        """Own a detached best-effort cleanup task's terminal result."""
        try:
            task.exception()
        except (asyncio.CancelledError, Exception):
            pass

    def _sanitize_ble_exc(self, exc: BaseException) -> str:
        """Return a safe string representation of *exc* for BLE paths.

        When ``ble_pin`` is configured, raw exception text may include the
        PIN in args or string representation.  This method replaces it with
        the exception class name and a redaction notice so that logs,
        diagnostics, and error messages never leak the PIN.
        """
        if isinstance(exc, MeshCoreBluetoothUnavailableError):
            # Recreate the fixed message instead of trusting mutable SDK args.
            return str(MeshCoreBluetoothUnavailableError())
        if self._config.ble_pin is not None:
            return f"{type(exc).__name__}: [details redacted]"
        return str(exc)

    async def _find_ble_device(self, address_or_name: str) -> object | None:
        """Pre-scan for a BLE device matching *address_or_name*.

        On some Linux BlueZ stacks, ``BleakClient(address)`` fails
        with ``le-connection-abort-by-local`` while passing a
        ``BLEDevice`` from a live scan succeeds.

        Return the device, or ``None`` for an ordinary discovery miss. A typed
        host Bluetooth outage is transient but must reach the session recovery
        owner rather than triggering address-only connection attempts.
        """
        try:
            from bleak import BleakScanner

            def _match(d: object, adv: object) -> bool:
                d_addr: str = getattr(d, "address", "")
                adv_name: str = getattr(adv, "local_name", "") or ""
                if d_addr and d_addr.lower() == address_or_name.lower():
                    return True
                if adv_name and address_or_name.lower() in adv_name.lower():
                    return True
                return False

            device: object = await BleakScanner.find_device_by_filter(
                _match, timeout=5.0
            )
            return device
        except Exception as exc:
            if type(exc).__name__ == "BleakBluetoothNotAvailableError":
                raise MeshCoreBluetoothUnavailableError() from exc
            return None

    async def _create_ble_with_retries(
        self,
        meshcore_module: Any,
        address: str,
        ble_device: object | None,
        *,
        pin: str | None = None,
    ) -> Any:
        """Make one recovery factory call, or up to three initial probe calls.

        Both exceptions and ``None`` returns from ``create_ble()`` are
        treated as retryable failures. Initial setup may retry after a two-second
        delay and fresh scan. Background recovery delegates retry timing to its
        existing outer owner, avoiding nested bursts. Host unavailability skips
        the remaining initial attempts too.

        Returns the connected MeshCore instance on success.
        Raises :class:`MeshCoreConnectionError` after all attempts
        are exhausted.
        """
        _max_attempts = 1 if self._diag.reconnecting else 3
        _last_reason: str = "unknown"

        for attempt in range(1, _max_attempts + 1):
            try:
                _ble_kwargs: dict[str, Any] = {
                    "address": address,
                    "device": ble_device,
                }
                if pin is not None and not self._diag.reconnecting:
                    _ble_kwargs["pin"] = pin
                # MEDRE owns reconnect and always creates a fresh SDK client.
                _ble_kwargs["auto_reconnect"] = False
                self._logger.info(
                    "MeshCoreSession %s: BLE setup attempt %d/%d for %s (%s)",
                    self._adapter_id,
                    attempt,
                    _max_attempts,
                    address,
                    (
                        "discovered device"
                        if ble_device is not None
                        else "address fallback"
                    ),
                )
                result = await meshcore_module.MeshCore.create_ble(
                    **_ble_kwargs,
                )
                if result is not None:
                    return result

                # create_ble returned None — treat as retryable.
                _last_reason = "create_ble returned None"
                self._logger.debug(
                    "MeshCoreSession %s: BLE connection attempt %d/%d "
                    "returned None; retrying in 2s",
                    self._adapter_id,
                    attempt,
                    _max_attempts,
                )
            except Exception as exc:
                _last_reason = self._sanitize_ble_exc(exc)
                if type(exc).__name__ == "BleakBluetoothNotAvailableError":
                    raise MeshCoreBluetoothUnavailableError() from exc
                if self._permanent_setup_error(exc):
                    raise MeshCoreConnectionError(
                        f"BLE setup contract failed: {_last_reason}", transient=False
                    ) from exc
                if attempt == _max_attempts:
                    raise MeshCoreConnectionError(
                        f"BLE connection failed after {_max_attempts} "
                        f"attempt(s): {_last_reason}"
                    ) from exc
                self._logger.debug(
                    "MeshCoreSession %s: BLE connection attempt %d/%d "
                    "failed (%s); retrying in 2s",
                    self._adapter_id,
                    attempt,
                    _max_attempts,
                    self._sanitize_ble_exc(exc),
                )

            if attempt < _max_attempts:
                await asyncio.sleep(2.0)

                # Best-effort stale BlueZ cleanup BEFORE re-scan so
                # BlueZ adapter state is clean when the scanner looks
                # up the device.  Cleanup failure must not prevent the
                # subsequent re-scan and retry.
                try:
                    await self._disconnect_stale_ble_client(address)
                except Exception:
                    pass  # best-effort — proceed with re-scan

                # Re-scan: the device may have stopped and restarted
                # advertising in the meantime.  Best-effort: if bleak
                # is unavailable (not installed), skip the re-scan and
                # fall through with ble_device = None for address-only
                # fallback on the next create_ble().
                try:
                    ble_device = await self._find_ble_device(address)
                except MeshCoreConnectionError:
                    raise
                except Exception:
                    ble_device = None

        raise MeshCoreConnectionError(
            f"No response from MeshCore node after "
            f"{_max_attempts} BLE attempt(s): {_last_reason}"
        )

    async def _connect_real(self) -> None:
        """Create a real SDK client, connect, and subscribe to events."""
        if not HAS_MESHCORE:
            raise MeshCoreConnectionError(
                "meshcore SDK not installed; pip install 'medre-adapter-meshcore' "
                "or use connection_type='fake'"
            )

        # Deferred import — the SDK is only touched inside this method.
        mc = cast(_MeshCoreModule, importlib.import_module("meshcore"))
        # Retain the module so the backlog drain can read the SDK event
        # types without importing the SDK a second time.
        self._sdk_module = mc

        # Clean up any stale SDK client from a previous connection
        # (e.g. before reconnect).  On first connect _meshcore is None
        # so this is a no-op.
        await self._cleanup_stale_sdk()

        try:
            if self._config.connection_type == "tcp":
                self._meshcore = await mc.MeshCore.create_tcp(
                    self._config.host or "localhost",
                    self._config.port if self._config.port is not None else 4000,
                    auto_reconnect=False,
                )
                if self._meshcore is None:
                    raise MeshCoreConnectionError(
                        "No response from MeshCore node (TCP)"
                    )
            elif self._config.connection_type == "serial":
                # Construct the serial client MEDRE-side instead of calling
                # MeshCore.create_serial.  The pinned SDK's factory retries
                # once with ``dtr=not dtr`` when the first connect produces
                # no protocol response (e.g. a slow board boot), re-opening
                # the port with DTR asserted — and SerialConnection latches
                # ``transport.serial.dtr`` in connection_made, so the line
                # is asserted before any post-hoc check could run.
                #
                # Deassert DTR/RTS unconditionally: the pinned SDK defaults
                # dtr=True, which holds IO0 low on boards whose USB-UART
                # auto-download circuit drives it (e.g. LilyGO T-LoRa
                # V2.1).  IO0 held low makes the companion boot into the ROM
                # bootloader or MeshCore "CLI rescue" mode, killing the
                # serial protocol.  Deasserted is the safe state for every
                # observed board, so MEDRE never runs the inversion
                # heuristic and a failed handshake raises for the normal
                # startup classification/retry handling instead.
                # cx_dly mirrors create_serial's default (0.1), which differs
                # from SerialConnection's own default (0.2).
                connection = mc.SerialConnection(
                    self._config.serial_port or "/dev/ttyUSB0",
                    self._config.serial_baudrate,
                    cx_dly=0.1,
                    rts=False,
                    dtr=False,
                )
                # Retain ownership before awaiting connect().  The SDK may
                # open the serial transport and then raise while completing
                # its protocol handshake; keeping the client on the session
                # lets the common failure cleanup reliably disconnect that
                # partially-initialised transport.
                self._meshcore = mc.MeshCore(connection, auto_reconnect=False)
                connect_result = await self._meshcore.connect()
                if connect_result is None:
                    raise MeshCoreConnectionError(
                        "No response from MeshCore node (serial)"
                    )
            elif self._config.connection_type == "ble":
                _addr = self._config.ble_address or ""

                # Guard: empty address matches any BLE device in the
                # substring filter, so fail fast instead.
                if not _addr:
                    raise MeshCoreConnectionError("BLE address not configured")

                # -- Stale BlueZ cleanup (reconnect safety) --
                await self._disconnect_stale_ble_client(_addr)

                # Let the BLE adapter settle before scanning/connecting.
                await asyncio.sleep(0.5)

                # Pre-scan for the BLE device to obtain a BLEDevice.
                ble_device = await self._find_ble_device(_addr)

                # -- BLE connection attempt with retry --
                self._meshcore = await self._create_ble_with_retries(
                    mc,
                    _addr,
                    ble_device,
                    pin=self._config.ble_pin,
                )
            else:
                raise MeshCoreConnectionError(
                    f"Unsupported connection_type: " f"{self._config.connection_type!r}"
                )

        except MeshCoreConnectionError as exc:
            # Preserve safe error diagnostics before cleanup.  For BLE
            # paths the exception text is already sanitized by
            # _sanitize_ble_exc() in helper methods; do not reintroduce
            # raw SDK exception text (no-secret discipline for ble_pin).
            _safe_reason = (
                self._sanitize_ble_exc(exc)
                if self._config.connection_type == "ble"
                else str(exc)
            )
            self._diag.last_error = _safe_reason
            # Clean up partially-initialised SDK client on failure.
            if self._meshcore is not None:
                try:
                    await asyncio.wait_for(
                        self._meshcore.disconnect(), timeout=_SDK_LIFECYCLE_TIMEOUT
                    )
                except Exception as disconnect_exc:
                    self._logger.debug(
                        "MeshCoreSession %s: error during cleanup disconnect: %s",
                        self._adapter_id,
                        self._sanitize_ble_exc(disconnect_exc),
                    )
                self._meshcore = None
            raise
        except Exception as exc:
            # Clean up partially-initialised SDK client on failure.
            if self._meshcore is not None:
                try:
                    await asyncio.wait_for(
                        self._meshcore.disconnect(), timeout=_SDK_LIFECYCLE_TIMEOUT
                    )
                except Exception as disconnect_exc:
                    self._logger.debug(
                        "MeshCoreSession %s: error during cleanup disconnect: %s",
                        self._adapter_id,
                        self._sanitize_ble_exc(disconnect_exc),
                    )
                self._meshcore = None
            _safe_reason = (
                self._sanitize_ble_exc(exc)
                if self._config.connection_type == "ble"
                else str(exc)
            )
            self._diag.last_error = _safe_reason
            raise MeshCoreConnectionError(
                f"Failed to connect ({self._config.connection_type}): "
                f"{_safe_reason}",
                transient=not self._permanent_setup_error(exc),
            ) from exc

        # Connection succeeded — now subscribe to events.
        # If subscription fails, clean up the client before propagating.
        if not self._diag.reconnecting:
            self._diag.reconnect_attempts = 0
        try:
            self._subscribe_events(mc)
        except Exception as exc:
            # Subscription failure after successful client creation.
            self._diag.last_error = self._sanitize_ble_exc(exc)
            if self._meshcore is not None:
                try:
                    await asyncio.wait_for(
                        self._meshcore.disconnect(), timeout=_SDK_LIFECYCLE_TIMEOUT
                    )
                except Exception as disconnect_exc:
                    self._logger.debug(
                        "MeshCoreSession %s: error during cleanup disconnect: %s",
                        self._adapter_id,
                        self._sanitize_ble_exc(disconnect_exc),
                    )
                self._meshcore = None
            self._subscriptions.clear()
            raise MeshCoreConnectionError(
                f"Failed to subscribe to events: {self._diag.last_error}",
                transient=not self._permanent_setup_error(exc),
            ) from exc

        # MeshCore.create_*() already performs connect() and its required
        # send_appstart(). Do not send APP_START a second time here. The SDK's
        # internal SELF_INFO subscription is installed before connect(); its
        # public self_info snapshot may already reflect that handshake, and
        # subsequent SELF_INFO callbacks refresh diagnostics.
        self._capture_self_info(getattr(self._meshcore, "self_info", None))

        # The handshake/subscription has succeeded. A disconnect during the
        # following await must stay observable rather than being overwritten.
        self._set_connected(True)

        # The pinned SDK exposes the notification and native fetch primitives.
        # Do not start its auto-fetch loop as a second owner of GET_NEXT_MSG.
        event_types = mc.EventType
        if hasattr(event_types, "MESSAGES_WAITING") and hasattr(
            event_types, "NO_MORE_MSGS"
        ):
            client = self._meshcore
            wake = asyncio.Event()
            wake.set()  # An already queued backlog need not send another push.
            self._fetch_wake = wake

            async def on_waiting(_event: Any) -> None:
                if self._meshcore is client and self._fetch_wake is wake:
                    wake.set()

            self._subscriptions.append(
                client.subscribe(event_types.MESSAGES_WAITING, on_waiting)
            )
        else:
            # Compatibility for SDKs without the explicit notification API.
            # This path uses the SDK alone; never start a competing drain.
            try:
                if hasattr(self._meshcore, "start_auto_message_fetching"):
                    await asyncio.wait_for(
                        self._meshcore.start_auto_message_fetching(),
                        timeout=_SDK_LIFECYCLE_TIMEOUT,
                    )
            except Exception as exc:
                self._logger.debug(
                    "MeshCoreSession %s: legacy auto-fetch startup failed: %s",
                    self._adapter_id,
                    type(exc).__name__,
                )

        if not self._diag.connected:
            raise MeshCoreConnectionError("Native transport disconnected during setup")

    def _start_receive_pump(self) -> None:
        """Schedule only after the connection setup owner has returned."""
        if self._fetch_wake is None or self._stop_requested or not self._diag.connected:
            return
        if self._fetch_task is None or self._fetch_task.done():
            self._fetch_task = asyncio.create_task(
                self._receive_pump(self._meshcore, self._fetch_wake),
                name=f"meshcore-receive:{self._adapter_id}",
            )
            self._fetch_task.add_done_callback(self._log_task_exception)

    async def _stop_receive_pump(self) -> None:
        self._fetch_wake = None
        task, self._fetch_task = self._fetch_task, None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _receive_pump(self, client: Any, wake: asyncio.Event) -> None:
        """Own initial and notification-driven fetches on this SDK connection.

        The event is cleared before draining so notifications that arrive during
        a request (including NO_MORE_MSGS) cannot be lost. Each request is
        serialized with native sends: the SDK correlates some replies by type,
        not by command ID — one fetch runs at a time, and the next fetch is
        issued only after the previous one returned.

        A fetch window that elapses with no inbound traffic parks the pump
        at once: the node's fetch is a destructive pop, so the window may
        have ended because the node popped a message whose frame was lost,
        and every further fetch would pop another message into a link that
        is not returning what it pops. The reserved capacity permit is
        released, as any interrupted drain's is, and a frame that pops
        after the window still publishes through the SDK event callback —
        which wakes the pump, so a slow-but-delivering link resumes its
        interrupted drain on the same connection. A window that elapses
        while inbound traffic is still arriving — a late frame, or a
        notification — means the link is delivering, however slowly: the
        pump keeps the connection and re-issues at a bounded rate. A parked
        wait is bounded by the idle-link liveness probe: if nothing wakes
        the pump within _PROBE_IDLE_INTERVAL, it asks the node one cheap
        local query; a probe that times out or errors is a silent-dead link
        and retires the connection. An answered probe re-arms exactly one
        fetch for a drain the elapsed window interrupted; a drain that
        ended with the node's NO_MORE_MSGS is complete and an answered
        probe never fetches for it. Only a real transport or protocol
        failure retires the connection.

        A connection that follows a failure-driven retirement — its
        predecessor died of a fetch or probe failure, or of a transport loss
        while a fetch round was in flight — issues no fetch until one probe
        is answered: the node's fetch is a destructive pop, and a link whose
        server-to-client direction is still broken would swallow one queued
        message per fetch. A fresh start or a reconnect after a clean
        disconnect fetches immediately.

        A binary channel-data frame is popped but never relayed: the
        adapter publishes text messages only, and the node's FIFO queue
        must still advance past the frame or every message queued behind
        it would be stuck. Such a round releases its reserved permit —
        nothing will publish from the frame — logs one DEBUG line, and
        continues the drain immediately; the pop is accounted for, so it
        is never mistaken for lost traffic, and it neither stalls the
        drain nor retires the connection.
        """
        types = self._sdk_module.EventType
        message_types = {
            types.CONTACT_MSG_RECV,
            types.CHANNEL_MSG_RECV,
        }
        channel_data_type = getattr(types, "CHANNEL_DATA_RECV", None)
        idle_rounds = 0
        # A notification is consumed for pacing independently of the
        # extra drain needed when the empty-queue response races a push.
        pending_notification = False
        # A new connection starts with no drain history: nothing has been
        # interrupted yet.
        self._pump_drain_incomplete = False
        self._pump_parked = False
        try:
            if (
                self._meshcore is client
                and self._diag.connected
                and not self._stop_requested
                and self._receive_failure_pending
            ):
                # This connection follows a failure-driven retirement, so
                # the server-to-client direction is unproven: fetching
                # into it would pop a queued message the link may never
                # return. Earn the right to fetch with one liveness probe;
                # an unanswered probe raises into the retire handler below
                # without issuing any fetch. A clean start or a
                # clean-disconnect reconnect never arrives here.
                if getattr(client.commands, "send_device_query", None) is None:
                    # Nothing to probe with on this SDK: fetch as before
                    # rather than gate the drain forever.
                    self._receive_failure_pending = False
                else:
                    if not await self._probe_idle_link(client):
                        return
            while (
                self._meshcore is client
                and self._diag.connected
                and not self._stop_requested
            ):
                if not await self._await_wake_with_probe(client, wake):
                    break
                wake.clear()
                pending_notification = False
                while (
                    self._meshcore is client
                    and self._diag.connected
                    and not self._stop_requested
                ):
                    self._pump_parked = False
                    if self._wait_for_receive_capacity is not None:
                        await self._wait_for_receive_capacity()
                    async with self._send_lock:
                        if (
                            self._meshcore is not client
                            or not self._diag.connected
                            or self._stop_requested
                        ):
                            return
                        # A late callback may consume a reserved permit while
                        # this pump waits behind a send. Recheck without
                        # awaiting inside the lock; wait for capacity outside.
                        permit_ready = getattr(self, "_receive_permit_ready", None)
                        if permit_ready is not None and not permit_ready():
                            continue
                        # Keep the pending extra drain but consume each push
                        # as liveness evidence at most once.
                        notified = wake.is_set()
                        pending_notification |= notified
                        wake.clear()
                        inbound_before = self._fetch_inbound_seen
                        self._fetch_round_in_flight = True
                        try:
                            async with asyncio.timeout(_FETCH_ROUND_TIMEOUT + 0.5):
                                event = await client.commands.get_msg(
                                    timeout=_FETCH_ROUND_TIMEOUT
                                )
                        finally:
                            self._fetch_round_in_flight = False
                    notified = notified or wake.is_set()
                    pending_notification |= notified
                    wake.clear()
                    if event.type == types.NO_MORE_MSGS:
                        idle_rounds = 0
                        self._pump_silent_rounds = 0
                        self._pump_drain_incomplete = False
                        self._release_fetch_permit()
                        # The drain is complete: the pump is parked until
                        # the next notification.
                        self._pump_parked = True
                        if pending_notification:
                            wake.set()
                        break
                    if self._fetch_window_elapsed(event, types):
                        idle_rounds += 1
                        if self._fetch_inbound_seen != inbound_before or notified:
                            # Late frames and notifications arriving between
                            # rounds prove the link is slow but delivering;
                            # re-issuing at a bounded rate keeps draining it.
                            self._pump_silent_rounds = 0
                            self._logger.debug(
                                "MeshCoreSession %s: fetch window elapsed with "
                                "no event, but inbound traffic is arriving "
                                "(%d consecutive idle round(s)); re-issuing "
                                "at a bounded rate on the same connection",
                                self._adapter_id,
                                idle_rounds,
                            )
                            if idle_rounds > _FETCH_IDLE_IMMEDIATE_REISSUES:
                                # Pace further re-issues: wait for a
                                # notification or the floor delay, so a slow
                                # link neither spins nor strands its backlog.
                                try:
                                    await asyncio.wait_for(
                                        wake.wait(),
                                        timeout=_FETCH_IDLE_REISSUE_DELAY,
                                    )
                                except asyncio.TimeoutError:
                                    pass
                            await asyncio.sleep(0)
                            continue
                        # No inbound traffic at all: the window may have
                        # ended because the node popped a message whose
                        # frame was lost, and every further fetch would pop
                        # another message into a link that is not returning
                        # what it pops. Park at once instead. Keep the
                        # silent round where a disconnect handler can see
                        # it: this round popped a message the link never
                        # returned, however the connection ends. Traffic or
                        # a notification resumes the drain; so does one
                        # answered liveness probe, because the last drain
                        # ended without the node's NO_MORE_MSGS.
                        idle_rounds = 0
                        self._pump_silent_rounds += 1
                        self._pump_drain_incomplete = True
                        self._release_fetch_permit()
                        # Parked: a frame publishing through the event
                        # callback now wakes the pump.
                        self._pump_parked = True
                        self._logger.debug(
                            "MeshCoreSession %s: fetch window elapsed with "
                            "no inbound traffic (%d unanswered round(s)); "
                            "parking the receive pump until traffic or an "
                            "answered probe",
                            self._adapter_id,
                            self._pump_silent_rounds,
                        )
                        break
                    if event.type is channel_data_type:
                        # Popped from the node's FIFO queue but never
                        # relayed: the adapter publishes text messages
                        # only. Release the round's permit — nothing
                        # will publish from this frame — and continue
                        # the drain; one DEBUG line accounts for the
                        # pop so it cannot be mistaken for lost
                        # traffic or a failure.
                        idle_rounds = 0
                        self._release_fetch_permit()
                        self._logger.debug(
                            "MeshCoreSession %s: fetched a binary "
                            "channel-data frame; not relayed, the "
                            "round's admission permit is released and "
                            "the drain continues",
                            self._adapter_id,
                        )
                        await asyncio.sleep(0)
                        continue
                    if event.type not in message_types:
                        raise RuntimeError(
                            "native fetch returned "
                            f"{self._fetch_outcome_detail(event, types)}"
                            " instead of a message or empty queue"
                        )
                    idle_rounds = 0
                    # Publication stays in the SDK event callback; yield to
                    # its dispatcher and to pending sends before fetching again.
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if (
                self._meshcore is client
                and self._diag.connected
                and not self._stop_requested
            ):
                self._logger.warning(
                    "MeshCoreSession %s: receive pump failed (%s: %s); retiring connection",
                    self._adapter_id,
                    type(exc).__name__,
                    exc,
                )
                # The diagnostics surface carries why the pump retired:
                # a fetch or probe failure names the SDK's ERROR payload
                # reason in its message, a transport exception names
                # itself.
                self._diag.last_error = self._sanitize_ble_exc(exc)
                # The retirement is failure-driven (a fetch or probe
                # failed): the next connection must prove the link
                # answers before its first fetch.
                self._receive_failure_pending = True
                self._set_connected(False)
                self._ensure_recovery_task()
        finally:
            self._release_fetch_permit()

    async def _await_wake_with_probe(self, client: Any, wake: asyncio.Event) -> bool:
        """Wait for the pump's wakeup, probing the idle link when it is quiet.

        A parked wait never expires on its own, so a link that died
        silently would hold the pump parked while the session looks
        connected. This wait expires into one liveness probe every
        ``_PROBE_IDLE_INTERVAL`` while the quiet continues; a failed probe
        raises so the pump's ordinary retire path — the same handler a
        fetch failure uses — runs the existing recovery loop. An answered
        probe proves the link delivers, so it
        re-arms exactly one fetch for a drain an elapsed window interrupted
        (``_pump_drain_incomplete``): if the re-armed fetch's window elapses
        with no traffic the pump parks again, and the next answered probe
        re-arms it again. A drain that ended with the node's NO_MORE_MSGS is
        complete; an answered probe never fetches for it — the notification
        does.

        Returns ``True`` when the pump may issue a fetch now — the wake
        fired, or an answered probe re-armed an interrupted drain — and
        ``False`` when the connection ended.
        """
        while (
            self._meshcore is client
            and self._diag.connected
            and not self._stop_requested
        ):
            if wake.is_set():
                return True
            try:
                await asyncio.wait_for(wake.wait(), timeout=_PROBE_IDLE_INTERVAL)
                return True
            except TimeoutError:
                pass
            answered = await self._probe_idle_link(client)
            if answered and self._pump_drain_incomplete:
                return True
        return False

    async def _probe_idle_link(self, client: Any) -> bool:
        """Confirm a parked link (or an unproven reconnect) still answers.

        The pinned SDK's device query is answered by the companion over the
        control connection itself — no radio traffic — and its
        ``DEVICE_INFO`` reply type cannot be confused with a message event,
        so a late or stray reply can never be mistaken for fetched traffic.
        It runs under the send lock: one probe at a time, never concurrent
        with a fetch or a send (the SDK correlates some replies by event
        type, not request identity). The probe gets the same
        ``_PROBE_IDLE_INTERVAL`` budget as the SDK's own default command
        timeout; only an unanswered query (the SDK's own elapsed-wait
        report) or a local failure to send the query is treated as a
        silent-dead link: raise so the pump's existing retire path runs.
        A reply the node itself sent — a ``DEVICE_INFO`` answer, the
        wire's numeric error frame, or bytes that arrived but failed to
        parse — proves the link is alive and leaves the connection alone
        — and clears the failure-driven gate, so a reconnect that
        followed a failure may fetch. Stop, retire, and reconnect cancel
        the probe with the pump task, and the SDK unsubscribes the
        waiter on cancellation, so a late reply matches no subscription.

        Returns ``True`` when a probe ran and a reply that is neither an
        elapsed wait nor a local send failure came back — the link is
        proven to deliver — and ``False`` when no probe ran (the SDK has
        no device query, or the connection ended first), so the caller
        must not treat quiet as an answer.
        """
        device_query = getattr(client.commands, "send_device_query", None)
        if device_query is None:
            # Without the SDK's local query there is nothing safe to probe
            # with; the wait repeats without a probe and cannot retire a
            # silent link.
            return False
        async with self._send_lock:
            if (
                self._meshcore is not client
                or not self._diag.connected
                or self._stop_requested
            ):
                return False
            self._logger.debug(
                "MeshCoreSession %s: probing the idle receive link",
                self._adapter_id,
            )
            try:
                # The SDK's send() waits _PROBE_IDLE_INTERVAL internally (its
                # default command timeout) and then reports an ERROR event.
                # This outer guard sits strictly above that bound, mirroring
                # the fetch round guard: it exists only to bound a hung
                # writer, never to race the SDK's own timeout.
                async with asyncio.timeout(_PROBE_IDLE_INTERVAL + 0.5):
                    result = await device_query()
            except TimeoutError as exc:
                raise RuntimeError("idle-link liveness probe timed out") from exc
            if (
                self._meshcore is not client
                or not self._diag.connected
                or self._stop_requested
            ):
                return False
        types = self._sdk_module.EventType
        result_type = getattr(result, "type", None)
        payload = getattr(result, "payload", None)
        reason = payload.get("reason") if isinstance(payload, dict) else None
        # The pinned SDK reports three shapes on this query's ERROR
        # event: an elapsed wait carries a reason of "timeout" or
        # "no_event_received"; a local failure to send or wait carries a
        # string under "error"; a reply the node itself sent carries an
        # int under "error_code" (the wire's error frame), and bytes
        # that arrived but failed to parse carry a non-elapsed reason.
        # Only an elapsed wait or a local failure to send means no reply
        # came from the node; any other reply proves the link is alive
        # and clears the failure-driven gate.
        if result_type is None or result_type not in (
            getattr(types, "DEVICE_INFO", None),
            types.ERROR,
        ):
            raise RuntimeError("idle-link liveness probe returned an unexpected result")
        if result_type == types.ERROR and (
            reason in _PROBE_UNANSWERED_REASONS
            or (
                isinstance(payload, dict)
                and "error" in payload
                and type(payload["error"]) is not int
                and type(payload.get("error_code")) is not int
            )
        ):
            raise RuntimeError(f"idle-link liveness probe unanswered ({reason})")
        self._receive_failure_pending = False
        self._logger.debug(
            "MeshCoreSession %s: idle-link probe answered %s; the link is alive",
            self._adapter_id,
            getattr(result_type, "name", result_type),
        )
        return True

    @staticmethod
    def _fetch_window_elapsed(event: Any, types: Any) -> bool:
        """Report whether the fetch returned because the window was idle.

        ``meshcore`` maps an elapsed command-response wait to an ``ERROR``
        event whose payload reason is ``no_event_received`` (or ``timeout``).
        Those describe transport uncertainty, not a remote rejection, so the
        pump treats them as an empty window rather than a failure.
        """
        if event.type != types.ERROR:
            return False
        payload = getattr(event, "payload", None)
        if not isinstance(payload, dict):
            return False
        return payload.get("reason") in _TRANSIENT_SDK_SEND_REASONS

    @staticmethod
    def _fetch_outcome_detail(event: Any, types: Any) -> str:
        """Summarize a non-message fetch outcome for the retire log."""
        if event.type == types.ERROR:
            payload = getattr(event, "payload", None)
            if isinstance(payload, dict):
                reason = payload.get("reason") or payload.get("error")
                if reason is not None:
                    return f"ERROR({reason})"
            return "ERROR"
        return str(getattr(event.type, "name", event.type))

    def _release_fetch_permit(self) -> None:
        """Release this pump's unconsumed inbound-capacity permit.

        The pump holds at most one permit, acquired before a fetch and
        consumed by the fetched message's publication. Drains that end
        without a fetched message — an empty queue, a retirement, stop —
        release the permit here and on the pump's exit path.
        """
        release = self._release_receive_permit
        if release is None:
            return
        try:
            release()
        except Exception as exc:
            self._logger.debug(
                "MeshCoreSession %s: fetch permit release failed: %s",
                self._adapter_id,
                exc,
            )

    def _subscribe_events(self, mc: Any) -> None:
        """Subscribe to SDK event types for inbound messages + disconnects.

        Subscriptions:
          - ``CONTACT_MSG_RECV``: inbound DM messages
          - ``CHANNEL_MSG_RECV``: inbound channel messages
          - ``DISCONNECTED``: connection loss detection
          - ``CONTACTS``: contact list updates (diagnostics only)
          - ``SELF_INFO``: self-info updates (diagnostics only)

        Contact/self-info subscriptions are **diagnostics-only** — no
        topology canonical events are emitted, and contact lists are not
        stored in canonical metadata.  Only aggregate counts and
        timestamps are recorded.
        """
        client = self._meshcore
        if client is None:
            return

        def subscribe(
            event_type: Any, callback: Callable[[Any], Awaitable[None]]
        ) -> None:
            async def on_event(event: Any) -> None:
                # The SDK may already have scheduled this callback when
                # we unsubscribe. Only the current client owns this pump.
                if self._meshcore is client and not self._stop_requested:
                    await callback(event)

            self._subscriptions.append(client.subscribe(event_type, on_event))

        subscribe(mc.EventType.CONTACT_MSG_RECV, self._on_sdk_event)
        subscribe(mc.EventType.CHANNEL_MSG_RECV, self._on_sdk_event)
        subscribe(mc.EventType.DISCONNECTED, self._on_disconnect_event)
        if hasattr(mc.EventType, "CONTACTS"):
            subscribe(mc.EventType.CONTACTS, self._on_contacts_event)
        if hasattr(mc.EventType, "SELF_INFO"):
            subscribe(mc.EventType.SELF_INFO, self._on_self_info_event)

    def _capture_self_info(self, self_info: Any) -> None:
        """Extract safe diagnostics from an SDK self-info snapshot or event.

        ``MeshCore.create_*()`` performs the initial ``send_appstart()``. The
        SDK updates its public ``self_info`` mapping from SELF_INFO dispatch,
        which may complete before or after the factory returns. Event objects
        are still accepted for callback/test compatibility.
        """
        payload: dict[str, Any] | None = None
        if hasattr(self_info, "payload") and isinstance(self_info.payload, dict):
            payload = self_info.payload
        elif isinstance(self_info, dict):
            payload = self_info

        # Build a complete replacement before publishing it. Partial payloads
        # clear unavailable facts instead of merging different announcements.
        name_value = payload.get("name") if payload is not None else None
        name = name_value if isinstance(name_value, str) else None

        # Public key prefix (first 6 hex bytes / 12 hex chars).
        prefix: str | None = None
        pubkey = payload.get("public_key") if payload is not None else None
        if isinstance(pubkey, str) and len(pubkey) >= 12:
            candidate = pubkey[:12].lower()
            if all(char in "0123456789abcdef" for char in candidate):
                prefix = candidate
        elif isinstance(pubkey, bytes) and len(pubkey) >= 6:
            prefix = pubkey[:6].hex()

        # Radio frequency (if present).
        frequency: float | None = None
        freq = payload.get("freq") if payload is not None else None
        if freq is None and payload is not None:
            freq = payload.get("radio_freq")
        if isinstance(freq, (int, float)) and not isinstance(freq, bool):
            try:
                f = float(freq)
            except OverflowError:
                pass
            else:
                if math.isfinite(f) and f > 0.0:
                    frequency = f
        self._set_self_info(name, prefix, frequency)

    async def _unsubscribe_all(self) -> None:
        """Unsubscribe all registered callbacks."""
        if self._meshcore is not None:
            for sub in self._subscriptions:
                try:
                    self._meshcore.unsubscribe(sub)
                except Exception:
                    pass
        self._subscriptions.clear()

    # ------------------------------------------------------------------
    # SDK event handlers
    # ------------------------------------------------------------------

    async def _on_sdk_event(self, event: Any) -> None:
        """Handle an inbound SDK event.

        Extracts the payload dict from the SDK ``Event`` object and
        forwards it to the registered message callback.  The payload
        is a plain dict — no SDK objects leak into the adapter layer.
        """
        # The receive pump counts these deliveries to tell an idle-but-
        # delivering link from a silent one; bump before any early exit.
        self._fetch_inbound_seen += 1
        # A frame that pops after its fetch window has elapsed still
        # publishes here — and it proves the link delivers, so it wakes a
        # parked pump: a slow-but-delivering link then resumes its
        # interrupted drain on the same connection instead of waiting for
        # a new notification. A frame published while a drain is running
        # sets no wake: that drain fetches again anyway, and a wake set
        # now would only survive as a stale one after the drain's final
        # empty-queue round.
        if self._pump_parked and self._fetch_wake is not None:
            self._fetch_wake.set()

        if self._message_callback is None:
            return

        try:
            # SDK Event has .payload (dict) and .type (EventType).
            payload: dict[str, Any]
            if isinstance(event, dict):
                payload = event
            elif hasattr(event, "payload"):
                payload = dict(event.payload) if isinstance(event.payload, dict) else {}
            else:
                payload = {}

            self._diag.last_message_time = datetime.now(timezone.utc)
            result = self._message_callback(payload)
            if asyncio.iscoroutine(result):
                task = asyncio.ensure_future(result)
                task.add_done_callback(self._log_task_exception)
                # Yield once so short callbacks can complete within this
                # turn.  Long-running callbacks continue in the background
                # after the first internal await point.
                await asyncio.sleep(0)
        except Exception as exc:
            self._logger.exception(
                "MeshCoreSession %s: error processing inbound event: %s",
                self._adapter_id,
                exc,
            )

    def _log_task_exception(self, task: asyncio.Task[Any]) -> None:
        """Done callback for fire-and-forget tasks — logs exceptions
        to prevent 'Task exception was never retrieved'."""
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._logger.warning(
                "MeshCoreSession %s: unhandled exception in background task: %s",
                self._adapter_id,
                exc,
            )

    async def _on_disconnect_event(self, event: Any) -> None:
        """Handle SDK disconnect event — trigger reconnect if appropriate."""
        # A transport loss is unclean when this connection had destructive
        # pops whose frames never arrived: a fetch round still in flight,
        # or already-completed fetch rounds that came back with no event at
        # all. The next connection must prove it can deliver before it
        # fetches. A disconnect with no fetch in flight and no unanswered
        # fetch rounds popped nothing and stays clean — the next connection
        # fetches immediately.
        if self._fetch_round_in_flight or self._pump_silent_rounds > 0:
            self._receive_failure_pending = True
        self._set_connected(False)
        self._logger.warning("MeshCoreSession %s: SDK disconnected", self._adapter_id)

        if self._stop_requested or not self._started:
            # Initial setup owns its result; it starts recovery explicitly
            # after any failure, rather than racing another setup task.
            return

        self._ensure_recovery_task()

    async def _on_contacts_event(self, event: Any) -> None:
        """Handle SDK CONTACTS event — diagnostics-only contact list update.

        Per meshcore_py, the CONTACTS event payload contains a list/dict
        of contacts.  We record the count and update timestamp for
        observability.  **No** topology canonical events are emitted,
        and contact lists are not stored in canonical metadata.
        """
        try:
            payload: dict[str, Any]
            if isinstance(event, dict):
                payload = event
            elif hasattr(event, "payload") and isinstance(event.payload, dict):
                payload = event.payload
            else:
                payload = {}

            # Extract contact count.  The SDK may return a list or a dict.
            contacts_raw = payload.get("contacts")
            if isinstance(contacts_raw, (list, tuple)):
                self._diag.known_contact_count = len(contacts_raw)
            elif isinstance(contacts_raw, dict):
                self._diag.known_contact_count = len(contacts_raw)
            # If neither, leave count unchanged.

            self._diag.last_contact_update_time = datetime.now(timezone.utc)
        except Exception as exc:
            self._logger.debug(
                "MeshCoreSession %s: error processing CONTACTS event: %s",
                self._adapter_id,
                exc,
            )

    async def _on_self_info_event(self, event: Any) -> None:
        """Handle SDK SELF_INFO event — diagnostics-only self-info update.

        Per meshcore_py, the SELF_INFO event payload contains fields like
        ``name``, ``public_key``, and radio parameters.  We delegate to
        :meth:`_capture_self_info` for safe extraction.
        """
        try:
            payload: dict[str, Any] | None = None
            if isinstance(event, dict):
                payload = event
            elif hasattr(event, "payload") and isinstance(event.payload, dict):
                payload = event.payload
            if payload is not None:
                self._capture_self_info(payload)
        except Exception as exc:
            self._logger.debug(
                "MeshCoreSession %s: error processing SELF_INFO event: %s",
                self._adapter_id,
                exc,
            )

    # ------------------------------------------------------------------
    # Reconnect
    # ------------------------------------------------------------------

    async def _reconnect_loop(self) -> None:
        """Bounded exponential backoff reconnect loop.

        BLE delays grow from ten seconds to five minutes, with jitter bounded by
        those limits. TCP/serial retain the existing one-to-thirty-second policy.
        Retry until stopped by
        default; a positive internal attempt cap is retained for strict probes.
        """
        self._diag.reconnecting = True
        self._diag.reconnect_attempts = 0
        ble = self._config.connection_type == "ble"
        base_delay = _BLE_RECONNECT_BASE_DELAY if ble else _RECONNECT_BASE_DELAY
        max_delay = _BLE_RECONNECT_MAX_DELAY if ble else _RECONNECT_MAX_DELAY

        try:
            while not self._stop_requested and (
                _RECONNECT_MAX_ATTEMPTS == 0
                or self._diag.reconnect_attempts < _RECONNECT_MAX_ATTEMPTS
            ):
                delay = min(
                    base_delay * (2 ** min(self._diag.reconnect_attempts, 30)),
                    max_delay,
                )
                # Apply jitter.
                jitter = delay * _RECONNECT_JITTER_FRACTION
                delay = delay + random.uniform(-jitter, jitter)
                delay = min(max_delay, max(base_delay if ble else 0.0, delay))

                self._logger.info(
                    "MeshCoreSession %s: reconnect attempt %d%s in %.1fs",
                    self._adapter_id,
                    self._diag.reconnect_attempts + 1,
                    f"/{_RECONNECT_MAX_ATTEMPTS}" if _RECONNECT_MAX_ATTEMPTS else "",
                    delay,
                )

                await asyncio.sleep(delay)

                if self._stop_requested:
                    break

                try:
                    await self._connection_attempt()
                    self._contact_retry_delays.clear()
                    self._logger.info(
                        "MeshCoreSession %s: reconnected successfully",
                        self._adapter_id,
                    )
                    self._diag.reconnecting = False
                    self._diag.reconnect_attempts = 0
                    self._start_receive_pump()
                    return
                except Exception as exc:
                    self._diag.reconnect_attempts += 1
                    self._diag.last_error = self._sanitize_ble_exc(exc)
                    if self._permanent_setup_error(exc):
                        self._diag.reconnecting = False
                        self._logger.error(
                            "MeshCoreSession %s: recovery stopped after permanent "
                            "setup failure: %s",
                            self._adapter_id,
                            self._diag.last_error,
                        )
                        return
                    self._logger.warning(
                        "MeshCoreSession %s: reconnect failed (attempt %d): %s",
                        self._adapter_id,
                        self._diag.reconnect_attempts,
                        self._diag.last_error,
                    )

            if self._stop_requested:
                self._diag.reconnecting = False
                return
            # A positive attempt cap is supported for strict internal callers.
            self._diag.last_error = (
                f"Reconnect exhausted after {self._diag.reconnect_attempts} attempts"
            )
            self._diag.reconnecting = False
            self._logger.error(
                "MeshCoreSession %s: %s", self._adapter_id, self._diag.last_error
            )
        except asyncio.CancelledError:
            self._diag.reconnecting = False
            raise

    # ------------------------------------------------------------------
    # Real outbound
    # ------------------------------------------------------------------

    @property
    def local_node_name(self) -> str | None:
        """Own advertised node name captured from SDK self-info, with config fallback."""
        return self._diag.device_name or self._config.identity

    @property
    def group_text_budget(self) -> int:
        """Safe group payload bytes after the device's firmware-added prefix."""
        return group_text_budget(self._config.max_text_bytes, self._diag.device_name)

    async def _send_real(
        self,
        contact_id: str,
        text: str,
        *,
        channel_index: int | None = None,
        before_native_send: Callable[[], Awaitable[None]] | None = None,
    ) -> str | None:
        """Send via the paced owner; guarded packets permit one native call.

        ``before_native_send`` commits the core packet boundary inside this
        lock. Its exceptions propagate before SDK use. Unguarded calls use
        the bounded retry policy described below.

        .. warning::

           **Unbound retry risk.**  Unguarded direct calls may resubmit when
           a response is lost. A core-bound packet with ``before_native_send``
           makes at most one native SDK command and retains uncertainty.

        For DM sends, the SDK ``suggested_timeout`` (milliseconds) is
        extracted from the successful result and used as the retry delay
        for transient failures on subsequent attempts.  It is clamped to
        ``[_SUGGESTED_TIMEOUT_FLOOR, _SUGGESTED_TIMEOUT_CEIL]`` and
        converted to seconds.  Channel sends have no ACK identity and do
        not require or overclaim a suggested_timeout.
        """
        if self._meshcore is None:
            raise MeshCoreSendError("SDK client not initialised", transient=False)
        # Keep the exact native client identity across the storage grant await.
        # A reconnect must never redirect an already-consumed grant to its new SDK.
        sdk_client = self._meshcore

        limit = (
            self.group_text_budget if channel_index is not None else GROUP_TEXT_BYTES
        )
        if len(text.encode("utf-8")) > limit:
            raise MeshCoreSendError(
                f"Text exceeds MeshCore wire budget of {limit} UTF-8 bytes",
                transient=False,
            )

        # Pacing + send are serialized so concurrent calls honour the
        # configured delay between transmissions.
        async with self._send_lock:
            # Pacing: sleep before the real send so consecutive sends are
            # spaced by message_delay_seconds.  The delay applies once per
            # send_text() call, not per retry attempt.
            if self._config.message_delay_seconds > 0:
                await asyncio.sleep(self._config.message_delay_seconds)

            if before_native_send is not None:
                if (
                    self._stop_requested
                    or not self._diag.connected
                    or self._meshcore is not sdk_client
                ):
                    raise MeshCoreSendError(
                        "Session unavailable before native handoff", transient=True
                    )
                await before_native_send()
                # The grant awaited SQLite while stop()/reconnect may have
                # changed the session. It is now consumed, so abort without
                # any native call; core retains this packet as uncertain.
                if (
                    self._stop_requested
                    or not self._diag.connected
                    or self._meshcore is not sdk_client
                ):
                    raise MeshCoreSendError(
                        "Session changed after native handoff grant",
                        transient=False,
                    )
            attempt_limit = 1 if before_native_send is not None else _SEND_MAX_RETRIES
            last_exc: Exception | None = None
            for attempt in range(1, attempt_limit + 1):
                try:
                    if channel_index is not None:
                        result = await self._meshcore.commands.send_chan_msg(
                            channel_index, text
                        )
                    else:
                        result = await self._meshcore.commands.send_msg(
                            contact_id, text
                        )

                    # Check for SDK-level error.
                    if hasattr(result, "is_error") and result.is_error():
                        reason = (
                            result.payload.get("reason", "unknown")
                            if isinstance(result.payload, dict)
                            else "unknown"
                        )
                        if reason in _TRANSIENT_SDK_SEND_REASONS:
                            # The pinned SDK converts command-response timeout/no-event
                            # conditions into ERROR events. Raise a transport-like
                            # exception here so the existing bounded retry path handles
                            # the uncertainty and preserves cancellation semantics.
                            raise TimeoutError(f"SDK send error: {reason}")
                        self._diag.permanent_delivery_failures += 1
                        raise MeshCoreSendError(
                            f"SDK send error: {reason}",
                            transient=False,
                        )

                    # Extract native message ID and suggested_timeout if available.
                    # Per meshcore_py, MSG_SENT returns {'expected_ack': bytes(4),
                    # 'suggested_timeout': int}. Channel sends return OK with no ID.
                    # Use expected_ack as the native_id for DMs (channel sends
                    # honestly have no canonical identity from the SDK).
                    native_id: str | None = None
                    timeout_extracted = False
                    if isinstance(result, dict):
                        raw_ack = result.get("expected_ack")
                        native_id = _extract_expected_ack(raw_ack)
                        if native_id is None:
                            # Defensive fallback for older SDKs.
                            raw_mid = result.get("message_id")
                            if isinstance(raw_mid, bytes):
                                native_id = raw_mid.hex()
                            elif raw_mid is not None:
                                native_id = str(raw_mid)
                        # Extract suggested_timeout for DM retry delay.
                        if channel_index is None:
                            st = _extract_suggested_timeout(result)
                            if st is not None:
                                self._contact_retry_delays[
                                    _retry_delay_contact_key(contact_id)
                                ] = st
                                self._diag.sdk_suggested_timeouts_used += 1
                                timeout_extracted = True
                    else:
                        payload = getattr(result, "payload", None)
                        if isinstance(payload, dict):
                            raw_ack = payload.get("expected_ack")
                            native_id = _extract_expected_ack(raw_ack)
                            if native_id is None:
                                raw_mid = payload.get("message_id")
                                if isinstance(raw_mid, bytes):
                                    native_id = raw_mid.hex()
                                elif raw_mid is not None:
                                    native_id = str(raw_mid)
                            # Extract suggested_timeout for DM retry delay.
                            if channel_index is None:
                                st = _extract_suggested_timeout(payload)
                                if st is not None:
                                    self._contact_retry_delays[
                                        _retry_delay_contact_key(contact_id)
                                    ] = st
                                    self._diag.sdk_suggested_timeouts_used += 1
                                    timeout_extracted = True
                        if native_id is None:
                            attrs = getattr(result, "attributes", None)
                            if isinstance(attrs, dict):
                                raw_ack = attrs.get("expected_ack")
                                native_id = _extract_expected_ack(raw_ack)
                                if native_id is None:
                                    raw_mid = attrs.get("message_id")
                                    if isinstance(raw_mid, bytes):
                                        native_id = raw_mid.hex()
                                    elif raw_mid is not None:
                                        native_id = str(raw_mid)

                        # Always try attributes for timeout, even if native_id
                        # was already found in payload.
                        if not timeout_extracted and channel_index is None:
                            attrs = getattr(result, "attributes", None)
                            if isinstance(attrs, dict):
                                st = _extract_suggested_timeout(attrs)
                                if st is not None:
                                    self._contact_retry_delays[
                                        _retry_delay_contact_key(contact_id)
                                    ] = st
                                    self._diag.sdk_suggested_timeouts_used += 1

                    return str(native_id) if native_id is not None else None

                except MeshCoreSendError:
                    raise
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    last_exc = exc
                    self._diag.transient_delivery_failures += 1
                    self._logger.warning(
                        "MeshCoreSession %s: send attempt %d/%d failed: %s",
                        self._adapter_id,
                        attempt,
                        attempt_limit,
                        exc,
                    )
                    if attempt < attempt_limit:
                        # Use cached SDK suggested_timeout for DM retries
                        # when available, otherwise fall back to linear backoff.
                        # Only DM sends (channel_index is None) use cached delays.
                        if channel_index is None:
                            cached = self._contact_retry_delays.get(
                                _retry_delay_contact_key(contact_id)
                            )
                            if cached is not None:
                                await asyncio.sleep(cached)
                            else:
                                await asyncio.sleep(0.1 * attempt)
                        else:
                            await asyncio.sleep(0.1 * attempt)

            self._diag.permanent_delivery_failures += 1
            raise MeshCoreSendError(
                f"Send failed after {attempt_limit} attempts: {last_exc}"
            ) from last_exc
