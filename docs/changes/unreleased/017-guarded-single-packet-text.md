# Guard ordinary native text against ambiguous resubmission

Short, clipped and fallback text now uses a one-packet radio plan in TCP, serial
and BLE modes. Core persists the native boundary under its existing exact
attempt authority. Each granted call makes at most one SDK command; acceptance
or uncertain submission remains bound after reopening storage. The wire text,
UTF-8 clipping and attribution are unchanged. Empty native text, including an
effective zero-byte budget, fails before dispatch.

Core-bound prebuilt results without a grant are refused before SDK use.
Synthetic fake-mode rendering and direct unbound session retries keep their
existing behavior. Shared plan and handoff schemas already support one packet;
there are no new configuration fields, dependency changes or metadata versions.

Pinned real-SDK tests exercise companion-protocol acceptance, lost responses,
disconnect and cancellation through the core packet owner. They establish no
physical RF delivery or recipient acknowledgment.
