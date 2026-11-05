# Qualify MeshCore admission across process-lifetime restart cycles

`tests/test_meshcore_process_lifetime_admission.py` runs 25 restart cycles in
one process, each saturating admission beyond the bound, stranding one
cancellation-resistant publisher behind a timed-out bounded drain, and
restarting. The tracked publication-task registry comes back empty after
every stop while a process-wide census of adapter-created pending tasks grows
by exactly one test-stranded publisher per cycle — all 25 alive and untracked at
the peak — and returns to empty once every stranded publisher settles through
late success or late cancellation. Every restarted session admits a full
window while all prior strands are alive, and each retired session's late
pre-start cancellation reports through its own pressure sink, never the
restarted session's.

Dedup reservations cannot cross a session boundary: stop and start clear the
map, so each stranded identity is re-admitted by the next session, and a
stale ownership token cannot evict a reservation the active session
re-committed for the same key. The retained memory evidence takes its
baseline from the first ten cycles with their strands still pending; the peak
growth of the remaining fifteen cycles stays under the 256,000-byte extra-bytes
bound, and the footprint returns below twice that baseline after all strands
settle. The publisher is a test double; these shapes do not prove real SDK,
hardware or RF behavior.

Peak growth is qualified only with a test-owned tracing session. Borrowed
tracing checks sampled retained growth, preserves the caller's tracer, and
marks transient peak growth as unqualified in the evidence record.
