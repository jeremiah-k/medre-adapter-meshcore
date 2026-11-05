# MeshCore synchronous-callback ingress pressure

`tests/test_meshcore_ingress_pressure.py` emits 32 and 256 distinct relayable
channel callbacks with publication held by an event. These callbacks exercise
the real classifier, native dedup identity, codec and the synchronous adapter
callback on the asyncio owner loop, without a physical companion or SDK socket.

```sh
MEDRE_PRESSURE_REPORT_DIR=./pressure-results pytest \
  tests/test_meshcore_ingress_pressure.py -q
```

While the publisher was held, the adapter retained exactly one asyncio task
per distinct packet: 32 and 256 outstanding tasks, respectively. All events
published after release and the task set drained. Shutdown of one blocked
publisher cancels the retained task, observes its exit and refuses subsequent
callbacks. Releasing the sink cannot complete a publication after stop. A
publication task cancelled while the adapter remains started releases its
provisional dedup reservation, so a retransmission of the same identity is
re-admitted; the release is ownership-checked, and a cancellation after a
successful publish keeps the admitted key. A
separate queued-but-unstarted case bypasses drain cancellation to exercise the
coroutine's independent lifecycle check. Optional JSON
reports contain counters and elapsed local time, not message bodies; latency
is not an RF/device/soak throughput claim.

This is proof of linear pre-admission task growth, not evidence of loss or a
sustainable capacity threshold. The callback runs on the event loop and cannot
wait synchronously for an async semaphore slot without deadlock. A finite
producer buffer would therefore require an explicit overflow disposition and
operator-visible health/metrics, plus tests for dedup rollback and restart.
No lossy or blocking admission policy is introduced by this measurement.
