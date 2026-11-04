# Measure MeshCore SDK callback scheduling pressure

Add a repeatable held-publication burst harness for 32 and 256 unique messages,
with content-free measurement output, observed publisher cancellation and
a queued-callback lifecycle fence. This demonstrates
unbounded pre-admission task growth in the existing callback owner without
changing runtime behavior or assuming an acceptable loss/overflow policy.
