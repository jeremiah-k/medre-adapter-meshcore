# Report native connection health after startup and recovery

Publish completed MeshCore connection transitions through the optional runtime
health sink. Healthy native connections clear transient degradation and refresh
cached diagnostics without another probe. Share the health calculation between
probes and reports, ignore retired session callbacks, and isolate diagnostic
observer failures from native ownership and delivery.
