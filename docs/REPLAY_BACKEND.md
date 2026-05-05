# Replay Backend

`gputrace report` automatically uses Apple's `MTLReplayer.app` in headless CLI
mode when profiler data is not already cached inside the `.gputrace` bundle.

The cache lives at:

```text
TRACE.gputrace/gputrace-profile/<stem>.gpuprofiler_raw/
```

That directory contains `streamData`, `Profiling_f_*.raw`, `Counters_f_*.raw`,
and `Timeline_f_*.raw` files.

This backend is an implementation detail of `report`; users should not run
internal analyzer or replay subcommands directly.
