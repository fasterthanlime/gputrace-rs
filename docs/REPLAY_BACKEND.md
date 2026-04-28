# Replay Backend

`gputrace profile` runs Apple's `MTLReplayer.app` headlessly to produce a
`.gpuprofiler_raw` bundle from a `.gputrace` capture. It does not use Xcode
or any UI automation, and does not require Accessibility permission.

## Pipeline

```
.gputrace                               (produced by your app under capture)
    │
    ▼
gputrace profile <trace> --output <dir>
    │
    │  spawns:
    │    /usr/bin/open -W -a /System/Library/CoreServices/MTLReplayer.app \
    │      --args -CLI <trace> -collectProfilerData --all -runningInCI -verbose \
    │             --output <dir>
    ▼
<dir>/<stem>.gpuprofiler_raw/
    streamData
    Counters_f_*.raw
    Profiling_f_*.raw
    Timeline_f_*.raw
    │
    ▼
gputrace report --output <md_dir> <trace>          (auto-discovers the bundle)
   or:
gputrace report --output <md_dir> --profiler <dir> <trace>
    │
    ▼
markdown reports: index.md, insights.md, shaders.md, counters.md,
                  timing.md, xcode-mio.md, profiler.md, ...
```

For a 4.9 GB / 482-dispatch trace on M4 Pro, expect roughly:

- `gputrace profile` — 12 s wall clock
- `gputrace report` — 6 s wall clock

## Invocation

`gputrace profile <trace>` defaults the output directory to
`<trace>-perfdata` next to the trace. Override with `--output`. Capture
`MTLReplayer`'s diagnostic stdout/stderr with `--stdout-log` /
`--stderr-log` if you need to debug a failed run.

`gputrace report` looks for the profiler bundle in this order:

1. `$GPUTRACE_PROFILER_DIR` (env var; set by `gputrace report --profiler`)
2. `<trace>.gpuprofiler_raw` adjacent to the trace
3. `/private/tmp/com.apple.gputools.profiling/<stem>_stream.gpuprofiler_raw`
   (this is also where Xcode writes when you click Profile in the UI, so
   already-Xcode-profiled traces are picked up automatically)
4. Any `*.gpuprofiler_raw` directory inside the `.gputrace` bundle

## Operational notes

- macOS is required. `MTLReplayer.app` ships with the OS at
  `/System/Library/CoreServices/MTLReplayer.app/`.
- macOS VMs with a paravirt GPU can hit GPU command-buffer timeouts on
  non-trivial traces. Use real Apple Silicon hardware for production
  profiling.
- The profile output uses the same on-disk format that Xcode's
  `Export Performance Data` produces, so all existing `gputrace`
  analyzers (`xcode-mio`, `raw-counters`, `shaders`, `timing`, `insights`,
  …) consume it directly.
