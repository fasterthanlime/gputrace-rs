# GPU Trace Profiling Workflow

This is the end-to-end workflow for taking an existing Metal `.gputrace`,
running profile/replay against it headlessly, and analyzing the result with
`gputrace`.

`gputrace` does not perform live/direct capture. The application under test
must produce the initial `.gputrace` bundle, typically by running with
`METAL_CAPTURE_ENABLED=1` and whatever app-specific capture environment
selects the pulse, phase, or step.

The profile step drives Apple's `MTLReplayer.app` directly via
LaunchServices. **No Xcode UI, no Accessibility permission, no AX
automation.** See `docs/REPLAY_BACKEND_INVESTIGATION.md` for why the older
AX-based pipeline was replaced.

## 1. Install

```bash
cargo xtask install
```

Builds, installs, and signs the binary at `~/.cargo/bin/gputrace`. There
is no Accessibility prompt to approve — the modern profile path needs no
TCC permissions.

## 2. Create a Small Input Capture

Keep the capture small. Replay can use very large amounts of memory when
profiling a trace with many dispatches or a large resource snapshot.

For Bee-style captures, keep the capture window focused on one useful phase
and one pulse or step. The exact environment belongs to the application,
but the important `gputrace`-side assumptions are:

- `METAL_CAPTURE_ENABLED=1` is set for the app run.
- The app writes a `.gputrace` bundle.
- The capture is small enough to replay end-to-end on the same machine.
- Only one trace is being profiled at a time.

Example output path used in development:

```text
/Users/amos/bearcove/bee/target/gputrace-captures/tq4-A-baseline-verify-p3s0.gputrace
```

## 3. Profile

```bash
gputrace profile \
  /abs/path/input.gputrace \
  --output /abs/path/input-perfdata
```

`--output` is optional; it defaults to `<trace>-perfdata` next to the
trace.

Internally this runs:

```text
/usr/bin/open -W -a /System/Library/CoreServices/MTLReplayer.app \
  --args -CLI <trace> -collectProfilerData --all -runningInCI -verbose \
         --output <output_dir>
```

When it returns, `<output_dir>/<stem>.gpuprofiler_raw/` contains:

```text
streamData
Counters_f_0..N.raw
Timeline_f_0..N.raw
Profiling_f_0..N.raw
```

For a 4.9 GB / 482-dispatch trace on M4 Pro, expect ~12 s wall-clock.

If profiling fails because MTLReplayer exits without producing
`streamData`, capture the diagnostic output:

```bash
gputrace profile /abs/path/input.gputrace \
  --output /tmp/perfdata \
  --stdout-log /tmp/mtl.stdout --stderr-log /tmp/mtl.stderr
cat /tmp/mtl.stdout
```

The `#CI-INFO#` lines from MTLReplayer make most failure modes (missing
device, archive open failure, GPU timeout) immediately diagnosable.

## 4. Verify the Output Bundle

Check for profiler raw files:

```bash
ls /abs/path/input-perfdata/*.gpuprofiler_raw/
```

Compare directory sizes if useful:

```bash
du -sh /abs/path/input.gputrace /abs/path/input-perfdata
```

The profile output is typically larger than the original trace because it
includes per-pass counter, timeline, and profiling streams in addition to
`streamData`.

## 5. Analyze the Profiled Trace

Run the high-signal reports first:

```bash
gputrace report /abs/path/input-perfdata.gputrace --output /abs/path/input-report
```

The report directory is the preferred LLM handoff artifact. Start at
`index.md`, then inspect `xcode-mio.md`, `analysis.md`, `insights.md`,
`profiler.md`, `timing.md`, `shaders.md`, `counters.md`, and
`profiler-coverage.md`.

Then use individual commands only for targeted drill-down:

```bash
gputrace analyze /abs/path/input-perfdata.gputrace
gputrace profiler /abs/path/input-perfdata.gputrace --format text
gputrace xcode-mio /abs/path/input-perfdata.gputrace
gputrace insights /abs/path/input-perfdata.gputrace --min-level high
gputrace buffers list /abs/path/input-perfdata.gputrace --format json
gputrace command-buffers /abs/path/input-perfdata.gputrace
gputrace encoders /abs/path/input-perfdata.gputrace
gputrace kernels /abs/path/input-perfdata.gputrace
gputrace api-calls /abs/path/input-perfdata.gputrace
gputrace shaders /abs/path/input-perfdata.gputrace --format json
```

On profile bundles, structural parser data can be sparse while
`.gpuprofiler_raw/streamData` has the useful dispatch list. The `report`
command records structural-parser failures in `index.md` and keeps the
useful profiler/MIO-backed sections available. The default `xcode-mio`
format is the LLM-friendly summary; `--format summary-json` is the compact
machine-readable form. `shaders` includes `Addr Hits` / `Addr %` in text
output and `profiling_address_hits` / `profiling_address_percent` in
JSON/CSV when address samples can be joined through APS program-address
mappings — treat those as an additional shader-local signal; real
duration/cost ranking remains primary.

If a trace reports large unused resources, check `analyze` and
`buffers list` for `unused_resource_groups`; those come from
`unused-device-resources-*` sidecars and report logical resource bytes.

For source mapping, search the source tree by the hot kernel names reported by
`profiler` or `insights`:

```bash
rg -n 'hot_kernel_name|another_hot_kernel' /abs/path/source/root
```

If source extraction is available for the shader:

```bash
gputrace shader-source /abs/path/input-perfdata.gputrace hot_kernel_name \
  --search-path /abs/path/source/root
gputrace shader-hotspots /abs/path/input-perfdata.gputrace hot_kernel_name \
  --search-path /abs/path/source/root \
  --format json
```

## 7. Interpret the Reports

Prefer profiler-backed `streamData` when it is present. A raw capture may show
one wrapper dispatch while `streamData` expands the same workload into many
inner profiler dispatches with real names and durations.

Use this priority order:

1. `gputrace report`: one markdown directory containing the retained
   high-signal profiler, MIO, timing, shader, counter, and coverage views.
2. `gputrace profiler`: profiler directory inventory, dispatch durations,
   occupancy samples, pipeline compilation stats, and the debug-only
   `pipeline_id_scan_costs` field.
3. `gputrace analyze`: compact JSON summary and top timed kernels.
4. `gputrace insights`: heuristic optimization hints from profiler-backed
   timing/counter data.
5. `gputrace api-calls`, `command-buffers`, `encoders`, `kernels`:
   structural trace interpretation.
6. `gputrace timeline`: useful for visualization, but some spans can remain
   synthetic when profiler data does not expose every timestamp directly.

Watch for disagreements between views. Xcode's Performance/Cost view is
not the same thing as dispatch-duration ranking. If you need Xcode's
counter view, export the counter CSV from Xcode and pass it to
`xcode-counters`/`validate-counters` explicitly.

For Xcode-exported counter CSVs, pass the CSV explicitly unless the filename is
an exact trace-name match:

```bash
gputrace xcode-counters /abs/path/input-perfdata.gputrace \
  --csv '/abs/path/input-perfdata Counters.csv' \
  --format summary
```

The summary view highlights top invocations, memory bandwidth, low occupancy,
buffer L1 misses, and limiter signals. Use `--metric <name> --top <n>` for a
focused ranked table.

For offline analysis without an exported Xcode counter CSV, use
`export-counters` as the structured feed:

```bash
gputrace export-counters /abs/path/input-perfdata.gputrace --format json
gputrace export-counters /abs/path/input-perfdata.gputrace --format csv
```

The JSON output combines profiler/timeline rows and decoded APS counter sample
rows when present. Use `metric_source` to distinguish `profile-dispatch-time`,
`profile-execution-cost`, `aps-counter-samples`, and fallback `raw-counter`
rows. Fallback raw-counter rows are suppressed when profiler/APS rows are
available, because the fallback path is heuristic. APS rows include `metrics`
and `metric_metadata`; metadata is populated
from local Apple/Xcode counter catalogs with counter keys, units, groups,
timeline groups, visibility flags, and descriptions where available.

For end-user raw counter inspection without a counter CSV, use `raw-counters`.
It reports the decoded `.gpuprofiler_raw/streamData` metadata, schemas,
`GPRWCNTR` streams, raw counter ids, and any matching derived counter
names from installed AGX Metal statistics/perf counter catalogs. It also
exposes embedded APS trace-id maps and `program_address_mappings` for
joining samples to shader code. `profiling_address_summary` uses those
ranges to scan `Profiling_f_*` payloads and rank shader/function low32
address hits. In JSON, `derived_metrics` contains finite values from
running the locally available AGX `*-derived.js` formulas against decoded
raw variables; `grouped_derived_metrics` splits those values by raw
sample group/source and includes counter graph metadata plus
profiler-dispatch join fields only when the bundle exposes overlapping
raw counter and `streamData` tick windows:

```bash
gputrace raw-counters /abs/path/input-perfdata.gputrace --format text
gputrace raw-counters /abs/path/input-perfdata.gputrace --format json
```

To see what parts of the profiler bundle are decoded versus still opaque,
use the coverage report:

```bash
gputrace profiler-coverage /abs/path/input-perfdata.gputrace --format text
gputrace profiler-coverage /abs/path/input-perfdata.gputrace --format json
```

It reports byte share for `streamData`, `Profiling_f_*`, `Counters_f_*`,
`Timeline_f_*`, and other raw families, plus decoded APS archive counts
and the largest opaque files.

If you are debugging counter parity or correlating against an exported
counter CSV, use the structured probe:

```bash
gputrace raw-counter-probe /abs/path/input-perfdata.gputrace --metric "Instruction Throughput Limiter"
gputrace raw-counter-probe /abs/path/input-perfdata.gputrace --metric "ALU Utilization" --format json
```

The raw probe decodes aggregate metadata such as timebase, encoder sample
indices, encoder trace-id rows, `GPRWCNTR` record sizes, and per-pass
counter schemas.

## 6. Common Failure Modes

- `gputrace profile` exits 0 but no `streamData` appears: rerun with
  `--stdout-log /tmp/mtl.stdout --stderr-log /tmp/mtl.stderr` and inspect
  the `#CI-INFO#` lines for the failure reason (commonly: archive open
  failure, missing/incompatible Metal device, GPU command-buffer timeout).
- GPU command-buffer timeout: usually means the trace is too large or
  you're running on a paravirt-GPU VM. Capture a smaller pulse/step, or
  run profile on real Apple Silicon hardware.
- `profiler` says no `.gpuprofiler_raw`: rerun `gputrace profile` to
  produce one, or pass `gputrace report --profiler <DIR>` to point at an
  existing one.
- Raw trace shows one dispatch but profiler shows many: use the profiler
  report for timing and optimization ranking.
- `xcode-counters` refuses nearby CSVs: pass `--csv` with the exact CSV;
  this avoids accidentally analyzing counters from a different trace.
