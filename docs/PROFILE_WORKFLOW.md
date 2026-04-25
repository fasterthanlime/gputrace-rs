# GPU Trace Profiling Workflow

This is the end-to-end workflow for taking an existing Metal `.gputrace`,
profiling it in Xcode, exporting embedded performance data, and analyzing the
result with `gputrace`.

`gputrace` does not perform live/direct capture. The application under test
must produce the initial `.gputrace` bundle, typically by running with
`METAL_CAPTURE_ENABLED=1` and whatever app-specific capture environment selects
the pulse, phase, or step.

## 1. Install and Check Permissions

```bash
cargo xtask install
gputrace xcode-check-permissions
```

`cargo xtask install` builds, installs, signs, and verifies the binary at
`~/.cargo/bin/gputrace`. Xcode automation requires macOS Accessibility
permission for that installed binary.

If permission is missing, approve `gputrace` in System Settings > Privacy &
Security > Accessibility, then rerun:

```bash
gputrace xcode-check-permissions
```

## 2. Create a Small Input Capture

Keep the capture small. Xcode replay can use very large amounts of memory when
profiling a trace with many dispatches or a large resource snapshot.

For Bee-style captures, keep the capture window focused on one useful phase and
one pulse or step. The exact environment belongs to the application, but the
important gputrace-side assumptions are:

- `METAL_CAPTURE_ENABLED=1` is set for the app run.
- The app writes a `.gputrace` bundle.
- The capture is small enough for Xcode to replay and export.
- Only one trace is being profiled at a time.

Example output path used in development:

```text
/Users/amos/bearcove/bee/target/gputrace-captures/tq4-A-baseline-verify-p3s0.gputrace
```

## 3. Profile and Export in One Command

For a fresh unprofiled trace, prefer the single command:

```bash
gputrace xcode-profile run \
  /abs/path/input.gputrace \
  --output /abs/path/input-perfdata.gputrace \
  --timeout-seconds 300
```

This opens the trace in Xcode, clicks Replay/Profile when needed, waits for the
profiled view to complete, switches to Summary, clicks Export, enables
`Embed performance data` when present, saves the exported bundle, and verifies
that the output path exists.

The exported bundle should contain a nested `.gpuprofiler_raw` directory with
files such as:

```text
streamData
Counters_f_0.raw
Timeline_f_0.raw
Profiling_f_0.raw
```

## 4. Recover or Drive Individual Xcode Steps

If Xcode is already open or a run is interrupted, use the subcommands directly.

Check status:

```bash
gputrace xcode-profile check-status /abs/path/input.gputrace --format json
```

Open a trace:

```bash
gputrace xcode-profile open /abs/path/input.gputrace --foreground
```

Click replay/profile:

```bash
gputrace xcode-profile run-profile /abs/path/input.gputrace --format json
```

Wait for profiling to complete:

```bash
gputrace xcode-profile wait-profile /abs/path/input.gputrace \
  --timeout-seconds 300 \
  --format json
```

Export a trace that is already profiled in Xcode:

```bash
gputrace xcode-profile export \
  /abs/path/input-perfdata.gputrace \
  --trace /abs/path/input.gputrace \
  --format json
```

Useful AX/Xcode debugging probes:

```bash
gputrace xcode-windows --format json
gputrace xcode-status /abs/path/input.gputrace --format json
gputrace xcode-profile list-buttons /abs/path/input.gputrace --format json
gputrace xcode-profile list-tabs /abs/path/input.gputrace --format json
gputrace xcode-profile show-summary /abs/path/input.gputrace --format json
gputrace xcode-profile click-button Export --trace /abs/path/input.gputrace --format json
```

## 5. Verify the Export Contains Profiler Data

Check for profiler raw files:

```bash
rg --files /abs/path/input-perfdata.gputrace | rg 'gpuprofiler_raw|streamData|Counters_f_|Timeline_f_|Profiling_f_'
```

Compare bundle sizes if useful:

```bash
du -sh /abs/path/input.gputrace /abs/path/input-perfdata.gputrace
```

A perf-data export is often larger than the original trace because it embeds
profiler data and imported resources.

## 6. Analyze the Profiled Trace

Run the high-signal reports first:

```bash
gputrace analyze /abs/path/input-perfdata.gputrace
gputrace profiler /abs/path/input-perfdata.gputrace --format text
gputrace profiler /abs/path/input-perfdata.gputrace --format json
gputrace insights /abs/path/input-perfdata.gputrace --min-level high
gputrace buffers list /abs/path/input-perfdata.gputrace --format json
```

Then inspect structure and attribution:

```bash
gputrace command-buffers /abs/path/input-perfdata.gputrace
gputrace encoders /abs/path/input-perfdata.gputrace
gputrace kernels /abs/path/input-perfdata.gputrace
gputrace api-calls /abs/path/input-perfdata.gputrace
gputrace shaders /abs/path/input-perfdata.gputrace --format json
```

On Xcode-exported profiler bundles, structural parser data can be sparse while
`.gpuprofiler_raw/streamData` has the useful dispatch list. The structural
commands above tolerate malformed short records and use profiler-backed
dispatch/name fallbacks where the bundle shape permits it.

If Xcode reports large unused resources, check `analyze` and `buffers list` for
`unused_resource_groups`; those are parsed from
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

1. `gputrace profiler`: profiler directory inventory, dispatch durations,
   occupancy samples, pipeline compilation stats, and the debug-only
   `pipeline_id_scan_costs` field.
2. `gputrace analyze`: compact JSON summary and top timed kernels.
3. `gputrace insights`: heuristic optimization hints from profiler-backed
   timing/counter data.
4. `gputrace api-calls`, `command-buffers`, `encoders`, `kernels`: structural
   trace interpretation.
5. `gputrace timeline`: useful for visualization, but some spans can remain
   synthetic when Xcode profiler data does not expose every timestamp directly.

Watch for disagreements between views. Xcode's Performance/Cost view is not the
same thing as dispatch-duration ranking. The old Go-style `Profiling_f_*`
pipeline-id byte scan is retained as `pipeline_id_scan_costs` for
reverse-engineering only; do not treat it as Xcode Cost. If you need Xcode's
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

If you are debugging Xcode raw counter parity, start with the structured
`streamData` decoder:

```bash
gputrace raw-counter-probe /abs/path/input-perfdata.gputrace --metric "Instruction Throughput Limiter"
gputrace raw-counter-probe /abs/path/input-perfdata.gputrace --metric "ALU Utilization" --format json
```

The raw probe decodes aggregate metadata such as timebase, encoder sample
indices, encoder trace-id rows, `GPRWCNTR` record sizes, and per-pass counter
schemas. It is intentionally separate from the main analysis commands until the
remaining private counter-name and Xcode aggregation rules are fully mapped.

## 8. Common Failure Modes

- Xcode opens but Replay is not clicked: run `xcode-profile list-buttons` and
  `xcode-profile check-status` to inspect the AX state.
- Export dialog opens but save fails: rerun `xcode-profile export`; the save
  sheet can expose a very large AX tree if the file browser has many rows.
- Xcode uses too much memory or replay crashes: capture a smaller pulse, step,
  or phase.
- `profiler` says no `.gpuprofiler_raw`: export from Xcode with performance
  data embedded, or rerun `xcode-profile run`.
- Raw trace shows one dispatch but profiler shows many: use the profiler report
  for timing and optimization ranking.
- `xcode-counters` refuses nearby CSVs: pass `--csv` with the exact Xcode CSV;
  this avoids accidentally analyzing counters from a different trace.
