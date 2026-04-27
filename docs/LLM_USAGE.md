# LLM Usage Guide

This guide is for coding agents or analysis agents using `gputrace` as a local
tool. Prefer machine-readable formats first, then render Markdown for human
handoff.

## Basic Rules

- Use absolute trace paths when possible.
- Prefer `--format json` for structured inspection.
- Prefer `--csv` for tabular diff slices that will be post-processed.
- Prefer `--markdown` or `--md-out` only for human-facing summaries.
- Do not use missing Go-era features: pprof, Perfetto export, live capture, or
  web serving.
- Xcode automation requires macOS Accessibility permission for the installed
  `gputrace` binary.

## Installation Check

```bash
cargo xtask install
gputrace --version
gputrace xcode-check-permissions --no-prompt
```

If Accessibility is not granted, run:

```bash
gputrace xcode-check-permissions
```

Then approve `gputrace` in System Settings > Privacy & Security >
Accessibility.

## Triage A Single Trace

Run these first:

```bash
gputrace stats /abs/path/trace.gputrace
gputrace analyze /abs/path/trace.gputrace
gputrace profiler /abs/path/trace.gputrace --format json
gputrace timing /abs/path/trace.gputrace --format json
```

For exported Xcode profile bundles, `analyze` and `buffers list` should both
surface unused-resource sidecars when Xcode recorded them. Check these fields
before concluding that buffer/resource analysis is empty:

```bash
gputrace analyze /abs/path/trace-perfdata.gputrace
gputrace buffers list /abs/path/trace-perfdata.gputrace --format json
```

If profiler data is missing, the trace may still support structural commands:

```bash
gputrace command-buffers /abs/path/trace.gputrace --format json
gputrace encoders /abs/path/trace.gputrace --format json
gputrace kernels /abs/path/trace.gputrace --format json
gputrace buffers list /abs/path/trace.gputrace --format json
```

## Xcode Profile Workflow

Use this when given an unprofiled `.gputrace` and asked to collect performance
data through Xcode:

```bash
gputrace xcode-check-permissions
gputrace xcode-profile run /abs/path/input.gputrace --output /abs/path/input-perfdata.gputrace --timeout-seconds 300
gputrace profiler /abs/path/input-perfdata.gputrace --format json
```

For the complete operational runbook, including split recovery commands,
export verification, and interpretation caveats, see
[`docs/PROFILE_WORKFLOW.md`](PROFILE_WORKFLOW.md).

Useful debugging commands:

```bash
gputrace xcode-windows --format json
gputrace xcode-status /abs/path/input.gputrace --format json
gputrace xcode-profile list-buttons /abs/path/input.gputrace --format json
gputrace xcode-profile list-tabs /abs/path/input.gputrace --format json
```

## Diff Workflow

For a quick human triage:

```bash
gputrace diff /abs/path/left-perfdata.gputrace /abs/path/right-perfdata.gputrace --quick --by-encoder
```

For structured output:

```bash
gputrace diff /abs/path/left-perfdata.gputrace /abs/path/right-perfdata.gputrace --json
```

For focused CSV views:

```bash
gputrace diff left.gputrace right.gputrace --csv --by function --limit 50
gputrace diff left.gputrace right.gputrace --csv --by encoder --limit 50
gputrace diff left.gputrace right.gputrace --csv --by dispatch --min-delta-us 30 --limit 100
gputrace diff left.gputrace right.gputrace --csv --by timeline-windows --limit 25
gputrace diff left.gputrace right.gputrace --csv --by unmatched --limit 100
```

For a report artifact:

```bash
gputrace diff left.gputrace right.gputrace --markdown --md-out /abs/path/diff-report.md --limit 25
```

For benchmark directories containing Go/Python trace pairs:

```bash
gputrace diff --bench-dir /abs/path/bench-traces --quick --by-encoder
gputrace diff --bench-dir /abs/path/bench-traces --json
```

## Shader And Counter Workflow

```bash
gputrace xcode-counters trace-perfdata.gputrace --format summary
gputrace xcode-counters trace-perfdata.gputrace --format json
gputrace export-counters trace-perfdata.gputrace --format json
gputrace shaders trace-perfdata.gputrace --format json
gputrace shader-hotspots trace-perfdata.gputrace kernel_name --search-path /abs/path/src --format json
gputrace shader-source trace-perfdata.gputrace kernel_name --search-path /abs/path/src --format text
```

If an Xcode counter CSV was exported separately:

```bash
gputrace xcode-counters trace-perfdata.gputrace --csv /abs/path/Counters.csv --format json
gputrace xcode-counters trace-perfdata.gputrace --csv /abs/path/Counters.csv --metric "Kernel Invocations" --top 20
gputrace validate-counters trace-perfdata.gputrace --csv /abs/path/Counters.csv --format json
```

`xcode-counters` only auto-discovers exact trace-name CSV matches. If there are
nearby unrelated `*Counters.csv` files, pass `--csv` explicitly.

Profiler-backed exports may have little structural dispatch data while
`streamData` contains the useful kernel list. In that case, `command-buffers`,
`encoders`, `shaders`, `shader-source`, `shader-hotspots`, and
`mtlb functions --used-only` use profiler timing/name fallbacks where possible.

Do not interpret `profiler.stream_data_summary.pipeline_id_scan_costs` as
Xcode Cost. It is a retained debug signal from scanning `Profiling_f_*` bytes
for known pipeline IDs and is known to disagree with Xcode's Cost view on real
exports. For ranking without a counter CSV, use profiler duration fields. For
Xcode counter parity, use an exported Xcode counter CSV with `xcode-counters`
or `validate-counters`.

For a single machine-readable offline feed, prefer `export-counters --format
json`. It combines profiler/timeline rows with decoded APS counter sample rows
when present. Inspect each row's `metric_source`:

- `profile-dispatch-time`: per-kernel rows synthesized from real `streamData`
  dispatch durations when Xcode Cost rows are not separately decoded.
- `profile-execution-cost`: per-kernel rows from decoded execution-cost data
  when present.
- `aps-counter-samples`: rows from decoded `APSCounterData` sample windows.
- `raw-counter` or timeline-derived sources: fallback timeline/counter rows,
  emitted only when richer profiler/APS rows are unavailable.

For `aps-counter-samples`, JSON rows include `metrics` and `metric_metadata`.
`metric_metadata` contains the Apple counter key, type, description, unit,
counter graph groups, timeline groups, and visibility flags from local
Xcode/AGX catalogs when available. Do not require or look for a
`Counters.csv` file for this path; CSV exports are only for `xcode-counters`
parity/validation.

For end-user raw counter inspection without a counter CSV, use `raw-counters`.
It reads `.gpuprofiler_raw/streamData` and reports aggregate metadata,
per-sample-group schemas, decoded `GPRWCNTR` streams, raw counter ids, and
trace-id maps from embedded APS metadata such as `TraceId to BatchId` and
`TraceId to SampleIndex`. It also exposes APS `program_address_mappings`: the
encoder trace id, draw/function index, shader index, binary id, mapped address,
and mapped size records that bridge USC/MIO samples back to shader address
ranges. `profiling_address_summary` scans `Profiling_f_*` payloads against
those ranges and reports low32 address-derived shader/function hit counts. When
available, it enriches raw hashes from installed AGX Metal statistics/perf
counter plists under `/System/Library/Extensions`. The JSON report includes
`derived_metrics` when local AGX `*-derived.js` files can be evaluated from
decoded raw variables. Treat these as offline Apple-formula counter values; they
do not depend on, or require, an exported Xcode counter CSV.
`grouped_derived_metrics` contains the same formula output split by raw counter
sample group/source and includes counter graph metadata where local Xcode/AGX
catalogs expose it. It also carries profiler dispatch metadata when a trace's
raw counter timestamps overlap `streamData` dispatch tick windows; if they do
not, the report warns rather than fabricating a dispatch join:

```bash
gputrace raw-counters trace-perfdata.gputrace --format text
gputrace raw-counters trace-perfdata.gputrace --format json
gputrace raw-counters trace-perfdata.gputrace --format csv
```

When you need to know whether a profiler export is fully understood, run:

```bash
gputrace profiler-coverage trace-perfdata.gputrace --format json
```

Treat this as the source of truth for reversal coverage. It reports byte share
by profiler-bundle family and explicitly labels `streamData`, `Profiling_f_*`,
`Counters_f_*`, `Timeline_f_*`, and other raw files as semantic, partial,
heuristic, or opaque. Do not infer that missing values are unavailable from
Xcode unless this report shows the relevant bytes have been accounted for.

For raw counter reverse-engineering and CSV correlation, use the hidden
structured `APSCounterData` probe instead of old `Counters_f_N` assumptions:

```bash
gputrace raw-counter-probe trace-perfdata.gputrace --format text
gputrace raw-counter-probe trace-perfdata.gputrace --metric "Instruction Throughput Limiter" --format json
gputrace raw-counter-probe trace-perfdata.gputrace --metric "ALU Utilization" --format json
```

`raw-counter-probe` is hidden from top-level help because it is a
format-reversal/debugging command, not the user-facing report. Its normal path decodes
`.gpuprofiler_raw/streamData` aggregate metadata, `GPRWCNTR` record sizes,
per-sample-group counter schemas from `Subdivided Dictionary/passList`, and
normalized `raw_counter / GRC_GPU_CYCLES * 100` candidates. It reports
candidate matches against an exported Xcode counter CSV when one is discoverable
or passed with `--csv`.

Only use `--scan-files` when intentionally investigating raw `Counters_f_*.raw`
record shapes; it scans large files and is not needed for the structured
aggregate decoder.

## Markdown Rendering

Use built-in Markdown commands when producing human-readable summaries:

```bash
gputrace markdown analyze trace.gputrace > analysis.md
gputrace markdown diff left.gputrace right.gputrace > diff.md
gputrace markdown buffers trace.gputrace > buffers.md
gputrace markdown buffers-diff left.gputrace right.gputrace > buffers-diff.md
```

`markdown render` converts Markdown text to HTML:

```bash
gputrace markdown render '# Title'
```

## Output Selection

Use this decision table:

| Need | Command style |
| --- | --- |
| Parse in an agent | `--format json` or `--json` |
| Sort/filter in shell | `--csv --by ...` |
| Send to a human | `--markdown --md-out ...` |
| Fast performance answer | `diff --quick --by-encoder` |
| Xcode automation state | `xcode-status`, `xcode-windows`, `xcode-profile list-*` with `--format json` |

## Common Failure Modes

- `Accessibility permission is required`: run `gputrace xcode-check-permissions`
  and approve the installed binary in System Settings.
- Empty profiler/timing output: the trace may not contain `.gpuprofiler_raw`;
  use structural analysis or run `xcode-profile run`.
- `xcode-counters` refuses a nearby CSV: pass the exact Xcode-exported CSV with
  `--csv`; do not let an agent pick a same-directory CSV by guesswork.
- Diff has many unmatched dispatches: compare the same workload and prefer
  profiled `-perfdata.gputrace` bundles.
- Shader source not found: pass all relevant source roots with repeated
  `--search-path`.
