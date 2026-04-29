# LLM Usage Guide

This guide is for coding agents or analysis agents using `gputrace` as a local
tool. Single-trace work is always: profile, then report, then read the
markdown directory.

## Basic Rules

- Use absolute trace paths.
- For single-trace triage, use `gputrace report --output DIR` and read the
  markdown files it writes. Do not invoke per-section commands like
  `profiler`, `analyze`, `xcode-mio`, `insights`, `shaders`,
  `raw-counters`, `profiler-coverage`, `export-counters`,
  `xcode-counters`, `raw-counter-probe`, `command-buffers`, `encoders`,
  `kernels`, `api-calls`, `timeline`, or `buffers list` — `report` already
  writes those sections into the report directory in one shot. The
  individual commands exist for plumbing/parity tests, not for users.
- Use `diff` (not `report`) when comparing two profiled traces.
- Do not use missing Go-era features: pprof, Perfetto export, live capture,
  or web serving.
- Xcode automation requires macOS Accessibility permission for the
  installed `gputrace` binary.

## Installation Check

```bash
cargo xtask install
gputrace --version
gputrace xcode-check-permissions --no-prompt
```

If Accessibility is not granted, run `gputrace xcode-check-permissions` and
approve `gputrace` in System Settings > Privacy & Security > Accessibility.

## Triage A Single Trace

Profile the original `.gputrace`, then run `report` against it. The perfdata
bundle is just a wrapper around `<stem>.gpuprofiler_raw/`; pass that
directory to `--profiler`. Pointing `report` at the perfdata bundle directly
fails with `missing required trace file: .../metadata`.

```bash
gputrace profile /abs/path/trace.gputrace \
  --output /abs/path/trace-perfdata.gputrace
gputrace report /abs/path/trace.gputrace \
  --profiler /abs/path/trace-perfdata.gputrace/trace.gpuprofiler_raw \
  --output /abs/path/trace-report
```

Read `/abs/path/trace-report/index.md`, then follow links into
`xcode-mio.md`, `analysis.md`, `insights.md`, `profiler.md`, `timing.md`,
`shaders.md`, `counters.md`, and `profiler-coverage.md`. The report reuses
the Xcode MIO private-framework summary across analysis/insights and
records structural-parser failures in `index.md` rather than expecting an
agent to run individual commands.

For source mapping, search the source tree by the hot kernel names the
report calls out:

```bash
rg -n 'hot_kernel_name|another_hot_kernel' /abs/path/source/root
```

## Diff Workflow

`diff` is the one workflow `report` does not cover.

For a quick human triage:

```bash
gputrace diff /abs/path/left-perfdata.gputrace /abs/path/right-perfdata.gputrace --quick --by-encoder
```

For structured output:

```bash
gputrace diff /abs/path/left-perfdata.gputrace /abs/path/right-perfdata.gputrace --json
```

For focused CSV slices:

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

## Common Failure Modes

- `Accessibility permission is required`: run
  `gputrace xcode-check-permissions` and approve the installed binary in
  System Settings.
- `report` exits with `missing required trace file: .../metadata`: you
  pointed it at the `-perfdata.gputrace` bundle. Pass the original
  `.gputrace` and use `--profiler` for the raw directory.
- `report` says no `.gpuprofiler_raw`: the trace was never profiled. Run
  `gputrace profile` first, or pass `--profiler <DIR>` to point at an
  existing raw directory.
- Diff has many unmatched dispatches: compare the same workload and prefer
  profiled `-perfdata.gputrace` bundles on both sides.
