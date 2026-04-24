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
gputrace xcode-counters trace-perfdata.gputrace --format json
gputrace xcode-counters trace-perfdata.gputrace --format detailed --top 20
gputrace shaders trace-perfdata.gputrace --format json
gputrace shader-hotspots trace-perfdata.gputrace kernel_name --search-path /abs/path/src --format json
gputrace shader-source trace-perfdata.gputrace kernel_name --search-path /abs/path/src --format text
```

If an Xcode counter CSV was exported separately:

```bash
gputrace xcode-counters trace-perfdata.gputrace --csv /abs/path/Counters.csv --format json
gputrace validate-counters trace-perfdata.gputrace --csv /abs/path/Counters.csv --format json
```

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
- Diff has many unmatched dispatches: compare the same workload and prefer
  profiled `-perfdata.gputrace` bundles.
- Shader source not found: pass all relevant source roots with repeated
  `--search-path`.
