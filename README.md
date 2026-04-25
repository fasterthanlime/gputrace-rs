# gputrace-rs

Rust port of the retained `gputrace` functionality for Apple Metal `.gputrace`
bundles.

## Install

```bash
cargo xtask install
```

The installer builds the `gputrace` release binary, copies it to
`~/.cargo/bin/gputrace`, signs the installed copy in place for stable macOS TCC
identity, verifies `gputrace --version`, and checks/requests Accessibility
permission for Xcode automation.

Accessibility is a macOS privacy permission, not a codesign entitlement. If the
installer reports that Accessibility is not granted, enable
`~/.cargo/bin/gputrace` in System Settings > Privacy & Security > Accessibility.

## Quick Start

```bash
# High-level trace structure.
gputrace stats trace.gputrace
gputrace analyze trace.gputrace

# Profiler and timing summaries when .gpuprofiler_raw data is present.
gputrace profiler trace-perfdata.gputrace --format json
gputrace timing trace-perfdata.gputrace --format csv

# Xcode-exported counter CSV analysis.
gputrace xcode-counters trace-perfdata.gputrace --format summary
gputrace xcode-counters trace-perfdata.gputrace --csv Counters.csv --format json

# Shader and source attribution.
gputrace shaders trace-perfdata.gputrace --format json
gputrace shader-source trace-perfdata.gputrace kernel_name --search-path src

# Diff two profiled traces.
gputrace diff left-perfdata.gputrace right-perfdata.gputrace --quick
gputrace diff left-perfdata.gputrace right-perfdata.gputrace --markdown --md-out report.md
gputrace diff left-perfdata.gputrace right-perfdata.gputrace --csv --by function

# Auto-discover the newest Go/Python benchmark pair.
gputrace diff --bench-dir /path/to/bench-traces --quick --by-encoder

# Xcode profile automation.
gputrace xcode-check-permissions
gputrace xcode-profile run trace.gputrace --output trace-perfdata.gputrace
```

`xcode-counters` auto-discovers only exact trace-name CSV matches. Pass `--csv`
when a directory contains unrelated `*Counters.csv` files.

`profiler` reports real `streamData` dispatch timing. Its
`pipeline_id_scan_costs` field is a debug-only scan of `Profiling_f_*` bytes,
not Xcode's Performance/Cost percentage.

## Main Commands

| Area | Commands |
| --- | --- |
| Trace overview | `stats`, `analyze`, `dump`, `dump-records`, `api-calls` |
| Profiling and timing | `profiler`, `timing`, `timeline`, `xcode-counters`, `export-counters`, `validate-counters` |
| Diffing | `diff`, `markdown diff` |
| Shader analysis | `shaders`, `shader-source`, `shader-hotspots`, `correlate` |
| Command structure | `command-buffers`, `encoders`, `kernels`, `dependencies`, `tree`, `graph`, `fences` |
| Buffer analysis | `buffers`, `buffer-access`, `buffer-timeline`, `clear-buffers` |
| Xcode automation | `xcode-profile`, `xcode-status`, `xcode-wait`, `xcode-buttons`, `xcode-tabs`, `xcode-export-counters`, `xcode-export-memory` |
| Metal libraries | `mtlb`, `mtlb-functions`, `mtlb-stats`, `mtlb-inventory` |
| Markdown | `markdown render`, `markdown analyze`, `markdown diff`, `markdown buffers`, `markdown buffers-diff` |

Run `gputrace <command> --help` for the exact flags.

## Scope

Included in this Rust port:

- Trace parsing and structural analysis.
- Profiler streamData/raw timing analysis.
- Xcode UI automation for profiling and exports.
- Xcode counter CSV import/validation.
- Shader, buffer, command-buffer, timing, diff, and markdown reporting.
- Benchmark pair discovery for diffing Go/Python trace outputs.

Not included by design:

- pprof export.
- Perfetto export.
- Live/direct GPU capture.
- Web UI serving.

## LLM Usage

For agent-oriented workflows, see [docs/LLM_USAGE.md](docs/LLM_USAGE.md).

For the full capture-profile-export-analyze loop through Xcode, see
[docs/PROFILE_WORKFLOW.md](docs/PROFILE_WORKFLOW.md).
