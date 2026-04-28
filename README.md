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
# One-shot agent-readable report directory.
gputrace report trace-perfdata.gputrace --output trace-report

# High-level trace structure.
gputrace stats trace.gputrace
gputrace analyze trace.gputrace

# Profiler and timing summaries when .gpuprofiler_raw data is present.
gputrace profiler trace-perfdata.gputrace --format json
gputrace profiler-coverage trace-perfdata.gputrace --format text
gputrace xcode-mio trace-perfdata.gputrace
gputrace xcode-mio trace-perfdata.gputrace --format summary-json
gputrace timing trace-perfdata.gputrace --format csv

# Xcode-exported counter CSV analysis.
gputrace xcode-counters trace-perfdata.gputrace --format summary
gputrace xcode-counters trace-perfdata.gputrace --csv Counters.csv --format json

# Raw profiler-bundle counter structures, without a counter CSV.
gputrace raw-counters trace-perfdata.gputrace --format text
gputrace raw-counters trace-perfdata.gputrace --format json

# Combined offline profile/counter export for tools and LLMs.
gputrace export-counters trace-perfdata.gputrace --format json
gputrace export-counters trace-perfdata.gputrace --format csv

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
gputrace profile trace.gputrace --output trace-perfdata.gputrace
```

`xcode-counters` auto-discovers only exact trace-name CSV matches. Pass `--csv`
when a directory contains unrelated `*Counters.csv` files.

`report` is the preferred entry point for agents. It writes a directory of
Markdown files (`index.md`, `xcode-mio.md`, `analysis.md`, `insights.md`,
`profiler.md`, `timing.md`, `shaders.md`, `counters.md`, and coverage notes)
from one CLI invocation. It loads Xcode's private MIO decoder once and reuses
that summary across analysis/insights. Legacy structural parser failures are
listed in `index.md` as diagnostics instead of forcing agents to run those
commands manually.

`profiler` reports real `streamData` dispatch timing. Its
`pipeline_id_scan_costs` field is a debug-only scan of `Profiling_f_*` bytes,
not Xcode's Performance/Cost percentage.
`xcode-mio` is macOS-only and loads Xcode's private `GTShaderProfiler`
framework to decode the same exported `streamData` topology Xcode uses:
GPU command count, encoder count, pipeline list, pipeline object ids, pipeline
addresses, function names, and shader-binary references. The default output is
an LLM-friendly summary; `--format json` still dumps the full private object
graph. It intentionally reports unresolved cost records as counts rather than
presenting them as Xcode Cost percentages.
`shaders` keeps duration/cost as the primary ranking, but adds `Addr Hits` /
`profiling_address_hits` when `Profiling_f_*` address samples can be joined
through APS program-address mappings.

`profiler-coverage` reports byte coverage for Xcode `.gpuprofiler_raw` exports.
Use it when checking reversal progress: it groups `streamData`,
`Profiling_f_*`, `Counters_f_*`, `Timeline_f_*`, and other raw files by byte
share and labels each family as semantic, heuristic, partial, or opaque.

`export-counters` is the preferred structured offline feed for downstream tools.
It does not need an Xcode counter CSV. Its rows include `metric_source` so
consumers can distinguish `profile-dispatch-time`, `profile-execution-cost`
when available, and `aps-counter-samples`. Legacy timeline/raw-counter fallback
rows are emitted only when richer profiler/APS rows are unavailable, because
they are heuristic and can be noisier than the decoded profiler data.
JSON output also includes `metrics` plus `metric_metadata` for APS-derived
metrics, including Apple/Xcode counter graph keys, units, groups, timeline
groups, visibility, and descriptions where the local Xcode/AGX catalogs provide
them.

`raw-counters` reads the profiler bundle directly and enriches raw counter hashes
with installed AGX Metal catalog names where the local system provides them.
It also exposes APS trace-id maps and `program_address_mappings`, the
encoder/draw/function/binary/address table Xcode uses to bridge USC/MIO samples
back to shader code ranges. `profiling_address_summary` scans `Profiling_f_*`
payloads against those ranges and reports address-derived shader/function hit
counts. Its JSON output includes `derived_metrics` computed by running Apple's
local AGX `*-derived.js` formulas against decoded raw counter variables; it does
not require an Xcode counter CSV. `grouped_derived_metrics` repeats those
formula evaluations per raw counter sample group/source, includes Xcode counter
graph metadata where available, and carries profiler-dispatch fields only when
the raw counter timestamps overlap `streamData` dispatch tick windows.

## Main Commands

| Area | Commands |
| --- | --- |
| Trace overview | `report`, `stats`, `analyze`, `dump`, `dump-records`, `api-calls` |
| Profiling and timing | `profiler`, `xcode-mio`, `timing`, `timeline`, `raw-counters`, `xcode-counters`, `export-counters`, `validate-counters` |
| Diffing | `diff`, `markdown diff` |
| Shader analysis | `shaders`, `shader-source`, `shader-hotspots`, `correlate` |
| Command structure | `command-buffers`, `encoders`, `kernels`, `dependencies`, `tree`, `graph`, `fences` |
| Buffer analysis | `buffers`, `buffer-access`, `buffer-timeline`, `clear-buffers` |
| Xcode automation | `profile`, `xcode-profile`, `xcode-check-permissions` |
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
