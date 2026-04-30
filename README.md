# gputrace-rs

Rust port of the retained `gputrace` functionality for Apple Metal `.gputrace`
bundles.

## Install

```bash
cargo xtask install
```

The installer builds the `gputrace` release binary, copies it to
`~/.cargo/bin/gputrace`, signs the installed copy in place for stable macOS TCC
identity, and verifies `gputrace --version`.

## Quick Start

```bash
# 1. Profile: drives MTLReplayer headlessly against the original .gputrace
#    and writes a sibling -perfdata.gputrace bundle whose only contents are
#    <stem>.gpuprofiler_raw/ (streamData + Profiling_f_*.raw + Counters_f_*.raw
#    + Timeline_f_*.raw). It does NOT copy the trace metadata.
gputrace profile trace.gputrace --output trace-perfdata.gputrace

# 2. Report: takes the ORIGINAL .gputrace and writes one markdown directory
#    (index.md, xcode-mio.md, analysis.md, insights.md, profiler.md,
#    timing.md, shaders.md, counters.md, profiler-coverage.md, ...).
#    Pointing it at the -perfdata.gputrace bundle fails with
#    `missing required trace file: .../metadata`.
gputrace report trace.gputrace \
  --profiler trace-perfdata.gputrace/trace.gpuprofiler_raw \
  --output trace-report

# Diff two profiled traces.
gputrace diff left-perfdata.gputrace right-perfdata.gputrace --quick
gputrace diff left-perfdata.gputrace right-perfdata.gputrace --markdown --md-out report.md

```

`report` is the only analysis command end users should need. It loads Xcode's
private MIO decoder once and reuses that summary across analysis/insights, and
records structural-parser failures in `index.md` rather than forcing agents to
re-run individual commands.

The per-section drill-down commands (`profiler`, `analyze`, `xcode-mio`,
`insights`, `shaders`, `raw-counters`, `profiler-coverage`,
`export-counters`, `xcode-counters`, `raw-counter-probe`,
`shader-source`/`shader-hotspots`, `command-buffers`, `encoders`,
`kernels`, `api-calls`, `timeline`, `buffers`, etc.) all write into the same
markdown sections that `report` produces in one shot. They exist for plumbing
and parity tests; everything they expose is already in the report directory.

Run `gputrace <command> --help` for the exact flags of any command.

## Scope

Included in this Rust port:

- Trace parsing and structural analysis.
- Profiler streamData/raw timing analysis.
- Headless MTLReplayer profiling.
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
