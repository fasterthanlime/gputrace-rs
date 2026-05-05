# gputrace-rs

`gputrace` turns an existing Apple Metal `.gputrace` capture into a Markdown GPU
performance report.

The public workflow is one command:

```bash
gputrace report /abs/path/input.gputrace
```

If the trace already contains cached profiler data, `report` reuses it. If not,
it prints a line saying it is profiling, runs Apple's headless `MTLReplayer.app`,
stores both the profiler data and the Markdown report inside the `.gputrace` bundle.

Internal subcommands still exist for tests and reverse engineering, but they are
not part of normal use.

## Install

```bash
cargo xtask install
```

The installer builds the release binary, copies it to
`~/.cargo/bin/gputrace`, signs the installed copy for stable macOS identity,
and verifies `gputrace --version`.

## Capture

`gputrace` does not perform live capture. Your application must first create a
`.gputrace` bundle, usually by running with Metal capture enabled.

Keep captures small: one useful pulse, phase, or workload step. Profiling
replays the trace and can use substantial memory for large captures.

## Report

Run:

```bash
gputrace report /abs/path/input.gputrace
```

By default, this writes:

```text
/abs/path/input.gputrace/gputrace-profile/input.gpuprofiler_raw/
/abs/path/input.gputrace/gputrace-report/
```

Use `--output` only when you want the Markdown report somewhere else:

```bash
gputrace report /abs/path/input.gputrace --output /abs/path/report-dir
```

The report directory contains Markdown files such as:

```text
index.md
analysis.md
insights.md
timing.md
shaders.md
counters.md
xcode-mio.md
profiler.md
profiler-coverage.md
```

Start with `index.md`. It links the rest of the report and records parser or
profiler failures in one place.

## What The Report Means

The report combines:

- `.gputrace` structure: command buffers, encoders, resources, shader names,
  and API command topology.
- Cached `.gpuprofiler_raw` data: MTLReplayer/Xcode profiler streams.
- Native AGX timing analysis from `Profiling_f_*.raw`.
- Xcode private MIO topology when available, used internally for command and
  pipeline mapping.

GPU cost percentages are estimates unless explicitly labeled as imported Xcode
ground truth. Apple does not expose the exact Xcode shader cost model as a
stable public file format.

For source mapping, search your source tree by hot kernel names from the
report:

```bash
rg -n 'hot_kernel_name|another_hot_kernel' /abs/path/source/root
```

## Common Failure Modes

### `report` starts profiling and then no `streamData` appears

The replay failed. Run again with MTLReplayer logs if you are debugging through
an internal command, or capture a smaller trace. The common causes are archive
open failure, missing/incompatible Metal device, or GPU timeout.

### GPU command-buffer timeout

The trace is usually too large or was profiled on unsuitable hardware. Capture
a smaller workload window and profile on real Apple Silicon hardware.

### The report looks stale

Delete the cached profiler directory and rerun `report`:

```bash
rm -rf /abs/path/input.gputrace/gputrace-profile

gputrace report /abs/path/input.gputrace
```

## Internal Commands

Only `report` is public. Other subcommands are hidden from help and are for
report implementation, regression checks, and reverse engineering.
