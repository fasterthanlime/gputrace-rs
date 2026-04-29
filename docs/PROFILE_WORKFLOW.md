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

Run `report`. It is the only analysis command you should need; the
per-section drill-down commands (`profiler`, `analyze`, `xcode-mio`,
`insights`, `shaders`, `raw-counters`, `profiler-coverage`,
`export-counters`, `xcode-counters`, `raw-counter-probe`,
`shader-source`/`shader-hotspots`, etc.) all write into the same markdown
files that `report` already produces in one shot. They exist for plumbing
and parity tests, not for end users.

`report` takes the **original** `.gputrace` bundle (the one with the
`metadata` file), not the `-perfdata.gputrace` bundle that `profile` wrote.
The perfdata bundle is just a wrapper around `<stem>.gpuprofiler_raw/` and
has no trace metadata; pointing `report` at it fails with
`missing required trace file: .../metadata`.

When the profiler raw directory is not adjacent to the trace as
`<trace>.gpuprofiler_raw/`, point at it with `--profiler`:

```bash
gputrace report /abs/path/input.gputrace \
  --profiler /abs/path/input-perfdata.gputrace/input.gpuprofiler_raw \
  --output /abs/path/input-report
```

The report directory is the preferred LLM handoff artifact. Start at
`index.md`, then inspect `xcode-mio.md`, `analysis.md`, `insights.md`,
`profiler.md`, `timing.md`, `shaders.md`, `counters.md`, and
`profiler-coverage.md`.

For source mapping, search the source tree by the hot kernel names that the
report calls out:

```bash
rg -n 'hot_kernel_name|another_hot_kernel' /abs/path/source/root
```

## 6. Common Failure Modes

- `gputrace profile` exits 0 but no `streamData` appears: rerun with
  `--stdout-log /tmp/mtl.stdout --stderr-log /tmp/mtl.stderr` and inspect
  the `#CI-INFO#` lines for the failure reason (commonly: archive open
  failure, missing/incompatible Metal device, GPU command-buffer timeout).
- GPU command-buffer timeout: usually means the trace is too large or
  you're running on a paravirt-GPU VM. Capture a smaller pulse/step, or
  run profile on real Apple Silicon hardware.
- `report` exits with `missing required trace file: .../metadata`: you
  pointed it at the `-perfdata.gputrace` bundle. Pass the original
  `.gputrace` and use `--profiler` for the raw directory.
- `report` says no `.gpuprofiler_raw`: rerun `gputrace profile` to produce
  one, or pass `--profiler <DIR>` to point at an existing one.
