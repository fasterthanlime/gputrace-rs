# Agent Guide

This project has several GPU profiling paths. Use this guide before
starting new analysis work.

## Default Path

Use native decoding for product work:

```bash
cargo run --release -- timing --agxps TRACE.gputrace
```

This is the stable direction. Its pipeline costs are approximate
candidate metrics, not exact Xcode percentages.

## Xcode Oracle Path

Use this only to compare against Xcode-copied ground truth:

```bash
cargo run --release -- xcode-command-costs --table XCODE_TABLE.txt TRACE.gputrace
```

The table should come from Xcode's **Counters -> GPU Commands ->
Compute Kernel** view. Copy/paste the cells into a tab-delimited text
file.

This command is for validation and error measurement. It is not a
decoder for hidden Xcode data.

## Private MIO Path

Use this only for reverse-engineering or topology debugging:

```bash
cargo run --release -- xcode-mio -f raw-text TRACE.gputrace
```

It requires Xcode's private `GTShaderProfiler.framework`, is slow, and
may break across Xcode/macOS versions.

What works:

- GPU command, encoder, and pipeline topology.
- Mapping Xcode pipeline addresses to command rows.
- Some private shader/profiler metadata.
- Probes showing where exact Xcode timing would come from.

What does not work:

- Exact Xcode per-command `Execution Cost`.
- Public Compute Kernel table extraction.
- Private cost timeline population in standalone mode.

Current exact-Xcode blocker:

- Xcode UI computes command cost from
  `gpuCommandForFunctionIndex:subCommandIndex:` ->
  `timingInfo.computeTime`.
- For MIO raw profiles, `GTMioShaderProfilerGPUCommand.timingInfo`
  calls `costForLevel:levelIdentifier:scope:scopeIdentifier:cost:`.
- Our standalone path currently gets zero cost values from that private
  cost timeline.

## What Not To Chase

- Counter-name obfuscation maps. They are not the current timing
  blocker.
- `GTMioNonOverlappingCounters` as the public Compute Kernel table. It
  did not expose Xcode's displayed columns on raw-directory profiles.
- Broad private-framework spelunking without a narrow hypothesis.
- The large QA trace for first-pass RE. Use the synthetic trace first.

## Useful Fixtures

Small synthetic trace:

```text
/tmp/gputrace-sample/sample.gputrace
/tmp/gputrace-sample/sample-perfdata/sample.gpuprofiler_raw
```

Real QA trace:

```text
/Users/amos/bearcove/bee/target/gputrace-captures/qa-decode-ar-legacy.gputrace
```

## Ground Truth Summary

For the synthetic trace, Xcode's command costs are function execution
time divided by total GPU time:

```text
total GPU time: 417.25 us
light_add total: 397 ns = 0.095%
heavy_alu total: 416.85 us = 99.904%
```

See `docs/AGXPS_API.md` for the detailed RE notes.

