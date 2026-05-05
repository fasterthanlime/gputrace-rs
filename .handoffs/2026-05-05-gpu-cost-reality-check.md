# Handoff: GPU Cost Reality Check

## Completed

- Committed `5a727bd` documenting the Xcode shader command timing path and blocker.
- Added focused `gpuCommandForFunctionIndex:subCommandIndex:` probing in `src/xcode_mio.rs`.
- Updated `docs/AGXPS_API.md` with current synthetic Xcode oracle numbers and private cost-timeline findings.

## Active Work

### Origin

The user is trying to make `gputrace-rs` report GPU timing/cost data that is useful for real Metal workloads and ideally comparable to Xcode.

The current user concern:

> "there are so many different subcommands now. It's really hard for an agent coming in to understand what it is they're supposed to do, what it is they're supposed to, to use, what's you know, what's working, what's not working."

The practical goal is no longer "keep reverse-engineering indefinitely." It is to make the current state clear and quota-efficient.

### The Problem

There are multiple GPU analysis commands and several partially-successful reverse-engineering paths:

- `timing`
- `xcode-mio`
- `xcode-command-costs`
- AGXPS raw profile decoding
- Xcode private MIO topology decoding
- private Xcode shader-profiler command timing probes
- pasted Xcode table comparison

A fresh agent can easily waste quota chasing dead ends that are already documented. The next useful work is documentation/productization: make it explicit which command is stable, which command is an oracle, and which RE paths are blocked.

### Current State

Branch: `main`

Recent commits:

- `5a727bd xcode-mio: checkpoint shader command timing RE`
- `18bb2a5 xcode-mio: add command cost oracle plumbing`
- `d57b902 xcode-mio: print AGXPS rows by ESL address`

Worktree should be clean except for any docs added after this handoff.

What is known:

- Xcode's displayed command `Execution Cost` is command function execution time divided by total GPU time.
- On the synthetic trace, exact Xcode times are:
  - total GPU time: `417.25 us`
  - `light_add`: `82 ns`, `46 ns`, `118 ns`, `124 ns`, `27 ns`
  - `heavy_alu`: `76.69 us`, `68.75 us`, `89.16 us`, `89.20 us`, `93.07 us`
- Xcode UI path from disassembly:
  - `shaderProfilerResults()`
  - `gpuCommandForFunctionIndex:subCommandIndex:`
  - returned command `timingInfo.computeTime`
  - divide by `result.timingInfo.time`
- Our standalone `xcode-mio` path gets `GTMioShaderProfilerGPUCommand` objects, but their `timingInfo` is zero because it calls `costForLevel:levelIdentifier:scope:scopeIdentifier:cost:` against a private cost timeline that is not populated in our standalone processing path.
- Therefore exact private-Xcode parity is blocked on private cost-timeline population, not on the formula.

### Technical Context

Important files:

- `docs/AGXPS_API.md`
  - Main RE diary and current truth source.
  - Look for "Xcode's private command-time path".
- `src/xcode_mio.rs`
  - Private Xcode MIO backend.
  - `decode_shader_profiler_gpu_command_times` probes `gpuCommandForFunctionIndex:subCommandIndex:`.
- `src/xcode_command_costs.rs`
  - Compares a pasted Xcode GPU Commands table against native candidate metrics.
- `src/cli.rs`
  - CLI subcommand definitions.

Important commands:

```bash
cargo run --release -- timing --agxps /tmp/gputrace-sample/sample.gputrace
cargo run --release -- xcode-mio -f raw-text /tmp/gputrace-sample/sample.gputrace
cargo run --release -- xcode-command-costs --table /tmp/sample-compute-kernel.txt /tmp/gputrace-sample/sample.gputrace
```

Important traces:

- Synthetic small trace:
  - `/tmp/gputrace-sample/sample.gputrace`
  - `/tmp/gputrace-sample/sample-perfdata/sample.gpuprofiler_raw`
- Real QA trace:
  - `/Users/amos/bearcove/bee/target/gputrace-captures/qa-decode-ar-legacy.gputrace`

Observed private backend speed:

- Small synthetic trace with `xcode-mio`: about `17s` per run in `--release`.
- Real traces are expected to be much slower.

### Success Criteria

For the next useful chunk, do not chase exact Xcode first. Instead:

1. Add a concise guide explaining which command to run for each purpose.
2. Label private-framework outputs as experimental/oracle-only.
3. Label native AGXPS metrics as approximate candidate metrics.
4. Keep exact-Xcode RE behind a hard timebox if resumed.
5. Make the report output avoid implying exact Xcode parity.

### Files to Touch

- `docs/AGENT_GUIDE.md` or equivalent: add a short decision map.
- `docs/LLM_USAGE.md`: link the guide if possible.
- `README.md`: optionally link the guide from the command overview.
- `src/report.rs` / `src/analysis/*`: only if output wording falsely implies exactness.

### Decisions Made

- Do not ship private `GTShaderProfiler` as the main product path.
- Use private Xcode integration as an oracle and RE aid only.
- Use native profile decoding for product-facing reports.
- Prefer AGX analyzer-weighted / `w1` candidate metrics with explicit labels and fixture-backed error bounds.
- Stop open-ended private RE for now because it burns quota and may break on any Xcode/macOS update.

### What NOT to Do

- Do not keep spelunking private frameworks without a fixed hypothesis and timebox.
- Do not claim `xcode-mio` gives exact Xcode command costs.
- Do not chase counter-name obfuscation maps for this problem; it is not the current blocker.
- Do not recompute SHA-256 counter names; that was already ruled out.
- Do not treat `GTMioNonOverlappingCounters` as the public Compute Kernel table; it only showed internal `PredicatedALUPercentage=100` on raw-directory profiles.

### Blockers/Gotchas

- `xcode-mio` can decode topology but not exact command cost because cost timeline values are zero in standalone mode.
- Xcode UI has extra live model/proxy state that our direct private framework path may not recreate.
- `gpuCommandForFunctionIndex:subCommandIndex:` responding to a selector does not mean timing is populated.
- The synthetic trace is the right loop for RE; the QA trace is too large for fast iteration.
- The user's Codex quota is a real constraint. Prefer small, committed, high-signal changes.

## Bootstrap

```bash
git status
cargo check --all-features --all-targets --message-format=short
rg -n "Xcode's private command-time path|Execution Cost|xcode-command-costs" docs src
```

