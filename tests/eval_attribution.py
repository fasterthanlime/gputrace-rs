#!/usr/bin/env python3
"""Evaluate per-pipeline Cost % vs Xcode ground truth across attribution modes.

Runs `gputrace xcode-mio --format json` under three values of
`GPUTRACE_MIO_ATTRIB` (legacy / equal / commands) on the realistic trace and
prints a comparison table plus error summaries.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
GPUTRACE = REPO / "target" / "release" / "gputrace"
TRACE = Path(
    "/Users/amos/bearcove/bee/target/gputrace-captures/qa-decode-ar-legacy.gputrace"
)

# Xcode "Cost %" column from the Shaders tab on `qa-decode-ar-legacy.gputrace`.
XCODE_GROUND_TRUTH = {
    0x72A65E300: ("tq6_1s_matvec_prerot_qa", 69.98),
    0x72A65DC00: ("tq6_1s_matmul_prerot_tile_wide", 9.35),
    0x72A63AD80: ("tq1s_attention_sequence_gqa_roped_cache_k8_v4", 9.13),
    0x72A639C00: ("tq6_fused_rmsnorm_activation_quantize", 4.33),
    0x72A65E680: ("tq6_activation_quantize", 2.54),
    0x72A65DF80: ("tq1s_argmax_rows", 1.45),
    0x72A65EA00: ("tq6_silu_mul_activation_quantize", 1.35),
    0x72A63B480: ("tq1s_add_residual_rms_norm_heads_to", 0.66),
    0x72A63A680: ("tq1s_rms_norm_qk_pair_q_rope_to", 0.44),
    0x72A63AA00: ("tq1s_quantize_and_store_k_tq8_v_tq4", 0.39),
    0x72A63BB80: ("tq1s_add_inplace", 0.30),
    0x72A65D880: ("tq1s_rotate_hidden", 0.02),
    0x72A65D500: ("tq1s_rms_norm_heads_to", 0.02),
    0x72A639880: ("tq6_1s_rows", 0.03),
    0x72A639500: ("tq1s_fill_range_attention_mask", 0.01),
}

# The four heavyweights we care most about; the "top-4" error metric is
# computed across these.
HEAVY = {
    "tq6_1s_matvec_prerot_qa",
    "tq6_fused_rmsnorm_activation_quantize",
    "tq1s_attention_sequence_gqa_roped_cache_k8_v4",
    "tq6_1s_matmul_prerot_tile_wide",
}

MODES = [
    "legacy",
    "equal",
    "commands",
    "inv_commands",
    "first_idx",
    "min_commands",
    "work_addr",
]


def run_mode(mode: str, out_path: Path) -> dict:
    env = os.environ.copy()
    env["GPUTRACE_MIO_ATTRIB"] = mode
    cmd = [str(GPUTRACE), "xcode-mio", str(TRACE), "--format", "json"]
    with open(out_path, "wb") as f:
        proc = subprocess.run(
            cmd, env=env, stdout=f, stderr=subprocess.PIPE, check=False
        )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr.decode("utf-8", errors="replace"))
        raise SystemExit(proc.returncode)
    return json.loads(out_path.read_text())


def per_pipeline_pct(data: dict, field: str = "duration_ns") -> dict:
    """Returns {pipeline_address: (function_name, pct)} using sum(field) over agxps_trace_costs."""
    rows = []
    for p in data["pipelines"]:
        s = sum(c.get(field, 0) for c in p.get("agxps_trace_costs", []))
        rows.append((p.get("pipeline_address", 0), p.get("function_name", "?"), s))
    total = sum(s for _, _, s in rows)
    return {
        addr: (name, (s * 100.0 / total) if total else 0.0) for addr, name, s in rows
    }


FIELDS = [
    "duration_ns",
    "analyzer_weighted_duration",
    "analyzer_avg_duration_sum",
    "stats_word0",
    "stats_word1",
    "execution_events",
    "record_cliques",
    "matched_work_cliques",
]


def summary_for(mode_data: dict, field: str) -> tuple[dict, float, float, float, float]:
    pcts = per_pipeline_pct(mode_data, field=field)
    errs, heavy = [], []
    for addr, (gt_name, gt) in XCODE_GROUND_TRUTH.items():
        r = pcts.get(addr)
        if r is None:
            continue
        err = abs(r[1] - gt)
        errs.append(err)
        if gt_name in HEAVY:
            heavy.append(err)
    if not errs:
        return pcts, 0.0, 0.0, 0.0, 0.0
    return (
        pcts,
        max(errs),
        sum(errs) / len(errs),
        max(heavy) if heavy else 0.0,
        (sum(heavy) / len(heavy)) if heavy else 0.0,
    )


def main() -> None:
    field = "duration_ns"
    sweep = False
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--field":
            field = sys.argv[i + 2]
        elif arg == "--sweep":
            sweep = True
    print(f"Field used to compute Cost %: {field}")
    print(f"Trace: {TRACE}")
    print()
    mode_data = {}
    for mode in MODES:
        out = Path(f"/tmp/real-mio-{mode}.json")
        mode_data[mode] = run_mode(mode, out)
    results = {m: per_pipeline_pct(mode_data[m], field=field) for m in MODES}

    if sweep:
        # Print a field x mode summary grid.
        print("Field x mode summary (top4_max abs error):")
        header = f"  {'field':<28}" + "".join(f" {m:>13}" for m in MODES)
        print(header)
        for f in FIELDS:
            row = f"  {f:<28}"
            for m in MODES:
                _, _, _, top4_max, _ = summary_for(mode_data[m], f)
                row += f" {top4_max:>13.2f}"
            print(row)
        print()
        print("Field x mode summary (mean abs error):")
        print(header)
        for f in FIELDS:
            row = f"  {f:<28}"
            for m in MODES:
                _, _, mean_err, _, _ = summary_for(mode_data[m], f)
                row += f" {mean_err:>13.2f}"
            print(row)
        return

    # Build the comparison table sorted by Xcode % desc.
    addrs = sorted(XCODE_GROUND_TRUTH.keys(), key=lambda a: -XCODE_GROUND_TRUTH[a][1])

    header = f"{'function':<46} {'xcode':>6}  " + "  ".join(
        f"{m:>13} {'err':>6}" for m in MODES
    )
    print(header)
    print("-" * len(header))

    errs = {m: [] for m in MODES}
    heavy_errs = {m: [] for m in MODES}
    for addr in addrs:
        name, gt = XCODE_GROUND_TRUTH[addr]
        row = f"{name:<46} {gt:>6.2f}  "
        for mode in MODES:
            r = results[mode].get(addr)
            if r is None:
                row += f"{'-':>13} {'-':>6}  "
                continue
            _, pct = r
            err = pct - gt
            row += f"{pct:>13.2f} {err:>+6.2f}  "
            errs[mode].append(abs(err))
            if name in HEAVY:
                heavy_errs[mode].append(abs(err))
        print(row)

    print()
    print("Summary (absolute error in percentage points):")
    for mode in MODES:
        if not errs[mode]:
            continue
        max_err = max(errs[mode])
        mean_err = sum(errs[mode]) / len(errs[mode])
        max_heavy = max(heavy_errs[mode]) if heavy_errs[mode] else 0.0
        mean_heavy = (
            sum(heavy_errs[mode]) / len(heavy_errs[mode]) if heavy_errs[mode] else 0.0
        )
        print(
            f"  {mode:<10}  max={max_err:5.2f}  mean={mean_err:5.2f}  "
            f"top4_max={max_heavy:5.2f}  top4_mean={mean_heavy:5.2f}"
        )


if __name__ == "__main__":
    main()
