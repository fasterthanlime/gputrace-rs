#!/usr/bin/env python3
"""Why is fused_rmsnorm still under-attributed even when shared ESLs go to it?"""

import json
from collections import defaultdict

# Use min_commands which is our current best.
data = json.load(open("/tmp/real-mio-min_commands.json"))

target_names = {
    "tq6_1s_matvec_prerot_qa": 69.98,
    "tq6_fused_rmsnorm_activation_quantize": 4.33,
    "tq1s_attention_sequence_gqa_roped_cache_k8_v4": 9.13,
    "tq6_1s_matmul_prerot_tile_wide": 9.35,
}


for p in data["pipelines"]:
    name = p.get("function_name", "?")
    if name not in target_names:
        continue
    costs = p.get("agxps_trace_costs", [])
    if not costs:
        continue
    print(
        f"\n=== {name}  (xcode={target_names[name]}, gpu_command_count={p['gpu_command_count']}) ==="
    )
    print(f"  # cliques in agxps_trace_costs: {len(costs)}")
    # sum some fields
    sums = defaultdict(int)
    for c in costs:
        for k in (
            "command_count",
            "record_cliques",
            "matched_work_cliques",
            "duration_ns",
            "analyzer_weighted_duration",
            "execution_events",
        ):
            sums[k] += c.get(k, 0)
    for k, v in sums.items():
        print(f"  sum({k}) = {v}")
    # top-5 by duration_ns
    sorted_costs = sorted(costs, key=lambda c: -c.get("duration_ns", 0))
    print("  top 5 by duration_ns:")
    for c in sorted_costs[:5]:
        print(
            f"    addr=0x{c['shader_address']:x}  work=0x{c['work_shader_address']:x} "
            f"cmd_count={c['command_count']:>4} record_cliques={c['record_cliques']:>4} "
            f"duration_ns={c['duration_ns']:>10} analyzer_weighted={c.get('analyzer_weighted_duration', 0):>10}"
        )
