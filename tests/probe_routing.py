#!/usr/bin/env python3
"""Probe alternative routing keys for clique attribution.

(1) Is `work_shader_address` more pipeline-unique than `esl_shader_address`?
(2) Do GPU commands carry pipeline-id and timestamps so we could route
    timing records to whichever command's time window they fall inside?
"""

import json
from pathlib import Path

data = json.load(open("/tmp/real-mio-legacy.json"))

# (1) Per the agxps_trace_costs already populated, which (esl, work) addresses
#     map to which pipelines? Look for collisions on work_shader_address.
work_to_pipes = {}
esl_to_pipes = {}
for p in data["pipelines"]:
    for c in p.get("agxps_trace_costs", []):
        work = c.get("work_shader_address", 0)
        esl = c.get("shader_address", 0)
        work_to_pipes.setdefault(work, set()).add(p.get("function_name", "?"))
        esl_to_pipes.setdefault(esl, set()).add(p.get("function_name", "?"))

shared_work = {a: ps for a, ps in work_to_pipes.items() if len(ps) > 1}
shared_esl = {a: ps for a, ps in esl_to_pipes.items() if len(ps) > 1}
print(f"esl shared (in computed costs): {len(shared_esl)} of {len(esl_to_pipes)}")
print(f"work shared (in computed costs): {len(shared_work)} of {len(work_to_pipes)}")
print()

# (2) Inspect the gpu_commands list. Do they have timestamps + pipeline_id?
cmds = data.get("gpu_commands", [])
print(f"# gpu_commands: {len(cmds)}")
if cmds:
    print("first command keys:", sorted(cmds[0].keys()))
    print("first command:", json.dumps(cmds[0], indent=2)[:1200])
print()

# (3) What does timeline_pipeline_state_ids look like?
tpsi = data.get("timeline_pipeline_state_ids", [])
print(f"# timeline_pipeline_state_ids: {len(tpsi)}")
if tpsi:
    print("first item:", tpsi[0])

# (5) Walk all shader_binary_references and bucket by (raw5, raw6).
from collections import defaultdict

buckets = defaultdict(int)
addr_by_kind_to_pipes = defaultdict(lambda: defaultdict(set))
for p in data["pipelines"]:
    for ref in p.get("shader_binary_references", []):
        kind = (ref.get("raw5"), ref.get("raw6"))
        buckets[kind] += 1
        addr_by_kind_to_pipes[kind][ref["address"]].add(p.get("function_name", "?"))
print("\nreference kind buckets (raw5,raw6) -> count:")
for kind, n in sorted(buckets.items(), key=lambda kv: -kv[1]):
    addr_to_pipes = addr_by_kind_to_pipes[kind]
    shared = sum(1 for ps in addr_to_pipes.values() if len(ps) > 1)
    print(
        f"  {kind}: {n:>4} refs, {len(addr_to_pipes):>4} unique addrs, {shared:>3} shared across pipelines"
    )

# (6) For shared ESL addrs, what other reference kinds does each pipeline have?
#     Maybe one of them happens to match the work_shader_address.
shader_addresses_per_pipeline_by_kind = {}
for p in data["pipelines"]:
    by_kind = defaultdict(set)
    for ref in p.get("shader_binary_references", []):
        by_kind[(ref.get("raw5"), ref.get("raw6"))].add(ref["address"])
    shader_addresses_per_pipeline_by_kind[p["index"]] = by_kind

# Print which kinds matvec and fused_rmsnorm have
name_to_idx = {p.get("function_name"): p["index"] for p in data["pipelines"]}
for name in [
    "tq6_1s_matvec_prerot_qa",
    "tq6_fused_rmsnorm_activation_quantize",
    "tq1s_attention_sequence_gqa_roped_cache_k8_v4",
]:
    idx = name_to_idx[name]
    print(f"\nkinds for {name} (idx {idx}):")
    for kind, addrs in sorted(
        shader_addresses_per_pipeline_by_kind[idx].items(), key=lambda kv: -len(kv[1])
    ):
        print(f"  {kind}: {len(addrs)} addrs")

# (7) For each pipeline pair that share an ESL address, print one example of
#     all the kinds of refs they hold (do they differ in some unique field?)
print()

# (8) For each pipeline that has agxps costs, list the work_shader_address(es)
# we observed and compare to the (2,8) and (6,8) reference addresses.
print("work_shader_address vs (2,8)/(6,8) refs per pipeline:")
for p in data["pipelines"]:
    if not p.get("agxps_trace_costs"):
        continue
    works = sorted(set(c["work_shader_address"] for c in p["agxps_trace_costs"]))
    refs_28 = sorted(
        ref["address"]
        for ref in p.get("shader_binary_references", [])
        if (ref.get("raw5"), ref.get("raw6")) == (2, 8)
    )
    refs_68 = sorted(
        ref["address"]
        for ref in p.get("shader_binary_references", [])
        if (ref.get("raw5"), ref.get("raw6")) == (6, 8)
    )
    print(
        f"  {p.get('function_name', '?'):<48} works={['0x%x' % w for w in works]} "
        f"refs(2,8)={['0x%x' % a for a in refs_28]} refs(6,8)={['0x%x' % a for a in refs_68]}"
    )

# (4) gpu_command_function_times: per-command durations from durationForDraw
fts = data.get("gpu_command_function_times", [])
print()
print(f"# gpu_command_function_times: {len(fts)}")
if fts:
    print("keys:", sorted(fts[0].keys()))
    print("first row:", fts[0])
    # Aggregate per pipeline_index per source.
    from collections import defaultdict

    by_src_pipe = defaultdict(lambda: defaultdict(int))
    for r in fts:
        by_src_pipe[r.get("source")][r.get("pipeline_index")] += r.get("duration_ns", 0)
    pipe_names = {p["index"]: p.get("function_name", "?") for p in data["pipelines"]}
    for src, pmap in by_src_pipe.items():
        total = sum(pmap.values())
        if not total:
            continue
        print(f"\nsource={src!r}  total ns={total}")
        rows = sorted(pmap.items(), key=lambda kv: -kv[1])
        for pidx, ns in rows:
            print(
                f"  {ns * 100 / total:6.2f}%  pipeline_index={pidx:>2} {pipe_names.get(pidx, '?')}"
            )
