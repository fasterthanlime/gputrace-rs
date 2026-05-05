#!/usr/bin/env python3
"""Inspector for xcode-mio --format json output.

Usage: python3 inspect_mio.py /path/to/mio.json
"""

import json
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/real-mio.json"
data = json.load(open(path))

print("top-level keys:", sorted(data.keys()))
print("# pipelines:", len(data.get("pipelines", [])))
p0 = data["pipelines"][0]
print("pipeline keys:", sorted(p0.keys()))
print()

# Show shared ESL addresses across pipelines
addr_to_pipes = {}
for p in data["pipelines"]:
    for ref in p.get("shader_binary_references", []):
        if ref.get("raw5") == 6 and ref.get("raw6") == 28:
            addr_to_pipes.setdefault(ref["address"], []).append(
                (p["index"], p.get("function_name", "?"))
            )
shared = {a: ps for a, ps in addr_to_pipes.items() if len(ps) > 1}
print(f"shared ESL addresses: {len(shared)} / {len(addr_to_pipes)}")
for a, ps in list(shared.items())[:10]:
    names = [n for _, n in ps]
    print(f"  0x{a:x}: {names}")
print()

# Show per-pipeline duration sum and percentages
durations = []
for p in data["pipelines"]:
    s = sum(c.get("duration_ns", 0) for c in p.get("agxps_trace_costs", []))
    if s > 0:
        durations.append((p.get("function_name", "?"), p.get("pipeline_address", 0), s))
total = sum(d for _, _, d in durations)
durations.sort(key=lambda x: -x[2])
print(f"# pipelines with agxps_trace_costs: {len(durations)}; total ns: {total}")
for name, addr, d in durations:
    pct = d * 100.0 / total if total else 0.0
    print(f"  {pct:6.2f}%  0x{addr:x}  {name}")
print()

# Show pipeline iteration order vs gpu_command_count
print("pipeline_index | gpu_command_count | function_name")
for p in data["pipelines"]:
    print(
        f"  {p['index']:>3} | {p.get('gpu_command_count', 0):>6} | {p.get('function_name', '?')}"
    )
print()

# For shared addresses, which pipeline wins under last-write-wins (max index)?
print("shared-addr resolution under current code (last index wins):")
for a, ps in shared.items():
    winner_idx, winner_name = max(ps, key=lambda x: x[0])
    losers = [n for i, n in ps if i != winner_idx]
    pass  # too verbose; just count winners

from collections import Counter

winner_counter = Counter()
for a, ps in shared.items():
    winner_idx, winner_name = max(ps, key=lambda x: x[0])
    winner_counter[winner_name] += 1
print("shared-address winners (which pipeline absorbs them):")
for name, n in winner_counter.most_common():
    print(f"  {n:>3} {name}")
print()

# For shared addresses: do per-pipeline `record_count` values let us infer
# how to split the clique work? If record_count is roughly proportional to
# how many GPU commands of that pipeline reference the ESL, weighting by it
# should be a clean attribution scheme.
print("per-pipeline record_count for first 8 shared addresses:")
addr_to_refs = {}
for p in data["pipelines"]:
    for ref in p.get("shader_binary_references", []):
        if ref.get("raw5") == 6 and ref.get("raw6") == 28:
            addr_to_refs.setdefault(ref["address"], []).append(
                (p["index"], p.get("function_name", "?"), ref.get("record_count", 0))
            )
for a, refs in list(shared.items())[:8]:
    parts = [f"{n}={rc}" for _, n, rc in addr_to_refs[a]]
    print(f"  0x{a:x}: {parts}")
