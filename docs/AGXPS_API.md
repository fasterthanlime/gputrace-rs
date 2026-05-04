# Decoding Profiling_f / Counters_f via Apple's `agxps` C API

`Profiling_f_*.raw` carries the per-USC-stream sample data Xcode uses to
compute kernel cost percentages. The on-disk format isn't documented but
**is decodable via Apple's `agxps` C API** — exported by
`GPUToolsReplay.framework` (part of macOS, not Xcode) and reachable
either by linking MTLReplayer.app's process or by dlopen.

## Where the API lives

| Build | Path | Symbols |
|---|---|---|
| **macOS (host or VM)** | `/System/Library/PrivateFrameworks/GPUToolsReplay.framework/GPUToolsReplay` (in dyld_shared_cache) | 15 `agxps_aps_*` + ~358 `agxps_gpu_*` / `agxps_timeseries_*` |
| **Xcode (host only)** | `/Applications/Xcode.app/Contents/PlugIns/GPUDebugger.ideplugin/Contents/Frameworks/GTShaderProfiler.framework/Versions/A/GTShaderProfiler` (real on-disk binary) | 374 `agxps_*` (richer API including `agxps_aps_kick_time_stats_create_sampled` and friends) |

The macOS-on-VM build has a leaner API surface but is enough to decode
the sample stream and read per-kick timestamps, software IDs, and
counter values.

## Verified end-to-end working call chain (macOS 26.4 VM, no GPU)

Done via `lldb` driving `MTLReplayer` to which `GPUToolsReplay` is
already linked. Successfully decoded `Profiling_f_0.raw` from the
`gputrace-sample` trace into 48 kicks.

```
gpu = agxps_gpu_create(/*gen=*/16, /*variant=*/3, /*rev=*/1, /*flag=*/false)

descriptor = agxps_aps_descriptor_create()           // 0x68-byte struct
*(uintptr_t*)(descriptor + 0x00) = (uintptr_t)gpu    // gpu pointer
*(u32*)(descriptor + 0x08) = 0x100                   // see "validation" below
*(u32*)(descriptor + 0x0c) = 0x400
*(u32*)(descriptor + 0x10) = 0                       // 0 is OK
*(u64*)(descriptor + 0x18) = 0x1000                  // already set by descriptor_create

parser = agxps_aps_parser_create(descriptor)         // returns NULL if dispatch lookup fails

uint8_t out[4096] = {0};
profile_data = agxps_aps_parser_parse(parser,
                                      file_bytes, file_size,
                                      /*type=*/0x21, out)
// out[0] = 0 on success; non-zero = error code (use agxps_aps_parse_error_type_to_string)

n = agxps_aps_profile_data_get_kicks_num(profile_data)
agxps_aps_profile_data_get_kick_start       (profile_data, &starts[0],  start_idx, count)
agxps_aps_profile_data_get_kick_software_id (profile_data, &swids[0],   start_idx, count)
agxps_aps_profile_data_get_usc_timestamps   (profile_data, &tses[0],    start_idx, count)
```

## What we got: 48 kicks for a 10-dispatch trace on M4 Pro

The `gputrace-sample` workload was 5×`light_add` + 5×`heavy_alu`
interleaved, ~10 dispatches. One `Profiling_f` file (one of 20 USC
streams) decoded into 48 kicks. Software IDs cluster into 4 distinct
high-32-bit groups (likely 4 distinct kernels / pipeline-state hashes),
each with multiple sequence numbers spread across 3 sub-channels:

```
kick[ 0]  start=1739461754897    swid=0x3761d72234921602
kick[ 1]  start=3058016714783    swid=0x3761d72334921604
kick[ 2]  start=4294967296045    swid=0x3761d72434921606
kick[ 3]  start=2121713844245    swid=0x33920b3634921607   ← rare group
kick[ 4]  start=3079491551263    swid=0x3761d72234921601
...
kick[37]  start=314288526855510  swid=0x19e4518f3492160b
kick[38]  start=327478371421671  swid=0xe85… (bottom 5 are this group)
...
```

Bit layout we suspect for `swid`:
- bits 0..15: kick sequence number
- bits 16..31: trace/process tag (`0x3492` constant on this run)
- bits 32..47: USC-channel sub-index (3 values per kernel-group)
- bits 48..63: kernel-group hash (one per pipeline / clique type)

So the four high16 prefixes (`0x3761`, `0x3392`, `0x19e4`, `0xe85…`)
correspond to four distinct compute pipelines. We haven't verified
this against the streamData metadata yet — that's a documented
follow-up.

## The dispatch tree

`agxps_aps_parser_create` does an internal red-black-tree lookup
keyed on `(gen, variant, rev_with_aps_fallback)` to find a per-GPU
parser implementation. The tree lives in `__DATA.__bss` of
`GPUToolsReplay`:

| Slide | Tree address |
|---|---|
| `framework_base + 0x4DF0B578` | global root pointer |

In our VM run with `GPUToolsReplay` slid to `0x25742d000`, the tree
root was at `0x2a5406578`. **It's populated by C++ static
constructors that run during `dyld` load**, contrary to my earlier
assumption that something else triggered it. 53 entries on this
build — one per supported (gen, variant, rev) triple. Each tree
node is 0x38 bytes:

```
+0x00 left*
+0x08 right*
+0x10 parent*  (low bit = red/black flag)
+0x18 padding/color
+0x20 u64 (gen | (variant << 32))
+0x28 u32 rev_with_aps_fallback  + u32 padding
+0x30 PAC-signed function pointer  → per-GPU parser_create
```

The first node we examined had `(gen=15, variant=4, rev=1)` and a
parser-impl pointer `0x25747c03c`.

## Descriptor layout (96 bytes total, validated by per-GPU parser_create)

`agxps_aps_descriptor_create()` zeroes a 0x68-byte struct, then writes:

```
+0x00 = NULL                     (gpu pointer; you must populate)
+0x18 = 0x1000                   (acceptable: 0x400, 0x1000, 0x40000)
+0x30 = -1
+0x58 = 0x32
```

Per-GPU `parser_create` validates:

| Field | Required value |
|---|---|
| `desc[0x08]` (u32) | power of 2 in `[0x10, 0x800]` |
| `desc[0x0c]` (u32) | power of 2 in `[0x40, 0x2000]` |
| `desc[0x10]` (u32) | `0` OR power of 2 in `[0x80, 0x8000]` |
| `desc[0x18]` (u64) | exactly one of `{0x400, 0x1000, 0x40000}` |

`{0x100, 0x400, 0, 0x1000}` works. These are likely sample-period /
ring-buffer sizing knobs; default values that aren't outlandish for
a typical Xcode capture.

## GPU table

`agxps_aps_gpu_find_supported_revision(gen, variant, rev_in, *rev_out)`
exhaustively probed: 26 supported (gen, variant) pairs across gens
15..20, all rev=1. **M4 Pro is `gen=16, variant=3`** — verified
empirically: only that combo decoded our trace's `Profiling_f_0.raw`
without `Encountered tile start while a tile was still active` errors.

```
gen=15 variant=4..7
gen=16 variant=3..6     ← gen=16 variant=3 = M4 Pro
gen=17 variant=2..7
gen=18 variant=1..4
gen=19 variant=2..7
gen=20 variant=2..3
```

`agxps_gpu_create(gen, variant, rev, false)` allocates a 0x28-byte
heap struct with `[gen u32][variant u32][rev u32][rev_with_aps_fallback
u32][16 zero bytes]`. The VM build automatically writes
`rev_with_aps_fallback` after `find_supported_revision` runs, even
if rev_in == rev_out — different from the host build which only
writes when they differ.

## profile_type

The 4th argument to `parser_parse` is `0x21` — an enum value
(`eAPSProfilingType`). Confirmed working empirically; haven't
enumerated other values.

## Error reporting

`parser_parse` writes a u64 error code into `out[0]` on failure
(returns non-NULL `profile_data` even on error — only `out[0]` tells
you the parse succeeded). Decode with
`agxps_aps_parse_error_type_to_string(code) -> const char *`. Useful
codes seen so far:

| code | message |
|---|---|
| 0 | (success) |
| 2 | `Encountered tile start while a tile was still active on the same channel` (= wrong GPU gen/variant for the data) |

## profile_data getters (VM build, 9 functions)

All `profile_data_get_*_by_index` functions take `(profile_data, out_buf,
start_idx, count)` and return `int` (1 = success, 0 = bounds
violation). Output buffer is filled with `count` u64 values.

```c
unsigned int agxps_aps_profile_data_get_kicks_num     (void *pd);
int          agxps_aps_profile_data_get_kick_start    (void *pd, uint64_t *out, size_t start, size_t count);
int          agxps_aps_profile_data_get_kick_software_id(void *pd, uint64_t *out, size_t start, size_t count);
int          agxps_aps_profile_data_get_usc_timestamps(void *pd, uint64_t *out, size_t start, size_t count);
unsigned int agxps_aps_profile_data_get_counter_num   (void *pd);
// ...get_counter_index, get_counter_values_by_index, get_counter_group_metadata_by_index,
//    get_counter_values_num_by_index — virtual dispatch through profile_data internals,
//    haven't worked out their full sigs yet.
```

For the qa-decode trace's Profiling_f_0:
- `get_kicks_num` = 48
- `get_counter_num` = 12
- `usc_timestamps` vector length = 115 394 (per-USC sample timestamps)

## Open questions / next steps

1. **No `kick_end` or `kick_duration` in the VM build.** Per-kick
   GPU time has to be computed from neighbor kick starts (`end[i] =
   start[i+1]`), or from ranges of `usc_timestamps` falling between
   kicks, or from a function we haven't found yet. The host's richer
   build has `agxps_aps_kick_time_stats_create_sampled` which probably
   does this for us — testable on the host directly via Xcode-loaded
   GTShaderProfiler.

2. **Bridging `kick_software_id` → kernel name.** The high 16 bits
   of `swid` look like they identify a pipeline/clique. Need to
   correlate against streamData's pipeline IDs or
   `pipelineStateInfoData` to confirm. Likely a hash of
   `pipeline_address` or `pipeline_id`.

3. **`get_counter_*` virtual dispatch.** The counter-value getters
   call into a vtable on the parser. Need to either decode the vtable
   layout or just call them blindly with reasonable signatures.

4. **profile_type semantics.** `0x21` works; haven't tried other
   values. `eAPSProfilingType` enum likely has options for
   per-kick, per-instruction, etc.

## Tooling used

- **macOS VM with SIP off** + `lldb-rpc-server` exposed via MCP:
  drove the entire experiment by attaching to MTLReplayer pre-main,
  reading the populated dispatch tree, calling functions via
  `expression`, populating descriptors, single-stepping into the
  per-GPU parser.
- The Apple GPU is paravirtualized in the VM, so we can't *generate*
  perfdata in the VM — but we don't need to. The decoder runs fine
  without a real GPU. Copied a real `Profiling_f_0.raw` from the
  host (where `gputrace profile` had generated it via MTLReplayer
  on actual hardware) and decoded it successfully.

## Implications for gputrace-rs

We can write a Rust FFI binding that:
1. `dlopen`s `GPUToolsReplay.framework` on macOS
2. Resolves `agxps_*` symbols by raw offset (they're not exported
   in the dyld trie; need to use `GPUToolsReplay`'s framework slide
   + known offsets, or `dlsym_aware_addr` tricks)
3. Calls `parser_create`/`parser_parse` per-Profiling_f file
4. Iterates kicks and aggregates per-pipeline cost

The only remaining piece is symbol resolution — the agxps_aps_*
symbols are global text symbols but not in the export trie. Options:
- Parse the LC_SYMTAB ourselves (works without entitlements)
- Use private `dyld_get_image_symbol_by_offset` via a small bootstrap
- Use Mach-O parsing on the cached binary header

Once that's done, the rest of the FFI is straightforward — all
calls are plain C ABI with simple types.
