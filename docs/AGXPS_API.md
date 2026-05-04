# Decoding Profiling_f / Counters_f via Apple's `agxps` C API

`Profiling_f_*.raw` and `Counters_f_*.raw` carry the per-USC-stream sample
data Xcode uses to compute kernel cost percentages. The on-disk format
isn't documented and isn't trivial to reverse from bytes alone — but
`GTShaderProfiler.framework` ships a complete C API for parsing them, and
the symbols are dynamically linkable.

## Where the API lives

```
/Applications/Xcode.app/Contents/PlugIns/GPUDebugger.ideplugin/
    Contents/Frameworks/GTShaderProfiler.framework/Versions/A/GTShaderProfiler
```

374 exported `agxps_*` C symbols. Verified callable via `dlopen` +
`dlsym` from a plain Swift CLI (no entitlements needed, no SIP
considerations).

## Function chain we want

```
agxps_gpu_create(gen, variant, rev, false) -> agxps_gpu_t
agxps_aps_descriptor_create(...)            -> agxps_aps_descriptor_t   (96-byte struct)
agxps_aps_parser_create(descriptor)         -> agxps_aps_parser_t
agxps_aps_parser_parse(parser, bytes, size, profile_type, &out_data)
                                            -> agxps_aps_profile_data_t
agxps_aps_profile_data_get_esl_cliques_num(profile_data)
agxps_aps_profile_data_get_esl_clique_{start,end,kick_id}(profile_data, idx)
agxps_aps_profile_data_destroy / parser_destroy
```

`profile_type` was `0x21` in the `LoadAPSTraceDataAtIndex` caller.
Likely an `eAPSProfilingType` enum value (= USC sampling).

## Signatures partially recovered from disasm

### `agxps_gpu_create(uint32_t gen, uint32_t variant, uint32_t rev, bool flag)`

Returns a heap-allocated 0x28-byte struct:

```
+0x00 u32 gen
+0x04 u32 variant
+0x08 u32 rev
+0x0c u32 rev (duplicated)
+0x10 u64 zero
+0x18 u64 zero
+0x20 u64 zero (alt rev set later via agxps_aps_gpu_find_supported_revision)
```

For M4 Pro we don't know the right `(gen, variant, rev)` triple yet —
need to discover empirically, or borrow them from the sample trace's
encoded GPU info. Apple internal generations are small ints (gen ~9-17
range based on the `cmp w0, #0x11` checks in callers).

### `agxps_aps_descriptor_create()` — return-by-value

Indirect-result via `x8`, no register args. Returns a ~96-byte struct.
Swift can call this directly if we declare the return type as a
fixed-size tuple and let the ABI route the indirect return correctly.

### `agxps_aps_parser_create(const descriptor_t *desc, ...)`

5 register args (x0..x4) but only x0 (descriptor pointer) is normally
used. Validates `desc->gpu` via `agxps_gpu_is_valid` and returns NULL
if it's invalid — empirically NULL is what we get when calling with
NULL or zero-init descriptor. Need a real `agxps_gpu_create` result in
the descriptor for it to succeed.

### `agxps_aps_parser_parse(parser, const uint8_t *data, size_t size, uint32_t type, void *out)`

Returns `profile_data` handle (or NULL). `out` is a pointer to a
caller-provided struct that receives auxiliary outputs (size unknown,
the LoadAPSTraceDataAtIndex caller used `sp+0x9c..sp+0xX`, ~0x300+
bytes).

### `agxps_aps_profile_data_get_esl_clique_{start,end,kick_id}(profile_data, u32 idx)`

`start`/`end` return `u64` ticks; `kick_id` returns `u32` (= dispatch
index). Use these to compute per-dispatch GPU duration.

## What we learned trying to call this from outside Xcode

Empirical follow-up of the API: `dlopen`/`dlsym` works fine, every
function symbol resolves, and the supported GPU set is discoverable
by sweeping `agxps_aps_gpu_find_supported_revision`. On this M-series
host the (gen, variant) pairs the linked agxps reports as supported are:

```
gen=15 variant=4..7        -> all rev=1
gen=16 variant=3..6        -> all rev=1
gen=17 variant=2..7        -> all rev=1
gen=18 variant=1..4        -> all rev=1
gen=19 variant=2..7        -> all rev=1
gen=20 variant=2..3        -> all rev=1
```

So 26 supported GPUs total, M4 Pro is somewhere in `gen=16, variant=3..6`.

`agxps_gpu_create(gen, variant, 1, false)` succeeds for each of those
and returns a 0x28-byte heap struct: `[gen u32][variant u32][rev u32]
[rev_with_aps_fallback u32][16 zero bytes]`. **Quirk**: `gpu_create`
only writes `rev_with_aps_fallback` (offset 0xc) when
`agxps_aps_gpu_find_supported_revision` returns a *different* rev than
you passed in — pass rev=1 (the only supported one) and it stays at 0.
You have to write it yourself with `*(uint32_t *)(gpu + 0xc) = 1` to
match what `parser_create` expects.

### The blocker

`agxps_aps_parser_create(descriptor)` always returns `NULL` for every
combination tried, even with a freshly constructed gpu descriptor whose
`(gen, variant, rev_aps_fallback)` triple matches the supported table.
Reading the disassembly closely:

1. Validates `descriptor[0]` is a non-NULL gpu pointer (`gpu_is_valid`
   = `cmp x0, 0; cset w0, ne` — just NULL check). Passes.
2. Reads `(gen, variant, rev_aps_fallback)` off the gpu, packs them
   into a 12-byte stack key, and calls a tree-lookup helper at
   `0x5067e0` against a global red-black tree at `0xeef108`.
3. If the lookup returns the end iterator (= "not found"), returns
   `NULL`. **This is what we hit.**
4. Otherwise, loads a function pointer at `result+0x30` and tail-calls
   it with `descriptor` as the only argument — that's the per-GPU
   parser_create implementation.

The tree at `0xeef108` is empty when we `dlopen` the framework
ourselves. Static C++ constructors (`__init_offsets` section, 26
entries of 4 bytes) ran on dlopen, but none of them registered any
parser implementations into the tree. Apple presumably populates it
via a separate registration step we haven't identified yet — possibly
a `+[GTShaderProfiler initialize]` Obj-C class init, possibly a call
into a sibling framework, possibly something the GPU-debugger
ide-plugin invokes only when its UI path runs.

### Open path forward

Two tactics to unblock:

1. **lldb-attach in a VM**: spin up a headless Xcode session that's
   already actively decoded a profiler bundle (via `gputrace profile`'s
   MTLReplayer path or by opening a trace through the IDE), break in
   `agxps_aps_parser_create`'s hashtable lookup, and dump the live
   tree. That gives us:
   - The shape and contents of the populated tree (entries per GPU,
     pointer to per-GPU parser_create_for_gpu function).
   - Whatever side-table or class-init triggered the registrations,
     by stepping back up the call stack from a known good state.
   This is what `r2 + ghidra-in-a-VM` was good at last time.

2. **Look for a whole-program shortcut**: instead of building parser
   ourselves, drive an existing Apple binary that already produces
   per-kernel cost as text/JSON. `MTLReplayer.app -collectProfilerData
   --all` produces the perfdata bundle we already have; there might
   be sibling flags (`-emitCostJSON`, `-print-clique-stats`, etc.) that
   make it dump decoded results. Worth `strings | grep -i cost|stats`
   on `MTLReplayer` and the `xcrun gpus_*` family before another full
   RE pass.

### Functions verified working (no descriptor needed)

These don't go through the per-GPU dispatch, so they're callable
straight from `dlopen` today and could be exposed as a tiny Rust
binding even without unblocking `parser_create`:

- `agxps_gpu_create(gen, variant, rev, flag)`
- `agxps_gpu_destroy`, `gpu_clone`, `gpu_is_valid`
- `agxps_gpu_get_{gen, variant, rev, rev_with_aps_fallback}`
- `agxps_aps_gpu_find_supported_revision(gen, variant, rev_in, *rev_out)`
- `agxps_aps_descriptor_create()` (returns a 0x68-byte default; gpu
  pointer at offset 0 is `NULL`, you populate it before passing to
  parser_create)
- `agxps_gpu_get_num_physical_uscs`, `_l2_caches`, etc. (static GPU
  topology queries — useful for sanity-checking our trace's
  per-stream file counts)

A defensive Rust FFI shim that just exposes those would already let
us label `Profiling_f_N.raw` files by their target USC, and would be
a clean substrate for the rest once we crack `parser_create`.

### Useful functions on the populated profile_data side

(Once `parser_parse` succeeds, these are the getters that give us the
numbers we want — already documented for completeness.)

- `agxps_aps_profile_data_get_esl_cliques_num`
- `agxps_aps_profile_data_get_esl_clique_{start, end, kick_id, esl_id, missing_end}`
- `agxps_aps_profile_data_get_hw_clique_{end, esl_id, ...}`
- `agxps_aps_profile_data_get_counter_values_by_index`
- `agxps_aps_profile_data_destroy`
- `agxps_aps_kick_time_stats_create_sampled` — given a populated
  `profile_data`, computes per-kick (per-dispatch) GPU-time stats by
  summing samples. This is exactly the "kernel cost" aggregation Xcode
  shows; once we have a working parser, this is the one-call shortcut
  to a per-dispatch cost map.

## Tooling notes

- `swift -interpret` buffers stdout aggressively; use `swiftc` to compile
  a binary and unbuffered `write(1, …)` for trace prints if iterating
  on a parser experiment.
- `llvm-objdump --start-address` flag was ignored on the version I
  tried (LLVM 22.1.4); dumped the whole `__text` and `sed -n '<line>,<line>p'`
  worked fine.
- `otool -L` shows GTShaderProfiler links `libcompression`, `libz`,
  `libbz2` — the on-disk format is likely compressed/encoded, which
  is why direct byte-pattern decoding doesn't get you anywhere.
- See `/tmp/agxps_test.swift` (transient scratch) for the dlopen+dlsym
  experiment template.

## The fbuf / FB::Stream format (separate finding)

Different format from `Profiling_f`: this is the function-buffer
serialization for API-call records. Versioned headers
`dy_fbuf_header_v{0,1,2,4}_t` of sizes 12, 24, 28, 36 bytes. After
the header, a null-terminated format string describes argument types
(format chars: `b`=u8, `f`/`i`=u32, `d`/`l`/`p`/`t`/`w`=u64, `C`=u64,
`S`=string, `U`=string-or-NULL pointer, `(<digits>)`=count, `<...>`=tuple).
Then aligned arguments follow. Not directly relevant to USC sample
decoding but documents the broader trace machinery. Implemented in
`GPUTools::FB::Decoder::DecodeHeader` / `DecodeCore` /
`DecodeArguments` in `GPUToolsCore.framework`.
