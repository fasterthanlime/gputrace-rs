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

## The plan (next session)

1. Empirically discover `(gen, variant, rev)` for M4 Pro by calling
   `agxps_gpu_create` with candidate values until `agxps_gpu_is_valid`
   returns true. There are also `agxps_gpu_gen_from_string` / `_to_string`
   helpers — feed it `"AGX2"` / similar to get back known-good ints.
2. Build a Rust FFI module behind a `#[cfg(target_os = "macos")]` flag
   that `dlopen`s `GTShaderProfiler.framework` and binds the agxps
   functions we need.
3. For each `Profiling_f_N.raw`, call `parser_parse` to get a
   `profile_data`, then iterate `esl_cliques` to sum (end-start)
   per kick_id.
4. Map `kick_id` → kernel name via streamData's pipeline metadata
   (the kick number aligns with dispatch index in the encoder's
   gpuCommandInfoData stream).
5. Wire the result into `timing.md` as a separate "GPU-time-backed"
   kernel cost column alongside the existing dispatch-cadence
   estimate, and verify against Xcode's Performance tab on the
   `gputrace-sample` sample binary (5× heavy_alu / 5× light_add,
   ~1000× cost ratio expected).

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
