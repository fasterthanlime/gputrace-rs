//! Raw FFI bindings to Apple's private `agxps_aps_*` sample-decoder API.
//!
//! These functions live in
//! `/System/Library/PrivateFrameworks/GPUToolsReplay.framework/GPUToolsReplay`
//! (part of macOS, in the dyld_shared_cache) and decode `Profiling_f_*.raw`
//! files into per-kick timestamps and counter values. The symbols are NOT in
//! the framework's export trie — `dlsym` returns NULL for them — so we resolve
//! them at runtime by their offset within the framework's text segment.
//!
//! See `docs/AGXPS_API.md` for how the call chain was reverse-engineered.
//!
//! ## Status
//!
//! Experimental scaffold. Verified working against macOS 26.4
//! `GPUToolsReplay` (UUID `B1DEE264-D3AF-38F9-BC6E-821AFAE2DB30`). Offsets
//! are hardcoded for that build; loading on other versions returns
//! [`Error::UnsupportedFrameworkVersion`].

#![cfg(target_os = "macos")]

use std::ffi::{CStr, c_char, c_int, c_long, c_uint, c_void};
use std::os::raw::c_uchar;

mod offsets;

/// Errors returned from the agxps decoder pipeline.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("framework not loadable: {0}")]
    Dlopen(String),
    #[error("framework UUID {found} not in offset table; supported: {supported:?}")]
    UnsupportedFrameworkVersion {
        found: String,
        supported: Vec<String>,
    },
    #[error("agxps_gpu_create({generation}, {variant}, {rev}) failed")]
    GpuCreate { generation: u32, variant: u32, rev: u32 },
    #[error("agxps_aps_parser_create returned NULL — descriptor invalid for this GPU")]
    ParserCreate,
    #[error("agxps_aps_parser_parse error code {code}: {message}")]
    ParserParse { code: u64, message: String },
    #[error("agxps_aps_profile_data_get_kick_* call failed (out-of-bounds)")]
    KickAccess,
}

pub type Result<T> = std::result::Result<T, Error>;

// -------- Raw C ABI types --------

/// Opaque GPU descriptor returned by `agxps_gpu_create`.
pub type AgxpsGpu = *mut c_void;

/// 0x68-byte descriptor struct passed to `agxps_aps_parser_create`.
#[repr(C)]
pub struct AgxpsApsDescriptor {
    pub gpu: *mut c_void,        // +0x00
    pub field_0x08: u32,         // +0x08 — power of 2 in [0x10, 0x800]
    pub field_0x0c: u32,         // +0x0c — power of 2 in [0x40, 0x2000]
    pub field_0x10: u32,         // +0x10 — 0 or power of 2 in [0x80, 0x8000]
    pub field_0x14: u32,         // +0x14 — padding
    pub field_0x18: u64,         // +0x18 — must be 0x400, 0x1000, or 0x40000
    pub _pad_0x20: [u8; 0x10],   // +0x20..0x30
    pub field_0x30: u64,         // +0x30 — set to -1 by default
    pub _pad_0x38: [u8; 0x20],   // +0x38..0x58
    pub field_0x58: u64,         // +0x58 — set to 0x32 by default
    pub _pad_0x60: [u8; 0x08],   // +0x60..0x68
}

const _: () = assert!(std::mem::size_of::<AgxpsApsDescriptor>() == 0x68);

impl AgxpsApsDescriptor {
    /// A descriptor matching what we found works empirically against
    /// `GPUToolsReplay` decode of M4 Pro perfdata.
    pub fn defaults_for(gpu: AgxpsGpu) -> Self {
        Self {
            gpu,
            field_0x08: 0x100,
            field_0x0c: 0x400,
            field_0x10: 0,
            field_0x14: 0,
            field_0x18: 0x1000,
            _pad_0x20: [0; 0x10],
            field_0x30: u64::MAX, // -1
            _pad_0x38: [0; 0x20],
            field_0x58: 0x32,
            _pad_0x60: [0; 0x08],
        }
    }
}

/// Opaque parser handle.
pub type AgxpsApsParser = *mut c_void;

/// Opaque decoded-profile-data handle.
pub type AgxpsApsProfileData = *mut c_void;

/// `eAPSProfilingType` value used by `LoadAPSTraceDataAtIndex`. Other
/// values exist but haven't been enumerated.
pub const APS_PROFILING_TYPE_USC_SAMPLES: u32 = 0x21;

// Function-pointer types matching the agxps C ABI.
type FnGpuCreate =
    unsafe extern "C" fn(generation: u32, variant: u32, rev: u32, flag: bool) -> AgxpsGpu;
type FnGpuDestroy = unsafe extern "C" fn(AgxpsGpu);
type FnDescriptorCreate = unsafe extern "C" fn() -> AgxpsApsDescriptor;
type FnParserCreate = unsafe extern "C" fn(*const AgxpsApsDescriptor) -> AgxpsApsParser;
type FnParserDestroy = unsafe extern "C" fn(AgxpsApsParser);
type FnParserParse = unsafe extern "C" fn(
    parser: AgxpsApsParser,
    bytes: *const c_uchar,
    size: c_long,
    profile_type: u32,
    out: *mut c_void,
) -> AgxpsApsProfileData;
type FnProfileDataDestroy = unsafe extern "C" fn(AgxpsApsProfileData);
type FnGetKicksNum = unsafe extern "C" fn(AgxpsApsProfileData) -> c_uint;
type FnGetCounterNum = unsafe extern "C" fn(AgxpsApsProfileData) -> c_uint;
type FnGetU64Range = unsafe extern "C" fn(
    pd: AgxpsApsProfileData,
    out: *mut u64,
    start_idx: u64,
    count: u64,
) -> c_int;
type FnParseErrorString = unsafe extern "C" fn(code: u64) -> *const c_char;

/// Resolved function-pointer table. All Send/Sync because the underlying
/// functions are stateless C functions reading from per-call inputs.
pub struct AgxpsApi {
    pub gpu_create: FnGpuCreate,
    pub gpu_destroy: FnGpuDestroy,
    pub descriptor_create: FnDescriptorCreate,
    pub parser_create: FnParserCreate,
    pub parser_destroy: FnParserDestroy,
    pub parser_parse: FnParserParse,
    pub profile_data_destroy: FnProfileDataDestroy,
    pub get_kicks_num: FnGetKicksNum,
    pub get_counter_num: FnGetCounterNum,
    pub get_kick_start: FnGetU64Range,
    pub get_kick_software_id: FnGetU64Range,
    pub get_usc_timestamps: FnGetU64Range,
    pub parse_error_string: FnParseErrorString,
}

unsafe impl Send for AgxpsApi {}
unsafe impl Sync for AgxpsApi {}

const FRAMEWORK_PATH: &str =
    "/System/Library/PrivateFrameworks/GPUToolsReplay.framework/GPUToolsReplay";

/// Load `GPUToolsReplay` and resolve the agxps function table by offset.
///
/// Returns [`Error::UnsupportedFrameworkVersion`] if the loaded binary's UUID
/// isn't one we have offsets for. The `_keep_alive` field on the returned
/// struct holds the dlopen handle so the framework stays mapped for the life
/// of the API table.
pub fn load() -> Result<LoadedApi> {
    use std::ffi::CString;

    let path = CString::new(FRAMEWORK_PATH).unwrap();
    let handle =
        unsafe { libc::dlopen(path.as_ptr(), libc::RTLD_LAZY | libc::RTLD_LOCAL) };
    if handle.is_null() {
        let err = unsafe { CStr::from_ptr(libc::dlerror()) }
            .to_string_lossy()
            .into_owned();
        return Err(Error::Dlopen(err));
    }

    let (base, _slide, uuid) = unsafe { framework_base_and_uuid() }
        .map_err(|e| Error::Dlopen(format!("can't read GPUToolsReplay header: {e}")))?;

    let table = offsets::lookup(&uuid).ok_or_else(|| Error::UnsupportedFrameworkVersion {
        found: uuid.clone(),
        supported: offsets::supported_uuids(),
    })?;

    // Offsets are relative to the framework's runtime image base
    // (`_dyld_get_image_header`), so just add directly. Cache build differences
    // and ASLR slide are absorbed into `base`.
    let resolve = |off: usize| (base as usize + off) as *const c_void;
    let api = AgxpsApi {
        gpu_create: unsafe { std::mem::transmute(resolve(table.gpu_create)) },
        gpu_destroy: unsafe { std::mem::transmute(resolve(table.gpu_destroy)) },
        descriptor_create: unsafe { std::mem::transmute(resolve(table.descriptor_create)) },
        parser_create: unsafe { std::mem::transmute(resolve(table.parser_create)) },
        parser_destroy: unsafe { std::mem::transmute(resolve(table.parser_destroy)) },
        parser_parse: unsafe { std::mem::transmute(resolve(table.parser_parse)) },
        profile_data_destroy: unsafe { std::mem::transmute(resolve(table.profile_data_destroy)) },
        get_kicks_num: unsafe { std::mem::transmute(resolve(table.get_kicks_num)) },
        get_counter_num: unsafe { std::mem::transmute(resolve(table.get_counter_num)) },
        get_kick_start: unsafe { std::mem::transmute(resolve(table.get_kick_start)) },
        get_kick_software_id: unsafe { std::mem::transmute(resolve(table.get_kick_software_id)) },
        get_usc_timestamps: unsafe { std::mem::transmute(resolve(table.get_usc_timestamps)) },
        parse_error_string: unsafe { std::mem::transmute(resolve(table.parse_error_string)) },
    };

    Ok(LoadedApi {
        api,
        _keep_alive: handle,
        framework_uuid: uuid,
    })
}

/// Find `GPUToolsReplay`'s loaded base address, ASLR slide, and UUID by
/// walking the dyld image list. Has to be called *after* a successful
/// `dlopen` of the framework.
unsafe fn framework_base_and_uuid() -> std::result::Result<(*const c_void, isize, String), &'static str> {
    use std::ffi::CStr;

    unsafe extern "C" {
        fn _dyld_image_count() -> u32;
        fn _dyld_get_image_name(idx: u32) -> *const c_char;
        fn _dyld_get_image_header(idx: u32) -> *const c_void;
        fn _dyld_get_image_vmaddr_slide(idx: u32) -> isize;
    }

    let count = unsafe { _dyld_image_count() };
    for i in 0..count {
        let name_ptr = unsafe { _dyld_get_image_name(i) };
        if name_ptr.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(name_ptr) }.to_string_lossy();
        if !name.contains("GPUToolsReplay") {
            continue;
        }
        let header = unsafe { _dyld_get_image_header(i) };
        if header.is_null() {
            return Err("null header");
        }
        let slide = unsafe { _dyld_get_image_vmaddr_slide(i) };
        let uuid = unsafe { read_uuid_from_header(header) }.ok_or("no LC_UUID")?;
        return Ok((header, slide, uuid));
    }
    Err("GPUToolsReplay not found in image list")
}

unsafe fn read_uuid_from_header(header: *const c_void) -> Option<String> {
    // Mach-O header layout: magic + cpu + cputype + filetype + ncmds + sizeofcmds + flags + reserved.
    // 64-bit header is 32 bytes; load commands start right after.
    const LC_UUID: u32 = 0x1b;
    let header_bytes = header as *const u8;
    let ncmds = unsafe { *(header_bytes.add(16) as *const u32) };
    let mut cursor = unsafe { header_bytes.add(32) };
    for _ in 0..ncmds {
        let cmd = unsafe { *(cursor as *const u32) };
        let cmdsize = unsafe { *(cursor.add(4) as *const u32) };
        if cmd == LC_UUID {
            let uuid_bytes = unsafe { std::slice::from_raw_parts(cursor.add(8), 16) };
            return Some(format_uuid(uuid_bytes));
        }
        cursor = unsafe { cursor.add(cmdsize as usize) };
    }
    None
}

fn format_uuid(bytes: &[u8]) -> String {
    format!(
        "{:02X}{:02X}{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    )
}

/// A loaded agxps API plus the dlopen handle keeping it mapped.
pub struct LoadedApi {
    pub api: AgxpsApi,
    pub framework_uuid: String,
    _keep_alive: *mut c_void,
}

unsafe impl Send for LoadedApi {}
unsafe impl Sync for LoadedApi {}

impl LoadedApi {
    /// High-level: parse one `Profiling_f_*.raw` for the given GPU and return
    /// per-kick start ticks + software IDs. Caller drops the returned struct
    /// to release everything.
    pub fn parse_profiling(
        &self,
        generation: u32,
        variant: u32,
        rev: u32,
        bytes: &[u8],
    ) -> Result<DecodedProfile> {
        let api = &self.api;

        let gpu = unsafe { (api.gpu_create)(generation, variant, rev, false) };
        if gpu.is_null() {
            return Err(Error::GpuCreate { generation, variant, rev });
        }

        let descriptor = AgxpsApsDescriptor::defaults_for(gpu);
        let parser = unsafe { (api.parser_create)(&descriptor) };
        if parser.is_null() {
            // can't destroy gpu — see leak comment below
            return Err(Error::ParserCreate);
        }

        // The `out` struct's exact size isn't decoded yet; LoadAPSTraceDataAtIndex
        // reserved ~0x300 bytes on its stack. 4 KB is generous and zero-init
        // makes error detection (out[0] != 0) reliable.
        let mut out = vec![0u8; 4096];
        let profile_data = unsafe {
            (api.parser_parse)(
                parser,
                bytes.as_ptr(),
                bytes.len() as c_long,
                APS_PROFILING_TYPE_USC_SAMPLES,
                out.as_mut_ptr() as *mut c_void,
            )
        };
        let err_code = u64::from_le_bytes(out[..8].try_into().unwrap());
        if err_code != 0 {
            let msg = unsafe {
                let s = (api.parse_error_string)(err_code);
                if s.is_null() {
                    "(null)".to_owned()
                } else {
                    CStr::from_ptr(s).to_string_lossy().into_owned()
                }
            };
            return Err(Error::ParserParse {
                code: err_code,
                message: msg,
            });
        }

        let n = unsafe { (api.get_kicks_num)(profile_data) } as usize;
        let mut starts = vec![0u64; n];
        let mut swids = vec![0u64; n];
        if n > 0 {
            let ok1 = unsafe {
                (api.get_kick_start)(profile_data, starts.as_mut_ptr(), 0, n as u64)
            };
            let ok2 = unsafe {
                (api.get_kick_software_id)(profile_data, swids.as_mut_ptr(), 0, n as u64)
            };
            if ok1 == 0 || ok2 == 0 {
                return Err(Error::KickAccess);
            }
        }

        let counter_num = unsafe { (api.get_counter_num)(profile_data) };

        // Intentional leak: `agxps_gpu_destroy` appears to call `delete[]`
        // on a pointer that `agxps_gpu_create` allocated with `new` (single
        // object). Calling it hangs or crashes on real builds. The gpu/
        // parser/profile_data structs are small and the caller decodes one
        // file per process anyway, so we let them ride.
        let _ = (parser, profile_data, gpu);

        Ok(DecodedProfile {
            kick_starts: starts,
            kick_software_ids: swids,
            counter_num,
        })
    }
}

/// Per-kick decoded data from one `Profiling_f_*.raw` stream.
#[derive(Debug, Clone)]
pub struct DecodedProfile {
    /// Kick start times in raw GPU-ticks (units TBD — empirically large u64
    /// values that are *not* nanoseconds-since-boot, likely cycles or some
    /// internal counter).
    pub kick_starts: Vec<u64>,
    /// Per-kick `software_id`. The high 16 bits appear to encode a
    /// pipeline/clique hash that's stable across kicks of the same kernel.
    pub kick_software_ids: Vec<u64>,
    /// Number of counters available on this profile_data.
    pub counter_num: u32,
}

impl DecodedProfile {
    /// Quick summary by software-id high16 (= clique/kernel-group).
    pub fn group_by_clique(&self) -> std::collections::BTreeMap<u16, usize> {
        let mut counts = std::collections::BTreeMap::new();
        for swid in &self.kick_software_ids {
            let prefix = (*swid >> 48) as u16;
            *counts.entry(prefix).or_insert(0usize) += 1;
        }
        counts
    }
}

mod libc {
    use std::ffi::c_char;
    use std::ffi::c_int;
    use std::ffi::c_void;

    pub const RTLD_LAZY: c_int = 1;
    pub const RTLD_LOCAL: c_int = 4;

    unsafe extern "C" {
        pub fn dlopen(path: *const c_char, mode: c_int) -> *mut c_void;
        pub fn dlerror() -> *const c_char;
    }
}
