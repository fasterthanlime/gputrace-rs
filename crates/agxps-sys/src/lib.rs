//! FFI bindings to Apple's `agxps_aps_*` sample-decoder API as bundled
//! inside Xcode's `GPUDebugger.ideplugin` (specifically
//! `GTShaderProfiler.framework`).
//!
//! The full agxps surface — ~100 functions including `kick_end`,
//! `kick_id`, `kick_time_stats_create_sampled`, synchronized/operating
//! timestamps, work_clique/hw_clique/esl_clique queries, instruction
//! traces — is exported from `GTShaderProfiler.framework` as ordinary
//! external symbols, so `dlsym` resolves them directly. No offset
//! tables, no UUID-keyed compatibility lookup.
//!
//! Compare with `agxps-noxcode-sys`, which targets the much smaller
//! 15-function subset shipped in macOS's `GPUToolsReplay.framework`
//! and resolves them by hardcoded text-section offsets per build UUID.
//! Use this crate when Xcode is installed; fall back to
//! `agxps-noxcode-sys` only when it isn't.

#![cfg(target_os = "macos")]

use std::ffi::{CStr, c_char, c_int, c_long, c_uint, c_void};
use std::os::raw::c_uchar;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("framework not loadable: {0}")]
    Dlopen(String),
    #[error("agxps_gpu_create({generation}, {variant}, {rev}) failed")]
    GpuCreate { generation: u32, variant: u32, rev: u32 },
    #[error("agxps_aps_parser_create returned NULL — descriptor invalid for this GPU")]
    ParserCreate,
    #[error("agxps_aps_parser_parse error code {code}: {message}")]
    ParserParse { code: u64, message: String },
    #[error("agxps_aps_profile_data_get_kick_* range fetch failed")]
    KickAccess,
    #[error("missing symbol in framework: {0}")]
    MissingSymbol(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;

// -------- Raw C ABI types --------

pub type AgxpsGpu = *mut c_void;
pub type AgxpsApsParser = *mut c_void;
pub type AgxpsApsProfileData = *mut c_void;

/// 0x68-byte descriptor passed to `agxps_aps_parser_create`. The
/// non-zero fields are values empirically known to satisfy the per-GPU
/// validators; see `docs/AGXPS_API.md` for the rules each field has to
/// obey.
#[repr(C)]
pub struct AgxpsApsDescriptor {
    pub gpu: *mut c_void,        // +0x00
    pub field_0x08: u32,         // +0x08 — power of 2 in [0x10, 0x800]
    pub field_0x0c: u32,         // +0x0c — power of 2 in [0x40, 0x2000]
    pub field_0x10: u32,         // +0x10 — 0 or power of 2 in [0x80, 0x8000]
    pub field_0x14: u32,
    pub field_0x18: u64,         // +0x18 — must be 0x400, 0x1000, or 0x40000
    pub _pad_0x20: [u8; 0x10],
    pub field_0x30: u64,         // +0x30 — set to -1 by default
    pub _pad_0x38: [u8; 0x20],
    pub field_0x58: u64,         // +0x58 — set to 0x32 by default
    pub _pad_0x60: [u8; 0x08],
}

const _: () = assert!(std::mem::size_of::<AgxpsApsDescriptor>() == 0x68);

impl AgxpsApsDescriptor {
    pub fn defaults_for(gpu: AgxpsGpu) -> Self {
        Self {
            gpu,
            field_0x08: 0x100,
            field_0x0c: 0x400,
            field_0x10: 0,
            field_0x14: 0,
            field_0x18: 0x1000,
            _pad_0x20: [0; 0x10],
            field_0x30: u64::MAX,
            _pad_0x38: [0; 0x20],
            field_0x58: 0x32,
            _pad_0x60: [0; 0x08],
        }
    }
}

/// Profile-type tag for USC sample streams (`Profiling_f_*.raw`).
pub const APS_PROFILING_TYPE_USC_SAMPLES: u32 = 0x21;

// Function-pointer types matching the agxps C ABI.
pub type FnGpuCreate =
    unsafe extern "C" fn(generation: u32, variant: u32, rev: u32, flag: bool) -> AgxpsGpu;
pub type FnGpuDestroy = unsafe extern "C" fn(AgxpsGpu);
pub type FnDescriptorCreate = unsafe extern "C" fn() -> AgxpsApsDescriptor;
pub type FnParserCreate = unsafe extern "C" fn(*const AgxpsApsDescriptor) -> AgxpsApsParser;
pub type FnParserDestroy = unsafe extern "C" fn(AgxpsApsParser);
pub type FnParserParse = unsafe extern "C" fn(
    parser: AgxpsApsParser,
    bytes: *const c_uchar,
    size: c_long,
    profile_type: u32,
    out: *mut c_void,
) -> AgxpsApsProfileData;
pub type FnProfileDataDestroy = unsafe extern "C" fn(AgxpsApsProfileData);
pub type FnGetCount = unsafe extern "C" fn(AgxpsApsProfileData) -> c_uint;
pub type FnGetU64Range = unsafe extern "C" fn(
    pd: AgxpsApsProfileData,
    out: *mut u64,
    start_idx: u64,
    count: u64,
) -> c_int;
pub type FnGetU8Range = unsafe extern "C" fn(
    pd: AgxpsApsProfileData,
    out: *mut u8,
    start_idx: u64,
    count: u64,
) -> c_int;
pub type FnParseErrorString = unsafe extern "C" fn(code: u64) -> *const c_char;

/// Resolved function-pointer table. All fields are non-null on success
/// (we treat any missing symbol as a hard load error).
pub struct AgxpsApi {
    pub gpu_create: FnGpuCreate,
    pub gpu_destroy: FnGpuDestroy,
    pub descriptor_create: FnDescriptorCreate,
    pub parser_create: FnParserCreate,
    pub parser_destroy: FnParserDestroy,
    pub parser_parse: FnParserParse,
    pub profile_data_destroy: FnProfileDataDestroy,
    pub get_kicks_num: FnGetCount,
    pub get_counter_num: FnGetCount,
    pub get_kick_start: FnGetU64Range,
    pub get_kick_end: FnGetU64Range,
    pub get_kick_software_id: FnGetU64Range,
    pub get_kick_id: FnGetU64Range,
    pub get_kick_kick_slot: FnGetU64Range,
    pub get_kick_missing_end: FnGetU8Range,
    pub get_usc_timestamps: FnGetU64Range,
    pub get_usc_timestamps_num: FnGetCount,
    pub get_synchronized_timestamps: FnGetU64Range,
    pub get_synchronized_timestamps_num: FnGetCount,
    pub get_operating_frequencies: FnGetU64Range,
    pub parse_error_string: FnParseErrorString,
}

unsafe impl Send for AgxpsApi {}
unsafe impl Sync for AgxpsApi {}

const DEFAULT_FRAMEWORK_PATH: &str =
    "/Applications/Xcode.app/Contents/PlugIns/GPUDebugger.ideplugin/Contents/Frameworks/GTShaderProfiler.framework/GTShaderProfiler";

/// dlopen `GTShaderProfiler` and dlsym every needed symbol. Returns
/// [`Error::MissingSymbol`] if any required entry-point isn't exported
/// (which would mean we're looking at an Xcode version with a renamed
/// or missing function — worth flagging early).
///
/// Set `AGXPS_FRAMEWORK_PATH` env var to override the default Xcode
/// location (e.g. for Xcode-beta.app or a custom toolchain).
pub fn load() -> Result<LoadedApi> {
    use std::ffi::CString;

    let path = std::env::var("AGXPS_FRAMEWORK_PATH")
        .unwrap_or_else(|_| DEFAULT_FRAMEWORK_PATH.to_string());
    let cpath = CString::new(path.clone()).unwrap();
    let handle = unsafe { libc::dlopen(cpath.as_ptr(), libc::RTLD_LAZY | libc::RTLD_LOCAL) };
    if handle.is_null() {
        let err = unsafe { CStr::from_ptr(libc::dlerror()) }
            .to_string_lossy()
            .into_owned();
        return Err(Error::Dlopen(format!("{path}: {err}")));
    }

    let api = AgxpsApi {
        gpu_create: load_sym(handle, "agxps_gpu_create")?,
        gpu_destroy: load_sym(handle, "agxps_gpu_destroy")?,
        descriptor_create: load_sym(handle, "agxps_aps_descriptor_create")?,
        parser_create: load_sym(handle, "agxps_aps_parser_create")?,
        parser_destroy: load_sym(handle, "agxps_aps_parser_destroy")?,
        parser_parse: load_sym(handle, "agxps_aps_parser_parse")?,
        profile_data_destroy: load_sym(handle, "agxps_aps_profile_data_destroy")?,
        get_kicks_num: load_sym(handle, "agxps_aps_profile_data_get_kicks_num")?,
        get_counter_num: load_sym(handle, "agxps_aps_profile_data_get_counter_num")?,
        get_kick_start: load_sym(handle, "agxps_aps_profile_data_get_kick_start")?,
        get_kick_end: load_sym(handle, "agxps_aps_profile_data_get_kick_end")?,
        get_kick_software_id: load_sym(handle, "agxps_aps_profile_data_get_kick_software_id")?,
        get_kick_id: load_sym(handle, "agxps_aps_profile_data_get_kick_id")?,
        get_kick_kick_slot: load_sym(handle, "agxps_aps_profile_data_get_kick_kick_slot")?,
        get_kick_missing_end: load_sym(handle, "agxps_aps_profile_data_get_kick_missing_end")?,
        get_usc_timestamps: load_sym(handle, "agxps_aps_profile_data_get_usc_timestamps")?,
        get_usc_timestamps_num: load_sym(handle, "agxps_aps_profile_data_get_usc_timestamps_num")?,
        get_synchronized_timestamps: load_sym(
            handle,
            "agxps_aps_profile_data_get_synchronized_timestamps",
        )?,
        get_synchronized_timestamps_num: load_sym(
            handle,
            "agxps_aps_profile_data_get_synchronized_timestamps_num",
        )?,
        get_operating_frequencies: load_sym(
            handle,
            "agxps_aps_profile_data_get_operating_frequencies",
        )?,
        parse_error_string: load_sym(handle, "agxps_aps_parse_error_type_to_string")?,
    };

    Ok(LoadedApi {
        api,
        framework_path: path,
        _keep_alive: handle,
    })
}

unsafe fn load_sym_raw(handle: *mut c_void, name: &str) -> Option<*mut c_void> {
    use std::ffi::CString;
    let cname = CString::new(name).unwrap();
    let p = unsafe { libc::dlsym(handle, cname.as_ptr()) };
    if p.is_null() { None } else { Some(p) }
}

fn load_sym<T>(handle: *mut c_void, name: &'static str) -> Result<T> {
    let p = unsafe { load_sym_raw(handle, name) }.ok_or(Error::MissingSymbol(name))?;
    Ok(unsafe { std::mem::transmute_copy(&p) })
}

pub struct LoadedApi {
    pub api: AgxpsApi,
    pub framework_path: String,
    _keep_alive: *mut c_void,
}

unsafe impl Send for LoadedApi {}
unsafe impl Sync for LoadedApi {}

impl LoadedApi {
    /// Decode a single `Profiling_f_*.raw` and return per-kick data
    /// (start, end, software_id, kick_id, kick_slot, missing_end) plus
    /// the raw `usc_timestamps` and `synchronized_timestamps` vectors.
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
            return Err(Error::ParserCreate);
        }

        let mut out = vec![0u8; 4096];
        let pd = unsafe {
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
            return Err(Error::ParserParse { code: err_code, message: msg });
        }

        let n = unsafe { (api.get_kicks_num)(pd) } as usize;
        let mut starts = vec![0u64; n];
        let mut ends = vec![0u64; n];
        let mut swids = vec![0u64; n];
        let mut kids = vec![0u64; n];
        let mut slots = vec![0u64; n];
        let mut missing = vec![0u8; n];
        if n > 0 {
            let nu = n as u64;
            let ok = unsafe {
                (api.get_kick_start)(pd, starts.as_mut_ptr(), 0, nu) != 0
                    && (api.get_kick_end)(pd, ends.as_mut_ptr(), 0, nu) != 0
                    && (api.get_kick_software_id)(pd, swids.as_mut_ptr(), 0, nu) != 0
                    && (api.get_kick_id)(pd, kids.as_mut_ptr(), 0, nu) != 0
                    && (api.get_kick_kick_slot)(pd, slots.as_mut_ptr(), 0, nu) != 0
                    && (api.get_kick_missing_end)(pd, missing.as_mut_ptr(), 0, nu) != 0
            };
            if !ok {
                return Err(Error::KickAccess);
            }
        }

        let usc_n = unsafe { (api.get_usc_timestamps_num)(pd) } as usize;
        let mut usc_timestamps = vec![0u64; usc_n];
        if usc_n > 0 {
            let _ = unsafe {
                (api.get_usc_timestamps)(pd, usc_timestamps.as_mut_ptr(), 0, usc_n as u64)
            };
        }

        let sync_n = unsafe { (api.get_synchronized_timestamps_num)(pd) } as usize;
        let mut sync_timestamps = vec![0u64; sync_n];
        if sync_n > 0 {
            let _ = unsafe {
                (api.get_synchronized_timestamps)(
                    pd,
                    sync_timestamps.as_mut_ptr(),
                    0,
                    sync_n as u64,
                )
            };
        }

        let counter_num = unsafe { (api.get_counter_num)(pd) };

        // See the noxcode crate for why we leak instead of calling destroy.
        let _ = (parser, pd, gpu);

        Ok(DecodedProfile {
            kick_starts: starts,
            kick_ends: ends,
            kick_software_ids: swids,
            kick_ids: kids,
            kick_kick_slots: slots,
            kick_missing_ends: missing.into_iter().map(|b| b != 0).collect(),
            usc_timestamps,
            synchronized_timestamps: sync_timestamps,
            counter_num,
        })
    }
}

/// Per-kick decoded data from one `Profiling_f_*.raw` stream.
#[derive(Debug, Clone)]
pub struct DecodedProfile {
    pub kick_starts: Vec<u64>,
    pub kick_ends: Vec<u64>,
    pub kick_software_ids: Vec<u64>,
    pub kick_ids: Vec<u64>,
    pub kick_kick_slots: Vec<u64>,
    pub kick_missing_ends: Vec<bool>,
    pub usc_timestamps: Vec<u64>,
    pub synchronized_timestamps: Vec<u64>,
    pub counter_num: u32,
}

impl DecodedProfile {
    /// Kick count grouped by `software_id` high 16 bits (= kernel/clique).
    pub fn group_by_clique(&self) -> std::collections::BTreeMap<u16, usize> {
        let mut counts = std::collections::BTreeMap::new();
        for swid in &self.kick_software_ids {
            *counts.entry((*swid >> 48) as u16).or_insert(0usize) += 1;
        }
        counts
    }

    /// Sum of `kick_end - kick_start` (in raw GPU ticks) grouped by
    /// `software_id` high 16 bits. Kicks with `missing_end=true` are
    /// skipped.
    pub fn duration_by_clique(&self) -> std::collections::BTreeMap<u16, u64> {
        let mut sums = std::collections::BTreeMap::new();
        for i in 0..self.kick_starts.len() {
            if self.kick_missing_ends[i] {
                continue;
            }
            let dur = self.kick_ends[i].saturating_sub(self.kick_starts[i]);
            let prefix = (self.kick_software_ids[i] >> 48) as u16;
            *sums.entry(prefix).or_insert(0u64) += dur;
        }
        sums
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
        pub fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
        pub fn dlerror() -> *const c_char;
    }
}
