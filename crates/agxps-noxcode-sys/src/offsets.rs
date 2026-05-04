//! `agxps_*` symbol offsets — relative to GPUToolsReplay's runtime image base
//! (i.e. what `_dyld_get_image_header(idx)` returns), keyed by Mach-O LC_UUID.
//!
//! Computed as `image_base_offset = bracket_addr - framework_text_vmaddr`,
//! where `bracket_addr` is what lldb prints in `GPUToolsReplay[0x...]` and
//! `framework_text_vmaddr` is the binary's preferred __TEXT vmaddr (from
//! `dyld_info -segments`).
//!
//! Equivalently, the offset is consistent across machines and equals
//! `runtime_addr - image_base` regardless of ASLR slide or which dyld_shared
//! cache build assigned the framework.
//!
//! ## Extracting offsets for a new build
//!
//! ```sh
//! lldb /System/Library/CoreServices/MTLReplayer.app/Contents/MacOS/MTLReplayer -b \
//!   -o "image dump sections GPUToolsReplay" \
//!   -o "image lookup --regex --name 'agxps_(aps_|gpu_create$|gpu_destroy$)'"
//! ```
//!
//! From the output, take the framework's __TEXT container start and each
//! function's bracketed address, then `offset = bracket - __TEXT_start`.

#[derive(Clone, Copy)]
pub struct SymbolTable {
    pub gpu_create: usize,
    pub gpu_destroy: usize,
    pub descriptor_create: usize,
    pub parser_create: usize,
    pub parser_destroy: usize,
    pub parser_parse: usize,
    pub profile_data_destroy: usize,
    pub get_kicks_num: usize,
    pub get_counter_num: usize,
    pub get_kick_start: usize,
    pub get_kick_software_id: usize,
    pub get_usc_timestamps: usize,
    pub parse_error_string: usize,
}

/// Offsets within `GPUToolsReplay` from macOS 26.4. Verified against UUID
/// `B1DEE264-D3AF-38F9-BC6E-821AFAE2DB30` end-to-end on M4 Pro perfdata.
const TABLE_B1DEE264: SymbolTable = SymbolTable {
    gpu_create: 0x22fac,
    gpu_destroy: 0x2308c,
    descriptor_create: 0x4e6dc,
    parser_create: 0x4e764,
    parser_destroy: 0x4e87c,
    parser_parse: 0x4e8a0,
    profile_data_destroy: 0x4e8cc,
    get_kicks_num: 0x4eab4,
    get_counter_num: 0x4eac8,
    get_kick_start: 0x4ea04,
    get_kick_software_id: 0x4ea5c,
    get_usc_timestamps: 0x4e984,
    parse_error_string: 0x4eb10,
};

const TABLE: &[(&str, SymbolTable)] = &[("B1DEE264-D3AF-38F9-BC6E-821AFAE2DB30", TABLE_B1DEE264)];

pub fn lookup(uuid: &str) -> Option<SymbolTable> {
    TABLE
        .iter()
        .find_map(|(u, t)| (u.eq_ignore_ascii_case(uuid)).then_some(*t))
}

pub fn supported_uuids() -> Vec<String> {
    TABLE.iter().map(|(u, _)| u.to_string()).collect()
}
