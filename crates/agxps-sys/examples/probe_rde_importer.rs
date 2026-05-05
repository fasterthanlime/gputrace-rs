//! Probe Xcode's private `XRGPUATRCImporter` RDE entry points.
//!
//! Usage:
//! ```
//! cargo run -p agxps-sys --example probe_rde_importer -- /path/to/Counters_f_0.raw
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::fs;
use std::mem;
use std::path::PathBuf;
use std::process;

const GT_SHADER_PROFILER_FRAMEWORK: &str = "/Applications/Xcode.app/Contents/PlugIns/GPUDebugger.ideplugin/Contents/Frameworks/GTShaderProfiler.framework/Versions/A/GTShaderProfiler";
const RTLD_NOW: c_int = 0x2;
const RTLD_GLOBAL: c_int = 0x8;

const DEFAULT_COUNTER_COUNTS: &[u32] = &[36, 31, 21, 15, 13, 10, 9, 7, 5, 1];

type Id = *mut c_void;
type Class = *mut c_void;
type Sel = *mut c_void;

type Result<T> = std::result::Result<T, String>;

unsafe extern "C" {
    fn dlopen(path: *const c_char, mode: c_int) -> *mut c_void;
    fn dlerror() -> *const c_char;
    fn objc_lookUpClass(name: *const c_char) -> Class;
    fn sel_registerName(name: *const c_char) -> Sel;
    fn objc_msgSend();
}

#[link(name = "Foundation", kind = "framework")]
unsafe extern "C" {}

#[link(name = "objc")]
unsafe extern "C" {}

fn main() {
    let args = Args::parse();
    let bytes = fs::read(&args.path).unwrap_or_else(|error| {
        eprintln!("read {}: {error}", args.path.display());
        process::exit(1);
    });

    println!("file: {}", args.path.display());
    println!("bytes: {}", bytes.len());
    print_byte_overview(&bytes);
    print_candidate_record_chains(&bytes, &args.counter_counts, args.min_records);

    match unsafe { run_objc_probe(&bytes, &args) } {
        Ok(()) => {}
        Err(error) => {
            eprintln!("probe failed: {error}");
            process::exit(1);
        }
    }
}

#[derive(Debug)]
struct Args {
    path: PathBuf,
    counter_counts: Vec<u32>,
    min_records: usize,
    max_slices: usize,
    full_direct: bool,
}

impl Args {
    fn parse() -> Self {
        let mut path = None;
        let mut counter_counts = Vec::new();
        let mut min_records = 8usize;
        let mut max_slices = 16usize;
        let mut full_direct = false;
        let mut iter = env::args().skip(1);
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--counter-count" => {
                    let value = iter.next().unwrap_or_else(|| usage());
                    counter_counts.push(value.parse().unwrap_or_else(|_| usage()));
                }
                "--min-records" => {
                    let value = iter.next().unwrap_or_else(|| usage());
                    min_records = value.parse().unwrap_or_else(|_| usage());
                }
                "--max-slices" => {
                    let value = iter.next().unwrap_or_else(|| usage());
                    max_slices = value.parse().unwrap_or_else(|_| usage());
                }
                "--full-direct" => {
                    full_direct = true;
                }
                "-h" | "--help" => usage(),
                _ if arg.starts_with('-') => usage(),
                _ => {
                    if path.replace(PathBuf::from(arg)).is_some() {
                        usage();
                    }
                }
            }
        }
        let path = path.unwrap_or_else(|| usage());
        if counter_counts.is_empty() {
            counter_counts.extend_from_slice(DEFAULT_COUNTER_COUNTS);
        }
        Self {
            path,
            counter_counts,
            min_records,
            max_slices,
            full_direct,
        }
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: probe_rde_importer <raw-file> [--counter-count N ...] [--min-records N] [--max-slices N] [--full-direct]"
    );
    process::exit(2);
}

fn print_byte_overview(bytes: &[u8]) {
    let mut page_words = BTreeMap::<u32, usize>::new();
    for page in bytes.chunks(4096) {
        if page.len() >= 4 {
            let word = u32::from_le_bytes(page[0..4].try_into().unwrap());
            *page_words.entry(word).or_default() += 1;
        }
    }
    let mut page_words = page_words.into_iter().collect::<Vec<_>>();
    page_words.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    println!("4k pages: {}", bytes.len().div_ceil(4096));
    println!("top page-start u32 values:");
    for (word, count) in page_words.into_iter().take(12) {
        println!("  0x{word:08x}: {count}");
    }

    for needle in [
        b"AGXPCTR2".as_slice(),
        b"CTRSAMPL".as_slice(),
        b"GPRWCNTR".as_slice(),
    ] {
        let count = count_subslice(bytes, needle);
        println!("{} occurrences: {count}", String::from_utf8_lossy(needle));
    }
}

fn count_subslice(haystack: &[u8], needle: &[u8]) -> usize {
    if needle.is_empty() || haystack.len() < needle.len() {
        return 0;
    }
    haystack
        .windows(needle.len())
        .filter(|window| *window == needle)
        .count()
}

fn print_candidate_record_chains(bytes: &[u8], counter_counts: &[u32], min_records: usize) {
    let candidates = find_candidate_record_chains(bytes, counter_counts, min_records, 16);
    println!(
        "direct RDE-record-shaped chains (static scan): {}",
        candidates.len()
    );
    for candidate in candidates.iter().take(16) {
        println!(
            "  off={} len={} records={} counter_count={} types={:?}",
            candidate.offset,
            candidate.byte_len,
            candidate.records,
            candidate.counter_count,
            candidate.types
        );
    }
}

#[derive(Debug, Clone)]
struct RecordChainCandidate {
    offset: usize,
    byte_len: usize,
    records: usize,
    counter_count: u32,
    types: BTreeSet<u8>,
}

fn find_candidate_record_chains(
    bytes: &[u8],
    counter_counts: &[u32],
    min_records: usize,
    limit: usize,
) -> Vec<RecordChainCandidate> {
    let mut candidates = Vec::new();
    if bytes.len() < 0x18 {
        return candidates;
    }
    for offset in 0..=(bytes.len() - 0x18) {
        for counter_count in counter_counts {
            if let Some(candidate) = record_chain_at(bytes, offset, *counter_count)
                && candidate.records >= min_records
            {
                candidates.push(candidate);
            }
        }
    }
    candidates.sort_by(|left, right| {
        right
            .records
            .cmp(&left.records)
            .then_with(|| right.byte_len.cmp(&left.byte_len))
            .then_with(|| left.offset.cmp(&right.offset))
            .then_with(|| left.counter_count.cmp(&right.counter_count))
    });
    candidates.dedup_by_key(|candidate| {
        (
            candidate.offset,
            candidate.byte_len,
            candidate.counter_count,
        )
    });
    candidates.truncate(limit);
    candidates
}

fn record_chain_at(
    bytes: &[u8],
    offset: usize,
    counter_count: u32,
) -> Option<RecordChainCandidate> {
    let mut pos = offset;
    let mut records = 0usize;
    let mut types = BTreeSet::new();
    let expected_payload = counter_count as usize * 8;
    loop {
        if pos + 0x18 > bytes.len() {
            break;
        }
        let record_type = bytes[pos + 0x15];
        let record_len =
            u16::from_le_bytes(bytes[pos + 0x16..pos + 0x18].try_into().unwrap()) as usize;
        if record_len < 0x18 || pos + record_len > bytes.len() {
            break;
        }
        let payload_len = record_len - 0x18;
        let valid = if record_type <= 6 {
            payload_len == expected_payload
        } else if record_type == 7 {
            payload_len > 0 && payload_len % 0x20 == 0
        } else {
            false
        };
        if !valid {
            break;
        }
        records += 1;
        types.insert(record_type);
        pos += record_len;
    }
    (records > 0).then_some(RecordChainCandidate {
        offset,
        byte_len: pos - offset,
        records,
        counter_count,
        types,
    })
}

unsafe fn run_objc_probe(bytes: &[u8], args: &Args) -> Result<()> {
    unsafe {
        load_framework()?;
        let pool_class = lookup_class("NSAutoreleasePool")?;
        let pool = send_id(send_id(pool_class, "alloc")?, "init")?;
        let importer_class = lookup_class("XRGPUATRCImporter")?;
        let importer = send_id(send_id(importer_class, "alloc")?, "init")?;
        let container = new_container()?;

        println!("\nObjective-C probe:");
        println!("  importer: 0x{:x}", importer as usize);
        println!("  container: 0x{:x}", container as usize);

        let agx = send_i8_ptr_u64_id(
            importer,
            "_parseAGXBlock:length:container:",
            bytes.as_ptr(),
            bytes.len() as u64,
            container,
        )?;
        println!("  _parseAGXBlock(full file): {agx}");
        print_container_summary(container)?;

        if args.full_direct {
            let full_direct = try_parse_rde_buffer(
                importer,
                container,
                bytes,
                0,
                bytes.len(),
                &args.counter_counts,
            )?;
            println!(
                "  parseRDEBuffer(full file): {} successful guesses",
                full_direct
            );
            print_container_summary(container)?;
        } else {
            println!("  parseRDEBuffer(full file): skipped (use --full-direct to risk it)");
        }

        let candidates = find_candidate_record_chains(
            bytes,
            &args.counter_counts,
            args.min_records,
            args.max_slices,
        );
        let mut successes = 0usize;
        for candidate in &candidates {
            let slice = &bytes[candidate.offset..candidate.offset + candidate.byte_len];
            let ok = send_i8_ptr_u32_u32_u32_u32_u32_id(
                importer,
                "parseRDEBuffer:size:sampleCount:counterCount:rdeSourceIndex:rdeBufferIndex:container:",
                slice.as_ptr(),
                slice.len() as u32,
                candidate.records as u32,
                candidate.counter_count,
                0,
                successes as u32,
                container,
            )?;
            if ok != 0 {
                println!(
                    "  parseRDEBuffer(candidate off={} len={} records={} counter_count={}): {ok}",
                    candidate.offset,
                    candidate.byte_len,
                    candidate.records,
                    candidate.counter_count
                );
                successes += 1;
            }
        }
        println!("  parseRDEBuffer(candidate slices): {successes} successful guesses");
        print_container_summary(container)?;

        let _ = send_void(pool, "drain");
    }
    Ok(())
}

unsafe fn new_container() -> Result<Id> {
    unsafe {
        let dict_class = lookup_class("NSDictionary")?;
        let config = send_id(dict_class, "dictionary")?;
        let base_folder = nsstring("/tmp")?;
        let container_class = lookup_class("XRGPUAPSDataContainer")?;
        let container = send_id(container_class, "alloc")?;
        let initialized = send_id_id_id_u64_allow_nil(
            container,
            "initWithConfig:baseFolder:variant:",
            config,
            base_folder,
            3,
        )?;
        if initialized.is_null() {
            new_container_with_manual_ivars(base_folder)
        } else {
            Ok(initialized)
        }
    }
}

unsafe fn new_container_with_manual_ivars(base_folder: Id) -> Result<Id> {
    unsafe {
        let container_class = lookup_class("XRGPUAPSDataContainer")?;
        let container = send_id(send_id(container_class, "alloc")?, "init")?;
        let mutable_dict_class = lookup_class("NSMutableDictionary")?;
        let mutable_array_class = lookup_class("NSMutableArray")?;

        write_ivar_id(container, 8, send_id(mutable_dict_class, "dictionary")?);
        write_ivar_id(container, 16, send_id(mutable_dict_class, "dictionary")?);
        write_ivar_id(container, 24, send_id(mutable_array_class, "array")?);
        write_ivar_id(container, 32, send_id(mutable_array_class, "array")?);
        write_ivar_id(container, 40, send_id(mutable_array_class, "array")?);
        write_ivar_id(container, 48, base_folder);
        write_ivar_u64(container, 72, 3);
        println!(
            "  note: manually initialized XRGPUAPSDataContainer ivars after ATRC config load failed"
        );
        Ok(container)
    }
}

unsafe fn write_ivar_id(object: Id, offset: usize, value: Id) {
    unsafe {
        let slot = (object as *mut u8).add(offset).cast::<Id>();
        *slot = value;
    }
}

unsafe fn write_ivar_u64(object: Id, offset: usize, value: u64) {
    unsafe {
        let slot = (object as *mut u8).add(offset).cast::<u64>();
        *slot = value;
    }
}

unsafe fn try_parse_rde_buffer(
    importer: Id,
    container: Id,
    bytes: &[u8],
    offset: usize,
    len: usize,
    counter_counts: &[u32],
) -> Result<usize> {
    let mut successes = 0usize;
    for counter_count in counter_counts {
        let record_size = 0x18 + (*counter_count as usize * 8);
        if len < record_size || !len.is_multiple_of(record_size) {
            continue;
        }
        let sample_count = len / record_size;
        let ok = unsafe {
            send_i8_ptr_u32_u32_u32_u32_u32_id(
                importer,
                "parseRDEBuffer:size:sampleCount:counterCount:rdeSourceIndex:rdeBufferIndex:container:",
                bytes[offset..offset + len].as_ptr(),
                len as u32,
                sample_count as u32,
                *counter_count,
                0,
                successes as u32,
                container,
            )?
        };
        if ok != 0 {
            println!(
                "    full-file direct RDE guess ok: counter_count={counter_count} sample_count={sample_count}"
            );
            successes += 1;
        }
    }
    Ok(successes)
}

unsafe fn print_container_summary(container: Id) -> Result<()> {
    let sources = unsafe { send_u64(container, "numRDEs")? };
    println!("  container RDE sources: {sources}");
    for source in 0..sources.min(16) {
        let buffers = unsafe { send_u64_u64(container, "numBuffersAtRDEIndex:", source)? };
        println!("    source {source}: buffers={buffers}");
    }
    Ok(())
}

unsafe fn load_framework() -> Result<()> {
    let path = env::var("AGXPS_FRAMEWORK_PATH")
        .unwrap_or_else(|_| GT_SHADER_PROFILER_FRAMEWORK.to_owned());
    let cpath = CString::new(path.clone()).map_err(|_| "framework path contains NUL".to_owned())?;
    let handle = unsafe { dlopen(cpath.as_ptr(), RTLD_NOW | RTLD_GLOBAL) };
    if handle.is_null() {
        let error = unsafe {
            let ptr = dlerror();
            if ptr.is_null() {
                "unknown dlopen error".to_owned()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };
        Err(format!("dlopen {path}: {error}"))
    } else {
        println!("framework: {path}");
        Ok(())
    }
}

unsafe fn lookup_class(name: &str) -> Result<Class> {
    let cname = CString::new(name).unwrap();
    let class = unsafe { objc_lookUpClass(cname.as_ptr()) };
    if class.is_null() {
        Err(format!("Objective-C class {name} is not available"))
    } else {
        Ok(class)
    }
}

unsafe fn selector(name: &str) -> Result<Sel> {
    let cname = CString::new(name).map_err(|_| "selector contains NUL".to_owned())?;
    let sel = unsafe { sel_registerName(cname.as_ptr()) };
    if sel.is_null() {
        Err(format!("Objective-C selector {name} is not available"))
    } else {
        Ok(sel)
    }
}

unsafe fn send_id(receiver: Id, sel: &str) -> Result<Id> {
    if receiver.is_null() {
        return Err(format!("nil Objective-C receiver for {sel}"));
    }
    let sel_ptr = unsafe { selector(sel)? };
    let f: extern "C" fn(Id, Sel) -> Id = unsafe { mem::transmute(objc_msgSend as *const ()) };
    let value = f(receiver, sel_ptr);
    if value.is_null() {
        Err(format!("Objective-C message {sel} returned nil"))
    } else {
        Ok(value)
    }
}

unsafe fn send_id_id_id_u64_allow_nil(
    receiver: Id,
    sel: &str,
    first: Id,
    second: Id,
    third: u64,
) -> Result<Id> {
    if receiver.is_null() {
        return Err(format!("nil Objective-C receiver for {sel}"));
    }
    let sel_ptr = unsafe { selector(sel)? };
    let f: extern "C" fn(Id, Sel, Id, Id, u64) -> Id =
        unsafe { mem::transmute(objc_msgSend as *const ()) };
    Ok(f(receiver, sel_ptr, first, second, third))
}

unsafe fn send_void(receiver: Id, sel: &str) -> Result<()> {
    if receiver.is_null() {
        return Err(format!("nil Objective-C receiver for {sel}"));
    }
    let sel_ptr = unsafe { selector(sel)? };
    let f: extern "C" fn(Id, Sel) = unsafe { mem::transmute(objc_msgSend as *const ()) };
    f(receiver, sel_ptr);
    Ok(())
}

unsafe fn send_u64(receiver: Id, sel: &str) -> Result<u64> {
    if receiver.is_null() {
        return Err(format!("nil Objective-C receiver for {sel}"));
    }
    let sel_ptr = unsafe { selector(sel)? };
    let f: extern "C" fn(Id, Sel) -> u64 = unsafe { mem::transmute(objc_msgSend as *const ()) };
    Ok(f(receiver, sel_ptr))
}

unsafe fn send_u64_u64(receiver: Id, sel: &str, arg: u64) -> Result<u64> {
    if receiver.is_null() {
        return Err(format!("nil Objective-C receiver for {sel}"));
    }
    let sel_ptr = unsafe { selector(sel)? };
    let f: extern "C" fn(Id, Sel, u64) -> u64 =
        unsafe { mem::transmute(objc_msgSend as *const ()) };
    Ok(f(receiver, sel_ptr, arg))
}

unsafe fn send_i8_ptr_u64_id(
    receiver: Id,
    sel: &str,
    data: *const u8,
    len: u64,
    container: Id,
) -> Result<i8> {
    if receiver.is_null() {
        return Err(format!("nil Objective-C receiver for {sel}"));
    }
    let sel_ptr = unsafe { selector(sel)? };
    let f: extern "C" fn(Id, Sel, *const u8, u64, Id) -> i8 =
        unsafe { mem::transmute(objc_msgSend as *const ()) };
    Ok(f(receiver, sel_ptr, data, len, container))
}

unsafe fn send_i8_ptr_u32_u32_u32_u32_u32_id(
    receiver: Id,
    sel: &str,
    data: *const u8,
    size: u32,
    sample_count: u32,
    counter_count: u32,
    source_index: u32,
    buffer_index: u32,
    container: Id,
) -> Result<i8> {
    if receiver.is_null() {
        return Err(format!("nil Objective-C receiver for {sel}"));
    }
    let sel_ptr = unsafe { selector(sel)? };
    let f: extern "C" fn(Id, Sel, *const u8, u32, u32, u32, u32, u32, Id) -> i8 =
        unsafe { mem::transmute(objc_msgSend as *const ()) };
    Ok(f(
        receiver,
        sel_ptr,
        data,
        size,
        sample_count,
        counter_count,
        source_index,
        buffer_index,
        container,
    ))
}

unsafe fn nsstring(value: &str) -> Result<Id> {
    let cvalue = CString::new(value).map_err(|_| "NSString value contains NUL".to_owned())?;
    let class = unsafe { lookup_class("NSString")? };
    let sel = unsafe { selector("stringWithUTF8String:")? };
    let f: extern "C" fn(Id, Sel, *const c_char) -> Id =
        unsafe { mem::transmute(objc_msgSend as *const ()) };
    let value = f(class, sel, cvalue.as_ptr());
    if value.is_null() {
        Err("NSString stringWithUTF8String returned nil".to_owned())
    } else {
        Ok(value)
    }
}
