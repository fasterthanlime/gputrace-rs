//! Probe `agxps_aps_timing_analyzer_*` command records.
//!
//! Usage:
//! ```
//! cargo run -p agxps-sys --example probe_timing_analyzer -- /path/to/Profiling_f_*.raw
//! ```

use std::collections::BTreeMap;
use std::env;
use std::ffi::{CStr, c_long};
use std::fs;
use std::process;

const TIMING_ANALYZER_KIND: u32 = 1;

fn main() {
    let paths = env::args().skip(1).collect::<Vec<_>>();
    if paths.is_empty() {
        eprintln!("usage: probe_timing_analyzer <Profiling_f_N.raw> [Profiling_f_N.raw...]");
        process::exit(2);
    }

    let generation: u32 = env::var("AGXPS_GEN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    let variant: u32 = env::var("AGXPS_VARIANT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let rev: u32 = env::var("AGXPS_REV")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    let loaded = agxps_sys::load().unwrap_or_else(|error| {
        eprintln!("load: {error}");
        process::exit(1);
    });
    println!("framework: {}", loaded.framework_path);

    let mut all_records = Vec::new();
    let mut total_discarded = 0u64;
    for path in &paths {
        let bytes = fs::read(path).unwrap_or_else(|error| {
            eprintln!("read {path}: {error}");
            process::exit(1);
        });

        let raw = unsafe { parse_raw_profile(&loaded, generation, variant, rev, &bytes) }
            .unwrap_or_else(|error| {
                eprintln!("parse {path}: {error}");
                process::exit(1);
            });

        let analyzer = unsafe { (loaded.api.timing_analyzer_create)(TIMING_ANALYZER_KIND) };
        if analyzer.is_null() {
            eprintln!("timing_analyzer_create({TIMING_ANALYZER_KIND}) returned NULL");
            process::exit(1);
        }
        unsafe {
            (loaded.api.timing_analyzer_process_usc)(analyzer, raw.profile_data);
            (loaded.api.timing_analyzer_finish)(analyzer);
        }

        let command_count = unsafe {
            (loaded.api.timing_analyzer_get_num_commands)(analyzer, TIMING_ANALYZER_KIND)
        } as usize;
        let discarded = unsafe {
            (loaded.api.timing_analyzer_get_num_discarded_work_cliques)(
                analyzer,
                TIMING_ANALYZER_KIND,
            )
        };
        total_discarded += discarded;

        let mut records = unsafe { fetch_records(&loaded, analyzer, command_count) }
            .unwrap_or_else(|error| {
                eprintln!("timing-analyzer fetch {path}: {error}");
                process::exit(1);
            });
        println!(
            "file: {path} bytes={} commands={} discarded_work_cliques={}",
            bytes.len(),
            command_count,
            discarded,
        );

        all_records.append(&mut records);
        unsafe { (loaded.api.timing_analyzer_destroy)(analyzer) };
        let _ = raw;
    }

    println!("\ntiming_analyzer_commands: {}", all_records.len());
    println!("discarded_work_cliques: {total_discarded}");

    println!(
        "\nfirst {} timing-analyzer records:",
        all_records.len().min(32)
    );
    println!(
        "  idx  prefix  work_shader         esl_shader          start_ns        end_ns          esl_start       duration_ns  cliques uscs avg_dur min_dur max_dur stddev"
    );
    for (idx, record) in all_records.iter().take(32).enumerate() {
        println!(
            "  {idx:>3}  {prefix}  0x{work_shader:016x}  0x{esl_shader:016x}  {start:>14}  {end:>14}  {esl_start:>14}  {duration:>11}  {cliques:>7} {uscs:>4} {avg:>7} {min:>7} {max:>7} {stddev:>6}",
            prefix = format_prefix(record.prefix()),
            work_shader = record.shader_address,
            esl_shader = record.esl_shader_address,
            start = record.start_ns,
            end = record.end_ns,
            esl_start = record.esl_start_ns,
            duration = record.duration_ns(),
            cliques = record.work_cliques,
            uscs = record.num_uscs,
            avg = record.avg_clique_duration,
            min = record.min_clique_duration,
            max = record.max_clique_duration,
            stddev = record.stddev_clique_duration,
        );
    }

    let mut groups = BTreeMap::<(u16, u64), Group>::new();
    for record in &all_records {
        let group = groups
            .entry((record.prefix(), record.shader_address))
            .or_default();
        group.commands += 1;
        group.work_cliques += record.work_cliques;
        group.duration_ns += record.duration_ns();
        group.num_uscs += record.num_uscs;
    }
    let total_duration = groups.values().map(|group| group.duration_ns).sum::<u64>();
    let total_cliques = groups.values().map(|group| group.work_cliques).sum::<u64>();

    println!("\ngrouped by kick software-id high16 + shader address:");
    println!(
        "  prefix  shader_address      commands   cliques     uscs  clique_share  duration_ns  duration_share"
    );
    for ((prefix, shader_address), group) in groups {
        let clique_share = if total_cliques > 0 {
            100.0 * group.work_cliques as f64 / total_cliques as f64
        } else {
            0.0
        };
        let duration_share = if total_duration > 0 {
            100.0 * group.duration_ns as f64 / total_duration as f64
        } else {
            0.0
        };
        println!(
            "  {prefix}  0x{shader_address:016x}  {commands:>8} {cliques:>9} {uscs:>8}  {clique_share:>10.4}% {duration:>12}  {duration_share:>12.4}%",
            prefix = format_prefix(prefix),
            commands = group.commands,
            cliques = group.work_cliques,
            uscs = group.num_uscs,
            duration = group.duration_ns,
        );
    }
}

struct RawProfile {
    profile_data: agxps_sys::AgxpsApsProfileData,
}

#[derive(Clone, Copy)]
struct TimingRecord {
    start_ns: u64,
    end_ns: u64,
    shader_address: u64,
    esl_start_ns: u64,
    esl_shader_address: u64,
    avg_clique_duration: u64,
    min_clique_duration: u64,
    max_clique_duration: u64,
    stddev_clique_duration: u64,
    work_cliques: u64,
    num_uscs: u64,
    kick_software_id: u64,
}

impl TimingRecord {
    fn prefix(self) -> u16 {
        (self.kick_software_id >> 48) as u16
    }

    fn duration_ns(self) -> u64 {
        self.end_ns.saturating_sub(self.start_ns)
    }
}

#[derive(Default)]
struct Group {
    commands: usize,
    work_cliques: u64,
    num_uscs: u64,
    duration_ns: u64,
}

unsafe fn parse_raw_profile(
    loaded: &agxps_sys::LoadedApi,
    generation: u32,
    variant: u32,
    rev: u32,
    bytes: &[u8],
) -> Result<RawProfile, String> {
    let api = &loaded.api;
    let gpu = unsafe { (api.gpu_create)(generation, variant, rev, false) };
    if gpu.is_null() {
        return Err(format!(
            "agxps_gpu_create({generation}, {variant}, {rev}) failed"
        ));
    }

    let descriptor = agxps_sys::AgxpsApsDescriptor::defaults_for(gpu);
    let parser = unsafe { (api.parser_create)(&descriptor) };
    if parser.is_null() {
        return Err("agxps_aps_parser_create returned NULL".to_owned());
    }

    let mut out = vec![0u8; 4096];
    let profile_data = unsafe {
        (api.parser_parse)(
            parser,
            bytes.as_ptr(),
            bytes.len() as c_long,
            agxps_sys::APS_PROFILING_TYPE_USC_SAMPLES,
            out.as_mut_ptr().cast(),
        )
    };
    let err_code = u64::from_le_bytes(out[..8].try_into().unwrap());
    if err_code != 0 {
        let message = unsafe {
            let ptr = (api.parse_error_string)(err_code);
            if ptr.is_null() {
                "(null)".to_owned()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };
        return Err(format!("parser error {err_code}: {message}"));
    }

    let _ = (parser, gpu);
    Ok(RawProfile { profile_data })
}

unsafe fn fetch_records(
    loaded: &agxps_sys::LoadedApi,
    analyzer: agxps_sys::AgxpsApsTimingAnalyzer,
    count: usize,
) -> Result<Vec<TimingRecord>, String> {
    let api = &loaded.api;
    let mut starts = vec![0u64; count];
    let mut ends = vec![0u64; count];
    let mut shaders = vec![0u64; count];
    let mut esl_starts = vec![0u64; count];
    let mut esl_shaders = vec![0u64; count];
    let mut avg_durations = vec![0u64; count];
    let mut min_durations = vec![0u64; count];
    let mut max_durations = vec![0u64; count];
    let mut stddev_durations = vec![0u64; count];
    let mut cliques = vec![0u64; count];
    let mut uscs = vec![0u64; count];
    let mut swids = vec![0u64; count];
    if count > 0 {
        let ok = unsafe {
            (api.timing_analyzer_get_work_start)(
                analyzer,
                TIMING_ANALYZER_KIND,
                starts.as_mut_ptr(),
                0,
                count as u64,
            ) != 0
                && (api.timing_analyzer_get_work_end)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    ends.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_work_shader_address)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    shaders.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_esl_start)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    esl_starts.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_esl_shader_address)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    esl_shaders.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_work_cliques_average_duration)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    avg_durations.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_work_cliques_min_duration)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    min_durations.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_work_cliques_max_duration)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    max_durations.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_work_cliques_stddev_duration)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    stddev_durations.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_num_work_cliques)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    cliques.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_num_uscs)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    uscs.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                && (api.timing_analyzer_get_kick_software_id)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    swids.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
        };
        if !ok {
            return Err("timing-analyzer range getter failed".to_owned());
        }
    }

    Ok((0..count)
        .map(|idx| TimingRecord {
            start_ns: starts[idx],
            end_ns: ends[idx],
            shader_address: shaders[idx],
            esl_start_ns: esl_starts[idx],
            esl_shader_address: esl_shaders[idx],
            avg_clique_duration: avg_durations[idx],
            min_clique_duration: min_durations[idx],
            max_clique_duration: max_durations[idx],
            stddev_clique_duration: stddev_durations[idx],
            work_cliques: cliques[idx],
            num_uscs: uscs[idx],
            kick_software_id: swids[idx],
        })
        .collect())
}

fn format_prefix(prefix: u16) -> String {
    format!("0x{prefix:04x}")
}
