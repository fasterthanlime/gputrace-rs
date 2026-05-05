//! Join timing-analyzer command spans with work-clique instruction stats.
//!
//! Usage:
//! ```
//! cargo run -p agxps-sys --example probe_timing_trace_join -- /path/to/Profiling_f_*.raw
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
        eprintln!("usage: probe_timing_trace_join <Profiling_f_N.raw> [Profiling_f_N.raw...]");
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

    let mut groups = BTreeMap::<(u16, u64), ShaderGroup>::new();
    let mut totals = Totals::default();
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
        let records =
            unsafe { timing_records(&loaded, raw.profile_data) }.unwrap_or_else(|error| {
                eprintln!("timing analyzer {path}: {error}");
                process::exit(1);
            });
        let work =
            unsafe { fetch_work_cliques(&loaded, raw.profile_data) }.unwrap_or_else(|error| {
                eprintln!("work cliques {path}: {error}");
                process::exit(1);
            });

        let mut file_matched = 0usize;
        let mut file_missing = 0usize;
        let mut file_unmatched = 0usize;

        totals.commands += records.len();
        for record in &records {
            let group = groups
                .entry((record.prefix(), record.esl_shader_address))
                .or_default();
            group.commands += 1;
            group.work_shader_address = record.shader_address;
            group.record_cliques += record.work_cliques;
            group.duration_ns += record.duration_ns();
        }

        for idx in 0..work.traces.len() {
            totals.work_cliques += 1;
            if work.missing_ends[idx] != 0 {
                totals.missing_work_cliques += 1;
                file_missing += 1;
                continue;
            }

            let start_ns =
                unsafe { (loaded.api.get_system_timestamp)(raw.profile_data, work.starts[idx]) };
            let end_ns =
                unsafe { (loaded.api.get_system_timestamp)(raw.profile_data, work.ends[idx]) };
            let Some(record_idx) = find_record(&records, start_ns, end_ns) else {
                totals.unmatched_work_cliques += 1;
                file_unmatched += 1;
                continue;
            };

            let record = records[record_idx];
            let group = groups
                .entry((record.prefix(), record.esl_shader_address))
                .or_default();
            group.work_shader_address = record.shader_address;
            group.matched_work_cliques += 1;
            let events = unsafe {
                (loaded.api.instruction_trace_get_execution_events_num)(
                    raw.profile_data,
                    work.traces[idx],
                )
            };
            group.execution_events += events;
            let stats = unsafe {
                (loaded.api.instruction_trace_get_instruction_stats)(
                    raw.gpu,
                    raw.profile_data,
                    work.traces[idx],
                )
            };
            for (dst, src) in group.stats_words.iter_mut().zip(stats.words) {
                *dst += src as u128;
            }
            totals.matched_work_cliques += 1;
            file_matched += 1;
        }

        println!(
            "file: {path} bytes={} commands={} work_cliques={} matched={} unmatched={} missing={}",
            bytes.len(),
            records.len(),
            work.traces.len(),
            file_matched,
            file_unmatched,
            file_missing,
        );
        let _ = raw;
    }

    println!("\ntotals:");
    println!(
        "  commands={} work_cliques={} matched={} unmatched={} missing={}",
        totals.commands,
        totals.work_cliques,
        totals.matched_work_cliques,
        totals.unmatched_work_cliques,
        totals.missing_work_cliques,
    );

    let total_duration = groups.values().map(|group| group.duration_ns).sum::<u64>();
    let total_record_cliques = groups
        .values()
        .map(|group| group.record_cliques)
        .sum::<u64>();
    let total_events = groups
        .values()
        .map(|group| group.execution_events)
        .sum::<u64>();
    let total_w0 = groups
        .values()
        .map(|group| group.stats_words[0])
        .sum::<u128>();
    let total_w1 = groups
        .values()
        .map(|group| group.stats_words[1])
        .sum::<u128>();

    println!("\njoined by timing prefix + ESL shader address:");
    println!(
        "  prefix  esl_shader          work_shader         cmds  rec_cliques matched  duration_ns dur_share  events ev_share        w0 w0_share        w1 w1_share"
    );
    for ((prefix, shader_address), group) in groups {
        let duration_share = share_u64(group.duration_ns, total_duration);
        let event_share = share_u64(group.execution_events, total_events);
        let w0_share = share_u128(group.stats_words[0], total_w0);
        let w1_share = share_u128(group.stats_words[1], total_w1);
        println!(
            "  {prefix}  0x{shader_address:016x} 0x{work_shader:016x} {commands:>5} {rec_cliques:>12} {matched:>7} {duration:>12} {duration_share:>8.3}% {events:>7} {event_share:>8.3}% {w0:>9} {w0_share:>8.3}% {w1:>9} {w1_share:>8.3}%",
            prefix = format_prefix(prefix),
            work_shader = group.work_shader_address,
            commands = group.commands,
            rec_cliques = group.record_cliques,
            matched = group.matched_work_cliques,
            duration = group.duration_ns,
            events = group.execution_events,
            w0 = group.stats_words[0],
            w1 = group.stats_words[1],
        );
    }

    println!("\nrecord_cliques_total={total_record_cliques}");
}

#[derive(Default)]
struct Totals {
    commands: usize,
    work_cliques: usize,
    matched_work_cliques: usize,
    unmatched_work_cliques: usize,
    missing_work_cliques: usize,
}

#[derive(Default)]
struct ShaderGroup {
    work_shader_address: u64,
    commands: usize,
    record_cliques: u64,
    duration_ns: u64,
    matched_work_cliques: usize,
    execution_events: u64,
    stats_words: [u128; 14],
}

#[derive(Clone, Copy)]
struct TimingRecord {
    start_ns: u64,
    end_ns: u64,
    shader_address: u64,
    esl_shader_address: u64,
    work_cliques: u64,
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

struct RawProfile {
    gpu: agxps_sys::AgxpsGpu,
    profile_data: agxps_sys::AgxpsApsProfileData,
}

struct WorkCliques {
    starts: Vec<u64>,
    ends: Vec<u64>,
    missing_ends: Vec<u8>,
    traces: Vec<agxps_sys::AgxpsApsCliqueInstructionTrace>,
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

    let _ = parser;
    Ok(RawProfile { gpu, profile_data })
}

unsafe fn timing_records(
    loaded: &agxps_sys::LoadedApi,
    profile_data: agxps_sys::AgxpsApsProfileData,
) -> Result<Vec<TimingRecord>, String> {
    let api = &loaded.api;
    let analyzer = unsafe { (api.timing_analyzer_create)(TIMING_ANALYZER_KIND) };
    if analyzer.is_null() {
        return Err(format!(
            "timing_analyzer_create({TIMING_ANALYZER_KIND}) returned NULL"
        ));
    }

    unsafe {
        (api.timing_analyzer_process_usc)(analyzer, profile_data);
        (api.timing_analyzer_finish)(analyzer);
    }
    let count =
        unsafe { (api.timing_analyzer_get_num_commands)(analyzer, TIMING_ANALYZER_KIND) } as usize;
    let result = unsafe { fetch_timing_records(loaded, analyzer, count) };
    unsafe { (api.timing_analyzer_destroy)(analyzer) };
    result
}

unsafe fn fetch_timing_records(
    loaded: &agxps_sys::LoadedApi,
    analyzer: agxps_sys::AgxpsApsTimingAnalyzer,
    count: usize,
) -> Result<Vec<TimingRecord>, String> {
    let api = &loaded.api;
    let mut starts = vec![0u64; count];
    let mut ends = vec![0u64; count];
    let mut shaders = vec![0u64; count];
    let mut esl_shaders = vec![0u64; count];
    let mut cliques = vec![0u64; count];
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
                && (api.timing_analyzer_get_esl_shader_address)(
                    analyzer,
                    TIMING_ANALYZER_KIND,
                    esl_shaders.as_mut_ptr(),
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
            esl_shader_address: esl_shaders[idx],
            work_cliques: cliques[idx],
            kick_software_id: swids[idx],
        })
        .collect())
}

unsafe fn fetch_work_cliques(
    loaded: &agxps_sys::LoadedApi,
    profile_data: agxps_sys::AgxpsApsProfileData,
) -> Result<WorkCliques, String> {
    let api = &loaded.api;
    let n = unsafe { (api.get_work_cliques_num)(profile_data) } as usize;
    let mut starts = vec![0u64; n];
    let mut ends = vec![0u64; n];
    let mut missing_ends = vec![0u8; n];
    let mut traces = vec![0u64; n];
    if n > 0 {
        let ok = unsafe {
            (api.get_work_clique_start)(profile_data, starts.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_work_clique_end)(profile_data, ends.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_work_clique_missing_end)(
                    profile_data,
                    missing_ends.as_mut_ptr(),
                    0,
                    n as u64,
                ) != 0
                && (api.get_work_clique_instruction_trace)(
                    profile_data,
                    traces.as_mut_ptr(),
                    0,
                    n as u64,
                ) != 0
        };
        if !ok {
            return Err("work-clique range getter failed".to_owned());
        }
    }
    Ok(WorkCliques {
        starts,
        ends,
        missing_ends,
        traces,
    })
}

fn find_record(records: &[TimingRecord], start_ns: u64, end_ns: u64) -> Option<usize> {
    let mut best = None;
    for (idx, record) in records.iter().enumerate() {
        let contains_range = record.start_ns <= start_ns && end_ns <= record.end_ns;
        let contains_start = record.start_ns <= start_ns && start_ns <= record.end_ns;
        let overlaps = record.start_ns <= end_ns && start_ns <= record.end_ns;
        if !contains_range && !contains_start && !overlaps {
            continue;
        }
        let rank = if contains_range {
            0
        } else if contains_start {
            1
        } else {
            2
        };
        let duration = record.duration_ns();
        if best
            .map(|(_, best_rank, best_duration)| (rank, duration) < (best_rank, best_duration))
            .unwrap_or(true)
        {
            best = Some((idx, rank, duration));
        }
    }
    best.map(|(idx, _, _)| idx)
}

fn share_u64(value: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        100.0 * value as f64 / total as f64
    }
}

fn share_u128(value: u128, total: u128) -> f64 {
    if total == 0 {
        0.0
    } else {
        100.0 * value as f64 / total as f64
    }
}

fn format_prefix(prefix: u16) -> String {
    format!("0x{prefix:04x}")
}
