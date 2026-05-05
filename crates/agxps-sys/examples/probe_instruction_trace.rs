//! Probe work-clique instruction traces in `Profiling_f_*.raw`.
//!
//! Usage:
//! ```
//! cargo run -p agxps-sys --example probe_instruction_trace -- /path/to/Profiling_f_0.raw
//! ```

use std::collections::BTreeMap;
use std::env;
use std::ffi::{CStr, c_long};
use std::fs;
use std::process;

fn main() {
    let path = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: probe_instruction_trace <Profiling_f_N.raw>");
        process::exit(2);
    });

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

    let bytes = fs::read(&path).unwrap_or_else(|error| {
        eprintln!("read {path}: {error}");
        process::exit(1);
    });
    println!("loaded {} bytes from {path}", bytes.len());

    let loaded = agxps_sys::load().unwrap_or_else(|error| {
        eprintln!("load: {error}");
        process::exit(1);
    });
    println!("framework: {}", loaded.framework_path);

    let raw = unsafe { parse_raw_profile(&loaded, generation, variant, rev, &bytes) }
        .unwrap_or_else(|error| {
            eprintln!("parse: {error}");
            process::exit(1);
        });

    let work = unsafe { fetch_work_cliques(&loaded, raw.profile_data) }.unwrap_or_else(|error| {
        eprintln!("work-clique fetch: {error}");
        process::exit(1);
    });
    println!("kicks: {}", raw.kick_ids.len());
    println!("work_cliques: {}", work.traces.len());

    let mut groups = BTreeMap::<u16, Group>::new();
    let mut rows = Vec::new();
    for idx in 0..work.traces.len() {
        let trace = work.traces[idx];
        let kick_id = work.kick_ids[idx];
        let missing_end = work.missing_ends[idx] != 0;
        let start_t = agxps_sys::unpack_time_sample(work.starts[idx]).0;
        let end_t = agxps_sys::unpack_time_sample(work.ends[idx]).0;
        let prefix = prefix_for_work_clique(&raw, kick_id, start_t, end_t);
        let lifetime_ticks = if missing_end {
            0
        } else {
            end_t.saturating_sub(start_t) as u64
        };

        let counts = unsafe { trace_counts(&loaded, raw.profile_data, trace) };
        let stats = unsafe {
            (loaded.api.instruction_trace_get_instruction_stats)(raw.gpu, raw.profile_data, trace)
        };

        let group = groups.entry(prefix).or_default();
        group.work_cliques += 1;
        group.missing_ends += usize::from(missing_end);
        group.lifetime_ticks += lifetime_ticks;
        group.pc_advances += counts.pc_advances;
        group.init_pcs += counts.init_pcs;
        group.thread_execution_changes += counts.thread_execution_changes;
        group.timestamp_references += counts.timestamp_references;
        group.execution_events += counts.execution_events;
        if counts.total() > 0 || stats.words.iter().any(|word| *word != 0) {
            group.traced_work_cliques += 1;
        }
        for (dst, src) in group.stats_words.iter_mut().zip(stats.words) {
            *dst += src as u128;
        }

        if idx < 24 {
            rows.push(Row {
                idx,
                prefix,
                kick_id,
                trace,
                start_t,
                end_t,
                missing_end,
                counts,
                stats,
            });
        }
    }

    println!("\nfirst {} work cliques:", rows.len());
    println!(
        "  idx  prefix  kick_id  trace_ref            time        pc_adv init_pc thread  ts_ref events  stats"
    );
    for row in rows {
        let end = if row.missing_end {
            "missing".to_owned()
        } else {
            row.end_t.to_string()
        };
        println!(
            "  {idx:>3}  {prefix}  {kick_id:>7}  0x{trace:016x}  {start:>6}..{end:<7}  {pc:>6} {init:>7} {thread:>6} {ts:>7} {events:>6}  {stats}",
            idx = row.idx,
            prefix = format_prefix(row.prefix),
            kick_id = row.kick_id,
            trace = row.trace,
            start = row.start_t,
            pc = row.counts.pc_advances,
            init = row.counts.init_pcs,
            thread = row.counts.thread_execution_changes,
            ts = row.counts.timestamp_references,
            events = row.counts.execution_events,
            stats = format_nonzero_u64(&row.stats.words, 8),
        );
    }

    println!("\ngrouped by work-clique kick software-id high16:");
    println!(
        "  prefix  cliques traced missing   lifetime  pc_adv init_pc thread  ts_ref  events  stats"
    );
    for (prefix, group) in groups {
        println!(
            "  {prefix} {cliques:>8} {traced:>6} {missing:>7} {lifetime:>10} {pc:>7} {init:>7} {thread:>6} {ts:>7} {events:>7}  {stats}",
            prefix = format_prefix(prefix),
            cliques = group.work_cliques,
            traced = group.traced_work_cliques,
            missing = group.missing_ends,
            lifetime = group.lifetime_ticks,
            pc = group.pc_advances,
            init = group.init_pcs,
            thread = group.thread_execution_changes,
            ts = group.timestamp_references,
            events = group.execution_events,
            stats = format_nonzero_u128(&group.stats_words, 10),
        );
    }

    // Match the other examples: keep private framework-owned objects alive for
    // the lifetime of this short process.
    let _ = raw;
}

#[derive(Clone, Copy)]
struct TraceCounts {
    pc_advances: u64,
    init_pcs: u64,
    thread_execution_changes: u64,
    timestamp_references: u64,
    execution_events: u64,
}

impl TraceCounts {
    fn total(self) -> u64 {
        self.pc_advances
            + self.init_pcs
            + self.thread_execution_changes
            + self.timestamp_references
            + self.execution_events
    }
}

#[derive(Default)]
struct Group {
    work_cliques: usize,
    traced_work_cliques: usize,
    missing_ends: usize,
    lifetime_ticks: u64,
    pc_advances: u64,
    init_pcs: u64,
    thread_execution_changes: u64,
    timestamp_references: u64,
    execution_events: u64,
    stats_words: [u128; 14],
}

struct Row {
    idx: usize,
    prefix: u16,
    kick_id: u32,
    trace: agxps_sys::AgxpsApsCliqueInstructionTrace,
    start_t: u32,
    end_t: u32,
    missing_end: bool,
    counts: TraceCounts,
    stats: agxps_sys::AgxpsApsInstructionStats,
}

struct RawProfile {
    gpu: agxps_sys::AgxpsGpu,
    profile_data: agxps_sys::AgxpsApsProfileData,
    starts: Vec<u64>,
    ends: Vec<u64>,
    kick_ids: Vec<u64>,
    missing_ends: Vec<bool>,
    swids: Vec<u64>,
}

struct WorkCliques {
    starts: Vec<u64>,
    ends: Vec<u64>,
    kick_ids: Vec<u32>,
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

    let n = unsafe { (api.get_kicks_num)(profile_data) } as usize;
    let mut starts = vec![0u64; n];
    let mut ends = vec![0u64; n];
    let mut kick_ids = vec![0u64; n];
    let mut missing = vec![0u8; n];
    let mut swids = vec![0u64; n];
    if n > 0 {
        let ok = unsafe {
            (api.get_kick_start)(profile_data, starts.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_kick_end)(profile_data, ends.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_kick_id)(profile_data, kick_ids.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_kick_missing_end)(profile_data, missing.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_kick_software_id)(profile_data, swids.as_mut_ptr(), 0, n as u64) != 0
        };
        if !ok {
            return Err("kick metadata getter failed".to_owned());
        }
    }

    let _ = parser;
    Ok(RawProfile {
        gpu,
        profile_data,
        starts,
        ends,
        kick_ids,
        missing_ends: missing.into_iter().map(|value| value != 0).collect(),
        swids,
    })
}

unsafe fn fetch_work_cliques(
    loaded: &agxps_sys::LoadedApi,
    profile_data: agxps_sys::AgxpsApsProfileData,
) -> Result<WorkCliques, String> {
    let api = &loaded.api;
    let n = unsafe { (api.get_work_cliques_num)(profile_data) } as usize;
    let mut starts = vec![0u64; n];
    let mut ends = vec![0u64; n];
    let mut kick_ids = vec![0u32; n];
    let mut missing_ends = vec![0u8; n];
    let mut traces = vec![0u64; n];
    if n > 0 {
        let ok = unsafe {
            (api.get_work_clique_start)(profile_data, starts.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_work_clique_end)(profile_data, ends.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_work_clique_kick_id)(profile_data, kick_ids.as_mut_ptr(), 0, n as u64)
                    != 0
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
        kick_ids,
        missing_ends,
        traces,
    })
}

unsafe fn trace_counts(
    loaded: &agxps_sys::LoadedApi,
    profile_data: agxps_sys::AgxpsApsProfileData,
    trace: agxps_sys::AgxpsApsCliqueInstructionTrace,
) -> TraceCounts {
    let api = &loaded.api;
    TraceCounts {
        pc_advances: unsafe { (api.instruction_trace_get_pc_advances_num)(profile_data, trace) },
        init_pcs: unsafe { (api.instruction_trace_get_init_pcs_num)(profile_data, trace) },
        thread_execution_changes: unsafe {
            (api.instruction_trace_get_thread_execution_changes_num)(profile_data, trace)
        },
        timestamp_references: unsafe {
            (api.instruction_trace_get_timestamp_references_num)(profile_data, trace)
        },
        execution_events: unsafe {
            (api.instruction_trace_get_execution_events_num)(profile_data, trace)
        },
    }
}

fn format_prefix(prefix: u16) -> String {
    if prefix == u16::MAX {
        "unknown".to_owned()
    } else {
        format!("0x{prefix:04x}")
    }
}

fn prefix_for_work_clique(raw: &RawProfile, kick_id: u32, start_t: u32, end_t: u32) -> u16 {
    if let Some(swid) = raw.swids.get(kick_id as usize) {
        return (swid >> 48) as u16;
    }

    for (candidate_id, swid) in raw.kick_ids.iter().zip(&raw.swids) {
        if *candidate_id as u32 == kick_id {
            return (swid >> 48) as u16;
        }
    }

    let mut best = None;
    for idx in 0..raw.swids.len() {
        if raw.missing_ends[idx] {
            continue;
        }
        let kick_start = agxps_sys::unpack_time_sample(raw.starts[idx]).0;
        let kick_end = agxps_sys::unpack_time_sample(raw.ends[idx]).0;
        if kick_start <= start_t && end_t <= kick_end {
            let duration = kick_end.saturating_sub(kick_start);
            let prefix = (raw.swids[idx] >> 48) as u16;
            if best
                .map(|(best_duration, _)| duration < best_duration)
                .unwrap_or(true)
            {
                best = Some((duration, prefix));
            }
        }
    }

    best.map(|(_, prefix)| prefix).unwrap_or(u16::MAX)
}

fn format_nonzero_u64(words: &[u64], limit: usize) -> String {
    let parts = words
        .iter()
        .enumerate()
        .filter(|(_, value)| **value != 0)
        .take(limit)
        .map(|(idx, value)| format!("w{idx}={value}"))
        .collect::<Vec<_>>();
    if parts.is_empty() {
        "-".to_owned()
    } else {
        parts.join(",")
    }
}

fn format_nonzero_u128(words: &[u128], limit: usize) -> String {
    let parts = words
        .iter()
        .enumerate()
        .filter(|(_, value)| **value != 0)
        .take(limit)
        .map(|(idx, value)| format!("w{idx}={value}"))
        .collect::<Vec<_>>();
    if parts.is_empty() {
        "-".to_owned()
    } else {
        parts.join(",")
    }
}
