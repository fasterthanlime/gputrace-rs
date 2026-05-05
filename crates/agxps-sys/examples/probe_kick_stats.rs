//! Probe `agxps_aps_kick_time_stats_create`.
//!
//! Usage:
//! ```
//! cargo run -p agxps-sys --example probe_kick_stats -- /path/to/Profiling_f_0.raw
//! ```

use std::collections::BTreeMap;
use std::env;
use std::ffi::{CStr, c_long};
use std::fs;
use std::process;
use std::sync::{Arc, Mutex};

use block2::RcBlock;

fn main() {
    let path = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: probe_kick_stats <Profiling_f_N.raw>");
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
    println!("kicks: {}", raw.swids.len());
    if let Some(first_start) = raw.starts.first().copied() {
        let usc = unsafe { (loaded.api.get_usc_timestamp)(raw.profile_data, first_start) };
        println!("first kick start raw=0x{first_start:016x} usc_timestamp={usc}");
    }

    let mut groups = BTreeMap::<u16, usize>::new();
    for (swid, missing) in raw.swids.iter().zip(&raw.missing_ends) {
        if *missing {
            continue;
        }
        *groups.entry((swid >> 48) as u16).or_default() += 1;
    }

    for timestamp_kind in [0u32, 1] {
        let label = match timestamp_kind {
            0 => "system",
            1 => "usc",
            _ => unreachable!(),
        };
        println!("\n{label} timestamp stats by software-id high16:");
        println!(
            "  prefix    accepted       min          mean         median          max          sum        share"
        );

        let mut rows = Vec::new();
        for prefix in groups.keys().copied() {
            let state = Arc::new(Mutex::new(PrefixFilterState::new(
                raw.swids.clone(),
                raw.missing_ends.clone(),
                prefix,
            )));
            let callback_state = Arc::clone(&state);
            let filter: RcBlock<dyn Fn() -> i32> = RcBlock::new(move || -> i32 {
                if callback_state
                    .lock()
                    .map(|mut state| state.next())
                    .unwrap_or(false)
                {
                    1
                } else {
                    0
                }
            });
            let stats = unsafe {
                (loaded.api.kick_time_stats_create_sampled)(
                    raw.profile_data,
                    timestamp_kind,
                    0,
                    1,
                    0,
                    raw.swids.len() as u64,
                    RcBlock::as_ptr(&filter).cast(),
                )
            };
            let accepted = state.lock().map(|state| state.accepted).unwrap_or(0);
            if stats.is_null() || accepted == 0 {
                rows.push((prefix, accepted, f64::NAN, f64::NAN, f64::NAN, f64::NAN));
                continue;
            }
            let min = unsafe { (loaded.api.stats_min)(stats) };
            let mean = unsafe { (loaded.api.stats_mean)(stats) };
            let median = unsafe { (loaded.api.stats_median)(stats) };
            let max = unsafe { (loaded.api.stats_max)(stats) };
            unsafe { (loaded.api.stats_destroy)(stats) };
            rows.push((prefix, accepted, min, mean, median, max));
        }

        let total_sum: f64 = rows
            .iter()
            .map(|(_, accepted, _, mean, _, _)| *mean * *accepted as f64)
            .filter(|value| value.is_finite())
            .sum();
        for (prefix, accepted, min, mean, median, max) in rows {
            let sum = mean * accepted as f64;
            let share = if total_sum > 0.0 && sum.is_finite() {
                100.0 * sum / total_sum
            } else {
                0.0
            };
            println!(
                "  0x{prefix:04x}  {accepted:>8}  {min:>10.1}  {mean:>12.1}  {median:>12.1}  {max:>11.1}  {sum:>11.1}  {share:>8.4}%"
            );
        }
    }

    // Match the existing examples: the private objects are process-owned for
    // this short-lived RE probe. Destroying parser/profile_data has crashed on
    // some Xcode builds while experimenting.
    let _ = raw;
}

struct RawProfile {
    profile_data: agxps_sys::AgxpsApsProfileData,
    starts: Vec<u64>,
    swids: Vec<u64>,
    missing_ends: Vec<bool>,
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
    let mut swids = vec![0u64; n];
    let mut missing = vec![0u8; n];
    if n > 0 {
        let ok = unsafe {
            (api.get_kick_start)(profile_data, starts.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_kick_software_id)(profile_data, swids.as_mut_ptr(), 0, n as u64) != 0
                && (api.get_kick_missing_end)(profile_data, missing.as_mut_ptr(), 0, n as u64) != 0
        };
        if !ok {
            return Err("kick metadata getter failed".to_owned());
        }
    }

    let _ = (parser, gpu);
    Ok(RawProfile {
        profile_data,
        starts,
        swids,
        missing_ends: missing.into_iter().map(|value| value != 0).collect(),
    })
}

struct PrefixFilterState {
    swids: Vec<u64>,
    missing_ends: Vec<bool>,
    cursor: usize,
    accepted: usize,
    prefix: u16,
}

impl PrefixFilterState {
    fn new(swids: Vec<u64>, missing_ends: Vec<bool>, prefix: u16) -> Self {
        Self {
            swids,
            missing_ends,
            cursor: 0,
            accepted: 0,
            prefix,
        }
    }

    fn next(&mut self) -> bool {
        if self.cursor >= self.swids.len() {
            return false;
        }
        let index = self.cursor;
        self.cursor += 1;
        let matches = !self.missing_ends[index] && (self.swids[index] >> 48) as u16 == self.prefix;
        if matches {
            self.accepted += 1;
        }
        matches
    }
}
