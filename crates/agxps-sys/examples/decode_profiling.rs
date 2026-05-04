//! Decode a single `Profiling_f_*.raw` file and print per-kick info.
//!
//! Usage:
//! ```
//! cargo run -p agxps-sys --example decode_profiling -- /path/to/Profiling_f_0.raw
//! ```
//!
//! By default assumes the host is M4 Pro (`gen=16, variant=3, rev=1`).
//! Override with `AGXPS_GEN`, `AGXPS_VARIANT`, `AGXPS_REV` env vars.

use std::env;
use std::fs;
use std::process;

fn main() {
    let path = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: decode_profiling <Profiling_f_N.raw>");
        process::exit(2);
    });

    let generation: u32 = env::var("AGXPS_GEN").ok().and_then(|v| v.parse().ok()).unwrap_or(16);
    let variant: u32 = env::var("AGXPS_VARIANT").ok().and_then(|v| v.parse().ok()).unwrap_or(3);
    let rev: u32 = env::var("AGXPS_REV").ok().and_then(|v| v.parse().ok()).unwrap_or(1);

    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("read {}: {e}", path);
            process::exit(1);
        }
    };
    println!("loaded {} bytes from {}", bytes.len(), path);

    let loaded = match agxps_sys::load() {
        Ok(l) => l,
        Err(e) => {
            eprintln!("load: {e}");
            process::exit(1);
        }
    };
    println!("framework UUID: {}", loaded.framework_uuid);

    let decoded = match loaded.parse_profiling(generation, variant, rev, &bytes) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("parse_profiling(gen={generation}, variant={variant}, rev={rev}): {e}");
            process::exit(1);
        }
    };

    println!("\ndecoded:");
    println!("  kicks:           {}", decoded.kick_starts.len());
    println!("  usc_timestamps:  {}", decoded.usc_timestamps.len());
    println!("  counters:        {}", decoded.counter_num);

    let kick_groups = decoded.group_by_clique();
    let total_kicks: usize = kick_groups.values().sum();

    println!("\ngrouped by software-id high16 (= kernel/clique):");
    println!("  prefix    kicks  share");
    for (prefix, kicks) in &kick_groups {
        let share = if total_kicks == 0 {
            0.0
        } else {
            100.0 * *kicks as f64 / total_kicks as f64
        };
        println!("  0x{prefix:04x}  {kicks:>5}  {share:>6.2}%");
    }

    println!("\nfirst 16 kicks:");
    for i in 0..decoded.kick_starts.len().min(16) {
        println!(
            "  [{i:>3}] start={:>20} swid=0x{:016x}",
            decoded.kick_starts[i], decoded.kick_software_ids[i],
        );
    }
}
