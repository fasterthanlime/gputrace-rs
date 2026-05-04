//! Probe Xcode's agxps counter-name obfuscation map API.
//!
//! Usage:
//! ```
//! cargo run -p agxps-sys --example probe_obfuscation -- [--map path.csv] [name-or-hash...]
//! ```
//!
//! The map format accepted by `agxps_load_counter_obfuscation_map` is a
//! two-column CSV-like text file with rows `readable_name,obfuscated_name`.

use std::env;
use std::path::PathBuf;
use std::process;

fn main() {
    let mut map_path = None;
    let mut probes = Vec::new();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--map" {
            let Some(path) = args.next() else {
                eprintln!("--map requires a path");
                process::exit(2);
            };
            map_path = Some(PathBuf::from(path));
        } else {
            probes.push(arg);
        }
    }

    if probes.is_empty() {
        probes.extend([
            "ALUUtilization".to_owned(),
            "ALU Utilization".to_owned(),
            "79E88035C9BC883D403F17831B8C9264E643C6B76E9B3C1451B49B0F672C32BF".to_owned(),
        ]);
    }

    let loaded = match agxps_sys::load() {
        Ok(loaded) => loaded,
        Err(e) => {
            eprintln!("load: {e}");
            process::exit(1);
        }
    };
    println!("framework: {}", loaded.framework_path);

    loaded.unload_counter_obfuscation_map();
    println!("load(NULL): {}", loaded.load_counter_obfuscation_map(None));

    if let Some(path) = &map_path {
        println!(
            "load({}): {}",
            path.display(),
            loaded.load_counter_obfuscation_map(Some(path))
        );
    }

    println!("\ninput, obfuscated_name(input), deobfuscate_name(input)");
    for probe in probes {
        println!(
            "{probe}, {}, {}",
            loaded.obfuscated_counter_name(&probe),
            loaded.deobfuscate_counter_name(&probe)
        );
    }
}
