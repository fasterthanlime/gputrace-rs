use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerFileEntry {
    pub name: String,
    pub size: u64,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerReport {
    pub input_path: PathBuf,
    pub profiler_directory: PathBuf,
    pub stream_data_present: bool,
    pub timeline_file_count: usize,
    pub counter_file_count: usize,
    pub profiling_file_count: usize,
    pub kdebug_file_count: usize,
    pub other_file_count: usize,
    pub total_bytes: u64,
    pub files: Vec<ProfilerFileEntry>,
    pub notes: Vec<String>,
}

pub fn report<P: AsRef<Path>>(path: P) -> Result<ProfilerReport> {
    let input_path = path.as_ref().to_path_buf();
    let profiler_directory =
        find_profiler_directory(&input_path).ok_or_else(|| Error::NotFound(input_path.clone()))?;

    let mut files = Vec::new();
    let mut stream_data_present = false;
    let mut timeline_file_count = 0;
    let mut counter_file_count = 0;
    let mut profiling_file_count = 0;
    let mut kdebug_file_count = 0;
    let mut other_file_count = 0;
    let mut total_bytes = 0;

    for entry in fs::read_dir(&profiler_directory)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().into_owned();
        let kind = classify_file(&name);
        match kind.as_str() {
            "streamData" => stream_data_present = true,
            "timeline" => timeline_file_count += 1,
            "counter" => counter_file_count += 1,
            "profiling" => profiling_file_count += 1,
            "kdebug" => kdebug_file_count += 1,
            _ => other_file_count += 1,
        }

        total_bytes += metadata.len();
        files.push(ProfilerFileEntry {
            name,
            size: metadata.len(),
            kind,
        });
    }

    files.sort_by(|left, right| left.name.cmp(&right.name));

    let mut notes = Vec::new();
    if !stream_data_present {
        notes.push(
            "streamData is missing, so dispatch-level profiler joins are unavailable.".to_owned(),
        );
    }
    notes.push(
        "This report inventories .gpuprofiler_raw artifacts only; detailed counter parsing is not implemented yet."
            .to_owned(),
    );

    Ok(ProfilerReport {
        input_path,
        profiler_directory,
        stream_data_present,
        timeline_file_count,
        counter_file_count,
        profiling_file_count,
        kdebug_file_count,
        other_file_count,
        total_bytes,
        files,
        notes,
    })
}

pub fn format_report(report: &ProfilerReport) -> String {
    let mut out = String::new();
    out.push_str("GPU Profiler Inventory\n");
    out.push_str("======================\n");
    out.push_str(&format!(
        "profiler_directory={}\n",
        report.profiler_directory.display()
    ));
    out.push_str(&format!(
        "files={} total_bytes={} streamData={}\n",
        report.files.len(),
        report.total_bytes,
        if report.stream_data_present {
            "present"
        } else {
            "missing"
        }
    ));
    out.push_str(&format!(
        "timeline={} counter={} profiling={} kdebug={} other={}\n",
        report.timeline_file_count,
        report.counter_file_count,
        report.profiling_file_count,
        report.kdebug_file_count,
        report.other_file_count
    ));

    for note in &report.notes {
        out.push_str(&format!("~ {note}\n"));
    }

    for file in &report.files {
        out.push_str(&format!(
            "  {:<10} {:>10} {}\n",
            file.kind, file.size, file.name
        ));
    }

    out
}

fn find_profiler_directory(path: &Path) -> Option<PathBuf> {
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext == "gpuprofiler_raw")
    {
        return path.is_dir().then(|| path.to_path_buf());
    }

    let adjacent = PathBuf::from(format!("{}.gpuprofiler_raw", path.display()));
    if adjacent.is_dir() {
        return Some(adjacent);
    }

    let metadata = fs::metadata(path).ok()?;
    if !metadata.is_dir() {
        return None;
    }

    fs::read_dir(path)
        .ok()?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|entry| {
            entry.is_dir()
                && entry
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext == "gpuprofiler_raw")
        })
}

fn classify_file(name: &str) -> String {
    if name == "streamData" {
        "streamData".to_owned()
    } else if name.starts_with("Timeline_f_") && name.ends_with(".raw") {
        "timeline".to_owned()
    } else if name.starts_with("Counters_f_") && name.ends_with(".raw") {
        "counter".to_owned()
    } else if name.starts_with("Profiling_f_") && name.ends_with(".raw") {
        "profiling".to_owned()
    } else if name.starts_with("kdebug") && name.ends_with(".raw") {
        "kdebug".to_owned()
    } else {
        "other".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn finds_adjacent_profiler_directory() {
        let dir = tempdir().unwrap();
        let trace_path = dir.path().join("sample.gputrace");
        fs::create_dir(&trace_path).unwrap();
        let profiler_dir = dir.path().join("sample.gputrace.gpuprofiler_raw");
        fs::create_dir(&profiler_dir).unwrap();
        fs::write(profiler_dir.join("streamData"), [0u8; 8]).unwrap();
        fs::write(profiler_dir.join("Timeline_f_0.raw"), [0u8; 16]).unwrap();

        let report = report(&trace_path).unwrap();
        assert_eq!(report.profiler_directory, profiler_dir);
        assert!(report.stream_data_present);
        assert_eq!(report.timeline_file_count, 1);
    }

    #[test]
    fn finds_nested_profiler_directory_inside_trace_bundle() {
        let dir = tempdir().unwrap();
        let trace_path = dir.path().join("sample.gputrace");
        let profiler_dir = trace_path.join("capture.gpuprofiler_raw");
        fs::create_dir_all(&profiler_dir).unwrap();
        fs::write(profiler_dir.join("Counters_f_4.raw"), [0u8; 4]).unwrap();
        fs::write(profiler_dir.join("Profiling_f_1.raw"), [0u8; 12]).unwrap();

        let report = report(&trace_path).unwrap();
        assert_eq!(report.profiler_directory, profiler_dir);
        assert_eq!(report.counter_file_count, 1);
        assert_eq!(report.profiling_file_count, 1);
        assert!(!report.stream_data_present);
    }

    #[test]
    fn formats_inventory_report() {
        let report = ProfilerReport {
            input_path: PathBuf::from("trace.gputrace"),
            profiler_directory: PathBuf::from("trace.gputrace.gpuprofiler_raw"),
            stream_data_present: true,
            timeline_file_count: 1,
            counter_file_count: 2,
            profiling_file_count: 1,
            kdebug_file_count: 0,
            other_file_count: 1,
            total_bytes: 42,
            files: vec![ProfilerFileEntry {
                name: "streamData".to_owned(),
                size: 42,
                kind: "streamData".to_owned(),
            }],
            notes: vec!["Detailed counter parsing is not implemented yet.".to_owned()],
        };

        let text = format_report(&report);
        assert!(text.contains("GPU Profiler Inventory"));
        assert!(text.contains("streamData=present"));
        assert!(text.contains("Detailed counter parsing"));
    }
}
