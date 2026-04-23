use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

const ZERO_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearBufferFile {
    pub path: PathBuf,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClearBuffersReport {
    pub files: Vec<ClearBufferFile>,
    pub total_bytes: u64,
    pub skipped_symlinks: usize,
}

impl ClearBuffersReport {
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClearBuffersRunReport {
    pub files_cleared: usize,
    pub bytes_cleared: u64,
}

pub fn inventory<P: AsRef<Path>>(trace_path: P) -> Result<ClearBuffersReport> {
    let trace_path = trace_path.as_ref();
    let metadata = fs::metadata(trace_path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::NotFound(trace_path.to_path_buf())
        } else {
            Error::Io(err)
        }
    })?;
    if !metadata.is_dir() {
        return Err(Error::NotDirectory(trace_path.to_path_buf()));
    }

    let mut files = Vec::new();
    let mut total_bytes = 0;
    let mut skipped_symlinks = 0;

    for entry in fs::read_dir(trace_path)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with("MTLBuffer-") {
            continue;
        }

        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            skipped_symlinks += 1;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }

        total_bytes += metadata.len();
        files.push(ClearBufferFile {
            path,
            size: metadata.len(),
        });
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));

    Ok(ClearBuffersReport {
        files,
        total_bytes,
        skipped_symlinks,
    })
}

pub fn format_byte_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    match bytes {
        n if n >= GB => format!("{:.2} GB", n as f64 / GB as f64),
        n if n >= MB => format!("{:.2} MB", n as f64 / MB as f64),
        n if n >= KB => format!("{:.2} KB", n as f64 / KB as f64),
        n => format!("{n} B"),
    }
}

pub fn format_report(report: &ClearBuffersReport) -> String {
    if report.is_empty() {
        return "No MTLBuffer files found".to_owned();
    }

    let mut out = format!(
        "Found {} buffer files ({} total)",
        report.file_count(),
        format_byte_size(report.total_bytes)
    );
    if report.skipped_symlinks > 0 {
        out.push('\n');
        out.push_str(&format!("Will skip {} symlinks", report.skipped_symlinks));
    }
    out
}

pub fn clear_files(files: &[ClearBufferFile]) -> Result<ClearBuffersRunReport> {
    let mut run = ClearBuffersRunReport::default();
    for file in files {
        clear_file(&file.path, file.size)?;
        run.files_cleared += 1;
        run.bytes_cleared += file.size;
    }
    Ok(run)
}

pub fn clear_report(report: &ClearBuffersReport) -> Result<ClearBuffersRunReport> {
    clear_files(&report.files)
}

pub fn clear_file<P: AsRef<Path>>(path: P, size: u64) -> Result<()> {
    let mut file = File::options().write(true).truncate(true).open(path)?;
    let zeros = vec![0u8; ZERO_CHUNK_SIZE];
    let mut remaining = size;

    while remaining > 0 {
        let write_len = remaining.min(zeros.len() as u64) as usize;
        file.write_all(&zeros[..write_len])?;
        remaining -= write_len as u64;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use tempfile::tempdir;

    #[cfg(unix)]
    use std::os::unix::fs as unix_fs;

    #[test]
    fn formats_byte_sizes() {
        assert_eq!(format_byte_size(999), "999 B");
        assert_eq!(format_byte_size(1024), "1.00 KB");
        assert_eq!(format_byte_size(5 * 1024 * 1024), "5.00 MB");
        assert_eq!(format_byte_size(3 * 1024 * 1024 * 1024), "3.00 GB");
    }

    #[test]
    fn inventories_top_level_buffer_files_and_skips_non_files() {
        let dir = tempdir().unwrap();
        let trace_path = dir.path();

        fs::write(trace_path.join("MTLBuffer-2-0"), [1u8; 4]).unwrap();
        fs::write(trace_path.join("MTLBuffer-1-0"), [2u8; 2]).unwrap();
        fs::write(trace_path.join("other"), [3u8; 8]).unwrap();
        fs::create_dir(trace_path.join("MTLBuffer-dir")).unwrap();
        fs::create_dir(trace_path.join("nested")).unwrap();
        fs::write(trace_path.join("nested").join("MTLBuffer-3-0"), [4u8; 16]).unwrap();

        let report = inventory(trace_path).unwrap();

        assert_eq!(report.file_count(), 2);
        assert_eq!(report.total_bytes, 6);
        assert_eq!(report.skipped_symlinks, 0);
        assert_eq!(
            report
                .files
                .iter()
                .map(|file| {
                    file.path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned()
                })
                .collect::<Vec<_>>(),
            vec!["MTLBuffer-1-0".to_owned(), "MTLBuffer-2-0".to_owned()]
        );
    }

    #[cfg(unix)]
    #[test]
    fn inventories_symlinks_without_following_them() {
        let dir = tempdir().unwrap();
        let trace_path = dir.path();

        let target = trace_path.join("MTLBuffer-1-0");
        fs::write(&target, [1u8; 8]).unwrap();
        unix_fs::symlink(&target, trace_path.join("MTLBuffer-1-1")).unwrap();

        let report = inventory(trace_path).unwrap();

        assert_eq!(report.file_count(), 1);
        assert_eq!(report.total_bytes, 8);
        assert_eq!(report.skipped_symlinks, 1);
    }

    #[test]
    fn clears_files_without_changing_their_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MTLBuffer-1-0");
        fs::write(&path, [7u8; 5]).unwrap();

        clear_file(&path, 5).unwrap();

        assert_eq!(fs::metadata(&path).unwrap().len(), 5);
        assert_eq!(fs::read(&path).unwrap(), vec![0u8; 5]);
    }
}
