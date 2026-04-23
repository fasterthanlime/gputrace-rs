use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use memchr::memmem;
use serde::Serialize;
use walkdir::WalkDir;

use crate::error::{Error, Result};
use crate::trace::TraceBundle;

pub const MAGIC_MTLB: &[u8; 4] = b"MTLB";
const MTLB_HEADER_LEN: usize = 48;
const FUNCTION_SCAN_LIMIT: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MTLBFileKind {
    DirectFile,
    EmbeddedCandidate,
}

#[derive(Debug, Clone, Serialize)]
pub struct MTLBHeaderReport {
    pub magic: String,
    pub version: u32,
    pub flags: u32,
    pub reserved: u32,
    pub total_size: u64,
    pub function_table_offset: u64,
    pub string_table_offset: u64,
    pub bytecode_offset: u64,
    pub header_len: usize,
    pub size_matches_header: bool,
    pub offsets_within_file: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct MTLBFileReport {
    pub path: PathBuf,
    pub file_name: String,
    pub kind: MTLBFileKind,
    pub file_size: u64,
    pub header: MTLBHeaderReport,
    pub best_effort_function_count: usize,
    pub best_effort_function_names: Vec<String>,
    pub magic_offsets: Vec<u64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MTLBEmbeddedCandidateReport {
    pub path: PathBuf,
    pub file_name: String,
    pub file_size: u64,
    pub magic_offsets: Vec<u64>,
    pub offset_count: usize,
    pub in_capture_file: bool,
    pub in_device_resource: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct MTLBScanError {
    pub path: PathBuf,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MTLBBundleReport {
    pub bundle_path: PathBuf,
    pub scanned_files: usize,
    pub scanned_bytes: u64,
    pub direct_files: Vec<MTLBFileReport>,
    pub embedded_candidates: Vec<MTLBEmbeddedCandidateReport>,
    pub scan_errors: Vec<MTLBScanError>,
}

pub fn inspect_file(path: impl AsRef<Path>) -> Result<MTLBFileReport> {
    let path = path.as_ref().to_path_buf();
    let data = fs::read(&path)?;
    inspect_bytes(path, data, MTLBFileKind::DirectFile)
}

pub fn scan_bundle(bundle_path: impl AsRef<Path>) -> Result<MTLBBundleReport> {
    let bundle_path = bundle_path.as_ref().to_path_buf();
    let metadata = fs::metadata(&bundle_path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => Error::NotFound(bundle_path.clone()),
        _ => Error::Io(error),
    })?;
    if !metadata.is_dir() {
        return Err(Error::NotDirectory(bundle_path));
    }

    let trace_bundle = TraceBundle::open(&bundle_path).ok();
    let capture_path = trace_bundle
        .as_ref()
        .map(|trace| trace.capture_path.clone());
    let device_resources: BTreeSet<_> = trace_bundle
        .as_ref()
        .map(|trace| {
            trace
                .device_resources
                .iter()
                .map(|resource| resource.path.clone())
                .collect()
        })
        .unwrap_or_default();

    let mut report = MTLBBundleReport {
        bundle_path: bundle_path.clone(),
        scanned_files: 0,
        scanned_bytes: 0,
        direct_files: Vec::new(),
        embedded_candidates: Vec::new(),
        scan_errors: Vec::new(),
    };

    for entry in WalkDir::new(&bundle_path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
    {
        let path = entry.path().to_path_buf();
        report.scanned_files += 1;
        let file_size = entry.metadata().map(|meta| meta.len()).unwrap_or_default();
        report.scanned_bytes += file_size;

        let data = match fs::read(&path) {
            Ok(data) => data,
            Err(error) => {
                report.scan_errors.push(MTLBScanError {
                    path,
                    message: error.to_string(),
                });
                continue;
            }
        };

        let magic_offsets = find_magic_offsets(&data);
        if magic_offsets.is_empty() {
            continue;
        }

        if magic_offsets.first() == Some(&0) {
            match inspect_bytes(path.clone(), data, MTLBFileKind::DirectFile) {
                Ok(file_report) => report.direct_files.push(file_report),
                Err(error) => report.scan_errors.push(MTLBScanError {
                    path,
                    message: error.to_string(),
                }),
            }
            continue;
        }

        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
            .to_owned();
        report
            .embedded_candidates
            .push(MTLBEmbeddedCandidateReport {
                path: path.clone(),
                file_name,
                file_size,
                offset_count: magic_offsets.len(),
                magic_offsets,
                in_capture_file: capture_path
                    .as_ref()
                    .is_some_and(|capture| capture == &path),
                in_device_resource: device_resources.contains(&path),
            });
    }

    report
        .direct_files
        .sort_by(|left, right| left.path.cmp(&right.path));
    report
        .embedded_candidates
        .sort_by(|left, right| left.path.cmp(&right.path));
    report
        .scan_errors
        .sort_by(|left, right| left.path.cmp(&right.path));

    Ok(report)
}

pub fn format_file_report(report: &MTLBFileReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("File: {}\n", report.path.display()));
    out.push_str(&format!("Kind: {:?}\n", report.kind));
    out.push_str(&format!("Size: {} bytes\n", report.file_size));
    out.push_str("Header:\n");
    out.push_str(&format!("  Magic:          {}\n", report.header.magic));
    out.push_str(&format!("  Version:        {}\n", report.header.version));
    out.push_str(&format!("  Flags:          0x{:x}\n", report.header.flags));
    out.push_str(&format!(
        "  Reserved:       0x{:x}\n",
        report.header.reserved
    ));
    out.push_str(&format!(
        "  Total Size:     {} bytes{}\n",
        report.header.total_size,
        if report.header.size_matches_header {
            ""
        } else {
            " (header mismatch)"
        }
    ));
    out.push_str(&format!(
        "  Function Table: 0x{:x}\n",
        report.header.function_table_offset
    ));
    out.push_str(&format!(
        "  String Table:   0x{:x}\n",
        report.header.string_table_offset
    ));
    out.push_str(&format!(
        "  Bytecode:       0x{:x}\n",
        report.header.bytecode_offset
    ));
    out.push_str(&format!(
        "  Offsets Valid:  {}\n",
        if report.header.offsets_within_file {
            "yes"
        } else {
            "no"
        }
    ));

    out.push_str(&format!(
        "\nBest-effort Functions: {}\n",
        report.best_effort_function_count
    ));
    for name in report.best_effort_function_names.iter().take(20) {
        out.push_str(&format!("  - {name}\n"));
    }
    if report.best_effort_function_names.len() > 20 {
        out.push_str(&format!(
            "  ... {} more\n",
            report.best_effort_function_names.len() - 20
        ));
    }

    if !report.magic_offsets.is_empty() {
        out.push_str("\nMagic Offsets:\n");
        for offset in &report.magic_offsets {
            out.push_str(&format!("  - 0x{offset:x}\n"));
        }
    }

    if !report.warnings.is_empty() {
        out.push_str("\nWarnings:\n");
        for warning in &report.warnings {
            out.push_str(&format!("  - {warning}\n"));
        }
    }

    out
}

pub fn format_bundle_report(report: &MTLBBundleReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("Bundle: {}\n", report.bundle_path.display()));
    out.push_str(&format!("Scanned Files: {}\n", report.scanned_files));
    out.push_str(&format!("Scanned Bytes: {}\n", report.scanned_bytes));
    out.push_str(&format!(
        "Direct MTLB Files: {}\n",
        report.direct_files.len()
    ));
    out.push_str(&format!(
        "Embedded MTLB Candidates: {}\n",
        report.embedded_candidates.len()
    ));
    out.push_str(&format!("Scan Errors: {}\n", report.scan_errors.len()));

    if !report.direct_files.is_empty() {
        out.push_str("\nDirect Files:\n");
        for file in &report.direct_files {
            out.push_str(&format!(
                "  - {} ({} bytes, {} functions)\n",
                file.path.display(),
                file.file_size,
                file.best_effort_function_count
            ));
        }
    }

    if !report.embedded_candidates.is_empty() {
        out.push_str("\nEmbedded Candidates:\n");
        for candidate in &report.embedded_candidates {
            let location = if candidate.in_capture_file {
                "capture"
            } else if candidate.in_device_resource {
                "device-resource"
            } else {
                "other"
            };
            out.push_str(&format!(
                "  - {} ({} hit(s), first at 0x{:x}, {})\n",
                candidate.path.display(),
                candidate.offset_count,
                candidate.magic_offsets.first().copied().unwrap_or_default(),
                location
            ));
        }
    }

    if !report.scan_errors.is_empty() {
        out.push_str("\nScan Errors:\n");
        for error in &report.scan_errors {
            out.push_str(&format!(
                "  - {}: {}\n",
                error.path.display(),
                error.message
            ));
        }
    }

    out
}

fn inspect_bytes(path: PathBuf, data: Vec<u8>, kind: MTLBFileKind) -> Result<MTLBFileReport> {
    if data.len() < MTLB_HEADER_LEN {
        return Err(Error::InvalidInput(format!(
            "data too short for MTLB header: {} bytes",
            data.len()
        )));
    }
    if &data[..4] != MAGIC_MTLB {
        return Err(Error::InvalidInput(format!(
            "invalid MTLB file (magic bytes mismatch): {}",
            path.display()
        )));
    }

    let file_size = data.len() as u64;
    let header = parse_header(&data, file_size);
    let magic_offsets = find_magic_offsets(&data);
    let mut warnings = header_warnings(&header, file_size);
    let best_effort_function_names = scan_function_names(&data, &header, &mut warnings);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown")
        .to_owned();

    Ok(MTLBFileReport {
        path,
        file_name,
        kind,
        file_size,
        header,
        best_effort_function_count: best_effort_function_names.len(),
        best_effort_function_names,
        magic_offsets,
        warnings,
    })
}

fn parse_header(data: &[u8], file_size: u64) -> MTLBHeaderReport {
    let magic = String::from_utf8_lossy(&data[..4]).into_owned();
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let flags = u32::from_le_bytes(data[8..12].try_into().unwrap());
    let reserved = u32::from_le_bytes(data[12..16].try_into().unwrap());
    let total_size = u64::from_le_bytes(data[16..24].try_into().unwrap());
    let function_table_offset = u64::from_le_bytes(data[24..32].try_into().unwrap());
    let string_table_offset = u64::from_le_bytes(data[32..40].try_into().unwrap());
    let bytecode_offset = u64::from_le_bytes(data[40..48].try_into().unwrap());

    let offsets_within_file = function_table_offset <= file_size
        && string_table_offset <= file_size
        && bytecode_offset <= file_size;

    MTLBHeaderReport {
        magic,
        version,
        flags,
        reserved,
        total_size,
        function_table_offset,
        string_table_offset,
        bytecode_offset,
        header_len: MTLB_HEADER_LEN,
        size_matches_header: total_size == file_size,
        offsets_within_file,
    }
}

fn header_warnings(header: &MTLBHeaderReport, file_size: u64) -> Vec<String> {
    let mut warnings = Vec::new();
    if !header.size_matches_header {
        warnings.push(format!(
            "header total size {} does not match file size {}",
            header.total_size, file_size
        ));
    }
    if !header.offsets_within_file {
        warnings.push("one or more header offsets point beyond the file".to_owned());
    }
    if header.function_table_offset < header.header_len as u64 {
        warnings.push("function table starts inside the header".to_owned());
    }
    if header.string_table_offset < header.function_table_offset {
        warnings.push("string table offset precedes function table offset".to_owned());
    }
    if header.bytecode_offset < header.string_table_offset {
        warnings.push("bytecode offset precedes string table offset".to_owned());
    }
    warnings
}

fn scan_function_names(
    data: &[u8],
    header: &MTLBHeaderReport,
    warnings: &mut Vec<String>,
) -> Vec<String> {
    let start = header.function_table_offset as usize;
    if start >= data.len() {
        warnings.push("cannot scan functions: function table offset is out of range".to_owned());
        return Vec::new();
    }

    let end = match header.bytecode_offset as usize {
        0 => data.len(),
        value if value > start && value <= data.len() => value,
        _ => data.len(),
    };
    let haystack = &data[start..end];
    let tags = [b"NAMED\0".as_slice(), b"NAME;\0".as_slice()];
    let mut names = Vec::new();
    let mut seen = BTreeSet::new();
    let mut cursor = 0;

    while cursor < haystack.len() && names.len() < FUNCTION_SCAN_LIMIT {
        let mut best_match = None;
        for tag in tags {
            if let Some(relative) = memmem::find(&haystack[cursor..], tag) {
                let absolute = cursor + relative;
                if best_match.is_none_or(|(best, _)| absolute < best) {
                    best_match = Some((absolute, tag.len()));
                }
            }
        }

        let Some((tag_pos, tag_len)) = best_match else {
            break;
        };
        let name_start = tag_pos + tag_len;
        let Some(name_len) = haystack[name_start..].iter().position(|byte| *byte == 0) else {
            break;
        };
        let raw_name = &haystack[name_start..name_start + name_len];
        cursor = name_start + name_len + 1;

        if raw_name.is_empty() || !is_plausible_function_name(raw_name) {
            continue;
        }

        let name = String::from_utf8_lossy(raw_name).into_owned();
        if seen.insert(name.clone()) {
            names.push(name);
        }
    }

    if names.len() == FUNCTION_SCAN_LIMIT {
        warnings.push(format!(
            "best-effort function scan hit the {} name cap",
            FUNCTION_SCAN_LIMIT
        ));
    }

    names
}

fn is_plausible_function_name(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| {
        matches!(
            byte,
            b'a'..=b'z'
                | b'A'..=b'Z'
                | b'0'..=b'9'
                | b'_'
                | b':'
                | b'.'
                | b'$'
                | b'['
                | b']'
                | b'<'
                | b'>'
                | b'('
                | b')'
                | b','
                | b' '
                | b'-'
        )
    })
}

fn find_magic_offsets(data: &[u8]) -> Vec<u64> {
    memmem::find_iter(data, MAGIC_MTLB)
        .map(|offset| offset as u64)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    fn sample_mtlb(
        function_names: &[&str],
        total_size: u64,
        function_table_offset: u64,
        string_table_offset: u64,
        bytecode_offset: u64,
    ) -> Vec<u8> {
        let mut data = vec![0_u8; total_size as usize];
        data[0..4].copy_from_slice(MAGIC_MTLB);
        data[4..8].copy_from_slice(&7_u32.to_le_bytes());
        data[8..12].copy_from_slice(&0x10_u32.to_le_bytes());
        data[12..16].copy_from_slice(&0_u32.to_le_bytes());
        data[16..24].copy_from_slice(&total_size.to_le_bytes());
        data[24..32].copy_from_slice(&function_table_offset.to_le_bytes());
        data[32..40].copy_from_slice(&string_table_offset.to_le_bytes());
        data[40..48].copy_from_slice(&bytecode_offset.to_le_bytes());

        let mut cursor = function_table_offset as usize;
        for name in function_names {
            data[cursor..cursor + 6].copy_from_slice(b"NAMED\0");
            cursor += 6;
            let bytes = name.as_bytes();
            data[cursor..cursor + bytes.len()].copy_from_slice(bytes);
            cursor += bytes.len();
            data[cursor] = 0;
            cursor += 1;
        }

        data
    }

    #[test]
    fn inspect_file_parses_header_and_functions() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("library.mtlb");
        let data = sample_mtlb(&["kernel_main", "helper_fn"], 192, 48, 96, 160);
        fs::write(&path, data).unwrap();

        let report = inspect_file(&path).unwrap();

        assert_eq!(report.file_name, "library.mtlb");
        assert_eq!(report.file_size, 192);
        assert_eq!(report.header.version, 7);
        assert_eq!(report.header.flags, 0x10);
        assert_eq!(report.best_effort_function_count, 2);
        assert_eq!(
            report.best_effort_function_names,
            vec!["kernel_main".to_owned(), "helper_fn".to_owned()]
        );
        assert_eq!(report.magic_offsets, vec![0]);
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn inspect_file_rejects_non_mtlb_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("not-a-library.bin");
        fs::write(&path, b"NOTM").unwrap();

        let error = inspect_file(&path).unwrap_err();
        assert!(matches!(error, Error::InvalidInput(_)));
    }

    #[test]
    fn scan_bundle_finds_direct_files_and_embedded_candidates() {
        let dir = tempdir().unwrap();
        let bundle = dir.path().join("sample.gputrace");
        fs::create_dir(&bundle).unwrap();

        let direct_path = bundle.join("sidecar.mtlb");
        fs::write(&direct_path, sample_mtlb(&["a"], 128, 48, 72, 96)).unwrap();

        let embedded_path = bundle.join("capture");
        let mut embedded = vec![0_u8; 256];
        embedded[64..68].copy_from_slice(MAGIC_MTLB);
        embedded[192..196].copy_from_slice(MAGIC_MTLB);
        fs::write(&embedded_path, embedded).unwrap();

        let report = scan_bundle(&bundle).unwrap();

        assert_eq!(report.scanned_files, 2);
        assert_eq!(report.direct_files.len(), 1);
        assert_eq!(report.embedded_candidates.len(), 1);
        assert_eq!(report.direct_files[0].path, direct_path);
        assert_eq!(report.embedded_candidates[0].path, embedded_path);
        assert_eq!(report.embedded_candidates[0].magic_offsets, vec![64, 192]);
    }

    #[test]
    fn inspect_file_reports_header_mismatch_warning() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mismatch.mtlb");
        let mut data = sample_mtlb(&["kernel_main"], 192, 48, 96, 160);
        data[16..24].copy_from_slice(&200_u64.to_le_bytes());
        fs::write(&path, data).unwrap();

        let report = inspect_file(&path).unwrap();

        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("does not match file size"))
        );
    }

    #[test]
    fn formatters_surface_inventory() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("library.mtlb");
        fs::write(&path, sample_mtlb(&["kernel_main"], 160, 48, 96, 128)).unwrap();

        let file_report = inspect_file(&path).unwrap();
        let bundle_report = MTLBBundleReport {
            bundle_path: dir.path().to_path_buf(),
            scanned_files: 1,
            scanned_bytes: 160,
            direct_files: vec![file_report.clone()],
            embedded_candidates: Vec::new(),
            scan_errors: Vec::new(),
        };

        let file_text = format_file_report(&file_report);
        let bundle_text = format_bundle_report(&bundle_report);

        assert!(file_text.contains("Best-effort Functions: 1"));
        assert!(file_text.contains("kernel_main"));
        assert!(bundle_text.contains("Direct MTLB Files: 1"));
        assert!(bundle_text.contains("library.mtlb"));
    }
}
