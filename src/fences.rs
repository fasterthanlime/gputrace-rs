use std::collections::BTreeMap;
use std::fs;

use serde::Serialize;

use crate::error::Result;
use crate::trace::{MTSPRecord, RecordType, TraceBundle};

pub const LEGACY_INFERRED_FENCE_ADDR: u64 = 0x9df0ec000;
const CULUL_MARKER: &[u8] = b"Culul\0\0\0";

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FenceOpKind {
    WaitLike,
    UpdateLike,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum FenceConfidence {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FenceLabelSource {
    DeviceLabel,
    LegacyInferredAddress,
}

#[derive(Debug, Clone, Serialize)]
pub struct FenceOperation {
    pub sequence: usize,
    pub offset: usize,
    pub marker_offset: usize,
    pub record_size: u32,
    pub icb_addr: u64,
    pub label: Option<String>,
    pub label_source: Option<FenceLabelSource>,
    pub op_kind: FenceOpKind,
    pub confidence: FenceConfidence,
    pub marker_count: u32,
    pub field_1: u32,
    pub field_2: u32,
    pub field_3: u32,
    pub structured_icb_addr: Option<u64>,
    pub structured_field_1: Option<u32>,
    pub structured_field_2: Option<u32>,
    pub structured_field_3: Option<u32>,
    pub payload_size: u32,
    pub payload_addr: u64,
    pub array_count: u32,
    pub array_stride: u32,
    pub array_addresses: Vec<u64>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FencesReport {
    pub synthetic: bool,
    pub heuristic: bool,
    pub total_records: usize,
    pub culul_records: usize,
    pub labeled_fence_candidates: usize,
    pub unlabeled_pattern_candidates: usize,
    pub total_operations: usize,
    pub legacy_inferred_fence_addr: u64,
    pub notes: Vec<String>,
    pub operations: Vec<FenceOperation>,
}

pub fn report(trace: &TraceBundle) -> Result<FencesReport> {
    let capture_records = trace.mtsp_records()?;
    let mut device_resource_records = Vec::new();
    for resource in &trace.device_resources {
        let data = fs::read(&resource.path)?;
        device_resource_records.push(MTSPRecord::parse_stream(&data)?);
    }
    Ok(build_report(&capture_records, &device_resource_records))
}

pub fn build_report(
    capture_records: &[MTSPRecord],
    device_resource_records: &[Vec<MTSPRecord>],
) -> FencesReport {
    let label_map = collect_device_labels(capture_records, device_resource_records);

    let mut operations = Vec::new();
    let mut culul_records = 0usize;
    let mut labeled_fence_candidates = 0usize;
    let mut unlabeled_pattern_candidates = 0usize;

    for record in capture_records {
        if record.record_type != RecordType::Culul {
            continue;
        }
        culul_records += 1;

        let Some(go) = parse_go_fence_fields(record) else {
            continue;
        };

        let parsed = record.parse_culul_structured().ok();
        let label = label_map
            .get(&go.icb_addr)
            .cloned()
            .or_else(|| go.has_legacy_addr.then(|| "fences (inferred)".to_owned()));
        let has_exact_go_label = label
            .as_deref()
            .is_some_and(|value| value == "fences" || value == "fences (inferred)");
        let mentions_fence = label
            .as_deref()
            .is_some_and(|value| label_mentions_fence(value) && !has_exact_go_label);
        let op_kind = classify_op_kind(go.field_1);
        let has_wait_update_shape = op_kind != FenceOpKind::Unknown;

        if has_exact_go_label || go.has_legacy_addr {
            labeled_fence_candidates += 1;

            let mut evidence = Vec::new();
            let (label_source, confidence) = if has_exact_go_label && !go.has_legacy_addr {
                evidence.push("matched Go fence rule: device label is exactly 'fences'".to_owned());
                (Some(FenceLabelSource::DeviceLabel), FenceConfidence::High)
            } else {
                evidence.push("matched Go fence rule: legacy inferred fence address".to_owned());
                (
                    Some(FenceLabelSource::LegacyInferredAddress),
                    FenceConfidence::Low,
                )
            };

            if has_wait_update_shape {
                evidence.push(format!(
                    "Go field1=0x{:x} matches {}",
                    go.field_1,
                    format_op_type(op_kind)
                ));
            } else {
                evidence.push(format!(
                    "Go field1=0x{:x} did not match the Go wait/update constants",
                    go.field_1
                ));
            }
            if let Some(parsed) = parsed.as_ref() {
                evidence.push(format!(
                    "structured decode: icb=0x{:x} f1=0x{:x} f2=0x{:x} f3=0x{:x}",
                    parsed.icb_addr, parsed.field_1, parsed.field_2, parsed.field_3
                ));
            }

            operations.push(FenceOperation {
                sequence: operations.len(),
                offset: go.absolute_marker_offset,
                marker_offset: go.marker_offset,
                record_size: parsed
                    .as_ref()
                    .map_or(record.size as u32, |value| value.record_size),
                icb_addr: go.icb_addr,
                label,
                label_source,
                op_kind,
                confidence,
                marker_count: parsed.as_ref().map_or(0, |value| value.marker_count),
                field_1: go.field_1,
                field_2: go.field_2,
                field_3: parsed.as_ref().map_or(0, |value| value.field_3),
                structured_icb_addr: parsed.as_ref().map(|value| value.icb_addr),
                structured_field_1: parsed.as_ref().map(|value| value.field_1),
                structured_field_2: parsed.as_ref().map(|value| value.field_2),
                structured_field_3: parsed.as_ref().map(|value| value.field_3),
                payload_size: parsed.as_ref().map_or(0, |value| value.payload_size),
                payload_addr: parsed.as_ref().map_or(0, |value| value.payload_addr),
                array_count: parsed.as_ref().map_or(0, |value| value.array_count),
                array_stride: parsed.as_ref().map_or(0, |value| value.array_stride),
                array_addresses: parsed.map_or_else(Vec::new, |value| value.array_addresses),
                evidence,
            });
        } else if mentions_fence || has_wait_update_shape {
            unlabeled_pattern_candidates += 1;
        }
    }

    let mut notes = vec![
        "This report uses the Go command's Culul marker offsets and exact fence label/address rules.".to_owned(),
        "Metal fence/shared-event semantics are still not fully decoded; op types preserve the Go field1 constants.".to_owned(),
    ];
    if unlabeled_pattern_candidates > 0 {
        notes.push(format!(
            "{unlabeled_pattern_candidates} unlabeled Culul record(s) looked wait/update-like but were excluded to avoid overstating certainty."
        ));
    }
    if operations.is_empty() {
        notes
            .push("No labeled fence operations were found with the current heuristics.".to_owned());
    }

    FencesReport {
        synthetic: true,
        heuristic: true,
        total_records: capture_records.len(),
        culul_records,
        labeled_fence_candidates,
        unlabeled_pattern_candidates,
        total_operations: operations.len(),
        legacy_inferred_fence_addr: LEGACY_INFERRED_FENCE_ADDR,
        notes,
        operations,
    }
}

pub fn format_report(report: &FencesReport) -> String {
    let mut out = String::new();
    out.push_str("Fence report (Go-compatible scan)\n");
    out.push_str(
        "Derived from raw Culul marker offsets plus CS label extraction; this matches the Go fence command's primary fields.\n\n",
    );
    out.push_str(&format!(
        "records={} culul={} labeled_candidates={} unlabeled_pattern_candidates={} reported_ops={}\n",
        report.total_records,
        report.culul_records,
        report.labeled_fence_candidates,
        report.unlabeled_pattern_candidates,
        report.total_operations
    ));
    out.push_str(&format!(
        "legacy_inferred_fence_addr=0x{:x}\n\n",
        report.legacy_inferred_fence_addr
    ));

    for note in &report.notes {
        out.push_str(&format!("note: {note}\n"));
    }
    if !report.notes.is_empty() {
        out.push('\n');
    }

    if report.operations.is_empty() {
        out.push_str("No fence operations matched the current heuristics.\n");
        return out;
    }

    out.push_str(&format!(
        "{:<4} {:<10} {:<18} {:<12} {:<8} {:<20} {}\n",
        "#", "Offset", "ICB", "Kind", "Conf", "Label", "Fields"
    ));
    for op in &report.operations {
        out.push_str(&format!(
            "{:<4} 0x{:<8x} 0x{:<16x} {:<12} {:<8} {:<20} f1=0x{:x} f2=0x{:x} f3=0x{:x}\n",
            op.sequence,
            op.offset,
            op.icb_addr,
            format_op_kind(op.op_kind),
            format_confidence(op.confidence),
            truncate(op.label.as_deref().unwrap_or("unknown"), 20),
            op.field_1,
            op.field_2,
            op.field_3
        ));
        if !op.evidence.is_empty() {
            out.push_str(&format!("     evidence: {}\n", op.evidence.join("; ")));
        }
    }

    out
}

fn collect_device_labels(
    capture_records: &[MTSPRecord],
    device_resource_records: &[Vec<MTSPRecord>],
) -> BTreeMap<u64, String> {
    let mut labels = BTreeMap::new();
    for record in capture_records
        .iter()
        .chain(device_resource_records.iter().flatten())
    {
        match record.record_type {
            RecordType::CS | RecordType::CSuwuw => {
                if let (Some(address), Some(label)) = (record.address, record.label.as_ref()) {
                    labels.insert(address, label.clone());
                }
            }
            _ => {}
        }
    }
    labels
}

fn label_mentions_fence(label: &str) -> bool {
    label.to_ascii_lowercase().contains("fence")
}

#[derive(Debug, Clone, Copy)]
struct GoFenceFields {
    absolute_marker_offset: usize,
    marker_offset: usize,
    icb_addr: u64,
    field_1: u32,
    field_2: u32,
    has_legacy_addr: bool,
}

fn parse_go_fence_fields(record: &MTSPRecord) -> Option<GoFenceFields> {
    let marker_offset = find_bytes(&record.data, CULUL_MARKER)?;
    let icb_addr = read_u64(&record.data, marker_offset + 8)?;
    let field_1 = read_u32(&record.data, marker_offset + 16)?;
    let field_2 = read_u32(&record.data, marker_offset + 20)?;
    Some(GoFenceFields {
        absolute_marker_offset: record.offset + marker_offset,
        marker_offset,
        icb_addr,
        field_1,
        field_2,
        has_legacy_addr: icb_addr == LEGACY_INFERRED_FENCE_ADDR,
    })
}

fn classify_op_kind(field_1: u32) -> FenceOpKind {
    match field_1 {
        0x800 => FenceOpKind::WaitLike,
        0x80000 => FenceOpKind::UpdateLike,
        _ => FenceOpKind::Unknown,
    }
}

fn format_op_type(kind: FenceOpKind) -> &'static str {
    match kind {
        FenceOpKind::WaitLike => "Wait?",
        FenceOpKind::UpdateLike => "Update?",
        FenceOpKind::Unknown => "Unknown",
    }
}

fn format_op_kind(kind: FenceOpKind) -> &'static str {
    match kind {
        FenceOpKind::WaitLike => "wait-like",
        FenceOpKind::UpdateLike => "update-like",
        FenceOpKind::Unknown => "unknown",
    }
}

fn format_confidence(confidence: FenceConfidence) -> &'static str {
    match confidence {
        FenceConfidence::High => "high",
        FenceConfidence::Medium => "medium",
        FenceConfidence::Low => "low",
    }
}

fn truncate(value: &str, max_len: usize) -> String {
    if value.chars().count() <= max_len {
        return value.to_owned();
    }
    value
        .chars()
        .take(max_len.saturating_sub(3))
        .collect::<String>()
        + "..."
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        data.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::MTSPRecord;

    fn make_cs_record(address: u64, label: &str) -> MTSPRecord {
        let size = 16 + label.len() + 1;
        let mut bytes = vec![0u8; size];
        bytes[0..4].copy_from_slice(&(size as u32).to_le_bytes());
        bytes[4..8].copy_from_slice(b"CS\0\0");
        bytes[8..16].copy_from_slice(&address.to_le_bytes());
        bytes[16..16 + label.len()].copy_from_slice(label.as_bytes());
        MTSPRecord::parse_stream(&bytes).unwrap().remove(0)
    }

    fn make_culul_record(icb_addr: u64, field_1: u32, field_2: u32, field_3: u32) -> MTSPRecord {
        let size = 0x58usize;
        let mut bytes = vec![0u8; size];
        bytes[0..4].copy_from_slice(&(size as u32).to_le_bytes());
        bytes[8..13].copy_from_slice(b"Culul");
        bytes[0x10..0x18].copy_from_slice(&icb_addr.to_le_bytes());
        bytes[0x18..0x1c].copy_from_slice(&field_1.to_le_bytes());
        bytes[0x1c..0x20].copy_from_slice(&field_2.to_le_bytes());
        bytes[0x20..0x24].copy_from_slice(&1u32.to_le_bytes());
        bytes[0x28..0x30].copy_from_slice(&icb_addr.to_le_bytes());
        bytes[0x30..0x34].copy_from_slice(&field_1.to_le_bytes());
        bytes[0x34..0x38].copy_from_slice(&field_2.to_le_bytes());
        bytes[0x38..0x3c].copy_from_slice(&field_3.to_le_bytes());
        bytes[0x40..0x44].copy_from_slice(&16u32.to_le_bytes());
        bytes[0x48..0x50].copy_from_slice(&0xfeed_cafeu64.to_le_bytes());
        bytes[0x50..0x54].copy_from_slice(&0u32.to_le_bytes());
        bytes[0x54..0x58].copy_from_slice(&8u32.to_le_bytes());
        MTSPRecord::parse_stream(&bytes).unwrap().remove(0)
    }

    #[test]
    fn build_report_includes_labeled_fence_records() {
        let capture_records = vec![make_culul_record(0x1234, 0x800, 0x11, 0x22)];
        let device_records = vec![vec![make_cs_record(0x1234, "fences")]];

        let report = build_report(&capture_records, &device_records);

        assert_eq!(report.total_operations, 1);
        let op = &report.operations[0];
        assert_eq!(op.label.as_deref(), Some("fences"));
        assert_eq!(op.op_kind, FenceOpKind::WaitLike);
        assert_eq!(op.confidence, FenceConfidence::High);
        assert_eq!(op.label_source, Some(FenceLabelSource::DeviceLabel));
        assert_eq!(op.offset, 0x8);
        assert_eq!(op.marker_offset, 0x8);
        assert_eq!(op.field_1, 0x800);
        assert_eq!(op.field_2, 0x11);
        assert_eq!(op.structured_field_1, Some(0x800));
    }

    #[test]
    fn build_report_keeps_legacy_inferred_address_support() {
        let capture_records = vec![make_culul_record(LEGACY_INFERRED_FENCE_ADDR, 0x80000, 0, 0)];

        let report = build_report(&capture_records, &[]);

        assert_eq!(report.total_operations, 1);
        let op = &report.operations[0];
        assert_eq!(op.op_kind, FenceOpKind::UpdateLike);
        assert_eq!(op.confidence, FenceConfidence::Low);
        assert_eq!(
            op.label_source,
            Some(FenceLabelSource::LegacyInferredAddress)
        );
    }

    #[test]
    fn build_report_counts_unlabeled_pattern_matches_without_reporting_them() {
        let capture_records = vec![make_culul_record(0xbeef, 0x800, 0, 0)];

        let report = build_report(&capture_records, &[]);

        assert_eq!(report.total_operations, 0);
        assert_eq!(report.unlabeled_pattern_candidates, 1);
    }

    #[test]
    fn format_report_explains_heuristics() {
        let capture_records = vec![make_culul_record(0x1234, 0x800, 0x11, 0x22)];
        let device_records = vec![vec![make_cs_record(0x1234, "fences")]];
        let report = build_report(&capture_records, &device_records);

        let rendered = format_report(&report);

        assert!(rendered.contains("Fence report (Go-compatible scan)"));
        assert!(rendered.contains("wait-like"));
        assert!(rendered.contains("matched Go fence rule"));
    }
}
