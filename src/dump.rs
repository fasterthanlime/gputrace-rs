use std::fmt::Write;

use serde::Serialize;

use crate::error::Result;
use crate::trace::{
    CDispatchRecord, CRecord, CiRecord, CtRecord, CtURecord, CttRecord, CulRecord, CululRecord,
    CuwRecord, MTSPRecord, RecordType,
};

pub const DEFAULT_HEX_PREVIEW_BYTES: usize = 32;

#[derive(Debug, Clone, Default, Serialize)]
pub struct DumpFilter {
    pub record_type: Option<RecordType>,
    pub text_contains: Option<String>,
    pub start_index: usize,
    pub limit: Option<usize>,
    pub include_hex_preview: bool,
    pub max_preview_bytes: usize,
}

impl DumpFilter {
    pub fn with_type(record_type: RecordType) -> Self {
        Self {
            record_type: Some(record_type),
            ..Self::default()
        }
    }

    fn normalized(self) -> Self {
        Self {
            max_preview_bytes: if self.max_preview_bytes == 0 {
                DEFAULT_HEX_PREVIEW_BYTES
            } else {
                self.max_preview_bytes
            },
            ..self
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RecordTypeCount {
    pub record_type: RecordType,
    pub count: usize,
    pub total_bytes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecordDumpSummary {
    pub total_records: usize,
    pub total_bytes: usize,
    pub shown_records: usize,
    pub shown_bytes: usize,
    pub counts_by_type: Vec<RecordTypeCount>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "fields")]
pub enum ParsedRecordFields {
    Ct(CtRecord),
    Ctt(CttRecord),
    Ci(CiRecord),
    Cul(CulRecord),
    Cuw(CuwRecord),
    CtU(CtURecord),
    Culul(CululRecord),
    C(CRecord),
    Dispatch(CDispatchRecord),
}

#[derive(Debug, Clone, Serialize)]
pub struct RecordDumpEntry {
    pub index: usize,
    pub record_type: RecordType,
    pub offset: usize,
    pub size: usize,
    pub label: Option<String>,
    pub address: Option<u64>,
    pub function_address: Option<u64>,
    pub parsed: Option<ParsedRecordFields>,
    pub hex_preview: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecordDumpReport {
    pub filter: DumpFilter,
    pub summary: RecordDumpSummary,
    pub records: Vec<RecordDumpEntry>,
}

pub fn summarize_records(records: &[MTSPRecord]) -> RecordDumpSummary {
    summarize_with_slice(records, records)
}

pub fn build_record_dump(records: &[MTSPRecord], filter: DumpFilter) -> RecordDumpReport {
    let filter = filter.normalized();
    let filtered = records
        .iter()
        .enumerate()
        .skip(filter.start_index)
        .filter(|(_, record)| matches_filter(record, &filter))
        .take(filter.limit.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();

    let shown_records = filtered
        .into_iter()
        .map(|(index, record)| RecordDumpEntry {
            index,
            record_type: record.record_type,
            offset: record.offset,
            size: record.size,
            label: record.label.clone(),
            address: record.address,
            function_address: record.function_address,
            parsed: parse_record_fields(record),
            hex_preview: filter
                .include_hex_preview
                .then(|| hex_preview(record, filter.max_preview_bytes)),
        })
        .collect::<Vec<_>>();

    RecordDumpReport {
        summary: summarize_with_entries(records, &shown_records),
        filter,
        records: shown_records,
    }
}

pub fn parse_record_dump(data: &[u8], filter: DumpFilter) -> Result<RecordDumpReport> {
    let records = MTSPRecord::parse_stream(data)?;
    Ok(build_record_dump(&records, filter))
}

pub fn format_record_counts(summary: &RecordDumpSummary) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "records: {} shown / {} total",
        summary.shown_records, summary.total_records
    );
    let _ = writeln!(
        output,
        "bytes:   {} shown / {} total",
        summary.shown_bytes, summary.total_bytes
    );
    let _ = writeln!(output);
    let _ = writeln!(output, "type         count   bytes");
    let _ = writeln!(output, "--------------------------");
    for count in &summary.counts_by_type {
        let _ = writeln!(
            output,
            "{:<12} {:>5} {:>7}",
            count.record_type, count.count, count.total_bytes
        );
    }
    output
}

pub fn format_record_listing(report: &RecordDumpReport) -> String {
    let mut output = String::new();
    if let Some(record_type) = report.filter.record_type {
        let _ = writeln!(output, "filter: type={record_type}");
    }
    if let Some(text) = &report.filter.text_contains {
        let _ = writeln!(output, "filter: text contains {text:?}");
    }
    if report.filter.start_index > 0 {
        let _ = writeln!(output, "filter: start index={}", report.filter.start_index);
    }
    if let Some(limit) = report.filter.limit {
        let _ = writeln!(output, "filter: limit={limit}");
    }
    if !output.is_empty() {
        let _ = writeln!(output);
    }

    output.push_str(&format_record_counts(&report.summary));
    output.push('\n');

    for entry in &report.records {
        let _ = write!(
            output,
            "[{}] offset=0x{:x} type={} size={}",
            entry.index, entry.offset, entry.record_type, entry.size
        );
        if let Some(label) = &entry.label {
            let _ = write!(output, " label={label:?}");
        }
        if let Some(address) = entry.address {
            let _ = write!(output, " addr=0x{address:x}");
        }
        if let Some(function_address) = entry.function_address {
            let _ = write!(output, " func=0x{function_address:x}");
        }
        let _ = writeln!(output);

        if let Some(parsed) = &entry.parsed {
            let _ = writeln!(output, "  parsed: {}", format_parsed_fields(parsed));
        }
        if let Some(hex_preview) = &entry.hex_preview {
            let _ = writeln!(output, "  bytes: {hex_preview}");
        }
    }

    output
}

fn summarize_with_entries(
    all_records: &[MTSPRecord],
    shown_records: &[RecordDumpEntry],
) -> RecordDumpSummary {
    let total_bytes = all_records.iter().map(|record| record.size).sum();
    let shown_bytes = shown_records.iter().map(|record| record.size).sum();

    let mut counts =
        shown_records
            .iter()
            .fold(Vec::<RecordTypeCount>::new(), |mut counts, entry| {
                if let Some(existing) = counts
                    .iter_mut()
                    .find(|count| count.record_type == entry.record_type)
                {
                    existing.count += 1;
                    existing.total_bytes += entry.size;
                } else {
                    counts.push(RecordTypeCount {
                        record_type: entry.record_type,
                        count: 1,
                        total_bytes: entry.size,
                    });
                }
                counts
            });
    counts.sort_by_key(|count| count.record_type.to_string());

    RecordDumpSummary {
        total_records: all_records.len(),
        total_bytes,
        shown_records: shown_records.len(),
        shown_bytes,
        counts_by_type: counts,
    }
}

fn summarize_with_slice(
    all_records: &[MTSPRecord],
    shown_records: &[MTSPRecord],
) -> RecordDumpSummary {
    let entries = shown_records
        .iter()
        .enumerate()
        .map(|(index, record)| RecordDumpEntry {
            index,
            record_type: record.record_type,
            offset: record.offset,
            size: record.size,
            label: record.label.clone(),
            address: record.address,
            function_address: record.function_address,
            parsed: None,
            hex_preview: None,
        })
        .collect::<Vec<_>>();
    summarize_with_entries(all_records, &entries)
}

fn matches_filter(record: &MTSPRecord, filter: &DumpFilter) -> bool {
    if filter
        .record_type
        .is_some_and(|record_type| record.record_type != record_type)
    {
        return false;
    }

    if let Some(text) = &filter.text_contains {
        let needle = text.to_ascii_lowercase();
        let mut haystack = record.record_type.to_string().to_ascii_lowercase();
        if let Some(label) = &record.label {
            haystack.push(' ');
            haystack.push_str(&label.to_ascii_lowercase());
        }
        if let Some(address) = record.address {
            let _ = write!(haystack, " 0x{address:x}");
        }
        if let Some(function_address) = record.function_address {
            let _ = write!(haystack, " 0x{function_address:x}");
        }
        if !haystack.contains(&needle) {
            return false;
        }
    }

    true
}

fn parse_record_fields(record: &MTSPRecord) -> Option<ParsedRecordFields> {
    match record.record_type {
        RecordType::Ct => record.parse_ct_record().ok().map(ParsedRecordFields::Ct),
        RecordType::Ctt => record.parse_ctt_record().ok().map(ParsedRecordFields::Ctt),
        RecordType::Ci => record.parse_ci_record().ok().map(ParsedRecordFields::Ci),
        RecordType::Cul => record.parse_cul_record().ok().map(ParsedRecordFields::Cul),
        RecordType::Cuw => record.parse_cuw_record().ok().map(ParsedRecordFields::Cuw),
        RecordType::CtU => record.parse_ctu_record().ok().map(ParsedRecordFields::CtU),
        RecordType::Culul => record
            .parse_culul_structured()
            .ok()
            .map(ParsedRecordFields::Culul),
        RecordType::C => record.parse_c_record().ok().map(ParsedRecordFields::C),
        RecordType::C3ul => record
            .parse_dispatch_record()
            .ok()
            .map(ParsedRecordFields::Dispatch),
        _ => None,
    }
}

fn hex_preview(record: &MTSPRecord, max_bytes: usize) -> String {
    let preview_len = record.data.len().min(max_bytes);
    let mut output = record.data[..preview_len]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    if preview_len < record.data.len() {
        let _ = write!(output, " ... (+{} bytes)", record.data.len() - preview_len);
    }
    output
}

fn format_parsed_fields(parsed: &ParsedRecordFields) -> String {
    match parsed {
        ParsedRecordFields::Ct(record) => format!(
            "Ct pipeline=0x{:x} function=0x{:x} bindings={}",
            record.pipeline_addr, record.function_addr, record.binding_count
        ),
        ParsedRecordFields::Ctt(record) => format!(
            "Ctt device=0x{:x} function=0x{:x} pipeline=0x{:x} bindings={}",
            record.device_addr, record.function_addr, record.pipeline_addr, record.binding_count
        ),
        ParsedRecordFields::Ci(record) => format!(
            "Ci icb=0x{:x} count={} flags=0x{:x}",
            record.icb_addr, record.count, record.command_flags
        ),
        ParsedRecordFields::Cul(record) => format!(
            "Cul buffer=0x{:x} markers={} flags=0x{:x}",
            record.buffer_addr, record.marker_count, record.command_flags
        ),
        ParsedRecordFields::Cuw(record) => format!(
            "Cuw buffer=0x{:x} markers={} flags=0x{:x}",
            record.buffer_addr, record.marker_count, record.command_flags
        ),
        ParsedRecordFields::CtU(record) => {
            format!("CtU address=0x{:x} name={:?}", record.address, record.name)
        }
        ParsedRecordFields::Culul(record) => format!(
            "Culul icb=0x{:x} payload=0x{:x} array_count={}",
            record.icb_addr, record.payload_addr, record.array_count
        ),
        ParsedRecordFields::C(record) => {
            format!(
                "C encoder=0x{:x} flags=0x{:x}",
                record.encoder_addr, record.command_flags
            )
        }
        ParsedRecordFields::Dispatch(record) => format!(
            "dispatch encoder=0x{:x} grid={:?} group={:?}",
            record.encoder_id, record.grid_size, record.group_size
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cs_record() -> MTSPRecord {
        let mut data = vec![0u8; 32];
        data[0..4].copy_from_slice(&(32u32).to_le_bytes());
        data[8..12].copy_from_slice(b"CS\0\0");
        data[12..20].copy_from_slice(&0x1234_u64.to_le_bytes());
        data[20..27].copy_from_slice(b"Kernel\0");
        MTSPRecord::parse_stream(&data).unwrap().remove(0)
    }

    fn make_ct_record() -> MTSPRecord {
        let mut data = vec![0u8; 64];
        let marker_offset = 16;
        data[0..4].copy_from_slice(&(64u32).to_le_bytes());
        data[marker_offset..marker_offset + 4].copy_from_slice(b"Ct\0\0");
        data[marker_offset + 4..marker_offset + 12].copy_from_slice(&0x1111_u64.to_le_bytes());
        data[marker_offset + 12..marker_offset + 20].copy_from_slice(&0x2222_u64.to_le_bytes());
        data[marker_offset + 20..marker_offset + 24].copy_from_slice(&1u32.to_le_bytes());
        data[marker_offset + 24..marker_offset + 28].copy_from_slice(&8u32.to_le_bytes());
        data[marker_offset + 28..marker_offset + 36].copy_from_slice(&0x3333_u64.to_le_bytes());

        MTSPRecord {
            record_type: RecordType::Ct,
            offset: 32,
            size: data.len(),
            label: None,
            address: None,
            function_address: None,
            data,
        }
    }

    fn make_dispatch_record() -> MTSPRecord {
        let mut data = vec![0u8; 0x68];
        data[0..4].copy_from_slice(&(0x68u32).to_le_bytes());
        data[8..18].copy_from_slice(b"C@3ul@3ul\0");
        data[0x30..0x38].copy_from_slice(&0xaaaa_u64.to_le_bytes());
        data[0x38..0x40].copy_from_slice(&8u64.to_le_bytes());
        data[0x40..0x48].copy_from_slice(&4u64.to_le_bytes());
        data[0x48..0x50].copy_from_slice(&2u64.to_le_bytes());
        data[0x50..0x58].copy_from_slice(&32u64.to_le_bytes());
        data[0x58..0x60].copy_from_slice(&1u64.to_le_bytes());
        data[0x60..0x68].copy_from_slice(&1u64.to_le_bytes());

        MTSPRecord {
            record_type: RecordType::C3ul,
            offset: 96,
            size: data.len(),
            label: None,
            address: None,
            function_address: None,
            data,
        }
    }

    fn sample_records() -> Vec<MTSPRecord> {
        vec![make_cs_record(), make_ct_record(), make_dispatch_record()]
    }

    #[test]
    fn summarizes_record_counts() {
        let summary = summarize_records(&sample_records());
        assert_eq!(summary.total_records, 3);
        assert_eq!(summary.shown_records, 3);
        assert_eq!(summary.total_bytes, 32 + 64 + 0x68);
        assert_eq!(summary.counts_by_type.len(), 3);
        assert_eq!(summary.counts_by_type[0].record_type, RecordType::C3ul);
    }

    #[test]
    fn builds_filtered_record_dump() {
        let report = build_record_dump(
            &sample_records(),
            DumpFilter {
                text_contains: Some("kernel".into()),
                include_hex_preview: true,
                max_preview_bytes: 8,
                ..DumpFilter::default()
            },
        );

        assert_eq!(report.records.len(), 1);
        assert_eq!(report.records[0].record_type, RecordType::CS);
        assert_eq!(report.records[0].label.as_deref(), Some("Kernel"));
        assert_eq!(
            report.records[0].hex_preview.as_deref(),
            Some("20 00 00 00 00 00 00 00 ... (+24 bytes)")
        );
        assert_eq!(report.summary.shown_records, 1);
    }

    #[test]
    fn builds_typed_limited_dump_with_parsed_fields() {
        let report = build_record_dump(
            &sample_records(),
            DumpFilter {
                record_type: Some(RecordType::Ct),
                limit: Some(1),
                ..DumpFilter::default()
            },
        );

        assert_eq!(report.records.len(), 1);
        match report.records[0].parsed.as_ref() {
            Some(ParsedRecordFields::Ct(record)) => {
                assert_eq!(record.pipeline_addr, 0x1111);
                assert_eq!(record.function_addr, 0x2222);
                assert_eq!(record.buffer_bindings, vec![0x3333]);
            }
            other => panic!("unexpected parsed fields: {other:?}"),
        }
    }

    #[test]
    fn formats_listing_and_counts() {
        let report = build_record_dump(
            &sample_records(),
            DumpFilter {
                start_index: 1,
                limit: Some(1),
                record_type: Some(RecordType::Ct),
                ..DumpFilter::default()
            },
        );

        let counts = format_record_counts(&report.summary);
        assert!(counts.contains("records: 1 shown / 3 total"));
        assert!(counts.contains("Ct"));

        let listing = format_record_listing(&report);
        assert!(listing.contains("filter: type=Ct"));
        assert!(listing.contains("filter: start index=1"));
        assert!(listing.contains("[1] offset=0x20 type=Ct size=64"));
        assert!(listing.contains("parsed: Ct pipeline=0x1111 function=0x2222 bindings=1"));
    }

    #[test]
    fn serializes_report_as_json() {
        let report = build_record_dump(
            &sample_records(),
            DumpFilter {
                record_type: Some(RecordType::C3ul),
                ..DumpFilter::default()
            },
        );

        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["filter"]["record_type"], "C3ul");
        assert_eq!(json["summary"]["shown_records"], 1);
        assert_eq!(json["records"][0]["parsed"]["kind"], "Dispatch");
        assert_eq!(json["records"][0]["parsed"]["fields"]["encoder_id"], 0xaaaa);
    }

    #[test]
    fn parses_dump_from_mtsp_stream() {
        let mut stream = Vec::new();
        stream.extend_from_slice(b"MTSP");
        stream.extend_from_slice(&1u32.to_le_bytes());
        stream.extend_from_slice(&0u32.to_le_bytes());
        stream.extend_from_slice(&0u32.to_le_bytes());
        stream.extend_from_slice(&make_cs_record().data);

        let report = parse_record_dump(&stream, DumpFilter::default()).unwrap();
        assert_eq!(report.records.len(), 1);
        assert_eq!(report.records[0].label.as_deref(), Some("Kernel"));
    }
}
