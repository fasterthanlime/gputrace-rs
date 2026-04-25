use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::mem;
use std::path::{Path, PathBuf};

use plist::{Dictionary, Uid, Value};
use serde::Serialize;

use crate::counter_names::ALL_COUNTER_NAMES;
use crate::profiler;
use crate::trace::TraceBundle;
use crate::xcode_counters;

type SampleBlob = (String, Vec<u8>);

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CounterLimiter {
    pub encoder_index: usize,
    pub occupancy_manager: Option<f64>,
    pub alu_utilization: Option<f64>,
    pub compute_shader_launch: Option<f64>,
    pub instruction_throughput: Option<f64>,
    pub integer_complex: Option<f64>,
    pub control_flow: Option<f64>,
    pub f32_limiter: Option<f64>,
    pub l1_cache: Option<f64>,
    pub last_level_cache: Option<f64>,
    pub device_memory_bandwidth_gbps: Option<f64>,
    pub buffer_l1_read_bandwidth_gbps: Option<f64>,
    pub buffer_l1_write_bandwidth_gbps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CounterFileMetric {
    pub file_index: usize,
    pub metric_name: String,
    pub unit: Option<String>,
    pub encoder_index: usize,
    pub record_count: usize,
    pub sample_count: usize,
    pub aggregation: String,
    pub total_value: f64,
    pub representative_value: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub mean_value: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterProbeReport {
    pub profiler_directory: PathBuf,
    pub csv_source: Option<PathBuf>,
    pub targets: Vec<RawCounterProbeTarget>,
    pub stream_archives: Vec<RawCounterStreamArchive>,
    pub structured_layouts: Vec<RawCounterStructuredLayout>,
    pub normalized_counters: Vec<RawCounterNormalizedMetric>,
    pub structured_samples: Vec<RawCounterStructuredSample>,
    pub files: Vec<RawCounterProbeFile>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterProbeTarget {
    pub metric: String,
    pub row_index: usize,
    pub encoder_label: String,
    pub value: f64,
    pub tolerance: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterProbeFile {
    pub file_index: usize,
    pub file_name: String,
    pub byte_len: usize,
    pub page_count_4k: Option<usize>,
    pub marker_count: usize,
    pub top_record_shapes: Vec<RawCounterRecordShape>,
    pub matches: Vec<RawCounterProbeMatch>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterRecordShape {
    pub tag: String,
    pub size: usize,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterStreamArchive {
    pub group: String,
    pub index: usize,
    pub byte_len: usize,
    pub source: Option<String>,
    pub serial: Option<u64>,
    pub source_index: Option<u64>,
    pub ring_buffer_index: Option<u64>,
    pub data_file: Option<String>,
    pub shader_profiler_data_len: Option<usize>,
    pub fields: Vec<RawCounterFieldSummary>,
    pub data_fields: Vec<RawCounterDataField>,
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterFieldSummary {
    pub key: String,
    pub kind: String,
    pub len: Option<usize>,
    pub keys: Vec<String>,
    pub children: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterDataField {
    pub key: String,
    pub byte_len: usize,
    pub prefix_hex: String,
    pub f32_preview: Vec<f64>,
    pub u32_preview: Vec<u32>,
    pub u64_preview: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterStructuredSample {
    pub path: String,
    pub byte_len: usize,
    pub gprw_record_size: Option<usize>,
    pub gprw_record_count: Option<usize>,
    pub matches: Vec<RawCounterProbeMatch>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterStructuredLayout {
    pub path: String,
    pub byte_len: usize,
    pub gprw_record_size: usize,
    pub gprw_record_count: usize,
    pub u64_columns: Vec<RawCounterColumnStat>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterColumnStat {
    pub index: usize,
    pub min: u64,
    pub max: u64,
    pub mean: f64,
    pub nonzero_count: usize,
    pub first_values: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterNormalizedMetric {
    pub path: String,
    pub counter_index: usize,
    pub raw_name: String,
    pub sample_count: usize,
    pub mean_percent: f64,
    pub max_percent: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterProbeMatch {
    pub metric: String,
    pub row_index: usize,
    pub encoder_label: String,
    pub target: f64,
    pub tolerance: f64,
    pub encoding: String,
    pub count: usize,
    pub examples: Vec<RawCounterProbeExample>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterProbeExample {
    pub offset: usize,
    pub page_4k: usize,
    pub value: f64,
    pub record_tag: Option<String>,
    pub record_size: Option<usize>,
}

pub fn counter_file_metric_name(file_index: usize) -> Option<&'static str> {
    file_index
        .checked_sub(4)
        .and_then(|index| ALL_COUNTER_NAMES.get(index).copied())
}

pub fn probe_raw_counters(
    trace: &TraceBundle,
    csv_path: Option<PathBuf>,
    metric_filter: Option<&str>,
) -> crate::Result<RawCounterProbeReport> {
    let profiler_directory = profiler::find_profiler_directory(&trace.path)
        .ok_or_else(|| crate::Error::NotFound(trace.path.clone()))?;
    let csv_data = xcode_counters::parse(trace, csv_path).ok();
    let mut targets = Vec::new();
    if let Some(csv_data) = &csv_data {
        for row in &csv_data.encoders {
            for (metric, value) in &row.counters {
                if let Some(filter) = metric_filter
                    && !metric.contains(filter)
                {
                    continue;
                }
                if is_probe_metric(metric) && value.is_finite() {
                    targets.push(RawCounterProbeTarget {
                        metric: metric.clone(),
                        row_index: row.index,
                        encoder_label: row.encoder_label.clone(),
                        value: *value,
                        tolerance: raw_probe_tolerance(*value),
                    });
                }
            }
        }
    }

    let mut files = fs::read_dir(&profiler_directory)?
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            let file_index = name
                .strip_prefix("Counters_f_")
                .and_then(|rest| rest.strip_suffix(".raw"))
                .and_then(|rest| rest.parse::<usize>().ok())?;
            path.is_file().then_some((file_index, name, path))
        })
        .collect::<Vec<_>>();
    files.sort_by_key(|(file_index, _, _)| *file_index);

    let mut file_reports = Vec::new();
    for (file_index, file_name, path) in files {
        let data = fs::read(path)?;
        let markers = find_raw_counter_markers(&data);
        let top_record_shapes = summarize_raw_counter_shapes(&data, &markers);
        let matches = probe_counter_targets(&data, &markers, &targets);
        file_reports.push(RawCounterProbeFile {
            file_index,
            file_name,
            byte_len: data.len(),
            page_count_4k: (data.len() % 4096 == 0).then_some(data.len() / 4096),
            marker_count: markers.len(),
            top_record_shapes,
            matches,
        });
    }

    let structured_samples = probe_structured_counter_samples(&trace.path, &targets);

    Ok(RawCounterProbeReport {
        profiler_directory,
        csv_source: csv_data.map(|data| data.source),
        targets,
        stream_archives: probe_stream_archives(&trace.path),
        structured_layouts: probe_structured_counter_layouts(&trace.path),
        normalized_counters: probe_normalized_counter_metrics(&trace.path),
        structured_samples,
        files: file_reports,
    })
}

pub fn format_raw_counter_probe(report: &RawCounterProbeReport) -> String {
    let mut out = String::new();
    out.push_str("Raw Counter Probe\n");
    out.push_str("=================\n");
    out.push_str(&format!(
        "profiler_directory={}\n",
        report.profiler_directory.display()
    ));
    if let Some(csv_source) = &report.csv_source {
        out.push_str(&format!("csv_source={}\n", csv_source.display()));
    }
    out.push_str(&format!("targets={}\n\n", report.targets.len()));

    if !report.stream_archives.is_empty() {
        out.push_str("streamData archives\n");
        for archive in &report.stream_archives {
            out.push_str(&format!(
                "  {}[{}]: bytes={} source={} serial={} source_index={} ring_buffer={} file={} shader_data={} keys={}\n",
                archive.group,
                archive.index,
                archive.byte_len,
                archive.source.as_deref().unwrap_or("-"),
                archive.serial.map(|value| value.to_string()).unwrap_or_else(|| "-".to_owned()),
                archive
                    .source_index
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                archive
                    .ring_buffer_index
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                archive.data_file.as_deref().unwrap_or("-"),
                archive
                    .shader_profiler_data_len
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                archive.keys.join(",")
            ));
            for field in archive.data_fields.iter().take(8) {
                out.push_str(&format!(
                    "    data {}: bytes={} prefix={} u32={:?} u64={:?} f32={:?}\n",
                    field.key,
                    field.byte_len,
                    field.prefix_hex,
                    field.u32_preview,
                    field.u64_preview,
                    field.f32_preview
                ));
            }
            for field in archive
                .fields
                .iter()
                .filter(|field| field.kind != "data")
                .take(8)
            {
                out.push_str(&format!(
                    "    field {}: kind={} len={} keys={} children={}\n",
                    field.key,
                    field.kind,
                    field
                        .len
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_owned()),
                    field.keys.join(","),
                    field.children.join(",")
                ));
            }
        }
        out.push('\n');
    }

    if !report.structured_samples.is_empty() {
        out.push_str("structured derived samples\n");
        for sample in report.structured_samples.iter().take(32) {
            out.push_str(&format!(
                "  {}: bytes={} record_size={} records={}\n",
                sample.path,
                sample.byte_len,
                sample
                    .gprw_record_size
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                sample
                    .gprw_record_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned())
            ));
            for matched in sample.matches.iter().take(6) {
                out.push_str(&format!(
                    "    {} row={} target={:.4} {} hits={}",
                    matched.metric,
                    matched.row_index,
                    matched.target,
                    matched.encoding,
                    matched.count
                ));
                for example in matched.examples.iter().take(3) {
                    out.push_str(&format!(
                        " @{}(p{} {:.4})",
                        example.offset, example.page_4k, example.value
                    ));
                }
                out.push('\n');
            }
        }
        out.push('\n');
    }

    if !report.structured_layouts.is_empty() {
        out.push_str("structured derived layouts\n");
        for layout in report.structured_layouts.iter().take(32) {
            out.push_str(&format!(
                "  {}: bytes={} record_size={} records={} columns={}\n",
                layout.path,
                layout.byte_len,
                layout.gprw_record_size,
                layout.gprw_record_count,
                layout.u64_columns.len()
            ));
            for column in layout.u64_columns.iter().take(10) {
                out.push_str(&format!(
                    "    u64[{}]: min={} max={} mean={:.2} nonzero={} first={:?}\n",
                    column.index,
                    column.min,
                    column.max,
                    column.mean,
                    column.nonzero_count,
                    column.first_values
                ));
            }
        }
        out.push('\n');
    }

    if !report.normalized_counters.is_empty() {
        out.push_str("normalized derived counters\n");
        for metric in report.normalized_counters.iter().take(32) {
            out.push_str(&format!(
                "  {} [{}] {}: mean={:.2}% max={:.2}% samples={}\n",
                metric.path,
                metric.counter_index,
                metric.raw_name,
                metric.mean_percent,
                metric.max_percent,
                metric.sample_count
            ));
        }
        out.push('\n');
    }

    for file in &report.files {
        out.push_str(&format!(
            "Counters_f_{}: bytes={} pages4k={} markers={}\n",
            file.file_index,
            file.byte_len,
            file.page_count_4k
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            file.marker_count,
        ));
        if !file.top_record_shapes.is_empty() {
            out.push_str("  top shapes:");
            for shape in file.top_record_shapes.iter().take(8) {
                out.push_str(&format!(" {}:{}x{}", shape.tag, shape.size, shape.count));
            }
            out.push('\n');
        }
        for matched in file.matches.iter().take(12) {
            out.push_str(&format!(
                "  {} row={} target={:.4} +/- {:.4} {} hits={}",
                matched.metric,
                matched.row_index,
                matched.target,
                matched.tolerance,
                matched.encoding,
                matched.count
            ));
            for example in matched.examples.iter().take(3) {
                out.push_str(&format!(
                    " @{}(p{} {} {} {:.4})",
                    example.offset,
                    example.page_4k,
                    example.record_tag.as_deref().unwrap_or("-"),
                    example
                        .record_size
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_owned()),
                    example.value
                ));
            }
            out.push('\n');
        }
        out.push('\n');
    }
    out
}

fn probe_stream_archives(trace_path: &Path) -> Vec<RawCounterStreamArchive> {
    let Some(profiler_dir) = profiler::find_profiler_directory(trace_path) else {
        return Vec::new();
    };
    let stream_data_path = profiler_dir.join("streamData");
    let Ok(plist) = Value::from_file(stream_data_path) else {
        return Vec::new();
    };
    let Some(archive) = plist.as_dictionary() else {
        return Vec::new();
    };
    let Some(objects) = archive.get("$objects").and_then(Value::as_array) else {
        return Vec::new();
    };
    let Some(root) = objects.get(1).and_then(Value::as_dictionary) else {
        return Vec::new();
    };

    ["APSData", "APSCounterData", "APSTimelineData"]
        .into_iter()
        .flat_map(|group| {
            ns_data_array_from_root_key(objects, root, group)
                .into_iter()
                .enumerate()
                .filter_map(move |(index, bytes)| summarize_stream_archive(group, index, &bytes))
        })
        .collect()
}

fn probe_structured_counter_samples(
    trace_path: &Path,
    targets: &[RawCounterProbeTarget],
) -> Vec<RawCounterStructuredSample> {
    if targets.is_empty() {
        return Vec::new();
    }
    let Some(profiler_dir) = profiler::find_profiler_directory(trace_path) else {
        return Vec::new();
    };
    let stream_data_path = profiler_dir.join("streamData");
    let Ok(plist) = Value::from_file(stream_data_path) else {
        return Vec::new();
    };
    let Some(archive) = plist.as_dictionary() else {
        return Vec::new();
    };
    let Some(objects) = archive.get("$objects").and_then(Value::as_array) else {
        return Vec::new();
    };
    let Some(root) = objects.get(1).and_then(Value::as_dictionary) else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for (archive_index, bytes) in ns_data_array_from_root_key(objects, root, "APSCounterData")
        .into_iter()
        .enumerate()
    {
        let Some(keyed) = parse_keyed_archive_dictionary(&bytes) else {
            continue;
        };
        let Some(samples) = keyed.get("Derived Counter Sample Data") else {
            continue;
        };
        let mut blobs = Vec::new();
        collect_data_blobs(
            &format!("APSCounterData[{archive_index}]/Derived Counter Sample Data"),
            samples,
            &mut blobs,
        );
        for (path, data) in blobs {
            if !data.starts_with(b"GPRWCNTR") {
                continue;
            }
            let matches = probe_counter_targets(&data, &[], targets);
            if matches.is_empty() {
                continue;
            }
            let (gprw_record_size, gprw_record_count) = gprw_record_info(&data).unwrap_or((0, 0));
            out.push(RawCounterStructuredSample {
                path,
                byte_len: data.len(),
                gprw_record_size: (gprw_record_size > 0).then_some(gprw_record_size),
                gprw_record_count: (gprw_record_count > 0).then_some(gprw_record_count),
                matches,
            });
        }
    }
    out.sort_by(|left, right| {
        let left_hits: usize = left.matches.iter().map(|matched| matched.count).sum();
        let right_hits: usize = right.matches.iter().map(|matched| matched.count).sum();
        right_hits
            .cmp(&left_hits)
            .then_with(|| left.path.cmp(&right.path))
    });
    out
}

fn probe_structured_counter_layouts(trace_path: &Path) -> Vec<RawCounterStructuredLayout> {
    let Some(profiler_dir) = profiler::find_profiler_directory(trace_path) else {
        return Vec::new();
    };
    let stream_data_path = profiler_dir.join("streamData");
    let Ok(plist) = Value::from_file(stream_data_path) else {
        return Vec::new();
    };
    let Some(archive) = plist.as_dictionary() else {
        return Vec::new();
    };
    let Some(objects) = archive.get("$objects").and_then(Value::as_array) else {
        return Vec::new();
    };
    let Some(root) = objects.get(1).and_then(Value::as_dictionary) else {
        return Vec::new();
    };

    let mut layouts = Vec::new();
    for (archive_index, bytes) in ns_data_array_from_root_key(objects, root, "APSCounterData")
        .into_iter()
        .enumerate()
    {
        let Some(keyed) = parse_keyed_archive_dictionary(&bytes) else {
            continue;
        };
        let Some(samples) = keyed.get("Derived Counter Sample Data") else {
            continue;
        };
        let mut blobs = Vec::new();
        collect_data_blobs(
            &format!("APSCounterData[{archive_index}]/Derived Counter Sample Data"),
            samples,
            &mut blobs,
        );
        for (path, data) in blobs {
            let Some((record_size, record_count)) = gprw_record_info(&data) else {
                continue;
            };
            if record_count < 2 {
                continue;
            }
            layouts.push(RawCounterStructuredLayout {
                path,
                byte_len: data.len(),
                gprw_record_size: record_size,
                gprw_record_count: record_count,
                u64_columns: summarize_gprw_u64_columns(&data, record_size),
            });
        }
    }
    layouts.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| right.gprw_record_count.cmp(&left.gprw_record_count))
    });
    layouts
}

fn probe_normalized_counter_metrics(trace_path: &Path) -> Vec<RawCounterNormalizedMetric> {
    let Some((counter_names, sample_blobs)) = counter_names_and_sample_blobs(trace_path) else {
        return Vec::new();
    };
    if counter_names.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::new();
    for (path, data) in sample_blobs {
        if !path.contains("/Derived Counter Sample Data/")
            || path.split('/').nth_back(1) != Some("1")
        {
            continue;
        }
        let Some((record_size, _)) = gprw_record_info(&data) else {
            continue;
        };
        let records = gprw_u64_records(&data, record_size);
        if records.is_empty() {
            continue;
        }
        for (counter_index, raw_name) in counter_names.iter().enumerate() {
            let value_column = 8 + counter_index;
            let mut values = Vec::new();
            for record in &records {
                let Some(denominator) = record.get(2).copied() else {
                    continue;
                };
                let Some(value) = record.get(value_column).copied() else {
                    continue;
                };
                if denominator == 0 {
                    continue;
                }
                values.push(value as f64 / denominator as f64 * 100.0);
            }
            if values.is_empty() {
                continue;
            }
            let max_percent = values.iter().copied().fold(0.0, f64::max);
            let mean_percent = values.iter().sum::<f64>() / values.len() as f64;
            metrics.push(RawCounterNormalizedMetric {
                path: path.clone(),
                counter_index,
                raw_name: raw_name.clone(),
                sample_count: values.len(),
                mean_percent,
                max_percent,
            });
        }
    }
    metrics.sort_by(|left, right| {
        right
            .mean_percent
            .partial_cmp(&left.mean_percent)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.counter_index.cmp(&right.counter_index))
    });
    metrics
}

fn counter_names_and_sample_blobs(trace_path: &Path) -> Option<(Vec<String>, Vec<SampleBlob>)> {
    let profiler_dir = profiler::find_profiler_directory(trace_path)?;
    let stream_data_path = profiler_dir.join("streamData");
    let plist = Value::from_file(stream_data_path).ok()?;
    let archive = plist.as_dictionary()?;
    let objects = archive.get("$objects").and_then(Value::as_array)?;
    let root = objects.get(1).and_then(Value::as_dictionary)?;
    let counter_archives = ns_data_array_from_root_key(objects, root, "APSCounterData");

    let mut counter_names = Vec::new();
    let mut sample_blobs = Vec::new();
    for (archive_index, bytes) in counter_archives.into_iter().enumerate() {
        let Some(keyed) = parse_keyed_archive_dictionary(&bytes) else {
            continue;
        };
        if counter_names.is_empty()
            && let Some(names) = keyed.get("limiter sample counters")
        {
            counter_names = string_array_values(names);
        }
        if let Some(samples) = keyed.get("Derived Counter Sample Data") {
            collect_data_blobs(
                &format!("APSCounterData[{archive_index}]/Derived Counter Sample Data"),
                samples,
                &mut sample_blobs,
            );
        }
    }
    Some((counter_names, sample_blobs))
}

fn string_array_values(value: &StreamArchiveValue) -> Vec<String> {
    let StreamArchiveValue::Array(children) = value else {
        return Vec::new();
    };
    children
        .iter()
        .filter_map(|child| match child {
            StreamArchiveValue::String(value) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

fn gprw_u64_records(data: &[u8], record_size: usize) -> Vec<Vec<u64>> {
    let mut records = Vec::new();
    for offset in gprw_magic_offsets(data) {
        let Some(record) = data.get(offset..offset + record_size) else {
            continue;
        };
        records.push(
            record
                .chunks_exact(mem::size_of::<u64>())
                .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
                .collect(),
        );
    }
    records
}

fn summarize_gprw_u64_columns(data: &[u8], record_size: usize) -> Vec<RawCounterColumnStat> {
    if record_size < mem::size_of::<u64>() {
        return Vec::new();
    }
    let columns = record_size / mem::size_of::<u64>();
    let mut values_by_column = vec![Vec::<u64>::new(); columns];
    for offset in gprw_magic_offsets(data) {
        let Some(record) = data.get(offset..offset + record_size) else {
            continue;
        };
        for (index, chunk) in record.chunks_exact(mem::size_of::<u64>()).enumerate() {
            values_by_column[index].push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
    }
    values_by_column
        .into_iter()
        .enumerate()
        .filter_map(|(index, values)| summarize_u64_column(index, values))
        .collect()
}

fn summarize_u64_column(index: usize, values: Vec<u64>) -> Option<RawCounterColumnStat> {
    if values.is_empty() {
        return None;
    }
    let min = values.iter().min().copied()?;
    let max = values.iter().max().copied()?;
    let total = values
        .iter()
        .fold(0.0, |total, value| total + *value as f64);
    let nonzero_count = values.iter().filter(|value| **value != 0).count();
    Some(RawCounterColumnStat {
        index,
        min,
        max,
        mean: total / values.len() as f64,
        nonzero_count,
        first_values: values.into_iter().take(6).collect(),
    })
}

fn collect_data_blobs(path: &str, value: &StreamArchiveValue, out: &mut Vec<(String, Vec<u8>)>) {
    match value {
        StreamArchiveValue::Data(data) => out.push((path.to_owned(), data.clone())),
        StreamArchiveValue::Array(children) => {
            for (index, child) in children.iter().enumerate() {
                collect_data_blobs(&format!("{path}/{index}"), child, out);
            }
        }
        _ => {}
    }
}

fn summarize_stream_archive(
    group: &str,
    index: usize,
    bytes: &[u8],
) -> Option<RawCounterStreamArchive> {
    let keyed = parse_keyed_archive_dictionary(bytes)?;
    let keys = keyed.keys().cloned().collect::<Vec<_>>();
    let data_file = keyed
        .get("APSCounterDataFile")
        .or_else(|| keyed.get("APSTraceDataFile"))
        .or_else(|| keyed.get("File"))
        .and_then(StreamArchiveValue::as_string)
        .map(ToOwned::to_owned);
    Some(RawCounterStreamArchive {
        group: group.to_owned(),
        index,
        byte_len: bytes.len(),
        source: keyed
            .get("Source")
            .and_then(StreamArchiveValue::as_string)
            .map(ToOwned::to_owned),
        serial: keyed.get("Serial").and_then(StreamArchiveValue::as_u64),
        source_index: keyed
            .get("SourceIndex")
            .and_then(StreamArchiveValue::as_u64),
        ring_buffer_index: keyed
            .get("RingBufferIndex")
            .and_then(StreamArchiveValue::as_u64),
        data_file,
        shader_profiler_data_len: keyed
            .get("ShaderProfilerData")
            .and_then(StreamArchiveValue::as_data_len),
        fields: keyed
            .iter()
            .map(|(key, value)| summarize_field(key, value))
            .collect(),
        data_fields: keyed
            .iter()
            .filter_map(|(key, value)| {
                let StreamArchiveValue::Data(data) = value else {
                    return None;
                };
                Some(summarize_data_field(key, data))
            })
            .collect(),
        keys,
    })
}

fn summarize_field(key: &str, value: &StreamArchiveValue) -> RawCounterFieldSummary {
    let (kind, len, keys) = match value {
        StreamArchiveValue::String(_) => ("string", None, Vec::new()),
        StreamArchiveValue::Integer(_) => ("integer", None, Vec::new()),
        StreamArchiveValue::Data(data) => ("data", Some(data.len()), Vec::new()),
        StreamArchiveValue::Array(children) => ("array", Some(children.len()), Vec::new()),
        StreamArchiveValue::Dictionary(keys) => ("dictionary", Some(keys.len()), keys.clone()),
        StreamArchiveValue::Other => ("other", None, Vec::new()),
    };
    let children = match value {
        StreamArchiveValue::Array(children) => children
            .iter()
            .take(32)
            .enumerate()
            .map(|(index, child)| format!("{index}:{}", child.short_summary()))
            .collect(),
        _ => Vec::new(),
    };
    RawCounterFieldSummary {
        key: key.to_owned(),
        kind: kind.to_owned(),
        len,
        keys,
        children,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum StreamArchiveValue {
    String(String),
    Integer(u64),
    Data(Vec<u8>),
    Array(Vec<StreamArchiveValue>),
    Dictionary(Vec<String>),
    Other,
}

impl StreamArchiveValue {
    fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Integer(value) => Some(*value),
            _ => None,
        }
    }

    fn as_data_len(&self) -> Option<usize> {
        match self {
            Self::Data(value) => Some(value.len()),
            _ => None,
        }
    }

    fn short_summary(&self) -> String {
        match self {
            Self::String(value) => format!("string:{value}"),
            Self::Integer(value) => format!("integer:{value}"),
            Self::Data(data) => match summarize_gprw_data(data) {
                Some(summary) => summary,
                None => format!("data:{}:{}", data.len(), format_hex_prefix(data, 16)),
            },
            Self::Array(children) => {
                let nested = children
                    .iter()
                    .take(8)
                    .map(StreamArchiveValue::short_summary)
                    .collect::<Vec<_>>()
                    .join("|");
                format!("array:{}:[{}]", children.len(), nested)
            }
            Self::Dictionary(keys) => format!("dictionary:{}:{}", keys.len(), keys.join("|")),
            Self::Other => "other".to_owned(),
        }
    }
}

fn summarize_gprw_data(data: &[u8]) -> Option<String> {
    let (record_size, record_count) = gprw_record_info(data)?;
    let magic_offsets = gprw_magic_offsets(data);
    let repeated_magic = magic_offsets.len() > 1;
    let first = if repeated_magic {
        data.get(..record_size)?
    } else {
        data.get(8..8 + record_size)?
    };
    let fields = preview_u64_values(first, 8)
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("|");
    let magic_deltas = summarize_magic_deltas(&magic_offsets);
    Some(format!(
        "gprw:{}:record_size={record_size}:records={record_count}:magics={}:deltas={}:u64={fields}",
        data.len(),
        magic_offsets.len(),
        magic_deltas
    ))
}

fn gprw_record_info(data: &[u8]) -> Option<(usize, usize)> {
    if data.get(..8)? != b"GPRWCNTR" {
        return None;
    }
    let magic_offsets = gprw_magic_offsets(data);
    if let Some(record_size) = dominant_magic_delta(&magic_offsets)
        && record_size > 0
    {
        return Some((record_size, magic_offsets.len()));
    }
    let record_size = 168;
    let record_count = data[8..].len() / record_size;
    Some((record_size, record_count))
}

fn gprw_magic_offsets(data: &[u8]) -> Vec<usize> {
    data.windows(8)
        .enumerate()
        .filter_map(|(offset, window)| (window == b"GPRWCNTR").then_some(offset))
        .collect()
}

fn summarize_magic_deltas(offsets: &[usize]) -> String {
    let mut counts = BTreeMap::<usize, usize>::new();
    for pair in offsets.windows(2) {
        *counts.entry(pair[1] - pair[0]).or_default() += 1;
    }
    let mut counts = counts.into_iter().collect::<Vec<_>>();
    counts.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    counts
        .into_iter()
        .take(4)
        .map(|(delta, count)| format!("{delta}x{count}"))
        .collect::<Vec<_>>()
        .join("|")
}

fn dominant_magic_delta(offsets: &[usize]) -> Option<usize> {
    let mut counts = BTreeMap::<usize, usize>::new();
    for pair in offsets.windows(2) {
        *counts.entry(pair[1] - pair[0]).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        .map(|(delta, _)| delta)
}

fn parse_keyed_archive_dictionary(bytes: &[u8]) -> Option<BTreeMap<String, StreamArchiveValue>> {
    let plist = Value::from_reader(Cursor::new(bytes)).ok()?;
    let archive = plist.as_dictionary()?;
    let objects = archive.get("$objects").and_then(Value::as_array)?;
    let top = archive.get("$top").and_then(Value::as_dictionary)?;
    let root_uid = top.get("root").and_then(as_uid)?;
    let root = object_dictionary(objects, root_uid)?;
    keyed_dictionary_values(objects, root)
}

fn keyed_dictionary_values(
    objects: &[Value],
    root: &Dictionary,
) -> Option<BTreeMap<String, StreamArchiveValue>> {
    let keys = root.get("NS.keys").and_then(Value::as_array)?;
    let values = root.get("NS.objects").and_then(Value::as_array)?;
    if keys.len() != values.len() {
        return None;
    }

    let mut out = BTreeMap::new();
    for (key, value) in keys.iter().zip(values.iter()) {
        let key_uid = as_uid(key)?;
        let key_name = object(objects, key_uid).and_then(Value::as_string)?;
        let resolved = resolve_value(objects, value)?;
        out.insert(
            key_name.to_owned(),
            summarize_stream_archive_value(objects, resolved),
        );
    }
    Some(out)
}

fn summarize_stream_archive_value(objects: &[Value], value: &Value) -> StreamArchiveValue {
    if let Some(value) = value.as_string() {
        return StreamArchiveValue::String(value.to_owned());
    }
    if let Some(value) = as_u64(value) {
        return StreamArchiveValue::Integer(value);
    }
    if let Some(data) = ns_data_from_value(value) {
        return StreamArchiveValue::Data(data.to_vec());
    }
    if let Some(array) = value
        .as_dictionary()
        .and_then(|dict| dict.get("NS.objects"))
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
    {
        return StreamArchiveValue::Array(
            array
                .iter()
                .take(512)
                .filter_map(|value| resolve_value(objects, value))
                .map(|value| summarize_stream_archive_value(objects, value))
                .collect(),
        );
    }
    if let Some(dict) = value.as_dictionary()
        && let Some(values) = keyed_dictionary_values(objects, dict)
    {
        return StreamArchiveValue::Dictionary(values.keys().cloned().collect());
    }
    StreamArchiveValue::Other
}

fn summarize_data_field(key: &str, data: &[u8]) -> RawCounterDataField {
    RawCounterDataField {
        key: key.to_owned(),
        byte_len: data.len(),
        prefix_hex: format_hex_prefix(data, 32),
        f32_preview: preview_f32_values(data, 8),
        u32_preview: preview_u32_values(data, 8),
        u64_preview: preview_u64_values(data, 4),
    }
}

fn format_hex_prefix(data: &[u8], max_len: usize) -> String {
    data.iter()
        .take(max_len)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn preview_f32_values(data: &[u8], max_count: usize) -> Vec<f64> {
    data.chunks_exact(mem::size_of::<f32>())
        .take(max_count)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()) as f64)
        .collect()
}

fn preview_u32_values(data: &[u8], max_count: usize) -> Vec<u32> {
    data.chunks_exact(mem::size_of::<u32>())
        .take(max_count)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

fn preview_u64_values(data: &[u8], max_count: usize) -> Vec<u64> {
    data.chunks_exact(mem::size_of::<u64>())
        .take(max_count)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

fn ns_data_array_from_root_key(objects: &[Value], root: &Dictionary, key: &str) -> Vec<Vec<u8>> {
    let Some(uid) = root.get(key).and_then(as_uid) else {
        return Vec::new();
    };
    let Some(array_dict) = object_dictionary(objects, uid) else {
        return Vec::new();
    };
    let Some(values) = array_dict.get("NS.objects").and_then(Value::as_array) else {
        return Vec::new();
    };

    values
        .iter()
        .filter_map(|value| resolve_value(objects, value))
        .filter_map(ns_data_from_value)
        .map(ToOwned::to_owned)
        .collect()
}

fn ns_data_from_value(value: &Value) -> Option<&[u8]> {
    value
        .as_dictionary()
        .and_then(|dict| dict.get("NS.data"))
        .and_then(Value::as_data)
        .or_else(|| value.as_data())
}

fn object(objects: &[Value], uid: Uid) -> Option<&Value> {
    objects.get(uid.get() as usize)
}

fn object_dictionary(objects: &[Value], uid: Uid) -> Option<&Dictionary> {
    object(objects, uid).and_then(Value::as_dictionary)
}

fn resolve_value<'a>(objects: &'a [Value], value: &'a Value) -> Option<&'a Value> {
    match value {
        Value::Uid(uid) => object(objects, *uid),
        value => Some(value),
    }
}

fn as_uid(value: &Value) -> Option<Uid> {
    match value {
        Value::Uid(uid) => Some(*uid),
        _ => None,
    }
}

fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Integer(value) => value.as_unsigned().or_else(|| {
            value
                .as_signed()
                .and_then(|value| u64::try_from(value).ok())
        }),
        _ => None,
    }
}

pub fn extract_counter_file_metrics(profiler_dir: &Path) -> Vec<CounterFileMetric> {
    let Ok(entries) = fs::read_dir(profiler_dir) else {
        return Vec::new();
    };

    let mut files = entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            let file_index = name
                .strip_prefix("Counters_f_")
                .and_then(|rest| rest.strip_suffix(".raw"))
                .and_then(|rest| rest.parse::<usize>().ok())?;
            path.is_file().then_some((file_index, path))
        })
        .collect::<Vec<_>>();
    files.sort_by_key(|(file_index, _)| *file_index);

    let mut metrics = Vec::new();
    for (file_index, path) in files {
        let Ok(data) = fs::read(path) else {
            continue;
        };
        metrics.extend(extract_counter_file_metrics_from_data(file_index, &data));
    }
    metrics.sort_by(|left, right| {
        left.encoder_index
            .cmp(&right.encoder_index)
            .then_with(|| left.file_index.cmp(&right.file_index))
    });
    metrics
}

pub fn extract_limiters(profiler_dir: &Path) -> Vec<CounterLimiter> {
    let Ok(entries) = fs::read_dir(profiler_dir) else {
        return Vec::new();
    };

    let mut encoder_limiters = BTreeMap::<usize, CounterLimiter>::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if !path.is_file() || !name.starts_with("Counters_f_") || !name.ends_with(".raw") {
            continue;
        }
        let Ok(data) = fs::read(path) else {
            continue;
        };
        let record_starts = find_record_starts(&data);
        if record_starts.is_empty() {
            continue;
        }

        let mut current_encoder = None;
        for (index, offset) in record_starts.iter().enumerate() {
            let next = record_starts.get(index + 1).copied().unwrap_or(data.len());
            let record_size = next.saturating_sub(*offset);
            if (2300..=2900).contains(&record_size) {
                let encoder_index = current_encoder.map(|value| value + 1).unwrap_or(0);
                current_encoder = Some(encoder_index);
                encoder_limiters
                    .entry(encoder_index)
                    .or_insert_with(|| CounterLimiter {
                        encoder_index,
                        occupancy_manager: None,
                        alu_utilization: None,
                        compute_shader_launch: None,
                        instruction_throughput: None,
                        integer_complex: None,
                        control_flow: None,
                        f32_limiter: None,
                        l1_cache: None,
                        last_level_cache: None,
                        device_memory_bandwidth_gbps: None,
                        buffer_l1_read_bandwidth_gbps: None,
                        buffer_l1_write_bandwidth_gbps: None,
                    });
                continue;
            }
            if record_size != 464 {
                continue;
            }
            let Some(encoder_index) = current_encoder else {
                continue;
            };
            let Some(record) = data.get(*offset..(*offset + record_size)) else {
                continue;
            };
            let limiter = encoder_limiters
                .entry(encoder_index)
                .or_insert_with(|| CounterLimiter {
                    encoder_index,
                    occupancy_manager: None,
                    alu_utilization: None,
                    compute_shader_launch: None,
                    instruction_throughput: None,
                    integer_complex: None,
                    control_flow: None,
                    f32_limiter: None,
                    l1_cache: None,
                    last_level_cache: None,
                    device_memory_bandwidth_gbps: None,
                    buffer_l1_read_bandwidth_gbps: None,
                    buffer_l1_write_bandwidth_gbps: None,
                });
            classify_record_metrics(record, limiter);
        }
    }

    encoder_limiters.into_values().collect()
}

pub fn extract_limiters_for_trace(path: &Path) -> Vec<CounterLimiter> {
    profiler::find_profiler_directory(path)
        .map(|dir| extract_limiters(&dir))
        .unwrap_or_default()
}

fn extract_counter_file_metrics_from_data(
    file_index: usize,
    data: &[u8],
) -> Vec<CounterFileMetric> {
    let record_starts = find_record_starts(data);
    if record_starts.is_empty() {
        return Vec::new();
    }

    let metric_name = counter_file_metric_name(file_index)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("Counters_f_{file_index}"));
    let unit = counter_metric_unit(&metric_name).map(ToOwned::to_owned);
    let mut current_encoder = None;
    let mut by_encoder = BTreeMap::<usize, (usize, Vec<f64>)>::new();

    for (index, offset) in record_starts.iter().enumerate() {
        let next = record_starts.get(index + 1).copied().unwrap_or(data.len());
        let record_size = next.saturating_sub(*offset);
        if (2300..=2900).contains(&record_size) {
            let encoder_index = current_encoder.map(|value| value + 1).unwrap_or(0);
            current_encoder = Some(encoder_index);
            by_encoder.entry(encoder_index).or_default();
            continue;
        }
        if record_size != 464 {
            continue;
        }
        let encoder_index = current_encoder.unwrap_or(0);
        let Some(record) = data.get(*offset..(*offset + record_size)) else {
            continue;
        };
        let values = extract_counter_record_values(record, &metric_name);
        if values.is_empty() {
            continue;
        }
        let (record_count, samples) = by_encoder.entry(encoder_index).or_default();
        *record_count += 1;
        samples.extend(values);
    }

    by_encoder
        .into_iter()
        .filter_map(|(encoder_index, (record_count, values))| {
            summarize_counter_values(
                file_index,
                metric_name.clone(),
                unit.clone(),
                encoder_index,
                record_count,
                values,
            )
        })
        .collect()
}

fn is_probe_metric(metric: &str) -> bool {
    matches!(
        metric,
        "ALU Utilization"
            | "Kernel Occupancy"
            | "Device Memory Bandwidth"
            | "GPU Read Bandwidth"
            | "GPU Write Bandwidth"
            | "Buffer L1 Miss Rate"
            | "Buffer L1 Read Accesses"
            | "Buffer L1 Read Bandwidth"
            | "Buffer L1 Write Accesses"
            | "Buffer L1 Write Bandwidth"
            | "Kernel Invocations"
    )
}

fn raw_probe_tolerance(value: f64) -> f64 {
    if value.abs() >= 1000.0 {
        (value.abs() * 0.001).max(1.0)
    } else {
        (value.abs() * 0.005).max(0.02)
    }
}

fn find_raw_counter_markers(data: &[u8]) -> Vec<usize> {
    const TAGS: &[u8] = &[0x0e, 0x2e, 0x4e, 0x6e, 0x8e, 0xae, 0xce, 0xee];
    let mut offsets = Vec::new();
    for offset in 0..data.len().saturating_sub(3) {
        if TAGS.contains(&data[offset])
            && data[offset + 1] == 0
            && data[offset + 2] == 0
            && data[offset + 3] == 0
        {
            offsets.push(offset);
        }
    }
    offsets
}

fn summarize_raw_counter_shapes(data: &[u8], markers: &[usize]) -> Vec<RawCounterRecordShape> {
    let mut counts = BTreeMap::<(u8, usize), usize>::new();
    for (index, offset) in markers.iter().enumerate() {
        let next = markers.get(index + 1).copied().unwrap_or(data.len());
        if next <= *offset {
            continue;
        }
        let tag = data[*offset];
        let size = next - *offset;
        *counts.entry((tag, size)).or_default() += 1;
    }
    let mut shapes = counts
        .into_iter()
        .map(|((tag, size), count)| RawCounterRecordShape {
            tag: format!("0x{tag:02x}"),
            size,
            count,
        })
        .collect::<Vec<_>>();
    shapes.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.size.cmp(&right.size))
            .then_with(|| left.tag.cmp(&right.tag))
    });
    shapes
}

fn probe_counter_targets(
    data: &[u8],
    markers: &[usize],
    targets: &[RawCounterProbeTarget],
) -> Vec<RawCounterProbeMatch> {
    let mut accum = BTreeMap::<(usize, &'static str), RawProbeAccum>::new();
    for offset in (0..data.len().saturating_sub(mem::size_of::<u32>())).step_by(4) {
        let float_value = f32::from_bits(u32::from_le_bytes(
            data[offset..offset + 4].try_into().unwrap(),
        )) as f64;
        if float_value.is_finite() {
            for (target_index, target) in targets.iter().enumerate() {
                if (float_value - target.value).abs() <= target.tolerance {
                    push_probe_match(
                        &mut accum,
                        target_index,
                        "f32",
                        data,
                        markers,
                        offset,
                        float_value,
                    );
                }
            }
        }

        if offset + mem::size_of::<u64>() <= data.len() {
            let uint_value = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            for (target_index, target) in targets.iter().enumerate() {
                if !target_allows_integer_encoding(&target.metric) {
                    continue;
                }
                let value = uint_value as f64;
                if (value - target.value).abs() <= target.tolerance {
                    push_probe_match(
                        &mut accum,
                        target_index,
                        "u64",
                        data,
                        markers,
                        offset,
                        value,
                    );
                }
            }
        }
    }

    let mut matches = accum
        .into_iter()
        .filter_map(|((target_index, encoding), accum)| {
            let target = targets.get(target_index)?;
            Some(RawCounterProbeMatch {
                metric: target.metric.clone(),
                row_index: target.row_index,
                encoder_label: target.encoder_label.clone(),
                target: target.value,
                tolerance: target.tolerance,
                encoding: encoding.to_owned(),
                count: accum.count,
                examples: accum.examples,
            })
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.metric.cmp(&right.metric))
            .then_with(|| left.row_index.cmp(&right.row_index))
            .then_with(|| left.encoding.cmp(&right.encoding))
    });
    matches
}

fn target_allows_integer_encoding(metric: &str) -> bool {
    metric.contains("Invocations") || metric.contains("Bytes")
}

#[derive(Default)]
struct RawProbeAccum {
    count: usize,
    examples: Vec<RawCounterProbeExample>,
}

fn push_probe_match(
    accum: &mut BTreeMap<(usize, &'static str), RawProbeAccum>,
    target_index: usize,
    encoding: &'static str,
    data: &[u8],
    markers: &[usize],
    offset: usize,
    value: f64,
) {
    let entry = accum.entry((target_index, encoding)).or_default();
    entry.count += 1;
    if entry.examples.len() >= 5 {
        return;
    }
    let (record_tag, record_size) = marker_for_offset(data, markers, offset)
        .map(|(tag, size)| (Some(format!("0x{tag:02x}")), Some(size)))
        .unwrap_or((None, None));
    entry.examples.push(RawCounterProbeExample {
        offset,
        page_4k: offset / 4096,
        value,
        record_tag,
        record_size,
    });
}

fn marker_for_offset(data: &[u8], markers: &[usize], offset: usize) -> Option<(u8, usize)> {
    let index = match markers.binary_search(&offset) {
        Ok(index) => index,
        Err(0) => return None,
        Err(index) => index - 1,
    };
    let start = markers[index];
    let next = markers.get(index + 1).copied().unwrap_or(data.len());
    (next > start).then_some((data[start], next - start))
}

fn summarize_counter_values(
    file_index: usize,
    metric_name: String,
    unit: Option<String>,
    encoder_index: usize,
    record_count: usize,
    mut values: Vec<f64>,
) -> Option<CounterFileMetric> {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return None;
    }

    values.sort_by(|left, right| left.total_cmp(right));
    let sample_count = values.len();
    let min_value = values[0];
    let max_value = values[sample_count - 1];
    let total_value = values.iter().sum::<f64>();
    let mean_value = total_value / sample_count as f64;
    let aggregation = counter_metric_aggregation(unit.as_deref()).to_owned();
    let representative_value = if aggregation == "sum" {
        total_value
    } else {
        median_sorted(&values)
    };
    let confidence = counter_metric_confidence(record_count, sample_count, min_value, max_value);

    Some(CounterFileMetric {
        file_index,
        metric_name,
        unit,
        encoder_index,
        record_count,
        sample_count,
        aggregation,
        total_value,
        representative_value,
        min_value,
        max_value,
        mean_value,
        confidence,
    })
}

fn extract_counter_record_values(record: &[u8], metric_name: &str) -> Vec<f64> {
    if counter_metric_unit(metric_name) == Some("bytes") {
        return extract_byte_count_candidates(record)
            .into_iter()
            .map(|value| value as f64)
            .collect();
    }

    let mut values = Vec::new();
    let mut seen = Vec::<u32>::new();
    let max = counter_metric_max_value(metric_name);
    for chunk in record[4..].chunks_exact(mem::size_of::<u32>()) {
        let bits = u32::from_le_bytes(chunk.try_into().unwrap());
        let value = f32::from_bits(bits) as f64;
        if value.is_finite() && value >= 0.000_001 && value <= max && !seen.contains(&bits) {
            seen.push(bits);
            values.push(value);
        }
    }
    values
}

fn extract_byte_count_candidates(data: &[u8]) -> Vec<u64> {
    const MIN_BYTES: u64 = 1_000;
    const MAX_BYTES: u64 = 100_000_000;

    let mut values = Vec::new();
    let mut seen = Vec::<u64>::new();
    for start in [0usize, 4] {
        let mut offset = start;
        while offset + mem::size_of::<u64>() <= data.len() {
            let value = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            if (MIN_BYTES..=MAX_BYTES).contains(&value) && !seen.contains(&value) {
                seen.push(value);
                values.push(value);
            }
            offset += mem::size_of::<u64>();
        }
    }
    values
}

fn counter_metric_unit(metric_name: &str) -> Option<&'static str> {
    if metric_name.contains("Bandwidth") {
        Some("GB/s")
    } else if metric_name.contains("Utilization")
        || metric_name.contains("Limiter")
        || metric_name.contains("Occupancy")
        || metric_name.contains("Miss Rate")
        || metric_name.contains("Inefficiency")
        || metric_name.contains("Residency")
        || metric_name.contains("Compression Ratio")
        || metric_name.contains("Average")
    {
        Some("%")
    } else if metric_name.contains("Bytes") {
        Some("bytes")
    } else if metric_name.contains("Instructions")
        || metric_name.contains("Invocations")
        || metric_name.contains("Accesses")
        || metric_name.contains("Calls")
        || metric_name.contains("Pixels")
        || metric_name.contains("Primitives")
        || metric_name.contains("Samples")
        || metric_name.contains("Vertices")
        || metric_name.contains("Triangles")
    {
        Some("count")
    } else {
        None
    }
}

fn counter_metric_aggregation(unit: Option<&str>) -> &'static str {
    if unit == Some("bytes") {
        "sum"
    } else {
        "average"
    }
}

fn counter_metric_max_value(metric_name: &str) -> f64 {
    match counter_metric_unit(metric_name) {
        Some("%") => 10_000.0,
        Some("GB/s") => 10_000.0,
        Some("bytes") => 1.0e18,
        Some("count") => 1.0e15,
        _ => 1.0e12,
    }
}

fn median_sorted(values: &[f64]) -> f64 {
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn counter_metric_confidence(
    record_count: usize,
    sample_count: usize,
    min_value: f64,
    max_value: f64,
) -> f64 {
    let record_confidence = (record_count as f64 / 4.0).min(1.0);
    let sample_confidence = (sample_count as f64 / 12.0).min(1.0);
    let spread_confidence = if max_value <= f64::EPSILON {
        0.0
    } else {
        1.0 - ((max_value - min_value).abs() / max_value).min(1.0)
    };
    (0.45 * record_confidence + 0.35 * sample_confidence + 0.20 * spread_confidence).min(1.0)
}

fn classify_record_metrics(record: &[u8], limiter: &mut CounterLimiter) {
    let percent_values = extract_float_values(record, 0.001, 100.0, 64);
    let bandwidth_values = extract_float_values(record, 0.1, 20.0, 24);

    let mut high_values = percent_values
        .iter()
        .copied()
        .filter(|value| (10.0..=100.0).contains(value))
        .collect::<Vec<_>>();
    high_values.sort_by(|left, right| right.partial_cmp(left).unwrap());
    for value in high_values {
        if limiter.occupancy_manager.is_none() && value >= 50.0 {
            limiter.occupancy_manager = Some(value);
            continue;
        }
        if limiter.alu_utilization.is_none() {
            limiter.alu_utilization = Some(value);
        }
    }

    let mut tiny_values = percent_values
        .iter()
        .copied()
        .filter(|value| (0.001..=0.25).contains(value))
        .collect::<Vec<_>>();
    tiny_values.sort_by(|left, right| right.partial_cmp(left).unwrap());
    for value in tiny_values {
        if limiter.compute_shader_launch.is_none() {
            limiter.compute_shader_launch = Some(value);
            continue;
        }
        if limiter.l1_cache.is_none() {
            limiter.l1_cache = Some(value);
            continue;
        }
        if limiter.last_level_cache.is_none() {
            limiter.last_level_cache = Some(value);
            continue;
        }
        if limiter.control_flow.is_none() {
            limiter.control_flow = Some(value);
        }
    }

    let mut medium_values = percent_values
        .iter()
        .copied()
        .filter(|value| (0.25..=10.0).contains(value))
        .collect::<Vec<_>>();
    medium_values.sort_by(|left, right| right.partial_cmp(left).unwrap());
    for value in medium_values {
        if limiter.f32_limiter.is_none() && value >= 4.0 {
            limiter.f32_limiter = Some(value);
            continue;
        }
        if limiter.integer_complex.is_none() && value >= 1.0 {
            limiter.integer_complex = Some(value);
            continue;
        }
        if limiter.instruction_throughput.is_none() {
            limiter.instruction_throughput = Some(value);
            continue;
        }
        if limiter.control_flow.is_none() {
            limiter.control_flow = Some(value);
        }
    }

    let mut bandwidth_candidates = bandwidth_values
        .into_iter()
        .filter(|value| value.is_finite() && *value >= 0.1)
        .collect::<Vec<_>>();
    bandwidth_candidates.sort_by(|left, right| right.partial_cmp(left).unwrap());
    for value in bandwidth_candidates {
        if limiter.device_memory_bandwidth_gbps.is_none() && value >= 1.0 {
            limiter.device_memory_bandwidth_gbps = Some(value);
            continue;
        }
        if limiter.buffer_l1_read_bandwidth_gbps.is_none() {
            limiter.buffer_l1_read_bandwidth_gbps = Some(value);
            continue;
        }
        if limiter.buffer_l1_write_bandwidth_gbps.is_none() {
            limiter.buffer_l1_write_bandwidth_gbps = Some(value);
        }
    }
}

fn find_record_starts(data: &[u8]) -> Vec<usize> {
    let mut starts = Vec::new();
    for i in 0..data.len().saturating_sub(mem::size_of::<u32>()) {
        if data[i..].starts_with(&[0x4e, 0x00, 0x00, 0x00]) {
            starts.push(i);
        }
    }
    starts
}

fn extract_float_values(data: &[u8], min: f64, max: f64, max_count: usize) -> Vec<f64> {
    let mut values = Vec::new();
    let mut seen = Vec::<u32>::new();
    for chunk in data.chunks_exact(mem::size_of::<u32>()) {
        if values.len() >= max_count {
            break;
        }
        let bits = u32::from_le_bytes(chunk.try_into().unwrap());
        let value = f32::from_bits(bits) as f64;
        if value.is_finite() && value >= min && value <= max && !seen.contains(&bits) {
            seen.push(bits);
            values.push(value);
        }
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_record(values: &[f32]) -> Vec<u8> {
        let mut record = vec![0u8; 464];
        record[0..4].copy_from_slice(&0x4e_u32.to_le_bytes());
        for (index, value) in values.iter().enumerate() {
            let offset = 4 + index * 4;
            record[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        }
        record
    }

    fn byte_sample_record(values: &[u64]) -> Vec<u8> {
        let mut record = vec![0u8; 464];
        record[0..4].copy_from_slice(&0x4e_u32.to_le_bytes());
        for (index, value) in values.iter().enumerate() {
            let offset = 8 + index * 8;
            record[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }
        record
    }

    #[test]
    fn extracts_limiters_from_counter_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("Counters_f_33.raw");

        let mut data = vec![0u8; 2400];
        data[0..4].copy_from_slice(&0x4e_u32.to_le_bytes());
        data.extend_from_slice(&sample_record(&[
            72.0, 58.0, 0.18, 0.12, 0.09, 0.04, 6.5, 2.4, 1.2, 8.2, 2.3, 0.7,
        ]));
        fs::write(&path, data).unwrap();

        let limiters = extract_limiters(dir.path());
        assert_eq!(limiters.len(), 1);
        assert_eq!(limiters[0].encoder_index, 0);
        assert_eq!(limiters[0].occupancy_manager, Some(72.0));
        assert_eq!(limiters[0].alu_utilization, Some(58.0));
        assert!((limiters[0].compute_shader_launch.unwrap() - 0.18).abs() < 0.001);
        assert!((limiters[0].l1_cache.unwrap() - 0.12).abs() < 0.001);
        assert!((limiters[0].last_level_cache.unwrap() - 0.09).abs() < 0.001);
        assert!((limiters[0].control_flow.unwrap() - 0.04).abs() < 0.001);
        assert!((limiters[0].f32_limiter.unwrap() - 8.2).abs() < 0.001);
        assert!((limiters[0].integer_complex.unwrap() - 6.5).abs() < 0.001);
        assert!((limiters[0].instruction_throughput.unwrap() - 2.4).abs() < 0.001);
        assert!((limiters[0].device_memory_bandwidth_gbps.unwrap() - 8.2).abs() < 0.001);
        assert!((limiters[0].buffer_l1_read_bandwidth_gbps.unwrap() - 6.5).abs() < 0.001);
        assert!((limiters[0].buffer_l1_write_bandwidth_gbps.unwrap() - 2.4).abs() < 0.001);
    }

    #[test]
    fn maps_counter_file_indices_using_go_csv_order() {
        assert_eq!(counter_file_metric_name(3), None);
        assert_eq!(counter_file_metric_name(12), Some("ALU Utilization"));
        assert_eq!(
            counter_file_metric_name(33),
            Some("Compute Shader Launch Limiter")
        );
        assert_eq!(counter_file_metric_name(107), Some("Kernel Occupancy"));
    }

    #[test]
    fn extracts_named_counter_file_metrics() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("Counters_f_12.raw");

        let mut data = vec![0u8; 2400];
        data[0..4].copy_from_slice(&0x4e_u32.to_le_bytes());
        data.extend_from_slice(&sample_record(&[12.0, 16.0, 20.0]));
        data.extend_from_slice(&sample_record(&[14.0, 18.0, 22.0]));
        fs::write(&path, data).unwrap();

        let metrics = extract_counter_file_metrics(dir.path());
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].file_index, 12);
        assert_eq!(metrics[0].metric_name, "ALU Utilization");
        assert_eq!(metrics[0].unit.as_deref(), Some("%"));
        assert_eq!(metrics[0].encoder_index, 0);
        assert_eq!(metrics[0].record_count, 2);
        assert_eq!(metrics[0].sample_count, 6);
        assert_eq!(metrics[0].aggregation, "average");
        assert_eq!(metrics[0].total_value, 102.0);
        assert_eq!(metrics[0].representative_value, 17.0);
        assert_eq!(metrics[0].min_value, 12.0);
        assert_eq!(metrics[0].max_value, 22.0);
        assert_eq!(metrics[0].mean_value, 17.0);
        assert!(metrics[0].confidence > 0.5);
    }

    #[test]
    fn sums_byte_counter_file_metrics_like_go() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("Counters_f_28.raw");

        let mut data = vec![0u8; 2400];
        data[0..4].copy_from_slice(&0x4e_u32.to_le_bytes());
        data.extend_from_slice(&byte_sample_record(&[4096, 8192]));
        data.extend_from_slice(&byte_sample_record(&[16384]));
        fs::write(&path, data).unwrap();

        let metrics = extract_counter_file_metrics(dir.path());
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].file_index, 28);
        assert_eq!(metrics[0].metric_name, "Bytes Read From Device Memory");
        assert_eq!(metrics[0].unit.as_deref(), Some("bytes"));
        assert_eq!(metrics[0].aggregation, "sum");
        assert_eq!(metrics[0].sample_count, 3);
        assert_eq!(metrics[0].total_value, 28_672.0);
        assert_eq!(metrics[0].representative_value, 28_672.0);
        assert!((metrics[0].mean_value - 9557.333).abs() < 0.01);
    }

    #[test]
    fn detects_gprw_record_size_from_magic_stride() {
        let mut data = Vec::new();
        for index in 0..3u64 {
            let mut record = vec![0u8; 64];
            record[0..8].copy_from_slice(b"GPRWCNTR");
            record[8..16].copy_from_slice(&(100 + index).to_le_bytes());
            record[16..24].copy_from_slice(&(200 + index).to_le_bytes());
            data.extend_from_slice(&record);
        }

        assert_eq!(gprw_record_info(&data), Some((64, 3)));
        let stats = summarize_gprw_u64_columns(&data, 64);
        assert_eq!(stats[0].min, u64::from_le_bytes(*b"GPRWCNTR"));
        assert_eq!(stats[1].first_values, vec![100, 101, 102]);
        assert_eq!(stats[2].first_values, vec![200, 201, 202]);
    }
}
