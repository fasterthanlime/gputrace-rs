use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Cursor;
use std::mem;
use std::path::{Path, PathBuf};

use plist::{Dictionary, Uid, Value};
use rquickjs::{Context, Runtime};
use serde::Serialize;

use crate::counter_names::ALL_COUNTER_NAMES;
use crate::profiler;
use crate::trace::TraceBundle;
use crate::xcode_counters;

type SampleBlob = (String, Vec<u8>);
type CounterSchemaByGroup = BTreeMap<usize, Vec<String>>;

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
    pub aggregate_metadata: Vec<RawCounterAggregateMetadata>,
    pub stream_archives: Vec<RawCounterStreamArchive>,
    pub structured_layouts: Vec<RawCounterStructuredLayout>,
    pub normalized_counters: Vec<RawCounterNormalizedMetric>,
    pub normalized_matches: Vec<RawCounterNormalizedMatch>,
    pub structured_samples: Vec<RawCounterStructuredSample>,
    pub files: Vec<RawCounterProbeFile>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCountersReport {
    pub trace_source: PathBuf,
    pub profiler_directory: PathBuf,
    pub aggregate_metadata: Vec<RawCounterAggregateMetadata>,
    pub schemas: Vec<RawCounterSchema>,
    pub streams: Vec<RawCounterDecodedStream>,
    pub metrics: Vec<RawCounterDecodedMetric>,
    pub derived_metrics: Vec<RawCounterJsDerivedMetric>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterSchema {
    pub sample_group: usize,
    pub counter_count: usize,
    pub counter_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterDecodedStream {
    pub path: String,
    pub sample_group: Option<usize>,
    pub source_index: Option<usize>,
    pub ring_index: Option<usize>,
    pub byte_len: usize,
    pub record_size: usize,
    pub record_count: usize,
    pub counter_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterDecodedMetric {
    pub path: String,
    pub sample_group: Option<usize>,
    pub source_index: Option<usize>,
    pub ring_index: Option<usize>,
    pub counter_index: usize,
    pub raw_name: String,
    pub sample_count: usize,
    pub min_percent_of_gpu_cycles: f64,
    pub mean_percent_of_gpu_cycles: f64,
    pub max_percent_of_gpu_cycles: f64,
    pub encoder_ids: Vec<u64>,
    pub kick_trace_ids: Vec<u64>,
    pub source_ids: Vec<u64>,
    pub derived_counter_matches: Vec<RawCounterDerivedCounterMatch>,
    pub hardware_selectors: Vec<RawCounterHardwareSelector>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RawCounterDerivedCounterMatch {
    pub key: String,
    pub name: String,
    pub counter_type: Option<String>,
    pub description: Option<String>,
    pub sources: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RawCounterHardwareSelector {
    pub partition: Option<u64>,
    pub select: Option<u64>,
    pub flag: Option<u64>,
    pub sources: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterJsDerivedMetric {
    pub key: String,
    pub name: String,
    pub counter_type: Option<String>,
    pub description: Option<String>,
    pub value: f64,
    pub source_script: PathBuf,
    pub source_catalog: PathBuf,
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
pub struct RawCounterAggregateMetadata {
    pub archive_index: usize,
    pub timebase_numer: Option<u64>,
    pub timebase_denom: Option<u64>,
    pub num_encoders: Option<u64>,
    pub perf_info: BTreeMap<String, u64>,
    pub encoder_sample_indices: Vec<RawCounterEncoderSampleIndex>,
    pub encoder_infos: Vec<RawCounterEncoderInfo>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterEncoderSampleIndex {
    pub row_index: usize,
    pub word0: u32,
    pub word1: u32,
    pub sample_index: u32,
    pub word3: u32,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterEncoderInfo {
    pub row_index: usize,
    pub trace_ids: Vec<u32>,
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
    pub sample_group: Option<usize>,
    pub source_index: Option<usize>,
    pub ring_index: Option<usize>,
    pub encoder_ids: Vec<u64>,
    pub kick_trace_ids: Vec<u64>,
    pub source_ids: Vec<u64>,
    pub counter_index: usize,
    pub raw_name: String,
    pub sample_count: usize,
    pub min_percent: f64,
    pub mean_percent: f64,
    pub max_percent: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RawCounterNormalizedMatch {
    pub metric: String,
    pub row_index: usize,
    pub encoder_label: String,
    pub target: f64,
    pub delta: f64,
    pub tolerance: f64,
    pub confidence: f64,
    pub counter: RawCounterNormalizedMetric,
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
    scan_files: bool,
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

    let mut file_reports = Vec::new();
    if scan_files {
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
    }

    let normalized_counters = probe_normalized_counter_metrics(&trace.path);
    let normalized_matches = match_normalized_counter_targets(&normalized_counters, &targets);
    let structured_samples = probe_structured_counter_samples(&trace.path, &targets);

    Ok(RawCounterProbeReport {
        profiler_directory,
        csv_source: csv_data.map(|data| data.source),
        targets,
        aggregate_metadata: probe_aggregate_counter_metadata(&trace.path),
        stream_archives: probe_stream_archives(&trace.path),
        structured_layouts: probe_structured_counter_layouts(&trace.path),
        normalized_counters,
        normalized_matches,
        structured_samples,
        files: file_reports,
    })
}

pub fn raw_counters_report(trace: &TraceBundle) -> crate::Result<RawCountersReport> {
    let profiler_directory = profiler::find_profiler_directory(&trace.path)
        .ok_or_else(|| crate::Error::NotFound(trace.path.clone()))?;
    let (schema_map, fallback_counter_names, _) =
        counter_schemas_and_sample_blobs(&trace.path).unwrap_or_default();
    let catalog = load_agx_counter_catalog();
    let aggregate_metadata = probe_aggregate_counter_metadata(&trace.path);
    let structured_layouts = probe_structured_counter_layouts(&trace.path);
    let normalized_counters = probe_normalized_counter_metrics(&trace.path);
    let js_variables = raw_counter_js_variables(&trace.path);
    let device_identifier = trace_agx_device_identifier(&trace.path);
    let derived_metrics =
        evaluate_agx_derived_metrics(&catalog, &js_variables, device_identifier.as_deref());
    let schemas = schema_map
        .iter()
        .map(|(sample_group, counter_names)| RawCounterSchema {
            sample_group: *sample_group,
            counter_count: counter_names.len(),
            counter_names: counter_names.clone(),
        })
        .collect::<Vec<_>>();
    let streams = structured_layouts
        .iter()
        .map(|layout| {
            let path_ids = parse_derived_counter_sample_path(&layout.path);
            RawCounterDecodedStream {
                path: layout.path.clone(),
                sample_group: path_ids.sample_group,
                source_index: path_ids.source_index,
                ring_index: path_ids.ring_index,
                byte_len: layout.byte_len,
                record_size: layout.gprw_record_size,
                record_count: layout.gprw_record_count,
                counter_count: path_ids
                    .sample_group
                    .and_then(|group| schema_map.get(&group))
                    .map(Vec::len),
            }
        })
        .collect::<Vec<_>>();
    let metrics = normalized_counters
        .into_iter()
        .map(|metric| {
            let derived_counter_matches = catalog
                .derived_by_hash
                .get(&metric.raw_name)
                .cloned()
                .unwrap_or_default();
            let hardware_selectors = catalog
                .hardware_by_hash
                .get(&metric.raw_name)
                .cloned()
                .unwrap_or_default();
            RawCounterDecodedMetric {
                path: metric.path,
                sample_group: metric.sample_group,
                source_index: metric.source_index,
                ring_index: metric.ring_index,
                counter_index: metric.counter_index,
                raw_name: metric.raw_name,
                sample_count: metric.sample_count,
                min_percent_of_gpu_cycles: metric.min_percent,
                mean_percent_of_gpu_cycles: metric.mean_percent,
                max_percent_of_gpu_cycles: metric.max_percent,
                encoder_ids: metric.encoder_ids,
                kick_trace_ids: metric.kick_trace_ids,
                source_ids: metric.source_ids,
                derived_counter_matches,
                hardware_selectors,
            }
        })
        .collect::<Vec<_>>();
    let mut warnings = Vec::new();
    if schemas.is_empty() && !fallback_counter_names.is_empty() {
        warnings.push(
            "using fallback limiter sample counter names; per-pass schemas were not found"
                .to_owned(),
        );
    }
    if aggregate_metadata.is_empty() {
        warnings.push("no aggregate APSCounterData metadata was decoded".to_owned());
    }
    if metrics.is_empty() {
        warnings.push("no normalized GPRWCNTR counter metrics were decoded".to_owned());
    }
    if !js_variables.is_empty() && derived_metrics.is_empty() {
        warnings.push("no AGX JavaScript derived counters evaluated to finite values".to_owned());
    }

    Ok(RawCountersReport {
        trace_source: trace.path.clone(),
        profiler_directory,
        aggregate_metadata,
        schemas,
        streams,
        metrics,
        derived_metrics,
        warnings,
    })
}

pub fn format_raw_counters_report(report: &RawCountersReport) -> String {
    let mut out = String::new();
    out.push_str("Raw counter report\n");
    out.push_str(&format!(
        "trace={} profiler_directory={}\n",
        report.trace_source.display(),
        report.profiler_directory.display()
    ));
    out.push_str(&format!(
        "metadata={} schemas={} streams={} metrics={} derived_metrics={}\n\n",
        report.aggregate_metadata.len(),
        report.schemas.len(),
        report.streams.len(),
        report.metrics.len(),
        report.derived_metrics.len()
    ));
    for warning in &report.warnings {
        out.push_str(&format!("warning: {warning}\n"));
    }
    if !report.warnings.is_empty() {
        out.push('\n');
    }
    for metadata in &report.aggregate_metadata {
        out.push_str(&format!(
            "APSCounterData[{}]: timebase={}/{} num_encoders={} perf={:?}\n",
            metadata.archive_index,
            metadata
                .timebase_numer
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            metadata
                .timebase_denom
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            metadata
                .num_encoders
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            metadata.perf_info
        ));
    }
    if !report.schemas.is_empty() {
        out.push_str("\nSchemas:\n");
        for schema in &report.schemas {
            out.push_str(&format!(
                "  group {}: counters={}\n",
                schema.sample_group, schema.counter_count
            ));
        }
    }
    if !report.streams.is_empty() {
        out.push_str("\nStreams:\n");
        for stream in report.streams.iter().take(24) {
            out.push_str(&format!(
                "  group={} source={} ring={} records={} record_size={} counters={} {}\n",
                format_optional_usize(stream.sample_group),
                format_optional_usize(stream.source_index),
                format_optional_usize(stream.ring_index),
                stream.record_count,
                stream.record_size,
                stream
                    .counter_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                stream.path
            ));
        }
    }
    let plausible_metrics = report
        .metrics
        .iter()
        .filter(|metric| {
            metric.mean_percent_of_gpu_cycles.is_finite()
                && metric.mean_percent_of_gpu_cycles >= 0.0
                && metric.mean_percent_of_gpu_cycles <= 500.0
                && metric.max_percent_of_gpu_cycles.is_finite()
                && metric.max_percent_of_gpu_cycles <= 500.0
        })
        .collect::<Vec<_>>();
    if !plausible_metrics.is_empty() {
        out.push_str("\nTop cycle-normalized counters (bounded estimate):\n");
        for metric in plausible_metrics.iter().take(32) {
            out.push_str(&format!(
                "  group={} source={} ring={} [{}] samples={} mean={:.2}% min={:.2}% max={:.2}% {}\n",
                format_optional_usize(metric.sample_group),
                format_optional_usize(metric.source_index),
                format_optional_usize(metric.ring_index),
                metric.counter_index,
                metric.sample_count,
                metric.mean_percent_of_gpu_cycles,
                metric.min_percent_of_gpu_cycles,
                metric.max_percent_of_gpu_cycles,
                format_raw_counter_metric_label(metric)
            ));
        }
    }
    if !report.derived_metrics.is_empty() {
        let mut derived_metrics = report
            .derived_metrics
            .iter()
            .filter(|metric| metric.value.is_finite())
            .collect::<Vec<_>>();
        derived_metrics.sort_by(|left, right| {
            right
                .value
                .abs()
                .partial_cmp(&left.value.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.name.cmp(&right.name))
        });
        out.push_str("\nTop AGX JavaScript-derived counters:\n");
        for metric in derived_metrics.iter().take(32) {
            out.push_str(&format!(
                "  {:>12.4} {} ({}) type={} script={}\n",
                metric.value,
                metric.name,
                metric.key,
                metric.counter_type.as_deref().unwrap_or("-"),
                metric
                    .source_script
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("-")
            ));
        }
    }
    let unbounded_count = report
        .metrics
        .iter()
        .filter(|metric| {
            !metric.mean_percent_of_gpu_cycles.is_finite()
                || metric.mean_percent_of_gpu_cycles < 0.0
                || metric.mean_percent_of_gpu_cycles > 500.0
                || !metric.max_percent_of_gpu_cycles.is_finite()
                || metric.max_percent_of_gpu_cycles > 500.0
        })
        .count();
    if unbounded_count > 0 {
        out.push_str(&format!(
            "\n{unbounded_count} decoded counters are outside the bounded percent-like range; use --format json/csv for the full raw-id table.\n"
        ));
    }
    out
}

pub fn format_raw_counters_csv(report: &RawCountersReport) -> String {
    let mut out = String::new();
    out.push_str("path,sample_group,source_index,ring_index,counter_index,raw_name,derived_counter_names,derived_counter_keys,hardware_selector_count,sample_count,min_percent_of_gpu_cycles,mean_percent_of_gpu_cycles,max_percent_of_gpu_cycles,encoder_id_count,kick_trace_id_count,source_id_count\n");
    for metric in &report.metrics {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{},{},{}\n",
            csv_escape(&metric.path),
            optional_usize_csv(metric.sample_group),
            optional_usize_csv(metric.source_index),
            optional_usize_csv(metric.ring_index),
            metric.counter_index,
            csv_escape(&metric.raw_name),
            csv_escape(&derived_counter_names(metric)),
            csv_escape(&derived_counter_keys(metric)),
            metric.hardware_selectors.len(),
            metric.sample_count,
            metric.min_percent_of_gpu_cycles,
            metric.mean_percent_of_gpu_cycles,
            metric.max_percent_of_gpu_cycles,
            metric.encoder_ids.len(),
            metric.kick_trace_ids.len(),
            metric.source_ids.len()
        ));
    }
    out
}

fn format_raw_counter_metric_label(metric: &RawCounterDecodedMetric) -> String {
    let Some(first) = metric.derived_counter_matches.first() else {
        return metric.raw_name.clone();
    };
    let extra = metric.derived_counter_matches.len().saturating_sub(1);
    if extra == 0 {
        format!("{} ({})", first.name, metric.raw_name)
    } else {
        format!("{} +{} ({})", first.name, extra, metric.raw_name)
    }
}

fn derived_counter_names(metric: &RawCounterDecodedMetric) -> String {
    metric
        .derived_counter_matches
        .iter()
        .map(|matched| matched.name.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join("|")
}

fn derived_counter_keys(metric: &RawCounterDecodedMetric) -> String {
    metric
        .derived_counter_matches
        .iter()
        .map(|matched| matched.key.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join("|")
}

fn optional_usize_csv(value: Option<usize>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

#[derive(Debug, Default)]
struct RawCounterCatalog {
    derived_by_hash: BTreeMap<String, Vec<RawCounterDerivedCounterMatch>>,
    hardware_by_hash: BTreeMap<String, Vec<RawCounterHardwareSelector>>,
    derived_definitions: Vec<RawCounterDerivedDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RawCounterDerivedCounterMatchKey {
    key: String,
    name: String,
    counter_type: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RawCounterHardwareSelectorKey {
    partition: Option<u64>,
    select: Option<u64>,
    flag: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RawCounterDerivedDefinition {
    key: String,
    name: String,
    counter_type: Option<String>,
    description: Option<String>,
    source_catalog: PathBuf,
    source_script: Option<PathBuf>,
}

fn load_agx_counter_catalog() -> RawCounterCatalog {
    let mut statistics_files = Vec::new();
    let mut perf_files = Vec::new();
    let mut derived_script_files = Vec::new();
    collect_agx_counter_catalog_files(
        Path::new("/System/Library/Extensions"),
        &mut statistics_files,
        &mut perf_files,
        &mut derived_script_files,
    );
    let derived_script_by_stem = derived_script_files
        .into_iter()
        .filter_map(|path| agx_statistics_stem(&path).map(|stem| (stem, path)))
        .collect::<BTreeMap<_, _>>();

    let mut derived =
        BTreeMap::<String, BTreeMap<RawCounterDerivedCounterMatchKey, BTreeSet<PathBuf>>>::new();
    let mut derived_definitions = BTreeSet::<RawCounterDerivedDefinition>::new();
    for path in statistics_files {
        let script_path = agx_statistics_stem(&path)
            .and_then(|stem| derived_script_by_stem.get(&stem))
            .cloned();
        add_statistics_counter_catalog(
            &path,
            script_path.as_deref(),
            &mut derived,
            &mut derived_definitions,
        );
    }

    let mut hardware =
        BTreeMap::<String, BTreeMap<RawCounterHardwareSelectorKey, BTreeSet<PathBuf>>>::new();
    for path in perf_files {
        add_perf_counter_catalog(&path, &mut hardware);
    }

    RawCounterCatalog {
        derived_by_hash: derived
            .into_iter()
            .map(|(hash, matches)| {
                let matches = matches
                    .into_iter()
                    .map(|(key, sources)| RawCounterDerivedCounterMatch {
                        key: key.key,
                        name: key.name,
                        counter_type: key.counter_type,
                        description: key.description,
                        sources: sources.into_iter().collect(),
                    })
                    .collect();
                (hash, matches)
            })
            .collect(),
        hardware_by_hash: hardware
            .into_iter()
            .map(|(hash, selectors)| {
                let selectors = selectors
                    .into_iter()
                    .map(|(key, sources)| RawCounterHardwareSelector {
                        partition: key.partition,
                        select: key.select,
                        flag: key.flag,
                        sources: sources.into_iter().collect(),
                    })
                    .collect();
                (hash, selectors)
            })
            .collect(),
        derived_definitions: derived_definitions.into_iter().collect(),
    }
}

fn collect_agx_counter_catalog_files(
    directory: &Path,
    statistics_files: &mut Vec<PathBuf>,
    perf_files: &mut Vec<PathBuf>,
    derived_script_files: &mut Vec<PathBuf>,
) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_agx_counter_catalog_files(
                &path,
                statistics_files,
                perf_files,
                derived_script_files,
            );
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|file_name| file_name.to_str()) else {
            continue;
        };
        if file_name.starts_with("AGXMetalStatisticsExternal")
            && file_name.ends_with("-counters.plist")
        {
            statistics_files.push(path);
        } else if file_name.starts_with("AGXMetalStatisticsExternal")
            && file_name.ends_with("-derived.js")
        {
            derived_script_files.push(path);
        } else if file_name == "AGXMetalPerfCountersExternal.plist" {
            perf_files.push(path);
        }
    }
}

fn add_statistics_counter_catalog(
    path: &Path,
    script_path: Option<&Path>,
    derived: &mut BTreeMap<String, BTreeMap<RawCounterDerivedCounterMatchKey, BTreeSet<PathBuf>>>,
    derived_definitions: &mut BTreeSet<RawCounterDerivedDefinition>,
) {
    let Ok(value) = Value::from_file(path) else {
        return;
    };
    let Some(root) = value.as_dictionary() else {
        return;
    };
    let Some(counters) = root.get("DerivedCounters").and_then(Value::as_dictionary) else {
        return;
    };

    for (key, value) in counters {
        let Some(counter) = value.as_dictionary() else {
            continue;
        };
        let Some(raw_hashes) = counter.get("counters").and_then(Value::as_array) else {
            continue;
        };
        let name = counter
            .get("name")
            .and_then(Value::as_string)
            .unwrap_or(key)
            .to_owned();
        let counter_type = counter
            .get("type")
            .and_then(Value::as_string)
            .map(str::to_owned);
        let description = counter
            .get("description")
            .and_then(Value::as_string)
            .map(str::to_owned);
        let match_key = RawCounterDerivedCounterMatchKey {
            key: key.clone(),
            name: name.clone(),
            counter_type: counter_type.clone(),
            description: description.clone(),
        };
        derived_definitions.insert(RawCounterDerivedDefinition {
            key: key.clone(),
            name,
            counter_type,
            description,
            source_catalog: path.to_path_buf(),
            source_script: script_path.map(Path::to_path_buf),
        });
        for raw_hash in raw_hashes.iter().filter_map(Value::as_string) {
            derived
                .entry(raw_hash.to_owned())
                .or_default()
                .entry(match_key.clone())
                .or_default()
                .insert(path.to_path_buf());
        }
    }
}

fn agx_statistics_stem(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    file_name
        .strip_suffix("-counters.plist")
        .or_else(|| file_name.strip_suffix("-derived.js"))
        .map(str::to_owned)
}

fn evaluate_agx_derived_metrics(
    catalog: &RawCounterCatalog,
    variables: &BTreeMap<String, f64>,
    device_identifier: Option<&str>,
) -> Vec<RawCounterJsDerivedMetric> {
    if variables.is_empty() {
        return Vec::new();
    }

    let definitions_by_script = catalog
        .derived_definitions
        .iter()
        .filter_map(|definition| {
            definition
                .source_script
                .as_ref()
                .map(|script| (script.clone(), definition.clone()))
        })
        .fold(
            BTreeMap::<PathBuf, Vec<RawCounterDerivedDefinition>>::new(),
            |mut grouped, (script, definition)| {
                grouped.entry(script).or_default().push(definition);
                grouped
            },
        );

    let Some((script_path, definitions)) =
        choose_agx_derived_script(definitions_by_script, device_identifier)
    else {
        return Vec::new();
    };
    let mut metrics = Vec::new();
    {
        let Ok(script_source) = fs::read_to_string(&script_path) else {
            return Vec::new();
        };
        metrics.extend(evaluate_agx_derived_script(
            &script_path,
            &script_source,
            &definitions,
            variables,
        ));
    }
    metrics.sort_by(|left, right| {
        right
            .value
            .abs()
            .partial_cmp(&left.value.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.key.cmp(&right.key))
    });
    metrics
}

fn choose_agx_derived_script(
    definitions_by_script: BTreeMap<PathBuf, Vec<RawCounterDerivedDefinition>>,
    device_identifier: Option<&str>,
) -> Option<(PathBuf, Vec<RawCounterDerivedDefinition>)> {
    definitions_by_script.into_iter().max_by(
        |(left_path, left_definitions), (right_path, right_definitions)| {
            agx_derived_script_score(left_path, left_definitions, device_identifier)
                .cmp(&agx_derived_script_score(
                    right_path,
                    right_definitions,
                    device_identifier,
                ))
                .then_with(|| right_path.cmp(left_path))
        },
    )
}

fn agx_derived_script_score(
    path: &Path,
    definitions: &[RawCounterDerivedDefinition],
    device_identifier: Option<&str>,
) -> (u8, u8, usize) {
    let stem = agx_statistics_stem(path).unwrap_or_default();
    let direct_match = device_identifier
        .filter(|identifier| stem.contains(identifier) || identifier.contains(&stem))
        .is_some() as u8;
    let compatibility = agx_derived_script_compatibility_rank(&stem, device_identifier);
    (direct_match, compatibility, definitions.len())
}

fn agx_derived_script_compatibility_rank(stem: &str, device_identifier: Option<&str>) -> u8 {
    if matches!(device_identifier, Some(identifier) if identifier.contains("G16X")) {
        return match () {
            _ if stem.contains("G14D") => 80,
            _ if stem.contains("G14C") => 70,
            _ if stem.contains("G14S") => 60,
            _ if stem.contains("G14G") => 50,
            _ => 0,
        };
    }
    match () {
        _ if stem.contains("G14D") => 40,
        _ if stem.contains("G14C") => 35,
        _ if stem.contains("G14S") => 30,
        _ if stem.contains("G14G") => 25,
        _ if stem.contains("A14X") => 15,
        _ if stem.contains("13_3") => 10,
        _ => 0,
    }
}

fn trace_agx_device_identifier(trace_path: &Path) -> Option<String> {
    let profiler_directory = profiler::find_profiler_directory(trace_path)?;
    let stream_data = profiler_directory.join("streamData");
    let data = fs::read(stream_data).ok()?;
    let text = String::from_utf8_lossy(&data);
    for marker in [
        "AGXMetalG16X",
        "AGXMetalG16G",
        "AGXMetalG14X",
        "AGXMetalG14G",
    ] {
        if text.contains(marker) {
            return Some(marker.trim_start_matches("AGXMetal").to_owned());
        }
    }
    None
}

fn evaluate_agx_derived_script(
    script_path: &Path,
    script_source: &str,
    definitions: &[RawCounterDerivedDefinition],
    variables: &BTreeMap<String, f64>,
) -> Vec<RawCounterJsDerivedMetric> {
    let Ok(runtime) = Runtime::new() else {
        return Vec::new();
    };
    let Ok(context) = Context::full(&runtime) else {
        return Vec::new();
    };

    context.with(|ctx| {
        let globals = ctx.globals();
        for (name, value) in variables {
            let _ = globals.set(name.as_str(), *value);
        }
        if ctx.eval::<(), _>(script_source).is_err() {
            return Vec::new();
        }

        let mut metrics = Vec::new();
        for definition in definitions {
            if !is_javascript_identifier(&definition.key) {
                continue;
            }
            let source = format!(
                "(function() {{ try {{ if (typeof {key} !== 'function') return null; const v = {key}(); return Number.isFinite(v) ? v : null; }} catch (e) {{ return null; }} }})()",
                key = definition.key
            );
            let Ok(value) = ctx.eval::<Option<f64>, _>(source.as_str()) else {
                continue;
            };
            let Some(value) = value else {
                continue;
            };
            if !value.is_finite() {
                continue;
            }
            metrics.push(RawCounterJsDerivedMetric {
                key: definition.key.clone(),
                name: definition.name.clone(),
                counter_type: definition.counter_type.clone(),
                description: definition.description.clone(),
                value,
                source_script: script_path.to_path_buf(),
                source_catalog: definition.source_catalog.clone(),
            });
        }
        metrics
    })
}

fn is_javascript_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first == '$' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch == '$' || ch.is_ascii_alphanumeric())
}

fn raw_counter_js_variables(trace_path: &Path) -> BTreeMap<String, f64> {
    let Some((counter_schemas, fallback_counter_names, sample_blobs)) =
        counter_schemas_and_sample_blobs(trace_path)
    else {
        return BTreeMap::new();
    };
    if counter_schemas.is_empty() && fallback_counter_names.is_empty() {
        return BTreeMap::new();
    }

    let mut accum = BTreeMap::<String, RawCounterJsVariableAccum>::new();
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
        let path_ids = parse_derived_counter_sample_path(&path);
        let Some(counter_names) = path_ids
            .sample_group
            .and_then(|group| counter_schemas.get(&group))
            .or((!fallback_counter_names.is_empty()).then_some(&fallback_counter_names))
        else {
            continue;
        };
        for (counter_index, raw_name) in counter_names.iter().enumerate() {
            let value_column = 8 + counter_index;
            for record in &records {
                let Some(value) = record.get(value_column).copied() else {
                    continue;
                };
                let normalized = record
                    .get(2)
                    .copied()
                    .filter(|denominator| *denominator != 0)
                    .map(|denominator| value as f64 / denominator as f64 * 100.0);
                accum
                    .entry(raw_name.clone())
                    .or_default()
                    .push(value as f64, normalized);
            }
        }
    }

    let mut variables = BTreeMap::new();
    for (raw_name, accum) in accum {
        let Some(raw_mean) = mean(&accum.raw_values) else {
            continue;
        };
        let normalized_mean = mean(&accum.normalized_values).unwrap_or(raw_mean);
        variables.insert(raw_name.clone(), raw_mean);
        variables.insert(format!("{raw_name}_norm"), normalized_mean);
    }
    variables
}

#[derive(Debug, Default)]
struct RawCounterJsVariableAccum {
    raw_values: Vec<f64>,
    normalized_values: Vec<f64>,
}

impl RawCounterJsVariableAccum {
    fn push(&mut self, raw: f64, normalized: Option<f64>) {
        self.raw_values.push(raw);
        if let Some(normalized) = normalized {
            self.normalized_values.push(normalized);
        }
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn add_perf_counter_catalog(
    path: &Path,
    hardware: &mut BTreeMap<String, BTreeMap<RawCounterHardwareSelectorKey, BTreeSet<PathBuf>>>,
) {
    let Ok(value) = Value::from_file(path) else {
        return;
    };
    let Some(root) = value.as_dictionary() else {
        return;
    };
    for (hash, value) in root {
        if !hash.starts_with('_') {
            continue;
        }
        let Some(selector) = value.as_dictionary() else {
            continue;
        };
        let key = RawCounterHardwareSelectorKey {
            partition: selector.get("Partition").and_then(plist_u64),
            select: selector.get("Select").and_then(plist_u64),
            flag: selector.get("Flag").and_then(plist_u64),
        };
        hardware
            .entry(hash.clone())
            .or_default()
            .entry(key)
            .or_default()
            .insert(path.to_path_buf());
    }
}

fn plist_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Integer(value) => value.as_unsigned(),
        _ => None,
    }
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

    if !report.aggregate_metadata.is_empty() {
        out.push_str("aggregate counter metadata\n");
        for metadata in &report.aggregate_metadata {
            out.push_str(&format!(
                "  APSCounterData[{}]: timebase={}/{} num_encoders={} perf={:?}\n",
                metadata.archive_index,
                metadata
                    .timebase_numer
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                metadata
                    .timebase_denom
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                metadata
                    .num_encoders
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                metadata.perf_info
            ));
            if !metadata.encoder_sample_indices.is_empty() {
                out.push_str("    encoder sample indices:");
                for row in metadata.encoder_sample_indices.iter().take(8) {
                    out.push_str(&format!(
                        " #{}=({}, {}, sample={}, {})",
                        row.row_index, row.word0, row.word1, row.sample_index, row.word3
                    ));
                }
                out.push('\n');
            }
            if !metadata.encoder_infos.is_empty() {
                out.push_str("    encoder infos:");
                for row in metadata.encoder_infos.iter().take(8) {
                    out.push_str(&format!(" #{}={:?}", row.row_index, row.trace_ids));
                }
                out.push('\n');
            }
        }
        out.push('\n');
    }

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
        if !report.normalized_matches.is_empty() {
            out.push_str("normalized counter target matches\n");
            for matched in report.normalized_matches.iter().take(32) {
                out.push_str(&format!(
                    "  {} row={} target={:.2}% delta={:.2}% confidence={:.2} -> {} group={} ring={} [{}] {}\n",
                    matched.metric,
                    matched.row_index,
                    matched.target,
                    matched.delta,
                    matched.confidence,
                    matched.counter.path,
                    format_optional_usize(matched.counter.sample_group),
                    format_optional_usize(matched.counter.ring_index),
                    matched.counter.counter_index,
                    matched.counter.raw_name
                ));
            }
            out.push('\n');
        }

        out.push_str("normalized derived counters\n");
        for metric in report.normalized_counters.iter().take(32) {
            out.push_str(&format!(
                "  {} group={} source={} ring={} encoders={} kicks={} sources={} [{}] {}: mean={:.2}% max={:.2}% samples={}\n",
                metric.path,
                format_optional_usize(metric.sample_group),
                format_optional_usize(metric.source_index),
                format_optional_usize(metric.ring_index),
                format_u64_hex_list(&metric.encoder_ids, 4),
                format_u64_hex_list(&metric.kick_trace_ids, 4),
                format_u64_hex_list(&metric.source_ids, 4),
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

fn format_optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

fn format_u64_hex_list(values: &[u64], limit: usize) -> String {
    if values.is_empty() {
        return "-".to_owned();
    }
    let mut formatted = values
        .iter()
        .take(limit)
        .map(|value| format!("0x{value:x}"))
        .collect::<Vec<_>>();
    if values.len() > limit {
        formatted.push(format!("+{}", values.len() - limit));
    }
    formatted.join("|")
}

fn probe_aggregate_counter_metadata(trace_path: &Path) -> Vec<RawCounterAggregateMetadata> {
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

    ns_data_array_from_root_key(objects, root, "APSCounterData")
        .into_iter()
        .enumerate()
        .filter_map(|(archive_index, bytes)| {
            let keyed = parse_keyed_archive_dictionary(&bytes)?;
            if !keyed.contains_key("Derived Counter Sample Data") {
                return None;
            }
            Some(RawCounterAggregateMetadata {
                archive_index,
                timebase_numer: array_integer(&keyed, "Timebase", 0),
                timebase_denom: array_integer(&keyed, "Timebase", 1),
                num_encoders: keyed
                    .get("Num Encoders")
                    .and_then(StreamArchiveValue::as_u64),
                perf_info: keyed
                    .get("Perf Info")
                    .and_then(StreamArchiveValue::as_dictionary)
                    .map(dictionary_u64_values)
                    .unwrap_or_default(),
                encoder_sample_indices: keyed
                    .get("Encoder Sample Index Data")
                    .and_then(StreamArchiveValue::as_data)
                    .map(parse_encoder_sample_indices)
                    .unwrap_or_default(),
                encoder_infos: keyed
                    .get("Encoder Infos")
                    .map(parse_encoder_infos)
                    .unwrap_or_default(),
            })
        })
        .collect()
}

fn array_integer(
    keyed: &BTreeMap<String, StreamArchiveValue>,
    key: &str,
    index: usize,
) -> Option<u64> {
    let StreamArchiveValue::Array(values) = keyed.get(key)? else {
        return None;
    };
    values.get(index).and_then(StreamArchiveValue::as_u64)
}

fn dictionary_u64_values(values: &BTreeMap<String, StreamArchiveValue>) -> BTreeMap<String, u64> {
    values
        .iter()
        .filter_map(|(key, value)| value.as_u64().map(|value| (key.clone(), value)))
        .collect()
}

fn parse_encoder_sample_indices(data: &[u8]) -> Vec<RawCounterEncoderSampleIndex> {
    data.chunks_exact(16)
        .enumerate()
        .map(|(row_index, chunk)| RawCounterEncoderSampleIndex {
            row_index,
            word0: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
            word1: u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
            sample_index: u32::from_le_bytes(chunk[8..12].try_into().unwrap()),
            word3: u32::from_le_bytes(chunk[12..16].try_into().unwrap()),
        })
        .collect()
}

fn parse_encoder_infos(value: &StreamArchiveValue) -> Vec<RawCounterEncoderInfo> {
    let StreamArchiveValue::Array(values) = value else {
        return Vec::new();
    };
    values
        .iter()
        .enumerate()
        .filter_map(|(row_index, value)| {
            let data = value.as_data()?;
            let trace_ids = data
                .chunks_exact(mem::size_of::<u32>())
                .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
                .collect::<Vec<_>>();
            Some(RawCounterEncoderInfo {
                row_index,
                trace_ids,
            })
        })
        .collect()
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
    let Some((counter_schemas, fallback_counter_names, sample_blobs)) =
        counter_schemas_and_sample_blobs(trace_path)
    else {
        return Vec::new();
    };
    if counter_schemas.is_empty() && fallback_counter_names.is_empty() {
        return Vec::new();
    }

    let mut accum = BTreeMap::<RawCounterMetricKey, RawCounterMetricAccum>::new();
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
        let path_ids = parse_derived_counter_sample_path(&path);
        let Some(counter_names) = path_ids
            .sample_group
            .and_then(|group| counter_schemas.get(&group))
            .or((!fallback_counter_names.is_empty()).then_some(&fallback_counter_names))
        else {
            continue;
        };
        for (counter_index, raw_name) in counter_names.iter().enumerate() {
            let value_column = 8 + counter_index;
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
                accum
                    .entry(RawCounterMetricKey {
                        path: path.clone(),
                        sample_group: path_ids.sample_group,
                        source_index: path_ids.source_index,
                        ring_index: path_ids.ring_index,
                        counter_index,
                        raw_name: raw_name.clone(),
                    })
                    .or_default()
                    .push(
                        value as f64 / denominator as f64 * 100.0,
                        record.get(4).copied(),
                        record.get(5).copied(),
                        record.get(7).copied(),
                    );
            }
        }
    }
    let mut metrics = accum
        .into_iter()
        .map(|(key, accum)| {
            let max_percent = accum.values.iter().copied().fold(0.0, f64::max);
            let min_percent = accum.values.iter().copied().fold(f64::INFINITY, f64::min);
            let mean_percent = accum.values.iter().sum::<f64>() / accum.values.len() as f64;
            RawCounterNormalizedMetric {
                path: key.path,
                sample_group: key.sample_group,
                source_index: key.source_index,
                ring_index: key.ring_index,
                encoder_ids: accum.encoder_ids.into_iter().collect(),
                kick_trace_ids: accum.kick_trace_ids.into_iter().collect(),
                source_ids: accum.source_ids.into_iter().collect(),
                counter_index: key.counter_index,
                raw_name: key.raw_name,
                sample_count: accum.values.len(),
                min_percent,
                mean_percent,
                max_percent,
            }
        })
        .collect::<Vec<_>>();
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

fn match_normalized_counter_targets(
    counters: &[RawCounterNormalizedMetric],
    targets: &[RawCounterProbeTarget],
) -> Vec<RawCounterNormalizedMatch> {
    let mut matches = Vec::new();
    for target in targets {
        if target.value.abs() < 5.0 {
            continue;
        }
        let tolerance = normalized_match_tolerance(target.value);
        for counter in counters {
            if counter.sample_count < 20 {
                continue;
            }
            let delta = (counter.mean_percent - target.value).abs();
            if delta > tolerance {
                continue;
            }
            matches.push(RawCounterNormalizedMatch {
                metric: target.metric.clone(),
                row_index: target.row_index,
                encoder_label: target.encoder_label.clone(),
                target: target.value,
                delta,
                tolerance,
                confidence: (1.0 - delta / tolerance).clamp(0.0, 1.0),
                counter: counter.clone(),
            });
        }
    }
    matches.sort_by(|left, right| {
        right
            .confidence
            .partial_cmp(&left.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.delta
                    .partial_cmp(&right.delta)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.metric.cmp(&right.metric))
            .then_with(|| left.row_index.cmp(&right.row_index))
    });
    matches
}

fn normalized_match_tolerance(target: f64) -> f64 {
    (target.abs() * 0.10).max(2.0)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RawCounterMetricKey {
    path: String,
    sample_group: Option<usize>,
    source_index: Option<usize>,
    ring_index: Option<usize>,
    counter_index: usize,
    raw_name: String,
}

#[derive(Debug, Default)]
struct RawCounterMetricAccum {
    values: Vec<f64>,
    encoder_ids: BTreeSet<u64>,
    kick_trace_ids: BTreeSet<u64>,
    source_ids: BTreeSet<u64>,
}

impl RawCounterMetricAccum {
    fn push(
        &mut self,
        value: f64,
        encoder_id: Option<u64>,
        kick_trace_id: Option<u64>,
        source_id: Option<u64>,
    ) {
        self.values.push(value);
        if let Some(encoder_id) = encoder_id {
            self.encoder_ids.insert(encoder_id);
        }
        if let Some(kick_trace_id) = kick_trace_id {
            self.kick_trace_ids.insert(kick_trace_id);
        }
        if let Some(source_id) = source_id {
            self.source_ids.insert(source_id);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DerivedCounterSamplePath {
    sample_group: Option<usize>,
    source_index: Option<usize>,
    ring_index: Option<usize>,
}

fn parse_derived_counter_sample_path(path: &str) -> DerivedCounterSamplePath {
    let mut parts = path.split('/');
    while let Some(part) = parts.next() {
        if part == "Derived Counter Sample Data" {
            return DerivedCounterSamplePath {
                sample_group: parts.next().and_then(|part| part.parse().ok()),
                source_index: parts.next().and_then(|part| part.parse().ok()),
                ring_index: parts.next().and_then(|part| part.parse().ok()),
            };
        }
    }
    DerivedCounterSamplePath {
        sample_group: None,
        source_index: None,
        ring_index: None,
    }
}

fn counter_schemas_and_sample_blobs(
    trace_path: &Path,
) -> Option<(CounterSchemaByGroup, Vec<String>, Vec<SampleBlob>)> {
    let profiler_dir = profiler::find_profiler_directory(trace_path)?;
    let stream_data_path = profiler_dir.join("streamData");
    let plist = Value::from_file(stream_data_path).ok()?;
    let archive = plist.as_dictionary()?;
    let objects = archive.get("$objects").and_then(Value::as_array)?;
    let root = objects.get(1).and_then(Value::as_dictionary)?;
    let counter_archives = ns_data_array_from_root_key(objects, root, "APSCounterData");

    let mut counter_schemas = BTreeMap::new();
    let mut fallback_counter_names = Vec::new();
    let mut sample_blobs = Vec::new();
    for (archive_index, bytes) in counter_archives.into_iter().enumerate() {
        let Some(keyed) = parse_keyed_archive_dictionary(&bytes) else {
            continue;
        };
        if fallback_counter_names.is_empty()
            && let Some(names) = keyed.get("limiter sample counters")
        {
            fallback_counter_names = string_array_values(names);
        }
        if counter_schemas.is_empty()
            && let Some(subdivided) = keyed.get("Subdivided Dictionary")
        {
            counter_schemas = counter_schemas_from_subdivided_dictionary(subdivided);
        }
        if let Some(samples) = keyed.get("Derived Counter Sample Data") {
            collect_data_blobs(
                &format!("APSCounterData[{archive_index}]/Derived Counter Sample Data"),
                samples,
                &mut sample_blobs,
            );
        }
    }
    Some((counter_schemas, fallback_counter_names, sample_blobs))
}

fn counter_schemas_from_subdivided_dictionary(value: &StreamArchiveValue) -> CounterSchemaByGroup {
    let mut schemas = BTreeMap::new();
    let Some(pass_list) = value
        .as_dictionary()
        .and_then(|values| values.get("passList"))
    else {
        return schemas;
    };
    let StreamArchiveValue::Array(passes) = pass_list else {
        return schemas;
    };
    for (group_index, pass) in passes.iter().enumerate() {
        let Some(names) = first_counter_schema(pass) else {
            continue;
        };
        let raw_names = names.into_iter().skip(7).collect::<Vec<_>>();
        if !raw_names.is_empty() {
            schemas.insert(group_index, raw_names);
        }
    }
    schemas
}

fn first_counter_schema(value: &StreamArchiveValue) -> Option<Vec<String>> {
    match value {
        StreamArchiveValue::Array(children) => {
            let direct = string_array_values(value);
            if direct.first().is_some_and(|name| name == "GRC_TIMESTAMP") && direct.len() > 7 {
                return Some(direct);
            }
            children.iter().find_map(first_counter_schema)
        }
        StreamArchiveValue::Dictionary(values) => values.values().find_map(first_counter_schema),
        _ => None,
    }
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
        StreamArchiveValue::Dictionary(values) => (
            "dictionary",
            Some(values.len()),
            values.keys().cloned().collect(),
        ),
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
    Dictionary(BTreeMap<String, StreamArchiveValue>),
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

    fn as_data(&self) -> Option<&[u8]> {
        match self {
            Self::Data(value) => Some(value),
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
            Self::Dictionary(values) => format!(
                "dictionary:{}:{}",
                values.len(),
                values.keys().cloned().collect::<Vec<_>>().join("|")
            ),
            Self::Other => "other".to_owned(),
        }
    }

    fn as_dictionary(&self) -> Option<&BTreeMap<String, StreamArchiveValue>> {
        match self {
            Self::Dictionary(values) => Some(values),
            _ => None,
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
    if let Some(dict) = value.as_dictionary()
        && let Some(values) = keyed_dictionary_values(objects, dict)
    {
        return StreamArchiveValue::Dictionary(values);
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
            | "Occupancy Manager Target"
            | "Compute Shader Launch Limiter"
            | "Instruction Throughput Limiter"
            | "Integer & Complex Limiter"
            | "Control Flow Limiter"
            | "F32 Limiter"
            | "L1 Cache Limiter"
            | "Last Level Cache Limiter"
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
    fn parses_derived_counter_sample_group_source_and_ring() {
        let path = "APSCounterData[44]/Derived Counter Sample Data/10/1/0";

        let parsed = parse_derived_counter_sample_path(path);

        assert_eq!(parsed.sample_group, Some(10));
        assert_eq!(parsed.source_index, Some(1));
        assert_eq!(parsed.ring_index, Some(0));
    }

    #[test]
    fn formats_raw_counter_report_without_unbounded_rows_in_text_summary() {
        let report = RawCountersReport {
            trace_source: PathBuf::from("trace.gputrace"),
            profiler_directory: PathBuf::from("trace.gputrace/raw"),
            aggregate_metadata: Vec::new(),
            schemas: vec![RawCounterSchema {
                sample_group: 0,
                counter_count: 2,
                counter_names: vec!["_hot".to_owned(), "_huge".to_owned()],
            }],
            streams: Vec::new(),
            metrics: vec![
                RawCounterDecodedMetric {
                    path: "APSCounterData[0]/Derived Counter Sample Data/0/1/0".to_owned(),
                    sample_group: Some(0),
                    source_index: Some(1),
                    ring_index: Some(0),
                    counter_index: 0,
                    raw_name: "_hot".to_owned(),
                    sample_count: 2,
                    min_percent_of_gpu_cycles: 1.0,
                    mean_percent_of_gpu_cycles: 12.0,
                    max_percent_of_gpu_cycles: 20.0,
                    encoder_ids: Vec::new(),
                    kick_trace_ids: Vec::new(),
                    source_ids: Vec::new(),
                    derived_counter_matches: Vec::new(),
                    hardware_selectors: Vec::new(),
                },
                RawCounterDecodedMetric {
                    path: "APSCounterData[0]/Derived Counter Sample Data/0/1/0".to_owned(),
                    sample_group: Some(0),
                    source_index: Some(1),
                    ring_index: Some(0),
                    counter_index: 1,
                    raw_name: "_huge".to_owned(),
                    sample_count: 2,
                    min_percent_of_gpu_cycles: 1.0,
                    mean_percent_of_gpu_cycles: 12.0,
                    max_percent_of_gpu_cycles: 900.0,
                    encoder_ids: Vec::new(),
                    kick_trace_ids: Vec::new(),
                    source_ids: Vec::new(),
                    derived_counter_matches: Vec::new(),
                    hardware_selectors: Vec::new(),
                },
            ],
            derived_metrics: Vec::new(),
            warnings: Vec::new(),
        };

        let formatted = format_raw_counters_report(&report);

        assert!(formatted.contains("_hot"));
        assert!(!formatted.contains("_huge"));
        assert!(formatted.contains("1 decoded counters are outside"));
    }

    #[test]
    fn formats_raw_counter_catalog_names_when_present() {
        let metric = RawCounterDecodedMetric {
            path: "APSCounterData[0]/Derived Counter Sample Data/0/1/0".to_owned(),
            sample_group: Some(0),
            source_index: Some(1),
            ring_index: Some(0),
            counter_index: 0,
            raw_name: "_hash".to_owned(),
            sample_count: 2,
            min_percent_of_gpu_cycles: 1.0,
            mean_percent_of_gpu_cycles: 12.0,
            max_percent_of_gpu_cycles: 20.0,
            encoder_ids: Vec::new(),
            kick_trace_ids: Vec::new(),
            source_ids: Vec::new(),
            derived_counter_matches: vec![RawCounterDerivedCounterMatch {
                key: "ALUUtilization".to_owned(),
                name: "ALU Utilization".to_owned(),
                counter_type: Some("Percentage".to_owned()),
                description: None,
                sources: vec![PathBuf::from("AGXMetalStatisticsExternal-counters.plist")],
            }],
            hardware_selectors: vec![RawCounterHardwareSelector {
                partition: Some(1),
                select: Some(2),
                flag: None,
                sources: vec![PathBuf::from("AGXMetalPerfCountersExternal.plist")],
            }],
        };
        let report = RawCountersReport {
            trace_source: PathBuf::from("trace.gputrace"),
            profiler_directory: PathBuf::from("trace.gputrace/raw"),
            aggregate_metadata: Vec::new(),
            schemas: Vec::new(),
            streams: Vec::new(),
            metrics: vec![metric],
            derived_metrics: Vec::new(),
            warnings: Vec::new(),
        };

        let text = format_raw_counters_report(&report);
        let csv = format_raw_counters_csv(&report);

        assert!(text.contains("ALU Utilization (_hash)"));
        assert!(csv.contains("ALU Utilization"));
        assert!(csv.contains("ALUUtilization"));
    }

    #[test]
    fn evaluates_agx_derived_javascript_formula() {
        let definitions = vec![RawCounterDerivedDefinition {
            key: "ALUUtilization".to_owned(),
            name: "ALU Utilization".to_owned(),
            counter_type: Some("Percentage".to_owned()),
            description: None,
            source_catalog: PathBuf::from("AGXMetalStatisticsExternalTest-counters.plist"),
            source_script: Some(PathBuf::from("AGXMetalStatisticsExternalTest-derived.js")),
        }];
        let variables = BTreeMap::from([("_hash_norm".to_owned(), 50.0)]);
        let metrics = evaluate_agx_derived_script(
            Path::new("AGXMetalStatisticsExternalTest-derived.js"),
            "var num_cores = 4; function ALUUtilization() { return _hash_norm / (2.0 * num_cores); }",
            &definitions,
            &variables,
        );

        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].key, "ALUUtilization");
        assert!((metrics[0].value - 6.25).abs() < 0.001);
    }

    #[test]
    fn chooses_single_g16_compatible_agx_derived_script() {
        let definitions_by_script = BTreeMap::from([
            (
                PathBuf::from("AGXMetalStatisticsExternalG14S-derived.js"),
                vec![RawCounterDerivedDefinition {
                    key: "A".to_owned(),
                    name: "A".to_owned(),
                    counter_type: None,
                    description: None,
                    source_catalog: PathBuf::from("AGXMetalStatisticsExternalG14S-counters.plist"),
                    source_script: None,
                }],
            ),
            (
                PathBuf::from("AGXMetalStatisticsExternalG14D-derived.js"),
                vec![RawCounterDerivedDefinition {
                    key: "B".to_owned(),
                    name: "B".to_owned(),
                    counter_type: None,
                    description: None,
                    source_catalog: PathBuf::from("AGXMetalStatisticsExternalG14D-counters.plist"),
                    source_script: None,
                }],
            ),
        ]);

        let (path, _) = choose_agx_derived_script(definitions_by_script, Some("G16X")).unwrap();

        assert_eq!(
            path,
            PathBuf::from("AGXMetalStatisticsExternalG14D-derived.js")
        );
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
