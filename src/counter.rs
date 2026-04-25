use std::collections::BTreeMap;
use std::fs;
use std::mem;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::counter_names::ALL_COUNTER_NAMES;
use crate::profiler;
use crate::trace::TraceBundle;
use crate::xcode_counters;

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

    Ok(RawCounterProbeReport {
        profiler_directory,
        csv_source: csv_data.map(|data| data.source),
        targets,
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
}
