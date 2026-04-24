use std::collections::BTreeMap;
use std::fs;
use std::mem;
use std::path::Path;

use serde::Serialize;

use crate::counter_names::ALL_COUNTER_NAMES;
use crate::profiler;

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
    pub representative_value: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub mean_value: f64,
    pub confidence: f64,
}

pub fn counter_file_metric_name(file_index: usize) -> Option<&'static str> {
    file_index
        .checked_sub(4)
        .and_then(|index| ALL_COUNTER_NAMES.get(index).copied())
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
    let mean_value = values.iter().sum::<f64>() / sample_count as f64;
    let representative_value = median_sorted(&values);
    let confidence = counter_metric_confidence(record_count, sample_count, min_value, max_value);

    Some(CounterFileMetric {
        file_index,
        metric_name,
        unit,
        encoder_index,
        record_count,
        sample_count,
        representative_value,
        min_value,
        max_value,
        mean_value,
        confidence,
    })
}

fn extract_counter_record_values(record: &[u8], metric_name: &str) -> Vec<f64> {
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
    if values.len() % 2 == 0 {
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
        assert_eq!(metrics[0].representative_value, 17.0);
        assert_eq!(metrics[0].min_value, 12.0);
        assert_eq!(metrics[0].max_value, 22.0);
        assert_eq!(metrics[0].mean_value, 17.0);
        assert!(metrics[0].confidence > 0.5);
    }
}
