use std::collections::BTreeMap;
use std::fs;
use std::mem;
use std::path::Path;

use serde::Serialize;

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
}
