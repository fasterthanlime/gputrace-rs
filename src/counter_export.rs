use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Serialize;

use crate::counter;
use crate::error::Result;
use crate::profiler;
use crate::timeline;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct CounterExportReport {
    pub trace_source: PathBuf,
    pub source: String,
    pub total_rows: usize,
    pub rows: Vec<CounterExportRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CounterExportRow {
    pub row_index: usize,
    pub command_buffer_index: usize,
    pub encoder_index: usize,
    pub encoder_label: String,
    pub kernel_name: Option<String>,
    pub pipeline_addr: Option<u64>,
    pub start_time_ns: u64,
    pub end_time_ns: u64,
    pub duration_ns: Option<u64>,
    pub dispatch_count: usize,
    pub metric_source: String,
    pub execution_cost_percent: Option<f64>,
    pub execution_cost_samples: usize,
    pub sample_count: usize,
    pub avg_sampling_density: Option<f64>,
    pub occupancy_percent: Option<f64>,
    pub occupancy_confidence: Option<f64>,
    pub occupancy_manager_percent: Option<f64>,
    pub alu_utilization_percent: Option<f64>,
    pub shader_launch_limiter_percent: Option<f64>,
    pub instruction_throughput_percent: Option<f64>,
    pub integer_complex_percent: Option<f64>,
    pub f32_limiter_percent: Option<f64>,
    pub l1_cache_percent: Option<f64>,
    pub last_level_cache_percent: Option<f64>,
    pub control_flow_percent: Option<f64>,
    pub device_memory_bandwidth_gbps: Option<f64>,
    pub buffer_l1_read_bandwidth_gbps: Option<f64>,
    pub buffer_l1_write_bandwidth_gbps: Option<f64>,
    pub temporary_register_count: Option<i64>,
    pub uniform_register_count: Option<i64>,
    pub spilled_bytes: Option<i64>,
    pub threadgroup_memory: Option<i64>,
    pub instruction_count: Option<i64>,
    pub alu_instruction_count: Option<i64>,
    pub branch_instruction_count: Option<i64>,
    pub compilation_time_ms: Option<f64>,
}

pub fn report(trace: &TraceBundle) -> Result<CounterExportReport> {
    let timeline = timeline::report(trace)?;
    let limiters = counter::extract_limiters_for_trace(&trace.path);
    let profiler_summary = profiler::stream_data_summary(&trace.path).ok();

    let limiters_by_encoder = limiters
        .into_iter()
        .map(|limiter| (limiter.encoder_index, limiter))
        .collect::<BTreeMap<_, _>>();

    let mut execution_cost_by_name = BTreeMap::<String, (f64, usize)>::new();
    let mut sample_stats_by_name = BTreeMap::<String, (usize, f64, usize)>::new();
    let mut pipeline_stats_by_name = BTreeMap::<String, profiler::ProfilerPipelineStats>::new();
    let mut occupancy_by_encoder = BTreeMap::<usize, (f64, f64)>::new();
    if let Some(summary) = &profiler_summary {
        for cost in &summary.execution_costs {
            let name = cost
                .function_name
                .clone()
                .unwrap_or_else(|| format!("pipeline_{}", cost.pipeline_id));
            let entry = execution_cost_by_name.entry(name).or_default();
            entry.0 += cost.cost_percent;
            entry.1 += cost.sample_count;
        }
        for dispatch in &summary.dispatches {
            let name = dispatch
                .function_name
                .clone()
                .unwrap_or_else(|| format!("pipeline_{}", dispatch.pipeline_index));
            let entry = sample_stats_by_name.entry(name).or_default();
            entry.0 += dispatch.sample_count;
            entry.1 += dispatch.sampling_density;
            entry.2 += 1;
        }
        for pipeline in &summary.pipelines {
            if let (Some(name), Some(stats)) = (&pipeline.function_name, &pipeline.stats) {
                pipeline_stats_by_name
                    .entry(name.clone())
                    .or_insert_with(|| stats.clone());
            }
        }
        for occupancy in &summary.occupancies {
            occupancy_by_encoder.insert(
                occupancy.encoder_index,
                (occupancy.occupancy_percent, occupancy.confidence),
            );
        }
    }

    let mut dispatches_by_encoder = BTreeMap::<usize, Vec<&timeline::TimelineDispatch>>::new();
    for dispatch in &timeline.dispatches {
        if let Some(encoder_index) = dispatch.encoder_index {
            dispatches_by_encoder
                .entry(encoder_index)
                .or_default()
                .push(dispatch);
        }
    }

    let mut rows = Vec::new();
    for encoder in &timeline.encoders {
        let encoder_dispatches = dispatches_by_encoder
            .get(&encoder.index)
            .cloned()
            .unwrap_or_default();
        let mut kernels = BTreeMap::<String, usize>::new();
        let mut pipeline_addr = None;
        for dispatch in &encoder_dispatches {
            if let Some(kernel_name) = &dispatch.kernel_name {
                *kernels.entry(kernel_name.clone()).or_default() += 1;
            }
            if pipeline_addr.is_none() {
                pipeline_addr = dispatch.encoder_address;
            }
        }
        let kernel_name = kernels
            .into_iter()
            .max_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)))
            .map(|(name, _)| name);
        let (execution_cost_percent, execution_cost_samples) = kernel_name
            .as_ref()
            .and_then(|name| execution_cost_by_name.get(name).copied())
            .map(|(percent, samples)| (Some(percent), samples))
            .unwrap_or((None, 0));
        let (sample_count, avg_sampling_density) = kernel_name
            .as_ref()
            .and_then(|name| sample_stats_by_name.get(name).copied())
            .map(|(samples, density_sum, density_count)| {
                let avg = if density_count == 0 {
                    None
                } else {
                    Some(density_sum / density_count as f64)
                };
                (samples, avg)
            })
            .unwrap_or((0, None));
        let pipeline_stats = kernel_name
            .as_ref()
            .and_then(|name| pipeline_stats_by_name.get(name));
        let limiter = limiters_by_encoder.get(&encoder.index);
        let occupancy = occupancy_by_encoder.get(&encoder.index).copied();

        rows.push(CounterExportRow {
            row_index: rows.len(),
            command_buffer_index: encoder.command_buffer_index,
            encoder_index: encoder.index,
            encoder_label: encoder.label.clone(),
            kernel_name,
            pipeline_addr,
            start_time_ns: encoder.start_time_ns,
            end_time_ns: encoder.end_time_ns,
            duration_ns: encoder.duration_ns,
            dispatch_count: encoder.dispatch_count,
            metric_source: if execution_cost_percent.is_some() {
                "execution-cost".to_owned()
            } else if sample_count > 0 {
                "streamData".to_owned()
            } else if limiter.is_some() {
                "raw-counter".to_owned()
            } else {
                timeline.source.clone()
            },
            execution_cost_percent,
            execution_cost_samples,
            sample_count,
            avg_sampling_density,
            occupancy_percent: occupancy.map(|(percent, _)| percent),
            occupancy_confidence: occupancy.map(|(_, confidence)| confidence),
            occupancy_manager_percent: limiter.and_then(|limiter| limiter.occupancy_manager),
            alu_utilization_percent: limiter.and_then(|limiter| limiter.alu_utilization),
            shader_launch_limiter_percent: limiter
                .and_then(|limiter| limiter.compute_shader_launch.map(normalize_percent_like)),
            instruction_throughput_percent: limiter
                .and_then(|limiter| limiter.instruction_throughput),
            integer_complex_percent: limiter
                .and_then(|limiter| limiter.integer_complex.map(normalize_percent_like)),
            f32_limiter_percent: limiter
                .and_then(|limiter| limiter.f32_limiter.map(normalize_percent_like)),
            l1_cache_percent: limiter
                .and_then(|limiter| limiter.l1_cache.map(normalize_percent_like)),
            last_level_cache_percent: limiter
                .and_then(|limiter| limiter.last_level_cache.map(normalize_percent_like)),
            control_flow_percent: limiter
                .and_then(|limiter| limiter.control_flow.map(normalize_percent_like)),
            device_memory_bandwidth_gbps: limiter
                .and_then(|limiter| limiter.device_memory_bandwidth_gbps),
            buffer_l1_read_bandwidth_gbps: limiter
                .and_then(|limiter| limiter.buffer_l1_read_bandwidth_gbps),
            buffer_l1_write_bandwidth_gbps: limiter
                .and_then(|limiter| limiter.buffer_l1_write_bandwidth_gbps),
            temporary_register_count: pipeline_stats.map(|stats| stats.temporary_register_count),
            uniform_register_count: pipeline_stats.map(|stats| stats.uniform_register_count),
            spilled_bytes: pipeline_stats.map(|stats| stats.spilled_bytes),
            threadgroup_memory: pipeline_stats.map(|stats| stats.threadgroup_memory),
            instruction_count: pipeline_stats.map(|stats| stats.instruction_count),
            alu_instruction_count: pipeline_stats.map(|stats| stats.alu_instruction_count),
            branch_instruction_count: pipeline_stats.map(|stats| stats.branch_instruction_count),
            compilation_time_ms: pipeline_stats.map(|stats| stats.compilation_time_ms),
        });
    }

    Ok(CounterExportReport {
        trace_source: trace.path.clone(),
        source: timeline.source,
        total_rows: rows.len(),
        rows,
    })
}

pub fn format_report(report: &CounterExportReport) -> String {
    let mut out = String::new();
    out.push_str("Counter export report\n");
    out.push_str(&format!(
        "trace={} source={} rows={}\n\n",
        report.trace_source.display(),
        report.source,
        report.total_rows
    ));
    out.push_str(
        "row cb enc label kernel source duration_ns dispatches exec% samples occ occ_mgr alu llc dev_bw regs spills inst\n",
    );
    for row in &report.rows {
        out.push_str(&format!(
            "{:>3} {:>2} {:>3} {:<16} {:<20} {:<14} {:>12} {:>10} {:>6} {:>7} {:>7} {:>7} {:>7} {:>7} {:>8} {:>6} {:>6} {:>6}\n",
            row.row_index,
            row.command_buffer_index,
            row.encoder_index,
            truncate(&row.encoder_label, 16),
            truncate(row.kernel_name.as_deref().unwrap_or("-"), 20),
            row.metric_source,
            row.duration_ns
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            row.dispatch_count,
            row.execution_cost_percent
                .map(|value| format!("{value:.2}"))
                .unwrap_or_else(|| "-".to_owned()),
            row.sample_count,
            row.occupancy_percent
                .map(|value| format!("{value:.1}"))
                .unwrap_or_else(|| "-".to_owned()),
            row.occupancy_manager_percent
                .map(|value| format!("{value:.1}"))
                .unwrap_or_else(|| "-".to_owned()),
            row.alu_utilization_percent
                .map(|value| format!("{value:.1}"))
                .unwrap_or_else(|| "-".to_owned()),
            row.last_level_cache_percent
                .map(|value| format!("{value:.1}"))
                .unwrap_or_else(|| "-".to_owned()),
            row.device_memory_bandwidth_gbps
                .map(|value| format!("{value:.2}"))
                .unwrap_or_else(|| "-".to_owned()),
            row.temporary_register_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            row.spilled_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            row.instruction_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
        ));
    }
    out
}

pub fn format_csv(report: &CounterExportReport) -> String {
    let mut out = String::new();
    out.push_str("row_index,command_buffer_index,encoder_index,encoder_label,kernel_name,pipeline_addr,start_time_ns,end_time_ns,duration_ns,dispatch_count,metric_source,execution_cost_percent,execution_cost_samples,sample_count,avg_sampling_density,occupancy_percent,occupancy_confidence,occupancy_manager_percent,alu_utilization_percent,shader_launch_limiter_percent,instruction_throughput_percent,integer_complex_percent,f32_limiter_percent,l1_cache_percent,last_level_cache_percent,control_flow_percent,device_memory_bandwidth_gbps,buffer_l1_read_bandwidth_gbps,buffer_l1_write_bandwidth_gbps,temporary_register_count,uniform_register_count,spilled_bytes,threadgroup_memory,instruction_count,alu_instruction_count,branch_instruction_count,compilation_time_ms\n");
    for row in &report.rows {
        let columns = vec![
            row.row_index.to_string(),
            row.command_buffer_index.to_string(),
            row.encoder_index.to_string(),
            csv_string(&row.encoder_label),
            csv_string(row.kernel_name.as_deref().unwrap_or("")),
            row.pipeline_addr
                .map(|value| format!("0x{value:x}"))
                .unwrap_or_default(),
            row.start_time_ns.to_string(),
            row.end_time_ns.to_string(),
            option_csv(row.duration_ns),
            row.dispatch_count.to_string(),
            csv_string(&row.metric_source),
            option_csv(row.execution_cost_percent),
            row.execution_cost_samples.to_string(),
            row.sample_count.to_string(),
            option_csv(row.avg_sampling_density),
            option_csv(row.occupancy_percent),
            option_csv(row.occupancy_confidence),
            option_csv(row.occupancy_manager_percent),
            option_csv(row.alu_utilization_percent),
            option_csv(row.shader_launch_limiter_percent),
            option_csv(row.instruction_throughput_percent),
            option_csv(row.integer_complex_percent),
            option_csv(row.f32_limiter_percent),
            option_csv(row.l1_cache_percent),
            option_csv(row.last_level_cache_percent),
            option_csv(row.control_flow_percent),
            option_csv(row.device_memory_bandwidth_gbps),
            option_csv(row.buffer_l1_read_bandwidth_gbps),
            option_csv(row.buffer_l1_write_bandwidth_gbps),
            option_csv(row.temporary_register_count),
            option_csv(row.uniform_register_count),
            option_csv(row.spilled_bytes),
            option_csv(row.threadgroup_memory),
            option_csv(row.instruction_count),
            option_csv(row.alu_instruction_count),
            option_csv(row.branch_instruction_count),
            option_csv(row.compilation_time_ms),
        ];
        out.push_str(&columns.join(","));
        out.push('\n');
    }
    out
}

fn truncate(value: &str, width: usize) -> String {
    if value.len() <= width {
        value.to_owned()
    } else {
        format!("{}...", &value[..width.saturating_sub(3)])
    }
}

fn csv_string(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn option_csv<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn normalize_percent_like(value: f64) -> f64 {
    if value <= 1.0 { value * 100.0 } else { value }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_counter_export_report() {
        let report = CounterExportReport {
            trace_source: PathBuf::from("/tmp/example.gputrace"),
            source: "streamData".into(),
            total_rows: 1,
            rows: vec![CounterExportRow {
                row_index: 0,
                command_buffer_index: 0,
                encoder_index: 2,
                encoder_label: "main_encoder".into(),
                kernel_name: Some("blur".into()),
                pipeline_addr: Some(0x1234),
                start_time_ns: 100,
                end_time_ns: 200,
                duration_ns: Some(100),
                dispatch_count: 3,
                metric_source: "execution-cost".into(),
                execution_cost_percent: Some(55.0),
                execution_cost_samples: 4,
                sample_count: 6,
                avg_sampling_density: Some(0.3),
                occupancy_percent: Some(37.5),
                occupancy_confidence: Some(0.8),
                occupancy_manager_percent: Some(80.0),
                alu_utilization_percent: Some(60.0),
                shader_launch_limiter_percent: Some(12.0),
                instruction_throughput_percent: Some(3.0),
                integer_complex_percent: Some(120.0),
                f32_limiter_percent: Some(650.0),
                l1_cache_percent: Some(8.0),
                last_level_cache_percent: Some(7.0),
                control_flow_percent: Some(9.0),
                device_memory_bandwidth_gbps: Some(3.2),
                buffer_l1_read_bandwidth_gbps: Some(1.4),
                buffer_l1_write_bandwidth_gbps: Some(0.8),
                temporary_register_count: Some(24),
                uniform_register_count: Some(12),
                spilled_bytes: Some(64),
                threadgroup_memory: Some(128),
                instruction_count: Some(1024),
                alu_instruction_count: Some(700),
                branch_instruction_count: Some(20),
                compilation_time_ms: Some(1.2),
            }],
        };

        let text = format_report(&report);
        assert!(text.contains("Counter export report"));
        assert!(text.contains("main_encoder"));
        assert!(text.contains("execution-cost"));

        let csv = format_csv(&report);
        assert!(csv.contains("encoder_label"));
        assert!(csv.contains("\"main_encoder\""));
        assert!(csv.contains("\"blur\""));
        assert!(csv.contains("55"));
    }
}
