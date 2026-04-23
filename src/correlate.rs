use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Serialize;

use crate::counter;
use crate::error::Result;
use crate::profiler;
use crate::shaders;
use crate::timing;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct CorrelationReport {
    pub synthetic: bool,
    pub trace_source: PathBuf,
    pub search_paths: Vec<PathBuf>,
    pub total_shaders: usize,
    pub correlated_sources: usize,
    pub uncorrelated_sources: usize,
    pub total_dispatches: usize,
    pub total_duration_ns: u64,
    pub shaders: Vec<CorrelatedShader>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CorrelatedShader {
    pub shader_name: String,
    pub pipeline_addr: u64,
    pub execution_count: usize,
    pub synthetic_total_duration_ns: u64,
    pub synthetic_avg_duration_ns: u64,
    pub synthetic_percent_of_total: f64,
    pub metric_source: String,
    pub execution_cost_percent: Option<f64>,
    pub execution_cost_samples: usize,
    pub sample_count: usize,
    pub avg_sampling_density: f64,
    pub occupancy_percent: Option<f64>,
    pub occupancy_confidence: Option<f64>,
    pub alu_utilization_percent: Option<f64>,
    pub last_level_cache_percent: Option<f64>,
    pub device_memory_bandwidth_gbps: Option<f64>,
    pub temporary_register_count: Option<i64>,
    pub spilled_bytes: Option<i64>,
    pub threadgroup_memory: Option<i64>,
    pub instruction_count: Option<i64>,
    pub alu_instruction_count: Option<i64>,
    pub branch_instruction_count: Option<i64>,
    pub compilation_time_ms: Option<f64>,
    pub encoder_count: usize,
    pub buffer_count: usize,
    pub source_file: Option<PathBuf>,
    pub source_line: Option<usize>,
    pub correlation_method: String,
}

pub fn report(trace: &TraceBundle, search_paths: &[PathBuf]) -> Result<CorrelationReport> {
    let timing = timing::report(trace)?;
    let profiler_summary = profiler::stream_data_summary(&trace.path).ok();
    let limiter_metrics = counter::extract_limiters_for_trace(&trace.path);
    let shader_report = shaders::report(trace, search_paths)?;
    let kernel_stats = trace.analyze_kernels()?;

    let shader_lookup: BTreeMap<_, _> = shader_report
        .shaders
        .into_iter()
        .map(|shader| (shader.name.clone(), shader))
        .collect();

    let mut shaders = Vec::new();
    let mut correlated_sources = 0usize;
    let mut sample_stats = BTreeMap::<String, (usize, f64, usize)>::new();
    let mut execution_cost_by_name = BTreeMap::<String, (f64, usize)>::new();
    let mut limiter_by_name = BTreeMap::<String, (f64, f64, f64, usize)>::new();
    if let Some(summary) = &profiler_summary {
        for dispatch in &summary.dispatches {
            let name = dispatch
                .function_name
                .clone()
                .unwrap_or_else(|| format!("pipeline_{}", dispatch.pipeline_index));
            let entry = sample_stats.entry(name).or_default();
            entry.0 += dispatch.sample_count;
            entry.1 += dispatch.sampling_density;
            entry.2 += 1;
        }
        for cost in &summary.execution_costs {
            let name = cost
                .function_name
                .clone()
                .unwrap_or_else(|| format!("pipeline_{}", cost.pipeline_id));
            let entry = execution_cost_by_name.entry(name).or_default();
            entry.0 += cost.cost_percent;
            entry.1 += cost.sample_count;
        }
        for limiter in &limiter_metrics {
            for dispatch in summary
                .dispatches
                .iter()
                .filter(|dispatch| dispatch.encoder_index == limiter.encoder_index)
            {
                let name = dispatch
                    .function_name
                    .clone()
                    .unwrap_or_else(|| format!("pipeline_{}", dispatch.pipeline_index));
                let entry = limiter_by_name.entry(name).or_default();
                entry.0 += limiter.alu_utilization.unwrap_or(0.0);
                entry.1 += limiter.last_level_cache.unwrap_or(0.0);
                entry.2 += limiter.device_memory_bandwidth_gbps.unwrap_or(0.0);
                entry.3 += 1;
            }
        }
    }

    for kernel in timing.kernels {
        let source = shader_lookup.get(&kernel.name);
        if source
            .and_then(|shader| shader.source_file.as_ref())
            .is_some()
        {
            correlated_sources += 1;
        }
        let kernel_stat = kernel_stats.get(&kernel.name);
        let pipeline_addr = kernel_stat
            .map(|value| value.pipeline_addr)
            .unwrap_or_default();
        let execution_count = kernel.dispatch_count;
        let synthetic_avg_duration_ns = if execution_count == 0 {
            0
        } else {
            kernel.synthetic_duration_ns / execution_count as u64
        };
        let (sample_count, avg_sampling_density) = sample_stats
            .get(&kernel.name)
            .map(|(samples, density_sum, count)| {
                let avg = if *count == 0 {
                    0.0
                } else {
                    *density_sum / *count as f64
                };
                (*samples, avg)
            })
            .unwrap_or((0, 0.0));
        let (execution_cost_percent, execution_cost_samples) = execution_cost_by_name
            .get(&kernel.name)
            .map(|(percent, samples)| (Some(*percent), *samples))
            .unwrap_or((None, 0));
        let limiter =
            limiter_by_name
                .get(&kernel.name)
                .and_then(|(alu_sum, llc_sum, bw_sum, count)| {
                    (*count > 0).then_some((
                        alu_sum / *count as f64,
                        llc_sum / *count as f64,
                        bw_sum / *count as f64,
                    ))
                });
        let metric_source = if execution_cost_percent.is_some() {
            "execution-cost"
        } else if source.and_then(|shader| shader.total_duration_ns).is_some() {
            "profiler-duration"
        } else if source
            .and_then(|shader| shader.simd_percent_of_total)
            .is_some()
        {
            "simd-groups"
        } else {
            "timing-only"
        };
        shaders.push(CorrelatedShader {
            shader_name: kernel.name.clone(),
            pipeline_addr,
            execution_count,
            synthetic_total_duration_ns: kernel.synthetic_duration_ns,
            synthetic_avg_duration_ns,
            synthetic_percent_of_total: kernel.percent_of_total,
            metric_source: metric_source.to_owned(),
            execution_cost_percent,
            execution_cost_samples,
            sample_count,
            avg_sampling_density,
            occupancy_percent: source.and_then(|shader| shader.occupancy_percent),
            occupancy_confidence: source.and_then(|shader| shader.occupancy_confidence),
            alu_utilization_percent: limiter
                .map(|(alu, _, _)| alu)
                .or_else(|| source.and_then(|shader| shader.alu_utilization_percent)),
            last_level_cache_percent: limiter
                .map(|(_, llc, _)| llc)
                .or_else(|| source.and_then(|shader| shader.last_level_cache_percent)),
            device_memory_bandwidth_gbps: limiter
                .map(|(_, _, bw)| bw)
                .or_else(|| source.and_then(|shader| shader.device_memory_bandwidth_gbps)),
            temporary_register_count: source.and_then(|shader| shader.temporary_register_count),
            spilled_bytes: source.and_then(|shader| shader.spilled_bytes),
            threadgroup_memory: source.and_then(|shader| shader.threadgroup_memory),
            instruction_count: source.and_then(|shader| shader.instruction_count),
            alu_instruction_count: source.and_then(|shader| shader.alu_instruction_count),
            branch_instruction_count: source.and_then(|shader| shader.branch_instruction_count),
            compilation_time_ms: source.and_then(|shader| shader.compilation_time_ms),
            encoder_count: kernel_stat
                .map(|value| value.encoder_labels.len())
                .unwrap_or(0),
            buffer_count: kernel_stat.map(|value| value.buffers.len()).unwrap_or(0),
            source_file: source.and_then(|shader| shader.source_file.clone()),
            source_line: source.and_then(|shader| shader.source_line),
            correlation_method: if source
                .and_then(|shader| shader.source_file.as_ref())
                .is_some()
            {
                "name".to_owned()
            } else {
                "timing-only".to_owned()
            },
        });
    }

    shaders.sort_by(|left, right| {
        right
            .synthetic_total_duration_ns
            .cmp(&left.synthetic_total_duration_ns)
            .then_with(|| right.execution_count.cmp(&left.execution_count))
            .then_with(|| left.shader_name.cmp(&right.shader_name))
    });

    Ok(CorrelationReport {
        synthetic: timing.synthetic,
        trace_source: trace.path.clone(),
        search_paths: search_paths.to_vec(),
        total_shaders: shaders.len(),
        correlated_sources,
        uncorrelated_sources: shaders.len().saturating_sub(correlated_sources),
        total_dispatches: timing.dispatch_count,
        total_duration_ns: timing.total_duration_ns,
        shaders,
    })
}

pub fn format_report(report: &CorrelationReport, verbose: bool) -> String {
    let mut out = String::new();
    if report.synthetic {
        out.push_str("Synthetic shader correlation report\n");
        out.push_str("Combines kernel timing, trace attribution, and optional source lookup.\n");
    } else {
        out.push_str("Profiler-backed shader correlation report\n");
        out.push_str(
            "Combines streamData timing, trace attribution, and optional source lookup.\n",
        );
    }
    out.push_str(
        "Includes profiler timing, execution cost, and correlated hardware counter summaries when available.\n\n",
    );
    out.push_str(&format!("trace={}\n", report.trace_source.display()));
    out.push_str(&format!(
        "shaders={} sources={}/{} dispatches={} total={} ns\n\n",
        report.total_shaders,
        report.correlated_sources,
        report.total_shaders,
        report.total_dispatches,
        report.total_duration_ns
    ));
    out.push_str(&format!(
        "{:<36} {:>10} {:>16} {:>8} {:>8} {:>8} {:<18}  {}\n",
        "Shader",
        "Dispatches",
        if report.synthetic {
            "Synthetic ns"
        } else {
            "Duration ns"
        },
        "Time %",
        "Exec %",
        "Samples",
        "Pipeline",
        "Source"
    ));
    for shader in &report.shaders {
        let source = match (&shader.source_file, shader.source_line) {
            (Some(path), Some(line)) => format!("{}:{}", path.display(), line),
            _ => "-".to_owned(),
        };
        out.push_str(&format!(
            "{:<36} {:>10} {:>16} {:>7.2} {:>7} {:>8} 0x{:<16x}  {}\n",
            truncate(&shader.shader_name, 36),
            shader.execution_count,
            shader.synthetic_total_duration_ns,
            shader.synthetic_percent_of_total,
            shader
                .execution_cost_percent
                .map(|value| format!("{value:.2}"))
                .unwrap_or_else(|| "-".to_owned()),
            shader.sample_count,
            shader.pipeline_addr,
            source
        ));
        if verbose {
            out.push_str(&format!(
                "           avg={} ns source={} samples/us={:.3} exec_samples={} occ={} occ_conf={} alu={} llc={} dev_bw={} regs={} spills={} tgmem={} inst={} alu_inst={} branch_inst={} compile_ms={} encoders={} buffers={} correlation={}\n",
                shader.synthetic_avg_duration_ns,
                shader.metric_source,
                shader.avg_sampling_density,
                shader.execution_cost_samples,
                shader
                    .occupancy_percent
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .occupancy_confidence
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .alu_utilization_percent
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .last_level_cache_percent
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .device_memory_bandwidth_gbps
                    .map(|value| format!("{value:.2} GB/s"))
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .temporary_register_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .spilled_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .threadgroup_memory
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .instruction_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .alu_instruction_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .branch_instruction_count
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .compilation_time_ms
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_owned()),
                shader.encoder_count,
                shader.buffer_count,
                shader.correlation_method
            ));
        }
    }
    out
}

fn truncate(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_owned();
    }
    let keep = width.saturating_sub(3);
    format!("{}...", &value[..keep])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_report() {
        let report = CorrelationReport {
            synthetic: true,
            trace_source: PathBuf::from("/tmp/example.gputrace"),
            search_paths: vec![],
            total_shaders: 1,
            correlated_sources: 1,
            uncorrelated_sources: 0,
            total_dispatches: 2,
            total_duration_ns: 120,
            shaders: vec![CorrelatedShader {
                shader_name: "kernel".into(),
                pipeline_addr: 0x1234,
                execution_count: 2,
                synthetic_total_duration_ns: 120,
                synthetic_avg_duration_ns: 60,
                synthetic_percent_of_total: 100.0,
                metric_source: "execution-cost".into(),
                execution_cost_percent: Some(75.0),
                execution_cost_samples: 3,
                sample_count: 4,
                avg_sampling_density: 0.2,
                occupancy_percent: Some(41.0),
                occupancy_confidence: Some(0.9),
                alu_utilization_percent: Some(61.0),
                last_level_cache_percent: Some(0.04),
                device_memory_bandwidth_gbps: Some(8.2),
                temporary_register_count: Some(64),
                spilled_bytes: Some(512),
                threadgroup_memory: Some(8192),
                instruction_count: Some(2048),
                alu_instruction_count: Some(1500),
                branch_instruction_count: Some(48),
                compilation_time_ms: Some(4.5),
                encoder_count: 1,
                buffer_count: 2,
                source_file: Some(PathBuf::from("/tmp/kernel.metal")),
                source_line: Some(42),
                correlation_method: "name".into(),
            }],
        };
        let output = format_report(&report, true);
        assert!(output.contains("Synthetic shader correlation report"));
        assert!(output.contains("kernel"));
        assert!(output.contains("/tmp/kernel.metal:42"));
        assert!(output.contains("correlation=name"));
        assert!(output.contains("source=execution-cost"));
        assert!(output.contains("samples/us=0.200"));
        assert!(output.contains("75.00"));
        assert!(output.contains("exec_samples=3"));
        assert!(output.contains("occ=41.00"));
        assert!(output.contains("alu=61.00"));
        assert!(output.contains("dev_bw=8.20 GB/s"));
        assert!(output.contains("regs=64"));
        assert!(output.contains("spills=512"));
        assert!(output.contains("alu_inst=1500"));
        assert!(output.contains("branch_inst=48"));
    }
}
