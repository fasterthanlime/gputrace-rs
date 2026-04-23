use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde::Serialize;
use walkdir::WalkDir;

use crate::counter;
use crate::error::{Error, Result};
use crate::profiler;
use crate::trace::TraceBundle;
use crate::xcode_counters;

#[derive(Debug, Clone, Serialize)]
pub struct ShaderReport {
    pub total_shaders: usize,
    pub indexed_files: usize,
    pub indexed_symbols: usize,
    pub shaders: Vec<ShaderEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShaderEntry {
    pub name: String,
    pub pipeline_addr: u64,
    pub dispatch_count: usize,
    pub metric_source: String,
    pub simd_groups: u64,
    pub simd_percent_of_total: Option<f64>,
    pub total_duration_ns: Option<u64>,
    pub percent_of_total: Option<f64>,
    pub execution_cost_percent: Option<f64>,
    pub execution_cost_samples: usize,
    pub sample_count: usize,
    pub avg_sampling_density: Option<f64>,
    pub occupancy_percent: Option<f64>,
    pub occupancy_confidence: Option<f64>,
    pub alu_utilization_percent: Option<f64>,
    pub kernel_alu_performance: Option<f64>,
    pub weighted_cost: Option<f64>,
    pub weighted_percent_of_total: Option<f64>,
    pub last_level_cache_percent: Option<f64>,
    pub device_memory_bandwidth_gbps: Option<f64>,
    pub gpu_read_bandwidth_gbps: Option<f64>,
    pub gpu_write_bandwidth_gbps: Option<f64>,
    pub buffer_l1_miss_rate_percent: Option<f64>,
    pub buffer_l1_read_accesses: Option<f64>,
    pub buffer_l1_write_accesses: Option<f64>,
    pub temporary_register_count: Option<i64>,
    pub spilled_bytes: Option<i64>,
    pub threadgroup_memory: Option<i64>,
    pub instruction_count: Option<i64>,
    pub alu_instruction_count: Option<i64>,
    pub branch_instruction_count: Option<i64>,
    pub compilation_time_ms: Option<f64>,
    pub source_file: Option<PathBuf>,
    pub source_line: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShaderSourceReport {
    pub shader_name: String,
    pub pipeline_addr: u64,
    pub dispatch_count: usize,
    pub source_file: PathBuf,
    pub source_line: usize,
    pub start_line: usize,
    pub end_line: usize,
    pub excerpt: Vec<SourceLine>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceLine {
    pub number: usize,
    pub text: String,
    pub highlight: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShaderHotspotReport {
    pub shader_name: String,
    pub pipeline_addr: u64,
    pub dispatch_count: usize,
    pub source_file: PathBuf,
    pub start_line: usize,
    pub end_line: usize,
    pub total_gpu_percent: f64,
    pub metric_source: String,
    pub lines: Vec<AttributedSourceLine>,
    pub hotspots: Vec<AttributedSourceLine>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttributedSourceLine {
    pub line_number: usize,
    pub text: String,
    pub instruction_type: String,
    pub complexity: u32,
    pub estimated_cost: f64,
    pub attributed_gpu_percent: f64,
    pub hotspot: bool,
    pub hints: Vec<String>,
}

#[derive(Debug, Clone)]
struct ShaderSourceIndex {
    kernel_to_file: BTreeMap<String, PathBuf>,
    kernel_to_line: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Copy)]
struct XcodeCounterMatch {
    alu_utilization_percent: Option<f64>,
    occupancy_percent: Option<f64>,
    device_memory_bandwidth_gbps: Option<f64>,
    kernel_alu_performance: Option<f64>,
    gpu_read_bandwidth_gbps: Option<f64>,
    gpu_write_bandwidth_gbps: Option<f64>,
    buffer_l1_miss_rate_percent: Option<f64>,
    buffer_l1_read_accesses: Option<f64>,
    buffer_l1_write_accesses: Option<f64>,
}

pub fn report(trace: &TraceBundle, search_paths: &[PathBuf]) -> Result<ShaderReport> {
    let index = ShaderSourceIndex::build(search_paths)?;
    let profiler_summary = profiler::stream_data_summary(&trace.path).ok();
    let limiter_metrics = counter::extract_limiters_for_trace(&trace.path);
    let xcode_counter_data = xcode_counters::parse(trace, None).ok();
    let dispatches = trace.dispatch_calls()?;
    let mut simd_groups_by_name = BTreeMap::<String, u64>::new();
    let mut total_simd_groups = 0u64;
    for dispatch in &dispatches {
        let Some(kernel_name) = &dispatch.kernel_name else {
            continue;
        };
        let simd_groups = dispatch_simd_groups(dispatch);
        if simd_groups == 0 {
            continue;
        }
        *simd_groups_by_name.entry(kernel_name.clone()).or_default() += simd_groups;
        total_simd_groups += simd_groups;
    }

    let mut duration_by_name = BTreeMap::<String, u64>::new();
    let mut execution_cost_by_name = BTreeMap::<String, f64>::new();
    let mut execution_cost_samples_by_name = BTreeMap::<String, usize>::new();
    let mut sample_count_by_name = BTreeMap::<String, usize>::new();
    let mut density_sum_by_name = BTreeMap::<String, f64>::new();
    let mut density_count_by_name = BTreeMap::<String, usize>::new();
    let mut occupancy_by_name = BTreeMap::<String, (f64, f64, usize)>::new();
    let mut limiter_by_name = BTreeMap::<String, (f64, f64, f64, usize)>::new();
    let mut pipeline_stats_by_addr = BTreeMap::<u64, profiler::ProfilerPipelineStats>::new();
    let mut pipeline_stats_by_name = BTreeMap::<String, profiler::ProfilerPipelineStats>::new();
    let mut total_duration_ns = 0u64;
    if let Some(summary) = &profiler_summary {
        total_duration_ns = summary.total_time_us.saturating_mul(1_000);
        for dispatch in &summary.dispatches {
            let name = dispatch
                .function_name
                .clone()
                .unwrap_or_else(|| format!("pipeline_{}", dispatch.pipeline_index));
            *duration_by_name.entry(name.clone()).or_default() +=
                dispatch.duration_us.saturating_mul(1_000);
            *sample_count_by_name.entry(name.clone()).or_default() += dispatch.sample_count;
            if dispatch.sample_count > 0 {
                *density_sum_by_name.entry(name.clone()).or_default() += dispatch.sampling_density;
                *density_count_by_name.entry(name).or_default() += 1;
            }
        }
        for cost in &summary.execution_costs {
            let name = cost
                .function_name
                .clone()
                .unwrap_or_else(|| format!("pipeline_{}", cost.pipeline_id));
            *execution_cost_by_name.entry(name.clone()).or_default() += cost.cost_percent;
            *execution_cost_samples_by_name.entry(name).or_default() += cost.sample_count;
        }
        for occupancy in &summary.occupancies {
            for dispatch in summary
                .dispatches
                .iter()
                .filter(|dispatch| dispatch.encoder_index == occupancy.encoder_index)
            {
                let name = dispatch
                    .function_name
                    .clone()
                    .unwrap_or_else(|| format!("pipeline_{}", dispatch.pipeline_index));
                let entry = occupancy_by_name.entry(name).or_default();
                entry.0 += occupancy.occupancy_percent;
                entry.1 += occupancy.confidence;
                entry.2 += 1;
            }
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
        for pipeline in &summary.pipelines {
            if let Some(stats) = &pipeline.stats {
                if pipeline.pipeline_address != 0 {
                    pipeline_stats_by_addr.insert(pipeline.pipeline_address, stats.clone());
                }
                if let Some(name) = &pipeline.function_name {
                    pipeline_stats_by_name
                        .entry(name.clone())
                        .or_insert_with(|| stats.clone());
                }
            }
        }
    }

    let mut shaders: Vec<_> = trace
        .analyze_kernels()?
        .into_values()
        .map(|kernel| {
            let kernel_name = kernel.name.clone();
            let (source_file, source_line) = match index.lookup(&kernel.name) {
                Some((file, line)) => (Some(file), Some(line)),
                None => (None, None),
            };
            let total_duration_ns_for_shader = duration_by_name.get(&kernel_name).copied();
            let percent_of_total = total_duration_ns_for_shader.and_then(|duration| {
                (total_duration_ns > 0)
                    .then(|| (duration as f64 / total_duration_ns as f64) * 100.0)
            });
            let simd_groups = simd_groups_by_name.get(&kernel_name).copied().unwrap_or(0);
            let simd_percent_of_total = (total_simd_groups > 0 && simd_groups > 0)
                .then(|| (simd_groups as f64 / total_simd_groups as f64) * 100.0);
            let avg_sampling_density = density_count_by_name
                .get(&kernel_name)
                .copied()
                .filter(|count| *count > 0)
                .and_then(|count| {
                    density_sum_by_name
                        .get(&kernel_name)
                        .map(|sum| *sum / count as f64)
                });
            let occupancy = occupancy_by_name.get(&kernel_name).and_then(
                |(occupancy_sum, confidence_sum, count)| {
                    (*count > 0).then_some((
                        occupancy_sum / *count as f64,
                        confidence_sum / *count as f64,
                    ))
                },
            );
            let limiter =
                limiter_by_name
                    .get(&kernel_name)
                    .and_then(|(alu_sum, llc_sum, bw_sum, count)| {
                        (*count > 0).then_some((
                            alu_sum / *count as f64,
                            llc_sum / *count as f64,
                            bw_sum / *count as f64,
                        ))
                    });
            let xcode_counter_match = xcode_counter_data
                .as_ref()
                .and_then(|data| match_xcode_counters(&kernel_name, data));
            let pipeline_stats = pipeline_stats_by_addr
                .get(&kernel.pipeline_addr)
                .cloned()
                .or_else(|| pipeline_stats_by_name.get(&kernel_name).cloned());
            let execution_cost_percent = execution_cost_by_name.get(&kernel_name).copied();
            let weighted_cost = xcode_counter_match
                .and_then(|entry| entry.kernel_alu_performance)
                .filter(|value| *value > 0.0)
                .map(|value| value.powf(0.30));
            let metric_source = if execution_cost_percent.is_some() {
                "execution-cost".to_owned()
            } else if weighted_cost.is_some() {
                "xcode-weighted".to_owned()
            } else if total_duration_ns_for_shader.is_some() {
                "profiler-duration".to_owned()
            } else if simd_percent_of_total.is_some() {
                "simd-groups".to_owned()
            } else if xcode_counter_match.is_some() {
                "xcode-counters".to_owned()
            } else {
                "unattributed".to_owned()
            };
            ShaderEntry {
                name: kernel_name.clone(),
                pipeline_addr: kernel.pipeline_addr,
                dispatch_count: kernel.dispatch_count,
                metric_source,
                simd_groups,
                simd_percent_of_total,
                total_duration_ns: total_duration_ns_for_shader,
                percent_of_total,
                execution_cost_percent,
                execution_cost_samples: execution_cost_samples_by_name
                    .get(&kernel_name)
                    .copied()
                    .unwrap_or(0),
                sample_count: sample_count_by_name.get(&kernel_name).copied().unwrap_or(0),
                avg_sampling_density,
                occupancy_percent: occupancy
                    .map(|(value, _)| value)
                    .or(xcode_counter_match.and_then(|entry| entry.occupancy_percent)),
                occupancy_confidence: occupancy.map(|(_, confidence)| confidence),
                alu_utilization_percent: limiter
                    .map(|(alu, _, _)| alu)
                    .or(xcode_counter_match.and_then(|entry| entry.alu_utilization_percent)),
                kernel_alu_performance: xcode_counter_match
                    .and_then(|entry| entry.kernel_alu_performance),
                weighted_cost,
                weighted_percent_of_total: None,
                last_level_cache_percent: limiter.map(|(_, llc, _)| llc),
                device_memory_bandwidth_gbps: limiter
                    .map(|(_, _, bw)| bw)
                    .or(xcode_counter_match.and_then(|entry| entry.device_memory_bandwidth_gbps)),
                gpu_read_bandwidth_gbps: xcode_counter_match
                    .and_then(|entry| entry.gpu_read_bandwidth_gbps),
                gpu_write_bandwidth_gbps: xcode_counter_match
                    .and_then(|entry| entry.gpu_write_bandwidth_gbps),
                buffer_l1_miss_rate_percent: xcode_counter_match
                    .and_then(|entry| entry.buffer_l1_miss_rate_percent),
                buffer_l1_read_accesses: xcode_counter_match
                    .and_then(|entry| entry.buffer_l1_read_accesses),
                buffer_l1_write_accesses: xcode_counter_match
                    .and_then(|entry| entry.buffer_l1_write_accesses),
                temporary_register_count: pipeline_stats
                    .as_ref()
                    .map(|stats| stats.temporary_register_count),
                spilled_bytes: pipeline_stats.as_ref().map(|stats| stats.spilled_bytes),
                threadgroup_memory: pipeline_stats
                    .as_ref()
                    .map(|stats| stats.threadgroup_memory),
                instruction_count: pipeline_stats.as_ref().map(|stats| stats.instruction_count),
                alu_instruction_count: pipeline_stats
                    .as_ref()
                    .map(|stats| stats.alu_instruction_count),
                branch_instruction_count: pipeline_stats
                    .as_ref()
                    .map(|stats| stats.branch_instruction_count),
                compilation_time_ms: pipeline_stats
                    .as_ref()
                    .map(|stats| stats.compilation_time_ms),
                source_file,
                source_line,
            }
        })
        .collect();
    let total_weighted_cost = shaders
        .iter()
        .filter(|shader| shader.execution_cost_percent.is_none())
        .filter_map(|shader| shader.weighted_cost)
        .sum::<f64>();
    if total_weighted_cost > 0.0 {
        for shader in &mut shaders {
            if shader.execution_cost_percent.is_none() {
                shader.weighted_percent_of_total = shader
                    .weighted_cost
                    .map(|value| (value / total_weighted_cost) * 100.0);
            }
        }
    }
    shaders.sort_by(|left, right| {
        compare_option_f64_desc(right.execution_cost_percent, left.execution_cost_percent)
            .then_with(|| {
                compare_option_f64_desc(
                    right.weighted_percent_of_total,
                    left.weighted_percent_of_total,
                )
            })
            .then_with(|| compare_option_u64_desc(right.total_duration_ns, left.total_duration_ns))
            .then_with(|| compare_option_f64_desc(right.percent_of_total, left.percent_of_total))
            .then_with(|| right.simd_groups.cmp(&left.simd_groups))
            .then_with(|| right.dispatch_count.cmp(&left.dispatch_count))
            .then_with(|| left.name.cmp(&right.name))
    });
    let (indexed_files, indexed_symbols) = index.stats();
    Ok(ShaderReport {
        total_shaders: shaders.len(),
        indexed_files,
        indexed_symbols,
        shaders,
    })
}

fn match_xcode_counters(
    kernel_name: &str,
    data: &xcode_counters::XcodeCounterData,
) -> Option<XcodeCounterMatch> {
    let normalized_kernel = normalize_for_matching(kernel_name);
    let mut exact = Vec::new();
    let mut fuzzy = Vec::new();

    for encoder in &data.encoders {
        let normalized_label = normalize_for_matching(&encoder.encoder_label);
        if normalized_label.is_empty() || normalized_kernel.is_empty() {
            continue;
        }
        if normalized_label == normalized_kernel {
            exact.push(encoder);
        } else if normalized_label.contains(&normalized_kernel)
            || normalized_kernel.contains(&normalized_label)
        {
            fuzzy.push(encoder);
        }
    }

    let matches = if !exact.is_empty() { exact } else { fuzzy };
    if matches.is_empty() {
        return None;
    }

    let mut alu_sum = 0.0;
    let mut alu_count = 0usize;
    let mut occupancy_sum = 0.0;
    let mut occupancy_count = 0usize;
    let mut bw_sum = 0.0;
    let mut bw_count = 0usize;
    let mut alu_perf_sum = 0.0;
    let mut alu_perf_count = 0usize;
    let mut gpu_read_bw_sum = 0.0;
    let mut gpu_read_bw_count = 0usize;
    let mut gpu_write_bw_sum = 0.0;
    let mut gpu_write_bw_count = 0usize;
    let mut l1_miss_sum = 0.0;
    let mut l1_miss_count = 0usize;
    let mut l1_read_acc_sum = 0.0;
    let mut l1_read_acc_count = 0usize;
    let mut l1_write_acc_sum = 0.0;
    let mut l1_write_acc_count = 0usize;

    for encoder in matches {
        if let Some(value) = encoder.counters.get("ALU Utilization").copied() {
            alu_sum += value;
            alu_count += 1;
        }
        if let Some(value) = encoder.counters.get("Kernel Occupancy").copied() {
            occupancy_sum += value;
            occupancy_count += 1;
        }
        if let Some(value) = encoder.counters.get("Device Memory Bandwidth").copied() {
            bw_sum += value;
            bw_count += 1;
        }
        if let Some(value) = encoder.counters.get("Kernel ALU Performance").copied() {
            alu_perf_sum += value;
            alu_perf_count += 1;
        }
        if let Some(value) = encoder.counters.get("GPU Read Bandwidth").copied() {
            gpu_read_bw_sum += value;
            gpu_read_bw_count += 1;
        }
        if let Some(value) = encoder.counters.get("GPU Write Bandwidth").copied() {
            gpu_write_bw_sum += value;
            gpu_write_bw_count += 1;
        }
        if let Some(value) = encoder.counters.get("Buffer L1 Miss Rate").copied() {
            l1_miss_sum += value;
            l1_miss_count += 1;
        }
        if let Some(value) = encoder.counters.get("Buffer L1 Read Accesses").copied() {
            l1_read_acc_sum += value;
            l1_read_acc_count += 1;
        }
        if let Some(value) = encoder.counters.get("Buffer L1 Write Accesses").copied() {
            l1_write_acc_sum += value;
            l1_write_acc_count += 1;
        }
    }

    Some(XcodeCounterMatch {
        alu_utilization_percent: (alu_count > 0).then(|| alu_sum / alu_count as f64),
        occupancy_percent: (occupancy_count > 0).then(|| occupancy_sum / occupancy_count as f64),
        device_memory_bandwidth_gbps: (bw_count > 0).then(|| bw_sum / bw_count as f64),
        kernel_alu_performance: (alu_perf_count > 0).then(|| alu_perf_sum / alu_perf_count as f64),
        gpu_read_bandwidth_gbps: (gpu_read_bw_count > 0)
            .then(|| gpu_read_bw_sum / gpu_read_bw_count as f64),
        gpu_write_bandwidth_gbps: (gpu_write_bw_count > 0)
            .then(|| gpu_write_bw_sum / gpu_write_bw_count as f64),
        buffer_l1_miss_rate_percent: (l1_miss_count > 0)
            .then(|| l1_miss_sum / l1_miss_count as f64),
        buffer_l1_read_accesses: (l1_read_acc_count > 0)
            .then(|| l1_read_acc_sum / l1_read_acc_count as f64),
        buffer_l1_write_accesses: (l1_write_acc_count > 0)
            .then(|| l1_write_acc_sum / l1_write_acc_count as f64),
    })
}

fn normalize_for_matching(name: &str) -> String {
    name.chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_lowercase())
            } else {
                None
            }
        })
        .collect()
}

pub fn source(
    trace: &TraceBundle,
    shader_name: &str,
    search_paths: &[PathBuf],
    context: usize,
) -> Result<ShaderSourceReport> {
    let index = ShaderSourceIndex::build(search_paths)?;
    let kernels = trace.analyze_kernels()?;
    let kernel = kernels
        .get(shader_name)
        .cloned()
        .or_else(|| {
            kernels.into_values().find(|kernel| {
                kernel.name.contains(shader_name) || shader_name.contains(&kernel.name)
            })
        })
        .ok_or_else(|| Error::InvalidInput(format!("shader not found in trace: {shader_name}")))?;
    let (source_file, source_line) = index
        .lookup(&kernel.name)
        .map(|(file, line)| (file, line))
        .ok_or_else(|| {
            Error::InvalidInput(format!("source not found for shader: {}", kernel.name))
        })?;
    let contents = fs::read_to_string(&source_file)?;
    let lines: Vec<_> = contents.lines().map(ToOwned::to_owned).collect();
    let start_line = source_line.saturating_sub(context).max(1);
    let end_line = (source_line + context).min(lines.len());
    let excerpt = (start_line..=end_line)
        .map(|number| SourceLine {
            number,
            text: lines[number - 1].clone(),
            highlight: number == source_line,
        })
        .collect();

    Ok(ShaderSourceReport {
        shader_name: kernel.name,
        pipeline_addr: kernel.pipeline_addr,
        dispatch_count: kernel.dispatch_count,
        source_file,
        source_line,
        start_line,
        end_line,
        excerpt,
    })
}

pub fn hotspot_report(
    trace: &TraceBundle,
    shader_name: &str,
    search_paths: &[PathBuf],
) -> Result<ShaderHotspotReport> {
    let report = report(trace, search_paths)?;
    let shader = report
        .shaders
        .into_iter()
        .find(|shader| {
            shader.name == shader_name
                || shader.name.contains(shader_name)
                || shader_name.contains(&shader.name)
        })
        .ok_or_else(|| Error::InvalidInput(format!("shader not found in trace: {shader_name}")))?;
    let source = source(trace, &shader.name, search_paths, 0)?;
    let contents = fs::read_to_string(&source.source_file)?;
    let file_lines: Vec<_> = contents.lines().map(ToOwned::to_owned).collect();
    let (start_line, end_line) = function_bounds(&file_lines, source.source_line);
    let metric_source = shader.metric_source.clone();
    let total_gpu_percent = shader
        .execution_cost_percent
        .or(shader.weighted_percent_of_total)
        .or(shader.percent_of_total)
        .or(shader.simd_percent_of_total)
        .unwrap_or(0.0);

    let mut lines = Vec::new();
    for number in start_line..=end_line {
        let text = file_lines[number - 1].clone();
        let trimmed = text.trim();
        let (instruction_type, complexity) = classify_instruction(trimmed);
        let estimated_cost = estimate_line_cost(trimmed, &instruction_type, complexity);
        lines.push(AttributedSourceLine {
            line_number: number,
            text,
            instruction_type,
            complexity,
            estimated_cost,
            attributed_gpu_percent: 0.0,
            hotspot: false,
            hints: Vec::new(),
        });
    }

    attribute_line_costs(
        &mut lines,
        total_gpu_percent,
        shader.instruction_count,
        shader.alu_instruction_count,
        shader.branch_instruction_count,
        shader.execution_cost_percent,
        shader.alu_utilization_percent,
        shader.last_level_cache_percent,
        shader.device_memory_bandwidth_gbps,
    );

    let hotspot_count = lines
        .iter()
        .filter(|line| line.estimated_cost > 0.0)
        .count()
        .max(1)
        .div_ceil(5);
    let mut hotspots = lines
        .iter()
        .filter(|line| line.estimated_cost > 0.0)
        .cloned()
        .collect::<Vec<_>>();
    hotspots.sort_by(|left, right| {
        right
            .attributed_gpu_percent
            .partial_cmp(&left.attributed_gpu_percent)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.complexity.cmp(&left.complexity))
            .then_with(|| left.line_number.cmp(&right.line_number))
    });
    hotspots.truncate(hotspot_count);
    let hotspot_lines: std::collections::BTreeSet<_> =
        hotspots.iter().map(|line| line.line_number).collect();

    for line in &mut lines {
        line.hotspot = hotspot_lines.contains(&line.line_number);
        line.hints = line_hints(line);
    }
    for hotspot in &mut hotspots {
        hotspot.hotspot = true;
        hotspot.hints = line_hints(hotspot);
    }

    Ok(ShaderHotspotReport {
        shader_name: shader.name,
        pipeline_addr: shader.pipeline_addr,
        dispatch_count: shader.dispatch_count,
        source_file: source.source_file,
        start_line,
        end_line,
        total_gpu_percent,
        metric_source,
        lines,
        hotspots,
    })
}

pub fn format_report(report: &ShaderReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{} shaders, {} indexed files, {} indexed symbols\n\n",
        report.total_shaders, report.indexed_files, report.indexed_symbols
    ));
    let has_profiler_timing = report
        .shaders
        .iter()
        .any(|shader| shader.total_duration_ns.is_some());
    let has_pipeline_stats = report
        .shaders
        .iter()
        .any(|shader| shader.instruction_count.is_some());
    let has_occupancy = report
        .shaders
        .iter()
        .any(|shader| shader.occupancy_percent.is_some());
    let has_counter_metrics = report.shaders.iter().any(|shader| {
        shader.alu_utilization_percent.is_some()
            || shader.last_level_cache_percent.is_some()
            || shader.device_memory_bandwidth_gbps.is_some()
            || shader.gpu_read_bandwidth_gbps.is_some()
            || shader.buffer_l1_miss_rate_percent.is_some()
    });
    let has_weighted_metrics = report.shaders.iter().any(|shader| {
        shader.weighted_percent_of_total.is_some() || shader.kernel_alu_performance.is_some()
    });
    let has_simd_groups = report.shaders.iter().any(|shader| shader.simd_groups > 0);
    if has_profiler_timing {
        out.push_str(&format!(
            "{:<32} {:<18} {:>10}",
            "Name", "Pipeline State", "Dispatches",
        ));
        if has_simd_groups {
            out.push_str(&format!(" {:>12} {:>8}", "SIMD Groups", "SIMD %"));
        }
        out.push_str(&format!(
            " {:>14} {:>8} {:>8}",
            "Duration ns", "Time %", "Exec %",
        ));
        if has_weighted_metrics {
            out.push_str(&format!(" {:>8} {:>10}", "Weight %", "ALU Perf"));
        }
        out.push_str(&format!(" {:>8} {:>10}", "Samples", "Samples/us"));
        if has_pipeline_stats {
            out.push_str(&format!(
                " {:>6} {:>8} {:>8} {:>8} {:>10}",
                "Regs", "Spills", "TGMem", "Inst", "Compile ms"
            ));
        }
        if has_occupancy {
            out.push_str(&format!(" {:>8}", "Occ %"));
        }
        if has_counter_metrics {
            out.push_str(&format!(
                " {:>8} {:>8} {:>10} {:>10} {:>10}",
                "ALU %", "LLC %", "Dev BW", "GPU R", "L1 Miss"
            ));
        }
        out.push_str("  Source\n");
    } else {
        out.push_str(&format!(
            "{:<32} {:<18} {:>10}",
            "Name", "Pipeline State", "Dispatches"
        ));
        if has_simd_groups {
            out.push_str(&format!(" {:>12} {:>8}", "SIMD Groups", "SIMD %"));
        }
        if has_weighted_metrics {
            out.push_str(&format!(" {:>8} {:>10}", "Weight %", "ALU Perf"));
        }
        if has_pipeline_stats {
            out.push_str(&format!(
                " {:>6} {:>8} {:>8} {:>8} {:>10}",
                "Regs", "Spills", "TGMem", "Inst", "Compile ms"
            ));
        }
        if has_occupancy {
            out.push_str(&format!(" {:>8}", "Occ %"));
        }
        if has_counter_metrics {
            out.push_str(&format!(
                " {:>8} {:>8} {:>10} {:>10} {:>10}",
                "ALU %", "LLC %", "Dev BW", "GPU R", "L1 Miss"
            ));
        }
        out.push_str("  Source\n");
    }
    for shader in &report.shaders {
        let source = match (&shader.source_file, shader.source_line) {
            (Some(file), Some(line)) => format!("{}:{}", file.display(), line),
            _ => "-".to_owned(),
        };
        if has_profiler_timing {
            out.push_str(&format!(
                "{:<32} 0x{:<16x} {:>10}",
                truncate(&shader.name, 36),
                shader.pipeline_addr,
                shader.dispatch_count,
            ));
            if has_simd_groups {
                out.push_str(&format!(
                    " {:>12} {:>8}",
                    shader.simd_groups,
                    shader
                        .simd_percent_of_total
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                ));
            }
            out.push_str(&format!(
                " {:>14} {:>7} {:>8}",
                shader
                    .total_duration_ns
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .percent_of_total
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_owned()),
                shader
                    .execution_cost_percent
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "-".to_owned()),
            ));
            if has_weighted_metrics {
                out.push_str(&format!(
                    " {:>8} {:>10}",
                    shader
                        .weighted_percent_of_total
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                    shader
                        .kernel_alu_performance
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                ));
            }
            out.push_str(&format!(
                " {:>8} {:>10}",
                shader.sample_count,
                shader
                    .avg_sampling_density
                    .map(|value| format!("{value:.3}"))
                    .unwrap_or_else(|| "-".to_owned()),
            ));
            if has_pipeline_stats {
                out.push_str(&format!(
                    " {:>6} {:>8} {:>8} {:>8} {:>10}",
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
                        .compilation_time_ms
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned())
                ));
            }
            if has_occupancy {
                out.push_str(&format!(
                    " {:>8}",
                    shader
                        .occupancy_percent
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned())
                ));
            }
            if has_counter_metrics {
                out.push_str(&format!(
                    " {:>8} {:>8} {:>10} {:>10} {:>10}",
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
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                    shader
                        .gpu_read_bandwidth_gbps
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                    shader
                        .buffer_l1_miss_rate_percent
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned())
                ));
            }
            out.push_str(&format!("  {source}\n"));
        } else {
            out.push_str(&format!(
                "{:<32} 0x{:<16x} {:>10}",
                truncate(&shader.name, 36),
                shader.pipeline_addr,
                shader.dispatch_count,
            ));
            if has_simd_groups {
                out.push_str(&format!(
                    " {:>12} {:>8}",
                    shader.simd_groups,
                    shader
                        .simd_percent_of_total
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                ));
            }
            if has_weighted_metrics {
                out.push_str(&format!(
                    " {:>8} {:>10}",
                    shader
                        .weighted_percent_of_total
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                    shader
                        .kernel_alu_performance
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                ));
            }
            if has_pipeline_stats {
                out.push_str(&format!(
                    " {:>6} {:>8} {:>8} {:>8} {:>10}",
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
                        .compilation_time_ms
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned())
                ));
            }
            if has_occupancy {
                out.push_str(&format!(
                    " {:>8}",
                    shader
                        .occupancy_percent
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned())
                ));
            }
            if has_counter_metrics {
                out.push_str(&format!(
                    " {:>8} {:>8} {:>10} {:>10} {:>10}",
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
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                    shader
                        .gpu_read_bandwidth_gbps
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned()),
                    shader
                        .buffer_l1_miss_rate_percent
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "-".to_owned())
                ));
            }
            out.push_str(&format!("  {source}\n"));
        }
    }
    out
}

pub fn format_csv(report: &ShaderReport) -> String {
    let mut out = String::new();
    out.push_str("name,pipeline_addr,dispatch_count,metric_source,simd_groups,simd_percent_of_total,total_duration_ns,percent_of_total,execution_cost_percent,weighted_cost,weighted_percent_of_total,kernel_alu_performance,execution_cost_samples,sample_count,avg_sampling_density,occupancy_percent,occupancy_confidence,alu_utilization_percent,last_level_cache_percent,device_memory_bandwidth_gbps,gpu_read_bandwidth_gbps,gpu_write_bandwidth_gbps,buffer_l1_miss_rate_percent,buffer_l1_read_accesses,buffer_l1_write_accesses,temporary_register_count,spilled_bytes,threadgroup_memory,instruction_count,alu_instruction_count,branch_instruction_count,compilation_time_ms,source_file,source_line\n");
    for shader in &report.shaders {
        let source_file = shader
            .source_file
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default();
        let columns = vec![
            format!("\"{}\"", shader.name.replace('"', "\"\"")),
            format!("0x{:x}", shader.pipeline_addr),
            shader.dispatch_count.to_string(),
            format!("\"{}\"", shader.metric_source.replace('"', "\"\"")),
            shader.simd_groups.to_string(),
            option_csv(shader.simd_percent_of_total),
            option_csv(shader.total_duration_ns),
            option_csv(shader.percent_of_total),
            option_csv(shader.execution_cost_percent),
            option_csv(shader.weighted_cost),
            option_csv(shader.weighted_percent_of_total),
            option_csv(shader.kernel_alu_performance),
            shader.execution_cost_samples.to_string(),
            shader.sample_count.to_string(),
            option_csv(shader.avg_sampling_density),
            option_csv(shader.occupancy_percent),
            option_csv(shader.occupancy_confidence),
            option_csv(shader.alu_utilization_percent),
            option_csv(shader.last_level_cache_percent),
            option_csv(shader.device_memory_bandwidth_gbps),
            option_csv(shader.gpu_read_bandwidth_gbps),
            option_csv(shader.gpu_write_bandwidth_gbps),
            option_csv(shader.buffer_l1_miss_rate_percent),
            option_csv(shader.buffer_l1_read_accesses),
            option_csv(shader.buffer_l1_write_accesses),
            option_csv(shader.temporary_register_count),
            option_csv(shader.spilled_bytes),
            option_csv(shader.threadgroup_memory),
            option_csv(shader.instruction_count),
            option_csv(shader.alu_instruction_count),
            option_csv(shader.branch_instruction_count),
            option_csv(shader.compilation_time_ms),
            format!("\"{}\"", source_file.replace('"', "\"\"")),
            option_csv(shader.source_line),
        ];
        out.push_str(&columns.join(","));
        out.push('\n');
    }
    out
}

pub fn format_source(report: &ShaderSourceReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("Shader: {}\n", report.shader_name));
    out.push_str(&format!("Pipeline: 0x{:x}\n", report.pipeline_addr));
    out.push_str(&format!("Dispatches: {}\n", report.dispatch_count));
    out.push_str(&format!(
        "Source: {}:{}\n\n",
        report.source_file.display(),
        report.source_line
    ));
    for line in &report.excerpt {
        let marker = if line.highlight { ">" } else { " " };
        out.push_str(&format!("{marker} {:>5} | {}\n", line.number, line.text));
    }
    out
}

pub fn format_hotspot_report(report: &ShaderHotspotReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("Shader: {}\n", report.shader_name));
    out.push_str(&format!("Pipeline: 0x{:x}\n", report.pipeline_addr));
    out.push_str(&format!("Dispatches: {}\n", report.dispatch_count));
    out.push_str(&format!(
        "Source: {}:{}-{}\n",
        report.source_file.display(),
        report.start_line,
        report.end_line
    ));
    out.push_str(&format!(
        "Attributed GPU %: {:.2} ({})\n\n",
        report.total_gpu_percent, report.metric_source
    ));
    out.push_str("Hot spots\n");
    for hotspot in &report.hotspots {
        out.push_str(&format!(
            "  L{:>4} {:>6.2}% {:<8} {}\n",
            hotspot.line_number,
            hotspot.attributed_gpu_percent,
            hotspot.instruction_type,
            hotspot.text.trim()
        ));
        for hint in &hotspot.hints {
            out.push_str(&format!("         hint: {hint}\n"));
        }
    }
    out.push_str("\nAnnotated source\n");
    for line in &report.lines {
        let marker = if line.hotspot { ">" } else { " " };
        out.push_str(&format!(
            "{marker} {:>5} {:>6.2}% {:<8} | {}\n",
            line.line_number, line.attributed_gpu_percent, line.instruction_type, line.text
        ));
    }
    out
}

pub fn default_search_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(env_paths) = env::var_os("GPUTRACE_SHADER_SEARCH_PATHS") {
        paths.extend(env::split_paths(&env_paths));
    }
    for candidate in [
        "/opt/homebrew/Cellar/mlx-c",
        "./mlx/backend/metal",
        "../mlx/backend/metal",
    ] {
        let path = PathBuf::from(candidate);
        if path.exists() {
            paths.push(path);
        }
    }
    paths
}

impl ShaderSourceIndex {
    fn build(search_paths: &[PathBuf]) -> Result<Self> {
        let mut index = Self {
            kernel_to_file: BTreeMap::new(),
            kernel_to_line: BTreeMap::new(),
        };
        let kernel_regex = Regex::new(r"kernel\s+void\s+(\w+)\s*\(")
            .map_err(|error| Error::InvalidInput(format!("invalid kernel regex: {error}")))?;
        let func_regex = Regex::new(
            r"^\s*(?:inline\s+)?(?:device\s+|constant\s+)?(?:void|float|int|half|uint)\s+(\w+)\s*\(",
        )
        .map_err(|error| Error::InvalidInput(format!("invalid function regex: {error}")))?;

        for root in search_paths {
            if !root.exists() {
                continue;
            }
            for entry in WalkDir::new(root)
                .into_iter()
                .filter_map(|entry| entry.ok())
            {
                if entry.file_type().is_dir() {
                    continue;
                }
                if entry.path().extension().and_then(|ext| ext.to_str()) != Some("metal") {
                    continue;
                }
                index.index_file(entry.path(), &kernel_regex, &func_regex)?;
            }
        }
        Ok(index)
    }

    fn index_file(&mut self, path: &Path, kernel_regex: &Regex, func_regex: &Regex) -> Result<()> {
        let contents = fs::read_to_string(path)?;
        for (line_idx, line) in contents.lines().enumerate() {
            if let Some(captures) = kernel_regex.captures(line)
                && let Some(name) = captures.get(1)
            {
                self.kernel_to_file
                    .insert(name.as_str().to_owned(), path.to_path_buf());
                self.kernel_to_line
                    .insert(name.as_str().to_owned(), line_idx + 1);
                continue;
            }
            if let Some(captures) = func_regex.captures(line)
                && let Some(name) = captures.get(1)
            {
                self.kernel_to_file
                    .entry(name.as_str().to_owned())
                    .or_insert_with(|| path.to_path_buf());
                self.kernel_to_line
                    .entry(name.as_str().to_owned())
                    .or_insert(line_idx + 1);
            }
        }
        Ok(())
    }

    fn lookup(&self, kernel_name: &str) -> Option<(PathBuf, usize)> {
        if let Some(file) = self.kernel_to_file.get(kernel_name) {
            return Some((
                file.clone(),
                *self.kernel_to_line.get(kernel_name).unwrap_or(&1),
            ));
        }
        let stripped = strip_type_suffixes(kernel_name);
        if let Some(file) = self.kernel_to_file.get(&stripped) {
            return Some((
                file.clone(),
                *self.kernel_to_line.get(&stripped).unwrap_or(&1),
            ));
        }
        for (known, file) in &self.kernel_to_file {
            if kernel_name.contains(known) || known.contains(kernel_name) {
                return Some((file.clone(), *self.kernel_to_line.get(known).unwrap_or(&1)));
            }
        }
        None
    }

    fn stats(&self) -> (usize, usize) {
        let files: std::collections::BTreeSet<_> = self.kernel_to_file.values().collect();
        (files.len(), self.kernel_to_file.len())
    }
}

fn strip_type_suffixes(name: &str) -> String {
    for suffix in [
        "_float32",
        "_float16",
        "_float",
        "_int32",
        "_int64",
        "_int",
        "_uint32",
        "_uint64",
        "_uint",
        "_half",
        "_bfloat16",
    ] {
        if let Some(stripped) = name.strip_suffix(suffix) {
            return stripped.to_owned();
        }
    }
    name.to_owned()
}

fn truncate(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_owned();
    }
    let keep = width.saturating_sub(3);
    format!("{}...", &value[..keep])
}

fn function_bounds(lines: &[String], source_line: usize) -> (usize, usize) {
    let mut start_line = source_line.max(1).min(lines.len().max(1));
    while start_line > 1 {
        let prev = lines[start_line - 2].trim();
        if prev.starts_with("kernel ") || prev.contains(" kernel ") {
            start_line -= 1;
            break;
        }
        if prev.ends_with('{') {
            start_line -= 1;
            break;
        }
        start_line -= 1;
    }

    let mut brace_depth = 0i32;
    let mut seen_open = false;
    let mut end_line = source_line.max(start_line);
    for (idx, line) in lines.iter().enumerate().skip(start_line - 1) {
        brace_depth += line.matches('{').count() as i32;
        if line.contains('{') {
            seen_open = true;
        }
        brace_depth -= line.matches('}').count() as i32;
        end_line = idx + 1;
        if seen_open && brace_depth <= 0 {
            break;
        }
    }
    (start_line, end_line)
}

fn classify_instruction(line: &str) -> (String, u32) {
    if line.is_empty() || line.starts_with("//") {
        return ("other".to_owned(), 0);
    }
    if line.contains("texture.")
        || line.contains(".sample(")
        || line.contains(".read(")
        || line.contains(".write(")
        || (line.contains("device") && (line.contains('[') || line.contains('*')))
    {
        let complexity = if line.contains("texture") { 5 } else { 3 };
        return ("memory".to_owned(), complexity);
    }
    if line.contains("if ")
        || line.contains("for ")
        || line.contains("while ")
        || line.contains("return")
    {
        return ("control".to_owned(), 2);
    }
    if line.contains('*')
        || line.contains('+')
        || line.contains('-')
        || line.contains('/')
        || line.contains("sqrt")
        || line.contains("exp")
        || line.contains("log")
        || line.contains("sin")
        || line.contains("cos")
    {
        let complexity = if line.contains("sin") || line.contains("cos") {
            5
        } else if line.contains("sqrt") || line.contains("exp") || line.contains("log") {
            4
        } else {
            2
        };
        return ("compute".to_owned(), complexity);
    }
    ("other".to_owned(), 1)
}

fn estimate_line_cost(line: &str, instruction_type: &str, complexity: u32) -> f64 {
    if line.is_empty() || line.starts_with("//") {
        return 0.0;
    }
    let mut base_cost = complexity as f64;
    match instruction_type {
        "memory" => base_cost *= 2.0,
        "compute" => base_cost *= 1.5,
        "control" => base_cost *= 1.0,
        _ => base_cost *= 0.5,
    }
    base_cost
}

fn attribute_line_costs(
    lines: &mut [AttributedSourceLine],
    total_gpu_percent: f64,
    instruction_count: Option<i64>,
    alu_instruction_count: Option<i64>,
    branch_instruction_count: Option<i64>,
    execution_cost_percent: Option<f64>,
    alu_utilization_percent: Option<f64>,
    last_level_cache_percent: Option<f64>,
    device_memory_bandwidth_gbps: Option<f64>,
) {
    let total_cost: f64 = lines.iter().map(|line| line.estimated_cost).sum();
    if total_cost <= f64::EPSILON || total_gpu_percent <= f64::EPSILON {
        return;
    }

    let mut compute_weight = 1.25;
    let mut memory_weight = 1.5;
    let mut control_weight = 0.75;
    if let Some(total_instructions) = instruction_count.filter(|value| *value > 0) {
        let total_instructions = total_instructions as f64;
        if let Some(alu) = alu_instruction_count {
            let alu_ratio = (alu.max(0) as f64 / total_instructions).clamp(0.0, 1.0);
            compute_weight += alu_ratio * 1.5;
            memory_weight += (1.0 - alu_ratio) * 0.35;
        }
        if let Some(branch) = branch_instruction_count {
            let branch_ratio = (branch.max(0) as f64 / total_instructions).clamp(0.0, 1.0);
            control_weight += branch_ratio * 3.0;
        }
    }

    if let Some(alu_utilization) = alu_utilization_percent {
        let normalized = normalize_percent_like(alu_utilization);
        if normalized > 50.0 {
            compute_weight += (normalized - 50.0) / 100.0;
        }
    }
    if let Some(llc) = last_level_cache_percent {
        let normalized = normalize_percent_like(llc);
        if normalized > 5.0 {
            memory_weight += (normalized / 100.0).min(0.75);
        }
    }
    if let Some(bandwidth) = device_memory_bandwidth_gbps {
        if bandwidth > 10.0 {
            memory_weight += 1.0;
        } else if bandwidth > 2.0 {
            memory_weight += 0.5;
        }
    }
    if let Some(exec_cost) = execution_cost_percent {
        if exec_cost > 40.0 {
            compute_weight += 0.35;
            memory_weight += 0.35;
        } else if exec_cost > 20.0 {
            compute_weight += 0.15;
            memory_weight += 0.15;
        }
    }

    let weighted_total: f64 = lines
        .iter()
        .map(|line| {
            let weight = match line.instruction_type.as_str() {
                "memory" => memory_weight,
                "compute" => compute_weight,
                "control" => control_weight,
                _ => 0.5,
            };
            line.estimated_cost * weight
        })
        .sum();
    if weighted_total <= f64::EPSILON {
        return;
    }

    for line in lines {
        let weight = match line.instruction_type.as_str() {
            "memory" => memory_weight,
            "compute" => compute_weight,
            "control" => control_weight,
            _ => 0.5,
        };
        line.attributed_gpu_percent =
            total_gpu_percent * ((line.estimated_cost * weight) / weighted_total);
    }
}

fn normalize_percent_like(value: f64) -> f64 {
    if value <= 1.0 { value * 100.0 } else { value }
}

fn line_hints(line: &AttributedSourceLine) -> Vec<String> {
    let mut hints = Vec::new();
    if !line.hotspot {
        return hints;
    }
    match line.instruction_type.as_str() {
        "memory" => {
            hints.push("Check access locality and coalescing around this load/store.".to_owned());
            if line.text.contains("texture") {
                hints.push(
                    "Texture fetch hot spots often benefit from cache-friendly sampling patterns."
                        .to_owned(),
                );
            }
        }
        "compute" => {
            hints.push("Math-heavy lines are candidates for approximation or common-subexpression cleanup.".to_owned());
            if line.text.contains("sqrt") || line.text.contains("exp") || line.text.contains("log")
            {
                hints.push(
                    "Transcendental operations are comparatively expensive on Apple GPUs."
                        .to_owned(),
                );
            }
        }
        "control" => {
            hints.push("Branch-heavy hot lines can cause SIMD divergence.".to_owned());
        }
        _ => {}
    }
    hints
}

fn dispatch_simd_groups(dispatch: &crate::trace::DispatchCall) -> u64 {
    let tg_x = div_ceil_or_one(dispatch.grid_size[0], dispatch.group_size[0]);
    let tg_y = div_ceil_or_one(dispatch.grid_size[1], dispatch.group_size[1]);
    let tg_z = div_ceil_or_one(dispatch.grid_size[2], dispatch.group_size[2]);
    let threadgroups = tg_x.saturating_mul(tg_y).saturating_mul(tg_z);
    let threads_per_group = dispatch.group_size[0] as u64
        * dispatch.group_size[1] as u64
        * dispatch.group_size[2] as u64;
    let total_threads = threadgroups.saturating_mul(threads_per_group);
    total_threads.div_ceil(32)
}

fn div_ceil_or_one(total: u32, per_group: u32) -> u64 {
    if per_group == 0 {
        return 1;
    }
    (total as u64).div_ceil(per_group as u64)
}

fn compare_option_f64_desc(left: Option<f64>, right: Option<f64>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left
            .partial_cmp(&right)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn compare_option_u64_desc(left: Option<u64>, right: Option<u64>) -> std::cmp::Ordering {
    left.unwrap_or_default().cmp(&right.unwrap_or_default())
}

fn option_csv<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xcode_counters::{XcodeCounterData, XcodeEncoderCounters};

    #[test]
    fn strips_type_suffixes() {
        assert_eq!(strip_type_suffixes("rope_float16"), "rope");
        assert_eq!(strip_type_suffixes("kernel"), "kernel");
    }

    #[test]
    fn matches_xcode_counters_by_normalized_encoder_label() {
        let data = XcodeCounterData {
            source: PathBuf::from("/tmp/example.csv"),
            metrics: vec![
                "ALU Utilization".into(),
                "Kernel Occupancy".into(),
                "Device Memory Bandwidth".into(),
                "Kernel ALU Performance".into(),
                "GPU Read Bandwidth".into(),
                "GPU Write Bandwidth".into(),
                "Buffer L1 Miss Rate".into(),
                "Buffer L1 Read Accesses".into(),
                "Buffer L1 Write Accesses".into(),
            ],
            encoders: vec![XcodeEncoderCounters {
                index: 0,
                function_index: 0,
                command_buffer_label: "cb0".into(),
                encoder_label: "encoder0_simple_add".into(),
                counters: BTreeMap::from([
                    ("ALU Utilization".into(), 62.5),
                    ("Kernel Occupancy".into(), 37.5),
                    ("Device Memory Bandwidth".into(), 4.2),
                    ("Kernel ALU Performance".into(), 2048.0),
                    ("GPU Read Bandwidth".into(), 6.1),
                    ("GPU Write Bandwidth".into(), 2.3),
                    ("Buffer L1 Miss Rate".into(), 11.0),
                    ("Buffer L1 Read Accesses".into(), 128.0),
                    ("Buffer L1 Write Accesses".into(), 16.0),
                ]),
            }],
        };

        let matched = match_xcode_counters("simple_add", &data).unwrap();
        assert_eq!(matched.alu_utilization_percent, Some(62.5));
        assert_eq!(matched.occupancy_percent, Some(37.5));
        assert_eq!(matched.device_memory_bandwidth_gbps, Some(4.2));
        assert_eq!(matched.kernel_alu_performance, Some(2048.0));
        assert_eq!(matched.gpu_read_bandwidth_gbps, Some(6.1));
        assert_eq!(matched.gpu_write_bandwidth_gbps, Some(2.3));
        assert_eq!(matched.buffer_l1_miss_rate_percent, Some(11.0));
        assert_eq!(matched.buffer_l1_read_accesses, Some(128.0));
        assert_eq!(matched.buffer_l1_write_accesses, Some(16.0));
    }

    #[test]
    fn formats_report_with_profiler_columns() {
        let report = ShaderReport {
            total_shaders: 1,
            indexed_files: 1,
            indexed_symbols: 1,
            shaders: vec![ShaderEntry {
                name: "kernel".into(),
                pipeline_addr: 0x1234,
                dispatch_count: 2,
                metric_source: "execution-cost".into(),
                simd_groups: 96,
                simd_percent_of_total: Some(48.0),
                total_duration_ns: Some(120),
                percent_of_total: Some(60.0),
                execution_cost_percent: Some(55.0),
                execution_cost_samples: 11,
                sample_count: 4,
                avg_sampling_density: Some(0.2),
                occupancy_percent: Some(37.5),
                occupancy_confidence: Some(0.8),
                alu_utilization_percent: Some(61.0),
                kernel_alu_performance: Some(2048.0),
                weighted_cost: Some(9.85),
                weighted_percent_of_total: Some(52.0),
                last_level_cache_percent: Some(0.04),
                device_memory_bandwidth_gbps: Some(8.2),
                gpu_read_bandwidth_gbps: Some(6.1),
                gpu_write_bandwidth_gbps: Some(2.3),
                buffer_l1_miss_rate_percent: Some(11.0),
                buffer_l1_read_accesses: Some(128.0),
                buffer_l1_write_accesses: Some(16.0),
                temporary_register_count: Some(48),
                spilled_bytes: Some(256),
                threadgroup_memory: Some(4096),
                instruction_count: Some(1024),
                alu_instruction_count: Some(800),
                branch_instruction_count: Some(16),
                compilation_time_ms: Some(3.5),
                source_file: Some(PathBuf::from("/tmp/kernel.metal")),
                source_line: Some(42),
            }],
        };

        let output = format_report(&report);
        assert!(output.contains("Duration ns"));
        assert!(output.contains("SIMD Groups"));
        assert!(output.contains("SIMD %"));
        assert!(output.contains("Time %"));
        assert!(output.contains("Exec %"));
        assert!(output.contains("Weight %"));
        assert!(output.contains("ALU Perf"));
        assert!(output.contains("Samples"));
        assert!(output.contains("Samples/us"));
        assert!(output.contains("Occ %"));
        assert!(output.contains("ALU %"));
        assert!(output.contains("Dev BW"));
        assert!(output.contains("GPU R"));
        assert!(output.contains("L1 Miss"));
        assert!(output.contains("Regs"));
        assert!(output.contains("Spills"));
        assert!(output.contains("Compile ms"));
        assert!(output.contains("60.00"));
        assert!(output.contains("48.00"));
        assert!(output.contains("55.00"));
        assert!(output.contains("52.00"));
        assert!(output.contains("2048.00"));
        assert!(output.contains("37.50"));
        assert!(output.contains("61.00"));
        assert!(output.contains("8.20"));
        assert!(output.contains("6.10"));
        assert!(output.contains("11.00"));
        assert!(output.contains("48"));
        assert!(output.contains("256"));
    }

    #[test]
    fn formats_csv_with_profiler_and_simd_columns() {
        let report = ShaderReport {
            total_shaders: 1,
            indexed_files: 0,
            indexed_symbols: 0,
            shaders: vec![ShaderEntry {
                name: "kernel".into(),
                pipeline_addr: 0x1234,
                dispatch_count: 2,
                metric_source: "xcode-weighted".into(),
                simd_groups: 96,
                simd_percent_of_total: Some(48.0),
                total_duration_ns: Some(120),
                percent_of_total: Some(60.0),
                execution_cost_percent: Some(55.0),
                weighted_cost: Some(9.85),
                weighted_percent_of_total: Some(52.0),
                kernel_alu_performance: Some(2048.0),
                execution_cost_samples: 11,
                sample_count: 4,
                avg_sampling_density: Some(0.2),
                occupancy_percent: Some(37.5),
                occupancy_confidence: Some(0.8),
                alu_utilization_percent: Some(61.0),
                last_level_cache_percent: Some(0.04),
                device_memory_bandwidth_gbps: Some(8.2),
                gpu_read_bandwidth_gbps: Some(6.1),
                gpu_write_bandwidth_gbps: Some(2.3),
                buffer_l1_miss_rate_percent: Some(11.0),
                buffer_l1_read_accesses: Some(128.0),
                buffer_l1_write_accesses: Some(16.0),
                temporary_register_count: Some(48),
                spilled_bytes: Some(256),
                threadgroup_memory: Some(4096),
                instruction_count: Some(1024),
                alu_instruction_count: Some(800),
                branch_instruction_count: Some(16),
                compilation_time_ms: Some(3.5),
                source_file: Some(PathBuf::from("/tmp/kernel.metal")),
                source_line: Some(42),
            }],
        };

        let output = format_csv(&report);
        assert!(output.contains("metric_source"));
        assert!(output.contains("weighted_percent_of_total"));
        assert!(output.contains("kernel_alu_performance"));
        assert!(output.contains("gpu_read_bandwidth_gbps"));
        assert!(output.contains("buffer_l1_miss_rate_percent"));
        assert!(output.contains("simd_groups"));
        assert!(output.contains("alu_utilization_percent"));
        assert!(output.contains("device_memory_bandwidth_gbps"));
        assert!(output.contains("simd_percent_of_total"));
        assert!(output.contains("\"kernel\",0x1234,2,\"xcode-weighted\",96,48"));
        assert!(output.contains("\"/tmp/kernel.metal\",42"));
    }

    #[test]
    fn hotspot_prefers_xcode_weighted_percent_when_execution_cost_is_missing() {
        let shader = ShaderEntry {
            name: "kernel".into(),
            pipeline_addr: 0x1234,
            dispatch_count: 2,
            metric_source: "xcode-weighted".into(),
            simd_groups: 0,
            simd_percent_of_total: None,
            total_duration_ns: Some(120),
            percent_of_total: Some(60.0),
            execution_cost_percent: None,
            weighted_cost: Some(9.85),
            weighted_percent_of_total: Some(52.0),
            kernel_alu_performance: Some(2048.0),
            execution_cost_samples: 0,
            sample_count: 0,
            avg_sampling_density: None,
            occupancy_percent: None,
            occupancy_confidence: None,
            alu_utilization_percent: Some(61.0),
            last_level_cache_percent: None,
            device_memory_bandwidth_gbps: None,
            gpu_read_bandwidth_gbps: None,
            gpu_write_bandwidth_gbps: None,
            buffer_l1_miss_rate_percent: None,
            buffer_l1_read_accesses: None,
            buffer_l1_write_accesses: None,
            temporary_register_count: None,
            spilled_bytes: None,
            threadgroup_memory: None,
            instruction_count: None,
            alu_instruction_count: None,
            branch_instruction_count: None,
            compilation_time_ms: None,
            source_file: Some(PathBuf::from("/tmp/kernel.metal")),
            source_line: Some(42),
        };

        assert_eq!(shader.metric_source, "xcode-weighted");
        assert_eq!(
            shader
                .execution_cost_percent
                .or(shader.weighted_percent_of_total)
                .or(shader.percent_of_total),
            Some(52.0)
        );
    }

    #[test]
    fn formats_hotspot_report() {
        let report = ShaderHotspotReport {
            shader_name: "kernel".into(),
            pipeline_addr: 0x1234,
            dispatch_count: 2,
            source_file: PathBuf::from("/tmp/kernel.metal"),
            start_line: 40,
            end_line: 44,
            total_gpu_percent: 55.0,
            metric_source: "execution-cost".into(),
            hotspots: vec![AttributedSourceLine {
                line_number: 42,
                text: "value = texture.read(index);".into(),
                instruction_type: "memory".into(),
                complexity: 5,
                estimated_cost: 10.0,
                attributed_gpu_percent: 22.5,
                hotspot: true,
                hints: vec!["Check access locality and coalescing around this load/store.".into()],
            }],
            lines: vec![AttributedSourceLine {
                line_number: 42,
                text: "value = texture.read(index);".into(),
                instruction_type: "memory".into(),
                complexity: 5,
                estimated_cost: 10.0,
                attributed_gpu_percent: 22.5,
                hotspot: true,
                hints: vec!["Check access locality and coalescing around this load/store.".into()],
            }],
        };

        let output = format_hotspot_report(&report);
        assert!(output.contains("Hot spots"));
        assert!(output.contains("Annotated source"));
        assert!(output.contains("execution-cost"));
        assert!(output.contains("22.50%"));
        assert!(output.contains("texture.read(index)"));
    }

    #[test]
    fn hotspot_weighting_prefers_compute_when_alu_mix_is_high() {
        let mut lines = vec![
            AttributedSourceLine {
                line_number: 10,
                text: "sum += a * b + c;".into(),
                instruction_type: "compute".into(),
                complexity: 3,
                estimated_cost: 6.0,
                attributed_gpu_percent: 0.0,
                hotspot: false,
                hints: vec![],
            },
            AttributedSourceLine {
                line_number: 11,
                text: "out[gid] = texture.read(gid);".into(),
                instruction_type: "memory".into(),
                complexity: 3,
                estimated_cost: 6.0,
                attributed_gpu_percent: 0.0,
                hotspot: false,
                hints: vec![],
            },
        ];

        attribute_line_costs(
            &mut lines,
            60.0,
            Some(1000),
            Some(850),
            Some(20),
            Some(55.0),
            Some(72.0),
            Some(0.03),
            Some(1.2),
        );

        assert!(lines[0].attributed_gpu_percent > lines[1].attributed_gpu_percent);
    }

    #[test]
    fn hotspot_weighting_prefers_memory_when_bandwidth_and_cache_pressure_are_high() {
        let mut lines = vec![
            AttributedSourceLine {
                line_number: 10,
                text: "sum += a * b + c;".into(),
                instruction_type: "compute".into(),
                complexity: 3,
                estimated_cost: 6.0,
                attributed_gpu_percent: 0.0,
                hotspot: false,
                hints: vec![],
            },
            AttributedSourceLine {
                line_number: 11,
                text: "out[gid] = texture.read(gid);".into(),
                instruction_type: "memory".into(),
                complexity: 3,
                estimated_cost: 6.0,
                attributed_gpu_percent: 0.0,
                hotspot: false,
                hints: vec![],
            },
        ];

        attribute_line_costs(
            &mut lines,
            60.0,
            Some(1000),
            Some(150),
            Some(20),
            Some(48.0),
            Some(24.0),
            Some(0.18),
            Some(14.0),
        );

        assert!(lines[1].attributed_gpu_percent > lines[0].attributed_gpu_percent);
    }
}
