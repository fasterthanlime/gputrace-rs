use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde::Serialize;
use walkdir::WalkDir;

use crate::error::{Error, Result};
use crate::profiler;
use crate::trace::TraceBundle;

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

pub fn report(trace: &TraceBundle, search_paths: &[PathBuf]) -> Result<ShaderReport> {
    let index = ShaderSourceIndex::build(search_paths)?;
    let profiler_summary = profiler::stream_data_summary(&trace.path).ok();
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
            let pipeline_stats = pipeline_stats_by_addr
                .get(&kernel.pipeline_addr)
                .cloned()
                .or_else(|| pipeline_stats_by_name.get(&kernel_name).cloned());
            ShaderEntry {
                name: kernel_name.clone(),
                pipeline_addr: kernel.pipeline_addr,
                dispatch_count: kernel.dispatch_count,
                simd_groups,
                simd_percent_of_total,
                total_duration_ns: total_duration_ns_for_shader,
                percent_of_total,
                execution_cost_percent: execution_cost_by_name.get(&kernel_name).copied(),
                execution_cost_samples: execution_cost_samples_by_name
                    .get(&kernel_name)
                    .copied()
                    .unwrap_or(0),
                sample_count: sample_count_by_name.get(&kernel_name).copied().unwrap_or(0),
                avg_sampling_density,
                occupancy_percent: occupancy.map(|(value, _)| value),
                occupancy_confidence: occupancy.map(|(_, confidence)| confidence),
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
    shaders.sort_by(|left, right| {
        compare_option_f64_desc(right.execution_cost_percent, left.execution_cost_percent)
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
    let metric_source = if shader.execution_cost_percent.is_some() {
        "execution-cost".to_owned()
    } else if shader.percent_of_total.is_some() {
        "profiler-duration".to_owned()
    } else if shader.simd_percent_of_total.is_some() {
        "simd-groups".to_owned()
    } else {
        "unattributed".to_owned()
    };
    let total_gpu_percent = shader
        .execution_cost_percent
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
            " {:>14} {:>8} {:>8} {:>8} {:>10}",
            "Duration ns", "Time %", "Exec %", "Samples", "Samples/us",
        ));
        if has_pipeline_stats {
            out.push_str(&format!(
                " {:>6} {:>8} {:>8} {:>8} {:>10}",
                "Regs", "Spills", "TGMem", "Inst", "Compile ms"
            ));
        }
        if has_occupancy {
            out.push_str(&format!(" {:>8}", "Occ %"));
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
        if has_pipeline_stats {
            out.push_str(&format!(
                " {:>6} {:>8} {:>8} {:>8} {:>10}",
                "Regs", "Spills", "TGMem", "Inst", "Compile ms"
            ));
        }
        if has_occupancy {
            out.push_str(&format!(" {:>8}", "Occ %"));
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
                " {:>14} {:>7} {:>8} {:>8} {:>10}",
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
            out.push_str(&format!("  {source}\n"));
        }
    }
    out
}

pub fn format_csv(report: &ShaderReport) -> String {
    let mut out = String::new();
    out.push_str("name,pipeline_addr,dispatch_count,simd_groups,simd_percent_of_total,total_duration_ns,percent_of_total,execution_cost_percent,execution_cost_samples,sample_count,avg_sampling_density,occupancy_percent,occupancy_confidence,temporary_register_count,spilled_bytes,threadgroup_memory,instruction_count,alu_instruction_count,branch_instruction_count,compilation_time_ms,source_file,source_line\n");
    for shader in &report.shaders {
        let source_file = shader
            .source_file
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default();
        out.push_str(&format!(
            "\"{}\",0x{:x},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},\"{}\",{}\n",
            shader.name.replace('"', "\"\""),
            shader.pipeline_addr,
            shader.dispatch_count,
            shader.simd_groups,
            option_csv(shader.simd_percent_of_total),
            option_csv(shader.total_duration_ns),
            option_csv(shader.percent_of_total),
            option_csv(shader.execution_cost_percent),
            shader.execution_cost_samples,
            shader.sample_count,
            option_csv(shader.avg_sampling_density),
            option_csv(shader.occupancy_percent),
            option_csv(shader.occupancy_confidence),
            option_csv(shader.temporary_register_count),
            option_csv(shader.spilled_bytes),
            option_csv(shader.threadgroup_memory),
            option_csv(shader.instruction_count),
            option_csv(shader.alu_instruction_count),
            option_csv(shader.branch_instruction_count),
            option_csv(shader.compilation_time_ms),
            source_file.replace('"', "\"\""),
            option_csv(shader.source_line),
        ));
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
) {
    let total_cost: f64 = lines.iter().map(|line| line.estimated_cost).sum();
    if total_cost <= f64::EPSILON || total_gpu_percent <= f64::EPSILON {
        return;
    }

    let mut compute_weight = 1.5;
    let mut memory_weight = 2.0;
    let mut control_weight = 1.0;
    if let Some(total_instructions) = instruction_count.filter(|value| *value > 0) {
        if let Some(alu) = alu_instruction_count {
            compute_weight += (alu.max(0) as f64 / total_instructions as f64) * 0.5;
        }
        if let Some(branch) = branch_instruction_count {
            control_weight += (branch.max(0) as f64 / total_instructions as f64) * 0.5;
        }
        memory_weight += (1.0 - (compute_weight - 1.5).min(1.0)) * 0.25;
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

    #[test]
    fn strips_type_suffixes() {
        assert_eq!(strip_type_suffixes("rope_float16"), "rope");
        assert_eq!(strip_type_suffixes("kernel"), "kernel");
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
        assert!(output.contains("Samples"));
        assert!(output.contains("Samples/us"));
        assert!(output.contains("Occ %"));
        assert!(output.contains("Regs"));
        assert!(output.contains("Spills"));
        assert!(output.contains("Compile ms"));
        assert!(output.contains("60.00"));
        assert!(output.contains("48.00"));
        assert!(output.contains("55.00"));
        assert!(output.contains("37.50"));
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
        assert!(output.contains("simd_groups"));
        assert!(output.contains("simd_percent_of_total"));
        assert!(output.contains("\"kernel\",0x1234,2,96,48"));
        assert!(output.contains("\"/tmp/kernel.metal\",42"));
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
}
