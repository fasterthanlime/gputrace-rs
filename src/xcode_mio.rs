use std::path::PathBuf;
use std::time::Instant;

use serde::Serialize;

use crate::error::{Error, Result};
use crate::profiler;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioReport {
    pub trace_source: PathBuf,
    pub profiler_directory: PathBuf,
    pub stream_data_path: PathBuf,
    pub framework_path: PathBuf,
    pub timings: XcodeMioTimings,
    pub gpu_command_count: usize,
    pub encoder_count: usize,
    pub pipeline_state_count: usize,
    pub draw_count: usize,
    pub cost_record_count: usize,
    pub gpu_time_ns: u64,
    pub cost_timeline: Option<XcodeMioCostTimeline>,
    pub timeline_candidates: Vec<XcodeMioTimelineCandidate>,
    pub timeline_binary_count: usize,
    pub timeline_binaries: Vec<XcodeMioTimelineBinary>,
    pub timeline_pipeline_state_ids: Vec<u64>,
    pub shader_binary_info: Vec<XcodeMioShaderBinaryInfo>,
    pub decoded_cost_records: Vec<XcodeMioDecodedCostRecord>,
    pub draw_timeline_records: Vec<XcodeMioDrawTimelineRecord>,
    pub draw_metadata_records: Vec<XcodeMioDrawMetadataRecord>,
    pub pipelines: Vec<XcodeMioPipeline>,
    pub gpu_commands: Vec<XcodeMioGpuCommand>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct XcodeMioTimings {
    pub total_ms: f64,
    pub locate_profiler_ms: f64,
    pub stream_data_summary_ms: f64,
    pub framework_load_ms: f64,
    pub stream_data_load_ms: f64,
    pub processor_init_ms: f64,
    pub process_stream_ms: f64,
    pub extract_result_ms: f64,
    pub decode_pipeline_commands_ms: f64,
    pub shader_profiler_probe_ms: f64,
    pub cost_timeline_request_ms: f64,
    pub cost_timeline_decode_ms: f64,
    pub final_metadata_ms: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipeline {
    pub index: usize,
    pub object_id: u64,
    pub pointer_id: u64,
    pub function_index: u64,
    pub gpu_command_count: usize,
    pub timeline_draw_count: usize,
    pub timeline_duration_ns: u64,
    pub timeline_total_cost: f64,
    pub pipeline_address: Option<u64>,
    pub function_name: Option<String>,
    pub binary_keys: Vec<u64>,
    pub all_binary_keys: Vec<u64>,
    pub shader_stats: Vec<XcodeMioPipelineShaderStat>,
    pub profiler_timings: Vec<XcodeMioPipelineProfilerTiming>,
    pub scope_costs: Vec<XcodeMioPipelineScopeCost>,
    pub shader_tracks: Vec<XcodeMioPipelineShaderTrack>,
    pub shader_binaries: Vec<XcodeMioPipelineShaderBinary>,
    pub shader_binary_costs: Vec<XcodeMioPipelineShaderBinaryCost>,
    pub agxps_trace_costs: Vec<XcodeMioPipelineAgxpsTraceCost>,
    pub shader_profiler_costs: Vec<XcodeMioPipelineShaderProfilerCost>,
    pub execution_history: Vec<XcodeMioPipelineExecutionHistory>,
    pub shader_binary_references: Vec<XcodeMioPipelineShaderBinaryReference>,
    pub pipeline_counters: Vec<XcodeMioPipelineCounter>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineProfilerTiming {
    pub source: &'static str,
    pub cycle_average: f64,
    pub cycle_min: f64,
    pub cycle_max: f64,
    pub time_average: f64,
    pub time_min: f64,
    pub time_max: f64,
    pub percentage_average: f64,
    pub percentage_min: f64,
    pub percentage_max: f64,
    pub surplus_cycles: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderProfilerCost {
    pub binary_key: String,
    pub full_path: Option<String>,
    pub type_name: Option<String>,
    pub shader_type: u32,
    pub addr_start: u32,
    pub addr_end: u32,
    pub total_binary_cost: f64,
    pub total_binary_samples: u64,
    pub pipeline_cost: f64,
    pub pipeline_cost_percent_sum: f64,
    pub nonzero_draw_count: usize,
    pub first_draw_index: usize,
    pub last_draw_index: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineAgxpsTraceCost {
    pub source: &'static str,
    pub shader_address: u64,
    pub work_shader_address: u64,
    pub command_count: usize,
    pub record_cliques: u64,
    pub analyzer_weighted_duration: u64,
    pub analyzer_avg_duration_sum: u64,
    pub matched_work_cliques: usize,
    pub duration_ns: u64,
    pub execution_events: u64,
    pub stats_word0: u64,
    pub stats_word1: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineCounter {
    pub name: String,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioTimelineBinary {
    pub index: u64,
    pub address: u64,
    pub program_type: u16,
    pub instruction_info_count: u64,
    pub instruction_executed: u64,
    pub duration_ns: u64,
    pub trace_count: u64,
    pub cost_count: u64,
    pub total_cost: f64,
    pub total_instruction_count: u64,
    pub instruction_cost_record_count: u64,
    pub instruction_nonzero_record_count: u64,
    pub instruction_total_cost: f64,
    pub instruction_total_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioTimelineCandidate {
    pub source: &'static str,
    pub draw_count: usize,
    pub pipeline_state_count: usize,
    pub cost_record_count: usize,
    pub gpu_time_ns: u64,
    pub global_gpu_time_ns: u64,
    pub timeline_duration_ns: u64,
    pub total_clique_cost: u64,
    pub gpu_cost: f64,
    pub gpu_cost_instruction_count: u64,
    pub timeline_binary_count: usize,
    pub shader_binary_info_count: usize,
    pub nonzero_cost_records: usize,
    pub decoded_total_cost: f64,
    pub decoded_instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderBinaryReference {
    pub raw5: u16,
    pub raw6: u16,
    pub raw1: u32,
    pub address: u64,
    pub timeline_binary_index: Option<u64>,
    pub record_count: usize,
    pub first_command_index: usize,
    pub last_command_index: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderBinary {
    pub pipeline_id_kind: &'static str,
    pub pipeline_id: u64,
    pub binary_index: u64,
    pub address: u64,
    pub program_type: u16,
    pub instruction_info_count: u64,
    pub instruction_executed: u64,
    pub duration_ns: u64,
    pub trace_count: u64,
    pub cost_count: u64,
    pub total_cost: f64,
    pub total_instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderBinaryCost {
    pub source: &'static str,
    pub pipeline_id_kind: &'static str,
    pub pipeline_id: u64,
    pub binary_index: u64,
    pub address: u64,
    pub program_type: u16,
    pub record_count: u64,
    pub nonzero_record_count: u64,
    pub total_cost: f64,
    pub total_instruction_count: u64,
    pub alu_cost: f64,
    pub non_alu_cost: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineExecutionHistory {
    pub style: u32,
    pub options: u32,
    pub program_type: u16,
    pub pipeline_id_kind: &'static str,
    pub pipeline_id: u64,
    pub top_cost_percentage: f64,
    pub duration_percentage: f64,
    pub total_duration_ns: u64,
    pub total_cost: f64,
    pub instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioShaderBinaryInfo {
    pub index: usize,
    pub raw0: u32,
    pub raw1: u32,
    pub raw2: u64,
    pub raw3: u64,
    pub raw4: u16,
    pub raw5: u16,
    pub raw6: u16,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderTrack {
    pub source: &'static str,
    pub pipeline_id_kind: &'static str,
    pub pipeline_id: u64,
    pub program_type: u16,
    pub track_id: i32,
    pub first_index: u64,
    pub start_timestamp_ns: u64,
    pub end_timestamp_ns: u64,
    pub duration_ns: u64,
    pub trace_count: u64,
    pub traces: Vec<XcodeMioBinaryTrace>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioBinaryTrace {
    pub index: usize,
    pub start_timestamp_ns: u64,
    pub end_timestamp_ns: u64,
    pub raw_identifier: u64,
    pub raw_index: u32,
    pub raw_count: u32,
    pub raw_program_type: u16,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderStat {
    pub shader_id_kind: &'static str,
    pub shader_id: u64,
    pub program_type: u16,
    pub number_of_cliques: u64,
    pub total_gpu_cycles: u64,
    pub total_latency: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineScopeCost {
    pub scope: u16,
    pub identifier_kind: &'static str,
    pub identifier: u64,
    pub level: u16,
    pub level_identifier: u32,
    pub alu_cost: f64,
    pub non_alu_cost: f64,
    pub total_cost: f64,
    pub instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioDecodedCostRecord {
    pub index: usize,
    pub level: u16,
    pub scope: u16,
    pub level_identifier: u32,
    pub scope_identifier: u64,
    pub alu_cost: f64,
    pub non_alu_cost: f64,
    pub total_cost: f64,
    pub instruction_count: u64,
    pub threads_executing_instruction: u64,
    pub cpi_weighted_instruction_count: u64,
    pub active_thread_instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioDrawTimelineRecord {
    pub index: usize,
    pub raw0: u64,
    pub raw1: u64,
    pub raw2: u32,
    pub raw3: u16,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioDrawMetadataRecord {
    pub index: usize,
    pub raw0: u32,
    pub raw1: u32,
    pub raw2: u32,
    pub raw3: u32,
    pub raw4: i32,
    pub raw5: u32,
    pub raw6: u64,
    pub raw7: u32,
    pub raw8: u32,
    pub raw9: u32,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioGpuCommand {
    pub index: usize,
    pub function_index: u64,
    pub sub_command_index: i32,
    pub encoder_index: usize,
    pub pipeline_index: usize,
    pub pipeline_object_id: u64,
    pub command_buffer_index: usize,
    pub function_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioCostTimeline {
    pub draw_count: usize,
    pub pipeline_state_count: usize,
    pub cost_record_count: usize,
    pub gpu_time_ns: u64,
    pub global_gpu_time_ns: u64,
    pub timeline_duration_ns: u64,
    pub total_clique_cost: u64,
    pub gpu_cost: f64,
    pub gpu_cost_instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioAnalysisReport {
    pub backend: &'static str,
    pub trace_source: PathBuf,
    pub timings: XcodeMioTimings,
    pub gpu_time_ns: u64,
    pub gpu_command_count: usize,
    pub pipeline_state_count: usize,
    pub cost_record_count: usize,
    pub top_pipelines: Vec<XcodeMioPipelineAnalysis>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineAnalysis {
    pub pipeline_index: usize,
    pub object_id: u64,
    pub pipeline_address: Option<u64>,
    pub function_name: Option<String>,
    pub command_count: usize,
    pub command_percent: f64,
    pub shader_binary_reference_count: usize,
    pub executable_shader_binary_reference_count: usize,
    pub unique_timeline_binary_count: usize,
    pub referenced_instruction_info_count: u64,
    pub xcode_time_percent: Option<f64>,
    pub xcode_time_average: Option<f64>,
    pub xcode_cycle_average: Option<f64>,
    pub timeline_duration_ns: u64,
    pub timeline_duration_percent: Option<f64>,
    pub timeline_total_cost: f64,
    pub timeline_cost_percent: Option<f64>,
    pub shader_profiler_cost: f64,
    pub shader_profiler_cost_percent: Option<f64>,
    pub shader_binary_cost: f64,
    pub shader_binary_cost_percent: Option<f64>,
    pub agxps_trace_cost: u64,
    pub agxps_trace_cost_percent: Option<f64>,
    pub agxps_trace_events: u64,
    pub agxps_trace_matched_work_cliques: usize,
    pub execution_top_cost_percent: Option<f64>,
    pub execution_duration_percent: Option<f64>,
    pub execution_total_cost: Option<f64>,
    pub execution_instruction_count: Option<u64>,
    pub counters: Vec<XcodeMioPipelineCounter>,
    pub metric_sources: Vec<String>,
}

pub fn report(trace: &TraceBundle) -> Result<XcodeMioReport> {
    report_with_profiler_summary(trace, None)
}

pub fn report_with_profiler_summary(
    trace: &TraceBundle,
    precomputed_profiler_summary: Option<&profiler::ProfilerStreamDataSummary>,
) -> Result<XcodeMioReport> {
    let total_start = Instant::now();
    let locate_start = Instant::now();
    let profiler_directory = profiler::find_profiler_directory(&trace.path)
        .ok_or_else(|| Error::NotFound(trace.path.clone()))?;
    let locate_profiler_ms = elapsed_ms(locate_start);
    let stream_data_path = profiler_directory.join("streamData");
    if !stream_data_path.is_file() {
        return Err(Error::NotFound(stream_data_path));
    }

    let summary_start = Instant::now();
    let profiler_summary_owned;
    let profiler_summary = if let Some(summary) = precomputed_profiler_summary {
        Some(summary)
    } else {
        profiler_summary_owned = profiler::stream_data_summary(&trace.path).ok();
        profiler_summary_owned.as_ref()
    };
    let mut timings = XcodeMioTimings {
        locate_profiler_ms,
        stream_data_summary_ms: elapsed_ms(summary_start),
        ..XcodeMioTimings::default()
    };
    let mut report = platform::decode(
        trace.path.clone(),
        profiler_directory,
        stream_data_path,
        profiler_summary,
        timings.clone(),
    )?;
    timings = report.timings.clone();
    timings.total_ms = elapsed_ms(total_start);
    report.timings = timings;
    Ok(report)
}

pub fn analysis_report(trace: &TraceBundle) -> Result<XcodeMioAnalysisReport> {
    Ok(summarize_report(&report(trace)?))
}

pub fn summarize_report(report: &XcodeMioReport) -> XcodeMioAnalysisReport {
    let command_denominator = report.gpu_command_count.max(1) as f64;
    let timeline_duration_denominator = report
        .cost_timeline
        .as_ref()
        .map(|timeline| timeline.global_gpu_time_ns)
        .filter(|value| *value > 0)
        .unwrap_or(report.gpu_time_ns);
    let timeline_cost_denominator = report
        .pipelines
        .iter()
        .map(|pipeline| pipeline.timeline_total_cost)
        .filter(|value| value.is_finite() && *value > 0.0)
        .sum::<f64>();
    let shader_profiler_cost_denominator = report
        .pipelines
        .iter()
        .flat_map(|pipeline| pipeline.shader_profiler_costs.iter())
        .map(|cost| cost.pipeline_cost)
        .filter(|value| value.is_finite() && *value > 0.0)
        .sum::<f64>();
    let shader_binary_cost_denominator = report
        .pipelines
        .iter()
        .flat_map(|pipeline| pipeline.shader_binary_costs.iter())
        .map(|cost| cost.total_cost)
        .filter(|value| value.is_finite() && *value > 0.0)
        .sum::<f64>();
    let agxps_trace_cost_denominator = report
        .pipelines
        .iter()
        .flat_map(|pipeline| pipeline.agxps_trace_costs.iter())
        .map(|cost| cost.stats_word1)
        .sum::<u64>();
    let timeline_binaries_by_index = report
        .timeline_binaries
        .iter()
        .map(|binary| (binary.index, binary))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut top_pipelines = report
        .pipelines
        .iter()
        .map(|pipeline| {
            let best_timing = pipeline.profiler_timings.iter().max_by(|left, right| {
                left.percentage_average
                    .partial_cmp(&right.percentage_average)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let shader_profiler_cost = pipeline
                .shader_profiler_costs
                .iter()
                .map(|cost| cost.pipeline_cost)
                .filter(|value| value.is_finite())
                .sum::<f64>();
            let shader_binary_cost = pipeline
                .shader_binary_costs
                .iter()
                .map(|cost| cost.total_cost)
                .filter(|value| value.is_finite())
                .sum::<f64>();
            let agxps_trace_cost = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.stats_word1)
                .sum::<u64>();
            let agxps_trace_events = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.execution_events)
                .sum::<u64>();
            let agxps_trace_matched_work_cliques = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.matched_work_cliques)
                .sum::<usize>();
            let best_execution = pipeline.execution_history.iter().max_by(|left, right| {
                left.top_cost_percentage
                    .partial_cmp(&right.top_cost_percentage)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let shader_binary_reference_count = pipeline
                .shader_binary_references
                .iter()
                .map(|reference| reference.record_count)
                .sum::<usize>();
            let executable_shader_binary_reference_count = pipeline
                .shader_binary_references
                .iter()
                .filter(|reference| reference.raw6 == 28)
                .map(|reference| reference.record_count)
                .sum::<usize>();
            let unique_timeline_binary_count = pipeline
                .shader_binary_references
                .iter()
                .map(|reference| reference.timeline_binary_index.unwrap_or(reference.address))
                .collect::<std::collections::BTreeSet<_>>()
                .len();
            let referenced_instruction_info_count = pipeline
                .shader_binary_references
                .iter()
                .filter_map(|reference| {
                    let binary =
                        timeline_binaries_by_index.get(&reference.timeline_binary_index?)?;
                    Some(
                        binary
                            .instruction_info_count
                            .saturating_mul(reference.record_count as u64),
                    )
                })
                .fold(0u64, |sum, count| sum.saturating_add(count));
            let mut counters = pipeline.pipeline_counters.clone();
            counters.sort_by(|left, right| {
                right
                    .value
                    .abs()
                    .partial_cmp(&left.value.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| left.name.cmp(&right.name))
            });
            counters.truncate(8);

            let mut metric_sources = Vec::new();
            if best_timing
                .is_some_and(|timing| timing.percentage_average > 0.0 || timing.time_average > 0.0)
            {
                metric_sources.push("xcode-profiler-timing".to_owned());
            }
            if pipeline.timeline_duration_ns > 0 || pipeline.timeline_total_cost > 0.0 {
                metric_sources.push("xcode-cost-timeline".to_owned());
            }
            if shader_profiler_cost > 0.0 {
                metric_sources.push("xcode-shader-profiler-cost".to_owned());
            }
            if shader_binary_cost > 0.0 {
                metric_sources.push("xcode-shader-binary-cost".to_owned());
            }
            if agxps_trace_cost > 0 {
                metric_sources.push("agxps-timing-trace".to_owned());
            }
            if best_execution.is_some_and(|history| {
                history.top_cost_percentage > 0.0
                    || history.duration_percentage > 0.0
                    || history.total_cost > 0.0
            }) {
                metric_sources.push("xcode-execution-history".to_owned());
            }
            if !counters.is_empty() {
                metric_sources.push("xcode-pipeline-counters".to_owned());
            }
            if pipeline.gpu_command_count > 0 {
                metric_sources.push("xcode-gpu-command-topology".to_owned());
            }
            if shader_binary_reference_count > 0 {
                metric_sources.push("xcode-shader-binary-references".to_owned());
            }

            XcodeMioPipelineAnalysis {
                pipeline_index: pipeline.index,
                object_id: pipeline.object_id,
                pipeline_address: pipeline.pipeline_address,
                function_name: pipeline.function_name.clone(),
                command_count: pipeline.gpu_command_count,
                command_percent: pipeline.gpu_command_count as f64 * 100.0 / command_denominator,
                shader_binary_reference_count,
                executable_shader_binary_reference_count,
                unique_timeline_binary_count,
                referenced_instruction_info_count,
                xcode_time_percent: best_timing.map(|timing| timing.percentage_average),
                xcode_time_average: best_timing.map(|timing| timing.time_average),
                xcode_cycle_average: best_timing.map(|timing| timing.cycle_average),
                timeline_duration_ns: pipeline.timeline_duration_ns,
                timeline_duration_percent: (timeline_duration_denominator > 0
                    && pipeline.timeline_duration_ns > 0)
                    .then(|| {
                        pipeline.timeline_duration_ns as f64 * 100.0
                            / timeline_duration_denominator as f64
                    }),
                timeline_total_cost: pipeline.timeline_total_cost,
                timeline_cost_percent: (timeline_cost_denominator > 0.0
                    && pipeline.timeline_total_cost > 0.0)
                    .then(|| pipeline.timeline_total_cost * 100.0 / timeline_cost_denominator),
                shader_profiler_cost,
                shader_profiler_cost_percent: (shader_profiler_cost_denominator > 0.0
                    && shader_profiler_cost > 0.0)
                    .then(|| shader_profiler_cost * 100.0 / shader_profiler_cost_denominator),
                shader_binary_cost,
                shader_binary_cost_percent: (shader_binary_cost_denominator > 0.0
                    && shader_binary_cost > 0.0)
                    .then(|| shader_binary_cost * 100.0 / shader_binary_cost_denominator),
                agxps_trace_cost,
                agxps_trace_cost_percent: (agxps_trace_cost_denominator > 0
                    && agxps_trace_cost > 0)
                    .then(|| agxps_trace_cost as f64 * 100.0 / agxps_trace_cost_denominator as f64),
                agxps_trace_events,
                agxps_trace_matched_work_cliques,
                execution_top_cost_percent: best_execution
                    .map(|history| history.top_cost_percentage),
                execution_duration_percent: best_execution
                    .map(|history| history.duration_percentage),
                execution_total_cost: best_execution.map(|history| history.total_cost),
                execution_instruction_count: best_execution
                    .map(|history| history.instruction_count),
                counters,
                metric_sources,
            }
        })
        .collect::<Vec<_>>();

    top_pipelines.sort_by(|left, right| {
        pipeline_rank_score(right)
            .partial_cmp(&pipeline_rank_score(left))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.command_count.cmp(&left.command_count))
            .then_with(|| left.pipeline_index.cmp(&right.pipeline_index))
    });

    XcodeMioAnalysisReport {
        backend: "xcode-mio",
        trace_source: report.trace_source.clone(),
        timings: report.timings.clone(),
        gpu_time_ns: report.gpu_time_ns,
        gpu_command_count: report.gpu_command_count,
        pipeline_state_count: report.pipeline_state_count,
        cost_record_count: report.cost_record_count,
        top_pipelines,
        warnings: report.warnings.clone(),
    }
}

pub fn format_analysis_report(report: &XcodeMioAnalysisReport) -> String {
    let mut out = String::new();
    out.push_str("Xcode MIO analysis\n");
    out.push_str(&format!(
        "backend={} gpu_time={:.3} ms commands={} pipelines={} cost_records={} wall={:.1} ms\n\n",
        report.backend,
        report.gpu_time_ns as f64 / 1_000_000.0,
        report.gpu_command_count,
        report.pipeline_state_count,
        report.cost_record_count,
        report.timings.total_ms,
    ));
    out.push_str(&format!(
        "timings: locate={:.1} ms stream_summary={:.1} ms framework={:.1} ms stream_load={:.1} ms process={:.1} ms extract={:.1} ms topology={:.1} ms probes={:.1} ms cost_request={:.1} ms cost_decode={:.1} ms metadata={:.1} ms\n\n",
        report.timings.locate_profiler_ms,
        report.timings.stream_data_summary_ms,
        report.timings.framework_load_ms,
        report.timings.stream_data_load_ms,
        report.timings.process_stream_ms,
        report.timings.extract_result_ms,
        report.timings.decode_pipeline_commands_ms,
        report.timings.shader_profiler_probe_ms,
        report.timings.cost_timeline_request_ms,
        report.timings.cost_timeline_decode_ms,
        report.timings.final_metadata_ms,
    ));
    out.push_str(&format!(
        "{:<42} {:>5} {:>7} {:>7} {:>7} {:>8} {:>8} {:>8} {:>8}\n",
        "Function", "Cmds", "Cmd %", "Bins", "ExecBin", "AGXPS %", "Time %", "TL Cost", "Exec %"
    ));
    for pipeline in report.top_pipelines.iter().take(25) {
        out.push_str(&format!(
            "{:<42} {:>5} {:>6.2}% {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}\n",
            truncate(
                pipeline
                    .function_name
                    .as_deref()
                    .unwrap_or("<unknown function>"),
                42,
            ),
            pipeline.command_count,
            pipeline.command_percent,
            pipeline.unique_timeline_binary_count,
            pipeline.executable_shader_binary_reference_count,
            format_optional_percent(pipeline.agxps_trace_cost_percent),
            format_optional_percent(pipeline.xcode_time_percent),
            format_optional_percent(pipeline.timeline_cost_percent),
            format_optional_percent(pipeline.execution_top_cost_percent),
        ));
    }
    if !report.warnings.is_empty() {
        out.push_str("\nWarnings:\n");
        for warning in &report.warnings {
            out.push_str(&format!("  - {warning}\n"));
        }
    }
    out
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1_000.0
}

fn pipeline_rank_score(pipeline: &XcodeMioPipelineAnalysis) -> f64 {
    [
        pipeline.execution_top_cost_percent,
        pipeline.agxps_trace_cost_percent,
        pipeline.shader_profiler_cost_percent,
        pipeline.timeline_cost_percent,
        pipeline.timeline_duration_percent,
        pipeline.xcode_time_percent,
        Some(pipeline.command_percent),
    ]
    .into_iter()
    .flatten()
    .filter(|value| value.is_finite())
    .fold(0.0, f64::max)
}

fn format_optional_percent(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map(|value| format!("{value:.2}%"))
        .unwrap_or_else(|| "-".to_owned())
}

fn truncate(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_owned();
    }
    let keep = width.saturating_sub(3);
    format!("{}...", &value[..keep])
}

pub fn format_report(report: &XcodeMioReport) -> String {
    let mut out = String::new();
    out.push_str("Xcode private MIO decode\n");
    out.push_str(&format!(
        "streamData: {}\nframework: {}\n\n",
        report.stream_data_path.display(),
        report.framework_path.display()
    ));
    out.push_str(&format!(
        "gpu_commands={} encoders={} pipelines={} draws={} cost_records={} gpu_time={:.3} ms\n\n",
        report.gpu_command_count,
        report.encoder_count,
        report.pipeline_state_count,
        report.draw_count,
        report.cost_record_count,
        report.gpu_time_ns as f64 / 1_000_000.0
    ));
    if let Some(timeline) = &report.cost_timeline {
        out.push_str(&format!(
            "cost_timeline: draws={} pipelines={} cost_records={} global_gpu_time={:.3} ms total_clique_cost={} gpu_cost={:.3} instr={}\n",
            timeline.draw_count,
            timeline.pipeline_state_count,
            timeline.cost_record_count,
            timeline.global_gpu_time_ns as f64 / 1_000_000.0,
            timeline.total_clique_cost,
            timeline.gpu_cost,
            timeline.gpu_cost_instruction_count,
        ));
        out.push('\n');
    } else {
        out.push('\n');
    }
    if !report.timeline_candidates.is_empty() {
        out.push_str("timeline candidates:\n");
        for candidate in &report.timeline_candidates {
            out.push_str(&format!(
                "  {:<28} draws={:<4} pipelines={:<3} costs={:<4} gpu={:>7.3} ms global={:>7.3} ms clique_cost={} gpu_cost={:.3} instr={} binaries={} shader_info={} nonzero_costs={} decoded_cost={:.3}\n",
                candidate.source,
                candidate.draw_count,
                candidate.pipeline_state_count,
                candidate.cost_record_count,
                candidate.gpu_time_ns as f64 / 1_000_000.0,
                candidate.global_gpu_time_ns as f64 / 1_000_000.0,
                candidate.total_clique_cost,
                candidate.gpu_cost,
                candidate.gpu_cost_instruction_count,
                candidate.timeline_binary_count,
                candidate.shader_binary_info_count,
                candidate.nonzero_cost_records,
                candidate.decoded_total_cost,
            ));
        }
        out.push('\n');
    }
    if report.timeline_binary_count > 0 || !report.shader_binary_info.is_empty() {
        out.push_str(&format!(
            "timeline_binaries={} timeline_pipeline_states={} shader_binary_info={}\n\n",
            report
                .timeline_binaries
                .len()
                .max(report.timeline_binary_count),
            report.timeline_pipeline_state_ids.len(),
            report.shader_binary_info.len()
        ));
    }
    let mut binary_references = report
        .pipelines
        .iter()
        .flat_map(|pipeline| {
            let mut groups = std::collections::BTreeMap::<(u16, u16, u32), (usize, usize)>::new();
            for reference in pipeline
                .shader_binary_references
                .iter()
                .filter(|reference| reference.raw5 == 6 && reference.raw6 == 28)
            {
                let entry = groups
                    .entry((reference.raw5, reference.raw6, reference.raw1))
                    .or_insert((0, 0));
                entry.0 += reference.record_count;
                entry.1 += 1;
            }
            groups
                .into_iter()
                .map(move |((raw5, raw6, raw1), (record_count, binary_count))| {
                    (pipeline, raw5, raw6, raw1, record_count, binary_count)
                })
        })
        .collect::<Vec<_>>();
    binary_references.sort_by(
        |(left_pipeline, _, _, left_raw1, left_count, _),
         (right_pipeline, _, _, right_raw1, right_count, _)| {
            right_count
                .cmp(left_count)
                .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
                .then_with(|| left_raw1.cmp(right_raw1))
        },
    );
    if !binary_references.is_empty() {
        out.push_str("Pipelines by shader-binary references:\n");
        for (pipeline, raw5, raw6, raw1, record_count, binary_count) in
            binary_references.iter().take(20)
        {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            out.push_str(&format!(
                "  {:>4} refs  {:>4} binaries  raw1={:<5} kind={}/{}  {:<56}\n",
                record_count, binary_count, raw1, raw5, raw6, name,
            ));
        }
        out.push('\n');
    }
    let mut pipeline_counters = report
        .pipelines
        .iter()
        .filter(|pipeline| !pipeline.pipeline_counters.is_empty())
        .collect::<Vec<_>>();
    pipeline_counters.sort_by(|left, right| {
        let left_max = left
            .pipeline_counters
            .iter()
            .map(|counter| counter.value.abs())
            .fold(0.0, f64::max);
        let right_max = right
            .pipeline_counters
            .iter()
            .map(|counter| counter.value.abs())
            .fold(0.0, f64::max);
        right_max
            .partial_cmp(&left_max)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.index.cmp(&right.index))
    });
    if !pipeline_counters.is_empty() {
        out.push_str("Pipelines by private pipeline counters:\n");
        for pipeline in pipeline_counters.iter().take(12) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let counters = pipeline
                .pipeline_counters
                .iter()
                .take(4)
                .map(|counter| format!("{}={:.3}", counter.name, counter.value))
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!("  {:<56} {}\n", name, counters));
        }
        out.push('\n');
    }
    let mut agxps_trace_costs = report
        .pipelines
        .iter()
        .filter(|pipeline| !pipeline.agxps_trace_costs.is_empty())
        .collect::<Vec<_>>();
    agxps_trace_costs.sort_by(|left, right| {
        let left_cost = left
            .agxps_trace_costs
            .iter()
            .map(|cost| cost.stats_word1)
            .sum::<u64>();
        let right_cost = right
            .agxps_trace_costs
            .iter()
            .map(|cost| cost.stats_word1)
            .sum::<u64>();
        right_cost
            .cmp(&left_cost)
            .then_with(|| left.index.cmp(&right.index))
    });
    let agxps_trace_denominator = report
        .pipelines
        .iter()
        .flat_map(|pipeline| pipeline.agxps_trace_costs.iter())
        .map(|cost| cost.stats_word1)
        .sum::<u64>();
    if agxps_trace_denominator > 0 {
        out.push_str("Pipelines by AGXPS timing-trace instruction stats:\n");
        for pipeline in agxps_trace_costs.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let total_cost = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.stats_word1)
                .sum::<u64>();
            let analyzer_weighted = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.analyzer_weighted_duration)
                .sum::<u64>();
            let total_events = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.execution_events)
                .sum::<u64>();
            let matched_cliques = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.matched_work_cliques)
                .sum::<usize>();
            let command_count = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.command_count)
                .sum::<usize>();
            let pct = 100.0 * total_cost as f64 / agxps_trace_denominator as f64;
            let top = pipeline
                .agxps_trace_costs
                .iter()
                .max_by_key(|cost| cost.stats_word1);
            let top_address = top
                .map(|cost| format!("0x{:x}", cost.shader_address))
                .unwrap_or_else(|| "-".to_owned());
            out.push_str(&format!(
                "  {:>6.2}% w1={:>12} analyzer_weighted={:>12} events={:>9} cliques={:>7} cmds={:>4} {:<56} top_esl={}\n",
                pct,
                total_cost,
                analyzer_weighted,
                total_events,
                matched_cliques,
                command_count,
                name,
                top_address,
            ));
        }
        out.push('\n');
    }
    let agxps_analyzer_denominator = report
        .pipelines
        .iter()
        .flat_map(|pipeline| pipeline.agxps_trace_costs.iter())
        .map(|cost| cost.analyzer_weighted_duration)
        .sum::<u64>();
    if agxps_analyzer_denominator > 0 {
        let mut pipelines = report
            .pipelines
            .iter()
            .filter(|pipeline| {
                pipeline
                    .agxps_trace_costs
                    .iter()
                    .any(|cost| cost.analyzer_weighted_duration > 0)
            })
            .collect::<Vec<_>>();
        pipelines.sort_by(|left, right| {
            let left_cost = left
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.analyzer_weighted_duration)
                .sum::<u64>();
            let right_cost = right
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.analyzer_weighted_duration)
                .sum::<u64>();
            right_cost
                .cmp(&left_cost)
                .then_with(|| left.index.cmp(&right.index))
        });
        out.push_str("Pipelines by AGXPS timing-analyzer clique duration:\n");
        for pipeline in pipelines.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let weighted = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.analyzer_weighted_duration)
                .sum::<u64>();
            let avg_sum = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.analyzer_avg_duration_sum)
                .sum::<u64>();
            let record_cliques = pipeline
                .agxps_trace_costs
                .iter()
                .map(|cost| cost.record_cliques)
                .sum::<u64>();
            let pct = 100.0 * weighted as f64 / agxps_analyzer_denominator as f64;
            out.push_str(&format!(
                "  {:>6.3}% weighted={:>12} avg_sum={:>10} rec_cliques={:>7} {:<56}\n",
                pct, weighted, avg_sum, record_cliques, name,
            ));
        }
        out.push('\n');
    }
    let agxps_event_denominator = report
        .pipelines
        .iter()
        .flat_map(|pipeline| pipeline.agxps_trace_costs.iter())
        .map(|cost| cost.execution_events)
        .sum::<u64>();
    let mut agxps_rows = report
        .pipelines
        .iter()
        .flat_map(|pipeline| {
            pipeline
                .agxps_trace_costs
                .iter()
                .map(move |cost| (pipeline, cost))
        })
        .collect::<Vec<_>>();
    agxps_rows.sort_by(|(left_pipeline, left_cost), (right_pipeline, right_cost)| {
        left_cost
            .shader_address
            .cmp(&right_cost.shader_address)
            .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
    });
    if !agxps_rows.is_empty() {
        out.push_str("AGXPS timing rows by ESL shader address:\n");
        out.push_str(
            "  row analyzer%      w1%   events% analyzer_weighted           w1     events cmds rec_cliques matched function      esl_shader\n",
        );
        for (row_index, (pipeline, cost)) in agxps_rows.iter().enumerate().take(80) {
            let name = pipeline.function_name.as_deref().unwrap_or("<unknown>");
            let analyzer_pct =
                percent_u64(cost.analyzer_weighted_duration, agxps_analyzer_denominator);
            let w1_pct = percent_u64(cost.stats_word1, agxps_trace_denominator);
            let events_pct = percent_u64(cost.execution_events, agxps_event_denominator);
            out.push_str(&format!(
                "  {row:>3} {analyzer_pct:>8.4}% {w1_pct:>8.4}% {events_pct:>8.4}% {weighted:>17} {w1:>12} {events:>10} {cmds:>4} {cliques:>11} {matched:>7} {name:<13} 0x{address:x}\n",
                row = row_index,
                weighted = cost.analyzer_weighted_duration,
                w1 = cost.stats_word1,
                events = cost.execution_events,
                cmds = cost.command_count,
                cliques = cost.record_cliques,
                matched = cost.matched_work_cliques,
                address = cost.shader_address,
            ));
        }
        out.push('\n');
    }
    let mut shader_binary_costs = report
        .pipelines
        .iter()
        .flat_map(|pipeline| {
            pipeline
                .shader_binary_costs
                .iter()
                .map(move |cost| (pipeline, cost))
        })
        .filter(|(_, cost)| cost.total_cost > 0.0 || cost.total_instruction_count > 0)
        .collect::<Vec<_>>();
    shader_binary_costs.sort_by(|(left_pipeline, left), (right_pipeline, right)| {
        right
            .total_cost
            .partial_cmp(&left.total_cost)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .total_instruction_count
                    .cmp(&left.total_instruction_count)
            })
            .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
    });
    if !shader_binary_costs.is_empty() {
        let denominator = shader_binary_costs
            .iter()
            .map(|(_, cost)| cost.total_cost)
            .filter(|value| value.is_finite() && *value > 0.0)
            .sum::<f64>();
        out.push_str("Pipelines by shader instruction-cost arrays:\n");
        for (pipeline, cost) in shader_binary_costs.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let pct = if denominator > 0.0 {
                100.0 * cost.total_cost / denominator
            } else {
                0.0
            };
            out.push_str(&format!(
                "  {:>6.2}% cost={:>10.3} instr={:>10} nonzero={:>4}/{:<4} ptype={:<2} {:<56} {}={} binary={} addr=0x{:x}\n",
                pct,
                cost.total_cost,
                cost.total_instruction_count,
                cost.nonzero_record_count,
                cost.record_count,
                cost.program_type,
                name,
                cost.pipeline_id_kind,
                cost.pipeline_id,
                cost.binary_index,
                cost.address,
            ));
        }
        out.push('\n');
    }
    let mut profiler_timings = report
        .pipelines
        .iter()
        .filter_map(|pipeline| {
            let best = pipeline.profiler_timings.iter().max_by(|left, right| {
                left.percentage_average
                    .partial_cmp(&right.percentage_average)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })?;
            (best.percentage_average > 0.0 || best.time_average > 0.0).then_some((pipeline, best))
        })
        .collect::<Vec<_>>();
    profiler_timings.sort_by(|(left_pipeline, left), (right_pipeline, right)| {
        right
            .percentage_average
            .partial_cmp(&left.percentage_average)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .time_average
                    .partial_cmp(&left.time_average)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
    });
    if !profiler_timings.is_empty() {
        out.push_str("Pipelines by Xcode profiler timing:\n");
        for (pipeline, timing) in profiler_timings.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            out.push_str(&format!(
                "  {:>6.2}% time={:>10.3} cycles={:>10.3} source={:<8} {:<56}\n",
                timing.percentage_average,
                timing.time_average,
                timing.cycle_average,
                timing.source,
                name,
            ));
        }
        out.push('\n');
    }
    let mut shader_profiler_costs = report
        .pipelines
        .iter()
        .filter(|pipeline| !pipeline.shader_profiler_costs.is_empty())
        .collect::<Vec<_>>();
    shader_profiler_costs.sort_by(|left, right| {
        let left_cost = left
            .shader_profiler_costs
            .iter()
            .map(|cost| cost.pipeline_cost)
            .sum::<f64>();
        let right_cost = right
            .shader_profiler_costs
            .iter()
            .map(|cost| cost.pipeline_cost)
            .sum::<f64>();
        right_cost
            .partial_cmp(&left_cost)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.index.cmp(&right.index))
    });
    let shader_profiler_denominator = report
        .pipelines
        .iter()
        .flat_map(|pipeline| pipeline.shader_profiler_costs.iter())
        .map(|cost| cost.pipeline_cost)
        .filter(|value| value.is_finite() && *value > 0.0)
        .sum::<f64>();
    if shader_profiler_denominator > 0.0 {
        out.push_str("Pipelines by Xcode shader-profiler per-draw cost:\n");
        for pipeline in shader_profiler_costs.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let pipeline_cost = pipeline
                .shader_profiler_costs
                .iter()
                .map(|cost| cost.pipeline_cost)
                .sum::<f64>();
            let draw_count = pipeline
                .shader_profiler_costs
                .iter()
                .map(|cost| cost.nonzero_draw_count)
                .sum::<usize>();
            let pct = 100.0 * pipeline_cost / shader_profiler_denominator;
            let top_binary = pipeline.shader_profiler_costs.iter().max_by(|left, right| {
                left.pipeline_cost
                    .partial_cmp(&right.pipeline_cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let key = top_binary
                .map(|cost| cost.binary_key.as_str())
                .unwrap_or("-");
            out.push_str(&format!(
                "  {:>6.2}% cost={:>10.3} draws={:>4} {:<56} key={}\n",
                pct, pipeline_cost, draw_count, name, key,
            ));
        }
        out.push('\n');
    }
    let mut execution_history = report
        .pipelines
        .iter()
        .flat_map(|pipeline| {
            pipeline
                .execution_history
                .iter()
                .map(move |history| (pipeline, history))
        })
        .filter(|(_, history)| {
            history.top_cost_percentage > 0.0
                || history.duration_percentage > 0.0
                || history.total_cost > 0.0
        })
        .collect::<Vec<_>>();
    execution_history.sort_by(|(left_pipeline, left), (right_pipeline, right)| {
        right
            .top_cost_percentage
            .partial_cmp(&left.top_cost_percentage)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .total_cost
                    .partial_cmp(&left.total_cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
    });
    if !execution_history.is_empty() {
        out.push_str("Pipelines by Xcode execution-history cost:\n");
        for (pipeline, history) in execution_history.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            out.push_str(&format!(
                "  {:>6.2}% dur={:>6.2}% cost={:>10.3} instr={:>10} ptype={:<2} style={}/{} {:<56} {}={}\n",
                history.top_cost_percentage,
                history.duration_percentage,
                history.total_cost,
                history.instruction_count,
                history.program_type,
                history.style,
                history.options,
                name,
                history.pipeline_id_kind,
                history.pipeline_id,
            ));
        }
        out.push('\n');
    }
    let mut shader_tracks = report
        .pipelines
        .iter()
        .flat_map(|pipeline| {
            pipeline
                .shader_tracks
                .iter()
                .map(move |track| (pipeline, track))
        })
        .filter(|(_, track)| track.duration_ns > 0 || track.trace_count > 0)
        .collect::<Vec<_>>();
    shader_tracks.sort_by(|(left_pipeline, left), (right_pipeline, right)| {
        right
            .duration_ns
            .cmp(&left.duration_ns)
            .then_with(|| right.trace_count.cmp(&left.trace_count))
            .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
            .then_with(|| left.program_type.cmp(&right.program_type))
    });
    if !shader_tracks.is_empty() {
        out.push_str("Pipelines by Xcode shader track duration:\n");
        let denominator = report
            .cost_timeline
            .as_ref()
            .map(|timeline| timeline.global_gpu_time_ns)
            .filter(|value| *value > 0)
            .unwrap_or(report.gpu_time_ns);
        for (pipeline, track) in shader_tracks.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let pct = if denominator > 0 {
                100.0 * track.duration_ns as f64 / denominator as f64
            } else {
                0.0
            };
            out.push_str(&format!(
                "  {:>6.2}% {:>9.3} ms  {:>4} traces  ptype={:<2} {:<56} id={}:{}\n",
                pct,
                track.duration_ns as f64 / 1_000_000.0,
                track.trace_count,
                track.program_type,
                name,
                track.pipeline_id_kind,
                track.pipeline_id,
            ));
        }
        out.push('\n');
    }
    let mut shader_binaries = report
        .pipelines
        .iter()
        .flat_map(|pipeline| {
            pipeline
                .shader_binaries
                .iter()
                .map(move |binary| (pipeline, binary))
        })
        .filter(|(_, binary)| {
            binary.duration_ns > 0 || binary.total_cost > 0.0 || binary.trace_count > 0
        })
        .collect::<Vec<_>>();
    shader_binaries.sort_by(|(left_pipeline, left), (right_pipeline, right)| {
        right
            .duration_ns
            .cmp(&left.duration_ns)
            .then_with(|| {
                right
                    .total_cost
                    .partial_cmp(&left.total_cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
    });
    if !shader_binaries.is_empty() {
        out.push_str("Pipelines by Xcode shader binary duration:\n");
        let denominator = report
            .cost_timeline
            .as_ref()
            .map(|timeline| timeline.global_gpu_time_ns)
            .filter(|value| *value > 0)
            .unwrap_or(report.gpu_time_ns);
        for (pipeline, binary) in shader_binaries.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let pct = if denominator > 0 {
                100.0 * binary.duration_ns as f64 / denominator as f64
            } else {
                0.0
            };
            out.push_str(&format!(
                "  {:>6.2}% {:>9.3} ms  cost={:>10.3} traces={:>4} ptype={:<2} {:<56} binary={} addr=0x{:x}\n",
                pct,
                binary.duration_ns as f64 / 1_000_000.0,
                binary.total_cost,
                binary.trace_count,
                binary.program_type,
                name,
                binary.binary_index,
                binary.address,
            ));
        }
        out.push('\n');
    }
    let mut draw_timeline = report
        .pipelines
        .iter()
        .filter(|pipeline| pipeline.timeline_duration_ns > 0 || pipeline.timeline_draw_count > 0)
        .collect::<Vec<_>>();
    draw_timeline.sort_by(|left, right| {
        right
            .timeline_duration_ns
            .cmp(&left.timeline_duration_ns)
            .then_with(|| left.index.cmp(&right.index))
    });
    let draw_duration_denominator = report
        .cost_timeline
        .as_ref()
        .map(|timeline| timeline.global_gpu_time_ns)
        .filter(|value| *value > 0)
        .unwrap_or(report.gpu_time_ns);
    if !draw_timeline.is_empty()
        && draw_timeline
            .iter()
            .any(|pipeline| pipeline.timeline_duration_ns > 0)
    {
        out.push_str("Pipelines by Xcode draw duration:\n");
        for pipeline in draw_timeline.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let pct = if draw_duration_denominator > 0 {
                100.0 * pipeline.timeline_duration_ns as f64 / draw_duration_denominator as f64
            } else {
                0.0
            };
            out.push_str(&format!(
                "  {:>6.2}% {:>9.3} ms  {:>4} draws  {:<56}\n",
                pct,
                pipeline.timeline_duration_ns as f64 / 1_000_000.0,
                pipeline.timeline_draw_count,
                name,
            ));
        }
        out.push('\n');
    }
    draw_timeline.sort_by(|left, right| {
        right
            .timeline_total_cost
            .partial_cmp(&left.timeline_total_cost)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.index.cmp(&right.index))
    });
    let draw_cost_denominator = report
        .pipelines
        .iter()
        .map(|pipeline| pipeline.timeline_total_cost)
        .filter(|value| value.is_finite() && *value > 0.0)
        .sum::<f64>();
    if !draw_timeline.is_empty() && draw_cost_denominator > 0.0 {
        out.push_str("Pipelines by Xcode draw-scope cost:\n");
        for pipeline in draw_timeline.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let pct = 100.0 * pipeline.timeline_total_cost / draw_cost_denominator;
            out.push_str(&format!(
                "  {:>6.2}% cost={:>10.3} {:>4} draws  {:<56}\n",
                pct, pipeline.timeline_total_cost, pipeline.timeline_draw_count, name,
            ));
        }
        out.push('\n');
    }
    out.push_str("Pipelines by command count:\n");
    let mut pipelines = report.pipelines.clone();
    pipelines.sort_by(|left, right| {
        right
            .gpu_command_count
            .cmp(&left.gpu_command_count)
            .then_with(|| left.index.cmp(&right.index))
    });
    for pipeline in pipelines {
        let name = pipeline
            .function_name
            .as_deref()
            .unwrap_or("<unknown function>");
        let address = pipeline
            .pipeline_address
            .map(|value| format!("0x{value:x}"))
            .unwrap_or_else(|| "-".to_owned());
        out.push_str(&format!(
            "  {:>2} {:>4} commands  {:<56} addr={} object_id={}\n",
            pipeline.index, pipeline.gpu_command_count, name, address, pipeline.object_id
        ));
    }

    if !report.warnings.is_empty() {
        out.push_str("\nWarnings:\n");
        for warning in &report.warnings {
            out.push_str(&format!("  - {warning}\n"));
        }
    }

    out
}

fn percent_u64(value: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        100.0 * value as f64 / denominator as f64
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::collections::{BTreeMap, BTreeSet};
    use std::ffi::{CStr, CString, c_char, c_int, c_long, c_void};
    use std::fs;
    use std::mem;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::{
        XcodeMioBinaryTrace, XcodeMioCostTimeline, XcodeMioDecodedCostRecord,
        XcodeMioDrawMetadataRecord, XcodeMioDrawTimelineRecord, XcodeMioGpuCommand,
        XcodeMioPipeline, XcodeMioPipelineAgxpsTraceCost, XcodeMioPipelineCounter,
        XcodeMioPipelineExecutionHistory, XcodeMioPipelineProfilerTiming,
        XcodeMioPipelineScopeCost, XcodeMioPipelineShaderBinary, XcodeMioPipelineShaderBinaryCost,
        XcodeMioPipelineShaderBinaryReference, XcodeMioPipelineShaderProfilerCost,
        XcodeMioPipelineShaderStat, XcodeMioPipelineShaderTrack, XcodeMioReport,
        XcodeMioShaderBinaryInfo, XcodeMioTimelineBinary, XcodeMioTimelineCandidate,
        XcodeMioTimings, elapsed_ms,
    };
    use crate::error::{Error, Result};
    use crate::profiler;
    use block2::RcBlock;

    type Id = *mut c_void;
    type Class = *mut c_void;
    type Sel = *mut c_void;
    type CfTypeRef = *const c_void;
    type CfStringRef = *const c_void;

    const RTLD_NOW: c_int = 0x2;
    const RTLD_GLOBAL: c_int = 0x8;
    const K_CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
    const GT_SHADER_PROFILER_FRAMEWORK: &str = "/Applications/Xcode.app/Contents/PlugIns/GPUDebugger.ideplugin/Contents/Frameworks/GTShaderProfiler.framework/Versions/A/GTShaderProfiler";
    const MTL_TOOLS_SHADER_PROFILER_FRAMEWORK: &str = "/Applications/Xcode.app/Contents/SharedFrameworks/MTLToolsShaderProfiler.framework/Versions/A/MTLToolsShaderProfiler";

    unsafe extern "C" {
        fn dlopen(path: *const c_char, mode: c_int) -> *mut c_void;
        fn open(path: *const c_char, oflag: c_int, ...) -> c_int;
        fn dup(fd: c_int) -> c_int;
        fn dup2(src: c_int, dst: c_int) -> c_int;
        fn close(fd: c_int) -> c_int;
        fn objc_lookUpClass(name: *const c_char) -> Class;
        fn sel_registerName(name: *const c_char) -> Sel;
        fn objc_msgSend();
        fn CFStringCreateWithCString(
            alloc: CfTypeRef,
            c_str: *const c_char,
            encoding: u32,
        ) -> CfStringRef;
        fn CFRelease(cf: CfTypeRef);
    }

    #[link(name = "Foundation", kind = "framework")]
    unsafe extern "C" {}

    #[link(name = "objc")]
    unsafe extern "C" {}

    pub fn decode(
        trace_source: PathBuf,
        profiler_directory: PathBuf,
        stream_data_path: PathBuf,
        profiler_summary: Option<&profiler::ProfilerStreamDataSummary>,
        mut timings: XcodeMioTimings,
    ) -> Result<XcodeMioReport> {
        let framework_path = PathBuf::from(GT_SHADER_PROFILER_FRAMEWORK);
        let silence = FdSilencer::new();
        let framework_start = Instant::now();
        let mut runtime = unsafe { Runtime::load()? };
        timings.framework_load_ms = elapsed_ms(framework_start);
        let stream_start = Instant::now();
        let stream = runtime.stream_data(&stream_data_path)?;
        timings.stream_data_load_ms = elapsed_ms(stream_start);
        let processor_start = Instant::now();
        let processor = unsafe { runtime.processor(stream)? };
        timings.processor_init_ms = elapsed_ms(processor_start);
        let process_start = Instant::now();
        unsafe {
            runtime.send_void(processor, "processStreamData")?;
            runtime.send_void(processor, "waitUntilFinished")?;
            if responds_to_selector(processor, "processShaderProfilerStreamData") {
                runtime.send_void(processor, "processShaderProfilerStreamData")?;
            }
            if responds_to_selector(processor, "waitUntilShaderProfilerFinished") {
                runtime.send_void(processor, "waitUntilShaderProfilerFinished")?;
            }
        }
        timings.process_stream_ms = elapsed_ms(process_start);
        let extract_start = Instant::now();
        let mio = unsafe { runtime.send_id(processor, "mioData")? };
        let result = unsafe { runtime.send_id(processor, "result")? };
        let shader_result = unsafe { runtime.send_id(result, "shaderProfilerResult")? };
        let gpu_commands = unsafe { runtime.send_id(shader_result, "gpuCommands")? };
        let pipelines = unsafe { runtime.send_id(shader_result, "pipelineStates")? };
        let encoders = unsafe { runtime.send_id(shader_result, "encoders")? };

        let pipeline_count = unsafe { runtime.array_count(pipelines)? };
        let command_count = unsafe { runtime.array_count(gpu_commands)? };
        let encoder_count = unsafe { runtime.array_count(encoders)? };
        timings.extract_result_ms = elapsed_ms(extract_start);
        let topology_start = Instant::now();
        let mut decoded_pipelines = Vec::with_capacity(pipeline_count);
        for index in 0..pipeline_count {
            let pipeline = unsafe { runtime.array_object(pipelines, index)? };
            let summary_pipeline =
                profiler_summary.and_then(|summary| summary.pipelines.get(index));
            let binary_keys = unsafe {
                runtime
                    .send_id_allow_nil(pipeline, "binaryKeys")
                    .ok()
                    .filter(|array| !array.is_null())
                    .map(|array| runtime.u64_array(array))
                    .transpose()?
                    .unwrap_or_default()
            };
            let all_binary_keys = unsafe {
                runtime
                    .send_id_allow_nil(pipeline, "allBinaryKeys")
                    .ok()
                    .filter(|array| !array.is_null())
                    .map(|array| runtime.u64_array(array))
                    .transpose()?
                    .unwrap_or_default()
            };
            decoded_pipelines.push(XcodeMioPipeline {
                index: unsafe { runtime.send_u32(pipeline, "index")? as usize },
                object_id: unsafe { runtime.send_u64(pipeline, "objectId")? },
                pointer_id: unsafe { runtime.send_u64(pipeline, "pointerId")? },
                function_index: unsafe { runtime.send_u64(pipeline, "functionIndex")? },
                gpu_command_count: unsafe {
                    runtime.send_u32(pipeline, "numGPUCommands")? as usize
                },
                timeline_draw_count: 0,
                timeline_duration_ns: 0,
                timeline_total_cost: 0.0,
                pipeline_address: summary_pipeline.map(|pipeline| pipeline.pipeline_address),
                function_name: summary_pipeline.and_then(|pipeline| pipeline.function_name.clone()),
                binary_keys,
                all_binary_keys,
                shader_stats: Vec::new(),
                profiler_timings: unsafe { decode_pipeline_profiler_timings(pipeline) },
                scope_costs: Vec::new(),
                shader_tracks: Vec::new(),
                shader_binaries: Vec::new(),
                shader_binary_costs: Vec::new(),
                agxps_trace_costs: Vec::new(),
                shader_profiler_costs: Vec::new(),
                execution_history: Vec::new(),
                shader_binary_references: Vec::new(),
                pipeline_counters: Vec::new(),
            });
        }

        let mut decoded_commands = Vec::with_capacity(command_count);
        for index in 0..command_count {
            let command = unsafe { runtime.array_object(gpu_commands, index)? };
            let pipeline_index =
                unsafe { runtime.send_u32(command, "pipelineInfoIndex")? as usize };
            let function_name = profiler_summary
                .and_then(|summary| summary.pipelines.get(pipeline_index))
                .and_then(|pipeline| pipeline.function_name.clone());
            decoded_commands.push(XcodeMioGpuCommand {
                index: unsafe { runtime.send_u32(command, "index")? as usize },
                function_index: unsafe { runtime.send_u64(command, "functionIndex")? },
                sub_command_index: unsafe { runtime.send_i32(command, "subCommandIndex")? },
                encoder_index: unsafe { runtime.send_u32(command, "encoderInfoIndex")? as usize },
                pipeline_index,
                pipeline_object_id: unsafe { runtime.send_u64(command, "pipelineStateObjectId")? },
                command_buffer_index: unsafe {
                    runtime.send_u32(command, "commandBufferIndex")? as usize
                },
                function_name,
            });
        }
        timings.decode_pipeline_commands_ms = elapsed_ms(topology_start);

        let mut warnings = Vec::new();
        let shader_probe_start = Instant::now();
        if let Err(error) = unsafe {
            runtime.decode_shader_profiler_costs(
                shader_result,
                &decoded_commands,
                &mut decoded_pipelines,
            )
        } {
            warnings.push(format!(
                "private shader-profiler cost probe failed: {error}"
            ));
        }
        if let Err(error) = unsafe {
            runtime.decode_shader_profiler_timing_info(shader_result, &mut decoded_pipelines)
        } {
            warnings.push(format!(
                "private shader-profiler timingInfo probe failed: {error}"
            ));
        }
        match unsafe { runtime.direct_shader_profiler_result(stream) } {
            Ok(direct_shader_result) => {
                if let Err(error) = unsafe {
                    runtime.merge_shader_profiler_pipeline_state_data(
                        direct_shader_result,
                        &mut decoded_pipelines,
                    )
                } {
                    warnings.push(format!(
                        "direct shader-profiler pipeline probe failed: {error}"
                    ));
                }
                if let Err(error) = unsafe {
                    runtime.decode_shader_profiler_timing_info(
                        direct_shader_result,
                        &mut decoded_pipelines,
                    )
                } {
                    warnings.push(format!(
                        "direct shader-profiler timingInfo probe failed: {error}"
                    ));
                }
                if let Err(error) = unsafe {
                    runtime.decode_shader_profiler_costs(
                        direct_shader_result,
                        &decoded_commands,
                        &mut decoded_pipelines,
                    )
                } {
                    warnings.push(format!("direct shader-profiler cost probe failed: {error}"));
                }
            }
            Err(error) => {
                warnings.push(format!("direct shader-profiler processor failed: {error}"));
            }
        }
        timings.shader_profiler_probe_ms = elapsed_ms(shader_probe_start);
        let cost_request_start = Instant::now();
        let (cost_timeline, cost_timeline_object) =
            match unsafe { runtime.request_cost_timeline(mio) } {
                Ok(Some(timeline)) => {
                    let object = timeline.object;
                    (Some(timeline.summary), Some(object))
                }
                Ok(None) => (None, None),
                Err(error) => {
                    warnings.push(format!("private cost timeline callback failed: {error}"));
                    (None, None)
                }
            };
        let timeline_candidates = unsafe { runtime.timeline_candidates(mio, cost_timeline_object) };
        timings.cost_timeline_request_ms = elapsed_ms(cost_request_start);
        let mut decoded_cost_records = Vec::new();
        let mut draw_timeline_records = Vec::new();
        let mut draw_metadata_records = Vec::new();
        let mut timeline_binary_count = 0;
        let mut timeline_binaries = Vec::new();
        let mut timeline_pipeline_state_ids = Vec::new();
        let mut shader_binary_info = Vec::new();
        if let Some(timeline) = cost_timeline_object {
            let cost_decode_start = Instant::now();
            timeline_binary_count = unsafe { runtime.timeline_binary_count(timeline) };
            timeline_binaries = unsafe { runtime.decode_timeline_binaries(timeline) };
            timeline_pipeline_state_ids =
                unsafe { runtime.decode_timeline_pipeline_state_ids(timeline) };
            shader_binary_info = unsafe { runtime.decode_shader_binary_info(timeline) };
            attach_shader_binary_references(
                &shader_binary_info,
                &timeline_binaries,
                &decoded_commands,
                &mut decoded_pipelines,
            );
            if let Err(error) =
                decode_agxps_timing_trace_costs(&profiler_directory, &mut decoded_pipelines)
            {
                warnings.push(format!("AGXPS timing-trace probe failed: {error}"));
            }
            decoded_cost_records = unsafe { runtime.decode_timeline_cost_records(timeline) };
            draw_timeline_records = unsafe { runtime.decode_draw_timeline_records(timeline) };
            draw_metadata_records = unsafe { runtime.decode_draw_metadata_records(timeline) };
            unsafe {
                runtime.decode_pipeline_draw_timeline(
                    mio,
                    timeline,
                    &draw_timeline_records,
                    &draw_metadata_records,
                    &decoded_commands,
                    &mut decoded_pipelines,
                )
            };
            if let Err(error) = unsafe {
                runtime.decode_pipeline_private_costs(
                    mio,
                    timeline,
                    &decoded_cost_records,
                    &mut decoded_pipelines,
                    &timeline_pipeline_state_ids,
                )
            } {
                warnings.push(format!("private per-pipeline cost probe failed: {error}"));
            }
            if let Err(error) =
                unsafe { runtime.decode_pipeline_counters(mio, &mut decoded_pipelines) }
            {
                warnings.push(format!("private pipeline counter probe failed: {error}"));
            }
            timings.cost_timeline_decode_ms = elapsed_ms(cost_decode_start);
        }
        if profiler_summary.is_some_and(|summary| summary.num_gpu_commands != command_count) {
            warnings.push("private MIO command count differs from streamData summary".to_owned());
        }
        if decoded_pipelines
            .iter()
            .any(|pipeline| pipeline.function_name.is_none())
        {
            warnings.push("one or more private MIO pipelines could not be named".to_owned());
        }

        let final_metadata_start = Instant::now();
        let draw_count = unsafe { runtime.send_u64(mio, "drawCount")? as usize };
        let cost_record_count = unsafe { runtime.send_u64(mio, "costCount")? as usize };
        let gpu_time_ns = unsafe { runtime.send_u64(mio, "gpuTime")? };
        timings.final_metadata_ms = elapsed_ms(final_metadata_start);

        let report = XcodeMioReport {
            trace_source,
            profiler_directory,
            stream_data_path,
            framework_path,
            timings,
            gpu_command_count: command_count,
            encoder_count,
            pipeline_state_count: pipeline_count,
            draw_count,
            cost_record_count,
            gpu_time_ns,
            cost_timeline,
            timeline_candidates,
            timeline_binary_count,
            timeline_binaries,
            timeline_pipeline_state_ids,
            shader_binary_info,
            decoded_cost_records,
            draw_timeline_records,
            draw_metadata_records,
            pipelines: decoded_pipelines,
            gpu_commands: decoded_commands,
            warnings,
        };
        drop(runtime);
        std::thread::sleep(std::time::Duration::from_millis(100));
        drop(silence);
        Ok(report)
    }

    struct Runtime {
        pool: Id,
    }

    impl Runtime {
        unsafe fn load() -> Result<Self> {
            unsafe {
                let _ = load_framework(MTL_TOOLS_SHADER_PROFILER_FRAMEWORK);
                load_framework(GT_SHADER_PROFILER_FRAMEWORK)?;
                let pool_class = lookup_class("NSAutoreleasePool")?;
                let pool = send_id(send_id(pool_class, "alloc")?, "init")?;
                Ok(Self { pool })
            }
        }

        fn stream_data(&mut self, stream_data_path: &std::path::Path) -> Result<Id> {
            let path = stream_data_path.to_string_lossy();
            let path = CString::new(path.as_bytes())
                .map_err(|_| Error::InvalidInput("streamData path contains NUL".to_owned()))?;
            unsafe {
                let path = CfString::new(path.as_ptr())?;
                let url_class = lookup_class("NSURL")?;
                let url = send_id_id(url_class, "fileURLWithPath:", path.ptr.cast_mut())?;
                let stream_class = lookup_class("GTShaderProfilerStreamData")?;
                send_id_id(stream_class, "dataFromArchivedDataURL:", url)
            }
        }

        unsafe fn processor(&mut self, stream: Id) -> Result<Id> {
            unsafe {
                let processor_class = lookup_class("GTShaderProfilerStreamDataProcessor")?;
                let processor = send_id(processor_class, "alloc")?;
                send_id_id_id(
                    processor,
                    "initWithStreamData:llvmHelperPath:",
                    stream,
                    std::ptr::null_mut(),
                )
            }
        }

        unsafe fn direct_shader_profiler_result(&mut self, stream: Id) -> Result<Id> {
            unsafe {
                let processor_class = lookup_class("GTAGX2StreamDataShaderProfilerProcessor")?;
                let processor = send_id(processor_class, "alloc")?;
                let processor = send_id_id(processor, "initWithStreamData:", stream)?;
                send_void(processor, "processStreamData")?;
                if responds_to_selector(processor, "waitUntilStreamDataFinished") {
                    send_void(processor, "waitUntilStreamDataFinished")?;
                }
                if responds_to_selector(processor, "waitUntilBatchIDCounterFinished") {
                    let _ = send_void(processor, "waitUntilBatchIDCounterFinished");
                }
                send_id(processor, "shaderProfilerResult")
            }
        }

        unsafe fn send_id(&mut self, receiver: Id, selector: &str) -> Result<Id> {
            unsafe { send_id(receiver, selector) }
        }

        unsafe fn send_id_allow_nil(&mut self, receiver: Id, selector: &str) -> Result<Id> {
            unsafe { send_id_allow_nil(receiver, selector) }
        }

        unsafe fn send_void(&mut self, receiver: Id, selector: &str) -> Result<()> {
            unsafe { send_void(receiver, selector) }
        }

        unsafe fn send_u64(&mut self, receiver: Id, selector: &str) -> Result<u64> {
            unsafe { send_u64(receiver, selector) }
        }

        unsafe fn send_u32(&mut self, receiver: Id, selector: &str) -> Result<u32> {
            unsafe { send_u32(receiver, selector) }
        }

        unsafe fn send_i32(&mut self, receiver: Id, selector: &str) -> Result<i32> {
            unsafe { send_i32(receiver, selector) }
        }

        unsafe fn array_count(&mut self, array: Id) -> Result<usize> {
            unsafe { Ok(send_u64(array, "count")? as usize) }
        }

        unsafe fn array_object(&mut self, array: Id, index: usize) -> Result<Id> {
            unsafe { send_id_usize(array, "objectAtIndex:", index) }
        }

        unsafe fn u64_array(&mut self, array: Id) -> Result<Vec<u64>> {
            let count = unsafe { self.array_count(array)? };
            let mut values = Vec::with_capacity(count);
            for index in 0..count {
                let value = unsafe { self.array_object(array, index)? };
                values.push(unsafe { send_u64(value, "unsignedLongLongValue")? });
            }
            Ok(values)
        }

        unsafe fn request_cost_timeline(&mut self, mio: Id) -> Result<Option<DecodedTimeline>> {
            let slot = Arc::new(Mutex::new(None::<usize>));
            let callback_slot = Arc::clone(&slot);
            let block = RcBlock::new(move |timeline: Id| {
                if !timeline.is_null()
                    && let Ok(mut slot) = callback_slot.lock()
                {
                    *slot = Some(timeline as usize);
                }
            });
            let block_ptr = RcBlock::as_ptr(&block).cast::<c_void>();
            unsafe {
                send_id_id_allow_nil(mio, "requestCostTimeline:", block_ptr.cast())?;
            }

            let deadline = Instant::now() + Duration::from_secs(20);
            let mut timeline = None;
            while Instant::now() < deadline {
                timeline = *slot
                    .lock()
                    .map_err(|_| Error::InvalidInput("cost timeline lock poisoned".to_owned()))?;
                if timeline.is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            let Some(timeline) = timeline.map(|value| value as Id) else {
                return Ok(None);
            };

            let (gpu_cost, gpu_cost_instruction_count) = unsafe { decode_gpu_cost(timeline) };
            Ok(Some(DecodedTimeline {
                object: timeline,
                summary: XcodeMioCostTimeline {
                    draw_count: unsafe { send_u64(timeline, "drawCount")? as usize },
                    pipeline_state_count: unsafe {
                        send_u64(timeline, "pipelineStateCount")? as usize
                    },
                    cost_record_count: unsafe { send_u64(timeline, "costCount")? as usize },
                    gpu_time_ns: unsafe { send_u64(timeline, "gpuTime")? },
                    global_gpu_time_ns: unsafe { send_u64(timeline, "globalGPUTime")? },
                    timeline_duration_ns: unsafe { send_u64(timeline, "timelineDuration")? },
                    total_clique_cost: unsafe { send_u64(timeline, "totalCliqueCost")? },
                    gpu_cost,
                    gpu_cost_instruction_count,
                },
            }))
        }

        unsafe fn timeline_candidates(
            &mut self,
            mio: Id,
            requested_timeline: Option<Id>,
        ) -> Vec<XcodeMioTimelineCandidate> {
            let mut seen = BTreeSet::<usize>::new();
            let mut candidates = Vec::new();

            if let Some(timeline) = requested_timeline
                && seen.insert(timeline as usize)
                && let Some(candidate) =
                    unsafe { self.describe_timeline("requestCostTimeline", timeline) }
            {
                candidates.push(candidate);
            }

            for (source, selector) in [
                ("mio.costTimeline", "costTimeline"),
                ("mio.overlappingTimeline", "overlappingTimeline"),
                ("mio.nonOverlappingTimeline", "nonOverlappingTimeline"),
            ] {
                if !unsafe { responds_to_selector(mio, selector) } {
                    continue;
                }
                let Ok(timeline) = (unsafe { send_id_allow_nil(mio, selector) }) else {
                    continue;
                };
                if timeline.is_null() || !seen.insert(timeline as usize) {
                    continue;
                }
                if let Some(candidate) = unsafe { self.describe_timeline(source, timeline) } {
                    candidates.push(candidate);
                }
            }

            candidates
        }

        unsafe fn describe_timeline(
            &mut self,
            source: &'static str,
            timeline: Id,
        ) -> Option<XcodeMioTimelineCandidate> {
            if timeline.is_null() {
                return None;
            }
            let cost_records = unsafe { self.decode_timeline_cost_records(timeline) };
            let nonzero_cost_records = cost_records
                .iter()
                .filter(|record| {
                    record.total_cost.is_finite() && record.total_cost > 0.0
                        || record.instruction_count > 0
                })
                .count();
            let decoded_total_cost = cost_records
                .iter()
                .map(|record| record.total_cost)
                .filter(|value| value.is_finite())
                .sum::<f64>();
            let decoded_instruction_count = cost_records
                .iter()
                .map(|record| record.instruction_count)
                .sum::<u64>();
            let (gpu_cost, gpu_cost_instruction_count) = unsafe { decode_gpu_cost(timeline) };

            Some(XcodeMioTimelineCandidate {
                source,
                draw_count: unsafe { send_u64_if_supported(timeline, "drawCount") as usize },
                pipeline_state_count: unsafe {
                    send_u64_if_supported(timeline, "pipelineStateCount") as usize
                },
                cost_record_count: unsafe { send_u64_if_supported(timeline, "costCount") as usize },
                gpu_time_ns: unsafe { send_u64_if_supported(timeline, "gpuTime") },
                global_gpu_time_ns: unsafe { send_u64_if_supported(timeline, "globalGPUTime") },
                timeline_duration_ns: unsafe {
                    send_u64_if_supported(timeline, "timelineDuration")
                },
                total_clique_cost: unsafe { send_u64_if_supported(timeline, "totalCliqueCost") },
                gpu_cost,
                gpu_cost_instruction_count,
                timeline_binary_count: unsafe { self.timeline_binary_count(timeline) },
                shader_binary_info_count: unsafe {
                    send_u64_if_supported(timeline, "shaderBinaryInfoCount") as usize
                },
                nonzero_cost_records,
                decoded_total_cost,
                decoded_instruction_count,
            })
        }

        unsafe fn decode_pipeline_private_costs(
            &mut self,
            mio: Id,
            timeline: Id,
            cost_records: &[XcodeMioDecodedCostRecord],
            pipelines: &mut [XcodeMioPipeline],
            timeline_pipeline_state_ids: &[u64],
        ) -> Result<()> {
            unsafe {
                self.decode_pipeline_shader_stats(timeline, pipelines)?;
                self.decode_pipeline_scope_costs(cost_records, pipelines);
                self.decode_pipeline_shader_tracks(
                    timeline,
                    pipelines,
                    "timeline_pipeline_state_program_type",
                )?;
                if !pipelines
                    .iter()
                    .any(|pipeline| !pipeline.shader_tracks.is_empty())
                {
                    self.decode_pipeline_shader_tracks(
                        mio,
                        pipelines,
                        "mio_pipeline_state_program_type",
                    )?;
                }
                self.decode_pipeline_shader_binaries(timeline, pipelines)?;
                self.decode_pipeline_execution_history(
                    timeline,
                    pipelines,
                    timeline_pipeline_state_ids,
                )?;
                self.decode_pipeline_execution_history(
                    mio,
                    pipelines,
                    timeline_pipeline_state_ids,
                )?;
            }
            Ok(())
        }

        unsafe fn decode_shader_profiler_costs(
            &mut self,
            shader_result: Id,
            gpu_commands: &[XcodeMioGpuCommand],
            pipelines: &mut [XcodeMioPipeline],
        ) -> Result<()> {
            unsafe {
                let binaries = self.send_id_allow_nil(shader_result, "shaderBinaries")?;
                if binaries.is_null() {
                    return Ok(());
                }
                let keys = self.send_id_allow_nil(binaries, "allKeys")?;
                let values = self.send_id_allow_nil(binaries, "allValues")?;
                if keys.is_null() || values.is_null() {
                    return Ok(());
                }
                let count = self.array_count(keys)?.min(self.array_count(values)?);
                for index in 0..count {
                    let key_object = self.array_object(keys, index)?;
                    let binary = self.array_object(values, index)?;
                    if !responds_to_selector(binary, "costForDrawAtIndex:")
                        || !responds_to_selector(binary, "totalCost")
                    {
                        continue;
                    }
                    let key = if responds_to_selector(key_object, "UTF8String") {
                        nsstring_to_string(key_object)
                    } else {
                        None
                    }
                    .unwrap_or_else(|| format!("shader_binary_{index}"));
                    let full_path = if responds_to_selector(binary, "fullPath") {
                        self.send_id_allow_nil(binary, "fullPath")
                            .ok()
                            .filter(|value| !value.is_null())
                            .filter(|value| responds_to_selector(*value, "UTF8String"))
                            .and_then(|value| nsstring_to_string(value))
                    } else {
                        None
                    };
                    let type_name = if responds_to_selector(binary, "typeName") {
                        self.send_id_allow_nil(binary, "typeName")
                            .ok()
                            .filter(|value| !value.is_null())
                            .filter(|value| responds_to_selector(*value, "UTF8String"))
                            .and_then(|value| nsstring_to_string(value))
                    } else {
                        None
                    };
                    let shader_type = if responds_to_selector(binary, "type") {
                        send_u32(binary, "type").unwrap_or(0)
                    } else {
                        0
                    };
                    let addr_start = if responds_to_selector(binary, "addrStart") {
                        send_u32(binary, "addrStart").unwrap_or(0)
                    } else {
                        0
                    };
                    let addr_end = if responds_to_selector(binary, "addrEnd") {
                        send_u32(binary, "addrEnd").unwrap_or(0)
                    } else {
                        0
                    };
                    let total_binary_cost = send_f64(binary, "totalCost").unwrap_or(0.0);
                    let total_binary_samples = if responds_to_selector(binary, "numSamples") {
                        send_u64(binary, "numSamples").unwrap_or(0)
                    } else {
                        0
                    };
                    if total_binary_cost <= 0.0 && total_binary_samples == 0 {
                        continue;
                    }
                    let has_cost_percentage =
                        responds_to_selector(binary, "costPercentageForDrawAtIndex:");
                    let mut by_pipeline =
                        BTreeMap::<usize, XcodeMioPipelineShaderProfilerCost>::new();
                    for draw_index in 0..gpu_commands.len() {
                        let cost = send_f64_u32(binary, "costForDrawAtIndex:", draw_index as u32)
                            .unwrap_or(0.0);
                        if !cost.is_finite() || cost <= 0.0 {
                            continue;
                        }
                        let Some(command) = gpu_commands.get(draw_index) else {
                            continue;
                        };
                        let percent = if has_cost_percentage {
                            send_f64_u32(binary, "costPercentageForDrawAtIndex:", draw_index as u32)
                                .unwrap_or(0.0)
                        } else {
                            0.0
                        };
                        by_pipeline
                            .entry(command.pipeline_index)
                            .and_modify(|entry| {
                                entry.pipeline_cost += cost;
                                if percent.is_finite() && percent > 0.0 {
                                    entry.pipeline_cost_percent_sum += percent;
                                }
                                entry.nonzero_draw_count += 1;
                                entry.first_draw_index = entry.first_draw_index.min(draw_index);
                                entry.last_draw_index = entry.last_draw_index.max(draw_index);
                            })
                            .or_insert_with(|| XcodeMioPipelineShaderProfilerCost {
                                binary_key: key.clone(),
                                full_path: full_path.clone(),
                                type_name: type_name.clone(),
                                shader_type,
                                addr_start,
                                addr_end,
                                total_binary_cost,
                                total_binary_samples,
                                pipeline_cost: cost,
                                pipeline_cost_percent_sum: if percent.is_finite() && percent > 0.0 {
                                    percent
                                } else {
                                    0.0
                                },
                                nonzero_draw_count: 1,
                                first_draw_index: draw_index,
                                last_draw_index: draw_index,
                            });
                    }
                    for (pipeline_index, cost) in by_pipeline {
                        if let Some(pipeline) = pipelines.get_mut(pipeline_index) {
                            pipeline.shader_profiler_costs.push(cost);
                        }
                    }
                }
                for pipeline in pipelines {
                    pipeline.shader_profiler_costs.sort_by(|left, right| {
                        right
                            .pipeline_cost
                            .partial_cmp(&left.pipeline_cost)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| left.binary_key.cmp(&right.binary_key))
                    });
                }
            }
            Ok(())
        }

        unsafe fn merge_shader_profiler_pipeline_state_data(
            &mut self,
            shader_result: Id,
            pipelines: &mut [XcodeMioPipeline],
        ) -> Result<()> {
            unsafe {
                let pipeline_states = self.send_id_allow_nil(shader_result, "pipelineStates")?;
                if pipeline_states.is_null() {
                    return Ok(());
                }
                let count = self.array_count(pipeline_states)?;
                for index in 0..count {
                    let pipeline_state = self.array_object(pipeline_states, index)?;
                    let object_id = if responds_to_selector(pipeline_state, "objectId") {
                        send_u64(pipeline_state, "objectId").unwrap_or(0)
                    } else {
                        0
                    };
                    let pipeline_index = if responds_to_selector(pipeline_state, "index") {
                        send_u32(pipeline_state, "index").unwrap_or(index as u32) as usize
                    } else {
                        index
                    };
                    let timings = decode_pipeline_profiler_timings(pipeline_state);
                    if timings.is_empty() {
                        continue;
                    }
                    let target_index = pipelines
                        .iter()
                        .position(|pipeline| pipeline.object_id == object_id && object_id != 0)
                        .or_else(|| (pipeline_index < pipelines.len()).then_some(pipeline_index));
                    let Some(target_index) = target_index else {
                        continue;
                    };
                    let target = &mut pipelines[target_index];
                    target.profiler_timings.extend(timings);
                    dedup_profiler_timings(&mut target.profiler_timings);
                }
            }
            Ok(())
        }

        unsafe fn decode_shader_profiler_timing_info(
            &mut self,
            shader_result: Id,
            pipelines: &mut [XcodeMioPipeline],
        ) -> Result<()> {
            unsafe {
                let result_timing = decode_timing_info(shader_result).unwrap_or_default();
                let denominator = result_timing
                    .compute_time
                    .max(result_timing.time)
                    .max(result_timing.vertex_time)
                    .max(result_timing.fragment_time);
                let pipeline_states = self.send_id_allow_nil(shader_result, "pipelineStates")?;
                if pipeline_states.is_null() {
                    return Ok(());
                }
                let count = self.array_count(pipeline_states)?;
                for index in 0..count {
                    let pipeline_state = self.array_object(pipeline_states, index)?;
                    let Some(timing) = decode_timing_info(pipeline_state) else {
                        continue;
                    };
                    let pipeline_time = timing
                        .compute_time
                        .max(timing.time)
                        .max(timing.vertex_time)
                        .max(timing.fragment_time);
                    if pipeline_time == 0 {
                        continue;
                    }
                    let object_id = if responds_to_selector(pipeline_state, "objectId") {
                        send_u64(pipeline_state, "objectId").unwrap_or(0)
                    } else {
                        0
                    };
                    let pipeline_index = if responds_to_selector(pipeline_state, "index") {
                        send_u32(pipeline_state, "index").unwrap_or(index as u32) as usize
                    } else {
                        index
                    };
                    let target_index = pipelines
                        .iter()
                        .position(|pipeline| pipeline.object_id == object_id && object_id != 0)
                        .or_else(|| (pipeline_index < pipelines.len()).then_some(pipeline_index));
                    let Some(target_index) = target_index else {
                        continue;
                    };
                    let percentage = if denominator > 0 {
                        100.0 * pipeline_time as f64 / denominator as f64
                    } else {
                        0.0
                    };
                    let target = &mut pipelines[target_index];
                    target
                        .profiler_timings
                        .push(XcodeMioPipelineProfilerTiming {
                            source: "timingInfo",
                            cycle_average: 0.0,
                            cycle_min: 0.0,
                            cycle_max: 0.0,
                            time_average: pipeline_time as f64 / 1_000_000.0,
                            time_min: 0.0,
                            time_max: 0.0,
                            percentage_average: percentage,
                            percentage_min: 0.0,
                            percentage_max: 0.0,
                            surplus_cycles: 0.0,
                        });
                    dedup_profiler_timings(&mut target.profiler_timings);
                }
            }
            Ok(())
        }

        unsafe fn decode_pipeline_counters(
            &mut self,
            mio: Id,
            pipelines: &mut [XcodeMioPipeline],
        ) -> Result<()> {
            unsafe {
                let counters = self.send_id_allow_nil(mio, "nonOverlappingCounters")?;
                if counters.is_null() {
                    return Ok(());
                }
                let names = self.send_id_allow_nil(counters, "pipelineStateCounterNames")?;
                if names.is_null() {
                    return Ok(());
                }
                let name_count = self.array_count(names)?;
                let value_count = send_u64(counters, "numPipelineStateCounters")? as usize;
                let count = name_count.min(value_count);
                let mut counter_names = Vec::with_capacity(count);
                for index in 0..count {
                    let name = self.array_object(names, index)?;
                    counter_names.push(
                        nsstring_to_string(name)
                            .unwrap_or_else(|| format!("pipeline_counter_{index}")),
                    );
                }
                for pipeline in pipelines {
                    let values = send_ptr_u64_u32(
                        counters,
                        "counterValuesForPipelineStateId:encoderFunctionIndex:",
                        pipeline.object_id,
                        pipeline.function_index as u32,
                    )?;
                    if values.is_null() {
                        continue;
                    }
                    let values = values.cast::<f64>();
                    for (index, name) in counter_names.iter().enumerate() {
                        let value = *values.add(index);
                        if !value.is_finite() || value == 0.0 {
                            continue;
                        }
                        pipeline.pipeline_counters.push(XcodeMioPipelineCounter {
                            name: name.clone(),
                            value,
                        });
                    }
                    pipeline.pipeline_counters.sort_by(|left, right| {
                        right
                            .value
                            .abs()
                            .partial_cmp(&left.value.abs())
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                }
            }
            Ok(())
        }

        unsafe fn decode_timeline_cost_records(
            &mut self,
            timeline: Id,
        ) -> Vec<XcodeMioDecodedCostRecord> {
            let Ok(cost_count) = (unsafe { send_u64(timeline, "costCount") }) else {
                return Vec::new();
            };
            let Ok(costs) = (unsafe { send_ptr(timeline, "costs") }) else {
                return Vec::new();
            };
            if costs.is_null() {
                return Vec::new();
            }
            let costs = costs.cast::<RawGtmioCostInfo>();
            let mut records = Vec::with_capacity(cost_count as usize);
            for index in 0..cost_count as usize {
                let raw = unsafe { *costs.add(index) };
                let mut populated = RawGtmioCostInfo::default();
                let cost = unsafe {
                    send_i8_u16_u64_cost_mut(
                        timeline,
                        "costForScope:scopeIdentifier:cost:",
                        raw.context.scope,
                        raw.context.scope_identifier,
                        &mut populated,
                    )
                }
                .ok()
                .filter(|found| *found != 0)
                .filter(|_| !populated.is_empty())
                .map(|_| populated)
                .unwrap_or_else(|| {
                    let mut populated = RawGtmioCostInfo::default();
                    let by_context = unsafe {
                        send_i8_context_cost_mut(
                            timeline,
                            "costForContext:cost:",
                            &raw.context,
                            &mut populated,
                        )
                    };
                    by_context
                        .ok()
                        .filter(|found| *found != 0)
                        .filter(|_| !populated.is_empty())
                        .map(|_| populated)
                        .unwrap_or_else(|| {
                            let mut populated = RawGtmioCostInfo::default();
                            let by_level = unsafe {
                                send_i8_u16_u32_u16_u64_cost_mut(
                                    timeline,
                                    "costForLevel:levelIdentifier:scope:scopeIdentifier:cost:",
                                    raw.context.level,
                                    raw.context.level_identifier,
                                    raw.context.scope,
                                    raw.context.scope_identifier,
                                    &mut populated,
                                )
                            };
                            by_level
                                .ok()
                                .filter(|found| *found != 0)
                                .filter(|_| !populated.is_empty())
                                .map(|_| populated)
                                .unwrap_or(raw)
                        })
                });
                records.push(XcodeMioDecodedCostRecord {
                    index,
                    level: cost.context.level,
                    scope: cost.context.scope,
                    level_identifier: cost.context.level_identifier,
                    scope_identifier: cost.context.scope_identifier,
                    alu_cost: cost.alu_cost,
                    non_alu_cost: cost.non_alu_cost,
                    total_cost: cost.alu_cost + cost.non_alu_cost,
                    instruction_count: cost.instruction_count,
                    threads_executing_instruction: cost.threads_executing_instruction,
                    cpi_weighted_instruction_count: cost.cpi_weighted_instruction_count,
                    active_thread_instruction_count: cost.active_thread_instruction_count,
                });
            }
            records
        }

        unsafe fn timeline_binary_count(&mut self, timeline: Id) -> usize {
            unsafe {
                let Ok(binaries) = send_id(timeline, "binaries") else {
                    return 0;
                };
                send_u64(binaries, "count").unwrap_or(0) as usize
            }
        }

        unsafe fn decode_timeline_binaries(&mut self, timeline: Id) -> Vec<XcodeMioTimelineBinary> {
            unsafe {
                let Ok(binaries) = send_id(timeline, "binaries") else {
                    return Vec::new();
                };
                let binary_count = send_u64(binaries, "count").unwrap_or(0);
                let mut decoded = Vec::with_capacity(binary_count as usize);
                for index in 0..binary_count as usize {
                    let Ok(binary) = send_id_usize(binaries, "objectAtIndex:", index) else {
                        continue;
                    };
                    decoded.push(decode_timeline_binary(binary, index as u64));
                }
                decoded
            }
        }

        unsafe fn decode_shader_binary_info(
            &mut self,
            timeline: Id,
        ) -> Vec<XcodeMioShaderBinaryInfo> {
            let Ok(info_count) = (unsafe { send_u64(timeline, "shaderBinaryInfoCount") }) else {
                return Vec::new();
            };
            let Ok(info) = (unsafe { send_ptr(timeline, "shaderBinaryInfo") }) else {
                return Vec::new();
            };
            if info.is_null() {
                return Vec::new();
            }
            let info = info.cast::<u8>();
            let mut records = Vec::with_capacity(info_count as usize);
            for index in 0..info_count as usize {
                let raw = unsafe { std::slice::from_raw_parts(info.add(index * 30), 30) };
                records.push(XcodeMioShaderBinaryInfo {
                    index,
                    raw0: read_u32(raw, 0),
                    raw1: read_u32(raw, 4),
                    raw2: read_u64(raw, 8),
                    raw3: read_u64(raw, 16),
                    raw4: read_u16(raw, 24),
                    raw5: read_u16(raw, 26),
                    raw6: read_u16(raw, 28),
                });
            }
            records
        }

        unsafe fn decode_timeline_pipeline_state_ids(&mut self, timeline: Id) -> Vec<u64> {
            let values = Arc::new(Mutex::new(Vec::new()));
            let callback_values = Arc::clone(&values);
            let block = RcBlock::new(move |pipeline_state_id: u64| {
                if let Ok(mut values) = callback_values.lock() {
                    values.push(pipeline_state_id);
                }
            });
            let block_ptr = RcBlock::as_ptr(&block).cast::<c_void>();
            if unsafe { send_void_id(timeline, "enumeratePipelineStates:", block_ptr.cast()) }
                .is_err()
            {
                return Vec::new();
            }
            let mut values = values
                .lock()
                .map(|values| values.clone())
                .unwrap_or_default();
            values.sort_unstable();
            values.dedup();
            values
        }

        unsafe fn decode_draw_timeline_records(
            &mut self,
            timeline: Id,
        ) -> Vec<XcodeMioDrawTimelineRecord> {
            let Ok(draw_count) = (unsafe { send_u64(timeline, "drawCount") }) else {
                return Vec::new();
            };
            let Ok(draws) = (unsafe { send_ptr(timeline, "drawTraces") }) else {
                return Vec::new();
            };
            if draws.is_null() {
                return Vec::new();
            }
            let draws = draws.cast::<RawGtmioDrawTrace>();
            let mut records = Vec::with_capacity(draw_count as usize);
            for index in 0..draw_count as usize {
                let draw = unsafe { *draws.add(index) };
                records.push(XcodeMioDrawTimelineRecord {
                    index,
                    raw0: draw.raw0,
                    raw1: draw.raw1,
                    raw2: draw.raw2,
                    raw3: draw.raw3,
                });
            }
            records
        }

        unsafe fn decode_draw_metadata_records(
            &mut self,
            timeline: Id,
        ) -> Vec<XcodeMioDrawMetadataRecord> {
            let Ok(draw_count) = (unsafe { send_u64(timeline, "drawCount") }) else {
                return Vec::new();
            };
            let Ok(draws) = (unsafe { send_ptr(timeline, "draws") }) else {
                return Vec::new();
            };
            if draws.is_null() {
                return Vec::new();
            }
            let draws = draws.cast::<u8>();
            let mut records = Vec::with_capacity(draw_count as usize);
            for index in 0..draw_count as usize {
                let draw = unsafe { std::slice::from_raw_parts(draws.add(index * 44), 44) };
                records.push(XcodeMioDrawMetadataRecord {
                    index,
                    raw0: read_u32(draw, 0),
                    raw1: read_u32(draw, 4),
                    raw2: read_u32(draw, 8),
                    raw3: read_u32(draw, 12),
                    raw4: read_i32(draw, 16),
                    raw5: read_u32(draw, 20),
                    raw6: read_u64(draw, 24),
                    raw7: read_u32(draw, 32),
                    raw8: read_u32(draw, 36),
                    raw9: read_u32(draw, 40),
                });
            }
            records
        }

        unsafe fn decode_pipeline_draw_timeline(
            &mut self,
            trace_data: Id,
            timeline: Id,
            draw_records: &[XcodeMioDrawTimelineRecord],
            draw_metadata: &[XcodeMioDrawMetadataRecord],
            gpu_commands: &[XcodeMioGpuCommand],
            pipelines: &mut [XcodeMioPipeline],
        ) {
            for draw in draw_metadata {
                let command_index = draw.raw0 as usize;
                let Some(command) = gpu_commands.get(command_index) else {
                    continue;
                };
                let Some(pipeline) = pipelines.get_mut(command.pipeline_index) else {
                    continue;
                };
                pipeline.timeline_draw_count += 1;
                let mut duration_ns = 0;
                if let Some(trace) = draw_records.get(draw.index) {
                    duration_ns = trace.raw1.saturating_sub(trace.raw0);
                }
                if duration_ns == 0 {
                    duration_ns =
                        unsafe { draw_duration(timeline, draw.index as u32, draw.raw7 as u16) };
                }
                if duration_ns == 0 {
                    duration_ns =
                        unsafe { draw_duration(trace_data, draw.index as u32, draw.raw7 as u16) };
                }
                pipeline.timeline_duration_ns += duration_ns;
                pipeline.timeline_total_cost +=
                    unsafe { draw_scope_cost(timeline, draw.raw0 as u64, draw.raw7) };
            }
        }

        unsafe fn decode_pipeline_shader_stats(
            &mut self,
            timeline: Id,
            pipelines: &mut [XcodeMioPipeline],
        ) -> Result<()> {
            unsafe {
                let stats_class = lookup_class("GTMioTraceDataStats")?;
                let stats = send_id(stats_class, "alloc")?;
                let stats = send_id_id(stats, "initWithTraceData:", timeline)?;
                send_void(stats, "build")?;

                for pipeline in pipelines {
                    let candidates = [
                        ("object_id", pipeline.object_id),
                        ("pointer_id", pipeline.pointer_id),
                        ("function_index", pipeline.function_index),
                    ];
                    for (shader_id_kind, shader_id) in candidates {
                        for program_type in program_type_candidates(pipeline) {
                            let stat = send_id_u64_u16_allow_nil(
                                stats,
                                "shaderStatForShader:programType:",
                                shader_id,
                                program_type,
                            )?;
                            if stat.is_null() {
                                continue;
                            }
                            let number_of_cliques = send_u64(stat, "numberOfCliques")?;
                            let total_gpu_cycles = send_u64(stat, "totalGPUCycles")?;
                            let total_latency = send_u64(stat, "totalLatency")?;
                            if number_of_cliques == 0 && total_gpu_cycles == 0 && total_latency == 0
                            {
                                continue;
                            }
                            pipeline.shader_stats.push(XcodeMioPipelineShaderStat {
                                shader_id_kind,
                                shader_id,
                                program_type,
                                number_of_cliques,
                                total_gpu_cycles,
                                total_latency,
                            });
                        }
                    }
                }
            }
            Ok(())
        }

        fn decode_pipeline_scope_costs(
            &mut self,
            cost_records: &[XcodeMioDecodedCostRecord],
            pipelines: &mut [XcodeMioPipeline],
        ) {
            for pipeline in pipelines {
                let identifiers = [
                    ("object_id", pipeline.object_id),
                    ("pointer_id", pipeline.pointer_id),
                    ("function_index", pipeline.function_index),
                    ("pipeline_index", pipeline.index as u64),
                ];
                for (identifier_kind, identifier) in identifiers {
                    for cost in cost_records {
                        if cost.scope_identifier != identifier {
                            continue;
                        }
                        if !cost.total_cost.is_finite() || cost.total_cost <= 0.0 {
                            continue;
                        }
                        pipeline.scope_costs.push(XcodeMioPipelineScopeCost {
                            scope: cost.scope,
                            identifier_kind,
                            identifier,
                            level: cost.level,
                            level_identifier: cost.level_identifier,
                            alu_cost: cost.alu_cost,
                            non_alu_cost: cost.non_alu_cost,
                            total_cost: cost.total_cost,
                            instruction_count: cost.instruction_count,
                        });
                    }
                }
            }
        }

        unsafe fn decode_pipeline_shader_tracks(
            &mut self,
            trace_data: Id,
            pipelines: &mut [XcodeMioPipeline],
            source: &'static str,
        ) -> Result<()> {
            unsafe {
                let helper_class = lookup_class("GTMioTraceDataHelper")?;
                let helper = send_id(helper_class, "alloc")?;
                let helper = send_id_id(helper, "initWithTraceData:", trace_data)?;
                let _ = send_void_i8(helper, "setShowDriverInternalShaders:", 1);
                let _ = send_void_i8(helper, "setShowDriverIntersectionShaders:", 1);
                let _ = send_void_i8(helper, "setShowESLShaders:", 1);

                for pipeline in pipelines {
                    let candidates = [
                        ("object_id", pipeline.object_id),
                        ("pointer_id", pipeline.pointer_id),
                        ("function_index", pipeline.function_index),
                        ("pipeline_index", pipeline.index as u64),
                    ];
                    for (pipeline_id_kind, pipeline_id) in candidates {
                        for program_type in program_type_candidates(pipeline) {
                            let track = send_id_u64_u16_allow_nil(
                                helper,
                                "generateAggregatedShaderTrackForPipelineState:programType:",
                                pipeline_id,
                                program_type,
                            )?;
                            if track.is_null() {
                                continue;
                            }
                            if let Some(decoded) = decode_shader_track(
                                track,
                                source,
                                pipeline_id_kind,
                                pipeline_id,
                                program_type,
                            )? {
                                pipeline.shader_tracks.push(decoded);
                            }
                        }
                        let tracks = send_id_u64_allow_nil(
                            helper,
                            "generateShaderTracksForPipelineState:",
                            pipeline_id,
                        )?;
                        if tracks.is_null() {
                            continue;
                        }
                        let track_count = send_u64(tracks, "count").unwrap_or(0);
                        for index in 0..track_count as usize {
                            let track = send_id_usize(tracks, "objectAtIndex:", index)?;
                            if let Some(decoded) = decode_shader_track(
                                track,
                                "shader_tracks_for_pipeline_state",
                                pipeline_id_kind,
                                pipeline_id,
                                u16::MAX,
                            )? {
                                pipeline.shader_tracks.push(decoded);
                            }
                        }
                    }
                }
            }
            Ok(())
        }

        unsafe fn decode_pipeline_shader_binaries(
            &mut self,
            timeline: Id,
            pipelines: &mut [XcodeMioPipeline],
        ) -> Result<()> {
            unsafe {
                let mut seen = BTreeSet::new();
                let binaries = send_id(timeline, "binaries")?;
                let binary_count = send_u64(binaries, "count")?;
                for binary_index in 0..binary_count as usize {
                    let binary = send_id_usize(binaries, "objectAtIndex:", binary_index)?;
                    let address = send_u64(binary, "address").unwrap_or(0);
                    for pipeline in pipelines.iter_mut() {
                        let candidates = [
                            ("object_id", pipeline.object_id),
                            ("pointer_id", pipeline.pointer_id),
                            ("function_index", pipeline.function_index),
                            ("pipeline_index", pipeline.index as u64),
                        ];
                        for (pipeline_id_kind, pipeline_id) in candidates {
                            let used = send_i8_u64(binary, "usedInPipelineState:", pipeline_id)
                                .unwrap_or(0)
                                != 0;
                            let joined_by_key =
                                pipeline.binary_keys.contains(&(binary_index as u64))
                                    || pipeline.all_binary_keys.contains(&(binary_index as u64));
                            let joined_by_address = address != 0
                                && (pipeline.binary_keys.contains(&address)
                                    || pipeline.all_binary_keys.contains(&address));
                            if !used && !joined_by_key && !joined_by_address {
                                continue;
                            }
                            let pipeline_id_kind = if used {
                                pipeline_id_kind
                            } else if joined_by_key {
                                "binary_key"
                            } else {
                                "binary_key_address"
                            };
                            let pipeline_id = if used {
                                pipeline_id
                            } else if joined_by_key {
                                binary_index as u64
                            } else {
                                address
                            };
                            push_decoded_binary(
                                pipeline,
                                &mut seen,
                                binary,
                                pipeline_id_kind,
                                pipeline_id,
                                binary_index as u64,
                            );
                            break;
                        }
                    }
                }
            }
            Ok(())
        }

        unsafe fn decode_pipeline_execution_history(
            &mut self,
            timeline: Id,
            pipelines: &mut [XcodeMioPipeline],
            timeline_pipeline_state_ids: &[u64],
        ) -> Result<()> {
            unsafe {
                if !responds_to_selector(
                    timeline,
                    "executionHistoryForPipelineState:programType:delegate:progressController:",
                ) {
                    return Ok(());
                }
                let history_class = lookup_class("GTMioShaderExecutionHistory")?;
                let delegate = lookup_class("GTMioShaderExecutionHistoryDefaultDelegate")
                    .ok()
                    .and_then(|class| send_id_allow_nil(class, "shared").ok())
                    .unwrap_or(std::ptr::null_mut());

                for style in [1_u32, 2, 4] {
                    for options in 0_u32..=15 {
                        let history = send_id(history_class, "alloc")?;
                        let history = send_id_id_u32_u32_id(
                            history,
                            "initWithTraceData:style:options:delegate:",
                            timeline,
                            style,
                            options,
                            delegate,
                        )?;
                        for pipeline in pipelines.iter_mut() {
                            let mut seen = BTreeSet::new();
                            let identifiers = execution_history_pipeline_ids(
                                pipeline,
                                timeline_pipeline_state_ids,
                            );
                            for (pipeline_id_kind, pipeline_id) in identifiers {
                                for program_type in program_type_candidates(pipeline) {
                                    if !seen.insert((pipeline_id_kind, pipeline_id, program_type)) {
                                        continue;
                                    }
                                    let generated = send_void_u64_u16_id_id(
                                        timeline,
                                        "executionHistoryForPipelineState:programType:delegate:progressController:",
                                        pipeline_id,
                                        program_type,
                                        history,
                                        std::ptr::null_mut(),
                                    )
                                    .is_ok();
                                    if !generated {
                                        continue;
                                    }
                                    let node =
                                        send_id_u32_allow_nil(history, "nodeForStyle:", style)
                                            .unwrap_or(std::ptr::null_mut());
                                    if node.is_null() {
                                        continue;
                                    }
                                    let decoded_node = decode_execution_history_node(node);
                                    let top_cost_percentage = decoded_node.top_cost_percentage;
                                    let duration_percentage = decoded_node.duration_percentage;
                                    let total_duration_ns = decoded_node.total_duration_ns;
                                    let total_cost = decoded_node.total_cost;
                                    if top_cost_percentage == 0.0
                                        && duration_percentage == 0.0
                                        && total_cost == 0.0
                                        && total_duration_ns == 0
                                    {
                                        continue;
                                    }
                                    pipeline.execution_history.push(
                                        XcodeMioPipelineExecutionHistory {
                                            style,
                                            options,
                                            program_type,
                                            pipeline_id_kind,
                                            pipeline_id,
                                            top_cost_percentage,
                                            duration_percentage,
                                            total_duration_ns,
                                            total_cost,
                                            instruction_count: decoded_node.instruction_count,
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    }

    struct DecodedTimeline {
        object: Id,
        summary: XcodeMioCostTimeline,
    }

    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    struct RawGtmioCostContext {
        level: u16,
        scope: u16,
        level_identifier: u32,
        scope_identifier: u64,
    }

    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    struct RawGtmioCostInfo {
        context: RawGtmioCostContext,
        alu_cost: f64,
        alu_cost_per_dm: [f64; 10],
        non_alu_cost: f64,
        non_alu_cost_per_dm: [f64; 10],
        instruction_count: u64,
        instruction_count_per_dm: [u64; 10],
        threads_executing_instruction: u64,
        cpi_weighted_instruction_count: u64,
        active_thread_instruction_count: u64,
    }

    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    struct RawGtStatistics {
        average: f64,
        minimum: f64,
        maximum: f64,
    }

    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    struct RawGtShaderProfilerTiming {
        cycles: RawGtStatistics,
        time: RawGtStatistics,
        percentage: RawGtStatistics,
        surplus_cycles: f64,
    }

    #[derive(Clone, Copy)]
    #[repr(C)]
    struct RawGtmioBinaryTrace {
        start_timestamp_ns: u64,
        end_timestamp_ns: u64,
        raw_identifier: u64,
        raw_index: u32,
        raw_count: u32,
        raw_program_type: u16,
    }

    #[derive(Clone, Copy)]
    #[repr(C)]
    struct RawGtmioDrawTrace {
        raw0: u64,
        raw1: u64,
        raw2: u32,
        raw3: u16,
    }

    impl RawGtmioCostInfo {
        fn is_empty(&self) -> bool {
            self.context.level == 0
                && self.context.scope == 0
                && self.context.level_identifier == 0
                && self.context.scope_identifier == 0
                && self.alu_cost == 0.0
                && self.non_alu_cost == 0.0
                && self.instruction_count == 0
                && self.threads_executing_instruction == 0
                && self.cpi_weighted_instruction_count == 0
                && self.active_thread_instruction_count == 0
        }
    }

    impl RawGtShaderProfilerTiming {
        fn has_useful_values(&self) -> bool {
            [
                self.cycles.average,
                self.cycles.minimum,
                self.cycles.maximum,
                self.time.average,
                self.time.minimum,
                self.time.maximum,
                self.percentage.average,
                self.percentage.minimum,
                self.percentage.maximum,
                self.surplus_cycles,
            ]
            .into_iter()
            .any(|value| value.is_finite() && value != 0.0)
        }
    }

    #[derive(Clone, Copy, Default)]
    struct DecodedCostAggregate {
        record_count: u64,
        nonzero_record_count: u64,
        total_cost: f64,
        total_instruction_count: u64,
        alu_cost: f64,
        non_alu_cost: f64,
    }

    #[derive(Clone, Copy, Default)]
    struct DecodedExecutionHistoryNode {
        top_cost_percentage: f64,
        duration_percentage: f64,
        total_duration_ns: u64,
        total_cost: f64,
        instruction_count: u64,
    }

    #[derive(Clone, Copy, Default)]
    struct DecodedTimingInfo {
        time: u64,
        vertex_time: u64,
        fragment_time: u64,
        compute_time: u64,
    }

    unsafe fn decode_shader_track(
        track: Id,
        source: &'static str,
        pipeline_id_kind: &'static str,
        pipeline_id: u64,
        program_type: u16,
    ) -> Result<Option<XcodeMioPipelineShaderTrack>> {
        unsafe {
            let duration_ns = send_u64(track, "duration")?;
            let trace_count = send_u64(track, "traceCount")?;
            if duration_ns == 0 && trace_count == 0 {
                return Ok(None);
            }
            let traces = decode_binary_traces(track, trace_count)?;
            Ok(Some(XcodeMioPipelineShaderTrack {
                source,
                pipeline_id_kind,
                pipeline_id,
                program_type,
                track_id: send_i32(track, "trackId")?,
                first_index: send_u64(track, "firstIndex").unwrap_or(0),
                start_timestamp_ns: send_u64(track, "startTimestamp")?,
                end_timestamp_ns: send_u64(track, "endTimestamp")?,
                duration_ns,
                trace_count,
                traces,
            }))
        }
    }

    unsafe fn decode_timing_info(receiver: Id) -> Option<DecodedTimingInfo> {
        unsafe {
            if !responds_to_selector(receiver, "timingInfo") {
                return None;
            }
            let timing_info = send_id_allow_nil(receiver, "timingInfo").ok()?;
            if timing_info.is_null() {
                return None;
            }
            let decoded = DecodedTimingInfo {
                time: send_u64(timing_info, "time").unwrap_or(0),
                vertex_time: send_u64(timing_info, "vertexTime").unwrap_or(0),
                fragment_time: send_u64(timing_info, "fragmentTime").unwrap_or(0),
                compute_time: send_u64(timing_info, "computeTime").unwrap_or(0),
            };
            (decoded.time != 0
                || decoded.vertex_time != 0
                || decoded.fragment_time != 0
                || decoded.compute_time != 0)
                .then_some(decoded)
        }
    }

    unsafe fn decode_pipeline_profiler_timings(
        pipeline: Id,
    ) -> Vec<XcodeMioPipelineProfilerTiming> {
        let mut timings = Vec::new();
        for (selector, source) in [
            ("timing", "total"),
            ("vertexTiming", "vertex"),
            ("fragmentTiming", "fragment"),
            ("computeTiming", "compute"),
        ] {
            if !unsafe { responds_to_selector(pipeline, selector) } {
                continue;
            }
            let Ok(raw) = (unsafe { send_profiler_timing(pipeline, selector) }) else {
                continue;
            };
            if !raw.has_useful_values() {
                continue;
            }
            timings.push(XcodeMioPipelineProfilerTiming {
                source,
                cycle_average: raw.cycles.average,
                cycle_min: raw.cycles.minimum,
                cycle_max: raw.cycles.maximum,
                time_average: raw.time.average,
                time_min: raw.time.minimum,
                time_max: raw.time.maximum,
                percentage_average: raw.percentage.average,
                percentage_min: raw.percentage.minimum,
                percentage_max: raw.percentage.maximum,
                surplus_cycles: raw.surplus_cycles,
            });
        }
        timings
    }

    fn dedup_profiler_timings(timings: &mut Vec<XcodeMioPipelineProfilerTiming>) {
        let mut seen = BTreeSet::new();
        timings.retain(|timing| {
            seen.insert((
                timing.source,
                timing.cycle_average.to_bits(),
                timing.time_average.to_bits(),
                timing.percentage_average.to_bits(),
                timing.surplus_cycles.to_bits(),
            ))
        });
    }

    unsafe fn decode_binary_traces(
        track: Id,
        trace_count: u64,
    ) -> Result<Vec<XcodeMioBinaryTrace>> {
        unsafe {
            let traces = send_ptr(track, "traces")?;
            if traces.is_null() || trace_count == 0 {
                return Ok(Vec::new());
            }
            let traces = traces.cast::<RawGtmioBinaryTrace>();
            let mut decoded = Vec::with_capacity(trace_count as usize);
            for index in 0..trace_count as usize {
                let raw = *traces.add(index);
                decoded.push(XcodeMioBinaryTrace {
                    index,
                    start_timestamp_ns: raw.start_timestamp_ns,
                    end_timestamp_ns: raw.end_timestamp_ns,
                    raw_identifier: raw.raw_identifier,
                    raw_index: raw.raw_index,
                    raw_count: raw.raw_count,
                    raw_program_type: raw.raw_program_type,
                });
            }
            Ok(decoded)
        }
    }

    fn decode_agxps_timing_trace_costs(
        profiler_directory: &Path,
        pipelines: &mut [XcodeMioPipeline],
    ) -> Result<()> {
        let address_to_pipeline = pipelines
            .iter()
            .enumerate()
            .flat_map(|(pipeline_index, pipeline)| {
                pipeline
                    .shader_binary_references
                    .iter()
                    .filter(|reference| reference.raw5 == 6 && reference.raw6 == 28)
                    .map(move |reference| (reference.address, pipeline_index))
            })
            .collect::<BTreeMap<_, _>>();
        if address_to_pipeline.is_empty() {
            return Ok(());
        }

        let paths = profiling_raw_paths(profiler_directory)?;
        if paths.is_empty() {
            return Ok(());
        }

        let loaded = agxps_sys::load()
            .map_err(|error| Error::InvalidInput(format!("agxps load failed: {error}")))?;
        let generation = u32_env("AGXPS_GEN", 16);
        let variant = u32_env("AGXPS_VARIANT", 3);
        let rev = u32_env("AGXPS_REV", 1);
        let mut groups = BTreeMap::<(usize, u64), XcodeMioPipelineAgxpsTraceCost>::new();

        for path in paths {
            let bytes = fs::read(&path)?;
            let raw = unsafe { parse_agxps_profile(&loaded, generation, variant, rev, &bytes) }?;
            let records = unsafe { agxps_timing_records(&loaded, raw.profile_data) }?;
            let relevant_records = records
                .into_iter()
                .filter_map(|record| {
                    let pipeline_index = address_to_pipeline
                        .get(&record.esl_shader_address)
                        .copied()?;
                    Some(AgxpsPipelineTimingRecord {
                        pipeline_index,
                        record,
                    })
                })
                .collect::<Vec<_>>();
            if relevant_records.is_empty() {
                continue;
            }

            for record in &relevant_records {
                let entry = agxps_trace_group(&mut groups, record);
                entry.command_count += 1;
                entry.record_cliques = entry
                    .record_cliques
                    .saturating_add(record.record.work_cliques);
                entry.analyzer_avg_duration_sum = entry
                    .analyzer_avg_duration_sum
                    .saturating_add(record.record.avg_clique_duration);
                entry.analyzer_weighted_duration =
                    entry
                        .analyzer_weighted_duration
                        .saturating_add(saturating_u128_to_u64(
                            u128::from(record.record.work_cliques)
                                * u128::from(record.record.avg_clique_duration),
                        ));
                entry.duration_ns = entry
                    .duration_ns
                    .saturating_add(record.record.duration_ns());
            }

            let work = unsafe { agxps_work_cliques(&loaded, raw.profile_data) }?;
            for index in 0..work.traces.len() {
                if work.missing_ends[index] != 0 {
                    continue;
                }
                let start_ns = unsafe {
                    (loaded.api.get_system_timestamp)(raw.profile_data, work.starts[index])
                };
                let end_ns = unsafe {
                    (loaded.api.get_system_timestamp)(raw.profile_data, work.ends[index])
                };
                let Some(record_index) =
                    find_agxps_timing_record(&relevant_records, start_ns, end_ns)
                else {
                    continue;
                };
                let record = &relevant_records[record_index];
                let entry = agxps_trace_group(&mut groups, record);
                entry.matched_work_cliques += 1;
                entry.execution_events = entry.execution_events.saturating_add(unsafe {
                    (loaded.api.instruction_trace_get_execution_events_num)(
                        raw.profile_data,
                        work.traces[index],
                    )
                });
                let stats = unsafe {
                    (loaded.api.instruction_trace_get_instruction_stats)(
                        raw.gpu,
                        raw.profile_data,
                        work.traces[index],
                    )
                };
                entry.stats_word0 = entry.stats_word0.saturating_add(stats.words[0]);
                entry.stats_word1 = entry.stats_word1.saturating_add(stats.words[1]);
            }

            let _ = raw;
        }

        for ((pipeline_index, _), cost) in groups {
            if let Some(pipeline) = pipelines.get_mut(pipeline_index) {
                pipeline.agxps_trace_costs.push(cost);
            }
        }
        for pipeline in pipelines {
            pipeline.agxps_trace_costs.sort_by(|left, right| {
                right
                    .stats_word1
                    .cmp(&left.stats_word1)
                    .then_with(|| left.shader_address.cmp(&right.shader_address))
            });
        }

        Ok(())
    }

    fn profiling_raw_paths(profiler_directory: &Path) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in fs::read_dir(profiler_directory)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with("Profiling_f_") && name.ends_with(".raw") {
                paths.push(path);
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn u32_env(name: &str, default: u32) -> u32 {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn agxps_trace_group<'a>(
        groups: &'a mut BTreeMap<(usize, u64), XcodeMioPipelineAgxpsTraceCost>,
        record: &AgxpsPipelineTimingRecord,
    ) -> &'a mut XcodeMioPipelineAgxpsTraceCost {
        groups
            .entry((record.pipeline_index, record.record.esl_shader_address))
            .or_insert_with(|| XcodeMioPipelineAgxpsTraceCost {
                source: "agxps-timing-trace",
                shader_address: record.record.esl_shader_address,
                work_shader_address: record.record.work_shader_address,
                command_count: 0,
                record_cliques: 0,
                analyzer_weighted_duration: 0,
                analyzer_avg_duration_sum: 0,
                matched_work_cliques: 0,
                duration_ns: 0,
                execution_events: 0,
                stats_word0: 0,
                stats_word1: 0,
            })
    }

    unsafe fn parse_agxps_profile(
        loaded: &agxps_sys::LoadedApi,
        generation: u32,
        variant: u32,
        rev: u32,
        bytes: &[u8],
    ) -> Result<AgxpsRawProfile> {
        let api = &loaded.api;
        let gpu = unsafe { (api.gpu_create)(generation, variant, rev, false) };
        if gpu.is_null() {
            return Err(Error::InvalidInput(format!(
                "agxps_gpu_create({generation}, {variant}, {rev}) failed"
            )));
        }

        let descriptor = agxps_sys::AgxpsApsDescriptor::defaults_for(gpu);
        let parser = unsafe { (api.parser_create)(&descriptor) };
        if parser.is_null() {
            return Err(Error::InvalidInput(
                "agxps_aps_parser_create returned NULL".to_owned(),
            ));
        }

        let mut out = vec![0u8; 4096];
        let profile_data = unsafe {
            (api.parser_parse)(
                parser,
                bytes.as_ptr(),
                bytes.len() as c_long,
                agxps_sys::APS_PROFILING_TYPE_USC_SAMPLES,
                out.as_mut_ptr().cast(),
            )
        };
        let err_code = u64::from_le_bytes(out[..8].try_into().unwrap());
        if err_code != 0 {
            let message = unsafe {
                let ptr = (api.parse_error_string)(err_code);
                if ptr.is_null() {
                    "(null)".to_owned()
                } else {
                    CStr::from_ptr(ptr).to_string_lossy().into_owned()
                }
            };
            return Err(Error::InvalidInput(format!(
                "agxps parser error {err_code}: {message}"
            )));
        }

        let _ = parser;
        Ok(AgxpsRawProfile { gpu, profile_data })
    }

    unsafe fn agxps_timing_records(
        loaded: &agxps_sys::LoadedApi,
        profile_data: agxps_sys::AgxpsApsProfileData,
    ) -> Result<Vec<AgxpsTimingRecord>> {
        const KIND: u32 = 1;
        let api = &loaded.api;
        let analyzer = unsafe { (api.timing_analyzer_create)(KIND) };
        if analyzer.is_null() {
            return Err(Error::InvalidInput(format!(
                "agxps_aps_timing_analyzer_create({KIND}) returned NULL"
            )));
        }
        unsafe {
            (api.timing_analyzer_process_usc)(analyzer, profile_data);
            (api.timing_analyzer_finish)(analyzer);
        }
        let count = unsafe { (api.timing_analyzer_get_num_commands)(analyzer, KIND) } as usize;
        let result = unsafe { agxps_fetch_timing_records(loaded, analyzer, count) };
        unsafe { (api.timing_analyzer_destroy)(analyzer) };
        result
    }

    unsafe fn agxps_fetch_timing_records(
        loaded: &agxps_sys::LoadedApi,
        analyzer: agxps_sys::AgxpsApsTimingAnalyzer,
        count: usize,
    ) -> Result<Vec<AgxpsTimingRecord>> {
        const KIND: u32 = 1;
        let api = &loaded.api;
        let mut starts = vec![0u64; count];
        let mut ends = vec![0u64; count];
        let mut work_shaders = vec![0u64; count];
        let mut esl_shaders = vec![0u64; count];
        let mut avg_durations = vec![0u64; count];
        let mut cliques = vec![0u64; count];
        if count > 0 {
            let ok = unsafe {
                (api.timing_analyzer_get_work_start)(
                    analyzer,
                    KIND,
                    starts.as_mut_ptr(),
                    0,
                    count as u64,
                ) != 0
                    && (api.timing_analyzer_get_work_end)(
                        analyzer,
                        KIND,
                        ends.as_mut_ptr(),
                        0,
                        count as u64,
                    ) != 0
                    && (api.timing_analyzer_get_work_shader_address)(
                        analyzer,
                        KIND,
                        work_shaders.as_mut_ptr(),
                        0,
                        count as u64,
                    ) != 0
                    && (api.timing_analyzer_get_esl_shader_address)(
                        analyzer,
                        KIND,
                        esl_shaders.as_mut_ptr(),
                        0,
                        count as u64,
                    ) != 0
                    && (api.timing_analyzer_get_work_cliques_average_duration)(
                        analyzer,
                        KIND,
                        avg_durations.as_mut_ptr(),
                        0,
                        count as u64,
                    ) != 0
                    && (api.timing_analyzer_get_num_work_cliques)(
                        analyzer,
                        KIND,
                        cliques.as_mut_ptr(),
                        0,
                        count as u64,
                    ) != 0
            };
            if !ok {
                return Err(Error::InvalidInput(
                    "AGXPS timing-analyzer range getter failed".to_owned(),
                ));
            }
        }

        Ok((0..count)
            .map(|index| AgxpsTimingRecord {
                start_ns: starts[index],
                end_ns: ends[index],
                work_shader_address: work_shaders[index],
                esl_shader_address: esl_shaders[index],
                avg_clique_duration: avg_durations[index],
                work_cliques: cliques[index],
            })
            .collect())
    }

    fn saturating_u128_to_u64(value: u128) -> u64 {
        value.min(u128::from(u64::MAX)) as u64
    }

    unsafe fn agxps_work_cliques(
        loaded: &agxps_sys::LoadedApi,
        profile_data: agxps_sys::AgxpsApsProfileData,
    ) -> Result<AgxpsWorkCliques> {
        let api = &loaded.api;
        let count = unsafe { (api.get_work_cliques_num)(profile_data) } as usize;
        let mut starts = vec![0u64; count];
        let mut ends = vec![0u64; count];
        let mut missing_ends = vec![0u8; count];
        let mut traces = vec![0u64; count];
        if count > 0 {
            let ok = unsafe {
                (api.get_work_clique_start)(profile_data, starts.as_mut_ptr(), 0, count as u64) != 0
                    && (api.get_work_clique_end)(profile_data, ends.as_mut_ptr(), 0, count as u64)
                        != 0
                    && (api.get_work_clique_missing_end)(
                        profile_data,
                        missing_ends.as_mut_ptr(),
                        0,
                        count as u64,
                    ) != 0
                    && (api.get_work_clique_instruction_trace)(
                        profile_data,
                        traces.as_mut_ptr(),
                        0,
                        count as u64,
                    ) != 0
            };
            if !ok {
                return Err(Error::InvalidInput(
                    "AGXPS work-clique range getter failed".to_owned(),
                ));
            }
        }
        Ok(AgxpsWorkCliques {
            starts,
            ends,
            missing_ends,
            traces,
        })
    }

    fn find_agxps_timing_record(
        records: &[AgxpsPipelineTimingRecord],
        start_ns: u64,
        end_ns: u64,
    ) -> Option<usize> {
        let mut best = None;
        for (index, record) in records.iter().enumerate() {
            let timing = record.record;
            let contains_range = timing.start_ns <= start_ns && end_ns <= timing.end_ns;
            let contains_start = timing.start_ns <= start_ns && start_ns <= timing.end_ns;
            let overlaps = timing.start_ns <= end_ns && start_ns <= timing.end_ns;
            if !contains_range && !contains_start && !overlaps {
                continue;
            }
            let rank = if contains_range {
                0
            } else if contains_start {
                1
            } else {
                2
            };
            let duration = timing.duration_ns();
            if best
                .map(|(_, best_rank, best_duration)| (rank, duration) < (best_rank, best_duration))
                .unwrap_or(true)
            {
                best = Some((index, rank, duration));
            }
        }
        best.map(|(index, _, _)| index)
    }

    struct AgxpsRawProfile {
        gpu: agxps_sys::AgxpsGpu,
        profile_data: agxps_sys::AgxpsApsProfileData,
    }

    #[derive(Clone, Copy)]
    struct AgxpsTimingRecord {
        start_ns: u64,
        end_ns: u64,
        work_shader_address: u64,
        esl_shader_address: u64,
        avg_clique_duration: u64,
        work_cliques: u64,
    }

    impl AgxpsTimingRecord {
        fn duration_ns(self) -> u64 {
            self.end_ns.saturating_sub(self.start_ns)
        }
    }

    #[derive(Clone, Copy)]
    struct AgxpsPipelineTimingRecord {
        pipeline_index: usize,
        record: AgxpsTimingRecord,
    }

    struct AgxpsWorkCliques {
        starts: Vec<u64>,
        ends: Vec<u64>,
        missing_ends: Vec<u8>,
        traces: Vec<agxps_sys::AgxpsApsCliqueInstructionTrace>,
    }

    fn attach_shader_binary_references(
        shader_binary_info: &[XcodeMioShaderBinaryInfo],
        timeline_binaries: &[XcodeMioTimelineBinary],
        gpu_commands: &[XcodeMioGpuCommand],
        pipelines: &mut [XcodeMioPipeline],
    ) {
        let binary_by_address = timeline_binaries
            .iter()
            .map(|binary| (binary.address, binary.index))
            .collect::<BTreeMap<_, _>>();
        let mut groups =
            BTreeMap::<(usize, u16, u16, u32, u64), XcodeMioPipelineShaderBinaryReference>::new();
        for info in shader_binary_info {
            let command_index = info.raw3 as usize;
            let Some(command) = gpu_commands.get(command_index) else {
                continue;
            };
            let key = (
                command.pipeline_index,
                info.raw5,
                info.raw6,
                info.raw1,
                info.raw2,
            );
            groups
                .entry(key)
                .and_modify(|reference| {
                    reference.record_count += 1;
                    reference.first_command_index =
                        reference.first_command_index.min(command_index);
                    reference.last_command_index = reference.last_command_index.max(command_index);
                })
                .or_insert_with(|| XcodeMioPipelineShaderBinaryReference {
                    raw5: info.raw5,
                    raw6: info.raw6,
                    raw1: info.raw1,
                    address: info.raw2,
                    timeline_binary_index: binary_by_address.get(&info.raw2).copied(),
                    record_count: 1,
                    first_command_index: command_index,
                    last_command_index: command_index,
                });
        }
        for ((pipeline_index, _, _, _, _), reference) in groups {
            if let Some(pipeline) = pipelines.get_mut(pipeline_index) {
                pipeline.shader_binary_references.push(reference);
            }
        }
    }

    fn program_type_candidates(pipeline: &XcodeMioPipeline) -> Vec<u16> {
        let mut candidates = BTreeSet::from([8_u16, 28_u16]);
        for binary in &pipeline.shader_binaries {
            candidates.insert(binary.program_type);
        }
        candidates.into_iter().collect()
    }

    fn execution_history_pipeline_ids(
        pipeline: &XcodeMioPipeline,
        timeline_pipeline_state_ids: &[u64],
    ) -> Vec<(&'static str, u64)> {
        let mut values = vec![
            ("object_id", pipeline.object_id),
            ("pointer_id", pipeline.pointer_id),
            ("function_index", pipeline.function_index),
            ("pipeline_index", pipeline.index as u64),
        ];
        let raw_ids = pipeline
            .shader_binary_references
            .iter()
            .filter(|reference| reference.raw5 == 6 && reference.raw6 == 28)
            .map(|reference| reference.raw1 as u64)
            .collect::<BTreeSet<_>>();
        values.extend(raw_ids.into_iter().map(|raw1| ("shader_binary_raw1", raw1)));
        values.extend(
            timeline_pipeline_state_ids
                .iter()
                .copied()
                .map(|pipeline_state_id| ("timeline_pipeline_state_id", pipeline_state_id)),
        );
        values
    }

    unsafe fn decode_timeline_binary(binary: Id, fallback_index: u64) -> XcodeMioTimelineBinary {
        unsafe {
            let index = send_u64(binary, "index").unwrap_or(fallback_index);
            let address = send_u64(binary, "address").unwrap_or(0);
            let program_type = send_u16(binary, "programType").unwrap_or(0);
            let instruction_info_count = send_u64(binary, "instructionInfoCount").unwrap_or(0);
            let instruction_executed = send_u64(binary, "instructionExecuted").unwrap_or(0);
            let duration_ns = send_u64(binary, "duration").unwrap_or(0);
            let trace_count = send_u64(binary, "traceCount").unwrap_or(0);
            let cost_count = send_u64(binary, "costCount").unwrap_or(0);
            let (total_cost, total_instruction_count) =
                decode_binary_total_cost(binary, cost_count, instruction_info_count);
            let instruction_costs =
                decode_binary_instruction_costs(binary, instruction_info_count, "instructionCosts");
            XcodeMioTimelineBinary {
                index,
                address,
                program_type,
                instruction_info_count,
                instruction_executed,
                duration_ns,
                trace_count,
                cost_count,
                total_cost,
                total_instruction_count,
                instruction_cost_record_count: instruction_costs.record_count,
                instruction_nonzero_record_count: instruction_costs.nonzero_record_count,
                instruction_total_cost: instruction_costs.total_cost,
                instruction_total_count: instruction_costs.total_instruction_count,
            }
        }
    }

    unsafe fn push_decoded_binary(
        pipeline: &mut XcodeMioPipeline,
        seen: &mut BTreeSet<(usize, u64, u16, u64)>,
        binary: Id,
        pipeline_id_kind: &'static str,
        pipeline_id: u64,
        fallback_binary_index: u64,
    ) {
        unsafe {
            let binary_index = send_u64(binary, "index").unwrap_or(fallback_binary_index);
            let address = send_u64(binary, "address").unwrap_or(0);
            let program_type = send_u16(binary, "programType").unwrap_or(0);
            if !seen.insert((pipeline.index, binary_index, program_type, address)) {
                return;
            }
            let instruction_info_count = send_u64(binary, "instructionInfoCount").unwrap_or(0);
            let instruction_executed = send_u64(binary, "instructionExecuted").unwrap_or(0);
            let duration_ns = send_u64(binary, "duration").unwrap_or(0);
            let trace_count = send_u64(binary, "traceCount").unwrap_or(0);
            let cost_count = send_u64(binary, "costCount").unwrap_or(0);
            let (total_cost, total_instruction_count) =
                decode_binary_total_cost(binary, cost_count, instruction_info_count);
            for (source, costs) in [
                (
                    "instructionCostsForPipelineState",
                    decode_binary_instruction_costs_for_pipeline_state(
                        binary,
                        pipeline_id,
                        instruction_info_count,
                    ),
                ),
                (
                    "instructionCosts",
                    decode_binary_instruction_costs(
                        binary,
                        instruction_info_count,
                        "instructionCosts",
                    ),
                ),
            ] {
                if costs.total_cost > 0.0 || costs.total_instruction_count > 0 {
                    pipeline
                        .shader_binary_costs
                        .push(XcodeMioPipelineShaderBinaryCost {
                            source,
                            pipeline_id_kind,
                            pipeline_id,
                            binary_index,
                            address,
                            program_type,
                            record_count: costs.record_count,
                            nonzero_record_count: costs.nonzero_record_count,
                            total_cost: costs.total_cost,
                            total_instruction_count: costs.total_instruction_count,
                            alu_cost: costs.alu_cost,
                            non_alu_cost: costs.non_alu_cost,
                        });
                }
            }
            pipeline.shader_binaries.push(XcodeMioPipelineShaderBinary {
                pipeline_id_kind,
                pipeline_id,
                binary_index,
                address,
                program_type,
                instruction_info_count,
                instruction_executed,
                duration_ns,
                trace_count,
                cost_count,
                total_cost,
                total_instruction_count,
            });
        }
    }

    unsafe fn decode_binary_total_cost(
        binary: Id,
        cost_count: u64,
        _instruction_info_count: u64,
    ) -> (f64, u64) {
        unsafe {
            if cost_count == 0 {
                return (0.0, 0);
            }
            let Ok(costs) = send_ptr(binary, "costs") else {
                return (0.0, 0);
            };
            if costs.is_null() {
                return (0.0, 0);
            }
            decode_cost_info_sum(costs.cast(), cost_count as usize)
        }
    }

    unsafe fn decode_gpu_cost(trace_data: Id) -> (f64, u64) {
        unsafe {
            let Ok(cost) = send_ptr(trace_data, "gpuCost") else {
                return (0.0, 0);
            };
            if cost.is_null() {
                return (0.0, 0);
            }
            let cost = *cost.cast::<RawGtmioCostInfo>();
            (cost.alu_cost + cost.non_alu_cost, cost.instruction_count)
        }
    }

    unsafe fn decode_binary_instruction_costs(
        binary: Id,
        instruction_info_count: u64,
        selector: &str,
    ) -> DecodedCostAggregate {
        unsafe {
            let Ok(costs) = send_ptr(binary, selector) else {
                return DecodedCostAggregate::default();
            };
            decode_instruction_cost_pointer(costs, instruction_info_count)
        }
    }

    unsafe fn decode_binary_instruction_costs_for_pipeline_state(
        binary: Id,
        pipeline_state_id: u64,
        instruction_info_count: u64,
    ) -> DecodedCostAggregate {
        unsafe {
            let Ok(costs) = send_ptr_u64(
                binary,
                "instructionCostsForPipelineState:",
                pipeline_state_id,
            ) else {
                return DecodedCostAggregate::default();
            };
            decode_instruction_cost_pointer(costs, instruction_info_count)
        }
    }

    unsafe fn decode_instruction_cost_pointer(
        costs: *const c_void,
        instruction_info_count: u64,
    ) -> DecodedCostAggregate {
        if costs.is_null() || instruction_info_count == 0 || instruction_info_count > 1_000_000 {
            return DecodedCostAggregate::default();
        }
        let costs = costs.cast::<RawGtmioCostInfo>();
        let mut aggregate = DecodedCostAggregate {
            record_count: instruction_info_count,
            ..DecodedCostAggregate::default()
        };
        for index in 0..instruction_info_count as usize {
            let cost = unsafe { *costs.add(index) };
            let total_cost = cost.alu_cost + cost.non_alu_cost;
            if !cost.is_empty() {
                aggregate.nonzero_record_count += 1;
            }
            if total_cost.is_finite() {
                aggregate.total_cost += total_cost;
                aggregate.alu_cost += cost.alu_cost;
                aggregate.non_alu_cost += cost.non_alu_cost;
            }
            aggregate.total_instruction_count = aggregate
                .total_instruction_count
                .saturating_add(cost.instruction_count);
        }
        aggregate
    }

    unsafe fn decode_execution_history_node(root: Id) -> DecodedExecutionHistoryNode {
        unsafe {
            let mut best = DecodedExecutionHistoryNode::default();
            let mut stack = vec![root];
            let mut visited = 0_usize;
            while let Some(node) = stack.pop() {
                visited += 1;
                if visited > 20_000 {
                    break;
                }
                let top_cost_percentage = send_f64(node, "topCostPercentage").unwrap_or(0.0);
                let duration_percentage = send_f64(node, "durationPercentage").unwrap_or(0.0);
                let total_duration_ns = send_u64(node, "totalDuration").unwrap_or(0);
                let mut cost = RawGtmioCostInfo::default();
                let found = send_i8_u16_u64_cost_mut(
                    node,
                    "costForScope:scopeIdentifier:cost:",
                    0,
                    0,
                    &mut cost,
                )
                .unwrap_or(0);
                let total_cost = if found != 0 {
                    cost.alu_cost + cost.non_alu_cost
                } else {
                    0.0
                };
                let candidate = DecodedExecutionHistoryNode {
                    top_cost_percentage,
                    duration_percentage,
                    total_duration_ns,
                    total_cost,
                    instruction_count: cost.instruction_count,
                };
                if candidate
                    .top_cost_percentage
                    .partial_cmp(&best.top_cost_percentage)
                    .unwrap_or(std::cmp::Ordering::Less)
                    .is_gt()
                    || (candidate.top_cost_percentage == best.top_cost_percentage
                        && candidate.total_cost > best.total_cost)
                {
                    best = candidate;
                }
                let Ok(children) = send_id_allow_nil(node, "children") else {
                    continue;
                };
                if children.is_null() {
                    continue;
                }
                let child_count = send_u64(children, "count").unwrap_or(0).min(20_000);
                for index in 0..child_count as usize {
                    if let Ok(child) = send_id_usize(children, "objectAtIndex:", index) {
                        stack.push(child);
                    }
                }
            }
            best
        }
    }

    unsafe fn decode_cost_info_sum(costs: *const RawGtmioCostInfo, count: usize) -> (f64, u64) {
        let mut total_cost = 0.0;
        let mut total_instruction_count = 0;
        for index in 0..count {
            let cost = unsafe { *costs.add(index) };
            total_cost += cost.alu_cost + cost.non_alu_cost;
            total_instruction_count += cost.instruction_count;
        }
        (total_cost, total_instruction_count)
    }

    unsafe fn draw_scope_cost(timeline: Id, draw_index: u64, metadata_program_type: u32) -> f64 {
        let mut best = 0.0_f64;
        let mut candidates = [u16::MAX; 18];
        candidates[0] = metadata_program_type as u16;
        for (index, value) in (0_u16..=16).enumerate() {
            candidates[index + 1] = value;
        }
        for scope in 0_u16..=16 {
            for program_type in candidates {
                if program_type == u16::MAX {
                    continue;
                }
                for selector in [
                    "totalCostForScope:scopeIdentifier:programType:",
                    "totalCostForScope:scopeIdentifier:dataMaster:",
                ] {
                    let cost = unsafe {
                        send_f64_u16_u64_u16(timeline, selector, scope, draw_index, program_type)
                    }
                    .unwrap_or(0.0);
                    if cost.is_finite() && cost > best {
                        best = cost;
                    }
                }
            }
        }
        best
    }

    unsafe fn draw_duration(timeline: Id, draw_index: u32, metadata_data_master: u16) -> u64 {
        let mut best = 0_u64;
        let mut candidates = BTreeSet::from([metadata_data_master, 0, 1, 2, 8, 28]);
        candidates.extend(0_u16..=16);
        for data_master in candidates {
            let duration = unsafe {
                send_u64_u32_u16(
                    timeline,
                    "durationForDraw:dataMaster:",
                    draw_index,
                    data_master,
                )
            }
            .unwrap_or(0);
            best = best.max(duration);
        }
        best
    }

    const _: () = assert!(std::mem::size_of::<RawGtmioCostContext>() == 16);
    const _: () = assert!(std::mem::size_of::<RawGtmioCostInfo>() == 304);
    const _: () = assert!(std::mem::size_of::<RawGtStatistics>() == 24);
    const _: () = assert!(std::mem::size_of::<RawGtShaderProfilerTiming>() == 80);
    const _: () = assert!(std::mem::size_of::<RawGtmioBinaryTrace>() == 40);
    const _: () = assert!(std::mem::size_of::<RawGtmioDrawTrace>() == 24);

    struct FdSilencer {
        stdout_fd: c_int,
        stderr_fd: c_int,
    }

    impl FdSilencer {
        fn new() -> Self {
            unsafe {
                let dev_null = c"/dev/null";
                let null_fd = open(dev_null.as_ptr(), 1);
                if null_fd < 0 {
                    return Self {
                        stdout_fd: -1,
                        stderr_fd: -1,
                    };
                }
                let stdout_fd = dup(1);
                let stderr_fd = dup(2);
                if stdout_fd >= 0 {
                    let _ = dup2(null_fd, 1);
                }
                if stderr_fd >= 0 {
                    let _ = dup2(null_fd, 2);
                }
                let _ = close(null_fd);
                Self {
                    stdout_fd,
                    stderr_fd,
                }
            }
        }
    }

    impl Drop for FdSilencer {
        fn drop(&mut self) {
            unsafe {
                if self.stdout_fd >= 0 {
                    let _ = dup2(self.stdout_fd, 1);
                    let _ = close(self.stdout_fd);
                }
                if self.stderr_fd >= 0 {
                    let _ = dup2(self.stderr_fd, 2);
                    let _ = close(self.stderr_fd);
                }
            }
        }
    }

    impl Drop for Runtime {
        fn drop(&mut self) {
            if !self.pool.is_null() {
                unsafe {
                    let _ = send_void(self.pool, "drain");
                }
            }
        }
    }

    struct CfString {
        ptr: CfStringRef,
    }

    impl CfString {
        unsafe fn new(value: *const c_char) -> Result<Self> {
            let ptr = unsafe {
                CFStringCreateWithCString(std::ptr::null(), value, K_CF_STRING_ENCODING_UTF8)
            };
            if ptr.is_null() {
                Err(Error::InvalidInput("failed to create CFString".to_owned()))
            } else {
                Ok(Self { ptr })
            }
        }
    }

    impl Drop for CfString {
        fn drop(&mut self) {
            unsafe {
                CFRelease(self.ptr);
            }
        }
    }

    fn read_u32(bytes: &[u8], offset: usize) -> u32 {
        u32::from_ne_bytes(bytes[offset..offset + 4].try_into().expect("u32 slice"))
    }

    fn read_i32(bytes: &[u8], offset: usize) -> i32 {
        i32::from_ne_bytes(bytes[offset..offset + 4].try_into().expect("i32 slice"))
    }

    fn read_u64(bytes: &[u8], offset: usize) -> u64 {
        u64::from_ne_bytes(bytes[offset..offset + 8].try_into().expect("u64 slice"))
    }

    fn read_u16(bytes: &[u8], offset: usize) -> u16 {
        u16::from_ne_bytes(bytes[offset..offset + 2].try_into().expect("u16 slice"))
    }

    unsafe fn nsstring_to_string(value: Id) -> Option<String> {
        let bytes = unsafe { send_ptr(value, "UTF8String").ok()? };
        if bytes.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(bytes.cast::<c_char>()) }
            .to_str()
            .ok()
            .map(ToOwned::to_owned)
    }

    unsafe fn load_framework(path: &str) -> Result<()> {
        let path = CString::new(path).expect("framework path contains no NUL");
        let handle = unsafe { dlopen(path.as_ptr(), RTLD_NOW | RTLD_GLOBAL) };
        if handle.is_null() {
            Err(Error::NotFound(PathBuf::from(
                path.to_string_lossy().into_owned(),
            )))
        } else {
            Ok(())
        }
    }

    unsafe fn lookup_class(name: &str) -> Result<Class> {
        let name = CString::new(name).expect("class name contains no NUL");
        let class = unsafe { objc_lookUpClass(name.as_ptr()) };
        if class.is_null() {
            Err(Error::InvalidInput(format!(
                "Objective-C class {} is not available",
                name.to_string_lossy()
            )))
        } else {
            Ok(class)
        }
    }

    unsafe fn selector(name: &str) -> Result<Sel> {
        let name = CString::new(name)
            .map_err(|_| Error::InvalidInput("selector contains NUL".to_owned()))?;
        let sel = unsafe { sel_registerName(name.as_ptr()) };
        if sel.is_null() {
            Err(Error::InvalidInput(format!(
                "Objective-C selector {} is not available",
                name.to_string_lossy()
            )))
        } else {
            Ok(sel)
        }
    }

    unsafe fn responds_to_selector(receiver: Id, selector_name: &str) -> bool {
        if receiver.is_null() {
            return false;
        }
        let Ok(responds_to_selector) = (unsafe { selector("respondsToSelector:") }) else {
            return false;
        };
        let Ok(target_selector) = (unsafe { selector(selector_name) }) else {
            return false;
        };
        let f: extern "C" fn(Id, Sel, Sel) -> i8 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        f(receiver, responds_to_selector, target_selector) != 0
    }

    unsafe fn send_id(receiver: Id, sel: &str) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> Id = unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_allow_nil(receiver: Id, sel: &str) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> Id = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_id_id(receiver: Id, sel: &str, arg: Id) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, arg);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_id_allow_nil(receiver: Id, sel: &str, arg: Id) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, arg))
    }

    unsafe fn send_id_id_id(receiver: Id, sel: &str, left: Id, right: Id) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id, Id) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, left, right);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_id_u32_u32_id(
        receiver: Id,
        sel: &str,
        first: Id,
        second: u32,
        third: u32,
        fourth: Id,
    ) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id, u32, u32, Id) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, first, second, third, fourth);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_u64_u16_allow_nil(
        receiver: Id,
        sel: &str,
        left: u64,
        right: u16,
    ) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u64, u16) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, left, right))
    }

    unsafe fn send_id_u64_allow_nil(receiver: Id, sel: &str, arg: u64) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u64) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, arg))
    }

    unsafe fn send_id_u32_allow_nil(receiver: Id, sel: &str, arg: u32) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u32) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, arg))
    }

    unsafe fn send_id_usize(receiver: Id, sel: &str, arg: usize) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, usize) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, arg);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_void(receiver: Id, sel: &str) -> Result<()> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) = unsafe { mem::transmute(objc_msgSend as *const ()) };
        f(receiver, sel);
        Ok(())
    }

    unsafe fn send_void_id(receiver: Id, sel: &str, arg: Id) -> Result<()> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id) = unsafe { mem::transmute(objc_msgSend as *const ()) };
        f(receiver, sel, arg);
        Ok(())
    }

    unsafe fn send_void_u64_u16_id_id(
        receiver: Id,
        sel: &str,
        first: u64,
        second: u16,
        third: Id,
        fourth: Id,
    ) -> Result<()> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u64, u16, Id, Id) =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        f(receiver, sel, first, second, third, fourth);
        Ok(())
    }

    unsafe fn send_u64(receiver: Id, sel: &str) -> Result<u64> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> u64 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_u64_if_supported(receiver: Id, sel: &str) -> u64 {
        if unsafe { responds_to_selector(receiver, sel) } {
            unsafe { send_u64(receiver, sel).unwrap_or(0) }
        } else {
            0
        }
    }

    unsafe fn send_u64_u32_u16(receiver: Id, sel: &str, first: u32, second: u16) -> Result<u64> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u32, u16) -> u64 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, first, second))
    }

    unsafe fn send_u32(receiver: Id, sel: &str) -> Result<u32> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> u32 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_u16(receiver: Id, sel: &str) -> Result<u16> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> u16 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_i32(receiver: Id, sel: &str) -> Result<i32> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> i32 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_i8_u64(receiver: Id, sel: &str, arg: u64) -> Result<i8> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u64) -> i8 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, arg))
    }

    unsafe fn send_f64(receiver: Id, sel: &str) -> Result<f64> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> f64 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_f64_u32(receiver: Id, sel: &str, arg: u32) -> Result<f64> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u32) -> f64 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, arg))
    }

    unsafe fn send_profiler_timing(receiver: Id, sel: &str) -> Result<RawGtShaderProfilerTiming> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> RawGtShaderProfilerTiming =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_void_i8(receiver: Id, sel: &str, arg: i8) -> Result<()> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, i8) = unsafe { mem::transmute(objc_msgSend as *const ()) };
        f(receiver, sel, arg);
        Ok(())
    }

    unsafe fn send_ptr(receiver: Id, sel: &str) -> Result<*const c_void> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> *const c_void =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_ptr_u64(receiver: Id, sel: &str, arg: u64) -> Result<*const c_void> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u64) -> *const c_void =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, arg))
    }

    unsafe fn send_ptr_u64_u32(
        receiver: Id,
        sel: &str,
        first: u64,
        second: u32,
    ) -> Result<*const c_void> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u64, u32) -> *const c_void =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, first, second))
    }

    unsafe fn send_i8_u16_u64_cost_mut(
        receiver: Id,
        sel: &str,
        first: u16,
        second: u64,
        third: *mut RawGtmioCostInfo,
    ) -> Result<i8> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u16, u64, *mut RawGtmioCostInfo) -> i8 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, first, second, third))
    }

    unsafe fn send_f64_u16_u64_u16(
        receiver: Id,
        sel: &str,
        first: u16,
        second: u64,
        third: u16,
    ) -> Result<f64> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u16, u64, u16) -> f64 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, first, second, third))
    }

    unsafe fn send_i8_context_cost_mut(
        receiver: Id,
        sel: &str,
        context: *const RawGtmioCostContext,
        cost: *mut RawGtmioCostInfo,
    ) -> Result<i8> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, *const RawGtmioCostContext, *mut RawGtmioCostInfo) -> i8 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, context, cost))
    }

    unsafe fn send_i8_u16_u32_u16_u64_cost_mut(
        receiver: Id,
        sel: &str,
        level: u16,
        level_identifier: u32,
        scope: u16,
        scope_identifier: u64,
        cost: *mut RawGtmioCostInfo,
    ) -> Result<i8> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u16, u32, u16, u64, *mut RawGtmioCostInfo) -> i8 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(
            receiver,
            sel,
            level,
            level_identifier,
            scope,
            scope_identifier,
            cost,
        ))
    }
}

#[cfg(not(target_os = "macos"))]
mod platform {
    use std::path::PathBuf;

    use super::{XcodeMioReport, XcodeMioTimings};
    use crate::error::{Error, Result};
    use crate::profiler;

    pub fn decode(
        _trace_source: PathBuf,
        _profiler_directory: PathBuf,
        _stream_data_path: PathBuf,
        _profiler_summary: Option<&profiler::ProfilerStreamDataSummary>,
        _timings: XcodeMioTimings,
    ) -> Result<XcodeMioReport> {
        Err(Error::Unsupported(
            "xcode-mio is only available on macOS with Xcode installed",
        ))
    }
}
