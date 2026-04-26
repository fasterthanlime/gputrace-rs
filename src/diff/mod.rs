use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use crate::analysis::{AnalysisReport, analyze};
use crate::counter_export;
use crate::error::{Error, Result};
use crate::profiler;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct DiffReport {
    pub left: AnalysisReport,
    pub right: AnalysisReport,
    pub buffer_changes: Vec<BufferChange>,
    pub buffer_lifecycle_changes: Vec<BufferLifecycleChange>,
    pub kernel_changes: Vec<KernelChange>,
    pub kernel_timing_changes: Vec<KernelTimingChange>,
    pub counter_metric_changes: Vec<CounterMetricChange>,
    pub profile_diff: Option<ProfileDiffReport>,
    pub summary: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DiffOptions {
    pub profile: ProfileDiffOptions,
    pub profile_only: bool,
}

#[derive(Debug, Clone)]
pub struct ProfileDiffOptions {
    pub limit: usize,
    pub min_delta_us: i64,
    pub only_encoder: Option<usize>,
    pub only_function: Option<String>,
}

impl Default for ProfileDiffOptions {
    fn default() -> Self {
        Self {
            limit: 20,
            min_delta_us: 0,
            only_encoder: None,
            only_function: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileDiffReport {
    pub schema_version: String,
    pub left_path: String,
    pub right_path: String,
    pub summary: ProfileDiffSummary,
    pub top_function_deltas: Vec<ProfileFunctionDelta>,
    pub top_dispatch_outliers: Vec<ProfileMatchPair>,
    pub encoder_deltas: Vec<ProfileEncoderDelta>,
    pub encoder_reports: Vec<ProfileEncoderReport>,
    pub pipeline_deltas: Vec<ProfilePipelineDelta>,
    pub unnamed_dispatch_deltas: Vec<ProfileUnnamedDispatchDelta>,
    pub timeline_spike_windows: Vec<ProfileSpikeWindow>,
    pub matched_pairs: Vec<ProfileMatchPair>,
    pub unmatched: Vec<ProfileUnmatchedDispatch>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileDiffSummary {
    pub left_label: String,
    pub right_label: String,
    pub left_dispatch_count: usize,
    pub right_dispatch_count: usize,
    pub dispatch_count_delta: isize,
    pub left_total_gpu_time_us: u64,
    pub right_total_gpu_time_us: u64,
    pub total_delta_us: i64,
    pub matched_delta_us: i64,
    pub unmatched_delta_us: i64,
    pub likely_cause: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileFunctionDelta {
    pub function_name: String,
    pub left_dispatch_count: usize,
    pub right_dispatch_count: usize,
    pub dispatch_count_delta: isize,
    pub matched_pairs: usize,
    pub left_total_us: u64,
    pub right_total_us: u64,
    pub total_delta_us: i64,
    pub first_occurrence_delta_us: i64,
    pub max_occurrence_delta_us: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileEncoderDelta {
    pub encoder_index: usize,
    pub left_dispatch_count: usize,
    pub right_dispatch_count: usize,
    pub dispatch_count_delta: isize,
    pub left_total_us: u64,
    pub right_total_us: u64,
    pub total_delta_us: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileEncoderReport {
    pub encoder_index: usize,
    pub left_dispatch_count: usize,
    pub right_dispatch_count: usize,
    pub matched_count: usize,
    pub matched_delta_us: i64,
    pub unmatched_left_count: usize,
    pub unmatched_right_count: usize,
    pub unmatched_count: usize,
    pub unmatched_delta_us: i64,
    pub top_dispatches: Vec<ProfileMatchPair>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfilePipelineDelta {
    pub pipeline_id: Option<i64>,
    pub function_name: String,
    pub left_dispatch_count: usize,
    pub right_dispatch_count: usize,
    pub dispatch_count_delta: isize,
    pub left_total_us: u64,
    pub right_total_us: u64,
    pub total_delta_us: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileUnnamedDispatchDelta {
    pub kernel_id: String,
    pub pipeline_id: Option<i64>,
    pub left_dispatch_count: usize,
    pub right_dispatch_count: usize,
    pub dispatch_count_delta: isize,
    pub left_total_us: u64,
    pub right_total_us: u64,
    pub total_delta_us: i64,
    pub top_outlier_delta_us: i64,
    pub top_outlier_left_source_index: Option<usize>,
    pub top_outlier_right_source_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileSpikeWindow {
    pub encoder_index: usize,
    pub left_start_source_index: usize,
    pub left_end_source_index: usize,
    pub right_start_source_index: usize,
    pub right_end_source_index: usize,
    pub match_count: usize,
    pub total_delta_us: i64,
    pub max_abs_delta_us: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileMatchPair {
    pub left_source_index: usize,
    pub right_source_index: usize,
    pub function_name: String,
    pub kernel_id: String,
    pub encoder_index: usize,
    pub left_pipeline_id: Option<i64>,
    pub right_pipeline_id: Option<i64>,
    pub left_duration_us: u64,
    pub right_duration_us: u64,
    pub delta_us: i64,
    pub match_method: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileUnmatchedDispatch {
    pub trace: String,
    pub source_index: usize,
    pub function_name: String,
    pub kernel_id: String,
    pub encoder_index: usize,
    pub pipeline_id: Option<i64>,
    pub duration_us: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KernelChange {
    pub name: String,
    pub left_dispatches: usize,
    pub right_dispatches: usize,
    pub delta: isize,
}

#[derive(Debug, Clone, Serialize)]
pub struct KernelTimingChange {
    pub name: String,
    pub left_duration_ns: u64,
    pub right_duration_ns: u64,
    pub duration_delta_ns: i64,
    pub left_percent_of_total: f64,
    pub right_percent_of_total: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferChange {
    pub name: String,
    pub status: BufferChangeStatus,
    pub left_uses: usize,
    pub right_uses: usize,
    pub left_encoders: usize,
    pub right_encoders: usize,
    pub left_command_buffers: usize,
    pub right_command_buffers: usize,
    pub delta: isize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub enum BufferChangeStatus {
    Added,
    Removed,
    Changed,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferLifecycleChange {
    pub name: String,
    pub status: BufferChangeStatus,
    pub left_command_buffer_span: usize,
    pub right_command_buffer_span: usize,
    pub command_buffer_span_delta: isize,
    pub left_dispatch_span: usize,
    pub right_dispatch_span: usize,
    pub dispatch_span_delta: isize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CounterMetricChange {
    pub name: String,
    pub left_kernel_invocations: Option<f64>,
    pub right_kernel_invocations: Option<f64>,
    pub left_execution_cost_percent: Option<f64>,
    pub right_execution_cost_percent: Option<f64>,
    pub left_occupancy_percent: Option<f64>,
    pub right_occupancy_percent: Option<f64>,
    pub left_alu_utilization_percent: Option<f64>,
    pub right_alu_utilization_percent: Option<f64>,
    pub left_last_level_cache_percent: Option<f64>,
    pub right_last_level_cache_percent: Option<f64>,
    pub left_device_memory_bandwidth_gbps: Option<f64>,
    pub right_device_memory_bandwidth_gbps: Option<f64>,
    pub left_gpu_read_bandwidth_gbps: Option<f64>,
    pub right_gpu_read_bandwidth_gbps: Option<f64>,
    pub left_gpu_write_bandwidth_gbps: Option<f64>,
    pub right_gpu_write_bandwidth_gbps: Option<f64>,
    pub left_buffer_device_memory_bytes_read: Option<f64>,
    pub right_buffer_device_memory_bytes_read: Option<f64>,
    pub left_buffer_device_memory_bytes_written: Option<f64>,
    pub right_buffer_device_memory_bytes_written: Option<f64>,
    pub left_bytes_read_from_device_memory: Option<f64>,
    pub right_bytes_read_from_device_memory: Option<f64>,
    pub left_bytes_written_to_device_memory: Option<f64>,
    pub right_bytes_written_to_device_memory: Option<f64>,
    pub left_buffer_l1_miss_rate_percent: Option<f64>,
    pub right_buffer_l1_miss_rate_percent: Option<f64>,
    pub left_buffer_l1_read_accesses: Option<f64>,
    pub right_buffer_l1_read_accesses: Option<f64>,
    pub left_buffer_l1_read_bandwidth_gbps: Option<f64>,
    pub right_buffer_l1_read_bandwidth_gbps: Option<f64>,
    pub left_buffer_l1_write_accesses: Option<f64>,
    pub right_buffer_l1_write_accesses: Option<f64>,
    pub left_buffer_l1_write_bandwidth_gbps: Option<f64>,
    pub right_buffer_l1_write_bandwidth_gbps: Option<f64>,
    pub left_compute_shader_launch_utilization_percent: Option<f64>,
    pub right_compute_shader_launch_utilization_percent: Option<f64>,
    pub left_control_flow_utilization_percent: Option<f64>,
    pub right_control_flow_utilization_percent: Option<f64>,
    pub left_instruction_throughput_utilization_percent: Option<f64>,
    pub right_instruction_throughput_utilization_percent: Option<f64>,
    pub left_integer_complex_utilization_percent: Option<f64>,
    pub right_integer_complex_utilization_percent: Option<f64>,
    pub left_integer_conditional_utilization_percent: Option<f64>,
    pub right_integer_conditional_utilization_percent: Option<f64>,
    pub left_f32_utilization_percent: Option<f64>,
    pub right_f32_utilization_percent: Option<f64>,
}

pub fn diff_paths(left: impl AsRef<Path>, right: impl AsRef<Path>) -> Result<DiffReport> {
    diff_paths_with_options(left, right, &DiffOptions::default())
}

pub fn diff_paths_with_options(
    left: impl AsRef<Path>,
    right: impl AsRef<Path>,
    options: &DiffOptions,
) -> Result<DiffReport> {
    if options.profile_only {
        return Ok(diff_profile_paths_report(
            left.as_ref(),
            right.as_ref(),
            &options.profile,
        ));
    }
    let (left, left_warnings) = open_trace_bundle_for_diff(left.as_ref())?;
    let (right, right_warnings) = open_trace_bundle_for_diff(right.as_ref())?;
    let mut report = diff_with_options(&left, &right, options);
    for warning in left_warnings.into_iter().chain(right_warnings) {
        report.summary.push(warning.clone());
        if let Some(profile) = &mut report.profile_diff {
            profile.warnings.push(warning);
        }
    }
    Ok(report)
}

fn diff_profile_paths_report(
    left: &Path,
    right: &Path,
    options: &ProfileDiffOptions,
) -> DiffReport {
    let profile_diff = diff_profile_paths(left, right, options);
    let mut summary = Vec::new();
    if let Some(profile) = &profile_diff {
        summary.push(format!(
            "Profile dispatch delta: {} -> {} ({:+}), GPU time {} -> {} us ({:+} us), matched delta {:+} us, unmatched delta {:+} us",
            profile.summary.left_dispatch_count,
            profile.summary.right_dispatch_count,
            profile.summary.dispatch_count_delta,
            profile.summary.left_total_gpu_time_us,
            profile.summary.right_total_gpu_time_us,
            profile.summary.total_delta_us,
            profile.summary.matched_delta_us,
            profile.summary.unmatched_delta_us
        ));
        summary.push(format!(
            "Likely profile cause: {}",
            profile.summary.likely_cause
        ));
        summary.extend(profile.warnings.clone());
    } else {
        summary.push("No profile dispatch data found for either input.".to_owned());
    }

    DiffReport {
        left: empty_analysis_report(left),
        right: empty_analysis_report(right),
        buffer_changes: Vec::new(),
        buffer_lifecycle_changes: Vec::new(),
        kernel_changes: Vec::new(),
        kernel_timing_changes: Vec::new(),
        counter_metric_changes: Vec::new(),
        profile_diff,
        summary,
    }
}

fn empty_analysis_report(path: &Path) -> AnalysisReport {
    AnalysisReport {
        trace: crate::trace::TraceSummary {
            trace_name: path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("unknown")
                .to_owned(),
            uuid: None,
            capture_version: None,
            graphics_api: None,
            device_id: None,
            capture_len: 0,
            device_resource_count: 0,
            device_resource_bytes: 0,
        },
        timing_synthetic: false,
        total_duration_ns: 0,
        command_buffer_count: 0,
        command_buffer_region_count: 0,
        compute_encoder_count: 0,
        dispatch_count: 0,
        pipeline_function_count: 0,
        kernel_count: 0,
        buffer_count: 0,
        shared_buffer_count: 0,
        single_use_buffer_count: 0,
        short_lived_buffer_count: 0,
        long_lived_buffer_count: 0,
        buffer_inventory_count: 0,
        buffer_inventory_bytes: 0,
        buffer_inventory_aliases: 0,
        unused_resource_count: 0,
        unused_resource_bytes: 0,
        kernel_stats: Vec::new(),
        timed_kernel_stats: Vec::new(),
        buffer_stats: Vec::new(),
        buffer_lifecycles: Vec::new(),
        largest_buffers: Vec::new(),
        unused_resource_groups: Vec::new(),
        findings: Vec::new(),
    }
}

fn open_trace_bundle_for_diff(path: &Path) -> Result<(TraceBundle, Vec<String>)> {
    match TraceBundle::open(path) {
        Ok(trace) => Ok((trace, Vec::new())),
        Err(error @ Error::MissingFile(_)) => {
            let Some(raw_sibling) = raw_capture_sibling(path) else {
                return Err(error);
            };
            match TraceBundle::open(&raw_sibling) {
                Ok(trace) => Ok((
                    trace,
                    vec![format!(
                        "Diff input {} is missing raw capture data; using sibling raw capture {} for structural analysis.",
                        path.display(),
                        raw_sibling.display()
                    )],
                )),
                Err(_) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

fn raw_capture_sibling(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix("-perfdata.gputrace")?;
    Some(path.with_file_name(format!("{stem}.gputrace")))
}

pub fn diff(left: &TraceBundle, right: &TraceBundle) -> DiffReport {
    diff_with_options(left, right, &DiffOptions::default())
}

pub fn format_profile_csv(report: &DiffReport, view: Option<&str>, limit: usize) -> Result<String> {
    let Some(profile) = &report.profile_diff else {
        return Ok(String::new());
    };
    let view = view.unwrap_or("function").trim();
    let limit = limit.max(1);
    let mut out = String::new();
    match view {
        "" | "function" => {
            out.push_str("function,dispatch_count_left,dispatch_count_right,dispatch_count_delta,matched_pairs,left_total_us,right_total_us,total_delta_us,first_occurrence_delta_us,max_occurrence_delta_us\n");
            for row in profile.top_function_deltas.iter().take(limit) {
                out.push_str(&format!(
                    "{},{},{},{},{},{},{},{},{},{}\n",
                    csv_string(&row.function_name),
                    row.left_dispatch_count,
                    row.right_dispatch_count,
                    row.dispatch_count_delta,
                    row.matched_pairs,
                    row.left_total_us,
                    row.right_total_us,
                    row.total_delta_us,
                    row.first_occurrence_delta_us,
                    row.max_occurrence_delta_us
                ));
            }
        }
        "encoder" => {
            out.push_str("encoder_index,dispatch_count_left,dispatch_count_right,dispatch_count_delta,left_total_us,right_total_us,total_delta_us\n");
            for row in profile.encoder_deltas.iter().take(limit) {
                out.push_str(&format!(
                    "{},{},{},{},{},{},{}\n",
                    row.encoder_index,
                    row.left_dispatch_count,
                    row.right_dispatch_count,
                    row.dispatch_count_delta,
                    row.left_total_us,
                    row.right_total_us,
                    row.total_delta_us
                ));
            }
        }
        "pipeline" => {
            out.push_str("pipeline_id,function,dispatch_count_left,dispatch_count_right,dispatch_count_delta,left_total_us,right_total_us,total_delta_us\n");
            for row in profile.pipeline_deltas.iter().take(limit) {
                out.push_str(&format!(
                    "{},{},{},{},{},{},{},{}\n",
                    option_csv(row.pipeline_id),
                    csv_string(&row.function_name),
                    row.left_dispatch_count,
                    row.right_dispatch_count,
                    row.dispatch_count_delta,
                    row.left_total_us,
                    row.right_total_us,
                    row.total_delta_us
                ));
            }
        }
        "timeline-windows" => {
            out.push_str("encoder_index,left_start,left_end,right_start,right_end,match_count,total_delta_us,max_abs_delta_us\n");
            for row in profile.timeline_spike_windows.iter().take(limit) {
                out.push_str(&format!(
                    "{},{},{},{},{},{},{},{}\n",
                    row.encoder_index,
                    row.left_start_source_index,
                    row.left_end_source_index,
                    row.right_start_source_index,
                    row.right_end_source_index,
                    row.match_count,
                    row.total_delta_us,
                    row.max_abs_delta_us
                ));
            }
        }
        "dispatch" | "matches" | "occurrences" => {
            out.push_str("left_index,right_index,encoder_index,left_pipeline_id,right_pipeline_id,function,left_us,right_us,delta_us,match_method,confidence\n");
            let rows = if view == "dispatch" {
                &profile.top_dispatch_outliers
            } else {
                &profile.matched_pairs
            };
            for row in rows.iter().take(limit) {
                out.push_str(&format!(
                    "{},{},{},{},{},{},{},{},{},{},{:.3}\n",
                    row.left_source_index,
                    row.right_source_index,
                    row.encoder_index,
                    option_csv(row.left_pipeline_id),
                    option_csv(row.right_pipeline_id),
                    csv_string(&row.function_name),
                    row.left_duration_us,
                    row.right_duration_us,
                    row.delta_us,
                    csv_string(&row.match_method),
                    row.confidence
                ));
            }
        }
        "unmatched" => {
            out.push_str(
                "trace,source_index,encoder_index,pipeline_id,function,kernel_id,duration_us\n",
            );
            for row in profile.unmatched.iter().take(limit) {
                out.push_str(&format!(
                    "{},{},{},{},{},{},{}\n",
                    csv_string(&row.trace),
                    row.source_index,
                    row.encoder_index,
                    option_csv(row.pipeline_id),
                    csv_string(&row.function_name),
                    csv_string(&row.kernel_id),
                    row.duration_us
                ));
            }
        }
        other => {
            return Err(Error::InvalidInput(format!(
                "invalid diff csv --by view: {other}"
            )));
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct ProfileTextOptions<'a> {
    pub by: Option<&'a str>,
    pub show_matches: bool,
    pub show_unmatched: bool,
    pub show_occurrences: bool,
    pub explain: bool,
    pub quick: bool,
    pub by_encoder: bool,
    pub limit: usize,
}

pub fn format_profile_text(
    report: &DiffReport,
    options: &ProfileTextOptions<'_>,
) -> Result<String> {
    let Some(profile) = &report.profile_diff else {
        let mut out = String::new();
        out.push_str("Trace Diff\n");
        for summary in &report.summary {
            writeln!(&mut out, "- {summary}").expect("writing to string cannot fail");
        }
        return Ok(out);
    };

    let limit = options.limit.max(1);
    let mut out = String::new();
    if options.quick {
        push_profile_quick_text(&mut out, profile, limit.min(10));
        if options.by_encoder {
            out.push('\n');
            push_profile_encoder_focus_text(&mut out, profile, limit);
        }
        return Ok(out);
    }
    if options.by_encoder {
        push_profile_encoder_focus_text(&mut out, profile, limit);
        return Ok(out);
    }

    let by = options.by.unwrap_or_default().trim();
    if by.is_empty() {
        push_profile_overview_text(&mut out, profile, options.explain);
        push_profile_function_text(&mut out, profile, limit);
        push_profile_dispatch_outliers_text(&mut out, profile, limit);
        if options.show_occurrences {
            push_profile_matches_text(&mut out, profile, limit, "Function Occurrences");
        }
        if options.show_matches {
            push_profile_matches_text(&mut out, profile, limit, "Matched Dispatches");
        }
        if options.show_unmatched {
            push_profile_unmatched_text(&mut out, profile, limit);
        }
        return Ok(out);
    }

    match by {
        "function" => push_profile_function_text(&mut out, profile, limit),
        "encoder" => push_profile_encoder_text(&mut out, profile, limit),
        "pipeline" => push_profile_pipeline_text(&mut out, profile, limit),
        "timeline-windows" => push_profile_spike_windows_text(&mut out, profile, limit),
        "dispatch" => push_profile_dispatch_outliers_text(&mut out, profile, limit),
        "matches" | "occurrences" => {
            push_profile_matches_text(&mut out, profile, limit, "Matched Dispatches")
        }
        "unmatched" => push_profile_unmatched_text(&mut out, profile, limit),
        other => {
            return Err(Error::InvalidInput(format!(
                "invalid diff text --by view: {other}"
            )));
        }
    }
    Ok(out)
}

fn push_profile_overview_text(out: &mut String, profile: &ProfileDiffReport, explain: bool) {
    writeln!(out, "Trace Diff").expect("writing to string cannot fail");
    writeln!(out, "Trace left: {}", profile.left_path).expect("writing to string cannot fail");
    writeln!(out, "Trace right: {}", profile.right_path).expect("writing to string cannot fail");
    writeln!(
        out,
        "Total GPU delta (left-right): {:+} us (left={} us right={} us)",
        profile.summary.total_delta_us,
        profile.summary.left_total_gpu_time_us,
        profile.summary.right_total_gpu_time_us
    )
    .expect("writing to string cannot fail");
    writeln!(
        out,
        "Matched delta: {:+} us",
        profile.summary.matched_delta_us
    )
    .expect("writing to string cannot fail");
    writeln!(
        out,
        "Structural/unmatched delta: {:+} us",
        profile.summary.unmatched_delta_us
    )
    .expect("writing to string cannot fail");
    writeln!(
        out,
        "Dispatch delta: {:+} (left={} right={})",
        profile.summary.dispatch_count_delta,
        profile.summary.left_dispatch_count,
        profile.summary.right_dispatch_count
    )
    .expect("writing to string cannot fail");
    writeln!(out, "Likely cause: {}", profile.summary.likely_cause)
        .expect("writing to string cannot fail");
    if explain {
        writeln!(
            out,
            "Interpretation: compare matched delta for common work against structural/unmatched delta to separate per-dispatch slowdown from command stream changes."
        )
        .expect("writing to string cannot fail");
    }
    for warning in &profile.warnings {
        writeln!(out, "Warning: {warning}").expect("writing to string cannot fail");
    }
}

fn push_profile_quick_text(out: &mut String, profile: &ProfileDiffReport, limit: usize) {
    writeln!(out, "Quick Triage").expect("writing to string cannot fail");
    writeln!(out, "Trace left: {}", profile.left_path).expect("writing to string cannot fail");
    writeln!(out, "Trace right: {}", profile.right_path).expect("writing to string cannot fail");
    writeln!(
        out,
        "Total GPU delta (matched common work): {:+} us",
        profile.summary.matched_delta_us
    )
    .expect("writing to string cannot fail");
    writeln!(
        out,
        "Total GPU delta (all dispatches): {:+} us (left={} us right={} us)",
        profile.summary.total_delta_us,
        profile.summary.left_total_gpu_time_us,
        profile.summary.right_total_gpu_time_us
    )
    .expect("writing to string cannot fail");
    writeln!(
        out,
        "Structural/unmatched delta: {:+} us",
        profile.summary.unmatched_delta_us
    )
    .expect("writing to string cannot fail");
    writeln!(
        out,
        "Dispatch delta (left-right): {:+}",
        profile.summary.dispatch_count_delta
    )
    .expect("writing to string cannot fail");
    push_profile_function_text(out, profile, limit);
    push_profile_dispatch_outliers_text(out, profile, limit);
    push_profile_unnamed_text(out, profile, limit);
    push_profile_spike_windows_text(out, profile, limit);
}

fn push_profile_function_text(out: &mut String, profile: &ProfileDiffReport, limit: usize) {
    writeln!(out, "\nTop Function Deltas").expect("writing to string cannot fail");
    writeln!(
        out,
        "{:<52} {:>8} {:>8} {:>10}",
        "function", "count_l", "count_r", "delta_us"
    )
    .expect("writing to string cannot fail");
    for row in profile.top_function_deltas.iter().take(limit) {
        writeln!(
            out,
            "{:<52} {:>8} {:>8} {:+>10}",
            truncate_text(&row.function_name, 52),
            row.left_dispatch_count,
            row.right_dispatch_count,
            row.total_delta_us
        )
        .expect("writing to string cannot fail");
    }
}

fn push_profile_encoder_text(out: &mut String, profile: &ProfileDiffReport, limit: usize) {
    writeln!(out, "\nEncoder Deltas").expect("writing to string cannot fail");
    writeln!(
        out,
        "{:<8} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10}",
        "encoder", "count_l", "count_r", "delta", "left_us", "right_us", "delta_us"
    )
    .expect("writing to string cannot fail");
    for row in profile.encoder_deltas.iter().take(limit) {
        writeln!(
            out,
            "{:<8} {:>8} {:>8} {:+>8} {:>10} {:>10} {:+>10}",
            row.encoder_index,
            row.left_dispatch_count,
            row.right_dispatch_count,
            row.dispatch_count_delta,
            row.left_total_us,
            row.right_total_us,
            row.total_delta_us
        )
        .expect("writing to string cannot fail");
    }
}

fn push_profile_encoder_focus_text(out: &mut String, profile: &ProfileDiffReport, limit: usize) {
    writeln!(out, "Encoder Focus").expect("writing to string cannot fail");
    writeln!(
        out,
        "{:<8} {:>8} {:>8} {:>8} {:>13} {:>10} {:>10}",
        "encoder", "count_l", "count_r", "matched", "matched_delta", "unmatched", "unmatched"
    )
    .expect("writing to string cannot fail");
    let total_abs: i64 = profile
        .encoder_reports
        .iter()
        .map(|row| row.matched_delta_us.abs())
        .sum();
    for row in profile.encoder_reports.iter().take(limit) {
        writeln!(
            out,
            "{:<8} {:>8} {:>8} {:>8} {:+>13} {:>10} {:+>10}",
            row.encoder_index,
            row.left_dispatch_count,
            row.right_dispatch_count,
            row.matched_count,
            row.matched_delta_us,
            row.unmatched_count,
            row.unmatched_delta_us
        )
        .expect("writing to string cannot fail");
        for (index, pair) in row.top_dispatches.iter().take(3).enumerate() {
            writeln!(
                out,
                "  top {:<2} left={:<6} right={:<6} pipe={:<8} fn={:<28} delta={:+7}",
                index + 1,
                pair.left_source_index,
                pair.right_source_index,
                option_csv(pair.left_pipeline_id),
                truncate_text(&pair.function_name, 28),
                pair.delta_us
            )
            .expect("writing to string cannot fail");
        }
    }
    if let Some(top) = profile.encoder_reports.first() {
        let share = if total_abs > 0 {
            top.matched_delta_us.abs() as f64 * 100.0 / total_abs as f64
        } else {
            0.0
        };
        let dominance = if share >= 60.0 {
            "dominates"
        } else {
            "does not dominate"
        };
        writeln!(
            out,
            "\nDominant encoder: {} ({:+} us matched, {:.1}% of matched encoder delta) -> {}",
            top.encoder_index, top.matched_delta_us, share, dominance
        )
        .expect("writing to string cannot fail");
    }
}

fn push_profile_pipeline_text(out: &mut String, profile: &ProfileDiffReport, limit: usize) {
    writeln!(out, "\nPipeline Deltas").expect("writing to string cannot fail");
    writeln!(
        out,
        "{:<10} {:<44} {:>8} {:>8} {:>10}",
        "pipeline", "function", "count_l", "count_r", "delta_us"
    )
    .expect("writing to string cannot fail");
    for row in profile.pipeline_deltas.iter().take(limit) {
        writeln!(
            out,
            "{:<10} {:<44} {:>8} {:>8} {:+>10}",
            option_csv(row.pipeline_id),
            truncate_text(&row.function_name, 44),
            row.left_dispatch_count,
            row.right_dispatch_count,
            row.total_delta_us
        )
        .expect("writing to string cannot fail");
    }
}

fn push_profile_dispatch_outliers_text(
    out: &mut String,
    profile: &ProfileDiffReport,
    limit: usize,
) {
    writeln!(out, "\nTop Dispatch Outliers").expect("writing to string cannot fail");
    writeln!(
        out,
        "{:<7} {:<7} {:<7} {:<8} {:<8} {:<40} {:>8} {:>8} {:>9}",
        "left", "right", "enc", "pipe_l", "pipe_r", "function", "left_us", "right_us", "delta"
    )
    .expect("writing to string cannot fail");
    for row in profile.top_dispatch_outliers.iter().take(limit) {
        writeln!(
            out,
            "{:<7} {:<7} {:<7} {:<8} {:<8} {:<40} {:>8} {:>8} {:+>9}",
            row.left_source_index,
            row.right_source_index,
            row.encoder_index,
            option_csv(row.left_pipeline_id),
            option_csv(row.right_pipeline_id),
            truncate_text(&row.function_name, 40),
            row.left_duration_us,
            row.right_duration_us,
            row.delta_us
        )
        .expect("writing to string cannot fail");
    }
}

fn push_profile_matches_text(
    out: &mut String,
    profile: &ProfileDiffReport,
    limit: usize,
    title: &str,
) {
    writeln!(out, "\n{title}").expect("writing to string cannot fail");
    writeln!(
        out,
        "{:<7} {:<7} {:<7} {:<40} {:>8} {:>8} {:>9} {:<18} {:>7}",
        "left", "right", "enc", "function", "left_us", "right_us", "delta", "method", "conf"
    )
    .expect("writing to string cannot fail");
    for row in profile.matched_pairs.iter().take(limit) {
        writeln!(
            out,
            "{:<7} {:<7} {:<7} {:<40} {:>8} {:>8} {:+>9} {:<18} {:>7.3}",
            row.left_source_index,
            row.right_source_index,
            row.encoder_index,
            truncate_text(&row.function_name, 40),
            row.left_duration_us,
            row.right_duration_us,
            row.delta_us,
            truncate_text(&row.match_method, 18),
            row.confidence
        )
        .expect("writing to string cannot fail");
    }
}

fn push_profile_unmatched_text(out: &mut String, profile: &ProfileDiffReport, limit: usize) {
    writeln!(out, "\nUnmatched Dispatches").expect("writing to string cannot fail");
    writeln!(
        out,
        "{:<6} {:<8} {:<7} {:<10} {:<40} {:>10}",
        "trace", "index", "enc", "pipeline", "function", "duration"
    )
    .expect("writing to string cannot fail");
    for row in profile.unmatched.iter().take(limit) {
        writeln!(
            out,
            "{:<6} {:<8} {:<7} {:<10} {:<40} {:>10}",
            row.trace,
            row.source_index,
            row.encoder_index,
            option_csv(row.pipeline_id),
            truncate_text(&row.function_name, 40),
            row.duration_us
        )
        .expect("writing to string cannot fail");
    }
}

fn push_profile_unnamed_text(out: &mut String, profile: &ProfileDiffReport, limit: usize) {
    writeln!(out, "\nUnnamed Dispatch Summary").expect("writing to string cannot fail");
    writeln!(
        out,
        "{:<10} {:<24} {:>8} {:>8} {:>10} {:>10} {:>10}",
        "pipeline", "kernel_id", "count_l", "count_r", "left_us", "right_us", "delta"
    )
    .expect("writing to string cannot fail");
    for row in profile.unnamed_dispatch_deltas.iter().take(limit) {
        writeln!(
            out,
            "{:<10} {:<24} {:>8} {:>8} {:>10} {:>10} {:+>10}",
            option_csv(row.pipeline_id),
            truncate_text(&row.kernel_id, 24),
            row.left_dispatch_count,
            row.right_dispatch_count,
            row.left_total_us,
            row.right_total_us,
            row.total_delta_us
        )
        .expect("writing to string cannot fail");
    }
}

fn push_profile_spike_windows_text(out: &mut String, profile: &ProfileDiffReport, limit: usize) {
    writeln!(out, "\nSpike Windows").expect("writing to string cannot fail");
    writeln!(
        out,
        "{:<7} {:<10} {:<10} {:<10} {:<10} {:<8} {:>10}",
        "enc", "left_start", "left_end", "right_st", "right_end", "matches", "cum_delta"
    )
    .expect("writing to string cannot fail");
    for row in profile.timeline_spike_windows.iter().take(limit) {
        writeln!(
            out,
            "{:<7} {:<10} {:<10} {:<10} {:<10} {:<8} {:+>10}",
            row.encoder_index,
            row.left_start_source_index,
            row.left_end_source_index,
            row.right_start_source_index,
            row.right_end_source_index,
            row.match_count,
            row.total_delta_us
        )
        .expect("writing to string cannot fail");
    }
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        truncated
    } else {
        value.to_owned()
    }
}

pub fn diff_with_options(
    left: &TraceBundle,
    right: &TraceBundle,
    options: &DiffOptions,
) -> DiffReport {
    let left_report = analyze(left);
    let right_report = analyze(right);
    let buffer_changes = diff_buffer_stats(&left_report, &right_report);
    let buffer_lifecycle_changes = diff_buffer_lifecycles(&left_report, &right_report);
    let kernel_changes = diff_kernel_stats(&left_report, &right_report);
    let kernel_timing_changes = diff_kernel_timing_stats(&left_report, &right_report);
    let counter_metric_changes = diff_counter_metrics(left, right);
    let profile_diff = diff_profile(left, right, &options.profile);
    let mut summary = Vec::new();
    let added_buffers = buffer_changes
        .iter()
        .filter(|change| change.status == BufferChangeStatus::Added)
        .count();
    let removed_buffers = buffer_changes
        .iter()
        .filter(|change| change.status == BufferChangeStatus::Removed)
        .count();

    if left_report.trace.capture_len != right_report.trace.capture_len {
        summary.push(format!(
            "Capture size changed: {} -> {} bytes",
            left_report.trace.capture_len, right_report.trace.capture_len
        ));
    }
    if left_report.trace.device_resource_count != right_report.trace.device_resource_count {
        summary.push(format!(
            "Device resource count changed: {} -> {}",
            left_report.trace.device_resource_count, right_report.trace.device_resource_count
        ));
    }
    if left_report.trace.device_id != right_report.trace.device_id {
        summary.push(format!(
            "Device ID changed: {:?} -> {:?}",
            left_report.trace.device_id, right_report.trace.device_id
        ));
    }
    if left_report.command_buffer_count != right_report.command_buffer_count {
        summary.push(format!(
            "Command buffer count changed: {} -> {}",
            left_report.command_buffer_count, right_report.command_buffer_count
        ));
    }
    if left_report.command_buffer_region_count != right_report.command_buffer_region_count {
        summary.push(format!(
            "Command buffer region count changed: {} -> {}",
            left_report.command_buffer_region_count, right_report.command_buffer_region_count
        ));
    }
    if left_report.compute_encoder_count != right_report.compute_encoder_count {
        summary.push(format!(
            "Compute encoder count changed: {} -> {}",
            left_report.compute_encoder_count, right_report.compute_encoder_count
        ));
    }
    if left_report.dispatch_count != right_report.dispatch_count {
        summary.push(format!(
            "Dispatch count changed: {} -> {}",
            left_report.dispatch_count, right_report.dispatch_count
        ));
    }
    if left_report.total_duration_ns != right_report.total_duration_ns {
        summary.push(format!(
            "Total kernel time changed: {} -> {} ns",
            left_report.total_duration_ns, right_report.total_duration_ns
        ));
    }
    if left_report.buffer_count != right_report.buffer_count {
        summary.push(format!(
            "Buffer count changed: {} -> {}",
            left_report.buffer_count, right_report.buffer_count
        ));
    }
    if left_report.buffer_inventory_count != right_report.buffer_inventory_count {
        summary.push(format!(
            "Backing buffer file count changed: {} -> {}",
            left_report.buffer_inventory_count, right_report.buffer_inventory_count
        ));
    }
    if left_report.buffer_inventory_bytes != right_report.buffer_inventory_bytes {
        summary.push(format!(
            "Backing buffer bytes changed: {} -> {}",
            left_report.buffer_inventory_bytes, right_report.buffer_inventory_bytes
        ));
    }
    if left_report.buffer_inventory_aliases != right_report.buffer_inventory_aliases {
        summary.push(format!(
            "Backing buffer alias count changed: {} -> {}",
            left_report.buffer_inventory_aliases, right_report.buffer_inventory_aliases
        ));
    }
    if left_report.unused_resource_count != right_report.unused_resource_count {
        summary.push(format!(
            "Unused resource entries changed: {} -> {}",
            left_report.unused_resource_count, right_report.unused_resource_count
        ));
    }
    if left_report.unused_resource_bytes != right_report.unused_resource_bytes {
        summary.push(format!(
            "Unused logical bytes changed: {} -> {}",
            left_report.unused_resource_bytes, right_report.unused_resource_bytes
        ));
    }
    if left_report.shared_buffer_count != right_report.shared_buffer_count {
        summary.push(format!(
            "Shared buffer count changed: {} -> {}",
            left_report.shared_buffer_count, right_report.shared_buffer_count
        ));
    }
    if added_buffers > 0 || removed_buffers > 0 {
        summary.push(format!(
            "Buffer inventory changed: {added_buffers} added, {removed_buffers} removed"
        ));
    }
    if let Some(change) = buffer_changes.first() {
        summary.push(format!(
            "Largest buffer use delta: {} [{}] ({} -> {}, delta {:+})",
            change.name,
            match change.status {
                BufferChangeStatus::Added => "added",
                BufferChangeStatus::Removed => "removed",
                BufferChangeStatus::Changed => "changed",
            },
            change.left_uses,
            change.right_uses,
            change.delta
        ));
    }
    if let Some(change) = buffer_lifecycle_changes.first() {
        summary.push(format!(
            "Largest buffer lifetime delta: {} (command buffers {} -> {}, dispatches {} -> {})",
            change.name,
            change.left_command_buffer_span,
            change.right_command_buffer_span,
            change.left_dispatch_span,
            change.right_dispatch_span
        ));
    }
    if let Some(change) = kernel_changes.first() {
        summary.push(format!(
            "Largest kernel dispatch delta: {} ({} -> {}, delta {:+})",
            change.name, change.left_dispatches, change.right_dispatches, change.delta
        ));
    }
    if let Some(change) = kernel_timing_changes.first() {
        summary.push(format!(
            "Largest kernel timing delta: {} ({} -> {} ns, delta {:+} ns)",
            change.name,
            change.left_duration_ns,
            change.right_duration_ns,
            change.duration_delta_ns
        ));
    }
    if let Some(change) = counter_metric_changes.first() {
        summary.push(format!(
            "Largest profiler metric delta: {} (inv {} -> {}, exec {} -> {}, occ {} -> {}, alu {} -> {}, llc {} -> {}, dev_bw {} -> {}, gpu_r {} -> {}, gpu_w {} -> {}, buf_dev_r {} -> {}, buf_dev_w {} -> {}, dev_r {} -> {}, dev_w {} -> {}, l1_miss {} -> {}, l1_racc {} -> {}, l1_rbw {} -> {}, l1_wacc {} -> {}, l1_wbw {} -> {}, csl_util {} -> {}, cf_util {} -> {}, ithr_util {} -> {}, ic_util {} -> {}, icond_util {} -> {}, f32_util {} -> {})",
            change.name,
            format_option_f64(change.left_kernel_invocations),
            format_option_f64(change.right_kernel_invocations),
            format_option_f64(change.left_execution_cost_percent),
            format_option_f64(change.right_execution_cost_percent),
            format_option_f64(change.left_occupancy_percent),
            format_option_f64(change.right_occupancy_percent),
            format_option_f64(change.left_alu_utilization_percent),
            format_option_f64(change.right_alu_utilization_percent),
            format_option_f64(change.left_last_level_cache_percent),
            format_option_f64(change.right_last_level_cache_percent),
            format_option_f64(change.left_device_memory_bandwidth_gbps),
            format_option_f64(change.right_device_memory_bandwidth_gbps),
            format_option_f64(change.left_gpu_read_bandwidth_gbps),
            format_option_f64(change.right_gpu_read_bandwidth_gbps),
            format_option_f64(change.left_gpu_write_bandwidth_gbps),
            format_option_f64(change.right_gpu_write_bandwidth_gbps),
            format_option_f64(change.left_buffer_device_memory_bytes_read),
            format_option_f64(change.right_buffer_device_memory_bytes_read),
            format_option_f64(change.left_buffer_device_memory_bytes_written),
            format_option_f64(change.right_buffer_device_memory_bytes_written),
            format_option_f64(change.left_bytes_read_from_device_memory),
            format_option_f64(change.right_bytes_read_from_device_memory),
            format_option_f64(change.left_bytes_written_to_device_memory),
            format_option_f64(change.right_bytes_written_to_device_memory),
            format_option_f64(change.left_buffer_l1_miss_rate_percent),
            format_option_f64(change.right_buffer_l1_miss_rate_percent),
            format_option_f64(change.left_buffer_l1_read_accesses),
            format_option_f64(change.right_buffer_l1_read_accesses),
            format_option_f64(change.left_buffer_l1_read_bandwidth_gbps),
            format_option_f64(change.right_buffer_l1_read_bandwidth_gbps),
            format_option_f64(change.left_buffer_l1_write_accesses),
            format_option_f64(change.right_buffer_l1_write_accesses),
            format_option_f64(change.left_buffer_l1_write_bandwidth_gbps),
            format_option_f64(change.right_buffer_l1_write_bandwidth_gbps),
            format_option_f64(change.left_compute_shader_launch_utilization_percent),
            format_option_f64(change.right_compute_shader_launch_utilization_percent),
            format_option_f64(change.left_control_flow_utilization_percent),
            format_option_f64(change.right_control_flow_utilization_percent),
            format_option_f64(change.left_instruction_throughput_utilization_percent),
            format_option_f64(change.right_instruction_throughput_utilization_percent),
            format_option_f64(change.left_integer_complex_utilization_percent),
            format_option_f64(change.right_integer_complex_utilization_percent),
            format_option_f64(change.left_integer_conditional_utilization_percent),
            format_option_f64(change.right_integer_conditional_utilization_percent),
            format_option_f64(change.left_f32_utilization_percent),
            format_option_f64(change.right_f32_utilization_percent),
        ));
    }
    if let Some(profile) = &profile_diff {
        summary.push(format!(
            "Profile dispatch delta: {} -> {} ({:+}), GPU time {} -> {} us ({:+} us), matched delta {:+} us, unmatched delta {:+} us",
            profile.summary.left_dispatch_count,
            profile.summary.right_dispatch_count,
            profile.summary.dispatch_count_delta,
            profile.summary.left_total_gpu_time_us,
            profile.summary.right_total_gpu_time_us,
            profile.summary.total_delta_us,
            profile.summary.matched_delta_us,
            profile.summary.unmatched_delta_us
        ));
        summary.push(format!(
            "Likely profile cause: {}",
            profile.summary.likely_cause
        ));
    }
    if summary.is_empty() {
        summary.push("No high-level differences detected yet.".to_owned());
    }

    DiffReport {
        left: left_report,
        right: right_report,
        buffer_changes,
        buffer_lifecycle_changes,
        kernel_changes,
        kernel_timing_changes,
        counter_metric_changes,
        profile_diff,
        summary,
    }
}

fn diff_counter_metrics(left: &TraceBundle, right: &TraceBundle) -> Vec<CounterMetricChange> {
    let left_report = match counter_export::report(left) {
        Ok(report) => report,
        Err(_) => return Vec::new(),
    };
    let right_report = match counter_export::report(right) {
        Ok(report) => report,
        Err(_) => return Vec::new(),
    };

    let left_map = aggregate_counter_metrics(&left_report);
    let right_map = aggregate_counter_metrics(&right_report);
    let mut names = std::collections::BTreeSet::new();
    names.extend(left_map.keys().cloned());
    names.extend(right_map.keys().cloned());

    let mut changes = Vec::new();
    for name in names {
        let left_metrics = left_map.get(&name).copied().unwrap_or_default();
        let right_metrics = right_map.get(&name).copied().unwrap_or_default();
        if metrics_equal(left_metrics, right_metrics) {
            continue;
        }
        changes.push(CounterMetricChange {
            name,
            left_kernel_invocations: left_metrics.kernel_invocations,
            right_kernel_invocations: right_metrics.kernel_invocations,
            left_execution_cost_percent: left_metrics.execution_cost_percent,
            right_execution_cost_percent: right_metrics.execution_cost_percent,
            left_occupancy_percent: left_metrics.occupancy_percent,
            right_occupancy_percent: right_metrics.occupancy_percent,
            left_alu_utilization_percent: left_metrics.alu_utilization_percent,
            right_alu_utilization_percent: right_metrics.alu_utilization_percent,
            left_last_level_cache_percent: left_metrics.last_level_cache_percent,
            right_last_level_cache_percent: right_metrics.last_level_cache_percent,
            left_device_memory_bandwidth_gbps: left_metrics.device_memory_bandwidth_gbps,
            right_device_memory_bandwidth_gbps: right_metrics.device_memory_bandwidth_gbps,
            left_gpu_read_bandwidth_gbps: left_metrics.gpu_read_bandwidth_gbps,
            right_gpu_read_bandwidth_gbps: right_metrics.gpu_read_bandwidth_gbps,
            left_gpu_write_bandwidth_gbps: left_metrics.gpu_write_bandwidth_gbps,
            right_gpu_write_bandwidth_gbps: right_metrics.gpu_write_bandwidth_gbps,
            left_buffer_device_memory_bytes_read: left_metrics.buffer_device_memory_bytes_read,
            right_buffer_device_memory_bytes_read: right_metrics.buffer_device_memory_bytes_read,
            left_buffer_device_memory_bytes_written: left_metrics
                .buffer_device_memory_bytes_written,
            right_buffer_device_memory_bytes_written: right_metrics
                .buffer_device_memory_bytes_written,
            left_bytes_read_from_device_memory: left_metrics.bytes_read_from_device_memory,
            right_bytes_read_from_device_memory: right_metrics.bytes_read_from_device_memory,
            left_bytes_written_to_device_memory: left_metrics.bytes_written_to_device_memory,
            right_bytes_written_to_device_memory: right_metrics.bytes_written_to_device_memory,
            left_buffer_l1_miss_rate_percent: left_metrics.buffer_l1_miss_rate_percent,
            right_buffer_l1_miss_rate_percent: right_metrics.buffer_l1_miss_rate_percent,
            left_buffer_l1_read_accesses: left_metrics.buffer_l1_read_accesses,
            right_buffer_l1_read_accesses: right_metrics.buffer_l1_read_accesses,
            left_buffer_l1_read_bandwidth_gbps: left_metrics.buffer_l1_read_bandwidth_gbps,
            right_buffer_l1_read_bandwidth_gbps: right_metrics.buffer_l1_read_bandwidth_gbps,
            left_buffer_l1_write_accesses: left_metrics.buffer_l1_write_accesses,
            right_buffer_l1_write_accesses: right_metrics.buffer_l1_write_accesses,
            left_buffer_l1_write_bandwidth_gbps: left_metrics.buffer_l1_write_bandwidth_gbps,
            right_buffer_l1_write_bandwidth_gbps: right_metrics.buffer_l1_write_bandwidth_gbps,
            left_compute_shader_launch_utilization_percent: left_metrics
                .compute_shader_launch_utilization_percent,
            right_compute_shader_launch_utilization_percent: right_metrics
                .compute_shader_launch_utilization_percent,
            left_control_flow_utilization_percent: left_metrics.control_flow_utilization_percent,
            right_control_flow_utilization_percent: right_metrics.control_flow_utilization_percent,
            left_instruction_throughput_utilization_percent: left_metrics
                .instruction_throughput_utilization_percent,
            right_instruction_throughput_utilization_percent: right_metrics
                .instruction_throughput_utilization_percent,
            left_integer_complex_utilization_percent: left_metrics
                .integer_complex_utilization_percent,
            right_integer_complex_utilization_percent: right_metrics
                .integer_complex_utilization_percent,
            left_integer_conditional_utilization_percent: left_metrics
                .integer_conditional_utilization_percent,
            right_integer_conditional_utilization_percent: right_metrics
                .integer_conditional_utilization_percent,
            left_f32_utilization_percent: left_metrics.f32_utilization_percent,
            right_f32_utilization_percent: right_metrics.f32_utilization_percent,
        });
    }
    changes.sort_by(|left, right| {
        aggregate_change_magnitude(right)
            .partial_cmp(&aggregate_change_magnitude(left))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.name.cmp(&right.name))
    });
    changes
}

#[derive(Debug, Clone)]
struct ProfileTraceData {
    path: String,
    label: String,
    dispatches: Vec<ProfileDispatch>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct ProfileDispatch {
    source_index: usize,
    function_name: String,
    function_key: String,
    kernel_id: String,
    pipeline_id: Option<i64>,
    encoder_index: usize,
    duration_us: u64,
}

fn diff_profile(
    left: &TraceBundle,
    right: &TraceBundle,
    options: &ProfileDiffOptions,
) -> Option<ProfileDiffReport> {
    diff_profile_paths(&left.path, &right.path, options)
}

fn diff_profile_paths(
    left: &Path,
    right: &Path,
    options: &ProfileDiffOptions,
) -> Option<ProfileDiffReport> {
    let prefer_sidecar = !has_stream_data(left) || !has_stream_data(right);
    let left_trace = load_profile_trace_from_path(left, options, prefer_sidecar);
    let right_trace = load_profile_trace_from_path(right, options, prefer_sidecar);
    if left_trace.dispatches.is_empty() && right_trace.dispatches.is_empty() {
        return None;
    }

    let (matches, unmatched_left, unmatched_right) =
        align_profile_dispatches(&left_trace.dispatches, &right_trace.dispatches);
    let mut warnings = left_trace.warnings.clone();
    warnings.extend(right_trace.warnings.clone());

    let left_total = total_profile_duration(&left_trace.dispatches);
    let right_total = total_profile_duration(&right_trace.dispatches);
    let matched_delta = matches.iter().map(|pair| pair.delta_us).sum::<i64>();
    let unmatched_left_total = unmatched_left
        .iter()
        .map(|index| left_trace.dispatches[*index].duration_us)
        .sum::<u64>();
    let unmatched_right_total = unmatched_right
        .iter()
        .map(|index| right_trace.dispatches[*index].duration_us)
        .sum::<u64>();
    let unmatched_delta = unmatched_left_total as i64 - unmatched_right_total as i64;

    let mut unmatched = unmatched_left
        .iter()
        .map(|index| unmatched_profile_dispatch("left", &left_trace.dispatches[*index]))
        .collect::<Vec<_>>();
    unmatched.extend(
        unmatched_right
            .iter()
            .map(|index| unmatched_profile_dispatch("right", &right_trace.dispatches[*index])),
    );

    let mut report = ProfileDiffReport {
        schema_version: "gputrace.diff.profile.v1".to_owned(),
        left_path: left_trace.path.clone(),
        right_path: right_trace.path.clone(),
        summary: ProfileDiffSummary {
            left_label: left_trace.label.clone(),
            right_label: right_trace.label.clone(),
            left_dispatch_count: left_trace.dispatches.len(),
            right_dispatch_count: right_trace.dispatches.len(),
            dispatch_count_delta: left_trace.dispatches.len() as isize
                - right_trace.dispatches.len() as isize,
            left_total_gpu_time_us: left_total,
            right_total_gpu_time_us: right_total,
            total_delta_us: left_total as i64 - right_total as i64,
            matched_delta_us: matched_delta,
            unmatched_delta_us: unmatched_delta,
            likely_cause: infer_profile_likely_cause(
                left_total as i64 - right_total as i64,
                matched_delta,
                unmatched_delta,
            ),
        },
        top_function_deltas: build_profile_function_deltas(
            &left_trace.dispatches,
            &right_trace.dispatches,
            &matches,
        ),
        top_dispatch_outliers: top_profile_outliers(&matches, options),
        encoder_deltas: build_profile_encoder_deltas(
            &left_trace.dispatches,
            &right_trace.dispatches,
        ),
        encoder_reports: build_profile_encoder_reports(
            &left_trace.dispatches,
            &right_trace.dispatches,
            &matches,
            &unmatched_left,
            &unmatched_right,
            options.limit,
        ),
        pipeline_deltas: build_profile_pipeline_deltas(
            &left_trace.dispatches,
            &right_trace.dispatches,
        ),
        unnamed_dispatch_deltas: build_profile_unnamed_deltas(
            &left_trace.dispatches,
            &right_trace.dispatches,
            &matches,
        ),
        timeline_spike_windows: build_profile_spike_windows(&matches, options),
        matched_pairs: matches,
        unmatched,
        warnings,
    };

    let limit = options.limit.max(1);
    truncate_profile_report(&mut report, limit);
    Some(report)
}

fn load_profile_trace_from_path(
    path: &Path,
    options: &ProfileDiffOptions,
    prefer_sidecar: bool,
) -> ProfileTraceData {
    let path_display = path.display().to_string();
    let label = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(path_display.as_str())
        .to_owned();
    let mut warnings = Vec::new();
    if prefer_sidecar
        && let Some(mut trace) = load_profile_trace_from_counter_sidecar(path, options, &label)
    {
        trace.warnings.push(
            "using adjacent export-counters JSON because at least one diff input lacks streamData"
                .to_owned(),
        );
        return trace;
    }
    let summary = match profiler::stream_data_summary(path) {
        Ok(summary) => summary,
        Err(error) => {
            if let Some(mut trace) = load_profile_trace_from_counter_sidecar(path, options, &label)
            {
                trace.warnings.push(format!(
                    "profile streamData unavailable for {label}: {error}; using adjacent export-counters JSON"
                ));
                return trace;
            }
            warnings.push(format!(
                "profile streamData unavailable for {label}: {error}"
            ));
            return ProfileTraceData {
                path: path_display,
                label,
                dispatches: Vec::new(),
                warnings,
            };
        }
    };

    let only_function = options
        .only_function
        .as_ref()
        .map(|value| value.to_ascii_lowercase());
    let mut dispatches = summary
        .dispatches
        .iter()
        .filter(|dispatch| {
            options
                .only_encoder
                .is_none_or(|encoder| dispatch.encoder_index == encoder)
        })
        .filter(|dispatch| {
            only_function.as_ref().is_none_or(|needle| {
                dispatch
                    .function_name
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains(needle)
            })
        })
        .map(|dispatch| {
            let function_name = dispatch.function_name.clone().unwrap_or_default();
            let function_key = profile_function_key(&function_name, dispatch);
            ProfileDispatch {
                source_index: dispatch.index,
                function_name,
                kernel_id: function_key.clone(),
                function_key,
                pipeline_id: dispatch.pipeline_id,
                encoder_index: dispatch.encoder_index,
                duration_us: dispatch.duration_us,
            }
        })
        .collect::<Vec<_>>();
    dispatches.sort_by_key(|dispatch| dispatch.source_index);

    if dispatches.is_empty() {
        warnings.push(format!("no profile dispatches after filtering for {label}"));
    }

    ProfileTraceData {
        path: path_display,
        label,
        dispatches,
        warnings,
    }
}

fn has_stream_data(path: &Path) -> bool {
    profiler::find_profiler_directory(path).is_some_and(|dir| dir.join("streamData").is_file())
}

#[derive(Debug, Deserialize)]
struct CounterSidecarReport {
    rows: Vec<CounterSidecarRow>,
}

#[derive(Debug, Deserialize)]
struct CounterSidecarRow {
    row_index: usize,
    encoder_index: usize,
    encoder_label: String,
    kernel_name: Option<String>,
    pipeline_addr: Option<u64>,
    duration_ns: u64,
    dispatch_count: usize,
    metric_source: String,
}

fn load_profile_trace_from_counter_sidecar(
    path: &Path,
    options: &ProfileDiffOptions,
    label: &str,
) -> Option<ProfileTraceData> {
    let sidecar = counter_sidecar_path(path)?;
    let data = std::fs::read(&sidecar).ok()?;
    let report: CounterSidecarReport = serde_json::from_slice(&data).ok()?;
    let only_function = options
        .only_function
        .as_ref()
        .map(|value| value.to_ascii_lowercase());
    let mut dispatches = Vec::new();
    for row in report.rows {
        if row.metric_source != "profile-dispatch-time" {
            continue;
        }
        if options
            .only_encoder
            .is_some_and(|encoder| row.encoder_index != encoder)
        {
            continue;
        }
        let function_name = row
            .kernel_name
            .clone()
            .filter(|name| !name.is_empty())
            .unwrap_or(row.encoder_label.clone());
        if only_function
            .as_ref()
            .is_some_and(|needle| !function_name.to_ascii_lowercase().contains(needle.as_str()))
        {
            continue;
        }

        let dispatch_count = row.dispatch_count.max(1);
        let total_us = row.duration_ns / 1_000;
        let per_dispatch_us = (total_us / dispatch_count as u64).max(1);
        for occurrence in 0..dispatch_count {
            let function_key = normalize_profile_name(&function_name);
            dispatches.push(ProfileDispatch {
                source_index: row
                    .row_index
                    .saturating_mul(1_000_000)
                    .saturating_add(occurrence),
                function_name: function_name.clone(),
                kernel_id: function_key.clone(),
                function_key,
                pipeline_id: row.pipeline_addr.map(|value| value as i64),
                encoder_index: row.encoder_index,
                duration_us: per_dispatch_us,
            });
        }
    }
    if dispatches.is_empty() {
        return None;
    }
    Some(ProfileTraceData {
        path: path.display().to_string(),
        label: label.to_owned(),
        dispatches,
        warnings: vec![format!(
            "loaded profile rows from sidecar {}",
            sidecar.display()
        )],
    })
}

fn counter_sidecar_path(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?.to_str()?;
    let stem = name
        .strip_suffix("-perfdata.gputrace")
        .or_else(|| name.strip_suffix(".gputrace"))?;
    let candidate = path.with_file_name(format!("{stem}-counters.json"));
    candidate.is_file().then_some(candidate)
}

fn profile_function_key(function_name: &str, dispatch: &profiler::ProfilerDispatch) -> String {
    let normalized = normalize_profile_name(function_name);
    if normalized.is_empty() {
        format!(
            "pipeline:{}:{}",
            dispatch
                .pipeline_id
                .unwrap_or(dispatch.pipeline_index as i64),
            dispatch.encoder_index
        )
    } else {
        normalized
    }
}

fn normalize_profile_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn align_profile_dispatches(
    left: &[ProfileDispatch],
    right: &[ProfileDispatch],
) -> (Vec<ProfileMatchPair>, Vec<usize>, Vec<usize>) {
    let mut right_by_key = BTreeMap::<String, Vec<usize>>::new();
    for (index, dispatch) in right.iter().enumerate() {
        right_by_key
            .entry(dispatch.function_key.clone())
            .or_default()
            .push(index);
    }

    let mut next_by_key = BTreeMap::<String, usize>::new();
    let mut matched_left = vec![false; left.len()];
    let mut matched_right = vec![false; right.len()];
    let mut matches = Vec::new();
    let mut last_right = None;

    for (left_index, left_dispatch) in left.iter().enumerate() {
        let Some(candidates) = right_by_key.get(&left_dispatch.function_key) else {
            continue;
        };
        let next = next_by_key
            .entry(left_dispatch.function_key.clone())
            .or_default();
        while *next < candidates.len() && last_right.is_some_and(|last| candidates[*next] <= last) {
            *next += 1;
        }
        if *next >= candidates.len() {
            continue;
        }
        let right_index = candidates[*next];
        *next += 1;
        last_right = Some(right_index);
        matched_left[left_index] = true;
        matched_right[right_index] = true;
        matches.push(profile_match_pair(
            left_dispatch,
            &right[right_index],
            "function_occurrence",
            0.95,
        ));
    }

    let mut anchors = Vec::with_capacity(matches.len() + 2);
    anchors.push((-1isize, -1isize));
    for pair in &matches {
        let left_index = left
            .iter()
            .position(|dispatch| dispatch.source_index == pair.left_source_index);
        let right_index = right
            .iter()
            .position(|dispatch| dispatch.source_index == pair.right_source_index);
        if let (Some(left_index), Some(right_index)) = (left_index, right_index) {
            anchors.push((left_index as isize, right_index as isize));
        }
    }
    anchors.sort_by_key(|(left_index, right_index)| (*left_index, *right_index));
    anchors.push((left.len() as isize, right.len() as isize));

    for window in anchors.windows(2) {
        let (prev_left, prev_right) = window[0];
        let (next_left, next_right) = window[1];
        let left_start = if prev_left < 0 {
            0
        } else {
            prev_left as usize + 1
        };
        let right_start = if prev_right < 0 {
            0
        } else {
            prev_right as usize + 1
        };
        let unmatched_left_region =
            collect_unmatched_range(&matched_left, left_start, next_left as usize);
        let unmatched_right_region =
            collect_unmatched_range(&matched_right, right_start, next_right as usize);
        if unmatched_left_region.is_empty() || unmatched_right_region.is_empty() {
            continue;
        }
        for (left_index, right_index) in
            align_profile_region(left, right, &unmatched_left_region, &unmatched_right_region)
        {
            if matched_left[left_index] || matched_right[right_index] {
                continue;
            }
            matched_left[left_index] = true;
            matched_right[right_index] = true;
            let left_dispatch = &left[left_index];
            let right_dispatch = &right[right_index];
            let (method, confidence) = if left_dispatch.function_key == right_dispatch.function_key
            {
                ("sequence_alignment", 0.78)
            } else if left_dispatch.pipeline_id.is_some()
                && left_dispatch.pipeline_id == right_dispatch.pipeline_id
            {
                ("sequence_pipeline", 0.60)
            } else {
                ("sequence_alignment", 0.72)
            };
            matches.push(profile_match_pair(
                left_dispatch,
                right_dispatch,
                method,
                confidence,
            ));
        }
    }
    matches.sort_by_key(|pair| (pair.left_source_index, pair.right_source_index));

    let unmatched_left = matched_left
        .iter()
        .enumerate()
        .filter_map(|(index, matched)| (!matched).then_some(index))
        .collect::<Vec<_>>();
    let unmatched_right = matched_right
        .iter()
        .enumerate()
        .filter_map(|(index, matched)| (!matched).then_some(index))
        .collect::<Vec<_>>();
    (matches, unmatched_left, unmatched_right)
}

fn collect_unmatched_range(matched: &[bool], start: usize, end: usize) -> Vec<usize> {
    let end = end.min(matched.len());
    if start >= end {
        return Vec::new();
    }
    matched[start..end]
        .iter()
        .enumerate()
        .filter_map(|(offset, matched)| (!matched).then_some(start + offset))
        .collect()
}

fn align_profile_region(
    left: &[ProfileDispatch],
    right: &[ProfileDispatch],
    left_indices: &[usize],
    right_indices: &[usize],
) -> Vec<(usize, usize)> {
    const DP_CELL_LIMIT: usize = 120_000;
    if left_indices.len() * right_indices.len() > DP_CELL_LIMIT {
        return align_profile_region_greedy(left, right, left_indices, right_indices);
    }

    let rows = left_indices.len();
    let cols = right_indices.len();
    let width = cols + 1;
    let mut dp = vec![0i64; (rows + 1) * (cols + 1)];
    let mut backtrack = vec![0u8; (rows + 1) * (cols + 1)];
    let index = |row: usize, col: usize| row * width + col;
    let gap = -3;

    for row in 1..=rows {
        dp[index(row, 0)] = dp[index(row - 1, 0)] + gap;
        backtrack[index(row, 0)] = 2;
    }
    for col in 1..=cols {
        dp[index(0, col)] = dp[index(0, col - 1)] + gap;
        backtrack[index(0, col)] = 3;
    }

    for row in 1..=rows {
        for col in 1..=cols {
            let left_dispatch = &left[left_indices[row - 1]];
            let right_dispatch = &right[right_indices[col - 1]];
            let diag =
                dp[index(row - 1, col - 1)] + profile_pair_score(left_dispatch, right_dispatch);
            let up = dp[index(row - 1, col)] + gap;
            let prev_left = dp[index(row, col - 1)] + gap;
            let (best, direction) = if up > diag && up >= prev_left {
                (up, 2)
            } else if prev_left > diag {
                (prev_left, 3)
            } else {
                (diag, 1)
            };
            dp[index(row, col)] = best;
            backtrack[index(row, col)] = direction;
        }
    }

    let mut row = rows;
    let mut col = cols;
    let mut pairs = Vec::new();
    while row > 0 && col > 0 {
        match backtrack[index(row, col)] {
            1 => {
                let left_index = left_indices[row - 1];
                let right_index = right_indices[col - 1];
                if profile_pair_score(&left[left_index], &right[right_index]) > 0 {
                    pairs.push((left_index, right_index));
                }
                row -= 1;
                col -= 1;
            }
            2 => row -= 1,
            3 => col -= 1,
            _ => break,
        }
    }
    pairs.reverse();
    pairs
}

fn align_profile_region_greedy(
    left: &[ProfileDispatch],
    right: &[ProfileDispatch],
    left_indices: &[usize],
    right_indices: &[usize],
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();
    let mut left_pos = 0;
    let mut right_pos = 0;
    while left_pos < left_indices.len() && right_pos < right_indices.len() {
        let left_dispatch = &left[left_indices[left_pos]];
        let right_dispatch = &right[right_indices[right_pos]];
        if profile_pair_score(left_dispatch, right_dispatch) > 0 {
            pairs.push((left_indices[left_pos], right_indices[right_pos]));
            left_pos += 1;
            right_pos += 1;
        } else if profile_match_ahead(left_dispatch, right, right_indices, right_pos, 4) {
            right_pos += 1;
        } else {
            left_pos += 1;
        }
    }
    pairs
}

fn profile_match_ahead(
    left_dispatch: &ProfileDispatch,
    right: &[ProfileDispatch],
    right_indices: &[usize],
    right_pos: usize,
    max_ahead: usize,
) -> bool {
    (1..=max_ahead).any(|ahead| {
        right_pos + ahead < right_indices.len()
            && left_dispatch.function_key == right[right_indices[right_pos + ahead]].function_key
    })
}

fn profile_pair_score(left: &ProfileDispatch, right: &ProfileDispatch) -> i64 {
    let mut score = -2;
    if !left.function_key.is_empty() && left.function_key == right.function_key {
        score = 6;
    } else if left.function_name.is_empty()
        && right.function_name.is_empty()
        && left.pipeline_id.is_some()
        && left.pipeline_id == right.pipeline_id
    {
        score = 5;
    } else if left.pipeline_id.is_some() && left.pipeline_id == right.pipeline_id {
        score = 2;
    } else if left.encoder_index == right.encoder_index {
        score = 1;
    }

    let duration_delta = left.duration_us.abs_diff(right.duration_us);
    if duration_delta <= 8 {
        score += 1;
    }
    if duration_delta >= 300 {
        score -= 1;
    }
    score
}

fn profile_match_pair(
    left: &ProfileDispatch,
    right: &ProfileDispatch,
    method: &str,
    confidence: f64,
) -> ProfileMatchPair {
    ProfileMatchPair {
        left_source_index: left.source_index,
        right_source_index: right.source_index,
        function_name: if left.function_name.is_empty() {
            safe_profile_name(&right.function_name)
        } else {
            safe_profile_name(&left.function_name)
        },
        kernel_id: if left.kernel_id.is_empty() {
            right.kernel_id.clone()
        } else {
            left.kernel_id.clone()
        },
        encoder_index: left.encoder_index,
        left_pipeline_id: left.pipeline_id,
        right_pipeline_id: right.pipeline_id,
        left_duration_us: left.duration_us,
        right_duration_us: right.duration_us,
        delta_us: left.duration_us as i64 - right.duration_us as i64,
        match_method: method.to_owned(),
        confidence,
    }
}

fn unmatched_profile_dispatch(trace: &str, dispatch: &ProfileDispatch) -> ProfileUnmatchedDispatch {
    ProfileUnmatchedDispatch {
        trace: trace.to_owned(),
        source_index: dispatch.source_index,
        function_name: safe_profile_name(&dispatch.function_name),
        kernel_id: dispatch.kernel_id.clone(),
        encoder_index: dispatch.encoder_index,
        pipeline_id: dispatch.pipeline_id,
        duration_us: dispatch.duration_us,
    }
}

fn safe_profile_name(name: &str) -> String {
    if name.trim().is_empty() {
        "(unnamed)".to_owned()
    } else {
        name.to_owned()
    }
}

fn total_profile_duration(dispatches: &[ProfileDispatch]) -> u64 {
    dispatches.iter().map(|dispatch| dispatch.duration_us).sum()
}

fn build_profile_function_deltas(
    left: &[ProfileDispatch],
    right: &[ProfileDispatch],
    matches: &[ProfileMatchPair],
) -> Vec<ProfileFunctionDelta> {
    #[derive(Default)]
    struct FunctionAgg {
        function_name: String,
        left_dispatch_count: usize,
        right_dispatch_count: usize,
        matched_pairs: usize,
        left_total_us: u64,
        right_total_us: u64,
        first_occurrence_delta_us: Option<i64>,
        max_occurrence_delta_us: i64,
    }

    let mut by_name = BTreeMap::<String, FunctionAgg>::new();
    for dispatch in left {
        let name = safe_profile_name(&dispatch.function_name);
        let agg = by_name.entry(name.clone()).or_insert_with(|| FunctionAgg {
            function_name: name,
            ..Default::default()
        });
        agg.left_dispatch_count += 1;
        agg.left_total_us += dispatch.duration_us;
    }
    for dispatch in right {
        let name = safe_profile_name(&dispatch.function_name);
        let agg = by_name.entry(name.clone()).or_insert_with(|| FunctionAgg {
            function_name: name,
            ..Default::default()
        });
        agg.right_dispatch_count += 1;
        agg.right_total_us += dispatch.duration_us;
    }
    for pair in matches {
        let agg = by_name
            .entry(pair.function_name.clone())
            .or_insert_with(|| FunctionAgg {
                function_name: pair.function_name.clone(),
                ..Default::default()
            });
        agg.matched_pairs += 1;
        if agg.first_occurrence_delta_us.is_none() {
            agg.first_occurrence_delta_us = Some(pair.delta_us);
        }
        if pair.delta_us.abs() > agg.max_occurrence_delta_us.abs() {
            agg.max_occurrence_delta_us = pair.delta_us;
        }
    }

    let mut deltas = by_name
        .into_values()
        .map(|agg| ProfileFunctionDelta {
            function_name: agg.function_name,
            left_dispatch_count: agg.left_dispatch_count,
            right_dispatch_count: agg.right_dispatch_count,
            dispatch_count_delta: agg.left_dispatch_count as isize
                - agg.right_dispatch_count as isize,
            matched_pairs: agg.matched_pairs,
            left_total_us: agg.left_total_us,
            right_total_us: agg.right_total_us,
            total_delta_us: agg.left_total_us as i64 - agg.right_total_us as i64,
            first_occurrence_delta_us: agg.first_occurrence_delta_us.unwrap_or_default(),
            max_occurrence_delta_us: agg.max_occurrence_delta_us,
        })
        .collect::<Vec<_>>();
    deltas.sort_by(|left, right| {
        right
            .total_delta_us
            .abs()
            .cmp(&left.total_delta_us.abs())
            .then_with(|| left.function_name.cmp(&right.function_name))
    });
    deltas
}

fn build_profile_encoder_deltas(
    left: &[ProfileDispatch],
    right: &[ProfileDispatch],
) -> Vec<ProfileEncoderDelta> {
    #[derive(Default)]
    struct EncoderAgg {
        left_dispatch_count: usize,
        right_dispatch_count: usize,
        left_total_us: u64,
        right_total_us: u64,
    }

    let mut by_encoder = BTreeMap::<usize, EncoderAgg>::new();
    for dispatch in left {
        let agg = by_encoder.entry(dispatch.encoder_index).or_default();
        agg.left_dispatch_count += 1;
        agg.left_total_us += dispatch.duration_us;
    }
    for dispatch in right {
        let agg = by_encoder.entry(dispatch.encoder_index).or_default();
        agg.right_dispatch_count += 1;
        agg.right_total_us += dispatch.duration_us;
    }

    let mut deltas = by_encoder
        .into_iter()
        .map(|(encoder_index, agg)| ProfileEncoderDelta {
            encoder_index,
            left_dispatch_count: agg.left_dispatch_count,
            right_dispatch_count: agg.right_dispatch_count,
            dispatch_count_delta: agg.left_dispatch_count as isize
                - agg.right_dispatch_count as isize,
            left_total_us: agg.left_total_us,
            right_total_us: agg.right_total_us,
            total_delta_us: agg.left_total_us as i64 - agg.right_total_us as i64,
        })
        .collect::<Vec<_>>();
    deltas.sort_by(|left, right| {
        right
            .total_delta_us
            .abs()
            .cmp(&left.total_delta_us.abs())
            .then_with(|| left.encoder_index.cmp(&right.encoder_index))
    });
    deltas
}

fn build_profile_encoder_reports(
    left: &[ProfileDispatch],
    right: &[ProfileDispatch],
    matches: &[ProfileMatchPair],
    unmatched_left: &[usize],
    unmatched_right: &[usize],
    limit: usize,
) -> Vec<ProfileEncoderReport> {
    #[derive(Default)]
    struct EncoderAgg {
        left_dispatch_count: usize,
        right_dispatch_count: usize,
        matched_count: usize,
        matched_delta_us: i64,
        unmatched_left_count: usize,
        unmatched_right_count: usize,
        unmatched_delta_us: i64,
        top_dispatches: Vec<ProfileMatchPair>,
    }

    let mut by_encoder = BTreeMap::<usize, EncoderAgg>::new();
    for dispatch in left {
        by_encoder
            .entry(dispatch.encoder_index)
            .or_default()
            .left_dispatch_count += 1;
    }
    for dispatch in right {
        by_encoder
            .entry(dispatch.encoder_index)
            .or_default()
            .right_dispatch_count += 1;
    }
    for index in unmatched_left {
        let dispatch = &left[*index];
        let agg = by_encoder.entry(dispatch.encoder_index).or_default();
        agg.unmatched_left_count += 1;
        agg.unmatched_delta_us += dispatch.duration_us as i64;
    }
    for index in unmatched_right {
        let dispatch = &right[*index];
        let agg = by_encoder.entry(dispatch.encoder_index).or_default();
        agg.unmatched_right_count += 1;
        agg.unmatched_delta_us -= dispatch.duration_us as i64;
    }
    for pair in matches {
        let agg = by_encoder.entry(pair.encoder_index).or_default();
        agg.matched_count += 1;
        agg.matched_delta_us += pair.delta_us;
        agg.top_dispatches.push(pair.clone());
    }

    let top_limit = limit.clamp(1, 5);
    let mut reports = by_encoder
        .into_iter()
        .map(|(encoder_index, mut agg)| {
            agg.top_dispatches.sort_by(|left, right| {
                right
                    .delta_us
                    .abs()
                    .cmp(&left.delta_us.abs())
                    .then_with(|| left.left_source_index.cmp(&right.left_source_index))
            });
            agg.top_dispatches.truncate(top_limit);
            ProfileEncoderReport {
                encoder_index,
                left_dispatch_count: agg.left_dispatch_count,
                right_dispatch_count: agg.right_dispatch_count,
                matched_count: agg.matched_count,
                matched_delta_us: agg.matched_delta_us,
                unmatched_left_count: agg.unmatched_left_count,
                unmatched_right_count: agg.unmatched_right_count,
                unmatched_count: agg.unmatched_left_count + agg.unmatched_right_count,
                unmatched_delta_us: agg.unmatched_delta_us,
                top_dispatches: agg.top_dispatches,
            }
        })
        .collect::<Vec<_>>();
    reports.sort_by(|left, right| {
        right
            .matched_delta_us
            .abs()
            .cmp(&left.matched_delta_us.abs())
            .then_with(|| left.encoder_index.cmp(&right.encoder_index))
    });
    reports
}

fn build_profile_pipeline_deltas(
    left: &[ProfileDispatch],
    right: &[ProfileDispatch],
) -> Vec<ProfilePipelineDelta> {
    #[derive(Default)]
    struct PipelineAgg {
        function_name: String,
        left_dispatch_count: usize,
        right_dispatch_count: usize,
        left_total_us: u64,
        right_total_us: u64,
    }

    let mut by_pipeline = BTreeMap::<Option<i64>, PipelineAgg>::new();
    for dispatch in left {
        let agg = by_pipeline.entry(dispatch.pipeline_id).or_default();
        if agg.function_name.is_empty() {
            agg.function_name = safe_profile_name(&dispatch.function_name);
        }
        agg.left_dispatch_count += 1;
        agg.left_total_us += dispatch.duration_us;
    }
    for dispatch in right {
        let agg = by_pipeline.entry(dispatch.pipeline_id).or_default();
        if agg.function_name.is_empty() {
            agg.function_name = safe_profile_name(&dispatch.function_name);
        }
        agg.right_dispatch_count += 1;
        agg.right_total_us += dispatch.duration_us;
    }
    let mut deltas = by_pipeline
        .into_iter()
        .map(|(pipeline_id, agg)| ProfilePipelineDelta {
            pipeline_id,
            function_name: agg.function_name,
            left_dispatch_count: agg.left_dispatch_count,
            right_dispatch_count: agg.right_dispatch_count,
            dispatch_count_delta: agg.left_dispatch_count as isize
                - agg.right_dispatch_count as isize,
            left_total_us: agg.left_total_us,
            right_total_us: agg.right_total_us,
            total_delta_us: agg.left_total_us as i64 - agg.right_total_us as i64,
        })
        .collect::<Vec<_>>();
    deltas.sort_by(|left, right| {
        right
            .total_delta_us
            .abs()
            .cmp(&left.total_delta_us.abs())
            .then_with(|| left.pipeline_id.cmp(&right.pipeline_id))
    });
    deltas
}

fn build_profile_unnamed_deltas(
    left: &[ProfileDispatch],
    right: &[ProfileDispatch],
    matches: &[ProfileMatchPair],
) -> Vec<ProfileUnnamedDispatchDelta> {
    #[derive(Default)]
    struct UnnamedAgg {
        kernel_id: String,
        pipeline_id: Option<i64>,
        left_dispatch_count: usize,
        right_dispatch_count: usize,
        left_total_us: u64,
        right_total_us: u64,
        top_outlier_delta_us: i64,
        top_outlier_left_source_index: Option<usize>,
        top_outlier_right_source_index: Option<usize>,
    }

    let mut by_kernel = BTreeMap::<String, UnnamedAgg>::new();
    for dispatch in left {
        if !dispatch.function_name.is_empty() {
            continue;
        }
        let key = dispatch.kernel_id.clone();
        let agg = by_kernel.entry(key.clone()).or_insert_with(|| UnnamedAgg {
            kernel_id: key,
            pipeline_id: dispatch.pipeline_id,
            ..Default::default()
        });
        agg.left_dispatch_count += 1;
        agg.left_total_us += dispatch.duration_us;
        agg.pipeline_id = agg.pipeline_id.or(dispatch.pipeline_id);
    }
    for dispatch in right {
        if !dispatch.function_name.is_empty() {
            continue;
        }
        let key = dispatch.kernel_id.clone();
        let agg = by_kernel.entry(key.clone()).or_insert_with(|| UnnamedAgg {
            kernel_id: key,
            pipeline_id: dispatch.pipeline_id,
            ..Default::default()
        });
        agg.right_dispatch_count += 1;
        agg.right_total_us += dispatch.duration_us;
        agg.pipeline_id = agg.pipeline_id.or(dispatch.pipeline_id);
    }
    for pair in matches {
        if pair.function_name != "(unnamed)" {
            continue;
        }
        let key = pair.kernel_id.clone();
        let agg = by_kernel.entry(key.clone()).or_insert_with(|| UnnamedAgg {
            kernel_id: key,
            pipeline_id: pair.left_pipeline_id.or(pair.right_pipeline_id),
            ..Default::default()
        });
        if pair.delta_us.abs() > agg.top_outlier_delta_us.abs() {
            agg.top_outlier_delta_us = pair.delta_us;
            agg.top_outlier_left_source_index = Some(pair.left_source_index);
            agg.top_outlier_right_source_index = Some(pair.right_source_index);
        }
    }

    let mut deltas = by_kernel
        .into_values()
        .map(|agg| ProfileUnnamedDispatchDelta {
            kernel_id: agg.kernel_id,
            pipeline_id: agg.pipeline_id,
            left_dispatch_count: agg.left_dispatch_count,
            right_dispatch_count: agg.right_dispatch_count,
            dispatch_count_delta: agg.left_dispatch_count as isize
                - agg.right_dispatch_count as isize,
            left_total_us: agg.left_total_us,
            right_total_us: agg.right_total_us,
            total_delta_us: agg.left_total_us as i64 - agg.right_total_us as i64,
            top_outlier_delta_us: agg.top_outlier_delta_us,
            top_outlier_left_source_index: agg.top_outlier_left_source_index,
            top_outlier_right_source_index: agg.top_outlier_right_source_index,
        })
        .collect::<Vec<_>>();
    deltas.sort_by(|left, right| {
        right
            .total_delta_us
            .abs()
            .cmp(&left.total_delta_us.abs())
            .then_with(|| left.pipeline_id.cmp(&right.pipeline_id))
            .then_with(|| left.kernel_id.cmp(&right.kernel_id))
    });
    deltas
}

fn build_profile_spike_windows(
    matches: &[ProfileMatchPair],
    options: &ProfileDiffOptions,
) -> Vec<ProfileSpikeWindow> {
    let threshold = options.min_delta_us.max(75);
    let mut candidates = matches
        .iter()
        .filter(|pair| pair.delta_us.abs() >= threshold)
        .cloned()
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Vec::new();
    }
    candidates.sort_by_key(|pair| (pair.left_source_index, pair.right_source_index));

    let mut windows = Vec::new();
    let mut current: Option<ProfileSpikeWindow> = None;
    for pair in candidates {
        let contiguous = current.as_ref().is_some_and(|window| {
            pair.encoder_index == window.encoder_index
                && pair.left_source_index <= window.left_end_source_index + 2
                && pair.right_source_index <= window.right_end_source_index + 2
        });
        if !contiguous {
            if let Some(window) = current.take() {
                windows.push(window);
            }
            current = Some(ProfileSpikeWindow {
                encoder_index: pair.encoder_index,
                left_start_source_index: pair.left_source_index,
                left_end_source_index: pair.left_source_index,
                right_start_source_index: pair.right_source_index,
                right_end_source_index: pair.right_source_index,
                match_count: 1,
                total_delta_us: pair.delta_us,
                max_abs_delta_us: pair.delta_us.abs(),
            });
            continue;
        }
        if let Some(window) = &mut current {
            window.left_end_source_index = pair.left_source_index;
            window.right_end_source_index = pair.right_source_index;
            window.match_count += 1;
            window.total_delta_us += pair.delta_us;
            window.max_abs_delta_us = window.max_abs_delta_us.max(pair.delta_us.abs());
        }
    }
    if let Some(window) = current {
        windows.push(window);
    }
    windows.sort_by(|left, right| {
        right
            .total_delta_us
            .abs()
            .cmp(&left.total_delta_us.abs())
            .then_with(|| left.encoder_index.cmp(&right.encoder_index))
            .then_with(|| {
                left.left_start_source_index
                    .cmp(&right.left_start_source_index)
            })
    });
    windows
}

fn top_profile_outliers(
    matches: &[ProfileMatchPair],
    options: &ProfileDiffOptions,
) -> Vec<ProfileMatchPair> {
    let min_delta = options.min_delta_us.max(0);
    let mut outliers = matches
        .iter()
        .filter(|pair| pair.delta_us.abs() >= min_delta)
        .cloned()
        .collect::<Vec<_>>();
    outliers.sort_by(|left, right| {
        right
            .delta_us
            .abs()
            .cmp(&left.delta_us.abs())
            .then_with(|| left.left_source_index.cmp(&right.left_source_index))
    });
    outliers
}

fn infer_profile_likely_cause(
    total_delta: i64,
    matched_delta: i64,
    unmatched_delta: i64,
) -> String {
    let abs_total = total_delta.abs();
    if abs_total == 0 {
        return "no measurable delta".to_owned();
    }
    let unmatched_ratio = unmatched_delta.abs() as f64 / abs_total as f64;
    if unmatched_ratio >= 0.35 {
        "structural command stream overhead".to_owned()
    } else if matched_delta.abs() >= 250 {
        "one-time warmup/growth spike".to_owned()
    } else {
        "repeated per-step slowdown".to_owned()
    }
}

fn truncate_profile_report(report: &mut ProfileDiffReport, limit: usize) {
    report.top_function_deltas.truncate(limit);
    report.top_dispatch_outliers.truncate(limit);
    report.encoder_deltas.truncate(limit);
    report.encoder_reports.truncate(limit);
    report.pipeline_deltas.truncate(limit);
    report.unnamed_dispatch_deltas.truncate(limit);
    report.timeline_spike_windows.truncate(limit);
    report.unmatched.truncate(limit * 4);
}

fn aggregate_counter_metrics(
    report: &counter_export::CounterExportReport,
) -> std::collections::BTreeMap<String, CounterAggregate> {
    let mut sums = std::collections::BTreeMap::<String, CounterAggregateSums>::new();
    for row in &report.rows {
        let Some(name) = row.kernel_name.clone() else {
            continue;
        };
        let entry = sums.entry(name).or_default();
        if row.kernel_invocations > 0 {
            entry.kernel_invocations_sum += row.kernel_invocations as f64;
            entry.kernel_invocations_count += 1;
        }
        if let Some(value) = row.execution_cost_percent {
            entry.execution_cost_sum += value;
            entry.execution_cost_count += 1;
        }
        if let Some(value) = row.occupancy_percent {
            entry.occupancy_sum += value;
            entry.occupancy_count += 1;
        }
        if let Some(value) = row.alu_utilization_percent {
            entry.alu_sum += value;
            entry.alu_count += 1;
        }
        if let Some(value) = row.last_level_cache_percent {
            entry.llc_sum += value;
            entry.llc_count += 1;
        }
        if let Some(value) = row.device_memory_bandwidth_gbps {
            entry.device_bw_sum += value;
            entry.device_bw_count += 1;
        }
        if let Some(value) = row.gpu_read_bandwidth_gbps {
            entry.gpu_read_bw_sum += value;
            entry.gpu_read_bw_count += 1;
        }
        if let Some(value) = row.gpu_write_bandwidth_gbps {
            entry.gpu_write_bw_sum += value;
            entry.gpu_write_bw_count += 1;
        }
        if let Some(value) = row.buffer_device_memory_bytes_read {
            entry.buffer_device_memory_read_sum += value;
            entry.buffer_device_memory_read_count += 1;
        }
        if let Some(value) = row.buffer_device_memory_bytes_written {
            entry.buffer_device_memory_written_sum += value;
            entry.buffer_device_memory_written_count += 1;
        }
        if let Some(value) = row.bytes_read_from_device_memory {
            entry.device_memory_read_sum += value;
            entry.device_memory_read_count += 1;
        }
        if let Some(value) = row.bytes_written_to_device_memory {
            entry.device_memory_written_sum += value;
            entry.device_memory_written_count += 1;
        }
        if let Some(value) = row.buffer_l1_miss_rate_percent {
            entry.buffer_l1_miss_rate_sum += value;
            entry.buffer_l1_miss_rate_count += 1;
        }
        if let Some(value) = row.buffer_l1_read_accesses {
            entry.buffer_l1_read_accesses_sum += value;
            entry.buffer_l1_read_accesses_count += 1;
        }
        if let Some(value) = row.buffer_l1_read_bandwidth_gbps {
            entry.buffer_l1_read_bw_sum += value;
            entry.buffer_l1_read_bw_count += 1;
        }
        if let Some(value) = row.buffer_l1_write_accesses {
            entry.buffer_l1_write_accesses_sum += value;
            entry.buffer_l1_write_accesses_count += 1;
        }
        if let Some(value) = row.buffer_l1_write_bandwidth_gbps {
            entry.buffer_l1_write_bw_sum += value;
            entry.buffer_l1_write_bw_count += 1;
        }
        if let Some(value) = row.compute_shader_launch_utilization_percent {
            entry.compute_shader_launch_util_sum += value;
            entry.compute_shader_launch_util_count += 1;
        }
        if let Some(value) = row.control_flow_utilization_percent {
            entry.control_flow_util_sum += value;
            entry.control_flow_util_count += 1;
        }
        if let Some(value) = row.instruction_throughput_utilization_percent {
            entry.instruction_throughput_util_sum += value;
            entry.instruction_throughput_util_count += 1;
        }
        if let Some(value) = row.integer_complex_utilization_percent {
            entry.integer_complex_util_sum += value;
            entry.integer_complex_util_count += 1;
        }
        if let Some(value) = row.integer_conditional_utilization_percent {
            entry.integer_conditional_util_sum += value;
            entry.integer_conditional_util_count += 1;
        }
        if let Some(value) = row.f32_utilization_percent {
            entry.f32_util_sum += value;
            entry.f32_util_count += 1;
        }
    }

    sums.into_iter()
        .map(|(name, sums)| {
            (
                name,
                CounterAggregate {
                    kernel_invocations: average_option(
                        sums.kernel_invocations_sum,
                        sums.kernel_invocations_count,
                    ),
                    execution_cost_percent: average_option(
                        sums.execution_cost_sum,
                        sums.execution_cost_count,
                    ),
                    occupancy_percent: average_option(sums.occupancy_sum, sums.occupancy_count),
                    alu_utilization_percent: average_option(sums.alu_sum, sums.alu_count),
                    last_level_cache_percent: average_option(sums.llc_sum, sums.llc_count),
                    device_memory_bandwidth_gbps: average_option(
                        sums.device_bw_sum,
                        sums.device_bw_count,
                    ),
                    gpu_read_bandwidth_gbps: average_option(
                        sums.gpu_read_bw_sum,
                        sums.gpu_read_bw_count,
                    ),
                    gpu_write_bandwidth_gbps: average_option(
                        sums.gpu_write_bw_sum,
                        sums.gpu_write_bw_count,
                    ),
                    buffer_device_memory_bytes_read: average_option(
                        sums.buffer_device_memory_read_sum,
                        sums.buffer_device_memory_read_count,
                    ),
                    buffer_device_memory_bytes_written: average_option(
                        sums.buffer_device_memory_written_sum,
                        sums.buffer_device_memory_written_count,
                    ),
                    bytes_read_from_device_memory: average_option(
                        sums.device_memory_read_sum,
                        sums.device_memory_read_count,
                    ),
                    bytes_written_to_device_memory: average_option(
                        sums.device_memory_written_sum,
                        sums.device_memory_written_count,
                    ),
                    buffer_l1_miss_rate_percent: average_option(
                        sums.buffer_l1_miss_rate_sum,
                        sums.buffer_l1_miss_rate_count,
                    ),
                    buffer_l1_read_accesses: average_option(
                        sums.buffer_l1_read_accesses_sum,
                        sums.buffer_l1_read_accesses_count,
                    ),
                    buffer_l1_read_bandwidth_gbps: average_option(
                        sums.buffer_l1_read_bw_sum,
                        sums.buffer_l1_read_bw_count,
                    ),
                    buffer_l1_write_accesses: average_option(
                        sums.buffer_l1_write_accesses_sum,
                        sums.buffer_l1_write_accesses_count,
                    ),
                    buffer_l1_write_bandwidth_gbps: average_option(
                        sums.buffer_l1_write_bw_sum,
                        sums.buffer_l1_write_bw_count,
                    ),
                    compute_shader_launch_utilization_percent: average_option(
                        sums.compute_shader_launch_util_sum,
                        sums.compute_shader_launch_util_count,
                    ),
                    control_flow_utilization_percent: average_option(
                        sums.control_flow_util_sum,
                        sums.control_flow_util_count,
                    ),
                    instruction_throughput_utilization_percent: average_option(
                        sums.instruction_throughput_util_sum,
                        sums.instruction_throughput_util_count,
                    ),
                    integer_complex_utilization_percent: average_option(
                        sums.integer_complex_util_sum,
                        sums.integer_complex_util_count,
                    ),
                    integer_conditional_utilization_percent: average_option(
                        sums.integer_conditional_util_sum,
                        sums.integer_conditional_util_count,
                    ),
                    f32_utilization_percent: average_option(sums.f32_util_sum, sums.f32_util_count),
                },
            )
        })
        .collect()
}

#[derive(Default, Clone, Copy)]
struct CounterAggregateSums {
    kernel_invocations_sum: f64,
    kernel_invocations_count: usize,
    execution_cost_sum: f64,
    execution_cost_count: usize,
    occupancy_sum: f64,
    occupancy_count: usize,
    alu_sum: f64,
    alu_count: usize,
    llc_sum: f64,
    llc_count: usize,
    device_bw_sum: f64,
    device_bw_count: usize,
    gpu_read_bw_sum: f64,
    gpu_read_bw_count: usize,
    gpu_write_bw_sum: f64,
    gpu_write_bw_count: usize,
    buffer_device_memory_read_sum: f64,
    buffer_device_memory_read_count: usize,
    buffer_device_memory_written_sum: f64,
    buffer_device_memory_written_count: usize,
    device_memory_read_sum: f64,
    device_memory_read_count: usize,
    device_memory_written_sum: f64,
    device_memory_written_count: usize,
    buffer_l1_miss_rate_sum: f64,
    buffer_l1_miss_rate_count: usize,
    buffer_l1_read_accesses_sum: f64,
    buffer_l1_read_accesses_count: usize,
    buffer_l1_read_bw_sum: f64,
    buffer_l1_read_bw_count: usize,
    buffer_l1_write_accesses_sum: f64,
    buffer_l1_write_accesses_count: usize,
    buffer_l1_write_bw_sum: f64,
    buffer_l1_write_bw_count: usize,
    compute_shader_launch_util_sum: f64,
    compute_shader_launch_util_count: usize,
    control_flow_util_sum: f64,
    control_flow_util_count: usize,
    instruction_throughput_util_sum: f64,
    instruction_throughput_util_count: usize,
    integer_complex_util_sum: f64,
    integer_complex_util_count: usize,
    integer_conditional_util_sum: f64,
    integer_conditional_util_count: usize,
    f32_util_sum: f64,
    f32_util_count: usize,
}

#[derive(Default, Clone, Copy)]
struct CounterAggregate {
    kernel_invocations: Option<f64>,
    execution_cost_percent: Option<f64>,
    occupancy_percent: Option<f64>,
    alu_utilization_percent: Option<f64>,
    last_level_cache_percent: Option<f64>,
    device_memory_bandwidth_gbps: Option<f64>,
    gpu_read_bandwidth_gbps: Option<f64>,
    gpu_write_bandwidth_gbps: Option<f64>,
    buffer_device_memory_bytes_read: Option<f64>,
    buffer_device_memory_bytes_written: Option<f64>,
    bytes_read_from_device_memory: Option<f64>,
    bytes_written_to_device_memory: Option<f64>,
    buffer_l1_miss_rate_percent: Option<f64>,
    buffer_l1_read_accesses: Option<f64>,
    buffer_l1_read_bandwidth_gbps: Option<f64>,
    buffer_l1_write_accesses: Option<f64>,
    buffer_l1_write_bandwidth_gbps: Option<f64>,
    compute_shader_launch_utilization_percent: Option<f64>,
    control_flow_utilization_percent: Option<f64>,
    instruction_throughput_utilization_percent: Option<f64>,
    integer_complex_utilization_percent: Option<f64>,
    integer_conditional_utilization_percent: Option<f64>,
    f32_utilization_percent: Option<f64>,
}

fn average_option(sum: f64, count: usize) -> Option<f64> {
    (count > 0).then(|| sum / count as f64)
}

fn csv_string(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn option_csv<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn metrics_equal(left: CounterAggregate, right: CounterAggregate) -> bool {
    approx_option_eq(left.kernel_invocations, right.kernel_invocations)
        && approx_option_eq(left.execution_cost_percent, right.execution_cost_percent)
        && approx_option_eq(left.occupancy_percent, right.occupancy_percent)
        && approx_option_eq(left.alu_utilization_percent, right.alu_utilization_percent)
        && approx_option_eq(
            left.last_level_cache_percent,
            right.last_level_cache_percent,
        )
        && approx_option_eq(
            left.device_memory_bandwidth_gbps,
            right.device_memory_bandwidth_gbps,
        )
        && approx_option_eq(left.gpu_read_bandwidth_gbps, right.gpu_read_bandwidth_gbps)
        && approx_option_eq(
            left.gpu_write_bandwidth_gbps,
            right.gpu_write_bandwidth_gbps,
        )
        && approx_option_eq(
            left.buffer_device_memory_bytes_read,
            right.buffer_device_memory_bytes_read,
        )
        && approx_option_eq(
            left.buffer_device_memory_bytes_written,
            right.buffer_device_memory_bytes_written,
        )
        && approx_option_eq(
            left.bytes_read_from_device_memory,
            right.bytes_read_from_device_memory,
        )
        && approx_option_eq(
            left.bytes_written_to_device_memory,
            right.bytes_written_to_device_memory,
        )
        && approx_option_eq(
            left.buffer_l1_miss_rate_percent,
            right.buffer_l1_miss_rate_percent,
        )
        && approx_option_eq(left.buffer_l1_read_accesses, right.buffer_l1_read_accesses)
        && approx_option_eq(
            left.buffer_l1_read_bandwidth_gbps,
            right.buffer_l1_read_bandwidth_gbps,
        )
        && approx_option_eq(
            left.buffer_l1_write_accesses,
            right.buffer_l1_write_accesses,
        )
        && approx_option_eq(
            left.buffer_l1_write_bandwidth_gbps,
            right.buffer_l1_write_bandwidth_gbps,
        )
        && approx_option_eq(
            left.compute_shader_launch_utilization_percent,
            right.compute_shader_launch_utilization_percent,
        )
        && approx_option_eq(
            left.control_flow_utilization_percent,
            right.control_flow_utilization_percent,
        )
        && approx_option_eq(
            left.instruction_throughput_utilization_percent,
            right.instruction_throughput_utilization_percent,
        )
        && approx_option_eq(
            left.integer_complex_utilization_percent,
            right.integer_complex_utilization_percent,
        )
        && approx_option_eq(
            left.integer_conditional_utilization_percent,
            right.integer_conditional_utilization_percent,
        )
        && approx_option_eq(left.f32_utilization_percent, right.f32_utilization_percent)
}

fn approx_option_eq(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => (left - right).abs() < f64::EPSILON,
        (None, None) => true,
        _ => false,
    }
}

fn aggregate_change_magnitude(change: &CounterMetricChange) -> f64 {
    option_delta(
        change.left_kernel_invocations,
        change.right_kernel_invocations,
    )
    .abs()
        + option_delta(
            change.left_execution_cost_percent,
            change.right_execution_cost_percent,
        )
        .abs()
        + option_delta(
            change.left_occupancy_percent,
            change.right_occupancy_percent,
        )
        .abs()
        + option_delta(
            change.left_alu_utilization_percent,
            change.right_alu_utilization_percent,
        )
        .abs()
        + option_delta(
            change.left_last_level_cache_percent,
            change.right_last_level_cache_percent,
        )
        .abs()
        + option_delta(
            change.left_device_memory_bandwidth_gbps,
            change.right_device_memory_bandwidth_gbps,
        )
        .abs()
        + option_delta(
            change.left_gpu_read_bandwidth_gbps,
            change.right_gpu_read_bandwidth_gbps,
        )
        .abs()
        + option_delta(
            change.left_gpu_write_bandwidth_gbps,
            change.right_gpu_write_bandwidth_gbps,
        )
        .abs()
        + option_delta(
            change.left_buffer_device_memory_bytes_read,
            change.right_buffer_device_memory_bytes_read,
        )
        .abs()
        + option_delta(
            change.left_buffer_device_memory_bytes_written,
            change.right_buffer_device_memory_bytes_written,
        )
        .abs()
        + option_delta(
            change.left_bytes_read_from_device_memory,
            change.right_bytes_read_from_device_memory,
        )
        .abs()
        + option_delta(
            change.left_bytes_written_to_device_memory,
            change.right_bytes_written_to_device_memory,
        )
        .abs()
        + option_delta(
            change.left_buffer_l1_miss_rate_percent,
            change.right_buffer_l1_miss_rate_percent,
        )
        .abs()
        + option_delta(
            change.left_buffer_l1_read_accesses,
            change.right_buffer_l1_read_accesses,
        )
        .abs()
        + option_delta(
            change.left_buffer_l1_read_bandwidth_gbps,
            change.right_buffer_l1_read_bandwidth_gbps,
        )
        .abs()
        + option_delta(
            change.left_buffer_l1_write_accesses,
            change.right_buffer_l1_write_accesses,
        )
        .abs()
        + option_delta(
            change.left_buffer_l1_write_bandwidth_gbps,
            change.right_buffer_l1_write_bandwidth_gbps,
        )
        .abs()
        + option_delta(
            change.left_compute_shader_launch_utilization_percent,
            change.right_compute_shader_launch_utilization_percent,
        )
        .abs()
        + option_delta(
            change.left_control_flow_utilization_percent,
            change.right_control_flow_utilization_percent,
        )
        .abs()
        + option_delta(
            change.left_instruction_throughput_utilization_percent,
            change.right_instruction_throughput_utilization_percent,
        )
        .abs()
        + option_delta(
            change.left_integer_complex_utilization_percent,
            change.right_integer_complex_utilization_percent,
        )
        .abs()
        + option_delta(
            change.left_integer_conditional_utilization_percent,
            change.right_integer_conditional_utilization_percent,
        )
        .abs()
        + option_delta(
            change.left_f32_utilization_percent,
            change.right_f32_utilization_percent,
        )
        .abs()
}

fn option_delta(left: Option<f64>, right: Option<f64>) -> f64 {
    right.unwrap_or_default() - left.unwrap_or_default()
}

fn format_option_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}"))
        .unwrap_or_else(|| "-".to_owned())
}

fn diff_buffer_stats(left: &AnalysisReport, right: &AnalysisReport) -> Vec<BufferChange> {
    let mut names = std::collections::BTreeSet::new();
    for stat in &left.buffer_stats {
        names.insert(stat.name.clone());
    }
    for stat in &right.buffer_stats {
        names.insert(stat.name.clone());
    }

    let left_map: std::collections::BTreeMap<_, _> = left
        .buffer_stats
        .iter()
        .map(|stat| (stat.name.as_str(), stat))
        .collect();
    let right_map: std::collections::BTreeMap<_, _> = right
        .buffer_stats
        .iter()
        .map(|stat| (stat.name.as_str(), stat))
        .collect();

    let mut changes = Vec::new();
    for name in names {
        let left_stat = left_map.get(name.as_str()).copied();
        let right_stat = right_map.get(name.as_str()).copied();
        let left_uses = left_stat.map_or(0, |stat| stat.use_count);
        let right_uses = right_stat.map_or(0, |stat| stat.use_count);
        let left_encoders = left_stat.map_or(0, |stat| stat.encoder_count);
        let right_encoders = right_stat.map_or(0, |stat| stat.encoder_count);
        let left_command_buffers = left_stat.map_or(0, |stat| stat.command_buffer_count);
        let right_command_buffers = right_stat.map_or(0, |stat| stat.command_buffer_count);
        if left_uses == right_uses
            && left_encoders == right_encoders
            && left_command_buffers == right_command_buffers
        {
            continue;
        }
        let status = match (left_stat, right_stat) {
            (None, Some(_)) => BufferChangeStatus::Added,
            (Some(_), None) => BufferChangeStatus::Removed,
            _ => BufferChangeStatus::Changed,
        };
        changes.push(BufferChange {
            name,
            status,
            left_uses,
            right_uses,
            left_encoders,
            right_encoders,
            left_command_buffers,
            right_command_buffers,
            delta: right_uses as isize - left_uses as isize,
        });
    }
    changes.sort_by(|left, right| {
        right
            .delta
            .abs()
            .cmp(&left.delta.abs())
            .then_with(|| left.name.cmp(&right.name))
    });
    changes
}

fn diff_kernel_stats(left: &AnalysisReport, right: &AnalysisReport) -> Vec<KernelChange> {
    let mut names = std::collections::BTreeSet::new();
    for stat in &left.kernel_stats {
        names.insert(stat.name.clone());
    }
    for stat in &right.kernel_stats {
        names.insert(stat.name.clone());
    }

    let left_map: std::collections::BTreeMap<_, _> = left
        .kernel_stats
        .iter()
        .map(|stat| (stat.name.as_str(), stat.dispatch_count))
        .collect();
    let right_map: std::collections::BTreeMap<_, _> = right
        .kernel_stats
        .iter()
        .map(|stat| (stat.name.as_str(), stat.dispatch_count))
        .collect();

    let mut changes = Vec::new();
    for name in names {
        let left_dispatches = left_map.get(name.as_str()).copied().unwrap_or_default();
        let right_dispatches = right_map.get(name.as_str()).copied().unwrap_or_default();
        if left_dispatches == right_dispatches {
            continue;
        }
        changes.push(KernelChange {
            name,
            left_dispatches,
            right_dispatches,
            delta: right_dispatches as isize - left_dispatches as isize,
        });
    }
    changes.sort_by(|left, right| {
        right
            .delta
            .abs()
            .cmp(&left.delta.abs())
            .then_with(|| left.name.cmp(&right.name))
    });
    changes
}

fn diff_buffer_lifecycles(
    left: &AnalysisReport,
    right: &AnalysisReport,
) -> Vec<BufferLifecycleChange> {
    let mut names = std::collections::BTreeSet::new();
    for stat in &left.buffer_lifecycles {
        names.insert(stat.name.clone());
    }
    for stat in &right.buffer_lifecycles {
        names.insert(stat.name.clone());
    }

    let left_map: std::collections::BTreeMap<_, _> = left
        .buffer_lifecycles
        .iter()
        .map(|stat| (stat.name.as_str(), stat))
        .collect();
    let right_map: std::collections::BTreeMap<_, _> = right
        .buffer_lifecycles
        .iter()
        .map(|stat| (stat.name.as_str(), stat))
        .collect();

    let mut changes = Vec::new();
    for name in names {
        let left_stat = left_map.get(name.as_str()).copied();
        let right_stat = right_map.get(name.as_str()).copied();
        let left_command_buffer_span = left_stat.map_or(0, |stat| stat.command_buffer_span);
        let left_dispatch_span = left_stat.map_or(0, |stat| stat.dispatch_span);
        let right_command_buffer_span = right_stat.map_or(0, |stat| stat.command_buffer_span);
        let right_dispatch_span = right_stat.map_or(0, |stat| stat.dispatch_span);
        if left_command_buffer_span == right_command_buffer_span
            && left_dispatch_span == right_dispatch_span
        {
            continue;
        }
        let command_buffer_span_delta =
            right_command_buffer_span as isize - left_command_buffer_span as isize;
        let dispatch_span_delta = right_dispatch_span as isize - left_dispatch_span as isize;
        let status = match (left_stat, right_stat) {
            (None, Some(_)) => BufferChangeStatus::Added,
            (Some(_), None) => BufferChangeStatus::Removed,
            _ => BufferChangeStatus::Changed,
        };
        changes.push(BufferLifecycleChange {
            name,
            status,
            left_command_buffer_span,
            right_command_buffer_span,
            command_buffer_span_delta,
            left_dispatch_span,
            right_dispatch_span,
            dispatch_span_delta,
        });
    }
    changes.sort_by(|left, right| {
        right
            .command_buffer_span_delta
            .abs()
            .cmp(&left.command_buffer_span_delta.abs())
            .then_with(|| {
                right
                    .dispatch_span_delta
                    .abs()
                    .cmp(&left.dispatch_span_delta.abs())
            })
            .then_with(|| left.name.cmp(&right.name))
    });
    changes
}

fn diff_kernel_timing_stats(
    left: &AnalysisReport,
    right: &AnalysisReport,
) -> Vec<KernelTimingChange> {
    let mut names = std::collections::BTreeSet::new();
    for stat in &left.timed_kernel_stats {
        names.insert(stat.name.clone());
    }
    for stat in &right.timed_kernel_stats {
        names.insert(stat.name.clone());
    }

    let left_map: std::collections::BTreeMap<_, _> = left
        .timed_kernel_stats
        .iter()
        .map(|stat| (stat.name.as_str(), stat))
        .collect();
    let right_map: std::collections::BTreeMap<_, _> = right
        .timed_kernel_stats
        .iter()
        .map(|stat| (stat.name.as_str(), stat))
        .collect();

    let mut changes = Vec::new();
    for name in names {
        let left_stat = left_map.get(name.as_str()).copied();
        let right_stat = right_map.get(name.as_str()).copied();
        let left_duration_ns = left_stat.map_or(0, |stat| stat.duration_ns);
        let right_duration_ns = right_stat.map_or(0, |stat| stat.duration_ns);
        let left_percent_of_total = left_stat.map_or(0.0, |stat| stat.percent_of_total);
        let right_percent_of_total = right_stat.map_or(0.0, |stat| stat.percent_of_total);
        if left_duration_ns == right_duration_ns
            && (left_percent_of_total - right_percent_of_total).abs() < f64::EPSILON
        {
            continue;
        }
        changes.push(KernelTimingChange {
            name,
            left_duration_ns,
            right_duration_ns,
            duration_delta_ns: right_duration_ns as i64 - left_duration_ns as i64,
            left_percent_of_total,
            right_percent_of_total,
        });
    }
    changes.sort_by(|left, right| {
        right
            .duration_delta_ns
            .abs()
            .cmp(&left.duration_delta_ns.abs())
            .then_with(|| left.name.cmp(&right.name))
    });
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{BufferLifecycle, BufferStat, TimedKernelStat};
    use crate::trace::{KernelStat, TraceSummary};

    #[test]
    fn computes_kernel_deltas() {
        let left = AnalysisReport {
            trace: TraceSummary {
                trace_name: "left".into(),
                uuid: None,
                capture_version: None,
                graphics_api: None,
                device_id: None,
                capture_len: 0,
                device_resource_count: 0,
                device_resource_bytes: 0,
            },
            timing_synthetic: false,
            total_duration_ns: 100,
            command_buffer_count: 0,
            command_buffer_region_count: 0,
            compute_encoder_count: 0,
            dispatch_count: 0,
            pipeline_function_count: 0,
            kernel_count: 2,
            buffer_count: 0,
            shared_buffer_count: 0,
            single_use_buffer_count: 0,
            short_lived_buffer_count: 0,
            long_lived_buffer_count: 0,
            buffer_inventory_count: 0,
            buffer_inventory_bytes: 0,
            buffer_inventory_aliases: 0,
            unused_resource_count: 0,
            unused_resource_bytes: 0,
            kernel_stats: vec![
                KernelStat {
                    name: "a".into(),
                    pipeline_addr: 1,
                    dispatch_count: 2,
                    encoder_labels: Default::default(),
                    buffers: Default::default(),
                },
                KernelStat {
                    name: "b".into(),
                    pipeline_addr: 2,
                    dispatch_count: 1,
                    encoder_labels: Default::default(),
                    buffers: Default::default(),
                },
            ],
            timed_kernel_stats: vec![
                TimedKernelStat {
                    name: "a".into(),
                    dispatch_count: 2,
                    duration_ns: 50,
                    percent_of_total: 50.0,
                },
                TimedKernelStat {
                    name: "b".into(),
                    dispatch_count: 1,
                    duration_ns: 20,
                    percent_of_total: 20.0,
                },
            ],
            buffer_stats: vec![],
            buffer_lifecycles: vec![],
            largest_buffers: vec![],
            unused_resource_groups: vec![],
            findings: vec![],
        };
        let right = AnalysisReport {
            kernel_stats: vec![
                KernelStat {
                    name: "a".into(),
                    pipeline_addr: 1,
                    dispatch_count: 5,
                    encoder_labels: Default::default(),
                    buffers: Default::default(),
                },
                KernelStat {
                    name: "c".into(),
                    pipeline_addr: 3,
                    dispatch_count: 4,
                    encoder_labels: Default::default(),
                    buffers: Default::default(),
                },
            ],
            timed_kernel_stats: vec![
                TimedKernelStat {
                    name: "a".into(),
                    dispatch_count: 5,
                    duration_ns: 80,
                    percent_of_total: 40.0,
                },
                TimedKernelStat {
                    name: "c".into(),
                    dispatch_count: 4,
                    duration_ns: 90,
                    percent_of_total: 45.0,
                },
            ],
            kernel_count: 2,
            ..left.clone()
        };

        let changes = diff_kernel_stats(&left, &right);
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].name, "c");
        assert_eq!(changes[0].delta, 4);

        let timing_changes = diff_kernel_timing_stats(&left, &right);
        assert_eq!(timing_changes.len(), 3);
        assert_eq!(timing_changes[0].name, "c");
        assert_eq!(timing_changes[0].duration_delta_ns, 90);
    }

    #[test]
    fn maps_perfdata_path_to_raw_capture_sibling() {
        assert_eq!(
            raw_capture_sibling(Path::new("/tmp/sample-perfdata.gputrace")),
            Some(PathBuf::from("/tmp/sample.gputrace"))
        );
        assert_eq!(raw_capture_sibling(Path::new("/tmp/sample.gputrace")), None);
    }

    #[test]
    fn computes_buffer_lifecycle_deltas() {
        let left = AnalysisReport {
            trace: TraceSummary {
                trace_name: "left".into(),
                uuid: None,
                capture_version: None,
                graphics_api: None,
                device_id: None,
                capture_len: 0,
                device_resource_count: 0,
                device_resource_bytes: 0,
            },
            timing_synthetic: true,
            total_duration_ns: 0,
            command_buffer_count: 0,
            command_buffer_region_count: 0,
            compute_encoder_count: 0,
            dispatch_count: 0,
            pipeline_function_count: 0,
            kernel_count: 0,
            buffer_count: 0,
            shared_buffer_count: 0,
            single_use_buffer_count: 0,
            short_lived_buffer_count: 0,
            long_lived_buffer_count: 0,
            buffer_inventory_count: 0,
            buffer_inventory_bytes: 0,
            buffer_inventory_aliases: 0,
            unused_resource_count: 0,
            unused_resource_bytes: 0,
            kernel_stats: vec![],
            timed_kernel_stats: vec![],
            buffer_stats: vec![BufferStat {
                name: "a".into(),
                address: Some(1),
                kernel_count: 1,
                use_count: 2,
                dispatch_count: 2,
                encoder_count: 1,
                command_buffer_count: 1,
                first_dispatch_index: 0,
                last_dispatch_index: 1,
            }],
            buffer_lifecycles: vec![
                BufferLifecycle {
                    name: "a".into(),
                    address: Some(1),
                    first_command_buffer_index: 0,
                    last_command_buffer_index: 0,
                    first_dispatch_index: 0,
                    last_dispatch_index: 1,
                    command_buffer_span: 1,
                    dispatch_span: 2,
                    use_count: 2,
                    kernel_count: 1,
                    encoder_count: 1,
                },
                BufferLifecycle {
                    name: "b".into(),
                    address: Some(2),
                    first_command_buffer_index: 0,
                    last_command_buffer_index: 1,
                    first_dispatch_index: 0,
                    last_dispatch_index: 3,
                    command_buffer_span: 2,
                    dispatch_span: 4,
                    use_count: 3,
                    kernel_count: 2,
                    encoder_count: 2,
                },
            ],
            largest_buffers: vec![],
            unused_resource_groups: vec![],
            findings: vec![],
        };
        let right = AnalysisReport {
            buffer_stats: vec![
                BufferStat {
                    name: "a".into(),
                    address: Some(1),
                    kernel_count: 2,
                    use_count: 5,
                    dispatch_count: 5,
                    encoder_count: 2,
                    command_buffer_count: 3,
                    first_dispatch_index: 0,
                    last_dispatch_index: 5,
                },
                BufferStat {
                    name: "c".into(),
                    address: Some(3),
                    kernel_count: 1,
                    use_count: 1,
                    dispatch_count: 1,
                    encoder_count: 1,
                    command_buffer_count: 1,
                    first_dispatch_index: 7,
                    last_dispatch_index: 7,
                },
            ],
            buffer_lifecycles: vec![
                BufferLifecycle {
                    name: "a".into(),
                    address: Some(1),
                    first_command_buffer_index: 0,
                    last_command_buffer_index: 2,
                    first_dispatch_index: 0,
                    last_dispatch_index: 5,
                    command_buffer_span: 3,
                    dispatch_span: 6,
                    use_count: 4,
                    kernel_count: 2,
                    encoder_count: 2,
                },
                BufferLifecycle {
                    name: "c".into(),
                    address: Some(3),
                    first_command_buffer_index: 1,
                    last_command_buffer_index: 1,
                    first_dispatch_index: 7,
                    last_dispatch_index: 7,
                    command_buffer_span: 1,
                    dispatch_span: 1,
                    use_count: 1,
                    kernel_count: 1,
                    encoder_count: 1,
                },
            ],
            ..left.clone()
        };

        let buffer_changes = diff_buffer_stats(&left, &right);
        assert_eq!(buffer_changes.len(), 2);
        assert_eq!(buffer_changes[0].name, "a");
        assert_eq!(buffer_changes[0].status, BufferChangeStatus::Changed);

        let changes = diff_buffer_lifecycles(&left, &right);
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].name, "a");
        assert_eq!(changes[0].status, BufferChangeStatus::Changed);
        assert_eq!(changes[0].command_buffer_span_delta, 2);
        assert_eq!(changes[0].dispatch_span_delta, 4);
    }

    #[test]
    fn profile_alignment_reports_matched_unmatched_and_outliers() {
        let left = vec![
            profile_dispatch(0, "gemm", 0, Some(11), 100),
            profile_dispatch(1, "copy", 0, Some(12), 20),
            profile_dispatch(2, "norm", 1, Some(13), 50),
        ];
        let right = vec![
            profile_dispatch(0, "gemm", 0, Some(11), 160),
            profile_dispatch(1, "norm", 1, Some(13), 30),
            profile_dispatch(2, "extra", 1, Some(14), 70),
        ];

        let (matches, unmatched_left, unmatched_right) = align_profile_dispatches(&left, &right);
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].function_name, "gemm");
        assert_eq!(matches[0].delta_us, -60);
        assert_eq!(unmatched_left, vec![1]);
        assert_eq!(unmatched_right, vec![2]);

        let function_deltas = build_profile_function_deltas(&left, &right, &matches);
        assert_eq!(function_deltas[0].function_name, "extra");
        assert_eq!(function_deltas[0].total_delta_us, -70);

        let outliers = top_profile_outliers(
            &matches,
            &ProfileDiffOptions {
                limit: 20,
                min_delta_us: 40,
                only_encoder: None,
                only_function: None,
            },
        );
        assert_eq!(outliers.len(), 1);
        assert_eq!(outliers[0].function_name, "gemm");
    }

    fn profile_dispatch(
        source_index: usize,
        function_name: &str,
        encoder_index: usize,
        pipeline_id: Option<i64>,
        duration_us: u64,
    ) -> ProfileDispatch {
        let function_key = if function_name.is_empty() {
            format!("pipeline:{}", pipeline_id.unwrap_or_default())
        } else {
            function_name.to_owned()
        };
        ProfileDispatch {
            source_index,
            function_name: function_name.to_owned(),
            kernel_id: function_key.clone(),
            function_key,
            pipeline_id,
            encoder_index,
            duration_us,
        }
    }
}
