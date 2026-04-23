use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

use crate::analysis::{AnalysisReport, analyze};
use crate::counter_export;
use crate::error::Result;
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
}

pub fn diff_paths(left: impl AsRef<Path>, right: impl AsRef<Path>) -> Result<DiffReport> {
    diff_paths_with_options(left, right, &DiffOptions::default())
}

pub fn diff_paths_with_options(
    left: impl AsRef<Path>,
    right: impl AsRef<Path>,
    options: &DiffOptions,
) -> Result<DiffReport> {
    let left = TraceBundle::open(left)?;
    let right = TraceBundle::open(right)?;
    Ok(diff_with_options(&left, &right, options))
}

pub fn diff(left: &TraceBundle, right: &TraceBundle) -> DiffReport {
    diff_with_options(left, right, &DiffOptions::default())
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
            "Largest profiler metric delta: {} (inv {} -> {}, exec {} -> {}, occ {} -> {}, alu {} -> {}, llc {} -> {}, dev_bw {} -> {}, gpu_r {} -> {}, gpu_w {} -> {}, l1_miss {} -> {}, l1_racc {} -> {}, l1_rbw {} -> {}, l1_wacc {} -> {}, l1_wbw {} -> {})",
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
    let left_trace = load_profile_trace(left, options);
    let right_trace = load_profile_trace(right, options);
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
        matched_pairs: matches,
        unmatched,
        warnings,
    };

    let limit = options.limit.max(1);
    truncate_profile_report(&mut report, limit);
    Some(report)
}

fn load_profile_trace(trace: &TraceBundle, options: &ProfileDiffOptions) -> ProfileTraceData {
    let path = trace.path.display().to_string();
    let label = trace
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(path.as_str())
        .to_owned();
    let mut warnings = Vec::new();
    let summary = match profiler::stream_data_summary(&trace.path) {
        Ok(summary) => summary,
        Err(error) => {
            warnings.push(format!(
                "profile streamData unavailable for {label}: {error}"
            ));
            return ProfileTraceData {
                path,
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
        path,
        label,
        dispatches,
        warnings,
    }
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
        return "no profile timing delta".to_owned();
    }
    let matched_ratio = matched_delta.abs() as f64 / abs_total as f64;
    let unmatched_ratio = unmatched_delta.abs() as f64 / abs_total as f64;
    if matched_ratio >= 0.65 && unmatched_ratio < 0.35 {
        "common matched dispatches changed duration".to_owned()
    } else if unmatched_ratio >= 0.65 && matched_ratio < 0.35 {
        "inserted or removed dispatches dominate".to_owned()
    } else {
        "mixed matched-duration and structural dispatch changes".to_owned()
    }
}

fn truncate_profile_report(report: &mut ProfileDiffReport, limit: usize) {
    report.top_function_deltas.truncate(limit);
    report.top_dispatch_outliers.truncate(limit);
    report.encoder_deltas.truncate(limit);
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
    buffer_l1_miss_rate_percent: Option<f64>,
    buffer_l1_read_accesses: Option<f64>,
    buffer_l1_read_bandwidth_gbps: Option<f64>,
    buffer_l1_write_accesses: Option<f64>,
    buffer_l1_write_bandwidth_gbps: Option<f64>,
}

fn average_option(sum: f64, count: usize) -> Option<f64> {
    (count > 0).then(|| sum / count as f64)
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
