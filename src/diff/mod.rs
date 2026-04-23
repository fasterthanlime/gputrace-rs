use serde::Serialize;
use std::path::Path;

use crate::analysis::{AnalysisReport, analyze};
use crate::error::Result;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct DiffReport {
    pub left: AnalysisReport,
    pub right: AnalysisReport,
    pub buffer_changes: Vec<BufferChange>,
    pub buffer_lifecycle_changes: Vec<BufferLifecycleChange>,
    pub kernel_changes: Vec<KernelChange>,
    pub kernel_timing_changes: Vec<KernelTimingChange>,
    pub summary: Vec<String>,
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

pub fn diff_paths(left: impl AsRef<Path>, right: impl AsRef<Path>) -> Result<DiffReport> {
    let left = TraceBundle::open(left)?;
    let right = TraceBundle::open(right)?;
    Ok(diff(&left, &right))
}

pub fn diff(left: &TraceBundle, right: &TraceBundle) -> DiffReport {
    let left_report = analyze(left);
    let right_report = analyze(right);
    let buffer_changes = diff_buffer_stats(&left_report, &right_report);
    let buffer_lifecycle_changes = diff_buffer_lifecycles(&left_report, &right_report);
    let kernel_changes = diff_kernel_stats(&left_report, &right_report);
    let kernel_timing_changes = diff_kernel_timing_stats(&left_report, &right_report);
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
        summary,
    }
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
}
