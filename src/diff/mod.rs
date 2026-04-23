use serde::Serialize;
use std::path::Path;

use crate::analysis::{AnalysisReport, analyze};
use crate::error::Result;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct DiffReport {
    pub left: AnalysisReport,
    pub right: AnalysisReport,
    pub kernel_changes: Vec<KernelChange>,
    pub summary: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KernelChange {
    pub name: String,
    pub left_dispatches: usize,
    pub right_dispatches: usize,
    pub delta: isize,
}

pub fn diff_paths(left: impl AsRef<Path>, right: impl AsRef<Path>) -> Result<DiffReport> {
    let left = TraceBundle::open(left)?;
    let right = TraceBundle::open(right)?;
    Ok(diff(&left, &right))
}

pub fn diff(left: &TraceBundle, right: &TraceBundle) -> DiffReport {
    let left_report = analyze(left);
    let right_report = analyze(right);
    let kernel_changes = diff_kernel_stats(&left_report, &right_report);
    let mut summary = Vec::new();

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
    if let Some(change) = kernel_changes.first() {
        summary.push(format!(
            "Largest kernel dispatch delta: {} ({} -> {}, delta {:+})",
            change.name, change.left_dispatches, change.right_dispatches, change.delta
        ));
    }
    if summary.is_empty() {
        summary.push("No high-level differences detected yet.".to_owned());
    }

    DiffReport {
        left: left_report,
        right: right_report,
        kernel_changes,
        summary,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
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
            command_buffer_count: 0,
            command_buffer_region_count: 0,
            compute_encoder_count: 0,
            dispatch_count: 0,
            pipeline_function_count: 0,
            kernel_count: 2,
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
            kernel_count: 2,
            ..left.clone()
        };

        let changes = diff_kernel_stats(&left, &right);
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].name, "c");
        assert_eq!(changes[0].delta, 4);
    }
}
