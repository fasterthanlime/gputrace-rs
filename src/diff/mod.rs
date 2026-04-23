use serde::Serialize;
use std::path::Path;

use crate::analysis::{AnalysisReport, analyze};
use crate::error::Result;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct DiffReport {
    pub left: AnalysisReport,
    pub right: AnalysisReport,
    pub summary: Vec<String>,
}

pub fn diff_paths(left: impl AsRef<Path>, right: impl AsRef<Path>) -> Result<DiffReport> {
    let left = TraceBundle::open(left)?;
    let right = TraceBundle::open(right)?;
    Ok(diff(&left, &right))
}

pub fn diff(left: &TraceBundle, right: &TraceBundle) -> DiffReport {
    let left_report = analyze(left);
    let right_report = analyze(right);
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
    if summary.is_empty() {
        summary.push("No high-level differences detected yet.".to_owned());
    }

    DiffReport {
        left: left_report,
        right: right_report,
        summary,
    }
}
