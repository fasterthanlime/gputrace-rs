use serde::Serialize;

use crate::trace::{TraceBundle, TraceSummary};

#[derive(Debug, Clone, Serialize)]
pub struct AnalysisReport {
    pub trace: TraceSummary,
    pub command_buffer_count: usize,
    pub compute_encoder_count: usize,
    pub dispatch_count: usize,
    pub pipeline_function_count: usize,
    pub findings: Vec<String>,
}

pub fn analyze(trace: &TraceBundle) -> AnalysisReport {
    let summary = trace.summary();
    let command_buffers = trace.command_buffers().unwrap_or_default();
    let compute_encoders = trace.compute_encoders().unwrap_or_default();
    let dispatches = trace.dispatch_calls().unwrap_or_default();
    let pipeline_function_map = trace.pipeline_function_map().unwrap_or_default();
    let mut findings = Vec::new();

    if summary.device_resource_count == 0 {
        findings.push("No device resource sidecar files were found.".to_owned());
    }
    if summary.capture_len == 0 {
        findings.push("Capture payload is empty.".to_owned());
    }
    if let Some(pointer_size) = trace.metadata.native_pointer_size {
        findings.push(format!("Native pointer size: {pointer_size}"));
    }
    if let Some(frames) = trace.metadata.captured_frames_count {
        findings.push(format!("Captured frames: {frames}"));
    }
    findings.push(format!("Command buffers: {}", command_buffers.len()));
    findings.push(format!("Compute encoders: {}", compute_encoders.len()));
    findings.push(format!("Dispatch calls: {}", dispatches.len()));
    if !pipeline_function_map.is_empty() {
        findings.push(format!(
            "Resolved {} pipeline-to-function mappings.",
            pipeline_function_map.len()
        ));
    }

    AnalysisReport {
        trace: summary,
        command_buffer_count: command_buffers.len(),
        compute_encoder_count: compute_encoders.len(),
        dispatch_count: dispatches.len(),
        pipeline_function_count: pipeline_function_map.len(),
        findings,
    }
}
