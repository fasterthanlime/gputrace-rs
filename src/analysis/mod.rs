use serde::Serialize;

use crate::trace::{KernelStat, TraceBundle, TraceSummary};

#[derive(Debug, Clone, Serialize)]
pub struct AnalysisReport {
    pub trace: TraceSummary,
    pub command_buffer_count: usize,
    pub command_buffer_region_count: usize,
    pub compute_encoder_count: usize,
    pub dispatch_count: usize,
    pub pipeline_function_count: usize,
    pub kernel_count: usize,
    pub kernel_stats: Vec<KernelStat>,
    pub findings: Vec<String>,
}

pub fn analyze(trace: &TraceBundle) -> AnalysisReport {
    let summary = trace.summary();
    let command_buffers = trace.command_buffers().unwrap_or_default();
    let command_buffer_regions = trace.command_buffer_regions().unwrap_or_default();
    let compute_encoders = trace.compute_encoders().unwrap_or_default();
    let dispatches = trace.dispatch_calls().unwrap_or_default();
    let pipeline_function_map = trace.pipeline_function_map().unwrap_or_default();
    let mut kernel_stats: Vec<_> = trace
        .analyze_kernels()
        .unwrap_or_default()
        .into_values()
        .collect();
    kernel_stats.sort_by(|left, right| {
        right
            .dispatch_count
            .cmp(&left.dispatch_count)
            .then_with(|| left.name.cmp(&right.name))
    });
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
    findings.push(format!(
        "Command buffer regions: {}",
        command_buffer_regions.len()
    ));
    findings.push(format!("Compute encoders: {}", compute_encoders.len()));
    findings.push(format!("Dispatch calls: {}", dispatches.len()));
    if !pipeline_function_map.is_empty() {
        findings.push(format!(
            "Resolved {} pipeline-to-function mappings.",
            pipeline_function_map.len()
        ));
    }
    if let Some(top_kernel) = kernel_stats.first() {
        findings.push(format!(
            "Top kernel: {} ({} dispatches)",
            top_kernel.name, top_kernel.dispatch_count
        ));
    }

    AnalysisReport {
        trace: summary,
        command_buffer_count: command_buffers.len(),
        command_buffer_region_count: command_buffer_regions.len(),
        compute_encoder_count: compute_encoders.len(),
        dispatch_count: dispatches.len(),
        pipeline_function_count: pipeline_function_map.len(),
        kernel_count: kernel_stats.len(),
        kernel_stats,
        findings,
    }
}
