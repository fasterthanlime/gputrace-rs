use serde::Serialize;

use crate::trace::{BufferAccessStat, KernelStat, TraceBundle, TraceSummary};

#[derive(Debug, Clone, Serialize)]
pub struct AnalysisReport {
    pub trace: TraceSummary,
    pub command_buffer_count: usize,
    pub command_buffer_region_count: usize,
    pub compute_encoder_count: usize,
    pub dispatch_count: usize,
    pub pipeline_function_count: usize,
    pub kernel_count: usize,
    pub buffer_count: usize,
    pub kernel_stats: Vec<KernelStat>,
    pub buffer_stats: Vec<BufferStat>,
    pub findings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferStat {
    pub name: String,
    pub address: Option<u64>,
    pub kernel_count: usize,
    pub use_count: usize,
    pub dispatch_count: usize,
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
    let mut buffer_stats: Vec<_> = trace
        .analyze_buffers()
        .unwrap_or_default()
        .into_values()
        .map(to_buffer_stat)
        .collect();
    buffer_stats.sort_by(|left, right| {
        right
            .use_count
            .cmp(&left.use_count)
            .then_with(|| right.kernel_count.cmp(&left.kernel_count))
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
        if !top_kernel.buffers.is_empty() {
            findings.push(format!(
                "Top kernel touches {} distinct bound buffers.",
                top_kernel.buffers.len()
            ));
        }
    }
    if let Some(top_buffer) = buffer_stats.first() {
        findings.push(format!(
            "Top buffer: {} ({} uses across {} kernels)",
            top_buffer.name, top_buffer.use_count, top_buffer.kernel_count
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
        buffer_count: buffer_stats.len(),
        kernel_stats,
        buffer_stats,
        findings,
    }
}

fn to_buffer_stat(buffer: BufferAccessStat) -> BufferStat {
    BufferStat {
        name: buffer.name,
        address: buffer.address,
        kernel_count: buffer.kernels.len(),
        use_count: buffer.use_count,
        dispatch_count: buffer.dispatch_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_buffer_access_stats() {
        let stat = BufferAccessStat {
            name: "buf1".into(),
            address: Some(0x10),
            use_count: 6,
            dispatch_count: 6,
            kernels: [("a".into(), 2), ("b".into(), 4)].into_iter().collect(),
        };

        let buffer = to_buffer_stat(stat);
        assert_eq!(buffer.name, "buf1");
        assert_eq!(buffer.address, Some(0x10));
        assert_eq!(buffer.kernel_count, 2);
        assert_eq!(buffer.use_count, 6);
        assert_eq!(buffer.dispatch_count, 6);
    }
}
