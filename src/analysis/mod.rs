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
    pub buffer_count: usize,
    pub kernel_stats: Vec<KernelStat>,
    pub buffer_stats: Vec<BufferStat>,
    pub findings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferStat {
    pub name: String,
    pub kernel_count: usize,
    pub use_count: usize,
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
    let buffer_stats = summarize_buffers(&kernel_stats);
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

fn summarize_buffers(kernel_stats: &[KernelStat]) -> Vec<BufferStat> {
    let mut buffers: std::collections::BTreeMap<String, BufferStat> =
        std::collections::BTreeMap::new();
    for kernel in kernel_stats {
        for (buffer_name, use_count) in &kernel.buffers {
            let entry = buffers
                .entry(buffer_name.clone())
                .or_insert_with(|| BufferStat {
                    name: buffer_name.clone(),
                    kernel_count: 0,
                    use_count: 0,
                });
            entry.kernel_count += 1;
            entry.use_count += use_count;
        }
    }
    let mut values: Vec<_> = buffers.into_values().collect();
    values.sort_by(|left, right| {
        right
            .use_count
            .cmp(&left.use_count)
            .then_with(|| right.kernel_count.cmp(&left.kernel_count))
            .then_with(|| left.name.cmp(&right.name))
    });
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::KernelStat;

    #[test]
    fn summarizes_buffers_across_kernels() {
        let stats = vec![
            KernelStat {
                name: "a".into(),
                pipeline_addr: 1,
                dispatch_count: 2,
                encoder_labels: Default::default(),
                buffers: [("buf1".into(), 2), ("buf2".into(), 1)]
                    .into_iter()
                    .collect(),
            },
            KernelStat {
                name: "b".into(),
                pipeline_addr: 2,
                dispatch_count: 3,
                encoder_labels: Default::default(),
                buffers: [("buf1".into(), 4)].into_iter().collect(),
            },
        ];

        let buffers = summarize_buffers(&stats);
        assert_eq!(buffers.len(), 2);
        assert_eq!(buffers[0].name, "buf1");
        assert_eq!(buffers[0].kernel_count, 2);
        assert_eq!(buffers[0].use_count, 6);
    }
}
