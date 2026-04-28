use serde::Serialize;

use crate::buffers;
use crate::timing;
use crate::trace::{BufferAccessStat, BufferLifecycleStat, KernelStat, TraceBundle, TraceSummary};
use crate::xcode_mio;

#[derive(Debug, Clone, Serialize)]
pub struct AnalysisReport {
    pub trace: TraceSummary,
    pub timing_synthetic: bool,
    pub total_duration_ns: u64,
    pub command_buffer_count: usize,
    pub command_buffer_region_count: usize,
    pub compute_encoder_count: usize,
    pub dispatch_count: usize,
    pub pipeline_function_count: usize,
    pub kernel_count: usize,
    pub buffer_count: usize,
    pub shared_buffer_count: usize,
    pub single_use_buffer_count: usize,
    pub short_lived_buffer_count: usize,
    pub long_lived_buffer_count: usize,
    pub buffer_inventory_count: usize,
    pub buffer_inventory_bytes: u64,
    pub buffer_inventory_aliases: usize,
    pub unused_resource_count: usize,
    pub unused_resource_bytes: u64,
    pub kernel_stats: Vec<KernelStat>,
    pub timed_kernel_stats: Vec<TimedKernelStat>,
    pub buffer_stats: Vec<BufferStat>,
    pub buffer_lifecycles: Vec<BufferLifecycle>,
    pub largest_buffers: Vec<InventoryBuffer>,
    pub unused_resource_groups: Vec<UnusedResourceGroupSummary>,
    pub xcode_mio: Option<xcode_mio::XcodeMioAnalysisReport>,
    pub findings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimedKernelStat {
    pub name: String,
    pub dispatch_count: usize,
    pub duration_ns: u64,
    pub percent_of_total: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferStat {
    pub name: String,
    pub address: Option<u64>,
    pub kernel_count: usize,
    pub use_count: usize,
    pub dispatch_count: usize,
    pub encoder_count: usize,
    pub command_buffer_count: usize,
    pub first_dispatch_index: usize,
    pub last_dispatch_index: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferLifecycle {
    pub name: String,
    pub address: Option<u64>,
    pub first_command_buffer_index: usize,
    pub last_command_buffer_index: usize,
    pub first_dispatch_index: usize,
    pub last_dispatch_index: usize,
    pub command_buffer_span: usize,
    pub dispatch_span: usize,
    pub use_count: usize,
    pub kernel_count: usize,
    pub encoder_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct InventoryBuffer {
    pub filename: String,
    pub size: u64,
    pub alias_count: usize,
    pub binding_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnusedResourceGroupSummary {
    pub label: String,
    pub count: usize,
    pub logical_bytes: u64,
    pub sample_buffers: Vec<String>,
}

pub fn analyze(trace: &TraceBundle) -> AnalysisReport {
    analyze_with_xcode_mio(trace, None)
}

pub fn analyze_with_xcode_mio(
    trace: &TraceBundle,
    precomputed_xcode_mio: Option<xcode_mio::XcodeMioAnalysisReport>,
) -> AnalysisReport {
    analyze_with_context(trace, precomputed_xcode_mio, None)
}

pub fn analyze_with_context(
    trace: &TraceBundle,
    precomputed_xcode_mio: Option<xcode_mio::XcodeMioAnalysisReport>,
    precomputed_timing: Option<timing::TimingReport>,
) -> AnalysisReport {
    let summary = trace.summary();
    let timing = precomputed_timing.or_else(|| timing::report(trace).ok());
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
    let mut buffer_lifecycles: Vec<_> = trace
        .analyze_buffer_lifecycles()
        .unwrap_or_default()
        .into_values()
        .map(to_buffer_lifecycle)
        .collect();
    buffer_lifecycles.sort_by(|left, right| {
        right
            .command_buffer_span
            .cmp(&left.command_buffer_span)
            .then_with(|| right.dispatch_span.cmp(&left.dispatch_span))
            .then_with(|| right.use_count.cmp(&left.use_count))
            .then_with(|| left.name.cmp(&right.name))
    });
    let shared_buffer_count = buffer_stats
        .iter()
        .filter(|buffer| buffer.encoder_count > 1)
        .count();
    let single_use_buffer_count = buffer_stats
        .iter()
        .filter(|buffer| buffer.use_count == 1)
        .count();
    let short_lived_buffer_count = buffer_lifecycles
        .iter()
        .filter(|buffer| buffer.dispatch_span <= 2 && buffer.use_count <= 2)
        .count();
    let average_dispatch_lifetime = if buffer_lifecycles.is_empty() {
        0.0
    } else {
        buffer_lifecycles
            .iter()
            .map(|buffer| buffer.dispatch_span)
            .sum::<usize>() as f64
            / buffer_lifecycles.len() as f64
    };
    let long_lived_buffer_count = buffer_lifecycles
        .iter()
        .filter(|buffer| {
            average_dispatch_lifetime > 0.0
                && buffer.dispatch_span as f64 > average_dispatch_lifetime * 3.0
        })
        .count();
    let inventory = buffers::analyze(trace).ok();
    let xcode_mio = precomputed_xcode_mio.or_else(|| xcode_mio::analysis_report(trace).ok());
    let timed_kernel_stats = timing
        .as_ref()
        .map(|report| {
            report
                .kernels
                .iter()
                .map(|kernel| TimedKernelStat {
                    name: kernel.name.clone(),
                    dispatch_count: kernel.dispatch_count,
                    duration_ns: kernel.synthetic_duration_ns,
                    percent_of_total: kernel.percent_of_total,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let largest_buffers = inventory
        .as_ref()
        .map(|inventory| {
            inventory
                .buffers
                .iter()
                .take(10)
                .map(|buffer| InventoryBuffer {
                    filename: buffer.filename.clone(),
                    size: buffer.size,
                    alias_count: buffer.aliases.len(),
                    binding_count: buffer.binding_count,
                })
                .collect()
        })
        .unwrap_or_default();
    let unused_resource_groups = inventory
        .as_ref()
        .map(|inventory| {
            inventory
                .unused_resources
                .groups
                .iter()
                .take(10)
                .map(|group| UnusedResourceGroupSummary {
                    label: group.label.clone(),
                    count: group.count,
                    logical_bytes: group.logical_bytes,
                    sample_buffers: group.sample_buffers.clone(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
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
    if let Some(timing) = &timing {
        findings.push(format!(
            "Timing source: {} ({} ns total)",
            if timing.synthetic {
                "synthetic"
            } else {
                "profiler"
            },
            timing.total_duration_ns
        ));
    }
    if let Some(report) = &xcode_mio {
        findings.push(format!(
            "Xcode MIO backend: {} commands, {} pipelines, {} cost records, {:.3} ms GPU time.",
            report.gpu_command_count,
            report.pipeline_state_count,
            report.cost_record_count,
            report.gpu_time_ns as f64 / 1_000_000.0
        ));
        if let Some(top) = report.top_pipelines.first() {
            findings.push(format!(
                "Top Xcode MIO pipeline: {} ({} commands, {:.1}% command share).",
                top.function_name.as_deref().unwrap_or("<unknown function>"),
                top.command_count,
                top.command_percent
            ));
        }
    }
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
    if let Some(top_timed_kernel) = timed_kernel_stats.first() {
        findings.push(format!(
            "Top timed kernel: {} ({} ns, {:.1}% of total)",
            top_timed_kernel.name, top_timed_kernel.duration_ns, top_timed_kernel.percent_of_total
        ));
    }
    if let Some(top_buffer) = buffer_stats.first() {
        findings.push(format!(
            "Top buffer: {} ({} uses across {} kernels, {} encoders)",
            top_buffer.name,
            top_buffer.use_count,
            top_buffer.kernel_count,
            top_buffer.encoder_count
        ));
    }
    if let Some(longest_lived_buffer) = buffer_lifecycles.first() {
        findings.push(format!(
            "Longest-lived buffer: {} ({} command buffers, {} dispatches, {} encoders)",
            longest_lived_buffer.name,
            longest_lived_buffer.command_buffer_span,
            longest_lived_buffer.dispatch_span,
            longest_lived_buffer.encoder_count
        ));
    }
    if shared_buffer_count > 0 {
        findings.push(format!(
            "Shared buffers: {} touched by more than one encoder.",
            shared_buffer_count
        ));
    }
    if single_use_buffer_count > 0 {
        findings.push(format!(
            "Single-use buffers: {} appear in exactly one attributed dispatch.",
            single_use_buffer_count
        ));
    }
    if short_lived_buffer_count > 0 {
        findings.push(format!(
            "Short-lived buffers: {} live for at most two dispatches.",
            short_lived_buffer_count
        ));
    }
    if long_lived_buffer_count > 0 {
        findings.push(format!(
            "Long-lived buffers: {} span more than 3x the average dispatch lifetime.",
            long_lived_buffer_count
        ));
    }
    if let Some(inventory) = &inventory {
        findings.push(format!(
            "Bundle buffer inventory: {} files, {} bytes, {} aliases.",
            inventory.total_buffers, inventory.total_bytes, inventory.total_aliases
        ));
        if buffer_stats.is_empty() && inventory.total_buffers > 0 {
            findings.push(
                "Structural buffer attribution is unavailable for this bundle; reporting backing-file inventory instead."
                    .to_owned(),
            );
        }
        if let Some(largest) = inventory.buffers.first() {
            findings.push(format!(
                "Largest backing buffer: {} ({} bytes, {} aliases, {} bindings)",
                largest.filename,
                largest.size,
                largest.aliases.len(),
                largest.binding_count
            ));
        }
        if inventory.unused_resources.total_entries > 0 {
            findings.push(format!(
                "Unused resource sidecars: {} entries, {} logical bytes.",
                inventory.unused_resources.total_entries,
                inventory.unused_resources.total_logical_bytes
            ));
        }
        if let Some(group) = inventory.unused_resources.groups.first() {
            findings.push(format!(
                "Top unused resource group: {} ({} entries, {} logical bytes)",
                group.label, group.count, group.logical_bytes
            ));
        }
    }

    AnalysisReport {
        trace: summary,
        timing_synthetic: timing
            .as_ref()
            .map(|report| report.synthetic)
            .unwrap_or(true),
        total_duration_ns: timing
            .as_ref()
            .map(|report| report.total_duration_ns)
            .unwrap_or(0),
        command_buffer_count: command_buffers.len(),
        command_buffer_region_count: command_buffer_regions.len(),
        compute_encoder_count: compute_encoders.len(),
        dispatch_count: dispatches.len(),
        pipeline_function_count: pipeline_function_map.len(),
        kernel_count: kernel_stats.len(),
        buffer_count: buffer_stats.len(),
        shared_buffer_count,
        single_use_buffer_count,
        short_lived_buffer_count,
        long_lived_buffer_count,
        buffer_inventory_count: inventory.as_ref().map_or(0, |inv| inv.total_buffers),
        buffer_inventory_bytes: inventory.as_ref().map_or(0, |inv| inv.total_bytes),
        buffer_inventory_aliases: inventory.as_ref().map_or(0, |inv| inv.total_aliases),
        unused_resource_count: inventory
            .as_ref()
            .map_or(0, |inv| inv.unused_resources.total_entries),
        unused_resource_bytes: inventory
            .as_ref()
            .map_or(0, |inv| inv.unused_resources.total_logical_bytes),
        kernel_stats,
        timed_kernel_stats,
        buffer_stats,
        buffer_lifecycles,
        largest_buffers,
        unused_resource_groups,
        xcode_mio,
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
        encoder_count: buffer.encoder_count,
        command_buffer_count: buffer.command_buffer_count,
        first_dispatch_index: buffer.first_dispatch_index,
        last_dispatch_index: buffer.last_dispatch_index,
    }
}

fn to_buffer_lifecycle(buffer: BufferLifecycleStat) -> BufferLifecycle {
    BufferLifecycle {
        name: buffer.name,
        address: buffer.address,
        first_command_buffer_index: buffer.first_command_buffer_index,
        last_command_buffer_index: buffer.last_command_buffer_index,
        first_dispatch_index: buffer.first_dispatch_index,
        last_dispatch_index: buffer.last_dispatch_index,
        command_buffer_span: buffer.command_buffer_span,
        dispatch_span: buffer.dispatch_span,
        use_count: buffer.use_count,
        kernel_count: buffer.kernels.len(),
        encoder_count: buffer.encoder_count,
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
            encoder_count: 2,
            command_buffer_count: 3,
            first_dispatch_index: 1,
            last_dispatch_index: 9,
            kernels: [("a".into(), 2), ("b".into(), 4)].into_iter().collect(),
        };

        let buffer = to_buffer_stat(stat);
        assert_eq!(buffer.name, "buf1");
        assert_eq!(buffer.address, Some(0x10));
        assert_eq!(buffer.kernel_count, 2);
        assert_eq!(buffer.use_count, 6);
        assert_eq!(buffer.dispatch_count, 6);
        assert_eq!(buffer.encoder_count, 2);
        assert_eq!(buffer.command_buffer_count, 3);
        assert_eq!(buffer.first_dispatch_index, 1);
        assert_eq!(buffer.last_dispatch_index, 9);
    }

    #[test]
    fn converts_buffer_lifecycle_stats() {
        let stat = BufferLifecycleStat {
            name: "buf1".into(),
            address: Some(0x10),
            first_command_buffer_index: 1,
            last_command_buffer_index: 3,
            first_dispatch_index: 2,
            last_dispatch_index: 7,
            command_buffer_span: 3,
            dispatch_span: 6,
            use_count: 4,
            encoder_count: 2,
            kernels: [("a".into(), 2), ("b".into(), 2)].into_iter().collect(),
        };

        let buffer = to_buffer_lifecycle(stat);
        assert_eq!(buffer.name, "buf1");
        assert_eq!(buffer.address, Some(0x10));
        assert_eq!(buffer.command_buffer_span, 3);
        assert_eq!(buffer.dispatch_span, 6);
        assert_eq!(buffer.use_count, 4);
        assert_eq!(buffer.kernel_count, 2);
        assert_eq!(buffer.encoder_count, 2);
    }
}
