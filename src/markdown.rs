use pulldown_cmark::{Options, Parser, html};

use crate::analysis::AnalysisReport;
use crate::diff::DiffReport;

pub fn render(markdown: &str) -> String {
    let parser = Parser::new_ext(markdown, Options::all());
    let mut html_out = String::new();
    html::push_html(&mut html_out, parser);
    html_out
}

pub fn analysis_report(report: &AnalysisReport) -> String {
    let mut out = String::new();
    out.push_str("# Trace Analysis\n\n");
    push_metric_block(
        &mut out,
        "Trace Summary",
        &[
            ("Trace", report.trace.trace_name.clone()),
            ("Capture bytes", report.trace.capture_len.to_string()),
            (
                "Timing source",
                if report.timing_synthetic {
                    "synthetic".to_owned()
                } else {
                    "profiler".to_owned()
                },
            ),
            (
                "Total kernel time",
                format!("{} ns", report.total_duration_ns),
            ),
            (
                "Device resources",
                format!(
                    "{} files / {} bytes",
                    report.trace.device_resource_count, report.trace.device_resource_bytes
                ),
            ),
        ],
    );
    push_metric_block(
        &mut out,
        "Execution Summary",
        &[
            ("Command buffers", report.command_buffer_count.to_string()),
            (
                "Command buffer regions",
                report.command_buffer_region_count.to_string(),
            ),
            ("Compute encoders", report.compute_encoder_count.to_string()),
            ("Dispatch calls", report.dispatch_count.to_string()),
            (
                "Pipeline mappings",
                report.pipeline_function_count.to_string(),
            ),
            ("Kernels", report.kernel_count.to_string()),
        ],
    );
    push_metric_block(
        &mut out,
        "Buffer Summary",
        &[
            ("Buffers", report.buffer_count.to_string()),
            ("Shared buffers", report.shared_buffer_count.to_string()),
            (
                "Single-use buffers",
                report.single_use_buffer_count.to_string(),
            ),
            (
                "Short-lived buffers",
                report.short_lived_buffer_count.to_string(),
            ),
            (
                "Long-lived buffers",
                report.long_lived_buffer_count.to_string(),
            ),
            (
                "Inventory buffers",
                report.buffer_inventory_count.to_string(),
            ),
            ("Inventory bytes", report.buffer_inventory_bytes.to_string()),
            (
                "Inventory aliases",
                report.buffer_inventory_aliases.to_string(),
            ),
        ],
    );
    if report.findings.is_empty() {
        out.push_str("No findings yet.\n");
    } else {
        push_section(
            &mut out,
            "Findings",
            report.findings.iter().map(String::as_str),
            10,
        );
    }
    if !report.kernel_stats.is_empty() {
        push_section(
            &mut out,
            "Kernels",
            report.kernel_stats.iter().map(|stat| {
                format!(
                    "- `{}`: {} dispatches, {} buffers\n",
                    stat.name,
                    stat.dispatch_count,
                    stat.buffers.len()
                )
            }),
            10,
        );
    }
    if !report.timed_kernel_stats.is_empty() {
        push_section(
            &mut out,
            "Kernel Timing",
            report.timed_kernel_stats.iter().map(|stat| {
                format!(
                    "- `{}`: {} ns, {:.1}% of total, {} dispatches\n",
                    stat.name, stat.duration_ns, stat.percent_of_total, stat.dispatch_count
                )
            }),
            10,
        );
    }
    if !report.buffer_stats.is_empty() {
        push_section(
            &mut out,
            "Buffers",
            report.buffer_stats.iter().map(|stat| {
                format!(
                "- `{}`: {} uses across {} kernels, {} encoders, {} command buffers, dispatches {}..{}\n",
                stat.name,
                stat.use_count,
                stat.kernel_count,
                stat.encoder_count,
                stat.command_buffer_count,
                stat.first_dispatch_index,
                stat.last_dispatch_index
                )
            }),
            10,
        );
    }
    if !report.buffer_lifecycles.is_empty() {
        push_section(
            &mut out,
            "Buffer Lifecycles",
            report.buffer_lifecycles.iter().map(|stat| {
                format!(
                "- `{}`: command buffers {}..{}, dispatches {}..{}, {} total uses, {} encoders\n",
                stat.name,
                stat.first_command_buffer_index,
                stat.last_command_buffer_index,
                stat.first_dispatch_index,
                stat.last_dispatch_index,
                stat.use_count,
                stat.encoder_count
                )
            }),
            10,
        );
    }
    if !report.largest_buffers.is_empty() {
        push_section(
            &mut out,
            "Largest Backing Buffers",
            report.largest_buffers.iter().map(|buffer| {
                format!(
                    "- `{}`: {} bytes, {} aliases, {} bindings\n",
                    buffer.filename, buffer.size, buffer.alias_count, buffer.binding_count
                )
            }),
            10,
        );
    }
    out
}

pub fn diff_report(report: &DiffReport) -> String {
    diff_report_with_limit(report, 10)
}

pub fn diff_report_with_limit(report: &DiffReport, limit: usize) -> String {
    let mut out = String::new();
    out.push_str("# Trace Diff\n\n");
    push_metric_block(
        &mut out,
        "Inputs",
        &[
            ("Left", report.left.trace.trace_name.clone()),
            ("Right", report.right.trace.trace_name.clone()),
        ],
    );
    push_section(
        &mut out,
        "Summary",
        report.summary.iter().map(String::as_str),
        limit,
    );
    if !report.kernel_changes.is_empty() {
        push_section(
            &mut out,
            "Kernel Changes",
            report.kernel_changes.iter().map(|change| {
                format!(
                    "- `{}`: {} -> {} ({:+})\n",
                    change.name, change.left_dispatches, change.right_dispatches, change.delta
                )
            }),
            limit,
        );
    }
    if !report.kernel_timing_changes.is_empty() {
        push_section(
            &mut out,
            "Kernel Timing Changes",
            report.kernel_timing_changes.iter().map(|change| {
                format!(
                    "- `{}`: {} -> {} ns ({:+} ns), {:.1}% -> {:.1}%\n",
                    change.name,
                    change.left_duration_ns,
                    change.right_duration_ns,
                    change.duration_delta_ns,
                    change.left_percent_of_total,
                    change.right_percent_of_total
                )
            }),
            limit,
        );
    }
    if !report.counter_metric_changes.is_empty() {
        push_section(
            &mut out,
            "Profiler Metric Changes",
            report.counter_metric_changes.iter().map(|change| {
                format!(
                    "- `{}`: inv {} -> {}, exec {} -> {}, Occ {} -> {}, ALU {} -> {}, LLC {} -> {}, Dev BW {} -> {}, GPU R {} -> {}, GPU W {} -> {}, Buf Dev R {} -> {}, Buf Dev W {} -> {}, Dev R {} -> {}, Dev W {} -> {}, L1 miss {} -> {}, L1 R acc {} -> {}, L1 R BW {} -> {}, L1 W acc {} -> {}, L1 W BW {} -> {}, CSL util {} -> {}, CF util {} -> {}, IThr util {} -> {}, IC util {} -> {}, ICond util {} -> {}, F32 util {} -> {}\n",
                    change.name,
                    option_metric(change.left_kernel_invocations),
                    option_metric(change.right_kernel_invocations),
                    option_metric(change.left_execution_cost_percent),
                    option_metric(change.right_execution_cost_percent),
                    option_metric(change.left_occupancy_percent),
                    option_metric(change.right_occupancy_percent),
                    option_metric(change.left_alu_utilization_percent),
                    option_metric(change.right_alu_utilization_percent),
                    option_metric(change.left_last_level_cache_percent),
                    option_metric(change.right_last_level_cache_percent),
                    option_metric(change.left_device_memory_bandwidth_gbps),
                    option_metric(change.right_device_memory_bandwidth_gbps),
                    option_metric(change.left_gpu_read_bandwidth_gbps),
                    option_metric(change.right_gpu_read_bandwidth_gbps),
                    option_metric(change.left_gpu_write_bandwidth_gbps),
                    option_metric(change.right_gpu_write_bandwidth_gbps),
                    option_metric(change.left_buffer_device_memory_bytes_read),
                    option_metric(change.right_buffer_device_memory_bytes_read),
                    option_metric(change.left_buffer_device_memory_bytes_written),
                    option_metric(change.right_buffer_device_memory_bytes_written),
                    option_metric(change.left_bytes_read_from_device_memory),
                    option_metric(change.right_bytes_read_from_device_memory),
                    option_metric(change.left_bytes_written_to_device_memory),
                    option_metric(change.right_bytes_written_to_device_memory),
                    option_metric(change.left_buffer_l1_miss_rate_percent),
                    option_metric(change.right_buffer_l1_miss_rate_percent),
                    option_metric(change.left_buffer_l1_read_accesses),
                    option_metric(change.right_buffer_l1_read_accesses),
                    option_metric(change.left_buffer_l1_read_bandwidth_gbps),
                    option_metric(change.right_buffer_l1_read_bandwidth_gbps),
                    option_metric(change.left_buffer_l1_write_accesses),
                    option_metric(change.right_buffer_l1_write_accesses),
                    option_metric(change.left_buffer_l1_write_bandwidth_gbps),
                    option_metric(change.right_buffer_l1_write_bandwidth_gbps),
                    option_metric(change.left_compute_shader_launch_utilization_percent),
                    option_metric(change.right_compute_shader_launch_utilization_percent),
                    option_metric(change.left_control_flow_utilization_percent),
                    option_metric(change.right_control_flow_utilization_percent),
                    option_metric(change.left_instruction_throughput_utilization_percent),
                    option_metric(change.right_instruction_throughput_utilization_percent),
                    option_metric(change.left_integer_complex_utilization_percent),
                    option_metric(change.right_integer_complex_utilization_percent),
                    option_metric(change.left_integer_conditional_utilization_percent),
                    option_metric(change.right_integer_conditional_utilization_percent),
                    option_metric(change.left_f32_utilization_percent),
                    option_metric(change.right_f32_utilization_percent),
                )
            }),
            limit,
        );
    }
    if let Some(profile) = &report.profile_diff {
        push_metric_block(
            &mut out,
            "Profile Diff",
            &[
                (
                    "Total GPU delta",
                    format!(
                        "{:+} us ({} -> {})",
                        profile.summary.total_delta_us,
                        profile.summary.left_total_gpu_time_us,
                        profile.summary.right_total_gpu_time_us
                    ),
                ),
                (
                    "Dispatch delta",
                    format!(
                        "{:+} ({} -> {})",
                        profile.summary.dispatch_count_delta,
                        profile.summary.left_dispatch_count,
                        profile.summary.right_dispatch_count
                    ),
                ),
                (
                    "Matched delta",
                    format!("{:+} us", profile.summary.matched_delta_us),
                ),
                (
                    "Unmatched delta",
                    format!("{:+} us", profile.summary.unmatched_delta_us),
                ),
                ("Likely cause", profile.summary.likely_cause.clone()),
            ],
        );
        if !profile.top_function_deltas.is_empty() {
            out.push_str("## Top Profile Function Deltas\n\n");
            out.push_str("| Function | Count Left | Count Right | Delta us |\n");
            out.push_str("|---|---:|---:|---:|\n");
            for delta in profile.top_function_deltas.iter().take(limit) {
                out.push_str(&format!(
                    "| `{}` | {} | {} | {:+} |\n",
                    escape_markdown_table_cell(&delta.function_name),
                    delta.left_dispatch_count,
                    delta.right_dispatch_count,
                    delta.total_delta_us
                ));
            }
            out.push('\n');
        }
        if !profile.top_dispatch_outliers.is_empty() {
            out.push_str("## Top Profile Dispatch Outliers\n\n");
            out.push_str(
                "| Left idx | Right idx | Encoder | Function | Left us | Right us | Delta us |\n",
            );
            out.push_str("|---:|---:|---:|---|---:|---:|---:|\n");
            for pair in profile.top_dispatch_outliers.iter().take(limit) {
                out.push_str(&format!(
                    "| {} | {} | {} | `{}` | {} | {} | {:+} |\n",
                    pair.left_source_index,
                    pair.right_source_index,
                    pair.encoder_index,
                    escape_markdown_table_cell(&pair.function_name),
                    pair.left_duration_us,
                    pair.right_duration_us,
                    pair.delta_us
                ));
            }
            out.push('\n');
        }
        if !profile.timeline_spike_windows.is_empty() {
            out.push_str("## Profile Spike Windows\n\n");
            out.push_str("| Encoder | Left start | Left end | Right start | Right end | Matches | Cumulative delta us | Max abs delta us |\n");
            out.push_str("|---:|---:|---:|---:|---:|---:|---:|---:|\n");
            for window in profile.timeline_spike_windows.iter().take(limit) {
                out.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {:+} | {} |\n",
                    window.encoder_index,
                    window.left_start_source_index,
                    window.left_end_source_index,
                    window.right_start_source_index,
                    window.right_end_source_index,
                    window.match_count,
                    window.total_delta_us,
                    window.max_abs_delta_us
                ));
            }
            out.push('\n');
        }
        if !profile.encoder_reports.is_empty() {
            out.push_str("## Profile Encoder Focus\n\n");
            out.push_str("| Encoder | Count Left | Count Right | Matched | Matched delta us | Unmatched | Unmatched delta us |\n");
            out.push_str("|---:|---:|---:|---:|---:|---:|---:|\n");
            for encoder in profile.encoder_reports.iter().take(limit) {
                out.push_str(&format!(
                    "| {} | {} | {} | {} | {:+} | {} | {:+} |\n",
                    encoder.encoder_index,
                    encoder.left_dispatch_count,
                    encoder.right_dispatch_count,
                    encoder.matched_count,
                    encoder.matched_delta_us,
                    encoder.unmatched_count,
                    encoder.unmatched_delta_us
                ));
            }
            out.push('\n');
        }
        if !profile.unnamed_dispatch_deltas.is_empty() {
            out.push_str("## Unnamed Profile Dispatch Deltas\n\n");
            out.push_str(
                "| Pipeline | Kernel id | Count Left | Count Right | Left us | Right us | Delta us |\n",
            );
            out.push_str("|---:|---|---:|---:|---:|---:|---:|\n");
            for delta in profile.unnamed_dispatch_deltas.iter().take(limit) {
                out.push_str(&format!(
                    "| {} | `{}` | {} | {} | {} | {} | {:+} |\n",
                    delta
                        .pipeline_id
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_owned()),
                    escape_markdown_table_cell(&delta.kernel_id),
                    delta.left_dispatch_count,
                    delta.right_dispatch_count,
                    delta.left_total_us,
                    delta.right_total_us,
                    delta.total_delta_us
                ));
            }
            out.push('\n');
        }
        if !profile.unmatched.is_empty() {
            out.push_str("## Unmatched Profile Dispatches\n\n");
            out.push_str("| Trace | Index | Encoder | Function | Duration us |\n");
            out.push_str("|---|---:|---:|---|---:|\n");
            for dispatch in profile.unmatched.iter().take(limit) {
                out.push_str(&format!(
                    "| {} | {} | {} | `{}` | {} |\n",
                    dispatch.trace,
                    dispatch.source_index,
                    dispatch.encoder_index,
                    escape_markdown_table_cell(&dispatch.function_name),
                    dispatch.duration_us
                ));
            }
            out.push('\n');
        }
    }
    if !report.buffer_changes.is_empty() {
        push_section(
            &mut out,
            "Buffer Changes",
            report.buffer_changes.iter().map(|change| {
                format!(
                "- `{}` [{}]: uses {} -> {} ({:+}), encoders {} -> {}, command buffers {} -> {}\n",
                change.name,
                buffer_change_status(change.status),
                change.left_uses,
                change.right_uses,
                change.delta,
                change.left_encoders,
                change.right_encoders,
                change.left_command_buffers,
                change.right_command_buffers
                )
            }),
            limit,
        );
    }
    if !report.buffer_lifecycle_changes.is_empty() {
        push_section(
            &mut out,
            "Buffer Lifetime Changes",
            report.buffer_lifecycle_changes.iter().map(|change| {
                format!(
                    "- `{}` [{}]: command buffers {} -> {} ({:+}), dispatches {} -> {} ({:+})\n",
                    change.name,
                    buffer_change_status(change.status),
                    change.left_command_buffer_span,
                    change.right_command_buffer_span,
                    change.command_buffer_span_delta,
                    change.left_dispatch_span,
                    change.right_dispatch_span,
                    change.dispatch_span_delta
                )
            }),
            limit,
        );
    }
    out
}

fn push_metric_block(out: &mut String, title: &str, metrics: &[(&str, String)]) {
    out.push_str(&format!("## {title}\n\n"));
    for (label, value) in metrics {
        out.push_str(&format!("* {label}: `{value}`\n"));
    }
    out.push('\n');
}

fn option_metric(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}"))
        .unwrap_or_else(|| "-".to_owned())
}

fn escape_markdown_table_cell(value: &str) -> String {
    value.replace('|', r"\|")
}

fn push_section(
    out: &mut String,
    title: &str,
    lines: impl IntoIterator<Item = impl AsRef<str>>,
    limit: usize,
) {
    let lines: Vec<String> = lines
        .into_iter()
        .map(|line| line.as_ref().trim_end().to_owned())
        .collect();
    if lines.is_empty() {
        return;
    }

    out.push_str(&format!("\n## {title}\n\n"));
    for line in lines.iter().take(limit) {
        out.push_str(line);
        out.push('\n');
    }
    if lines.len() > limit {
        out.push_str(&format!("_Showing {limit} of {} entries._\n", lines.len()));
    }
}

fn buffer_change_status(status: crate::diff::BufferChangeStatus) -> &'static str {
    match status {
        crate::diff::BufferChangeStatus::Added => "added",
        crate::diff::BufferChangeStatus::Removed => "removed",
        crate::diff::BufferChangeStatus::Changed => "changed",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::analysis::{
        AnalysisReport, BufferLifecycle, BufferStat, InventoryBuffer, TimedKernelStat,
    };
    use crate::diff::{
        BufferChange, BufferChangeStatus, BufferLifecycleChange, DiffReport, KernelChange,
        KernelTimingChange,
    };
    use crate::trace::{KernelStat, TraceSummary};

    use super::*;

    #[test]
    fn renders_markdown_to_html() {
        let html = render("# Title");
        assert!(html.contains("<h1>Title</h1>"));
    }

    #[test]
    fn formats_analysis_report_with_sections_and_limits() {
        let report = AnalysisReport {
            trace: TraceSummary {
                trace_name: "sample.gputrace".into(),
                uuid: None,
                capture_version: None,
                graphics_api: None,
                device_id: None,
                capture_len: 1024,
                device_resource_count: 2,
                device_resource_bytes: 2048,
            },
            timing_synthetic: false,
            total_duration_ns: 12_345,
            command_buffer_count: 3,
            command_buffer_region_count: 3,
            compute_encoder_count: 4,
            dispatch_count: 12,
            pipeline_function_count: 2,
            kernel_count: 11,
            buffer_count: 2,
            shared_buffer_count: 1,
            single_use_buffer_count: 0,
            short_lived_buffer_count: 1,
            long_lived_buffer_count: 0,
            buffer_inventory_count: 2,
            buffer_inventory_bytes: 4096,
            buffer_inventory_aliases: 1,
            kernel_stats: (0..11)
                .map(|index| KernelStat {
                    name: format!("kernel_{index}"),
                    pipeline_addr: index as u64,
                    dispatch_count: index + 1,
                    encoder_labels: BTreeMap::new(),
                    buffers: BTreeMap::new(),
                })
                .collect(),
            timed_kernel_stats: vec![TimedKernelStat {
                name: "kernel_0".into(),
                dispatch_count: 1,
                duration_ns: 500,
                percent_of_total: 25.0,
            }],
            buffer_stats: vec![BufferStat {
                name: "buf".into(),
                address: Some(1),
                kernel_count: 2,
                use_count: 7,
                dispatch_count: 7,
                encoder_count: 2,
                command_buffer_count: 1,
                first_dispatch_index: 0,
                last_dispatch_index: 6,
            }],
            buffer_lifecycles: vec![BufferLifecycle {
                name: "buf".into(),
                address: Some(1),
                first_command_buffer_index: 0,
                last_command_buffer_index: 1,
                first_dispatch_index: 0,
                last_dispatch_index: 6,
                command_buffer_span: 2,
                dispatch_span: 7,
                use_count: 7,
                kernel_count: 2,
                encoder_count: 2,
            }],
            largest_buffers: vec![InventoryBuffer {
                filename: "buf.bin".into(),
                size: 8192,
                alias_count: 1,
                binding_count: 2,
            }],
            findings: vec!["top level summary".into()],
        };

        let rendered = analysis_report(&report);
        assert!(rendered.contains("## Trace Summary"));
        assert!(rendered.contains("## Execution Summary"));
        assert!(rendered.contains("## Buffer Summary"));
        assert!(rendered.contains("## Findings"));
        assert!(rendered.contains("## Kernel Timing"));
        assert!(rendered.contains("_Showing 10 of 11 entries._"));
        assert!(rendered.contains("- `kernel_0`: 1 dispatches, 0 buffers"));
    }

    #[test]
    fn formats_diff_report_with_summary_section_and_limits() {
        let base = AnalysisReport {
            trace: TraceSummary {
                trace_name: "left".into(),
                uuid: None,
                capture_version: None,
                graphics_api: None,
                device_id: None,
                capture_len: 1,
                device_resource_count: 0,
                device_resource_bytes: 0,
            },
            timing_synthetic: false,
            total_duration_ns: 1_000,
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
            buffer_stats: vec![],
            buffer_lifecycles: vec![],
            largest_buffers: vec![],
            findings: vec![],
        };
        let report = DiffReport {
            left: base.clone(),
            right: AnalysisReport {
                trace: TraceSummary {
                    trace_name: "right".into(),
                    ..base.trace.clone()
                },
                ..base
            },
            buffer_changes: vec![BufferChange {
                name: "buf".into(),
                status: BufferChangeStatus::Changed,
                left_uses: 1,
                right_uses: 3,
                left_encoders: 1,
                right_encoders: 2,
                left_command_buffers: 1,
                right_command_buffers: 2,
                delta: 2,
            }],
            buffer_lifecycle_changes: vec![BufferLifecycleChange {
                name: "buf".into(),
                status: BufferChangeStatus::Added,
                left_command_buffer_span: 0,
                right_command_buffer_span: 2,
                command_buffer_span_delta: 2,
                left_dispatch_span: 0,
                right_dispatch_span: 4,
                dispatch_span_delta: 4,
            }],
            kernel_timing_changes: vec![KernelTimingChange {
                name: "kernel".into(),
                left_duration_ns: 100,
                right_duration_ns: 250,
                duration_delta_ns: 150,
                left_percent_of_total: 10.0,
                right_percent_of_total: 25.0,
            }],
            counter_metric_changes: vec![crate::diff::CounterMetricChange {
                name: "kernel".into(),
                left_kernel_invocations: Some(2.0),
                right_kernel_invocations: Some(5.0),
                left_execution_cost_percent: Some(40.0),
                right_execution_cost_percent: Some(55.0),
                left_occupancy_percent: Some(22.0),
                right_occupancy_percent: Some(31.0),
                left_alu_utilization_percent: Some(35.0),
                right_alu_utilization_percent: Some(48.0),
                left_last_level_cache_percent: Some(2.0),
                right_last_level_cache_percent: Some(6.0),
                left_device_memory_bandwidth_gbps: Some(4.0),
                right_device_memory_bandwidth_gbps: Some(9.0),
                left_gpu_read_bandwidth_gbps: Some(2.0),
                right_gpu_read_bandwidth_gbps: Some(4.0),
                left_gpu_write_bandwidth_gbps: Some(1.0),
                right_gpu_write_bandwidth_gbps: Some(2.0),
                left_buffer_device_memory_bytes_read: Some(1024.0),
                right_buffer_device_memory_bytes_read: Some(2048.0),
                left_buffer_device_memory_bytes_written: Some(512.0),
                right_buffer_device_memory_bytes_written: Some(1024.0),
                left_bytes_read_from_device_memory: Some(4096.0),
                right_bytes_read_from_device_memory: Some(8192.0),
                left_bytes_written_to_device_memory: Some(128.0),
                right_bytes_written_to_device_memory: Some(256.0),
                left_buffer_l1_miss_rate_percent: Some(3.0),
                right_buffer_l1_miss_rate_percent: Some(7.0),
                left_buffer_l1_read_accesses: Some(32.0),
                right_buffer_l1_read_accesses: Some(48.0),
                left_buffer_l1_read_bandwidth_gbps: Some(1.5),
                right_buffer_l1_read_bandwidth_gbps: Some(2.5),
                left_buffer_l1_write_accesses: Some(8.0),
                right_buffer_l1_write_accesses: Some(16.0),
                left_buffer_l1_write_bandwidth_gbps: Some(0.5),
                right_buffer_l1_write_bandwidth_gbps: Some(1.5),
                left_compute_shader_launch_utilization_percent: Some(12.0),
                right_compute_shader_launch_utilization_percent: Some(13.0),
                left_control_flow_utilization_percent: Some(14.0),
                right_control_flow_utilization_percent: Some(15.0),
                left_instruction_throughput_utilization_percent: Some(16.0),
                right_instruction_throughput_utilization_percent: Some(17.0),
                left_integer_complex_utilization_percent: Some(18.0),
                right_integer_complex_utilization_percent: Some(19.0),
                left_integer_conditional_utilization_percent: Some(20.0),
                right_integer_conditional_utilization_percent: Some(21.0),
                left_f32_utilization_percent: Some(22.0),
                right_f32_utilization_percent: Some(23.0),
            }],
            profile_diff: None,
            kernel_changes: (0..11)
                .map(|index| KernelChange {
                    name: format!("kernel_{index}"),
                    left_dispatches: index,
                    right_dispatches: index + 1,
                    delta: 1,
                })
                .collect(),
            summary: vec!["dispatch count changed".into()],
        };

        let rendered = diff_report(&report);
        assert!(rendered.contains("## Inputs"));
        assert!(rendered.contains("## Summary"));
        assert!(rendered.contains("## Kernel Changes"));
        assert!(rendered.contains("## Profiler Metric Changes"));
        assert!(rendered.contains("## Buffer Changes"));
        assert!(rendered.contains("## Buffer Lifetime Changes"));
        assert!(rendered.contains(
            "`kernel`: inv 2.00 -> 5.00, exec 40.00 -> 55.00, Occ 22.00 -> 31.00, ALU 35.00 -> 48.00, LLC 2.00 -> 6.00, Dev BW 4.00 -> 9.00, GPU R 2.00 -> 4.00"
        ));
        assert!(rendered.contains(
            "`buf` [changed]: uses 1 -> 3 (+2), encoders 1 -> 2, command buffers 1 -> 2"
        ));
        assert!(rendered.contains("_Showing 10 of 11 entries._"));

        let limited = diff_report_with_limit(&report, 3);
        assert!(limited.contains("_Showing 3 of 11 entries._"));
    }
}
