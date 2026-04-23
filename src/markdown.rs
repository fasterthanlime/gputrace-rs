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
    out.push_str(&format!("* Trace: `{}`\n", report.trace.trace_name));
    out.push_str(&format!(
        "* Capture bytes: `{}`\n",
        report.trace.capture_len
    ));
    out.push_str(&format!(
        "* Device resources: `{}` files / `{}` bytes\n\n",
        report.trace.device_resource_count, report.trace.device_resource_bytes
    ));
    out.push_str(&format!(
        "* Command buffers: `{}`\n",
        report.command_buffer_count
    ));
    out.push_str(&format!(
        "* Command buffer regions: `{}`\n",
        report.command_buffer_region_count
    ));
    out.push_str(&format!(
        "* Compute encoders: `{}`\n",
        report.compute_encoder_count
    ));
    out.push_str(&format!("* Dispatch calls: `{}`\n", report.dispatch_count));
    out.push_str(&format!(
        "* Pipeline mappings: `{}`\n",
        report.pipeline_function_count
    ));
    out.push_str(&format!("* Kernels: `{}`\n\n", report.kernel_count));
    out.push_str(&format!("* Buffers: `{}`\n", report.buffer_count));
    out.push_str(&format!(
        "* Shared buffers: `{}`\n",
        report.shared_buffer_count
    ));
    out.push_str(&format!(
        "* Single-use buffers: `{}`\n",
        report.single_use_buffer_count
    ));
    out.push_str(&format!(
        "* Short-lived buffers: `{}`\n",
        report.short_lived_buffer_count
    ));
    out.push_str(&format!(
        "* Long-lived buffers: `{}`\n\n",
        report.long_lived_buffer_count
    ));
    out.push_str(&format!(
        "* Inventory buffers: `{}`\n",
        report.buffer_inventory_count
    ));
    out.push_str(&format!(
        "* Inventory bytes: `{}`\n",
        report.buffer_inventory_bytes
    ));
    out.push_str(&format!(
        "* Inventory aliases: `{}`\n\n",
        report.buffer_inventory_aliases
    ));
    if report.findings.is_empty() {
        out.push_str("No findings yet.\n");
    } else {
        for finding in &report.findings {
            out.push_str(&format!("- {finding}\n"));
        }
    }
    if !report.kernel_stats.is_empty() {
        out.push_str("\n## Kernels\n\n");
        for stat in report.kernel_stats.iter().take(10) {
            out.push_str(&format!(
                "- `{}`: {} dispatches, {} buffers\n",
                stat.name,
                stat.dispatch_count,
                stat.buffers.len()
            ));
        }
    }
    if !report.buffer_stats.is_empty() {
        out.push_str("\n## Buffers\n\n");
        for stat in report.buffer_stats.iter().take(10) {
            out.push_str(&format!(
                "- `{}`: {} uses across {} kernels, {} encoders, {} command buffers, dispatches {}..{}\n",
                stat.name,
                stat.use_count,
                stat.kernel_count,
                stat.encoder_count,
                stat.command_buffer_count,
                stat.first_dispatch_index,
                stat.last_dispatch_index
            ));
        }
    }
    if !report.buffer_lifecycles.is_empty() {
        out.push_str("\n## Buffer Lifecycles\n\n");
        for stat in report.buffer_lifecycles.iter().take(10) {
            out.push_str(&format!(
                "- `{}`: command buffers {}..{}, dispatches {}..{}, {} total uses, {} encoders\n",
                stat.name,
                stat.first_command_buffer_index,
                stat.last_command_buffer_index,
                stat.first_dispatch_index,
                stat.last_dispatch_index,
                stat.use_count,
                stat.encoder_count
            ));
        }
    }
    if !report.largest_buffers.is_empty() {
        out.push_str("\n## Largest Backing Buffers\n\n");
        for buffer in report.largest_buffers.iter().take(10) {
            out.push_str(&format!(
                "- `{}`: {} bytes, {} aliases, {} bindings\n",
                buffer.filename, buffer.size, buffer.alias_count, buffer.binding_count
            ));
        }
    }
    out
}

pub fn diff_report(report: &DiffReport) -> String {
    let mut out = String::new();
    out.push_str("# Trace Diff\n\n");
    out.push_str(&format!("* Left: `{}`\n", report.left.trace.trace_name));
    out.push_str(&format!("* Right: `{}`\n\n", report.right.trace.trace_name));
    for line in &report.summary {
        out.push_str(&format!("- {line}\n"));
    }
    if !report.kernel_changes.is_empty() {
        out.push_str("\n## Kernel Changes\n\n");
        for change in report.kernel_changes.iter().take(10) {
            out.push_str(&format!(
                "- `{}`: {} -> {} ({:+})\n",
                change.name, change.left_dispatches, change.right_dispatches, change.delta
            ));
        }
    }
    if !report.buffer_changes.is_empty() {
        out.push_str("\n## Buffer Changes\n\n");
        for change in report.buffer_changes.iter().take(10) {
            out.push_str(&format!(
                "- `{}` [{}]: uses {} -> {} ({:+}), encoders {} -> {}, command buffers {} -> {}\n",
                change.name,
                match change.status {
                    crate::diff::BufferChangeStatus::Added => "added",
                    crate::diff::BufferChangeStatus::Removed => "removed",
                    crate::diff::BufferChangeStatus::Changed => "changed",
                },
                change.left_uses,
                change.right_uses,
                change.delta,
                change.left_encoders,
                change.right_encoders,
                change.left_command_buffers,
                change.right_command_buffers
            ));
        }
    }
    if !report.buffer_lifecycle_changes.is_empty() {
        out.push_str("\n## Buffer Lifetime Changes\n\n");
        for change in report.buffer_lifecycle_changes.iter().take(10) {
            out.push_str(&format!(
                "- `{}` [{}]: command buffers {} -> {} ({:+}), dispatches {} -> {} ({:+})\n",
                change.name,
                match change.status {
                    crate::diff::BufferChangeStatus::Added => "added",
                    crate::diff::BufferChangeStatus::Removed => "removed",
                    crate::diff::BufferChangeStatus::Changed => "changed",
                },
                change.left_command_buffer_span,
                change.right_command_buffer_span,
                change.command_buffer_span_delta,
                change.left_dispatch_span,
                change.right_dispatch_span,
                change.dispatch_span_delta
            ));
        }
    }
    out
}
