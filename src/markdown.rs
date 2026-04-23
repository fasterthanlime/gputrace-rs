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
    out
}
