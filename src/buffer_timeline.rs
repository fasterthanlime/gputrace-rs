use serde::Serialize;

use crate::trace::{BufferLifecycleStat, TraceBundle};

#[derive(Debug, Clone, Serialize)]
pub struct BufferTimelineReport {
    pub total_buffers: usize,
    pub total_allocations: usize,
    pub average_command_buffer_lifetime: f64,
    pub average_dispatch_lifetime: f64,
    pub min_dispatch_index: usize,
    pub max_dispatch_index: usize,
    pub buffers: Vec<BufferTimelineEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferTimelineEntry {
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
}

pub fn analyze(trace: &TraceBundle) -> BufferTimelineReport {
    let mut buffers: Vec<_> = trace
        .analyze_buffer_lifecycles()
        .unwrap_or_default()
        .into_values()
        .map(to_timeline_entry)
        .collect();
    buffers.sort_by(|left, right| {
        left.first_dispatch_index
            .cmp(&right.first_dispatch_index)
            .then_with(|| right.dispatch_span.cmp(&left.dispatch_span))
            .then_with(|| left.name.cmp(&right.name))
    });

    let total_buffers = buffers.len();
    let total_allocations = total_buffers;
    let average_command_buffer_lifetime = if total_buffers == 0 {
        0.0
    } else {
        buffers.iter().map(|b| b.command_buffer_span).sum::<usize>() as f64 / total_buffers as f64
    };
    let average_dispatch_lifetime = if total_buffers == 0 {
        0.0
    } else {
        buffers.iter().map(|b| b.dispatch_span).sum::<usize>() as f64 / total_buffers as f64
    };
    let min_dispatch_index = buffers
        .iter()
        .map(|b| b.first_dispatch_index)
        .min()
        .unwrap_or_default();
    let max_dispatch_index = buffers
        .iter()
        .map(|b| b.last_dispatch_index)
        .max()
        .unwrap_or_default();

    BufferTimelineReport {
        total_buffers,
        total_allocations,
        average_command_buffer_lifetime,
        average_dispatch_lifetime,
        min_dispatch_index,
        max_dispatch_index,
        buffers,
    }
}

pub fn format_summary(report: &BufferTimelineReport) -> String {
    let mut out = String::new();
    out.push_str("=== Buffer Timeline Summary ===\n\n");
    out.push_str("Overall Statistics:\n");
    out.push_str(&format!(
        "  Total Unique Buffers:  {}\n",
        report.total_buffers
    ));
    out.push_str(&format!(
        "  Total Allocations:     {}\n",
        report.total_allocations
    ));
    out.push_str(&format!(
        "  Average CB Lifetime:   {:.1} command buffers\n",
        report.average_command_buffer_lifetime
    ));
    out.push_str(&format!(
        "  Average Dispatch Life: {:.1} dispatches\n",
        report.average_dispatch_lifetime
    ));
    out.push_str(&format!(
        "  Dispatch Range:        {} - {} (span: {})\n\n",
        report.min_dispatch_index,
        report.max_dispatch_index,
        report
            .max_dispatch_index
            .saturating_sub(report.min_dispatch_index)
    ));

    out.push_str("Top 10 Longest-Lived Buffers:\n");
    for (index, buffer) in report
        .buffers
        .iter()
        .sorted_by_lifetime()
        .into_iter()
        .take(10)
        .enumerate()
    {
        out.push_str(&format!(
            "  [{}] {}: {} dispatches, {} command buffers, {} uses\n",
            index + 1,
            display_name(buffer),
            buffer.dispatch_span,
            buffer.command_buffer_span,
            buffer.use_count
        ));
    }
    out.push('\n');

    out.push_str("Top 10 Most Frequently Accessed Buffers:\n");
    for (index, buffer) in report
        .buffers
        .iter()
        .sorted_by_uses()
        .into_iter()
        .take(10)
        .enumerate()
    {
        out.push_str(&format!(
            "  [{}] {}: {} uses across {} kernels\n",
            index + 1,
            display_name(buffer),
            buffer.use_count,
            buffer.kernel_count
        ));
    }
    out.push('\n');

    let short_lived = report
        .buffers
        .iter()
        .filter(|buffer| buffer.dispatch_span <= 2 && buffer.use_count <= 2)
        .count();
    let single_use = report
        .buffers
        .iter()
        .filter(|buffer| buffer.use_count == 1)
        .count();
    let long_lived = report
        .buffers
        .iter()
        .filter(|buffer| {
            buffer.dispatch_span as f64 > report.average_dispatch_lifetime * 3.0
                && report.average_dispatch_lifetime > 0.0
        })
        .count();

    out.push_str("Optimization Insights:\n");
    if short_lived > 0 {
        out.push_str(&format!(
            "  - {} short-lived buffers detected; pooling may reduce churn\n",
            short_lived
        ));
    }
    if single_use > 0 {
        out.push_str(&format!(
            "  - {} buffers are touched only once; review temporary allocations\n",
            single_use
        ));
    }
    if long_lived > 0 {
        out.push_str(&format!(
            "  - {} long-lived buffers span far more dispatches than average\n",
            long_lived
        ));
    }
    if short_lived == 0 && single_use == 0 && long_lived == 0 {
        out.push_str("  - No obvious lifecycle outliers detected\n");
    }

    out
}

pub fn format_ascii(report: &BufferTimelineReport, width: usize) -> String {
    let width = width.max(40);
    let chart_width = width.saturating_sub(24);
    let mut out = String::new();
    out.push_str("=== Buffer Timeline ===\n\n");
    out.push_str("Summary:\n");
    out.push_str(&format!("  Total Buffers:      {}\n", report.total_buffers));
    out.push_str(&format!(
        "  Total Allocations:  {}\n",
        report.total_allocations
    ));
    out.push_str(&format!(
        "  Average Lifetime:   {:.1} dispatches\n",
        report.average_dispatch_lifetime
    ));
    out.push_str(&format!(
        "  Dispatch Range:     {} - {}\n\n",
        report.min_dispatch_index, report.max_dispatch_index
    ));

    out.push_str(&format!(
        "Buffer                  {:>6} {:>6} Timeline\n",
        "uses", "kernels"
    ));

    for buffer in report.buffers.iter().take(20) {
        out.push_str(&format!(
            "{:<22} {:>6} {:>6} {}\n",
            truncate_label(&display_name(buffer), 22),
            buffer.use_count,
            buffer.kernel_count,
            timeline_bar(
                buffer.first_dispatch_index,
                buffer.last_dispatch_index,
                report.min_dispatch_index,
                report.max_dispatch_index,
                chart_width,
            )
        ));
    }

    if report.buffers.len() > 20 {
        out.push_str(&format!(
            "\n... and {} more buffers\n",
            report.buffers.len() - 20
        ));
    }

    out
}

fn timeline_bar(
    first: usize,
    last: usize,
    min_index: usize,
    max_index: usize,
    width: usize,
) -> String {
    let width = width.max(8);
    let range = max_index.saturating_sub(min_index).max(1);
    let start = ((first.saturating_sub(min_index)) * (width - 1)) / range;
    let end = ((last.saturating_sub(min_index)) * (width - 1)) / range;
    let mut bar = vec![' '; width];
    for cell in bar.iter_mut().take(end + 1).skip(start) {
        *cell = '-';
    }
    bar[start] = '[';
    bar[end] = ']';
    bar.into_iter().collect()
}

fn to_timeline_entry(buffer: BufferLifecycleStat) -> BufferTimelineEntry {
    BufferTimelineEntry {
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
    }
}

fn display_name(buffer: &BufferTimelineEntry) -> String {
    match buffer.address {
        Some(address) if buffer.name.starts_with("0x") => format!("0x{address:x}"),
        _ => buffer.name.clone(),
    }
}

fn truncate_label(label: &str, width: usize) -> String {
    if label.len() <= width {
        return label.to_owned();
    }
    let keep = width.saturating_sub(1);
    format!("{}...", &label[..keep])
}

trait BufferTimelineIterExt<'a> {
    fn sorted_by_lifetime(self) -> Vec<&'a BufferTimelineEntry>;
    fn sorted_by_uses(self) -> Vec<&'a BufferTimelineEntry>;
}

impl<'a, I> BufferTimelineIterExt<'a> for I
where
    I: Iterator<Item = &'a BufferTimelineEntry>,
{
    fn sorted_by_lifetime(self) -> Vec<&'a BufferTimelineEntry> {
        let mut entries: Vec<_> = self.collect();
        entries.sort_by(|left, right| {
            right
                .dispatch_span
                .cmp(&left.dispatch_span)
                .then_with(|| right.command_buffer_span.cmp(&left.command_buffer_span))
                .then_with(|| left.name.cmp(&right.name))
        });
        entries
    }

    fn sorted_by_uses(self) -> Vec<&'a BufferTimelineEntry> {
        let mut entries: Vec<_> = self.collect();
        entries.sort_by(|left, right| {
            right
                .use_count
                .cmp(&left.use_count)
                .then_with(|| right.kernel_count.cmp(&left.kernel_count))
                .then_with(|| left.name.cmp(&right.name))
        });
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn draws_timeline_bar() {
        let bar = timeline_bar(2, 6, 0, 8, 9);
        assert_eq!(bar, "  [---]  ");
    }

    #[test]
    fn formats_summary() {
        let report = BufferTimelineReport {
            total_buffers: 1,
            total_allocations: 1,
            average_command_buffer_lifetime: 2.0,
            average_dispatch_lifetime: 4.0,
            min_dispatch_index: 0,
            max_dispatch_index: 4,
            buffers: vec![BufferTimelineEntry {
                name: "buf".into(),
                address: Some(0x10),
                first_command_buffer_index: 0,
                last_command_buffer_index: 1,
                first_dispatch_index: 0,
                last_dispatch_index: 4,
                command_buffer_span: 2,
                dispatch_span: 5,
                use_count: 2,
                kernel_count: 1,
            }],
        };

        let summary = format_summary(&report);
        assert!(summary.contains("Top 10 Longest-Lived Buffers"));
        assert!(summary.contains("buf: 5 dispatches"));
    }
}
