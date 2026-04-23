use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::analysis;
use crate::error::Result;
use crate::timing;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct KernelReport {
    pub total_kernels: usize,
    pub filter: Option<String>,
    pub kernels: Vec<KernelEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KernelEntry {
    pub name: String,
    pub pipeline_addr: u64,
    pub dispatch_count: usize,
    pub encoder_labels: BTreeMap<String, usize>,
    pub buffers: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EncoderReport {
    pub total_encoders: usize,
    pub command_buffer_count: usize,
    pub average_encoders_per_command_buffer: f64,
    pub encoders: Vec<EncoderEntry>,
    pub command_buffers: Vec<CommandBufferEncoderSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EncoderEntry {
    pub index: usize,
    pub label: String,
    pub address: u64,
    pub dispatch_count: usize,
    pub command_buffer_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandBufferEncoderSummary {
    pub index: usize,
    pub encoder_count: usize,
    pub dispatch_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DependencyReport {
    pub total_nodes: usize,
    pub total_edges: usize,
    pub nodes: Vec<DependencyNode>,
    pub edges: Vec<DependencyEdge>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DependencyNode {
    pub id: usize,
    pub label: String,
    pub command_buffer_index: usize,
    pub encoder_label: Option<String>,
    pub kernel_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DependencyEdge {
    pub from: usize,
    pub to: usize,
    pub buffers: Vec<String>,
    pub hazard: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandBuffersReport {
    pub total_command_buffers: usize,
    pub total_encoders: usize,
    pub total_dispatches: usize,
    pub average_encoders_per_buffer: f64,
    pub average_dispatches_per_buffer: f64,
    pub command_buffers: Vec<CommandBufferEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandBufferEntry {
    pub index: usize,
    pub offset: usize,
    pub timestamp_ns: u64,
    pub duration_ns: Option<u64>,
    pub encoder_count: usize,
    pub dispatch_count: usize,
    pub encoders: Vec<CommandBufferEncoderEntry>,
    pub kernels: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandBufferEncoderEntry {
    pub index: usize,
    pub label: String,
    pub address: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferAccessReport {
    pub total_buffers: usize,
    pub shared_buffers: usize,
    pub single_use_buffers: usize,
    pub short_lived_buffers: usize,
    pub long_lived_buffers: usize,
    pub total_encoders: usize,
    pub alias_count: usize,
    pub buffers: Vec<BufferAccessEntry>,
    pub encoders: Vec<BufferAccessEncoderEntry>,
    pub aliasing_instances: Vec<BufferAlias>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferAccessEntry {
    pub name: String,
    pub address: Option<u64>,
    pub use_count: usize,
    pub dispatch_count: usize,
    pub encoder_count: usize,
    pub command_buffer_count: usize,
    pub first_dispatch_index: usize,
    pub last_dispatch_index: usize,
    pub is_shared: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferAccessEncoderEntry {
    pub label: String,
    pub address: u64,
    pub unique_buffers: usize,
    pub total_buffer_uses: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferAlias {
    pub address: u64,
    pub names: Vec<String>,
}

pub fn kernels(trace: &TraceBundle, filter: Option<&str>) -> Result<KernelReport> {
    let filter_lower = filter.map(|value| value.to_ascii_lowercase());
    let mut kernels: Vec<_> = trace
        .analyze_kernels()?
        .into_values()
        .filter(|kernel| {
            filter_lower
                .as_ref()
                .is_none_or(|needle| kernel.name.to_ascii_lowercase().contains(needle))
        })
        .map(|kernel| KernelEntry {
            name: kernel.name,
            pipeline_addr: kernel.pipeline_addr,
            dispatch_count: kernel.dispatch_count,
            encoder_labels: kernel.encoder_labels,
            buffers: kernel.buffers,
        })
        .collect();
    kernels.sort_by(|left, right| {
        right
            .dispatch_count
            .cmp(&left.dispatch_count)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(KernelReport {
        total_kernels: kernels.len(),
        filter: filter.map(ToOwned::to_owned),
        kernels,
    })
}

pub fn format_kernels(report: &KernelReport, verbose: bool) -> String {
    let mut out = String::new();
    if let Some(filter) = &report.filter {
        out.push_str(&format!(
            "{} kernels matching {:?}\n\n",
            report.total_kernels, filter
        ));
    } else {
        out.push_str(&format!("{} kernels\n\n", report.total_kernels));
    }
    out.push_str(&format!(
        "{:<36} {:<18} {:>10}\n",
        "Name", "Pipeline State", "Dispatches"
    ));
    for kernel in &report.kernels {
        out.push_str(&format!(
            "{:<36} 0x{:<16x} {:>10}\n",
            truncate(&kernel.name, 36),
            kernel.pipeline_addr,
            kernel.dispatch_count
        ));
        if verbose {
            if !kernel.encoder_labels.is_empty() {
                out.push_str("           encoders:");
                for (label, count) in kernel.encoder_labels.iter().take(5) {
                    out.push_str(&format!(" {}({})", label, count));
                }
                out.push('\n');
            }
            if !kernel.buffers.is_empty() {
                out.push_str("           buffers:");
                for (name, count) in kernel.buffers.iter().take(5) {
                    out.push_str(&format!(" {}({})", name, count));
                }
                out.push('\n');
            }
        }
    }
    out
}

pub fn encoders(trace: &TraceBundle) -> Result<EncoderReport> {
    let encoders = trace.compute_encoders()?;
    let regions = trace.command_buffer_regions()?;
    let mut dispatch_counts: BTreeMap<u64, usize> = BTreeMap::new();
    let mut command_buffer_sets: BTreeMap<u64, BTreeSet<usize>> = BTreeMap::new();
    let mut command_buffers = Vec::new();

    for region in &regions {
        command_buffers.push(CommandBufferEncoderSummary {
            index: region.command_buffer.index,
            encoder_count: region.encoders.len(),
            dispatch_count: region.dispatches.len(),
        });
        for encoder in &region.encoders {
            command_buffer_sets
                .entry(encoder.address)
                .or_default()
                .insert(region.command_buffer.index);
        }
        for dispatch in &region.dispatches {
            if let Some(encoder_id) = dispatch.encoder_id {
                *dispatch_counts.entry(encoder_id).or_default() += 1;
            }
        }
    }

    let mut entries: Vec<_> = encoders
        .into_iter()
        .map(|encoder| EncoderEntry {
            index: encoder.index,
            label: encoder.label,
            address: encoder.address,
            dispatch_count: dispatch_counts
                .get(&encoder.address)
                .copied()
                .unwrap_or_default(),
            command_buffer_count: command_buffer_sets
                .get(&encoder.address)
                .map_or(0, BTreeSet::len),
        })
        .collect();
    entries.sort_by(|left, right| left.index.cmp(&right.index));

    let command_buffer_count = command_buffers.len();
    let average_encoders_per_command_buffer = if command_buffer_count == 0 {
        0.0
    } else {
        entries.len() as f64 / command_buffer_count as f64
    };

    Ok(EncoderReport {
        total_encoders: entries.len(),
        command_buffer_count,
        average_encoders_per_command_buffer,
        encoders: entries,
        command_buffers,
    })
}

pub fn dependencies(trace: &TraceBundle) -> Result<DependencyReport> {
    let regions = trace.command_buffer_regions()?;
    let mut nodes = Vec::new();
    let mut edges: BTreeMap<(usize, usize), BTreeSet<String>> = BTreeMap::new();
    let mut last_user: BTreeMap<String, usize> = BTreeMap::new();

    for region in regions {
        for dispatch in region.dispatches {
            let label = dispatch
                .kernel_name
                .clone()
                .or_else(|| {
                    region
                        .encoders
                        .iter()
                        .find(|encoder| Some(encoder.address) == dispatch.encoder_id)
                        .map(|encoder| encoder.label.clone())
                })
                .unwrap_or_else(|| format!("dispatch_{}", dispatch.index));

            nodes.push(DependencyNode {
                id: dispatch.index,
                label: label.clone(),
                command_buffer_index: region.command_buffer.index,
                encoder_label: region
                    .encoders
                    .iter()
                    .find(|encoder| Some(encoder.address) == dispatch.encoder_id)
                    .map(|encoder| encoder.label.clone())
                    .filter(|label| !label.is_empty()),
                kernel_name: dispatch.kernel_name.clone(),
            });

            let mut seen_buffers = BTreeSet::new();
            for buffer in dispatch.buffers {
                let buffer_name = buffer
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("0x{:x}", buffer.address));
                if !seen_buffers.insert(buffer_name.clone()) {
                    continue;
                }
                if let Some(previous) = last_user.insert(buffer_name.clone(), dispatch.index)
                    && previous != dispatch.index
                {
                    edges
                        .entry((previous, dispatch.index))
                        .or_default()
                        .insert(buffer_name);
                }
            }
        }
    }

    let edges: Vec<_> = edges
        .into_iter()
        .map(|((from, to), buffers)| DependencyEdge {
            from,
            to,
            buffers: buffers.into_iter().collect(),
            hazard: "RW".to_owned(),
        })
        .collect();

    Ok(DependencyReport {
        total_nodes: nodes.len(),
        total_edges: edges.len(),
        nodes,
        edges,
    })
}

pub fn command_buffers(trace: &TraceBundle) -> Result<CommandBuffersReport> {
    let command_buffers = trace.command_buffers()?;
    let regions = trace.command_buffer_regions()?;
    let timing = timing::report(trace)?;
    let timing_by_index: BTreeMap<_, _> = timing
        .command_buffers
        .into_iter()
        .map(|cb| (cb.index, cb))
        .collect();

    let mut entries = Vec::new();
    let mut total_encoders = 0usize;
    let mut total_dispatches = 0usize;

    for (idx, region) in regions.into_iter().enumerate() {
        let cb = command_buffers.get(idx).unwrap_or(&region.command_buffer);
        let kernels: BTreeSet<_> = region
            .dispatches
            .iter()
            .filter_map(|dispatch| dispatch.kernel_name.clone())
            .collect();
        let timing = timing_by_index.get(&region.command_buffer.index);
        total_encoders += region.encoders.len();
        total_dispatches += region.dispatches.len();
        entries.push(CommandBufferEntry {
            index: region.command_buffer.index,
            offset: cb.offset,
            timestamp_ns: cb.timestamp,
            duration_ns: timing.and_then(|cb| cb.duration_ns),
            encoder_count: region.encoders.len(),
            dispatch_count: region.dispatches.len(),
            encoders: region
                .encoders
                .into_iter()
                .map(|encoder| CommandBufferEncoderEntry {
                    index: encoder.index,
                    label: encoder.label,
                    address: encoder.address,
                })
                .collect(),
            kernels: kernels.into_iter().collect(),
        });
    }

    let total_command_buffers = entries.len();
    let average_encoders_per_buffer = if total_command_buffers == 0 {
        0.0
    } else {
        total_encoders as f64 / total_command_buffers as f64
    };
    let average_dispatches_per_buffer = if total_command_buffers == 0 {
        0.0
    } else {
        total_dispatches as f64 / total_command_buffers as f64
    };

    Ok(CommandBuffersReport {
        total_command_buffers,
        total_encoders,
        total_dispatches,
        average_encoders_per_buffer,
        average_dispatches_per_buffer,
        command_buffers: entries,
    })
}

pub fn buffer_access(trace: &TraceBundle) -> Result<BufferAccessReport> {
    let analysis = analysis::analyze(trace);
    let regions = trace.command_buffer_regions()?;
    let buffer_name_map = trace.buffer_name_map()?;

    let mut encoder_buffers: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    let mut encoder_uses: BTreeMap<u64, usize> = BTreeMap::new();
    let mut encoder_labels: BTreeMap<u64, String> = BTreeMap::new();
    for region in &regions {
        for encoder in &region.encoders {
            encoder_labels
                .entry(encoder.address)
                .or_insert_with(|| encoder.label.clone());
        }
        for dispatch in &region.dispatches {
            if let Some(encoder_id) = dispatch.encoder_id {
                for buffer in &dispatch.buffers {
                    let name = buffer
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("0x{:x}", buffer.address));
                    encoder_buffers.entry(encoder_id).or_default().insert(name);
                    *encoder_uses.entry(encoder_id).or_default() += 1;
                }
            }
        }
    }

    let mut aliases_by_address: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for (address, name) in buffer_name_map {
        aliases_by_address.entry(address).or_default().insert(name);
    }
    let aliasing_instances: Vec<_> = aliases_by_address
        .into_iter()
        .filter_map(|(address, names)| {
            (names.len() > 1).then(|| BufferAlias {
                address,
                names: names.into_iter().collect(),
            })
        })
        .collect();

    let buffers = analysis
        .buffer_stats
        .iter()
        .map(|buffer| BufferAccessEntry {
            name: buffer.name.clone(),
            address: buffer.address,
            use_count: buffer.use_count,
            dispatch_count: buffer.dispatch_count,
            encoder_count: buffer.encoder_count,
            command_buffer_count: buffer.command_buffer_count,
            first_dispatch_index: buffer.first_dispatch_index,
            last_dispatch_index: buffer.last_dispatch_index,
            is_shared: buffer.encoder_count > 1,
        })
        .collect();

    let mut encoders: Vec<_> = encoder_buffers
        .into_iter()
        .map(|(address, buffers)| BufferAccessEncoderEntry {
            label: encoder_labels
                .get(&address)
                .cloned()
                .unwrap_or_else(|| format!("0x{address:x}")),
            address,
            unique_buffers: buffers.len(),
            total_buffer_uses: encoder_uses.get(&address).copied().unwrap_or_default(),
        })
        .collect();
    encoders.sort_by(|left, right| {
        right
            .unique_buffers
            .cmp(&left.unique_buffers)
            .then_with(|| right.total_buffer_uses.cmp(&left.total_buffer_uses))
            .then_with(|| left.label.cmp(&right.label))
    });

    Ok(BufferAccessReport {
        total_buffers: analysis.buffer_count,
        shared_buffers: analysis.shared_buffer_count,
        single_use_buffers: analysis.single_use_buffer_count,
        short_lived_buffers: analysis.short_lived_buffer_count,
        long_lived_buffers: analysis.long_lived_buffer_count,
        total_encoders: encoders.len(),
        alias_count: aliasing_instances.len(),
        buffers,
        encoders,
        aliasing_instances,
    })
}

pub fn format_encoders(report: &EncoderReport, verbose: bool) -> String {
    let mut out = String::new();
    out.push_str(&format!("{} encoders\n", report.total_encoders));
    if verbose {
        out.push_str(&format!(
            "{} command buffers ({:.1} encoders/buffer avg)\n",
            report.command_buffer_count, report.average_encoders_per_command_buffer
        ));
    }
    out.push('\n');
    for encoder in &report.encoders {
        if encoder.label.is_empty() {
            out.push_str(&format!(
                "{:>4}: (unlabeled) 0x{:x}\n",
                encoder.index, encoder.address
            ));
        } else {
            out.push_str(&format!("{:>4}: {}\n", encoder.index, encoder.label));
        }
        if verbose {
            out.push_str(&format!(
                "      address=0x{:x} dispatches={} command_buffers={}\n",
                encoder.address, encoder.dispatch_count, encoder.command_buffer_count
            ));
        }
    }
    if verbose && !report.command_buffers.is_empty() {
        out.push_str("\nCommand buffers:\n");
        for cb in &report.command_buffers {
            out.push_str(&format!(
                "  CB {}: {} encoders, {} dispatches\n",
                cb.index, cb.encoder_count, cb.dispatch_count
            ));
        }
    }
    out
}

pub fn format_dependencies(report: &DependencyReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{} nodes, {} edges\n\n",
        report.total_nodes, report.total_edges
    ));
    for node in &report.nodes {
        out.push_str(&format!("n{}: {}", node.id, node.label));
        if let Some(encoder_label) = &node.encoder_label {
            out.push_str(&format!(" [{}]", encoder_label));
        }
        out.push_str(&format!(" (CB {})\n", node.command_buffer_index));
    }
    if !report.edges.is_empty() {
        out.push_str("\nEdges:\n");
        for edge in &report.edges {
            out.push_str(&format!(
                "  n{} -> n{} [{}] via {}\n",
                edge.from,
                edge.to,
                edge.hazard,
                edge.buffers.join(", ")
            ));
        }
    }
    out
}

pub fn format_dependencies_dot(report: &DependencyReport) -> String {
    let mut out = String::new();
    out.push_str("digraph G {\n");
    out.push_str("  rankdir=LR;\n");
    out.push_str("  node [shape=box, style=filled, fontname=\"Helvetica\"];\n");
    out.push_str("  edge [fontname=\"Helvetica\", fontsize=10];\n");
    for node in &report.nodes {
        let mut label = node.label.clone();
        if label.len() > 50 {
            label = format!("{}...", &label[..47]);
        }
        out.push_str(&format!(
            "  n{} [label=\"{}\"];\n",
            node.id,
            escape_dot(&label)
        ));
    }
    for edge in &report.edges {
        out.push_str(&format!(
            "  n{} -> n{} [label=\"{} ({})\"];\n",
            edge.from,
            edge.to,
            escape_dot(&edge.buffers.join(", ")),
            edge.hazard
        ));
    }
    out.push_str("}\n");
    out
}

pub fn format_command_buffers(report: &CommandBuffersReport, detailed: bool) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{} command buffers\n",
        report.total_command_buffers
    ));
    out.push_str(&format!(
        "total encoders={}, total dispatches={}, {:.1} encoders/buffer, {:.1} dispatches/buffer\n\n",
        report.total_encoders,
        report.total_dispatches,
        report.average_encoders_per_buffer,
        report.average_dispatches_per_buffer
    ));
    for cb in &report.command_buffers {
        out.push_str(&format!(
            "{:>4}: offset=0x{:08x} ts={} duration={} encoders={} dispatches={}\n",
            cb.index,
            cb.offset,
            cb.timestamp_ns,
            cb.duration_ns
                .map(|value| value.to_string())
                .unwrap_or_else(|| "?".to_owned()),
            cb.encoder_count,
            cb.dispatch_count
        ));
        if detailed {
            for encoder in &cb.encoders {
                let label = if encoder.label.is_empty() {
                    format!("0x{:x}", encoder.address)
                } else {
                    encoder.label.clone()
                };
                out.push_str(&format!(
                    "       encoder {:>3}: {} (0x{:x})\n",
                    encoder.index, label, encoder.address
                ));
            }
            if !cb.kernels.is_empty() {
                out.push_str(&format!("       kernels: {}\n", cb.kernels.join(", ")));
            }
        }
    }
    out
}

pub fn format_buffer_access(report: &BufferAccessReport, verbose: bool) -> String {
    let mut out = String::new();
    out.push_str("=== Buffer Access Analysis ===\n\n");
    out.push_str("Summary:\n");
    out.push_str(&format!("  Total Buffers:   {}\n", report.total_buffers));
    out.push_str(&format!(
        "  Shared Buffers:  {} (accessed by multiple encoders)\n",
        report.shared_buffers
    ));
    out.push_str(&format!(
        "  Single-use:      {}\n",
        report.single_use_buffers
    ));
    out.push_str(&format!(
        "  Short-lived:     {}\n",
        report.short_lived_buffers
    ));
    out.push_str(&format!(
        "  Long-lived:      {}\n",
        report.long_lived_buffers
    ));
    out.push_str(&format!("  Total Encoders:  {}\n", report.total_encoders));
    out.push_str(&format!("  Alias Sets:      {}\n\n", report.alias_count));

    if !report.aliasing_instances.is_empty() {
        out.push_str("Potential Aliasing:\n");
        for (index, alias) in report.aliasing_instances.iter().take(10).enumerate() {
            out.push_str(&format!(
                "  [{}] 0x{:016x} -> {}\n",
                index + 1,
                alias.address,
                alias.names.join(", ")
            ));
        }
        out.push('\n');
    }

    let mut shared: Vec<_> = report
        .buffers
        .iter()
        .filter(|buffer| buffer.is_shared)
        .collect();
    shared.sort_by(|left, right| {
        right
            .encoder_count
            .cmp(&left.encoder_count)
            .then_with(|| right.use_count.cmp(&left.use_count))
            .then_with(|| left.name.cmp(&right.name))
    });
    if !shared.is_empty() {
        out.push_str("Top Shared Buffers:\n");
        for (index, buffer) in shared.iter().take(10).enumerate() {
            out.push_str(&format!(
                "  [{}] {} - {} encoders, {} uses\n",
                index + 1,
                buffer.name,
                buffer.encoder_count,
                buffer.use_count
            ));
        }
        out.push('\n');
    }

    if verbose && !report.encoders.is_empty() {
        out.push_str("Per-Encoder Statistics:\n");
        for encoder in &report.encoders {
            out.push_str(&format!(
                "  {}: {} unique buffers, {} total accesses\n",
                encoder.label, encoder.unique_buffers, encoder.total_buffer_uses
            ));
        }
        out.push('\n');
    }

    out.push_str("Optimization Opportunities:\n");
    if report.shared_buffers > 0 {
        out.push_str(&format!(
            "  • {} buffers are shared across encoders\n",
            report.shared_buffers
        ));
    }
    if report.single_use_buffers > 0 {
        out.push_str(&format!(
            "  • {} buffers are only touched once\n",
            report.single_use_buffers
        ));
    }
    if report.short_lived_buffers > 0 {
        out.push_str(&format!(
            "  • {} buffers have short lifetimes and may be poolable\n",
            report.short_lived_buffers
        ));
    }
    if report.alias_count > 0 {
        out.push_str(&format!(
            "  • {} potential alias sets deserve review\n",
            report.alias_count
        ));
    }
    if report.shared_buffers == 0
        && report.single_use_buffers == 0
        && report.short_lived_buffers == 0
        && report.alias_count == 0
    {
        out.push_str("  • No obvious access-pattern outliers detected\n");
    }
    out
}

fn truncate(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_owned();
    }
    let keep = width.saturating_sub(3);
    format!("{}...", &value[..keep])
}

fn escape_dot(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_kernel_report() {
        let report = KernelReport {
            total_kernels: 1,
            filter: Some("copy".into()),
            kernels: vec![KernelEntry {
                name: "copy_kernel".into(),
                pipeline_addr: 0x1234,
                dispatch_count: 7,
                encoder_labels: [("copy".into(), 7)].into_iter().collect(),
                buffers: [("buf".into(), 3)].into_iter().collect(),
            }],
        };

        let rendered = format_kernels(&report, true);
        assert!(rendered.contains("copy_kernel"));
        assert!(rendered.contains("Dispatches"));
        assert!(rendered.contains("encoders:"));
        assert!(rendered.contains("buffers:"));
    }

    #[test]
    fn formats_encoder_report() {
        let report = EncoderReport {
            total_encoders: 1,
            command_buffer_count: 1,
            average_encoders_per_command_buffer: 1.0,
            encoders: vec![EncoderEntry {
                index: 0,
                label: "my_encoder".into(),
                address: 0x55,
                dispatch_count: 2,
                command_buffer_count: 1,
            }],
            command_buffers: vec![CommandBufferEncoderSummary {
                index: 0,
                encoder_count: 1,
                dispatch_count: 2,
            }],
        };

        let rendered = format_encoders(&report, true);
        assert!(rendered.contains("my_encoder"));
        assert!(rendered.contains("dispatches=2"));
        assert!(rendered.contains("CB 0"));
    }

    #[test]
    fn formats_dependency_dot() {
        let report = DependencyReport {
            total_nodes: 2,
            total_edges: 1,
            nodes: vec![
                DependencyNode {
                    id: 0,
                    label: "first".into(),
                    command_buffer_index: 0,
                    encoder_label: Some("enc0".into()),
                    kernel_name: Some("first".into()),
                },
                DependencyNode {
                    id: 1,
                    label: "second".into(),
                    command_buffer_index: 0,
                    encoder_label: Some("enc1".into()),
                    kernel_name: Some("second".into()),
                },
            ],
            edges: vec![DependencyEdge {
                from: 0,
                to: 1,
                buffers: vec!["buf".into()],
                hazard: "RW".into(),
            }],
        };

        let rendered = format_dependencies_dot(&report);
        assert!(rendered.contains("digraph G"));
        assert!(rendered.contains("n0 -> n1"));
        assert!(rendered.contains("buf (RW)"));
    }

    #[test]
    fn formats_command_buffer_report() {
        let report = CommandBuffersReport {
            total_command_buffers: 1,
            total_encoders: 1,
            total_dispatches: 2,
            average_encoders_per_buffer: 1.0,
            average_dispatches_per_buffer: 2.0,
            command_buffers: vec![CommandBufferEntry {
                index: 0,
                offset: 0x20,
                timestamp_ns: 100,
                duration_ns: Some(50),
                encoder_count: 1,
                dispatch_count: 2,
                encoders: vec![CommandBufferEncoderEntry {
                    index: 0,
                    label: "enc".into(),
                    address: 0x33,
                }],
                kernels: vec!["kernel".into()],
            }],
        };

        let rendered = format_command_buffers(&report, true);
        assert!(rendered.contains("1 command buffers"));
        assert!(rendered.contains("offset=0x00000020"));
        assert!(rendered.contains("encoder   0"));
        assert!(rendered.contains("kernels: kernel"));
    }

    #[test]
    fn formats_buffer_access_report() {
        let report = BufferAccessReport {
            total_buffers: 2,
            shared_buffers: 1,
            single_use_buffers: 1,
            short_lived_buffers: 1,
            long_lived_buffers: 0,
            total_encoders: 1,
            alias_count: 1,
            buffers: vec![
                BufferAccessEntry {
                    name: "buf".into(),
                    address: Some(1),
                    use_count: 4,
                    dispatch_count: 4,
                    encoder_count: 2,
                    command_buffer_count: 1,
                    first_dispatch_index: 0,
                    last_dispatch_index: 3,
                    is_shared: true,
                },
                BufferAccessEntry {
                    name: "tmp".into(),
                    address: Some(2),
                    use_count: 1,
                    dispatch_count: 1,
                    encoder_count: 1,
                    command_buffer_count: 1,
                    first_dispatch_index: 3,
                    last_dispatch_index: 3,
                    is_shared: false,
                },
            ],
            encoders: vec![BufferAccessEncoderEntry {
                label: "enc".into(),
                address: 1,
                unique_buffers: 2,
                total_buffer_uses: 5,
            }],
            aliasing_instances: vec![BufferAlias {
                address: 1,
                names: vec!["buf".into(), "buf_alias".into()],
            }],
        };

        let rendered = format_buffer_access(&report, true);
        assert!(rendered.contains("Top Shared Buffers"));
        assert!(rendered.contains("buf - 2 encoders, 4 uses"));
        assert!(rendered.contains("enc: 2 unique buffers, 5 total accesses"));
    }
}
