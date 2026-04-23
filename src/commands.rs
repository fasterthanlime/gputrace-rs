use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

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
}
