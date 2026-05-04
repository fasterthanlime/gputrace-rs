use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::error::Result;
use crate::trace::{CommandBufferRegion, MTLResourceUsage, TraceBundle};

#[derive(Debug, Clone, Serialize)]
pub struct AnalyzeUsageReport {
    pub total_buffers: usize,
    pub total_dispatches: usize,
    pub total_kernels: usize,
    pub approximations: Vec<String>,
    pub buffers: Vec<BufferUsageEntry>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BufferUsageEntry {
    pub name: String,
    pub address: u64,
    pub aliases: Vec<String>,
    pub binding_use_count: usize,
    pub dispatch_count: usize,
    pub first_dispatch_index: usize,
    pub last_dispatch_index: usize,
    pub usage: String,
    pub kernels: Vec<KernelUsageEntry>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct KernelUsageEntry {
    pub name: String,
    pub pipeline_addr: Option<u64>,
    pub binding_use_count: usize,
    pub dispatch_count: usize,
    pub usage: String,
}

pub fn build(trace: &TraceBundle) -> Result<AnalyzeUsageReport> {
    let regions = trace.command_buffer_regions()?;
    Ok(build_from_regions(&regions))
}

pub fn build_from_regions(regions: &[CommandBufferRegion]) -> AnalyzeUsageReport {
    let mut buffers = BTreeMap::<u64, BufferUsageAccum>::new();
    let mut kernels = BTreeSet::<(String, Option<u64>)>::new();
    let total_dispatches = regions.iter().map(|region| region.dispatches.len()).sum();

    for region in regions {
        for dispatch in &region.dispatches {
            let kernel_name = dispatch
                .kernel_name
                .clone()
                .unwrap_or_else(|| "unknown".to_owned());
            let kernel_key = (kernel_name.clone(), dispatch.pipeline_addr);
            kernels.insert(kernel_key.clone());

            for buffer in &dispatch.buffers {
                let entry = buffers
                    .entry(buffer.address)
                    .or_insert_with(|| BufferUsageAccum::new(buffer.address, dispatch.index));
                entry.binding_use_count += 1;
                entry.dispatches.insert(dispatch.index);
                entry.first_dispatch_index = entry.first_dispatch_index.min(dispatch.index);
                entry.last_dispatch_index = entry.last_dispatch_index.max(dispatch.index);
                entry.usage_bits |= buffer.usage.bits();
                if let Some(name) = &buffer.name {
                    entry.names.insert(name.clone());
                }

                let kernel = entry
                    .kernels
                    .entry(kernel_key.clone())
                    .or_insert_with(KernelUsageAccum::new);
                kernel.binding_use_count += 1;
                kernel.dispatches.insert(dispatch.index);
                kernel.usage_bits |= buffer.usage.bits();
            }
        }
    }

    let mut buffers: Vec<_> = buffers
        .into_values()
        .map(BufferUsageAccum::finish)
        .collect();
    buffers.sort_by(|left, right| {
        right
            .dispatch_count
            .cmp(&left.dispatch_count)
            .then_with(|| right.binding_use_count.cmp(&left.binding_use_count))
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.address.cmp(&right.address))
    });

    AnalyzeUsageReport {
        total_buffers: buffers.len(),
        total_dispatches,
        total_kernels: kernels.len(),
        approximations: vec![
            "Kernel attribution comes from the most recent pipeline state seen for each dispatch."
                .to_owned(),
            "Usage flags reflect declared binding access, not observed reads or writes.".to_owned(),
            "binding_use_count counts every bound slot occurrence; dispatch_count deduplicates repeated bindings within one dispatch."
                .to_owned(),
        ],
        buffers,
    }
}

pub fn format_text(report: &AnalyzeUsageReport) -> String {
    let mut out = String::new();
    out.push_str("Trace Buffer Usage Analysis\n");
    out.push_str("===========================\n");
    out.push_str(&format!(
        "{} buffers across {} dispatches and {} kernels\n",
        report.total_buffers, report.total_dispatches, report.total_kernels
    ));
    for approximation in &report.approximations {
        out.push_str(&format!("~ {approximation}\n"));
    }

    for buffer in &report.buffers {
        out.push_str(&format!(
            "\n{} (0x{:x}): {} dispatches, {} binding uses, usage {}\n",
            buffer.name,
            buffer.address,
            buffer.dispatch_count,
            buffer.binding_use_count,
            buffer.usage
        ));
        if !buffer.aliases.is_empty() {
            out.push_str(&format!("  aliases: {}\n", buffer.aliases.join(", ")));
        }
        out.push_str(&format!(
            "  dispatch span: {}..={}\n",
            buffer.first_dispatch_index, buffer.last_dispatch_index
        ));
        for kernel in &buffer.kernels {
            out.push_str("  - ");
            out.push_str(&kernel.name);
            if let Some(pipeline_addr) = kernel.pipeline_addr {
                out.push_str(&format!(" (0x{pipeline_addr:x})"));
            }
            out.push_str(&format!(
                ": {} dispatches, {} binding uses, usage {}\n",
                kernel.dispatch_count, kernel.binding_use_count, kernel.usage
            ));
        }
    }

    out
}

pub fn format_json(report: &AnalyzeUsageReport) -> Result<String> {
    Ok(serde_json::to_string_pretty(report)?)
}

pub fn format_dot(report: &AnalyzeUsageReport) -> String {
    let mut out = String::new();
    let mut kernel_nodes = BTreeMap::<(String, Option<u64>), String>::new();

    out.push_str("digraph AnalyzeUsage {\n");
    out.push_str("  rankdir=LR;\n");
    out.push_str("  graph [label=\"Trace Buffer Usage\", labelloc=t];\n");
    out.push_str("  node [fontname=\"Helvetica\"];\n");
    out.push_str("  edge [fontname=\"Helvetica\"];\n");
    out.push_str("  // Edge direction is descriptive only: kernels bind buffers.\n");

    for (index, buffer) in report.buffers.iter().enumerate() {
        let buffer_id = format!("buffer_{index}");
        let mut label = format!(
            "{}\\n0x{:x}\\n{} dispatches / {} bindings\\n{}",
            escape_dot_label(&buffer.name),
            buffer.address,
            buffer.dispatch_count,
            buffer.binding_use_count,
            escape_dot_label(&buffer.usage)
        );
        if !buffer.aliases.is_empty() {
            label.push_str("\\naliases: ");
            label.push_str(&escape_dot_label(&buffer.aliases.join(", ")));
        }
        out.push_str(&format!("  {buffer_id} [shape=box, label=\"{label}\"];\n"));

        for kernel in &buffer.kernels {
            let key = (kernel.name.clone(), kernel.pipeline_addr);
            let kernel_id = if let Some(existing) = kernel_nodes.get(&key) {
                existing.clone()
            } else {
                let id = format!("kernel_{}", kernel_nodes.len());
                let mut label = escape_dot_label(&kernel.name);
                if let Some(pipeline_addr) = kernel.pipeline_addr {
                    label.push_str(&format!("\\n0x{pipeline_addr:x}"));
                }
                out.push_str(&format!("  {id} [shape=ellipse, label=\"{label}\"];\n"));
                kernel_nodes.insert(key, id.clone());
                id
            };

            let edge_label = format!(
                "{} dispatches\\n{} bindings\\n{}",
                kernel.dispatch_count,
                kernel.binding_use_count,
                escape_dot_label(&kernel.usage)
            );
            out.push_str(&format!(
                "  {kernel_id} -> {buffer_id} [label=\"{edge_label}\"];\n"
            ));
        }
    }

    out.push_str("}\n");
    out
}

#[derive(Debug, Clone)]
struct BufferUsageAccum {
    address: u64,
    names: BTreeSet<String>,
    binding_use_count: usize,
    dispatches: BTreeSet<usize>,
    first_dispatch_index: usize,
    last_dispatch_index: usize,
    usage_bits: u8,
    kernels: BTreeMap<(String, Option<u64>), KernelUsageAccum>,
}

impl BufferUsageAccum {
    fn new(address: u64, dispatch_index: usize) -> Self {
        Self {
            address,
            names: BTreeSet::new(),
            binding_use_count: 0,
            dispatches: BTreeSet::new(),
            first_dispatch_index: dispatch_index,
            last_dispatch_index: dispatch_index,
            usage_bits: 0,
            kernels: BTreeMap::new(),
        }
    }

    fn finish(self) -> BufferUsageEntry {
        let mut names = self.names.into_iter().collect::<Vec<_>>();
        let name = if names.is_empty() {
            format!("Buffer@0x{:x}", self.address)
        } else {
            names.remove(0)
        };

        let mut kernels: Vec<_> = self
            .kernels
            .into_iter()
            .map(|((kernel_name, pipeline_addr), kernel)| KernelUsageEntry {
                name: kernel_name,
                pipeline_addr,
                binding_use_count: kernel.binding_use_count,
                dispatch_count: kernel.dispatches.len(),
                usage: usage_string(kernel.usage_bits),
            })
            .collect();
        kernels.sort_by(|left, right| {
            right
                .dispatch_count
                .cmp(&left.dispatch_count)
                .then_with(|| right.binding_use_count.cmp(&left.binding_use_count))
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.pipeline_addr.cmp(&right.pipeline_addr))
        });

        BufferUsageEntry {
            name,
            address: self.address,
            aliases: names,
            binding_use_count: self.binding_use_count,
            dispatch_count: self.dispatches.len(),
            first_dispatch_index: self.first_dispatch_index,
            last_dispatch_index: self.last_dispatch_index,
            usage: usage_string(self.usage_bits),
            kernels,
        }
    }
}

#[derive(Debug, Clone)]
struct KernelUsageAccum {
    binding_use_count: usize,
    dispatches: BTreeSet<usize>,
    usage_bits: u8,
}

impl KernelUsageAccum {
    fn new() -> Self {
        Self {
            binding_use_count: 0,
            dispatches: BTreeSet::new(),
            usage_bits: 0,
        }
    }
}

fn usage_string(bits: u8) -> String {
    let mut parts = Vec::new();
    if bits & MTLResourceUsage::READ.bits() != 0 {
        parts.push("read");
    }
    if bits & MTLResourceUsage::WRITE.bits() != 0 {
        parts.push("write");
    }
    if bits & MTLResourceUsage::SAMPLE.bits() != 0 {
        parts.push("sample");
    }
    if parts.is_empty() {
        parts.push("none");
    }
    parts.join("|")
}

fn escape_dot_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::{BoundBuffer, CommandBuffer, DispatchCall};

    #[test]
    fn aggregates_buffers_across_dispatches_and_kernels() {
        let report = build_from_regions(&[
            CommandBufferRegion {
                command_buffer: CommandBuffer {
                    index: 0,
                    timestamp: 1,
                    offset: 0,
                    artifact_hashes: Vec::new(),
                },
                end_offset: 10,
                encoders: vec![],
                pipeline_events: vec![],
                dispatches: vec![
                    DispatchCall {
                        index: 2,
                        offset: 1,
                        encoder_id: Some(11),
                        pipeline_addr: Some(0x10),
                        kernel_name: Some("blur".into()),
                        buffers: vec![
                            BoundBuffer {
                                address: 0xaa,
                                name: Some("input".into()),
                                index: 0,
                                usage: MTLResourceUsage::READ,
                            },
                            BoundBuffer {
                                address: 0xaa,
                                name: Some("source".into()),
                                index: 1,
                                usage: MTLResourceUsage::READ,
                            },
                            BoundBuffer {
                                address: 0xbb,
                                name: Some("output".into()),
                                index: 2,
                                usage: MTLResourceUsage::WRITE,
                            },
                        ],
                        grid_size: [1, 1, 1],
                        group_size: [1, 1, 1],
                    },
                    DispatchCall {
                        index: 5,
                        offset: 2,
                        encoder_id: Some(12),
                        pipeline_addr: Some(0x20),
                        kernel_name: Some("sharpen".into()),
                        buffers: vec![
                            BoundBuffer {
                                address: 0xaa,
                                name: Some("input".into()),
                                index: 0,
                                usage: MTLResourceUsage::READ | MTLResourceUsage::WRITE,
                            },
                            BoundBuffer {
                                address: 0xcc,
                                name: None,
                                index: 1,
                                usage: MTLResourceUsage::READ,
                            },
                        ],
                        grid_size: [1, 1, 1],
                        group_size: [1, 1, 1],
                    },
                ],
            },
            CommandBufferRegion {
                command_buffer: CommandBuffer {
                    index: 1,
                    timestamp: 2,
                    offset: 10,
                    artifact_hashes: Vec::new(),
                },
                end_offset: 20,
                encoders: vec![],
                pipeline_events: vec![],
                dispatches: vec![DispatchCall {
                    index: 8,
                    offset: 11,
                    encoder_id: Some(13),
                    pipeline_addr: Some(0x10),
                    kernel_name: Some("blur".into()),
                    buffers: vec![BoundBuffer {
                        address: 0xaa,
                        name: Some("input".into()),
                        index: 0,
                        usage: MTLResourceUsage::READ,
                    }],
                    grid_size: [1, 1, 1],
                    group_size: [1, 1, 1],
                }],
            },
        ]);

        assert_eq!(report.total_buffers, 3);
        assert_eq!(report.total_dispatches, 3);
        assert_eq!(report.total_kernels, 2);

        let input = &report.buffers[0];
        assert_eq!(input.name, "input");
        assert_eq!(input.address, 0xaa);
        assert_eq!(input.aliases, vec!["source"]);
        assert_eq!(input.binding_use_count, 4);
        assert_eq!(input.dispatch_count, 3);
        assert_eq!(input.first_dispatch_index, 2);
        assert_eq!(input.last_dispatch_index, 8);
        assert_eq!(input.usage, "read|write");
        assert_eq!(input.kernels.len(), 2);
        assert_eq!(
            input.kernels[0],
            KernelUsageEntry {
                name: "blur".into(),
                pipeline_addr: Some(0x10),
                binding_use_count: 3,
                dispatch_count: 2,
                usage: "read".into(),
            }
        );
        assert_eq!(
            input.kernels[1],
            KernelUsageEntry {
                name: "sharpen".into(),
                pipeline_addr: Some(0x20),
                binding_use_count: 1,
                dispatch_count: 1,
                usage: "read|write".into(),
            }
        );

        let unnamed = report
            .buffers
            .iter()
            .find(|buffer| buffer.address == 0xcc)
            .unwrap();
        assert_eq!(unnamed.name, "Buffer@0xcc");
        assert!(unnamed.aliases.is_empty());
    }

    #[test]
    fn formatters_emit_expected_sections() {
        let report = AnalyzeUsageReport {
            total_buffers: 1,
            total_dispatches: 2,
            total_kernels: 1,
            approximations: vec!["Usage flags are declarative.".into()],
            buffers: vec![BufferUsageEntry {
                name: "input".into(),
                address: 0xaa,
                aliases: vec!["source".into()],
                binding_use_count: 3,
                dispatch_count: 2,
                first_dispatch_index: 4,
                last_dispatch_index: 8,
                usage: "read".into(),
                kernels: vec![KernelUsageEntry {
                    name: "blur".into(),
                    pipeline_addr: Some(0x10),
                    binding_use_count: 3,
                    dispatch_count: 2,
                    usage: "read".into(),
                }],
            }],
        };

        let text = format_text(&report);
        assert!(text.contains("Trace Buffer Usage Analysis"));
        assert!(text.contains("input (0xaa): 2 dispatches, 3 binding uses, usage read"));
        assert!(text.contains("aliases: source"));
        assert!(text.contains("blur (0x10): 2 dispatches, 3 binding uses, usage read"));

        let dot = format_dot(&report);
        assert!(dot.contains("digraph AnalyzeUsage"));
        assert!(dot.contains("kernel_0"));
        assert!(dot.contains("buffer_0"));
        assert!(dot.contains("blur\\n0x10"));
        assert!(dot.contains("input\\n0xaa\\n2 dispatches / 3 bindings\\nread"));

        let json = format_json(&report).unwrap();
        assert!(json.contains("\"total_buffers\": 1"));
        assert!(json.contains("\"name\": \"input\""));
    }
}
