use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::error::Result;
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

fn truncate(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_owned();
    }
    let keep = width.saturating_sub(3);
    format!("{}...", &value[..keep])
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
}
