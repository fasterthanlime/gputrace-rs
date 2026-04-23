use std::collections::BTreeMap;

use serde::Serialize;

use crate::error::Result;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct TimingReport {
    pub synthetic: bool,
    pub total_duration_ns: u64,
    pub command_buffer_count: usize,
    pub encoder_count: usize,
    pub dispatch_count: usize,
    pub command_buffers: Vec<CommandBufferTiming>,
    pub encoders: Vec<EncoderTiming>,
    pub kernels: Vec<KernelTiming>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandBufferTiming {
    pub index: usize,
    pub timestamp_ns: u64,
    pub duration_ns: Option<u64>,
    pub encoder_count: usize,
    pub dispatch_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct EncoderTiming {
    pub label: String,
    pub address: u64,
    pub dispatch_count: usize,
    pub synthetic_duration_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KernelTiming {
    pub name: String,
    pub dispatch_count: usize,
    pub synthetic_duration_ns: u64,
    pub percent_of_total: f64,
}

pub fn report(trace: &TraceBundle) -> Result<TimingReport> {
    let command_buffers = trace.command_buffers()?;
    let regions = trace.command_buffer_regions()?;

    let mut command_buffer_timings = Vec::new();
    let mut encoder_stats: BTreeMap<u64, EncoderTiming> = BTreeMap::new();
    let mut kernel_stats: BTreeMap<String, KernelTiming> = BTreeMap::new();
    let mut total_duration_ns = 0u64;
    let mut total_dispatch_count = 0usize;

    for (index, region) in regions.iter().enumerate() {
        let timestamp_ns = command_buffers
            .get(index)
            .map_or(region.command_buffer.timestamp, |cb| cb.timestamp);
        let duration_ns = command_buffers
            .get(index + 1)
            .and_then(|next| next.timestamp.checked_sub(timestamp_ns));
        if let Some(duration_ns) = duration_ns {
            total_duration_ns = total_duration_ns.saturating_add(duration_ns);
        }
        total_dispatch_count += region.dispatches.len();
        command_buffer_timings.push(CommandBufferTiming {
            index: region.command_buffer.index,
            timestamp_ns,
            duration_ns,
            encoder_count: region.encoders.len(),
            dispatch_count: region.dispatches.len(),
        });

        let per_dispatch_duration = if let Some(duration_ns) = duration_ns {
            if region.dispatches.is_empty() {
                0
            } else {
                duration_ns / region.dispatches.len() as u64
            }
        } else {
            0
        };

        let mut encoder_dispatch_counts: BTreeMap<u64, usize> = BTreeMap::new();
        for dispatch in &region.dispatches {
            if let Some(encoder_id) = dispatch.encoder_id {
                *encoder_dispatch_counts.entry(encoder_id).or_default() += 1;
            }
            let kernel_name = dispatch
                .kernel_name
                .clone()
                .unwrap_or_else(|| "unknown".to_owned());
            let kernel = kernel_stats
                .entry(kernel_name.clone())
                .or_insert_with(|| KernelTiming {
                    name: kernel_name,
                    dispatch_count: 0,
                    synthetic_duration_ns: 0,
                    percent_of_total: 0.0,
                });
            kernel.dispatch_count += 1;
            kernel.synthetic_duration_ns = kernel
                .synthetic_duration_ns
                .saturating_add(per_dispatch_duration);
        }

        for encoder in &region.encoders {
            let entry = encoder_stats
                .entry(encoder.address)
                .or_insert_with(|| EncoderTiming {
                    label: encoder.label.clone(),
                    address: encoder.address,
                    dispatch_count: 0,
                    synthetic_duration_ns: 0,
                });
            let dispatch_count = encoder_dispatch_counts
                .get(&encoder.address)
                .copied()
                .unwrap_or(0);
            entry.dispatch_count += dispatch_count;
            entry.synthetic_duration_ns = entry
                .synthetic_duration_ns
                .saturating_add(per_dispatch_duration.saturating_mul(dispatch_count as u64));
        }
    }

    let mut encoders: Vec<_> = encoder_stats.into_values().collect();
    encoders.sort_by(|left, right| {
        right
            .synthetic_duration_ns
            .cmp(&left.synthetic_duration_ns)
            .then_with(|| left.label.cmp(&right.label))
    });

    let mut kernels: Vec<_> = kernel_stats.into_values().collect();
    for kernel in &mut kernels {
        if total_duration_ns > 0 {
            kernel.percent_of_total =
                (kernel.synthetic_duration_ns as f64 / total_duration_ns as f64) * 100.0;
        }
    }
    kernels.sort_by(|left, right| {
        right
            .synthetic_duration_ns
            .cmp(&left.synthetic_duration_ns)
            .then_with(|| left.name.cmp(&right.name))
    });

    Ok(TimingReport {
        synthetic: true,
        total_duration_ns,
        command_buffer_count: command_buffer_timings.len(),
        encoder_count: encoders.len(),
        dispatch_count: total_dispatch_count,
        command_buffers: command_buffer_timings,
        encoders,
        kernels,
    })
}

pub fn format_report(report: &TimingReport) -> String {
    let mut out = String::new();
    out.push_str("Synthetic timing report\n");
    out.push_str("Derived from command-buffer timestamps and dispatch attribution.\n\n");
    out.push_str(&format!(
        "total={} ns, command_buffers={}, encoders={}, dispatches={}\n\n",
        report.total_duration_ns,
        report.command_buffer_count,
        report.encoder_count,
        report.dispatch_count
    ));
    if !report.kernels.is_empty() {
        out.push_str("Kernels:\n");
        out.push_str(&format!(
            "{:<36} {:>10} {:>16} {:>8}\n",
            "Name", "Dispatches", "Synthetic ns", "%"
        ));
        for kernel in report.kernels.iter().take(20) {
            out.push_str(&format!(
                "{:<36} {:>10} {:>16} {:>7.2}\n",
                truncate(&kernel.name, 36),
                kernel.dispatch_count,
                kernel.synthetic_duration_ns,
                kernel.percent_of_total
            ));
        }
        out.push('\n');
    }
    if !report.encoders.is_empty() {
        out.push_str("Encoders:\n");
        out.push_str(&format!(
            "{:<32} {:>10} {:>16}\n",
            "Label", "Dispatches", "Synthetic ns"
        ));
        for encoder in report.encoders.iter().take(20) {
            let label = if encoder.label.is_empty() {
                format!("0x{:x}", encoder.address)
            } else {
                encoder.label.clone()
            };
            out.push_str(&format!(
                "{:<32} {:>10} {:>16}\n",
                truncate(&label, 32),
                encoder.dispatch_count,
                encoder.synthetic_duration_ns
            ));
        }
        out.push('\n');
    }
    if !report.command_buffers.is_empty() {
        out.push_str("Command buffers:\n");
        for cb in &report.command_buffers {
            out.push_str(&format!(
                "  CB {}: ts={} ns duration={} encoders={} dispatches={}\n",
                cb.index,
                cb.timestamp_ns,
                cb.duration_ns
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "?".to_owned()),
                cb.encoder_count,
                cb.dispatch_count
            ));
        }
    }
    out
}

pub fn format_csv(report: &TimingReport) -> String {
    let mut out = String::new();
    out.push_str("kind,name,dispatch_count,synthetic_duration_ns,percent_of_total\n");
    for kernel in &report.kernels {
        out.push_str(&format!(
            "kernel,{},{},{},{}\n",
            escape_csv(&kernel.name),
            kernel.dispatch_count,
            kernel.synthetic_duration_ns,
            kernel.percent_of_total
        ));
    }
    for encoder in &report.encoders {
        let label = if encoder.label.is_empty() {
            format!("0x{:x}", encoder.address)
        } else {
            encoder.label.clone()
        };
        out.push_str(&format!(
            "encoder,{},{},{},\n",
            escape_csv(&label),
            encoder.dispatch_count,
            encoder.synthetic_duration_ns
        ));
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

fn escape_csv(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_csv() {
        let report = TimingReport {
            synthetic: true,
            total_duration_ns: 100,
            command_buffer_count: 1,
            encoder_count: 1,
            dispatch_count: 2,
            command_buffers: vec![],
            encoders: vec![EncoderTiming {
                label: "enc".into(),
                address: 1,
                dispatch_count: 2,
                synthetic_duration_ns: 100,
            }],
            kernels: vec![KernelTiming {
                name: "kernel".into(),
                dispatch_count: 2,
                synthetic_duration_ns: 100,
                percent_of_total: 100.0,
            }],
        };
        let csv = format_csv(&report);
        assert!(csv.contains("kernel,kernel,2,100,100"));
        assert!(csv.contains("encoder,enc,2,100"));
    }
}
