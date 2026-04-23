use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Serialize;

use crate::error::Result;
use crate::shaders;
use crate::timing;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct CorrelationReport {
    pub synthetic: bool,
    pub trace_source: PathBuf,
    pub search_paths: Vec<PathBuf>,
    pub total_shaders: usize,
    pub correlated_sources: usize,
    pub uncorrelated_sources: usize,
    pub total_dispatches: usize,
    pub total_duration_ns: u64,
    pub shaders: Vec<CorrelatedShader>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CorrelatedShader {
    pub shader_name: String,
    pub pipeline_addr: u64,
    pub execution_count: usize,
    pub synthetic_total_duration_ns: u64,
    pub synthetic_avg_duration_ns: u64,
    pub synthetic_percent_of_total: f64,
    pub encoder_count: usize,
    pub buffer_count: usize,
    pub source_file: Option<PathBuf>,
    pub source_line: Option<usize>,
    pub correlation_method: String,
}

pub fn report(trace: &TraceBundle, search_paths: &[PathBuf]) -> Result<CorrelationReport> {
    let timing = timing::report(trace)?;
    let shader_report = shaders::report(trace, search_paths)?;
    let kernel_stats = trace.analyze_kernels()?;

    let shader_lookup: BTreeMap<_, _> = shader_report
        .shaders
        .into_iter()
        .map(|shader| (shader.name.clone(), shader))
        .collect();

    let mut shaders = Vec::new();
    let mut correlated_sources = 0usize;

    for kernel in timing.kernels {
        let source = shader_lookup.get(&kernel.name);
        if source
            .and_then(|shader| shader.source_file.as_ref())
            .is_some()
        {
            correlated_sources += 1;
        }
        let kernel_stat = kernel_stats.get(&kernel.name);
        let pipeline_addr = kernel_stat
            .map(|value| value.pipeline_addr)
            .unwrap_or_default();
        let execution_count = kernel.dispatch_count;
        let synthetic_avg_duration_ns = if execution_count == 0 {
            0
        } else {
            kernel.synthetic_duration_ns / execution_count as u64
        };
        shaders.push(CorrelatedShader {
            shader_name: kernel.name.clone(),
            pipeline_addr,
            execution_count,
            synthetic_total_duration_ns: kernel.synthetic_duration_ns,
            synthetic_avg_duration_ns,
            synthetic_percent_of_total: kernel.percent_of_total,
            encoder_count: kernel_stat
                .map(|value| value.encoder_labels.len())
                .unwrap_or(0),
            buffer_count: kernel_stat.map(|value| value.buffers.len()).unwrap_or(0),
            source_file: source.and_then(|shader| shader.source_file.clone()),
            source_line: source.and_then(|shader| shader.source_line),
            correlation_method: if source
                .and_then(|shader| shader.source_file.as_ref())
                .is_some()
            {
                "name".to_owned()
            } else {
                "timing-only".to_owned()
            },
        });
    }

    shaders.sort_by(|left, right| {
        right
            .synthetic_total_duration_ns
            .cmp(&left.synthetic_total_duration_ns)
            .then_with(|| right.execution_count.cmp(&left.execution_count))
            .then_with(|| left.shader_name.cmp(&right.shader_name))
    });

    Ok(CorrelationReport {
        synthetic: true,
        trace_source: trace.path.clone(),
        search_paths: search_paths.to_vec(),
        total_shaders: shaders.len(),
        correlated_sources,
        uncorrelated_sources: shaders.len().saturating_sub(correlated_sources),
        total_dispatches: timing.dispatch_count,
        total_duration_ns: timing.total_duration_ns,
        shaders,
    })
}

pub fn format_report(report: &CorrelationReport, verbose: bool) -> String {
    let mut out = String::new();
    out.push_str("Synthetic shader correlation report\n");
    out.push_str("Combines kernel timing, trace attribution, and optional source lookup.\n");
    out.push_str("Hardware profiler counters are not included in this report.\n\n");
    out.push_str(&format!("trace={}\n", report.trace_source.display()));
    out.push_str(&format!(
        "shaders={} sources={}/{} dispatches={} total={} ns\n\n",
        report.total_shaders,
        report.correlated_sources,
        report.total_shaders,
        report.total_dispatches,
        report.total_duration_ns
    ));
    out.push_str(&format!(
        "{:<36} {:>10} {:>16} {:>8} {:<18}  {}\n",
        "Shader", "Dispatches", "Synthetic ns", "%", "Pipeline", "Source"
    ));
    for shader in &report.shaders {
        let source = match (&shader.source_file, shader.source_line) {
            (Some(path), Some(line)) => format!("{}:{}", path.display(), line),
            _ => "-".to_owned(),
        };
        out.push_str(&format!(
            "{:<36} {:>10} {:>16} {:>7.2} 0x{:<16x}  {}\n",
            truncate(&shader.shader_name, 36),
            shader.execution_count,
            shader.synthetic_total_duration_ns,
            shader.synthetic_percent_of_total,
            shader.pipeline_addr,
            source
        ));
        if verbose {
            out.push_str(&format!(
                "           avg={} ns encoders={} buffers={} correlation={}\n",
                shader.synthetic_avg_duration_ns,
                shader.encoder_count,
                shader.buffer_count,
                shader.correlation_method
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
    fn formats_report() {
        let report = CorrelationReport {
            synthetic: true,
            trace_source: PathBuf::from("/tmp/example.gputrace"),
            search_paths: vec![],
            total_shaders: 1,
            correlated_sources: 1,
            uncorrelated_sources: 0,
            total_dispatches: 2,
            total_duration_ns: 120,
            shaders: vec![CorrelatedShader {
                shader_name: "kernel".into(),
                pipeline_addr: 0x1234,
                execution_count: 2,
                synthetic_total_duration_ns: 120,
                synthetic_avg_duration_ns: 60,
                synthetic_percent_of_total: 100.0,
                encoder_count: 1,
                buffer_count: 2,
                source_file: Some(PathBuf::from("/tmp/kernel.metal")),
                source_line: Some(42),
                correlation_method: "name".into(),
            }],
        };
        let output = format_report(&report, true);
        assert!(output.contains("Synthetic shader correlation report"));
        assert!(output.contains("kernel"));
        assert!(output.contains("/tmp/kernel.metal:42"));
        assert!(output.contains("correlation=name"));
    }
}
