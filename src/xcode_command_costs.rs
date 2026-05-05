use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use csv::ReaderBuilder;
use serde::Serialize;

use crate::error::{Error, Result};
use crate::trace::TraceBundle;
use crate::xcode_mio::{self, XcodeMioAnalysisReport};

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeCommandCostData {
    pub source: PathBuf,
    pub rows: Vec<XcodeCommandCostRow>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeCommandCostRow {
    pub index: usize,
    pub name: String,
    pub encoder: String,
    pub pipeline: String,
    pub pipeline_address: Option<u64>,
    pub execution_cost_percent: Option<f64>,
    pub kernel_invocations: Option<f64>,
    pub kernel_alu_instructions: Option<f64>,
    pub counters: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeCommandCostComparison {
    pub trace_source: PathBuf,
    pub table_source: PathBuf,
    pub command_rows: usize,
    pub mio_command_rows: usize,
    pub pipeline_count: usize,
    pub total_xcode_cost_percent: f64,
    pub pipelines: Vec<XcodeCommandPipelineCost>,
    pub analyzer_error: Option<XcodeCommandCostErrorStats>,
    pub w1_error: Option<XcodeCommandCostErrorStats>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeCommandPipelineCost {
    pub pipeline_address: Option<u64>,
    pub pipeline_label: String,
    pub function_name: Option<String>,
    pub command_count: usize,
    pub xcode_execution_cost_percent: f64,
    pub agxps_analyzer_cost_percent: Option<f64>,
    pub agxps_w1_cost_percent: Option<f64>,
    pub agxps_analyzer_delta_pp: Option<f64>,
    pub agxps_w1_delta_pp: Option<f64>,
    pub kernel_invocations: f64,
    pub kernel_alu_instructions: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeCommandCostErrorStats {
    pub matched_pipelines: usize,
    pub mean_absolute_error_pp: f64,
    pub root_mean_square_error_pp: f64,
    pub max_absolute_error_pp: f64,
    pub top5_mean_absolute_error_pp: f64,
}

pub fn compare(trace: &TraceBundle, table_path: PathBuf) -> Result<XcodeCommandCostComparison> {
    let data = parse_table(&table_path)?;
    let analysis = xcode_mio::agxps_analysis_report(trace, None)?;
    Ok(compare_with_analysis(trace, data, &analysis))
}

pub fn parse_table(path: &Path) -> Result<XcodeCommandCostData> {
    let bytes = fs::read(path)?;
    let delimiter = detect_delimiter(&bytes);
    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .flexible(true)
        .from_reader(bytes.as_slice());
    let headers = reader.headers()?.clone();
    let header_index = |name: &str| headers.iter().position(|header| header == name);
    let name_col = header_index("Name")
        .ok_or_else(|| Error::InvalidInput("missing GPU Commands table column: Name".to_owned()))?;
    let encoder_col = header_index("Encoder").ok_or_else(|| {
        Error::InvalidInput("missing GPU Commands table column: Encoder".to_owned())
    })?;
    let pipeline_col = header_index("Pipeline").ok_or_else(|| {
        Error::InvalidInput("missing GPU Commands table column: Pipeline".to_owned())
    })?;
    let execution_cost_col = header_index("Execution Cost").ok_or_else(|| {
        Error::InvalidInput("missing GPU Commands table column: Execution Cost".to_owned())
    })?;
    let invocations_col = header_index("Kernel Invocations");
    let alu_instructions_col = header_index("Kernel ALU Instructions");

    let metadata_columns = [
        header_index("Thumbnails"),
        Some(name_col),
        Some(encoder_col),
        Some(pipeline_col),
    ]
    .into_iter()
    .flatten()
    .collect::<std::collections::BTreeSet<_>>();

    let metric_columns = headers
        .iter()
        .enumerate()
        .filter(|(index, header)| !metadata_columns.contains(index) && !header.trim().is_empty())
        .map(|(index, header)| (index, header.to_owned()))
        .collect::<Vec<_>>();

    let mut rows = Vec::new();
    for (index, row) in reader.records().enumerate() {
        let row = row?;
        let pipeline = row.get(pipeline_col).unwrap_or_default().to_owned();
        let mut counters = BTreeMap::new();
        for (column, metric) in &metric_columns {
            if let Some(value) = row.get(*column).and_then(parse_localized_number) {
                counters.insert(metric.clone(), value);
            }
        }
        rows.push(XcodeCommandCostRow {
            index,
            name: row.get(name_col).unwrap_or_default().to_owned(),
            encoder: row.get(encoder_col).unwrap_or_default().to_owned(),
            pipeline_address: parse_pipeline_address(&pipeline),
            pipeline,
            execution_cost_percent: row.get(execution_cost_col).and_then(parse_localized_number),
            kernel_invocations: invocations_col
                .and_then(|column| row.get(column))
                .and_then(parse_localized_number),
            kernel_alu_instructions: alu_instructions_col
                .and_then(|column| row.get(column))
                .and_then(parse_localized_number),
            counters,
        });
    }

    Ok(XcodeCommandCostData {
        source: path.to_path_buf(),
        rows,
    })
}

pub fn compare_with_analysis(
    trace: &TraceBundle,
    data: XcodeCommandCostData,
    analysis: &XcodeMioAnalysisReport,
) -> XcodeCommandCostComparison {
    #[derive(Default)]
    struct Aggregate {
        pipeline_label: String,
        command_count: usize,
        xcode_cost: f64,
        invocations: f64,
        alu_instructions: f64,
    }

    let mut by_pipeline = BTreeMap::<Option<u64>, Aggregate>::new();
    for row in &data.rows {
        let entry = by_pipeline.entry(row.pipeline_address).or_default();
        if entry.pipeline_label.is_empty() {
            entry.pipeline_label = row.pipeline.clone();
        }
        entry.command_count += 1;
        entry.xcode_cost += row.execution_cost_percent.unwrap_or_default();
        entry.invocations += row.kernel_invocations.unwrap_or_default();
        entry.alu_instructions += row.kernel_alu_instructions.unwrap_or_default();
    }

    let by_address = analysis
        .top_pipelines
        .iter()
        .filter_map(|pipeline| pipeline.pipeline_address.map(|address| (address, pipeline)))
        .collect::<BTreeMap<_, _>>();

    let mut pipelines = by_pipeline
        .into_iter()
        .map(|(address, aggregate)| {
            let analysis_pipeline = address.and_then(|address| by_address.get(&address).copied());
            let analyzer = analysis_pipeline.and_then(|pipeline| {
                pipeline
                    .agxps_analyzer_cost_percent
                    .filter(|value| value.is_finite())
            });
            let w1 = analysis_pipeline.and_then(|pipeline| {
                pipeline
                    .agxps_trace_cost_percent
                    .filter(|value| value.is_finite())
            });
            XcodeCommandPipelineCost {
                pipeline_address: address,
                pipeline_label: aggregate.pipeline_label,
                function_name: analysis_pipeline
                    .and_then(|pipeline| pipeline.function_name.clone()),
                command_count: aggregate.command_count,
                xcode_execution_cost_percent: aggregate.xcode_cost,
                agxps_analyzer_cost_percent: analyzer,
                agxps_w1_cost_percent: w1,
                agxps_analyzer_delta_pp: analyzer.map(|value| value - aggregate.xcode_cost),
                agxps_w1_delta_pp: w1.map(|value| value - aggregate.xcode_cost),
                kernel_invocations: aggregate.invocations,
                kernel_alu_instructions: aggregate.alu_instructions,
            }
        })
        .collect::<Vec<_>>();

    pipelines.sort_by(|left, right| {
        right
            .xcode_execution_cost_percent
            .partial_cmp(&left.xcode_execution_cost_percent)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.command_count.cmp(&left.command_count))
    });

    let mut warnings = analysis.warnings.clone();
    if data.rows.len() != analysis.gpu_command_count {
        warnings.push(format!(
            "pasted table has {} command rows but MIO decoded {} GPU commands",
            data.rows.len(),
            analysis.gpu_command_count
        ));
    }
    let unmatched = pipelines
        .iter()
        .filter(|pipeline| {
            pipeline.pipeline_address.is_none()
                || (pipeline.agxps_analyzer_cost_percent.is_none()
                    && pipeline.agxps_w1_cost_percent.is_none())
        })
        .count();
    if unmatched > 0 {
        warnings.push(format!(
            "{unmatched} pasted pipeline(s) could not be matched to AGXPS pipeline metrics"
        ));
    }

    XcodeCommandCostComparison {
        trace_source: trace.path.clone(),
        table_source: data.source,
        command_rows: data.rows.len(),
        mio_command_rows: analysis.gpu_command_count,
        pipeline_count: pipelines.len(),
        total_xcode_cost_percent: pipelines
            .iter()
            .map(|pipeline| pipeline.xcode_execution_cost_percent)
            .sum(),
        analyzer_error: error_stats(&pipelines, |pipeline| pipeline.agxps_analyzer_delta_pp),
        w1_error: error_stats(&pipelines, |pipeline| pipeline.agxps_w1_delta_pp),
        pipelines,
        warnings,
    }
}

pub fn format_summary(report: &XcodeCommandCostComparison, top: Option<usize>) -> String {
    let mut out = String::new();
    out.push_str("Xcode GPU command costs\n");
    out.push_str(&format!(
        "trace={} table={} rows={} mio_commands={} pipelines={} xcode_total={:.3}%\n\n",
        report.trace_source.display(),
        report.table_source.display(),
        report.command_rows,
        report.mio_command_rows,
        report.pipeline_count,
        report.total_xcode_cost_percent
    ));

    out.push_str("Error vs pasted Xcode Execution Cost:\n");
    out.push_str(&format_error_line("AGX Ana", &report.analyzer_error));
    out.push_str(&format_error_line("AGX W1", &report.w1_error));
    out.push('\n');

    out.push_str(&format!(
        "{:<52} {:>8} {:>8} {:>8} {:>8} {:>8} {:>5} {:>13} {:>15} {}\n",
        "Function",
        "Xcode",
        "Ana",
        "Ana d",
        "W1",
        "W1 d",
        "Cmds",
        "Invocations",
        "ALU Instr",
        "Pipeline"
    ));
    for pipeline in report.pipelines.iter().take(top.unwrap_or(25)) {
        out.push_str(&format!(
            "{:<52} {:>7.3}% {:>8} {:>8} {:>8} {:>8} {:>5} {:>13.0} {:>15.0} {}\n",
            truncate(
                pipeline
                    .function_name
                    .as_deref()
                    .unwrap_or(&pipeline.pipeline_label),
                52
            ),
            pipeline.xcode_execution_cost_percent,
            format_optional_percent(pipeline.agxps_analyzer_cost_percent),
            format_optional_delta(pipeline.agxps_analyzer_delta_pp),
            format_optional_percent(pipeline.agxps_w1_cost_percent),
            format_optional_delta(pipeline.agxps_w1_delta_pp),
            pipeline.command_count,
            pipeline.kernel_invocations,
            pipeline.kernel_alu_instructions,
            format_pipeline_address(pipeline.pipeline_address)
        ));
    }

    if !report.warnings.is_empty() {
        out.push_str("\nWarnings:\n");
        for warning in &report.warnings {
            out.push_str(&format!("  - {warning}\n"));
        }
    }
    out
}

fn error_stats<F>(
    pipelines: &[XcodeCommandPipelineCost],
    delta: F,
) -> Option<XcodeCommandCostErrorStats>
where
    F: Fn(&XcodeCommandPipelineCost) -> Option<f64>,
{
    let mut deltas = pipelines.iter().filter_map(delta).collect::<Vec<_>>();
    if deltas.is_empty() {
        return None;
    }
    let matched_pipelines = deltas.len();
    let absolute = deltas.iter().map(|value| value.abs()).collect::<Vec<_>>();
    let mean_absolute_error_pp = absolute.iter().sum::<f64>() / matched_pipelines as f64;
    let root_mean_square_error_pp =
        (deltas.iter().map(|value| value * value).sum::<f64>() / matched_pipelines as f64).sqrt();
    let max_absolute_error_pp = absolute.iter().copied().fold(0.0, f64::max);

    deltas.truncate(5);
    let top5_mean_absolute_error_pp =
        deltas.iter().map(|value| value.abs()).sum::<f64>() / deltas.len() as f64;

    Some(XcodeCommandCostErrorStats {
        matched_pipelines,
        mean_absolute_error_pp,
        root_mean_square_error_pp,
        max_absolute_error_pp,
        top5_mean_absolute_error_pp,
    })
}

fn detect_delimiter(bytes: &[u8]) -> u8 {
    let first_line = bytes
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    if first_line.contains(&b'\t') {
        b'\t'
    } else if first_line.contains(&b';') {
        b';'
    } else {
        b','
    }
}

fn parse_pipeline_address(value: &str) -> Option<u64> {
    let start = value.find("0x")?;
    let hex = value[start + 2..]
        .chars()
        .take_while(|ch| ch.is_ascii_hexdigit())
        .collect::<String>();
    u64::from_str_radix(&hex, 16).ok()
}

fn parse_localized_number(value: &str) -> Option<f64> {
    let value = value.trim();
    if value.is_empty() || value == "-" {
        return None;
    }
    let mut normalized = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '%' | ' ' | '\t' | '\u{00a0}' | '\u{202f}' => {}
            ',' => normalized.push('.'),
            _ => normalized.push(ch),
        }
    }
    if normalized.is_empty() || normalized == "-" {
        None
    } else {
        normalized.parse().ok()
    }
}

fn format_error_line(label: &str, stats: &Option<XcodeCommandCostErrorStats>) -> String {
    match stats {
        Some(stats) => format!(
            "  {label:<7} matched={} MAE={:.3} pp RMSE={:.3} pp max={:.3} pp top5_MAE={:.3} pp\n",
            stats.matched_pipelines,
            stats.mean_absolute_error_pp,
            stats.root_mean_square_error_pp,
            stats.max_absolute_error_pp,
            stats.top5_mean_absolute_error_pp
        ),
        None => format!("  {label:<7} no matched pipelines\n"),
    }
}

fn format_optional_percent(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map(|value| format!("{value:.3}%"))
        .unwrap_or_else(|| "-".to_owned())
}

fn format_optional_delta(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map(|value| format!("{value:+.3}"))
        .unwrap_or_else(|| "-".to_owned())
}

fn format_pipeline_address(value: Option<u64>) -> String {
    value
        .map(|value| format!("0x{value:x}"))
        .unwrap_or_else(|| "-".to_owned())
}

fn truncate(value: &str, width: usize) -> String {
    if value.len() <= width {
        value.to_owned()
    } else {
        format!("{}...", &value[..width.saturating_sub(3)])
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::xcode_mio::{XcodeMioPipelineAnalysis, XcodeMioTimings};

    #[test]
    fn parses_pasted_compute_kernel_table() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("commands.txt");
        fs::write(
            &path,
            "Thumbnails\tName\tEncoder\tPipeline\tExecution Cost\tKernel Invocations\tKernel ALU Instructions\n\
             -\tdispatchThreadgroups\tencoder\tCompute Pipeline 0x72a65e300\t0,005%\t256 \t2\u{202f}496 \n\
             -\tdispatchThreadgroups\tencoder\tCompute Pipeline 0x72a65e300\t0,010%\t1\u{202f}024 \t24\u{202f}128 \n",
        )
        .unwrap();

        let data = parse_table(&path).unwrap();
        assert_eq!(data.rows.len(), 2);
        assert_eq!(data.rows[0].pipeline_address, Some(0x72a65e300));
        assert_eq!(data.rows[0].execution_cost_percent, Some(0.005));
        assert_eq!(data.rows[1].kernel_invocations, Some(1024.0));
        assert_eq!(data.rows[1].kernel_alu_instructions, Some(24128.0));
    }

    #[test]
    fn compares_pasted_costs_with_agxps_metrics() {
        let trace_dir = tempdir().unwrap();
        let table_dir = tempdir().unwrap();
        let table_path = table_dir.path().join("commands.txt");
        fs::write(
            &table_path,
            "Name\tEncoder\tPipeline\tExecution Cost\tKernel Invocations\tKernel ALU Instructions\n\
             dispatchThreadgroups\tencoder\tCompute Pipeline 0x1\t70,000%\t10\t100\n\
             dispatchThreadgroups\tencoder\tCompute Pipeline 0x2\t30,000%\t20\t200\n",
        )
        .unwrap();
        let data = parse_table(&table_path).unwrap();
        let trace = TraceBundle {
            path: trace_dir.path().to_path_buf(),
            metadata: Default::default(),
            capture_path: trace_dir.path().join("capture.gputrace"),
            capture_len: 0,
            device_resources: Vec::new(),
        };
        let analysis = XcodeMioAnalysisReport {
            backend: "xcode-mio",
            trace_source: trace.path.clone(),
            timings: XcodeMioTimings::default(),
            gpu_time_ns: 0,
            gpu_command_count: 2,
            pipeline_state_count: 2,
            cost_record_count: 0,
            top_pipelines: vec![
                pipeline_analysis(0x1, "a", Some(75.0), Some(71.0)),
                pipeline_analysis(0x2, "b", Some(25.0), Some(29.0)),
            ],
            warnings: Vec::new(),
        };

        let report = compare_with_analysis(&trace, data, &analysis);
        assert_eq!(report.pipeline_count, 2);
        assert_eq!(report.total_xcode_cost_percent, 100.0);
        assert_eq!(
            report
                .analyzer_error
                .as_ref()
                .unwrap()
                .mean_absolute_error_pp,
            5.0
        );
        assert_eq!(
            report.w1_error.as_ref().unwrap().mean_absolute_error_pp,
            1.0
        );
    }

    fn pipeline_analysis(
        address: u64,
        name: &str,
        analyzer: Option<f64>,
        w1: Option<f64>,
    ) -> XcodeMioPipelineAnalysis {
        XcodeMioPipelineAnalysis {
            pipeline_index: 0,
            object_id: address,
            pipeline_address: Some(address),
            function_name: Some(name.to_owned()),
            command_count: 1,
            command_percent: 0.0,
            shader_binary_reference_count: 0,
            executable_shader_binary_reference_count: 0,
            unique_timeline_binary_count: 0,
            referenced_instruction_info_count: 0,
            xcode_time_percent: None,
            xcode_time_average: None,
            xcode_cycle_average: None,
            timeline_duration_ns: 0,
            timeline_duration_percent: None,
            timeline_total_cost: 0.0,
            timeline_cost_percent: None,
            shader_profiler_cost: 0.0,
            shader_profiler_cost_percent: None,
            shader_binary_cost: 0.0,
            shader_binary_cost_percent: None,
            agxps_trace_cost: 0,
            agxps_trace_cost_percent: w1,
            agxps_trace_events: 0,
            agxps_trace_matched_work_cliques: 0,
            agxps_trace_duration_ns: 0,
            agxps_trace_duration_percent: None,
            agxps_analyzer_cost: 0,
            agxps_analyzer_cost_percent: analyzer,
            agxps_analyzer_avg_duration_sum: 0,
            agxps_analyzer_record_cliques: 0,
            execution_top_cost_percent: None,
            execution_duration_percent: None,
            execution_total_cost: None,
            execution_instruction_count: None,
            counters: Vec::new(),
            metric_sources: Vec::new(),
        }
    }
}
