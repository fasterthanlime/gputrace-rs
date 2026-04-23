use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::counter_export;
use crate::error::{Error, Result};
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeCounterData {
    pub source: PathBuf,
    pub encoders: Vec<XcodeEncoderCounters>,
    pub metrics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeEncoderCounters {
    pub index: usize,
    pub function_index: usize,
    pub command_buffer_label: String,
    pub encoder_label: String,
    pub counters: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CounterValidationReport {
    pub trace_source: PathBuf,
    pub csv_source: PathBuf,
    pub exported_row_count: usize,
    pub reference_row_count: usize,
    pub compared_metrics: Vec<String>,
    pub row_results: Vec<CounterValidationRow>,
    pub mismatches: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CounterValidationRow {
    pub encoder_index: usize,
    pub encoder_label: String,
    pub metrics: Vec<CounterMetricComparison>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CounterMetricComparison {
    pub name: String,
    pub exported: Option<f64>,
    pub reference: Option<f64>,
    pub delta: Option<f64>,
    pub within_tolerance: bool,
}

pub fn parse(trace: &TraceBundle, csv_path: Option<PathBuf>) -> Result<XcodeCounterData> {
    let source = match csv_path {
        Some(path) => path,
        None => find_counters_csv(&trace.path)?,
    };
    parse_csv_path(&source)
}

pub fn validate(
    trace: &TraceBundle,
    csv_path: Option<PathBuf>,
    tolerance: f64,
) -> Result<CounterValidationReport> {
    let exported = counter_export::report(trace)?;
    let imported = parse(trace, csv_path)?;
    let compared_metrics = vec![
        "Kernel Invocations".to_owned(),
        "ALU Utilization".to_owned(),
        "Kernel Occupancy".to_owned(),
        "Device Memory Bandwidth".to_owned(),
        "Buffer L1 Read Bandwidth".to_owned(),
        "Buffer L1 Write Bandwidth".to_owned(),
    ];

    let mut row_results = Vec::new();
    let mut mismatches = 0usize;
    let mut matched_reference_indices = BTreeSet::new();

    for exported_row in &exported.rows {
        let reference_row = match_reference_encoder(
            exported_row.encoder_index,
            &exported_row.encoder_label,
            &imported,
            &matched_reference_indices,
        );
        if let Some(reference_row) = reference_row {
            matched_reference_indices.insert(reference_row.index);
        }

        let metrics = vec![
            compare_metric(
                "Kernel Invocations",
                Some(exported_row.kernel_invocations as f64),
                reference_row.and_then(|row| row.counters.get("Kernel Invocations").copied()),
                tolerance,
            ),
            compare_metric(
                "ALU Utilization",
                exported_row.alu_utilization_percent,
                reference_row.and_then(|row| row.counters.get("ALU Utilization").copied()),
                tolerance,
            ),
            compare_metric(
                "Kernel Occupancy",
                exported_row.occupancy_percent,
                reference_row.and_then(|row| row.counters.get("Kernel Occupancy").copied()),
                tolerance,
            ),
            compare_metric(
                "Device Memory Bandwidth",
                exported_row.device_memory_bandwidth_gbps,
                reference_row.and_then(|row| row.counters.get("Device Memory Bandwidth").copied()),
                tolerance,
            ),
            compare_metric(
                "Buffer L1 Read Bandwidth",
                exported_row.buffer_l1_read_bandwidth_gbps,
                reference_row.and_then(|row| row.counters.get("Buffer L1 Read Bandwidth").copied()),
                tolerance,
            ),
            compare_metric(
                "Buffer L1 Write Bandwidth",
                exported_row.buffer_l1_write_bandwidth_gbps,
                reference_row
                    .and_then(|row| row.counters.get("Buffer L1 Write Bandwidth").copied()),
                tolerance,
            ),
        ];
        mismatches += metrics
            .iter()
            .filter(|metric| !metric.within_tolerance)
            .count();
        row_results.push(CounterValidationRow {
            encoder_index: exported_row.encoder_index,
            encoder_label: choose_encoder_label(&exported_row.encoder_label, reference_row),
            metrics,
        });
    }

    for reference_row in &imported.encoders {
        if matched_reference_indices.contains(&reference_row.index) {
            continue;
        }
        let metrics = compared_metrics
            .iter()
            .map(|metric| {
                compare_metric(
                    metric,
                    None,
                    reference_row.counters.get(metric).copied(),
                    tolerance,
                )
            })
            .collect::<Vec<_>>();
        mismatches += metrics
            .iter()
            .filter(|metric| !metric.within_tolerance)
            .count();
        row_results.push(CounterValidationRow {
            encoder_index: reference_row.index,
            encoder_label: reference_row.encoder_label.clone(),
            metrics,
        });
    }

    Ok(CounterValidationReport {
        trace_source: trace.path.clone(),
        csv_source: imported.source,
        exported_row_count: exported.rows.len(),
        reference_row_count: imported.encoders.len(),
        compared_metrics,
        row_results,
        mismatches,
    })
}

pub fn format_summary(data: &XcodeCounterData, metric: Option<&str>, top: Option<usize>) -> String {
    let key_metrics = [
        "ALU Utilization",
        "Kernel Occupancy",
        "Kernel Invocations",
        "GPU Read Bandwidth",
        "GPU Write Bandwidth",
        "Instruction Throughput Utilization",
    ];
    let encoders = filtered_encoders(data, metric, top);

    let mut out = String::new();
    out.push_str("Xcode counters\n");
    out.push_str(&format!(
        "source={} encoders={} metrics={}\n\n",
        data.source.display(),
        data.encoders.len(),
        data.metrics.len()
    ));
    out.push_str("idx encoder_label");
    for metric in key_metrics {
        out.push(' ');
        out.push_str(metric);
        out.push_str(" |");
    }
    out.push('\n');

    for encoder in encoders {
        out.push_str(&format!(
            "{:>3} {:<20}",
            encoder.index,
            truncate(&encoder.encoder_label, 20)
        ));
        for metric in key_metrics {
            let value = encoder
                .counters
                .get(metric)
                .map(|value| format_metric_value(metric, *value))
                .unwrap_or_else(|| "-".to_owned());
            out.push(' ');
            out.push_str(&format!("{:<12}|", truncate(&value, 12)));
        }
        out.push('\n');
    }
    out
}

pub fn format_detailed(
    data: &XcodeCounterData,
    metric: Option<&str>,
    top: Option<usize>,
) -> String {
    let mut out = String::new();
    for encoder in filtered_encoders(data, metric, top) {
        out.push_str(&format!(
            "Encoder {}\n  Function Index: {}\n  Command Buffer: {}\n  Encoder Label: {}\n",
            encoder.index,
            encoder.function_index,
            encoder.command_buffer_label,
            encoder.encoder_label
        ));
        for (name, value) in &encoder.counters {
            out.push_str(&format!("  {}: {:.4}\n", name, value));
        }
        out.push('\n');
    }
    out
}

pub fn format_metric_inventory(data: &XcodeCounterData) -> String {
    let mut out = String::new();
    out.push_str(&format!("Available metrics ({})\n", data.metrics.len()));
    for (index, metric) in data.metrics.iter().enumerate() {
        out.push_str(&format!("{:>3}. {}\n", index + 1, metric));
    }
    out
}

pub fn format_validation(report: &CounterValidationReport) -> String {
    let mut out = String::new();
    out.push_str("Counter validation\n");
    out.push_str(&format!(
        "trace={} csv={} exported_rows={} reference_rows={} mismatches={}\n\n",
        report.trace_source.display(),
        report.csv_source.display(),
        report.exported_row_count,
        report.reference_row_count,
        report.mismatches
    ));
    for row in &report.row_results {
        out.push_str(&format!(
            "Encoder {} {}\n",
            row.encoder_index, row.encoder_label
        ));
        for metric in &row.metrics {
            out.push_str(&format!(
                "  {}: exported={} reference={} delta={} {}\n",
                metric.name,
                metric
                    .exported
                    .map(|value| format!("{value:.4}"))
                    .unwrap_or_else(|| "-".to_owned()),
                metric
                    .reference
                    .map(|value| format!("{value:.4}"))
                    .unwrap_or_else(|| "-".to_owned()),
                metric
                    .delta
                    .map(|value| format!("{value:.4}"))
                    .unwrap_or_else(|| "-".to_owned()),
                if metric.within_tolerance {
                    "ok"
                } else {
                    "mismatch"
                }
            ));
        }
        out.push('\n');
    }
    out
}

fn compare_metric(
    name: &str,
    exported: Option<f64>,
    reference: Option<f64>,
    tolerance: f64,
) -> CounterMetricComparison {
    let delta = match (exported, reference) {
        (Some(exported), Some(reference)) => Some((exported - reference).abs()),
        _ => None,
    };
    let within_tolerance = match (exported, reference) {
        (Some(exported), Some(reference)) => (exported - reference).abs() <= tolerance,
        (None, None) => true,
        _ => false,
    };
    CounterMetricComparison {
        name: name.to_owned(),
        exported,
        reference,
        delta,
        within_tolerance,
    }
}

fn filtered_encoders<'a>(
    data: &'a XcodeCounterData,
    metric: Option<&str>,
    top: Option<usize>,
) -> Vec<&'a XcodeEncoderCounters> {
    let mut encoders = data.encoders.iter().collect::<Vec<_>>();
    if let Some(metric) = metric {
        encoders.sort_by(|left, right| {
            let left = left.counters.get(metric).copied().unwrap_or_default();
            let right = right.counters.get(metric).copied().unwrap_or_default();
            right
                .partial_cmp(&left)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    if let Some(top) = top {
        encoders.truncate(top);
    }
    encoders
}

fn match_reference_encoder<'a>(
    exported_index: usize,
    exported_label: &str,
    imported: &'a XcodeCounterData,
    used_reference_indices: &BTreeSet<usize>,
) -> Option<&'a XcodeEncoderCounters> {
    if let Some(exact) = imported
        .encoders
        .iter()
        .find(|row| row.index == exported_index && !used_reference_indices.contains(&row.index))
    {
        return Some(exact);
    }

    let normalized_label = normalize_for_matching(exported_label);
    if normalized_label.is_empty() {
        return None;
    }

    if let Some(exact) = imported.encoders.iter().find(|row| {
        !used_reference_indices.contains(&row.index)
            && normalize_for_matching(&row.encoder_label) == normalized_label
    }) {
        return Some(exact);
    }

    imported.encoders.iter().find(|row| {
        if used_reference_indices.contains(&row.index) {
            return false;
        }
        let normalized_row = normalize_for_matching(&row.encoder_label);
        !normalized_row.is_empty()
            && (normalized_row.contains(&normalized_label)
                || normalized_label.contains(&normalized_row))
    })
}

fn choose_encoder_label(
    exported_label: &str,
    reference_row: Option<&XcodeEncoderCounters>,
) -> String {
    if !exported_label.is_empty() {
        exported_label.to_owned()
    } else {
        reference_row
            .map(|row| row.encoder_label.clone())
            .unwrap_or_default()
    }
}

fn parse_csv_path(path: &Path) -> Result<XcodeCounterData> {
    let mut reader = csv::Reader::from_path(path)?;
    let headers = reader.headers()?.clone();
    if headers.len() < 5 {
        return Err(Error::InvalidInput(format!(
            "invalid counters CSV header: expected at least 5 columns, got {}",
            headers.len()
        )));
    }
    let metrics = headers
        .iter()
        .skip(5)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let mut encoders = Vec::new();
    for row in reader.records() {
        let row = row?;
        if row.len() < 5 {
            continue;
        }
        let mut counters = BTreeMap::new();
        for (index, metric) in metrics.iter().enumerate() {
            let column = index + 5;
            if let Some(value) = row.get(column).and_then(parse_optional_f64) {
                counters.insert(metric.clone(), value);
            }
        }
        encoders.push(XcodeEncoderCounters {
            index: row
                .get(0)
                .and_then(parse_optional_usize)
                .unwrap_or_default(),
            function_index: row
                .get(1)
                .and_then(parse_optional_usize)
                .unwrap_or_default(),
            command_buffer_label: row.get(2).unwrap_or_default().to_owned(),
            encoder_label: row.get(3).unwrap_or_default().to_owned(),
            counters,
        });
    }

    Ok(XcodeCounterData {
        source: path.to_path_buf(),
        encoders,
        metrics,
    })
}

fn find_counters_csv(trace_path: &Path) -> Result<PathBuf> {
    let base_name = trace_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::InvalidInput("trace path has no valid filename".to_owned()))?;
    let mut stripped = base_name.trim_end_matches(".gputrace").to_owned();
    for suffix in ["-perf", "-perf2", "-run1", "-run2", "-run3"] {
        stripped = stripped.trim_end_matches(suffix).to_owned();
    }

    let mut candidates = vec![
        trace_path
            .parent()
            .unwrap_or(trace_path)
            .join(format!("{base_name} Counters.csv")),
        trace_path
            .parent()
            .unwrap_or(trace_path)
            .join(format!("{stripped} Counters.csv")),
    ];
    if let Some(parent) = trace_path.parent().and_then(Path::parent) {
        candidates.push(parent.join(format!("{base_name} Counters.csv")));
        candidates.push(parent.join(format!("{stripped} Counters.csv")));
    }

    if let Some(found) = candidates.into_iter().find(|path| path.is_file()) {
        return Ok(found);
    }

    let dir = trace_path.parent().unwrap_or(trace_path);
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("Counters.csv"))
        {
            return Ok(path);
        }
    }

    Err(Error::NotFound(dir.join("Counters.csv")))
}

fn parse_optional_usize(value: &str) -> Option<usize> {
    value.trim().parse().ok()
}

fn parse_optional_f64(value: &str) -> Option<f64> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        value.parse().ok()
    }
}

fn format_metric_value(metric: &str, value: f64) -> String {
    if metric.contains("Bandwidth") {
        format!("{value:.2}")
    } else if metric.contains("Invocations") {
        format!("{value:.0}")
    } else {
        format!("{value:.2}")
    }
}

fn truncate(value: &str, width: usize) -> String {
    if value.len() <= width {
        value.to_owned()
    } else {
        format!("{}...", &value[..width.saturating_sub(3)])
    }
}

fn normalize_for_matching(name: &str) -> String {
    name.chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_lowercase())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn parses_xcode_counters_csv() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sample Counters.csv");
        fs::write(
            &path,
            "Index,Encoder FunctionIndex,CommandBuffer Label,Encoder Label,,ALU Utilization,Device Memory Bandwidth\n0,7,cb0,enc0,,62.5,4.2\n",
        )
        .unwrap();

        let data = parse_csv_path(&path).unwrap();
        assert_eq!(
            data.metrics,
            vec!["ALU Utilization", "Device Memory Bandwidth"]
        );
        assert_eq!(data.encoders.len(), 1);
        assert_eq!(data.encoders[0].index, 0);
        assert_eq!(data.encoders[0].function_index, 7);
        assert_eq!(
            data.encoders[0].counters.get("ALU Utilization").copied(),
            Some(62.5)
        );
    }

    #[test]
    fn formats_validation_report() {
        let report = CounterValidationReport {
            trace_source: PathBuf::from("/tmp/example.gputrace"),
            csv_source: PathBuf::from("/tmp/example Counters.csv"),
            exported_row_count: 1,
            reference_row_count: 1,
            compared_metrics: vec!["ALU Utilization".into()],
            mismatches: 1,
            row_results: vec![CounterValidationRow {
                encoder_index: 0,
                encoder_label: "enc0".into(),
                metrics: vec![CounterMetricComparison {
                    name: "ALU Utilization".into(),
                    exported: Some(40.0),
                    reference: Some(62.5),
                    delta: Some(22.5),
                    within_tolerance: false,
                }],
            }],
        };

        let text = format_validation(&report);
        assert!(text.contains("Counter validation"));
        assert!(text.contains("mismatches=1"));
        assert!(text.contains("ALU Utilization"));
    }

    #[test]
    fn finds_counters_csv_next_to_trace() {
        let dir = tempdir().unwrap();
        let trace = dir.path().join("foo-perf.gputrace");
        fs::create_dir(&trace).unwrap();
        let csv = dir.path().join("foo Counters.csv");
        fs::write(
            &csv,
            "Index,Encoder FunctionIndex,CommandBuffer Label,Encoder Label,\n",
        )
        .unwrap();

        let found = find_counters_csv(&trace).unwrap();
        assert_eq!(found, csv);
    }

    #[test]
    fn summary_can_sort_by_metric() {
        let data = XcodeCounterData {
            source: PathBuf::from("/tmp/example.csv"),
            metrics: vec!["ALU Utilization".into()],
            encoders: vec![
                XcodeEncoderCounters {
                    index: 0,
                    function_index: 0,
                    command_buffer_label: "cb0".into(),
                    encoder_label: "slow".into(),
                    counters: BTreeMap::from([("ALU Utilization".into(), 12.0)]),
                },
                XcodeEncoderCounters {
                    index: 1,
                    function_index: 1,
                    command_buffer_label: "cb1".into(),
                    encoder_label: "fast".into(),
                    counters: BTreeMap::from([("ALU Utilization".into(), 80.0)]),
                },
            ],
        };

        let text = format_summary(&data, Some("ALU Utilization"), Some(1));
        assert!(text.contains("fast"));
        assert!(!text.contains("slow"));
    }

    #[test]
    fn matches_reference_encoder_by_index_then_label() {
        let data = XcodeCounterData {
            source: PathBuf::from("/tmp/example.csv"),
            metrics: vec![],
            encoders: vec![
                XcodeEncoderCounters {
                    index: 7,
                    function_index: 0,
                    command_buffer_label: "cb0".into(),
                    encoder_label: "Compute Encoder 7 0x1234".into(),
                    counters: BTreeMap::new(),
                },
                XcodeEncoderCounters {
                    index: 99,
                    function_index: 1,
                    command_buffer_label: "cb1".into(),
                    encoder_label: "main_encoder".into(),
                    counters: BTreeMap::new(),
                },
            ],
        };

        let used = BTreeSet::new();
        assert_eq!(
            match_reference_encoder(7, "ignored", &data, &used).map(|row| row.index),
            Some(7)
        );
        assert_eq!(
            match_reference_encoder(1, "Main Encoder", &data, &used).map(|row| row.index),
            Some(99)
        );
    }
}
