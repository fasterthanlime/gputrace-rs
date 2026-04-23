use std::error::Error;
use std::path::{Path, PathBuf};

use csv::StringRecord;
use gputrace_rs::counter_export;
use gputrace_rs::trace::TraceBundle;

#[derive(Debug)]
struct FixtureCase {
    name: &'static str,
    trace_path: PathBuf,
    csv_path: PathBuf,
}

#[derive(Debug)]
struct ReferenceCounterRow {
    index: usize,
    encoder_label: String,
    kernel_invocations: Option<f64>,
    alu_utilization_percent: Option<f64>,
    kernel_occupancy_percent: Option<f64>,
    device_memory_bandwidth_gbps: Option<f64>,
    gpu_read_bandwidth_gbps: Option<f64>,
    gpu_write_bandwidth_gbps: Option<f64>,
    buffer_l1_miss_rate_percent: Option<f64>,
    buffer_l1_read_accesses: Option<f64>,
    buffer_l1_read_bandwidth_gbps: Option<f64>,
    buffer_l1_write_accesses: Option<f64>,
    buffer_l1_write_bandwidth_gbps: Option<f64>,
}

#[test]
fn validates_counter_export_against_xcode_reference_csv_when_fixtures_are_available()
-> Result<(), Box<dyn Error>> {
    let Some(repo_root) = go_repo_root() else {
        eprintln!("skipping fixture validation: sibling Go repo not found");
        return Ok(());
    };

    let cases = vec![
        fixture_case(
            &repo_root,
            "01-single-encoder",
            "01-single-encoder-run1-perf.gputrace",
            "01-single-encoder-run1 Counters.csv",
        ),
        fixture_case(
            &repo_root,
            "06-six-encoders",
            "06-six-encoders-run1-perf.gputrace",
            "06-six-encoders-run1 Counters.csv",
        ),
    ];

    let mut any_fixture = false;

    for case in cases {
        if !case.trace_path.exists() || !case.csv_path.exists() {
            eprintln!(
                "skipping fixture case {}: missing {} or {}",
                case.name,
                case.trace_path.display(),
                case.csv_path.display()
            );
            continue;
        }

        any_fixture = true;

        let trace = TraceBundle::open(&case.trace_path)?;
        let report = counter_export::report(&trace)?;
        let reference_rows = parse_reference_csv(&case.csv_path)?;

        assert_eq!(
            report.rows.len(),
            reference_rows.len(),
            "fixture {}: exported row count should match Xcode CSV encoder rows",
            case.name
        );

        for (actual, expected) in report.rows.iter().zip(reference_rows.iter()) {
            assert_eq!(
                actual.encoder_index, expected.index,
                "fixture {}: encoder index mismatch for {}",
                case.name, expected.encoder_label
            );

            if !actual.encoder_label.is_empty() && !expected.encoder_label.is_empty() {
                assert_eq!(
                    canonicalize_label(&actual.encoder_label),
                    canonicalize_label(&expected.encoder_label),
                    "fixture {}: encoder label mismatch at row {}",
                    case.name,
                    expected.index
                );
            }

            assert_metric_close(
                case.name,
                expected.index,
                "Kernel Invocations",
                Some(actual.kernel_invocations as f64),
                expected.kernel_invocations,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "ALU Utilization",
                actual.alu_utilization_percent,
                expected.alu_utilization_percent,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "Kernel Occupancy",
                actual.occupancy_percent,
                expected.kernel_occupancy_percent,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "Device Memory Bandwidth",
                actual.device_memory_bandwidth_gbps,
                expected.device_memory_bandwidth_gbps,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "GPU Read Bandwidth",
                actual.gpu_read_bandwidth_gbps,
                expected.gpu_read_bandwidth_gbps,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "GPU Write Bandwidth",
                actual.gpu_write_bandwidth_gbps,
                expected.gpu_write_bandwidth_gbps,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "Buffer L1 Miss Rate",
                actual.buffer_l1_miss_rate_percent,
                expected.buffer_l1_miss_rate_percent,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "Buffer L1 Read Accesses",
                actual.buffer_l1_read_accesses,
                expected.buffer_l1_read_accesses,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "Buffer L1 Read Bandwidth",
                actual.buffer_l1_read_bandwidth_gbps,
                expected.buffer_l1_read_bandwidth_gbps,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "Buffer L1 Write Accesses",
                actual.buffer_l1_write_accesses,
                expected.buffer_l1_write_accesses,
                0.5,
            );
            assert_metric_close(
                case.name,
                expected.index,
                "Buffer L1 Write Bandwidth",
                actual.buffer_l1_write_bandwidth_gbps,
                expected.buffer_l1_write_bandwidth_gbps,
                0.5,
            );
        }
    }

    if !any_fixture {
        eprintln!("skipping fixture validation: no perf traces or Xcode CSV fixtures available");
    }

    Ok(())
}

fn go_repo_root() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let sibling = manifest_dir.join("../gputrace");
    sibling.is_dir().then_some(sibling)
}

fn fixture_case(
    repo_root: &Path,
    fixture_dir: &'static str,
    trace_name: &'static str,
    csv_name: &'static str,
) -> FixtureCase {
    let base = repo_root.join("testdata/traces").join(fixture_dir);
    FixtureCase {
        name: fixture_dir,
        trace_path: base.join(trace_name),
        csv_path: base.join(csv_name),
    }
}

fn parse_reference_csv(path: &Path) -> Result<Vec<ReferenceCounterRow>, Box<dyn Error>> {
    let mut reader = csv::Reader::from_path(path)?;
    let headers = reader.headers()?.clone();
    let index_col = find_column(&headers, "Index")?;
    let encoder_label_col = find_column(&headers, "Encoder Label")?;
    let invocations_col = find_column(&headers, "Kernel Invocations")?;
    let alu_col = find_column(&headers, "ALU Utilization")?;
    let occupancy_col = find_column(&headers, "Kernel Occupancy")?;
    let device_bw_col = find_column(&headers, "Device Memory Bandwidth")?;
    let gpu_read_bw_col = find_column(&headers, "GPU Read Bandwidth")?;
    let gpu_write_bw_col = find_column(&headers, "GPU Write Bandwidth")?;
    let buffer_l1_miss_rate_col = find_column(&headers, "Buffer L1 Miss Rate")?;
    let buffer_l1_read_accesses_col = find_column(&headers, "Buffer L1 Read Accesses")?;
    let buffer_l1_read_bw_col = find_column(&headers, "Buffer L1 Read Bandwidth")?;
    let buffer_l1_write_accesses_col = find_column(&headers, "Buffer L1 Write Accesses")?;
    let buffer_l1_write_bw_col = find_column(&headers, "Buffer L1 Write Bandwidth")?;

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        rows.push(ReferenceCounterRow {
            index: parse_usize(record.get(index_col))?,
            encoder_label: record.get(encoder_label_col).unwrap_or_default().to_owned(),
            kernel_invocations: parse_optional_f64(record.get(invocations_col)),
            alu_utilization_percent: parse_optional_f64(record.get(alu_col)),
            kernel_occupancy_percent: parse_optional_f64(record.get(occupancy_col)),
            device_memory_bandwidth_gbps: parse_optional_f64(record.get(device_bw_col)),
            gpu_read_bandwidth_gbps: parse_optional_f64(record.get(gpu_read_bw_col)),
            gpu_write_bandwidth_gbps: parse_optional_f64(record.get(gpu_write_bw_col)),
            buffer_l1_miss_rate_percent: parse_optional_f64(record.get(buffer_l1_miss_rate_col)),
            buffer_l1_read_accesses: parse_optional_f64(record.get(buffer_l1_read_accesses_col)),
            buffer_l1_read_bandwidth_gbps: parse_optional_f64(record.get(buffer_l1_read_bw_col)),
            buffer_l1_write_accesses: parse_optional_f64(record.get(buffer_l1_write_accesses_col)),
            buffer_l1_write_bandwidth_gbps: parse_optional_f64(record.get(buffer_l1_write_bw_col)),
        });
    }
    Ok(rows)
}

fn find_column(headers: &StringRecord, name: &str) -> Result<usize, Box<dyn Error>> {
    headers
        .iter()
        .position(|header| header == name)
        .ok_or_else(|| format!("missing CSV column {name}").into())
}

fn parse_usize(value: Option<&str>) -> Result<usize, Box<dyn Error>> {
    value
        .unwrap_or_default()
        .trim()
        .parse::<usize>()
        .map_err(|error| error.into())
}

fn parse_optional_f64(value: Option<&str>) -> Option<f64> {
    let value = value.unwrap_or_default().trim();
    if value.is_empty() {
        None
    } else {
        value.parse::<f64>().ok()
    }
}

fn canonicalize_label(label: &str) -> String {
    label.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn assert_metric_close(
    fixture_name: &str,
    encoder_index: usize,
    metric_name: &str,
    actual: Option<f64>,
    expected: Option<f64>,
    tolerance: f64,
) {
    match (actual, expected) {
        (Some(actual), Some(expected)) => {
            let delta = (actual - expected).abs();
            assert!(
                delta <= tolerance,
                "fixture {} encoder {} {} mismatch: actual {:.4}, expected {:.4}, delta {:.4}, tolerance {:.4}",
                fixture_name,
                encoder_index,
                metric_name,
                actual,
                expected,
                delta,
                tolerance
            );
        }
        (None, Some(expected)) => {
            panic!(
                "fixture {} encoder {} missing {} in Rust export; expected {:.4}",
                fixture_name, encoder_index, metric_name, expected
            );
        }
        _ => {}
    }
}
