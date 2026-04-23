use serde::Serialize;

use crate::analysis;
use crate::error::{Error, Result};
use crate::timing;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InsightSeverity {
    Critical,
    High,
    Medium,
    Low,
    Info,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InsightType {
    Bottleneck,
    Optimization,
    AntiPattern,
    Info,
}

#[derive(Debug, Clone, Serialize)]
pub struct PerformanceInsight {
    pub insight_type: InsightType,
    pub severity: InsightSeverity,
    pub shader_name: Option<String>,
    pub title: String,
    pub description: String,
    pub recommendations: Vec<String>,
    pub impact: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InsightsReport {
    pub synthetic: bool,
    pub total_gpu_time_ms: f64,
    pub top_bottlenecks: Vec<String>,
    pub critical_count: usize,
    pub high_count: usize,
    pub medium_count: usize,
    pub low_count: usize,
    pub info_count: usize,
    pub insights: Vec<PerformanceInsight>,
}

pub fn report(trace: &TraceBundle, min_level: Option<&str>) -> Result<InsightsReport> {
    let analysis = analysis::analyze(trace);
    let timing = timing::report(trace)?;
    let time_label = if timing.synthetic {
        "synthetic GPU time"
    } else {
        "GPU time"
    };
    let mut insights = Vec::new();

    if let Some(top_kernel) = timing.kernels.first() {
        if top_kernel.percent_of_total > 50.0 {
            insights.push(PerformanceInsight {
                insight_type: InsightType::Bottleneck,
                severity: InsightSeverity::Critical,
                shader_name: Some(top_kernel.name.clone()),
                title: format!("{} dominates GPU time", top_kernel.name),
                description: format!(
                    "{} accounts for {:.1}% of {} across {} dispatches.",
                    top_kernel.name,
                    top_kernel.percent_of_total,
                    time_label,
                    top_kernel.dispatch_count
                ),
                recommendations: vec![
                    "Profile this shader path first.".to_owned(),
                    "Reduce work per dispatch or fuse adjacent passes only if total work drops."
                        .to_owned(),
                    "Check bound buffers and source for obvious bandwidth-heavy loops.".to_owned(),
                ],
                impact: Some("Dominates end-to-end GPU execution time.".to_owned()),
            });
        } else if top_kernel.percent_of_total > 30.0 {
            insights.push(PerformanceInsight {
                insight_type: InsightType::Bottleneck,
                severity: InsightSeverity::High,
                shader_name: Some(top_kernel.name.clone()),
                title: format!("{} is a major bottleneck", top_kernel.name),
                description: format!(
                    "{} accounts for {:.1}% of {} across {} dispatches.",
                    top_kernel.name,
                    top_kernel.percent_of_total,
                    time_label,
                    top_kernel.dispatch_count
                ),
                recommendations: vec![
                    "Focus optimization work on this shader before lower-rank kernels.".to_owned(),
                    "Inspect source attribution with `shader-source` and `correlate`.".to_owned(),
                ],
                impact: Some("Large contributor to GPU runtime.".to_owned()),
            });
        }
    }

    for kernel in &timing.kernels {
        let avg_duration_us =
            kernel.synthetic_duration_ns as f64 / kernel.dispatch_count.max(1) as f64 / 1_000.0;
        if kernel.dispatch_count > 100 && avg_duration_us < 50.0 && kernel.percent_of_total > 5.0 {
            insights.push(PerformanceInsight {
                insight_type: InsightType::Optimization,
                severity: InsightSeverity::High,
                shader_name: Some(kernel.name.clone()),
                title: format!("{} shows dispatch overhead pressure", kernel.name),
                description: format!(
                    "{} runs {} times with only {:.1} us average {}.",
                    kernel.name,
                    kernel.dispatch_count,
                    avg_duration_us,
                    if timing.synthetic {
                        "synthetic duration"
                    } else {
                        "duration"
                    }
                ),
                recommendations: vec![
                    "Batch small dispatches when semantics allow.".to_owned(),
                    "Consider kernel fusion if the intermediate state is cheap to keep on-GPU."
                        .to_owned(),
                    "Check whether encoder transitions are forcing extra micro-dispatches."
                        .to_owned(),
                ],
                impact: Some(
                    "Likely wasting CPU submission overhead on tiny dispatches.".to_owned(),
                ),
            });
        }
    }

    if analysis.shared_buffer_count > 0
        && analysis.shared_buffer_count * 2 >= analysis.buffer_count.max(1)
    {
        insights.push(PerformanceInsight {
            insight_type: InsightType::AntiPattern,
            severity: InsightSeverity::Medium,
            shader_name: None,
            title: "Buffer sharing is widespread across encoders".to_owned(),
            description: format!(
                "{} of {} attributed buffers are touched by multiple encoders.",
                analysis.shared_buffer_count, analysis.buffer_count
            ),
            recommendations: vec![
                "Inspect high-traffic shared buffers with `buffer-access`.".to_owned(),
                "Reduce cross-encoder sharing where it causes ordering constraints.".to_owned(),
                "Check whether some shared buffers can be split by phase or lifetime.".to_owned(),
            ],
            impact: Some(
                "Can increase synchronization pressure and dependency complexity.".to_owned(),
            ),
        });
    }

    if analysis.single_use_buffer_count > 0
        && analysis.single_use_buffer_count * 2 >= analysis.buffer_count.max(1)
    {
        insights.push(PerformanceInsight {
            insight_type: InsightType::Optimization,
            severity: InsightSeverity::Medium,
            shader_name: None,
            title: "Trace contains many single-use buffers".to_owned(),
            description: format!(
                "{} of {} buffers appear in exactly one attributed dispatch.",
                analysis.single_use_buffer_count, analysis.buffer_count
            ),
            recommendations: vec![
                "Review whether short-lived temporaries can be pooled or reused.".to_owned(),
                "Inspect bundle inventory to find large one-shot allocations.".to_owned(),
            ],
            impact: Some("Suggests allocation churn or poor buffer reuse.".to_owned()),
        });
    }

    if analysis.long_lived_buffer_count > 0 {
        insights.push(PerformanceInsight {
            insight_type: InsightType::Info,
            severity: InsightSeverity::Low,
            shader_name: None,
            title: "Long-lived buffers span much of the trace".to_owned(),
            description: format!(
                "{} buffers live for more than 3x the average dispatch lifetime.",
                analysis.long_lived_buffer_count
            ),
            recommendations: vec![
                "Inspect `buffer-timeline` for persistent allocations with low reuse density."
                    .to_owned(),
                "Separate hot persistent buffers from cold archival buffers if possible."
                    .to_owned(),
            ],
            impact: Some("May indicate memory residency pressure.".to_owned()),
        });
    }

    if analysis.buffer_inventory_aliases > 0 {
        insights.push(PerformanceInsight {
            insight_type: InsightType::Info,
            severity: InsightSeverity::Low,
            shader_name: None,
            title: "Bundle contains aliased backing buffers".to_owned(),
            description: format!(
                "The trace bundle exposes {} backing-buffer aliases.",
                analysis.buffer_inventory_aliases
            ),
            recommendations: vec![
                "Inspect `buffers diff` and `buffer-access` before assuming unique resources."
                    .to_owned(),
                "Treat alias-heavy traces carefully when reasoning about memory footprint."
                    .to_owned(),
            ],
            impact: Some("Can obscure true resource identity during analysis.".to_owned()),
        });
    }

    if analysis.kernel_count > 50 {
        insights.push(PerformanceInsight {
            insight_type: InsightType::Info,
            severity: InsightSeverity::Low,
            shader_name: None,
            title: "Trace uses many unique kernels".to_owned(),
            description: format!(
                "The trace attributes work to {} distinct kernels.",
                analysis.kernel_count
            ),
            recommendations: vec![
                "Check whether kernel specialization has exploded the shader set.".to_owned(),
                "Group related kernels with `tree --group-by pipeline`.".to_owned(),
            ],
            impact: Some(
                "May increase shader management and optimization surface area.".to_owned(),
            ),
        });
    }

    if let Some(top_kernel) = timing.kernels.first()
        && top_kernel.percent_of_total > 70.0
    {
        insights.push(PerformanceInsight {
            insight_type: InsightType::Info,
            severity: InsightSeverity::Info,
            shader_name: Some(top_kernel.name.clone()),
            title: "Optimization focus is unusually clear".to_owned(),
            description: format!(
                "{} alone accounts for {:.1}% of {}.",
                top_kernel.name, top_kernel.percent_of_total, time_label
            ),
            recommendations: vec![
                "Spend optimization time on this shader before broad trace-wide cleanup."
                    .to_owned(),
            ],
            impact: Some("A single hotspot likely dominates returns.".to_owned()),
        });
    }

    insights.sort_by_key(|insight| insight.severity);

    let min_severity = match min_level {
        Some(level) => Some(parse_severity(level)?),
        None => None,
    };
    if let Some(min_severity) = min_severity {
        insights.retain(|insight| insight.severity <= min_severity);
    }

    let critical_count = insights
        .iter()
        .filter(|insight| insight.severity == InsightSeverity::Critical)
        .count();
    let high_count = insights
        .iter()
        .filter(|insight| insight.severity == InsightSeverity::High)
        .count();
    let medium_count = insights
        .iter()
        .filter(|insight| insight.severity == InsightSeverity::Medium)
        .count();
    let low_count = insights
        .iter()
        .filter(|insight| insight.severity == InsightSeverity::Low)
        .count();
    let info_count = insights
        .iter()
        .filter(|insight| insight.severity == InsightSeverity::Info)
        .count();

    Ok(InsightsReport {
        synthetic: timing.synthetic,
        total_gpu_time_ms: timing.total_duration_ns as f64 / 1_000_000.0,
        top_bottlenecks: timing
            .kernels
            .iter()
            .take(5)
            .map(|kernel| kernel.name.clone())
            .collect(),
        critical_count,
        high_count,
        medium_count,
        low_count,
        info_count,
        insights,
    })
}

pub fn format_report(report: &InsightsReport) -> String {
    let mut out = String::new();
    out.push_str("=== GPU Performance Insights ===\n\n");
    if report.synthetic {
        out.push_str("Synthetic timing and trace attribution only.\n");
    } else {
        out.push_str("Profiler-backed timing with trace attribution.\n");
    }
    out.push_str(&format!(
        "Total GPU Time: {:.2} ms\n",
        report.total_gpu_time_ms
    ));
    out.push_str(&format!("Insights Found: {}\n", report.insights.len()));
    out.push_str(&format!(
        "  Critical: {}, High: {}, Medium: {}, Low: {}, Info: {}\n\n",
        report.critical_count,
        report.high_count,
        report.medium_count,
        report.low_count,
        report.info_count
    ));
    if !report.top_bottlenecks.is_empty() {
        out.push_str("Top Bottlenecks:\n");
        for (index, name) in report.top_bottlenecks.iter().enumerate() {
            out.push_str(&format!("  {}. {}\n", index + 1, name));
        }
        out.push('\n');
    }
    out.push_str("=== Detailed Insights ===\n\n");
    for (index, insight) in report.insights.iter().enumerate() {
        out.push_str(&format!(
            "[{}] [{}] {}\n",
            index + 1,
            severity_label(insight.severity),
            insight.title
        ));
        if let Some(shader_name) = &insight.shader_name {
            out.push_str(&format!("    Shader: {shader_name}\n"));
        }
        out.push_str(&format!(
            "    Type: {}\n\n",
            insight_type_label(insight.insight_type)
        ));
        out.push_str(&format!("    {}\n\n", insight.description));
        if let Some(impact) = &insight.impact {
            out.push_str(&format!("    Impact: {impact}\n\n"));
        }
        if !insight.recommendations.is_empty() {
            out.push_str("    Recommendations:\n");
            for recommendation in &insight.recommendations {
                out.push_str(&format!("      - {recommendation}\n"));
            }
            out.push('\n');
        }
        out.push_str(
            "    ----------------------------------------------------------------------\n\n",
        );
    }
    out
}

fn parse_severity(value: &str) -> Result<InsightSeverity> {
    match value.to_ascii_lowercase().as_str() {
        "critical" => Ok(InsightSeverity::Critical),
        "high" => Ok(InsightSeverity::High),
        "medium" => Ok(InsightSeverity::Medium),
        "low" => Ok(InsightSeverity::Low),
        "info" => Ok(InsightSeverity::Info),
        _ => Err(Error::InvalidInput(format!(
            "unknown severity level: {value} (expected critical, high, medium, low, or info)"
        ))),
    }
}

fn severity_label(value: InsightSeverity) -> &'static str {
    match value {
        InsightSeverity::Critical => "CRITICAL",
        InsightSeverity::High => "HIGH",
        InsightSeverity::Medium => "MEDIUM",
        InsightSeverity::Low => "LOW",
        InsightSeverity::Info => "INFO",
    }
}

fn insight_type_label(value: InsightType) -> &'static str {
    match value {
        InsightType::Bottleneck => "BOTTLENECK",
        InsightType::Optimization => "OPTIMIZATION",
        InsightType::AntiPattern => "ANTI-PATTERN",
        InsightType::Info => "INFO",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_severity_names() {
        assert_eq!(
            parse_severity("critical").expect("critical should parse"),
            InsightSeverity::Critical
        );
        assert_eq!(
            parse_severity("INFO").expect("info should parse"),
            InsightSeverity::Info
        );
        assert!(parse_severity("urgent").is_err());
    }

    #[test]
    fn formats_report() {
        let report = InsightsReport {
            synthetic: true,
            total_gpu_time_ms: 1.25,
            top_bottlenecks: vec!["kernel".into()],
            critical_count: 1,
            high_count: 0,
            medium_count: 0,
            low_count: 0,
            info_count: 0,
            insights: vec![PerformanceInsight {
                insight_type: InsightType::Bottleneck,
                severity: InsightSeverity::Critical,
                shader_name: Some("kernel".into()),
                title: "kernel dominates GPU time".into(),
                description: "kernel accounts for most time".into(),
                recommendations: vec!["profile it".into()],
                impact: Some("dominates runtime".into()),
            }],
        };
        let output = format_report(&report);
        assert!(output.contains("GPU Performance Insights"));
        assert!(output.contains("[CRITICAL]"));
        assert!(output.contains("kernel dominates GPU time"));
    }
}
