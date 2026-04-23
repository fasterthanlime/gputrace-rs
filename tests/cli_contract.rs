use clap::{CommandFactory, Parser};
use gputrace_rs::cli::Cli;

fn render_help(path: &[&str]) -> String {
    let mut command = Cli::command();
    for segment in path {
        command = command
            .find_subcommand_mut(segment)
            .unwrap_or_else(|| panic!("missing subcommand {segment}"))
            .clone();
    }

    let mut buffer = Vec::new();
    command.write_long_help(&mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

#[test]
fn top_level_help_lists_current_analysis_commands() {
    let help = render_help(&[]);

    for subcommand in [
        "analyze-usage",
        "clear-buffers",
        "timeline",
        "fences",
        "api-calls",
        "dump-records",
        "profiler",
        "mtlb-functions",
        "xcode-windows",
        "buffers",
    ] {
        assert!(
            help.contains(subcommand),
            "top-level help should mention {subcommand}\n{help}"
        );
    }
}

#[test]
fn analyze_usage_help_keeps_expected_formats() {
    let help = render_help(&["analyze-usage"]);

    assert!(help.contains("analyze-usage"));
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--format <FORMAT>"));
    assert!(help.contains("[default: text]"));
}

#[test]
fn clear_buffers_help_keeps_current_safety_flags() {
    let help = render_help(&["clear-buffers"]);

    assert!(help.contains("clear-buffers"));
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--dry-run"));
    assert!(help.contains("--yes"));
    assert!(help.contains("--format <FORMAT>"));
}

#[test]
fn timeline_help_mentions_raw_and_export_formats() {
    let help = render_help(&["timeline"]);

    assert!(help.contains("timeline"));
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--raw"));
    assert!(help.contains("--format <FORMAT>"));
}

#[test]
fn fences_help_keeps_format_flag() {
    let help = render_help(&["fences"]);

    assert!(help.contains("fences"));
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--format <FORMAT>"));
}

#[test]
fn api_calls_help_keeps_kernel_filter() {
    let help = render_help(&["api-calls"]);

    assert!(help.contains("api-calls"));
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--kernel <KERNEL>"));
    assert!(help.contains("--format <FORMAT>"));
}

#[test]
fn dump_records_help_keeps_filters() {
    let help = render_help(&["dump-records"]);

    for flag in [
        "--type <RECORD_TYPE>",
        "--contains <CONTAINS>",
        "--start-index <START_INDEX>",
        "--limit <LIMIT>",
        "--hex-preview",
    ] {
        assert!(
            help.contains(flag),
            "dump-records help missing {flag}\n{help}"
        );
    }
}

#[test]
fn buffers_help_lists_subcommands() {
    let help = render_help(&["buffers"]);

    for subcommand in ["list", "inspect", "diff"] {
        assert!(
            help.contains(subcommand),
            "buffers help should mention {subcommand}\n{help}"
        );
    }
}

#[test]
fn buffers_list_help_keeps_sorting_contract() {
    let help = render_help(&["buffers", "list"]);

    for flag in [
        "--format <FORMAT>",
        "--sort <SORT>",
        "--min-size <MIN_SIZE>",
    ] {
        assert!(
            help.contains(flag),
            "buffers list help missing {flag}\n{help}"
        );
    }
}

#[test]
fn buffers_inspect_help_keeps_content_flags() {
    let help = render_help(&["buffers", "inspect"]);

    for flag in [
        "--bytes <BYTES>",
        "--inspect-format <INSPECT_FORMAT>",
        "--format <FORMAT>",
    ] {
        assert!(
            help.contains(flag),
            "buffers inspect help missing {flag}\n{help}"
        );
    }
}

#[test]
fn important_top_level_commands_parse_their_existing_contracts() {
    for argv in [
        vec!["gputrace", "analyze-usage", "trace.gputrace"],
        vec![
            "gputrace",
            "analyze-usage",
            "trace.gputrace",
            "--format",
            "dot",
        ],
        vec!["gputrace", "clear-buffers", "--dry-run", "trace.gputrace"],
        vec![
            "gputrace",
            "clear-buffers",
            "--yes",
            "--format",
            "json",
            "trace.gputrace",
        ],
        vec![
            "gputrace",
            "timeline",
            "trace.gputrace",
            "--format",
            "chrome",
        ],
        vec![
            "gputrace",
            "timeline",
            "trace.gputrace",
            "--raw",
            "--format",
            "json",
        ],
        vec!["gputrace", "fences", "trace.gputrace", "--format", "json"],
        vec!["gputrace", "profiler", "trace.gputrace", "--format", "json"],
        vec![
            "gputrace",
            "mtlb-inventory",
            "trace.gputrace",
            "--format",
            "json",
        ],
        vec![
            "gputrace",
            "mtlb-stats",
            "trace.gputrace",
            "--format",
            "json",
        ],
        vec![
            "gputrace",
            "mtlb-functions",
            "trace.gputrace",
            "--used-only",
            "--format",
            "csv",
        ],
        vec![
            "gputrace",
            "api-calls",
            "trace.gputrace",
            "--kernel",
            "foo",
            "--format",
            "json",
        ],
        vec![
            "gputrace",
            "dump-records",
            "trace.gputrace",
            "--type",
            "Ct",
            "--contains",
            "kernel",
            "--start-index",
            "3",
            "--limit",
            "10",
            "--hex-preview",
            "--format",
            "json",
        ],
        vec![
            "gputrace",
            "buffers",
            "list",
            "trace.gputrace",
            "--format",
            "csv",
            "--sort",
            "name",
            "--min-size",
            "1MB",
        ],
        vec![
            "gputrace",
            "buffers",
            "inspect",
            "trace.gputrace",
            "buffer-1",
            "--bytes",
            "64",
            "--inspect-format",
            "float16",
            "--format",
            "json",
        ],
        vec![
            "gputrace",
            "buffers",
            "diff",
            "left.gputrace",
            "right.gputrace",
            "--format",
            "markdown",
        ],
        vec!["gputrace", "xcode-windows", "--format", "json"],
        vec![
            "gputrace",
            "xcode-inspect",
            "trace.gputrace",
            "--format",
            "json",
        ],
        vec![
            "gputrace",
            "xcode-buttons",
            "trace.gputrace",
            "--format",
            "json",
        ],
        vec!["gputrace", "xcode-tabs", "--format", "json"],
        vec![
            "gputrace",
            "xcode-ui-elements",
            "trace.gputrace",
            "--format",
            "json",
        ],
        vec![
            "gputrace",
            "xcode-menu-items",
            "File",
            "Export",
            "--format",
            "json",
        ],
    ] {
        Cli::try_parse_from(argv).unwrap();
    }
}

#[test]
fn clap_command_tree_is_consistent() {
    Cli::command().debug_assert();
}
