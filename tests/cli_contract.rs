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
        "dump",
        "dump-records",
        "export-counters",
        "raw-counters",
        "validate-counters",
        "profiler",
        "mtlb-functions",
        "xcode-counters",
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
fn export_counters_help_mentions_trace_and_formats() {
    let help = render_help(&["export-counters"]);

    assert!(help.contains("export-counters"));
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--format <FORMAT>"));
    assert!(help.contains("[default: csv]"));
    assert!(help.contains("APS counter sample rows"));
    assert!(help.contains("metric_metadata"));
    assert!(help.contains("xcode-counters"));
}

#[test]
fn raw_counters_help_mentions_trace_and_formats() {
    let help = render_help(&["raw-counters"]);

    assert!(help.contains("raw-counters"));
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--format <FORMAT>"));
    assert!(help.contains("[default: text]"));
    assert!(help.contains("APSCounterData"));
    assert!(help.contains("derived.js"));
    assert!(help.contains("independent of Xcode Counters.csv"));
}

#[test]
fn validate_counters_help_mentions_csv_and_tolerance() {
    let help = render_help(&["validate-counters"]);

    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--csv <CSV>"));
    assert!(help.contains("--tolerance <TOLERANCE>"));
}

#[test]
fn perfcounters_validate_alias_parses_to_validate_counters() {
    let cli = Cli::try_parse_from([
        "gputrace",
        "perfcounters-validate",
        "trace.gputrace",
        "--csv",
        "Counters.csv",
    ]);
    assert!(cli.is_ok());
}

#[test]
fn raw_counters_json_parses() {
    let cli = Cli::try_parse_from([
        "gputrace",
        "raw-counters",
        "trace.gputrace",
        "--format",
        "json",
    ]);
    assert!(cli.is_ok());
}

#[test]
fn dump_help_matches_go_surface() {
    let help = render_help(&["dump"]);

    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--filter <FILTER>"));
    assert!(help.contains("--buffers-only"));
    assert!(help.contains("--dispatch-only"));
    assert!(help.contains("--encoders-only"));
    assert!(help.contains("--command-buffer <COMMAND_BUFFER>"));
    assert!(help.contains("--json"));
    assert!(help.contains("--full"));
}

#[test]
fn mtlb_help_lists_go_subcommands() {
    let help = render_help(&["mtlb"]);

    for subcommand in [
        "list",
        "info",
        "inventory",
        "stats",
        "functions",
        "export-functions",
        "extract",
    ] {
        assert!(
            help.contains(subcommand),
            "mtlb help should mention {subcommand}\n{help}"
        );
    }
}

#[test]
fn mtlb_extract_help_mentions_selection_and_output_flags() {
    let help = render_help(&["mtlb", "extract"]);

    assert!(help.contains("<PATH>"));
    assert!(help.contains("--output <OUTPUT>"));
    assert!(help.contains("--library <LIBRARY>"));
    assert!(help.contains("--all"));
    assert!(help.contains("--output-dir <OUTPUT_DIR>"));
    assert!(help.contains("--format <FORMAT>"));
}

#[test]
fn mtlb_export_functions_defaults_to_csv() {
    let help = render_help(&["mtlb", "export-functions"]);

    assert!(help.contains("[default: csv]"));
}

#[test]
fn timing_profiler_alias_parses() {
    let cli = Cli::try_parse_from(["gputrace", "timing-profiler", "trace.gputrace", "--json"]);
    assert!(cli.is_ok());
}

#[test]
fn xcode_status_help_keeps_trace_and_format_flags() {
    let help = render_help(&["xcode-status"]);
    assert!(help.contains("xcode-status"));
    assert!(help.contains("--format"));
}

#[test]
fn xcode_counters_help_mentions_metric_and_top() {
    let help = render_help(&["xcode-counters"]);
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--metric <METRIC>"));
    assert!(help.contains("--top <TOP>"));
    assert!(help.contains("[default: summary]"));
}

#[test]
fn xcode_wait_help_mentions_status_and_timeout() {
    let help = render_help(&["xcode-wait"]);
    assert!(help.contains("--status"));
    assert!(help.contains("--timeout-seconds"));
}

#[test]
fn xcode_check_permissions_help_mentions_no_prompt() {
    let help = render_help(&["xcode-check-permissions"]);
    assert!(help.contains("--no-prompt"));
    assert!(help.contains("--format"));
}

#[test]
fn xcode_click_button_help_mentions_target() {
    let help = render_help(&["xcode-click-button"]);
    assert!(help.contains("<TARGET>"));
}

#[test]
fn xcode_checkboxes_help_mentions_format() {
    let help = render_help(&["xcode-checkboxes"]);
    assert!(help.contains("--format"));
}

#[test]
fn xcode_close_help_mentions_format() {
    let help = render_help(&["xcode-close"]);
    assert!(help.contains("--format"));
}

#[test]
fn xcode_ensure_checked_help_mentions_target() {
    let help = render_help(&["xcode-ensure-checked"]);
    assert!(help.contains("<TARGET>"));
}

#[test]
fn xcode_select_tab_help_mentions_target() {
    let help = render_help(&["xcode-select-tab"]);
    assert!(help.contains("<TARGET>"));
}

#[test]
fn xcode_toggle_checkbox_help_mentions_target() {
    let help = render_help(&["xcode-toggle-checkbox"]);
    assert!(help.contains("<TARGET>"));
}

#[test]
fn xcode_show_performance_help_mentions_format() {
    let help = render_help(&["xcode-show-performance"]);
    assert!(help.contains("--format"));
}

#[test]
fn xcode_show_summary_help_mentions_format() {
    let help = render_help(&["xcode-show-summary"]);
    assert!(help.contains("--format"));
}

#[test]
fn xcode_show_counters_help_mentions_format() {
    let help = render_help(&["xcode-show-counters"]);
    assert!(help.contains("--format"));
}

#[test]
fn xcode_show_memory_help_mentions_format() {
    let help = render_help(&["xcode-show-memory"]);
    assert!(help.contains("--format"));
}

#[test]
fn xcode_show_dependencies_help_mentions_format() {
    let help = render_help(&["xcode-show-dependencies"]);
    assert!(help.contains("--format"));
}

#[test]
fn xcode_export_counters_help_mentions_output() {
    let help = render_help(&["xcode-export-counters"]);
    assert!(help.contains("<OUTPUT>"));
    assert!(help.contains("--format"));
}

#[test]
fn xcode_export_memory_help_mentions_output() {
    let help = render_help(&["xcode-export-memory"]);
    assert!(help.contains("<OUTPUT>"));
    assert!(help.contains("--format"));
}

#[test]
fn xcode_profile_help_mentions_no_prompt() {
    let help = render_help(&["xcode-profile"]);
    assert!(help.contains("--no-prompt"));
    assert!(help.contains("--timeout-seconds"));
    assert!(help.contains("--wait-seconds"));
    assert!(help.contains("--force"));
    for subcommand in [
        "open",
        "check-status",
        "check-permissions",
        "run",
        "run-profile",
        "wait-profile",
        "export",
        "xcode-export-counters",
        "xcode-export-memory",
        "list-windows",
        "list-buttons",
        "show-performance",
    ] {
        assert!(
            help.contains(subcommand),
            "xcode-profile help should mention {subcommand}\n{help}"
        );
    }
}

#[test]
fn xcode_profile_aliases_and_nested_workflow_parse() {
    for args in [
        vec!["gputrace", "xp", "run", "trace.gputrace", "--no-prompt"],
        vec![
            "gputrace",
            "collect-xcode-profile",
            "check-status",
            "trace.gputrace",
            "--format",
            "json",
        ],
        vec!["gputrace", "xcode-profile", "run-profile", "trace.gputrace"],
        vec![
            "gputrace",
            "xcode-profile",
            "wait-profile",
            "trace.gputrace",
            "--timeout-seconds",
            "1",
        ],
        vec![
            "gputrace",
            "xcode-profile",
            "xcode-export-counters",
            "Counters.csv",
            "--trace",
            "trace.gputrace",
        ],
    ] {
        let cli = Cli::try_parse_from(args.clone());
        assert!(cli.is_ok(), "failed to parse {args:?}: {cli:?}");
    }
}

#[test]
fn shaders_help_mentions_format_and_search_path() {
    let help = render_help(&["shaders"]);
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("--format <FORMAT>"));
    assert!(help.contains("--search-path <SEARCH_PATHS>"));
}

#[test]
fn shader_hotspots_help_mentions_shader_and_format() {
    let help = render_help(&["shader-hotspots"]);
    assert!(help.contains("<TRACE>"));
    assert!(help.contains("<SHADER>"));
    assert!(help.contains("--format <FORMAT>"));
    assert!(help.contains("--search-path <SEARCH_PATHS>"));
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
fn diff_help_keeps_markdown_and_format_outputs() {
    let help = render_help(&["diff"]);

    assert!(help.contains("[LEFT]"));
    assert!(help.contains("[RIGHT]"));
    assert!(help.contains("--markdown"));
    assert!(help.contains("--json"));
    assert!(help.contains("--md-out <MD_OUT>"));
    assert!(help.contains("--format <FORMAT>"));
    assert!(help.contains("--limit <LIMIT>"));
    assert!(help.contains("--min-delta-us <MIN_DELTA_US>"));
    assert!(help.contains("--only-encoder <ONLY_ENCODER>"));
    assert!(help.contains("--only-function <ONLY_FUNCTION>"));
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
            "diff",
            "left.gputrace",
            "right.gputrace",
            "--format",
            "markdown",
            "--md-out",
            "diff.md",
            "--limit",
            "5",
            "--min-delta-us",
            "10",
            "--only-encoder",
            "2",
            "--only-function",
            "gemm",
        ],
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
