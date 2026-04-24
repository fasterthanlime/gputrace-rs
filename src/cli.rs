use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{io, io::Write};

use clap::{Args, Parser, Subcommand};

use crate::analysis;
use crate::analyze_usage;
use crate::apicalls;
use crate::automation::{self, OpenTraceOptions, XcodeLaunchMode, XcodeProfileRun};
use crate::buffer_timeline;
use crate::buffers;
use crate::clear_buffers;
use crate::commands;
use crate::correlate;
use crate::counter_export;
use crate::diff;
use crate::dump;
use crate::error::Result;
use crate::fences;
use crate::graphing;
use crate::insights;
use crate::markdown;
use crate::mtlb;
use crate::profiler;
use crate::shaders;
use crate::timeline;
use crate::timing;
use crate::trace::{RecordType, TraceBundle};
use crate::xcode_counters;

#[derive(Debug, Parser)]
#[command(name = "gputrace")]
#[command(version)]
#[command(about = "Tools for parsing, analyzing, diffing, and automating GPU traces")]
pub struct Cli {
    #[command(subcommand)]
    command: CommandSet,
}

#[derive(Debug, Subcommand)]
enum CommandSet {
    Stats(TracePath),
    Analyze(TracePath),
    AnalyzeUsage(AnalyzeUsageArgs),
    ApiCalls(ApiCallsArgs),
    ClearBuffers(ClearBuffersArgs),
    Dump(DumpArgs),
    DumpRecords(DumpRecordsArgs),
    ExportCounters(ExportCountersArgs),
    #[command(alias = "perfcounters-validate")]
    ValidateCounters(ValidateCountersArgs),
    Fences(FencesArgs),
    Mtlb(MtlbArgs),
    MtlbInventory(MtlbPathArgs),
    MtlbStats(MtlbPathArgs),
    MtlbFunctions(MtlbFunctionsArgs),
    Profiler(ProfilerArgs),
    Timeline(TimelineArgs),
    Kernels(KernelsArgs),
    Encoders(EncodersArgs),
    Dependencies(DependenciesArgs),
    Shaders(ShadersArgs),
    ShaderSource(ShaderSourceArgs),
    ShaderHotspots(ShaderHotspotsArgs),
    Correlate(CorrelateArgs),
    Timing(TimingArgs),
    #[command(hide = true)]
    TimingProfiler(TimingProfilerArgs),
    CommandBuffers(CommandBuffersArgs),
    BufferAccess(BufferAccessArgs),
    Tree(TreeArgs),
    Graph(GraphArgs),
    Buffers(BuffersArgs),
    BufferTimeline(BufferTimelineArgs),
    Insights(InsightsArgs),
    Diff(DiffArgs),
    Markdown(MarkdownArgs),
    Version(VersionArgs),
    XcodeButtons(XcodeTraceQueryArgs),
    XcodeCounters(XcodeCountersArgs),
    XcodeCheckPermissions(XcodePermissionArgs),
    XcodeCheckboxes(XcodeTraceQueryArgs),
    XcodeClickButton(XcodeActionArgs),
    XcodeClose(XcodeTraceQueryArgs),
    XcodeEnsureChecked(XcodeActionArgs),
    XcodeExportCounters(XcodeExportArgs),
    XcodeExportMemory(XcodeExportArgs),
    XcodeShowCounters(XcodeTraceQueryArgs),
    XcodeShowDependencies(XcodeTraceQueryArgs),
    XcodeShowMemory(XcodeTraceQueryArgs),
    XcodeShowPerformance(XcodeTraceQueryArgs),
    XcodeShowSummary(XcodeTraceQueryArgs),
    XcodeInspect(XcodeTraceQueryArgs),
    #[command(alias = "xp", alias = "collect-xcode-profile")]
    XcodeProfile(XcodeProfileArgs),
    XcodeSelectTab(XcodeActionArgs),
    XcodeStatus(XcodeTraceQueryArgs),
    XcodeTabs(XcodeTraceQueryArgs),
    XcodeToggleCheckbox(XcodeActionArgs),
    XcodeUiElements(XcodeTraceQueryArgs),
    XcodeWait(XcodeWaitArgs),
    XcodeWindows(XcodeFormatArgs),
    XcodeMenuItems(XcodeMenuItemsArgs),
}

#[derive(Debug, Args)]
struct TracePath {
    trace: PathBuf,
}

#[derive(Debug, Args)]
struct DiffArgs {
    left: Option<PathBuf>,
    right: Option<PathBuf>,
    #[arg(long = "left")]
    left_flag: Option<PathBuf>,
    #[arg(long = "right")]
    right_flag: Option<PathBuf>,
    #[arg(long)]
    markdown: bool,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    csv: bool,
    #[arg(long)]
    by: Option<String>,
    #[arg(long)]
    show_matches: bool,
    #[arg(long)]
    show_unmatched: bool,
    #[arg(long)]
    show_occurrences: bool,
    #[arg(long)]
    explain: bool,
    #[arg(long)]
    quick: bool,
    #[arg(long)]
    by_encoder: bool,
    #[arg(long)]
    bench_dir: Option<PathBuf>,
    #[arg(long)]
    md_out: Option<PathBuf>,
    #[arg(short, long)]
    format: Option<String>,
    #[arg(long, default_value_t = 10)]
    limit: usize,
    #[arg(long)]
    min_delta_us: Option<i64>,
    #[arg(long)]
    only_encoder: Option<usize>,
    #[arg(long)]
    only_function: Option<String>,
}

#[derive(Debug, Args)]
struct BufferTimelineArgs {
    trace: PathBuf,
    #[arg(short, long, default_value = "ascii")]
    format: String,
    #[arg(short, long, default_value_t = 100)]
    width: usize,
}

#[derive(Debug, Args)]
struct ApiCallsArgs {
    trace: PathBuf,
    #[arg(short = 'k', long)]
    kernel: Option<String>,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct AnalyzeUsageArgs {
    trace: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct ClearBuffersArgs {
    trace: PathBuf,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(short = 'y', long, default_value_t = false)]
    yes: bool,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct DumpArgs {
    trace: PathBuf,
    #[arg(long)]
    filter: Option<String>,
    #[arg(long, default_value_t = false)]
    buffers_only: bool,
    #[arg(long, default_value_t = false)]
    dispatch_only: bool,
    #[arg(long, default_value_t = false)]
    encoders_only: bool,
    #[arg(long)]
    command_buffer: Option<usize>,
    #[arg(long, default_value_t = false)]
    json: bool,
    #[arg(long, default_value_t = false)]
    full: bool,
}

#[derive(Debug, Args)]
struct VersionArgs {
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Debug, Args)]
struct DumpRecordsArgs {
    trace: PathBuf,
    #[arg(long = "type")]
    record_type: Option<String>,
    #[arg(long)]
    contains: Option<String>,
    #[arg(long, default_value_t = 0)]
    start_index: usize,
    #[arg(long)]
    limit: Option<usize>,
    #[arg(long)]
    hex_preview: bool,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct ExportCountersArgs {
    trace: PathBuf,
    #[arg(short, long, default_value = "csv")]
    format: String,
}

#[derive(Debug, Args)]
struct ValidateCountersArgs {
    trace: PathBuf,
    #[arg(long)]
    csv: Option<PathBuf>,
    #[arg(long, default_value_t = 0.5)]
    tolerance: f64,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct MtlbArgs {
    #[command(subcommand)]
    command: Option<MtlbCommand>,
    path: Option<PathBuf>,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Subcommand)]
enum MtlbCommand {
    List(MtlbPathArgs),
    Info(MtlbPathArgs),
    Inventory(MtlbPathArgs),
    Stats(MtlbPathArgs),
    Functions(MtlbFunctionsArgs),
    ExportFunctions(MtlbExportFunctionsArgs),
    Extract(MtlbExtractArgs),
}

#[derive(Debug, Args)]
struct MtlbPathArgs {
    path: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct MtlbFunctionsArgs {
    path: PathBuf,
    #[arg(long)]
    filter: Option<String>,
    #[arg(long, default_value_t = false)]
    used_only: bool,
    #[arg(long = "no-usage", default_value_t = false)]
    no_usage: bool,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct MtlbExportFunctionsArgs {
    path: PathBuf,
    #[arg(long)]
    filter: Option<String>,
    #[arg(long, default_value_t = false)]
    used_only: bool,
    #[arg(long = "no-usage", default_value_t = false)]
    no_usage: bool,
    #[arg(short, long, default_value = "csv")]
    format: String,
}

#[derive(Debug, Args)]
struct MtlbExtractArgs {
    path: PathBuf,
    #[arg(short, long)]
    output: Option<PathBuf>,
    #[arg(long)]
    library: Option<String>,
    #[arg(long, default_value_t = false)]
    all: bool,
    #[arg(long)]
    output_dir: Option<PathBuf>,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct FencesArgs {
    trace: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct XcodeFormatArgs {
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct XcodeTraceQueryArgs {
    trace: Option<PathBuf>,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct XcodeCountersArgs {
    trace: PathBuf,
    #[arg(long)]
    csv: Option<PathBuf>,
    #[arg(short, long, default_value = "summary")]
    format: String,
    #[arg(long)]
    metric: Option<String>,
    #[arg(long)]
    top: Option<usize>,
}

#[derive(Debug, Args)]
struct XcodePermissionArgs {
    #[arg(long, default_value_t = false)]
    no_prompt: bool,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct XcodeActionArgs {
    #[arg(long)]
    trace: Option<PathBuf>,
    target: String,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct XcodeWaitArgs {
    trace: Option<PathBuf>,
    #[arg(long, default_value_t = 300)]
    timeout_seconds: u64,
    #[arg(long, default_value = "complete")]
    status: String,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct XcodeMenuItemsArgs {
    menu_path: Vec<String>,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct XcodeExportArgs {
    #[arg(long)]
    trace: Option<PathBuf>,
    output: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct ProfilerArgs {
    path: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct TimelineArgs {
    trace: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
    #[arg(long, default_value_t = false)]
    raw: bool,
}

#[derive(Debug, Args)]
struct TimingProfilerArgs {
    trace: PathBuf,
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Debug, Args)]
struct InsightsArgs {
    trace: PathBuf,
    #[arg(long)]
    min_level: Option<String>,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct KernelsArgs {
    trace: PathBuf,
    #[arg(short, long)]
    filter: Option<String>,
    #[arg(short, long)]
    verbose: bool,
    #[arg(long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct EncodersArgs {
    trace: PathBuf,
    #[arg(short, long)]
    verbose: bool,
    #[arg(long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct DependenciesArgs {
    trace: PathBuf,
    #[arg(short, long, default_value = "dot")]
    format: String,
}

#[derive(Debug, Args)]
struct ShadersArgs {
    trace: PathBuf,
    #[arg(long = "search-path")]
    search_paths: Vec<PathBuf>,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct ShaderSourceArgs {
    trace: PathBuf,
    shader: String,
    #[arg(long = "search-path")]
    search_paths: Vec<PathBuf>,
    #[arg(long, default_value_t = 4)]
    context: usize,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct ShaderHotspotsArgs {
    trace: PathBuf,
    shader: String,
    #[arg(long = "search-path")]
    search_paths: Vec<PathBuf>,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct CorrelateArgs {
    trace: PathBuf,
    #[arg(long = "search-path")]
    search_paths: Vec<PathBuf>,
    #[arg(short, long)]
    verbose: bool,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct TimingArgs {
    trace: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct CommandBuffersArgs {
    trace: PathBuf,
    #[arg(short, long)]
    detailed: bool,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct BufferAccessArgs {
    trace: PathBuf,
    #[arg(short, long)]
    verbose: bool,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct TreeArgs {
    trace: PathBuf,
    #[arg(long, default_value = "encoder")]
    group_by: String,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct GraphArgs {
    trace: PathBuf,
    #[arg(long, default_value = "dot")]
    format: String,
    #[arg(long = "type", default_value = "hierarchy")]
    graph_type: String,
    #[arg(long)]
    show_timing: bool,
    #[arg(long)]
    show_memory: bool,
}

#[derive(Debug, Args)]
struct BuffersArgs {
    #[command(subcommand)]
    command: BuffersCommand,
}

#[derive(Debug, Subcommand)]
enum BuffersCommand {
    List {
        trace: PathBuf,
        #[arg(short, long, default_value = "table")]
        format: String,
        #[arg(long, default_value = "size")]
        sort: String,
        #[arg(long)]
        min_size: Option<String>,
    },
    Inspect {
        trace: PathBuf,
        buffer: String,
        #[arg(long, default_value_t = 256)]
        bytes: usize,
        #[arg(long = "inspect-format", default_value = "hex")]
        inspect_format: String,
        #[arg(short, long, default_value = "text")]
        format: String,
    },
    Diff {
        left: PathBuf,
        right: PathBuf,
        #[arg(short, long, default_value = "text")]
        format: String,
    },
}

#[derive(Debug, Args)]
struct MarkdownArgs {
    #[command(subcommand)]
    command: MarkdownCommand,
}

#[derive(Debug, Subcommand)]
enum MarkdownCommand {
    Render { input: String },
    Analyze { trace: PathBuf },
    Diff { left: PathBuf, right: PathBuf },
    Buffers { trace: PathBuf },
    BuffersDiff { left: PathBuf, right: PathBuf },
}

#[derive(Debug, Args)]
struct XcodeProfileArgs {
    #[command(subcommand)]
    command: Option<XcodeProfileCommand>,
    trace: Option<PathBuf>,
    #[arg(short, long)]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 300)]
    timeout_seconds: u64,
    #[arg(long, default_value_t = 0)]
    wait_seconds: u64,
    #[arg(long, default_value_t = false)]
    force: bool,
    #[arg(long, default_value_t = false)]
    no_prompt: bool,
    #[arg(long)]
    open_only: bool,
    #[arg(long)]
    activate: bool,
}

#[derive(Debug, Subcommand)]
enum XcodeProfileCommand {
    Open(XcodeProfileOpenArgs),
    Close(XcodeTraceQueryArgs),
    CheckStatus(XcodeTraceQueryArgs),
    CheckPermissions(XcodePermissionArgs),
    Run(XcodeProfileRunArgs),
    RunProfile(XcodeTraceQueryArgs),
    WaitProfile(XcodeProfileWaitArgs),
    Export(XcodeProfileExportArgs),
    XcodeExportCounters(XcodeExportArgs),
    XcodeExportMemory(XcodeExportArgs),
    ListWindows(XcodeFormatArgs),
    ListButtons(XcodeTraceQueryArgs),
    ClickButton(XcodeActionArgs),
    ListTabs(XcodeTraceQueryArgs),
    SelectTab(XcodeActionArgs),
    EnsureChecked(XcodeActionArgs),
    ToggleCheckbox(XcodeActionArgs),
    ShowPerformance(XcodeTraceQueryArgs),
    ShowSummary(XcodeTraceQueryArgs),
    ShowCounters(XcodeTraceQueryArgs),
    ShowMemory(XcodeTraceQueryArgs),
    ShowDependencies(XcodeTraceQueryArgs),
}

#[derive(Debug, Args)]
struct XcodeProfileOpenArgs {
    trace: PathBuf,
    #[arg(long, default_value_t = false)]
    foreground: bool,
    #[arg(long, default_value_t = 30)]
    timeout_seconds: u64,
}

#[derive(Debug, Args)]
struct XcodeProfileRunArgs {
    trace: PathBuf,
    #[arg(short, long)]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 300)]
    timeout_seconds: u64,
    #[arg(long, default_value_t = 0)]
    wait_seconds: u64,
    #[arg(long, default_value_t = false)]
    force: bool,
    #[arg(long, default_value_t = false)]
    no_prompt: bool,
    #[arg(long)]
    activate: bool,
}

#[derive(Debug, Args)]
struct XcodeProfileWaitArgs {
    trace: Option<PathBuf>,
    #[arg(long, default_value_t = 300)]
    timeout_seconds: u64,
    #[arg(short, long, default_value = "text")]
    format: String,
}

#[derive(Debug, Args)]
struct XcodeProfileExportArgs {
    #[arg(long)]
    trace: Option<PathBuf>,
    output: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        CommandSet::Stats(args) => {
            let trace = TraceBundle::open(args.trace)?;
            println!("{}", serde_json::to_string_pretty(&trace.summary())?);
        }
        CommandSet::Analyze(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = analysis::analyze(&trace);
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        CommandSet::AnalyzeUsage(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = analyze_usage::build(&trace)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", analyze_usage::format_text(&report)),
                "json" => print!("{}", analyze_usage::format_json(&report)?),
                "dot" => print!("{}", analyze_usage::format_dot(&report)),
                _ => return Err(crate::Error::Unsupported("unknown analyze-usage format")),
            }
        }
        CommandSet::ApiCalls(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = apicalls::report(&trace, args.kernel.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", apicalls::format_report(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown api-calls format")),
            }
        }
        CommandSet::ClearBuffers(args) => {
            let report = clear_buffers::inventory(&args.trace)?;
            if args.dry_run {
                match args.format.as_str() {
                    "text" | "table" => {
                        let mut text = clear_buffers::format_report(&report);
                        if !report.is_empty() {
                            text.push_str("\n\nDry run: no changes made\n");
                        }
                        print!("{text}");
                    }
                    "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                    _ => return Err(crate::Error::Unsupported("unknown clear-buffers format")),
                }
            } else if report.is_empty() {
                match args.format.as_str() {
                    "text" | "table" => print!("{}", clear_buffers::format_report(&report)),
                    "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                    _ => return Err(crate::Error::Unsupported("unknown clear-buffers format")),
                }
            } else {
                if !args.yes && !confirm_clear_buffers(&report)? {
                    print!("Cancelled\n");
                    return Ok(());
                }
                let run = clear_buffers::clear_report(&report)?;
                match args.format.as_str() {
                    "text" | "table" => {
                        print!(
                            "Zeroed {} buffer files ({})\n",
                            run.files_cleared,
                            clear_buffers::format_byte_size(run.bytes_cleared)
                        );
                    }
                    "json" => println!("{}", serde_json::to_string_pretty(&run)?),
                    _ => return Err(crate::Error::Unsupported("unknown clear-buffers format")),
                }
            }
        }
        CommandSet::Dump(args) => {
            let trace = TraceBundle::open(args.trace)?;
            if args.json {
                let report = apicalls::report(&trace, args.filter.as_deref())?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else if args.buffers_only {
                let report = buffers::analyze(&trace)?;
                print!("{}", buffers::format_table(&report));
            } else if args.dispatch_only {
                let report = apicalls::report(&trace, args.filter.as_deref())?;
                let report = apicalls::filter_call_kind_report(&report, "dispatch");
                print!("{}", apicalls::format_report(&report));
            } else if args.encoders_only {
                let report = commands::encoders(&trace)?;
                print!("{}", commands::format_encoders(&report, args.full));
            } else if let Some(command_buffer) = args.command_buffer {
                let report = apicalls::report(&trace, args.filter.as_deref())?;
                let filtered = apicalls::filter_command_buffer_report(&report, command_buffer);
                print!("{}", apicalls::format_report(&filtered));
            } else {
                let report = apicalls::report(&trace, args.filter.as_deref())?;
                print!("{}", apicalls::format_report(&report));
            }
        }
        CommandSet::DumpRecords(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let filter = dump::DumpFilter {
                record_type: args
                    .record_type
                    .as_deref()
                    .map(parse_record_type)
                    .transpose()?,
                text_contains: args.contains,
                start_index: args.start_index,
                limit: args.limit,
                include_hex_preview: args.hex_preview,
                max_preview_bytes: dump::DEFAULT_HEX_PREVIEW_BYTES,
            };
            let report = dump::parse_record_dump(&trace.capture_data()?, filter)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", dump::format_record_listing(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown dump-records format")),
            }
        }
        CommandSet::ExportCounters(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = counter_export::report(&trace)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", counter_export::format_report(&report)),
                "csv" | "xcode-csv" => print!("{}", counter_export::format_xcode_csv(&report)),
                "internal-csv" => print!("{}", counter_export::format_csv(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown export-counters format")),
            }
        }
        CommandSet::ValidateCounters(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = xcode_counters::validate(&trace, args.csv, args.tolerance)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", xcode_counters::format_validation(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown validate-counters format",
                    ));
                }
            }
            if report.mismatches > 0 {
                return Err(crate::Error::InvalidInput(format!(
                    "counter validation found {} mismatches",
                    report.mismatches
                )));
            }
        }
        CommandSet::Mtlb(args) => {
            run_mtlb_command(args)?;
        }
        CommandSet::MtlbInventory(args) => {
            let report = mtlb::inventory(args.path)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", mtlb::format_inventory_report(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown mtlb-inventory format")),
            }
        }
        CommandSet::MtlbStats(args) => {
            let report = mtlb::stats(args.path)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", mtlb::format_stats_report(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown mtlb-stats format")),
            }
        }
        CommandSet::MtlbFunctions(args) => {
            let report = mtlb::functions(
                args.path,
                &mtlb::MTLBFunctionsOptions {
                    filter: args.filter,
                    used_only: args.used_only,
                    include_usage: !args.no_usage,
                },
            )?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", mtlb::format_functions_report(&report)),
                "csv" => print!("{}", mtlb::export_functions_csv(&report)),
                "json" => print!("{}", mtlb::export_functions_json(&report)),
                _ => return Err(crate::Error::Unsupported("unknown mtlb-functions format")),
            }
        }
        CommandSet::Profiler(args) => {
            let report = profiler::report(args.path)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", profiler::format_report(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown profiler format")),
            }
        }
        CommandSet::Fences(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = fences::report(&trace)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", fences::format_report(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown fences format")),
            }
        }
        CommandSet::Timeline(args) => {
            let trace = TraceBundle::open(args.trace)?;
            if args.raw {
                let report = timeline::raw_report(&trace)?;
                match args.format.as_str() {
                    "text" | "table" => print!("{}", timeline::format_raw_report(&report)),
                    "json" => print!("{}", timeline::export_raw_json(&report)?),
                    _ => return Err(crate::Error::Unsupported("unknown raw timeline format")),
                }
            } else {
                let report = timeline::report(&trace)?;
                match args.format.as_str() {
                    "text" | "table" => print!("{}", timeline::format_report(&report)),
                    "json" => print!("{}", timeline::export_json(&report)?),
                    "chrome" => print!("{}", timeline::format_chrome_trace_json(&report)?),
                    "perfetto" => print!("{}", timeline::format_perfetto_trace_json(&report)?),
                    _ => return Err(crate::Error::Unsupported("unknown timeline format")),
                }
            }
        }
        CommandSet::Kernels(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = commands::kernels(&trace, args.filter.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", commands::format_kernels(&report, args.verbose)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown kernels format")),
            }
        }
        CommandSet::Encoders(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = commands::encoders(&trace)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", commands::format_encoders(&report, args.verbose)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown encoders format")),
            }
        }
        CommandSet::Dependencies(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = commands::dependencies(&trace)?;
            match args.format.as_str() {
                "dot" => print!("{}", commands::format_dependencies_dot(&report)),
                "text" | "table" => print!("{}", commands::format_dependencies(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown dependencies format")),
            }
        }
        CommandSet::Shaders(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let search_paths = if args.search_paths.is_empty() {
                shaders::default_search_paths()
            } else {
                args.search_paths
            };
            let report = shaders::report(&trace, &search_paths)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", shaders::format_report(&report)),
                "csv" => print!("{}", shaders::format_csv(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown shaders format")),
            }
        }
        CommandSet::ShaderSource(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let search_paths = if args.search_paths.is_empty() {
                shaders::default_search_paths()
            } else {
                args.search_paths
            };
            let report = shaders::source(&trace, &args.shader, &search_paths, args.context)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", shaders::format_source(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown shader-source format")),
            }
        }
        CommandSet::ShaderHotspots(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let search_paths = if args.search_paths.is_empty() {
                shaders::default_search_paths()
            } else {
                args.search_paths
            };
            let report = shaders::hotspot_report(&trace, &args.shader, &search_paths)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", shaders::format_hotspot_report(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown shader-hotspots format")),
            }
        }
        CommandSet::Correlate(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let search_paths = if args.search_paths.is_empty() {
                shaders::default_search_paths()
            } else {
                args.search_paths
            };
            let report = correlate::report(&trace, &search_paths)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", correlate::format_report(&report, args.verbose)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown correlate format")),
            }
        }
        CommandSet::Timing(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = timing::report(&trace)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", timing::format_report(&report)),
                "csv" => print!("{}", timing::format_csv(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown timing format")),
            }
        }
        CommandSet::TimingProfiler(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = timing::report(&trace)?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", timing::format_report(&report));
                if args.verbose {
                    print!("\n=== Detailed Information ===\n");
                    print!("Data Source: {}\n", trace.path.display());
                    print!("Encoders with timing: {}\n", report.encoders.len());
                    print!("Dispatches with timing: {}\n", report.dispatch_count);
                    print!("Timing source: {}\n", report.source);
                }
            }
        }
        CommandSet::CommandBuffers(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = commands::command_buffers(&trace)?;
            match args.format.as_str() {
                "text" | "table" => print!(
                    "{}",
                    commands::format_command_buffers(&report, args.detailed)
                ),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown command-buffers format")),
            }
        }
        CommandSet::BufferAccess(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = commands::buffer_access(&trace)?;
            match args.format.as_str() {
                "text" | "table" => {
                    print!("{}", commands::format_buffer_access(&report, args.verbose))
                }
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown buffer-access format")),
            }
        }
        CommandSet::Tree(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = commands::tree(&trace, &args.group_by)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", commands::format_tree(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown tree format")),
            }
        }
        CommandSet::Graph(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let output = graphing::generate(
                &trace,
                &args.graph_type,
                &args.format,
                args.show_timing,
                args.show_memory,
            )?;
            print!("{output}");
        }
        CommandSet::Buffers(args) => match args.command {
            BuffersCommand::List {
                trace,
                format,
                sort,
                min_size,
            } => {
                let trace = TraceBundle::open(trace)?;
                let options = buffers::BufferListOptions {
                    sort_by: Some(sort),
                    min_size: min_size.as_deref().map(buffers::parse_size).transpose()?,
                };
                let report = buffers::analyze_with_options(&trace, &options)?;
                match format.as_str() {
                    "table" | "text" => print!("{}", buffers::format_table(&report)),
                    "csv" => print!("{}", buffers::format_csv(&report)),
                    "markdown" => print!("{}", buffers::markdown_report(&report)),
                    "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                    _ => {
                        return Err(crate::Error::Unsupported("unknown buffers list format"));
                    }
                }
            }
            BuffersCommand::Inspect {
                trace,
                buffer,
                bytes,
                inspect_format,
                format,
            } => {
                let trace = TraceBundle::open(trace)?;
                let report = buffers::inspect(&trace, &buffer, bytes, &inspect_format)?;
                match format.as_str() {
                    "text" | "table" => print!("{}", buffers::format_inspection(&report)),
                    "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                    _ => {
                        return Err(crate::Error::Unsupported("unknown buffers inspect format"));
                    }
                }
            }
            BuffersCommand::Diff {
                left,
                right,
                format,
            } => {
                let left = TraceBundle::open(left)?;
                let right = TraceBundle::open(right)?;
                let report = buffers::diff(&left, &right)?;
                match format.as_str() {
                    "text" | "table" => print!("{}", buffers::format_diff(&report)),
                    "markdown" => print!("{}", buffers::markdown_diff(&report)),
                    "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                    _ => {
                        return Err(crate::Error::Unsupported("unknown buffers diff format"));
                    }
                }
            }
        },
        CommandSet::BufferTimeline(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = buffer_timeline::analyze(&trace);
            match args.format.as_str() {
                "ascii" => print!("{}", buffer_timeline::format_ascii(&report, args.width)),
                "summary" => print!("{}", buffer_timeline::format_summary(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown buffer timeline format")),
            }
        }
        CommandSet::Insights(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = insights::report(&trace, args.min_level.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", insights::format_report(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown insights format")),
            }
        }
        CommandSet::Diff(args) => {
            validate_diff_args(&args)?;
            let limit = args.limit;
            let min_delta_us = args.min_delta_us.unwrap_or_default();
            let only_encoder = args.only_encoder;
            let only_function = args.only_function.clone();
            let md_out = args.md_out.clone();
            let format_arg = args.format.clone();
            let markdown_flag = args.markdown;
            let json_flag = args.json;
            let csv_flag = args.csv;
            let by = args.by.clone();
            let show_matches = args.show_matches;
            let show_unmatched = args.show_unmatched;
            let show_occurrences = args.show_occurrences;
            let explain = args.explain;
            let quick = args.quick;
            let by_encoder = args.by_encoder;
            let (left, right, discover_note) = resolve_diff_inputs(args)?;
            let report = diff::diff_paths_with_options(
                left,
                right,
                &diff::DiffOptions {
                    profile: diff::ProfileDiffOptions {
                        limit,
                        min_delta_us,
                        only_encoder,
                        only_function,
                    },
                },
            )?;
            if let Some(path) = md_out.as_ref() {
                let mut text = String::new();
                if let Some(note) = &discover_note {
                    text.push_str(&format!("<!-- {note} -->\n\n"));
                }
                text.push_str(&markdown::diff_report_with_limit(&report, limit));
                fs::write(path, text)?;
            }

            let format = format_arg.as_deref().unwrap_or(if csv_flag {
                "csv"
            } else if markdown_flag {
                "markdown"
            } else {
                "text"
            });
            if json_flag {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                match format {
                    "markdown" | "md" => {
                        if let Some(note) = &discover_note {
                            println!("{note}\n");
                        }
                        print!("{}", markdown::diff_report_with_limit(&report, limit));
                    }
                    "text" | "table" => print!(
                        "{}",
                        diff::format_profile_text(
                            &report,
                            &diff::ProfileTextOptions {
                                by: by.as_deref(),
                                show_matches,
                                show_unmatched,
                                show_occurrences,
                                explain,
                                quick,
                                by_encoder,
                                limit,
                            },
                        )?
                    ),
                    "csv" => print!(
                        "{}",
                        diff::format_profile_csv(&report, by.as_deref(), limit)?
                    ),
                    "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                    _ => return Err(crate::Error::Unsupported("unknown diff format")),
                }
            }
        }
        CommandSet::Markdown(args) => match args.command {
            MarkdownCommand::Render { input } => {
                print!("{}", markdown::render(&input));
            }
            MarkdownCommand::Analyze { trace } => {
                let trace = TraceBundle::open(trace)?;
                let report = analysis::analyze(&trace);
                print!("{}", markdown::analysis_report(&report));
            }
            MarkdownCommand::Diff { left, right } => {
                let report = diff::diff_paths(left, right)?;
                print!("{}", markdown::diff_report(&report));
            }
            MarkdownCommand::Buffers { trace } => {
                let trace = TraceBundle::open(trace)?;
                let report = buffers::analyze(&trace)?;
                print!("{}", buffers::markdown_report(&report));
            }
            MarkdownCommand::BuffersDiff { left, right } => {
                let left = TraceBundle::open(left)?;
                let right = TraceBundle::open(right)?;
                let report = buffers::diff(&left, &right)?;
                print!("{}", buffers::markdown_diff(&report));
            }
        },
        CommandSet::Version(args) => {
            let info = serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "package": env!("CARGO_PKG_NAME"),
            });
            if args.json {
                println!("{}", serde_json::to_string_pretty(&info)?);
            } else {
                println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            }
        }
        CommandSet::XcodeWindows(args) => {
            let report = automation::list_windows()?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_windows(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-windows format")),
            }
        }
        CommandSet::XcodeInspect(args) => {
            let report = automation::inspect_window(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_window_snapshot(report.as_ref())),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-inspect format")),
            }
        }
        CommandSet::XcodeButtons(args) => {
            let report = automation::list_buttons(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_buttons(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-buttons format")),
            }
        }
        CommandSet::XcodeCounters(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = xcode_counters::parse(&trace, args.csv)?;
            match args.format.as_str() {
                "summary" | "text" | "table" => print!(
                    "{}",
                    xcode_counters::format_summary(&report, args.metric.as_deref(), args.top)
                ),
                "detailed" => print!(
                    "{}",
                    xcode_counters::format_detailed(&report, args.metric.as_deref(), args.top)
                ),
                "metrics" => print!("{}", xcode_counters::format_metric_inventory(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-counters format")),
            }
        }
        CommandSet::XcodeCheckPermissions(args) => {
            let report = automation::check_accessibility_permissions(!args.no_prompt)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_permissions(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-check-permissions format",
                    ));
                }
            }
            if !report.accessibility_granted {
                return Err(crate::Error::InvalidInput(
                    "accessibility permission required".to_owned(),
                ));
            }
        }
        CommandSet::XcodeCheckboxes(args) => {
            let report = automation::list_checkboxes(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_checkboxes(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-checkboxes format")),
            }
        }
        CommandSet::XcodeClickButton(args) => {
            let report = automation::click_button(args.trace.as_deref(), &[args.target.as_str()])?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-click-button format",
                    ));
                }
            }
        }
        CommandSet::XcodeEnsureChecked(args) => {
            let report = automation::ensure_checked(args.trace.as_deref(), &args.target)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-ensure-checked format",
                    ));
                }
            }
        }
        CommandSet::XcodeClose(args) => {
            let report = automation::close_window(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-close format")),
            }
        }
        CommandSet::XcodeExportCounters(args) => {
            let report = automation::export_counters(args.trace.as_deref(), &args.output)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_export(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-export-counters format",
                    ));
                }
            }
        }
        CommandSet::XcodeExportMemory(args) => {
            let report = automation::export_memory(args.trace.as_deref(), &args.output)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_export(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-export-memory format",
                    ));
                }
            }
        }
        CommandSet::XcodeShowPerformance(args) => {
            let report = automation::show_performance(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-show-performance format",
                    ));
                }
            }
        }
        CommandSet::XcodeShowSummary(args) => {
            let report = automation::show_summary(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-show-summary format",
                    ));
                }
            }
        }
        CommandSet::XcodeShowCounters(args) => {
            let report = automation::show_counters(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-show-counters format",
                    ));
                }
            }
        }
        CommandSet::XcodeShowMemory(args) => {
            let report = automation::show_memory(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-show-memory format",
                    ));
                }
            }
        }
        CommandSet::XcodeShowDependencies(args) => {
            let report = automation::show_dependencies(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-show-dependencies format",
                    ));
                }
            }
        }
        CommandSet::XcodeTabs(args) => {
            let report = automation::list_tabs(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_tabs(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-tabs format")),
            }
        }
        CommandSet::XcodeToggleCheckbox(args) => {
            let report = automation::toggle_checkbox(args.trace.as_deref(), &args.target)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-toggle-checkbox format",
                    ));
                }
            }
        }
        CommandSet::XcodeSelectTab(args) => {
            let report = automation::select_tab(args.trace.as_deref(), &args.target)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_action(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported("unknown xcode-select-tab format"));
                }
            }
        }
        CommandSet::XcodeUiElements(args) => {
            let report = automation::list_ui_elements(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_ui_elements(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-ui-elements format",
                    ));
                }
            }
        }
        CommandSet::XcodeMenuItems(args) => {
            let menu_segments = args
                .menu_path
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let report = automation::list_menu_items(&menu_segments)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_menu_items(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-menu-items format")),
            }
        }
        CommandSet::XcodeStatus(args) => {
            let report = automation::get_window_status(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_status(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-status format")),
            }
        }
        CommandSet::XcodeWait(args) => {
            let accepted = parse_wait_statuses(&args.status)?;
            let report = automation::wait_for_status(
                std::time::Duration::from_secs(args.timeout_seconds.max(1)),
                args.trace.as_deref(),
                &accepted,
            )?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_status(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown xcode-wait format")),
            }
        }
        CommandSet::XcodeProfile(args) => {
            run_xcode_profile_command(args)?;
        }
    }
    Ok(())
}

fn validate_diff_args(args: &DiffArgs) -> Result<()> {
    if args.json && args.csv {
        return Err(crate::Error::InvalidInput(
            "--json and --csv are mutually exclusive".to_owned(),
        ));
    }
    if args.limit == 0 {
        return Err(crate::Error::InvalidInput("--limit must be > 0".to_owned()));
    }
    if args.min_delta_us.is_some_and(|value| value < 0) {
        return Err(crate::Error::InvalidInput(
            "--min-delta-us must be >= 0".to_owned(),
        ));
    }
    validate_diff_by(args.by.as_deref())?;
    if args.left.is_some() != args.right.is_some() {
        return Err(crate::Error::InvalidInput(
            "expected 0 or 2 positional traces, got 1".to_owned(),
        ));
    }
    if (args.left_flag.is_some()) != (args.right_flag.is_some()) {
        return Err(crate::Error::InvalidInput(
            "--left and --right must be provided together".to_owned(),
        ));
    }
    if (args.left.is_some() || args.right.is_some())
        && (args.left_flag.is_some() || args.right_flag.is_some())
    {
        return Err(crate::Error::InvalidInput(
            "positional traces cannot be combined with --left/--right".to_owned(),
        ));
    }
    if (args.left.is_some() || args.right.is_some()) && args.bench_dir.is_some() {
        return Err(crate::Error::InvalidInput(
            "positional traces cannot be combined with --bench-dir".to_owned(),
        ));
    }
    let text_only_flags = args.show_matches
        || args.show_unmatched
        || args.show_occurrences
        || args.explain
        || args.by_encoder;
    if args.json && text_only_flags {
        return Err(crate::Error::InvalidInput(
            "--json cannot be combined with text-only flags (--show-*, --explain, --by-encoder)"
                .to_owned(),
        ));
    }
    if args.csv {
        if args.quick {
            return Err(crate::Error::InvalidInput(
                "--quick cannot be combined with --csv".to_owned(),
            ));
        }
        if text_only_flags {
            return Err(crate::Error::InvalidInput(
                "--csv cannot be combined with text-only flags (--show-*, --explain, --by-encoder)"
                    .to_owned(),
            ));
        }
        if args
            .by
            .as_deref()
            .is_some_and(|value| value.trim().contains(','))
        {
            return Err(crate::Error::InvalidInput(
                "--csv requires a single --by view".to_owned(),
            ));
        }
    }
    if args.quick {
        if args.json {
            return Err(crate::Error::InvalidInput(
                "--quick cannot be combined with --json".to_owned(),
            ));
        }
        if args
            .by
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        {
            return Err(crate::Error::InvalidInput(
                "--quick cannot be combined with --by".to_owned(),
            ));
        }
        if args.show_matches || args.show_unmatched || args.show_occurrences || args.explain {
            return Err(crate::Error::InvalidInput(
                "--quick cannot be combined with --show-matches/--show-unmatched/--show-occurrences/--explain"
                    .to_owned(),
            ));
        }
    }
    if args.by_encoder
        && args
            .by
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(crate::Error::InvalidInput(
            "--by-encoder cannot be combined with --by".to_owned(),
        ));
    }
    Ok(())
}

fn validate_diff_by(by: Option<&str>) -> Result<()> {
    let Some(by) = by.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let valid = matches!(
        by,
        "function"
            | "encoder"
            | "pipeline"
            | "timeline-windows"
            | "dispatch"
            | "unmatched"
            | "matches"
            | "occurrences"
    );
    if valid {
        Ok(())
    } else {
        Err(crate::Error::InvalidInput(format!(
            "invalid --by value {by:?}; expected one of function, encoder, pipeline, timeline-windows, dispatch, unmatched, matches, occurrences"
        )))
    }
}

fn run_xcode_profile_command(args: XcodeProfileArgs) -> Result<()> {
    match args.command {
        Some(XcodeProfileCommand::Open(args)) => {
            automation::open_trace_in_xcode_with_options(
                &args.trace,
                OpenTraceOptions {
                    launch_mode: if args.foreground {
                        XcodeLaunchMode::Foreground
                    } else {
                        XcodeLaunchMode::Background
                    },
                    wait_for_window: true,
                    timeout: std::time::Duration::from_secs(args.timeout_seconds.max(1)),
                },
            )?;
            Ok(())
        }
        Some(XcodeProfileCommand::Close(args)) => {
            let report = automation::close_window(args.trace.as_deref())?;
            print_xcode_action_report(&report, &args.format, "xcode-profile close")
        }
        Some(XcodeProfileCommand::CheckStatus(args)) => {
            let report = automation::get_window_status(args.trace.as_deref())?;
            print_xcode_status_report(&report, &args.format, "xcode-profile check-status")
        }
        Some(XcodeProfileCommand::CheckPermissions(args)) => {
            let report = automation::check_accessibility_permissions(!args.no_prompt)?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_permissions(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-profile check-permissions format",
                    ));
                }
            }
            if !report.accessibility_granted {
                return Err(crate::Error::InvalidInput(
                    "accessibility permission required".to_owned(),
                ));
            }
            Ok(())
        }
        Some(XcodeProfileCommand::Run(args)) => run_xcode_profile_full(
            args.trace,
            args.output,
            args.timeout_seconds,
            args.wait_seconds,
            args.force,
            args.no_prompt,
            args.activate,
        ),
        Some(XcodeProfileCommand::RunProfile(args)) => {
            let report = automation::click_button(args.trace.as_deref(), &["Profile", "Replay"])?;
            print_xcode_action_report(&report, &args.format, "xcode-profile run-profile")
        }
        Some(XcodeProfileCommand::WaitProfile(args)) => {
            let report = automation::wait_for_status(
                std::time::Duration::from_secs(args.timeout_seconds.max(1)),
                args.trace.as_deref(),
                &[automation::XcodeAutomationStatus::Complete],
            )?;
            print_xcode_status_report(&report, &args.format, "xcode-profile wait-profile")
        }
        Some(XcodeProfileCommand::Export(args)) => {
            let report = automation::export_profile(args.trace.as_deref(), &args.output)?;
            print_xcode_export_report(&report, &args.format, "xcode-profile export")
        }
        Some(XcodeProfileCommand::XcodeExportCounters(args)) => {
            let report = automation::export_counters(args.trace.as_deref(), &args.output)?;
            print_xcode_export_report(&report, &args.format, "xcode-profile xcode-export-counters")
        }
        Some(XcodeProfileCommand::XcodeExportMemory(args)) => {
            let report = automation::export_memory(args.trace.as_deref(), &args.output)?;
            print_xcode_export_report(&report, &args.format, "xcode-profile xcode-export-memory")
        }
        Some(XcodeProfileCommand::ListWindows(args)) => {
            let report = automation::list_windows()?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_windows(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-profile list-windows format",
                    ));
                }
            }
            Ok(())
        }
        Some(XcodeProfileCommand::ListButtons(args)) => {
            let report = automation::list_buttons(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_buttons(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-profile list-buttons format",
                    ));
                }
            }
            Ok(())
        }
        Some(XcodeProfileCommand::ClickButton(args)) => {
            let report = automation::click_button(args.trace.as_deref(), &[args.target.as_str()])?;
            print_xcode_action_report(&report, &args.format, "xcode-profile click-button")
        }
        Some(XcodeProfileCommand::ListTabs(args)) => {
            let report = automation::list_tabs(args.trace.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", format_xcode_tabs(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => {
                    return Err(crate::Error::Unsupported(
                        "unknown xcode-profile list-tabs format",
                    ));
                }
            }
            Ok(())
        }
        Some(XcodeProfileCommand::SelectTab(args)) => {
            let report = automation::select_tab(args.trace.as_deref(), &args.target)?;
            print_xcode_action_report(&report, &args.format, "xcode-profile select-tab")
        }
        Some(XcodeProfileCommand::EnsureChecked(args)) => {
            let report = automation::ensure_checked(args.trace.as_deref(), &args.target)?;
            print_xcode_action_report(&report, &args.format, "xcode-profile ensure-checked")
        }
        Some(XcodeProfileCommand::ToggleCheckbox(args)) => {
            let report = automation::toggle_checkbox(args.trace.as_deref(), &args.target)?;
            print_xcode_action_report(&report, &args.format, "xcode-profile toggle-checkbox")
        }
        Some(XcodeProfileCommand::ShowPerformance(args)) => {
            let report = automation::show_performance(args.trace.as_deref())?;
            print_xcode_action_report(&report, &args.format, "xcode-profile show-performance")
        }
        Some(XcodeProfileCommand::ShowSummary(args)) => {
            let report = automation::show_summary(args.trace.as_deref())?;
            print_xcode_action_report(&report, &args.format, "xcode-profile show-summary")
        }
        Some(XcodeProfileCommand::ShowCounters(args)) => {
            let report = automation::show_counters(args.trace.as_deref())?;
            print_xcode_action_report(&report, &args.format, "xcode-profile show-counters")
        }
        Some(XcodeProfileCommand::ShowMemory(args)) => {
            let report = automation::show_memory(args.trace.as_deref())?;
            print_xcode_action_report(&report, &args.format, "xcode-profile show-memory")
        }
        Some(XcodeProfileCommand::ShowDependencies(args)) => {
            let report = automation::show_dependencies(args.trace.as_deref())?;
            print_xcode_action_report(&report, &args.format, "xcode-profile show-dependencies")
        }
        None => {
            let trace = args.trace.ok_or_else(|| {
                crate::Error::InvalidInput(
                    "xcode-profile requires a trace or subcommand".to_owned(),
                )
            })?;
            if args.activate {
                automation::activate_xcode()?;
            }
            if args.open_only {
                automation::open_trace_in_xcode(&trace)?;
                Ok(())
            } else {
                run_xcode_profile_full(
                    trace,
                    args.output,
                    args.timeout_seconds,
                    args.wait_seconds,
                    args.force,
                    args.no_prompt,
                    false,
                )
            }
        }
    }
}

fn resolve_diff_inputs(args: DiffArgs) -> Result<(PathBuf, PathBuf, Option<String>)> {
    if args.left.is_some() && args.left_flag.is_some() {
        return Err(crate::Error::InvalidInput(
            "positional traces cannot be combined with --left/--right".to_owned(),
        ));
    }
    if args.right.is_some() && args.right_flag.is_some() {
        return Err(crate::Error::InvalidInput(
            "positional traces cannot be combined with --left/--right".to_owned(),
        ));
    }
    let explicit_left = args.left_flag.or(args.left);
    let explicit_right = args.right_flag.or(args.right);
    if explicit_left.is_some() != explicit_right.is_some() {
        return Err(crate::Error::InvalidInput(
            "diff requires both left and right trace paths".to_owned(),
        ));
    }
    if let (Some(left), Some(right)) = (explicit_left, explicit_right) {
        let note = args
            .bench_dir
            .as_ref()
            .map(|_| "--bench-dir ignored because --left/--right were provided".to_owned());
        return Ok((left, right, note));
    }
    let bench_dir = args.bench_dir.ok_or_else(|| {
        crate::Error::InvalidInput(
            "diff requires <left> <right>, --left/--right, or --bench-dir".to_owned(),
        )
    })?;
    let pair = discover_bench_pair(&bench_dir)?;
    let note = format!(
        "auto-pair: stem={} left={} right={}",
        pair.stem,
        pair.left
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("-"),
        pair.right
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("-")
    );
    let note = if pair.left_raw.is_some()
        || pair.right_raw.is_some()
        || pair.left_csv.is_some()
        || pair.right_csv.is_some()
    {
        format!(
            "{note} siblings(left_raw={} right_raw={} left_csv={} right_csv={})",
            option_file_name(pair.left_raw.as_deref()),
            option_file_name(pair.right_raw.as_deref()),
            option_file_name(pair.left_csv.as_deref()),
            option_file_name(pair.right_csv.as_deref())
        )
    } else {
        note
    };
    Ok((pair.left, pair.right, Some(note)))
}

fn option_file_name(path: Option<&Path>) -> String {
    path.and_then(|path| path.file_name())
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("-")
        .to_owned()
}

#[derive(Debug, Clone)]
struct BenchPair {
    stem: String,
    left: PathBuf,
    right: PathBuf,
    left_raw: Option<PathBuf>,
    right_raw: Option<PathBuf>,
    left_csv: Option<PathBuf>,
    right_csv: Option<PathBuf>,
    left_mtime: SystemTime,
    right_mtime: SystemTime,
}

#[derive(Debug, Clone)]
struct BenchCandidate {
    path: PathBuf,
    mtime: SystemTime,
}

#[derive(Default)]
struct BenchGroup {
    go_perf: Vec<BenchCandidate>,
    py_perf: Vec<BenchCandidate>,
    go_raw: Vec<BenchCandidate>,
    py_raw: Vec<BenchCandidate>,
}

fn discover_bench_pair(dir: &Path) -> Result<BenchPair> {
    let mut groups = std::collections::BTreeMap::<String, BenchGroup>::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some((stem, side, kind)) = classify_bench_trace_name(&name) else {
            continue;
        };
        let candidate = BenchCandidate {
            path: entry.path(),
            mtime: entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH),
        };
        let group = groups.entry(stem).or_default();
        match (side, kind) {
            ("go", "perf") => group.go_perf.push(candidate),
            ("py", "perf") => group.py_perf.push(candidate),
            ("go", "raw") => group.go_raw.push(candidate),
            ("py", "raw") => group.py_raw.push(candidate),
            _ => {}
        }
    }

    let mut pairs = groups
        .into_iter()
        .filter_map(|(stem, group)| select_bench_pair(dir, stem, group))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| {
        let left_time = left.left_mtime.max(left.right_mtime);
        let right_time = right.left_mtime.max(right.right_mtime);
        right_time
            .cmp(&left_time)
            .then_with(|| left.stem.cmp(&right.stem))
    });
    pairs.into_iter().next().ok_or_else(|| {
        crate::Error::InvalidInput(format!(
            "no Go/Python trace pair found in {}",
            dir.display()
        ))
    })
}

fn classify_bench_trace_name(name: &str) -> Option<(String, &'static str, &'static str)> {
    if !name.ends_with(".gputrace") {
        return None;
    }
    let perf = name.ends_with("-perfdata.gputrace");
    if let Some(stem) = classify_bench_side(name, "_Go") {
        return Some((stem, "go", if perf { "perf" } else { "raw" }));
    }
    if let Some(stem) = classify_bench_side(name, "_Python") {
        return Some((stem, "py", if perf { "perf" } else { "raw" }));
    }
    None
}

fn classify_bench_side(name: &str, marker: &str) -> Option<String> {
    let marker_index = name.rfind(marker)?;
    let after_marker = &name[marker_index + marker.len()..];
    if after_marker.contains('_') {
        Some(name[..marker_index].to_owned())
    } else {
        None
    }
}

fn select_bench_pair(dir: &Path, stem: String, group: BenchGroup) -> Option<BenchPair> {
    let go_perf = newest_bench_candidate(&group.go_perf);
    let py_perf = newest_bench_candidate(&group.py_perf);
    let go_raw = newest_bench_candidate(&group.go_raw);
    let py_raw = newest_bench_candidate(&group.py_raw);

    let mut pair = BenchPair {
        stem,
        left: PathBuf::new(),
        right: PathBuf::new(),
        left_raw: go_raw.map(|candidate| candidate.path.clone()),
        right_raw: py_raw.map(|candidate| candidate.path.clone()),
        left_csv: None,
        right_csv: None,
        left_mtime: SystemTime::UNIX_EPOCH,
        right_mtime: SystemTime::UNIX_EPOCH,
    };

    if let (Some(left), Some(right)) = (go_perf, py_perf) {
        pair.left = left.path.clone();
        pair.right = right.path.clone();
        pair.left_mtime = left.mtime;
        pair.right_mtime = right.mtime;
    } else if let (Some(left), Some(right)) = (go_raw, py_raw) {
        pair.left = left.path.clone();
        pair.right = right.path.clone();
        pair.left_mtime = left.mtime;
        pair.right_mtime = right.mtime;
    } else {
        return None;
    }
    pair.left_csv = find_sibling_counter_csv(dir, &pair.left);
    pair.right_csv = find_sibling_counter_csv(dir, &pair.right);
    Some(pair)
}

fn find_sibling_counter_csv(dir: &Path, trace_path: &Path) -> Option<PathBuf> {
    let base = trace_path.file_name()?.to_str()?;
    let stem = base
        .strip_suffix("-perfdata.gputrace")
        .or_else(|| base.strip_suffix(".gputrace"))?;
    let csv = dir.join(format!("{stem}_counters.csv"));
    csv.exists().then_some(csv)
}

fn newest_bench_candidate(candidates: &[BenchCandidate]) -> Option<&BenchCandidate> {
    candidates.iter().max_by_key(|candidate| candidate.mtime)
}

fn run_xcode_profile_full(
    trace: PathBuf,
    output: Option<PathBuf>,
    timeout_seconds: u64,
    wait_seconds: u64,
    force: bool,
    no_prompt: bool,
    activate: bool,
) -> Result<()> {
    if activate {
        automation::activate_xcode()?;
    }
    let report = automation::run_profile(&XcodeProfileRun {
        trace_path: trace,
        output_path: output,
        timeout_seconds,
        wait_for_running_profile_seconds: wait_seconds,
        force,
        prompt_for_permissions: !no_prompt,
    })?;
    print!("{}", format_xcode_export(&report));
    Ok(())
}

fn print_xcode_action_report(
    report: &automation::XcodeActionResult,
    format: &str,
    command_name: &'static str,
) -> Result<()> {
    match format {
        "text" | "table" => print!("{}", format_xcode_action(report)),
        "json" => println!("{}", serde_json::to_string_pretty(report)?),
        _ => {
            return Err(crate::Error::Unsupported(match command_name {
                "xcode-profile run-profile" => "unknown xcode-profile run-profile format",
                "xcode-profile click-button" => "unknown xcode-profile click-button format",
                "xcode-profile select-tab" => "unknown xcode-profile select-tab format",
                "xcode-profile ensure-checked" => "unknown xcode-profile ensure-checked format",
                "xcode-profile toggle-checkbox" => "unknown xcode-profile toggle-checkbox format",
                "xcode-profile show-performance" => "unknown xcode-profile show-performance format",
                "xcode-profile show-summary" => "unknown xcode-profile show-summary format",
                "xcode-profile show-counters" => "unknown xcode-profile show-counters format",
                "xcode-profile show-memory" => "unknown xcode-profile show-memory format",
                "xcode-profile show-dependencies" => {
                    "unknown xcode-profile show-dependencies format"
                }
                _ => "unknown xcode-profile action format",
            }));
        }
    }
    Ok(())
}

fn print_xcode_export_report(
    report: &automation::XcodeExportResult,
    format: &str,
    command_name: &'static str,
) -> Result<()> {
    match format {
        "text" | "table" => print!("{}", format_xcode_export(report)),
        "json" => println!("{}", serde_json::to_string_pretty(report)?),
        _ => {
            return Err(crate::Error::Unsupported(match command_name {
                "xcode-profile export" => "unknown xcode-profile export format",
                "xcode-profile xcode-export-counters" => {
                    "unknown xcode-profile xcode-export-counters format"
                }
                "xcode-profile xcode-export-memory" => {
                    "unknown xcode-profile xcode-export-memory format"
                }
                _ => "unknown xcode-profile export format",
            }));
        }
    }
    Ok(())
}

fn print_xcode_status_report(
    report: &automation::XcodeWindowStatus,
    format: &str,
    command_name: &'static str,
) -> Result<()> {
    match format {
        "text" | "table" => print!("{}", format_xcode_status(report)),
        "json" => println!("{}", serde_json::to_string_pretty(report)?),
        _ => {
            return Err(crate::Error::Unsupported(match command_name {
                "xcode-profile check-status" => "unknown xcode-profile check-status format",
                "xcode-profile wait-profile" => "unknown xcode-profile wait-profile format",
                _ => "unknown xcode-profile status format",
            }));
        }
    }
    Ok(())
}

fn run_mtlb_command(args: MtlbArgs) -> Result<()> {
    match args.command {
        Some(MtlbCommand::List(args)) | Some(MtlbCommand::Inventory(args)) => {
            print_mtlb_inventory(args.path, &args.format, "mtlb inventory")
        }
        Some(MtlbCommand::Info(args)) => print_mtlb_info(args.path, &args.format),
        Some(MtlbCommand::Stats(args)) => print_mtlb_stats(args.path, &args.format, "mtlb stats"),
        Some(MtlbCommand::Functions(args)) => print_mtlb_functions(
            args.path,
            args.filter,
            args.used_only,
            args.no_usage,
            &args.format,
            "mtlb functions",
        ),
        Some(MtlbCommand::ExportFunctions(args)) => print_mtlb_functions(
            args.path,
            args.filter,
            args.used_only,
            args.no_usage,
            &args.format,
            "mtlb export-functions",
        ),
        Some(MtlbCommand::Extract(args)) => {
            let report = mtlb::extract(
                args.path,
                &mtlb::MTLBExtractOptions {
                    output: args.output,
                    library: args.library,
                    all: args.all,
                    output_dir: args.output_dir,
                },
            )?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", mtlb::format_extract_report(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown mtlb extract format")),
            }
            Ok(())
        }
        None => {
            let path = args.path.ok_or_else(|| {
                crate::Error::InvalidInput("mtlb requires a path or subcommand".to_owned())
            })?;
            print_mtlb_info(path, &args.format)
        }
    }
}

fn print_mtlb_info(path: PathBuf, format: &str) -> Result<()> {
    let metadata = std::fs::metadata(&path).map_err(crate::Error::Io)?;
    if metadata.is_dir() {
        let report = mtlb::scan_bundle(path)?;
        match format {
            "text" | "table" => print!("{}", mtlb::format_bundle_report(&report)),
            "json" => println!("{}", serde_json::to_string_pretty(&report)?),
            _ => return Err(crate::Error::Unsupported("unknown mtlb info format")),
        }
    } else {
        let report = mtlb::inspect_file(path)?;
        match format {
            "text" | "table" => print!("{}", mtlb::format_file_report(&report)),
            "json" => println!("{}", serde_json::to_string_pretty(&report)?),
            _ => return Err(crate::Error::Unsupported("unknown mtlb info format")),
        }
    }
    Ok(())
}

fn print_mtlb_inventory(path: PathBuf, format: &str, command_name: &'static str) -> Result<()> {
    let report = mtlb::inventory(path)?;
    match format {
        "text" | "table" => print!("{}", mtlb::format_inventory_report(&report)),
        "json" => println!("{}", serde_json::to_string_pretty(&report)?),
        _ => {
            return Err(crate::Error::Unsupported(match command_name {
                "mtlb inventory" => "unknown mtlb inventory format",
                _ => "unknown mtlb list format",
            }));
        }
    }
    Ok(())
}

fn print_mtlb_stats(path: PathBuf, format: &str, command_name: &'static str) -> Result<()> {
    let report = mtlb::stats(path)?;
    match format {
        "text" | "table" => print!("{}", mtlb::format_stats_report(&report)),
        "json" => println!("{}", serde_json::to_string_pretty(&report)?),
        _ => {
            return Err(crate::Error::Unsupported(match command_name {
                "mtlb stats" => "unknown mtlb stats format",
                _ => "unknown mtlb-stats format",
            }));
        }
    }
    Ok(())
}

fn print_mtlb_functions(
    path: PathBuf,
    filter: Option<String>,
    used_only: bool,
    no_usage: bool,
    format: &str,
    command_name: &'static str,
) -> Result<()> {
    let report = mtlb::functions(
        path,
        &mtlb::MTLBFunctionsOptions {
            filter,
            used_only,
            include_usage: !no_usage,
        },
    )?;
    match format {
        "text" | "table" => print!("{}", mtlb::format_functions_report(&report)),
        "csv" => print!("{}", mtlb::export_functions_csv(&report)),
        "json" => print!("{}", mtlb::export_functions_json(&report)),
        _ => {
            return Err(crate::Error::Unsupported(match command_name {
                "mtlb export-functions" => "unknown mtlb export-functions format",
                _ => "unknown mtlb functions format",
            }));
        }
    }
    Ok(())
}

fn confirm_clear_buffers(report: &clear_buffers::ClearBuffersReport) -> Result<bool> {
    print!(
        "{}\n\nZero out all buffer files? [y/N]: ",
        clear_buffers::format_report(report)
    );
    io::stdout().flush()?;

    let mut response = String::new();
    io::stdin().read_line(&mut response)?;
    let response = response.trim().to_ascii_lowercase();
    Ok(matches!(response.as_str(), "y" | "yes"))
}

fn format_xcode_windows(windows: &[automation::XcodeWindowInfo]) -> String {
    if windows.is_empty() {
        return "No Xcode windows found\n".to_owned();
    }

    let mut out = String::new();
    for window in windows {
        out.push_str(&format!(
            "{} [{}{}{}]\n",
            window.title,
            window.role,
            if window.focused { ", focused" } else { "" },
            if window.main { ", main" } else { "" }
        ));
        if let Some(document) = &window.document {
            out.push_str(&format!("  document: {document}\n"));
        }
        if let Some(subrole) = &window.subrole {
            out.push_str(&format!("  subrole: {subrole}\n"));
        }
        if window.modal {
            out.push_str("  modal: yes\n");
        }
    }
    out
}

fn format_xcode_window_snapshot(snapshot: Option<&automation::XcodeWindowSnapshot>) -> String {
    match snapshot {
        None => "No matching Xcode window found\n".to_owned(),
        Some(snapshot) => {
            let mut out = String::new();
            out.push_str(&format!("{}\n", snapshot.window.title));
            out.push_str(&format!("  status: {:?}\n", snapshot.status));
            out.push_str(&format!(
                "  buttons: {}  tabs: {}  toolbars: {}\n",
                snapshot.button_count, snapshot.tab_count, snapshot.toolbar_count
            ));
            if let Some(document) = &snapshot.window.document {
                out.push_str(&format!("  document: {document}\n"));
            }
            out
        }
    }
}

fn format_xcode_buttons(buttons: &[automation::XcodeButtonInfo]) -> String {
    if buttons.is_empty() {
        return "No buttons found\n".to_owned();
    }

    let mut out = String::new();
    for button in buttons {
        out.push_str(&format!(
            "{} [{}]{}\n",
            button.name,
            if button.enabled {
                "enabled"
            } else {
                "disabled"
            },
            button
                .description
                .as_ref()
                .map(|description| format!(" - {description}"))
                .unwrap_or_default()
        ));
    }
    out
}

fn format_xcode_checkboxes(checkboxes: &[automation::XcodeCheckboxInfo]) -> String {
    if checkboxes.is_empty() {
        return "No checkboxes found\n".to_owned();
    }

    let mut out = String::new();
    for checkbox in checkboxes {
        out.push_str(&format!(
            "{} [{}; {}]{}\n",
            checkbox.name,
            if checkbox.checked {
                "checked"
            } else {
                "unchecked"
            },
            if checkbox.enabled {
                "enabled"
            } else {
                "disabled"
            },
            checkbox
                .description
                .as_ref()
                .map(|description| format!(" - {description}"))
                .unwrap_or_default()
        ));
    }
    out
}

fn format_xcode_action(result: &automation::XcodeActionResult) -> String {
    format!(
        "{}\n  action: {}\n  target: {}\n",
        result.window_title, result.action, result.target
    )
}

fn format_xcode_export(result: &automation::XcodeExportResult) -> String {
    format!(
        "{}\n  export: {}\n  output: {}\n",
        result.window_title,
        result.export_kind,
        result.output_path.display()
    )
}

fn format_xcode_permissions(result: &automation::XcodePermissionReport) -> String {
    format!(
        "Accessibility granted: {}\nXcode running: {}\nAX probe OK: {}\nPrompt opened: {}\n",
        if result.accessibility_granted {
            "yes"
        } else {
            "no"
        },
        if result.xcode_running { "yes" } else { "no" },
        if result.xcode_probe_ok { "yes" } else { "no" },
        if result.prompt_opened { "yes" } else { "no" },
    )
}

fn format_xcode_status(status: &automation::XcodeWindowStatus) -> String {
    let mut out = format!("status: {:?}\nraw: {}\n", status.status, status.raw.trim());
    if let Some(tab) = &status.current_tab {
        out.push_str(&format!("tab: {tab}\n"));
    }
    if !status.available_actions.is_empty() {
        out.push_str(&format!(
            "actions: {}\n",
            status.available_actions.join(", ")
        ));
    }
    out
}

fn format_xcode_tabs(tabs: &[automation::XcodeTabInfo]) -> String {
    if tabs.is_empty() {
        return "No tabs found\n".to_owned();
    }

    let mut out = String::new();
    for tab in tabs {
        out.push_str(&format!(
            "{} [{}{}]\n",
            tab.name,
            if tab.enabled { "enabled" } else { "disabled" },
            if tab.selected { ", selected" } else { "" }
        ));
        out.push_str(&format!("  role: {}", tab.role));
        if let Some(subrole) = &tab.subrole {
            out.push_str(&format!(" ({subrole})"));
        }
        out.push('\n');
    }
    out
}

fn parse_wait_statuses(raw: &str) -> Result<Vec<automation::XcodeAutomationStatus>> {
    let mut statuses = Vec::new();
    for segment in raw
        .split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
    {
        let status = match segment {
            "not-running" => automation::XcodeAutomationStatus::NotRunning,
            "initializing" => automation::XcodeAutomationStatus::Initializing,
            "replay-ready" => automation::XcodeAutomationStatus::ReplayReady,
            "running" => automation::XcodeAutomationStatus::Running,
            "complete" => automation::XcodeAutomationStatus::Complete,
            "unknown" => automation::XcodeAutomationStatus::Unknown,
            other => {
                return Err(crate::Error::InvalidInput(format!(
                    "unknown Xcode status: {other}"
                )));
            }
        };
        statuses.push(status);
    }
    if statuses.is_empty() {
        return Err(crate::Error::InvalidInput(
            "at least one Xcode status is required".to_owned(),
        ));
    }
    Ok(statuses)
}

fn format_xcode_menu_items(items: &[automation::XcodeMenuItemInfo]) -> String {
    if items.is_empty() {
        return "No menu items found\n".to_owned();
    }

    let mut out = String::new();
    for item in items {
        out.push_str(&format!(
            "{} [{}{}]\n",
            item.menu_path.join(" > "),
            if item.enabled { "enabled" } else { "disabled" },
            if item.has_submenu { ", submenu" } else { "" }
        ));
        out.push_str(&format!("  title: {}\n", item.title));
    }
    out
}

fn format_xcode_ui_elements(elements: &[automation::XcodeUiElementInfo]) -> String {
    if elements.is_empty() {
        return "No UI elements found\n".to_owned();
    }

    let mut out = String::new();
    for element in elements {
        out.push_str(&format!(
            "{} [{}]\n",
            element.path.join(" > "),
            element.role
        ));
        if let Some(title) = &element.title {
            out.push_str(&format!("  title: {title}\n"));
        }
        if let Some(description) = &element.description {
            out.push_str(&format!("  description: {description}\n"));
        }
        if let Some(identifier) = &element.identifier {
            out.push_str(&format!("  identifier: {identifier}\n"));
        }
        if let Some(enabled) = element.enabled {
            out.push_str(&format!(
                "  enabled: {}\n",
                if enabled { "yes" } else { "no" }
            ));
        }
    }
    out
}

fn parse_record_type(value: &str) -> Result<RecordType> {
    match value {
        "C" => Ok(RecordType::C),
        "C@3ul@3ul" | "C3ul" => Ok(RecordType::C3ul),
        "CS" => Ok(RecordType::CS),
        "CSuwuw" => Ok(RecordType::CSuwuw),
        "Ct" => Ok(RecordType::Ct),
        "Ctt" => Ok(RecordType::Ctt),
        "CtU" => Ok(RecordType::CtU),
        "Ctulul" => Ok(RecordType::Ctulul),
        "CU" => Ok(RecordType::CU),
        "Cui" => Ok(RecordType::Cui),
        "Cul" => Ok(RecordType::Cul),
        "Culul" => Ok(RecordType::Culul),
        "Cut" => Ok(RecordType::Cut),
        "Cuw" => Ok(RecordType::Cuw),
        "Ci" => Ok(RecordType::Ci),
        "CiulSl" => Ok(RecordType::CiulSl),
        "Ciulul" => Ok(RecordType::Ciulul),
        "Unknown" | "unknown" => Ok(RecordType::Unknown),
        _ => Err(crate::Error::InvalidInput(format!(
            "unknown record type: {value}"
        ))),
    }
}
