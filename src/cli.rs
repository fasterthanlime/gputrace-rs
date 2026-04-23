use std::path::PathBuf;
use std::{io, io::Write};

use clap::{Args, Parser, Subcommand};

use crate::analysis;
use crate::analyze_usage;
use crate::apicalls;
use crate::automation::{self, XcodeProfileRun};
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

#[derive(Debug, Parser)]
#[command(name = "gputrace")]
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
    DumpRecords(DumpRecordsArgs),
    ExportCounters(ExportCountersArgs),
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
    left: PathBuf,
    right: PathBuf,
    #[arg(long)]
    markdown: bool,
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
struct MtlbArgs {
    path: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
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
    open_only: bool,
    #[arg(long)]
    activate: bool,
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
                "csv" => print!("{}", counter_export::format_csv(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown export-counters format")),
            }
        }
        CommandSet::Mtlb(args) => {
            let metadata = std::fs::metadata(&args.path)?;
            match (metadata.is_dir(), args.format.as_str()) {
                (true, "text" | "table") => {
                    let report = mtlb::scan_bundle(args.path)?;
                    print!("{}", mtlb::format_bundle_report(&report));
                }
                (true, "json") => {
                    let report = mtlb::scan_bundle(args.path)?;
                    println!("{}", serde_json::to_string_pretty(&report)?);
                }
                (false, "text" | "table") => {
                    let report = mtlb::inspect_file(args.path)?;
                    print!("{}", mtlb::format_file_report(&report));
                }
                (false, "json") => {
                    let report = mtlb::inspect_file(args.path)?;
                    println!("{}", serde_json::to_string_pretty(&report)?);
                }
                _ => return Err(crate::Error::Unsupported("unknown mtlb format")),
            }
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
            let report = diff::diff_paths(args.left, args.right)?;
            if args.markdown {
                print!("{}", markdown::diff_report(&report));
            } else {
                println!("{}", serde_json::to_string_pretty(&report)?);
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
            if args.activate {
                automation::activate_xcode()?;
            }
            if args.open_only {
                automation::open_trace_in_xcode(&args.trace)?;
            } else {
                let report = automation::run_profile(&XcodeProfileRun {
                    trace_path: args.trace,
                    output_path: args.output,
                    timeout_seconds: args.timeout_seconds,
                    wait_for_running_profile_seconds: args.wait_seconds,
                    force: args.force,
                    prompt_for_permissions: !args.no_prompt,
                })?;
                print!("{}", format_xcode_export(&report));
            }
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
