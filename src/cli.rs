use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::analysis;
use crate::apicalls;
use crate::automation::{self, XcodeProfileRun};
use crate::buffer_timeline;
use crate::buffers;
use crate::commands;
use crate::correlate;
use crate::diff;
use crate::dump;
use crate::error::Result;
use crate::graphing;
use crate::insights;
use crate::markdown;
use crate::mtlb;
use crate::shaders;
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
    ApiCalls(ApiCallsArgs),
    DumpRecords(DumpRecordsArgs),
    Mtlb(MtlbArgs),
    Kernels(KernelsArgs),
    Encoders(EncodersArgs),
    Dependencies(DependenciesArgs),
    Shaders(ShadersArgs),
    ShaderSource(ShaderSourceArgs),
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
    XcodeProfile(XcodeProfileArgs),
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
struct MtlbArgs {
    path: PathBuf,
    #[arg(short, long, default_value = "text")]
    format: String,
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
        CommandSet::ApiCalls(args) => {
            let trace = TraceBundle::open(args.trace)?;
            let report = apicalls::report(&trace, args.kernel.as_deref())?;
            match args.format.as_str() {
                "text" | "table" => print!("{}", apicalls::format_report(&report)),
                "json" => println!("{}", serde_json::to_string_pretty(&report)?),
                _ => return Err(crate::Error::Unsupported("unknown api-calls format")),
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
        CommandSet::XcodeProfile(args) => {
            if args.activate {
                automation::activate_xcode()?;
            }
            if args.open_only {
                automation::open_trace_in_xcode(&args.trace)?;
            } else {
                automation::run_profile(&XcodeProfileRun {
                    trace_path: args.trace,
                    output_path: args.output,
                    timeout_seconds: args.timeout_seconds,
                })?;
            }
        }
    }
    Ok(())
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
