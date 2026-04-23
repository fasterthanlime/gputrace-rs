use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::analysis;
use crate::automation::{self, XcodeProfileRun};
use crate::buffer_timeline;
use crate::buffers;
use crate::diff;
use crate::error::Result;
use crate::markdown;
use crate::trace::TraceBundle;

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
    Buffers(BuffersArgs),
    BufferTimeline(BufferTimelineArgs),
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
        CommandSet::Buffers(args) => match args.command {
            BuffersCommand::List { trace, format } => {
                let trace = TraceBundle::open(trace)?;
                let report = buffers::analyze(&trace)?;
                match format.as_str() {
                    "table" | "text" => print!("{}", buffers::format_table(&report)),
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
