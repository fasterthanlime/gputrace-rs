use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Value, json};

use crate::error::Result;
use crate::profiler;
use crate::trace::{BoundBuffer, CommandBufferRegion, TraceBundle};

#[derive(Debug, Clone, Serialize)]
pub struct TimelineReport {
    pub synthetic: bool,
    pub command_buffers_profiler_backed: bool,
    pub start_time_ns: u64,
    pub end_time_ns: u64,
    pub duration_ns: u64,
    pub command_buffer_count: usize,
    pub encoder_count: usize,
    pub dispatch_count: usize,
    pub command_buffers: Vec<TimelineCommandBuffer>,
    pub encoders: Vec<TimelineEncoder>,
    pub dispatches: Vec<TimelineDispatch>,
    pub events: Vec<TimelineEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineCommandBuffer {
    pub index: usize,
    pub timestamp_ns: u64,
    pub duration_ns: Option<u64>,
    pub encoder_count: usize,
    pub dispatch_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineEncoder {
    pub index: usize,
    pub command_buffer_index: usize,
    pub label: String,
    pub address: u64,
    pub dispatch_count: usize,
    pub start_time_ns: u64,
    pub end_time_ns: u64,
    pub duration_ns: Option<u64>,
    pub synthetic: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineDispatch {
    pub index: usize,
    pub command_buffer_index: usize,
    pub encoder_index: Option<usize>,
    pub encoder_address: Option<u64>,
    pub encoder_label: Option<String>,
    pub kernel_name: Option<String>,
    pub start_time_ns: u64,
    pub end_time_ns: u64,
    pub duration_ns: Option<u64>,
    pub grid_size: [u32; 3],
    pub group_size: [u32; 3],
    pub buffers: Vec<TimelineBufferBinding>,
    pub synthetic: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineBufferBinding {
    pub index: usize,
    pub address: u64,
    pub name: Option<String>,
    pub usage: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineEvent {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    pub phase: String,
    pub timestamp_us: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_us: Option<u64>,
    pub process_id: u32,
    pub thread_id: u32,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub args: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawTimelineReport {
    pub profiler_directory: Option<PathBuf>,
    pub file_count: usize,
    pub files: Vec<RawTimelineFile>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawTimelineFile {
    pub path: PathBuf,
    pub file_index: Option<usize>,
    pub file_size: u64,
    pub header: Option<RawTimelineHeader>,
    pub header_timestamps: Vec<u64>,
    pub timestamp_span: Option<RawTimestampSpan>,
    pub heuristic_confidence: String,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawTimelineHeader {
    pub magic: u64,
    pub flags: u64,
    pub counter_count: u32,
    pub data_offset: u64,
    pub entry_count: u64,
    pub gpu_timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawTimestampSpan {
    pub first: u64,
    pub last: u64,
    pub count: usize,
}

#[derive(Debug)]
struct DispatchSpan {
    index: usize,
    encoder_address: Option<u64>,
    encoder_label: Option<String>,
    kernel_name: Option<String>,
    start_time_ns: u64,
    end_time_ns: u64,
    duration_ns: Option<u64>,
    grid_size: [u32; 3],
    group_size: [u32; 3],
    buffers: Vec<TimelineBufferBinding>,
    synthetic: bool,
}

pub fn report(trace: &TraceBundle) -> Result<TimelineReport> {
    let regions = trace.command_buffer_regions()?;
    let profiler_summary = profiler::stream_data_summary(&trace.path).ok();
    Ok(build(&regions, profiler_summary.as_ref()))
}

pub fn build(
    regions: &[CommandBufferRegion],
    profiler_summary: Option<&profiler::ProfilerStreamDataSummary>,
) -> TimelineReport {
    let mut command_buffers = Vec::new();
    let mut encoders = Vec::new();
    let mut dispatches = Vec::new();
    let mut events = Vec::new();
    let mut start_time_ns = u64::MAX;
    let mut end_time_ns = 0u64;
    let profiler_timeline = profiler_summary.and_then(|summary| summary.timeline.as_ref());
    let profiler_dispatches = profiler_summary.map(|summary| &summary.dispatches[..]);
    let command_buffers_profiler_backed =
        profiler_timeline.is_some_and(|timeline| !timeline.command_buffer_timestamps.is_empty());
    let mut profiler_dispatch_count = 0usize;

    events.push(metadata_event(1, 0, "process_name", "gputrace timeline"));
    events.push(metadata_event(1, 1, "thread_name", "Command Buffers"));

    for (region_pos, region) in regions.iter().enumerate() {
        let (timestamp_ns, duration_ns, command_buffer_synthetic) =
            profiler_command_buffer_span(region_pos, regions, profiler_timeline);

        command_buffers.push(TimelineCommandBuffer {
            index: region.command_buffer.index,
            timestamp_ns,
            duration_ns,
            encoder_count: region.encoders.len(),
            dispatch_count: region.dispatches.len(),
        });

        start_time_ns = start_time_ns.min(timestamp_ns);
        let cb_end_ns = duration_ns
            .map(|duration| timestamp_ns.saturating_add(duration))
            .unwrap_or(timestamp_ns);
        end_time_ns = end_time_ns.max(cb_end_ns);

        events.push(TimelineEvent {
            name: format!("CB#{}", region.command_buffer.index),
            category: Some("command_buffer".to_owned()),
            phase: "X".to_owned(),
            timestamp_us: ns_to_us(timestamp_ns),
            duration_us: duration_ns.map(ns_to_us),
            process_id: 1,
            thread_id: 1,
            args: BTreeMap::from([
                ("index".to_owned(), json!(region.command_buffer.index)),
                ("encoder_count".to_owned(), json!(region.encoders.len())),
                ("dispatch_count".to_owned(), json!(region.dispatches.len())),
                (
                    "profiler_backed".to_owned(),
                    json!(!command_buffer_synthetic),
                ),
                ("synthetic".to_owned(), json!(command_buffer_synthetic)),
            ]),
        });

        let dispatch_spans = build_dispatch_spans(
            region,
            command_buffer_start_ticks(profiler_timeline, region_pos),
            timestamp_ns,
            duration_ns,
            profiler_timeline,
            profiler_dispatches,
        );
        let mut encoder_dispatches: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
        for (dispatch_pos, span) in dispatch_spans.iter().enumerate() {
            if let Some(address) = span.encoder_address {
                encoder_dispatches
                    .entry(address)
                    .or_default()
                    .push(dispatch_pos);
            }
        }

        for encoder in &region.encoders {
            let label = encoder_label(&encoder.label, encoder.address);
            let thread_id = 1_000u32.saturating_add(encoder.index as u32);
            events.push(metadata_event(1, thread_id, "thread_name", &label));

            let dispatch_positions = encoder_dispatches.get(&encoder.address);
            let (encoder_start_ns, encoder_end_ns, encoder_duration_ns, synthetic) =
                if let Some(positions) = dispatch_positions {
                    let first = &dispatch_spans[*positions.first().unwrap()];
                    let last = &dispatch_spans[*positions.last().unwrap()];
                    let start = first.start_time_ns;
                    let end = last.end_time_ns.max(start);
                    let duration = end.checked_sub(start).filter(|duration| *duration > 0);
                    let synthetic = positions
                        .iter()
                        .any(|position| dispatch_spans[*position].synthetic());
                    (start, end, duration, synthetic)
                } else if let Some(cb_duration_ns) = duration_ns {
                    let slice = if region.encoders.is_empty() {
                        0
                    } else {
                        cb_duration_ns / region.encoders.len() as u64
                    };
                    let slot = region
                        .encoders
                        .iter()
                        .position(|candidate| candidate.address == encoder.address)
                        .unwrap_or(0) as u64;
                    let start = timestamp_ns.saturating_add(slice.saturating_mul(slot));
                    let end = start.saturating_add(slice);
                    let duration = (slice > 0).then_some(slice);
                    (start, end, duration, true)
                } else {
                    (timestamp_ns, timestamp_ns, None, true)
                };

            start_time_ns = start_time_ns.min(encoder_start_ns);
            end_time_ns = end_time_ns.max(encoder_end_ns);

            encoders.push(TimelineEncoder {
                index: encoder.index,
                command_buffer_index: region.command_buffer.index,
                label: encoder.label.clone(),
                address: encoder.address,
                dispatch_count: dispatch_positions.map_or(0, Vec::len),
                start_time_ns: encoder_start_ns,
                end_time_ns: encoder_end_ns,
                duration_ns: encoder_duration_ns,
                synthetic,
            });

            let mut args = BTreeMap::new();
            args.insert("index".to_owned(), json!(encoder.index));
            args.insert(
                "command_buffer_index".to_owned(),
                json!(region.command_buffer.index),
            );
            args.insert(
                "address".to_owned(),
                json!(format!("0x{:x}", encoder.address)),
            );
            args.insert(
                "dispatch_count".to_owned(),
                json!(dispatch_positions.map_or(0, Vec::len)),
            );
            args.insert("synthetic".to_owned(), json!(synthetic));

            events.push(TimelineEvent {
                name: label,
                category: Some("encoder".to_owned()),
                phase: "X".to_owned(),
                timestamp_us: ns_to_us(encoder_start_ns),
                duration_us: encoder_duration_ns.map(ns_to_us),
                process_id: 1,
                thread_id,
                args,
            });
        }

        for span in dispatch_spans {
            if !span.synthetic() {
                profiler_dispatch_count += 1;
            }
            let encoder_index = span.encoder_address.and_then(|address| {
                region
                    .encoders
                    .iter()
                    .find(|encoder| encoder.address == address)
                    .map(|encoder| encoder.index)
            });
            let thread_id = 2_000u32
                .saturating_add(encoder_index.unwrap_or(region.command_buffer.index) as u32);
            let thread_name = span
                .encoder_label
                .as_deref()
                .map(|label| format!("Dispatches: {label}"))
                .unwrap_or_else(|| format!("Dispatches: CB#{}", region.command_buffer.index));
            events.push(metadata_event(1, thread_id, "thread_name", &thread_name));

            let event_name = span
                .kernel_name
                .clone()
                .or_else(|| span.encoder_label.clone())
                .unwrap_or_else(|| format!("dispatch_{}", span.index));

            let mut args = BTreeMap::new();
            args.insert("index".to_owned(), json!(span.index));
            args.insert(
                "command_buffer_index".to_owned(),
                json!(region.command_buffer.index),
            );
            if let Some(address) = span.encoder_address {
                args.insert(
                    "encoder_address".to_owned(),
                    json!(format!("0x{address:x}")),
                );
            }
            if let Some(label) = &span.encoder_label {
                args.insert("encoder_label".to_owned(), json!(label));
            }
            args.insert("grid_size".to_owned(), json!(span.grid_size));
            args.insert("group_size".to_owned(), json!(span.group_size));
            args.insert("buffer_count".to_owned(), json!(span.buffers.len()));
            args.insert("synthetic".to_owned(), json!(span.synthetic()));

            start_time_ns = start_time_ns.min(span.start_time_ns);
            end_time_ns = end_time_ns.max(span.end_time_ns);

            dispatches.push(TimelineDispatch {
                index: span.index,
                command_buffer_index: region.command_buffer.index,
                encoder_index,
                encoder_address: span.encoder_address,
                encoder_label: span.encoder_label.clone(),
                kernel_name: span.kernel_name.clone(),
                start_time_ns: span.start_time_ns,
                end_time_ns: span.end_time_ns,
                duration_ns: span.duration_ns,
                grid_size: span.grid_size,
                group_size: span.group_size,
                buffers: span.buffers.clone(),
                synthetic: span.synthetic(),
            });

            events.push(TimelineEvent {
                name: event_name,
                category: Some("dispatch".to_owned()),
                phase: "X".to_owned(),
                timestamp_us: ns_to_us(span.start_time_ns),
                duration_us: span.duration_ns.map(ns_to_us),
                process_id: 1,
                thread_id,
                args,
            });
        }
    }

    if start_time_ns == u64::MAX {
        start_time_ns = 0;
    }

    let duration_ns = end_time_ns.saturating_sub(start_time_ns);

    events.sort_by(|left, right| {
        left.timestamp_us
            .cmp(&right.timestamp_us)
            .then_with(|| left.thread_id.cmp(&right.thread_id))
            .then_with(|| left.name.cmp(&right.name))
    });

    encoders.sort_by(|left, right| {
        left.command_buffer_index
            .cmp(&right.command_buffer_index)
            .then_with(|| left.index.cmp(&right.index))
    });
    dispatches.sort_by(|left, right| left.index.cmp(&right.index));

    TimelineReport {
        synthetic: !command_buffers_profiler_backed && profiler_dispatch_count == 0,
        command_buffers_profiler_backed,
        start_time_ns,
        end_time_ns,
        duration_ns,
        command_buffer_count: command_buffers.len(),
        encoder_count: encoders.len(),
        dispatch_count: dispatches.len(),
        command_buffers,
        encoders,
        dispatches,
        events,
    }
}

pub fn raw_report(trace: &TraceBundle) -> Result<RawTimelineReport> {
    build_raw_report_for_path(&trace.path)
}

pub fn export_json(report: &TimelineReport) -> Result<String> {
    Ok(serde_json::to_string_pretty(report)?)
}

pub fn export_raw_json(report: &RawTimelineReport) -> Result<String> {
    Ok(serde_json::to_string_pretty(report)?)
}

pub fn format_report(report: &TimelineReport) -> String {
    let mut out = String::new();
    if report.command_buffers_profiler_backed && !report.synthetic {
        out.push_str("Profiler-backed timeline report\n");
        out.push_str(
            "Command-buffer and dispatch spans come from profiler data when available; remaining gaps stay synthetic.\n\n",
        );
    } else if report.command_buffers_profiler_backed {
        out.push_str("Mixed timeline report\n");
        out.push_str(
            "Command-buffer spans come from profiler APSTimelineData; encoder and dispatch slices remain synthetic.\n\n",
        );
    } else {
        out.push_str("Synthetic timeline report\n");
        out.push_str("Derived from command-buffer ordering and adjacent timestamps.\n\n");
    }
    out.push_str(&format!(
        "start={} ns end={} ns duration={} ns command_buffers={} encoders={} dispatches={}\n\n",
        report.start_time_ns,
        report.end_time_ns,
        report.duration_ns,
        report.command_buffer_count,
        report.encoder_count,
        report.dispatch_count
    ));

    for cb in &report.command_buffers {
        out.push_str(&format!(
            "CB#{} start={:.3} ms duration={} encoders={} dispatches={}\n",
            cb.index,
            ns_to_ms(cb.timestamp_ns),
            format_duration_ms(cb.duration_ns),
            cb.encoder_count,
            cb.dispatch_count
        ));

        for encoder in report
            .encoders
            .iter()
            .filter(|encoder| encoder.command_buffer_index == cb.index)
        {
            let label = encoder_label(&encoder.label, encoder.address);
            out.push_str(&format!(
                "  encoder {} start={:.3} ms duration={} dispatches={}\n",
                label,
                ns_to_ms(encoder.start_time_ns),
                format_duration_ms(encoder.duration_ns),
                encoder.dispatch_count
            ));

            for dispatch in report
                .dispatches
                .iter()
                .filter(|dispatch| dispatch.command_buffer_index == cb.index)
                .filter(|dispatch| dispatch.encoder_index == Some(encoder.index))
            {
                let name = dispatch
                    .kernel_name
                    .clone()
                    .unwrap_or_else(|| format!("dispatch_{}", dispatch.index));
                out.push_str(&format!(
                    "    dispatch {} start={:.3} ms duration={} grid={:?} group={:?}\n",
                    name,
                    ns_to_ms(dispatch.start_time_ns),
                    format_duration_ms(dispatch.duration_ns),
                    dispatch.grid_size,
                    dispatch.group_size
                ));
            }
        }

        for dispatch in report
            .dispatches
            .iter()
            .filter(|dispatch| dispatch.command_buffer_index == cb.index)
            .filter(|dispatch| dispatch.encoder_index.is_none())
        {
            let name = dispatch
                .kernel_name
                .clone()
                .unwrap_or_else(|| format!("dispatch_{}", dispatch.index));
            out.push_str(&format!(
                "  dispatch {} start={:.3} ms duration={} grid={:?} group={:?}\n",
                name,
                ns_to_ms(dispatch.start_time_ns),
                format_duration_ms(dispatch.duration_ns),
                dispatch.grid_size,
                dispatch.group_size
            ));
        }
    }

    out
}

pub fn format_raw_report(report: &RawTimelineReport) -> String {
    let mut out = String::new();
    out.push_str("Raw timeline heuristic report\n");
    match &report.profiler_directory {
        Some(path) => out.push_str(&format!("profiler_directory={}\n", path.display())),
        None => out.push_str("profiler_directory=<not found>\n"),
    }
    out.push_str(&format!("files={}\n", report.file_count));
    for note in &report.notes {
        out.push_str(&format!("note: {note}\n"));
    }
    if !report.files.is_empty() {
        out.push('\n');
    }

    for file in &report.files {
        out.push_str(&format!("{}\n", file.path.display()));
        out.push_str(&format!(
            "  size={} confidence={}\n",
            file.file_size, file.heuristic_confidence
        ));
        if let Some(header) = &file.header {
            out.push_str(&format!(
                "  header magic=0x{:x} counters={} data_offset={} entries={} gpu_timestamp={}\n",
                header.magic,
                header.counter_count,
                header.data_offset,
                header.entry_count,
                header.gpu_timestamp
            ));
        } else {
            out.push_str("  header=<unavailable>\n");
        }
        if let Some(span) = &file.timestamp_span {
            out.push_str(&format!(
                "  header_timestamps={} first={} last={}\n",
                span.count, span.first, span.last
            ));
        } else {
            out.push_str("  header_timestamps=0\n");
        }
        for note in &file.notes {
            out.push_str(&format!("  note: {note}\n"));
        }
    }
    out
}

pub fn format_chrome_trace_json(report: &TimelineReport) -> Result<String> {
    format_trace_json(report)
}

pub fn format_perfetto_trace_json(report: &TimelineReport) -> Result<String> {
    format_trace_json(report)
}

fn format_trace_json(report: &TimelineReport) -> Result<String> {
    #[derive(Serialize)]
    struct ChromeTrace<'a> {
        #[serde(rename = "traceEvents")]
        trace_events: &'a [TimelineEvent],
        #[serde(rename = "displayTimeUnit")]
        display_time_unit: &'static str,
    }

    Ok(serde_json::to_string_pretty(&ChromeTrace {
        trace_events: &report.events,
        display_time_unit: "ms",
    })?)
}

fn metadata_event(process_id: u32, thread_id: u32, name: &str, value: &str) -> TimelineEvent {
    TimelineEvent {
        name: name.to_owned(),
        category: None,
        phase: "M".to_owned(),
        timestamp_us: 0,
        duration_us: None,
        process_id,
        thread_id,
        args: BTreeMap::from([("name".to_owned(), json!(value))]),
    }
}

fn profiler_command_buffer_span(
    region_pos: usize,
    regions: &[CommandBufferRegion],
    profiler_timeline: Option<&profiler::ProfilerTimelineInfo>,
) -> (u64, Option<u64>, bool) {
    if let Some(timeline) = profiler_timeline
        && let Some(entry) = timeline.command_buffer_timestamps.get(region_pos)
    {
        let first_start = timeline
            .command_buffer_timestamps
            .first()
            .map(|value| value.start_ticks)
            .unwrap_or(entry.start_ticks);
        let timestamp_ns = ticks_to_ns(
            entry.start_ticks.saturating_sub(first_start),
            timeline.timebase_numer,
            timeline.timebase_denom,
        );
        let duration_ns = ticks_to_ns(
            entry.end_ticks.saturating_sub(entry.start_ticks),
            timeline.timebase_numer,
            timeline.timebase_denom,
        );
        return (timestamp_ns, Some(duration_ns), false);
    }

    let timestamp_ns = regions[region_pos].command_buffer.timestamp;
    let next_timestamp = regions
        .get(region_pos + 1)
        .map(|next| next.command_buffer.timestamp);
    let duration_ns = next_timestamp.and_then(|next| {
        next.checked_sub(timestamp_ns)
            .filter(|duration| *duration > 0)
    });
    (timestamp_ns, duration_ns, true)
}

fn command_buffer_start_ticks(
    profiler_timeline: Option<&profiler::ProfilerTimelineInfo>,
    region_pos: usize,
) -> Option<u64> {
    profiler_timeline.and_then(|timeline| {
        timeline
            .command_buffer_timestamps
            .get(region_pos)
            .map(|entry| entry.start_ticks)
    })
}

fn build_dispatch_spans(
    region: &CommandBufferRegion,
    command_buffer_start_ticks: Option<u64>,
    command_buffer_start_ns: u64,
    duration_ns: Option<u64>,
    profiler_timeline: Option<&profiler::ProfilerTimelineInfo>,
    profiler_dispatches: Option<&[profiler::ProfilerDispatch]>,
) -> Vec<DispatchSpan> {
    let first_command_buffer_start_ticks = profiler_timeline.and_then(|timeline| {
        timeline
            .command_buffer_timestamps
            .first()
            .map(|entry| entry.start_ticks)
    });

    if let (
        Some(command_buffer_start_ticks),
        Some(first_start_ticks),
        Some(timeline),
        Some(dispatches),
    ) = (
        command_buffer_start_ticks,
        first_command_buffer_start_ticks,
        profiler_timeline,
        profiler_dispatches,
    ) {
        let command_buffer_end_ticks = timeline
            .command_buffer_timestamps
            .iter()
            .find(|entry| entry.start_ticks == command_buffer_start_ticks)
            .map(|entry| entry.end_ticks)
            .unwrap_or(command_buffer_start_ticks);
        let mut spans = Vec::with_capacity(region.dispatches.len());
        for dispatch in &region.dispatches {
            if let Some(profiler_dispatch) = dispatches.get(dispatch.index)
                && profiler_dispatch.end_ticks > profiler_dispatch.start_ticks
                && profiler_dispatch.start_ticks >= command_buffer_start_ticks
                && profiler_dispatch.end_ticks <= command_buffer_end_ticks
            {
                let start_time_ns = ticks_to_ns(
                    profiler_dispatch
                        .start_ticks
                        .saturating_sub(first_start_ticks),
                    timeline.timebase_numer,
                    timeline.timebase_denom,
                );
                let end_time_ns = ticks_to_ns(
                    profiler_dispatch
                        .end_ticks
                        .saturating_sub(first_start_ticks),
                    timeline.timebase_numer,
                    timeline.timebase_denom,
                );
                let (encoder_label, encoder_address) = dispatch
                    .encoder_id
                    .and_then(|address| {
                        region
                            .encoders
                            .iter()
                            .find(|encoder| encoder.address == address)
                            .map(|encoder| (Some(encoder.label.clone()), Some(encoder.address)))
                    })
                    .unwrap_or((None, dispatch.encoder_id));

                spans.push(DispatchSpan {
                    index: dispatch.index,
                    encoder_address,
                    encoder_label,
                    kernel_name: dispatch.kernel_name.clone(),
                    start_time_ns,
                    end_time_ns,
                    duration_ns: end_time_ns
                        .checked_sub(start_time_ns)
                        .filter(|duration| *duration > 0),
                    grid_size: dispatch.grid_size,
                    group_size: dispatch.group_size,
                    buffers: dispatch
                        .buffers
                        .iter()
                        .map(bound_buffer_to_timeline)
                        .collect(),
                    synthetic: false,
                });
                continue;
            }
        }
        if spans.len() == region.dispatches.len() {
            return spans;
        }
    }

    let dispatch_count = region.dispatches.len();
    let per_dispatch_duration = duration_ns.and_then(|duration| {
        if dispatch_count == 0 {
            None
        } else {
            let slice = duration / dispatch_count as u64;
            (slice > 0).then_some(slice)
        }
    });

    let mut current_start_ns = command_buffer_start_ns;
    let mut spans = Vec::with_capacity(dispatch_count);
    for dispatch in &region.dispatches {
        let span_duration_ns = per_dispatch_duration;
        let span_end_ns = span_duration_ns
            .map(|duration| current_start_ns.saturating_add(duration))
            .unwrap_or(current_start_ns);
        let (encoder_label, encoder_address) = dispatch
            .encoder_id
            .and_then(|address| {
                region
                    .encoders
                    .iter()
                    .find(|encoder| encoder.address == address)
                    .map(|encoder| (Some(encoder.label.clone()), Some(encoder.address)))
            })
            .unwrap_or((None, dispatch.encoder_id));

        spans.push(DispatchSpan {
            index: dispatch.index,
            encoder_address,
            encoder_label,
            kernel_name: dispatch.kernel_name.clone(),
            start_time_ns: current_start_ns,
            end_time_ns: span_end_ns,
            duration_ns: span_duration_ns,
            grid_size: dispatch.grid_size,
            group_size: dispatch.group_size,
            buffers: dispatch
                .buffers
                .iter()
                .map(bound_buffer_to_timeline)
                .collect(),
            synthetic: true,
        });
        current_start_ns = span_end_ns;
    }
    spans
}

fn bound_buffer_to_timeline(buffer: &BoundBuffer) -> TimelineBufferBinding {
    TimelineBufferBinding {
        index: buffer.index,
        address: buffer.address,
        name: buffer.name.clone(),
        usage: buffer.usage.to_string(),
    }
}

fn build_raw_report_for_path(trace_path: &Path) -> Result<RawTimelineReport> {
    let profiler_directory = find_profiler_directory(trace_path);
    let mut files = Vec::new();
    let mut notes = vec![
        "Header timestamps are low-confidence profiler sampling candidates, not GPU execution spans."
            .to_owned(),
        "Command-buffer and dispatch durations in the main timeline remain synthetic unless real profiler timing is added."
            .to_owned(),
    ];

    if let Some(dir) = &profiler_directory {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if !name.starts_with("Timeline_f_") || !name.ends_with(".raw") {
                continue;
            }
            files.push(parse_raw_timeline_file(&entry.path())?);
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        if files.is_empty() {
            notes
                .push("No Timeline_f_*.raw files were found in the profiler directory.".to_owned());
        }
    } else {
        notes.push(
            "No .gpuprofiler_raw directory was found next to or inside the trace bundle."
                .to_owned(),
        );
    }

    Ok(RawTimelineReport {
        profiler_directory,
        file_count: files.len(),
        files,
        notes,
    })
}

fn find_profiler_directory(trace_path: &Path) -> Option<PathBuf> {
    if trace_path.is_dir()
        && trace_path
            .extension()
            .is_some_and(|ext| ext == "gpuprofiler_raw")
    {
        return Some(trace_path.to_path_buf());
    }

    let direct = PathBuf::from(format!("{}.gpuprofiler_raw", trace_path.display()));
    if direct.is_dir() {
        return Some(direct);
    }

    fs::read_dir(trace_path)
        .ok()?
        .filter_map(|entry| entry.ok())
        .find_map(|entry| {
            if entry.file_type().ok()?.is_dir()
                && entry
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "gpuprofiler_raw")
            {
                Some(entry.path())
            } else {
                None
            }
        })
}

fn parse_raw_timeline_file(path: &Path) -> Result<RawTimelineFile> {
    let data = fs::read(path)?;
    let file_size = data.len() as u64;
    let header = parse_raw_timeline_header(&data);
    let header_timestamps = extract_header_timestamps(&data);
    let timestamp_span = match (header_timestamps.first(), header_timestamps.last()) {
        (Some(first), Some(last)) => Some(RawTimestampSpan {
            first: *first,
            last: *last,
            count: header_timestamps.len(),
        }),
        _ => None,
    };
    let mut notes = Vec::new();
    if header.is_none() {
        notes.push("File is smaller than the 256-byte timeline header.".to_owned());
    }
    if header_timestamps.is_empty() {
        notes.push(
            "No timestamp-like values were found in the 500..800 byte heuristic scan window."
                .to_owned(),
        );
    }
    notes.push("Parsing stops at header/sample discovery; packed payload decoding is intentionally not guessed.".to_owned());

    Ok(RawTimelineFile {
        path: path.to_path_buf(),
        file_index: parse_timeline_file_index(path),
        file_size,
        header,
        header_timestamps,
        timestamp_span,
        heuristic_confidence: "low".to_owned(),
        notes,
    })
}

fn parse_timeline_file_index(path: &Path) -> Option<usize> {
    let name = path.file_name()?.to_str()?;
    let index = name.strip_prefix("Timeline_f_")?.strip_suffix(".raw")?;
    index.parse().ok()
}

fn parse_raw_timeline_header(data: &[u8]) -> Option<RawTimelineHeader> {
    if data.len() < 256 {
        return None;
    }
    Some(RawTimelineHeader {
        magic: u64::from_le_bytes(data[0..8].try_into().ok()?),
        flags: u64::from_le_bytes(data[8..16].try_into().ok()?),
        counter_count: u32::from_le_bytes(data[12..16].try_into().ok()?),
        data_offset: u64::from_le_bytes(data[32..40].try_into().ok()?),
        entry_count: u64::from_le_bytes(data[80..88].try_into().ok()?),
        gpu_timestamp: u64::from_le_bytes(data[104..112].try_into().ok()?),
    })
}

fn extract_header_timestamps(data: &[u8]) -> Vec<u64> {
    let start = 500usize.min(data.len());
    let end = 800usize.min(data.len());
    if end <= start {
        return Vec::new();
    }

    let mut timestamps = Vec::new();
    let window = &data[start..end];
    let mut offset = 0usize;
    while offset + 8 <= window.len() {
        let candidate = u64::from_le_bytes(window[offset..offset + 8].try_into().unwrap());
        if candidate > 100_000 {
            timestamps.push(candidate);
        }
        offset += 8;
    }
    timestamps
}

fn encoder_label(label: &str, address: u64) -> String {
    if label.is_empty() {
        format!("0x{address:x}")
    } else {
        label.to_owned()
    }
}

fn ns_to_us(value: u64) -> u64 {
    value / 1_000
}

fn ticks_to_ns(ticks: u64, numer: u64, denom: u64) -> u64 {
    ticks.saturating_mul(numer.max(1)) / denom.max(1)
}

fn ns_to_ms(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}

fn format_duration_ms(duration_ns: Option<u64>) -> String {
    duration_ns
        .map(|duration| format!("{:.3} ms", ns_to_ms(duration)))
        .unwrap_or_else(|| "?".to_owned())
}

impl DispatchSpan {
    fn synthetic(&self) -> bool {
        self.synthetic
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use tempfile::tempdir;

    use crate::trace::{CommandBuffer, ComputeEncoder, DispatchCall, MTLResourceUsage};

    fn profiler_summary_with_timeline(
        timeline: profiler::ProfilerTimelineInfo,
        dispatches: Vec<profiler::ProfilerDispatch>,
    ) -> profiler::ProfilerStreamDataSummary {
        profiler::ProfilerStreamDataSummary {
            function_names: vec![],
            pipelines: vec![],
            execution_costs: vec![],
            dispatches,
            encoder_timings: vec![],
            timeline: Some(timeline),
            num_pipelines: 0,
            num_gpu_commands: 0,
            num_encoders: 0,
            total_time_us: 0,
        }
    }

    #[test]
    fn builds_synthetic_timeline_report() {
        let regions = vec![
            CommandBufferRegion {
                command_buffer: CommandBuffer {
                    index: 0,
                    timestamp: 1_000_000,
                    offset: 0,
                },
                end_offset: 100,
                encoders: vec![ComputeEncoder {
                    index: 0,
                    address: 0x10,
                    label: "main".to_owned(),
                    offset: 10,
                }],
                pipeline_events: vec![],
                dispatches: vec![
                    DispatchCall {
                        index: 0,
                        offset: 20,
                        encoder_id: Some(0x10),
                        pipeline_addr: Some(0xaa),
                        kernel_name: Some("k0".to_owned()),
                        buffers: vec![BoundBuffer {
                            address: 0x99,
                            name: Some("buf0".to_owned()),
                            index: 0,
                            usage: MTLResourceUsage::READ,
                        }],
                        grid_size: [8, 1, 1],
                        group_size: [4, 1, 1],
                    },
                    DispatchCall {
                        index: 1,
                        offset: 30,
                        encoder_id: Some(0x10),
                        pipeline_addr: Some(0xbb),
                        kernel_name: Some("k1".to_owned()),
                        buffers: vec![],
                        grid_size: [16, 1, 1],
                        group_size: [8, 1, 1],
                    },
                ],
            },
            CommandBufferRegion {
                command_buffer: CommandBuffer {
                    index: 1,
                    timestamp: 5_000_000,
                    offset: 100,
                },
                end_offset: 200,
                encoders: vec![],
                pipeline_events: vec![],
                dispatches: vec![],
            },
        ];

        let report = build(&regions, None);

        assert!(report.synthetic);
        assert!(!report.command_buffers_profiler_backed);
        assert_eq!(report.command_buffer_count, 2);
        assert_eq!(report.encoder_count, 1);
        assert_eq!(report.dispatch_count, 2);
        assert_eq!(report.duration_ns, 4_000_000);
        assert_eq!(report.command_buffers[0].duration_ns, Some(4_000_000));
        assert_eq!(report.encoders[0].duration_ns, Some(4_000_000));
        assert_eq!(report.dispatches[0].duration_ns, Some(2_000_000));
        assert_eq!(report.dispatches[1].start_time_ns, 3_000_000);

        let text = format_report(&report);
        assert!(text.contains("CB#0"));
        assert!(text.contains("dispatch k0"));

        let json = export_json(&report).unwrap();
        assert!(json.contains("\"command_buffer_count\": 2"));

        let chrome = format_chrome_trace_json(&report).unwrap();
        assert!(chrome.contains("\"traceEvents\""));
        assert!(chrome.contains("\"displayTimeUnit\": \"ms\""));
    }

    #[test]
    fn builds_profiler_backed_command_buffer_spans() {
        let regions = vec![
            CommandBufferRegion {
                command_buffer: CommandBuffer {
                    index: 0,
                    timestamp: 1_000_000,
                    offset: 0,
                },
                end_offset: 100,
                encoders: vec![],
                pipeline_events: vec![],
                dispatches: vec![],
            },
            CommandBufferRegion {
                command_buffer: CommandBuffer {
                    index: 1,
                    timestamp: 5_000_000,
                    offset: 100,
                },
                end_offset: 200,
                encoders: vec![],
                pipeline_events: vec![],
                dispatches: vec![],
            },
        ];

        let timeline = profiler::ProfilerTimelineInfo {
            command_buffer_timestamps: vec![
                profiler::ProfilerCommandBufferTimestamp {
                    index: 0,
                    start_ticks: 100,
                    end_ticks: 160,
                },
                profiler::ProfilerCommandBufferTimestamp {
                    index: 1,
                    start_ticks: 200,
                    end_ticks: 320,
                },
            ],
            encoder_profiles: vec![],
            timebase_numer: 125,
            timebase_denom: 5,
            absolute_time: 99_999,
        };

        let summary = profiler_summary_with_timeline(timeline, vec![]);
        let report = build(&regions, Some(&summary));

        assert!(!report.synthetic);
        assert!(report.command_buffers_profiler_backed);
        assert_eq!(report.command_buffers[0].timestamp_ns, 0);
        assert_eq!(report.command_buffers[0].duration_ns, Some(1_500));
        assert_eq!(report.command_buffers[1].timestamp_ns, 2_500);

        let text = format_report(&report);
        assert!(text.contains("Profiler-backed timeline report"));
    }

    #[test]
    fn builds_profiler_backed_dispatch_spans() {
        let regions = vec![CommandBufferRegion {
            command_buffer: CommandBuffer {
                index: 0,
                timestamp: 1_000_000,
                offset: 0,
            },
            end_offset: 100,
            encoders: vec![ComputeEncoder {
                index: 0,
                address: 0x10,
                label: "main".to_owned(),
                offset: 10,
            }],
            pipeline_events: vec![],
            dispatches: vec![
                DispatchCall {
                    index: 0,
                    offset: 20,
                    encoder_id: Some(0x10),
                    pipeline_addr: Some(0xaa),
                    kernel_name: Some("k0".to_owned()),
                    buffers: vec![],
                    grid_size: [8, 1, 1],
                    group_size: [4, 1, 1],
                },
                DispatchCall {
                    index: 1,
                    offset: 30,
                    encoder_id: Some(0x10),
                    pipeline_addr: Some(0xbb),
                    kernel_name: Some("k1".to_owned()),
                    buffers: vec![],
                    grid_size: [16, 1, 1],
                    group_size: [8, 1, 1],
                },
            ],
        }];

        let timeline = profiler::ProfilerTimelineInfo {
            command_buffer_timestamps: vec![profiler::ProfilerCommandBufferTimestamp {
                index: 0,
                start_ticks: 100,
                end_ticks: 220,
            }],
            encoder_profiles: vec![],
            timebase_numer: 10,
            timebase_denom: 1,
            absolute_time: 0,
        };
        let dispatches = vec![
            profiler::ProfilerDispatch {
                index: 0,
                pipeline_index: 0,
                pipeline_id: None,
                function_name: Some("k0".to_owned()),
                encoder_index: 0,
                cumulative_us: 50,
                duration_us: 50,
                sample_count: 2,
                sampling_density: 0.04,
                start_ticks: 100,
                end_ticks: 140,
            },
            profiler::ProfilerDispatch {
                index: 1,
                pipeline_index: 1,
                pipeline_id: None,
                function_name: Some("k1".to_owned()),
                encoder_index: 0,
                cumulative_us: 120,
                duration_us: 70,
                sample_count: 3,
                sampling_density: 0.043,
                start_ticks: 140,
                end_ticks: 220,
            },
        ];

        let summary = profiler_summary_with_timeline(timeline, dispatches);
        let report = build(&regions, Some(&summary));

        assert!(!report.synthetic);
        assert!(report.command_buffers_profiler_backed);
        assert_eq!(report.dispatches[0].start_time_ns, 0);
        assert_eq!(report.dispatches[0].duration_ns, Some(400));
        assert_eq!(report.dispatches[1].start_time_ns, 400);
        assert_eq!(report.dispatches[1].duration_ns, Some(800));
        assert!(!report.dispatches[0].synthetic);
        assert!(!report.encoders[0].synthetic);

        let text = format_report(&report);
        assert!(text.contains("Profiler-backed timeline report"));
    }

    #[test]
    fn parses_raw_timeline_header_and_timestamps() {
        let dir = tempdir().unwrap();
        let raw_path = dir.path().join("Timeline_f_3.raw");
        let mut data = vec![0u8; 1024];
        data[0..8].copy_from_slice(&0x773d413b0016b551u64.to_le_bytes());
        data[8..16].copy_from_slice(&0x12u64.to_le_bytes());
        data[12..16].copy_from_slice(&752u32.to_le_bytes());
        data[32..40].copy_from_slice(&0x3c000u64.to_le_bytes());
        data[80..88].copy_from_slice(&42u64.to_le_bytes());
        data[104..112].copy_from_slice(&123456789u64.to_le_bytes());
        data[500..508].copy_from_slice(&111_111u64.to_le_bytes());
        data[508..516].copy_from_slice(&222_222u64.to_le_bytes());
        fs::write(&raw_path, data).unwrap();

        let file = parse_raw_timeline_file(&raw_path).unwrap();

        assert_eq!(file.file_index, Some(3));
        assert_eq!(file.header.as_ref().unwrap().counter_count, 752);
        assert_eq!(file.header_timestamps, vec![111_111, 222_222]);
        assert_eq!(file.timestamp_span.as_ref().unwrap().count, 2);
        assert_eq!(file.heuristic_confidence, "low");
    }

    #[test]
    fn raw_report_finds_profiler_directory_inside_bundle() {
        let dir = tempdir().unwrap();
        let trace_path = dir.path().join("sample.gputrace");
        let profiler_dir = trace_path.join("capture.gpuprofiler_raw");
        fs::create_dir_all(&profiler_dir).unwrap();
        fs::write(profiler_dir.join("Timeline_f_0.raw"), vec![0u8; 256]).unwrap();

        let report = build_raw_report_for_path(&trace_path).unwrap();

        assert_eq!(report.file_count, 1);
        assert_eq!(
            report.profiler_directory.as_deref(),
            Some(profiler_dir.as_path())
        );

        let text = format_raw_report(&report);
        assert!(text.contains("Raw timeline heuristic report"));
        assert!(text.contains("Timeline_f_0.raw"));

        let json = export_raw_json(&report).unwrap();
        assert!(json.contains("\"file_count\": 1"));
    }
}
