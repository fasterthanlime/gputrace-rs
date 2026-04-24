use std::collections::{BTreeMap, BTreeSet};

use memchr::memmem;
use serde::Serialize;

use crate::error::Result;
use crate::trace::{
    BoundBuffer, CommandBufferRegion, ComputeEncoder, DispatchCall, PipelineStateEvent, TraceBundle,
};

#[derive(Debug, Clone, Serialize)]
pub struct ApiCallsReport {
    pub synthetic: bool,
    pub filter: Option<String>,
    pub total_init_calls: usize,
    pub total_command_buffers: usize,
    pub total_dispatches: usize,
    pub matched_dispatches: usize,
    pub total_calls: usize,
    pub init_calls: Vec<ApiInitCallEntry>,
    pub command_buffers: Vec<ApiCallCommandBuffer>,
    pub calls: Vec<ApiCallEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiInitCallEntry {
    pub sequence: usize,
    pub offset: usize,
    pub kind: String,
    pub address: Option<u64>,
    pub label: Option<String>,
    pub info: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiCallCommandBuffer {
    pub index: usize,
    pub timestamp_ns: u64,
    pub offset: usize,
    pub end_offset: usize,
    pub encoder_count: usize,
    pub dispatch_count: usize,
    pub call_count: usize,
    pub kernels: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiCallEntry {
    pub sequence: usize,
    pub command_buffer_index: usize,
    pub offset: usize,
    pub kind: String,
    pub call: String,
    pub details: String,
    pub encoder_index: Option<usize>,
    pub encoder_label: Option<String>,
    pub encoder_address: Option<u64>,
    pub pipeline_addr: Option<u64>,
    pub kernel_name: Option<String>,
    pub dispatch_index: Option<usize>,
    pub buffer_slot: Option<usize>,
    pub buffer_name: Option<String>,
    pub buffer_usage: Option<String>,
    pub grid_size: Option<[u32; 3]>,
    pub group_size: Option<[u32; 3]>,
}

pub fn report(trace: &TraceBundle, filter: Option<&str>) -> Result<ApiCallsReport> {
    let capture = trace.capture_data()?;
    let init_calls = parse_initialization_calls(&capture);
    let regions = trace.command_buffer_regions()?;
    Ok(report_from_regions_and_init(&regions, init_calls, filter))
}

pub fn filter_command_buffer_report(
    report: &ApiCallsReport,
    command_buffer_index: usize,
) -> ApiCallsReport {
    let command_buffers = report
        .command_buffers
        .iter()
        .filter(|command_buffer| command_buffer.index == command_buffer_index)
        .cloned()
        .collect::<Vec<_>>();
    let calls = report
        .calls
        .iter()
        .filter(|call| call.command_buffer_index == command_buffer_index)
        .cloned()
        .enumerate()
        .map(|(sequence, mut call)| {
            call.sequence = sequence;
            call
        })
        .collect::<Vec<_>>();

    report_with_filtered_rows(report, command_buffers, calls)
}

pub fn filter_call_kind_report(report: &ApiCallsReport, kind: &str) -> ApiCallsReport {
    let calls = report
        .calls
        .iter()
        .filter(|call| call.kind == kind)
        .cloned()
        .enumerate()
        .map(|(sequence, mut call)| {
            call.sequence = sequence;
            call
        })
        .collect::<Vec<_>>();
    let command_buffer_indexes = calls
        .iter()
        .map(|call| call.command_buffer_index)
        .collect::<BTreeSet<_>>();
    let command_buffers = report
        .command_buffers
        .iter()
        .filter(|command_buffer| command_buffer_indexes.contains(&command_buffer.index))
        .cloned()
        .map(|mut command_buffer| {
            command_buffer.call_count = calls
                .iter()
                .filter(|call| call.command_buffer_index == command_buffer.index)
                .count();
            command_buffer
        })
        .collect::<Vec<_>>();

    report_with_filtered_rows(report, command_buffers, calls)
}

pub fn format_report(report: &ApiCallsReport) -> String {
    let mut out = String::new();
    out.push_str("Synthetic API-call report\n");
    out.push_str(
        "Synthesized from initialization records, command-buffer regions, encoder attribution, pipeline-state events, and dispatch records.\n",
    );
    out.push_str("This is an honest approximation of API intent, not a verbatim intercepted call stream.\n\n");
    if let Some(filter) = &report.filter {
        out.push_str(&format!(
            "filter={filter:?}, init_calls={}, command_buffers={}, dispatches={} (matched={}), synthesized_calls={}\n\n",
            report.total_init_calls,
            report.total_command_buffers,
            report.total_dispatches,
            report.matched_dispatches,
            report.total_calls
        ));
    } else {
        out.push_str(&format!(
            "init_calls={}, command_buffers={}, dispatches={}, synthesized_calls={}\n\n",
            report.total_init_calls,
            report.total_command_buffers,
            report.total_dispatches,
            report.total_calls
        ));
    }

    if !report.init_calls.is_empty() {
        out.push_str("initialization\n");
        for call in &report.init_calls {
            let prefix = call
                .label
                .clone()
                .or_else(|| call.address.map(|address| format!("0x{address:x}")))
                .unwrap_or_else(|| "-".to_owned());
            out.push_str(&format!(
                "  #{:>4} @0x{:08x} {:<18} {} = {}\n",
                call.sequence, call.offset, call.kind, prefix, call.info
            ));
        }
        out.push('\n');
    }

    let calls_by_cb: BTreeMap<usize, Vec<&ApiCallEntry>> =
        report.calls.iter().fold(BTreeMap::new(), |mut acc, call| {
            acc.entry(call.command_buffer_index).or_default().push(call);
            acc
        });

    for cb in &report.command_buffers {
        out.push_str(&format!(
            "CB {}: ts={} ns offset=0x{:x} end=0x{:x} encoders={} dispatches={} calls={}\n",
            cb.index,
            cb.timestamp_ns,
            cb.offset,
            cb.end_offset,
            cb.encoder_count,
            cb.dispatch_count,
            cb.call_count
        ));
        if !cb.kernels.is_empty() {
            out.push_str(&format!("  kernels: {}\n", cb.kernels.join(", ")));
        }
        if let Some(calls) = calls_by_cb.get(&cb.index) {
            for call in calls {
                out.push_str(&format!(
                    "  #{:>4} @0x{:08x} {:<24} {}\n",
                    call.sequence, call.offset, call.call, call.details
                ));
            }
        }
        out.push('\n');
    }

    out
}

fn report_with_filtered_rows(
    report: &ApiCallsReport,
    command_buffers: Vec<ApiCallCommandBuffer>,
    calls: Vec<ApiCallEntry>,
) -> ApiCallsReport {
    ApiCallsReport {
        synthetic: report.synthetic,
        filter: report.filter.clone(),
        total_init_calls: report.total_init_calls,
        total_command_buffers: command_buffers.len(),
        total_dispatches: command_buffers
            .iter()
            .map(|command_buffer| command_buffer.dispatch_count)
            .sum(),
        matched_dispatches: calls.iter().filter(|call| call.kind == "dispatch").count(),
        total_calls: report.init_calls.len() + calls.len(),
        init_calls: report.init_calls.clone(),
        command_buffers,
        calls,
    }
}

#[cfg(test)]
fn report_from_regions(regions: &[CommandBufferRegion], filter: Option<&str>) -> ApiCallsReport {
    report_from_regions_and_init(regions, Vec::new(), filter)
}

fn report_from_regions_and_init(
    regions: &[CommandBufferRegion],
    init_calls: Vec<ApiInitCallEntry>,
    filter: Option<&str>,
) -> ApiCallsReport {
    let filter_lower = filter.map(|value| value.to_ascii_lowercase());
    let included_regions: Vec<_> = regions
        .iter()
        .filter(|region| region_matches(region, filter_lower.as_deref()))
        .collect();

    let total_dispatches = included_regions
        .iter()
        .map(|region| region.dispatches.len())
        .sum();
    let matched_dispatches = included_regions
        .iter()
        .map(|region| matched_dispatch_count(region, filter_lower.as_deref()))
        .sum();

    let mut calls = Vec::new();
    let mut command_buffers = Vec::new();

    for region in included_regions {
        let call_start = calls.len();
        push_call(
            &mut calls,
            region.command_buffer.index,
            region.command_buffer.offset,
            "command_buffer",
            "commandBuffer",
            format!(
                "observed command buffer region start (ts={} ns)",
                region.command_buffer.timestamp
            ),
        );

        let mut seen_encoders = BTreeSet::new();
        let mut last_pipeline_by_encoder = BTreeMap::<u64, u64>::new();

        for dispatch in &region.dispatches {
            let encoder = resolve_encoder(region, dispatch);
            if let Some(encoder) = encoder
                && seen_encoders.insert(encoder.address)
            {
                push_call_with_context(
                    &mut calls,
                    region.command_buffer.index,
                    encoder.offset,
                    "encoder",
                    "computeCommandEncoder",
                    format!("observed encoder {}", display_encoder(encoder)),
                    Some(encoder),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                );
            }

            let pipeline_event = resolve_pipeline_event(region, dispatch, encoder);
            let pipeline_addr = dispatch
                .pipeline_addr
                .or_else(|| pipeline_event.map(|event| event.pipeline_addr));
            let kernel_name = dispatch
                .kernel_name
                .clone()
                .or_else(|| encoder.and_then(kernel_name_from_encoder));

            if let Some(pipeline_addr) = pipeline_addr {
                let encoder_address = encoder.map(|value| value.address).unwrap_or_default();
                let changed = last_pipeline_by_encoder
                    .get(&encoder_address)
                    .copied()
                    .is_none_or(|previous| previous != pipeline_addr);
                if changed {
                    push_call_with_context(
                        &mut calls,
                        region.command_buffer.index,
                        pipeline_event.map_or(dispatch.offset, |event| event.offset),
                        "pipeline",
                        "setComputePipelineState",
                        format!(
                            "{} (0x{:x})",
                            kernel_name.clone().unwrap_or_else(|| "unknown".to_owned()),
                            pipeline_addr
                        ),
                        encoder,
                        Some(pipeline_addr),
                        kernel_name.clone(),
                        Some(dispatch.index),
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                    last_pipeline_by_encoder.insert(encoder_address, pipeline_addr);
                }
            }

            for buffer in &dispatch.buffers {
                push_call_with_context(
                    &mut calls,
                    region.command_buffer.index,
                    dispatch.offset,
                    "buffer_binding",
                    "setBuffer",
                    format!(
                        "slot={} buffer={} usage={}",
                        buffer.index,
                        display_buffer(buffer),
                        buffer.usage
                    ),
                    encoder,
                    pipeline_addr,
                    kernel_name.clone(),
                    Some(dispatch.index),
                    Some(buffer.index),
                    Some(display_buffer(buffer)),
                    Some(buffer.usage.to_string()),
                    None,
                    None,
                );
            }

            push_call_with_context(
                &mut calls,
                region.command_buffer.index,
                dispatch.offset,
                "dispatch",
                "dispatchThreadgroups",
                format!(
                    "kernel={} grid={} group={}",
                    kernel_name.clone().unwrap_or_else(|| "unknown".to_owned()),
                    format_dims(dispatch.grid_size),
                    format_dims(dispatch.group_size)
                ),
                encoder,
                pipeline_addr,
                kernel_name,
                Some(dispatch.index),
                None,
                None,
                None,
                Some(dispatch.grid_size),
                Some(dispatch.group_size),
            );
        }

        command_buffers.push(ApiCallCommandBuffer {
            index: region.command_buffer.index,
            timestamp_ns: region.command_buffer.timestamp,
            offset: region.command_buffer.offset,
            end_offset: region.end_offset,
            encoder_count: region.encoders.len(),
            dispatch_count: region.dispatches.len(),
            call_count: calls.len() - call_start,
            kernels: command_buffer_kernels(region),
        });
    }

    ApiCallsReport {
        synthetic: true,
        filter: filter.map(ToOwned::to_owned),
        total_init_calls: init_calls.len(),
        total_command_buffers: command_buffers.len(),
        total_dispatches,
        matched_dispatches,
        total_calls: init_calls.len() + calls.len(),
        init_calls,
        command_buffers,
        calls,
    }
}

fn parse_initialization_calls(capture: &[u8]) -> Vec<ApiInitCallEntry> {
    let first_command_buffer = memmem::find(capture, b"CUUU").unwrap_or(capture.len());
    let data = &capture[..first_command_buffer];
    let (cs_records, label_map) = parse_cs_records_from_init(data);
    let mut calls = Vec::new();

    for absolute in find_marker_offsets(data, b"CUt\0") {
        if let Some(address) = read_u64(data, absolute + 0x04)
            && address != 0
        {
            calls.push(init_call(
                absolute,
                "newResidencySet",
                Some(address),
                None,
                "[Device newResidencySetWithDescriptor:<data> error:nil]".to_owned(),
            ));
        }
    }

    for absolute in find_marker_offsets(data, b"CU\0\0") {
        if let Some(heap_addr) = read_u64(data, absolute + 0x24)
            && heap_addr != 0
        {
            calls.push(init_call(
                absolute,
                "newHeap",
                Some(heap_addr),
                None,
                "[Device newHeapWithDescriptor:<data>]".to_owned(),
            ));
        }
    }

    for absolute in find_marker_offsets(data, b"Culul") {
        let Some(heap_addr) = read_u64(data, absolute + 0x08) else {
            continue;
        };
        let Some(buffer_len) = read_u64(data, absolute + 0x10) else {
            continue;
        };
        let Some(buffer_addr) = read_u64(data, absolute + 0x24) else {
            continue;
        };
        calls.push(init_call(
            absolute,
            "newBuffer",
            Some(buffer_addr),
            None,
            format!(
                "[0x{heap_addr:x} newBufferWithLength:{buffer_len} options:HazardTrackingModeUntracked]"
            ),
        ));
        calls.push(init_call(
            absolute + 1,
            "bufferHeapOffset",
            Some(buffer_addr),
            None,
            format!("BufferHeapOffset(0x{buffer_addr:x}, 0)"),
        ));
    }

    for record in cs_records {
        if record.label.contains("Stream") || record.label.contains("Queue") {
            calls.push(init_call(
                record.offset,
                "newCommandQueue",
                Some(record.cs_address),
                Some(record.label.clone()),
                format!("{} = [Device newCommandQueue]", record.label),
            ));
        } else if looks_like_function_name(&record.label) && (record.cs_address >> 32) >= 0x7 {
            calls.push(init_call(
                record.offset,
                "newFunction",
                Some(record.cs_address),
                Some(record.label.clone()),
                format!(
                    "[0x{:x} newFunctionWithName:\"{}\"]",
                    record.cs_address, record.label
                ),
            ));
        }
    }

    for absolute in find_marker_offsets(data, b"Cui\0") {
        if let Some(event_addr) = read_u64(data, absolute + 0x0c)
            && event_addr != 0
        {
            calls.push(init_call(
                absolute,
                "newSharedEvent",
                Some(event_addr),
                None,
                "[Device newSharedEvent]".to_owned(),
            ));
        }
    }

    for absolute in find_marker_offsets(data, b"Ctt\0") {
        let Some(function_addr) = read_u64(data, absolute + 0x0c) else {
            continue;
        };
        let Some(pipeline_addr) = read_u64(data, absolute + 0x20) else {
            continue;
        };
        if pipeline_addr == 0 {
            continue;
        }
        let function_name = label_map
            .get(&function_addr)
            .cloned()
            .unwrap_or_else(|| "function".to_owned());
        calls.push(init_call(
            absolute,
            "newPipelineState",
            Some(pipeline_addr),
            None,
            format!("[Device newComputePipelineStateWithFunction:{function_name} error:nil]"),
        ));
    }

    for call in &mut calls {
        if call.label.is_none()
            && let Some(address) = call.address
            && let Some(label) = label_map.get(&address)
        {
            call.label = Some(label.clone());
        }
    }

    calls.sort_by(|left, right| {
        left.offset
            .cmp(&right.offset)
            .then_with(|| left.kind.cmp(&right.kind))
    });
    for (sequence, call) in calls.iter_mut().enumerate() {
        call.sequence = sequence;
    }
    calls
}

fn init_call(
    offset: usize,
    kind: &str,
    address: Option<u64>,
    label: Option<String>,
    info: String,
) -> ApiInitCallEntry {
    ApiInitCallEntry {
        sequence: 0,
        offset,
        kind: kind.to_owned(),
        address,
        label,
        info,
    }
}

#[derive(Debug)]
struct CsInitRecord {
    cs_address: u64,
    label: String,
    offset: usize,
}

fn parse_cs_records_from_init(data: &[u8]) -> (Vec<CsInitRecord>, BTreeMap<u64, String>) {
    let mut records = Vec::new();
    let mut labels = BTreeMap::new();
    for offset in find_marker_offsets(data, b"CS") {
        let Some(address) = read_u64(data, offset + 4) else {
            continue;
        };
        let label_start = offset + 12;
        let Some(label) = read_c_string(data, label_start, 128) else {
            continue;
        };
        if !is_printable_ascii(&label) {
            continue;
        }
        labels.insert(address, label.clone());
        records.push(CsInitRecord {
            cs_address: address,
            label: label.clone(),
            offset,
        });

        if looks_like_function_name(&label) {
            let search_start = label_start + label.len() + 1;
            let search_end = (offset + 0x30).min(data.len());
            if search_start < search_end
                && let Some(relative) = memmem::find(&data[search_start..search_end], b"t\0\0\0")
                && let Some(function_addr) = read_u64(data, search_start + relative + 4)
                && function_addr != 0
                && function_addr != address
            {
                labels.insert(function_addr, label);
            }
        }
    }
    (records, labels)
}

fn find_marker_offsets(data: &[u8], marker: &[u8]) -> Vec<usize> {
    memmem::find_iter(data, marker).collect()
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    data.get(offset..offset + 8)
        .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_c_string(data: &[u8], offset: usize, max_len: usize) -> Option<String> {
    let bytes = data.get(offset..)?;
    let len = bytes.iter().take(max_len).position(|byte| *byte == 0)?;
    (len > 0).then(|| String::from_utf8_lossy(&bytes[..len]).into_owned())
}

fn is_printable_ascii(value: &str) -> bool {
    value.bytes().all(|byte| (32..=126).contains(&byte))
}

fn looks_like_function_name(value: &str) -> bool {
    value.contains('_')
        || value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
}

fn push_call(
    calls: &mut Vec<ApiCallEntry>,
    command_buffer_index: usize,
    offset: usize,
    kind: &str,
    call: &str,
    details: String,
) {
    calls.push(ApiCallEntry {
        sequence: calls.len(),
        command_buffer_index,
        offset,
        kind: kind.to_owned(),
        call: call.to_owned(),
        details,
        encoder_index: None,
        encoder_label: None,
        encoder_address: None,
        pipeline_addr: None,
        kernel_name: None,
        dispatch_index: None,
        buffer_slot: None,
        buffer_name: None,
        buffer_usage: None,
        grid_size: None,
        group_size: None,
    });
}

#[allow(clippy::too_many_arguments)]
fn push_call_with_context(
    calls: &mut Vec<ApiCallEntry>,
    command_buffer_index: usize,
    offset: usize,
    kind: &str,
    call: &str,
    details: String,
    encoder: Option<&ComputeEncoder>,
    pipeline_addr: Option<u64>,
    kernel_name: Option<String>,
    dispatch_index: Option<usize>,
    buffer_slot: Option<usize>,
    buffer_name: Option<String>,
    buffer_usage: Option<String>,
    grid_size: Option<[u32; 3]>,
    group_size: Option<[u32; 3]>,
) {
    calls.push(ApiCallEntry {
        sequence: calls.len(),
        command_buffer_index,
        offset,
        kind: kind.to_owned(),
        call: call.to_owned(),
        details,
        encoder_index: encoder.map(|value| value.index),
        encoder_label: encoder
            .and_then(|value| (!value.label.is_empty()).then(|| value.label.clone())),
        encoder_address: encoder.map(|value| value.address),
        pipeline_addr,
        kernel_name,
        dispatch_index,
        buffer_slot,
        buffer_name,
        buffer_usage,
        grid_size,
        group_size,
    });
}

fn resolve_encoder<'a>(
    region: &'a CommandBufferRegion,
    dispatch: &DispatchCall,
) -> Option<&'a ComputeEncoder> {
    dispatch
        .encoder_id
        .and_then(|encoder_id| {
            region
                .encoders
                .iter()
                .find(|encoder| encoder.address == encoder_id)
        })
        .or_else(|| {
            region
                .encoders
                .iter()
                .rev()
                .find(|encoder| encoder.offset <= dispatch.offset)
        })
}

fn resolve_pipeline_event<'a>(
    region: &'a CommandBufferRegion,
    dispatch: &DispatchCall,
    encoder: Option<&ComputeEncoder>,
) -> Option<&'a PipelineStateEvent> {
    region.pipeline_events.iter().rev().find(|event| {
        event.offset <= dispatch.offset
            && encoder.is_none_or(|encoder| event.encoder_addr == encoder.address)
    })
}

fn kernel_name_from_encoder(encoder: &ComputeEncoder) -> Option<String> {
    (!encoder.label.is_empty()).then(|| encoder.label.clone())
}

fn command_buffer_kernels(region: &CommandBufferRegion) -> Vec<String> {
    region
        .dispatches
        .iter()
        .filter_map(|dispatch| dispatch.kernel_name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn region_matches(region: &CommandBufferRegion, filter_lower: Option<&str>) -> bool {
    filter_lower.is_none_or(|needle| {
        region
            .dispatches
            .iter()
            .any(|dispatch| dispatch_matches(region, dispatch, needle))
    })
}

fn matched_dispatch_count(region: &CommandBufferRegion, filter_lower: Option<&str>) -> usize {
    filter_lower.map_or(region.dispatches.len(), |needle| {
        region
            .dispatches
            .iter()
            .filter(|dispatch| dispatch_matches(region, dispatch, needle))
            .count()
    })
}

fn dispatch_matches(region: &CommandBufferRegion, dispatch: &DispatchCall, needle: &str) -> bool {
    dispatch
        .kernel_name
        .as_ref()
        .is_some_and(|name| name.to_ascii_lowercase().contains(needle))
        || resolve_encoder(region, dispatch).is_some_and(|encoder| {
            !encoder.label.is_empty() && encoder.label.to_ascii_lowercase().contains(needle)
        })
}

fn display_encoder(encoder: &ComputeEncoder) -> String {
    if encoder.label.is_empty() {
        format!("0x{:x}", encoder.address)
    } else {
        format!("{} (0x{:x})", encoder.label, encoder.address)
    }
}

fn display_buffer(buffer: &BoundBuffer) -> String {
    buffer
        .name
        .clone()
        .unwrap_or_else(|| format!("0x{:x}", buffer.address))
}

fn format_dims(value: [u32; 3]) -> String {
    format!("{}x{}x{}", value[0], value[1], value[2])
}

#[cfg(test)]
mod tests {
    use super::{
        filter_call_kind_report, filter_command_buffer_report, format_report,
        parse_initialization_calls, report_from_regions,
    };
    use crate::trace::{
        BoundBuffer, CommandBuffer, CommandBufferRegion, ComputeEncoder, DispatchCall,
        PipelineStateEvent,
    };

    #[test]
    fn synthesizes_encoder_pipeline_buffer_and_dispatch_calls_in_order() {
        let report = report_from_regions(&[sample_region()], None);
        let kinds: Vec<_> = report.calls.iter().map(|call| call.kind.as_str()).collect();
        assert_eq!(
            kinds,
            vec![
                "command_buffer",
                "encoder",
                "pipeline",
                "buffer_binding",
                "buffer_binding",
                "dispatch"
            ]
        );
        assert_eq!(report.command_buffers[0].call_count, 6);
        assert_eq!(report.calls[2].pipeline_addr, Some(0x1111));
        assert_eq!(report.calls[5].kernel_name.as_deref(), Some("copy_kernel"));
    }

    #[test]
    fn parses_go_style_initialization_calls_before_first_command_buffer() {
        let mut data = vec![0u8; 0x200];
        data[0x10..0x14].copy_from_slice(b"CUt\0");
        data[0x14..0x1c].copy_from_slice(&0x0afd_018000_u64.to_le_bytes());

        data[0x40..0x44].copy_from_slice(b"CU\0\0");
        data[0x64..0x6c].copy_from_slice(&0x106d_a56b0_u64.to_le_bytes());

        data[0x80..0x85].copy_from_slice(b"Culul");
        data[0x88..0x90].copy_from_slice(&0x106d_a56b0_u64.to_le_bytes());
        data[0x90..0x98].copy_from_slice(&16_u64.to_le_bytes());
        data[0xa4..0xac].copy_from_slice(&0x106d_a6190_u64.to_le_bytes());

        data[0xc0..0xc4].copy_from_slice(b"CS\0\0");
        data[0xc4..0xcc].copy_from_slice(&0x7000_000001_u64.to_le_bytes());
        data[0xcc..0xd7].copy_from_slice(b"gemm_kernel");
        data[0xd7] = 0;
        data[0xdc..0xe0].copy_from_slice(b"t\0\0\0");
        data[0xe0..0xe8].copy_from_slice(&0x1010_u64.to_le_bytes());

        data[0x100..0x104].copy_from_slice(b"Ctt\0");
        data[0x10c..0x114].copy_from_slice(&0x1010_u64.to_le_bytes());
        data[0x120..0x128].copy_from_slice(&0x2020_u64.to_le_bytes());

        data[0x140..0x144].copy_from_slice(b"Cui\0");
        data[0x14c..0x154].copy_from_slice(&0xafcc_88800_u64.to_le_bytes());

        data[0x180..0x184].copy_from_slice(b"CUUU");

        let calls = parse_initialization_calls(&data);
        let kinds = calls
            .iter()
            .map(|call| call.kind.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                "newResidencySet",
                "newHeap",
                "newBuffer",
                "bufferHeapOffset",
                "newFunction",
                "newPipelineState",
                "newSharedEvent"
            ]
        );
        assert_eq!(calls[4].label.as_deref(), Some("gemm_kernel"));
        assert!(calls[5].info.contains("gemm_kernel"));
    }

    #[test]
    fn filter_keeps_only_matching_command_buffers() {
        let mut non_matching = sample_region();
        non_matching.command_buffer.index = 9;
        non_matching.command_buffer.offset = 0x900;
        non_matching.dispatches[0].index = 1;
        non_matching.dispatches[0].kernel_name = Some("blur_kernel".into());

        let report = report_from_regions(&[sample_region(), non_matching], Some("copy"));
        assert_eq!(report.total_command_buffers, 1);
        assert_eq!(report.total_dispatches, 1);
        assert_eq!(report.matched_dispatches, 1);
        assert_eq!(report.command_buffers[0].index, 3);
    }

    #[test]
    fn falls_back_to_encoder_label_and_marks_report_as_synthetic() {
        let mut region = sample_region();
        region.dispatches[0].kernel_name = None;
        region.pipeline_events.clear();

        let report = report_from_regions(&[region], Some("main_encoder"));
        assert_eq!(report.total_command_buffers, 1);
        assert_eq!(
            report
                .calls
                .last()
                .and_then(|call| call.kernel_name.as_deref()),
            Some("main_encoder")
        );

        let rendered = format_report(&report);
        assert!(rendered.contains("Synthetic API-call report"));
        assert!(rendered.contains("honest approximation"));
        assert!(rendered.contains("dispatchThreadgroups"));
    }

    #[test]
    fn filters_rendered_report_by_command_buffer_or_call_kind() {
        let mut second = sample_region();
        second.command_buffer.index = 4;
        second.command_buffer.offset = 0x400;
        second.end_offset = 0x4ff;
        second.dispatches[0].kernel_name = Some("blur_kernel".into());

        let report = report_from_regions(&[sample_region(), second], None);
        let command_buffer = filter_command_buffer_report(&report, 4);
        assert_eq!(command_buffer.total_command_buffers, 1);
        assert_eq!(command_buffer.command_buffers[0].index, 4);
        assert!(command_buffer.calls.iter().all(|call| call.sequence < 6));

        let dispatches = filter_call_kind_report(&report, "dispatch");
        assert_eq!(dispatches.total_command_buffers, 2);
        assert_eq!(dispatches.total_calls, 2);
        assert!(dispatches.calls.iter().all(|call| call.kind == "dispatch"));
    }

    fn sample_region() -> CommandBufferRegion {
        CommandBufferRegion {
            command_buffer: CommandBuffer {
                index: 3,
                timestamp: 42,
                offset: 0x300,
            },
            end_offset: 0x3ff,
            encoders: vec![ComputeEncoder {
                index: 0,
                address: 0xe0,
                label: "main_encoder".into(),
                offset: 0x320,
            }],
            pipeline_events: vec![PipelineStateEvent {
                offset: 0x340,
                encoder_addr: 0xe0,
                pipeline_addr: 0x1111,
                function_addr: 0xaaaa,
                buffers: Vec::new(),
            }],
            dispatches: vec![DispatchCall {
                index: 0,
                offset: 0x350,
                encoder_id: Some(0xe0),
                pipeline_addr: None,
                kernel_name: Some("copy_kernel".into()),
                buffers: vec![
                    BoundBuffer {
                        address: 0xb0,
                        name: Some("input".into()),
                        index: 0,
                        usage: crate::trace::MTLResourceUsage::READ,
                    },
                    BoundBuffer {
                        address: 0xb1,
                        name: Some("output".into()),
                        index: 1,
                        usage: crate::trace::MTLResourceUsage::WRITE,
                    },
                ],
                grid_size: [64, 1, 1],
                group_size: [8, 1, 1],
            }],
        }
    }
}
