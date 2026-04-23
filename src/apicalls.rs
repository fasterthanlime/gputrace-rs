use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::error::Result;
use crate::trace::{
    BoundBuffer, CommandBufferRegion, ComputeEncoder, DispatchCall, PipelineStateEvent, TraceBundle,
};

#[derive(Debug, Clone, Serialize)]
pub struct ApiCallsReport {
    pub synthetic: bool,
    pub filter: Option<String>,
    pub total_command_buffers: usize,
    pub total_dispatches: usize,
    pub matched_dispatches: usize,
    pub total_calls: usize,
    pub command_buffers: Vec<ApiCallCommandBuffer>,
    pub calls: Vec<ApiCallEntry>,
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
    let regions = trace.command_buffer_regions()?;
    Ok(report_from_regions(&regions, filter))
}

pub fn format_report(report: &ApiCallsReport) -> String {
    let mut out = String::new();
    out.push_str("Synthetic API-call report\n");
    out.push_str(
        "Synthesized from command-buffer regions, encoder attribution, pipeline-state events, and dispatch records.\n",
    );
    out.push_str("This is an honest approximation of API intent, not a verbatim intercepted call stream.\n\n");
    if let Some(filter) = &report.filter {
        out.push_str(&format!(
            "filter={filter:?}, command_buffers={}, dispatches={} (matched={}), synthesized_calls={}\n\n",
            report.total_command_buffers,
            report.total_dispatches,
            report.matched_dispatches,
            report.total_calls
        ));
    } else {
        out.push_str(&format!(
            "command_buffers={}, dispatches={}, synthesized_calls={}\n\n",
            report.total_command_buffers, report.total_dispatches, report.total_calls
        ));
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

fn report_from_regions(regions: &[CommandBufferRegion], filter: Option<&str>) -> ApiCallsReport {
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
        total_command_buffers: command_buffers.len(),
        total_dispatches,
        matched_dispatches,
        total_calls: calls.len(),
        command_buffers,
        calls,
    }
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
    use super::{format_report, report_from_regions};
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
