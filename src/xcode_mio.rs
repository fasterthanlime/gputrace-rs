use std::path::PathBuf;

use serde::Serialize;

use crate::error::{Error, Result};
use crate::profiler;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioReport {
    pub trace_source: PathBuf,
    pub profiler_directory: PathBuf,
    pub stream_data_path: PathBuf,
    pub framework_path: PathBuf,
    pub gpu_command_count: usize,
    pub encoder_count: usize,
    pub pipeline_state_count: usize,
    pub draw_count: usize,
    pub cost_record_count: usize,
    pub gpu_time_ns: u64,
    pub cost_timeline: Option<XcodeMioCostTimeline>,
    pub timeline_binary_count: usize,
    pub timeline_binaries: Vec<XcodeMioTimelineBinary>,
    pub shader_binary_info: Vec<XcodeMioShaderBinaryInfo>,
    pub decoded_cost_records: Vec<XcodeMioDecodedCostRecord>,
    pub draw_timeline_records: Vec<XcodeMioDrawTimelineRecord>,
    pub draw_metadata_records: Vec<XcodeMioDrawMetadataRecord>,
    pub pipelines: Vec<XcodeMioPipeline>,
    pub gpu_commands: Vec<XcodeMioGpuCommand>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipeline {
    pub index: usize,
    pub object_id: u64,
    pub pointer_id: u64,
    pub function_index: u64,
    pub gpu_command_count: usize,
    pub timeline_draw_count: usize,
    pub timeline_duration_ns: u64,
    pub timeline_total_cost: f64,
    pub pipeline_address: Option<u64>,
    pub function_name: Option<String>,
    pub binary_keys: Vec<u64>,
    pub all_binary_keys: Vec<u64>,
    pub shader_stats: Vec<XcodeMioPipelineShaderStat>,
    pub scope_costs: Vec<XcodeMioPipelineScopeCost>,
    pub shader_tracks: Vec<XcodeMioPipelineShaderTrack>,
    pub shader_binaries: Vec<XcodeMioPipelineShaderBinary>,
    pub shader_binary_references: Vec<XcodeMioPipelineShaderBinaryReference>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioTimelineBinary {
    pub index: u64,
    pub address: u64,
    pub program_type: u16,
    pub instruction_info_count: u64,
    pub instruction_executed: u64,
    pub duration_ns: u64,
    pub trace_count: u64,
    pub cost_count: u64,
    pub total_cost: f64,
    pub total_instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderBinaryReference {
    pub raw5: u16,
    pub raw6: u16,
    pub raw1: u32,
    pub address: u64,
    pub timeline_binary_index: Option<u64>,
    pub record_count: usize,
    pub first_command_index: usize,
    pub last_command_index: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderBinary {
    pub pipeline_id_kind: &'static str,
    pub pipeline_id: u64,
    pub binary_index: u64,
    pub address: u64,
    pub program_type: u16,
    pub instruction_info_count: u64,
    pub instruction_executed: u64,
    pub duration_ns: u64,
    pub trace_count: u64,
    pub cost_count: u64,
    pub total_cost: f64,
    pub total_instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioShaderBinaryInfo {
    pub index: usize,
    pub raw0: u32,
    pub raw1: u32,
    pub raw2: u64,
    pub raw3: u64,
    pub raw4: u16,
    pub raw5: u16,
    pub raw6: u16,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderTrack {
    pub source: &'static str,
    pub pipeline_id_kind: &'static str,
    pub pipeline_id: u64,
    pub program_type: u16,
    pub track_id: i32,
    pub first_index: u64,
    pub start_timestamp_ns: u64,
    pub end_timestamp_ns: u64,
    pub duration_ns: u64,
    pub trace_count: u64,
    pub traces: Vec<XcodeMioBinaryTrace>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioBinaryTrace {
    pub index: usize,
    pub start_timestamp_ns: u64,
    pub end_timestamp_ns: u64,
    pub raw_identifier: u64,
    pub raw_index: u32,
    pub raw_count: u32,
    pub raw_program_type: u16,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineShaderStat {
    pub shader_id_kind: &'static str,
    pub shader_id: u64,
    pub program_type: u16,
    pub number_of_cliques: u64,
    pub total_gpu_cycles: u64,
    pub total_latency: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipelineScopeCost {
    pub scope: u16,
    pub identifier_kind: &'static str,
    pub identifier: u64,
    pub level: u16,
    pub level_identifier: u32,
    pub alu_cost: f64,
    pub non_alu_cost: f64,
    pub total_cost: f64,
    pub instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioDecodedCostRecord {
    pub index: usize,
    pub level: u16,
    pub scope: u16,
    pub level_identifier: u32,
    pub scope_identifier: u64,
    pub alu_cost: f64,
    pub non_alu_cost: f64,
    pub total_cost: f64,
    pub instruction_count: u64,
    pub threads_executing_instruction: u64,
    pub cpi_weighted_instruction_count: u64,
    pub active_thread_instruction_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioDrawTimelineRecord {
    pub index: usize,
    pub raw0: u64,
    pub raw1: u64,
    pub raw2: u32,
    pub raw3: u16,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioDrawMetadataRecord {
    pub index: usize,
    pub raw0: u32,
    pub raw1: u32,
    pub raw2: u32,
    pub raw3: u32,
    pub raw4: i32,
    pub raw5: u32,
    pub raw6: u64,
    pub raw7: u32,
    pub raw8: u32,
    pub raw9: u32,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioGpuCommand {
    pub index: usize,
    pub function_index: u64,
    pub sub_command_index: i32,
    pub encoder_index: usize,
    pub pipeline_index: usize,
    pub pipeline_object_id: u64,
    pub command_buffer_index: usize,
    pub function_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioCostTimeline {
    pub draw_count: usize,
    pub pipeline_state_count: usize,
    pub cost_record_count: usize,
    pub gpu_time_ns: u64,
    pub global_gpu_time_ns: u64,
    pub timeline_duration_ns: u64,
    pub total_clique_cost: u64,
}

pub fn report(trace: &TraceBundle) -> Result<XcodeMioReport> {
    let profiler_directory = profiler::find_profiler_directory(&trace.path)
        .ok_or_else(|| Error::NotFound(trace.path.clone()))?;
    let stream_data_path = profiler_directory.join("streamData");
    if !stream_data_path.is_file() {
        return Err(Error::NotFound(stream_data_path));
    }

    let profiler_summary = profiler::stream_data_summary(&trace.path).ok();
    platform::decode(
        trace.path.clone(),
        profiler_directory,
        stream_data_path,
        profiler_summary.as_ref(),
    )
}

pub fn format_report(report: &XcodeMioReport) -> String {
    let mut out = String::new();
    out.push_str("Xcode private MIO decode\n");
    out.push_str(&format!(
        "streamData: {}\nframework: {}\n\n",
        report.stream_data_path.display(),
        report.framework_path.display()
    ));
    out.push_str(&format!(
        "gpu_commands={} encoders={} pipelines={} draws={} cost_records={} gpu_time={:.3} ms\n\n",
        report.gpu_command_count,
        report.encoder_count,
        report.pipeline_state_count,
        report.draw_count,
        report.cost_record_count,
        report.gpu_time_ns as f64 / 1_000_000.0
    ));
    if let Some(timeline) = &report.cost_timeline {
        out.push_str(&format!(
            "cost_timeline: draws={} pipelines={} cost_records={} global_gpu_time={:.3} ms total_clique_cost={}\n",
            timeline.draw_count,
            timeline.pipeline_state_count,
            timeline.cost_record_count,
            timeline.global_gpu_time_ns as f64 / 1_000_000.0,
            timeline.total_clique_cost
        ));
        out.push('\n');
    } else {
        out.push('\n');
    }
    if report.timeline_binary_count > 0 || !report.shader_binary_info.is_empty() {
        out.push_str(&format!(
            "timeline_binaries={} shader_binary_info={}\n\n",
            report
                .timeline_binaries
                .len()
                .max(report.timeline_binary_count),
            report.shader_binary_info.len()
        ));
    }
    let mut binary_references = report
        .pipelines
        .iter()
        .flat_map(|pipeline| {
            let mut groups = std::collections::BTreeMap::<(u16, u16, u32), (usize, usize)>::new();
            for reference in pipeline
                .shader_binary_references
                .iter()
                .filter(|reference| reference.raw5 == 6 && reference.raw6 == 28)
            {
                let entry = groups
                    .entry((reference.raw5, reference.raw6, reference.raw1))
                    .or_insert((0, 0));
                entry.0 += reference.record_count;
                entry.1 += 1;
            }
            groups
                .into_iter()
                .map(move |((raw5, raw6, raw1), (record_count, binary_count))| {
                    (pipeline, raw5, raw6, raw1, record_count, binary_count)
                })
        })
        .collect::<Vec<_>>();
    binary_references.sort_by(
        |(left_pipeline, _, _, left_raw1, left_count, _),
         (right_pipeline, _, _, right_raw1, right_count, _)| {
            right_count
                .cmp(left_count)
                .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
                .then_with(|| left_raw1.cmp(right_raw1))
        },
    );
    if !binary_references.is_empty() {
        out.push_str("Pipelines by shader-binary references:\n");
        for (pipeline, raw5, raw6, raw1, record_count, binary_count) in
            binary_references.iter().take(20)
        {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            out.push_str(&format!(
                "  {:>4} refs  {:>4} binaries  raw1={:<5} kind={}/{}  {:<56}\n",
                record_count, binary_count, raw1, raw5, raw6, name,
            ));
        }
        out.push('\n');
    }
    let mut shader_tracks = report
        .pipelines
        .iter()
        .flat_map(|pipeline| {
            pipeline
                .shader_tracks
                .iter()
                .map(move |track| (pipeline, track))
        })
        .filter(|(_, track)| track.duration_ns > 0 || track.trace_count > 0)
        .collect::<Vec<_>>();
    shader_tracks.sort_by(|(left_pipeline, left), (right_pipeline, right)| {
        right
            .duration_ns
            .cmp(&left.duration_ns)
            .then_with(|| right.trace_count.cmp(&left.trace_count))
            .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
            .then_with(|| left.program_type.cmp(&right.program_type))
    });
    if !shader_tracks.is_empty() {
        out.push_str("Pipelines by Xcode shader track duration:\n");
        let denominator = report
            .cost_timeline
            .as_ref()
            .map(|timeline| timeline.global_gpu_time_ns)
            .filter(|value| *value > 0)
            .unwrap_or(report.gpu_time_ns);
        for (pipeline, track) in shader_tracks.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let pct = if denominator > 0 {
                100.0 * track.duration_ns as f64 / denominator as f64
            } else {
                0.0
            };
            out.push_str(&format!(
                "  {:>6.2}% {:>9.3} ms  {:>4} traces  ptype={:<2} {:<56} id={}:{}\n",
                pct,
                track.duration_ns as f64 / 1_000_000.0,
                track.trace_count,
                track.program_type,
                name,
                track.pipeline_id_kind,
                track.pipeline_id,
            ));
        }
        out.push('\n');
    }
    let mut shader_binaries = report
        .pipelines
        .iter()
        .flat_map(|pipeline| {
            pipeline
                .shader_binaries
                .iter()
                .map(move |binary| (pipeline, binary))
        })
        .filter(|(_, binary)| {
            binary.duration_ns > 0 || binary.total_cost > 0.0 || binary.trace_count > 0
        })
        .collect::<Vec<_>>();
    shader_binaries.sort_by(|(left_pipeline, left), (right_pipeline, right)| {
        right
            .duration_ns
            .cmp(&left.duration_ns)
            .then_with(|| {
                right
                    .total_cost
                    .partial_cmp(&left.total_cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left_pipeline.index.cmp(&right_pipeline.index))
    });
    if !shader_binaries.is_empty() {
        out.push_str("Pipelines by Xcode shader binary duration:\n");
        let denominator = report
            .cost_timeline
            .as_ref()
            .map(|timeline| timeline.global_gpu_time_ns)
            .filter(|value| *value > 0)
            .unwrap_or(report.gpu_time_ns);
        for (pipeline, binary) in shader_binaries.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let pct = if denominator > 0 {
                100.0 * binary.duration_ns as f64 / denominator as f64
            } else {
                0.0
            };
            out.push_str(&format!(
                "  {:>6.2}% {:>9.3} ms  cost={:>10.3} traces={:>4} ptype={:<2} {:<56} binary={} addr=0x{:x}\n",
                pct,
                binary.duration_ns as f64 / 1_000_000.0,
                binary.total_cost,
                binary.trace_count,
                binary.program_type,
                name,
                binary.binary_index,
                binary.address,
            ));
        }
        out.push('\n');
    }
    let mut draw_timeline = report
        .pipelines
        .iter()
        .filter(|pipeline| pipeline.timeline_duration_ns > 0 || pipeline.timeline_draw_count > 0)
        .collect::<Vec<_>>();
    draw_timeline.sort_by(|left, right| {
        right
            .timeline_total_cost
            .partial_cmp(&left.timeline_total_cost)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.index.cmp(&right.index))
    });
    let draw_cost_denominator = report
        .pipelines
        .iter()
        .map(|pipeline| pipeline.timeline_total_cost)
        .filter(|value| value.is_finite() && *value > 0.0)
        .sum::<f64>();
    if !draw_timeline.is_empty() && draw_cost_denominator > 0.0 {
        out.push_str("Pipelines by Xcode draw-scope cost:\n");
        for pipeline in draw_timeline.iter().take(20) {
            let name = pipeline
                .function_name
                .as_deref()
                .unwrap_or("<unknown function>");
            let pct = 100.0 * pipeline.timeline_total_cost / draw_cost_denominator;
            out.push_str(&format!(
                "  {:>6.2}% cost={:>10.3} {:>4} draws  {:<56}\n",
                pct, pipeline.timeline_total_cost, pipeline.timeline_draw_count, name,
            ));
        }
        out.push('\n');
    }
    out.push_str("Pipelines by command count:\n");
    let mut pipelines = report.pipelines.clone();
    pipelines.sort_by(|left, right| {
        right
            .gpu_command_count
            .cmp(&left.gpu_command_count)
            .then_with(|| left.index.cmp(&right.index))
    });
    for pipeline in pipelines {
        let name = pipeline
            .function_name
            .as_deref()
            .unwrap_or("<unknown function>");
        let address = pipeline
            .pipeline_address
            .map(|value| format!("0x{value:x}"))
            .unwrap_or_else(|| "-".to_owned());
        out.push_str(&format!(
            "  {:>2} {:>4} commands  {:<56} addr={} object_id={}\n",
            pipeline.index, pipeline.gpu_command_count, name, address, pipeline.object_id
        ));
    }

    if !report.warnings.is_empty() {
        out.push_str("\nWarnings:\n");
        for warning in &report.warnings {
            out.push_str(&format!("  - {warning}\n"));
        }
    }

    out
}

#[cfg(target_os = "macos")]
mod platform {
    use std::collections::{BTreeMap, BTreeSet};
    use std::ffi::{CString, c_char, c_int, c_void};
    use std::mem;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::{
        XcodeMioBinaryTrace, XcodeMioCostTimeline, XcodeMioDecodedCostRecord,
        XcodeMioDrawMetadataRecord, XcodeMioDrawTimelineRecord, XcodeMioGpuCommand,
        XcodeMioPipeline, XcodeMioPipelineScopeCost, XcodeMioPipelineShaderBinary,
        XcodeMioPipelineShaderBinaryReference, XcodeMioPipelineShaderStat,
        XcodeMioPipelineShaderTrack, XcodeMioReport, XcodeMioShaderBinaryInfo,
        XcodeMioTimelineBinary,
    };
    use crate::error::{Error, Result};
    use crate::profiler;
    use block2::RcBlock;

    type Id = *mut c_void;
    type Class = *mut c_void;
    type Sel = *mut c_void;
    type CfTypeRef = *const c_void;
    type CfStringRef = *const c_void;

    const RTLD_NOW: c_int = 0x2;
    const RTLD_GLOBAL: c_int = 0x8;
    const K_CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
    const GT_SHADER_PROFILER_FRAMEWORK: &str = "/Applications/Xcode.app/Contents/PlugIns/GPUDebugger.ideplugin/Contents/Frameworks/GTShaderProfiler.framework/Versions/A/GTShaderProfiler";
    const MTL_TOOLS_SHADER_PROFILER_FRAMEWORK: &str = "/Applications/Xcode.app/Contents/SharedFrameworks/MTLToolsShaderProfiler.framework/Versions/A/MTLToolsShaderProfiler";

    unsafe extern "C" {
        fn dlopen(path: *const c_char, mode: c_int) -> *mut c_void;
        fn open(path: *const c_char, oflag: c_int, ...) -> c_int;
        fn dup(fd: c_int) -> c_int;
        fn dup2(src: c_int, dst: c_int) -> c_int;
        fn close(fd: c_int) -> c_int;
        fn objc_lookUpClass(name: *const c_char) -> Class;
        fn sel_registerName(name: *const c_char) -> Sel;
        fn objc_msgSend();
        fn CFStringCreateWithCString(
            alloc: CfTypeRef,
            c_str: *const c_char,
            encoding: u32,
        ) -> CfStringRef;
        fn CFRelease(cf: CfTypeRef);
    }

    #[link(name = "Foundation", kind = "framework")]
    unsafe extern "C" {}

    #[link(name = "objc")]
    unsafe extern "C" {}

    pub fn decode(
        trace_source: PathBuf,
        profiler_directory: PathBuf,
        stream_data_path: PathBuf,
        profiler_summary: Option<&profiler::ProfilerStreamDataSummary>,
    ) -> Result<XcodeMioReport> {
        let framework_path = PathBuf::from(GT_SHADER_PROFILER_FRAMEWORK);
        let silence = FdSilencer::new();
        let mut runtime = unsafe { Runtime::load()? };
        let stream = runtime.stream_data(&stream_data_path)?;
        let processor = unsafe { runtime.processor(stream)? };
        unsafe {
            runtime.send_void(processor, "processStreamData")?;
            runtime.send_void(processor, "waitUntilFinished")?;
        }
        let mio = unsafe { runtime.send_id(processor, "mioData")? };
        let result = unsafe { runtime.send_id(processor, "result")? };
        let shader_result = unsafe { runtime.send_id(result, "shaderProfilerResult")? };
        let gpu_commands = unsafe { runtime.send_id(shader_result, "gpuCommands")? };
        let pipelines = unsafe { runtime.send_id(shader_result, "pipelineStates")? };
        let encoders = unsafe { runtime.send_id(shader_result, "encoders")? };

        let pipeline_count = unsafe { runtime.array_count(pipelines)? };
        let command_count = unsafe { runtime.array_count(gpu_commands)? };
        let encoder_count = unsafe { runtime.array_count(encoders)? };
        let mut decoded_pipelines = Vec::with_capacity(pipeline_count);
        for index in 0..pipeline_count {
            let pipeline = unsafe { runtime.array_object(pipelines, index)? };
            let summary_pipeline =
                profiler_summary.and_then(|summary| summary.pipelines.get(index));
            let binary_keys = unsafe {
                runtime
                    .send_id_allow_nil(pipeline, "binaryKeys")
                    .ok()
                    .filter(|array| !array.is_null())
                    .map(|array| runtime.u64_array(array))
                    .transpose()?
                    .unwrap_or_default()
            };
            let all_binary_keys = unsafe {
                runtime
                    .send_id_allow_nil(pipeline, "allBinaryKeys")
                    .ok()
                    .filter(|array| !array.is_null())
                    .map(|array| runtime.u64_array(array))
                    .transpose()?
                    .unwrap_or_default()
            };
            decoded_pipelines.push(XcodeMioPipeline {
                index: unsafe { runtime.send_u32(pipeline, "index")? as usize },
                object_id: unsafe { runtime.send_u64(pipeline, "objectId")? },
                pointer_id: unsafe { runtime.send_u64(pipeline, "pointerId")? },
                function_index: unsafe { runtime.send_u64(pipeline, "functionIndex")? },
                gpu_command_count: unsafe {
                    runtime.send_u32(pipeline, "numGPUCommands")? as usize
                },
                timeline_draw_count: 0,
                timeline_duration_ns: 0,
                timeline_total_cost: 0.0,
                pipeline_address: summary_pipeline.map(|pipeline| pipeline.pipeline_address),
                function_name: summary_pipeline.and_then(|pipeline| pipeline.function_name.clone()),
                binary_keys,
                all_binary_keys,
                shader_stats: Vec::new(),
                scope_costs: Vec::new(),
                shader_tracks: Vec::new(),
                shader_binaries: Vec::new(),
                shader_binary_references: Vec::new(),
            });
        }

        let mut decoded_commands = Vec::with_capacity(command_count);
        for index in 0..command_count {
            let command = unsafe { runtime.array_object(gpu_commands, index)? };
            let pipeline_index =
                unsafe { runtime.send_u32(command, "pipelineInfoIndex")? as usize };
            let function_name = profiler_summary
                .and_then(|summary| summary.pipelines.get(pipeline_index))
                .and_then(|pipeline| pipeline.function_name.clone());
            decoded_commands.push(XcodeMioGpuCommand {
                index: unsafe { runtime.send_u32(command, "index")? as usize },
                function_index: unsafe { runtime.send_u64(command, "functionIndex")? },
                sub_command_index: unsafe { runtime.send_i32(command, "subCommandIndex")? },
                encoder_index: unsafe { runtime.send_u32(command, "encoderInfoIndex")? as usize },
                pipeline_index,
                pipeline_object_id: unsafe { runtime.send_u64(command, "pipelineStateObjectId")? },
                command_buffer_index: unsafe {
                    runtime.send_u32(command, "commandBufferIndex")? as usize
                },
                function_name,
            });
        }

        let mut warnings = Vec::new();
        let (cost_timeline, cost_timeline_object) =
            match unsafe { runtime.request_cost_timeline(mio) } {
                Ok(Some(timeline)) => {
                    let object = timeline.object;
                    (Some(timeline.summary), Some(object))
                }
                Ok(None) => (None, None),
                Err(error) => {
                    warnings.push(format!("private cost timeline callback failed: {error}"));
                    (None, None)
                }
            };
        let mut decoded_cost_records = Vec::new();
        let mut draw_timeline_records = Vec::new();
        let mut draw_metadata_records = Vec::new();
        let mut timeline_binary_count = 0;
        let mut timeline_binaries = Vec::new();
        let mut shader_binary_info = Vec::new();
        if let Some(timeline) = cost_timeline_object {
            timeline_binary_count = unsafe { runtime.timeline_binary_count(timeline) };
            timeline_binaries = unsafe { runtime.decode_timeline_binaries(timeline) };
            shader_binary_info = unsafe { runtime.decode_shader_binary_info(timeline) };
            attach_shader_binary_references(
                &shader_binary_info,
                &timeline_binaries,
                &decoded_commands,
                &mut decoded_pipelines,
            );
            decoded_cost_records = unsafe { runtime.decode_timeline_cost_records(timeline) };
            draw_timeline_records = unsafe { runtime.decode_draw_timeline_records(timeline) };
            draw_metadata_records = unsafe { runtime.decode_draw_metadata_records(timeline) };
            unsafe {
                runtime.decode_pipeline_draw_timeline(
                    timeline,
                    &draw_timeline_records,
                    &draw_metadata_records,
                    &decoded_commands,
                    &mut decoded_pipelines,
                )
            };
            if let Err(error) = unsafe {
                runtime.decode_pipeline_private_costs(
                    mio,
                    timeline,
                    &decoded_cost_records,
                    &mut decoded_pipelines,
                )
            } {
                warnings.push(format!("private per-pipeline cost probe failed: {error}"));
            }
        }
        if profiler_summary.is_some_and(|summary| summary.num_gpu_commands != command_count) {
            warnings.push("private MIO command count differs from streamData summary".to_owned());
        }
        if decoded_pipelines
            .iter()
            .any(|pipeline| pipeline.function_name.is_none())
        {
            warnings.push("one or more private MIO pipelines could not be named".to_owned());
        }

        let report = XcodeMioReport {
            trace_source,
            profiler_directory,
            stream_data_path,
            framework_path,
            gpu_command_count: command_count,
            encoder_count,
            pipeline_state_count: pipeline_count,
            draw_count: unsafe { runtime.send_u64(mio, "drawCount")? as usize },
            cost_record_count: unsafe { runtime.send_u64(mio, "costCount")? as usize },
            gpu_time_ns: unsafe { runtime.send_u64(mio, "gpuTime")? },
            cost_timeline,
            timeline_binary_count,
            timeline_binaries,
            shader_binary_info,
            decoded_cost_records,
            draw_timeline_records,
            draw_metadata_records,
            pipelines: decoded_pipelines,
            gpu_commands: decoded_commands,
            warnings,
        };
        drop(runtime);
        std::thread::sleep(std::time::Duration::from_millis(100));
        drop(silence);
        Ok(report)
    }

    struct Runtime {
        pool: Id,
    }

    impl Runtime {
        unsafe fn load() -> Result<Self> {
            unsafe {
                let _ = load_framework(MTL_TOOLS_SHADER_PROFILER_FRAMEWORK);
                load_framework(GT_SHADER_PROFILER_FRAMEWORK)?;
                let pool_class = lookup_class("NSAutoreleasePool")?;
                let pool = send_id(send_id(pool_class, "alloc")?, "init")?;
                Ok(Self { pool })
            }
        }

        fn stream_data(&mut self, stream_data_path: &std::path::Path) -> Result<Id> {
            let path = stream_data_path.to_string_lossy();
            let path = CString::new(path.as_bytes())
                .map_err(|_| Error::InvalidInput("streamData path contains NUL".to_owned()))?;
            unsafe {
                let path = CfString::new(path.as_ptr())?;
                let url_class = lookup_class("NSURL")?;
                let url = send_id_id(url_class, "fileURLWithPath:", path.ptr.cast_mut())?;
                let stream_class = lookup_class("GTShaderProfilerStreamData")?;
                send_id_id(stream_class, "dataFromArchivedDataURL:", url)
            }
        }

        unsafe fn processor(&mut self, stream: Id) -> Result<Id> {
            unsafe {
                let processor_class = lookup_class("GTShaderProfilerStreamDataProcessor")?;
                let processor = send_id(processor_class, "alloc")?;
                send_id_id_id(
                    processor,
                    "initWithStreamData:llvmHelperPath:",
                    stream,
                    std::ptr::null_mut(),
                )
            }
        }

        unsafe fn send_id(&mut self, receiver: Id, selector: &str) -> Result<Id> {
            unsafe { send_id(receiver, selector) }
        }

        unsafe fn send_id_allow_nil(&mut self, receiver: Id, selector: &str) -> Result<Id> {
            unsafe { send_id_allow_nil(receiver, selector) }
        }

        unsafe fn send_void(&mut self, receiver: Id, selector: &str) -> Result<()> {
            unsafe { send_void(receiver, selector) }
        }

        unsafe fn send_u64(&mut self, receiver: Id, selector: &str) -> Result<u64> {
            unsafe { send_u64(receiver, selector) }
        }

        unsafe fn send_u32(&mut self, receiver: Id, selector: &str) -> Result<u32> {
            unsafe { send_u32(receiver, selector) }
        }

        unsafe fn send_i32(&mut self, receiver: Id, selector: &str) -> Result<i32> {
            unsafe { send_i32(receiver, selector) }
        }

        unsafe fn array_count(&mut self, array: Id) -> Result<usize> {
            unsafe { Ok(send_u64(array, "count")? as usize) }
        }

        unsafe fn array_object(&mut self, array: Id, index: usize) -> Result<Id> {
            unsafe { send_id_usize(array, "objectAtIndex:", index) }
        }

        unsafe fn u64_array(&mut self, array: Id) -> Result<Vec<u64>> {
            let count = unsafe { self.array_count(array)? };
            let mut values = Vec::with_capacity(count);
            for index in 0..count {
                let value = unsafe { self.array_object(array, index)? };
                values.push(unsafe { send_u64(value, "unsignedLongLongValue")? });
            }
            Ok(values)
        }

        unsafe fn request_cost_timeline(&mut self, mio: Id) -> Result<Option<DecodedTimeline>> {
            let slot = Arc::new(Mutex::new(None::<usize>));
            let callback_slot = Arc::clone(&slot);
            let block = RcBlock::new(move |timeline: Id| {
                if !timeline.is_null()
                    && let Ok(mut slot) = callback_slot.lock()
                {
                    *slot = Some(timeline as usize);
                }
            });
            let block_ptr = RcBlock::as_ptr(&block).cast::<c_void>();
            unsafe {
                send_id_id_allow_nil(mio, "requestCostTimeline:", block_ptr.cast())?;
            }

            let deadline = Instant::now() + Duration::from_secs(20);
            let mut timeline = None;
            while Instant::now() < deadline {
                timeline = *slot
                    .lock()
                    .map_err(|_| Error::InvalidInput("cost timeline lock poisoned".to_owned()))?;
                if timeline.is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            let Some(timeline) = timeline.map(|value| value as Id) else {
                return Ok(None);
            };

            Ok(Some(DecodedTimeline {
                object: timeline,
                summary: XcodeMioCostTimeline {
                    draw_count: unsafe { send_u64(timeline, "drawCount")? as usize },
                    pipeline_state_count: unsafe {
                        send_u64(timeline, "pipelineStateCount")? as usize
                    },
                    cost_record_count: unsafe { send_u64(timeline, "costCount")? as usize },
                    gpu_time_ns: unsafe { send_u64(timeline, "gpuTime")? },
                    global_gpu_time_ns: unsafe { send_u64(timeline, "globalGPUTime")? },
                    timeline_duration_ns: unsafe { send_u64(timeline, "timelineDuration")? },
                    total_clique_cost: unsafe { send_u64(timeline, "totalCliqueCost")? },
                },
            }))
        }

        unsafe fn decode_pipeline_private_costs(
            &mut self,
            mio: Id,
            timeline: Id,
            cost_records: &[XcodeMioDecodedCostRecord],
            pipelines: &mut [XcodeMioPipeline],
        ) -> Result<()> {
            unsafe {
                self.decode_pipeline_shader_stats(timeline, pipelines)?;
                self.decode_pipeline_scope_costs(cost_records, pipelines);
                self.decode_pipeline_shader_tracks(
                    timeline,
                    pipelines,
                    "timeline_pipeline_state_program_type",
                )?;
                if !pipelines
                    .iter()
                    .any(|pipeline| !pipeline.shader_tracks.is_empty())
                {
                    self.decode_pipeline_shader_tracks(
                        mio,
                        pipelines,
                        "mio_pipeline_state_program_type",
                    )?;
                }
                self.decode_pipeline_shader_binaries(timeline, pipelines)?;
            }
            Ok(())
        }

        unsafe fn decode_timeline_cost_records(
            &mut self,
            timeline: Id,
        ) -> Vec<XcodeMioDecodedCostRecord> {
            let Ok(cost_count) = (unsafe { send_u64(timeline, "costCount") }) else {
                return Vec::new();
            };
            let Ok(costs) = (unsafe { send_ptr(timeline, "costs") }) else {
                return Vec::new();
            };
            if costs.is_null() {
                return Vec::new();
            }
            let costs = costs.cast::<RawGtmioCostInfo>();
            let mut records = Vec::with_capacity(cost_count as usize);
            for index in 0..cost_count as usize {
                let raw = unsafe { *costs.add(index) };
                let mut populated = RawGtmioCostInfo::default();
                let cost = unsafe {
                    send_i8_u16_u64_cost_mut(
                        timeline,
                        "costForScope:scopeIdentifier:cost:",
                        raw.context.scope,
                        raw.context.scope_identifier,
                        &mut populated,
                    )
                }
                .ok()
                .filter(|found| *found != 0)
                .filter(|_| !populated.is_empty())
                .map(|_| populated)
                .unwrap_or_else(|| {
                    let mut populated = RawGtmioCostInfo::default();
                    let by_context = unsafe {
                        send_i8_context_cost_mut(
                            timeline,
                            "costForContext:cost:",
                            &raw.context,
                            &mut populated,
                        )
                    };
                    by_context
                        .ok()
                        .filter(|found| *found != 0)
                        .filter(|_| !populated.is_empty())
                        .map(|_| populated)
                        .unwrap_or_else(|| {
                            let mut populated = RawGtmioCostInfo::default();
                            let by_level = unsafe {
                                send_i8_u16_u32_u16_u64_cost_mut(
                                    timeline,
                                    "costForLevel:levelIdentifier:scope:scopeIdentifier:cost:",
                                    raw.context.level,
                                    raw.context.level_identifier,
                                    raw.context.scope,
                                    raw.context.scope_identifier,
                                    &mut populated,
                                )
                            };
                            by_level
                                .ok()
                                .filter(|found| *found != 0)
                                .filter(|_| !populated.is_empty())
                                .map(|_| populated)
                                .unwrap_or(raw)
                        })
                });
                records.push(XcodeMioDecodedCostRecord {
                    index,
                    level: cost.context.level,
                    scope: cost.context.scope,
                    level_identifier: cost.context.level_identifier,
                    scope_identifier: cost.context.scope_identifier,
                    alu_cost: cost.alu_cost,
                    non_alu_cost: cost.non_alu_cost,
                    total_cost: cost.alu_cost + cost.non_alu_cost,
                    instruction_count: cost.instruction_count,
                    threads_executing_instruction: cost.threads_executing_instruction,
                    cpi_weighted_instruction_count: cost.cpi_weighted_instruction_count,
                    active_thread_instruction_count: cost.active_thread_instruction_count,
                });
            }
            records
        }

        unsafe fn timeline_binary_count(&mut self, timeline: Id) -> usize {
            unsafe {
                let Ok(binaries) = send_id(timeline, "binaries") else {
                    return 0;
                };
                send_u64(binaries, "count").unwrap_or(0) as usize
            }
        }

        unsafe fn decode_timeline_binaries(&mut self, timeline: Id) -> Vec<XcodeMioTimelineBinary> {
            unsafe {
                let Ok(binaries) = send_id(timeline, "binaries") else {
                    return Vec::new();
                };
                let binary_count = send_u64(binaries, "count").unwrap_or(0);
                let mut decoded = Vec::with_capacity(binary_count as usize);
                for index in 0..binary_count as usize {
                    let Ok(binary) = send_id_usize(binaries, "objectAtIndex:", index) else {
                        continue;
                    };
                    decoded.push(decode_timeline_binary(binary, index as u64));
                }
                decoded
            }
        }

        unsafe fn decode_shader_binary_info(
            &mut self,
            timeline: Id,
        ) -> Vec<XcodeMioShaderBinaryInfo> {
            let Ok(info_count) = (unsafe { send_u64(timeline, "shaderBinaryInfoCount") }) else {
                return Vec::new();
            };
            let Ok(info) = (unsafe { send_ptr(timeline, "shaderBinaryInfo") }) else {
                return Vec::new();
            };
            if info.is_null() {
                return Vec::new();
            }
            let info = info.cast::<u8>();
            let mut records = Vec::with_capacity(info_count as usize);
            for index in 0..info_count as usize {
                let raw = unsafe { std::slice::from_raw_parts(info.add(index * 30), 30) };
                records.push(XcodeMioShaderBinaryInfo {
                    index,
                    raw0: read_u32(raw, 0),
                    raw1: read_u32(raw, 4),
                    raw2: read_u64(raw, 8),
                    raw3: read_u64(raw, 16),
                    raw4: read_u16(raw, 24),
                    raw5: read_u16(raw, 26),
                    raw6: read_u16(raw, 28),
                });
            }
            records
        }

        unsafe fn decode_draw_timeline_records(
            &mut self,
            timeline: Id,
        ) -> Vec<XcodeMioDrawTimelineRecord> {
            let Ok(draw_count) = (unsafe { send_u64(timeline, "drawCount") }) else {
                return Vec::new();
            };
            let Ok(draws) = (unsafe { send_ptr(timeline, "drawTraces") }) else {
                return Vec::new();
            };
            if draws.is_null() {
                return Vec::new();
            }
            let draws = draws.cast::<RawGtmioDrawTrace>();
            let mut records = Vec::with_capacity(draw_count as usize);
            for index in 0..draw_count as usize {
                let draw = unsafe { *draws.add(index) };
                records.push(XcodeMioDrawTimelineRecord {
                    index,
                    raw0: draw.raw0,
                    raw1: draw.raw1,
                    raw2: draw.raw2,
                    raw3: draw.raw3,
                });
            }
            records
        }

        unsafe fn decode_draw_metadata_records(
            &mut self,
            timeline: Id,
        ) -> Vec<XcodeMioDrawMetadataRecord> {
            let Ok(draw_count) = (unsafe { send_u64(timeline, "drawCount") }) else {
                return Vec::new();
            };
            let Ok(draws) = (unsafe { send_ptr(timeline, "draws") }) else {
                return Vec::new();
            };
            if draws.is_null() {
                return Vec::new();
            }
            let draws = draws.cast::<u8>();
            let mut records = Vec::with_capacity(draw_count as usize);
            for index in 0..draw_count as usize {
                let draw = unsafe { std::slice::from_raw_parts(draws.add(index * 44), 44) };
                records.push(XcodeMioDrawMetadataRecord {
                    index,
                    raw0: read_u32(draw, 0),
                    raw1: read_u32(draw, 4),
                    raw2: read_u32(draw, 8),
                    raw3: read_u32(draw, 12),
                    raw4: read_i32(draw, 16),
                    raw5: read_u32(draw, 20),
                    raw6: read_u64(draw, 24),
                    raw7: read_u32(draw, 32),
                    raw8: read_u32(draw, 36),
                    raw9: read_u32(draw, 40),
                });
            }
            records
        }

        unsafe fn decode_pipeline_draw_timeline(
            &mut self,
            timeline: Id,
            draw_records: &[XcodeMioDrawTimelineRecord],
            draw_metadata: &[XcodeMioDrawMetadataRecord],
            gpu_commands: &[XcodeMioGpuCommand],
            pipelines: &mut [XcodeMioPipeline],
        ) {
            for draw in draw_metadata {
                let command_index = draw.raw0 as usize;
                let Some(command) = gpu_commands.get(command_index) else {
                    continue;
                };
                let Some(pipeline) = pipelines.get_mut(command.pipeline_index) else {
                    continue;
                };
                pipeline.timeline_draw_count += 1;
                if let Some(trace) = draw_records.get(draw.index) {
                    pipeline.timeline_duration_ns += trace.raw1.saturating_sub(trace.raw0);
                }
                pipeline.timeline_total_cost +=
                    unsafe { draw_scope_cost(timeline, draw.raw0 as u64, draw.raw7) };
            }
        }

        unsafe fn decode_pipeline_shader_stats(
            &mut self,
            timeline: Id,
            pipelines: &mut [XcodeMioPipeline],
        ) -> Result<()> {
            unsafe {
                let stats_class = lookup_class("GTMioTraceDataStats")?;
                let stats = send_id(stats_class, "alloc")?;
                let stats = send_id_id(stats, "initWithTraceData:", timeline)?;
                send_void(stats, "build")?;

                for pipeline in pipelines {
                    let candidates = [
                        ("object_id", pipeline.object_id),
                        ("pointer_id", pipeline.pointer_id),
                        ("function_index", pipeline.function_index),
                    ];
                    for (shader_id_kind, shader_id) in candidates {
                        for program_type in 0..=16 {
                            let stat = send_id_u64_u16_allow_nil(
                                stats,
                                "shaderStatForShader:programType:",
                                shader_id,
                                program_type,
                            )?;
                            if stat.is_null() {
                                continue;
                            }
                            let number_of_cliques = send_u64(stat, "numberOfCliques")?;
                            let total_gpu_cycles = send_u64(stat, "totalGPUCycles")?;
                            let total_latency = send_u64(stat, "totalLatency")?;
                            if number_of_cliques == 0 && total_gpu_cycles == 0 && total_latency == 0
                            {
                                continue;
                            }
                            pipeline.shader_stats.push(XcodeMioPipelineShaderStat {
                                shader_id_kind,
                                shader_id,
                                program_type,
                                number_of_cliques,
                                total_gpu_cycles,
                                total_latency,
                            });
                        }
                    }
                }
            }
            Ok(())
        }

        fn decode_pipeline_scope_costs(
            &mut self,
            cost_records: &[XcodeMioDecodedCostRecord],
            pipelines: &mut [XcodeMioPipeline],
        ) {
            for pipeline in pipelines {
                let identifiers = [
                    ("object_id", pipeline.object_id),
                    ("pointer_id", pipeline.pointer_id),
                    ("function_index", pipeline.function_index),
                    ("pipeline_index", pipeline.index as u64),
                ];
                for (identifier_kind, identifier) in identifiers {
                    for cost in cost_records {
                        if cost.scope_identifier != identifier {
                            continue;
                        }
                        if !cost.total_cost.is_finite() || cost.total_cost <= 0.0 {
                            continue;
                        }
                        pipeline.scope_costs.push(XcodeMioPipelineScopeCost {
                            scope: cost.scope,
                            identifier_kind,
                            identifier,
                            level: cost.level,
                            level_identifier: cost.level_identifier,
                            alu_cost: cost.alu_cost,
                            non_alu_cost: cost.non_alu_cost,
                            total_cost: cost.total_cost,
                            instruction_count: cost.instruction_count,
                        });
                    }
                }
            }
        }

        unsafe fn decode_pipeline_shader_tracks(
            &mut self,
            trace_data: Id,
            pipelines: &mut [XcodeMioPipeline],
            source: &'static str,
        ) -> Result<()> {
            unsafe {
                let helper_class = lookup_class("GTMioTraceDataHelper")?;
                let helper = send_id(helper_class, "alloc")?;
                let helper = send_id_id(helper, "initWithTraceData:", trace_data)?;
                let _ = send_void_i8(helper, "setShowDriverInternalShaders:", 1);
                let _ = send_void_i8(helper, "setShowDriverIntersectionShaders:", 1);
                let _ = send_void_i8(helper, "setShowESLShaders:", 1);

                for pipeline in pipelines {
                    let candidates = [
                        ("object_id", pipeline.object_id),
                        ("pointer_id", pipeline.pointer_id),
                        ("function_index", pipeline.function_index),
                        ("pipeline_index", pipeline.index as u64),
                    ];
                    for (pipeline_id_kind, pipeline_id) in candidates {
                        for program_type in 0..=16 {
                            let track = send_id_u64_u16_allow_nil(
                                helper,
                                "generateAggregatedShaderTrackForPipelineState:programType:",
                                pipeline_id,
                                program_type,
                            )?;
                            if track.is_null() {
                                continue;
                            }
                            if let Some(decoded) = decode_shader_track(
                                track,
                                source,
                                pipeline_id_kind,
                                pipeline_id,
                                program_type,
                            )? {
                                pipeline.shader_tracks.push(decoded);
                            }
                        }
                        let tracks = send_id_u64_allow_nil(
                            helper,
                            "generateShaderTracksForPipelineState:",
                            pipeline_id,
                        )?;
                        if tracks.is_null() {
                            continue;
                        }
                        let track_count = send_u64(tracks, "count").unwrap_or(0);
                        for index in 0..track_count as usize {
                            let track = send_id_usize(tracks, "objectAtIndex:", index)?;
                            if let Some(decoded) = decode_shader_track(
                                track,
                                "shader_tracks_for_pipeline_state",
                                pipeline_id_kind,
                                pipeline_id,
                                u16::MAX,
                            )? {
                                pipeline.shader_tracks.push(decoded);
                            }
                        }
                    }
                }
            }
            Ok(())
        }

        unsafe fn decode_pipeline_shader_binaries(
            &mut self,
            timeline: Id,
            pipelines: &mut [XcodeMioPipeline],
        ) -> Result<()> {
            unsafe {
                let mut seen = BTreeSet::new();
                let binaries = send_id(timeline, "binaries")?;
                let binary_count = send_u64(binaries, "count")?;
                for binary_index in 0..binary_count as usize {
                    let binary = send_id_usize(binaries, "objectAtIndex:", binary_index)?;
                    let address = send_u64(binary, "address").unwrap_or(0);
                    for pipeline in pipelines.iter_mut() {
                        let candidates = [
                            ("object_id", pipeline.object_id),
                            ("pointer_id", pipeline.pointer_id),
                            ("function_index", pipeline.function_index),
                            ("pipeline_index", pipeline.index as u64),
                        ];
                        for (pipeline_id_kind, pipeline_id) in candidates {
                            let used = send_i8_u64(binary, "usedInPipelineState:", pipeline_id)
                                .unwrap_or(0)
                                != 0;
                            let joined_by_key =
                                pipeline.binary_keys.contains(&(binary_index as u64))
                                    || pipeline.all_binary_keys.contains(&(binary_index as u64));
                            let joined_by_address = address != 0
                                && (pipeline.binary_keys.contains(&address)
                                    || pipeline.all_binary_keys.contains(&address));
                            if !used && !joined_by_key && !joined_by_address {
                                continue;
                            }
                            let pipeline_id_kind = if used {
                                pipeline_id_kind
                            } else if joined_by_key {
                                "binary_key"
                            } else {
                                "binary_key_address"
                            };
                            let pipeline_id = if used {
                                pipeline_id
                            } else if joined_by_key {
                                binary_index as u64
                            } else {
                                address
                            };
                            push_decoded_binary(
                                pipeline,
                                &mut seen,
                                binary,
                                pipeline_id_kind,
                                pipeline_id,
                                binary_index as u64,
                            );
                            break;
                        }
                    }
                }
            }
            Ok(())
        }
    }

    struct DecodedTimeline {
        object: Id,
        summary: XcodeMioCostTimeline,
    }

    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    struct RawGtmioCostContext {
        level: u16,
        scope: u16,
        level_identifier: u32,
        scope_identifier: u64,
    }

    #[derive(Clone, Copy, Default)]
    #[repr(C)]
    struct RawGtmioCostInfo {
        context: RawGtmioCostContext,
        alu_cost: f64,
        alu_cost_per_dm: [f64; 10],
        non_alu_cost: f64,
        non_alu_cost_per_dm: [f64; 10],
        instruction_count: u64,
        instruction_count_per_dm: [u64; 10],
        threads_executing_instruction: u64,
        cpi_weighted_instruction_count: u64,
        active_thread_instruction_count: u64,
    }

    #[derive(Clone, Copy)]
    #[repr(C)]
    struct RawGtmioBinaryTrace {
        start_timestamp_ns: u64,
        end_timestamp_ns: u64,
        raw_identifier: u64,
        raw_index: u32,
        raw_count: u32,
        raw_program_type: u16,
    }

    #[derive(Clone, Copy)]
    #[repr(C)]
    struct RawGtmioDrawTrace {
        raw0: u64,
        raw1: u64,
        raw2: u32,
        raw3: u16,
    }

    impl RawGtmioCostInfo {
        fn is_empty(&self) -> bool {
            self.context.level == 0
                && self.context.scope == 0
                && self.context.level_identifier == 0
                && self.context.scope_identifier == 0
                && self.alu_cost == 0.0
                && self.non_alu_cost == 0.0
                && self.instruction_count == 0
                && self.threads_executing_instruction == 0
                && self.cpi_weighted_instruction_count == 0
                && self.active_thread_instruction_count == 0
        }
    }

    unsafe fn decode_shader_track(
        track: Id,
        source: &'static str,
        pipeline_id_kind: &'static str,
        pipeline_id: u64,
        program_type: u16,
    ) -> Result<Option<XcodeMioPipelineShaderTrack>> {
        unsafe {
            let duration_ns = send_u64(track, "duration")?;
            let trace_count = send_u64(track, "traceCount")?;
            if duration_ns == 0 && trace_count == 0 {
                return Ok(None);
            }
            let traces = decode_binary_traces(track, trace_count)?;
            Ok(Some(XcodeMioPipelineShaderTrack {
                source,
                pipeline_id_kind,
                pipeline_id,
                program_type,
                track_id: send_i32(track, "trackId")?,
                first_index: send_u64(track, "firstIndex").unwrap_or(0),
                start_timestamp_ns: send_u64(track, "startTimestamp")?,
                end_timestamp_ns: send_u64(track, "endTimestamp")?,
                duration_ns,
                trace_count,
                traces,
            }))
        }
    }

    unsafe fn decode_binary_traces(
        track: Id,
        trace_count: u64,
    ) -> Result<Vec<XcodeMioBinaryTrace>> {
        unsafe {
            let traces = send_ptr(track, "traces")?;
            if traces.is_null() || trace_count == 0 {
                return Ok(Vec::new());
            }
            let traces = traces.cast::<RawGtmioBinaryTrace>();
            let mut decoded = Vec::with_capacity(trace_count as usize);
            for index in 0..trace_count as usize {
                let raw = *traces.add(index);
                decoded.push(XcodeMioBinaryTrace {
                    index,
                    start_timestamp_ns: raw.start_timestamp_ns,
                    end_timestamp_ns: raw.end_timestamp_ns,
                    raw_identifier: raw.raw_identifier,
                    raw_index: raw.raw_index,
                    raw_count: raw.raw_count,
                    raw_program_type: raw.raw_program_type,
                });
            }
            Ok(decoded)
        }
    }

    fn attach_shader_binary_references(
        shader_binary_info: &[XcodeMioShaderBinaryInfo],
        timeline_binaries: &[XcodeMioTimelineBinary],
        gpu_commands: &[XcodeMioGpuCommand],
        pipelines: &mut [XcodeMioPipeline],
    ) {
        let binary_by_address = timeline_binaries
            .iter()
            .map(|binary| (binary.address, binary.index))
            .collect::<BTreeMap<_, _>>();
        let mut groups =
            BTreeMap::<(usize, u16, u16, u32, u64), XcodeMioPipelineShaderBinaryReference>::new();
        for info in shader_binary_info {
            let command_index = info.raw3 as usize;
            let Some(command) = gpu_commands.get(command_index) else {
                continue;
            };
            let key = (
                command.pipeline_index,
                info.raw5,
                info.raw6,
                info.raw1,
                info.raw2,
            );
            groups
                .entry(key)
                .and_modify(|reference| {
                    reference.record_count += 1;
                    reference.first_command_index =
                        reference.first_command_index.min(command_index);
                    reference.last_command_index = reference.last_command_index.max(command_index);
                })
                .or_insert_with(|| XcodeMioPipelineShaderBinaryReference {
                    raw5: info.raw5,
                    raw6: info.raw6,
                    raw1: info.raw1,
                    address: info.raw2,
                    timeline_binary_index: binary_by_address.get(&info.raw2).copied(),
                    record_count: 1,
                    first_command_index: command_index,
                    last_command_index: command_index,
                });
        }
        for ((pipeline_index, _, _, _, _), reference) in groups {
            if let Some(pipeline) = pipelines.get_mut(pipeline_index) {
                pipeline.shader_binary_references.push(reference);
            }
        }
    }

    unsafe fn decode_timeline_binary(binary: Id, fallback_index: u64) -> XcodeMioTimelineBinary {
        unsafe {
            let index = send_u64(binary, "index").unwrap_or(fallback_index);
            let address = send_u64(binary, "address").unwrap_or(0);
            let program_type = send_u16(binary, "programType").unwrap_or(0);
            let instruction_info_count = send_u64(binary, "instructionInfoCount").unwrap_or(0);
            let instruction_executed = send_u64(binary, "instructionExecuted").unwrap_or(0);
            let duration_ns = send_u64(binary, "duration").unwrap_or(0);
            let trace_count = send_u64(binary, "traceCount").unwrap_or(0);
            let cost_count = send_u64(binary, "costCount").unwrap_or(0);
            let (total_cost, total_instruction_count) =
                decode_binary_total_cost(binary, cost_count, instruction_info_count);
            XcodeMioTimelineBinary {
                index,
                address,
                program_type,
                instruction_info_count,
                instruction_executed,
                duration_ns,
                trace_count,
                cost_count,
                total_cost,
                total_instruction_count,
            }
        }
    }

    unsafe fn push_decoded_binary(
        pipeline: &mut XcodeMioPipeline,
        seen: &mut BTreeSet<(usize, u64, u16, u64)>,
        binary: Id,
        pipeline_id_kind: &'static str,
        pipeline_id: u64,
        fallback_binary_index: u64,
    ) {
        unsafe {
            let binary_index = send_u64(binary, "index").unwrap_or(fallback_binary_index);
            let address = send_u64(binary, "address").unwrap_or(0);
            let program_type = send_u16(binary, "programType").unwrap_or(0);
            if !seen.insert((pipeline.index, binary_index, program_type, address)) {
                return;
            }
            let instruction_info_count = send_u64(binary, "instructionInfoCount").unwrap_or(0);
            let instruction_executed = send_u64(binary, "instructionExecuted").unwrap_or(0);
            let duration_ns = send_u64(binary, "duration").unwrap_or(0);
            let trace_count = send_u64(binary, "traceCount").unwrap_or(0);
            let cost_count = send_u64(binary, "costCount").unwrap_or(0);
            let (total_cost, total_instruction_count) =
                decode_binary_total_cost(binary, cost_count, instruction_info_count);
            pipeline.shader_binaries.push(XcodeMioPipelineShaderBinary {
                pipeline_id_kind,
                pipeline_id,
                binary_index,
                address,
                program_type,
                instruction_info_count,
                instruction_executed,
                duration_ns,
                trace_count,
                cost_count,
                total_cost,
                total_instruction_count,
            });
        }
    }

    unsafe fn decode_binary_total_cost(
        binary: Id,
        cost_count: u64,
        _instruction_info_count: u64,
    ) -> (f64, u64) {
        unsafe {
            if cost_count == 0 {
                return (0.0, 0);
            }
            let Ok(costs) = send_ptr(binary, "costs") else {
                return (0.0, 0);
            };
            if costs.is_null() {
                return (0.0, 0);
            }
            decode_cost_info_sum(costs.cast(), cost_count as usize)
        }
    }

    unsafe fn decode_cost_info_sum(costs: *const RawGtmioCostInfo, count: usize) -> (f64, u64) {
        let mut total_cost = 0.0;
        let mut total_instruction_count = 0;
        for index in 0..count {
            let cost = unsafe { *costs.add(index) };
            total_cost += cost.alu_cost + cost.non_alu_cost;
            total_instruction_count += cost.instruction_count;
        }
        (total_cost, total_instruction_count)
    }

    unsafe fn draw_scope_cost(timeline: Id, draw_index: u64, metadata_program_type: u32) -> f64 {
        let mut best = 0.0_f64;
        let mut candidates = [u16::MAX; 18];
        candidates[0] = metadata_program_type as u16;
        for (index, value) in (0_u16..=16).enumerate() {
            candidates[index + 1] = value;
        }
        for scope in 0_u16..=16 {
            for program_type in candidates {
                if program_type == u16::MAX {
                    continue;
                }
                for selector in [
                    "totalCostForScope:scopeIdentifier:programType:",
                    "totalCostForScope:scopeIdentifier:dataMaster:",
                ] {
                    let cost = unsafe {
                        send_f64_u16_u64_u16(timeline, selector, scope, draw_index, program_type)
                    }
                    .unwrap_or(0.0);
                    if cost.is_finite() && cost > best {
                        best = cost;
                    }
                }
            }
        }
        best
    }

    const _: () = assert!(std::mem::size_of::<RawGtmioCostContext>() == 16);
    const _: () = assert!(std::mem::size_of::<RawGtmioCostInfo>() == 304);
    const _: () = assert!(std::mem::size_of::<RawGtmioBinaryTrace>() == 40);
    const _: () = assert!(std::mem::size_of::<RawGtmioDrawTrace>() == 24);

    struct FdSilencer {
        stdout_fd: c_int,
        stderr_fd: c_int,
    }

    impl FdSilencer {
        fn new() -> Self {
            unsafe {
                let dev_null = c"/dev/null";
                let null_fd = open(dev_null.as_ptr(), 1);
                if null_fd < 0 {
                    return Self {
                        stdout_fd: -1,
                        stderr_fd: -1,
                    };
                }
                let stdout_fd = dup(1);
                let stderr_fd = dup(2);
                if stdout_fd >= 0 {
                    let _ = dup2(null_fd, 1);
                }
                if stderr_fd >= 0 {
                    let _ = dup2(null_fd, 2);
                }
                let _ = close(null_fd);
                Self {
                    stdout_fd,
                    stderr_fd,
                }
            }
        }
    }

    impl Drop for FdSilencer {
        fn drop(&mut self) {
            unsafe {
                if self.stdout_fd >= 0 {
                    let _ = dup2(self.stdout_fd, 1);
                    let _ = close(self.stdout_fd);
                }
                if self.stderr_fd >= 0 {
                    let _ = dup2(self.stderr_fd, 2);
                    let _ = close(self.stderr_fd);
                }
            }
        }
    }

    impl Drop for Runtime {
        fn drop(&mut self) {
            if !self.pool.is_null() {
                unsafe {
                    let _ = send_void(self.pool, "drain");
                }
            }
        }
    }

    struct CfString {
        ptr: CfStringRef,
    }

    impl CfString {
        unsafe fn new(value: *const c_char) -> Result<Self> {
            let ptr = unsafe {
                CFStringCreateWithCString(std::ptr::null(), value, K_CF_STRING_ENCODING_UTF8)
            };
            if ptr.is_null() {
                Err(Error::InvalidInput("failed to create CFString".to_owned()))
            } else {
                Ok(Self { ptr })
            }
        }
    }

    impl Drop for CfString {
        fn drop(&mut self) {
            unsafe {
                CFRelease(self.ptr);
            }
        }
    }

    fn read_u32(bytes: &[u8], offset: usize) -> u32 {
        u32::from_ne_bytes(bytes[offset..offset + 4].try_into().expect("u32 slice"))
    }

    fn read_i32(bytes: &[u8], offset: usize) -> i32 {
        i32::from_ne_bytes(bytes[offset..offset + 4].try_into().expect("i32 slice"))
    }

    fn read_u64(bytes: &[u8], offset: usize) -> u64 {
        u64::from_ne_bytes(bytes[offset..offset + 8].try_into().expect("u64 slice"))
    }

    fn read_u16(bytes: &[u8], offset: usize) -> u16 {
        u16::from_ne_bytes(bytes[offset..offset + 2].try_into().expect("u16 slice"))
    }

    unsafe fn load_framework(path: &str) -> Result<()> {
        let path = CString::new(path).expect("framework path contains no NUL");
        let handle = unsafe { dlopen(path.as_ptr(), RTLD_NOW | RTLD_GLOBAL) };
        if handle.is_null() {
            Err(Error::NotFound(PathBuf::from(
                path.to_string_lossy().into_owned(),
            )))
        } else {
            Ok(())
        }
    }

    unsafe fn lookup_class(name: &str) -> Result<Class> {
        let name = CString::new(name).expect("class name contains no NUL");
        let class = unsafe { objc_lookUpClass(name.as_ptr()) };
        if class.is_null() {
            Err(Error::InvalidInput(format!(
                "Objective-C class {} is not available",
                name.to_string_lossy()
            )))
        } else {
            Ok(class)
        }
    }

    unsafe fn selector(name: &str) -> Result<Sel> {
        let name = CString::new(name)
            .map_err(|_| Error::InvalidInput("selector contains NUL".to_owned()))?;
        let sel = unsafe { sel_registerName(name.as_ptr()) };
        if sel.is_null() {
            Err(Error::InvalidInput(format!(
                "Objective-C selector {} is not available",
                name.to_string_lossy()
            )))
        } else {
            Ok(sel)
        }
    }

    unsafe fn send_id(receiver: Id, sel: &str) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> Id = unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_allow_nil(receiver: Id, sel: &str) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> Id = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_id_id(receiver: Id, sel: &str, arg: Id) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, arg);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_id_allow_nil(receiver: Id, sel: &str, arg: Id) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, arg))
    }

    unsafe fn send_id_id_id(receiver: Id, sel: &str, left: Id, right: Id) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id, Id) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, left, right);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_u64_u16_allow_nil(
        receiver: Id,
        sel: &str,
        left: u64,
        right: u16,
    ) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u64, u16) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, left, right))
    }

    unsafe fn send_id_u64_allow_nil(receiver: Id, sel: &str, arg: u64) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u64) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, arg))
    }

    unsafe fn send_id_usize(receiver: Id, sel: &str, arg: usize) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, usize) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, arg);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_void(receiver: Id, sel: &str) -> Result<()> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) = unsafe { mem::transmute(objc_msgSend as *const ()) };
        f(receiver, sel);
        Ok(())
    }

    unsafe fn send_u64(receiver: Id, sel: &str) -> Result<u64> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> u64 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_u32(receiver: Id, sel: &str) -> Result<u32> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> u32 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_u16(receiver: Id, sel: &str) -> Result<u16> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> u16 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_i32(receiver: Id, sel: &str) -> Result<i32> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> i32 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_i8_u64(receiver: Id, sel: &str, arg: u64) -> Result<i8> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u64) -> i8 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, arg))
    }

    unsafe fn send_void_i8(receiver: Id, sel: &str, arg: i8) -> Result<()> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, i8) = unsafe { mem::transmute(objc_msgSend as *const ()) };
        f(receiver, sel, arg);
        Ok(())
    }

    unsafe fn send_ptr(receiver: Id, sel: &str) -> Result<*const c_void> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> *const c_void =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_i8_u16_u64_cost_mut(
        receiver: Id,
        sel: &str,
        first: u16,
        second: u64,
        third: *mut RawGtmioCostInfo,
    ) -> Result<i8> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u16, u64, *mut RawGtmioCostInfo) -> i8 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, first, second, third))
    }

    unsafe fn send_f64_u16_u64_u16(
        receiver: Id,
        sel: &str,
        first: u16,
        second: u64,
        third: u16,
    ) -> Result<f64> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u16, u64, u16) -> f64 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, first, second, third))
    }

    unsafe fn send_i8_context_cost_mut(
        receiver: Id,
        sel: &str,
        context: *const RawGtmioCostContext,
        cost: *mut RawGtmioCostInfo,
    ) -> Result<i8> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, *const RawGtmioCostContext, *mut RawGtmioCostInfo) -> i8 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel, context, cost))
    }

    unsafe fn send_i8_u16_u32_u16_u64_cost_mut(
        receiver: Id,
        sel: &str,
        level: u16,
        level_identifier: u32,
        scope: u16,
        scope_identifier: u64,
        cost: *mut RawGtmioCostInfo,
    ) -> Result<i8> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, u16, u32, u16, u64, *mut RawGtmioCostInfo) -> i8 =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(
            receiver,
            sel,
            level,
            level_identifier,
            scope,
            scope_identifier,
            cost,
        ))
    }
}

#[cfg(not(target_os = "macos"))]
mod platform {
    use std::path::PathBuf;

    use super::XcodeMioReport;
    use crate::error::{Error, Result};
    use crate::profiler;

    pub fn decode(
        _trace_source: PathBuf,
        _profiler_directory: PathBuf,
        _stream_data_path: PathBuf,
        _profiler_summary: Option<&profiler::ProfilerStreamDataSummary>,
    ) -> Result<XcodeMioReport> {
        Err(Error::Unsupported(
            "xcode-mio is only available on macOS with Xcode installed",
        ))
    }
}
