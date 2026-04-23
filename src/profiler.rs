use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use plist::{Dictionary, Uid, Value};
use serde::Serialize;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerFileEntry {
    pub name: String,
    pub size: u64,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerPipeline {
    pub pipeline_id: i64,
    pub pipeline_address: u64,
    pub function_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerDispatch {
    pub index: usize,
    pub pipeline_index: usize,
    pub pipeline_id: Option<i64>,
    pub function_name: Option<String>,
    pub encoder_index: usize,
    pub cumulative_us: u64,
    pub duration_us: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerEncoderTiming {
    pub index: usize,
    pub sequence_id: u64,
    pub start_timestamp: u64,
    pub end_offset_micros: u64,
    pub duration_micros: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerCommandBufferTimestamp {
    pub index: usize,
    pub start_ticks: u64,
    pub end_ticks: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerTimelineInfo {
    pub command_buffer_timestamps: Vec<ProfilerCommandBufferTimestamp>,
    pub timebase_numer: u64,
    pub timebase_denom: u64,
    pub absolute_time: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerStreamDataSummary {
    pub function_names: Vec<String>,
    pub pipelines: Vec<ProfilerPipeline>,
    pub dispatches: Vec<ProfilerDispatch>,
    pub encoder_timings: Vec<ProfilerEncoderTiming>,
    pub timeline: Option<ProfilerTimelineInfo>,
    pub num_pipelines: usize,
    pub num_gpu_commands: usize,
    pub num_encoders: usize,
    pub total_time_us: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProfilerReport {
    pub input_path: PathBuf,
    pub profiler_directory: PathBuf,
    pub stream_data_present: bool,
    pub stream_data_summary: Option<ProfilerStreamDataSummary>,
    pub timeline_file_count: usize,
    pub counter_file_count: usize,
    pub profiling_file_count: usize,
    pub kdebug_file_count: usize,
    pub other_file_count: usize,
    pub total_bytes: u64,
    pub files: Vec<ProfilerFileEntry>,
    pub notes: Vec<String>,
}

pub fn report<P: AsRef<Path>>(path: P) -> Result<ProfilerReport> {
    let input_path = path.as_ref().to_path_buf();
    let profiler_directory =
        find_profiler_directory(&input_path).ok_or_else(|| Error::NotFound(input_path.clone()))?;

    let mut files = Vec::new();
    let mut stream_data_present = false;
    let mut timeline_file_count = 0;
    let mut counter_file_count = 0;
    let mut profiling_file_count = 0;
    let mut kdebug_file_count = 0;
    let mut other_file_count = 0;
    let mut total_bytes = 0;

    for entry in fs::read_dir(&profiler_directory)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().into_owned();
        let kind = classify_file(&name);
        match kind.as_str() {
            "streamData" => stream_data_present = true,
            "timeline" => timeline_file_count += 1,
            "counter" => counter_file_count += 1,
            "profiling" => profiling_file_count += 1,
            "kdebug" => kdebug_file_count += 1,
            _ => other_file_count += 1,
        }

        total_bytes += metadata.len();
        files.push(ProfilerFileEntry {
            name,
            size: metadata.len(),
            kind,
        });
    }

    files.sort_by(|left, right| left.name.cmp(&right.name));

    let stream_data_summary = if stream_data_present {
        let stream_data_path = profiler_directory.join("streamData");
        Some(parse_stream_data(&stream_data_path)?)
    } else {
        None
    };

    let mut notes = Vec::new();
    if !stream_data_present {
        notes.push(
            "streamData is missing, so dispatch-level profiler joins are unavailable.".to_owned(),
        );
    } else {
        notes.push(
            "streamData timing and dispatch summaries are real profiler data from the bundle."
                .to_owned(),
        );
    }
    notes.push(
        "Timeline_f_*, Counters_f_*, and Profiling_f_* raw files are only inventoried here; detailed counter parsing is still incomplete."
            .to_owned(),
    );

    Ok(ProfilerReport {
        input_path,
        profiler_directory,
        stream_data_present,
        stream_data_summary,
        timeline_file_count,
        counter_file_count,
        profiling_file_count,
        kdebug_file_count,
        other_file_count,
        total_bytes,
        files,
        notes,
    })
}

pub fn stream_data_summary<P: AsRef<Path>>(path: P) -> Result<ProfilerStreamDataSummary> {
    let input_path = path.as_ref().to_path_buf();
    let profiler_directory =
        find_profiler_directory(&input_path).ok_or_else(|| Error::NotFound(input_path.clone()))?;
    let stream_data_path = profiler_directory.join("streamData");
    if !stream_data_path.is_file() {
        return Err(Error::MissingFile(stream_data_path));
    }
    parse_stream_data(&stream_data_path)
}

pub fn format_report(report: &ProfilerReport) -> String {
    let mut out = String::new();
    out.push_str("GPU Profiler Inventory\n");
    out.push_str("======================\n");
    out.push_str(&format!(
        "profiler_directory={}\n",
        report.profiler_directory.display()
    ));
    out.push_str(&format!(
        "files={} total_bytes={} streamData={}\n",
        report.files.len(),
        report.total_bytes,
        if report.stream_data_present {
            "present"
        } else {
            "missing"
        }
    ));
    out.push_str(&format!(
        "timeline={} counter={} profiling={} kdebug={} other={}\n",
        report.timeline_file_count,
        report.counter_file_count,
        report.profiling_file_count,
        report.kdebug_file_count,
        report.other_file_count
    ));

    if let Some(summary) = &report.stream_data_summary {
        out.push_str("\nstreamData summary\n");
        out.push_str("------------------\n");
        out.push_str(&format!(
            "pipelines={} dispatches={} encoders={} total_time={} us functions={}\n",
            summary.num_pipelines,
            summary.num_gpu_commands,
            summary.num_encoders,
            summary.total_time_us,
            summary.function_names.len()
        ));
        if let Some(timeline) = &summary.timeline {
            out.push_str(&format!(
                "command_buffers={} timebase={}/{} absolute_time={}\n",
                timeline.command_buffer_timestamps.len(),
                timeline.timebase_numer,
                timeline.timebase_denom,
                timeline.absolute_time
            ));
        }

        let top = top_dispatch_functions(summary);
        if !top.is_empty() {
            out.push_str("top functions by dispatch time\n");
            for (name, count, time) in top.into_iter().take(5) {
                out.push_str(&format!("  - {name}: {count} dispatches, {time} us\n"));
            }
        }
    }

    for note in &report.notes {
        out.push_str(&format!("~ {note}\n"));
    }

    for file in &report.files {
        out.push_str(&format!(
            "  {:<10} {:>10} {}\n",
            file.kind, file.size, file.name
        ));
    }

    out
}

fn top_dispatch_functions(summary: &ProfilerStreamDataSummary) -> Vec<(String, usize, u64)> {
    let mut by_name = BTreeMap::<String, (usize, u64)>::new();
    for dispatch in &summary.dispatches {
        let name = dispatch
            .function_name
            .clone()
            .unwrap_or_else(|| format!("pipeline_{}", dispatch.pipeline_index));
        let entry = by_name.entry(name).or_default();
        entry.0 += 1;
        entry.1 += dispatch.duration_us;
    }

    let mut rows = by_name
        .into_iter()
        .map(|(name, (count, time))| (name, count, time))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)));
    rows
}

fn parse_stream_data(path: &Path) -> Result<ProfilerStreamDataSummary> {
    let plist = Value::from_file(path)?;
    let archive = plist
        .as_dictionary()
        .ok_or(Error::InvalidTrace("invalid streamData archive"))?;
    let objects = archive
        .get("$objects")
        .and_then(Value::as_array)
        .ok_or(Error::InvalidTrace("streamData archive missing $objects"))?;
    let root = objects
        .get(1)
        .and_then(Value::as_dictionary)
        .ok_or(Error::InvalidTrace("streamData archive missing object 1"))?;

    let function_names = extract_function_names(objects, root);
    let (pipeline_addresses, pipeline_functions) =
        extract_pipeline_info(objects, root, &function_names);
    let pipelines = extract_pipelines(objects, root, &pipeline_addresses, &pipeline_functions);
    let encoder_timings = extract_encoder_timings(objects, root);
    let dispatches = extract_dispatches(objects, root, &pipelines);
    let timeline = extract_timeline(objects, root);

    Ok(ProfilerStreamDataSummary {
        function_names,
        num_pipelines: pipelines.len(),
        num_gpu_commands: dispatches.len(),
        num_encoders: encoder_timings.len(),
        total_time_us: encoder_timings
            .iter()
            .map(|encoder| encoder.duration_micros)
            .sum(),
        pipelines,
        dispatches,
        encoder_timings,
        timeline,
    })
}

fn extract_function_names(objects: &[Value], root: &Dictionary) -> Vec<String> {
    ns_objects_from_root_key(objects, root, "strings")
        .into_iter()
        .filter_map(|value| value.as_string().map(ToOwned::to_owned))
        .collect()
}

fn extract_pipeline_info(
    objects: &[Value],
    root: &Dictionary,
    function_names: &[String],
) -> (Vec<u64>, Vec<Option<String>>) {
    let Some(data) = ns_data_from_root_key(objects, root, "pipelineStateInfoData") else {
        return (Vec::new(), Vec::new());
    };
    let record_size = root
        .get("pipelineStateInfoSize")
        .and_then(as_u64)
        .unwrap_or(40) as usize;
    if record_size == 0 {
        return (Vec::new(), Vec::new());
    }

    let function_info_data = ns_data_from_root_key(objects, root, "functionInfoData");
    let function_info_size = root.get("functionInfoSize").and_then(as_u64).unwrap_or(48) as usize;

    let record_count = data.len() / record_size;
    let function_info_count = function_info_data
        .map(|bytes| bytes.len() / function_info_size)
        .unwrap_or(0);

    let mut addresses = vec![0; record_count];
    let mut functions = vec![None; record_count];
    for index in 0..record_count {
        let offset = index * record_size;
        let record = &data[offset..offset + record_size];
        if record.len() >= 16 {
            addresses[index] = read_u64(record, 8);
        }

        if let Some(function_info_data) = function_info_data
            && index < function_info_count
            && function_info_size >= 32
        {
            let info_offset = index * function_info_size;
            let info = &function_info_data[info_offset..info_offset + function_info_size];
            let string_index = read_u32(info, 28) as usize;
            if let Some(name) = function_names.get(string_index) {
                functions[index] = Some(name.clone());
                continue;
            }
        }

        if let Some(name) = function_names.get(index) {
            functions[index] = Some(name.clone());
        }
    }

    (addresses, functions)
}

fn extract_pipelines(
    objects: &[Value],
    root: &Dictionary,
    pipeline_addresses: &[u64],
    pipeline_functions: &[Option<String>],
) -> Vec<ProfilerPipeline> {
    let Some(uid) = root.get("pipelinePerformanceStatistics").and_then(as_uid) else {
        return pipeline_addresses
            .iter()
            .enumerate()
            .map(|(index, address)| ProfilerPipeline {
                pipeline_id: index as i64,
                pipeline_address: *address,
                function_name: pipeline_functions.get(index).cloned().flatten(),
            })
            .collect();
    };

    let Some(stats_dict) = object_dictionary(objects, uid) else {
        return Vec::new();
    };
    let Some(keys) = stats_dict.get("NS.keys").and_then(Value::as_array) else {
        return Vec::new();
    };
    let Some(values) = stats_dict.get("NS.objects").and_then(Value::as_array) else {
        return Vec::new();
    };

    let mut pipelines = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        if index >= values.len() {
            break;
        }

        let pipeline_id = match key {
            Value::Uid(uid) => object(objects, *uid)
                .and_then(as_i64)
                .unwrap_or(index as i64),
            _ => as_i64(key).unwrap_or(index as i64),
        };

        pipelines.push(ProfilerPipeline {
            pipeline_id,
            pipeline_address: pipeline_addresses.get(index).copied().unwrap_or_default(),
            function_name: pipeline_functions.get(index).cloned().flatten(),
        });
    }

    pipelines
}

fn extract_encoder_timings(objects: &[Value], root: &Dictionary) -> Vec<ProfilerEncoderTiming> {
    let Some(data) = ns_data_from_root_key(objects, root, "encoderInfoData") else {
        return Vec::new();
    };
    let record_size = root.get("encoderInfoSize").and_then(as_u64).unwrap_or(40) as usize;
    if record_size == 0 {
        return Vec::new();
    }

    let record_count = data.len() / record_size;
    let mut encoders = Vec::with_capacity(record_count);
    let mut previous_end = 0;
    for index in 0..record_count {
        let offset = index * record_size;
        let record = &data[offset..offset + record_size];
        if record.len() < 24 {
            continue;
        }
        let end_offset = read_u64(record, 16);
        let duration = if index == 0 {
            end_offset
        } else {
            end_offset.saturating_sub(previous_end)
        };
        previous_end = end_offset;
        encoders.push(ProfilerEncoderTiming {
            index,
            sequence_id: read_u64(record, 0),
            start_timestamp: read_u64(record, 8),
            end_offset_micros: end_offset,
            duration_micros: duration,
        });
    }
    encoders
}

fn extract_dispatches(
    objects: &[Value],
    root: &Dictionary,
    pipelines: &[ProfilerPipeline],
) -> Vec<ProfilerDispatch> {
    let Some(data) = ns_data_from_root_key(objects, root, "gpuCommandInfoData") else {
        return Vec::new();
    };
    let record_size = root
        .get("gpuCommandInfoSize")
        .and_then(as_u64)
        .unwrap_or(32) as usize;
    if record_size == 0 {
        return Vec::new();
    }

    let record_count = data.len() / record_size;
    let mut dispatches = Vec::with_capacity(record_count);
    let mut previous_cumulative_us = 0;
    for index in 0..record_count {
        let offset = index * record_size;
        let record = &data[offset..offset + record_size];
        if record.len() < 28 {
            continue;
        }

        let pipeline_index = (read_u64(record, 8) >> 32) as usize;
        let cumulative_us = read_u64(record, 16);
        let duration_us = if index == 0 {
            cumulative_us
        } else {
            cumulative_us.saturating_sub(previous_cumulative_us)
        };
        previous_cumulative_us = cumulative_us;

        dispatches.push(ProfilerDispatch {
            index,
            pipeline_index,
            pipeline_id: pipelines
                .get(pipeline_index)
                .map(|pipeline| pipeline.pipeline_id),
            function_name: pipelines
                .get(pipeline_index)
                .and_then(|pipeline| pipeline.function_name.clone()),
            encoder_index: read_u32(record, 24) as usize,
            cumulative_us,
            duration_us,
        });
    }
    dispatches
}

fn extract_timeline(objects: &[Value], root: &Dictionary) -> Option<ProfilerTimelineInfo> {
    let blobs = ns_data_array_from_root_key(objects, root, "APSTimelineData");
    (!blobs.is_empty())
        .then(|| parse_aps_timeline_data(&blobs))
        .flatten()
}

fn parse_aps_timeline_data(blobs: &[Vec<u8>]) -> Option<ProfilerTimelineInfo> {
    let mut info = ProfilerTimelineInfo {
        command_buffer_timestamps: Vec::new(),
        timebase_numer: 1,
        timebase_denom: 1,
        absolute_time: 0,
    };

    for blob in blobs.iter().rev() {
        if blob.len() > 1000 && parse_timeline_metadata_blob(blob, &mut info) {
            return Some(info);
        }
    }

    for blob in blobs.iter().rev() {
        if parse_timeline_metadata_blob(blob, &mut info) {
            return Some(info);
        }
    }

    None
}

fn parse_timeline_metadata_blob(data: &[u8], info: &mut ProfilerTimelineInfo) -> bool {
    let Ok(plist) = Value::from_reader(Cursor::new(data)) else {
        return false;
    };
    let Some(archive) = plist.as_dictionary() else {
        return false;
    };
    let Some(objects) = archive.get("$objects").and_then(Value::as_array) else {
        return false;
    };
    let Some(top) = archive.get("$top").and_then(Value::as_dictionary) else {
        return false;
    };
    let Some(root_uid) = top.get("root").and_then(as_uid) else {
        return false;
    };
    let Some(root) = object_dictionary(objects, root_uid) else {
        return false;
    };
    let Some(keys) = root.get("NS.keys").and_then(Value::as_array) else {
        return false;
    };
    let Some(values) = root.get("NS.objects").and_then(Value::as_array) else {
        return false;
    };
    if keys.len() != values.len() {
        return false;
    }

    let mut found = false;
    for (key, value) in keys.iter().zip(values.iter()) {
        let Some(key_uid) = as_uid(key) else {
            continue;
        };
        let Some(key_name) = object(objects, key_uid).and_then(Value::as_string) else {
            continue;
        };
        let Some(resolved) = resolve_value(objects, value) else {
            continue;
        };

        match key_name {
            "Command Buffer Timestamps" => {
                if let Some(data) = ns_data_from_value(resolved) {
                    info.command_buffer_timestamps = parse_command_buffer_timestamps(data);
                    found = !info.command_buffer_timestamps.is_empty();
                }
            }
            "Absolute Time" => {
                info.absolute_time = extract_scalar_u64(objects, resolved).unwrap_or_default();
            }
            "Timebase" => {
                if let Some((numer, denom)) = extract_timebase(objects, resolved) {
                    info.timebase_numer = numer.max(1);
                    info.timebase_denom = denom.max(1);
                }
            }
            _ => {}
        }
    }

    found
}

fn parse_command_buffer_timestamps(data: &[u8]) -> Vec<ProfilerCommandBufferTimestamp> {
    let mut timestamps = Vec::with_capacity(data.len() / 16);
    for (index, chunk) in data.chunks_exact(16).enumerate() {
        timestamps.push(ProfilerCommandBufferTimestamp {
            index,
            start_ticks: read_u64(chunk, 0),
            end_ticks: read_u64(chunk, 8),
        });
    }
    timestamps
}

pub(crate) fn find_profiler_directory(path: &Path) -> Option<PathBuf> {
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext == "gpuprofiler_raw")
    {
        return path.is_dir().then(|| path.to_path_buf());
    }

    let adjacent = PathBuf::from(format!("{}.gpuprofiler_raw", path.display()));
    if adjacent.is_dir() {
        return Some(adjacent);
    }

    let metadata = fs::metadata(path).ok()?;
    if !metadata.is_dir() {
        return None;
    }

    fs::read_dir(path)
        .ok()?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|entry| {
            entry.is_dir()
                && entry
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext == "gpuprofiler_raw")
        })
}

fn classify_file(name: &str) -> String {
    if name == "streamData" {
        "streamData".to_owned()
    } else if name.starts_with("Timeline_f_") && name.ends_with(".raw") {
        "timeline".to_owned()
    } else if name.starts_with("Counters_f_") && name.ends_with(".raw") {
        "counter".to_owned()
    } else if name.starts_with("Profiling_f_") && name.ends_with(".raw") {
        "profiling".to_owned()
    } else if name.starts_with("kdebug") && name.ends_with(".raw") {
        "kdebug".to_owned()
    } else {
        "other".to_owned()
    }
}

fn object<'a>(objects: &'a [Value], uid: Uid) -> Option<&'a Value> {
    objects.get(uid.get() as usize)
}

fn object_dictionary<'a>(objects: &'a [Value], uid: Uid) -> Option<&'a Dictionary> {
    object(objects, uid).and_then(Value::as_dictionary)
}

fn ns_data_from_root_key<'a>(
    objects: &'a [Value],
    root: &Dictionary,
    key: &str,
) -> Option<&'a [u8]> {
    root.get(key)
        .and_then(as_uid)
        .and_then(|uid| object_dictionary(objects, uid))
        .and_then(|dict| dict.get("NS.data"))
        .and_then(Value::as_data)
}

fn ns_objects_from_root_key<'a>(
    objects: &'a [Value],
    root: &Dictionary,
    key: &str,
) -> Vec<&'a Value> {
    let Some(uid) = root.get(key).and_then(as_uid) else {
        return Vec::new();
    };
    let Some(array_dict) = object_dictionary(objects, uid) else {
        return Vec::new();
    };
    let Some(values) = array_dict.get("NS.objects").and_then(Value::as_array) else {
        return Vec::new();
    };

    values
        .iter()
        .filter_map(|value| match value {
            Value::Uid(uid) => object(objects, *uid),
            _ => Some(value),
        })
        .collect()
}

fn ns_data_array_from_root_key(objects: &[Value], root: &Dictionary, key: &str) -> Vec<Vec<u8>> {
    let Some(uid) = root.get(key).and_then(as_uid) else {
        return Vec::new();
    };
    let Some(array_dict) = object_dictionary(objects, uid) else {
        return Vec::new();
    };
    let Some(values) = array_dict.get("NS.objects").and_then(Value::as_array) else {
        return Vec::new();
    };

    values
        .iter()
        .filter_map(|value| resolve_value(objects, value))
        .filter_map(|value| {
            value
                .as_dictionary()
                .and_then(|dict| dict.get("NS.data"))
                .and_then(Value::as_data)
                .map(|bytes| bytes.to_vec())
        })
        .collect()
}

fn ns_data_from_value(value: &Value) -> Option<&[u8]> {
    value
        .as_dictionary()
        .and_then(|dict| dict.get("NS.data"))
        .and_then(Value::as_data)
        .or_else(|| value.as_data())
}

fn extract_scalar_u64(objects: &[Value], value: &Value) -> Option<u64> {
    as_u64(value).or_else(|| {
        value
            .as_dictionary()
            .and_then(|dict| dict.get("NS.objects"))
            .and_then(Value::as_array)
            .and_then(|entries| entries.first())
            .and_then(|entry| resolve_value(objects, entry))
            .and_then(as_u64)
    })
}

fn extract_timebase(objects: &[Value], value: &Value) -> Option<(u64, u64)> {
    let entries = value
        .as_dictionary()
        .and_then(|dict| dict.get("NS.objects"))
        .and_then(Value::as_array)
        .or_else(|| value.as_array())?;

    let numer = entries
        .first()
        .and_then(|entry| resolve_value(objects, entry))
        .and_then(|entry| extract_scalar_u64(objects, entry))?;
    let denom = entries
        .get(1)
        .and_then(|entry| resolve_value(objects, entry))
        .and_then(|entry| extract_scalar_u64(objects, entry))?;
    Some((numer, denom))
}

fn resolve_value<'a>(objects: &'a [Value], value: &'a Value) -> Option<&'a Value> {
    match value {
        Value::Uid(uid) => object(objects, *uid),
        other => Some(other),
    }
}

fn as_uid(value: &Value) -> Option<Uid> {
    match value {
        Value::Uid(uid) => Some(*uid),
        _ => None,
    }
}

fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Integer(value) => value
            .as_unsigned()
            .or_else(|| value.as_signed().map(|v| v as u64)),
        Value::Real(value) => Some(*value as u64),
        _ => None,
    }
}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(value) => value
            .as_signed()
            .or_else(|| value.as_unsigned().map(|v| v as i64)),
        Value::Real(value) => Some(*value as i64),
        _ => None,
    }
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    data.get(offset..offset + 4)
        .map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap()))
        .unwrap_or_default()
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    data.get(offset..offset + 8)
        .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    use plist::Integer;
    use tempfile::tempdir;

    fn uid(value: u64) -> Value {
        Value::Uid(Uid::new(value))
    }

    fn string(value: &str) -> Value {
        Value::String(value.to_owned())
    }

    fn integer(value: i64) -> Value {
        Value::Integer(Integer::from(value))
    }

    fn dict(entries: &[(&str, Value)]) -> Value {
        let mut dict = Dictionary::new();
        for (key, value) in entries {
            dict.insert((*key).to_owned(), value.clone());
        }
        Value::Dictionary(dict)
    }

    fn data(bytes: Vec<u8>) -> Value {
        dict(&[("NS.data", Value::Data(bytes))])
    }

    fn array(entries: &[Value]) -> Value {
        dict(&[("NS.objects", Value::Array(entries.to_vec()))])
    }

    fn array_uids(values: &[u64]) -> Value {
        dict(&[(
            "NS.objects",
            Value::Array(values.iter().copied().map(uid).collect()),
        )])
    }

    fn dict_uids(keys: &[u64], values: &[u64]) -> Value {
        dict(&[
            (
                "NS.keys",
                Value::Array(keys.iter().copied().map(uid).collect()),
            ),
            (
                "NS.objects",
                Value::Array(values.iter().copied().map(uid).collect()),
            ),
        ])
    }

    fn streamdata_fixture() -> Value {
        let mut pipeline_state = vec![0u8; 40];
        pipeline_state[8..16].copy_from_slice(&0x1111_u64.to_le_bytes());

        let mut function_info = vec![0u8; 48];
        function_info[28..32].copy_from_slice(&(0_u32).to_le_bytes());

        let mut encoder_info = vec![0u8; 40];
        encoder_info[0..8].copy_from_slice(&7_u64.to_le_bytes());
        encoder_info[8..16].copy_from_slice(&100_u64.to_le_bytes());
        encoder_info[16..24].copy_from_slice(&250_u64.to_le_bytes());

        let mut gpu_command = vec![0u8; 32];
        gpu_command[8..16].copy_from_slice(&(0_u64 << 32).to_le_bytes());
        gpu_command[16..24].copy_from_slice(&90_u64.to_le_bytes());
        gpu_command[24..28].copy_from_slice(&0_u32.to_le_bytes());

        let objects = vec![
            string("$null"),
            dict(&[
                ("strings", uid(2)),
                ("pipelineStateInfoData", uid(4)),
                ("pipelineStateInfoSize", integer(40)),
                ("functionInfoData", uid(5)),
                ("functionInfoSize", integer(48)),
                ("pipelinePerformanceStatistics", uid(6)),
                ("encoderInfoData", uid(10)),
                ("encoderInfoSize", integer(40)),
                ("gpuCommandInfoData", uid(11)),
                ("gpuCommandInfoSize", integer(32)),
            ]),
            array_uids(&[3]),
            string("kernel_main"),
            data(pipeline_state),
            data(function_info),
            dict_uids(&[7], &[8]),
            integer(27),
            dict(&[
                ("NS.keys", Value::Array(vec![])),
                ("NS.objects", Value::Array(vec![])),
            ]),
            dict(&[
                ("NS.keys", Value::Array(vec![])),
                ("NS.objects", Value::Array(vec![])),
            ]),
            data(encoder_info),
            data(gpu_command),
        ];

        dict(&[("$objects", Value::Array(objects))])
    }

    fn timeline_blob() -> Vec<u8> {
        let mut timestamps = Vec::new();
        timestamps.extend_from_slice(&100_u64.to_le_bytes());
        timestamps.extend_from_slice(&160_u64.to_le_bytes());
        timestamps.extend_from_slice(&200_u64.to_le_bytes());
        timestamps.extend_from_slice(&320_u64.to_le_bytes());

        let metadata = dict(&[
            ("$top", dict(&[("root", uid(1))])),
            (
                "$objects",
                Value::Array(vec![
                    string("$null"),
                    dict_uids(&[2, 3, 4], &[5, 9, 6]),
                    string("Command Buffer Timestamps"),
                    string("Absolute Time"),
                    string("Timebase"),
                    data(timestamps),
                    array(&[uid(7), uid(8)]),
                    integer(125),
                    integer(3),
                    integer(99_999),
                ]),
            ),
        ]);

        let dir = tempdir().unwrap();
        let path = dir.path().join("metadata.plist");
        metadata.to_file_binary(&path).unwrap();
        fs::read(path).unwrap()
    }

    fn streamdata_fixture_with_timeline() -> Value {
        let mut fixture = streamdata_fixture();
        let objects = fixture
            .as_dictionary_mut()
            .unwrap()
            .get_mut("$objects")
            .and_then(Value::as_array_mut)
            .unwrap();
        objects[1]
            .as_dictionary_mut()
            .unwrap()
            .insert("APSTimelineData".to_owned(), uid(12));
        objects.push(array(&[data(vec![0u8; 32]), data(timeline_blob())]));
        fixture
    }

    #[test]
    fn finds_adjacent_profiler_directory() {
        let dir = tempdir().unwrap();
        let trace_path = dir.path().join("sample.gputrace");
        fs::create_dir(&trace_path).unwrap();
        let profiler_dir = dir.path().join("sample.gputrace.gpuprofiler_raw");
        fs::create_dir(&profiler_dir).unwrap();
        fs::write(profiler_dir.join("streamData"), [0u8; 8]).unwrap();
        fs::write(profiler_dir.join("Timeline_f_0.raw"), [0u8; 16]).unwrap();

        let report = find_profiler_directory(&trace_path).unwrap();
        assert_eq!(report, profiler_dir);
    }

    #[test]
    fn parses_streamdata_summary_from_fixture() {
        let dir = tempdir().unwrap();
        let stream_data_path = dir.path().join("streamData");
        streamdata_fixture()
            .to_file_binary(&stream_data_path)
            .unwrap();

        let summary = parse_stream_data(&stream_data_path).unwrap();
        assert_eq!(summary.function_names, vec!["kernel_main".to_owned()]);
        assert_eq!(summary.num_pipelines, 1);
        assert_eq!(summary.num_encoders, 1);
        assert_eq!(summary.num_gpu_commands, 1);
        assert_eq!(summary.total_time_us, 250);
        assert_eq!(summary.pipelines[0].pipeline_id, 27);
        assert_eq!(summary.pipelines[0].pipeline_address, 0x1111);
        assert_eq!(
            summary.pipelines[0].function_name.as_deref(),
            Some("kernel_main")
        );
        assert_eq!(
            summary.dispatches[0].function_name.as_deref(),
            Some("kernel_main")
        );
        assert_eq!(summary.dispatches[0].duration_us, 90);
        assert!(summary.timeline.is_none());
    }

    #[test]
    fn parses_timeline_metadata_from_streamdata() {
        let dir = tempdir().unwrap();
        let stream_data_path = dir.path().join("streamData");
        streamdata_fixture_with_timeline()
            .to_file_binary(&stream_data_path)
            .unwrap();

        let summary = parse_stream_data(&stream_data_path).unwrap();
        let timeline = summary.timeline.expect("timeline metadata");
        assert_eq!(timeline.timebase_numer, 125);
        assert_eq!(timeline.timebase_denom, 3);
        assert_eq!(timeline.absolute_time, 99_999);
        assert_eq!(timeline.command_buffer_timestamps.len(), 2);
        assert_eq!(timeline.command_buffer_timestamps[0].start_ticks, 100);
        assert_eq!(timeline.command_buffer_timestamps[0].end_ticks, 160);
        assert_eq!(timeline.command_buffer_timestamps[1].start_ticks, 200);
        assert_eq!(timeline.command_buffer_timestamps[1].end_ticks, 320);
    }

    #[test]
    fn reports_streamdata_summary_when_present() {
        let dir = tempdir().unwrap();
        let trace_path = dir.path().join("sample.gputrace");
        fs::create_dir(&trace_path).unwrap();
        let profiler_dir = dir.path().join("sample.gputrace.gpuprofiler_raw");
        fs::create_dir(&profiler_dir).unwrap();
        streamdata_fixture()
            .to_file_binary(profiler_dir.join("streamData"))
            .unwrap();
        fs::write(profiler_dir.join("Timeline_f_0.raw"), [0u8; 16]).unwrap();

        let report = report(&trace_path).unwrap();
        assert!(report.stream_data_present);
        assert!(report.stream_data_summary.is_some());
        let text = format_report(&report);
        assert!(text.contains("streamData summary"));
        assert!(text.contains("kernel_main"));
    }

    #[test]
    fn finds_nested_profiler_directory_inside_trace_bundle() {
        let dir = tempdir().unwrap();
        let trace_path = dir.path().join("sample.gputrace");
        let profiler_dir = trace_path.join("capture.gpuprofiler_raw");
        fs::create_dir_all(&profiler_dir).unwrap();
        fs::write(profiler_dir.join("Counters_f_4.raw"), [0u8; 4]).unwrap();
        fs::write(profiler_dir.join("Profiling_f_1.raw"), [0u8; 12]).unwrap();

        let report = report(&trace_path).unwrap();
        assert_eq!(report.profiler_directory, profiler_dir);
        assert_eq!(report.counter_file_count, 1);
        assert_eq!(report.profiling_file_count, 1);
        assert!(!report.stream_data_present);
    }

    #[test]
    fn formats_inventory_report() {
        let report = ProfilerReport {
            input_path: PathBuf::from("trace.gputrace"),
            profiler_directory: PathBuf::from("trace.gputrace.gpuprofiler_raw"),
            stream_data_present: true,
            stream_data_summary: Some(ProfilerStreamDataSummary {
                function_names: vec!["kernel_main".to_owned()],
                pipelines: vec![ProfilerPipeline {
                    pipeline_id: 27,
                    pipeline_address: 0x1111,
                    function_name: Some("kernel_main".to_owned()),
                }],
                dispatches: vec![ProfilerDispatch {
                    index: 0,
                    pipeline_index: 0,
                    pipeline_id: Some(27),
                    function_name: Some("kernel_main".to_owned()),
                    encoder_index: 0,
                    cumulative_us: 90,
                    duration_us: 90,
                }],
                encoder_timings: vec![ProfilerEncoderTiming {
                    index: 0,
                    sequence_id: 7,
                    start_timestamp: 100,
                    end_offset_micros: 250,
                    duration_micros: 250,
                }],
                timeline: Some(ProfilerTimelineInfo {
                    command_buffer_timestamps: vec![ProfilerCommandBufferTimestamp {
                        index: 0,
                        start_ticks: 100,
                        end_ticks: 160,
                    }],
                    timebase_numer: 125,
                    timebase_denom: 3,
                    absolute_time: 99_999,
                }),
                num_pipelines: 1,
                num_gpu_commands: 1,
                num_encoders: 1,
                total_time_us: 250,
            }),
            timeline_file_count: 1,
            counter_file_count: 2,
            profiling_file_count: 1,
            kdebug_file_count: 0,
            other_file_count: 1,
            total_bytes: 42,
            files: vec![ProfilerFileEntry {
                name: "streamData".to_owned(),
                size: 42,
                kind: "streamData".to_owned(),
            }],
            notes: vec!["Detailed counter parsing is not implemented yet.".to_owned()],
        };

        let text = format_report(&report);
        assert!(text.contains("GPU Profiler Inventory"));
        assert!(text.contains("streamData=present"));
        assert!(text.contains("kernel_main"));
        assert!(text.contains("command_buffers=1"));
    }
}
