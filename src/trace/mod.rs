mod mtsp;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use plist::Value;
use serde::Serialize;

use crate::error::{Error, Result};

pub use mtsp::{
    CDispatchRecord, CRecord, CiRecord, CtRecord, CtURecord, CttRecord, CulRecord, CululRecord,
    CuwRecord, MTLResourceUsage, MTSPHeader, MTSPRecord, RecordType, ResourceBinding,
};

pub const MAGIC_MTSP: &[u8; 4] = b"MTSP";

#[derive(Debug, Clone, Serialize)]
pub struct TraceBundle {
    pub path: PathBuf,
    pub metadata: Metadata,
    pub capture_path: PathBuf,
    pub capture_len: u64,
    pub device_resources: Vec<DeviceResource>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Metadata {
    pub uuid: Option<String>,
    pub capture_version: Option<i64>,
    pub graphics_api: Option<i64>,
    pub device_id: Option<i64>,
    pub native_pointer_size: Option<i64>,
    pub captured_frames_count: Option<i64>,
    pub boundary_less: Option<bool>,
    pub library_link_versions: BTreeMap<String, i64>,
    pub unused_buffer_count: Option<i64>,
    pub unused_texture_count: Option<i64>,
    pub unused_function_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeviceResource {
    pub path: PathBuf,
    pub len: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceSummary {
    pub trace_name: String,
    pub uuid: Option<String>,
    pub capture_version: Option<i64>,
    pub graphics_api: Option<i64>,
    pub device_id: Option<i64>,
    pub capture_len: u64,
    pub device_resource_count: usize,
    pub device_resource_bytes: u64,
}

pub type PipelineFunctionMap = BTreeMap<u64, String>;

#[derive(Debug, Clone, Serialize)]
pub struct CommandBuffer {
    pub index: usize,
    pub timestamp: u64,
    pub offset: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ComputeEncoder {
    pub index: usize,
    pub address: u64,
    pub label: String,
    pub offset: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DispatchCall {
    pub index: usize,
    pub offset: usize,
    pub encoder_id: Option<u64>,
    pub grid_size: [u32; 3],
    pub group_size: [u32; 3],
}

#[derive(Debug, Clone, Serialize)]
pub struct KernelStat {
    pub name: String,
    pub pipeline_addr: u64,
    pub dispatch_count: usize,
    pub encoder_labels: BTreeMap<String, usize>,
}

impl TraceBundle {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let info = match fs::metadata(&path) {
            Ok(info) => info,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::NotFound(path));
            }
            Err(error) => return Err(error.into()),
        };
        if !info.is_dir() {
            return Err(Error::NotDirectory(path));
        }

        let metadata = parse_metadata(&path)?;
        let capture_path = load_capture_path(&path)?;
        let capture_len = fs::metadata(&capture_path)?.len();
        let device_resources = load_device_resources(&path)?;

        Ok(Self {
            path,
            metadata,
            capture_path,
            capture_len,
            device_resources,
        })
    }

    pub fn summary(&self) -> TraceSummary {
        TraceSummary {
            trace_name: self
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("unknown")
                .to_owned(),
            uuid: self.metadata.uuid.clone(),
            capture_version: self.metadata.capture_version,
            graphics_api: self.metadata.graphics_api,
            device_id: self.metadata.device_id,
            capture_len: self.capture_len,
            device_resource_count: self.device_resources.len(),
            device_resource_bytes: self.device_resources.iter().map(|entry| entry.len).sum(),
        }
    }

    pub fn capture_data(&self) -> Result<Vec<u8>> {
        Ok(fs::read(&self.capture_path)?)
    }

    pub fn mtsp_header(&self) -> Result<MTSPHeader> {
        let data = self.capture_data()?;
        MTSPHeader::parse(&data)
    }

    pub fn mtsp_records(&self) -> Result<Vec<MTSPRecord>> {
        let data = self.capture_data()?;
        MTSPRecord::parse_stream(&data)
    }

    pub fn pipeline_function_map(&self) -> Result<PipelineFunctionMap> {
        let capture = self.capture_data()?;
        let mut label_map = BTreeMap::new();
        collect_labels_from_data(&capture, &mut label_map)?;
        for resource in &self.device_resources {
            let data = fs::read(&resource.path)?;
            collect_labels_from_data(&data, &mut label_map)?;
        }

        let mut result = BTreeMap::new();
        collect_pipeline_mappings_from_data(&capture, &label_map, &mut result)?;
        for resource in &self.device_resources {
            let data = fs::read(&resource.path)?;
            collect_pipeline_mappings_from_data(&data, &label_map, &mut result)?;
        }
        Ok(result)
    }

    pub fn command_buffers(&self) -> Result<Vec<CommandBuffer>> {
        let data = self.capture_data()?;
        Ok(parse_command_buffers(&data))
    }

    pub fn compute_encoders(&self) -> Result<Vec<ComputeEncoder>> {
        let capture = self.capture_data()?;
        let mut encoders = parse_compute_encoders(&capture);
        if encoders.is_empty() {
            for resource in &self.device_resources {
                let data = fs::read(&resource.path)?;
                encoders.extend(parse_compute_encoders(&data));
            }
        }
        dedupe_encoders(encoders)
    }

    pub fn dispatch_calls(&self) -> Result<Vec<DispatchCall>> {
        let capture = self.capture_data()?;
        let records = MTSPRecord::parse_stream(&capture)?;
        let mut dispatches = Vec::new();
        for record in records {
            if record.record_type != RecordType::C3ul {
                continue;
            }
            let dispatch = record.parse_dispatch_record()?;
            dispatches.push(DispatchCall {
                index: dispatches.len(),
                offset: record.offset,
                encoder_id: Some(dispatch.encoder_id),
                grid_size: dispatch.grid_size,
                group_size: dispatch.group_size,
            });
        }
        Ok(dispatches)
    }

    pub fn analyze_kernels(&self) -> Result<BTreeMap<String, KernelStat>> {
        let pipeline_map = self.pipeline_function_map()?;
        let encoders = self.compute_encoders()?;
        let dispatches = self.dispatch_calls()?;
        let mut stats = BTreeMap::new();

        for (addr, name) in &pipeline_map {
            stats.entry(name.clone()).or_insert_with(|| KernelStat {
                name: name.clone(),
                pipeline_addr: *addr,
                dispatch_count: 0,
                encoder_labels: BTreeMap::new(),
            });
        }

        let encoder_by_addr: BTreeMap<u64, &ComputeEncoder> = encoders
            .iter()
            .map(|encoder| (encoder.address, encoder))
            .collect();
        let encoder_pipeline_map =
            collect_encoder_pipeline_map(&self.capture_data()?, &pipeline_map)?;

        for dispatch in dispatches {
            let encoder = dispatch
                .encoder_id
                .and_then(|encoder_id| encoder_by_addr.get(&encoder_id).copied());

            let name = dispatch
                .encoder_id
                .and_then(|encoder_id| encoder_pipeline_map.get(&encoder_id))
                .cloned()
                .or_else(|| encoder.map(|encoder| encoder.label.clone()))
                .unwrap_or_else(|| "unknown".to_owned());

            let pipeline_addr = dispatch
                .encoder_id
                .and_then(|encoder_id| {
                    encoder_pipeline_map
                        .iter()
                        .find_map(|(candidate, kernel_name)| {
                            (*candidate == encoder_id).then_some(kernel_name)
                        })
                })
                .and_then(|kernel_name| {
                    pipeline_map
                        .iter()
                        .find_map(|(addr, candidate)| (candidate == kernel_name).then_some(*addr))
                })
                .unwrap_or_default();

            let stat = stats.entry(name.clone()).or_insert_with(|| KernelStat {
                name: name.clone(),
                pipeline_addr,
                dispatch_count: 0,
                encoder_labels: BTreeMap::new(),
            });
            stat.dispatch_count += 1;
            if let Some(encoder) = encoder
                && !encoder.label.is_empty()
            {
                *stat
                    .encoder_labels
                    .entry(encoder.label.clone())
                    .or_default() += 1;
            }
        }

        if stats
            .get("unknown")
            .is_some_and(|entry| entry.dispatch_count == 0)
        {
            stats.remove("unknown");
        }

        Ok(stats)
    }
}

fn parse_metadata(bundle_path: &Path) -> Result<Metadata> {
    let metadata_path = bundle_path.join("metadata");
    if !metadata_path.exists() {
        return Err(Error::MissingFile(metadata_path));
    }
    let plist = Value::from_file(&metadata_path)?;
    let Some(dict) = plist.as_dictionary() else {
        return Err(Error::InvalidTrace("metadata plist was not a dictionary"));
    };

    let mut metadata = Metadata {
        uuid: dict.get("(uuid)").and_then(as_string),
        capture_version: dict
            .get("DYCaptureSession.capture_version")
            .and_then(as_integer),
        graphics_api: dict
            .get("DYCaptureSession.graphics_api")
            .and_then(as_integer),
        device_id: dict.get("DYCaptureSession.deviceId").and_then(as_integer),
        native_pointer_size: dict
            .get("DYCaptureSession.nativePointerSize")
            .and_then(as_integer),
        captured_frames_count: dict
            .get("DYCaptureEngine.captured_frames_count")
            .and_then(as_integer),
        boundary_less: dict
            .get("DYCaptureSession.boundaryLess")
            .and_then(Value::as_boolean),
        library_link_versions: BTreeMap::new(),
        unused_buffer_count: dict
            .get("DYCaptureSession.unusedBufferCount")
            .and_then(as_integer),
        unused_texture_count: dict
            .get("DYCaptureSession.unusedTextureCount")
            .and_then(as_integer),
        unused_function_count: dict
            .get("DYCaptureSession.unusedFunctionCount")
            .and_then(as_integer),
    };

    if let Some(libraries) = dict
        .get("DYCaptureSession.library_link_time_versions")
        .and_then(Value::as_dictionary)
    {
        for (name, value) in libraries {
            if let Some(version) = as_integer(value) {
                metadata.library_link_versions.insert(name.clone(), version);
            }
        }
    }

    Ok(metadata)
}

fn load_capture_path(bundle_path: &Path) -> Result<PathBuf> {
    for candidate in ["capture", "unsorted-capture"] {
        let path = bundle_path.join(candidate);
        if path.exists() {
            let bytes = fs::read(&path)?;
            if bytes.get(..4) != Some(MAGIC_MTSP.as_slice()) {
                return Err(Error::InvalidTrace("capture file did not start with MTSP"));
            }
            return Ok(path);
        }
    }

    Err(Error::MissingFile(bundle_path.join("capture")))
}

fn load_device_resources(bundle_path: &Path) -> Result<Vec<DeviceResource>> {
    let mut resources = Vec::new();
    for entry in fs::read_dir(bundle_path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with("device-resources-") {
            continue;
        }
        let path = entry.path();
        resources.push(DeviceResource {
            len: fs::metadata(&path)?.len(),
            path,
        });
    }
    resources.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(resources)
}

fn as_string(value: &Value) -> Option<String> {
    value.as_string().map(ToOwned::to_owned)
}

fn as_integer(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(value) => value
            .as_signed()
            .or_else(|| value.as_unsigned().map(|v| v as i64)),
        Value::Real(value) => Some(*value as i64),
        _ => None,
    }
}

fn collect_labels_from_data(data: &[u8], labels: &mut BTreeMap<u64, String>) -> Result<()> {
    let records = MTSPRecord::parse_stream(data)?;
    for record in records {
        if record.record_type == RecordType::CS
            && let (Some(address), Some(label)) = (record.address, record.label)
        {
            labels.insert(address, label);
        }
    }
    Ok(())
}

fn collect_pipeline_mappings_from_data(
    data: &[u8],
    labels: &BTreeMap<u64, String>,
    result: &mut PipelineFunctionMap,
) -> Result<()> {
    let records = MTSPRecord::parse_stream(data)?;
    for record in records {
        if record.record_type != RecordType::Ctt {
            continue;
        }
        let ctt = record.parse_ctt_record()?;
        if let Some(label) = labels.get(&ctt.function_addr) {
            result.insert(ctt.pipeline_addr, label.clone());
        }
    }
    Ok(())
}

fn parse_command_buffers(data: &[u8]) -> Vec<CommandBuffer> {
    let marker = b"CUUU";
    let mut command_buffers = Vec::new();
    let mut offset = 0usize;
    while let Some(pos) = find_bytes_from(data, marker, offset) {
        if pos + 12 <= data.len() {
            command_buffers.push(CommandBuffer {
                index: command_buffers.len(),
                timestamp: u64::from_le_bytes(data[pos + 4..pos + 12].try_into().unwrap()),
                offset: pos,
            });
        }
        offset = pos + 4;
    }
    command_buffers
}

fn parse_compute_encoders(data: &[u8]) -> Vec<ComputeEncoder> {
    let mut encoders = Vec::new();
    let marker = b"CS\0\0";
    let mut offset = 0usize;
    while let Some(pos) = find_bytes_from(data, marker, offset) {
        let address_start = pos + 4;
        let label_start = pos + 12;
        if address_start + 8 > data.len() || label_start >= data.len() {
            break;
        }
        let address =
            u64::from_le_bytes(data[address_start..address_start + 8].try_into().unwrap());
        let label = read_c_string_bytes(data, label_start).unwrap_or_default();
        if !label.is_empty() {
            encoders.push(ComputeEncoder {
                index: encoders.len(),
                address,
                label,
                offset: pos,
            });
        }
        offset = pos + 4;
    }
    encoders
}

fn dedupe_encoders(mut encoders: Vec<ComputeEncoder>) -> Result<Vec<ComputeEncoder>> {
    encoders.sort_by_key(|encoder| (encoder.offset, encoder.address));
    encoders.dedup_by(|left, right| left.offset == right.offset && left.address == right.address);
    for (index, encoder) in encoders.iter_mut().enumerate() {
        encoder.index = index;
    }
    Ok(encoders)
}

fn collect_encoder_pipeline_map(
    data: &[u8],
    pipeline_map: &PipelineFunctionMap,
) -> Result<BTreeMap<u64, String>> {
    let records = MTSPRecord::parse_stream(data)?;
    let mut result = BTreeMap::new();
    for record in records {
        if record.record_type != RecordType::Ct {
            continue;
        }
        let ct = record.parse_ct_record()?;
        if let Some(name) = pipeline_map.get(&ct.pipeline_addr) {
            result.insert(ct.function_addr, name.clone());
        }
    }
    Ok(result)
}

fn find_bytes_from(data: &[u8], needle: &[u8], offset: usize) -> Option<usize> {
    data.get(offset..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|relative| offset + relative)
}

fn read_c_string_bytes(data: &[u8], offset: usize) -> Option<String> {
    let tail = data.get(offset..)?;
    let end = tail
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(tail.len());
    if end == 0 {
        return None;
    }
    let value = &tail[..end];
    if value
        .iter()
        .all(|byte| byte.is_ascii_graphic() || *byte == b' ')
    {
        Some(String::from_utf8_lossy(value).into_owned())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_command_buffer_markers() {
        let mut data = vec![0u8; 64];
        data[8..12].copy_from_slice(b"CUUU");
        data[12..20].copy_from_slice(&42u64.to_le_bytes());
        data[24..28].copy_from_slice(b"CUUU");
        data[28..36].copy_from_slice(&99u64.to_le_bytes());

        let buffers = parse_command_buffers(&data);
        assert_eq!(buffers.len(), 2);
        assert_eq!(buffers[0].timestamp, 42);
        assert_eq!(buffers[1].timestamp, 99);
    }

    #[test]
    fn parses_compute_encoders_from_cs_records() {
        let mut data = vec![0u8; 64];
        data[8..12].copy_from_slice(b"CS\0\0");
        data[12..20].copy_from_slice(&0x1234u64.to_le_bytes());
        data[20..27].copy_from_slice(b"Kernel\0");

        let encoders = parse_compute_encoders(&data);
        assert_eq!(encoders.len(), 1);
        assert_eq!(encoders[0].address, 0x1234);
        assert_eq!(encoders[0].label, "Kernel");
    }
}
