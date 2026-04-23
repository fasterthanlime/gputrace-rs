mod mtsp;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use plist::Value;
use serde::Serialize;

use crate::error::{Error, Result};

pub use mtsp::{CtRecord, MTLResourceUsage, MTSPHeader, MTSPRecord, RecordType, ResourceBinding};

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
