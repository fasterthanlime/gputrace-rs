use std::fmt;

use serde::Serialize;

use crate::error::{Error, Result};

use super::MAGIC_MTSP;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct MTSPHeader {
    pub version: u32,
    pub reserved_1: u32,
    pub reserved_2: u32,
}

impl MTSPHeader {
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 16 {
            return Err(Error::InvalidTrace(
                "capture data too small for MTSP header",
            ));
        }
        if data.get(..4) != Some(MAGIC_MTSP.as_slice()) {
            return Err(Error::InvalidTrace("capture data did not start with MTSP"));
        }
        Ok(Self {
            version: read_u32(data, 4)?,
            reserved_1: read_u32(data, 8)?,
            reserved_2: read_u32(data, 12)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RecordType {
    C,
    C3ul,
    CS,
    CSuwuw,
    Ct,
    Ctt,
    CtU,
    Ctulul,
    CU,
    Cui,
    Cul,
    Culul,
    Cut,
    Cuw,
    Ci,
    CiulSl,
    Ciulul,
    Unknown,
}

impl fmt::Display for RecordType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::C => "C",
            Self::C3ul => "C@3ul@3ul",
            Self::CS => "CS",
            Self::CSuwuw => "CSuwuw",
            Self::Ct => "Ct",
            Self::Ctt => "Ctt",
            Self::CtU => "CtU<b>ulul",
            Self::Ctulul => "Ctulul",
            Self::CU => "CU",
            Self::Cui => "Cui",
            Self::Cul => "Cul",
            Self::Culul => "Culul",
            Self::Cut => "Cut",
            Self::Cuw => "Cuw",
            Self::Ci => "Ci",
            Self::CiulSl => "CiulSl",
            Self::Ciulul => "Ciulul",
            Self::Unknown => "Unknown",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MTSPRecord {
    pub record_type: RecordType,
    pub offset: usize,
    pub size: usize,
    pub label: Option<String>,
    pub address: Option<u64>,
    pub function_address: Option<u64>,
    #[serde(skip_serializing)]
    pub data: Vec<u8>,
}

impl MTSPRecord {
    pub fn parse_stream(data: &[u8]) -> Result<Vec<Self>> {
        let mut offset = 0usize;
        if data.get(..4) == Some(MAGIC_MTSP.as_slice()) {
            MTSPHeader::parse(data)?;
            offset = 16;
        }

        let mut records = Vec::new();
        while offset + 8 <= data.len() {
            let record_size =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            if record_size == 0 || record_size > 0x400000 || offset + record_size > data.len() {
                offset += 4;
                continue;
            }

            let record_data = data[offset..offset + record_size].to_vec();
            let record_type = detect_record_type(&record_data);
            let mut record = MTSPRecord {
                record_type,
                offset,
                size: record_size,
                label: None,
                address: None,
                function_address: None,
                data: record_data,
            };

            match record.record_type {
                RecordType::CS => record.parse_cs_record(),
                RecordType::CSuwuw => record.parse_csuwuw_record(),
                RecordType::CiulSl => record.parse_ciulsl_record(),
                RecordType::CU | RecordType::Cut => record.parse_cu_record(),
                RecordType::Culul => record.parse_culul_record(),
                _ => {}
            }

            records.push(record);
            offset += record_size;
        }

        Ok(records)
    }

    pub fn parse_ct_record(&self) -> Result<CtRecord> {
        if self.record_type != RecordType::Ct {
            return Err(Error::InvalidTrace("record was not a Ct record"));
        }
        let Some(base) = find_bytes(&self.data, b"Ct\0\0") else {
            return Err(Error::InvalidTrace("Ct marker not found"));
        };
        if base + 28 > self.data.len() {
            return Err(Error::InvalidTrace("Ct record too small"));
        }

        let binding_count = read_u32(&self.data, base + 20)?;
        let stride = read_u32(&self.data, base + 24)?;

        let mut buffer_bindings = Vec::new();
        let mut resource_bindings = Vec::new();
        if binding_count > 0 && stride == 8 {
            let bindings_offset = base + 28;
            let bytes_needed = binding_count as usize * 8;
            if bindings_offset + bytes_needed <= self.data.len() {
                for index in 0..binding_count as usize {
                    let addr = read_u64(&self.data, bindings_offset + index * 8)?;
                    buffer_bindings.push(addr);
                    resource_bindings.push(ResourceBinding {
                        address: addr,
                        index,
                        usage: MTLResourceUsage::READ | MTLResourceUsage::WRITE,
                    });
                }
            }
        }

        Ok(CtRecord {
            record_size: self.size as u32,
            command_flags: read_u32(&self.data, 0).unwrap_or_default(),
            pipeline_addr: read_u64(&self.data, base + 4)?,
            function_addr: read_u64(&self.data, base + 12)?,
            binding_count,
            stride,
            buffer_bindings,
            resource_bindings,
        })
    }

    fn parse_csuwuw_record(&mut self) {
        let Some(base) = find_bytes(&self.data, b"CSuwuw") else {
            return;
        };
        let addr_start = base + 9;
        if addr_start + 8 > self.data.len() {
            return;
        }
        self.address = read_u64(&self.data, addr_start).ok();
        let mut str_start = addr_start + 8;
        while str_start < self.data.len() && self.data[str_start] == 0 {
            str_start += 1;
        }
        self.label = read_c_string(&self.data, str_start);
    }

    fn parse_cs_record(&mut self) {
        let Some(base) = find_bytes(&self.data, b"CS\0\0") else {
            return;
        };
        let addr_start = base + 4;
        if addr_start + 8 > self.data.len() {
            return;
        }
        self.address = read_u64(&self.data, addr_start).ok();
        self.label = read_c_string(&self.data, addr_start + 8);
    }

    fn parse_ciulsl_record(&mut self) {
        let Some(base) = find_bytes(&self.data, b"CiulSl") else {
            return;
        };
        self.function_address = read_u64(&self.data, base + 8).ok();
    }

    fn parse_cu_record(&mut self) {
        for start in 0..self.data.len().saturating_sub(8) {
            let slice = &self.data[start..self.data.len().min(start + 64)];
            if looks_like_hex_label(slice) {
                let end = slice
                    .iter()
                    .position(|byte| !is_hex_or_dash(*byte))
                    .unwrap_or(slice.len());
                if end >= 8 {
                    self.label = Some(String::from_utf8_lossy(&slice[..end]).into_owned());
                    return;
                }
            }
        }
    }

    fn parse_culul_record(&mut self) {
        let Some(base) = find_bytes(&self.data, b"Culul") else {
            return;
        };
        self.address = read_u64(&self.data, base + 5).ok();
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CtRecord {
    pub record_size: u32,
    pub command_flags: u32,
    pub pipeline_addr: u64,
    pub function_addr: u64,
    pub binding_count: u32,
    pub stride: u32,
    pub buffer_bindings: Vec<u64>,
    pub resource_bindings: Vec<ResourceBinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MTLResourceUsage(u8);

impl MTLResourceUsage {
    pub const READ: Self = Self(0x01);
    pub const WRITE: Self = Self(0x02);
    pub const SAMPLE: Self = Self(0x04);
}

impl std::ops::BitOr for MTLResourceUsage {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceBinding {
    pub address: u64,
    pub index: usize,
    pub usage: MTLResourceUsage,
}

fn detect_record_type(data: &[u8]) -> RecordType {
    if data.len() < 16 {
        return RecordType::Unknown;
    }

    for i in 4..data.len().min(128) {
        if starts_with_at(data, i, b"Culul") {
            return RecordType::Culul;
        }
        if starts_with_at(data, i, b"Cuw") {
            return RecordType::Cuw;
        }
        if starts_with_at(data, i, b"Cut") {
            return RecordType::Cut;
        }
        if starts_with_at(data, i, b"Ciulul") {
            return RecordType::Ciulul;
        }
        if starts_with_at(data, i, b"CtU<b>ulul") {
            return RecordType::CtU;
        }
        if starts_with_at(data, i, b"Ctulul") {
            return RecordType::Ctulul;
        }
        if starts_with_at(data, i, b"C@3ul@3ul\0") {
            return RecordType::C3ul;
        }
        if starts_with_at(data, i, b"C\0\0\0") {
            return RecordType::C;
        }
        if starts_with_at(data, i, b"CSuwuw") {
            return RecordType::CSuwuw;
        }
        if starts_with_at(data, i, b"CS") && data.get(i + 2) == Some(&0) {
            return RecordType::CS;
        }
        if starts_with_at(data, i, b"Ctt") {
            return RecordType::Ctt;
        }
        if starts_with_at(data, i, b"Ct")
            && data
                .get(i + 2)
                .is_some_and(|next| *next != b't' && *next != b'u')
            && data.get(i + 2) == Some(&0)
        {
            return RecordType::Ct;
        }
        if starts_with_at(data, i, b"CiulSl") {
            return RecordType::CiulSl;
        }
        if starts_with_at(data, i, b"Cul") {
            return RecordType::Cul;
        }
        if starts_with_at(data, i, b"CU") && data.get(i + 2) == Some(&0) {
            return RecordType::CU;
        }
        if starts_with_at(data, i, b"Ci") && data.get(i + 2) == Some(&0) {
            if starts_with_at(data, i, b"Cui") {
                return RecordType::Cui;
            }
            return RecordType::Ci;
        }
    }

    RecordType::Unknown
}

fn looks_like_hex_label(data: &[u8]) -> bool {
    let mut count = 0usize;
    for byte in data.iter().take(32) {
        if is_hex(*byte) {
            count += 1;
        } else if *byte == 0 {
            break;
        } else {
            return false;
        }
    }
    count >= 8
}

fn is_hex_or_dash(byte: u8) -> bool {
    is_hex(byte) || byte == b'-'
}

fn is_hex(byte: u8) -> bool {
    byte.is_ascii_hexdigit()
}

fn starts_with_at(data: &[u8], offset: usize, needle: &[u8]) -> bool {
    data.get(offset..offset + needle.len()) == Some(needle)
}

fn find_bytes(data: &[u8], needle: &[u8]) -> Option<usize> {
    data.windows(needle.len())
        .position(|window| window == needle)
}

fn read_c_string(data: &[u8], offset: usize) -> Option<String> {
    let tail = data.get(offset..)?;
    let end = tail.iter().position(|byte| *byte == 0)?;
    if end == 0 {
        return None;
    }
    Some(String::from_utf8_lossy(&tail[..end]).into_owned())
}

fn read_u32(data: &[u8], offset: usize) -> Result<u32> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or(Error::InvalidTrace("short u32 read"))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64(data: &[u8], offset: usize) -> Result<u64> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or(Error::InvalidTrace("short u64 read"))?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mtsp_header() {
        let data = [b'M', b'T', b'S', b'P', 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0];
        let header = MTSPHeader::parse(&data).unwrap();
        assert_eq!(header.version, 1);
        assert_eq!(header.reserved_1, 2);
        assert_eq!(header.reserved_2, 3);
    }

    #[test]
    fn parses_ct_record() {
        let mut data = vec![0u8; 100];
        let marker_offset = 16;
        data[marker_offset..marker_offset + 4].copy_from_slice(b"Ct\0\0");

        let pipeline_addr = 0x1122334455667788u64;
        let function_addr = 0x8877665544332211u64;
        let binding_count = 2u32;
        let stride = 8u32;
        let binding_1 = 0xAABBCCDDEEFF0011u64;
        let binding_2 = 0x1100FFEEDDCCBBAAu64;

        data[marker_offset + 4..marker_offset + 12].copy_from_slice(&pipeline_addr.to_le_bytes());
        data[marker_offset + 12..marker_offset + 20].copy_from_slice(&function_addr.to_le_bytes());
        data[marker_offset + 20..marker_offset + 24].copy_from_slice(&binding_count.to_le_bytes());
        data[marker_offset + 24..marker_offset + 28].copy_from_slice(&stride.to_le_bytes());
        data[marker_offset + 28..marker_offset + 36].copy_from_slice(&binding_1.to_le_bytes());
        data[marker_offset + 36..marker_offset + 44].copy_from_slice(&binding_2.to_le_bytes());

        let record = MTSPRecord {
            record_type: RecordType::Ct,
            offset: 0,
            size: data.len(),
            label: None,
            address: None,
            function_address: None,
            data,
        };

        let ct = record.parse_ct_record().unwrap();
        assert_eq!(ct.pipeline_addr, pipeline_addr);
        assert_eq!(ct.function_addr, function_addr);
        assert_eq!(ct.binding_count, binding_count);
        assert_eq!(ct.buffer_bindings, vec![binding_1, binding_2]);
    }

    #[test]
    fn parses_csuwuw_record() {
        let mut data = vec![0u8; 100];
        let marker_offset = 10;
        data[marker_offset..marker_offset + 6].copy_from_slice(b"CSuwuw");

        let func_addr = 0x00CAFEBABE112233u64;
        let addr_offset = marker_offset + 9;
        data[addr_offset..addr_offset + 8].copy_from_slice(&func_addr.to_le_bytes());
        let string_start = addr_offset + 10;
        data[string_start..string_start + 12].copy_from_slice(b"MyKernelFunc");
        data[string_start + 12] = 0;

        let mut record = MTSPRecord {
            record_type: RecordType::CSuwuw,
            offset: 0,
            size: data.len(),
            label: None,
            address: None,
            function_address: None,
            data,
        };
        record.parse_csuwuw_record();
        assert_eq!(record.address, Some(func_addr));
        assert_eq!(record.label.as_deref(), Some("MyKernelFunc"));
    }

    #[test]
    fn parses_ciulsl_record() {
        let mut data = vec![0u8; 64];
        let marker_offset = 10;
        data[marker_offset..marker_offset + 6].copy_from_slice(b"CiulSl");
        let func_addr = 0xDEADBEEFu64;
        data[marker_offset + 8..marker_offset + 16].copy_from_slice(&func_addr.to_le_bytes());

        let mut record = MTSPRecord {
            record_type: RecordType::CiulSl,
            offset: 0,
            size: data.len(),
            label: None,
            address: None,
            function_address: None,
            data,
        };
        record.parse_ciulsl_record();
        assert_eq!(record.function_address, Some(func_addr));
    }

    #[test]
    fn parses_records_from_stream() {
        let mut stream = Vec::new();
        stream.extend_from_slice(b"MTSP");
        stream.extend_from_slice(&1u32.to_le_bytes());
        stream.extend_from_slice(&0u32.to_le_bytes());
        stream.extend_from_slice(&0u32.to_le_bytes());

        let mut record = vec![0u8; 32];
        record[0..4].copy_from_slice(&(32u32).to_le_bytes());
        record[8..12].copy_from_slice(b"CS\0\0");
        let addr = 0x1234u64;
        record[12..20].copy_from_slice(&addr.to_le_bytes());
        record[20..27].copy_from_slice(b"Kernel\0");
        stream.extend_from_slice(&record);

        let records = MTSPRecord::parse_stream(&stream).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_type, RecordType::CS);
        assert_eq!(records[0].address, Some(addr));
        assert_eq!(records[0].label.as_deref(), Some("Kernel"));
    }
}
