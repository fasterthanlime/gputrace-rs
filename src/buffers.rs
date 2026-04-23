use std::collections::BTreeMap;
use std::fs;

use serde::Serialize;

use crate::error::Result;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize)]
pub struct BufferInventory {
    pub total_buffers: usize,
    pub total_bytes: u64,
    pub total_aliases: usize,
    pub buffers: Vec<BufferInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferInfo {
    pub id: String,
    pub filename: String,
    pub size: u64,
    pub address: Option<u64>,
    pub aliases: Vec<String>,
    pub binding_count: usize,
    pub bindings: Vec<BufferBindingInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferBindingInfo {
    pub encoder_label: String,
    pub index: usize,
    pub dispatch_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferInventoryDiff {
    pub left: BufferInventory,
    pub right: BufferInventory,
    pub added: Vec<BufferInfo>,
    pub removed: Vec<BufferInfo>,
    pub changed: Vec<BufferInventoryChange>,
    pub summary: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferInventoryChange {
    pub name: String,
    pub left_size: u64,
    pub right_size: u64,
    pub size_delta: i64,
    pub left_aliases: usize,
    pub right_aliases: usize,
    pub left_bindings: usize,
    pub right_bindings: usize,
}

pub fn analyze(trace: &TraceBundle) -> Result<BufferInventory> {
    let address_map = collect_buffer_addresses(trace)?;
    let binding_map = collect_binding_summaries(trace)?;
    let mut entries = scan_buffer_files(trace, &address_map, &binding_map)?;
    entries.sort_by(|left, right| {
        right
            .size
            .cmp(&left.size)
            .then_with(|| left.filename.cmp(&right.filename))
    });

    Ok(BufferInventory {
        total_buffers: entries.len(),
        total_bytes: entries.iter().map(|entry| entry.size).sum(),
        total_aliases: entries.iter().map(|entry| entry.aliases.len()).sum(),
        buffers: entries,
    })
}

pub fn diff(left: &TraceBundle, right: &TraceBundle) -> Result<BufferInventoryDiff> {
    let left = analyze(left)?;
    let right = analyze(right)?;
    Ok(diff_reports(left, right))
}

pub fn format_table(report: &BufferInventory) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{} buffers, {} bytes, {} aliases\n\n",
        report.total_buffers, report.total_bytes, report.total_aliases
    ));
    out.push_str(&format!(
        "{:<10} {:<28} {:>12} {:>8} {:>8}  {}\n",
        "ID", "Filename", "Size", "Alias", "Bind", "Address"
    ));
    for buffer in &report.buffers {
        out.push_str(&format!(
            "{:<10} {:<28} {:>12} {:>8} {:>8}  {}\n",
            truncate(&buffer.id, 10),
            truncate(&buffer.filename, 28),
            buffer.size,
            buffer.aliases.len(),
            buffer.binding_count,
            buffer
                .address
                .map(|addr| format!("0x{addr:x}"))
                .unwrap_or_else(|| "-".to_owned())
        ));
        for binding in buffer.bindings.iter().take(4) {
            out.push_str(&format!(
                "{:<10}   -> {} [slot {}] x{}\n",
                "", binding.encoder_label, binding.index, binding.dispatch_count
            ));
        }
        if buffer.bindings.len() > 4 {
            out.push_str(&format!(
                "{:<10}   -> ... and {} more bindings\n",
                "",
                buffer.bindings.len() - 4
            ));
        }
        for alias in buffer.aliases.iter().take(4) {
            out.push_str(&format!("{:<10}   = {}\n", "", alias));
        }
        if buffer.aliases.len() > 4 {
            out.push_str(&format!(
                "{:<10}   = ... and {} more aliases\n",
                "",
                buffer.aliases.len() - 4
            ));
        }
    }
    out
}

pub fn format_diff(report: &BufferInventoryDiff) -> String {
    let mut out = String::new();
    out.push_str("=== Buffer Inventory Diff ===\n\n");
    for line in &report.summary {
        out.push_str(&format!("- {line}\n"));
    }
    if !report.added.is_empty() {
        out.push_str("\nAdded buffers:\n");
        for buffer in report.added.iter().take(10) {
            out.push_str(&format!(
                "- {} ({} bytes, {} bindings)\n",
                buffer.filename, buffer.size, buffer.binding_count
            ));
        }
    }
    if !report.removed.is_empty() {
        out.push_str("\nRemoved buffers:\n");
        for buffer in report.removed.iter().take(10) {
            out.push_str(&format!(
                "- {} ({} bytes, {} bindings)\n",
                buffer.filename, buffer.size, buffer.binding_count
            ));
        }
    }
    if !report.changed.is_empty() {
        out.push_str("\nChanged buffers:\n");
        for change in report.changed.iter().take(10) {
            out.push_str(&format!(
                "- {}: size {} -> {} ({:+}), aliases {} -> {}, bindings {} -> {}\n",
                change.name,
                change.left_size,
                change.right_size,
                change.size_delta,
                change.left_aliases,
                change.right_aliases,
                change.left_bindings,
                change.right_bindings
            ));
        }
    }
    out
}

pub fn markdown_report(report: &BufferInventory) -> String {
    let mut out = String::new();
    out.push_str("# Buffer Inventory\n\n");
    out.push_str(&format!("* Buffers: `{}`\n", report.total_buffers));
    out.push_str(&format!("* Total bytes: `{}`\n", report.total_bytes));
    out.push_str(&format!("* Aliases: `{}`\n\n", report.total_aliases));
    out.push_str("## Largest Buffers\n\n");
    for buffer in report.buffers.iter().take(20) {
        out.push_str(&format!(
            "- `{}`: {} bytes, {} aliases, {} bindings\n",
            buffer.filename,
            buffer.size,
            buffer.aliases.len(),
            buffer.binding_count
        ));
    }
    out
}

pub fn markdown_diff(report: &BufferInventoryDiff) -> String {
    let mut out = String::new();
    out.push_str("# Buffer Inventory Diff\n\n");
    for line in &report.summary {
        out.push_str(&format!("- {line}\n"));
    }
    if !report.added.is_empty() {
        out.push_str("\n## Added\n\n");
        for buffer in report.added.iter().take(20) {
            out.push_str(&format!(
                "- `{}`: {} bytes, {} bindings\n",
                buffer.filename, buffer.size, buffer.binding_count
            ));
        }
    }
    if !report.removed.is_empty() {
        out.push_str("\n## Removed\n\n");
        for buffer in report.removed.iter().take(20) {
            out.push_str(&format!(
                "- `{}`: {} bytes, {} bindings\n",
                buffer.filename, buffer.size, buffer.binding_count
            ));
        }
    }
    if !report.changed.is_empty() {
        out.push_str("\n## Changed\n\n");
        for change in report.changed.iter().take(20) {
            out.push_str(&format!(
                "- `{}`: size {} -> {} ({:+}), aliases {} -> {}, bindings {} -> {}\n",
                change.name,
                change.left_size,
                change.right_size,
                change.size_delta,
                change.left_aliases,
                change.right_aliases,
                change.left_bindings,
                change.right_bindings
            ));
        }
    }
    out
}

fn diff_reports(left: BufferInventory, right: BufferInventory) -> BufferInventoryDiff {
    let left_map: BTreeMap<_, _> = left
        .buffers
        .iter()
        .map(|buffer| (&buffer.filename, buffer))
        .collect();
    let right_map: BTreeMap<_, _> = right
        .buffers
        .iter()
        .map(|buffer| (&buffer.filename, buffer))
        .collect();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for buffer in &right.buffers {
        if !left_map.contains_key(&buffer.filename) {
            added.push(buffer.clone());
        }
    }
    for buffer in &left.buffers {
        if !right_map.contains_key(&buffer.filename) {
            removed.push(buffer.clone());
        }
    }
    for (name, left_buffer) in &left_map {
        let Some(right_buffer) = right_map.get(name) else {
            continue;
        };
        if left_buffer.size != right_buffer.size
            || left_buffer.aliases.len() != right_buffer.aliases.len()
            || left_buffer.binding_count != right_buffer.binding_count
        {
            changed.push(BufferInventoryChange {
                name: (*name).clone(),
                left_size: left_buffer.size,
                right_size: right_buffer.size,
                size_delta: right_buffer.size as i64 - left_buffer.size as i64,
                left_aliases: left_buffer.aliases.len(),
                right_aliases: right_buffer.aliases.len(),
                left_bindings: left_buffer.binding_count,
                right_bindings: right_buffer.binding_count,
            });
        }
    }
    added.sort_by(|left, right| {
        right
            .size
            .cmp(&left.size)
            .then_with(|| left.filename.cmp(&right.filename))
    });
    removed.sort_by(|left, right| {
        right
            .size
            .cmp(&left.size)
            .then_with(|| left.filename.cmp(&right.filename))
    });
    changed.sort_by(|left, right| {
        right
            .size_delta
            .abs()
            .cmp(&left.size_delta.abs())
            .then_with(|| left.name.cmp(&right.name))
    });

    let mut summary = vec![
        format!(
            "Buffer count: {} -> {}",
            left.total_buffers, right.total_buffers
        ),
        format!("Total bytes: {} -> {}", left.total_bytes, right.total_bytes),
        format!(
            "Alias count: {} -> {}",
            left.total_aliases, right.total_aliases
        ),
        format!(
            "Inventory changes: {} added, {} removed, {} changed",
            added.len(),
            removed.len(),
            changed.len()
        ),
    ];
    if let Some(change) = changed.first() {
        summary.push(format!(
            "Largest size delta: {} ({} -> {}, {:+})",
            change.name, change.left_size, change.right_size, change.size_delta
        ));
    }

    BufferInventoryDiff {
        left,
        right,
        added,
        removed,
        changed,
        summary,
    }
}

fn collect_buffer_addresses(trace: &TraceBundle) -> Result<BTreeMap<String, u64>> {
    let mut names = BTreeMap::new();
    for (address, name) in trace.buffer_name_map()? {
        names.insert(normalize_buffer_name(&name), address);
    }
    Ok(names)
}

fn collect_binding_summaries(
    trace: &TraceBundle,
) -> Result<BTreeMap<String, BTreeMap<(String, usize), usize>>> {
    let mut bindings: BTreeMap<String, BTreeMap<(String, usize), usize>> = BTreeMap::new();
    for region in trace.command_buffer_regions()? {
        for dispatch in region.dispatches {
            let label = dispatch
                .kernel_name
                .clone()
                .unwrap_or_else(|| "unknown".to_owned());
            for buffer in dispatch.buffers {
                let name = buffer
                    .name
                    .clone()
                    .map(|name| normalize_buffer_name(&name))
                    .unwrap_or_else(|| format!("0x{:x}", buffer.address));
                *bindings
                    .entry(name)
                    .or_default()
                    .entry((label.clone(), buffer.index))
                    .or_default() += 1;
            }
        }
    }
    Ok(bindings)
}

fn scan_buffer_files(
    trace: &TraceBundle,
    address_map: &BTreeMap<String, u64>,
    binding_map: &BTreeMap<String, BTreeMap<(String, usize), usize>>,
) -> Result<Vec<BufferInfo>> {
    #[derive(Default)]
    struct Accum {
        id: String,
        filename: String,
        size: u64,
        aliases: Vec<String>,
    }

    let mut entries: BTreeMap<String, Accum> = BTreeMap::new();
    for dir_entry in fs::read_dir(&trace.path)? {
        let dir_entry = dir_entry?;
        let file_name = dir_entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if !is_bufferish(file_name) {
            continue;
        }
        let normalized = normalize_buffer_name(file_name);
        let entry = entries.entry(normalized).or_default();
        entry.id = parse_buffer_id(file_name);

        let meta = fs::symlink_metadata(dir_entry.path())?;
        if meta.file_type().is_symlink() {
            entry.aliases.push(file_name.to_owned());
            continue;
        }
        if entry.filename.is_empty() || is_primary_variant(file_name) {
            entry.filename = file_name.to_owned();
            entry.size = meta.len();
        }
    }

    let mut buffers = Vec::new();
    for (name, entry) in entries {
        if entry.filename.is_empty() {
            continue;
        }
        let bindings = binding_map
            .get(&name)
            .map(|map| {
                let mut bindings: Vec<_> = map
                    .iter()
                    .map(
                        |((encoder_label, index), dispatch_count)| BufferBindingInfo {
                            encoder_label: encoder_label.clone(),
                            index: *index,
                            dispatch_count: *dispatch_count,
                        },
                    )
                    .collect();
                bindings.sort_by(|left, right| {
                    right
                        .dispatch_count
                        .cmp(&left.dispatch_count)
                        .then_with(|| left.encoder_label.cmp(&right.encoder_label))
                        .then_with(|| left.index.cmp(&right.index))
                });
                bindings
            })
            .unwrap_or_default();

        buffers.push(BufferInfo {
            id: entry.id,
            filename: entry.filename,
            size: entry.size,
            address: address_map.get(&name).copied(),
            aliases: entry.aliases,
            binding_count: bindings.len(),
            bindings,
        });
    }
    Ok(buffers)
}

fn is_bufferish(name: &str) -> bool {
    name.starts_with("MTLBuffer-") || name.starts_with("MTLHeap-")
}

fn parse_buffer_id(name: &str) -> String {
    let trimmed = name
        .strip_prefix("MTLBuffer-")
        .or_else(|| name.strip_prefix("MTLHeap-"))
        .unwrap_or(name);
    trimmed.split('-').next().unwrap_or(trimmed).to_owned()
}

fn normalize_buffer_name(name: &str) -> String {
    if !is_bufferish(name) {
        return name.to_owned();
    }
    let Some((prefix, suffix)) = name.rsplit_once('-') else {
        return name.to_owned();
    };
    if suffix.parse::<usize>().is_ok() {
        format!("{prefix}-0")
    } else {
        name.to_owned()
    }
}

fn is_primary_variant(name: &str) -> bool {
    name.ends_with("-0")
}

fn truncate(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_owned();
    }
    let keep = width.saturating_sub(3);
    format!("{}...", &value[..keep])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_buffer_name_suffixes() {
        assert_eq!(normalize_buffer_name("MTLBuffer-93-7"), "MTLBuffer-93-0");
        assert_eq!(normalize_buffer_name("MTLHeap-11-2"), "MTLHeap-11-0");
        assert_eq!(normalize_buffer_name("other"), "other");
    }

    #[test]
    fn computes_inventory_diff() {
        let left = BufferInventory {
            total_buffers: 1,
            total_bytes: 32,
            total_aliases: 0,
            buffers: vec![BufferInfo {
                id: "1".into(),
                filename: "MTLBuffer-1-0".into(),
                size: 32,
                address: Some(1),
                aliases: vec![],
                binding_count: 1,
                bindings: vec![],
            }],
        };
        let right = BufferInventory {
            total_buffers: 2,
            total_bytes: 96,
            total_aliases: 1,
            buffers: vec![
                BufferInfo {
                    id: "1".into(),
                    filename: "MTLBuffer-1-0".into(),
                    size: 64,
                    address: Some(1),
                    aliases: vec!["MTLBuffer-1-1".into()],
                    binding_count: 3,
                    bindings: vec![],
                },
                BufferInfo {
                    id: "2".into(),
                    filename: "MTLBuffer-2-0".into(),
                    size: 32,
                    address: Some(2),
                    aliases: vec![],
                    binding_count: 1,
                    bindings: vec![],
                },
            ],
        };

        let diff = diff_reports(left, right);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.changed.len(), 1);
        assert_eq!(diff.changed[0].name, "MTLBuffer-1-0");
        assert_eq!(diff.changed[0].size_delta, 32);
    }
}
