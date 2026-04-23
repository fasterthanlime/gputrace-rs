use crate::commands;
use crate::error::{Error, Result};
use crate::trace::TraceBundle;

pub fn generate(
    trace: &TraceBundle,
    graph_type: &str,
    format: &str,
    show_timing: bool,
    show_memory: bool,
) -> Result<String> {
    match (graph_type, format) {
        ("hierarchy", "dot") => hierarchy_dot(trace, show_timing),
        ("hierarchy", "mermaid") => hierarchy_mermaid(trace, show_timing),
        ("flow", "dot") => flow_dot(trace),
        ("flow", "mermaid") => flow_mermaid(trace),
        ("resources", "dot") => resources_dot(trace, show_memory),
        ("resources", "mermaid") => resources_mermaid(trace, show_memory),
        (_, "dot" | "mermaid") => Err(Error::InvalidInput(format!(
            "unsupported graph type: {graph_type} (expected hierarchy, flow, resources)"
        ))),
        _ => Err(Error::InvalidInput(format!(
            "unsupported graph format: {format} (expected dot or mermaid)"
        ))),
    }
}

fn hierarchy_dot(trace: &TraceBundle, show_timing: bool) -> Result<String> {
    let tree = commands::tree(trace, "encoder")?;
    let timings = if show_timing {
        Some(crate::timing::report(trace)?)
    } else {
        None
    };
    let mut out = String::new();
    out.push_str("digraph GPUTrace {\n");
    out.push_str("  rankdir=LR;\n");
    out.push_str("  node [shape=box, style=rounded];\n");
    out.push_str(
        "  trace [label=\"GPU Trace\", shape=ellipse, style=filled, fillcolor=lightblue];\n",
    );
    for (idx, node) in tree.nodes.iter().enumerate() {
        let id = format!("cb{idx}");
        let label = with_optional_timing(node.label.clone(), &timings, idx);
        out.push_str(&format!(
            "  {id} [label=\"{}\", style=filled, fillcolor=lightgreen];\n",
            escape(&label)
        ));
        out.push_str(&format!("  trace -> {id};\n"));
        emit_dot_children(&mut out, &id, &node.children, idx, "  ");
    }
    out.push_str("}\n");
    Ok(out)
}

fn hierarchy_mermaid(trace: &TraceBundle, show_timing: bool) -> Result<String> {
    let tree = commands::tree(trace, "encoder")?;
    let timings = if show_timing {
        Some(crate::timing::report(trace)?)
    } else {
        None
    };
    let mut out = String::new();
    out.push_str("graph LR\n");
    out.push_str("  trace([GPU Trace])\n");
    for (idx, node) in tree.nodes.iter().enumerate() {
        let id = format!("cb{idx}");
        let label = with_optional_timing(node.label.clone(), &timings, idx);
        out.push_str(&format!("  {id}[\"{}\"]\n", escape_mermaid(&label)));
        out.push_str(&format!("  trace --> {id}\n"));
        emit_mermaid_children(&mut out, &id, &node.children, idx);
    }
    Ok(out)
}

fn flow_dot(trace: &TraceBundle) -> Result<String> {
    let deps = commands::dependencies(trace)?;
    let mut out = String::new();
    out.push_str("digraph GPUTrace {\n");
    out.push_str("  rankdir=TB;\n");
    out.push_str("  node [shape=box, style=rounded];\n");
    for node in &deps.nodes {
        out.push_str(&format!(
            "  n{} [label=\"{}\"];\n",
            node.id,
            escape(&node.label)
        ));
    }
    for edge in &deps.edges {
        out.push_str(&format!(
            "  n{} -> n{} [label=\"{} ({})\"];\n",
            edge.from,
            edge.to,
            escape(&edge.buffers.join(", ")),
            edge.hazard
        ));
    }
    out.push_str("}\n");
    Ok(out)
}

fn flow_mermaid(trace: &TraceBundle) -> Result<String> {
    let deps = commands::dependencies(trace)?;
    let mut out = String::new();
    out.push_str("graph TB\n");
    for node in &deps.nodes {
        out.push_str(&format!(
            "  n{}[\"{}\"]\n",
            node.id,
            escape_mermaid(&node.label)
        ));
    }
    for edge in &deps.edges {
        out.push_str(&format!(
            "  n{} -->|{} ({})| n{}\n",
            edge.from,
            escape_mermaid(&edge.buffers.join(", ")),
            edge.hazard,
            edge.to
        ));
    }
    Ok(out)
}

fn resources_dot(trace: &TraceBundle, show_memory: bool) -> Result<String> {
    let access = commands::buffer_access(trace)?;
    let mut out = String::new();
    out.push_str("digraph GPUTrace {\n");
    out.push_str("  rankdir=LR;\n");
    out.push_str("  node [shape=box, style=rounded];\n");
    out.push_str(
        "  trace [label=\"Resources\", shape=ellipse, style=filled, fillcolor=lightblue];\n",
    );
    for (idx, buffer) in access.buffers.iter().enumerate() {
        let id = format!("buf{idx}");
        let mut label = buffer.name.clone();
        if show_memory {
            label.push_str(&format!(
                "\\nuses={} encoders={} cbs={}",
                buffer.use_count, buffer.encoder_count, buffer.command_buffer_count
            ));
        }
        out.push_str(&format!(
            "  {id} [label=\"{}\", style=filled, fillcolor=lightyellow];\n",
            escape(&label)
        ));
        out.push_str(&format!("  trace -> {id};\n"));
    }
    out.push_str("}\n");
    Ok(out)
}

fn resources_mermaid(trace: &TraceBundle, show_memory: bool) -> Result<String> {
    let access = commands::buffer_access(trace)?;
    let mut out = String::new();
    out.push_str("graph LR\n");
    out.push_str("  trace([Resources])\n");
    for (idx, buffer) in access.buffers.iter().enumerate() {
        let id = format!("buf{idx}");
        let mut label = buffer.name.clone();
        if show_memory {
            label.push_str(&format!(
                "<br/>uses={} encoders={} cbs={}",
                buffer.use_count, buffer.encoder_count, buffer.command_buffer_count
            ));
        }
        out.push_str(&format!("  {id}[\"{}\"]\n", escape_mermaid(&label)));
        out.push_str(&format!("  trace --> {id}\n"));
    }
    Ok(out)
}

fn emit_dot_children(
    out: &mut String,
    parent: &str,
    children: &[commands::TreeNode],
    path: usize,
    prefix: &str,
) {
    for (idx, child) in children.iter().enumerate() {
        let id = format!("{parent}_{path}_{idx}");
        out.push_str(&format!(
            "{prefix}{id} [label=\"{}\"];\n",
            escape(&child.label)
        ));
        out.push_str(&format!("{prefix}{parent} -> {id};\n"));
        emit_dot_children(out, &id, &child.children, idx, prefix);
    }
}

fn emit_mermaid_children(
    out: &mut String,
    parent: &str,
    children: &[commands::TreeNode],
    path: usize,
) {
    for (idx, child) in children.iter().enumerate() {
        let id = format!("{parent}_{path}_{idx}");
        out.push_str(&format!("  {id}[\"{}\"]\n", escape_mermaid(&child.label)));
        out.push_str(&format!("  {parent} --> {id}\n"));
        emit_mermaid_children(out, &id, &child.children, idx);
    }
}

fn with_optional_timing(
    label: String,
    timings: &Option<crate::timing::TimingReport>,
    cb_index: usize,
) -> String {
    if let Some(timings) = timings
        && let Some(cb) = timings
            .command_buffers
            .iter()
            .find(|cb| cb.index == cb_index)
        && let Some(duration) = cb.duration_ns
    {
        return format!("{label}\\nduration={duration}");
    }
    label
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn escape_mermaid(value: &str) -> String {
    value.replace('"', "'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_dot() {
        assert_eq!(escape("a\"b"), "a\\\"b");
    }

    #[test]
    fn escapes_mermaid() {
        assert_eq!(escape_mermaid("a\"b"), "a'b");
    }
}
