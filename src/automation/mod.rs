use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::error::{Error, Result};

const XCODE_APP_NAME: &str = "Xcode";
const DEFAULT_OPEN_TIMEOUT: Duration = Duration::from_secs(30);
const STATUS_POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Serialize)]
pub struct XcodeProfileRun {
    pub trace_path: PathBuf,
    pub output_path: Option<PathBuf>,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XcodeLaunchMode {
    Foreground,
    Background,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenTraceOptions {
    pub launch_mode: XcodeLaunchMode,
    pub wait_for_window: bool,
    pub timeout: Duration,
}

impl Default for OpenTraceOptions {
    fn default() -> Self {
        Self {
            launch_mode: XcodeLaunchMode::Foreground,
            wait_for_window: true,
            timeout: DEFAULT_OPEN_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum XcodeAutomationStatus {
    NotRunning,
    Initializing,
    ReplayReady,
    Running,
    Complete,
    Unknown,
}

impl XcodeAutomationStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XcodeWindowStatus {
    pub status: XcodeAutomationStatus,
    pub raw: String,
}

pub fn activate_xcode() -> Result<()> {
    run_osascript(r#"tell application "Xcode" to activate"#)?;
    Ok(())
}

pub fn run_profile(request: &XcodeProfileRun) -> Result<()> {
    validate_trace_path(&request.trace_path)?;

    open_trace_in_xcode_with_options(
        &request.trace_path,
        OpenTraceOptions {
            launch_mode: XcodeLaunchMode::Foreground,
            wait_for_window: true,
            timeout: Duration::from_secs(request.timeout_seconds.max(1)),
        },
    )?;

    Err(Error::Unsupported(
        "xcode-profile replay/export automation is not implemented yet; only open and wait helpers are available",
    ))
}

pub fn run_osascript(script: &str) -> Result<String> {
    let output = Command::new("osascript")
        .arg("-s")
        .arg("s")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;

            child
                .stdin
                .as_mut()
                .ok_or_else(|| std::io::Error::other("failed to open osascript stdin"))?
                .write_all(script.as_bytes())?;
            child.wait_with_output()
        })?;

    parse_osascript_output(script, output)
}

pub fn open_trace_in_xcode(path: impl AsRef<Path>) -> Result<()> {
    open_trace_in_xcode_with_options(path, OpenTraceOptions::default())
}

pub fn open_trace_in_xcode_with_options(
    path: impl AsRef<Path>,
    options: OpenTraceOptions,
) -> Result<()> {
    let path = validate_trace_path(path)?;
    let output = run_open_command(&path, options.launch_mode)?;

    if !output.status.success() {
        return Err(command_output_error(
            "open",
            &["-a", XCODE_APP_NAME],
            &output,
        ));
    }

    if options.wait_for_window {
        wait_for_xcode_window(options.timeout)?;
    }

    Ok(())
}

pub fn wait_for_xcode_window(timeout: Duration) -> Result<()> {
    wait_for_condition(timeout, STATUS_POLL_INTERVAL, || {
        xcode_window_count().map(|count| count > 0)
    })?
    .then_some(())
    .ok_or_else(|| {
        Error::InvalidInput(format!(
            "timed out after {}s waiting for an Xcode window",
            timeout.as_secs()
        ))
    })
}

pub fn wait_for_status(
    timeout: Duration,
    trace_path: Option<&Path>,
    accepted: &[XcodeAutomationStatus],
) -> Result<XcodeWindowStatus> {
    let mut last = None;
    let found = wait_for_condition(timeout, STATUS_POLL_INTERVAL, || {
        let status = get_window_status(trace_path)?;
        let matched = accepted.contains(&status.status);
        last = Some(status.clone());
        Ok(matched)
    })?;

    if found {
        last.ok_or_else(|| Error::InvalidInput("missing Xcode status result".into()))
    } else {
        let detail = last
            .map(|status| format!("last status was {}", status.raw))
            .unwrap_or_else(|| "no status was observed".to_owned());
        Err(Error::InvalidInput(format!(
            "timed out after {}s waiting for Xcode status: {detail}",
            timeout.as_secs()
        )))
    }
}

pub fn get_window_status(trace_path: Option<&Path>) -> Result<XcodeWindowStatus> {
    let script = build_status_script(trace_path);
    let raw = run_osascript(&script)?;
    Ok(XcodeWindowStatus {
        status: parse_status(&raw),
        raw,
    })
}

pub fn dismiss_startup_dialogs() -> Result<bool> {
    let script = r#"
tell application "System Events"
    if not (exists process "Xcode") then
        return "not-running"
    end if

    tell process "Xcode"
        repeat with buttonName in {"Reopen", "Continue", "Open"}
            try
                click button buttonName of window 1
                return "dismissed:" & buttonName
            end try
        end repeat
    end tell
end tell

return "none"
"#;

    let raw = run_osascript(script)?;
    Ok(raw.starts_with("dismissed:"))
}

fn validate_trace_path(path: impl AsRef<Path>) -> Result<PathBuf> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(Error::NotFound(path.to_path_buf()));
    }
    let metadata = path.metadata()?;
    if !metadata.is_dir() {
        return Err(Error::NotDirectory(path.to_path_buf()));
    }
    Ok(path.to_path_buf())
}

fn run_open_command(path: &Path, launch_mode: XcodeLaunchMode) -> Result<Output> {
    let mut command = Command::new("open");
    command.arg("-a").arg(XCODE_APP_NAME);
    if matches!(launch_mode, XcodeLaunchMode::Background) {
        command.arg("-g");
    }
    command.arg(path);
    Ok(command.output()?)
}

fn xcode_window_count() -> Result<u32> {
    let script = r#"
tell application "System Events"
    if not (exists process "Xcode") then
        return "not-running"
    end if
end tell

tell application "Xcode"
    try
        return count of windows
    on error errMsg number errNum
        error "failed to inspect Xcode windows (" & errNum & "): " & errMsg
    end try
end tell
"#;

    let raw = run_osascript(script)?;
    parse_window_count(&raw)
}

fn build_status_script(trace_path: Option<&Path>) -> String {
    let title_filter = trace_path
        .and_then(Path::file_name)
        .map(applescript_string_literal);

    let target_window = match title_filter {
        Some(filter) => format!(
            r#"
            set targetWindow to missing value
            repeat with candidateWindow in windows
                try
                    if name of candidateWindow contains {filter} then
                        set targetWindow to candidateWindow
                        exit repeat
                    end if
                end try
            end repeat
            if targetWindow is missing value then
                return "unknown"
            end if
"#
        ),
        None => r#"
            if (count of windows) is 0 then
                return "not-running"
            end if
            set targetWindow to window 1
"#
        .to_owned(),
    };

    format!(
        r#"
tell application "System Events"
    if not (exists process "Xcode") then
        return "not-running"
    end if

    tell process "Xcode"
        {target_window}

        try
            set allElements to entire contents of targetWindow
        on error
            return "unknown"
        end try

        repeat with elem in allElements
            try
                if class of elem is button then
                    set btnName to name of elem
                    if btnName is "Show Performance" then
                        return "complete"
                    end if
                end if
            end try
        end repeat

        repeat with elem in allElements
            try
                if class of elem is static text or class of elem is text field then
                    set textValue to value of elem as text
                    if textValue contains "Profiling GPU Trace" then
                        return "running"
                    end if
                    if textValue contains "Performance data not available" then
                        return "replay-ready"
                    end if
                end if
            end try
        end repeat

        repeat with elem in allElements
            try
                if class of elem is button then
                    set btnName to name of elem
                    if btnName is "Export" and enabled of elem then
                        return "complete"
                    end if
                    if btnName is "Stop" or btnName is "Stop GPU workload" then
                        if enabled of elem then
                            return "running"
                        end if
                    end if
                    if btnName is "Profile" or btnName is "Replay" then
                        if enabled of elem then
                            return "replay-ready"
                        else
                            return "initializing"
                        end if
                    end if
                end if
            end try
        end repeat
    end tell
end tell

return "unknown"
"#
    )
}

fn applescript_string_literal(value: &OsStr) -> String {
    let value = value.to_string_lossy();
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

fn parse_osascript_output(script: &str, output: Output) -> Result<String> {
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        return Ok(stdout);
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let detail = if stderr.is_empty() {
        stdout
    } else if stdout.is_empty() {
        stderr
    } else {
        format!("{stderr}; stdout: {stdout}")
    };

    Err(Error::InvalidInput(format!(
        "osascript failed for {}: {}",
        summarize_script(script),
        if detail.is_empty() {
            "unknown error".to_owned()
        } else {
            detail
        }
    )))
}

fn summarize_script(script: &str) -> String {
    let first_line = script
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("<empty script>");

    if first_line.len() > 60 {
        format!("{}...", &first_line[..60])
    } else {
        first_line.to_owned()
    }
}

fn command_output_error(program: &str, args: &[&str], output: &Output) -> Error {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let mut detail = format!(
        "{} {} exited with {}",
        program,
        args.join(" "),
        output.status
    );
    if !stderr.is_empty() {
        detail.push_str(": ");
        detail.push_str(&stderr);
    } else if !stdout.is_empty() {
        detail.push_str(": ");
        detail.push_str(&stdout);
    }
    Error::InvalidInput(detail)
}

fn parse_window_count(raw: &str) -> Result<u32> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("not-running") {
        return Ok(0);
    }
    trimmed
        .parse::<u32>()
        .map_err(|_| Error::InvalidInput(format!("unexpected Xcode window count: {trimmed}")))
}

fn parse_status(raw: &str) -> XcodeAutomationStatus {
    match raw.trim() {
        "not-running" => XcodeAutomationStatus::NotRunning,
        "initializing" => XcodeAutomationStatus::Initializing,
        "replay-ready" => XcodeAutomationStatus::ReplayReady,
        "running" => XcodeAutomationStatus::Running,
        "complete" => XcodeAutomationStatus::Complete,
        _ => XcodeAutomationStatus::Unknown,
    }
}

fn wait_for_condition<F>(timeout: Duration, poll_interval: Duration, mut f: F) -> Result<bool>
where
    F: FnMut() -> Result<bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if f()? {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        thread::sleep(poll_interval);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_status_maps_known_values() {
        assert_eq!(
            parse_status("not-running"),
            XcodeAutomationStatus::NotRunning
        );
        assert_eq!(
            parse_status("initializing"),
            XcodeAutomationStatus::Initializing
        );
        assert_eq!(
            parse_status("replay-ready"),
            XcodeAutomationStatus::ReplayReady
        );
        assert_eq!(parse_status("running"), XcodeAutomationStatus::Running);
        assert_eq!(parse_status("complete"), XcodeAutomationStatus::Complete);
    }

    #[test]
    fn parse_status_falls_back_to_unknown() {
        assert_eq!(parse_status("weird"), XcodeAutomationStatus::Unknown);
        assert_eq!(parse_status(""), XcodeAutomationStatus::Unknown);
    }

    #[test]
    fn parse_window_count_handles_not_running() {
        assert_eq!(parse_window_count("not-running").unwrap(), 0);
    }

    #[test]
    fn parse_window_count_rejects_invalid_values() {
        let err = parse_window_count("abc").unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)));
    }

    #[test]
    fn applescript_string_literal_escapes_quotes_and_backslashes() {
        let value = OsStr::new("trace\"name\\test.gputrace");
        assert_eq!(
            applescript_string_literal(value),
            "\"trace\\\"name\\\\test.gputrace\""
        );
    }

    #[test]
    fn status_script_targets_specific_trace_name() {
        let script = build_status_script(Some(Path::new("/tmp/My Trace.gputrace")));
        assert!(script.contains("My Trace.gputrace"));
        assert!(script.contains("targetWindow"));
    }

    #[test]
    fn status_script_without_trace_uses_front_window() {
        let script = build_status_script(None);
        assert!(script.contains("set targetWindow to window 1"));
    }
}
