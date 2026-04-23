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
const FIELD_SEPARATOR: char = '\u{1f}';
const RECORD_SEPARATOR: char = '\u{1e}';

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeWindowStatus {
    pub status: XcodeAutomationStatus,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeWindowInfo {
    pub title: String,
    pub document: Option<String>,
    pub role: String,
    pub subrole: Option<String>,
    pub focused: bool,
    pub main: bool,
    pub modal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeWindowSnapshot {
    pub window: XcodeWindowInfo,
    pub button_count: usize,
    pub tab_count: usize,
    pub toolbar_count: usize,
    pub status: XcodeAutomationStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeButtonInfo {
    pub window_title: String,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeCheckboxInfo {
    pub window_title: String,
    pub name: String,
    pub description: Option<String>,
    pub checked: bool,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeTabInfo {
    pub window_title: String,
    pub role: String,
    pub subrole: Option<String>,
    pub name: String,
    pub selected: bool,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeMenuItemInfo {
    pub menu_path: Vec<String>,
    pub title: String,
    pub enabled: bool,
    pub has_submenu: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeUiElementInfo {
    pub path: Vec<String>,
    pub role: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub identifier: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeActionResult {
    pub window_title: String,
    pub action: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodeExportResult {
    pub window_title: String,
    pub export_kind: String,
    pub output_path: PathBuf,
}

pub fn activate_xcode() -> Result<()> {
    run_osascript(r#"tell application "Xcode" to activate"#)?;
    Ok(())
}

pub fn list_windows() -> Result<Vec<XcodeWindowInfo>> {
    let raw = run_osascript(&build_windows_script())?;
    parse_windows_output(&raw)
}

pub fn inspect_window(trace_path: Option<&Path>) -> Result<Option<XcodeWindowSnapshot>> {
    let raw = run_osascript(&build_window_snapshot_script(trace_path))?;
    parse_window_snapshot_output(&raw)
}

pub fn list_buttons(trace_path: Option<&Path>) -> Result<Vec<XcodeButtonInfo>> {
    let raw = run_osascript(&build_buttons_script(trace_path))?;
    parse_buttons_output(&raw)
}

pub fn list_checkboxes(trace_path: Option<&Path>) -> Result<Vec<XcodeCheckboxInfo>> {
    let raw = run_osascript(&build_checkboxes_script(trace_path))?;
    parse_checkboxes_output(&raw)
}

pub fn list_tabs(trace_path: Option<&Path>) -> Result<Vec<XcodeTabInfo>> {
    let raw = run_osascript(&build_tabs_script(trace_path))?;
    parse_tabs_output(&raw)
}

pub fn list_menu_items(menu_path: &[&str]) -> Result<Vec<XcodeMenuItemInfo>> {
    let raw = run_osascript(&build_menu_items_script(menu_path))?;
    parse_menu_items_output(&raw)
}

pub fn list_ui_elements(trace_path: Option<&Path>) -> Result<Vec<XcodeUiElementInfo>> {
    let raw = run_osascript(&build_ui_elements_script(trace_path))?;
    parse_ui_elements_output(&raw)
}

pub fn click_button(trace_path: Option<&Path>, button_names: &[&str]) -> Result<XcodeActionResult> {
    let raw = run_osascript(&build_click_button_script(trace_path, button_names))?;
    parse_action_output(&raw)
}

pub fn select_tab(trace_path: Option<&Path>, tab_name: &str) -> Result<XcodeActionResult> {
    let raw = run_osascript(&build_select_tab_script(trace_path, tab_name))?;
    parse_action_output(&raw)
}

pub fn click_menu_item(menu_path: &[&str]) -> Result<XcodeActionResult> {
    let raw = run_osascript(&build_click_menu_item_script(menu_path))?;
    parse_action_output(&raw)
}

pub fn close_window(trace_path: Option<&Path>) -> Result<XcodeActionResult> {
    let raw = run_osascript(&build_close_window_script(trace_path))?;
    parse_action_output(&raw)
}

pub fn ensure_checked(trace_path: Option<&Path>, checkbox_name: &str) -> Result<XcodeActionResult> {
    let raw = run_osascript(&build_checkbox_action_script(
        trace_path,
        checkbox_name,
        "ensure-checked",
    ))?;
    parse_action_output(&raw)
}

pub fn toggle_checkbox(
    trace_path: Option<&Path>,
    checkbox_name: &str,
) -> Result<XcodeActionResult> {
    let raw = run_osascript(&build_checkbox_action_script(
        trace_path,
        checkbox_name,
        "toggle-checkbox",
    ))?;
    parse_action_output(&raw)
}

pub fn show_performance(trace_path: Option<&Path>) -> Result<XcodeActionResult> {
    click_button(trace_path, &["Show Performance"])
}

pub fn show_dependencies(trace_path: Option<&Path>) -> Result<XcodeActionResult> {
    click_button(trace_path, &["Show Dependencies"])
}

pub fn show_summary(trace_path: Option<&Path>) -> Result<XcodeActionResult> {
    select_tab(trace_path, "Summary")
}

pub fn show_counters(trace_path: Option<&Path>) -> Result<XcodeActionResult> {
    select_tab(trace_path, "Counters")
}

pub fn show_memory(trace_path: Option<&Path>) -> Result<XcodeActionResult> {
    select_tab(trace_path, "Memory")
}

pub fn export_counters(trace_path: Option<&Path>, output_path: &Path) -> Result<XcodeExportResult> {
    let output_path = prepare_export_output_path(output_path)?;
    let trace_path = trace_path.map(validate_trace_path).transpose()?;

    let _ = show_performance(trace_path.as_deref());
    let _ = show_counters(trace_path.as_deref());
    let _ = click_menu_item(&["Editor", "Export GPU Counters…"])
        .or_else(|_| click_menu_item(&["Editor", "Export GPU Counters..."]))
        .or_else(|_| click_menu_item(&["Editor", "Export GPU Counters"]))?;
    let window_title = finish_export_sheet(output_path.as_path(), "gpu-counters")?;

    Ok(XcodeExportResult {
        window_title,
        export_kind: "gpu-counters".to_owned(),
        output_path,
    })
}

pub fn export_memory(trace_path: Option<&Path>, output_path: &Path) -> Result<XcodeExportResult> {
    let output_path = prepare_export_output_path(output_path)?;
    let trace_path = trace_path.map(validate_trace_path).transpose()?;

    let _ = show_performance(trace_path.as_deref());
    let _ = show_memory(trace_path.as_deref());
    let _ = click_menu_item(&["Editor", "Export Memory Report…"])
        .or_else(|_| click_menu_item(&["Editor", "Export Memory Report..."]))
        .or_else(|_| click_menu_item(&["Editor", "Export Memory Report"]))?;
    let window_title = finish_export_sheet(output_path.as_path(), "memory-report")?;

    Ok(XcodeExportResult {
        window_title,
        export_kind: "memory-report".to_owned(),
        output_path,
    })
}

pub fn run_profile(request: &XcodeProfileRun) -> Result<XcodeExportResult> {
    validate_trace_path(&request.trace_path)?;
    let output_path = request
        .output_path
        .clone()
        .unwrap_or_else(|| default_profile_output_path(&request.trace_path));

    open_trace_in_xcode_with_options(
        &request.trace_path,
        OpenTraceOptions {
            launch_mode: XcodeLaunchMode::Foreground,
            wait_for_window: true,
            timeout: Duration::from_secs(request.timeout_seconds.max(1)),
        },
    )?;

    let _ = dismiss_startup_dialogs();
    let trace_path = Some(request.trace_path.as_path());
    let status = get_window_status(trace_path)?;
    if !matches!(
        status.status,
        XcodeAutomationStatus::ReplayReady
            | XcodeAutomationStatus::Complete
            | XcodeAutomationStatus::Running
    ) {
        let _ = wait_for_status(
            Duration::from_secs(request.timeout_seconds.max(1)),
            trace_path,
            &[
                XcodeAutomationStatus::ReplayReady,
                XcodeAutomationStatus::Complete,
                XcodeAutomationStatus::Running,
            ],
        )?;
    }

    let status = get_window_status(trace_path)?;
    match status.status {
        XcodeAutomationStatus::ReplayReady => {
            let _ = click_button(trace_path, &["Profile", "Replay"])?;
            let _ = wait_for_status(
                Duration::from_secs(request.timeout_seconds.max(1)),
                trace_path,
                &[XcodeAutomationStatus::Complete],
            )?;
        }
        XcodeAutomationStatus::Running => {
            let _ = wait_for_status(
                Duration::from_secs(request.timeout_seconds.max(1)),
                trace_path,
                &[XcodeAutomationStatus::Complete],
            )?;
        }
        XcodeAutomationStatus::Complete => {}
        _ => {
            return Err(Error::InvalidInput(format!(
                "Xcode trace window is not ready to profile: {}",
                status.raw
            )));
        }
    }

    let _ = show_performance(trace_path);

    let export = export_profile_trace(trace_path, &output_path)?;
    let _ = close_window(trace_path);
    Ok(export)
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

fn default_profile_output_path(trace_path: &Path) -> PathBuf {
    let parent = trace_path.parent().unwrap_or_else(|| Path::new("."));
    let stem = trace_path
        .file_stem()
        .and_then(OsStr::to_str)
        .unwrap_or("trace");
    parent.join(format!("{stem}-perfdata.gputrace"))
}

fn xcode_window_count() -> Result<u32> {
    let raw = run_osascript(&build_window_count_script())?;
    parse_window_count(&raw)
}

fn build_status_script(trace_path: Option<&Path>) -> String {
    let target_window = build_target_window_clause(trace_path, "unknown", true);

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

fn build_window_count_script() -> String {
    r#"
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
"#
    .to_owned()
}

fn build_windows_script() -> String {
    format!(
        r#"
set fieldSeparator to ASCII character 31
set recordSeparator to ASCII character 30

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        set outputLines to {{}}
        repeat with candidateWindow in windows
            set titleText to my element_name(candidateWindow)
            set documentText to my attribute_text(candidateWindow, "AXDocument")
            set roleText to my role_text(candidateWindow)
            set subroleText to my optional_text(my attribute_text(candidateWindow, "AXSubrole"))
            set focusedText to my boolean_text(my attribute_bool(candidateWindow, "AXFocused"))
            set mainText to my boolean_text(my attribute_bool(candidateWindow, "AXMain"))
            set modalText to my boolean_text(my attribute_bool(candidateWindow, "AXModal"))
            set end of outputLines to titleText & fieldSeparator & documentText & fieldSeparator & roleText & fieldSeparator & subroleText & fieldSeparator & focusedText & fieldSeparator & mainText & fieldSeparator & modalText
        end repeat
        return my join_records(outputLines, recordSeparator)
    end tell
end tell

{helpers}
"#,
        app = XCODE_APP_NAME,
        helpers = common_applescript_helpers()
    )
}

fn build_window_snapshot_script(trace_path: Option<&Path>) -> String {
    let target_window = build_target_window_clause(trace_path, "missing-window", false);
    format!(
        r#"
set fieldSeparator to ASCII character 31

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        {target_window}

        if targetWindow is missing value then
            return "missing-window"
        end if

        set titleText to my element_name(targetWindow)
        set documentText to my attribute_text(targetWindow, "AXDocument")
        set roleText to my role_text(targetWindow)
        set subroleText to my optional_text(my attribute_text(targetWindow, "AXSubrole"))
        set focusedText to my boolean_text(my attribute_bool(targetWindow, "AXFocused"))
        set mainText to my boolean_text(my attribute_bool(targetWindow, "AXMain"))
        set modalText to my boolean_text(my attribute_bool(targetWindow, "AXModal"))
        set statusText to "unknown"
        set buttonCount to 0
        set tabCount to 0
        set toolbarCount to 0

        try
            set allElements to entire contents of targetWindow
        on error
            set allElements to {{}}
        end try

        repeat with elem in allElements
            try
                set roleName to my role_text(elem)
                if roleName is "AXButton" then
                    set buttonCount to buttonCount + 1
                    set buttonName to my element_name(elem)
                    if buttonName is "Show Performance" then
                        set statusText to "complete"
                    else if (buttonName is "Stop" or buttonName is "Stop GPU workload") and (my attribute_bool(elem, "AXEnabled")) then
                        set statusText to "running"
                    else if (buttonName is "Profile" or buttonName is "Replay") and statusText is not "running" and statusText is not "complete" then
                        if my attribute_bool(elem, "AXEnabled") then
                            set statusText to "replay-ready"
                        else
                            set statusText to "initializing"
                        end if
                    end if
                else if roleName is "AXToolbar" then
                    set toolbarCount to toolbarCount + 1
                else if roleName is "AXRadioButton" then
                    set tabCount to tabCount + 1
                end if

                if statusText is not "complete" then
                    set textValue to my element_text(elem)
                    if textValue contains "Profiling GPU Trace" then
                        set statusText to "running"
                    else if textValue contains "Performance data not available" and statusText is not "running" then
                        set statusText to "replay-ready"
                    end if
                end if
            end try
        end repeat

        return titleText & fieldSeparator & documentText & fieldSeparator & roleText & fieldSeparator & subroleText & fieldSeparator & focusedText & fieldSeparator & mainText & fieldSeparator & modalText & fieldSeparator & buttonCount & fieldSeparator & tabCount & fieldSeparator & toolbarCount & fieldSeparator & statusText
    end tell
end tell

{helpers}
"#,
        app = XCODE_APP_NAME,
        target_window = target_window,
        helpers = common_applescript_helpers()
    )
}

fn build_buttons_script(trace_path: Option<&Path>) -> String {
    let target_window = build_target_window_clause(trace_path, "not-running", false);
    format!(
        r#"
set fieldSeparator to ASCII character 31
set recordSeparator to ASCII character 30

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        {target_window}

        if targetWindow is missing value then
            return ""
        end if

        set windowTitle to my element_name(targetWindow)
        set outputLines to {{}}
        set allElements to entire contents of targetWindow
        repeat with elem in allElements
            try
                if my role_text(elem) is "AXButton" then
                    set buttonName to my element_name(elem)
                    set buttonDescription to my optional_text(my attribute_text(elem, "AXDescription"))
                    set enabledText to my boolean_text(my attribute_bool(elem, "AXEnabled"))
                    set end of outputLines to windowTitle & fieldSeparator & buttonName & fieldSeparator & buttonDescription & fieldSeparator & enabledText
                end if
            end try
        end repeat
        return my join_records(outputLines, recordSeparator)
    end tell
end tell

{helpers}
"#,
        app = XCODE_APP_NAME,
        target_window = target_window,
        helpers = common_applescript_helpers()
    )
}

fn build_checkboxes_script(trace_path: Option<&Path>) -> String {
    let target_window = build_target_window_clause(trace_path, "not-running", false);
    format!(
        r#"
set fieldSeparator to ASCII character 31
set recordSeparator to ASCII character 30

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        {target_window}

        if targetWindow is missing value then
            return ""
        end if

        set windowTitle to my element_name(targetWindow)
        set outputLines to {{}}
        set allElements to entire contents of targetWindow
        repeat with elem in allElements
            try
                if my role_text(elem) is "AXCheckBox" then
                    set checkboxName to my element_name(elem)
                    set checkboxDescription to my optional_text(my attribute_text(elem, "AXDescription"))
                    set checkedText to my boolean_text(my attribute_bool(elem, "AXValue"))
                    set enabledText to my boolean_text(my attribute_bool(elem, "AXEnabled"))
                    set end of outputLines to windowTitle & fieldSeparator & checkboxName & fieldSeparator & checkboxDescription & fieldSeparator & checkedText & fieldSeparator & enabledText
                end if
            end try
        end repeat
        return my join_records(outputLines, recordSeparator)
    end tell
end tell

{helpers}
"#,
        app = XCODE_APP_NAME,
        target_window = target_window,
        helpers = common_applescript_helpers()
    )
}

fn build_tabs_script(trace_path: Option<&Path>) -> String {
    let target_window = build_target_window_clause(trace_path, "not-running", false);
    format!(
        r#"
set fieldSeparator to ASCII character 31
set recordSeparator to ASCII character 30

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        {target_window}

        if targetWindow is missing value then
            return ""
        end if

        set windowTitle to my element_name(targetWindow)
        set outputLines to {{}}
        set allElements to entire contents of targetWindow
        repeat with elem in allElements
            try
                set roleName to my role_text(elem)
                set subroleText to my optional_text(my attribute_text(elem, "AXSubrole"))
                if roleName is "AXRadioButton" or subroleText is "AXTabButton" then
                    set nameText to my element_name(elem)
                    set selectedText to my boolean_text(my attribute_bool(elem, "AXValue"))
                    set enabledText to my boolean_text(my attribute_bool(elem, "AXEnabled"))
                    set end of outputLines to windowTitle & fieldSeparator & roleName & fieldSeparator & subroleText & fieldSeparator & nameText & fieldSeparator & selectedText & fieldSeparator & enabledText
                end if
            end try
        end repeat
        return my join_records(outputLines, recordSeparator)
    end tell
end tell

{helpers}
"#,
        app = XCODE_APP_NAME,
        target_window = target_window,
        helpers = common_applescript_helpers()
    )
}

fn build_menu_items_script(menu_path: &[&str]) -> String {
    let menu_path = menu_path
        .iter()
        .map(|segment| applescript_string_literal(OsStr::new(segment)))
        .collect::<Vec<_>>();
    let traversal = match menu_path.as_slice() {
        [] => {
            r#"
        set outputLines to {}
        repeat with menuBarItem in menu bar items of menu bar 1
            set menuTitle to my element_name(menuBarItem)
            set enabledText to my boolean_text(my attribute_bool(menuBarItem, "AXEnabled"))
            set submenuText to my boolean_text(true)
            set end of outputLines to menuTitle & fieldSeparator & menuTitle & fieldSeparator & enabledText & fieldSeparator & submenuText
        end repeat
        return my join_records(outputLines, recordSeparator)
"#
            .to_owned()
        }
        [root] => format!(
            r#"
        set targetMenuBarItem to menu bar item {root} of menu bar 1
        set targetMenu to menu 1 of targetMenuBarItem
        set outputLines to {{}}
        repeat with menuItemRef in menu items of targetMenu
            set itemTitle to my element_name(menuItemRef)
            set enabledText to my boolean_text(my attribute_bool(menuItemRef, "AXEnabled"))
            set hasSubmenuText to my boolean_text(my has_submenu(menuItemRef))
            set end of outputLines to {root} & fieldSeparator & itemTitle & fieldSeparator & enabledText & fieldSeparator & hasSubmenuText
        end repeat
        return my join_records(outputLines, recordSeparator)
"#
        ),
        _ => {
            let root = menu_path[0].clone();
            let mut traversal = format!(
                "        set currentMenu to menu 1 of menu bar item {root} of menu bar 1\n"
            );
            let mut visible_path = vec![menu_path[0].clone()];
            for segment in &menu_path[1..] {
                visible_path.push(segment.clone());
                traversal.push_str(&format!(
                    "        set currentMenu to menu 1 of menu item {segment} of currentMenu\n"
                ));
            }
            let path_string = visible_path.join(" & \">\" & ");
            traversal.push_str(&format!(
                r#"        set outputLines to {{}}
        repeat with menuItemRef in menu items of currentMenu
            set itemTitle to my element_name(menuItemRef)
            set enabledText to my boolean_text(my attribute_bool(menuItemRef, "AXEnabled"))
            set hasSubmenuText to my boolean_text(my has_submenu(menuItemRef))
            set end of outputLines to ({path_string}) & fieldSeparator & itemTitle & fieldSeparator & enabledText & fieldSeparator & hasSubmenuText
        end repeat
        return my join_records(outputLines, recordSeparator)
"#
            ));
            traversal
        }
    };

    format!(
        r#"
set fieldSeparator to ASCII character 31
set recordSeparator to ASCII character 30

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
{traversal}
    end tell
end tell

{helpers}
"#,
        app = XCODE_APP_NAME,
        traversal = traversal,
        helpers = common_applescript_helpers()
    )
}

fn build_click_menu_item_script(menu_path: &[&str]) -> String {
    if menu_path.len() < 2 {
        return r#"error "menu path requires at least a menu bar item and one menu item""#
            .to_owned();
    }

    let segments = menu_path
        .iter()
        .map(|segment| applescript_string_literal(OsStr::new(segment)))
        .collect::<Vec<_>>();

    let root = &segments[0];
    let path_joined = segments.join(" & \">\" & ");
    let mut traversal =
        format!("        set currentMenu to menu 1 of menu bar item {root} of menu bar 1\n");
    for segment in &segments[1..segments.len() - 1] {
        traversal.push_str(&format!(
            "        set currentMenu to menu 1 of menu item {segment} of currentMenu\n"
        ));
    }
    let target = &segments[segments.len() - 1];

    format!(
        r#"
set fieldSeparator to ASCII character 31

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
{traversal}
        set targetItem to menu item {target} of currentMenu
        if not my attribute_bool(targetItem, "AXEnabled") then
            return "missing-action"
        end if
        click targetItem
        return "{app}" & fieldSeparator & "click-menu-item" & fieldSeparator & ({path_joined})
    end tell
end tell

{helpers}
"#,
        app = XCODE_APP_NAME,
        traversal = traversal,
        target = target,
        path_joined = path_joined,
        helpers = common_applescript_helpers()
    )
}

fn build_ui_elements_script(trace_path: Option<&Path>) -> String {
    let target_window = build_target_window_clause(trace_path, "not-running", false);
    format!(
        r#"
set fieldSeparator to ASCII character 31
set recordSeparator to ASCII character 30

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        {target_window}

        if targetWindow is missing value then
            return ""
        end if

        set outputLines to {{}}
        my collect_children(targetWindow, "Window", outputLines)
        return my join_records(outputLines, recordSeparator)
    end tell
end tell

{helpers}

on collect_children(parentElement, pathText, outputLines)
    try
        set childElements to UI elements of parentElement
    on error
        try
            set childElements to entire contents of parentElement
        on error
            set childElements to {{}}
        end try
    end try

    repeat with elem in childElements
        try
            set roleText to my role_text(elem)
            set titleText to my optional_text(my element_name(elem))
            set descriptionText to my optional_text(my attribute_text(elem, "AXDescription"))
            set identifierText to my optional_text(my attribute_text(elem, "AXIdentifier"))
            set enabledText to my optional_boolean_text(my optional_attribute_bool(elem, "AXEnabled"))
            set labelText to roleText
            if titleText is not "" then
                set labelText to roleText & "(" & titleText & ")"
            end if
            set childPath to pathText & ">" & labelText
            set end of outputLines to childPath & fieldSeparator & roleText & fieldSeparator & titleText & fieldSeparator & descriptionText & fieldSeparator & identifierText & fieldSeparator & enabledText
            my collect_children(elem, childPath, outputLines)
        end try
    end repeat
end collect_children
"#,
        app = XCODE_APP_NAME,
        target_window = target_window,
        helpers = common_applescript_helpers()
    )
}

fn build_click_button_script(trace_path: Option<&Path>, button_names: &[&str]) -> String {
    let target_window = build_target_window_clause(trace_path, "missing-window", false);
    let button_names = button_names
        .iter()
        .map(|name| applescript_string_literal(OsStr::new(name)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"
set fieldSeparator to ASCII character 31

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        {target_window}

        if targetWindow is missing value then
            return "missing-window"
        end if

        set windowTitle to my element_name(targetWindow)
        set allElements to {{}}
        try
            set allElements to entire contents of targetWindow
        end try

        repeat with requestedName in {{{button_names}}}
            repeat with elem in allElements
                try
                    if my role_text(elem) is "AXButton" then
                        set buttonName to my element_name(elem)
                        if buttonName is requestedName and my attribute_bool(elem, "AXEnabled") then
                            click elem
                            return windowTitle & fieldSeparator & "click-button" & fieldSeparator & buttonName
                        end if
                    end if
                end try
            end repeat
        end repeat
    end tell
end tell

return "missing-action"
"#,
        app = XCODE_APP_NAME,
        target_window = target_window,
        button_names = button_names
    )
}

fn build_close_window_script(trace_path: Option<&Path>) -> String {
    let target_window = build_target_window_clause(trace_path, "missing-window", false);
    format!(
        r#"
set fieldSeparator to ASCII character 31

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        {target_window}

        if targetWindow is missing value then
            return "missing-window"
        end if

        set windowTitle to my element_name(targetWindow)
        try
            click button 1 of targetWindow
            return windowTitle & fieldSeparator & "close-window" & fieldSeparator & windowTitle
        on error
        end try

        try
            perform action "AXClose" of targetWindow
            return windowTitle & fieldSeparator & "close-window" & fieldSeparator & windowTitle
        on error
            return "missing-action"
        end try
    end tell
end tell
"#,
        app = XCODE_APP_NAME,
        target_window = target_window
    )
}

fn build_checkbox_action_script(
    trace_path: Option<&Path>,
    checkbox_name: &str,
    mode: &str,
) -> String {
    let target_window = build_target_window_clause(trace_path, "missing-window", false);
    let checkbox_name = applescript_string_literal(OsStr::new(checkbox_name));
    let action = if mode == "ensure-checked" {
        r#"
                        if my attribute_bool(elem, "AXValue") then
                            return windowTitle & fieldSeparator & "ensure-checked" & fieldSeparator & requestedName
                        end if
                        click elem
                        delay 0.2
                        if my attribute_bool(elem, "AXValue") then
                            return windowTitle & fieldSeparator & "ensure-checked" & fieldSeparator & requestedName
                        end if
"#
    } else {
        r#"
                        click elem
                        delay 0.2
                        return windowTitle & fieldSeparator & "toggle-checkbox" & fieldSeparator & requestedName
"#
    };

    format!(
        r#"
set fieldSeparator to ASCII character 31

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        {target_window}

        if targetWindow is missing value then
            return "missing-window"
        end if

        set windowTitle to my element_name(targetWindow)
        set requestedName to {checkbox_name}
        set allElements to {{}}
        try
            set allElements to entire contents of targetWindow
        end try

        repeat with elem in allElements
            try
                if my role_text(elem) is "AXCheckBox" and my attribute_bool(elem, "AXEnabled") then
                    set itemName to my element_name(elem)
                    set itemDescription to my optional_text(my attribute_text(elem, "AXDescription"))
                    if itemName contains requestedName or requestedName contains itemName or itemDescription contains requestedName then
{action}                    end if
                end if
            end try
        end repeat
    end tell
end tell

return "missing-action"

{helpers}
"#,
        app = XCODE_APP_NAME,
        target_window = target_window,
        checkbox_name = checkbox_name,
        action = action,
        helpers = common_applescript_helpers()
    )
}

fn build_save_export_script(output_dir: &Path, output_name: &OsStr) -> String {
    let output_dir = applescript_string_literal(output_dir.as_os_str());
    let output_name = applescript_string_literal(output_name);
    format!(
        r#"
set fieldSeparator to ASCII character 31

tell application "{app}"
    activate
end tell

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        set frontmost to true

        set saveSheet to missing value
        repeat with attempt from 1 to 40
            try
                if exists sheet 1 of window 1 then
                    set saveSheet to sheet 1 of window 1
                    exit repeat
                end if
            end try
            delay 0.25
        end repeat

        if saveSheet is missing value then
            return "missing-window"
        end if

        set windowTitle to my element_name(window 1)

        keystroke "g" using {{command down, shift down}}
        delay 0.4
        keystroke {output_dir}
        delay 0.2
        key code 36
        delay 0.8

        keystroke "a" using {{command down}}
        delay 0.2
        keystroke {output_name}
        delay 0.3

        repeat with elem in entire contents of saveSheet
            try
                if my role_text(elem) is "AXCheckBox" then
                    set checkName to my element_name(elem)
                    if checkName contains "performance" or checkName contains "Embed" then
                        if not (my attribute_bool(elem, "AXValue")) then
                            click elem
                        end if
                    end if
                end if
            end try
        end repeat

        repeat with requestedName in {{"Save", "Export"}}
            repeat with elem in entire contents of saveSheet
                try
                    if my role_text(elem) is "AXButton" and my element_name(elem) is requestedName and my attribute_bool(elem, "AXEnabled") then
                        click elem
                        delay 0.5
                        repeat with replaceElem in entire contents of window 1
                            try
                                if my role_text(replaceElem) is "AXButton" and my element_name(replaceElem) is "Replace" and my attribute_bool(replaceElem, "AXEnabled") then
                                    click replaceElem
                                    exit repeat
                                end if
                            end try
                        end repeat
                        return windowTitle & fieldSeparator & "save-export" & fieldSeparator & {output_name}
                    end if
                end try
            end repeat
        end repeat
    end tell
end tell

return "missing-action"

{helpers}
"#,
        app = XCODE_APP_NAME,
        output_dir = output_dir,
        output_name = output_name,
        helpers = common_applescript_helpers()
    )
}

fn prepare_export_output_path(output_path: &Path) -> Result<PathBuf> {
    let output_path = output_path.to_path_buf();
    let parent = output_path.parent().ok_or_else(|| {
        Error::InvalidInput(format!(
            "export path has no parent directory: {}",
            output_path.display()
        ))
    })?;
    std::fs::create_dir_all(parent)?;
    if output_path.exists() {
        let metadata = output_path.metadata()?;
        if metadata.is_dir() {
            std::fs::remove_dir_all(&output_path)?;
        } else {
            std::fs::remove_file(&output_path)?;
        }
    }
    Ok(output_path)
}

fn finish_export_sheet(output_path: &Path, export_kind: &str) -> Result<String> {
    let parent = output_path.parent().ok_or_else(|| {
        Error::InvalidInput(format!(
            "export path has no parent directory: {}",
            output_path.display()
        ))
    })?;
    let file_name = output_path.file_name().ok_or_else(|| {
        Error::InvalidInput(format!(
            "export path has no file name: {}",
            output_path.display()
        ))
    })?;
    let raw = run_osascript(&build_save_export_script(parent, file_name))?;
    let action = parse_action_output(&raw)?;
    wait_for_export_path(output_path, Duration::from_secs(30))?;
    if action.target != file_name.to_string_lossy() {
        return Err(Error::InvalidInput(format!(
            "unexpected {export_kind} export target: {}",
            action.target
        )));
    }
    Ok(action.window_title)
}

fn export_profile_trace(
    trace_path: Option<&Path>,
    output_path: &Path,
) -> Result<XcodeExportResult> {
    let output_path = prepare_export_output_path(output_path)?;
    let _ = click_menu_item(&["File", "Export…"])
        .or_else(|_| click_menu_item(&["File", "Export..."]))
        .or_else(|_| click_menu_item(&["File", "Export"]))?;
    let window_title = finish_export_sheet(output_path.as_path(), "profile-trace")?;
    Ok(XcodeExportResult {
        window_title,
        export_kind: trace_path
            .and_then(Path::file_name)
            .map(|_| "profile-trace")
            .unwrap_or("profile-trace")
            .to_owned(),
        output_path,
    })
}

fn wait_for_export_path(output_path: &Path, timeout: Duration) -> Result<()> {
    let file_name = output_path
        .file_name()
        .ok_or_else(|| {
            Error::InvalidInput(format!("missing file name: {}", output_path.display()))
        })?
        .to_owned();
    let mut candidates = vec![output_path.to_path_buf()];
    if let Some(parent) = output_path.parent() {
        if let Ok(resolved_parent) = std::fs::canonicalize(parent) {
            let resolved = resolved_parent.join(&file_name);
            if resolved != output_path {
                candidates.push(resolved);
            }
        }
    }

    let found = wait_for_condition(timeout, Duration::from_millis(500), || {
        Ok(candidates.iter().any(|candidate| candidate.exists()))
    })?;

    if found {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "timed out waiting for export output: {}",
            output_path.display()
        )))
    }
}

fn build_select_tab_script(trace_path: Option<&Path>, tab_name: &str) -> String {
    let target_window = build_target_window_clause(trace_path, "missing-window", false);
    let tab_name = applescript_string_literal(OsStr::new(tab_name));
    format!(
        r#"
set fieldSeparator to ASCII character 31

tell application "System Events"
    if not (exists process "{app}") then
        return "not-running"
    end if

    tell process "{app}"
        {target_window}

        if targetWindow is missing value then
            return "missing-window"
        end if

        set windowTitle to my element_name(targetWindow)
        set allElements to {{}}
        try
            set allElements to entire contents of targetWindow
        end try

        repeat with elem in allElements
            try
                set roleName to my role_text(elem)
                set subroleText to my optional_text(my attribute_text(elem, "AXSubrole"))
                if (roleName is "AXRadioButton" or subroleText is "AXTabButton") and my element_name(elem) is {tab_name} then
                    if my attribute_bool(elem, "AXEnabled") then
                        click elem
                        return windowTitle & fieldSeparator & "select-tab" & fieldSeparator & {tab_name}
                    end if
                end if
            end try
        end repeat
    end tell
end tell

return "missing-action"
"#,
        app = XCODE_APP_NAME,
        target_window = target_window,
        tab_name = tab_name
    )
}

fn build_target_window_clause(
    trace_path: Option<&Path>,
    missing_result: &str,
    return_when_empty: bool,
) -> String {
    let title_filter = trace_path
        .and_then(Path::file_name)
        .map(applescript_string_literal);

    match title_filter {
        Some(filter) => format!(
            r#"
        set targetWindow to missing value
        repeat with candidateWindow in windows
            try
                set windowName to my element_name(candidateWindow)
                set documentName to my attribute_text(candidateWindow, "AXDocument")
                if windowName contains {filter} or documentName contains {filter} then
                    set targetWindow to candidateWindow
                    exit repeat
                end if
            end try
        end repeat
        if targetWindow is missing value then
            return "{missing_result}"
        end if
"#
        ),
        None if return_when_empty => format!(
            r#"
        if (count of windows) is 0 then
            return "{missing_result}"
        end if
        set targetWindow to window 1
"#
        ),
        None => format!(
            r#"
        if (count of windows) is 0 then
            set targetWindow to missing value
        else
            set targetWindow to window 1
        end if
"#
        ),
    }
}

fn common_applescript_helpers() -> String {
    r#"
on join_records(recordsList, recordSeparator)
    if (count of recordsList) is 0 then
        return ""
    end if
    set oldDelimiters to AppleScript's text item delimiters
    set AppleScript's text item delimiters to recordSeparator
    set joined to recordsList as text
    set AppleScript's text item delimiters to oldDelimiters
    return joined
end join_records

on sanitize_text(valueText)
    if valueText is missing value then
        return ""
    end if
    set textValue to valueText as text
    set textValue to my replace_text(textValue, ASCII character 31, " ")
    set textValue to my replace_text(textValue, ASCII character 30, " ")
    set textValue to my replace_text(textValue, linefeed, " ")
    set textValue to my replace_text(textValue, return, " ")
    return textValue
end sanitize_text

on replace_text(sourceText, findText, replaceText)
    set oldDelimiters to AppleScript's text item delimiters
    set AppleScript's text item delimiters to findText
    set textItems to every text item of sourceText
    set AppleScript's text item delimiters to replaceText
    set newText to textItems as text
    set AppleScript's text item delimiters to oldDelimiters
    return newText
end replace_text

on optional_text(valueText)
    if valueText is missing value then
        return ""
    end if
    return my sanitize_text(valueText)
end optional_text

on element_name(elementRef)
    try
        return my sanitize_text(name of elementRef)
    on error
        try
            return my sanitize_text(value of elementRef)
        on error
            return ""
        end try
    end try
end element_name

on element_text(elementRef)
    try
        return my sanitize_text(value of elementRef)
    on error
        try
            return my sanitize_text(name of elementRef)
        on error
            return ""
        end try
    end try
end element_text

on role_text(elementRef)
    try
        return my sanitize_text(value of attribute "AXRole" of elementRef)
    on error
        try
            return my sanitize_text(role of elementRef)
        on error
            return ""
        end try
    end try
end role_text

on attribute_text(elementRef, attributeName)
    try
        return my sanitize_text(value of attribute attributeName of elementRef)
    on error
        return ""
    end try
end attribute_text

on optional_attribute_bool(elementRef, attributeName)
    try
        return value of attribute attributeName of elementRef
    on error
        return missing value
    end try
end optional_attribute_bool

on attribute_bool(elementRef, attributeName)
    set valueText to my optional_attribute_bool(elementRef, attributeName)
    if valueText is missing value then
        return false
    end if
    return valueText as boolean
end attribute_bool

on boolean_text(flag)
    if flag then
        return "true"
    end if
    return "false"
end boolean_text

on optional_boolean_text(flag)
    if flag is missing value then
        return ""
    end if
    return my boolean_text(flag as boolean)
end optional_boolean_text

on has_submenu(menuItemRef)
    try
        set ignoredMenu to menu 1 of menuItemRef
        return true
    on error
        return false
    end try
end has_submenu
"#
    .to_owned()
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

fn parse_windows_output(raw: &str) -> Result<Vec<XcodeWindowInfo>> {
    parse_records(raw, 7)?
        .into_iter()
        .map(|columns| {
            Ok(XcodeWindowInfo {
                title: columns[0].clone(),
                document: optional_string(&columns[1]),
                role: columns[2].clone(),
                subrole: optional_string(&columns[3]),
                focused: parse_bool(&columns[4])?,
                main: parse_bool(&columns[5])?,
                modal: parse_bool(&columns[6])?,
            })
        })
        .collect()
}

fn parse_window_snapshot_output(raw: &str) -> Result<Option<XcodeWindowSnapshot>> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("not-running")
        || trimmed.eq_ignore_ascii_case("missing-window")
    {
        return Ok(None);
    }

    let columns = parse_single_record(raw, 11)?;
    Ok(Some(XcodeWindowSnapshot {
        window: XcodeWindowInfo {
            title: columns[0].clone(),
            document: optional_string(&columns[1]),
            role: columns[2].clone(),
            subrole: optional_string(&columns[3]),
            focused: parse_bool(&columns[4])?,
            main: parse_bool(&columns[5])?,
            modal: parse_bool(&columns[6])?,
        },
        button_count: parse_usize(&columns[7])?,
        tab_count: parse_usize(&columns[8])?,
        toolbar_count: parse_usize(&columns[9])?,
        status: parse_status(&columns[10]),
    }))
}

fn parse_buttons_output(raw: &str) -> Result<Vec<XcodeButtonInfo>> {
    parse_records(raw, 4)?
        .into_iter()
        .map(|columns| {
            Ok(XcodeButtonInfo {
                window_title: columns[0].clone(),
                name: columns[1].clone(),
                description: optional_string(&columns[2]),
                enabled: parse_bool(&columns[3])?,
            })
        })
        .collect()
}

fn parse_checkboxes_output(raw: &str) -> Result<Vec<XcodeCheckboxInfo>> {
    parse_records(raw, 5)?
        .into_iter()
        .map(|columns| {
            Ok(XcodeCheckboxInfo {
                window_title: columns[0].clone(),
                name: columns[1].clone(),
                description: optional_string(&columns[2]),
                checked: parse_bool(&columns[3])?,
                enabled: parse_bool(&columns[4])?,
            })
        })
        .collect()
}

fn parse_tabs_output(raw: &str) -> Result<Vec<XcodeTabInfo>> {
    parse_records(raw, 6)?
        .into_iter()
        .map(|columns| {
            Ok(XcodeTabInfo {
                window_title: columns[0].clone(),
                role: columns[1].clone(),
                subrole: optional_string(&columns[2]),
                name: columns[3].clone(),
                selected: parse_bool(&columns[4])?,
                enabled: parse_bool(&columns[5])?,
            })
        })
        .collect()
}

fn parse_menu_items_output(raw: &str) -> Result<Vec<XcodeMenuItemInfo>> {
    parse_records(raw, 4)?
        .into_iter()
        .map(|columns| {
            Ok(XcodeMenuItemInfo {
                menu_path: columns[0].split('>').map(ToOwned::to_owned).collect(),
                title: columns[1].clone(),
                enabled: parse_bool(&columns[2])?,
                has_submenu: parse_bool(&columns[3])?,
            })
        })
        .collect()
}

fn parse_ui_elements_output(raw: &str) -> Result<Vec<XcodeUiElementInfo>> {
    parse_records(raw, 6)?
        .into_iter()
        .map(|columns| {
            Ok(XcodeUiElementInfo {
                path: columns[0].split('>').map(ToOwned::to_owned).collect(),
                role: columns[1].clone(),
                title: optional_string(&columns[2]),
                description: optional_string(&columns[3]),
                identifier: optional_string(&columns[4]),
                enabled: optional_bool(&columns[5])?,
            })
        })
        .collect()
}

fn parse_action_output(raw: &str) -> Result<XcodeActionResult> {
    let trimmed = raw.trim();
    if matches!(trimmed, "not-running" | "missing-window" | "missing-action") {
        return Err(Error::InvalidInput(format!(
            "Xcode automation action failed: {trimmed}"
        )));
    }

    let columns = parse_single_record(raw, 3)?;
    Ok(XcodeActionResult {
        window_title: columns[0].clone(),
        action: columns[1].clone(),
        target: columns[2].clone(),
    })
}

fn parse_records(raw: &str, expected_columns: usize) -> Result<Vec<Vec<String>>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("not-running") {
        return Ok(Vec::new());
    }

    trimmed
        .split(RECORD_SEPARATOR)
        .filter(|record| !record.is_empty())
        .map(|record| {
            let columns = record
                .split(FIELD_SEPARATOR)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            if columns.len() != expected_columns {
                return Err(Error::InvalidInput(format!(
                    "expected {expected_columns} columns but got {} in automation output",
                    columns.len()
                )));
            }
            Ok(columns)
        })
        .collect()
}

fn parse_single_record(raw: &str, expected_columns: usize) -> Result<Vec<String>> {
    let mut rows = parse_records(raw, expected_columns)?;
    if rows.len() != 1 {
        return Err(Error::InvalidInput(format!(
            "expected 1 record but got {} in automation output",
            rows.len()
        )));
    }
    Ok(rows.remove(0))
}

fn parse_bool(raw: &str) -> Result<bool> {
    match raw {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(Error::InvalidInput(format!(
            "unexpected boolean value in automation output: {other}"
        ))),
    }
}

fn optional_bool(raw: &str) -> Result<Option<bool>> {
    if raw.is_empty() {
        return Ok(None);
    }
    parse_bool(raw).map(Some)
}

fn parse_usize(raw: &str) -> Result<usize> {
    raw.parse::<usize>().map_err(|_| {
        Error::InvalidInput(format!(
            "unexpected integer value in automation output: {raw}"
        ))
    })
}

fn optional_string(raw: &str) -> Option<String> {
    (!raw.is_empty()).then(|| raw.to_owned())
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

    fn record(columns: &[&str]) -> String {
        columns.join(&FIELD_SEPARATOR.to_string())
    }

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

    #[test]
    fn build_windows_script_includes_helper_surface() {
        let script = build_windows_script();
        assert!(script.contains("AXDocument"));
        assert!(script.contains("AXSubrole"));
        assert!(script.contains("join_records"));
    }

    #[test]
    fn build_buttons_script_targets_named_trace() {
        let script = build_buttons_script(Some(Path::new("/tmp/Profile Trace.gputrace")));
        assert!(script.contains("Profile Trace.gputrace"));
        assert!(script.contains("AXButton"));
        assert!(script.contains("AXDescription"));
    }

    #[test]
    fn build_checkboxes_script_targets_named_trace() {
        let script = build_checkboxes_script(Some(Path::new("/tmp/Profile Trace.gputrace")));
        assert!(script.contains("Profile Trace.gputrace"));
        assert!(script.contains("AXCheckBox"));
        assert!(script.contains("AXDescription"));
    }

    #[test]
    fn build_menu_items_script_supports_nested_paths() {
        let script = build_menu_items_script(&["Editor", "Performance"]);
        assert!(script.contains("menu bar item \"Editor\" of menu bar 1"));
        assert!(script.contains("menu item \"Performance\" of currentMenu"));
    }

    #[test]
    fn parse_windows_output_decodes_window_rows() {
        let raw = record(&[
            "Trace A",
            "/tmp/Trace A.gputrace",
            "AXWindow",
            "AXStandardWindow",
            "true",
            "true",
            "false",
        ]);
        let windows = parse_windows_output(&raw).unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].title, "Trace A");
        assert_eq!(
            windows[0].document.as_deref(),
            Some("/tmp/Trace A.gputrace")
        );
        assert!(windows[0].focused);
        assert!(windows[0].main);
        assert!(!windows[0].modal);
    }

    #[test]
    fn parse_window_snapshot_output_decodes_counts_and_status() {
        let raw = record(&[
            "Trace A", "", "AXWindow", "", "false", "true", "false", "12", "3", "1", "running",
        ]);
        let snapshot = parse_window_snapshot_output(&raw).unwrap().unwrap();
        assert_eq!(snapshot.window.title, "Trace A");
        assert_eq!(snapshot.button_count, 12);
        assert_eq!(snapshot.tab_count, 3);
        assert_eq!(snapshot.toolbar_count, 1);
        assert_eq!(snapshot.status, XcodeAutomationStatus::Running);
    }

    #[test]
    fn parse_buttons_output_decodes_optional_description() {
        let raw = record(&["Trace A", "Replay", "", "true"]);
        let buttons = parse_buttons_output(&raw).unwrap();
        assert_eq!(buttons.len(), 1);
        assert_eq!(buttons[0].name, "Replay");
        assert_eq!(buttons[0].description, None);
        assert!(buttons[0].enabled);
    }

    #[test]
    fn parse_checkboxes_output_decodes_checked_state() {
        let raw = record(&["Trace A", "Profile after replay", "", "true", "true"]);
        let checkboxes = parse_checkboxes_output(&raw).unwrap();
        assert_eq!(checkboxes.len(), 1);
        assert_eq!(checkboxes[0].name, "Profile after replay");
        assert!(checkboxes[0].checked);
        assert!(checkboxes[0].enabled);
    }

    #[test]
    fn parse_tabs_output_decodes_selection() {
        let raw = record(&[
            "Trace A",
            "AXRadioButton",
            "AXTabButton",
            "Counters",
            "true",
            "true",
        ]);
        let tabs = parse_tabs_output(&raw).unwrap();
        assert_eq!(tabs.len(), 1);
        assert_eq!(tabs[0].name, "Counters");
        assert!(tabs[0].selected);
        assert_eq!(tabs[0].subrole.as_deref(), Some("AXTabButton"));
    }

    #[test]
    fn build_click_button_script_targets_named_trace() {
        let script =
            build_click_button_script(Some(Path::new("/tmp/Profile Trace.gputrace")), &["Replay"]);
        assert!(script.contains("Profile Trace.gputrace"));
        assert!(script.contains("\"Replay\""));
        assert!(script.contains("click-button"));
    }

    #[test]
    fn build_select_tab_script_targets_named_tab() {
        let script =
            build_select_tab_script(Some(Path::new("/tmp/Profile Trace.gputrace")), "Counters");
        assert!(script.contains("Profile Trace.gputrace"));
        assert!(script.contains("\"Counters\""));
        assert!(script.contains("select-tab"));
    }

    #[test]
    fn build_click_menu_item_script_traverses_nested_menus() {
        let script = build_click_menu_item_script(&["Editor", "Export GPU Counters..."]);
        assert!(script.contains("menu bar item \"Editor\" of menu bar 1"));
        assert!(script.contains("menu item \"Export GPU Counters...\" of currentMenu"));
        assert!(script.contains("click-menu-item"));
    }

    #[test]
    fn build_close_window_script_targets_named_trace() {
        let script = build_close_window_script(Some(Path::new("/tmp/Profile Trace.gputrace")));
        assert!(script.contains("Profile Trace.gputrace"));
        assert!(script.contains("close-window"));
    }

    #[test]
    fn build_checkbox_action_script_targets_requested_checkbox() {
        let script = build_checkbox_action_script(
            Some(Path::new("/tmp/Profile Trace.gputrace")),
            "Profile after replay",
            "ensure-checked",
        );
        assert!(script.contains("Profile Trace.gputrace"));
        assert!(script.contains("Profile after replay"));
        assert!(script.contains("ensure-checked"));
    }

    #[test]
    fn build_save_export_script_includes_output_location() {
        let script = build_save_export_script(Path::new("/tmp/out"), OsStr::new("trace.gputrace"));
        assert!(script.contains("/tmp/out"));
        assert!(script.contains("trace.gputrace"));
        assert!(script.contains("save-export"));
    }

    #[test]
    fn parse_action_output_decodes_result() {
        let raw = record(&["Trace A", "click-button", "Replay"]);
        let result = parse_action_output(&raw).unwrap();
        assert_eq!(result.window_title, "Trace A");
        assert_eq!(result.action, "click-button");
        assert_eq!(result.target, "Replay");
    }

    #[test]
    fn parse_menu_items_output_splits_menu_path() {
        let raw = record(&["Editor>Performance", "Counters", "true", "false"]);
        let items = parse_menu_items_output(&raw).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].menu_path, vec!["Editor", "Performance"]);
        assert_eq!(items[0].title, "Counters");
        assert!(items[0].enabled);
        assert!(!items[0].has_submenu);
    }

    #[test]
    fn parse_ui_elements_output_decodes_tree_paths() {
        let raw = record(&[
            "Window>AXGroup(Editor)>AXButton(Replay)",
            "AXButton",
            "Replay",
            "",
            "replayButton",
            "true",
        ]);
        let elements = parse_ui_elements_output(&raw).unwrap();
        assert_eq!(elements.len(), 1);
        assert_eq!(
            elements[0].path,
            vec!["Window", "AXGroup(Editor)", "AXButton(Replay)"]
        );
        assert_eq!(elements[0].identifier.as_deref(), Some("replayButton"));
        assert_eq!(elements[0].enabled, Some(true));
    }

    #[test]
    fn parse_records_rejects_wrong_column_count() {
        let err = parse_records("one\u{1f}two", 3).unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)));
    }

    #[test]
    fn parse_window_snapshot_output_handles_missing_window() {
        assert_eq!(
            parse_window_snapshot_output("missing-window").unwrap(),
            None
        );
        assert_eq!(parse_window_snapshot_output("not-running").unwrap(), None);
    }

    #[test]
    fn default_profile_output_path_appends_perfdata_suffix() {
        let output = default_profile_output_path(Path::new("/tmp/My Trace.gputrace"));
        assert_eq!(output, Path::new("/tmp/My Trace-perfdata.gputrace"));
    }
}
