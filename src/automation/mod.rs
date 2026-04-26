#![allow(dead_code)]

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;
use tracing::{info, warn};

use crate::error::{Error, Result};
use crate::profiler;

#[cfg(target_os = "macos")]
mod ax;

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
    pub prompt_for_permissions: bool,
    pub wait_for_running_profile_seconds: u64,
    pub force: bool,
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
    pub current_tab: Option<String>,
    pub available_actions: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XcodePermissionReport {
    pub accessibility_granted: bool,
    pub xcode_running: bool,
    pub xcode_probe_ok: bool,
    pub prompt_opened: bool,
}

pub fn check_accessibility_permissions(prompt: bool) -> Result<XcodePermissionReport> {
    ax::check_accessibility_permissions(prompt)
}

pub fn activate_xcode() -> Result<()> {
    ax::activate_xcode()
}

pub fn open_accessibility_preferences() -> Result<()> {
    let output = Command::new("open")
        .arg("x-apple.systempreferences:com.apple.settings.PrivacySecurity.extension?Privacy_Accessibility")
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(command_output_error(
            "open",
            &["Accessibility settings"],
            &output,
        ))
    }
}

pub fn list_windows() -> Result<Vec<XcodeWindowInfo>> {
    ax::list_windows()
}

pub fn inspect_window(trace_path: Option<&Path>) -> Result<Option<XcodeWindowSnapshot>> {
    ax::inspect_window(trace_path)
}

pub fn list_buttons(trace_path: Option<&Path>) -> Result<Vec<XcodeButtonInfo>> {
    ax::list_buttons(trace_path)
}

pub fn list_checkboxes(trace_path: Option<&Path>) -> Result<Vec<XcodeCheckboxInfo>> {
    ax::list_checkboxes(trace_path)
}

pub fn list_tabs(trace_path: Option<&Path>) -> Result<Vec<XcodeTabInfo>> {
    ax::list_tabs(trace_path)
}

pub fn list_menu_items(menu_path: &[&str]) -> Result<Vec<XcodeMenuItemInfo>> {
    ax::list_menu_items(menu_path)
}

pub fn list_ui_elements(trace_path: Option<&Path>) -> Result<Vec<XcodeUiElementInfo>> {
    ax::list_ui_elements(trace_path)
}

pub fn click_button(trace_path: Option<&Path>, button_names: &[&str]) -> Result<XcodeActionResult> {
    ax::click_button(trace_path, button_names)
}

pub fn select_tab(trace_path: Option<&Path>, tab_name: &str) -> Result<XcodeActionResult> {
    ax::select_tab(trace_path, tab_name)
}

pub fn click_menu_item(menu_path: &[&str]) -> Result<XcodeActionResult> {
    ax::click_menu_item(menu_path)
}

pub fn close_window(trace_path: Option<&Path>) -> Result<XcodeActionResult> {
    ax::close_window(trace_path)
}

pub fn ensure_checked(trace_path: Option<&Path>, checkbox_name: &str) -> Result<XcodeActionResult> {
    ax::ensure_checked(trace_path, checkbox_name)
}

pub fn toggle_checkbox(
    trace_path: Option<&Path>,
    checkbox_name: &str,
) -> Result<XcodeActionResult> {
    ax::toggle_checkbox(trace_path, checkbox_name)
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
    info!(
        trace_path = ?trace_path,
        output_path = %output_path.display(),
        "xcode export counters requested"
    );
    let output_path = prepare_export_output_path(output_path)?;
    let trace_path = trace_path.map(validate_trace_path).transpose()?;

    info!("xcode export counters: show performance tab");
    let _ = show_performance(trace_path.as_deref());
    info!("xcode export counters: show counters tab");
    let _ = show_counters(trace_path.as_deref());
    info!("xcode export counters: open export counters menu item");
    let _ = click_menu_item(&["Editor", "Export GPU Counters…"])
        .or_else(|_| click_menu_item(&["Editor", "Export GPU Counters..."]))
        .or_else(|_| click_menu_item(&["Editor", "Export GPU Counters"]))?;
    let window_title =
        finish_export_sheet(output_path.as_path(), trace_path.as_deref(), "gpu-counters")?;

    Ok(XcodeExportResult {
        window_title,
        export_kind: "gpu-counters".to_owned(),
        output_path,
    })
}

pub fn export_memory(trace_path: Option<&Path>, output_path: &Path) -> Result<XcodeExportResult> {
    info!(
        trace_path = ?trace_path,
        output_path = %output_path.display(),
        "xcode export memory requested"
    );
    let output_path = prepare_export_output_path(output_path)?;
    let trace_path = trace_path.map(validate_trace_path).transpose()?;

    info!("xcode export memory: show performance tab");
    let _ = show_performance(trace_path.as_deref());
    info!("xcode export memory: show memory tab");
    let _ = show_memory(trace_path.as_deref());
    info!("xcode export memory: open export memory menu item");
    let _ = click_menu_item(&["Editor", "Export Memory Report…"])
        .or_else(|_| click_menu_item(&["Editor", "Export Memory Report..."]))
        .or_else(|_| click_menu_item(&["Editor", "Export Memory Report"]))?;
    let window_title = finish_export_sheet(
        output_path.as_path(),
        trace_path.as_deref(),
        "memory-report",
    )?;

    Ok(XcodeExportResult {
        window_title,
        export_kind: "memory-report".to_owned(),
        output_path,
    })
}

pub fn export_profile(trace_path: Option<&Path>, output_path: &Path) -> Result<XcodeExportResult> {
    export_profile_trace(trace_path, output_path)
}

pub fn run_profile(request: &XcodeProfileRun) -> Result<XcodeExportResult> {
    info!(request = ?request, "xcode profile run requested");
    let permissions = check_accessibility_permissions(request.prompt_for_permissions)?;
    info!(permissions = ?permissions, "xcode profile permissions checked");
    if !permissions.accessibility_granted {
        return Err(Error::InvalidInput(
            "Accessibility permission is required for Xcode automation. Grant access in System Settings > Privacy & Security > Accessibility and retry.".to_owned(),
        ));
    }
    info!("xcode profile: wait for any existing profile to finish");
    wait_for_running_profile(
        Duration::from_secs(request.wait_for_running_profile_seconds),
        request.force,
    )?;

    info!(trace_path = %request.trace_path.display(), "xcode profile: validate trace");
    validate_trace_path(&request.trace_path)?;
    let output_path = request
        .output_path
        .clone()
        .unwrap_or_else(|| default_profile_output_path(&request.trace_path));
    info!(output_path = %output_path.display(), "xcode profile: resolved output path");

    info!(trace_path = %request.trace_path.display(), "xcode profile: open trace in Xcode");
    open_trace_in_xcode_with_options(
        &request.trace_path,
        OpenTraceOptions {
            launch_mode: XcodeLaunchMode::Foreground,
            wait_for_window: true,
            timeout: Duration::from_secs(request.timeout_seconds.max(1)),
        },
    )?;

    info!("xcode profile: dismiss startup dialogs");
    let _ = dismiss_startup_dialogs();
    let trace_path = Some(request.trace_path.as_path());
    let status = get_window_status(trace_path)?;
    info!(status = ?status, "xcode profile: initial window status");
    if !matches!(
        status.status,
        XcodeAutomationStatus::ReplayReady
            | XcodeAutomationStatus::Complete
            | XcodeAutomationStatus::Running
    ) {
        info!("xcode profile: wait for ready/running/complete status");
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
    info!(status = ?status, "xcode profile: status before replay/profile decision");
    match status.status {
        XcodeAutomationStatus::ReplayReady => {
            info!("xcode profile: clicking Profile/Replay");
            let _ = click_button(trace_path, &["Profile", "Replay"])?;
            info!("xcode profile: waiting for completion after replay");
            let _ = wait_for_status(
                Duration::from_secs(request.timeout_seconds.max(1)),
                trace_path,
                &[XcodeAutomationStatus::Complete],
            )?;
        }
        XcodeAutomationStatus::Running => {
            info!("xcode profile: already running, waiting for completion");
            let _ = wait_for_status(
                Duration::from_secs(request.timeout_seconds.max(1)),
                trace_path,
                &[XcodeAutomationStatus::Complete],
            )?;
        }
        XcodeAutomationStatus::Complete => {
            info!("xcode profile: already complete");
        }
        _ => {
            return Err(Error::InvalidInput(format!(
                "Xcode trace window is not ready to profile: {}",
                status.raw
            )));
        }
    }

    info!("xcode profile: show performance");
    let _ = show_performance(trace_path);

    info!("xcode profile: export profiled trace");
    let export = export_profile_trace(trace_path, &output_path)?;
    info!("xcode profile: close trace window");
    let _ = close_window(trace_path);
    Ok(export)
}

pub fn wait_for_running_profile(timeout: Duration, force: bool) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut first_attempt = true;
    loop {
        let Some(window_title) = running_profile_window()? else {
            return Ok(());
        };

        if force {
            return Ok(());
        }

        if timeout.is_zero() || Instant::now() >= deadline {
            if timeout.is_zero() {
                return Err(Error::InvalidInput(format!(
                    "profiling is running in \"{window_title}\". Use --wait-seconds to wait or --force to proceed anyway"
                )));
            }
            return Err(Error::InvalidInput(format!(
                "timed out waiting for profiling to complete in \"{window_title}\""
            )));
        }

        if first_attempt {
            first_attempt = false;
        }
        thread::sleep(Duration::from_secs(2));
    }
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
    ax::get_window_status(trace_path)
}

pub fn dismiss_startup_dialogs() -> Result<bool> {
    ax::dismiss_startup_dialogs()
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

fn prepare_export_output_path(output_path: &Path) -> Result<PathBuf> {
    let output_path = output_path.to_path_buf();
    info!(output_path = %output_path.display(), "prepare export output path");
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
            info!(path = %output_path.display(), "remove existing export directory");
            std::fs::remove_dir_all(&output_path)?;
        } else {
            info!(path = %output_path.display(), "remove existing export file");
            std::fs::remove_file(&output_path)?;
        }
    }
    Ok(output_path)
}

fn finish_export_sheet(
    output_path: &Path,
    trace_path: Option<&Path>,
    export_kind: &str,
) -> Result<String> {
    info!(
        output_path = %output_path.display(),
        trace_path = ?trace_path,
        export_kind,
        "finish export sheet wrapper: begin"
    );
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
    info!(
        parent = %parent.display(),
        file_name = ?file_name,
        "finish export sheet wrapper: resolved parent and file name"
    );
    let action = ax::finish_export_sheet(parent, file_name, trace_path)?;
    info!(action = ?action, "finish export sheet wrapper: AX action returned");
    let actual_output = wait_for_export_path(output_path, trace_path, Duration::from_secs(30))?;
    info!(actual_output = %actual_output.display(), "finish export sheet wrapper: export path found");
    if export_kind == "profile-trace" {
        info!("finish export sheet wrapper: wait for complete profile export");
        wait_for_complete_profile_export(&actual_output, Duration::from_secs(300))?;
    }
    if actual_output != output_path {
        info!(
            from = %actual_output.display(),
            to = %output_path.display(),
            "finish export sheet wrapper: copy exported path to requested path"
        );
        copy_path(&actual_output, output_path)?;
    }
    if export_kind == "profile-trace" {
        info!("finish export sheet wrapper: validate complete profile export");
        validate_complete_profile_export(output_path)?;
    }
    if action.target != file_name.to_string_lossy() {
        return Err(Error::InvalidInput(format!(
            "unexpected {export_kind} export target: {}",
            action.target
        )));
    }
    Ok(action.window_title)
}

fn wait_for_complete_profile_export(output_path: &Path, timeout: Duration) -> Result<()> {
    info!(
        output_path = %output_path.display(),
        timeout_ms = timeout.as_millis(),
        "wait for complete profile export"
    );
    let complete = wait_for_condition(timeout, Duration::from_millis(500), || {
        Ok(is_complete_profile_export(output_path))
    })?;
    if complete {
        info!(output_path = %output_path.display(), "profile export is complete");
        Ok(())
    } else {
        warn!(output_path = %output_path.display(), "profile export stayed incomplete");
        Err(Error::InvalidInput(format!(
            "profile export stayed incomplete: {} (expected capture/unsorted-capture or store0/index/metadata, plus .gpuprofiler_raw/streamData)",
            output_path.display()
        )))
    }
}

fn validate_complete_profile_export(output_path: &Path) -> Result<()> {
    if is_complete_profile_export(output_path) {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "profile export is incomplete after copy: {} (expected capture/unsorted-capture or store0/index/metadata, plus .gpuprofiler_raw/streamData)",
            output_path.display()
        )))
    }
}

fn is_complete_profile_export(output_path: &Path) -> bool {
    let has_capture =
        output_path.join("capture").is_file() || output_path.join("unsorted-capture").is_file();
    let has_store_layout = output_path.join("store0").is_file()
        && output_path.join("index").is_file()
        && output_path.join("metadata").is_file();
    let has_stream_data = profiler::find_profiler_directory(output_path)
        .is_some_and(|path| path.join("streamData").is_file());
    info!(
        output_path = %output_path.display(),
        has_capture,
        has_store_layout,
        has_stream_data,
        "checked complete profile export"
    );
    (has_capture || has_store_layout) && has_stream_data
}

fn export_profile_trace(
    trace_path: Option<&Path>,
    output_path: &Path,
) -> Result<XcodeExportResult> {
    info!(
        trace_path = ?trace_path,
        output_path = %output_path.display(),
        "xcode profile export requested"
    );
    let output_path = prepare_export_output_path(output_path)?;
    info!("xcode profile export: show summary");
    let _ = show_summary(trace_path);
    info!("xcode profile export: click Export or File > Export");
    let _ = click_button(trace_path, &["Export"])
        .or_else(|_| click_menu_item(&["File", "Export…"]))
        .or_else(|_| click_menu_item(&["File", "Export..."]))
        .or_else(|_| click_menu_item(&["File", "Export"]))?;
    info!("xcode profile export: finish save sheet");
    let window_title = finish_export_sheet(output_path.as_path(), trace_path, "profile-trace")?;
    Ok(XcodeExportResult {
        window_title,
        export_kind: "profile-trace".to_owned(),
        output_path,
    })
}

fn wait_for_export_path(
    output_path: &Path,
    trace_path: Option<&Path>,
    timeout: Duration,
) -> Result<PathBuf> {
    let candidates = export_output_candidates(output_path, trace_path)?;
    info!(
        output_path = %output_path.display(),
        candidates = ?candidates,
        timeout_ms = timeout.as_millis(),
        "wait for export path"
    );
    let mut found_path = None;
    let found = wait_for_condition(timeout, Duration::from_millis(500), || {
        found_path = candidates
            .iter()
            .find(|candidate| candidate.exists())
            .cloned();
        Ok(found_path.is_some())
    })?;

    if found {
        info!(found_path = ?found_path, "export path appeared");
        found_path.ok_or_else(|| {
            Error::InvalidInput(format!(
                "export output was reported present but no path was captured: {}",
                output_path.display()
            ))
        })
    } else {
        warn!(output_path = %output_path.display(), candidates = ?candidates, "timed out waiting for export path");
        Err(Error::InvalidInput(format!(
            "timed out waiting for export output: {} (also checked {})",
            output_path.display(),
            candidates
                .iter()
                .map(|candidate| candidate.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }
}

fn export_output_candidates(output_path: &Path, trace_path: Option<&Path>) -> Result<Vec<PathBuf>> {
    export_output_candidates_with_home(
        output_path,
        trace_path,
        std::env::var_os("HOME").map(PathBuf::from),
        Some(std::env::temp_dir()),
    )
}

fn export_output_candidates_with_home(
    output_path: &Path,
    trace_path: Option<&Path>,
    home: Option<PathBuf>,
    temp_dir: Option<PathBuf>,
) -> Result<Vec<PathBuf>> {
    let file_name = output_path
        .file_name()
        .ok_or_else(|| {
            Error::InvalidInput(format!("missing file name: {}", output_path.display()))
        })?
        .to_owned();
    let mut candidates = vec![output_path.to_path_buf()];
    if let Some(parent) = output_path.parent()
        && let Ok(resolved_parent) = std::fs::canonicalize(parent)
    {
        let resolved = resolved_parent.join(&file_name);
        if !candidates.contains(&resolved) {
            candidates.push(resolved);
        }
    }
    if let Some(trace_path) = trace_path
        && let Some(input_dir) = trace_path.parent()
    {
        let sibling = input_dir.join(&file_name);
        if !candidates.contains(&sibling) {
            candidates.push(sibling.clone());
        }
        if let Ok(resolved_input_dir) = std::fs::canonicalize(input_dir) {
            let resolved = resolved_input_dir.join(&file_name);
            if !candidates.contains(&resolved) {
                candidates.push(resolved);
            }
        }
    }
    if let Some(home) = home {
        for candidate in [
            home.join("Downloads").join(&file_name),
            home.join("Desktop").join(&file_name),
        ] {
            if !candidates.contains(&candidate) {
                candidates.push(candidate);
            }
        }
    }
    if let Some(temp_dir) = temp_dir {
        let candidate = temp_dir.join(&file_name);
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }
    Ok(candidates)
}

fn copy_path(source: &Path, destination: &Path) -> Result<()> {
    if source.is_dir() {
        std::fs::create_dir_all(destination)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            let source_path = entry.path();
            let dest_path = destination.join(entry.file_name());
            copy_path(&source_path, &dest_path)?;
        }
        return Ok(());
    }

    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(source, destination)?;
    Ok(())
}

fn xcode_window_count() -> Result<u32> {
    Ok(ax::list_windows()?.len().try_into().unwrap_or(u32::MAX))
}

fn running_profile_window() -> Result<Option<String>> {
    ax::running_profile_window()
}

fn is_xcode_running() -> Result<bool> {
    let output = Command::new("pgrep").arg("-x").arg("Xcode").output()?;
    Ok(output.status.success())
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
    fn default_profile_output_path_appends_perfdata_suffix() {
        let output = default_profile_output_path(Path::new("/tmp/My Trace.gputrace"));
        assert_eq!(output, Path::new("/tmp/My Trace-perfdata.gputrace"));
    }

    #[test]
    fn export_output_candidates_include_trace_dir_and_home_fallbacks() {
        let temp_home = tempfile::tempdir().unwrap();
        let output = Path::new("/tmp/out/trace.gputrace");
        let trace = Path::new("/captures/input/trace.gputrace");
        let candidates = export_output_candidates_with_home(
            output,
            Some(trace),
            Some(temp_home.path().to_path_buf()),
            Some(PathBuf::from("/private/tmp")),
        )
        .unwrap();
        assert!(candidates.contains(&PathBuf::from("/tmp/out/trace.gputrace")));
        assert!(candidates.contains(&PathBuf::from("/captures/input/trace.gputrace")));
        assert!(candidates.contains(&temp_home.path().join("Downloads").join("trace.gputrace")));
        assert!(candidates.contains(&temp_home.path().join("Desktop").join("trace.gputrace")));
        assert!(candidates.contains(&PathBuf::from("/private/tmp/trace.gputrace")));
    }

    #[test]
    fn copy_path_copies_directories_recursively() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.gputrace");
        let nested = source.join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(source.join("root.txt"), b"root").unwrap();
        std::fs::write(nested.join("leaf.txt"), b"leaf").unwrap();

        let destination = dir.path().join("dest.gputrace");
        copy_path(&source, &destination).unwrap();

        assert_eq!(
            std::fs::read(destination.join("root.txt")).unwrap(),
            b"root"
        );
        assert_eq!(
            std::fs::read(destination.join("nested").join("leaf.txt")).unwrap(),
            b"leaf"
        );
    }

    #[test]
    fn profile_export_completion_accepts_capture_or_store_layout_with_stream_data() {
        let dir = tempfile::tempdir().unwrap();
        let export = dir.path().join("trace-perfdata.gputrace");
        std::fs::create_dir_all(&export).unwrap();
        std::fs::write(export.join("index"), b"index").unwrap();
        std::fs::write(export.join("metadata"), b"metadata").unwrap();
        std::fs::write(export.join("store0"), b"packed").unwrap();

        assert!(!is_complete_profile_export(&export));

        std::fs::write(export.join("capture"), b"capture").unwrap();
        assert!(!is_complete_profile_export(&export));

        let profiler_dir = export.join("trace.gputrace.gpuprofiler_raw");
        std::fs::create_dir_all(&profiler_dir).unwrap();
        std::fs::write(profiler_dir.join("streamData"), b"stream").unwrap();

        assert!(is_complete_profile_export(&export));

        std::fs::remove_file(export.join("capture")).unwrap();
        assert!(is_complete_profile_export(&export));

        std::fs::remove_file(export.join("store0")).unwrap();
        assert!(!is_complete_profile_export(&export));
    }
}
