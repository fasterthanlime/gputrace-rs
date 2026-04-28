use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use serde::Serialize;

use crate::error::{Error, Result};

const MTL_REPLAYER_APP: &str = "/System/Library/CoreServices/MTLReplayer.app";
const MTL_REPLAYER_BIN: &str =
    "/System/Library/CoreServices/MTLReplayer.app/Contents/MacOS/MTLReplayer";

/// Configuration for a single MTLReplayer profile run.
#[derive(Debug, Clone)]
pub struct ProfileOptions {
    /// Path to the input `.gputrace` bundle.
    pub trace: PathBuf,
    /// Directory MTLReplayer writes the resulting `<stem>.gpuprofiler_raw`
    /// bundle into. Created if missing.
    pub output_dir: PathBuf,
    /// Optional file MTLReplayer's stdout is captured to. Useful for
    /// debugging — contains the `#CI-INFO#` lines.
    pub stdout_log: Option<PathBuf>,
    /// Optional file MTLReplayer's stderr is captured to.
    pub stderr_log: Option<PathBuf>,
}

/// Result of a single MTLReplayer profile run.
#[derive(Debug, Clone, Serialize)]
pub struct ProfileReport {
    pub trace: PathBuf,
    pub output_dir: PathBuf,
    /// The `<stem>.gpuprofiler_raw` directory the engine produced inside
    /// `output_dir`, if found.
    pub gpuprofiler_raw: Option<PathBuf>,
    /// Whether the expected `streamData` file is present.
    pub has_stream_data: bool,
    /// Total wall time in milliseconds.
    pub elapsed_ms: f64,
    /// Whether the underlying `open` invocation reported a non-zero exit.
    pub open_exit_code: i32,
}

/// Run `MTLReplayer.app -CLI <trace> -collectProfilerData --all --output <dir>`
/// via LaunchServices (`open -W -a`). Blocks until MTLReplayer exits, then
/// inspects the output directory for the produced `<stem>.gpuprofiler_raw`
/// bundle.
///
/// MTLReplayer is launched through `open` rather than directly because Apple's
/// trust cache puts a launch constraint on the binary; only LaunchServices /
/// CoreServicesUIAgent satisfies it. The `-CLI` flag is the gating switch
/// that puts MTLReplayer into headless CLI mode (otherwise it enters
/// NSApplicationMain as the GTDisplayService companion and idles).
///
/// `-collectProfilerData --all` is what actually drives the per-draw
/// pre-play / rewind / playTo / counter-collection loop the engine runs;
/// `-profileTrace` alone only sets a config bit.
pub fn profile(options: &ProfileOptions) -> Result<ProfileReport> {
    if !Path::new(MTL_REPLAYER_BIN).exists() {
        return Err(Error::NotFound(PathBuf::from(MTL_REPLAYER_BIN)));
    }
    if !options.trace.exists() {
        return Err(Error::NotFound(options.trace.clone()));
    }

    fs::create_dir_all(&options.output_dir)?;
    if let Some(path) = &options.stdout_log {
        ensure_writable_log_target(path)?;
    }
    if let Some(path) = &options.stderr_log {
        ensure_writable_log_target(path)?;
    }

    let mut command = Command::new("/usr/bin/open");
    command.arg("-W").arg("-a").arg(MTL_REPLAYER_APP);
    if let Some(path) = &options.stdout_log {
        command.arg("-o").arg(path);
    }
    if let Some(path) = &options.stderr_log {
        command.arg("-e").arg(path);
    }
    command
        .arg("--args")
        .arg("-CLI")
        .arg(&options.trace)
        .arg("-collectProfilerData")
        .arg("--all")
        .arg("-runningInCI")
        .arg("-verbose")
        .arg("--output")
        .arg(&options.output_dir);

    let start = Instant::now();
    let status = command.status()?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;

    let gpuprofiler_raw = find_gpuprofiler_raw(&options.output_dir);
    let has_stream_data = gpuprofiler_raw
        .as_ref()
        .map(|dir| dir.join("streamData").is_file())
        .unwrap_or(false);

    Ok(ProfileReport {
        trace: options.trace.clone(),
        output_dir: options.output_dir.clone(),
        gpuprofiler_raw,
        has_stream_data,
        elapsed_ms,
        open_exit_code: status.code().unwrap_or(-1),
    })
}

fn ensure_writable_log_target(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    if !path.exists() {
        fs::File::create(path)?;
    }
    Ok(())
}

fn find_gpuprofiler_raw(output_dir: &Path) -> Option<PathBuf> {
    let entries = fs::read_dir(output_dir).ok()?;
    let mut best: Option<PathBuf> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(OsStr::to_str) == Some("gpuprofiler_raw") && path.is_dir() {
            best = Some(path);
            break;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_replayer_returns_not_found() {
        // We can't easily make MTLReplayer disappear, so this just exercises
        // the input-validation path: a missing trace path should fail
        // before we ever shell out.
        let result = profile(&ProfileOptions {
            trace: PathBuf::from("/this/path/does/not/exist.gputrace"),
            output_dir: std::env::temp_dir().join("replay_service_missing"),
            stdout_log: None,
            stderr_log: None,
        });
        assert!(matches!(result, Err(Error::NotFound(_))));
    }
}
