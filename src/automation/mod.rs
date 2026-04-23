use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Serialize;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Serialize)]
pub struct XcodeProfileRun {
    pub trace_path: PathBuf,
    pub output_path: Option<PathBuf>,
    pub timeout_seconds: u64,
}

pub fn activate_xcode() -> Result<()> {
    run_osascript(r#"tell application "Xcode" to activate"#)?;
    Ok(())
}

pub fn run_profile(request: &XcodeProfileRun) -> Result<()> {
    activate_xcode()?;
    if !request.trace_path.exists() {
        return Err(Error::NotFound(request.trace_path.clone()));
    }
    Err(Error::Unsupported(
        "xcode-profile automation has only been scaffolded so far",
    ))
}

pub fn run_osascript(script: &str) -> Result<String> {
    let output = Command::new("osascript").arg("-e").arg(script).output()?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(std::io::Error::other(stderr.trim().to_owned()).into())
    }
}

pub fn open_trace_in_xcode(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(Error::NotFound(path.to_path_buf()));
    }
    let status = Command::new("open")
        .arg("-a")
        .arg("Xcode")
        .arg(path)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::Unsupported("failed to open trace bundle in Xcode"))
    }
}
