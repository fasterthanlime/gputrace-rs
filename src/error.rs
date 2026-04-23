use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("trace bundle not found: {0}")]
    NotFound(PathBuf),
    #[error("trace path is not a directory bundle: {0}")]
    NotDirectory(PathBuf),
    #[error("missing required trace file: {0}")]
    MissingFile(PathBuf),
    #[error("invalid trace bundle: {0}")]
    InvalidTrace(&'static str),
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Plist(#[from] plist::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
