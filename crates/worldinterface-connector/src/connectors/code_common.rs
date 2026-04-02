use std::io;
use std::path::{Path, PathBuf};

use serde_json::Value;
use uuid::Uuid;

use crate::error::ConnectorError;

/// Atomic write via temp file + rename.
pub fn write_atomic(path: &Path, run_id: Uuid, content: &str) -> Result<(), ConnectorError> {
    let tmp_path = path.with_extension(format!("tmp-{run_id}"));
    std::fs::write(&tmp_path, content.as_bytes())
        .map_err(|e| classify_io_error(&e, &path.display().to_string()))?;
    std::fs::rename(&tmp_path, path)
        .map_err(|e| classify_io_error(&e, &path.display().to_string()))?;
    Ok(())
}

/// Idempotency marker file path.
pub fn marker_path(path: &str, run_id: Uuid) -> PathBuf {
    PathBuf::from(format!("{path}.wid-{run_id}"))
}

/// Read a UTF-8 text file with consistent connector error classification.
pub fn read_utf8_file(path: &Path) -> Result<String, ConnectorError> {
    let path_str = path.display().to_string();
    let content = std::fs::read(path).map_err(|e| classify_io_error(&e, &path_str))?;
    String::from_utf8(content)
        .map_err(|_| ConnectorError::Terminal(format!("file is not valid UTF-8: {path_str}")))
}

/// Load a previously stored idempotent result from the marker file.
pub fn load_marker_result(path: &str, run_id: Uuid) -> Result<Option<Value>, ConnectorError> {
    let marker = marker_path(path, run_id);
    if !marker.exists() {
        return Ok(None);
    }

    let bytes = std::fs::read(&marker).map_err(|e| {
        ConnectorError::Retryable(format!("failed to read marker file {}: {e}", marker.display()))
    })?;

    let value = serde_json::from_slice(&bytes).map_err(|e| {
        ConnectorError::Retryable(format!("failed to parse marker file {}: {e}", marker.display()))
    })?;
    Ok(Some(value))
}

/// Persist the successful result payload to the marker file for idempotent retries.
pub fn store_marker_result(path: &str, run_id: Uuid, value: &Value) -> Result<(), ConnectorError> {
    let marker = marker_path(path, run_id);
    let bytes = serde_json::to_vec(value)
        .map_err(|e| ConnectorError::Retryable(format!("failed to serialize marker file: {e}")))?;
    std::fs::write(&marker, bytes).map_err(|e| {
        ConnectorError::Retryable(format!("failed to write marker file {}: {e}", marker.display()))
    })?;
    Ok(())
}

/// Classify I/O errors into ConnectorError variants.
pub fn classify_io_error(err: &io::Error, path: &str) -> ConnectorError {
    match err.kind() {
        io::ErrorKind::NotFound => ConnectorError::Terminal(format!("file not found: {path}")),
        io::ErrorKind::PermissionDenied => {
            ConnectorError::Terminal(format!("permission denied: {path}"))
        }
        _ => ConnectorError::Retryable(format!("I/O error on {path}: {err}")),
    }
}
