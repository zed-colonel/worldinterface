//! The `fs.write` connector — writes content to the filesystem with idempotency enforcement.
//!
//! Uses a marker file `{path}.wid-{run_id}` to enforce idempotency. On retry
//! with the same `run_id`, the write is skipped if the marker exists.

use std::io;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use uuid::Uuid;
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

/// Writes content to the local filesystem with marker-based idempotency.
pub struct FsWriteConnector;

impl Connector for FsWriteConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "fs.write".into(),
            display_name: "File Write".into(),
            description: "Writes content to the local filesystem with idempotency enforcement."
                .into(),
            category: ConnectorCategory::FileSystem,
            input_schema: Some(json!({
                "type": "object",
                "required": ["path", "content"],
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" },
                    "mode": {
                        "type": "string",
                        "enum": ["create", "overwrite", "append"],
                        "default": "create"
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "bytes_written": { "type": "integer" }
                }
            })),
            idempotent: true,
            side_effects: true,
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        let path_str = params.get("path").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'path' (expected string)".into())
        })?;
        let content = params.get("content").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'content' (expected string)".into())
        })?;
        let mode = params.get("mode").and_then(Value::as_str).unwrap_or("create");

        let path = Path::new(path_str);
        let marker = marker_path(path_str, ctx.run_id);
        let bytes = content.len();

        // Idempotency check: if marker exists, this run_id already wrote
        if marker.exists() {
            tracing::info!(
                %ctx.run_id,
                path = %path_str,
                "marker exists, skipping write (idempotent retry)"
            );
            let actual_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            return Ok(json!({ "path": path_str, "bytes_written": actual_size }));
        }

        // Validate parent directory exists
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(ConnectorError::Terminal(format!(
                    "parent directory does not exist: {}",
                    parent.display()
                )));
            }
        }

        match mode {
            "create" => {
                if path.exists() {
                    return Err(ConnectorError::Terminal(format!(
                        "file already exists (mode: create): {path_str}"
                    )));
                }
                write_atomic(path, ctx.run_id, content)?;
            }
            "overwrite" => {
                write_atomic(path, ctx.run_id, content)?;
            }
            "append" => {
                use std::fs::OpenOptions;
                use std::io::Write;
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .map_err(|e| classify_io_error(&e, path_str))?;
                file.write_all(content.as_bytes()).map_err(|e| classify_io_error(&e, path_str))?;
            }
            other => {
                return Err(ConnectorError::InvalidParams(format!(
                    "unknown mode: '{other}' (expected create, overwrite, or append)"
                )));
            }
        }

        // Write marker file to record this run_id completed
        std::fs::write(&marker, b"")
            .map_err(|e| ConnectorError::Retryable(format!("failed to write marker file: {e}")))?;

        Ok(json!({ "path": path_str, "bytes_written": bytes }))
    }
}

/// Atomic write via temp file + rename.
fn write_atomic(path: &Path, run_id: Uuid, content: &str) -> Result<(), ConnectorError> {
    let tmp_path = path.with_extension(format!("tmp-{run_id}"));
    std::fs::write(&tmp_path, content.as_bytes())
        .map_err(|e| classify_io_error(&e, &path.display().to_string()))?;
    std::fs::rename(&tmp_path, path)
        .map_err(|e| classify_io_error(&e, &path.display().to_string()))?;
    Ok(())
}

fn marker_path(path: &str, run_id: Uuid) -> PathBuf {
    PathBuf::from(format!("{path}.wid-{run_id}"))
}

fn classify_io_error(err: &io::Error, path: &str) -> ConnectorError {
    match err.kind() {
        io::ErrorKind::PermissionDenied => {
            ConnectorError::Terminal(format!("permission denied: {path}"))
        }
        io::ErrorKind::NotFound => {
            ConnectorError::Terminal(format!("parent directory does not exist: {path}"))
        }
        _ => ConnectorError::Retryable(format!("I/O error writing {path}: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;
    use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};

    use super::*;
    use crate::context::{CancellationToken, InvocationContext};

    fn test_ctx() -> InvocationContext {
        InvocationContext {
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            step_run_id: StepRunId::new(),
            run_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            attempt_number: 1,
            cancellation: CancellationToken::new(),
        }
    }

    #[test]
    fn write_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("out.txt");
        let ctx = test_ctx();

        let result = FsWriteConnector
            .invoke(&ctx, &json!({"path": file_path.to_str().unwrap(), "content": "hello"}))
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "hello");
        assert_eq!(result["bytes_written"], 5);
    }

    #[test]
    fn write_creates_marker() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("out.txt");
        let ctx = test_ctx();

        FsWriteConnector
            .invoke(&ctx, &json!({"path": file_path.to_str().unwrap(), "content": "data"}))
            .unwrap();

        let expected_marker = marker_path(file_path.to_str().unwrap(), ctx.run_id);
        assert!(expected_marker.exists());
    }

    #[test]
    fn write_idempotent_same_run_id() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("out.txt");
        let ctx = test_ctx();

        // First write
        FsWriteConnector
            .invoke(&ctx, &json!({"path": file_path.to_str().unwrap(), "content": "first"}))
            .unwrap();

        // Second write with same ctx (same run_id) — should succeed without error
        let result = FsWriteConnector
            .invoke(&ctx, &json!({"path": file_path.to_str().unwrap(), "content": "second"}))
            .unwrap();

        // File should still have original content
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "first");
        assert_eq!(result["bytes_written"], 5); // reports the actual file size
    }

    #[test]
    fn write_different_run_id_in_overwrite_mode() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("out.txt");

        let ctx1 = test_ctx();
        FsWriteConnector
            .invoke(
                &ctx1,
                &json!({
                    "path": file_path.to_str().unwrap(),
                    "content": "first",
                    "mode": "overwrite"
                }),
            )
            .unwrap();

        let ctx2 = test_ctx();
        FsWriteConnector
            .invoke(
                &ctx2,
                &json!({
                    "path": file_path.to_str().unwrap(),
                    "content": "second",
                    "mode": "overwrite"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "second");
    }

    #[test]
    fn write_create_mode_rejects_existing() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("out.txt");
        std::fs::write(&file_path, "pre-existing").unwrap();

        let ctx = test_ctx();
        let result = FsWriteConnector
            .invoke(&ctx, &json!({"path": file_path.to_str().unwrap(), "content": "new"}));

        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn write_append_mode() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("out.txt");
        std::fs::write(&file_path, "hello").unwrap();

        let ctx = test_ctx();
        FsWriteConnector
            .invoke(
                &ctx,
                &json!({
                    "path": file_path.to_str().unwrap(),
                    "content": " world",
                    "mode": "append"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "hello world");
    }

    #[test]
    fn write_append_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("out.txt");

        let ctx = test_ctx();

        // First append
        FsWriteConnector
            .invoke(
                &ctx,
                &json!({
                    "path": file_path.to_str().unwrap(),
                    "content": "data",
                    "mode": "append"
                }),
            )
            .unwrap();

        // Second append with same run_id — marker should prevent double-append
        FsWriteConnector
            .invoke(
                &ctx,
                &json!({
                    "path": file_path.to_str().unwrap(),
                    "content": "data",
                    "mode": "append"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "data");
    }

    #[test]
    fn write_default_mode_is_create() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("out.txt");

        let ctx = test_ctx();
        FsWriteConnector
            .invoke(&ctx, &json!({"path": file_path.to_str().unwrap(), "content": "created"}))
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "created");

        // A second create with a different run_id should fail (file exists, no marker for new run)
        let ctx2 = test_ctx();
        let result = FsWriteConnector
            .invoke(&ctx2, &json!({"path": file_path.to_str().unwrap(), "content": "again"}));
        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn write_missing_path_is_invalid_params() {
        let ctx = test_ctx();
        let result = FsWriteConnector.invoke(&ctx, &json!({"content": "data"}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn write_output_has_bytes_written() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("out.txt");
        let ctx = test_ctx();

        let result = FsWriteConnector
            .invoke(&ctx, &json!({"path": file_path.to_str().unwrap(), "content": "twelve chars"}))
            .unwrap();

        assert_eq!(result["bytes_written"], 12);
    }

    #[test]
    fn write_descriptor() {
        let desc = FsWriteConnector.describe();
        assert_eq!(desc.name, "fs.write");
        assert_eq!(desc.category, ConnectorCategory::FileSystem);
        assert!(desc.idempotent);
        assert!(desc.side_effects);
    }
}
