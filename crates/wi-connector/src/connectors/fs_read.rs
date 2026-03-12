//! The `fs.read` connector — reads file contents from the local filesystem.

use std::io;

use serde_json::{json, Value};
use wi_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

/// Reads file contents as UTF-8 string from the local filesystem.
pub struct FsReadConnector;

impl Connector for FsReadConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "fs.read".into(),
            display_name: "File Read".into(),
            description: "Reads file contents from the local filesystem.".into(),
            category: ConnectorCategory::FileSystem,
            input_schema: Some(json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": { "type": "string" }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "size": { "type": "integer" }
                }
            })),
            idempotent: true,
            side_effects: false,
        }
    }

    fn invoke(&self, _ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        let path = params.get("path").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'path' (expected string)".into())
        })?;

        let content = std::fs::read(path).map_err(|e| classify_io_error(&e, path))?;

        let text = String::from_utf8(content)
            .map_err(|_| ConnectorError::Terminal(format!("file is not valid UTF-8: {path}")))?;

        let size = text.len();
        Ok(json!({ "content": text, "size": size }))
    }
}

fn classify_io_error(err: &io::Error, path: &str) -> ConnectorError {
    match err.kind() {
        io::ErrorKind::NotFound => ConnectorError::Terminal(format!("file not found: {path}")),
        io::ErrorKind::PermissionDenied => {
            ConnectorError::Terminal(format!("permission denied: {path}"))
        }
        _ => ConnectorError::Retryable(format!("I/O error reading {path}: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde_json::json;
    use uuid::Uuid;
    use wi_core::id::{FlowRunId, NodeId, StepRunId};

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
    fn read_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let ctx = test_ctx();
        let result =
            FsReadConnector.invoke(&ctx, &json!({"path": file_path.to_str().unwrap()})).unwrap();

        assert_eq!(result["content"], "hello world");
        assert_eq!(result["size"], 11);
    }

    #[test]
    fn read_nonexistent_is_terminal() {
        let ctx = test_ctx();
        let result =
            FsReadConnector.invoke(&ctx, &json!({"path": "/tmp/nonexistent_ui_test_file"}));
        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn read_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("empty.txt");
        std::fs::File::create(&file_path).unwrap();

        let ctx = test_ctx();
        let result =
            FsReadConnector.invoke(&ctx, &json!({"path": file_path.to_str().unwrap()})).unwrap();

        assert_eq!(result["content"], "");
        assert_eq!(result["size"], 0);
    }

    #[test]
    fn read_utf8_content() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("unicode.txt");
        let mut f = std::fs::File::create(&file_path).unwrap();
        f.write_all("Héllo wörld 🌍".as_bytes()).unwrap();

        let ctx = test_ctx();
        let result =
            FsReadConnector.invoke(&ctx, &json!({"path": file_path.to_str().unwrap()})).unwrap();

        assert_eq!(result["content"], "Héllo wörld 🌍");
    }

    #[test]
    fn read_missing_path_is_invalid_params() {
        let ctx = test_ctx();
        let result = FsReadConnector.invoke(&ctx, &json!({}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn read_descriptor() {
        let desc = FsReadConnector.describe();
        assert_eq!(desc.name, "fs.read");
        assert_eq!(desc.category, ConnectorCategory::FileSystem);
        assert!(desc.idempotent);
        assert!(!desc.side_effects);
    }
}
