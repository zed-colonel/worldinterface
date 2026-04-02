use std::path::Path;

use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use super::code_common;
use super::gitignore_check;
use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

pub struct CodeReadConnector;

impl Connector for CodeReadConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.read".into(),
            display_name: "Code Read".into(),
            description: "Reads file contents with line numbers, supporting windowed reads for navigating large files. Respects .gitignore.".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["file_path"],
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "The absolute path to the file to read"
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Line number to start reading from (1-based). Default: 1",
                        "minimum": 1
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of lines to return. Default: 2000",
                        "minimum": 1
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "start_line": { "type": "integer" },
                    "end_line": { "type": "integer" },
                    "total_lines": { "type": "integer" },
                    "file_size_bytes": { "type": "integer" },
                    "truncated": { "type": "boolean" }
                }
            })),
            idempotent: true,
            side_effects: false,
            is_read_only: true,
            is_mutating: false,
            is_concurrency_safe: true,
            requires_read_before_write: false,
        }
    }

    fn invoke(&self, _ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        let path_str = params.get("file_path").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'file_path' (expected string)".into())
        })?;
        let offset = params.get("offset").and_then(Value::as_u64).unwrap_or(1) as usize;
        let limit = params.get("limit").and_then(Value::as_u64).unwrap_or(2000) as usize;

        let path = Path::new(path_str);
        if gitignore_check::is_gitignored(path) {
            return Err(ConnectorError::Terminal(format!("path is gitignored: {path_str}")));
        }

        let text = code_common::read_utf8_file(path)?;
        let file_size_bytes = text.len();
        let lines: Vec<&str> = text.lines().collect();
        let total_lines = lines.len();

        let start_idx = offset.saturating_sub(1).min(total_lines);
        let end_idx = start_idx.saturating_add(limit).min(total_lines);
        let truncated = end_idx < total_lines;

        let numbered = lines[start_idx..end_idx]
            .iter()
            .enumerate()
            .map(|(idx, line)| format!("{:>6}\t{}", start_idx + idx + 1, line))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(json!({
            "content": numbered,
            "start_line": start_idx + 1,
            "end_line": end_idx,
            "total_lines": total_lines,
            "file_size_bytes": file_size_bytes,
            "truncated": truncated,
        }))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;
    use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};

    use super::*;
    use crate::context::CancellationToken;

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

    fn sample_file() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("sample.rs"), "one\ntwo\nthree\nfour\nfive\n").unwrap();
        dir
    }

    #[test]
    fn code_read_basic() {
        let dir = sample_file();
        let file = dir.path().join("sample.rs");
        let result = CodeReadConnector
            .invoke(&test_ctx(), &json!({"file_path": file.to_str().unwrap()}))
            .unwrap();

        assert!(result["content"].as_str().unwrap().contains("     1\tone"));
        assert_eq!(result["start_line"], 1);
        assert_eq!(result["end_line"], 5);
        assert_eq!(result["total_lines"], 5);
        assert_eq!(result["truncated"], false);
    }

    #[test]
    fn code_read_with_offset_and_limit() {
        let dir = sample_file();
        let file = dir.path().join("sample.rs");
        let result = CodeReadConnector
            .invoke(
                &test_ctx(),
                &json!({"file_path": file.to_str().unwrap(), "offset": 2, "limit": 2}),
            )
            .unwrap();

        assert_eq!(result["start_line"], 2);
        assert_eq!(result["end_line"], 3);
        assert_eq!(result["truncated"], true);
        assert_eq!(result["content"], "     2\ttwo\n     3\tthree");
    }

    #[test]
    fn code_read_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("empty.rs");
        std::fs::write(&file, "").unwrap();

        let result = CodeReadConnector
            .invoke(&test_ctx(), &json!({"file_path": file.to_str().unwrap()}))
            .unwrap();

        assert_eq!(result["content"], "");
        assert_eq!(result["total_lines"], 0);
        assert_eq!(result["truncated"], false);
    }

    #[test]
    fn code_read_nonexistent_terminal() {
        let result = CodeReadConnector
            .invoke(&test_ctx(), &json!({"file_path": "/tmp/no-such-code-read-file"}));
        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn code_read_gitignored_rejected() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".gitignore"), "*.secret\n").unwrap();
        let file = dir.path().join("config.secret");
        std::fs::write(&file, "x").unwrap();

        let result =
            CodeReadConnector.invoke(&test_ctx(), &json!({"file_path": file.to_str().unwrap()}));
        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn code_read_missing_file_path_invalid_params() {
        let result = CodeReadConnector.invoke(&test_ctx(), &json!({}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn code_read_descriptor() {
        let desc = CodeReadConnector.describe();
        assert_eq!(desc.name, "code.read");
        assert_eq!(desc.category, ConnectorCategory::Code);
        assert!(desc.is_read_only);
        assert!(desc.is_concurrency_safe);
        assert!(!desc.is_mutating);
        assert!(!desc.requires_read_before_write);
    }

    #[test]
    fn code_read_offset_beyond_file() {
        let dir = sample_file();
        let file = dir.path().join("sample.rs");
        let result = CodeReadConnector
            .invoke(&test_ctx(), &json!({"file_path": file.to_str().unwrap(), "offset": 99}))
            .unwrap();

        assert_eq!(result["content"], "");
        assert_eq!(result["truncated"], false);
    }

    #[test]
    fn code_read_utf8_content() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("unicode.rs");
        std::fs::write(&file, "Héllo\n🌍\n").unwrap();

        let result = CodeReadConnector
            .invoke(&test_ctx(), &json!({"file_path": file.to_str().unwrap()}))
            .unwrap();

        assert!(result["content"].as_str().unwrap().contains("Héllo"));
        assert!(result["content"].as_str().unwrap().contains("🌍"));
    }
}
