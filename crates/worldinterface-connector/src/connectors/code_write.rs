use std::path::Path;

use serde_json::{json, Value};
use similar::{ChangeTag, TextDiff};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use super::code_common;
use super::gitignore_check;
use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

pub struct CodeWriteConnector;

impl Connector for CodeWriteConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.write".into(),
            display_name: "Code Write".into(),
            description: "Writes content to a file, creating it if it doesn't exist or overwriting if it does. Returns a unified diff for overwrites. Respects .gitignore.".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["file_path", "content"],
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "The absolute path to the file to write (must be absolute, not relative)"
                    },
                    "content": {
                        "type": "string",
                        "description": "The content to write to the file"
                    },
                    "create_dirs": {
                        "type": "boolean",
                        "description": "Create parent directories if they don't exist (default: false)",
                        "default": false
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "file_path": { "type": "string" },
                    "bytes_written": { "type": "integer" },
                    "created": { "type": "boolean" },
                    "diff": {
                        "type": "object",
                        "properties": {
                            "unified": { "type": "string" },
                            "lines_added": { "type": "integer" },
                            "lines_removed": { "type": "integer" }
                        }
                    }
                }
            })),
            idempotent: true,
            side_effects: true,
            is_read_only: false,
            is_mutating: true,
            is_concurrency_safe: false,
            requires_read_before_write: true,
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        let path_str = params.get("file_path").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'file_path' (expected string)".into())
        })?;
        let content = params.get("content").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'content' (expected string)".into())
        })?;
        let create_dirs = params.get("create_dirs").and_then(Value::as_bool).unwrap_or(false);

        let path = Path::new(path_str);
        if gitignore_check::is_gitignored(path) {
            return Err(ConnectorError::Terminal(format!("path is gitignored: {path_str}")));
        }
        if let Some(result) = code_common::load_marker_result(path_str, ctx.run_id)? {
            return Ok(result);
        }

        if create_dirs {
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| code_common::classify_io_error(&e, path_str))?;
                }
            }
        } else if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(ConnectorError::Terminal(format!(
                    "parent directory does not exist: {} (use create_dirs: true)",
                    parent.display()
                )));
            }
        }

        let created = !path.exists();
        let diff_json = if created {
            Value::Null
        } else {
            let original = code_common::read_utf8_file(path)?;
            let diff = TextDiff::from_lines(&original, content);
            let unified = diff
                .unified_diff()
                .header(&format!("a/{path_str}"), &format!("b/{path_str}"))
                .to_string();
            let (lines_added, lines_removed) = count_diff_lines(&diff);
            json!({
                "unified": unified,
                "lines_added": lines_added,
                "lines_removed": lines_removed
            })
        };

        code_common::write_atomic(path, ctx.run_id, content)?;

        let result = json!({
            "file_path": path_str,
            "bytes_written": content.len(),
            "created": created,
            "diff": diff_json
        });
        code_common::store_marker_result(path_str, ctx.run_id, &result)?;

        Ok(result)
    }
}

fn count_diff_lines(diff: &TextDiff<'_, '_, str>) -> (usize, usize) {
    let mut added = 0;
    let mut removed = 0;

    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Insert => added += 1,
            ChangeTag::Delete => removed += 1,
            ChangeTag::Equal => {}
        }
    }

    (added, removed)
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

    #[test]
    fn code_write_creates_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("new.rs");

        let result = CodeWriteConnector
            .invoke(
                &test_ctx(),
                &json!({"file_path": file.to_str().unwrap(), "content": "fn main() {}\n"}),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "fn main() {}\n");
        assert_eq!(result["created"], true);
        assert!(result["diff"].is_null());
    }

    #[test]
    fn code_write_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "old\n").unwrap();

        let result = CodeWriteConnector
            .invoke(&test_ctx(), &json!({"file_path": file.to_str().unwrap(), "content": "new\n"}))
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "new\n");
        assert_eq!(result["created"], false);
        assert!(result["diff"]["unified"].as_str().unwrap().contains("-old"));
        assert!(result["diff"]["unified"].as_str().unwrap().contains("+new"));
    }

    #[test]
    fn code_write_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        let ctx = test_ctx();
        let params = json!({"file_path": file.to_str().unwrap(), "content": "new\n"});

        let first = CodeWriteConnector.invoke(&ctx, &params).unwrap();
        let second = CodeWriteConnector.invoke(&ctx, &params).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn code_write_create_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("nested").join("sample.rs");

        CodeWriteConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "content": "new\n",
                    "create_dirs": true
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "new\n");
    }

    #[test]
    fn code_write_no_create_dirs_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("nested").join("sample.rs");

        let result = CodeWriteConnector
            .invoke(&test_ctx(), &json!({"file_path": file.to_str().unwrap(), "content": "new\n"}));

        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn code_write_gitignored_rejected() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".gitignore"), "*.secret\n").unwrap();
        let file = dir.path().join("config.secret");

        let result = CodeWriteConnector.invoke(
            &test_ctx(),
            &json!({"file_path": file.to_str().unwrap(), "content": "secret\n"}),
        );

        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn code_write_missing_params() {
        let result = CodeWriteConnector.invoke(&test_ctx(), &json!({}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn code_write_descriptor() {
        let desc = CodeWriteConnector.describe();
        assert_eq!(desc.name, "code.write");
        assert_eq!(desc.category, ConnectorCategory::Code);
        assert!(desc.is_mutating);
        assert!(desc.requires_read_before_write);
        assert!(!desc.is_read_only);
        assert!(!desc.is_concurrency_safe);
    }

    #[test]
    fn code_write_empty_content() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("empty.rs");

        CodeWriteConnector
            .invoke(&test_ctx(), &json!({"file_path": file.to_str().unwrap(), "content": ""}))
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "");
    }

    #[test]
    fn code_write_utf8_content() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("unicode.rs");

        CodeWriteConnector
            .invoke(
                &test_ctx(),
                &json!({"file_path": file.to_str().unwrap(), "content": "Héllo 🌍\n"}),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "Héllo 🌍\n");
    }
}
