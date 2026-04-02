use std::path::Path;

use serde_json::{json, Value};
use similar::{ChangeTag, TextDiff};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use super::code_common;
use super::gitignore_check;
use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

pub struct CodeEditConnector;

impl Connector for CodeEditConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.edit".into(),
            display_name: "Code Edit".into(),
            description: "Performs targeted string replacement in a file. The old_string must match exactly once (unless replace_all is true). Returns a unified diff of the change. Respects .gitignore.".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["file_path", "old_string", "new_string"],
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "The absolute path to the file to modify"
                    },
                    "old_string": {
                        "type": "string",
                        "description": "The exact text to find and replace"
                    },
                    "new_string": {
                        "type": "string",
                        "description": "The replacement text"
                    },
                    "replace_all": {
                        "type": "boolean",
                        "description": "Replace all occurrences (default: false)",
                        "default": false
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "file_path": { "type": "string" },
                    "replacements_made": { "type": "integer" },
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
            idempotent: false,
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
        let old_string = params.get("old_string").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams(
                "missing or invalid 'old_string' (expected string)".into(),
            )
        })?;
        let new_string = params.get("new_string").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams(
                "missing or invalid 'new_string' (expected string)".into(),
            )
        })?;
        let replace_all = params.get("replace_all").and_then(Value::as_bool).unwrap_or(false);

        if old_string.is_empty() {
            return Err(ConnectorError::InvalidParams("old_string must not be empty".into()));
        }
        if old_string == new_string {
            return Err(ConnectorError::InvalidParams(
                "old_string and new_string are identical".into(),
            ));
        }

        let path = Path::new(path_str);
        if gitignore_check::is_gitignored(path) {
            return Err(ConnectorError::Terminal(format!("path is gitignored: {path_str}")));
        }
        if let Some(result) = code_common::load_marker_result(path_str, ctx.run_id)? {
            return Ok(result);
        }

        let original = code_common::read_utf8_file(path)?;
        let count = original.matches(old_string).count();

        if count == 0 {
            return Err(ConnectorError::Terminal(format!("old_string not found in {path_str}")));
        }
        if count > 1 && !replace_all {
            return Err(ConnectorError::Terminal(format!(
                "old_string found {count} times in {path_str} — use replace_all: true to replace all, or provide a larger context string to match uniquely"
            )));
        }

        let new_content = if replace_all {
            original.replace(old_string, new_string)
        } else {
            original.replacen(old_string, new_string, 1)
        };

        let diff = TextDiff::from_lines(&original, &new_content);
        let unified = diff
            .unified_diff()
            .header(&format!("a/{path_str}"), &format!("b/{path_str}"))
            .to_string();
        let (lines_added, lines_removed) = count_diff_lines(&diff);

        code_common::write_atomic(path, ctx.run_id, &new_content)?;

        let result = json!({
            "file_path": path_str,
            "replacements_made": if replace_all { count } else { 1 },
            "diff": {
                "unified": unified,
                "lines_added": lines_added,
                "lines_removed": lines_removed
            }
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
    fn code_edit_single_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "let value = 1;\n").unwrap();

        let result = CodeEditConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "old_string": "1",
                    "new_string": "2"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "let value = 2;\n");
        assert_eq!(result["replacements_made"], 1);
        assert!(result["diff"]["unified"].as_str().unwrap().contains("-let value = 1;"));
    }

    #[test]
    fn code_edit_replace_all() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "x x x\n").unwrap();

        let result = CodeEditConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "old_string": "x",
                    "new_string": "y",
                    "replace_all": true
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "y y y\n");
        assert_eq!(result["replacements_made"], 3);
    }

    #[test]
    fn code_edit_ambiguous_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "x x\n").unwrap();

        let result = CodeEditConnector.invoke(
            &test_ctx(),
            &json!({
                "file_path": file.to_str().unwrap(),
                "old_string": "x",
                "new_string": "y"
            }),
        );

        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn code_edit_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "x\n").unwrap();

        let result = CodeEditConnector.invoke(
            &test_ctx(),
            &json!({
                "file_path": file.to_str().unwrap(),
                "old_string": "missing",
                "new_string": "y"
            }),
        );

        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn code_edit_same_string_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "x\n").unwrap();

        let result = CodeEditConnector.invoke(
            &test_ctx(),
            &json!({
                "file_path": file.to_str().unwrap(),
                "old_string": "x",
                "new_string": "x"
            }),
        );

        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn code_edit_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "let value = 1;\n").unwrap();
        let ctx = test_ctx();
        let params = json!({
            "file_path": file.to_str().unwrap(),
            "old_string": "1",
            "new_string": "2"
        });

        let first = CodeEditConnector.invoke(&ctx, &params).unwrap();
        let second = CodeEditConnector.invoke(&ctx, &params).unwrap();

        assert_eq!(first, second);
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "let value = 2;\n");
    }

    #[test]
    fn code_edit_gitignored_rejected() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".gitignore"), "*.secret\n").unwrap();
        let file = dir.path().join("config.secret");
        std::fs::write(&file, "secret\n").unwrap();

        let result = CodeEditConnector.invoke(
            &test_ctx(),
            &json!({
                "file_path": file.to_str().unwrap(),
                "old_string": "secret",
                "new_string": "public"
            }),
        );

        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn code_edit_nonexistent_terminal() {
        let result = CodeEditConnector.invoke(
            &test_ctx(),
            &json!({
                "file_path": "/tmp/no-such-code-edit-file",
                "old_string": "x",
                "new_string": "y"
            }),
        );
        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn code_edit_missing_params() {
        let result = CodeEditConnector.invoke(&test_ctx(), &json!({}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn code_edit_descriptor() {
        let desc = CodeEditConnector.describe();
        assert_eq!(desc.name, "code.edit");
        assert_eq!(desc.category, ConnectorCategory::Code);
        assert!(desc.is_mutating);
        assert!(desc.requires_read_before_write);
        assert!(!desc.is_read_only);
        assert!(!desc.is_concurrency_safe);
    }

    #[test]
    fn code_edit_multiline_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "a\nb\nc\n").unwrap();

        CodeEditConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "old_string": "a\nb",
                    "new_string": "x\ny"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "x\ny\nc\n");
    }
}
