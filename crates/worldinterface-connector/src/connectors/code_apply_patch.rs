use std::path::Path;

use regex::Regex;
use serde_json::{json, Value};
use similar::{ChangeTag, TextDiff};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use super::code_common;
use super::gitignore_check;
use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

pub struct CodeApplyPatchConnector;

impl Connector for CodeApplyPatchConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.apply_patch".into(),
            display_name: "Code Apply Patch".into(),
            description: "Applies a unified diff patch to a file. Supports multi-hunk patches with context validation. Can create new files. Returns a summary of changes applied. Respects .gitignore.".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["file_path", "patch"],
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "The absolute path to the file to patch"
                    },
                    "patch": {
                        "type": "string",
                        "description": "Unified diff content with @@ hunk headers. Context lines (space prefix) are validated against the file. Supports multiple hunks."
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "file_path": { "type": "string" },
                    "hunks_applied": { "type": "integer" },
                    "diff": {
                        "type": "object",
                        "properties": {
                            "unified": { "type": "string" },
                            "lines_added": { "type": "integer" },
                            "lines_removed": { "type": "integer" }
                        }
                    },
                    "created": { "type": "boolean" }
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
        let patch = params.get("patch").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'patch' (expected string)".into())
        })?;

        let path = Path::new(path_str);
        if gitignore_check::is_gitignored(path) {
            return Err(ConnectorError::Terminal(format!("path is gitignored: {path_str}")));
        }
        if let Some(result) = code_common::load_marker_result(path_str, ctx.run_id)? {
            return Ok(result);
        }

        let hunks = parse_patch(patch)?;
        let created = !path.exists();

        let original = if created {
            validate_creation_only(path_str, &hunks)?;
            String::new()
        } else {
            code_common::read_utf8_file(path)?
        };
        let original_lines = split_lines(&original);
        let new_lines = apply_hunks(original_lines, &hunks)?;
        let new_content = join_lines(&new_lines);

        let diff = TextDiff::from_lines(&original, &new_content);
        let unified = diff
            .unified_diff()
            .header(&format!("a/{path_str}"), &format!("b/{path_str}"))
            .to_string();
        let (lines_added, lines_removed) = count_diff_lines(&diff);

        code_common::write_atomic(path, ctx.run_id, &new_content)?;

        let result = json!({
            "file_path": path_str,
            "hunks_applied": hunks.len(),
            "created": created,
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

#[derive(Clone, Debug)]
struct Hunk {
    old_start: usize,
    old_count: usize,
    new_count: usize,
    lines: Vec<HunkLine>,
}

#[derive(Clone, Debug)]
enum HunkLine {
    Context(String),
    Add(String),
    Remove(String),
}

fn parse_patch(patch: &str) -> Result<Vec<Hunk>, ConnectorError> {
    let header_re = Regex::new(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: .*)?$").unwrap();
    let mut hunks = Vec::new();
    let mut current: Option<Hunk> = None;

    for line in patch.lines() {
        if let Some(captures) = header_re.captures(line) {
            if let Some(hunk) = current.take() {
                validate_hunk_counts(&hunk, line)?;
                hunks.push(hunk);
            }

            let old_start = captures[1].parse::<usize>().map_err(|_| {
                ConnectorError::InvalidParams(format!("invalid hunk header: {line}"))
            })?;
            let old_count = captures
                .get(2)
                .map(|value| value.as_str().parse::<usize>())
                .transpose()
                .map_err(|_| ConnectorError::InvalidParams(format!("invalid hunk header: {line}")))?
                .unwrap_or(1);
            let new_count = captures
                .get(4)
                .map(|value| value.as_str().parse::<usize>())
                .transpose()
                .map_err(|_| ConnectorError::InvalidParams(format!("invalid hunk header: {line}")))?
                .unwrap_or(1);

            current = Some(Hunk { old_start, old_count, new_count, lines: Vec::new() });
            continue;
        }

        let Some(hunk) = current.as_mut() else {
            continue;
        };

        if line.starts_with('\\') {
            continue;
        }

        let Some(prefix) = line.chars().next() else {
            continue;
        };
        let content = line[1..].to_string();
        match prefix {
            ' ' => hunk.lines.push(HunkLine::Context(content)),
            '+' => hunk.lines.push(HunkLine::Add(content)),
            '-' => hunk.lines.push(HunkLine::Remove(content)),
            _ => {}
        }
    }

    if let Some(hunk) = current.take() {
        validate_hunk_counts(&hunk, patch)?;
        hunks.push(hunk);
    }

    if hunks.is_empty() {
        return Err(ConnectorError::InvalidParams("no valid hunks found in patch".into()));
    }

    hunks.sort_by_key(|hunk| hunk.old_start);
    Ok(hunks)
}

fn validate_hunk_counts(hunk: &Hunk, line: &str) -> Result<(), ConnectorError> {
    let actual_old = hunk.lines.iter().filter(|line| !matches!(line, HunkLine::Add(_))).count();
    let actual_new = hunk.lines.iter().filter(|line| !matches!(line, HunkLine::Remove(_))).count();

    if actual_old != hunk.old_count || actual_new != hunk.new_count {
        return Err(ConnectorError::InvalidParams(format!("invalid hunk header: {line}")));
    }

    Ok(())
}

fn validate_creation_only(path_str: &str, hunks: &[Hunk]) -> Result<(), ConnectorError> {
    let creation_only = hunks.iter().all(|hunk| {
        hunk.old_count == 0 && hunk.lines.iter().all(|line| matches!(line, HunkLine::Add(_)))
    });

    if creation_only {
        Ok(())
    } else {
        Err(ConnectorError::Terminal(format!("file not found: {path_str}")))
    }
}

fn apply_hunks(mut file_lines: Vec<String>, hunks: &[Hunk]) -> Result<Vec<String>, ConnectorError> {
    let mut line_offset = 0isize;

    for hunk in hunks {
        let base_start = if hunk.old_start == 0 { 0 } else { hunk.old_start - 1 };
        let adjusted_start = base_start as isize + line_offset;
        if adjusted_start < 0 {
            return Err(ConnectorError::Terminal(
                "context mismatch at line 1: expected start before file, found beginning of file"
                    .to_string(),
            ));
        }
        let start = adjusted_start as usize;
        let expected_old = hunk
            .lines
            .iter()
            .filter_map(|line| match line {
                HunkLine::Context(content) | HunkLine::Remove(content) => Some(content.as_str()),
                HunkLine::Add(_) => None,
            })
            .collect::<Vec<_>>();
        let replacement = hunk
            .lines
            .iter()
            .filter_map(|line| match line {
                HunkLine::Context(content) | HunkLine::Add(content) => Some(content.clone()),
                HunkLine::Remove(_) => None,
            })
            .collect::<Vec<_>>();

        if start > file_lines.len() {
            return Err(ConnectorError::Terminal(format!(
                "context mismatch at line {}: expected '{}', found '<EOF>'",
                start + 1,
                expected_old.first().copied().unwrap_or("")
            )));
        }

        for (index, expected) in expected_old.iter().enumerate() {
            let actual = file_lines.get(start + index).map(String::as_str).unwrap_or("<EOF>");
            if actual != *expected {
                return Err(ConnectorError::Terminal(format!(
                    "context mismatch at line {}: expected '{}', found '{}'",
                    start + index + 1,
                    expected,
                    actual
                )));
            }
        }

        let end = start + hunk.old_count;
        if end > file_lines.len() {
            return Err(ConnectorError::Terminal(format!(
                "context mismatch at line {}: expected '<EOF>', found '<EOF>'",
                file_lines.len() + 1
            )));
        }

        file_lines.splice(start..end, replacement);
        line_offset += hunk.new_count as isize - hunk.old_count as isize;
    }

    Ok(file_lines)
}

fn split_lines(content: &str) -> Vec<String> {
    if content.is_empty() {
        Vec::new()
    } else {
        content.split('\n').map(str::to_string).collect()
    }
}

fn join_lines(lines: &[String]) -> String {
    if lines.is_empty() {
        String::new()
    } else {
        lines.join("\n")
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
    fn apply_patch_add_lines() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "alpha\nbeta\n").unwrap();

        CodeApplyPatchConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "patch": "@@ -1,2 +1,3 @@\n alpha\n+inserted\n beta"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "alpha\ninserted\nbeta\n");
    }

    #[test]
    fn apply_patch_remove_lines() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "alpha\nbeta\ngamma\n").unwrap();

        CodeApplyPatchConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "patch": "@@ -1,3 +1,2 @@\n alpha\n-beta\n gamma"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "alpha\ngamma\n");
    }

    #[test]
    fn apply_patch_replace_lines() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "alpha\nbeta\n").unwrap();

        CodeApplyPatchConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "patch": "@@ -1,2 +1,2 @@\n alpha\n-beta\n+delta"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "alpha\ndelta\n");
    }

    #[test]
    fn apply_patch_multi_hunk() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "one\ntwo\nthree\nfour\n").unwrap();

        CodeApplyPatchConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "patch": "@@ -1,2 +1,2 @@\n-one\n+ONE\n two\n@@ -3,2 +3,2 @@\n three\n-four\n+FOUR"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "ONE\ntwo\nthree\nFOUR\n");
    }

    #[test]
    fn apply_patch_context_validation() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "alpha\nbeta\n").unwrap();

        let result = CodeApplyPatchConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "patch": "@@ -1,2 +1,2 @@\n alpha\n-beta\n+gamma"
                }),
            )
            .unwrap();

        assert_eq!(result["hunks_applied"], 1);
    }

    #[test]
    fn apply_patch_context_mismatch_error() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "alpha\nbeta\n").unwrap();

        let result = CodeApplyPatchConnector.invoke(
            &test_ctx(),
            &json!({
                "file_path": file.to_str().unwrap(),
                "patch": "@@ -1,2 +1,2 @@\n wrong\n-beta\n+gamma"
            }),
        );

        assert!(
            matches!(result, Err(ConnectorError::Terminal(message)) if message.contains("context mismatch"))
        );
    }

    #[test]
    fn apply_patch_create_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("new.rs");

        let result = CodeApplyPatchConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "patch": "@@ -0,0 +1,2 @@\n+fn main() {}\n+"
                }),
            )
            .unwrap();

        assert_eq!(std::fs::read_to_string(&file).unwrap(), "fn main() {}\n");
        assert_eq!(result["created"], true);
    }

    #[test]
    fn apply_patch_respects_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".gitignore"), "*.secret\n").unwrap();
        let file = dir.path().join("config.secret");

        let result = CodeApplyPatchConnector.invoke(
            &test_ctx(),
            &json!({
                "file_path": file.to_str().unwrap(),
                "patch": "@@ -0,0 +1,1 @@\n+secret"
            }),
        );

        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn apply_patch_idempotent_retry() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "alpha\nbeta\n").unwrap();
        let ctx = test_ctx();
        let params = json!({
            "file_path": file.to_str().unwrap(),
            "patch": "@@ -1,2 +1,2 @@\n alpha\n-beta\n+gamma"
        });

        let first = CodeApplyPatchConnector.invoke(&ctx, &params).unwrap();
        let second = CodeApplyPatchConnector.invoke(&ctx, &params).unwrap();

        assert_eq!(first, second);
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "alpha\ngamma\n");
    }

    #[test]
    fn apply_patch_returns_diff_summary() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "alpha\nbeta\n").unwrap();

        let result = CodeApplyPatchConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "file_path": file.to_str().unwrap(),
                    "patch": "@@ -1,2 +1,2 @@\n alpha\n-beta\n+gamma"
                }),
            )
            .unwrap();

        assert!(result["diff"]["unified"].as_str().unwrap().contains("+gamma"));
        assert_eq!(result["diff"]["lines_added"], 1);
        assert_eq!(result["diff"]["lines_removed"], 1);
    }

    #[test]
    fn apply_patch_invalid_patch_format() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sample.rs");
        std::fs::write(&file, "alpha\n").unwrap();

        let result = CodeApplyPatchConnector.invoke(
            &test_ctx(),
            &json!({"file_path": file.to_str().unwrap(), "patch": "not a diff"}),
        );

        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn apply_patch_missing_file_path() {
        let result = CodeApplyPatchConnector.invoke(&test_ctx(), &json!({"patch": "x"}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn apply_patch_nonexistent_file_not_creation() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("missing.rs");

        let result = CodeApplyPatchConnector.invoke(
            &test_ctx(),
            &json!({
                "file_path": file.to_str().unwrap(),
                "patch": "@@ -1,1 +1,1 @@\n-old\n+new"
            }),
        );

        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn apply_patch_descriptor_metadata() {
        let desc = CodeApplyPatchConnector.describe();
        assert_eq!(desc.name, "code.apply_patch");
        assert_eq!(desc.category, ConnectorCategory::Code);
        assert!(desc.is_mutating);
        assert!(desc.requires_read_before_write);
        assert!(!desc.is_read_only);
        assert!(!desc.is_concurrency_safe);
    }
}
