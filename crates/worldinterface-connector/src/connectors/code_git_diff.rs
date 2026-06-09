//! `code.git_diff` connector (E10-S3, W-103).
//!
//! Produces structured diff output from git state. Read-only, concurrency-safe.
//! Shells out to `git diff` with appropriate flags and parses the output.

use std::path::Path;
use std::process::Command;

use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

pub struct CodeGitDiffConnector;

impl Connector for CodeGitDiffConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.git_diff".into(),
            display_name: "Code Git Diff".into(),
            description: "Produces structured diff output from git state. Supports unstaged, \
                          staged, and committed (between refs) modes."
                .into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the git repository root"
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["unstaged", "staged", "committed"],
                        "description": "Diff mode: unstaged (default), staged, or committed"
                    },
                    "from_ref": {
                        "type": "string",
                        "description": "Starting ref for committed mode (e.g., HEAD~3, main)"
                    },
                    "to_ref": {
                        "type": "string",
                        "description": "Ending ref for committed mode (e.g., HEAD)"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Optional: scope diff to a single file (relative to repo root)"
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "mode": { "type": "string" },
                    "files": { "type": "array" },
                    "total_files": { "type": "integer" },
                    "total_added": { "type": "integer" },
                    "total_removed": { "type": "integer" }
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
        let repo_path = params
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ConnectorError::InvalidParams("missing 'path'".into()))?;
        let mode = params.get("mode").and_then(Value::as_str).unwrap_or("unstaged");
        let file_path = params.get("file_path").and_then(Value::as_str);

        let repo = Path::new(repo_path);
        if !repo.exists() {
            return Err(ConnectorError::terminal(format!(
                "repository path does not exist: {repo_path}"
            )));
        }

        let check = Command::new("git")
            .args(["rev-parse", "--git-dir"])
            .current_dir(repo)
            .output()
            .map_err(|e| ConnectorError::terminal(format!("failed to run git: {e}")))?;
        if !check.status.success() {
            return Err(ConnectorError::terminal(format!("not a git repository: {repo_path}")));
        }

        let mut args =
            vec!["diff".to_string(), "--no-ext-diff".to_string(), "--find-renames".to_string()];
        match mode {
            "unstaged" => {}
            "staged" => args.push("--cached".into()),
            "committed" => {
                let from_ref = params.get("from_ref").and_then(Value::as_str).unwrap_or("HEAD~1");
                let to_ref = params.get("to_ref").and_then(Value::as_str).unwrap_or("HEAD");
                args.push(from_ref.into());
                args.push(to_ref.into());
            }
            other => return Err(ConnectorError::InvalidParams(format!("invalid 'mode': {other}"))),
        }

        if let Some(file_path) = file_path {
            args.push("--".into());
            args.push(file_path.into());
        }

        let output = Command::new("git")
            .args(&args)
            .current_dir(repo)
            .output()
            .map_err(|e| ConnectorError::terminal(format!("git diff failed: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(ConnectorError::terminal(format!("git diff failed: {stderr}")));
        }

        let diff_text = String::from_utf8_lossy(&output.stdout);
        let files = parse_diff_output(&diff_text);
        let total_added: i64 =
            files.iter().map(|file| file["lines_added"].as_i64().unwrap_or(0)).sum();
        let total_removed: i64 =
            files.iter().map(|file| file["lines_removed"].as_i64().unwrap_or(0)).sum();

        Ok(json!({
            "mode": mode,
            "files": files,
            "total_files": files.len(),
            "total_added": total_added,
            "total_removed": total_removed,
        }))
    }
}

/// Extract the file path from a `diff --git a/PATH b/PATH` line.
///
/// Handles paths containing spaces and ` b/` substrings by using the
/// known structure: both paths are identical, so `a/X b/X` means the
/// boundary ` b/` is exactly at `prefix_len + path_len`.
fn extract_diff_path(line: &str) -> Option<String> {
    // "diff --git a/PATH b/PATH"
    let after_prefix = line.strip_prefix("diff --git a/")?;
    // after_prefix is "PATH b/PATH" — the path repeats, so its length is
    // (total_len - len(" b/")) / 2
    let sep = " b/";
    let total = after_prefix.len();
    // The separator " b/" has length 3; total = path_len + 3 + path_len
    if total < 4 {
        return None;
    }
    let path_len = (total - sep.len()) / 2;
    let path = &after_prefix[..path_len];

    // Sanity check: verify the b/ portion matches
    let expected_suffix = format!("{sep}{path}");
    if after_prefix.ends_with(&expected_suffix) {
        Some(path.to_string())
    } else {
        // Fallback for edge cases (e.g., renames where a/ and b/ paths differ)
        after_prefix.rsplit_once(sep).map(|(_, b_path)| b_path.to_string())
    }
}

/// Parse unified diff output into structured per-file entries.
fn parse_diff_output(diff_text: &str) -> Vec<Value> {
    let mut files = Vec::new();
    let mut current_path: Option<String> = None;
    let mut current_diff = String::new();
    let mut added = 0i64;
    let mut removed = 0i64;
    let mut operation = "modified";

    for line in diff_text.lines() {
        if line.starts_with("diff --git ") {
            if let Some(path) = current_path.take() {
                files.push(json!({
                    "path": path,
                    "operation": operation,
                    "lines_added": added,
                    "lines_removed": removed,
                    "diff": current_diff,
                }));
            }

            current_path = extract_diff_path(line);
            current_diff.clear();
            current_diff.push_str(line);
            current_diff.push('\n');
            added = 0;
            removed = 0;
            operation = "modified";
            continue;
        }

        if current_path.is_none() {
            continue;
        }

        current_diff.push_str(line);
        current_diff.push('\n');

        if line.starts_with('+') && !line.starts_with("+++") {
            added += 1;
        } else if line.starts_with('-') && !line.starts_with("---") {
            removed += 1;
        } else if line.starts_with("new file") {
            operation = "added";
        } else if line.starts_with("deleted file") {
            operation = "deleted";
        } else if line.starts_with("rename from") {
            operation = "renamed";
        }
    }

    if let Some(path) = current_path {
        files.push(json!({
            "path": path,
            "operation": operation,
            "lines_added": added,
            "lines_removed": removed,
            "diff": current_diff,
        }));
    }

    files
}

#[cfg(test)]
mod tests {
    use std::process::Command;

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

    fn init_git_repo(dir: &std::path::Path) {
        Command::new("git").args(["init"]).current_dir(dir).output().unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(dir)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(dir)
            .output()
            .unwrap();
        std::fs::write(dir.join("hello.txt"), "hello world\n").unwrap();
        Command::new("git").args(["add", "."]).current_dir(dir).output().unwrap();
        Command::new("git").args(["commit", "-m", "initial"]).current_dir(dir).output().unwrap();
    }

    #[test]
    fn describe_is_read_only() {
        let desc = CodeGitDiffConnector.describe();
        assert_eq!(desc.name, "code.git_diff");
        assert!(desc.is_read_only);
        assert!(!desc.is_mutating);
        assert!(desc.is_concurrency_safe);
        assert_eq!(desc.category, ConnectorCategory::Code);
    }

    #[test]
    fn unstaged_diff_with_changes() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join("hello.txt"), "hello world\nline 2\n").unwrap();

        let result = CodeGitDiffConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "mode": "unstaged"
                }),
            )
            .unwrap();

        assert_eq!(result["mode"], "unstaged");
        assert_eq!(result["total_files"], 1);
        assert!(result["total_added"].as_i64().unwrap() > 0);
        let files = result["files"].as_array().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["path"], "hello.txt");
        assert_eq!(files[0]["operation"], "modified");
        assert!(!files[0]["diff"].as_str().unwrap().is_empty());
    }

    #[test]
    fn staged_diff() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join("hello.txt"), "changed\n").unwrap();
        Command::new("git").args(["add", "hello.txt"]).current_dir(dir.path()).output().unwrap();

        let result = CodeGitDiffConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "mode": "staged"
                }),
            )
            .unwrap();

        assert_eq!(result["mode"], "staged");
        assert_eq!(result["total_files"], 1);
    }

    #[test]
    fn committed_diff_between_refs() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join("hello.txt"), "changed\n").unwrap();
        Command::new("git").args(["add", "."]).current_dir(dir.path()).output().unwrap();
        Command::new("git")
            .args(["commit", "-m", "second"])
            .current_dir(dir.path())
            .output()
            .unwrap();

        let result = CodeGitDiffConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "mode": "committed",
                    "from_ref": "HEAD~1",
                    "to_ref": "HEAD"
                }),
            )
            .unwrap();

        assert_eq!(result["mode"], "committed");
        assert_eq!(result["total_files"], 1);
    }

    #[test]
    fn no_changes_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());

        let result = CodeGitDiffConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "mode": "unstaged"
                }),
            )
            .unwrap();

        assert_eq!(result["total_files"], 0);
        assert!(result["files"].as_array().unwrap().is_empty());
    }

    #[test]
    fn scoped_to_single_file() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join("hello.txt"), "changed\n").unwrap();
        std::fs::write(dir.path().join("other.txt"), "new file\n").unwrap();
        Command::new("git").args(["add", "other.txt"]).current_dir(dir.path()).output().unwrap();

        let result = CodeGitDiffConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "mode": "unstaged",
                    "file_path": "hello.txt"
                }),
            )
            .unwrap();

        assert_eq!(result["total_files"], 1);
        let files = result["files"].as_array().unwrap();
        assert_eq!(files[0]["path"], "hello.txt");
    }

    #[test]
    fn not_a_git_repo() {
        let dir = tempfile::tempdir().unwrap();

        let result = CodeGitDiffConnector.invoke(
            &test_ctx(),
            &json!({
                "path": dir.path().to_str().unwrap(),
                "mode": "unstaged"
            }),
        );

        assert!(result.is_err());
    }

    #[test]
    fn default_mode_is_unstaged() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());

        let result = CodeGitDiffConnector
            .invoke(&test_ctx(), &json!({ "path": dir.path().to_str().unwrap() }))
            .unwrap();

        assert_eq!(result["mode"], "unstaged");
    }

    #[test]
    fn detects_new_file() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join("new_file.rs"), "fn main() {}\n").unwrap();
        Command::new("git").args(["add", "new_file.rs"]).current_dir(dir.path()).output().unwrap();

        let result = CodeGitDiffConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "mode": "staged"
                }),
            )
            .unwrap();

        let files = result["files"].as_array().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["operation"], "added");
    }

    #[test]
    fn parse_diff_basic() {
        let diff = "diff --git a/src/lib.rs b/src/lib.rs\n--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1,3 +1,4 @@\n line 1\n-old\n+new\n+added\n line 3\n";
        let files = parse_diff_output(diff);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["path"], "src/lib.rs");
        assert_eq!(files[0]["lines_added"], 2);
        assert_eq!(files[0]["lines_removed"], 1);
    }

    #[test]
    fn extract_diff_path_simple() {
        let path = extract_diff_path("diff --git a/src/lib.rs b/src/lib.rs");
        assert_eq!(path.as_deref(), Some("src/lib.rs"));
    }

    #[test]
    fn extract_diff_path_with_b_in_name() {
        // Path containing " b/" as a substring should still parse correctly
        let path = extract_diff_path("diff --git a/lib b/utils.rs b/lib b/utils.rs");
        assert_eq!(path.as_deref(), Some("lib b/utils.rs"));
    }

    #[test]
    fn extract_diff_path_spaces() {
        let path = extract_diff_path("diff --git a/my file.rs b/my file.rs");
        assert_eq!(path.as_deref(), Some("my file.rs"));
    }
}
