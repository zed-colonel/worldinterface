use std::path::{Path, PathBuf};
use std::time::SystemTime;

use globset::Glob;
use ignore::WalkBuilder;
use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

pub struct CodeGlobConnector;

impl Connector for CodeGlobConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.glob".into(),
            display_name: "Code Glob".into(),
            description: "Finds files matching a glob pattern, sorted by modification time (most recent first). Respects .gitignore. Supports standard glob syntax: *, **, ?, {a,b}, [abc].".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["pattern"],
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern to match files (e.g., '**/*.rs', 'src/**/*.ts')"
                    },
                    "path": {
                        "type": "string",
                        "description": "Directory to search in. Defaults to current working directory"
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "files": { "type": "array", "items": { "type": "string" } },
                    "total_matches": { "type": "integer" },
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
        let pattern = params.get("pattern").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'pattern' (expected string)".into())
        })?;
        let matcher = Glob::new(pattern)
            .map_err(|err| ConnectorError::InvalidParams(format!("invalid glob pattern: {err}")))?
            .compile_matcher();

        let root = resolve_root(params.get("path").and_then(Value::as_str))?;
        let mut matches = Vec::new();
        let walker = WalkBuilder::new(&root).build();

        for item in walker {
            let entry = item.map_err(walk_error)?;
            if entry.depth() == 0
                || !entry.file_type().map(|file_type| file_type.is_file()).unwrap_or(false)
            {
                continue;
            }

            let rel = entry.path().strip_prefix(&root).map_err(|err| {
                ConnectorError::Retryable(format!("failed to relativize path: {err}"))
            })?;
            if matcher.is_match(normalize_path(rel)) {
                let modified = std::fs::metadata(entry.path())
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                matches.push((entry.path().to_path_buf(), modified));
            }
        }

        matches.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

        let total_matches = matches.len();
        let truncated = total_matches > 1000;
        let files = matches
            .into_iter()
            .take(1000)
            .map(|(path, _)| path.display().to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "files": files,
            "total_matches": total_matches,
            "truncated": truncated,
        }))
    }
}

fn resolve_root(path_str: Option<&str>) -> Result<PathBuf, ConnectorError> {
    let root = match path_str {
        Some(path) => PathBuf::from(path),
        None => std::env::current_dir().map_err(|err| {
            ConnectorError::Retryable(format!("failed to get current directory: {err}"))
        })?,
    };

    if !root.exists() {
        return Err(ConnectorError::Terminal(format!("directory not found: {}", root.display())));
    }

    let root = root.canonicalize().map_err(|err| match err.kind() {
        std::io::ErrorKind::PermissionDenied => {
            ConnectorError::Terminal(format!("permission denied: {}", root.display()))
        }
        _ => ConnectorError::Retryable(format!("I/O error on {}: {err}", root.display())),
    })?;
    if !root.is_dir() {
        return Err(ConnectorError::Terminal(format!("not a directory: {}", root.display())));
    }

    Ok(root)
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn walk_error(err: ignore::Error) -> ConnectorError {
    if let Some(io_err) = err.io_error() {
        return match io_err.kind() {
            std::io::ErrorKind::PermissionDenied => {
                ConnectorError::Terminal("permission denied during directory walk".into())
            }
            _ => ConnectorError::Retryable(format!("directory walk error: {io_err}")),
        };
    }
    ConnectorError::Retryable(format!("directory walk error: {err}"))
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

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
    fn glob_basic_pattern() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("main.rs"), "fn main() {}\n").unwrap();
        std::fs::write(dir.path().join("README.md"), "# readme\n").unwrap();

        let result = CodeGlobConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "pattern": "*.rs"}))
            .unwrap();
        let files = result["files"].as_array().unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0], json!(dir.path().join("main.rs").display().to_string()));
    }

    #[test]
    fn glob_recursive_pattern() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src").join("nested")).unwrap();
        std::fs::write(dir.path().join("src").join("nested").join("lib.rs"), "x\n").unwrap();

        let result = CodeGlobConnector
            .invoke(
                &test_ctx(),
                &json!({"path": dir.path().to_str().unwrap(), "pattern": "**/*.rs"}),
            )
            .unwrap();

        assert_eq!(result["total_matches"], 1);
        assert_eq!(
            result["files"][0],
            json!(dir.path().join("src").join("nested").join("lib.rs").display().to_string())
        );
    }

    #[test]
    fn glob_respects_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".gitignore"), "ignored/\n").unwrap();
        std::fs::create_dir_all(dir.path().join("ignored")).unwrap();
        std::fs::write(dir.path().join("ignored").join("hidden.rs"), "x\n").unwrap();
        std::fs::write(dir.path().join("visible.rs"), "x\n").unwrap();

        let result = CodeGlobConnector
            .invoke(
                &test_ctx(),
                &json!({"path": dir.path().to_str().unwrap(), "pattern": "**/*.rs"}),
            )
            .unwrap();
        let files = result["files"].as_array().unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0], json!(dir.path().join("visible.rs").display().to_string()));
    }

    #[test]
    fn glob_sorted_by_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let older = dir.path().join("older.rs");
        let newer = dir.path().join("newer.rs");
        std::fs::write(&older, "old\n").unwrap();
        sleep(Duration::from_millis(20));
        std::fs::write(&newer, "new\n").unwrap();

        let result = CodeGlobConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "pattern": "*.rs"}))
            .unwrap();
        let files = result["files"].as_array().unwrap();

        assert_eq!(files[0], json!(newer.display().to_string()));
        assert_eq!(files[1], json!(older.display().to_string()));
    }

    #[test]
    fn glob_no_matches_empty() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("main.rs"), "fn main() {}\n").unwrap();

        let result = CodeGlobConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "pattern": "*.ts"}))
            .unwrap();

        assert_eq!(result["files"], json!([]));
        assert_eq!(result["total_matches"], 0);
        assert_eq!(result["truncated"], false);
    }

    #[test]
    fn glob_nonexistent_path_terminal() {
        let result = CodeGlobConnector
            .invoke(&test_ctx(), &json!({"path": "/tmp/no-such-code-glob", "pattern": "*.rs"}));
        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn glob_missing_pattern_invalid_params() {
        let result = CodeGlobConnector.invoke(&test_ctx(), &json!({}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn glob_invalid_pattern_invalid_params() {
        let dir = tempfile::tempdir().unwrap();
        let result = CodeGlobConnector.invoke(
            &test_ctx(),
            &json!({"path": dir.path().to_str().unwrap(), "pattern": "[invalid"}),
        );
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn glob_descriptor_metadata() {
        let desc = CodeGlobConnector.describe();
        assert_eq!(desc.name, "code.glob");
        assert_eq!(desc.category, ConnectorCategory::Code);
        assert!(desc.is_read_only);
        assert!(desc.is_concurrency_safe);
        assert!(!desc.is_mutating);
        assert!(!desc.requires_read_before_write);
    }
}
