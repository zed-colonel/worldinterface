use std::path::{Path, PathBuf};

use ignore::WalkBuilder;
use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

pub struct CodeLsConnector;

impl Connector for CodeLsConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.ls".into(),
            display_name: "Code List Directory".into(),
            description: "Lists directory contents with type indicators, respecting .gitignore. Supports depth control and entry limits for navigating large directories.".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "The absolute path to the directory to list"
                    },
                    "depth": {
                        "type": "integer",
                        "description": "Maximum recursion depth. Default: 2",
                        "minimum": 1
                    },
                    "max_entries": {
                        "type": "integer",
                        "description": "Maximum number of entries to return. Default: 200",
                        "minimum": 1
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "total_entries": { "type": "integer" },
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
        let path_str = params.get("path").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'path' (expected string)".into())
        })?;
        let depth = params.get("depth").and_then(Value::as_u64).unwrap_or(2) as usize;
        let max_entries = params.get("max_entries").and_then(Value::as_u64).unwrap_or(200) as usize;

        let root = validate_directory(path_str)?;
        let mut entries = collect_entries(&root, depth)?;
        entries.sort_by(|left, right| left.components.cmp(&right.components));

        let total_entries = entries.len();
        let truncated = total_entries > max_entries;
        let content = entries
            .into_iter()
            .take(max_entries)
            .map(|entry| format!("{}{}", "  ".repeat(entry.depth), entry.label))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(json!({
            "path": root.display().to_string(),
            "content": content,
            "total_entries": total_entries,
            "truncated": truncated,
        }))
    }
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ListedEntry {
    components: Vec<String>,
    depth: usize,
    label: String,
}

fn validate_directory(path_str: &str) -> Result<PathBuf, ConnectorError> {
    let path = Path::new(path_str);
    if !path.exists() {
        return Err(ConnectorError::Terminal(format!("directory not found: {path_str}")));
    }

    let root = path.canonicalize().map_err(|err| match err.kind() {
        std::io::ErrorKind::PermissionDenied => {
            ConnectorError::Terminal(format!("permission denied: {path_str}"))
        }
        _ => ConnectorError::Retryable(format!("I/O error on {path_str}: {err}")),
    })?;

    if !root.is_dir() {
        return Err(ConnectorError::Terminal(format!("not a directory: {path_str}")));
    }

    Ok(root)
}

fn collect_entries(root: &Path, depth: usize) -> Result<Vec<ListedEntry>, ConnectorError> {
    let mut entries = Vec::new();
    let walker = WalkBuilder::new(root).max_depth(Some(depth)).build();

    for item in walker {
        let entry = item.map_err(walk_error)?;
        if entry.depth() == 0 {
            continue;
        }

        let rel = entry.path().strip_prefix(root).map_err(|err| {
            ConnectorError::Retryable(format!("failed to relativize path: {err}"))
        })?;
        if rel.as_os_str().is_empty() {
            continue;
        }

        let file_type = std::fs::symlink_metadata(entry.path())
            .map_err(|err| walk_path_error(entry.path(), &err))?
            .file_type();
        let mut components = rel
            .components()
            .map(|component| component.as_os_str().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        let name = components.pop().unwrap_or_default();
        let suffix = if file_type.is_symlink() {
            "@"
        } else if file_type.is_dir() {
            "/"
        } else {
            ""
        };
        components.push(name.clone());

        entries.push(ListedEntry {
            depth: components.len().saturating_sub(1),
            label: format!("{name}{suffix}"),
            components,
        });
    }

    Ok(entries)
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

fn walk_path_error(path: &Path, err: &std::io::Error) -> ConnectorError {
    match err.kind() {
        std::io::ErrorKind::PermissionDenied => {
            ConnectorError::Terminal(format!("permission denied: {}", path.display()))
        }
        _ => ConnectorError::Retryable(format!("I/O error on {}: {err}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

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
    fn ls_basic_listing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Cargo.toml"), "[package]\n").unwrap();
        std::fs::create_dir(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src").join("main.rs"), "fn main() {}\n").unwrap();

        let result = CodeLsConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "depth": 2}))
            .unwrap();
        let content = result["content"].as_str().unwrap();

        assert!(content.contains("Cargo.toml"));
        assert!(content.contains("src/"));
        assert!(content.contains("  main.rs"));
    }

    #[test]
    fn ls_depth_control() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src").join("nested")).unwrap();
        std::fs::write(dir.path().join("src").join("nested").join("lib.rs"), "x\n").unwrap();

        let shallow = CodeLsConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "depth": 1}))
            .unwrap();
        let deep = CodeLsConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "depth": 3}))
            .unwrap();

        assert!(!shallow["content"].as_str().unwrap().contains("lib.rs"));
        assert!(deep["content"].as_str().unwrap().contains("    lib.rs"));
    }

    #[test]
    fn ls_respects_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".gitignore"), "ignored/\n").unwrap();
        std::fs::create_dir_all(dir.path().join("ignored")).unwrap();
        std::fs::write(dir.path().join("ignored").join("secret.txt"), "x\n").unwrap();
        std::fs::write(dir.path().join("keep.txt"), "ok\n").unwrap();

        let result = CodeLsConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "depth": 3}))
            .unwrap();
        let content = result["content"].as_str().unwrap();

        assert!(content.contains("keep.txt"));
        assert!(!content.contains("ignored/"));
        assert!(!content.contains("secret.txt"));
    }

    #[test]
    fn ls_max_entries_truncation() {
        let dir = tempfile::tempdir().unwrap();
        for index in 0..4 {
            std::fs::write(dir.path().join(format!("file-{index}.txt")), "x\n").unwrap();
        }

        let result = CodeLsConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "max_entries": 2}))
            .unwrap();

        assert_eq!(result["total_entries"], 4);
        assert_eq!(result["truncated"], true);
        assert_eq!(result["content"].as_str().unwrap().lines().count(), 2);
    }

    #[test]
    fn ls_empty_directory() {
        let dir = tempfile::tempdir().unwrap();

        let result = CodeLsConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap()}))
            .unwrap();

        assert_eq!(result["content"], "");
        assert_eq!(result["total_entries"], 0);
        assert_eq!(result["truncated"], false);
    }

    #[test]
    fn ls_nonexistent_path_terminal() {
        let result = CodeLsConnector.invoke(&test_ctx(), &json!({"path": "/tmp/no-such-code-ls"}));
        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn ls_missing_path_invalid_params() {
        let result = CodeLsConnector.invoke(&test_ctx(), &json!({}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn ls_type_indicators() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("plain.txt"), "x\n").unwrap();
        std::fs::create_dir(dir.path().join("dir")).unwrap();
        symlink(dir.path().join("plain.txt"), dir.path().join("link")).unwrap();

        let result = CodeLsConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap()}))
            .unwrap();
        let content = result["content"].as_str().unwrap();

        assert!(content.contains("plain.txt"));
        assert!(content.contains("dir/"));
        assert!(content.contains("link@"));
    }

    #[test]
    fn ls_descriptor_metadata() {
        let desc = CodeLsConnector.describe();
        assert_eq!(desc.name, "code.ls");
        assert_eq!(desc.category, ConnectorCategory::Code);
        assert!(desc.is_read_only);
        assert!(desc.is_concurrency_safe);
        assert!(!desc.is_mutating);
        assert!(!desc.requires_read_before_write);
    }
}
