use std::collections::HashSet;
use std::path::{Path, PathBuf};

use globset::Glob;
use ignore::WalkBuilder;
use regex::RegexBuilder;
use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

pub struct CodeGrepConnector;

impl Connector for CodeGrepConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.grep".into(),
            display_name: "Code Grep".into(),
            description: "Searches file contents using regex patterns. Supports multiple output modes (matching lines, file paths, counts), glob-based file filtering, context lines, and case-insensitive matching. Respects .gitignore.".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["pattern"],
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regex pattern to search for in file contents"
                    },
                    "path": {
                        "type": "string",
                        "description": "Directory to search in. Defaults to current working directory"
                    },
                    "glob": {
                        "type": "string",
                        "description": "File pattern filter (e.g., '*.rs', '*.{ts,tsx}')"
                    },
                    "output_mode": {
                        "type": "string",
                        "enum": ["content", "files_with_matches", "count"],
                        "description": "Output format. Default: files_with_matches"
                    },
                    "case_insensitive": {
                        "type": "boolean",
                        "description": "Enable case-insensitive matching. Default: false"
                    },
                    "context_before": {
                        "type": "integer",
                        "description": "Number of lines to show before each match (content mode only)",
                        "minimum": 0
                    },
                    "context_after": {
                        "type": "integer",
                        "description": "Number of lines to show after each match (content mode only)",
                        "minimum": 0
                    },
                    "context": {
                        "type": "integer",
                        "description": "Number of lines before and after each match (content mode only). Overridden by context_before or context_after when specified.",
                        "minimum": 0
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum number of matching lines/files to return. Default: 250",
                        "minimum": 1
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "total_matches": { "type": "integer" },
                    "files_searched": { "type": "integer" },
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
        let regex = RegexBuilder::new(pattern)
            .case_insensitive(
                params.get("case_insensitive").and_then(Value::as_bool).unwrap_or(false),
            )
            .build()
            .map_err(|err| ConnectorError::InvalidParams(format!("invalid regex: {err}")))?;
        let root = resolve_root(params.get("path").and_then(Value::as_str))?;
        let glob_matcher = params
            .get("glob")
            .and_then(Value::as_str)
            .map(|pattern| {
                Glob::new(pattern).map(|glob| glob.compile_matcher()).map_err(|err| {
                    ConnectorError::InvalidParams(format!("invalid glob filter: {err}"))
                })
            })
            .transpose()?;

        let output_mode = OutputMode::parse(params.get("output_mode").and_then(Value::as_str))?;
        let shared_context = params.get("context").and_then(Value::as_u64).unwrap_or(0) as usize;
        let context_before = params
            .get("context_before")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(shared_context);
        let context_after = params
            .get("context_after")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(shared_context);
        let max_results = params.get("max_results").and_then(Value::as_u64).unwrap_or(250) as usize;

        let walker = WalkBuilder::new(&root).build();
        let mut files_searched = 0usize;
        let mut total_matches = 0usize;
        let mut truncated = false;
        let mut rendered_entries = 0usize;
        let mut output = Vec::new();

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
            if let Some(matcher) = &glob_matcher {
                if !matcher.is_match(normalize_path(rel)) {
                    continue;
                }
            }

            let bytes = match std::fs::read(entry.path()) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            if bytes.contains(&0) {
                continue;
            }

            let content = match String::from_utf8(bytes) {
                Ok(content) => content,
                Err(_) => continue,
            };
            files_searched += 1;

            let lines = content.lines().map(str::to_string).collect::<Vec<_>>();
            let match_lines = lines
                .iter()
                .enumerate()
                .filter_map(|(index, line)| regex.is_match(line).then_some(index))
                .collect::<Vec<_>>();
            if match_lines.is_empty() {
                continue;
            }

            let path_str = entry.path().display().to_string();
            match output_mode {
                OutputMode::FilesWithMatches => {
                    total_matches += 1;
                    if rendered_entries < max_results {
                        output.push(path_str);
                        rendered_entries += 1;
                    } else {
                        truncated = true;
                    }
                }
                OutputMode::Count => {
                    total_matches += match_lines.len();
                    if rendered_entries < max_results {
                        output.push(format!("{path_str}:{}", match_lines.len()));
                        rendered_entries += 1;
                    } else {
                        truncated = true;
                    }
                }
                OutputMode::Content => {
                    total_matches += match_lines.len();
                    if rendered_entries >= max_results {
                        truncated = true;
                        continue;
                    }

                    let (lines_out, rendered_matches, was_truncated) = render_content_groups(
                        &path_str,
                        &lines,
                        &match_lines,
                        context_before,
                        context_after,
                        max_results - rendered_entries,
                    );
                    if !lines_out.is_empty() {
                        output.extend(lines_out);
                    }
                    rendered_entries += rendered_matches;
                    truncated |= was_truncated || rendered_entries < total_matches;
                }
            }
        }

        Ok(json!({
            "content": output.join("\n"),
            "total_matches": total_matches,
            "files_searched": files_searched,
            "truncated": truncated,
        }))
    }
}

#[derive(Clone, Copy)]
enum OutputMode {
    Content,
    FilesWithMatches,
    Count,
}

impl OutputMode {
    fn parse(value: Option<&str>) -> Result<Self, ConnectorError> {
        match value.unwrap_or("files_with_matches") {
            "content" => Ok(Self::Content),
            "files_with_matches" => Ok(Self::FilesWithMatches),
            "count" => Ok(Self::Count),
            other => Err(ConnectorError::InvalidParams(format!("invalid output_mode: {other}"))),
        }
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

fn render_content_groups(
    path: &str,
    lines: &[String],
    matches: &[usize],
    context_before: usize,
    context_after: usize,
    limit: usize,
) -> (Vec<String>, usize, bool) {
    if matches.is_empty() || limit == 0 {
        return (Vec::new(), 0, !matches.is_empty());
    }

    let mut groups: Vec<(usize, usize, Vec<usize>)> = Vec::new();
    for &match_index in matches {
        let start = match_index.saturating_sub(context_before);
        let end = (match_index + context_after).min(lines.len().saturating_sub(1));
        if let Some((last_start, last_end, members)) = groups.last_mut() {
            if start <= *last_end + 1 {
                *last_start = (*last_start).min(start);
                *last_end = (*last_end).max(end);
                members.push(match_index);
                continue;
            }
        }
        groups.push((start, end, vec![match_index]));
    }

    let mut output = Vec::new();
    let mut rendered_matches = 0usize;
    let mut truncated = false;

    for (start, end, members) in groups {
        if rendered_matches >= limit {
            truncated = true;
            break;
        }

        if !output.is_empty() {
            output.push("--".into());
        }

        let member_set = members.into_iter().collect::<HashSet<_>>();
        for (line_index, line) in lines.iter().enumerate().take(end + 1).skip(start) {
            let is_match = member_set.contains(&line_index);
            if is_match && rendered_matches >= limit {
                truncated = true;
                break;
            }

            let separator = if is_match { ':' } else { '-' };
            output.push(format!("{path}{separator}{}{separator}{line}", line_index + 1));
            if is_match {
                rendered_matches += 1;
            }
        }
    }

    if rendered_matches < matches.len() {
        truncated = true;
    }

    (output, rendered_matches, truncated)
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
    fn grep_basic_files_with_matches() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.rs");
        let b = dir.path().join("b.rs");
        std::fs::write(&a, "fn alpha() {}\n").unwrap();
        std::fs::write(&b, "fn beta() {}\n").unwrap();

        let result = CodeGrepConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "pattern": "alpha"}))
            .unwrap();

        assert_eq!(result["total_matches"], 1);
        assert_eq!(result["files_searched"], 2);
        assert_eq!(result["content"], json!(a.display().to_string()));
    }

    #[test]
    fn grep_content_mode() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("main.rs");
        std::fs::write(&file, "fn alpha() {}\nfn beta() {}\n").unwrap();

        let result = CodeGrepConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "pattern": "beta",
                    "output_mode": "content"
                }),
            )
            .unwrap();

        assert_eq!(result["content"], json!(format!("{}:2:fn beta() {{}}", file.display())));
        assert_eq!(result["total_matches"], 1);
    }

    #[test]
    fn grep_count_mode() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("main.rs");
        std::fs::write(&file, "alpha\nalpha\nbeta\n").unwrap();

        let result = CodeGrepConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "pattern": "alpha",
                    "output_mode": "count"
                }),
            )
            .unwrap();

        assert_eq!(result["content"], json!(format!("{}:2", file.display())));
        assert_eq!(result["total_matches"], 2);
    }

    #[test]
    fn grep_glob_filter() {
        let dir = tempfile::tempdir().unwrap();
        let rs = dir.path().join("lib.rs");
        let md = dir.path().join("README.md");
        std::fs::write(&rs, "alpha\n").unwrap();
        std::fs::write(&md, "alpha\n").unwrap();

        let result = CodeGrepConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "pattern": "alpha",
                    "glob": "*.rs"
                }),
            )
            .unwrap();

        assert_eq!(result["content"], json!(rs.display().to_string()));
        assert_eq!(result["files_searched"], 1);
    }

    #[test]
    fn grep_case_insensitive() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("main.rs");
        std::fs::write(&file, "Alpha\n").unwrap();

        let result = CodeGrepConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "pattern": "alpha",
                    "case_insensitive": true
                }),
            )
            .unwrap();

        assert_eq!(result["content"], json!(file.display().to_string()));
    }

    #[test]
    fn grep_context_lines() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("main.rs");
        std::fs::write(&file, "before\nmatch\nafter\n").unwrap();

        let result = CodeGrepConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "pattern": "match",
                    "output_mode": "content",
                    "context": 1
                }),
            )
            .unwrap();
        let content = result["content"].as_str().unwrap();

        assert!(content.contains(&format!("{}-1-before", file.display())));
        assert!(content.contains(&format!("{}:2:match", file.display())));
        assert!(content.contains(&format!("{}-3-after", file.display())));
    }

    #[test]
    fn grep_respects_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".gitignore"), "ignored/\n").unwrap();
        std::fs::create_dir_all(dir.path().join("ignored")).unwrap();
        std::fs::write(dir.path().join("ignored").join("secret.rs"), "alpha\n").unwrap();
        let visible = dir.path().join("visible.rs");
        std::fs::write(&visible, "alpha\n").unwrap();

        let result = CodeGrepConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "pattern": "alpha"}))
            .unwrap();

        assert_eq!(result["content"], json!(visible.display().to_string()));
        assert_eq!(result["files_searched"], 1);
    }

    #[test]
    fn grep_no_matches_empty() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("main.rs"), "alpha\n").unwrap();

        let result = CodeGrepConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "pattern": "beta"}))
            .unwrap();

        assert_eq!(result["content"], "");
        assert_eq!(result["total_matches"], 0);
        assert_eq!(result["truncated"], false);
    }

    #[test]
    fn grep_invalid_regex_invalid_params() {
        let dir = tempfile::tempdir().unwrap();
        let result = CodeGrepConnector
            .invoke(&test_ctx(), &json!({"path": dir.path().to_str().unwrap(), "pattern": "["}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn grep_missing_pattern_invalid_params() {
        let result = CodeGrepConnector.invoke(&test_ctx(), &json!({}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn grep_nonexistent_path_terminal() {
        let result = CodeGrepConnector
            .invoke(&test_ctx(), &json!({"path": "/tmp/no-such-code-grep", "pattern": "alpha"}));
        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    #[test]
    fn grep_max_results_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("main.rs");
        std::fs::write(&file, "alpha\nalpha\nalpha\n").unwrap();

        let result = CodeGrepConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "pattern": "alpha",
                    "output_mode": "content",
                    "max_results": 2
                }),
            )
            .unwrap();

        assert_eq!(result["total_matches"], 3);
        assert_eq!(result["truncated"], true);
        assert_eq!(result["content"].as_str().unwrap().matches(":").count(), 4);
    }

    #[test]
    fn grep_descriptor_metadata() {
        let desc = CodeGrepConnector.describe();
        assert_eq!(desc.name, "code.grep");
        assert_eq!(desc.category, ConnectorCategory::Code);
        assert!(desc.is_read_only);
        assert!(desc.is_concurrency_safe);
        assert!(!desc.is_mutating);
        assert!(!desc.requires_read_before_write);
    }
}
