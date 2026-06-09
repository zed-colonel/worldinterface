use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use ignore::WalkBuilder;
use regex::{Regex, RegexBuilder};
use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

const DEFAULT_FILE_LIMIT: usize = 6;
const DEFAULT_CONTEXT_LINES: usize = 2;
const DEFAULT_MAX_MATCHES_PER_FILE: usize = 5;
const MAX_FILE_LIMIT: usize = 20;
const MAX_CONTEXT_LINES: usize = 8;
const MAX_MATCHES_PER_FILE: usize = 20;
const MAX_FILE_BYTES: usize = 512 * 1024;
const MAX_SCAN_FILES: usize = 10_000;
const MAX_PATTERNS: usize = 16;

pub struct RepoContextConnector;

impl Connector for RepoContextConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "repo.context".into(),
            display_name: "Repo Context".into(),
            description: "Builds a compact, line-grounded repository context pack for a coding \
                          task. It combines likely files, symbols, optional regex patterns, and \
                          context windows around matches. Respects .gitignore."
                .into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language task text or localization query"
                    },
                    "path": {
                        "type": "string",
                        "description": "Absolute repository/workspace directory. Defaults to current working directory"
                    },
                    "symbols": {
                        "type": "array",
                        "description": "Identifiers, type names, function names, modules, or filenames to prioritize",
                        "items": { "type": "string" }
                    },
                    "patterns": {
                        "type": "array",
                        "description": "Optional regex patterns to search for. If omitted, patterns are derived from query and symbols",
                        "items": { "type": "string" }
                    },
                    "target_paths": {
                        "type": "array",
                        "description": "Optional absolute or workspace-relative files/directories to inspect first",
                        "items": { "type": "string" }
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of files to return. Default: 6, max: 20",
                        "minimum": 1
                    },
                    "context": {
                        "type": "integer",
                        "description": "Number of lines before and after each match. Default: 2, max: 8",
                        "minimum": 0
                    },
                    "max_matches_per_file": {
                        "type": "integer",
                        "description": "Maximum rendered matches per returned file. Default: 5, max: 20",
                        "minimum": 1
                    },
                    "include_tests": {
                        "type": "boolean",
                        "description": "Whether test files and tests/ directories should be considered. Default: true"
                    },
                    "case_insensitive": {
                        "type": "boolean",
                        "description": "Whether regex matching is case-insensitive. Default: true"
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "workspace_root": { "type": "string" },
                    "patterns": { "type": "array", "items": { "type": "string" } },
                    "files": { "type": "array" },
                    "scanned_files": { "type": "integer" },
                    "truncated_scan": { "type": "boolean" }
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
        let root = resolve_root(params.get("path").and_then(Value::as_str))?;
        let query = params.get("query").and_then(Value::as_str).unwrap_or("").trim().to_string();
        let symbols = parse_string_array(params.get("symbols"), "symbols")?;
        let explicit_patterns = parse_string_array(params.get("patterns"), "patterns")?;
        let target_paths = parse_string_array(params.get("target_paths"), "target_paths")?;

        if query.is_empty()
            && symbols.is_empty()
            && explicit_patterns.is_empty()
            && target_paths.is_empty()
        {
            return Err(ConnectorError::InvalidParams(
                "repo.context requires at least one of 'query', 'symbols', 'patterns', or 'target_paths'"
                    .into(),
            ));
        }

        let limit = clamp_usize(
            params.get("limit").and_then(Value::as_u64).unwrap_or(DEFAULT_FILE_LIMIT as u64),
            1,
            MAX_FILE_LIMIT,
        );
        let context = clamp_usize(
            params.get("context").and_then(Value::as_u64).unwrap_or(DEFAULT_CONTEXT_LINES as u64),
            0,
            MAX_CONTEXT_LINES,
        );
        let max_matches_per_file = clamp_usize(
            params
                .get("max_matches_per_file")
                .and_then(Value::as_u64)
                .unwrap_or(DEFAULT_MAX_MATCHES_PER_FILE as u64),
            1,
            MAX_MATCHES_PER_FILE,
        );
        let include_tests = params.get("include_tests").and_then(Value::as_bool).unwrap_or(true);
        let case_insensitive =
            params.get("case_insensitive").and_then(Value::as_bool).unwrap_or(true);

        let intent = ContextIntent::new(query, symbols, explicit_patterns)?;
        let regexes = compile_patterns(&intent.patterns, case_insensitive)?;
        let search_roots = resolve_target_paths(&root, &target_paths)?;
        let (mut files, scanned_files, truncated_scan) = collect_context_files(
            &root,
            &search_roots,
            &intent,
            &regexes,
            include_tests,
            context,
            max_matches_per_file,
        )?;
        files.sort_by(|left, right| {
            right.score.cmp(&left.score).then_with(|| left.path.cmp(&right.path))
        });
        files.truncate(limit);

        let content = render_context_pack(&root, &intent, &files, scanned_files, truncated_scan);

        Ok(json!({
            "content": content,
            "workspace_root": root.display().to_string(),
            "patterns": intent.patterns,
            "files": files.iter().map(ContextFile::to_json).collect::<Vec<_>>(),
            "scanned_files": scanned_files,
            "truncated_scan": truncated_scan,
        }))
    }
}

#[derive(Debug, Clone)]
struct ContextIntent {
    query: String,
    symbols: Vec<String>,
    terms: Vec<String>,
    patterns: Vec<String>,
}

impl ContextIntent {
    fn new(
        query: String,
        symbols: Vec<String>,
        explicit_patterns: Vec<String>,
    ) -> Result<Self, ConnectorError> {
        let symbols = normalize_list(symbols);
        let mut terms = tokenize(&query);
        for symbol in &symbols {
            for token in tokenize(symbol) {
                if !terms.contains(&token) {
                    terms.push(token);
                }
            }
        }

        let patterns = if explicit_patterns.is_empty() {
            derive_patterns(&symbols, &terms)
        } else {
            normalize_list(explicit_patterns)
        };
        if patterns.is_empty() {
            return Err(ConnectorError::InvalidParams(
                "repo.context could not derive any usable search pattern".into(),
            ));
        }

        Ok(Self { query, symbols, terms, patterns })
    }
}

#[derive(Debug, Clone)]
struct ContextFile {
    path: String,
    score: i64,
    why: Vec<String>,
    matches: Vec<ContextMatch>,
}

impl ContextFile {
    fn to_json(&self) -> Value {
        json!({
            "path": self.path,
            "score": self.score,
            "why": self.why,
            "matches": self.matches.iter().map(ContextMatch::to_json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone)]
struct ContextMatch {
    line: usize,
    pattern: String,
    preview: String,
    context_start: usize,
    context_end: usize,
    context_lines: Vec<RenderedLine>,
}

impl ContextMatch {
    fn to_json(&self) -> Value {
        json!({
            "line": self.line,
            "pattern": self.pattern,
            "preview": self.preview,
            "context_start": self.context_start,
            "context_end": self.context_end,
        })
    }
}

#[derive(Debug, Clone)]
struct RenderedLine {
    number: usize,
    text: String,
    is_match: bool,
}

#[derive(Clone)]
enum SearchRoot {
    File(PathBuf),
    Directory(PathBuf),
}

fn parse_string_array(value: Option<&Value>, field: &str) -> Result<Vec<String>, ConnectorError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(items) = value.as_array() else {
        return Err(ConnectorError::InvalidParams(format!(
            "invalid '{field}' (expected array of strings)"
        )));
    };

    let mut parsed = Vec::new();
    for item in items {
        let Some(text) = item.as_str() else {
            return Err(ConnectorError::InvalidParams(format!(
                "invalid '{field}' entry (expected string)"
            )));
        };
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            parsed.push(trimmed.to_string());
        }
    }
    Ok(parsed)
}

fn normalize_list(items: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::new();
    for item in items {
        let item = item.trim();
        if item.is_empty() || !seen.insert(item.to_string()) {
            continue;
        }
        normalized.push(item.to_string());
    }
    normalized
}

fn tokenize(text: &str) -> Vec<String> {
    const STOPWORDS: &[&str] = &[
        "the",
        "and",
        "for",
        "with",
        "from",
        "that",
        "this",
        "into",
        "need",
        "add",
        "fix",
        "update",
        "change",
        "rename",
        "function",
        "method",
        "file",
        "code",
        "task",
        "issue",
        "bug",
        "please",
        "should",
        "would",
        "could",
        "have",
        "has",
        "had",
        "been",
        "using",
        "use",
        "tests",
        "test",
        "module",
        "crate",
        "repo",
        "workspace",
    ];

    let mut seen = BTreeSet::new();
    let mut tokens = Vec::new();
    for token in text
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'))
        .filter(|token| !token.is_empty())
    {
        let token = token.to_string();
        let token_lower = token.to_lowercase();
        let meaningful = token.len() >= 3 || token.contains('_') || token.contains('-');
        if meaningful && !STOPWORDS.contains(&token_lower.as_str()) && seen.insert(token.clone()) {
            tokens.push(token);
        }
    }
    tokens
}

fn derive_patterns(symbols: &[String], terms: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut patterns = Vec::new();
    for value in symbols.iter().chain(terms.iter()) {
        let trimmed = value.trim();
        if trimmed.len() < 2 {
            continue;
        }
        let pattern = regex::escape(trimmed);
        if seen.insert(pattern.clone()) {
            patterns.push(pattern);
        }
        if patterns.len() >= MAX_PATTERNS {
            break;
        }
    }
    patterns
}

fn compile_patterns(
    patterns: &[String],
    case_insensitive: bool,
) -> Result<Vec<Regex>, ConnectorError> {
    patterns
        .iter()
        .map(|pattern| {
            RegexBuilder::new(pattern).case_insensitive(case_insensitive).build().map_err(|err| {
                ConnectorError::InvalidParams(format!("invalid regex '{pattern}': {err}"))
            })
        })
        .collect()
}

fn clamp_usize(value: u64, min: usize, max: usize) -> usize {
    (value as usize).clamp(min, max)
}

fn resolve_root(path_str: Option<&str>) -> Result<PathBuf, ConnectorError> {
    let root = match path_str {
        Some(path_str) => PathBuf::from(path_str),
        None => std::env::current_dir().map_err(|err| {
            ConnectorError::Retryable(format!("failed to get current directory: {err}"))
        })?,
    };

    if !root.exists() {
        return Err(ConnectorError::terminal(format!("directory not found: {}", root.display())));
    }

    let root = root.canonicalize().map_err(|err| match err.kind() {
        std::io::ErrorKind::PermissionDenied => {
            ConnectorError::terminal(format!("permission denied: {}", root.display()))
        }
        _ => ConnectorError::Retryable(format!("I/O error on {}: {err}", root.display())),
    })?;

    if !root.is_dir() {
        return Err(ConnectorError::terminal(format!("not a directory: {}", root.display())));
    }

    Ok(root)
}

fn resolve_target_paths(
    root: &Path,
    target_paths: &[String],
) -> Result<Vec<SearchRoot>, ConnectorError> {
    if target_paths.is_empty() {
        return Ok(vec![SearchRoot::Directory(root.to_path_buf())]);
    }

    let mut roots = Vec::new();
    for target in target_paths {
        let raw = PathBuf::from(target);
        let candidate = if raw.is_absolute() { raw } else { root.join(raw) };
        if !candidate.exists() {
            return Err(ConnectorError::terminal(format!(
                "target path not found: {}",
                candidate.display()
            )));
        }
        let candidate = candidate.canonicalize().map_err(|err| {
            ConnectorError::Retryable(format!("I/O error on {}: {err}", candidate.display()))
        })?;
        if candidate.is_file() {
            roots.push(SearchRoot::File(candidate));
        } else if candidate.is_dir() {
            roots.push(SearchRoot::Directory(candidate));
        } else {
            return Err(ConnectorError::terminal(format!(
                "target path is neither file nor directory: {}",
                candidate.display()
            )));
        }
    }
    Ok(roots)
}

fn collect_context_files(
    root: &Path,
    search_roots: &[SearchRoot],
    intent: &ContextIntent,
    regexes: &[Regex],
    include_tests: bool,
    context: usize,
    max_matches_per_file: usize,
) -> Result<(Vec<ContextFile>, usize, bool), ConnectorError> {
    let mut files = Vec::new();
    let mut seen = BTreeSet::new();
    let mut scanned_files = 0usize;
    let mut truncated_scan = false;

    for search_root in search_roots {
        match search_root {
            SearchRoot::File(path) => {
                if seen.insert(path.clone()) {
                    scanned_files += 1;
                    if let Some(file) = score_context_file(
                        root,
                        path,
                        intent,
                        regexes,
                        include_tests,
                        context,
                        max_matches_per_file,
                    )? {
                        files.push(file);
                    }
                }
            }
            SearchRoot::Directory(path) => {
                for item in WalkBuilder::new(path).build() {
                    let entry = item.map_err(walk_error)?;
                    if entry.depth() == 0
                        || !entry.file_type().map(|file_type| file_type.is_file()).unwrap_or(false)
                    {
                        continue;
                    }
                    let path = entry.path().to_path_buf();
                    if !seen.insert(path.clone()) {
                        continue;
                    }
                    scanned_files += 1;
                    if scanned_files > MAX_SCAN_FILES {
                        truncated_scan = true;
                        break;
                    }
                    if let Some(file) = score_context_file(
                        root,
                        &path,
                        intent,
                        regexes,
                        include_tests,
                        context,
                        max_matches_per_file,
                    )? {
                        files.push(file);
                    }
                }
            }
        }
        if truncated_scan {
            break;
        }
    }

    Ok((files, scanned_files.min(MAX_SCAN_FILES), truncated_scan))
}

fn score_context_file(
    root: &Path,
    path: &Path,
    intent: &ContextIntent,
    regexes: &[Regex],
    include_tests: bool,
    context: usize,
    max_matches_per_file: usize,
) -> Result<Option<ContextFile>, ConnectorError> {
    let rel = path.strip_prefix(root).unwrap_or(path);
    if !include_tests && looks_like_test_path(rel) {
        return Ok(None);
    }
    if !should_inspect_contents(path) {
        return Ok(None);
    }
    let Some(text) = read_text_if_small(path)? else {
        return Ok(None);
    };

    let rel_display = normalize_path(rel);
    let rel_lower = rel_display.to_lowercase();
    let mut score = base_path_score(path, rel);
    let mut why = Vec::new();
    let mut seen_reasons = BTreeSet::new();

    for symbol in &intent.symbols {
        if rel_lower.contains(&symbol.to_lowercase()) {
            score += 20;
            push_reason(&mut why, &mut seen_reasons, format!("path matches symbol '{symbol}'"));
        }
    }
    for term in &intent.terms {
        if rel_lower.contains(&term.to_lowercase()) {
            score += 8;
            push_reason(&mut why, &mut seen_reasons, format!("path matches query term '{term}'"));
        }
    }

    let lines = text.lines().map(str::to_string).collect::<Vec<_>>();
    let mut matches = Vec::new();
    for (pattern_index, regex) in regexes.iter().enumerate() {
        for (line_index, line) in lines.iter().enumerate() {
            if !regex.is_match(line) {
                continue;
            }
            let pattern = intent
                .patterns
                .get(pattern_index)
                .cloned()
                .unwrap_or_else(|| regex.as_str().to_string());
            score +=
                if intent.symbols.iter().any(|symbol| pattern.contains(symbol)) { 12 } else { 6 };
            push_reason(&mut why, &mut seen_reasons, format!("content matches '{pattern}'"));
            if matches.len() < max_matches_per_file {
                matches.push(render_match(&lines, line_index, &pattern, context));
            }
        }
    }

    if matches.is_empty() && score <= base_path_score(path, rel) {
        return Ok(None);
    }

    Ok(Some(ContextFile {
        path: path.display().to_string(),
        score,
        why: why.into_iter().take(5).collect(),
        matches,
    }))
}

fn render_match(
    lines: &[String],
    match_index: usize,
    pattern: &str,
    context: usize,
) -> ContextMatch {
    let start = match_index.saturating_sub(context);
    let end = (match_index + context).min(lines.len().saturating_sub(1));
    let context_lines = lines
        .iter()
        .enumerate()
        .take(end + 1)
        .skip(start)
        .map(|(index, line)| RenderedLine {
            number: index + 1,
            text: truncate_line(line),
            is_match: index == match_index,
        })
        .collect::<Vec<_>>();

    ContextMatch {
        line: match_index + 1,
        pattern: pattern.to_string(),
        preview: truncate_line(&lines[match_index]),
        context_start: start + 1,
        context_end: end + 1,
        context_lines,
    }
}

fn render_context_pack(
    root: &Path,
    intent: &ContextIntent,
    files: &[ContextFile],
    scanned_files: usize,
    truncated_scan: bool,
) -> String {
    let mut out = String::new();
    out.push_str("Repo context pack\n");
    out.push_str(&format!("Workspace: {}\n", root.display()));
    if !intent.query.is_empty() {
        out.push_str(&format!("Query: {}\n", intent.query));
    }
    if !intent.symbols.is_empty() {
        out.push_str(&format!("Symbols: {}\n", intent.symbols.join(", ")));
    }
    out.push_str(&format!("Patterns: {}\n", intent.patterns.join(", ")));
    out.push_str(&format!(
        "Scanned files: {scanned_files}{}\n",
        if truncated_scan { " (truncated)" } else { "" }
    ));

    if files.is_empty() {
        out.push_str("\nNo relevant files found.\n");
        return out;
    }

    out.push_str("\nTop files:\n");
    for file in files {
        out.push_str(&format!("- {} (score {})", file.path, file.score));
        if !file.why.is_empty() {
            out.push_str(&format!(": {}", file.why.join("; ")));
        }
        out.push('\n');
    }

    out.push_str("\nEvidence:\n");
    for file in files {
        out.push_str(&format!("### {}\n", file.path));
        if file.matches.is_empty() {
            out.push_str("No line matches; ranked by path/query metadata.\n");
            continue;
        }
        for item in &file.matches {
            out.push_str(&format!("Match line {} pattern `{}`:\n", item.line, item.pattern));
            for line in &item.context_lines {
                let marker = if line.is_match { ':' } else { '-' };
                out.push_str(&format!("{}{} {}\n", line.number, marker, line.text));
            }
        }
    }

    out
}

fn base_path_score(path: &Path, rel: &Path) -> i64 {
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or_default();
    let rel = normalize_path(rel);
    if rel.ends_with("Cargo.toml") {
        5
    } else if ext == "rs" && looks_like_test_path(Path::new(&rel)) {
        4
    } else if ext == "rs" {
        6
    } else if matches!(ext, "py" | "ts" | "tsx" | "js" | "jsx" | "go" | "java" | "c" | "cpp") {
        3
    } else {
        1
    }
}

fn looks_like_test_path(rel: &Path) -> bool {
    let rel_str = normalize_path(rel).to_lowercase();
    rel_str.starts_with("tests/")
        || rel_str.contains("/tests/")
        || rel_str.ends_with("_test.rs")
        || rel_str.ends_with("_tests.rs")
        || rel_str.starts_with("test/")
        || rel_str.contains("/test/")
        || rel_str.contains("integration_test")
}

fn should_inspect_contents(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()).unwrap_or_default(),
        "rs" | "toml"
            | "json"
            | "yaml"
            | "yml"
            | "md"
            | "txt"
            | "py"
            | "ts"
            | "tsx"
            | "js"
            | "jsx"
            | "go"
            | "java"
            | "kt"
            | "c"
            | "cc"
            | "cpp"
            | "h"
            | "hpp"
            | "cs"
            | "rb"
            | "php"
            | "swift"
    )
}

fn read_text_if_small(path: &Path) -> Result<Option<String>, ConnectorError> {
    let metadata = std::fs::metadata(path).map_err(|err| classify_path_error(path, &err))?;
    if metadata.len() as usize > MAX_FILE_BYTES {
        return Ok(None);
    }

    let bytes = std::fs::read(path).map_err(|err| classify_path_error(path, &err))?;
    if bytes.contains(&0) {
        return Ok(None);
    }

    Ok(String::from_utf8(bytes).ok())
}

fn truncate_line(line: &str) -> String {
    const MAX_LINE: usize = 240;
    if line.len() <= MAX_LINE {
        line.to_string()
    } else {
        format!("{}...", &line[..MAX_LINE])
    }
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn push_reason(reasons: &mut Vec<String>, seen: &mut BTreeSet<String>, reason: String) {
    if seen.insert(reason.clone()) {
        reasons.push(reason);
    }
}

fn classify_path_error(path: &Path, err: &std::io::Error) -> ConnectorError {
    match err.kind() {
        std::io::ErrorKind::NotFound => {
            ConnectorError::terminal(format!("file not found: {}", path.display()))
        }
        std::io::ErrorKind::PermissionDenied => {
            ConnectorError::terminal(format!("permission denied: {}", path.display()))
        }
        _ => ConnectorError::Retryable(format!("I/O error on {}: {err}", path.display())),
    }
}

fn walk_error(err: ignore::Error) -> ConnectorError {
    if let Some(io_err) = err.io_error() {
        return match io_err.kind() {
            std::io::ErrorKind::PermissionDenied => {
                ConnectorError::terminal("permission denied during directory walk")
            }
            _ => ConnectorError::Retryable(format!("directory walk error: {io_err}")),
        };
    }
    ConnectorError::Retryable(format!("directory walk error: {err}"))
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
    fn repo_context_builds_line_grounded_pack() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src/maps")).unwrap();
        let file = dir.path().join("src/maps/mod.rs");
        std::fs::write(
            &file,
            "enum Map {\n    HashMap,\n    LruHashMap,\n}\nimpl TryFrom<Map> for HashMap {}\n",
        )
        .unwrap();

        let result = RepoContextConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "query": "HashMap TryFrom LruHashMap",
                    "symbols": ["HashMap", "LruHashMap", "TryFrom"],
                    "limit": 3,
                    "context": 1
                }),
            )
            .unwrap();

        assert_eq!(result["files"][0]["path"], file.display().to_string());
        let content = result["content"].as_str().unwrap();
        assert!(content.contains("Repo context pack"));
        assert!(content.contains("impl TryFrom<Map> for HashMap"));
        assert!(content.contains("LruHashMap"));
    }

    #[test]
    fn repo_context_honors_target_paths() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        let relevant = dir.path().join("src/relevant.rs");
        let other = dir.path().join("src/other.rs");
        std::fs::write(&relevant, "fn target_symbol() {}\n").unwrap();
        std::fs::write(&other, "fn target_symbol() {}\n").unwrap();

        let result = RepoContextConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "symbols": ["target_symbol"],
                    "target_paths": [relevant.to_str().unwrap()],
                    "limit": 5
                }),
            )
            .unwrap();

        assert_eq!(result["files"].as_array().unwrap().len(), 1);
        assert_eq!(result["files"][0]["path"], relevant.display().to_string());
    }

    #[test]
    fn repo_context_requires_signal() {
        let err = RepoContextConnector
            .invoke(&test_ctx(), &json!({}))
            .expect_err("empty request should fail");
        assert!(format!("{err}").contains("requires at least one"));
    }
}
