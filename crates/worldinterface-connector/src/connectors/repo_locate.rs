use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cargo_metadata::{MetadataCommand, Package};
use ignore::WalkBuilder;
use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

const DEFAULT_LIMIT: usize = 8;
const MAX_FILE_BYTES: usize = 256 * 1024;
const MAX_SCAN_FILES: usize = 10_000;

pub struct RepoLocateConnector;

impl Connector for RepoLocateConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "repo.locate".into(),
            display_name: "Repo Locate".into(),
            description: "Ranks likely files, modules, manifests, and packages for a coding task \
                          based on task text and symbols. Returns absolute candidate paths plus \
                          Rust package/test hints when a Cargo workspace is detected. Respects \
                          .gitignore."
                .into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language task text or concise localization query"
                    },
                    "path": {
                        "type": "string",
                        "description": "Absolute directory to search. Defaults to current working directory"
                    },
                    "symbols": {
                        "type": "array",
                        "description": "Optional identifiers, filenames, module names, or paths to prioritize",
                        "items": { "type": "string" }
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of ranked candidates to return. Default: 8",
                        "minimum": 1
                    },
                    "include_tests": {
                        "type": "boolean",
                        "description": "Whether test files and tests/ directories should be considered. Default: true"
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "workspace_root": { "type": "string" },
                    "workspace_type": { "type": "string" },
                    "candidates": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": { "type": "string" },
                                "kind": { "type": "string" },
                                "package": { "type": ["string", "null"] },
                                "score": { "type": "integer" },
                                "why": {
                                    "type": "array",
                                    "items": { "type": "string" }
                                },
                                "test_target_hint": { "type": ["string", "null"] }
                            }
                        }
                    },
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
        let query = params.get("query").and_then(Value::as_str).unwrap_or("").trim().to_string();
        let symbols = parse_symbols(params.get("symbols"))?;
        if query.is_empty() && symbols.is_empty() {
            return Err(ConnectorError::InvalidParams(
                "repo.locate requires a non-empty 'query' or 'symbols'".into(),
            ));
        }

        let root = resolve_root(params.get("path").and_then(Value::as_str))?;
        let limit =
            params.get("limit").and_then(Value::as_u64).unwrap_or(DEFAULT_LIMIT as u64) as usize;
        let include_tests = params.get("include_tests").and_then(Value::as_bool).unwrap_or(true);

        let intent = SearchIntent::new(query, symbols, include_tests);
        let cargo_info = CargoWorkspaceInfo::discover(&root);
        let (mut candidates, scanned_files, truncated_scan) =
            collect_candidates(&root, &intent, cargo_info.as_ref())?;
        candidates.sort_by(|left, right| {
            right.score.cmp(&left.score).then_with(|| left.path.cmp(&right.path))
        });
        candidates.truncate(limit.max(1));

        let content = render_candidates(&root, &intent, &candidates, cargo_info.as_ref());

        Ok(json!({
            "content": content,
            "workspace_root": root.display().to_string(),
            "workspace_type": cargo_info.as_ref().map(|_| "cargo").unwrap_or("generic"),
            "candidates": candidates.iter().map(Candidate::to_json).collect::<Vec<_>>(),
            "scanned_files": scanned_files,
            "truncated_scan": truncated_scan,
        }))
    }
}

#[derive(Debug, Clone)]
struct SearchIntent {
    query: String,
    query_lower: String,
    symbols: Vec<String>,
    terms: Vec<String>,
    include_tests: bool,
}

impl SearchIntent {
    fn new(query: String, symbols: Vec<String>, include_tests: bool) -> Self {
        let query_lower = query.to_lowercase();
        let symbols = normalize_symbol_list(symbols);
        let mut terms = tokenize(&query_lower);
        for symbol in &symbols {
            if !terms.contains(symbol) {
                terms.push(symbol.clone());
            }
        }
        Self { query, query_lower, symbols, terms, include_tests }
    }
}

#[derive(Debug, Clone)]
struct CargoWorkspaceInfo {
    packages: Vec<CargoPackageInfo>,
}

#[derive(Debug, Clone)]
struct CargoPackageInfo {
    name: String,
    manifest_dir: PathBuf,
}

impl CargoWorkspaceInfo {
    fn discover(root: &Path) -> Option<Self> {
        let manifest_path = find_manifest_path(root)?;
        let metadata =
            MetadataCommand::new().manifest_path(&manifest_path).no_deps().exec().ok()?;

        let mut packages =
            metadata.packages.iter().map(CargoPackageInfo::from_package).collect::<Vec<_>>();
        packages.sort_by(|left, right| {
            right.manifest_dir.components().count().cmp(&left.manifest_dir.components().count())
        });
        Some(Self { packages })
    }

    fn package_for_path(&self, path: &Path) -> Option<&CargoPackageInfo> {
        self.packages.iter().find(|package| path.starts_with(&package.manifest_dir))
    }
}

impl CargoPackageInfo {
    fn from_package(package: &Package) -> Self {
        let manifest_path = package.manifest_path.clone().into_std_path_buf();
        let manifest_dir =
            manifest_path.parent().map(Path::to_path_buf).unwrap_or_else(|| manifest_path.clone());
        Self { name: package.name.clone(), manifest_dir }
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    path: String,
    kind: String,
    package: Option<String>,
    score: i64,
    why: Vec<String>,
    test_target_hint: Option<String>,
}

impl Candidate {
    fn to_json(&self) -> Value {
        json!({
            "path": self.path,
            "kind": self.kind,
            "package": self.package,
            "score": self.score,
            "why": self.why,
            "test_target_hint": self.test_target_hint,
        })
    }
}

fn parse_symbols(value: Option<&Value>) -> Result<Vec<String>, ConnectorError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(items) = value.as_array() else {
        return Err(ConnectorError::InvalidParams(
            "invalid 'symbols' (expected array of strings)".into(),
        ));
    };

    let mut symbols = Vec::new();
    for item in items {
        let Some(symbol) = item.as_str() else {
            return Err(ConnectorError::InvalidParams(
                "invalid 'symbols' entry (expected string)".into(),
            ));
        };
        let trimmed = symbol.trim();
        if !trimmed.is_empty() {
            symbols.push(trimmed.to_string());
        }
    }
    Ok(symbols)
}

fn normalize_symbol_list(symbols: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::new();
    for symbol in symbols {
        let symbol = symbol.trim().to_lowercase();
        if symbol.is_empty() || !seen.insert(symbol.clone()) {
            continue;
        }
        normalized.push(symbol);
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
        let token = token.to_lowercase();
        let meaningful = token.len() >= 3 || token.contains('_') || token.contains('-');
        if meaningful && !STOPWORDS.contains(&token.as_str()) && seen.insert(token.clone()) {
            tokens.push(token);
        }
    }
    tokens
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

fn find_manifest_path(root: &Path) -> Option<PathBuf> {
    let direct = root.join("Cargo.toml");
    if direct.is_file() {
        return Some(direct);
    }

    root.ancestors()
        .skip(1)
        .map(|ancestor| ancestor.join("Cargo.toml"))
        .find(|candidate| candidate.is_file())
}

fn collect_candidates(
    root: &Path,
    intent: &SearchIntent,
    cargo_info: Option<&CargoWorkspaceInfo>,
) -> Result<(Vec<Candidate>, usize, bool), ConnectorError> {
    let walker = WalkBuilder::new(root).build();
    let mut candidates = Vec::new();
    let mut scanned_files = 0usize;
    let mut truncated_scan = false;

    for item in walker {
        let entry = item.map_err(walk_error)?;
        if entry.depth() == 0
            || !entry.file_type().map(|file_type| file_type.is_file()).unwrap_or(false)
        {
            continue;
        }

        let rel = entry.path().strip_prefix(root).map_err(|err| {
            ConnectorError::Retryable(format!("failed to relativize path: {err}"))
        })?;
        if !intent.include_tests && looks_like_test_path(rel) {
            continue;
        }

        scanned_files += 1;
        if scanned_files > MAX_SCAN_FILES {
            truncated_scan = true;
            break;
        }

        if let Some(candidate) = score_candidate(root, entry.path(), rel, intent, cargo_info)? {
            candidates.push(candidate);
        }
    }

    Ok((candidates, scanned_files.min(MAX_SCAN_FILES), truncated_scan))
}

fn score_candidate(
    _root: &Path,
    path: &Path,
    rel: &Path,
    intent: &SearchIntent,
    cargo_info: Option<&CargoWorkspaceInfo>,
) -> Result<Option<Candidate>, ConnectorError> {
    let rel_display = normalize_path(rel);
    let rel_lower = rel_display.to_lowercase();
    let file_name =
        path.file_name().and_then(|name| name.to_str()).unwrap_or_default().to_lowercase();
    let kind = classify_kind(path, rel);
    let package =
        cargo_info.and_then(|info| info.package_for_path(path)).map(|package| package.name.clone());

    let mut score = base_kind_score(kind.as_str());
    let mut why = Vec::new();
    let mut seen_reasons = BTreeSet::new();

    for symbol in &intent.symbols {
        if file_name == *symbol || file_name.contains(symbol) {
            score += 30;
            push_reason(&mut why, &mut seen_reasons, format!("filename matches symbol '{symbol}'"));
        } else if rel_lower.contains(symbol) {
            score += 20;
            push_reason(&mut why, &mut seen_reasons, format!("path matches symbol '{symbol}'"));
        }
    }

    for term in &intent.terms {
        if file_name.contains(term) {
            score += 12;
            push_reason(
                &mut why,
                &mut seen_reasons,
                format!("filename matches query term '{term}'"),
            );
        } else if rel_lower.contains(term) {
            score += 8;
            push_reason(&mut why, &mut seen_reasons, format!("path matches query term '{term}'"));
        }
    }

    if let Some(package) = &package {
        let package_lower = package.to_lowercase();
        for term in &intent.terms {
            if package_lower.contains(term) {
                score += 6;
                push_reason(
                    &mut why,
                    &mut seen_reasons,
                    format!("belongs to package '{package}' matching '{term}'"),
                );
                break;
            }
        }
    }

    if should_inspect_contents(path) {
        if let Some(text) = read_text_if_small(path)? {
            let text_lower = text.to_lowercase();
            if !intent.query_lower.is_empty() && text_lower.contains(&intent.query_lower) {
                score += 10;
                push_reason(&mut why, &mut seen_reasons, "content matches the full query".into());
            }

            for symbol in &intent.symbols {
                let hits = count_occurrences(&text_lower, symbol);
                if hits > 0 {
                    score += (hits.min(5) as i64) * 5;
                    push_reason(
                        &mut why,
                        &mut seen_reasons,
                        format!("content contains symbol '{symbol}' ({hits} hits)"),
                    );
                }
            }

            for term in &intent.terms {
                let hits = count_occurrences(&text_lower, term);
                if hits > 0 {
                    score += hits.min(4) as i64;
                    push_reason(
                        &mut why,
                        &mut seen_reasons,
                        format!("content contains query term '{term}' ({hits} hits)"),
                    );
                }
            }
        }
    }

    if score <= 0 {
        return Ok(None);
    }

    Ok(Some(Candidate {
        path: path.display().to_string(),
        kind: kind.clone(),
        package: package.clone(),
        score,
        why: why.into_iter().take(4).collect(),
        test_target_hint: build_test_target_hint(package.as_deref(), &kind, path, rel),
    }))
}

fn classify_kind(path: &Path, rel: &Path) -> String {
    let name = path.file_name().and_then(|name| name.to_str()).unwrap_or_default();
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or_default();
    if name == "Cargo.toml" {
        "manifest".into()
    } else if ext == "rs" && looks_like_test_path(rel) {
        "rust_test".into()
    } else if ext == "rs" {
        "rust_source".into()
    } else if matches!(ext, "toml" | "json" | "yaml" | "yml") {
        "config".into()
    } else if matches!(ext, "md" | "txt") {
        "docs".into()
    } else if matches!(
        ext,
        "py" | "ts"
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
    ) {
        "source".into()
    } else {
        "file".into()
    }
}

fn base_kind_score(kind: &str) -> i64 {
    match kind {
        "rust_source" => 6,
        "rust_test" => 4,
        "manifest" => 5,
        "config" => 2,
        "source" => 3,
        _ => 0,
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

    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) => return Err(classify_path_error(path, &err)),
    };
    if bytes.contains(&0) {
        return Ok(None);
    }

    Ok(String::from_utf8(bytes).ok())
}

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    if needle.is_empty() {
        0
    } else {
        haystack.match_indices(needle).count()
    }
}

fn push_reason(reasons: &mut Vec<String>, seen: &mut BTreeSet<String>, reason: String) {
    if seen.insert(reason.clone()) {
        reasons.push(reason);
    }
}

fn build_test_target_hint(
    package: Option<&str>,
    kind: &str,
    path: &Path,
    rel: &Path,
) -> Option<String> {
    let package = package?;
    if kind == "rust_test" {
        let stem = path.file_stem().and_then(|stem| stem.to_str()).unwrap_or_default();
        if stem.is_empty() || stem == "mod" {
            return Some(format!("cargo test -p {package}"));
        }
        return Some(format!("cargo test -p {package} {stem}"));
    }

    if normalize_path(rel).ends_with("Cargo.toml") {
        Some(format!("cargo test -p {package}"))
    } else {
        Some(format!("cargo test -p {package}"))
    }
}

fn render_candidates(
    root: &Path,
    intent: &SearchIntent,
    candidates: &[Candidate],
    cargo_info: Option<&CargoWorkspaceInfo>,
) -> String {
    let mut out = String::new();
    if candidates.is_empty() {
        let workspace_type = cargo_info.map(|_| "cargo workspace").unwrap_or("generic repository");
        out.push_str(&format!(
            "No ranked candidates found under {} for query '{}' and symbols [{}] in {}.",
            root.display(),
            intent.query,
            intent.symbols.join(", "),
            workspace_type
        ));
        return out;
    }

    out.push_str(&format!(
        "Ranked candidates under {} for query '{}' and symbols [{}]:\n",
        root.display(),
        intent.query,
        intent.symbols.join(", ")
    ));

    for (index, candidate) in candidates.iter().enumerate() {
        let package_suffix = candidate
            .package
            .as_ref()
            .map(|package| format!(", package={package}"))
            .unwrap_or_default();
        let hint_suffix = candidate
            .test_target_hint
            .as_ref()
            .map(|hint| format!(", test_hint={hint}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "{}. {} [{}{}, score={}{}] — {}\n",
            index + 1,
            candidate.path,
            candidate.kind,
            package_suffix,
            candidate.score,
            hint_suffix,
            candidate.why.join("; ")
        ));
    }

    out
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
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

    fn write_sample_rust_repo(dir: &Path) {
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::create_dir_all(dir.join("tests")).unwrap();
        std::fs::create_dir_all(dir.join(".git")).unwrap();
        std::fs::write(
            dir.join("Cargo.toml"),
            r#"[package]
name = "sample-rust"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("src/lib.rs"),
            r#"pub fn parse_config_line(line: &str) -> Option<(&str, &str)> {
    line.split_once('=')
}
"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("tests/parse_config.rs"),
            r#"#[test]
fn parse_config_line_handles_spaces() {}
"#,
        )
        .unwrap();
    }

    #[test]
    fn repo_locate_finds_source_file_by_symbol() {
        let dir = tempfile::tempdir().unwrap();
        write_sample_rust_repo(dir.path());

        let result = RepoLocateConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "query": "add a unit test for parse_config_line",
                    "symbols": ["parse_config_line"],
                }),
            )
            .unwrap();

        let content = result["content"].as_str().unwrap();
        assert!(content.contains("src/lib.rs"));
        assert_eq!(result["workspace_type"], "cargo");
        assert_eq!(result["candidates"][0]["package"], "sample-rust");
    }

    #[test]
    fn repo_locate_can_exclude_tests() {
        let dir = tempfile::tempdir().unwrap();
        write_sample_rust_repo(dir.path());

        let result = RepoLocateConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "query": "parse config tests",
                    "symbols": ["parse_config"],
                    "include_tests": false,
                }),
            )
            .unwrap();

        let candidate_paths = result["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|candidate| candidate["path"].as_str())
            .collect::<Vec<_>>();
        assert!(
            candidate_paths.iter().all(|path| !path.contains("/tests/")),
            "unexpected test file candidates: {candidate_paths:?}"
        );
    }

    #[test]
    fn repo_locate_respects_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        write_sample_rust_repo(dir.path());
        std::fs::write(dir.path().join(".gitignore"), "ignored/\n").unwrap();
        std::fs::create_dir_all(dir.path().join("ignored")).unwrap();
        std::fs::write(dir.path().join("ignored/secret.rs"), "pub fn parse_config_line() {}\n")
            .unwrap();

        let result = RepoLocateConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "symbols": ["parse_config_line"],
                }),
            )
            .unwrap();

        let content = result["content"].as_str().unwrap();
        assert!(!content.contains("ignored/secret.rs"));
    }

    #[test]
    fn repo_locate_requires_query_or_symbols() {
        let dir = tempfile::tempdir().unwrap();
        write_sample_rust_repo(dir.path());

        let result = RepoLocateConnector.invoke(
            &test_ctx(),
            &json!({
                "path": dir.path().to_str().unwrap(),
            }),
        );
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn repo_locate_descriptor() {
        let desc = RepoLocateConnector.describe();
        assert_eq!(desc.name, "repo.locate");
        assert_eq!(desc.category, ConnectorCategory::Code);
        assert!(desc.is_read_only);
        assert!(!desc.is_mutating);
    }
}
