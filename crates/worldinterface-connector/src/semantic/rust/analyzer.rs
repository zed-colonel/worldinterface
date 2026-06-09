use std::cmp::Reverse;
use std::path::{Path, PathBuf};

use ignore::WalkBuilder;
use proc_macro2::Span;
use quote::ToTokens;
use regex::Regex;
use syn::spanned::Spanned;
use syn::{ImplItem, Item, TraitItem};

use crate::error::ConnectorError;
use crate::semantic::types::{
    SemanticImplMatch, SemanticRange, SemanticReadResult, SemanticReferenceMatch,
    SemanticSymbolMatch,
};
use crate::semantic::SymbolSelector;

const MAX_SEMANTIC_FILES: usize = 10_000;

#[derive(Debug, Clone)]
struct ByteRange {
    start: usize,
    end: usize,
}

#[derive(Debug, Clone)]
struct RustFile {
    path: PathBuf,
    text: String,
    line_offsets: Vec<usize>,
    base_module_path: Vec<String>,
}

#[derive(Debug, Clone)]
struct SymbolRecord {
    symbol_id: String,
    name: String,
    kind: String,
    file_path: String,
    range: SemanticRange,
    byte_range: ByteRange,
    container_name: Option<String>,
    signature_summary: Option<String>,
    module_path: Vec<String>,
    self_type: Option<String>,
    trait_name: Option<String>,
}

#[derive(Debug)]
struct WorkspaceIndex {
    files: Vec<RustFile>,
    symbols: Vec<SymbolRecord>,
}

pub(crate) fn lookup_symbol(
    workspace_root: &Path,
    query: &str,
    path_hint: Option<&str>,
    kind_hint: Option<&str>,
    limit: usize,
) -> Result<Vec<SemanticSymbolMatch>, ConnectorError> {
    let query = query.trim();
    if query.is_empty() {
        return Err(ConnectorError::InvalidParams(
            "code.symbol requires a non-empty 'query'".into(),
        ));
    }

    let index = WorkspaceIndex::build(workspace_root)?;
    let mut matches = index
        .symbols
        .iter()
        .filter_map(|symbol| score_symbol(symbol, query, path_hint, kind_hint))
        .collect::<Vec<_>>();

    matches.sort_by_key(|(score, symbol)| {
        (Reverse(*score), symbol.file_path.clone(), symbol.range.start_line, symbol.name.clone())
    });
    matches.truncate(limit.max(1));

    Ok(matches.into_iter().map(|(_, symbol)| symbol).collect())
}

pub(crate) fn read_symbol(
    workspace_root: &Path,
    selector: SymbolSelector,
    include_body: bool,
) -> Result<SemanticReadResult, ConnectorError> {
    let index = WorkspaceIndex::build(workspace_root)?;
    let symbol = resolve_symbol(&index, selector)?;
    let file = index
        .files
        .iter()
        .find(|file| file.path == PathBuf::from(&symbol.file_path))
        .ok_or_else(|| ConnectorError::terminal("symbol file missing from semantic index"))?;

    let mut content = file
        .text
        .get(symbol.byte_range.start..symbol.byte_range.end)
        .ok_or_else(|| ConnectorError::terminal("failed to slice symbol content"))?
        .to_string();
    if !include_body {
        content = content.lines().next().unwrap_or("").to_string();
    }

    Ok(SemanticReadResult {
        symbol: to_symbol_match(symbol.clone(), 0, Vec::new()),
        content,
        file_path: symbol.file_path.clone(),
        range: symbol.range.clone(),
        enclosing_context: symbol.container_name.clone(),
    })
}

pub(crate) fn find_references(
    workspace_root: &Path,
    selector: SymbolSelector,
    limit: usize,
    include_tests: bool,
) -> Result<Vec<SemanticReferenceMatch>, ConnectorError> {
    let index = WorkspaceIndex::build(workspace_root)?;
    let symbol = resolve_symbol(&index, selector)?;
    let needle = regex::escape(&symbol.name);
    let regex = Regex::new(&format!(r"\b{needle}\b"))
        .map_err(|e| ConnectorError::Retryable(format!("reference regex build failed: {e}")))?;

    let mut refs = Vec::new();
    for file in &index.files {
        if !include_tests && looks_like_test_path(&file.path) {
            continue;
        }
        for found in regex.find_iter(&file.text) {
            let range =
                byte_range_to_semantic_range(&file.line_offsets, found.start(), found.end());
            refs.push(SemanticReferenceMatch {
                file_path: file.path.display().to_string(),
                range,
                context_snippet: line_snippet(&file.text, found.start()),
            });
            if refs.len() >= limit.max(1) {
                return Ok(refs);
            }
        }
    }

    Ok(refs)
}

pub(crate) fn find_impls(
    workspace_root: &Path,
    selector: SymbolSelector,
    limit: usize,
) -> Result<Vec<SemanticImplMatch>, ConnectorError> {
    let index = WorkspaceIndex::build(workspace_root)?;
    let resolved = resolve_symbol(&index, selector.clone()).ok();
    let query = match (resolved.as_ref(), selector) {
        (Some(symbol), _) => symbol
            .trait_name
            .clone()
            .or_else(|| symbol.self_type.clone())
            .unwrap_or_else(|| symbol.name.clone()),
        (None, SymbolSelector::Query { query, .. }) => query,
        (None, SymbolSelector::SymbolId(symbol_id)) => {
            return Err(ConnectorError::terminal(format!("unknown symbol id: {symbol_id}")));
        }
    };
    let query_lower = query.to_lowercase();

    let mut matches = index
        .symbols
        .iter()
        .filter(|symbol| symbol.kind == "impl")
        .filter_map(|symbol| {
            let type_name = symbol.self_type.as_deref().unwrap_or(&symbol.name);
            let trait_name = symbol.trait_name.as_deref();
            let score = if type_name.eq_ignore_ascii_case(&query) {
                120
            } else if trait_name.is_some_and(|name| name.eq_ignore_ascii_case(&query)) {
                110
            } else if type_name.to_lowercase().contains(&query_lower)
                || trait_name.is_some_and(|name| name.to_lowercase().contains(&query_lower))
                || symbol
                    .signature_summary
                    .as_deref()
                    .unwrap_or_default()
                    .to_lowercase()
                    .contains(&query_lower)
            {
                60
            } else {
                0
            };

            (score > 0).then_some((
                score,
                SemanticImplMatch {
                    symbol_id: symbol.symbol_id.clone(),
                    file_path: symbol.file_path.clone(),
                    range: symbol.range.clone(),
                    type_name: type_name.to_string(),
                    trait_name: symbol.trait_name.clone(),
                    impl_summary: symbol.signature_summary.clone().unwrap_or_else(|| {
                        format!("impl {}", symbol.self_type.as_deref().unwrap_or(&symbol.name))
                    }),
                },
            ))
        })
        .collect::<Vec<_>>();

    matches.sort_by_key(|(score, item)| {
        (Reverse(*score), item.file_path.clone(), item.range.start_line, item.impl_summary.clone())
    });
    matches.truncate(limit.max(1));

    Ok(matches.into_iter().map(|(_, item)| item).collect())
}

impl WorkspaceIndex {
    fn build(workspace_root: &Path) -> Result<Self, ConnectorError> {
        let mut files = Vec::new();
        let mut scanned = 0_usize;
        for entry in WalkBuilder::new(workspace_root)
            .hidden(false)
            .git_ignore(true)
            .git_exclude(true)
            .parents(true)
            .build()
        {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    return Err(ConnectorError::Retryable(format!(
                        "semantic workspace walk failed: {err}"
                    )))
                }
            };
            let path = entry.path();
            if !entry.file_type().is_some_and(|ft| ft.is_file()) {
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            if path.components().any(|part| part.as_os_str() == "target") {
                continue;
            }
            scanned += 1;
            if scanned > MAX_SEMANTIC_FILES {
                break;
            }
            let text = std::fs::read_to_string(path).map_err(|e| {
                ConnectorError::Retryable(format!(
                    "failed to read Rust source {}: {e}",
                    path.display()
                ))
            })?;
            files.push(RustFile {
                path: path.to_path_buf(),
                line_offsets: line_offsets(&text),
                base_module_path: module_path_from_file(workspace_root, path),
                text,
            });
        }

        if files.is_empty() {
            return Err(ConnectorError::terminal(format!(
                "no Rust source files found under {}",
                workspace_root.display()
            )));
        }

        let mut symbols = Vec::new();
        for file in &files {
            let parsed = match syn::parse_file(&file.text) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            collect_items(&parsed.items, file, &file.base_module_path, None, None, &mut symbols);
        }

        if symbols.is_empty() {
            return Err(ConnectorError::terminal(format!(
                "semantic indexing produced no symbols under {}",
                workspace_root.display()
            )));
        }

        Ok(Self { files, symbols })
    }
}

fn collect_items(
    items: &[Item],
    file: &RustFile,
    module_path: &[String],
    impl_context: Option<(String, Option<String>)>,
    trait_context: Option<String>,
    symbols: &mut Vec<SymbolRecord>,
) {
    for item in items {
        match item {
            Item::Fn(item_fn) => {
                push_symbol(
                    symbols,
                    file,
                    item_fn.sig.ident.to_string(),
                    if impl_context.is_some() || trait_context.is_some() {
                        "method"
                    } else {
                        "function"
                    },
                    item_fn.span(),
                    impl_context
                        .as_ref()
                        .map(|(summary, _)| summary.clone())
                        .or_else(|| trait_context.clone()),
                    Some(normalize_tokens(item_fn.sig.to_token_stream().to_string())),
                    module_path,
                    impl_context.as_ref().map(|(_, self_type)| self_type.clone()).flatten(),
                    impl_context.as_ref().and_then(|(_, trait_name)| trait_name.clone()),
                );
            }
            Item::Struct(item_struct) => {
                push_symbol(
                    symbols,
                    file,
                    item_struct.ident.to_string(),
                    "struct",
                    item_struct.span(),
                    None,
                    Some(normalize_tokens(item_struct.to_token_stream().to_string())),
                    module_path,
                    None,
                    None,
                );
            }
            Item::Enum(item_enum) => {
                push_symbol(
                    symbols,
                    file,
                    item_enum.ident.to_string(),
                    "enum",
                    item_enum.span(),
                    None,
                    Some(normalize_tokens(item_enum.to_token_stream().to_string())),
                    module_path,
                    None,
                    None,
                );
            }
            Item::Trait(item_trait) => {
                let trait_name = item_trait.ident.to_string();
                push_symbol(
                    symbols,
                    file,
                    trait_name.clone(),
                    "trait",
                    item_trait.span(),
                    None,
                    Some(normalize_tokens(item_trait.ident.to_string())),
                    module_path,
                    None,
                    Some(trait_name.clone()),
                );
                for trait_item in &item_trait.items {
                    if let TraitItem::Fn(trait_fn) = trait_item {
                        push_symbol(
                            symbols,
                            file,
                            trait_fn.sig.ident.to_string(),
                            "method",
                            trait_fn.span(),
                            Some(trait_name.clone()),
                            Some(normalize_tokens(trait_fn.sig.to_token_stream().to_string())),
                            module_path,
                            None,
                            Some(trait_name.clone()),
                        );
                    }
                }
            }
            Item::Type(item_type) => {
                push_symbol(
                    symbols,
                    file,
                    item_type.ident.to_string(),
                    "type_alias",
                    item_type.span(),
                    None,
                    Some(normalize_tokens(item_type.to_token_stream().to_string())),
                    module_path,
                    None,
                    None,
                );
            }
            Item::Mod(item_mod) => {
                let module_name = item_mod.ident.to_string();
                push_symbol(
                    symbols,
                    file,
                    module_name.clone(),
                    "module",
                    item_mod.span(),
                    None,
                    Some(module_name.clone()),
                    module_path,
                    None,
                    None,
                );
                if let Some((_, inner_items)) = &item_mod.content {
                    let mut nested = module_path.to_vec();
                    nested.push(module_name);
                    collect_items(inner_items, file, &nested, None, None, symbols);
                }
            }
            Item::Impl(item_impl) => {
                let self_type_full =
                    normalize_tokens(item_impl.self_ty.to_token_stream().to_string());
                let self_type_name = short_type_name(&self_type_full);
                let trait_name = item_impl.trait_.as_ref().map(|(_, path, _)| {
                    short_type_name(&normalize_tokens(path.to_token_stream().to_string()))
                });
                let impl_summary = trait_name
                    .as_ref()
                    .map(|name| format!("impl {name} for {self_type_full}"))
                    .unwrap_or_else(|| format!("impl {self_type_full}"));
                push_symbol(
                    symbols,
                    file,
                    self_type_name.clone(),
                    "impl",
                    item_impl.span(),
                    trait_name.clone(),
                    Some(impl_summary.clone()),
                    module_path,
                    Some(self_type_name.clone()),
                    trait_name.clone(),
                );
                let ctx = Some((impl_summary, Some(self_type_name)));
                for impl_item in &item_impl.items {
                    if let ImplItem::Fn(method) = impl_item {
                        push_symbol(
                            symbols,
                            file,
                            method.sig.ident.to_string(),
                            "method",
                            method.span(),
                            ctx.as_ref().map(|(summary, _)| summary.clone()),
                            Some(normalize_tokens(method.sig.to_token_stream().to_string())),
                            module_path,
                            ctx.as_ref().and_then(|(_, self_type)| self_type.clone()),
                            trait_name.clone(),
                        );
                    }
                }
            }
            _ => {}
        }
    }
}

fn push_symbol(
    symbols: &mut Vec<SymbolRecord>,
    file: &RustFile,
    name: String,
    kind: &str,
    span: Span,
    container_name: Option<String>,
    signature_summary: Option<String>,
    module_path: &[String],
    self_type: Option<String>,
    trait_name: Option<String>,
) {
    let Some((range, byte_range)) = span_to_ranges(file, span) else {
        return;
    };
    let symbol_id = format!(
        "rust:{}:{}:{}:{}:{}",
        file.path.display(),
        kind,
        name,
        range.start_line,
        range.start_col
    );
    symbols.push(SymbolRecord {
        symbol_id,
        name,
        kind: kind.into(),
        file_path: file.path.display().to_string(),
        range,
        byte_range,
        container_name,
        signature_summary,
        module_path: module_path.to_vec(),
        self_type,
        trait_name,
    });
}

fn resolve_symbol(
    index: &WorkspaceIndex,
    selector: SymbolSelector,
) -> Result<SymbolRecord, ConnectorError> {
    match selector {
        SymbolSelector::SymbolId(symbol_id) => index
            .symbols
            .iter()
            .find(|symbol| symbol.symbol_id == symbol_id)
            .cloned()
            .ok_or_else(|| ConnectorError::terminal(format!("unknown symbol id: {symbol_id}"))),
        SymbolSelector::Query { query, path_hint, kind_hint } => {
            let best_symbol_id = index
                .symbols
                .iter()
                .filter_map(|symbol| {
                    score_symbol(symbol, &query, path_hint.as_deref(), kind_hint.as_deref()).map(
                        |(score, _)| {
                            (
                                score,
                                symbol.symbol_id.clone(),
                                symbol.file_path.clone(),
                                symbol.range.start_line,
                            )
                        },
                    )
                })
                .max_by_key(|(score, _, file_path, start_line)| {
                    (*score, Reverse(file_path.clone()), Reverse(*start_line))
                })
                .map(|(_, symbol_id, _, _)| symbol_id)
                .ok_or_else(|| {
                    ConnectorError::terminal(format!(
                        "no semantic symbol matches found for query: {query}"
                    ))
                })?;

            index
                .symbols
                .iter()
                .find(|symbol| symbol.symbol_id == best_symbol_id)
                .cloned()
                .ok_or_else(|| ConnectorError::terminal("best semantic symbol was not in index"))
        }
    }
}

fn score_symbol(
    symbol: &SymbolRecord,
    query: &str,
    path_hint: Option<&str>,
    kind_hint: Option<&str>,
) -> Option<(i64, SemanticSymbolMatch)> {
    let query_lower = query.trim().to_lowercase();
    let mut score = 0_i64;
    let mut why = Vec::new();
    let name_lower = symbol.name.to_lowercase();

    if name_lower == query_lower {
        score += 120;
        why.push("exact name match".into());
    } else if name_lower.contains(&query_lower) {
        score += 80;
        why.push("name contains query".into());
    } else if symbol
        .signature_summary
        .as_deref()
        .unwrap_or_default()
        .to_lowercase()
        .contains(&query_lower)
    {
        score += 50;
        why.push("signature matches query".into());
    } else if symbol
        .container_name
        .as_deref()
        .unwrap_or_default()
        .to_lowercase()
        .contains(&query_lower)
    {
        score += 30;
        why.push("container matches query".into());
    } else {
        return None;
    }

    if let Some(kind_hint) = kind_hint {
        if symbol.kind.eq_ignore_ascii_case(kind_hint) {
            score += 20;
            why.push("kind hint matched".into());
        } else {
            score -= 10;
        }
    }

    if let Some(path_hint) = path_hint {
        let normalized = path_hint.to_lowercase();
        if symbol.file_path.to_lowercase().contains(&normalized)
            || symbol.module_path.join("::").to_lowercase().contains(&normalized)
        {
            score += 25;
            why.push("path hint matched".into());
        }
    }

    Some((score, to_symbol_match(symbol.clone(), score, why)))
}

fn to_symbol_match(symbol: SymbolRecord, score: i64, why: Vec<String>) -> SemanticSymbolMatch {
    SemanticSymbolMatch {
        symbol_id: symbol.symbol_id,
        name: symbol.name,
        kind: symbol.kind,
        file_path: symbol.file_path,
        range: symbol.range,
        container_name: symbol.container_name,
        signature_summary: symbol.signature_summary,
        score,
        why,
    }
}

fn span_to_ranges(file: &RustFile, span: Span) -> Option<(SemanticRange, ByteRange)> {
    let start = span.start();
    let end = span.end();
    if start.line == 0 || end.line == 0 {
        return None;
    }
    let start_offset = line_col_to_offset(&file.line_offsets, &file.text, start.line, start.column);
    let end_offset = line_col_to_offset(&file.line_offsets, &file.text, end.line, end.column);
    if end_offset < start_offset || end_offset > file.text.len() {
        return None;
    }
    Some((
        SemanticRange {
            start_line: start.line as u32,
            start_col: start.column as u32 + 1,
            end_line: end.line as u32,
            end_col: end.column as u32 + 1,
        },
        ByteRange { start: start_offset, end: end_offset },
    ))
}

fn line_offsets(text: &str) -> Vec<usize> {
    let mut offsets = vec![0];
    for (idx, byte) in text.bytes().enumerate() {
        if byte == b'\n' {
            offsets.push(idx + 1);
        }
    }
    offsets
}

fn line_col_to_offset(line_offsets: &[usize], text: &str, line: usize, col: usize) -> usize {
    let line_idx = line.saturating_sub(1).min(line_offsets.len().saturating_sub(1));
    let start = line_offsets[line_idx];
    let line_end = line_offsets.get(line_idx + 1).copied().unwrap_or(text.len());
    start.saturating_add(col).min(line_end)
}

fn byte_range_to_semantic_range(line_offsets: &[usize], start: usize, end: usize) -> SemanticRange {
    let (start_line, start_col) = offset_to_line_col(line_offsets, start);
    let (end_line, end_col) = offset_to_line_col(line_offsets, end);
    SemanticRange {
        start_line: start_line as u32,
        start_col: start_col as u32,
        end_line: end_line as u32,
        end_col: end_col as u32,
    }
}

fn offset_to_line_col(line_offsets: &[usize], offset: usize) -> (usize, usize) {
    let idx = match line_offsets.binary_search(&offset) {
        Ok(idx) => idx,
        Err(idx) => idx.saturating_sub(1),
    };
    let line_start = line_offsets.get(idx).copied().unwrap_or(0);
    (idx + 1, offset.saturating_sub(line_start) + 1)
}

fn line_snippet(text: &str, byte_offset: usize) -> String {
    let start = text[..byte_offset].rfind('\n').map(|idx| idx + 1).unwrap_or(0);
    let end = text[byte_offset..].find('\n').map(|idx| byte_offset + idx).unwrap_or(text.len());
    text[start..end].trim().to_string()
}

fn module_path_from_file(workspace_root: &Path, path: &Path) -> Vec<String> {
    let Ok(relative) = path.strip_prefix(workspace_root) else {
        return Vec::new();
    };
    let mut segments = relative
        .iter()
        .filter_map(|part| part.to_str())
        .map(|part| part.trim_end_matches(".rs").to_string())
        .collect::<Vec<_>>();

    if segments.first().is_some_and(|segment| segment == "src" || segment == "tests") {
        segments.remove(0);
    }
    if segments
        .last()
        .is_some_and(|segment| segment == "lib" || segment == "main" || segment == "mod")
    {
        segments.pop();
    }
    segments.into_iter().filter(|segment| !segment.is_empty()).collect()
}

fn normalize_tokens(tokens: String) -> String {
    tokens.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn short_type_name(raw: &str) -> String {
    let trimmed = raw.trim();
    let without_generics = trimmed.split('<').next().unwrap_or(trimmed);
    without_generics.rsplit("::").next().unwrap_or(without_generics).trim().to_string()
}

fn looks_like_test_path(path: &Path) -> bool {
    path.components().any(|component| component.as_os_str() == "tests")
        || path.file_name().and_then(|name| name.to_str()).is_some_and(|name| name.contains("test"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn sample_workspace() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname = \"semantic-fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        fs::create_dir_all(dir.path().join("src")).unwrap();
        fs::write(
            dir.path().join("src/lib.rs"),
            r#"
pub trait Renderable {
    fn render(&self) -> String;
}

pub struct Widget {
    value: String,
}

impl Renderable for Widget {
    fn render(&self) -> String {
        self.value.clone()
    }
}

impl Widget {
    pub fn new(value: &str) -> Self {
        Self { value: value.into() }
    }
}

pub fn build_widget() -> String {
    let widget = Widget::new("hello");
    widget.render()
}
"#,
        )
        .unwrap();
        dir
    }

    #[test]
    fn lookup_symbol_finds_function() {
        let dir = sample_workspace();
        let results = lookup_symbol(dir.path(), "build_widget", None, Some("function"), 5).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].name, "build_widget");
    }

    #[test]
    fn read_symbol_returns_symbol_body() {
        let dir = sample_workspace();
        let result = read_symbol(
            dir.path(),
            SymbolSelector::Query {
                query: "build_widget".into(),
                path_hint: None,
                kind_hint: Some("function".into()),
            },
            true,
        )
        .unwrap();
        assert!(result.content.contains("Widget::new"));
        assert_eq!(result.symbol.name, "build_widget");
    }

    #[test]
    fn find_references_returns_usage_lines() {
        let dir = sample_workspace();
        let refs = find_references(
            dir.path(),
            SymbolSelector::Query {
                query: "Widget".into(),
                path_hint: None,
                kind_hint: Some("struct".into()),
            },
            10,
            true,
        )
        .unwrap();
        assert!(!refs.is_empty());
        assert!(refs.iter().any(|entry| entry.context_snippet.contains("Widget::new")));
    }

    #[test]
    fn find_impls_returns_trait_impl() {
        let dir = sample_workspace();
        let impls = find_impls(
            dir.path(),
            SymbolSelector::Query {
                query: "Widget".into(),
                path_hint: None,
                kind_hint: Some("struct".into()),
            },
            10,
        )
        .unwrap();
        assert!(!impls.is_empty());
        assert!(impls.iter().any(|entry| entry.impl_summary.contains("Renderable")));
    }
}
