mod rust;
pub(crate) mod types;

use std::path::{Path, PathBuf};

use cargo_metadata::MetadataCommand;

use crate::error::ConnectorError;

pub(crate) use types::{
    SemanticImplMatch, SemanticReadResult, SemanticReferenceMatch, SemanticSymbolMatch,
};

#[derive(Debug, Clone)]
pub(crate) enum SymbolSelector {
    SymbolId(String),
    Query { query: String, path_hint: Option<String>, kind_hint: Option<String> },
}

pub(crate) trait SemanticBackend {
    fn lookup_symbol(
        &self,
        workspace_root: &Path,
        query: &str,
        path_hint: Option<&str>,
        kind_hint: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SemanticSymbolMatch>, ConnectorError>;

    fn read_symbol(
        &self,
        workspace_root: &Path,
        selector: SymbolSelector,
        include_body: bool,
    ) -> Result<SemanticReadResult, ConnectorError>;

    fn find_references(
        &self,
        workspace_root: &Path,
        selector: SymbolSelector,
        limit: usize,
        include_tests: bool,
    ) -> Result<Vec<SemanticReferenceMatch>, ConnectorError>;

    fn find_impls(
        &self,
        workspace_root: &Path,
        selector: SymbolSelector,
        limit: usize,
    ) -> Result<Vec<SemanticImplMatch>, ConnectorError>;
}

pub(crate) fn lookup_symbol(
    path: &str,
    query: &str,
    path_hint: Option<&str>,
    kind_hint: Option<&str>,
    limit: usize,
) -> Result<(PathBuf, Vec<SemanticSymbolMatch>), ConnectorError> {
    let workspace_root = resolve_workspace_root(path)?;
    let backend = backend_for(&workspace_root)?;
    let matches = backend.lookup_symbol(&workspace_root, query, path_hint, kind_hint, limit)?;
    Ok((workspace_root, matches))
}

pub(crate) fn read_symbol(
    path: &str,
    selector: SymbolSelector,
    include_body: bool,
) -> Result<(PathBuf, SemanticReadResult), ConnectorError> {
    let workspace_root = resolve_workspace_root(path)?;
    let backend = backend_for(&workspace_root)?;
    let result = backend.read_symbol(&workspace_root, selector, include_body)?;
    Ok((workspace_root, result))
}

pub(crate) fn find_references(
    path: &str,
    selector: SymbolSelector,
    limit: usize,
    include_tests: bool,
) -> Result<(PathBuf, Vec<SemanticReferenceMatch>), ConnectorError> {
    let workspace_root = resolve_workspace_root(path)?;
    let backend = backend_for(&workspace_root)?;
    let matches = backend.find_references(&workspace_root, selector, limit, include_tests)?;
    Ok((workspace_root, matches))
}

pub(crate) fn find_impls(
    path: &str,
    selector: SymbolSelector,
    limit: usize,
) -> Result<(PathBuf, Vec<SemanticImplMatch>), ConnectorError> {
    let workspace_root = resolve_workspace_root(path)?;
    let backend = backend_for(&workspace_root)?;
    let matches = backend.find_impls(&workspace_root, selector, limit)?;
    Ok((workspace_root, matches))
}

fn backend_for(workspace_root: &Path) -> Result<Box<dyn SemanticBackend>, ConnectorError> {
    if rust::looks_like_rust_workspace(workspace_root) {
        Ok(Box::new(rust::RustSemanticBackend))
    } else {
        Err(ConnectorError::terminal(format!(
            "no semantic backend available for workspace: {}",
            workspace_root.display()
        )))
    }
}

fn resolve_workspace_root(path: &str) -> Result<PathBuf, ConnectorError> {
    let input = PathBuf::from(path);
    if !input.is_absolute() {
        return Err(ConnectorError::InvalidParams(
            "invalid 'path' (expected absolute workspace directory)".into(),
        ));
    }

    let canonical = input.canonicalize().map_err(|e| {
        ConnectorError::terminal(format!(
            "failed to resolve workspace path {}: {e}",
            input.display()
        ))
    })?;
    let base = if canonical.is_file() {
        canonical.parent().map(Path::to_path_buf).ok_or_else(|| {
            ConnectorError::terminal(format!(
                "could not resolve parent directory for {}",
                canonical.display()
            ))
        })?
    } else {
        canonical
    };

    if let Some(manifest_path) = find_manifest_path(&base) {
        if let Ok(metadata) = MetadataCommand::new().manifest_path(&manifest_path).no_deps().exec()
        {
            return Ok(metadata.workspace_root.as_std_path().to_path_buf());
        }
    }

    Ok(base)
}

fn find_manifest_path(root: &Path) -> Option<PathBuf> {
    let mut current = Some(root);
    while let Some(dir) = current {
        let manifest = dir.join("Cargo.toml");
        if manifest.is_file() {
            return Some(manifest);
        }
        current = dir.parent();
    }
    None
}
