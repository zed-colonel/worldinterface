mod analyzer;

use std::path::Path;

use super::{SemanticBackend, SemanticImplMatch, SemanticReadResult, SemanticReferenceMatch};
use super::{SemanticSymbolMatch, SymbolSelector};
use crate::error::ConnectorError;

pub(crate) struct RustSemanticBackend;

impl SemanticBackend for RustSemanticBackend {
    fn lookup_symbol(
        &self,
        workspace_root: &Path,
        query: &str,
        path_hint: Option<&str>,
        kind_hint: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SemanticSymbolMatch>, ConnectorError> {
        analyzer::lookup_symbol(workspace_root, query, path_hint, kind_hint, limit)
    }

    fn read_symbol(
        &self,
        workspace_root: &Path,
        selector: SymbolSelector,
        include_body: bool,
    ) -> Result<SemanticReadResult, ConnectorError> {
        analyzer::read_symbol(workspace_root, selector, include_body)
    }

    fn find_references(
        &self,
        workspace_root: &Path,
        selector: SymbolSelector,
        limit: usize,
        include_tests: bool,
    ) -> Result<Vec<SemanticReferenceMatch>, ConnectorError> {
        analyzer::find_references(workspace_root, selector, limit, include_tests)
    }

    fn find_impls(
        &self,
        workspace_root: &Path,
        selector: SymbolSelector,
        limit: usize,
    ) -> Result<Vec<SemanticImplMatch>, ConnectorError> {
        analyzer::find_impls(workspace_root, selector, limit)
    }
}

pub(crate) fn looks_like_rust_workspace(workspace_root: &Path) -> bool {
    workspace_root.join("Cargo.toml").is_file()
        || workspace_root.join("src").is_dir()
        || workspace_root
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("rs"))
}
