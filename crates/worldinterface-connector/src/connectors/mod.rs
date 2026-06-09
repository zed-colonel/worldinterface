//! Built-in connectors and default registry builder.

pub mod code_apply_patch;
pub mod code_common;
pub mod code_edit;
pub mod code_fuzzy;
pub mod code_git_diff;
pub mod code_glob;
pub mod code_grep;
pub mod code_impls;
pub mod code_ls;
pub mod code_read;
pub mod code_read_symbol;
pub mod code_references;
pub mod code_symbol;
pub mod code_test;
pub mod code_write;
pub mod delay;
pub mod fs_read;
pub mod fs_write;
pub mod gitignore_check;
pub mod http_request;
pub mod repo_context;
pub mod repo_locate;
pub mod sandbox_exec;
pub mod shell_exec;
pub mod signal_await;
pub mod signal_emit;

use std::sync::Arc;

pub use code_apply_patch::CodeApplyPatchConnector;
pub use code_edit::CodeEditConnector;
pub use code_git_diff::CodeGitDiffConnector;
pub use code_glob::CodeGlobConnector;
pub use code_grep::CodeGrepConnector;
pub use code_impls::CodeImplsConnector;
pub use code_ls::CodeLsConnector;
pub use code_read::CodeReadConnector;
pub use code_read_symbol::CodeReadSymbolConnector;
pub use code_references::CodeReferencesConnector;
pub use code_symbol::CodeSymbolConnector;
pub use code_test::CodeTestConnector;
pub use code_write::CodeWriteConnector;
pub use delay::DelayConnector;
pub use fs_read::FsReadConnector;
pub use fs_write::FsWriteConnector;
pub use http_request::HttpRequestConnector;
pub use repo_context::RepoContextConnector;
pub use repo_locate::RepoLocateConnector;
pub use sandbox_exec::SandboxExecConnector;
pub use shell_exec::ShellExecConnector;
pub use signal_await::SignalAwaitConnector;
pub use signal_emit::SignalEmitConnector;

use crate::registry::ConnectorRegistry;

/// Build a ConnectorRegistry pre-populated with all built-in connectors.
pub fn default_registry() -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(DelayConnector));
    registry.register(Arc::new(HttpRequestConnector::new()));
    registry.register(Arc::new(FsReadConnector));
    registry.register(Arc::new(FsWriteConnector));
    registry.register(Arc::new(ShellExecConnector::new()));
    registry.register(Arc::new(SandboxExecConnector::new()));
    registry.register(Arc::new(CodeReadConnector));
    registry.register(Arc::new(CodeEditConnector));
    registry.register(Arc::new(CodeWriteConnector));
    registry.register(Arc::new(CodeGrepConnector));
    registry.register(Arc::new(CodeGlobConnector));
    registry.register(Arc::new(CodeLsConnector));
    registry.register(Arc::new(CodeApplyPatchConnector));
    registry.register(Arc::new(CodeGitDiffConnector));
    registry.register(Arc::new(RepoContextConnector));
    registry.register(Arc::new(RepoLocateConnector));
    registry.register(Arc::new(CodeSymbolConnector));
    registry.register(Arc::new(CodeReadSymbolConnector));
    registry.register(Arc::new(CodeReferencesConnector));
    registry.register(Arc::new(CodeImplsConnector));
    registry.register(Arc::new(CodeTestConnector));
    registry
}

#[cfg(test)]
mod tests {
    use worldinterface_core::descriptor::ConnectorCategory;

    use super::*;

    #[test]
    fn default_registry_has_21_connectors() {
        let registry = default_registry();
        assert_eq!(registry.len(), 21);
        assert!(registry.get("delay").is_some());
        assert!(registry.get("http.request").is_some());
        assert!(registry.get("fs.read").is_some());
        assert!(registry.get("fs.write").is_some());
        assert!(registry.get("shell.exec").is_some());
        assert!(registry.get("sandbox.exec").is_some());
        assert!(registry.get("code.read").is_some());
        assert!(registry.get("code.edit").is_some());
        assert!(registry.get("code.write").is_some());
        assert!(registry.get("code.grep").is_some());
        assert!(registry.get("code.glob").is_some());
        assert!(registry.get("code.ls").is_some());
        assert!(registry.get("code.apply_patch").is_some());
        assert!(registry.get("code.git_diff").is_some());
        assert!(registry.get("repo.context").is_some());
        assert!(registry.get("repo.locate").is_some());
        assert!(registry.get("code.symbol").is_some());
        assert!(registry.get("code.read_symbol").is_some());
        assert!(registry.get("code.references").is_some());
        assert!(registry.get("code.impls").is_some());
        assert!(registry.get("code.test").is_some());
    }

    #[test]
    fn new_code_connectors_all_code_category() {
        let registry = default_registry();
        for name in [
            "code.read",
            "code.edit",
            "code.write",
            "code.grep",
            "code.glob",
            "code.ls",
            "code.apply_patch",
            "code.git_diff",
            "repo.context",
            "repo.locate",
            "code.symbol",
            "code.read_symbol",
            "code.references",
            "code.impls",
            "code.test",
        ] {
            let desc = registry.describe(name).unwrap();
            assert_eq!(desc.category, ConnectorCategory::Code);
        }
    }

    // ── T16: signal_connectors_have_signal_category ──
    #[test]
    fn signal_connectors_have_signal_category() {
        use crate::signal_registry::SignalRegistry;
        use crate::traits::Connector;

        let registry = Arc::new(SignalRegistry::new());
        let await_conn = SignalAwaitConnector::new(Arc::clone(&registry));
        let emit_conn = SignalEmitConnector::new(registry);

        let await_desc = await_conn.describe();
        assert_eq!(await_desc.category, ConnectorCategory::Custom("signal".into()));

        let emit_desc = emit_conn.describe();
        assert_eq!(emit_desc.category, ConnectorCategory::Custom("signal".into()));
    }
}
