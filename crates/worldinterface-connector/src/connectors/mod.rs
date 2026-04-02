//! Built-in connectors and default registry builder.

pub mod code_common;
pub mod code_edit;
pub mod code_read;
pub mod code_write;
pub mod delay;
pub mod fs_read;
pub mod fs_write;
pub mod gitignore_check;
pub mod http_request;
pub mod peer_resolve;
pub mod sandbox_exec;
pub mod shell_exec;

use std::sync::Arc;

pub use code_edit::CodeEditConnector;
pub use code_read::CodeReadConnector;
pub use code_write::CodeWriteConnector;
pub use delay::DelayConnector;
pub use fs_read::FsReadConnector;
pub use fs_write::FsWriteConnector;
pub use http_request::HttpRequestConnector;
pub use peer_resolve::PeerResolveConnector;
pub use sandbox_exec::SandboxExecConnector;
pub use shell_exec::ShellExecConnector;

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
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    use worldinterface_core::descriptor::ConnectorCategory;

    #[test]
    fn default_registry_has_code_connectors() {
        let registry = default_registry();
        assert_eq!(registry.len(), 9);
        assert!(registry.get("delay").is_some());
        assert!(registry.get("http.request").is_some());
        assert!(registry.get("fs.read").is_some());
        assert!(registry.get("fs.write").is_some());
        assert!(registry.get("shell.exec").is_some());
        assert!(registry.get("sandbox.exec").is_some());
        assert!(registry.get("code.read").is_some());
        assert!(registry.get("code.edit").is_some());
        assert!(registry.get("code.write").is_some());
    }

    #[test]
    fn code_connectors_all_code_category() {
        let registry = default_registry();
        for name in ["code.read", "code.edit", "code.write"] {
            let desc = registry.describe(name).unwrap();
            assert_eq!(desc.category, ConnectorCategory::Code);
        }
    }
}
