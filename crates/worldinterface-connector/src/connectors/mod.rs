//! Built-in connectors and default registry builder.

pub mod delay;
pub mod fs_read;
pub mod fs_write;
pub mod http_request;

use std::sync::Arc;

pub use delay::DelayConnector;
pub use fs_read::FsReadConnector;
pub use fs_write::FsWriteConnector;
pub use http_request::HttpRequestConnector;

use crate::registry::ConnectorRegistry;

/// Build a ConnectorRegistry pre-populated with all built-in connectors.
pub fn default_registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(Arc::new(DelayConnector));
    registry.register(Arc::new(HttpRequestConnector::new()));
    registry.register(Arc::new(FsReadConnector));
    registry.register(Arc::new(FsWriteConnector));
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_registry_has_all_builtins() {
        let registry = default_registry();
        assert_eq!(registry.len(), 4);
        assert!(registry.get("delay").is_some());
        assert!(registry.get("http.request").is_some());
        assert!(registry.get("fs.read").is_some());
        assert!(registry.get("fs.write").is_some());
    }
}
