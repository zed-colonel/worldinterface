//! Central registry for connector lookup and discovery.

use std::collections::HashMap;
use std::sync::Arc;

use worldinterface_core::descriptor::Descriptor;

use crate::traits::Connector;

/// Central registry for connector lookup and discovery.
///
/// Connectors register by name (matching the `name` field in their Descriptor).
/// The registry is constructed at startup and is immutable during execution.
pub struct ConnectorRegistry {
    connectors: HashMap<String, Arc<dyn Connector>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self { connectors: HashMap::new() }
    }

    /// Register a connector. Panics if a connector with the same name is
    /// already registered (names must be unique).
    pub fn register(&mut self, connector: Arc<dyn Connector>) {
        let name = connector.describe().name;
        if self.connectors.contains_key(&name) {
            panic!("duplicate connector registration: '{name}'");
        }
        self.connectors.insert(name, connector);
    }

    /// Look up a connector by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Connector>> {
        self.connectors.get(name).cloned()
    }

    /// List all registered connector descriptors.
    pub fn list_capabilities(&self) -> Vec<Descriptor> {
        let mut descriptors: Vec<_> = self.connectors.values().map(|c| c.describe()).collect();
        descriptors.sort_by(|a, b| a.name.cmp(&b.name));
        descriptors
    }

    /// Describe a specific connector by name.
    pub fn describe(&self, name: &str) -> Option<Descriptor> {
        self.connectors.get(name).map(|c| c.describe())
    }

    /// Returns the number of registered connectors.
    pub fn len(&self) -> usize {
        self.connectors.len()
    }

    /// Returns true if no connectors are registered.
    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty()
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

    use super::*;
    use crate::context::InvocationContext;
    use crate::error::ConnectorError;

    struct StubConnector {
        name: String,
    }

    impl StubConnector {
        fn new(name: &str) -> Self {
            Self { name: name.to_string() }
        }
    }

    impl Connector for StubConnector {
        fn describe(&self) -> Descriptor {
            Descriptor {
                name: self.name.clone(),
                display_name: self.name.clone(),
                description: "stub".into(),
                category: ConnectorCategory::Custom("test".into()),
                input_schema: None,
                output_schema: None,
                idempotent: true,
                side_effects: false,
            }
        }

        fn invoke(
            &self,
            _ctx: &InvocationContext,
            _params: &Value,
        ) -> Result<Value, ConnectorError> {
            Ok(Value::Null)
        }
    }

    #[test]
    fn register_and_lookup() {
        let mut registry = ConnectorRegistry::new();
        registry.register(Arc::new(StubConnector::new("test.one")));
        let c = registry.get("test.one");
        assert!(c.is_some());
        assert_eq!(c.unwrap().describe().name, "test.one");
    }

    #[test]
    fn lookup_nonexistent_returns_none() {
        let registry = ConnectorRegistry::new();
        assert!(registry.get("no.such").is_none());
    }

    #[test]
    fn list_capabilities_returns_all() {
        let mut registry = ConnectorRegistry::new();
        registry.register(Arc::new(StubConnector::new("c")));
        registry.register(Arc::new(StubConnector::new("a")));
        registry.register(Arc::new(StubConnector::new("b")));
        let caps = registry.list_capabilities();
        assert_eq!(caps.len(), 3);
        // Sorted by name
        assert_eq!(caps[0].name, "a");
        assert_eq!(caps[1].name, "b");
        assert_eq!(caps[2].name, "c");
    }

    #[test]
    fn describe_returns_descriptor() {
        let mut registry = ConnectorRegistry::new();
        registry.register(Arc::new(StubConnector::new("test.desc")));
        let desc = registry.describe("test.desc");
        assert!(desc.is_some());
        assert_eq!(desc.unwrap().name, "test.desc");
    }

    #[test]
    fn describe_nonexistent_returns_none() {
        let registry = ConnectorRegistry::new();
        assert!(registry.describe("nope").is_none());
    }

    #[test]
    #[should_panic(expected = "duplicate connector registration: 'dup'")]
    fn register_duplicate_panics() {
        let mut registry = ConnectorRegistry::new();
        registry.register(Arc::new(StubConnector::new("dup")));
        registry.register(Arc::new(StubConnector::new("dup")));
    }

    #[test]
    fn len_and_is_empty() {
        let mut registry = ConnectorRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        registry.register(Arc::new(StubConnector::new("x")));
        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
    }
}
