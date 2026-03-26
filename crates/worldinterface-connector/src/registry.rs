//! Central registry for connector lookup and discovery.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use worldinterface_core::descriptor::Descriptor;

use crate::traits::Connector;

/// Errors from the connector registry.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("duplicate connector: {0}")]
    DuplicateConnector(String),
    #[error("connector not found: {0}")]
    NotFound(String),
}

/// Central registry for connector lookup and discovery.
///
/// Connectors register by name (matching the `name` field in their Descriptor).
/// The registry uses `RwLock` to allow concurrent reads and exclusive writes,
/// supporting runtime hot-loading of connectors.
pub struct ConnectorRegistry {
    connectors: RwLock<HashMap<String, Arc<dyn Connector>>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self { connectors: RwLock::new(HashMap::new()) }
    }

    /// Register a connector. Panics if a connector with the same name is
    /// already registered (names must be unique).
    ///
    /// For fallible registration at runtime, use [`register_runtime`](Self::register_runtime).
    pub fn register(&self, connector: Arc<dyn Connector>) {
        self.register_runtime(connector).expect("duplicate connector registration");
    }

    /// Register a connector at runtime, returning an error on duplicate names.
    pub fn register_runtime(&self, connector: Arc<dyn Connector>) -> Result<(), RegistryError> {
        let name = connector.describe().name;
        let mut map = self.connectors.write().unwrap();
        if map.contains_key(&name) {
            return Err(RegistryError::DuplicateConnector(name));
        }
        map.insert(name, connector);
        Ok(())
    }

    /// Unregister a connector by name, returning an error if not found.
    pub fn unregister(&self, name: &str) -> Result<(), RegistryError> {
        let mut map = self.connectors.write().unwrap();
        if map.remove(name).is_none() {
            return Err(RegistryError::NotFound(name.to_string()));
        }
        Ok(())
    }

    /// Look up a connector by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Connector>> {
        let map = self.connectors.read().unwrap();
        map.get(name).cloned()
    }

    /// List all registered connector descriptors.
    pub fn list_capabilities(&self) -> Vec<Descriptor> {
        let map = self.connectors.read().unwrap();
        let mut descriptors: Vec<_> = map.values().map(|c| c.describe()).collect();
        descriptors.sort_by(|a, b| a.name.cmp(&b.name));
        descriptors
    }

    /// Describe a specific connector by name.
    pub fn describe(&self, name: &str) -> Option<Descriptor> {
        let map = self.connectors.read().unwrap();
        map.get(name).map(|c| c.describe())
    }

    /// Returns the number of registered connectors.
    pub fn len(&self) -> usize {
        let map = self.connectors.read().unwrap();
        map.len()
    }

    /// Returns true if no connectors are registered.
    pub fn is_empty(&self) -> bool {
        let map = self.connectors.read().unwrap();
        map.is_empty()
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
        let registry = ConnectorRegistry::new();
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
        let registry = ConnectorRegistry::new();
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
        let registry = ConnectorRegistry::new();
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
    #[should_panic(expected = "duplicate connector registration")]
    fn register_duplicate_panics() {
        let registry = ConnectorRegistry::new();
        registry.register(Arc::new(StubConnector::new("dup")));
        registry.register(Arc::new(StubConnector::new("dup")));
    }

    #[test]
    fn len_and_is_empty() {
        let registry = ConnectorRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        registry.register(Arc::new(StubConnector::new("x")));
        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
    }

    // ── E5S3-T1: concurrent reads don't block ──

    #[test]
    fn registry_rwlock_concurrent_reads() {
        let registry = Arc::new(ConnectorRegistry::new());
        registry.register(Arc::new(StubConnector::new("alpha")));
        registry.register(Arc::new(StubConnector::new("beta")));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let reg = Arc::clone(&registry);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        assert!(reg.get("alpha").is_some());
                        assert_eq!(reg.len(), 2);
                        let caps = reg.list_capabilities();
                        assert_eq!(caps.len(), 2);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    // ── E5S3-T2: register_runtime success ──

    #[test]
    fn registry_register_runtime_success() {
        let registry = ConnectorRegistry::new();
        let result = registry.register_runtime(Arc::new(StubConnector::new("rt.new")));
        assert!(result.is_ok());
        assert!(registry.get("rt.new").is_some());
        assert_eq!(registry.len(), 1);
    }

    // ── E5S3-T3: register_runtime duplicate returns Err ──

    #[test]
    fn registry_register_runtime_duplicate() {
        let registry = ConnectorRegistry::new();
        registry.register(Arc::new(StubConnector::new("rt.dup")));
        let result = registry.register_runtime(Arc::new(StubConnector::new("rt.dup")));
        assert!(result.is_err());
        assert!(
            matches!(result, Err(RegistryError::DuplicateConnector(ref name)) if name == "rt.dup")
        );
    }

    // ── E5S3-T4: unregister success ──

    #[test]
    fn registry_unregister_success() {
        let registry = ConnectorRegistry::new();
        registry.register(Arc::new(StubConnector::new("rm.me")));
        assert!(registry.get("rm.me").is_some());

        let result = registry.unregister("rm.me");
        assert!(result.is_ok());
        assert!(registry.get("rm.me").is_none());
        assert_eq!(registry.len(), 0);
        // Not in list_capabilities either
        assert!(registry.list_capabilities().is_empty());
    }

    // ── E5S3-T5: unregister not found ──

    #[test]
    fn registry_unregister_not_found() {
        let registry = ConnectorRegistry::new();
        let result = registry.unregister("ghost");
        assert!(result.is_err());
        assert!(matches!(result, Err(RegistryError::NotFound(ref name)) if name == "ghost"));
    }
}
