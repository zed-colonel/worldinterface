//! In-memory webhook registry backed by ContextStore for durability.

use std::collections::HashMap;

use wi_contextstore::ContextStore;

use crate::error::WebhookError;
use crate::registration::WebhookRegistration;
use crate::types::WebhookId;

/// Global key in ContextStore for persisting webhook registrations.
const WEBHOOKS_GLOBAL_KEY: &str = "webhook_registrations";

/// In-memory registry of webhook registrations, backed by ContextStore for durability.
pub struct WebhookRegistry {
    /// Path -> Registration mapping for O(1) lookup on invocation.
    by_path: HashMap<String, WebhookRegistration>,
    /// ID -> Path mapping for O(1) lookup on deletion.
    by_id: HashMap<WebhookId, String>,
}

impl WebhookRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self { by_path: HashMap::new(), by_id: HashMap::new() }
    }

    /// Load all webhook registrations from ContextStore.
    ///
    /// Called at daemon startup to restore registrations from the previous session.
    pub fn load_from_store(store: &dyn ContextStore) -> Result<Self, WebhookError> {
        let mut registry = Self::new();
        if let Some(value) = store.get_global(WEBHOOKS_GLOBAL_KEY)? {
            let registrations: Vec<WebhookRegistration> = serde_json::from_value(value)?;
            for reg in registrations {
                registry.by_id.insert(reg.id, reg.path.clone());
                registry.by_path.insert(reg.path.clone(), reg);
            }
        }
        Ok(registry)
    }

    /// Persist all registrations to ContextStore.
    fn persist(&self, store: &dyn ContextStore) -> Result<(), WebhookError> {
        let registrations: Vec<&WebhookRegistration> = self.by_path.values().collect();
        let value = serde_json::to_value(registrations)?;
        store.upsert_global(WEBHOOKS_GLOBAL_KEY, &value)?;
        Ok(())
    }

    /// Register a new webhook. Returns error if the path is already registered.
    pub fn register(
        &mut self,
        registration: WebhookRegistration,
        store: &dyn ContextStore,
    ) -> Result<(), WebhookError> {
        if self.by_path.contains_key(&registration.path) {
            return Err(WebhookError::PathAlreadyRegistered(registration.path.clone()));
        }
        self.by_id.insert(registration.id, registration.path.clone());
        self.by_path.insert(registration.path.clone(), registration);
        self.persist(store)?;
        Ok(())
    }

    /// Remove a webhook by ID. Returns the removed registration, or error if not found.
    pub fn remove(
        &mut self,
        id: WebhookId,
        store: &dyn ContextStore,
    ) -> Result<WebhookRegistration, WebhookError> {
        let path = self.by_id.remove(&id).ok_or(WebhookError::WebhookNotFound(id))?;
        let registration = self.by_path.remove(&path).expect("inconsistent registry");
        self.persist(store)?;
        Ok(registration)
    }

    /// Look up a webhook by path. Used by the catch-all webhook handler.
    pub fn get_by_path(&self, path: &str) -> Option<&WebhookRegistration> {
        self.by_path.get(path)
    }

    /// Look up the path for a webhook ID.
    pub fn by_id_to_path(&self, id: &WebhookId) -> Option<&str> {
        self.by_id.get(id).map(|s| s.as_str())
    }

    /// List all registered webhooks, sorted by creation time.
    pub fn list(&self) -> Vec<&WebhookRegistration> {
        let mut registrations: Vec<_> = self.by_path.values().collect();
        registrations.sort_by_key(|r| r.created_at);
        registrations
    }

    /// Number of registered webhooks.
    pub fn len(&self) -> usize {
        self.by_path.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.by_path.is_empty()
    }
}

impl Default for WebhookRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wi_contextstore::SqliteContextStore;
    use wi_core::flowspec::*;
    use wi_core::id::NodeId;

    use super::*;

    fn make_store() -> SqliteContextStore {
        SqliteContextStore::in_memory().unwrap()
    }

    fn make_registration(path: &str, created_at: u64) -> WebhookRegistration {
        let node_id = NodeId::new();
        WebhookRegistration {
            id: WebhookId::new(),
            path: path.to_string(),
            flow_spec: FlowSpec {
                id: None,
                name: None,
                nodes: vec![Node {
                    id: node_id,
                    label: None,
                    node_type: NodeType::Connector(ConnectorNode {
                        connector: "delay".into(),
                        params: json!({"duration_ms": 10}),
                        idempotency_config: None,
                    }),
                }],
                edges: vec![],
                params: None,
            },
            description: None,
            created_at,
        }
    }

    #[test]
    fn register_and_lookup() {
        let store = make_store();
        let mut registry = WebhookRegistry::new();
        let reg = make_registration("github/push", 1000);
        let id = reg.id;
        registry.register(reg, &store).unwrap();

        let found = registry.get_by_path("github/push").unwrap();
        assert_eq!(found.id, id);
        assert_eq!(found.path, "github/push");
    }

    #[test]
    fn register_duplicate_path_rejected() {
        let store = make_store();
        let mut registry = WebhookRegistry::new();
        let reg1 = make_registration("github/push", 1000);
        registry.register(reg1, &store).unwrap();

        let reg2 = make_registration("github/push", 2000);
        let err = registry.register(reg2, &store).unwrap_err();
        assert!(matches!(err, WebhookError::PathAlreadyRegistered(_)));
    }

    #[test]
    fn remove_by_id() {
        let store = make_store();
        let mut registry = WebhookRegistry::new();
        let reg = make_registration("github/push", 1000);
        let id = reg.id;
        registry.register(reg, &store).unwrap();

        let removed = registry.remove(id, &store).unwrap();
        assert_eq!(removed.path, "github/push");
        assert!(registry.get_by_path("github/push").is_none());
    }

    #[test]
    fn remove_nonexistent_returns_error() {
        let store = make_store();
        let mut registry = WebhookRegistry::new();
        let err = registry.remove(WebhookId::new(), &store).unwrap_err();
        assert!(matches!(err, WebhookError::WebhookNotFound(_)));
    }

    #[test]
    fn list_returns_all_sorted_by_created_at() {
        let store = make_store();
        let mut registry = WebhookRegistry::new();
        registry.register(make_registration("c-path", 3000), &store).unwrap();
        registry.register(make_registration("a-path", 1000), &store).unwrap();
        registry.register(make_registration("b-path", 2000), &store).unwrap();

        let list = registry.list();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].path, "a-path");
        assert_eq!(list[1].path, "b-path");
        assert_eq!(list[2].path, "c-path");
    }

    #[test]
    fn len_tracks_count() {
        let store = make_store();
        let mut registry = WebhookRegistry::new();
        let reg1 = make_registration("path1", 1000);
        let reg2 = make_registration("path2", 2000);
        let id1 = reg1.id;
        registry.register(reg1, &store).unwrap();
        registry.register(reg2, &store).unwrap();
        assert_eq!(registry.len(), 2);

        registry.remove(id1, &store).unwrap();
        assert_eq!(registry.len(), 1);
    }

    // T-4: Persistence tests

    #[test]
    fn persist_and_load_roundtrip() {
        let store = make_store();
        let mut registry = WebhookRegistry::new();
        registry.register(make_registration("path1", 1000), &store).unwrap();
        registry.register(make_registration("path2", 2000), &store).unwrap();

        // Load into a new registry from the same store
        let loaded = WebhookRegistry::load_from_store(&store).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.get_by_path("path1").is_some());
        assert!(loaded.get_by_path("path2").is_some());
    }

    #[test]
    fn load_from_empty_store() {
        let store = make_store();
        let loaded = WebhookRegistry::load_from_store(&store).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn persist_after_remove() {
        let store = make_store();
        let mut registry = WebhookRegistry::new();
        let reg1 = make_registration("path1", 1000);
        let id1 = reg1.id;
        registry.register(reg1, &store).unwrap();
        registry.register(make_registration("path2", 2000), &store).unwrap();

        registry.remove(id1, &store).unwrap();

        let loaded = WebhookRegistry::load_from_store(&store).unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.get_by_path("path1").is_none());
        assert!(loaded.get_by_path("path2").is_some());
    }
}
