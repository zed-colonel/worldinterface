//! Configuration for the ContextStore.

use std::path::PathBuf;

/// Configuration for the ContextStore.
#[derive(Debug, Clone)]
pub struct ContextStoreConfig {
    /// Path to the SQLite database file.
    /// If None, uses in-memory storage (testing only).
    pub path: Option<PathBuf>,
}

impl Default for ContextStoreConfig {
    fn default() -> Self {
        Self { path: Some(PathBuf::from("contextstore.db")) }
    }
}
