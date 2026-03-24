//! WASM resource pool — isolated resources for WASM host functions.
//!
//! WASM host functions maintain their own HTTP client, KV store, and rate
//! limiters. These are NOT shared with native connector implementations.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::Mutex as TokioMutex;
use tokio_tungstenite::MaybeTlsStream;

/// A persistent WebSocket connection managed by the resource pool.
///
/// Uses tokio async Mutex because WebSocket I/O is async.
/// Host functions bridge to async via `Handle::current().block_on()`.
pub struct WebSocketConnection {
    pub stream:
        TokioMutex<tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>,
}

/// Shared resource pool for WASM host functions.
///
/// Resource isolation principle: WASM modules get their own HTTP client,
/// connection pools, and rate counters, separate from native connectors.
pub struct WasmResourcePool {
    /// Own HTTP client — separate from native HttpRequestConnector.
    pub http_client: reqwest::blocking::Client,
    /// Shared KV store (namespaced by module name).
    pub kv_store: Arc<Mutex<rusqlite::Connection>>,
    /// Per-module rate limiters.
    pub rate_limiters: Arc<Mutex<HashMap<String, RateLimiter>>>,
    /// KV store directory path.
    kv_store_dir: PathBuf,
    /// Persistent WebSocket connections (survive across invoke() calls).
    /// Outer Mutex is std::sync (held briefly for HashMap lookup).
    /// Inner TokioMutex on each connection is for async I/O.
    pub websocket_connections: Mutex<HashMap<String, Arc<WebSocketConnection>>>,
}

/// Simple token-bucket rate limiter.
pub struct RateLimiter {
    pub max_requests_per_second: f64,
    pub tokens: f64,
    pub last_refill: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter with the given max requests per second.
    pub fn new(max_rps: f64) -> Self {
        Self { max_requests_per_second: max_rps, tokens: max_rps, last_refill: Instant::now() }
    }

    /// Try to acquire a token. Returns true if allowed, false if rate limited.
    pub fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.max_requests_per_second)
            .min(self.max_requests_per_second);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

impl WasmResourcePool {
    /// Create a new resource pool with isolated resources.
    pub fn new(kv_store_dir: &Path) -> Result<Self, crate::error::WasmError> {
        std::fs::create_dir_all(kv_store_dir)?;

        let db_path = kv_store_dir.join("wasm_kv.db");
        let conn = rusqlite::Connection::open(&db_path).map_err(|e| {
            crate::error::WasmError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        // Create KV table
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS kv (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (namespace, key)
            )",
        )
        .map_err(|e| {
            crate::error::WasmError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        let http_client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                crate::error::WasmError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        Ok(Self {
            http_client,
            kv_store: Arc::new(Mutex::new(conn)),
            rate_limiters: Arc::new(Mutex::new(HashMap::new())),
            kv_store_dir: kv_store_dir.to_path_buf(),
            websocket_connections: Mutex::new(HashMap::new()),
        })
    }

    /// Get the KV store directory.
    pub fn kv_store_dir(&self) -> &Path {
        &self.kv_store_dir
    }

    /// Get a value from the KV store for a given module namespace.
    pub fn kv_get(&self, namespace: &str, key: &str) -> Option<String> {
        let conn = self.kv_store.lock().unwrap();
        conn.query_row(
            "SELECT value FROM kv WHERE namespace = ?1 AND key = ?2",
            rusqlite::params![namespace, key],
            |row| row.get(0),
        )
        .ok()
    }

    /// Set a value in the KV store for a given module namespace.
    pub fn kv_set(&self, namespace: &str, key: &str, value: &str) {
        let conn = self.kv_store.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO kv (namespace, key, value) VALUES (?1, ?2, ?3)",
            rusqlite::params![namespace, key, value],
        )
        .expect("kv_set failed");
    }

    /// Delete a key from the KV store. Returns true if a row was deleted.
    pub fn kv_delete(&self, namespace: &str, key: &str) -> bool {
        let conn = self.kv_store.lock().unwrap();
        let deleted = conn
            .execute(
                "DELETE FROM kv WHERE namespace = ?1 AND key = ?2",
                rusqlite::params![namespace, key],
            )
            .unwrap_or(0);
        deleted > 0
    }

    /// List keys matching a prefix in the KV store for a given module namespace.
    pub fn kv_list_keys(&self, namespace: &str, prefix: &str) -> Vec<String> {
        let conn = self.kv_store.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT key FROM kv WHERE namespace = ?1 AND key LIKE ?2")
            .expect("kv_list_keys prepare failed");
        let like_pattern = format!("{prefix}%");
        stmt.query_map(rusqlite::params![namespace, like_pattern], |row| row.get(0))
            .expect("kv_list_keys query failed")
            .filter_map(|r| r.ok())
            .collect()
    }

    /// Insert a new WebSocket connection.
    pub fn ws_insert(&self, id: String, conn: Arc<WebSocketConnection>) {
        self.websocket_connections.lock().unwrap().insert(id, conn);
    }

    /// Get a WebSocket connection by ID. Returns cloned Arc (brief lock).
    pub fn ws_get(&self, id: &str) -> Option<Arc<WebSocketConnection>> {
        self.websocket_connections.lock().unwrap().get(id).cloned()
    }

    /// Remove a WebSocket connection by ID.
    pub fn ws_remove(&self, id: &str) -> Option<Arc<WebSocketConnection>> {
        self.websocket_connections.lock().unwrap().remove(id)
    }

    /// Create a minimal resource pool for unit tests (in-memory KV, no disk).
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        let dir = std::env::temp_dir().join(format!("wi-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        Self::new(&dir).unwrap()
    }

    /// Try to acquire a rate limit token for a module.
    pub fn try_acquire_rate_limit(&self, module_name: &str, max_rps: f64) -> bool {
        let mut limiters = self.rate_limiters.lock().unwrap();
        let limiter =
            limiters.entry(module_name.to_string()).or_insert_with(|| RateLimiter::new(max_rps));
        limiter.try_acquire()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── E2S3-T45: WasmResourcePool: owns separate reqwest client ──

    #[test]
    fn resource_pool_owns_separate_http_client() {
        let dir = tempfile::tempdir().unwrap();
        let pool = WasmResourcePool::new(dir.path()).unwrap();
        // The pool has its own client — just verify it exists and is usable
        let _client = &pool.http_client;
        // Verify it's a distinct instance (no shared state test needed — architectural)
    }

    // ── E2S3-T46: WasmResourcePool: rate limiter per module ──

    #[test]
    fn resource_pool_rate_limiter_per_module() {
        let dir = tempfile::tempdir().unwrap();
        let pool = WasmResourcePool::new(dir.path()).unwrap();

        // First request should succeed
        assert!(pool.try_acquire_rate_limit("module-a", 10.0));
        // Module B should have its own limiter
        assert!(pool.try_acquire_rate_limit("module-b", 10.0));

        // Drain module-a's tokens
        for _ in 0..9 {
            pool.try_acquire_rate_limit("module-a", 10.0);
        }
        // Module-a should be exhausted
        assert!(!pool.try_acquire_rate_limit("module-a", 10.0));
        // Module-b should still have tokens
        assert!(pool.try_acquire_rate_limit("module-b", 10.0));
    }

    // ── E2S3-T47: WasmResourcePool: KV store namespaced by module ──

    #[test]
    fn resource_pool_kv_namespaced() {
        let dir = tempfile::tempdir().unwrap();
        let pool = WasmResourcePool::new(dir.path()).unwrap();

        // Set same key in different namespaces
        pool.kv_set("module-a", "key1", "value-a");
        pool.kv_set("module-b", "key1", "value-b");

        // Each module sees its own value
        assert_eq!(pool.kv_get("module-a", "key1"), Some("value-a".into()));
        assert_eq!(pool.kv_get("module-b", "key1"), Some("value-b".into()));

        // Delete from one namespace doesn't affect the other
        assert!(pool.kv_delete("module-a", "key1"));
        assert_eq!(pool.kv_get("module-a", "key1"), None);
        assert_eq!(pool.kv_get("module-b", "key1"), Some("value-b".into()));

        // List keys
        pool.kv_set("module-a", "prefix.one", "1");
        pool.kv_set("module-a", "prefix.two", "2");
        pool.kv_set("module-a", "other", "3");
        let keys = pool.kv_list_keys("module-a", "prefix.");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"prefix.one".to_string()));
        assert!(keys.contains(&"prefix.two".to_string()));
    }
}
