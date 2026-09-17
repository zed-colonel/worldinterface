//! SQLite-backed ContextStore implementation.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::Connection;
use serde_json::Value;
use uuid::Uuid;
use worldinterface_core::id::{FlowRunId, NodeId};

use crate::error::ContextStoreError;
use crate::store::ContextStore;

/// SQLite-backed ContextStore implementation.
///
/// Uses WAL mode for concurrent read access and a single `Mutex<Connection>`
/// for thread-safe write access. All data is stored in two STRICT tables:
/// `outputs` for node outputs and `globals` for global key-value pairs.
pub struct SqliteContextStore {
    conn: Mutex<Connection>,
}

impl SqliteContextStore {
    /// Open or create a ContextStore at the given path.
    ///
    /// Creates the database file and tables if they don't exist.
    /// Enables WAL mode and sets a busy timeout for concurrent access.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ContextStoreError> {
        let conn =
            Connection::open(path).map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
        Self::initialize(conn)
    }

    /// Create an in-memory ContextStore (for testing).
    pub fn in_memory() -> Result<Self, ContextStoreError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
        Self::initialize(conn)
    }

    fn initialize(conn: Connection) -> Result<Self, ContextStoreError> {
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
        conn.pragma_update(None, "busy_timeout", 5000)
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
        // FULL synchronous ensures durability across power loss in WAL mode.
        // The default in WAL mode is NORMAL, which is adequate for most cases
        // but does not guarantee fsync after every transaction commit.
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS outputs (
                flow_run_id TEXT NOT NULL,
                node_id     TEXT NOT NULL,
                value       BLOB NOT NULL,
                written_at  TEXT NOT NULL,
                PRIMARY KEY (flow_run_id, node_id)
            ) STRICT;

            CREATE TABLE IF NOT EXISTS globals (
                key        TEXT NOT NULL PRIMARY KEY,
                value      BLOB NOT NULL,
                written_at TEXT NOT NULL
            ) STRICT;",
        )
        .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        Ok(Self { conn: Mutex::new(conn) })
    }
}

impl ContextStore for SqliteContextStore {
    fn put(
        &self,
        flow_run_id: FlowRunId,
        node_id: NodeId,
        value: &Value,
    ) -> Result<(), ContextStoreError> {
        let conn = self.conn.lock().map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
        let value_bytes = serde_json::to_vec(value)?;
        let now = chrono::Utc::now().to_rfc3339();

        match conn.execute(
            "INSERT INTO outputs (flow_run_id, node_id, value, written_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![flow_run_id.to_string(), node_id.to_string(), value_bytes, now,],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(ContextStoreError::AlreadyExists { flow_run_id, node_id })
            }
            Err(e) => Err(ContextStoreError::StorageError(e.to_string())),
        }
    }

    fn get(
        &self,
        flow_run_id: FlowRunId,
        node_id: NodeId,
    ) -> Result<Option<Value>, ContextStoreError> {
        let conn = self.conn.lock().map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        let mut stmt = conn
            .prepare("SELECT value FROM outputs WHERE flow_run_id = ?1 AND node_id = ?2")
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        let mut rows = stmt
            .query(rusqlite::params![flow_run_id.to_string(), node_id.to_string(),])
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        match rows.next().map_err(|e| ContextStoreError::StorageError(e.to_string()))? {
            Some(row) => {
                let bytes: Vec<u8> =
                    row.get(0).map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
                let value: Value = serde_json::from_slice(&bytes)
                    .map_err(|e| ContextStoreError::DeserializationFailed { source: e })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn list_keys(&self, flow_run_id: FlowRunId) -> Result<Vec<NodeId>, ContextStoreError> {
        let conn = self.conn.lock().map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        let mut stmt = conn
            .prepare("SELECT node_id FROM outputs WHERE flow_run_id = ?1")
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        let rows = stmt
            .query_map(rusqlite::params![flow_run_id.to_string()], |row| {
                let node_id_str: String = row.get(0)?;
                Ok(node_id_str)
            })
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        let mut keys = Vec::new();
        for row in rows {
            let node_id_str = row.map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
            let uuid = Uuid::parse_str(&node_id_str)
                .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
            keys.push(NodeId::from(uuid));
        }
        Ok(keys)
    }

    fn put_global(&self, key: &str, value: &Value) -> Result<(), ContextStoreError> {
        let conn = self.conn.lock().map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
        let value_bytes = serde_json::to_vec(value)?;
        let now = chrono::Utc::now().to_rfc3339();

        match conn.execute(
            "INSERT INTO globals (key, value, written_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![key, value_bytes, now],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(ContextStoreError::GlobalAlreadyExists { key: key.to_string() })
            }
            Err(e) => Err(ContextStoreError::StorageError(e.to_string())),
        }
    }

    fn upsert_global(&self, key: &str, value: &Value) -> Result<(), ContextStoreError> {
        let conn = self.conn.lock().map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
        let value_bytes = serde_json::to_vec(value)?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO globals (key, value, written_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, written_at = \
             excluded.written_at",
            rusqlite::params![key, value_bytes, now],
        )
        .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        Ok(())
    }

    fn get_global(&self, key: &str) -> Result<Option<Value>, ContextStoreError> {
        let conn = self.conn.lock().map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        let mut stmt = conn
            .prepare("SELECT value FROM globals WHERE key = ?1")
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        let mut rows = stmt
            .query(rusqlite::params![key])
            .map_err(|e| ContextStoreError::StorageError(e.to_string()))?;

        match rows.next().map_err(|e| ContextStoreError::StorageError(e.to_string()))? {
            Some(row) => {
                let bytes: Vec<u8> =
                    row.get(0).map_err(|e| ContextStoreError::StorageError(e.to_string()))?;
                let value: Value = serde_json::from_slice(&bytes)
                    .map_err(|e| ContextStoreError::DeserializationFailed { source: e })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;
    use worldinterface_core::id::{FlowRunId, NodeId};

    use super::*;
    use crate::store::ContextStore;

    // ── T-1: Write-Once Enforcement ──────────────────────────────────

    #[test]
    fn put_and_get_roundtrip() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!({"key": "value", "num": 42});

        store.put(fr, n, &val).unwrap();
        let got = store.get(fr, n).unwrap().unwrap();
        assert_eq!(val, got);
    }

    #[test]
    fn put_returns_ok_on_first_write() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        assert!(store.put(fr, n, &json!("hello")).is_ok());
    }

    #[test]
    fn put_rejects_duplicate() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();

        store.put(fr, n, &json!(1)).unwrap();
        let err = store.put(fr, n, &json!(2)).unwrap_err();
        assert!(
            matches!(err, ContextStoreError::AlreadyExists { .. }),
            "expected AlreadyExists, got: {err:?}"
        );
    }

    #[test]
    fn put_duplicate_preserves_original() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let original = json!({"original": true});
        let replacement = json!({"original": false});

        store.put(fr, n, &original).unwrap();
        let _ = store.put(fr, n, &replacement); // ignore error
        let got = store.get(fr, n).unwrap().unwrap();
        assert_eq!(original, got);
    }

    #[test]
    fn put_different_nodes_same_flow() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();

        store.put(fr, n1, &json!("a")).unwrap();
        store.put(fr, n2, &json!("b")).unwrap();
        assert_eq!(store.get(fr, n1).unwrap().unwrap(), json!("a"));
        assert_eq!(store.get(fr, n2).unwrap().unwrap(), json!("b"));
    }

    #[test]
    fn put_same_node_different_flows() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr1 = FlowRunId::new();
        let fr2 = FlowRunId::new();
        let n = NodeId::new();

        store.put(fr1, n, &json!(1)).unwrap();
        store.put(fr2, n, &json!(2)).unwrap();
        assert_eq!(store.get(fr1, n).unwrap().unwrap(), json!(1));
        assert_eq!(store.get(fr2, n).unwrap().unwrap(), json!(2));
    }

    #[test]
    fn put_complex_json_value() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!({
            "nested": {"deep": {"value": [1, 2, 3]}},
            "null_field": null,
            "float": 1.234,
            "bool": true,
            "empty_array": [],
            "empty_object": {}
        });

        store.put(fr, n, &val).unwrap();
        assert_eq!(store.get(fr, n).unwrap().unwrap(), val);
    }

    #[test]
    fn put_null_value() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();

        store.put(fr, n, &Value::Null).unwrap();
        assert_eq!(store.get(fr, n).unwrap().unwrap(), Value::Null);
    }

    // ── T-2: Read Behavior ───────────────────────────────────────────

    #[test]
    fn get_nonexistent_returns_none() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        assert_eq!(store.get(fr, n).unwrap(), None);
    }

    #[test]
    fn get_after_put_returns_some() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!("test");

        store.put(fr, n, &val).unwrap();
        assert!(store.get(fr, n).unwrap().is_some());
    }

    #[test]
    fn list_keys_empty_flow() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        assert!(store.list_keys(fr).unwrap().is_empty());
    }

    #[test]
    fn list_keys_returns_written_nodes() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let n3 = NodeId::new();

        store.put(fr, n1, &json!(1)).unwrap();
        store.put(fr, n2, &json!(2)).unwrap();
        store.put(fr, n3, &json!(3)).unwrap();

        let keys: HashSet<NodeId> = store.list_keys(fr).unwrap().into_iter().collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&n1));
        assert!(keys.contains(&n2));
        assert!(keys.contains(&n3));
    }

    #[test]
    fn list_keys_scoped_to_flow() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr1 = FlowRunId::new();
        let fr2 = FlowRunId::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();

        store.put(fr1, n1, &json!(1)).unwrap();
        store.put(fr2, n2, &json!(2)).unwrap();

        let keys1: Vec<NodeId> = store.list_keys(fr1).unwrap();
        assert_eq!(keys1.len(), 1);
        assert_eq!(keys1[0], n1);
    }

    // ── T-3: Globals ─────────────────────────────────────────────────

    #[test]
    fn global_put_and_get_roundtrip() {
        let store = SqliteContextStore::in_memory().unwrap();
        let val = json!({"global": true});

        store.put_global("my_key", &val).unwrap();
        assert_eq!(store.get_global("my_key").unwrap().unwrap(), val);
    }

    #[test]
    fn global_put_rejects_duplicate() {
        let store = SqliteContextStore::in_memory().unwrap();

        store.put_global("key", &json!(1)).unwrap();
        let err = store.put_global("key", &json!(2)).unwrap_err();
        assert!(
            matches!(err, ContextStoreError::GlobalAlreadyExists { .. }),
            "expected GlobalAlreadyExists, got: {err:?}"
        );
    }

    #[test]
    fn global_get_nonexistent_returns_none() {
        let store = SqliteContextStore::in_memory().unwrap();
        assert_eq!(store.get_global("nonexistent").unwrap(), None);
    }

    #[test]
    fn globals_independent_of_outputs() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();

        store.put(fr, n, &json!("output")).unwrap();
        store.put_global("global_key", &json!("global")).unwrap();

        // Neither affects the other
        assert_eq!(store.get(fr, n).unwrap().unwrap(), json!("output"));
        assert_eq!(store.get_global("global_key").unwrap().unwrap(), json!("global"));
    }

    // ── T-4: Upsert Globals ───────────────────────────────────────────

    #[test]
    fn upsert_global_creates_new_entry() {
        let store = SqliteContextStore::in_memory().unwrap();
        let val = json!({"version": 1});

        store.upsert_global("my_key", &val).unwrap();
        assert_eq!(store.get_global("my_key").unwrap().unwrap(), val);
    }

    #[test]
    fn upsert_global_overwrites_existing_entry() {
        let store = SqliteContextStore::in_memory().unwrap();
        let val1 = json!({"version": 1});
        let val2 = json!({"version": 2});

        store.upsert_global("my_key", &val1).unwrap();
        store.upsert_global("my_key", &val2).unwrap();
        assert_eq!(store.get_global("my_key").unwrap().unwrap(), val2);
    }

    #[test]
    fn upsert_global_and_get_global_round_trip() {
        let store = SqliteContextStore::in_memory().unwrap();

        // Create via upsert
        store.upsert_global("key1", &json!("first")).unwrap();
        assert_eq!(store.get_global("key1").unwrap().unwrap(), json!("first"));

        // Update via upsert
        store.upsert_global("key1", &json!("second")).unwrap();
        assert_eq!(store.get_global("key1").unwrap().unwrap(), json!("second"));

        // put_global still rejects duplicate
        let err = store.put_global("key1", &json!("third")).unwrap_err();
        assert!(matches!(err, ContextStoreError::GlobalAlreadyExists { .. }));

        // Value unchanged after rejected put_global
        assert_eq!(store.get_global("key1").unwrap().unwrap(), json!("second"));
    }

    // ── T-5: SQLite-Specific Behavior ────────────────────────────────

    #[test]
    fn open_creates_db_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        assert!(!path.exists());

        let _store = SqliteContextStore::open(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn open_creates_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let _store = SqliteContextStore::open(&path).unwrap();

        // Verify tables exist by opening a raw connection
        let conn = Connection::open(&path).unwrap();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"globals".to_string()));
        assert!(tables.contains(&"outputs".to_string()));
    }

    #[test]
    fn open_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        let store1 = SqliteContextStore::open(&path).unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        store1.put(fr, n, &json!("data")).unwrap();
        drop(store1);

        // Second open succeeds and data is preserved
        let store2 = SqliteContextStore::open(&path).unwrap();
        assert_eq!(store2.get(fr, n).unwrap().unwrap(), json!("data"));
    }

    #[test]
    fn wal_mode_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let store = SqliteContextStore::open(&path).unwrap();

        let conn = store.conn.lock().unwrap();
        let mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0)).unwrap();
        assert_eq!(mode, "wal");
    }

    #[test]
    fn in_memory_is_isolated() {
        let store1 = SqliteContextStore::in_memory().unwrap();
        let store2 = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();

        store1.put(fr, n, &json!("only in store1")).unwrap();
        assert_eq!(store2.get(fr, n).unwrap(), None);
    }

    // ── T-6: Crash Simulation (File-Backed) ──────────────────────────

    #[test]
    fn data_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!({"survived": true});

        {
            let store = SqliteContextStore::open(&path).unwrap();
            store.put(fr, n, &val).unwrap();
            // store dropped here — simulates clean shutdown
        }

        let store = SqliteContextStore::open(&path).unwrap();
        assert_eq!(store.get(fr, n).unwrap().unwrap(), val);
    }

    #[test]
    fn crash_after_write_before_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!("written_but_not_completed");

        {
            let store = SqliteContextStore::open(&path).unwrap();
            store.put(fr, n, &val).unwrap();
            // Simulates crash: drop without calling any completion
        }

        // After restart, data IS present (durable)
        let store = SqliteContextStore::open(&path).unwrap();
        assert_eq!(store.get(fr, n).unwrap().unwrap(), val);

        // And put returns AlreadyExists (idempotent retry path)
        let err = store.put(fr, n, &json!("retry")).unwrap_err();
        assert!(matches!(err, ContextStoreError::AlreadyExists { .. }));
    }

    #[test]
    fn crash_before_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let fr = FlowRunId::new();
        let n = NodeId::new();

        {
            let _store = SqliteContextStore::open(&path).unwrap();
            // Simulates crash before any write
        }

        let store = SqliteContextStore::open(&path).unwrap();
        assert_eq!(store.get(fr, n).unwrap(), None);
    }

    #[test]
    fn multiple_flows_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let fr1 = FlowRunId::new();
        let fr2 = FlowRunId::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();

        {
            let store = SqliteContextStore::open(&path).unwrap();
            store.put(fr1, n1, &json!("flow1")).unwrap();
            store.put(fr2, n2, &json!("flow2")).unwrap();
        }

        let store = SqliteContextStore::open(&path).unwrap();
        assert_eq!(store.get(fr1, n1).unwrap().unwrap(), json!("flow1"));
        assert_eq!(store.get(fr2, n2).unwrap().unwrap(), json!("flow2"));
    }

    // ── T-7: Edge Cases ──────────────────────────────────────────────

    #[test]
    fn empty_string_value() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!("");

        store.put(fr, n, &val).unwrap();
        assert_eq!(store.get(fr, n).unwrap().unwrap(), val);
    }

    #[test]
    fn large_value() {
        let store = SqliteContextStore::in_memory().unwrap();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        // ~1MB JSON blob
        let big_string = "x".repeat(1_000_000);
        let val = json!({"data": big_string});

        store.put(fr, n, &val).unwrap();
        assert_eq!(store.get(fr, n).unwrap().unwrap(), val);
    }

    #[test]
    fn special_characters_in_global_key() {
        let store = SqliteContextStore::in_memory().unwrap();
        let keys = [
            "key with spaces",
            "unicode: 你好世界 🌍",
            "slashes/and\\backslashes",
            "quotes\"and'apostrophes",
            "",
        ];

        for (i, key) in keys.iter().enumerate() {
            let val = json!(i);
            store.put_global(key, &val).unwrap();
            assert_eq!(store.get_global(key).unwrap().unwrap(), val);
        }
    }

    #[test]
    fn concurrent_reads_after_write() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(SqliteContextStore::in_memory().unwrap());
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!({"concurrent": true});

        store.put(fr, n, &val).unwrap();

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = Arc::clone(&store);
                let expected = val.clone();
                thread::spawn(move || {
                    let got = store.get(fr, n).unwrap().unwrap();
                    assert_eq!(got, expected);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
