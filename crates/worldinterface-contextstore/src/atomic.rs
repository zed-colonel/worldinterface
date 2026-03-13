//! Atomic write-before-complete protocol.
//!
//! Enforces Invariant 2: "Step complete only after ContextStore write succeeds."
//! The [`AtomicWriter`] bridges the ContextStore with the Step handler (Sprint 4).

use std::sync::Arc;

use serde_json::Value;
use worldinterface_core::id::{FlowRunId, NodeId};

use crate::error::{AtomicWriteError, ContextStoreError};
use crate::store::ContextStore;

/// Encapsulates the atomic write-before-complete discipline.
///
/// The protocol:
/// 1. Write output to ContextStore
/// 2. If write succeeds → call the completion callback
/// 3. If write fails → do NOT call completion; the Step will be retried
/// 4. If write succeeds but completion fails → on retry, write returns
///    `AlreadyExists` → the Step knows the output is already durable and
///    can safely re-attempt completion
pub struct AtomicWriter<S: ContextStore> {
    store: Arc<S>,
}

impl<S: ContextStore> AtomicWriter<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    /// Execute the write-before-complete protocol.
    ///
    /// `complete_fn` is called only after the write succeeds. If the write
    /// returns `AlreadyExists`, this is treated as success (idempotent retry)
    /// and `complete_fn` is still called.
    ///
    /// Returns the result of `complete_fn`, or the write error if the write
    /// failed for a non-idempotent reason.
    pub fn write_and_complete<F, T>(
        &self,
        flow_run_id: FlowRunId,
        node_id: NodeId,
        value: &Value,
        complete_fn: F,
    ) -> Result<T, AtomicWriteError>
    where
        F: FnOnce() -> Result<T, Box<dyn std::error::Error + Send + Sync>>,
    {
        // Step 1: Attempt write
        match self.store.put(flow_run_id, node_id, value) {
            Ok(()) => {
                // Fresh write succeeded → proceed to completion
            }
            Err(ContextStoreError::AlreadyExists { .. }) => {
                // Idempotent retry: output already durable → proceed to completion
                tracing::info!(
                    %flow_run_id, %node_id,
                    "output already exists (idempotent retry), proceeding to completion"
                );
            }
            Err(e) => {
                // Write failed → do NOT complete
                return Err(AtomicWriteError::WriteFailed(e));
            }
        }

        // Step 2: Write succeeded (or was already present) → complete
        complete_fn().map_err(AtomicWriteError::CompletionFailed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use serde_json::json;
    use worldinterface_core::id::{FlowRunId, NodeId};

    use super::*;
    use crate::sqlite::SqliteContextStore;

    fn make_writer() -> (Arc<SqliteContextStore>, AtomicWriter<SqliteContextStore>) {
        let store = Arc::new(SqliteContextStore::in_memory().unwrap());
        let writer = AtomicWriter::new(Arc::clone(&store));
        (store, writer)
    }

    #[test]
    fn atomic_write_and_complete_success() {
        let (store, writer) = make_writer();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!("output");

        let result = writer.write_and_complete(fr, n, &val, || Ok("done"));
        assert_eq!(result.unwrap(), "done");

        // Value is readable
        assert_eq!(store.get(fr, n).unwrap().unwrap(), val);
    }

    #[test]
    fn atomic_write_already_exists_still_completes() {
        let (store, writer) = make_writer();
        let fr = FlowRunId::new();
        let n = NodeId::new();

        // Pre-write the value (simulates a previous successful write)
        store.put(fr, n, &json!("pre-existing")).unwrap();

        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = Arc::clone(&completed);

        let result = writer.write_and_complete(fr, n, &json!("new_attempt"), move || {
            completed_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        assert!(result.is_ok());
        assert!(completed.load(Ordering::SeqCst), "completion callback was not called");
    }

    #[test]
    fn atomic_write_fails_no_completion() {
        // Use a store that's been intentionally broken
        let fr = FlowRunId::new();
        let n = NodeId::new();

        struct FailingStore;
        impl ContextStore for FailingStore {
            fn put(&self, _: FlowRunId, _: NodeId, _: &Value) -> Result<(), ContextStoreError> {
                Err(ContextStoreError::StorageError("disk full".to_string()))
            }
            fn get(&self, _: FlowRunId, _: NodeId) -> Result<Option<Value>, ContextStoreError> {
                unreachable!()
            }
            fn list_keys(&self, _: FlowRunId) -> Result<Vec<NodeId>, ContextStoreError> {
                unreachable!()
            }
            fn put_global(&self, _: &str, _: &Value) -> Result<(), ContextStoreError> {
                unreachable!()
            }
            fn upsert_global(&self, _: &str, _: &Value) -> Result<(), ContextStoreError> {
                unreachable!()
            }
            fn get_global(&self, _: &str) -> Result<Option<Value>, ContextStoreError> {
                unreachable!()
            }
        }

        let failing_writer = AtomicWriter::new(Arc::new(FailingStore));
        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = Arc::clone(&completed);

        let result = failing_writer.write_and_complete(fr, n, &json!("val"), move || {
            completed_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), AtomicWriteError::WriteFailed(_)),
            "expected WriteFailed"
        );
        assert!(
            !completed.load(Ordering::SeqCst),
            "completion callback should NOT have been called"
        );
    }

    #[test]
    fn atomic_completion_fails_value_still_durable() {
        let (store, writer) = make_writer();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!("durable_even_if_complete_fails");

        let result: Result<(), AtomicWriteError> =
            writer.write_and_complete(fr, n, &val, || Err("completion crashed".into()));

        // Error is CompletionFailed
        assert!(matches!(result.unwrap_err(), AtomicWriteError::CompletionFailed(_)));

        // But the value IS in the store
        assert_eq!(store.get(fr, n).unwrap().unwrap(), val);
    }

    #[test]
    fn atomic_retry_after_completion_failure() {
        let (store, writer) = make_writer();
        let fr = FlowRunId::new();
        let n = NodeId::new();
        let val = json!("retry_test");

        // First attempt: write succeeds, completion fails
        let result: Result<(), AtomicWriteError> =
            writer.write_and_complete(fr, n, &val, || Err("oops".into()));
        assert!(matches!(result.unwrap_err(), AtomicWriteError::CompletionFailed(_)));

        // Second attempt (retry): write returns AlreadyExists → completion retried
        let result = writer.write_and_complete(fr, n, &val, || Ok("completed on retry"));
        assert_eq!(result.unwrap(), "completed on retry");

        // Original value still in store
        assert_eq!(store.get(fr, n).unwrap().unwrap(), val);
    }
}
