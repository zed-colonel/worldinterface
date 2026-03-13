//! Atomic durable store for WorldInterface node outputs.
//!
//! ContextStore provides write-once, read-many storage for flow node outputs,
//! keyed by `(FlowRunId, NodeId)`. It enforces the atomic write-before-complete
//! discipline required by the Invariant Boundaries Policy.

pub mod atomic;
pub mod config;
pub mod error;
pub mod sqlite;
pub mod store;

pub use atomic::AtomicWriter;
pub use config::ContextStoreConfig;
pub use error::{AtomicWriteError, ContextStoreError};
pub use sqlite::SqliteContextStore;
pub use store::ContextStore;

#[cfg(test)]
mod proptest_tests {
    use proptest::prelude::*;
    use serde_json::Value;
    use worldinterface_core::id::{FlowRunId, NodeId};

    use super::*;

    /// Generate arbitrary JSON values for property-based tests.
    fn arb_json_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| Value::Number(n.into())),
            "[a-zA-Z0-9 ]{0,100}".prop_map(Value::String),
            prop::collection::vec(
                prop_oneof![
                    Just(Value::Null),
                    any::<bool>().prop_map(Value::Bool),
                    any::<i64>().prop_map(|n| Value::Number(n.into())),
                    "[a-zA-Z0-9]{0,20}".prop_map(Value::String),
                ],
                0..5
            )
            .prop_map(Value::Array),
        ]
    }

    proptest! {
        #[test]
        fn any_json_value_roundtrips(val in arb_json_value()) {
            let store = SqliteContextStore::in_memory().unwrap();
            let fr = FlowRunId::new();
            let n = NodeId::new();

            store.put(fr, n, &val).unwrap();
            let got = store.get(fr, n).unwrap().unwrap();
            prop_assert_eq!(val, got);
        }

        #[test]
        fn write_once_is_enforced(_val1 in arb_json_value(), _val2 in arb_json_value()) {
            let store = SqliteContextStore::in_memory().unwrap();
            let fr = FlowRunId::new();
            let n = NodeId::new();

            store.put(fr, n, &_val1).unwrap();
            let result = store.put(fr, n, &_val2);
            let is_already_exists = matches!(result, Err(ContextStoreError::AlreadyExists { .. }));
            prop_assert!(is_already_exists, "expected AlreadyExists error");
        }

        #[test]
        fn list_keys_matches_puts(count in 1usize..10) {
            let store = SqliteContextStore::in_memory().unwrap();
            let fr = FlowRunId::new();
            let mut expected_nodes = std::collections::HashSet::new();

            for _ in 0..count {
                let n = NodeId::new();
                store.put(fr, n, &serde_json::json!(null)).unwrap();
                expected_nodes.insert(n);
            }

            let keys: std::collections::HashSet<NodeId> =
                store.list_keys(fr).unwrap().into_iter().collect();
            prop_assert_eq!(expected_nodes, keys);
        }
    }
}
