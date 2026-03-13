//! Helper functions for the Host.

use std::collections::HashMap;

use actionqueue_core::ids::TaskId;
use serde_json::Value;
use worldinterface_contextstore::ContextStore;
use worldinterface_contextstore::SqliteContextStore;
use worldinterface_core::flowspec::{ConnectorNode, FlowSpec, Node, NodeType};
use worldinterface_core::id::{FlowRunId, NodeId};

use crate::error::HostError;

const COORDINATOR_MAP_KEY: &str = "host:coordinator_map";

/// Persist the coordinator map to ContextStore globals.
pub(crate) fn persist_coordinator_map(
    store: &SqliteContextStore,
    coordinator_map: &std::sync::Mutex<HashMap<FlowRunId, TaskId>>,
) -> Result<(), HostError> {
    let map = coordinator_map.lock().unwrap();
    let serialized = serde_json::to_value(
        map.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<String, String>>(),
    )?;
    store.upsert_global(COORDINATOR_MAP_KEY, &serialized)?;
    Ok(())
}

/// Restore the coordinator map from ContextStore globals.
pub(crate) fn restore_coordinator_map(
    store: &SqliteContextStore,
) -> Result<HashMap<FlowRunId, TaskId>, HostError> {
    match store.get_global(COORDINATOR_MAP_KEY)? {
        Some(value) => {
            let raw: HashMap<String, String> = serde_json::from_value(value)?;
            let mut map = HashMap::new();
            for (k, v) in raw {
                let flow_run_id = FlowRunId::from(k.parse::<uuid::Uuid>()?);
                let task_id = TaskId::from_uuid(v.parse::<uuid::Uuid>()?);
                map.insert(flow_run_id, task_id);
            }
            Ok(map)
        }
        None => Ok(HashMap::new()),
    }
}

/// Build a single-node FlowSpec for `invoke_single`.
pub(crate) fn build_single_node_flowspec(connector_name: &str, params: Value) -> FlowSpec {
    let node_id = NodeId::new();
    FlowSpec {
        id: None,
        name: Some(format!("invoke_single:{connector_name}")),
        nodes: vec![Node {
            id: node_id,
            label: Some(connector_name.to_string()),
            node_type: NodeType::Connector(ConnectorNode {
                connector: connector_name.to_string(),
                params,
                idempotency_config: None,
            }),
        }],
        edges: vec![],
        params: None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn coordinator_map_serialization_round_trip() {
        let store = SqliteContextStore::in_memory().unwrap();
        let map_mutex = std::sync::Mutex::new(HashMap::new());

        // Insert 3 entries
        let entries: Vec<(FlowRunId, TaskId)> =
            (0..3).map(|_| (FlowRunId::new(), TaskId::new())).collect();

        {
            let mut map = map_mutex.lock().unwrap();
            for (frid, tid) in &entries {
                map.insert(*frid, *tid);
            }
        }

        // Persist
        persist_coordinator_map(&store, &map_mutex).unwrap();

        // Restore
        let restored = restore_coordinator_map(&store).unwrap();
        assert_eq!(restored.len(), 3);
        for (frid, tid) in &entries {
            assert_eq!(restored.get(frid), Some(tid));
        }
    }

    #[test]
    fn coordinator_map_empty_on_fresh_store() {
        let store = SqliteContextStore::in_memory().unwrap();
        let restored = restore_coordinator_map(&store).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn coordinator_map_upsert_overwrites() {
        let store = SqliteContextStore::in_memory().unwrap();
        let map_mutex = std::sync::Mutex::new(HashMap::new());

        let frid = FlowRunId::new();
        let tid1 = TaskId::new();
        let tid2 = TaskId::new();

        // First persist
        {
            let mut map = map_mutex.lock().unwrap();
            map.insert(frid, tid1);
        }
        persist_coordinator_map(&store, &map_mutex).unwrap();

        // Second persist with additional entry
        {
            let mut map = map_mutex.lock().unwrap();
            map.insert(FlowRunId::new(), tid2);
        }
        persist_coordinator_map(&store, &map_mutex).unwrap();

        let restored = restore_coordinator_map(&store).unwrap();
        assert_eq!(restored.len(), 2);
    }

    #[test]
    fn build_single_node_flowspec_creates_valid_spec() {
        let spec = build_single_node_flowspec("delay", json!({"duration_ms": 10}));
        assert_eq!(spec.nodes.len(), 1);
        assert!(spec.edges.is_empty());
        assert_eq!(spec.name, Some("invoke_single:delay".to_string()));

        match &spec.nodes[0].node_type {
            NodeType::Connector(cn) => {
                assert_eq!(cn.connector, "delay");
                assert_eq!(cn.params, json!({"duration_ms": 10}));
            }
            _ => panic!("expected connector node"),
        }

        // Validates successfully
        spec.validate().unwrap();
    }
}
