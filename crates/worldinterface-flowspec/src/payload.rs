//! Payload types for Coordinator and Step tasks.
//!
//! These are serialized as JSON and stored in AQ `TaskPayload`. The multiplexed
//! handler (Sprint 4) uses `TaskType` to route execution.

use std::collections::HashMap;

use actionqueue_core::ids::TaskId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use worldinterface_core::flowspec::{FlowSpec, NodeType};
use worldinterface_core::id::{FlowRunId, NodeId};

/// Discriminator used by the multiplexed handler to route execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    Coordinator,
    Step,
}

/// Payload for the Coordinator task. Contains everything needed to
/// orchestrate a FlowRun.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorPayload {
    /// Discriminator for the multiplexed handler.
    pub task_type: TaskType,
    /// The FlowSpec being executed (immutable after compilation).
    pub flow_spec: FlowSpec,
    /// The assigned FlowRunId.
    pub flow_run_id: FlowRunId,
    /// Pre-computed NodeId -> TaskId mapping.
    pub node_task_map: HashMap<NodeId, TaskId>,
    /// Pre-computed dependency declarations.
    pub dependencies: HashMap<TaskId, Vec<TaskId>>,
}

/// Payload for a Step task. Contains everything a Step handler needs to
/// invoke a connector or execute a transform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepPayload {
    /// Discriminator for the multiplexed handler.
    pub task_type: TaskType,
    /// The FlowRunId this step belongs to.
    pub flow_run_id: FlowRunId,
    /// Which node this step executes.
    pub node_id: NodeId,
    /// The node's type-specific configuration.
    pub node_type: NodeType,
    /// Flow-level parameters (from FlowSpec.params).
    pub flow_params: Option<Value>,
}

/// JSON content type used for all payloads.
pub const PAYLOAD_CONTENT_TYPE: &str = "application/json";

#[cfg(test)]
mod tests {
    use serde_json::json;
    use worldinterface_core::flowspec::*;

    use super::*;

    fn sample_flow_spec() -> FlowSpec {
        let id = NodeId::new();
        FlowSpec {
            id: None,
            name: Some("test".into()),
            nodes: vec![Node {
                id,
                label: None,
                node_type: NodeType::Connector(ConnectorNode {
                    connector: "http.request".into(),
                    params: json!({"url": "https://example.com"}),
                    idempotency_config: None,
                }),
            }],
            edges: vec![],
            params: Some(json!({"timeout": 30})),
        }
    }

    #[test]
    fn coordinator_payload_roundtrip() {
        let spec = sample_flow_spec();
        let payload = CoordinatorPayload {
            task_type: TaskType::Coordinator,
            flow_spec: spec.clone(),
            flow_run_id: FlowRunId::new(),
            node_task_map: HashMap::new(),
            dependencies: HashMap::new(),
        };
        let json = serde_json::to_string(&payload).unwrap();
        let back: CoordinatorPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_type, TaskType::Coordinator);
        assert_eq!(back.flow_spec, spec);
    }

    #[test]
    fn step_payload_roundtrip() {
        let node_id = NodeId::new();
        let node_type = NodeType::Connector(ConnectorNode {
            connector: "http.request".into(),
            params: json!({}),
            idempotency_config: None,
        });
        let payload = StepPayload {
            task_type: TaskType::Step,
            flow_run_id: FlowRunId::new(),
            node_id,
            node_type: node_type.clone(),
            flow_params: Some(json!({"key": "value"})),
        };
        let json = serde_json::to_string(&payload).unwrap();
        let back: StepPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(back.task_type, TaskType::Step);
        assert_eq!(back.node_id, node_id);
        assert_eq!(back.node_type, node_type);
        assert_eq!(back.flow_params, Some(json!({"key": "value"})));
    }

    #[test]
    fn coordinator_payload_contains_task_type() {
        let payload = CoordinatorPayload {
            task_type: TaskType::Coordinator,
            flow_spec: sample_flow_spec(),
            flow_run_id: FlowRunId::new(),
            node_task_map: HashMap::new(),
            dependencies: HashMap::new(),
        };
        let value: serde_json::Value = serde_json::to_value(&payload).unwrap();
        assert_eq!(value["task_type"], "coordinator");
    }

    #[test]
    fn step_payload_contains_task_type() {
        let payload = StepPayload {
            task_type: TaskType::Step,
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            node_type: NodeType::Transform(TransformNode {
                transform: TransformType::Identity,
                input: json!({}),
            }),
            flow_params: None,
        };
        let value: serde_json::Value = serde_json::to_value(&payload).unwrap();
        assert_eq!(value["task_type"], "step");
    }

    #[test]
    fn step_payload_contains_node_type() {
        // Connector
        let payload = StepPayload {
            task_type: TaskType::Step,
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            node_type: NodeType::Connector(ConnectorNode {
                connector: "test".into(),
                params: json!({}),
                idempotency_config: None,
            }),
            flow_params: None,
        };
        let value: serde_json::Value = serde_json::to_value(&payload).unwrap();
        assert!(value["node_type"]["connector"].is_object());

        // Transform
        let payload = StepPayload {
            task_type: TaskType::Step,
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            node_type: NodeType::Transform(TransformNode {
                transform: TransformType::Identity,
                input: json!({}),
            }),
            flow_params: None,
        };
        let value: serde_json::Value = serde_json::to_value(&payload).unwrap();
        assert!(value["node_type"]["transform"].is_object());

        // Branch
        let target = NodeId::new();
        let payload = StepPayload {
            task_type: TaskType::Step,
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            node_type: NodeType::Branch(BranchNode {
                condition: BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                then_edge: target,
                else_edge: None,
            }),
            flow_params: None,
        };
        let value: serde_json::Value = serde_json::to_value(&payload).unwrap();
        assert!(value["node_type"]["branch"].is_object());
    }

    #[test]
    fn coordinator_payload_contains_full_spec() {
        let spec = sample_flow_spec();
        let payload = CoordinatorPayload {
            task_type: TaskType::Coordinator,
            flow_spec: spec.clone(),
            flow_run_id: FlowRunId::new(),
            node_task_map: HashMap::new(),
            dependencies: HashMap::new(),
        };
        let json = serde_json::to_string(&payload).unwrap();
        let back: CoordinatorPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(back.flow_spec, spec);
    }
}
