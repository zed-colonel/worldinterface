//! Core domain types for WorldInterface.
//!
//! This crate defines the foundational types shared across all WI crates:
//! - Identity types ([`id::FlowId`], [`id::FlowRunId`], [`id::NodeId`], [`id::StepRunId`])
//! - FlowSpec model (declarative workflow graph) — [`flowspec::FlowSpec`]
//! - Connector descriptors — [`descriptor::Descriptor`]
//! - Receipt artifacts — [`receipt::Receipt`]
//!
//! `worldinterface-core` has no dependency on ActionQueue. It is a pure domain model crate.

pub mod descriptor;
pub mod flowspec;
pub mod id;
pub mod metrics;
pub mod receipt;
pub mod streaming;

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use crate::flowspec::*;
    use crate::id::*;

    fn sample_linear_flow() -> FlowSpec {
        let ids: Vec<NodeId> = (0..3).map(|_| NodeId::new()).collect();
        FlowSpec {
            id: Some(FlowId::new()),
            name: Some("test-pipeline".into()),
            nodes: vec![
                Node {
                    id: ids[0],
                    label: Some("Fetch data".into()),
                    node_type: NodeType::Connector(ConnectorNode {
                        connector: "http.request".into(),
                        params: json!({"url": "https://example.com", "method": "GET"}),
                        idempotency_config: None,
                    }),
                },
                Node {
                    id: ids[1],
                    label: Some("Transform".into()),
                    node_type: NodeType::Transform(TransformNode {
                        transform: TransformType::FieldMapping(FieldMappingSpec {
                            mappings: vec![FieldMapping {
                                from: "body.data".into(),
                                to: "result".into(),
                            }],
                        }),
                        input: json!({}),
                    }),
                },
                Node {
                    id: ids[2],
                    label: Some("Save".into()),
                    node_type: NodeType::Connector(ConnectorNode {
                        connector: "fs.write".into(),
                        params: json!({"path": "/tmp/out.json"}),
                        idempotency_config: Some(IdempotencyConfig {
                            strategy: IdempotencyStrategy::RunId,
                        }),
                    }),
                },
            ],
            edges: vec![
                Edge { from: ids[0], to: ids[1], condition: None },
                Edge { from: ids[1], to: ids[2], condition: None },
            ],
            params: Some(json!({"timeout": 30})),
        }
    }

    fn sample_branch_flow() -> FlowSpec {
        let ids: Vec<NodeId> = (0..4).map(|_| NodeId::new()).collect();
        FlowSpec {
            id: None,
            name: None,
            nodes: vec![
                Node {
                    id: ids[0],
                    label: None,
                    node_type: NodeType::Connector(ConnectorNode {
                        connector: "http.request".into(),
                        params: json!({}),
                        idempotency_config: None,
                    }),
                },
                Node {
                    id: ids[1],
                    label: None,
                    node_type: NodeType::Branch(BranchNode {
                        condition: BranchCondition::Equals {
                            left: ParamRef::NodeOutput { node_id: ids[0], path: "status".into() },
                            right: json!(200),
                        },
                        then_edge: ids[2],
                        else_edge: Some(ids[3]),
                    }),
                },
                Node {
                    id: ids[2],
                    label: None,
                    node_type: NodeType::Transform(TransformNode {
                        transform: TransformType::Identity,
                        input: json!({}),
                    }),
                },
                Node {
                    id: ids[3],
                    label: None,
                    node_type: NodeType::Connector(ConnectorNode {
                        connector: "delay".into(),
                        params: json!({"duration_ms": 1000}),
                        idempotency_config: None,
                    }),
                },
            ],
            edges: vec![
                Edge { from: ids[0], to: ids[1], condition: None },
                Edge { from: ids[1], to: ids[2], condition: Some(EdgeCondition::BranchTrue) },
                Edge { from: ids[1], to: ids[3], condition: Some(EdgeCondition::BranchFalse) },
            ],
            params: None,
        }
    }

    #[test]
    fn flowspec_json_roundtrip_full() {
        let spec = sample_linear_flow();
        let json = serde_json::to_string_pretty(&spec).unwrap();
        let back: FlowSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn flowspec_json_roundtrip_branch() {
        let spec = sample_branch_flow();
        let json = serde_json::to_string_pretty(&spec).unwrap();
        let back: FlowSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn flowspec_yaml_roundtrip() {
        let spec = sample_linear_flow();
        let yaml = serde_yaml::to_string(&spec).unwrap();
        let back: FlowSpec = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn flowspec_minimal_single_node() {
        let id = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![Node {
                id,
                label: None,
                node_type: NodeType::Connector(ConnectorNode {
                    connector: "delay".into(),
                    params: json!({}),
                    idempotency_config: None,
                }),
            }],
            edges: vec![],
            params: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: FlowSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn flowspec_ephemeral_has_no_id() {
        let spec = sample_branch_flow();
        assert!(spec.id.is_none());
        assert!(spec.name.is_none());
        // Top-level FlowSpec should not have "id" or "name" keys when None.
        // (Node objects have their own "id" field, so we check the top-level
        // JSON object keys directly.)
        let json_value: serde_json::Value = serde_json::to_value(&spec).unwrap();
        let obj = json_value.as_object().unwrap();
        assert!(!obj.contains_key("id"));
        assert!(!obj.contains_key("name"));
    }

    #[test]
    fn flowspec_named_has_id() {
        let spec = sample_linear_flow();
        assert!(spec.id.is_some());
        assert!(spec.name.is_some());
    }

    #[test]
    fn flowspec_optional_fields_none_survive_roundtrip() {
        let id = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![Node {
                id,
                label: None,
                node_type: NodeType::Connector(ConnectorNode {
                    connector: "delay".into(),
                    params: json!({}),
                    idempotency_config: None,
                }),
            }],
            edges: vec![],
            params: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: FlowSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, None);
        assert_eq!(back.name, None);
        assert_eq!(back.params, None);
        assert_eq!(back.nodes[0].label, None);
    }

    #[test]
    fn flowspec_ignores_unknown_fields() {
        // H-1: Forward compatibility — unknown fields should be ignored
        let json = r#"{
            "nodes": [{
                "id": "00000000-0000-0000-0000-000000000001",
                "node_type": {"connector": {"connector": "delay", "params": {}}},
                "future_field": "should be ignored"
            }],
            "edges": [],
            "also_unknown": true
        }"#;
        let spec: FlowSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.nodes.len(), 1);
    }

    // --- T-6: Property-based tests ---

    /// Build a valid linear FlowSpec with `n` connector nodes.
    fn linear_flow(n: usize) -> FlowSpec {
        let ids: Vec<NodeId> = (0..n).map(|_| NodeId::new()).collect();
        let nodes = ids
            .iter()
            .map(|&id| Node {
                id,
                label: None,
                node_type: NodeType::Connector(ConnectorNode {
                    connector: "test".into(),
                    params: json!({}),
                    idempotency_config: None,
                }),
            })
            .collect();
        let edges =
            ids.windows(2).map(|w| Edge { from: w[0], to: w[1], condition: None }).collect();
        FlowSpec { id: None, name: None, nodes, edges, params: None }
    }

    proptest! {
        #[test]
        fn validation_is_deterministic(n in 1usize..=20) {
            let spec = linear_flow(n);
            let r1 = spec.validate();
            let r2 = spec.validate();
            prop_assert!(r1.is_ok(), "first call failed");
            prop_assert!(r2.is_ok(), "second call failed");
        }

        #[test]
        fn valid_spec_survives_json_roundtrip(n in 1usize..=20) {
            let spec = linear_flow(n);
            prop_assert!(spec.validate().is_ok());
            let json_str = serde_json::to_string(&spec).unwrap();
            let back: FlowSpec = serde_json::from_str(&json_str).unwrap();
            prop_assert_eq!(&spec, &back);
            prop_assert!(back.validate().is_ok());
        }

        #[test]
        fn generated_specs_have_unique_node_ids(n in 1usize..=20) {
            let spec = linear_flow(n);
            let result = spec.validate();
            prop_assert!(result.is_ok(), "linear flow with {} nodes should be valid", n);
        }
    }
}
