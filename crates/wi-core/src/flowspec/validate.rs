//! FlowSpec validation.
//!
//! Validation is a pure function that checks a FlowSpec for structural
//! correctness. It collects all errors rather than failing on the first one.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::flowspec::branch::{BranchCondition, ParamRef};
use crate::flowspec::topo;
use crate::flowspec::{FlowSpec, NodeType};
use crate::id::NodeId;

/// Validate a FlowSpec, returning all errors found.
pub fn validate(spec: &FlowSpec) -> Result<(), ValidationError> {
    let mut diagnostics = Vec::new();

    // V-6: Must have at least one node
    if spec.nodes.is_empty() {
        diagnostics.push(ValidationDiagnostic {
            rule: ValidationRule::NoNodes,
            node_id: None,
            message: "FlowSpec must have at least one node".into(),
        });
        return Err(ValidationError { errors: diagnostics });
    }

    // V-1: Build node index and detect duplicate IDs
    let mut node_index: HashMap<NodeId, usize> = HashMap::new();
    for (i, node) in spec.nodes.iter().enumerate() {
        if let Some(_prev) = node_index.insert(node.id, i) {
            diagnostics.push(ValidationDiagnostic {
                rule: ValidationRule::DuplicateNodeId,
                node_id: Some(node.id),
                message: format!("Duplicate node ID: {}", node.id),
            });
        }
    }

    // V-10: Empty connector names
    for node in &spec.nodes {
        if let NodeType::Connector(ref c) = node.node_type {
            if c.connector.is_empty() {
                diagnostics.push(ValidationDiagnostic {
                    rule: ValidationRule::EmptyConnectorName,
                    node_id: Some(node.id),
                    message: format!("Node {} has an empty connector name", node.id),
                });
            }
        }
    }

    // V-2, V-3, V-11: Validate edges
    for edge in &spec.edges {
        if !node_index.contains_key(&edge.from) {
            diagnostics.push(ValidationDiagnostic {
                rule: ValidationRule::DanglingEdgeSource,
                node_id: Some(edge.from),
                message: format!("Edge source {} does not reference an existing node", edge.from),
            });
        }
        if !node_index.contains_key(&edge.to) {
            diagnostics.push(ValidationDiagnostic {
                rule: ValidationRule::DanglingEdgeTarget,
                node_id: Some(edge.to),
                message: format!("Edge target {} does not reference an existing node", edge.to),
            });
        }
        if edge.from == edge.to {
            diagnostics.push(ValidationDiagnostic {
                rule: ValidationRule::SelfLoop,
                node_id: Some(edge.from),
                message: format!("Edge creates a self-loop on node {}", edge.from),
            });
        }
    }

    // V-7, V-8: Validate branch node targets
    let edge_set: HashSet<(NodeId, NodeId)> = spec.edges.iter().map(|e| (e.from, e.to)).collect();

    for node in &spec.nodes {
        if let NodeType::Branch(ref branch) = node.node_type {
            // V-7: then_edge must reference an existing node
            if !node_index.contains_key(&branch.then_edge) {
                diagnostics.push(ValidationDiagnostic {
                    rule: ValidationRule::BranchTargetMissing,
                    node_id: Some(node.id),
                    message: format!(
                        "Branch node {} then_edge references nonexistent node {}",
                        node.id, branch.then_edge
                    ),
                });
            } else {
                // V-8: must have a corresponding edge
                if !edge_set.contains(&(node.id, branch.then_edge)) {
                    diagnostics.push(ValidationDiagnostic {
                        rule: ValidationRule::BranchTargetNotInEdges,
                        node_id: Some(node.id),
                        message: format!(
                            "Branch node {} then_edge {} has no corresponding edge",
                            node.id, branch.then_edge
                        ),
                    });
                }
            }

            if let Some(else_target) = branch.else_edge {
                if !node_index.contains_key(&else_target) {
                    diagnostics.push(ValidationDiagnostic {
                        rule: ValidationRule::BranchTargetMissing,
                        node_id: Some(node.id),
                        message: format!(
                            "Branch node {} else_edge references nonexistent node {}",
                            node.id, else_target
                        ),
                    });
                } else if !edge_set.contains(&(node.id, else_target)) {
                    diagnostics.push(ValidationDiagnostic {
                        rule: ValidationRule::BranchTargetNotInEdges,
                        node_id: Some(node.id),
                        message: format!(
                            "Branch node {} else_edge {} has no corresponding edge",
                            node.id, else_target
                        ),
                    });
                }
            }
        }
    }

    // V-12: ParamRef::NodeOutput references must point to existing nodes
    for node in &spec.nodes {
        if let NodeType::Branch(ref branch) = node.node_type {
            let refs = match &branch.condition {
                BranchCondition::Exists(param_ref) => vec![param_ref],
                BranchCondition::Equals { left, .. } => vec![left],
                BranchCondition::Expression(_) => vec![],
            };
            for param_ref in refs {
                if let ParamRef::NodeOutput { node_id, .. } = param_ref {
                    if !node_index.contains_key(node_id) {
                        diagnostics.push(ValidationDiagnostic {
                            rule: ValidationRule::InvalidParamRef,
                            node_id: Some(node.id),
                            message: format!(
                                "Branch node {} references nonexistent node {} in condition",
                                node.id, node_id
                            ),
                        });
                    }
                }
            }
        }
    }

    // Build adjacency structures for root and reachability analysis
    let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
    let mut successors: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for node in &spec.nodes {
        in_degree.entry(node.id).or_insert(0);
        successors.entry(node.id).or_default();
    }
    for edge in &spec.edges {
        if node_index.contains_key(&edge.from) && node_index.contains_key(&edge.to) {
            *in_degree.entry(edge.to).or_insert(0) += 1;
            successors.entry(edge.from).or_default().push(edge.to);
        }
    }

    // V-9: Multiple roots (nodes with in-degree 0)
    let roots: Vec<NodeId> =
        in_degree.iter().filter(|(_, &deg)| deg == 0).map(|(&id, _)| id).collect();

    if roots.len() > 1 {
        diagnostics.push(ValidationDiagnostic {
            rule: ValidationRule::MultipleRoots,
            node_id: None,
            message: format!(
                "FlowSpec has {} root nodes (expected at most 1): {}",
                roots.len(),
                roots.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ")
            ),
        });
    }

    // V-4: Cycle detection via topological sort
    if let Err(remaining) = topo::topological_sort(spec) {
        diagnostics.push(ValidationDiagnostic {
            rule: ValidationRule::CycleDetected,
            node_id: None,
            message: format!(
                "FlowSpec contains a cycle ({} of {} nodes could not be topologically sorted)",
                remaining.len(),
                spec.nodes.len()
            ),
        });
    }

    // V-5: Disconnected nodes (not reachable from any root)
    // BFS from all roots
    if !roots.is_empty() {
        let mut reachable: HashSet<NodeId> = HashSet::new();
        let mut bfs_queue: VecDeque<NodeId> = roots.iter().copied().collect();
        while let Some(node_id) = bfs_queue.pop_front() {
            if reachable.insert(node_id) {
                if let Some(succs) = successors.get(&node_id) {
                    for &succ in succs {
                        if !reachable.contains(&succ) {
                            bfs_queue.push_back(succ);
                        }
                    }
                }
            }
        }

        for node in &spec.nodes {
            if !reachable.contains(&node.id) {
                diagnostics.push(ValidationDiagnostic {
                    rule: ValidationRule::DisconnectedNode,
                    node_id: Some(node.id),
                    message: format!("Node {} is not reachable from any root", node.id),
                });
            }
        }
    }

    if diagnostics.is_empty() {
        Ok(())
    } else {
        Err(ValidationError { errors: diagnostics })
    }
}

/// All validation errors found in a FlowSpec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub errors: Vec<ValidationDiagnostic>,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "FlowSpec validation failed with {} error(s):", self.errors.len())?;
        for diag in &self.errors {
            writeln!(f, "  - [{}] {}", diag.rule, diag.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationError {}

impl ValidationError {
    /// Check whether a specific rule was violated.
    pub fn has_rule(&self, rule: ValidationRule) -> bool {
        self.errors.iter().any(|d| d.rule == rule)
    }
}

/// A single validation diagnostic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationDiagnostic {
    /// Which validation rule was violated.
    pub rule: ValidationRule,
    /// The node that caused the error, if applicable.
    pub node_id: Option<NodeId>,
    /// Human-readable description of the error.
    pub message: String,
}

/// Identifies which validation rule was violated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationRule {
    DuplicateNodeId,
    DanglingEdgeSource,
    DanglingEdgeTarget,
    CycleDetected,
    DisconnectedNode,
    NoNodes,
    BranchTargetMissing,
    BranchTargetNotInEdges,
    MultipleRoots,
    EmptyConnectorName,
    SelfLoop,
    InvalidParamRef,
}

impl fmt::Display for ValidationRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNodeId => write!(f, "duplicate_node_id"),
            Self::DanglingEdgeSource => write!(f, "dangling_edge_source"),
            Self::DanglingEdgeTarget => write!(f, "dangling_edge_target"),
            Self::CycleDetected => write!(f, "cycle_detected"),
            Self::DisconnectedNode => write!(f, "disconnected_node"),
            Self::NoNodes => write!(f, "no_nodes"),
            Self::BranchTargetMissing => write!(f, "branch_target_missing"),
            Self::BranchTargetNotInEdges => write!(f, "branch_target_not_in_edges"),
            Self::MultipleRoots => write!(f, "multiple_roots"),
            Self::EmptyConnectorName => write!(f, "empty_connector_name"),
            Self::SelfLoop => write!(f, "self_loop"),
            Self::InvalidParamRef => write!(f, "invalid_param_ref"),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::flowspec::*;

    fn connector_node(id: NodeId, name: &str) -> Node {
        Node {
            id,
            label: None,
            node_type: NodeType::Connector(ConnectorNode {
                connector: name.into(),
                params: json!({}),
                idempotency_config: None,
            }),
        }
    }

    fn branch_node(id: NodeId, then_edge: NodeId, else_edge: Option<NodeId>) -> Node {
        Node {
            id,
            label: None,
            node_type: NodeType::Branch(BranchNode {
                condition: BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                then_edge,
                else_edge,
            }),
        }
    }

    fn edge(from: NodeId, to: NodeId) -> Edge {
        Edge { from, to, condition: None }
    }

    fn make_ids(n: usize) -> Vec<NodeId> {
        (0..n).map(|_| NodeId::new()).collect()
    }

    #[test]
    fn valid_linear_flow() {
        let ids = make_ids(3);
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![
                connector_node(ids[0], "http.request"),
                connector_node(ids[1], "transform"),
                connector_node(ids[2], "fs.write"),
            ],
            edges: vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
            params: None,
        };
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn valid_branch_flow() {
        let ids = make_ids(4);
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![
                connector_node(ids[0], "http.request"),
                branch_node(ids[1], ids[2], Some(ids[3])),
                connector_node(ids[2], "fs.write"),
                connector_node(ids[3], "delay"),
            ],
            edges: vec![
                edge(ids[0], ids[1]),
                Edge { from: ids[1], to: ids[2], condition: Some(EdgeCondition::BranchTrue) },
                Edge { from: ids[1], to: ids[3], condition: Some(EdgeCondition::BranchFalse) },
            ],
            params: None,
        };
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn valid_single_node() {
        let id = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(id, "delay")],
            edges: vec![],
            params: None,
        };
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn reject_no_nodes() {
        let spec = FlowSpec { id: None, name: None, nodes: vec![], edges: vec![], params: None };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::NoNodes));
    }

    #[test]
    fn reject_duplicate_node_id() {
        let id = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(id, "a"), connector_node(id, "b")],
            edges: vec![],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::DuplicateNodeId));
    }

    #[test]
    fn reject_dangling_source() {
        let ids = make_ids(2);
        let phantom = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            edges: vec![edge(phantom, ids[1])],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::DanglingEdgeSource));
    }

    #[test]
    fn reject_dangling_target() {
        let ids = make_ids(2);
        let phantom = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            edges: vec![edge(ids[0], phantom)],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::DanglingEdgeTarget));
    }

    #[test]
    fn reject_cycle_simple() {
        let ids = make_ids(2);
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            edges: vec![edge(ids[0], ids[1]), edge(ids[1], ids[0])],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::CycleDetected));
    }

    #[test]
    fn reject_cycle_complex() {
        let ids = make_ids(3);
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            edges: vec![edge(ids[0], ids[1]), edge(ids[1], ids[2]), edge(ids[2], ids[0])],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::CycleDetected));
    }

    #[test]
    fn reject_self_loop() {
        let id = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(id, "a")],
            edges: vec![edge(id, id)],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::SelfLoop));
    }

    #[test]
    fn reject_disconnected_node() {
        let ids = make_ids(4);
        // A→B is the main flow (A is the sole root).
        // C→D→C forms an isolated cycle: both have in-degree > 0 (not roots)
        // but are unreachable from root A. This triggers DisconnectedNode
        // without MultipleRoots.
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
                connector_node(ids[3], "d"),
            ],
            edges: vec![edge(ids[0], ids[1]), edge(ids[2], ids[3]), edge(ids[3], ids[2])],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::DisconnectedNode));
        assert!(err.has_rule(ValidationRule::CycleDetected));
        assert!(!err.has_rule(ValidationRule::MultipleRoots));
    }

    #[test]
    fn reject_branch_target_missing() {
        let ids = make_ids(2);
        let phantom = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(ids[0], "a"), branch_node(ids[1], phantom, None)],
            edges: vec![edge(ids[0], ids[1]), edge(ids[1], phantom)],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::BranchTargetMissing));
    }

    #[test]
    fn reject_branch_not_in_edges() {
        let ids = make_ids(3);
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![
                connector_node(ids[0], "a"),
                branch_node(ids[1], ids[2], None),
                connector_node(ids[2], "b"),
            ],
            // Edge from A→Branch exists, but no edge from Branch→B
            edges: vec![edge(ids[0], ids[1])],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::BranchTargetNotInEdges));
    }

    #[test]
    fn reject_multiple_roots() {
        let ids = make_ids(2);
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            edges: vec![],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::MultipleRoots));
    }

    #[test]
    fn reject_empty_connector_name() {
        let id = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(id, "")],
            edges: vec![],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::EmptyConnectorName));
    }

    #[test]
    fn reject_invalid_param_ref() {
        let ids = make_ids(3);
        let phantom = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![
                connector_node(ids[0], "a"),
                Node {
                    id: ids[1],
                    label: None,
                    node_type: NodeType::Branch(BranchNode {
                        condition: BranchCondition::Exists(ParamRef::NodeOutput {
                            node_id: phantom,
                            path: "status".into(),
                        }),
                        then_edge: ids[2],
                        else_edge: None,
                    }),
                },
                connector_node(ids[2], "b"),
            ],
            edges: vec![
                edge(ids[0], ids[1]),
                Edge { from: ids[1], to: ids[2], condition: Some(EdgeCondition::BranchTrue) },
            ],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.has_rule(ValidationRule::InvalidParamRef));
    }

    #[test]
    fn multiple_errors_reported() {
        let id = NodeId::new();
        let phantom = NodeId::new();
        let spec = FlowSpec {
            id: None,
            name: None,
            nodes: vec![connector_node(id, "")], // V-10: empty name
            edges: vec![
                edge(id, id),      // V-11: self-loop
                edge(id, phantom), // V-3: dangling target
            ],
            params: None,
        };
        let err = spec.validate().unwrap_err();
        assert!(err.errors.len() >= 3);
        assert!(err.has_rule(ValidationRule::EmptyConnectorName));
        assert!(err.has_rule(ValidationRule::SelfLoop));
        assert!(err.has_rule(ValidationRule::DanglingEdgeTarget));
    }
}
