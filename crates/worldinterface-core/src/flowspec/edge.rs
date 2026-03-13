//! Edge types for FlowSpec graphs.

use serde::{Deserialize, Serialize};

use crate::id::NodeId;

/// A directed edge between two nodes in a FlowSpec graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    /// Source node.
    pub from: NodeId,
    /// Target node.
    pub to: NodeId,
    /// Optional condition on this edge (used with branch nodes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<EdgeCondition>,
}

/// Condition under which an edge is taken.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeCondition {
    /// Edge taken when branch evaluates true.
    BranchTrue,
    /// Edge taken when branch evaluates false.
    BranchFalse,
    /// Unconditional edge (always taken).
    Always,
}
