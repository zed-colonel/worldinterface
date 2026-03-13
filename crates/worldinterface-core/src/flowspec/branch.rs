//! Branch (conditional routing) node types for FlowSpec.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::id::NodeId;

/// A conditional routing node. Evaluates a condition and directs flow
/// to one of two target nodes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchNode {
    /// The condition to evaluate.
    pub condition: BranchCondition,
    /// Target node when condition is true.
    pub then_edge: NodeId,
    /// Target node when condition is false. None means the false branch
    /// is skipped (flow continues past the branch).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub else_edge: Option<NodeId>,
}

/// A condition that determines branch routing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchCondition {
    /// A runtime-evaluated expression string.
    Expression(String),
    /// Equality comparison between a referenced value and a literal.
    Equals { left: ParamRef, right: Value },
    /// True if the referenced value is non-null.
    Exists(ParamRef),
}

/// A reference to a value available at runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParamRef {
    /// Reference to a specific node's output at a JSON path.
    NodeOutput {
        node_id: NodeId,
        /// Dot-notation path into the node's output value.
        path: String,
    },
    /// Reference to a flow-level parameter at a JSON path.
    FlowParam {
        /// Dot-notation path into `FlowSpec.params`.
        path: String,
    },
}
