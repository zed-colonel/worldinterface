//! FlowSpec — the declarative workflow graph model.
//!
//! A `FlowSpec` is the primary input to WorldInterface. It describes a
//! directed acyclic graph of nodes (connectors, transforms, branches) connected
//! by edges. FlowSpecs can be ephemeral (agent-submitted JSON) or named
//! (persisted, reusable).

pub mod branch;
pub mod connector;
pub mod edge;
pub mod topo;
pub mod transform;
pub mod validate;

pub use branch::{BranchCondition, BranchNode, ParamRef};
pub use connector::{ConnectorNode, IdempotencyConfig, IdempotencyStrategy};
pub use edge::{Edge, EdgeCondition};
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub use transform::{FieldMapping, FieldMappingSpec, TransformNode, TransformType};
pub use validate::{ValidationDiagnostic, ValidationError, ValidationRule};

use crate::id::{FlowId, NodeId};

/// A declarative workflow graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowSpec {
    /// Flow identity. `None` for ephemeral (agent-submitted) flows; the system
    /// assigns an ID at submission time if not provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<FlowId>,
    /// Human-readable name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Ordered list of nodes in the graph.
    pub nodes: Vec<Node>,
    /// Directed edges between nodes.
    pub edges: Vec<Edge>,
    /// Flow-level parameters available to all nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl FlowSpec {
    /// Validate this FlowSpec, returning all validation errors found.
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate::validate(self)
    }
}

/// A node in a FlowSpec graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Unique identity of this node within the FlowSpec.
    pub id: NodeId,
    /// Human-readable label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The node's type and type-specific configuration.
    pub node_type: NodeType,
}

/// The type of a node, determining its behavior.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    /// A boundary-crossing node that invokes an external connector.
    Connector(ConnectorNode),
    /// A pure transform node with no side effects.
    Transform(TransformNode),
    /// A conditional routing node.
    Branch(BranchNode),
}
