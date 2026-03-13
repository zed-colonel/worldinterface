//! Pure transform node types for FlowSpec.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A pure transform node with no side effects. Reshapes data between
/// boundary crossings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransformNode {
    /// The type of transform to apply.
    pub transform: TransformType,
    /// Transform-specific input configuration.
    #[serde(default = "default_input")]
    pub input: Value,
}

fn default_input() -> Value {
    Value::Object(serde_json::Map::new())
}

/// Available transform types. This is a closed set in v1.0-alpha.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformType {
    /// Passthrough — forwards input unchanged. Useful for testing.
    Identity,
    /// Rename and restructure fields via explicit mappings.
    FieldMapping(FieldMappingSpec),
}

/// Specification for field-level data mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldMappingSpec {
    pub mappings: Vec<FieldMapping>,
}

/// A single field mapping from a source path to a target path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldMapping {
    /// Source path in dot-notation (e.g., "body.data.items").
    pub from: String,
    /// Target path in dot-notation.
    pub to: String,
}
