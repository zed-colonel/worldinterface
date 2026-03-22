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

/// Available transform types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformType {
    /// Passthrough — forwards input unchanged. Useful for testing.
    Identity,
    /// Rename and restructure fields via explicit mappings.
    FieldMapping(FieldMappingSpec),
    /// Conditional pass/reject based on input value.
    Filter(FilterSpec),
    /// Template string with `{{path}}` references resolved against input.
    StringTemplate(StringTemplateSpec),
    /// Flatten nested arrays into a single array.
    ArrayFlatten {
        /// Dot-notation path to the array of arrays.
        path: String,
    },
    /// Apply a field mapping to each element of an input array.
    ArrayMap {
        /// Dot-notation path to the input array.
        path: String,
        /// Mapping to apply to each element.
        mapping: FieldMappingSpec,
    },
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

/// Conditional filter: passes input through if condition is true, returns null otherwise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterSpec {
    /// The condition to evaluate against the input.
    pub condition: FilterCondition,
}

/// Conditions for the filter transform. Operates on the transform's immediate
/// input value using simple dot-notation paths.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterCondition {
    /// True if `input[path] == value`.
    Equals { path: String, value: Value },
    /// True if `input[path]` is non-null and exists.
    Exists { path: String },
    /// True if `input[path] != value`.
    NotEquals { path: String, value: Value },
    /// True if `input[path]` is null or missing.
    NotExists { path: String },
}

/// Template string with `{{path}}` references resolved against the input JSON.
///
/// Example: `"Hello, {{user.name}}! You have {{stats.count}} items."`
/// With input `{"user": {"name": "Alice"}, "stats": {"count": 42}}`
/// Produces: `"Hello, Alice! You have 42 items."`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StringTemplateSpec {
    /// The template string containing `{{path}}` references.
    pub template: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // ── E2S1-T40: FilterSpec roundtrip ──

    #[test]
    fn filter_spec_roundtrip() {
        let spec = FilterSpec {
            condition: FilterCondition::Equals { path: "status".into(), value: json!("ok") },
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: FilterSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }

    // ── E2S1-T41: FilterCondition variants roundtrip ──

    #[test]
    fn filter_condition_variants_roundtrip() {
        let variants = vec![
            FilterCondition::Equals { path: "x".into(), value: json!(42) },
            FilterCondition::Exists { path: "key".into() },
            FilterCondition::NotEquals { path: "y".into(), value: json!("no") },
            FilterCondition::NotExists { path: "z".into() },
        ];
        for cond in variants {
            let json = serde_json::to_string(&cond).unwrap();
            let back: FilterCondition = serde_json::from_str(&json).unwrap();
            assert_eq!(cond, back);
        }
    }

    // ── E2S1-T42: StringTemplateSpec roundtrip ──

    #[test]
    fn string_template_spec_roundtrip() {
        let spec = StringTemplateSpec { template: "Hello, {{name}}!".into() };
        let json = serde_json::to_string(&spec).unwrap();
        let back: StringTemplateSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }

    // ── E2S1-T43: ArrayFlatten roundtrip ──

    #[test]
    fn array_flatten_roundtrip() {
        let tt = TransformType::ArrayFlatten { path: "data.items".into() };
        let json = serde_json::to_string(&tt).unwrap();
        let back: TransformType = serde_json::from_str(&json).unwrap();
        assert_eq!(tt, back);
    }

    // ── E2S1-T44: ArrayMap roundtrip ──

    #[test]
    fn array_map_roundtrip() {
        let tt = TransformType::ArrayMap {
            path: "users".into(),
            mapping: FieldMappingSpec {
                mappings: vec![FieldMapping { from: "name".into(), to: "display_name".into() }],
            },
        };
        let json = serde_json::to_string(&tt).unwrap();
        let back: TransformType = serde_json::from_str(&json).unwrap();
        assert_eq!(tt, back);
    }

    // ── E2S1-T45: ConnectorCategory::Shell roundtrip ──
    // (Tested in descriptor.rs — included here for completeness reference)
}
