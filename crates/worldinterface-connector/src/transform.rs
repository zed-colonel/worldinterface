//! Transform executor for pure data transformations.
//!
//! Transforms are pure functions that reshape data between boundary crossings.
//! They are NOT connectors — they have no side effects, require no idempotency
//! keys, and produce no receipts.

use serde_json::Value;
use worldinterface_core::flowspec::transform::{FieldMappingSpec, TransformType};

use crate::error::TransformError;

/// Execute a pure transform on the given input.
///
/// Transforms are deterministic: the same input always produces the same output.
/// They have no side effects and need no external context.
pub fn execute_transform(
    transform_type: &TransformType,
    input: &Value,
) -> Result<Value, TransformError> {
    match transform_type {
        TransformType::Identity => Ok(input.clone()),
        TransformType::FieldMapping(spec) => execute_field_mapping(spec, input),
    }
}

fn execute_field_mapping(spec: &FieldMappingSpec, input: &Value) -> Result<Value, TransformError> {
    let mut output = serde_json::Map::new();
    for mapping in &spec.mappings {
        let value = resolve_path(input, &mapping.from)
            .ok_or_else(|| TransformError::FieldNotFound { path: mapping.from.clone() })?;
        set_path(&mut output, &mapping.to, value.clone());
    }
    Ok(Value::Object(output))
}

/// Navigate a JSON value using dot-notation paths.
///
/// `"body.data.items"` resolves to `value["body"]["data"]["items"]`.
/// Returns `None` if any segment is missing. Array indexing is not supported
/// in v1.0-alpha.
pub fn resolve_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(segment)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Set a value at a dot-notation path, creating intermediate objects as needed.
///
/// `"output.transformed.name"` creates
/// `{"output": {"transformed": {"name": <value>}}}`.
pub fn set_path(map: &mut serde_json::Map<String, Value>, path: &str, value: Value) {
    let segments: Vec<&str> = path.split('.').collect();
    if segments.is_empty() {
        return;
    }
    if segments.len() == 1 {
        map.insert(segments[0].to_string(), value);
        return;
    }

    // Build nested structure from the inside out
    let mut current_value = value;
    for segment in segments[1..].iter().rev() {
        let mut obj = serde_json::Map::new();
        obj.insert(segment.to_string(), current_value);
        current_value = Value::Object(obj);
    }

    // Merge into existing structure at the top level
    let top = segments[0];
    match map.get_mut(top) {
        Some(Value::Object(existing)) => {
            if let Value::Object(new_obj) = current_value {
                merge_maps(existing, new_obj);
            }
        }
        _ => {
            map.insert(top.to_string(), current_value);
        }
    }
}

fn merge_maps(target: &mut serde_json::Map<String, Value>, source: serde_json::Map<String, Value>) {
    for (key, value) in source {
        if let Some(Value::Object(existing)) = target.get_mut(&key) {
            if let Value::Object(new_obj) = value {
                merge_maps(existing, new_obj);
                continue;
            }
        }
        target.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use worldinterface_core::flowspec::transform::{FieldMapping, FieldMappingSpec};

    use super::*;

    // T-9: Path Resolution Utilities

    #[test]
    fn resolve_simple_key() {
        let val = json!({"a": 1});
        assert_eq!(resolve_path(&val, "a"), Some(&json!(1)));
    }

    #[test]
    fn resolve_nested_path() {
        let val = json!({"a": {"b": {"c": 3}}});
        assert_eq!(resolve_path(&val, "a.b.c"), Some(&json!(3)));
    }

    #[test]
    fn resolve_missing_returns_none() {
        let val = json!({"a": 1});
        assert_eq!(resolve_path(&val, "b"), None);
    }

    #[test]
    fn resolve_array_element_not_supported() {
        let val = json!({"a": [1, 2]});
        assert_eq!(resolve_path(&val, "a.0"), None);
    }

    #[test]
    fn set_simple_key() {
        let mut map = serde_json::Map::new();
        set_path(&mut map, "a", json!(1));
        assert_eq!(Value::Object(map), json!({"a": 1}));
    }

    #[test]
    fn set_nested_path_creates_intermediates() {
        let mut map = serde_json::Map::new();
        set_path(&mut map, "a.b.c", json!(3));
        assert_eq!(Value::Object(map), json!({"a": {"b": {"c": 3}}}));
    }

    // T-7: Transform Executor

    #[test]
    fn identity_returns_input_unchanged() {
        let input = json!({"key": "value", "num": 42});
        let result = execute_transform(&TransformType::Identity, &input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn identity_with_null() {
        let result = execute_transform(&TransformType::Identity, &Value::Null).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn identity_with_complex_object() {
        let input = json!({
            "nested": {"array": [1, 2, 3], "deep": {"value": true}},
            "list": ["a", "b"]
        });
        let result = execute_transform(&TransformType::Identity, &input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn field_mapping_single_field() {
        let spec =
            FieldMappingSpec { mappings: vec![FieldMapping { from: "a".into(), to: "b".into() }] };
        let input = json!({"a": 1});
        let result = execute_transform(&TransformType::FieldMapping(spec), &input).unwrap();
        assert_eq!(result, json!({"b": 1}));
    }

    #[test]
    fn field_mapping_nested_source() {
        let spec = FieldMappingSpec {
            mappings: vec![FieldMapping { from: "a.b".into(), to: "x".into() }],
        };
        let input = json!({"a": {"b": 1}});
        let result = execute_transform(&TransformType::FieldMapping(spec), &input).unwrap();
        assert_eq!(result, json!({"x": 1}));
    }

    #[test]
    fn field_mapping_nested_target() {
        let spec = FieldMappingSpec {
            mappings: vec![FieldMapping { from: "a".into(), to: "x.y".into() }],
        };
        let input = json!({"a": 1});
        let result = execute_transform(&TransformType::FieldMapping(spec), &input).unwrap();
        assert_eq!(result, json!({"x": {"y": 1}}));
    }

    #[test]
    fn field_mapping_multiple_fields() {
        let spec = FieldMappingSpec {
            mappings: vec![
                FieldMapping { from: "a".into(), to: "x".into() },
                FieldMapping { from: "b".into(), to: "y".into() },
            ],
        };
        let input = json!({"a": 1, "b": 2});
        let result = execute_transform(&TransformType::FieldMapping(spec), &input).unwrap();
        assert_eq!(result, json!({"x": 1, "y": 2}));
    }

    #[test]
    fn field_mapping_missing_field_errors() {
        let spec = FieldMappingSpec {
            mappings: vec![FieldMapping { from: "nonexistent".into(), to: "out".into() }],
        };
        let input = json!({"a": 1});
        let result = execute_transform(&TransformType::FieldMapping(spec), &input);
        assert!(matches!(
            result,
            Err(TransformError::FieldNotFound { path }) if path == "nonexistent"
        ));
    }

    #[test]
    fn field_mapping_preserves_types() {
        let spec = FieldMappingSpec {
            mappings: vec![
                FieldMapping { from: "int".into(), to: "o_int".into() },
                FieldMapping { from: "str".into(), to: "o_str".into() },
                FieldMapping { from: "bool".into(), to: "o_bool".into() },
                FieldMapping { from: "null".into(), to: "o_null".into() },
                FieldMapping { from: "arr".into(), to: "o_arr".into() },
                FieldMapping { from: "obj".into(), to: "o_obj".into() },
            ],
        };
        let input = json!({
            "int": 42,
            "str": "hello",
            "bool": true,
            "null": null,
            "arr": [1, 2, 3],
            "obj": {"key": "val"}
        });
        let result = execute_transform(&TransformType::FieldMapping(spec), &input).unwrap();
        assert_eq!(result["o_int"], json!(42));
        assert_eq!(result["o_str"], json!("hello"));
        assert_eq!(result["o_bool"], json!(true));
        assert_eq!(result["o_null"], json!(null));
        assert_eq!(result["o_arr"], json!([1, 2, 3]));
        assert_eq!(result["o_obj"], json!({"key": "val"}));
    }
}
