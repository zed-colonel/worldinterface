//! Transform executor for pure data transformations.
//!
//! Transforms are pure functions that reshape data between boundary crossings.
//! They are NOT connectors — they have no side effects, require no idempotency
//! keys, and produce no receipts.

use serde_json::Value;
use worldinterface_core::flowspec::transform::{
    FieldMappingSpec, FilterCondition, FilterSpec, StringTemplateSpec, TransformType,
};

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
        TransformType::Filter(spec) => execute_filter(spec, input),
        TransformType::StringTemplate(spec) => execute_string_template(spec, input),
        TransformType::ArrayFlatten { path } => execute_array_flatten(path, input),
        TransformType::ArrayMap { path, mapping } => execute_array_map(path, mapping, input),
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

fn execute_filter(spec: &FilterSpec, input: &Value) -> Result<Value, TransformError> {
    let passes = match &spec.condition {
        FilterCondition::Equals { path, value } => {
            resolve_path(input, path).is_some_and(|v| v == value)
        }
        FilterCondition::Exists { path } => resolve_path(input, path).is_some_and(|v| !v.is_null()),
        FilterCondition::NotEquals { path, value } => {
            resolve_path(input, path).is_none_or(|v| v != value)
        }
        FilterCondition::NotExists { path } => {
            resolve_path(input, path).is_none_or(|v| v.is_null())
        }
    };
    if passes {
        Ok(input.clone())
    } else {
        Ok(Value::Null)
    }
}

fn execute_string_template(
    spec: &StringTemplateSpec,
    input: &Value,
) -> Result<Value, TransformError> {
    let mut result = spec.template.clone();
    let mut search_from = 0;
    while let Some(offset) = result[search_from..].find("{{") {
        let start = search_from + offset;
        let Some(end_offset) = result[start..].find("}}") else {
            break;
        };
        let end = start + end_offset + 2;
        let path = result[start + 2..end - 2].trim();
        let value = resolve_path(input, path)
            .ok_or_else(|| TransformError::FieldNotFound { path: path.to_string() })?;
        let replacement = match value {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        result.replace_range(start..end, &replacement);
        search_from = start + replacement.len();
    }
    Ok(Value::String(result))
}

fn execute_array_flatten(path: &str, input: &Value) -> Result<Value, TransformError> {
    let arr = resolve_path(input, path)
        .ok_or_else(|| TransformError::FieldNotFound { path: path.to_string() })?;
    let outer = arr.as_array().ok_or_else(|| TransformError::TypeMismatch {
        expected: "array".into(),
        actual: value_type_name(arr).into(),
    })?;
    let mut flat = Vec::new();
    for item in outer {
        match item.as_array() {
            Some(inner) => flat.extend(inner.iter().cloned()),
            None => flat.push(item.clone()),
        }
    }
    Ok(Value::Array(flat))
}

fn execute_array_map(
    path: &str,
    mapping: &FieldMappingSpec,
    input: &Value,
) -> Result<Value, TransformError> {
    let arr = resolve_path(input, path)
        .ok_or_else(|| TransformError::FieldNotFound { path: path.to_string() })?;
    let items = arr.as_array().ok_or_else(|| TransformError::TypeMismatch {
        expected: "array".into(),
        actual: value_type_name(arr).into(),
    })?;
    let mut mapped = Vec::with_capacity(items.len());
    for item in items {
        mapped.push(execute_field_mapping(mapping, item)?);
    }
    Ok(Value::Array(mapped))
}

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
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

    // ── E2S1-T19: Filter equals passes ──

    #[test]
    fn filter_equals_passes() {
        let spec = FilterSpec {
            condition: FilterCondition::Equals { path: "status".into(), value: json!("ok") },
        };
        let input = json!({"status": "ok"});
        let result = execute_transform(&TransformType::Filter(spec), &input).unwrap();
        assert_eq!(result, input);
    }

    // ── E2S1-T20: Filter equals rejects ──

    #[test]
    fn filter_equals_rejects() {
        let spec = FilterSpec {
            condition: FilterCondition::Equals { path: "status".into(), value: json!("ok") },
        };
        let input = json!({"status": "error"});
        let result = execute_transform(&TransformType::Filter(spec), &input).unwrap();
        assert_eq!(result, Value::Null);
    }

    // ── E2S1-T21: Filter exists present ──

    #[test]
    fn filter_exists_present() {
        let spec = FilterSpec { condition: FilterCondition::Exists { path: "key".into() } };
        let input = json!({"key": 42});
        let result = execute_transform(&TransformType::Filter(spec), &input).unwrap();
        assert_eq!(result, input);
    }

    // ── E2S1-T22: Filter exists null ──

    #[test]
    fn filter_exists_null() {
        let spec = FilterSpec { condition: FilterCondition::Exists { path: "key".into() } };
        let input = json!({"key": null});
        let result = execute_transform(&TransformType::Filter(spec), &input).unwrap();
        assert_eq!(result, Value::Null);
    }

    // ── E2S1-T23: Filter exists missing ──

    #[test]
    fn filter_exists_missing() {
        let spec = FilterSpec { condition: FilterCondition::Exists { path: "key".into() } };
        let input = json!({});
        let result = execute_transform(&TransformType::Filter(spec), &input).unwrap();
        assert_eq!(result, Value::Null);
    }

    // ── E2S1-T24: Filter not equals ──

    #[test]
    fn filter_not_equals() {
        let spec = FilterSpec {
            condition: FilterCondition::NotEquals { path: "x".into(), value: json!(2) },
        };
        let input = json!({"x": 1});
        let result = execute_transform(&TransformType::Filter(spec), &input).unwrap();
        assert_eq!(result, input);
    }

    // ── E2S1-T25: Filter not exists ──

    #[test]
    fn filter_not_exists() {
        let spec = FilterSpec { condition: FilterCondition::NotExists { path: "key".into() } };
        let input = json!({});
        let result = execute_transform(&TransformType::Filter(spec), &input).unwrap();
        assert_eq!(result, input);
    }

    // ── E2S1-T26: Filter nested path ──

    #[test]
    fn filter_nested_path() {
        let spec = FilterSpec {
            condition: FilterCondition::Equals { path: "a.b".into(), value: json!("c") },
        };
        let input = json!({"a": {"b": "c"}});
        let result = execute_transform(&TransformType::Filter(spec), &input).unwrap();
        assert_eq!(result, input);
    }

    // ── E2S1-T27: String template single ──

    #[test]
    fn string_template_single() {
        let spec = StringTemplateSpec { template: "Hello, {{name}}!".into() };
        let input = json!({"name": "Alice"});
        let result = execute_transform(&TransformType::StringTemplate(spec), &input).unwrap();
        assert_eq!(result, json!("Hello, Alice!"));
    }

    // ── E2S1-T28: String template multiple ──

    #[test]
    fn string_template_multiple() {
        let spec = StringTemplateSpec { template: "{{greeting}}, {{name}}!".into() };
        let input = json!({"greeting": "Hi", "name": "Bob"});
        let result = execute_transform(&TransformType::StringTemplate(spec), &input).unwrap();
        assert_eq!(result, json!("Hi, Bob!"));
    }

    // ── E2S1-T29: String template nested path ──

    #[test]
    fn string_template_nested_path() {
        let spec = StringTemplateSpec { template: "User: {{user.name}}".into() };
        let input = json!({"user": {"name": "Bob"}});
        let result = execute_transform(&TransformType::StringTemplate(spec), &input).unwrap();
        assert_eq!(result, json!("User: Bob"));
    }

    // ── E2S1-T30: String template non-string value ──

    #[test]
    fn string_template_non_string_value() {
        let spec = StringTemplateSpec { template: "Count: {{count}}".into() };
        let input = json!({"count": 42});
        let result = execute_transform(&TransformType::StringTemplate(spec), &input).unwrap();
        assert_eq!(result, json!("Count: 42"));
    }

    // ── E2S1-T31: String template missing path ──

    #[test]
    fn string_template_missing_path() {
        let spec = StringTemplateSpec { template: "{{nonexistent}}".into() };
        let input = json!({});
        let result = execute_transform(&TransformType::StringTemplate(spec), &input);
        assert!(matches!(
            result,
            Err(TransformError::FieldNotFound { path }) if path == "nonexistent"
        ));
    }

    // ── E2S1-T32: String template no templates ──

    #[test]
    fn string_template_no_templates() {
        let spec = StringTemplateSpec { template: "plain text".into() };
        let input = json!({});
        let result = execute_transform(&TransformType::StringTemplate(spec), &input).unwrap();
        assert_eq!(result, json!("plain text"));
    }

    // ── E2S1-T33: Array flatten nested ──

    #[test]
    fn array_flatten_nested() {
        let input = json!({"data": [[1, 2], [3, 4]]});
        let result =
            execute_transform(&TransformType::ArrayFlatten { path: "data".into() }, &input)
                .unwrap();
        assert_eq!(result, json!([1, 2, 3, 4]));
    }

    // ── E2S1-T34: Array flatten mixed ──

    #[test]
    fn array_flatten_mixed() {
        let input = json!({"data": [[1, 2], 3, [4]]});
        let result =
            execute_transform(&TransformType::ArrayFlatten { path: "data".into() }, &input)
                .unwrap();
        assert_eq!(result, json!([1, 2, 3, 4]));
    }

    // ── E2S1-T35: Array flatten empty ──

    #[test]
    fn array_flatten_empty() {
        let input = json!({"data": []});
        let result =
            execute_transform(&TransformType::ArrayFlatten { path: "data".into() }, &input)
                .unwrap();
        assert_eq!(result, json!([]));
    }

    // ── E2S1-T36: Array flatten not array ──

    #[test]
    fn array_flatten_not_array() {
        let input = json!({"data": "not an array"});
        let result =
            execute_transform(&TransformType::ArrayFlatten { path: "data".into() }, &input);
        assert!(matches!(result, Err(TransformError::TypeMismatch { .. })));
    }

    // ── E2S1-T37: Array map basic ──

    #[test]
    fn array_map_basic() {
        let input = json!({
            "users": [
                {"first": "Alice", "last": "A"},
                {"first": "Bob", "last": "B"}
            ]
        });
        let result = execute_transform(
            &TransformType::ArrayMap {
                path: "users".into(),
                mapping: FieldMappingSpec {
                    mappings: vec![FieldMapping { from: "first".into(), to: "name".into() }],
                },
            },
            &input,
        )
        .unwrap();
        assert_eq!(result, json!([{"name": "Alice"}, {"name": "Bob"}]));
    }

    // ── E2S1-T38: Array map empty ──

    #[test]
    fn array_map_empty() {
        let input = json!({"items": []});
        let result = execute_transform(
            &TransformType::ArrayMap {
                path: "items".into(),
                mapping: FieldMappingSpec {
                    mappings: vec![FieldMapping { from: "a".into(), to: "b".into() }],
                },
            },
            &input,
        )
        .unwrap();
        assert_eq!(result, json!([]));
    }

    // ── E2S1-T39: Array map nested path ──

    #[test]
    fn array_map_nested_path() {
        let input = json!({
            "data": {
                "users": [
                    {"name": "Alice"},
                    {"name": "Bob"}
                ]
            }
        });
        let result = execute_transform(
            &TransformType::ArrayMap {
                path: "data.users".into(),
                mapping: FieldMappingSpec {
                    mappings: vec![FieldMapping { from: "name".into(), to: "display".into() }],
                },
            },
            &input,
        )
        .unwrap();
        assert_eq!(result, json!([{"display": "Alice"}, {"display": "Bob"}]));
    }
}
