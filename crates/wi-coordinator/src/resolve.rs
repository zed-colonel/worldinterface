//! Parameter resolution — substitutes template references in JSON values.
//!
//! Template syntax:
//! - `{{nodes.<node_id>.<path>}}` — reference to a node's output
//! - `{{params.<path>}}` — reference to a flow-level parameter
//!
//! Two resolution modes:
//! - **Full replacement**: entire string is one template → preserves type
//! - **String interpolation**: template within a larger string → toString

use serde_json::Value;
use wi_connector::transform::resolve_path;
use wi_contextstore::ContextStore;
use wi_core::id::{trigger_input_node_id, FlowRunId, NodeId};

use crate::error::ResolveError;

/// Resolve template references in a JSON Value tree.
///
/// Walks the Value recursively. For string values containing `{{...}}` patterns,
/// substitutes the referenced value from ContextStore or flow_params.
pub fn resolve_params(
    params: &Value,
    flow_run_id: FlowRunId,
    flow_params: Option<&Value>,
    store: &dyn ContextStore,
) -> Result<Value, ResolveError> {
    match params {
        Value::String(s) => resolve_string(s, flow_run_id, flow_params, store),
        Value::Object(map) => {
            let mut resolved = serde_json::Map::new();
            for (key, value) in map {
                resolved
                    .insert(key.clone(), resolve_params(value, flow_run_id, flow_params, store)?);
            }
            Ok(Value::Object(resolved))
        }
        Value::Array(arr) => {
            let resolved: Result<Vec<Value>, ResolveError> =
                arr.iter().map(|v| resolve_params(v, flow_run_id, flow_params, store)).collect();
            Ok(Value::Array(resolved?))
        }
        // Numbers, booleans, null — pass through unchanged
        other => Ok(other.clone()),
    }
}

/// Resolve a string that may contain template references.
fn resolve_string(
    s: &str,
    flow_run_id: FlowRunId,
    flow_params: Option<&Value>,
    store: &dyn ContextStore,
) -> Result<Value, ResolveError> {
    // Fast path: no templates
    if !s.contains("{{") {
        return Ok(Value::String(s.to_string()));
    }

    // Check for full replacement: entire string is a single template
    let trimmed = s.trim();
    if trimmed.starts_with("{{") && trimmed.ends_with("}}") {
        // Check there's only one template (no nested {{ after the first)
        let inner = &trimmed[2..trimmed.len() - 2];
        if !inner.contains("{{") && !inner.contains("}}") {
            return resolve_reference(inner.trim(), flow_run_id, flow_params, store);
        }
    }

    // String interpolation mode: replace each {{...}} with its string representation
    let mut result = String::new();
    let mut remaining = s;

    while let Some(start) = remaining.find("{{") {
        result.push_str(&remaining[..start]);
        let after_open = &remaining[start + 2..];
        if let Some(end) = after_open.find("}}") {
            let ref_str = after_open[..end].trim();
            let value = resolve_reference(ref_str, flow_run_id, flow_params, store)?;
            result.push_str(&value_to_string(&value));
            remaining = &after_open[end + 2..];
        } else {
            // Unclosed template — treat as literal
            result.push_str("{{");
            remaining = after_open;
        }
    }
    result.push_str(remaining);

    Ok(Value::String(result))
}

/// Resolve a single template reference (without the `{{ }}` delimiters).
fn resolve_reference(
    reference: &str,
    flow_run_id: FlowRunId,
    flow_params: Option<&Value>,
    store: &dyn ContextStore,
) -> Result<Value, ResolveError> {
    if let Some(rest) = reference.strip_prefix("nodes.") {
        resolve_node_reference(rest, flow_run_id, store)
    } else if let Some(rest) = reference.strip_prefix("params.") {
        resolve_param_reference(rest, flow_params)
    } else if let Some(rest) = reference.strip_prefix("trigger.") {
        resolve_trigger_reference(rest, flow_run_id, store)
    } else {
        // Unknown reference type — treat as missing param
        Err(ResolveError::FlowParamNotFound { path: reference.to_string() })
    }
}

/// Resolve `nodes.<node_id>.<path>` — node_id is a 36-char UUID.
fn resolve_node_reference(
    rest: &str,
    flow_run_id: FlowRunId,
    store: &dyn ContextStore,
) -> Result<Value, ResolveError> {
    // node_id is a UUID (36 chars: 8-4-4-4-12)
    if rest.len() < 36 {
        return Err(ResolveError::FlowParamNotFound { path: format!("nodes.{rest}") });
    }

    let node_id_str = &rest[..36];
    let node_id: NodeId = node_id_str
        .parse::<uuid::Uuid>()
        .map(NodeId::from)
        .map_err(|_| ResolveError::FlowParamNotFound { path: format!("nodes.{rest}") })?;

    let output =
        store.get(flow_run_id, node_id)?.ok_or(ResolveError::NodeOutputNotFound { node_id })?;

    // After the UUID, there may be a dot-separated path
    let after_uuid = &rest[36..];
    if after_uuid.is_empty() {
        return Ok(output);
    }

    // Strip leading dot
    let path = after_uuid.strip_prefix('.').unwrap_or(after_uuid);

    // If the path starts with "output.", strip it (backward compat with template syntax)
    let path = path
        .strip_prefix("output.")
        .or_else(|| if path == "output" { Some("") } else { Some(path) })
        .unwrap_or(path);

    if path.is_empty() {
        return Ok(output);
    }

    resolve_path(&output, path)
        .cloned()
        .ok_or_else(|| ResolveError::PathNotFound { node_id, path: path.to_string() })
}

/// Resolve `params.<path>`.
fn resolve_param_reference(path: &str, flow_params: Option<&Value>) -> Result<Value, ResolveError> {
    let params =
        flow_params.ok_or_else(|| ResolveError::FlowParamNotFound { path: path.to_string() })?;

    resolve_path(params, path)
        .cloned()
        .ok_or_else(|| ResolveError::FlowParamNotFound { path: path.to_string() })
}

/// Resolve `trigger.<path>` — reads from the well-known trigger input node in ContextStore.
fn resolve_trigger_reference(
    path: &str,
    flow_run_id: FlowRunId,
    store: &dyn ContextStore,
) -> Result<Value, ResolveError> {
    let trigger_node_id = trigger_input_node_id();
    let trigger_data =
        store.get(flow_run_id, trigger_node_id)?.ok_or(ResolveError::TriggerInputNotFound)?;

    if path.is_empty() {
        return Ok(trigger_data);
    }

    resolve_path(&trigger_data, path)
        .cloned()
        .ok_or_else(|| ResolveError::FlowParamNotFound { path: format!("trigger.{}", path) })
}

/// Convert a JSON value to a string for interpolation.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        // For objects and arrays, use compact JSON
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wi_contextstore::SqliteContextStore;

    use super::*;

    fn make_store() -> SqliteContextStore {
        SqliteContextStore::in_memory().unwrap()
    }

    // T-6: Parameter Resolution

    #[test]
    fn resolve_no_templates() {
        let store = make_store();
        let input = json!({"key": "plain value", "num": 42});
        let result = resolve_params(&input, FlowRunId::new(), None, &store).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn resolve_simple_node_ref() {
        let store = make_store();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        store.put(fr, node_id, &json!({"field": "hello"})).unwrap();

        let template = format!("{{{{nodes.{}.field}}}}", node_id.as_ref());
        let input = Value::String(template);
        let result = resolve_params(&input, fr, None, &store).unwrap();
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn resolve_flow_param() {
        let store = make_store();
        let params = json!({"key": "value"});
        let input = json!("{{params.key}}");
        let result = resolve_params(&input, FlowRunId::new(), Some(&params), &store).unwrap();
        assert_eq!(result, json!("value"));
    }

    #[test]
    fn resolve_nested_path() {
        let store = make_store();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        store.put(fr, node_id, &json!({"a": {"b": {"c": 42}}})).unwrap();

        let template = format!("{{{{nodes.{}.a.b.c}}}}", node_id.as_ref());
        let input = Value::String(template);
        let result = resolve_params(&input, fr, None, &store).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn resolve_full_replacement_preserves_type() {
        let store = make_store();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        let obj = json!({"nested": [1, 2, 3], "flag": true});
        store.put(fr, node_id, &obj).unwrap();

        let template = format!("{{{{nodes.{}}}}}", node_id.as_ref());
        let input = Value::String(template);
        let result = resolve_params(&input, fr, None, &store).unwrap();
        assert_eq!(result, obj);
    }

    #[test]
    fn resolve_string_interpolation() {
        let store = make_store();
        let params = json!({"id": "abc123"});
        let input = json!("prefix-{{params.id}}-suffix");
        let result = resolve_params(&input, FlowRunId::new(), Some(&params), &store).unwrap();
        assert_eq!(result, json!("prefix-abc123-suffix"));
    }

    #[test]
    fn resolve_nested_object() {
        let store = make_store();
        let params = json!({"host": "example.com", "port": 8080});
        let input = json!({
            "url": "https://{{params.host}}:{{params.port}}/api",
            "nested": {
                "value": "{{params.host}}"
            }
        });
        let result = resolve_params(&input, FlowRunId::new(), Some(&params), &store).unwrap();
        assert_eq!(
            result,
            json!({
                "url": "https://example.com:8080/api",
                "nested": {
                    "value": "example.com"
                }
            })
        );
    }

    #[test]
    fn resolve_missing_node_output() {
        let store = make_store();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        let template = format!("{{{{nodes.{}}}}}", node_id.as_ref());
        let input = Value::String(template);
        let result = resolve_params(&input, fr, None, &store);
        assert!(matches!(result, Err(ResolveError::NodeOutputNotFound { .. })));
    }

    #[test]
    fn resolve_missing_path_in_output() {
        let store = make_store();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        store.put(fr, node_id, &json!({"a": 1})).unwrap();

        let template = format!("{{{{nodes.{}.nonexistent}}}}", node_id.as_ref());
        let input = Value::String(template);
        let result = resolve_params(&input, fr, None, &store);
        assert!(matches!(result, Err(ResolveError::PathNotFound { .. })));
    }

    #[test]
    fn resolve_missing_flow_param() {
        let store = make_store();
        let params = json!({"a": 1});
        let input = json!("{{params.nonexistent}}");
        let result = resolve_params(&input, FlowRunId::new(), Some(&params), &store);
        assert!(matches!(result, Err(ResolveError::FlowParamNotFound { .. })));
    }

    #[test]
    fn resolve_no_flow_params_provided() {
        let store = make_store();
        let input = json!("{{params.key}}");
        let result = resolve_params(&input, FlowRunId::new(), None, &store);
        assert!(matches!(result, Err(ResolveError::FlowParamNotFound { .. })));
    }

    #[test]
    fn resolve_with_output_prefix() {
        let store = make_store();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        store.put(fr, node_id, &json!({"field": "value"})).unwrap();

        let template = format!("{{{{nodes.{}.output.field}}}}", node_id.as_ref());
        let input = Value::String(template);
        let result = resolve_params(&input, fr, None, &store).unwrap();
        assert_eq!(result, json!("value"));
    }

    // T-7: Trigger resolution tests

    #[test]
    fn resolve_trigger_body_field() {
        let store = make_store();
        let fr = FlowRunId::new();
        let trigger_data = json!({
            "body": {"event": "push"},
            "headers": {"content-type": "application/json"},
            "method": "POST",
            "path": "github/push",
            "received_at": 1741200000
        });
        store.put(fr, trigger_input_node_id(), &trigger_data).unwrap();

        let input = json!("{{trigger.body.event}}");
        let result = resolve_params(&input, fr, None, &store).unwrap();
        assert_eq!(result, json!("push"));
    }

    #[test]
    fn resolve_trigger_headers() {
        let store = make_store();
        let fr = FlowRunId::new();
        let trigger_data = json!({
            "body": {},
            "headers": {"content-type": "application/json"},
            "method": "POST",
            "path": "test",
            "received_at": 0
        });
        store.put(fr, trigger_input_node_id(), &trigger_data).unwrap();

        let input = json!("{{trigger.headers.content-type}}");
        let result = resolve_params(&input, fr, None, &store).unwrap();
        assert_eq!(result, json!("application/json"));
    }

    #[test]
    fn resolve_trigger_full_body() {
        let store = make_store();
        let fr = FlowRunId::new();
        let body = json!({"message": "hello", "count": 5});
        let trigger_data = json!({
            "body": body,
            "headers": {},
            "method": "POST",
            "path": "test",
            "received_at": 0
        });
        store.put(fr, trigger_input_node_id(), &trigger_data).unwrap();

        let input = json!("{{trigger.body}}");
        let result = resolve_params(&input, fr, None, &store).unwrap();
        assert_eq!(result, json!({"message": "hello", "count": 5}));
    }

    #[test]
    fn resolve_trigger_missing_returns_error() {
        let store = make_store();
        let fr = FlowRunId::new();
        // No trigger data written
        let input = json!("{{trigger.body}}");
        let result = resolve_params(&input, fr, None, &store);
        assert!(matches!(result, Err(ResolveError::TriggerInputNotFound)));
    }

    #[test]
    fn resolve_trigger_nested_path() {
        let store = make_store();
        let fr = FlowRunId::new();
        let trigger_data = json!({
            "body": {"payload": {"action": "opened"}},
            "headers": {},
            "method": "POST",
            "path": "test",
            "received_at": 0
        });
        store.put(fr, trigger_input_node_id(), &trigger_data).unwrap();

        let input = json!("{{trigger.body.payload.action}}");
        let result = resolve_params(&input, fr, None, &store).unwrap();
        assert_eq!(result, json!("opened"));
    }

    #[test]
    fn resolve_array_with_templates() {
        let store = make_store();
        let params = json!({"a": "x", "b": "y"});
        let input = json!(["{{params.a}}", "{{params.b}}", "literal"]);
        let result = resolve_params(&input, FlowRunId::new(), Some(&params), &store).unwrap();
        assert_eq!(result, json!(["x", "y", "literal"]));
    }
}
