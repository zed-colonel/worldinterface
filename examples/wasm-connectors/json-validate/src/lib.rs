//! JSON Schema Validator — reference WASM connector (pure computation).
//!
//! Validates a JSON document against a simple JSON Schema subset.
//! Supports `type`, `required`, `properties`, `minimum`, `maximum`,
//! `minLength`, `maxLength`, and `enum` keywords.

wit_bindgen::generate!({
    world: "connector-world",
    path: "../../../crates/worldinterface-wasm/wit",
});

struct JsonValidateConnector;

/// Validate `document` against `schema`, collecting errors.
fn validate(document: &serde_json::Value, schema: &serde_json::Value) -> Vec<String> {
    let mut errors = Vec::new();

    // type check
    if let Some(ty) = schema.get("type").and_then(|v| v.as_str()) {
        let ok = match ty {
            "object" => document.is_object(),
            "array" => document.is_array(),
            "string" => document.is_string(),
            "number" | "integer" => document.is_number(),
            "boolean" => document.is_boolean(),
            "null" => document.is_null(),
            _ => true,
        };
        if !ok {
            errors.push(format!("expected type '{ty}', got {}", type_name(document)));
        }
    }

    // required check
    if let (Some(required), Some(obj)) = (
        schema.get("required").and_then(|v| v.as_array()),
        document.as_object(),
    ) {
        for field in required {
            if let Some(name) = field.as_str() {
                if !obj.contains_key(name) {
                    errors.push(format!("missing required field '{name}'"));
                }
            }
        }
    }

    // properties (recurse)
    if let (Some(props), Some(obj)) = (
        schema.get("properties").and_then(|v| v.as_object()),
        document.as_object(),
    ) {
        for (key, prop_schema) in props {
            if let Some(value) = obj.get(key) {
                let sub_errors = validate(value, prop_schema);
                for e in sub_errors {
                    errors.push(format!("{key}: {e}"));
                }
            }
        }
    }

    // minimum / maximum
    if let Some(min) = schema.get("minimum").and_then(|v| v.as_f64()) {
        if let Some(val) = document.as_f64() {
            if val < min {
                errors.push(format!("value {val} is less than minimum {min}"));
            }
        }
    }
    if let Some(max) = schema.get("maximum").and_then(|v| v.as_f64()) {
        if let Some(val) = document.as_f64() {
            if val > max {
                errors.push(format!("value {val} is greater than maximum {max}"));
            }
        }
    }

    // minLength / maxLength
    if let Some(min) = schema.get("minLength").and_then(|v| v.as_u64()) {
        if let Some(s) = document.as_str() {
            if (s.len() as u64) < min {
                errors.push(format!("string length {} is less than minLength {min}", s.len()));
            }
        }
    }
    if let Some(max) = schema.get("maxLength").and_then(|v| v.as_u64()) {
        if let Some(s) = document.as_str() {
            if (s.len() as u64) > max {
                errors.push(format!(
                    "string length {} is greater than maxLength {max}",
                    s.len()
                ));
            }
        }
    }

    // enum
    if let Some(variants) = schema.get("enum").and_then(|v| v.as_array()) {
        if !variants.contains(document) {
            errors.push(format!("value not in enum: {document}"));
        }
    }

    errors
}

fn type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

impl exports::exo::connector::connector::Guest for JsonValidateConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "json.validate".to_string(),
            display_name: "JSON Schema Validator".to_string(),
            description: "Validates a JSON document against a JSON Schema.".to_string(),
            input_schema: Some(
                r#"{"type":"object","required":["document","schema"],"properties":{"document":{"description":"JSON value to validate"},"schema":{"description":"JSON Schema to validate against"}}}"#
                    .to_string(),
            ),
            output_schema: Some(
                r#"{"type":"object","properties":{"valid":{"type":"boolean"},"errors":{"type":"array","items":{"type":"string"}}}}"#
                    .to_string(),
            ),
            idempotent: true,
            side_effects: false,
        }
    }

    fn invoke(
        _ctx: exports::exo::connector::connector::InvocationContext,
        params: String,
    ) -> Result<String, String> {
        let input: serde_json::Value =
            serde_json::from_str(&params).map_err(|e| format!("invalid params JSON: {e}"))?;

        let document = input.get("document").ok_or("missing 'document' field")?;
        let schema = input.get("schema").ok_or("missing 'schema' field")?;

        let errors = validate(document, schema);

        let result = serde_json::json!({
            "valid": errors.is_empty(),
            "errors": errors,
        });

        serde_json::to_string(&result).map_err(|e| format!("serialization error: {e}"))
    }
}

export!(JsonValidateConnector);
