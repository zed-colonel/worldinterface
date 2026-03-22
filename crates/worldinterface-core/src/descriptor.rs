//! Connector descriptors for capability discovery.
//!
//! A `Descriptor` advertises what a connector does, what parameters it expects,
//! and what output it produces. Descriptors power the discovery API
//! (`list_capabilities`, `describe`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Self-description of a connector's capabilities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Descriptor {
    /// Unique registry name (e.g., "http.request", "fs.write").
    pub name: String,
    /// Human-readable display name.
    pub display_name: String,
    /// Description of what this connector does.
    pub description: String,
    /// Category for grouping/filtering.
    pub category: ConnectorCategory,
    /// JSON Schema describing expected input parameters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
    /// JSON Schema describing the output shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    /// Whether the connector is naturally idempotent (safe to retry).
    pub idempotent: bool,
    /// Whether the connector has external side effects.
    pub side_effects: bool,
}

/// Category of a connector, used for grouping and filtering in discovery.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorCategory {
    Http,
    FileSystem,
    Delay,
    Transform,
    Shell,
    Sandbox,
    Wasm(String),
    Custom(String),
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn sample_descriptor() -> Descriptor {
        Descriptor {
            name: "http.request".into(),
            display_name: "HTTP Request".into(),
            description: "Makes an HTTP request to an external URL.".into(),
            category: ConnectorCategory::Http,
            input_schema: Some(json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string" },
                    "method": { "type": "string" }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "status": { "type": "integer" },
                    "body": { "type": "string" }
                }
            })),
            idempotent: false,
            side_effects: true,
        }
    }

    #[test]
    fn descriptor_json_roundtrip() {
        let desc = sample_descriptor();
        let json = serde_json::to_string(&desc).unwrap();
        let back: Descriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(desc, back);
    }

    #[test]
    fn category_variants_roundtrip() {
        let categories = vec![
            ConnectorCategory::Http,
            ConnectorCategory::FileSystem,
            ConnectorCategory::Delay,
            ConnectorCategory::Transform,
            ConnectorCategory::Shell,
            ConnectorCategory::Sandbox,
            ConnectorCategory::Wasm("test.echo".into()),
            ConnectorCategory::Custom("my_plugin".into()),
        ];
        for cat in categories {
            let json = serde_json::to_string(&cat).unwrap();
            let back: ConnectorCategory = serde_json::from_str(&json).unwrap();
            assert_eq!(cat, back);
        }
    }

    #[test]
    fn descriptor_with_no_schemas() {
        let desc = Descriptor {
            name: "delay".into(),
            display_name: "Delay".into(),
            description: "Waits for a specified duration.".into(),
            category: ConnectorCategory::Delay,
            input_schema: None,
            output_schema: None,
            idempotent: true,
            side_effects: false,
        };
        let json = serde_json::to_string(&desc).unwrap();
        let back: Descriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(desc, back);
        // Verify schemas are omitted from serialized form
        assert!(!json.contains("input_schema"));
        assert!(!json.contains("output_schema"));
    }

    // ── E2S3-T49: ConnectorCategory::Wasm serde roundtrip ──

    #[test]
    fn connector_category_wasm_roundtrip() {
        let cat = ConnectorCategory::Wasm("test.echo".into());
        let json = serde_json::to_string(&cat).unwrap();
        let back: ConnectorCategory = serde_json::from_str(&json).unwrap();
        assert_eq!(cat, back);
        // Verify it contains the module name
        assert!(json.contains("test.echo"));
    }

    // ── E2S2-T19: ConnectorCategory::Sandbox roundtrip ──

    #[test]
    fn connector_category_sandbox_roundtrip() {
        let cat = ConnectorCategory::Sandbox;
        let json = serde_json::to_string(&cat).unwrap();
        assert_eq!(json, "\"sandbox\"");
        let back: ConnectorCategory = serde_json::from_str(&json).unwrap();
        assert_eq!(cat, back);
    }
}
