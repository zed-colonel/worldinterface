//! Webhook registration model.

use serde::{Deserialize, Serialize};
use worldinterface_core::flowspec::FlowSpec;

use crate::types::WebhookId;

/// A registered webhook that maps an HTTP path to a FlowSpec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookRegistration {
    /// Unique webhook identifier.
    pub id: WebhookId,

    /// The URL path that triggers this webhook (e.g., "github/push", "stripe/invoice").
    /// Does not include the `/webhooks/` prefix -- that is added by the router.
    pub path: String,

    /// The FlowSpec to execute when the webhook fires.
    pub flow_spec: FlowSpec,

    /// Optional human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Unix timestamp (seconds) when the webhook was registered.
    pub created_at: u64,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use worldinterface_core::flowspec::*;
    use worldinterface_core::id::NodeId;

    use super::*;

    fn sample_registration() -> WebhookRegistration {
        let node_id = NodeId::new();
        WebhookRegistration {
            id: WebhookId::new(),
            path: "github/push".to_string(),
            flow_spec: FlowSpec {
                id: None,
                name: Some("test".into()),
                nodes: vec![Node {
                    id: node_id,
                    label: None,
                    node_type: NodeType::Connector(ConnectorNode {
                        connector: "delay".into(),
                        params: json!({"duration_ms": 10}),
                        idempotency_config: None,
                    }),
                }],
                edges: vec![],
                params: None,
            },
            description: Some("GitHub push handler".to_string()),
            created_at: 1741200000,
        }
    }

    #[test]
    fn serialization_roundtrip() {
        let reg = sample_registration();
        let json = serde_json::to_string(&reg).unwrap();
        let back: WebhookRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(reg.id, back.id);
        assert_eq!(reg.path, back.path);
        assert_eq!(reg.description, back.description);
        assert_eq!(reg.created_at, back.created_at);
    }

    #[test]
    fn serialization_without_description() {
        let mut reg = sample_registration();
        reg.description = None;
        let json = serde_json::to_string(&reg).unwrap();
        let back: WebhookRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(back.description, None);
    }
}
