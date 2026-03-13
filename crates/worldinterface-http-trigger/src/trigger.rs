//! Trigger input data written to ContextStore when a webhook fires.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Structured trigger input written to ContextStore when a webhook fires.
///
/// Flow nodes access this via `{{trigger.body}}`, `{{trigger.headers}}`, etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerInput {
    /// The HTTP request body (parsed as JSON if valid, or wrapped as a JSON string).
    pub body: Value,

    /// HTTP request headers as a flat JSON object (header-name -> value).
    pub headers: Value,

    /// The HTTP method (always "POST" for webhooks, included for completeness).
    pub method: String,

    /// The webhook path that was matched.
    pub path: String,

    /// Source IP address of the request (from `x-forwarded-for` or connection peer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_addr: Option<String>,

    /// Unix timestamp (seconds) of when the webhook was received.
    pub received_at: u64,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn sample_trigger_input() -> TriggerInput {
        TriggerInput {
            body: json!({"event": "push", "ref": "refs/heads/main"}),
            headers: json!({"content-type": "application/json", "x-github-event": "push"}),
            method: "POST".to_string(),
            path: "github/push".to_string(),
            source_addr: Some("192.168.1.1".to_string()),
            received_at: 1741200000,
        }
    }

    #[test]
    fn json_body_preserved_as_json() {
        let input = sample_trigger_input();
        assert!(input.body.is_object());
        assert_eq!(input.body["event"], "push");
    }

    #[test]
    fn non_json_body_becomes_string() {
        let input = TriggerInput {
            body: Value::String("plain text body".to_string()),
            headers: json!({}),
            method: "POST".to_string(),
            path: "test".to_string(),
            source_addr: None,
            received_at: 0,
        };
        assert!(input.body.is_string());
        assert_eq!(input.body.as_str().unwrap(), "plain text body");
    }

    #[test]
    fn trigger_input_serializes() {
        let input = sample_trigger_input();
        let json = serde_json::to_string(&input).unwrap();
        let back: TriggerInput = serde_json::from_str(&json).unwrap();
        assert_eq!(input.body, back.body);
        assert_eq!(input.headers, back.headers);
        assert_eq!(input.method, back.method);
        assert_eq!(input.path, back.path);
        assert_eq!(input.source_addr, back.source_addr);
        assert_eq!(input.received_at, back.received_at);
    }

    #[test]
    fn headers_as_json_object() {
        let input = sample_trigger_input();
        let headers = input.headers.as_object().unwrap();
        assert_eq!(headers.len(), 2);
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(headers["x-github-event"], "push");
    }
}
