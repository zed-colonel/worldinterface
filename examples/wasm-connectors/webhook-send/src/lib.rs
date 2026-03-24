//! webhook.send — generic outbound HTTP webhook WASM connector.

wit_bindgen::generate!({
    world: "connector-world",
    path: "../../../crates/worldinterface-wasm/wit",
});

use exo::connector::http;

struct WebhookSendConnector;

impl exports::exo::connector::connector::Guest for WebhookSendConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "webhook.send".to_string(),
            display_name: "Webhook Send".to_string(),
            description: "Sends an HTTP request to a specified URL".to_string(),
            input_schema: Some(INPUT_SCHEMA.to_string()),
            output_schema: Some(OUTPUT_SCHEMA.to_string()),
            idempotent: false,
            side_effects: true,
        }
    }

    fn invoke(
        _ctx: exports::exo::connector::connector::InvocationContext,
        params: String,
    ) -> Result<String, String> {
        let parsed: serde_json::Value =
            serde_json::from_str(&params).map_err(|e| format!("invalid params JSON: {e}"))?;

        let url = parsed["url"].as_str().ok_or("missing required field 'url'")?;
        let method = parsed["method"].as_str().unwrap_or("POST");
        let body = parsed["body"].as_str().map(|s| s.to_string());

        // Build headers from params
        let mut headers = Vec::new();
        if let Some(h) = parsed["headers"].as_object() {
            for (key, value) in h {
                if let Some(v) = value.as_str() {
                    headers.push((key.clone(), v.to_string()));
                }
            }
        }
        // Default Content-Type if body present and no Content-Type set
        if body.is_some() && !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("content-type")) {
            headers.push(("Content-Type".into(), "application/json".into()));
        }

        let response = http::request(method, url, &headers, body.as_deref())
            .map_err(|e| format!("HTTP request failed: {e}"))?;

        Ok(serde_json::json!({
            "status": response.status,
            "headers": response.headers.iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect::<serde_json::Map<String, serde_json::Value>>(),
            "body": response.body,
        })
        .to_string())
    }
}

const INPUT_SCHEMA: &str = r#"{"type":"object","required":["url"],"properties":{"url":{"type":"string","description":"Target URL"},"method":{"type":"string","default":"POST","description":"HTTP method"},"headers":{"type":"object","description":"Request headers"},"body":{"type":"string","description":"Request body"}}}"#;

const OUTPUT_SCHEMA: &str = r#"{"type":"object","properties":{"status":{"type":"integer"},"headers":{"type":"object"},"body":{"type":"string"}}}"#;

export!(WebhookSendConnector);
