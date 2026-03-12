//! Webhook invocation handler.

use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;
use serde_json::Value;
use wi_core::id::FlowRunId;
use wi_host::EmbeddedHost;

use crate::error::WebhookError;
use crate::receipt::create_trigger_receipt;
use crate::registry::WebhookRegistry;
use crate::trigger::TriggerInput;

/// Handle an incoming webhook invocation.
///
/// Looks up the registered webhook, builds trigger input from the request,
/// submits the flow with trigger data injected into ContextStore, and
/// returns the FlowRunId and receipt.
pub async fn handle_webhook(
    path: &str,
    body: &[u8],
    headers: &HeaderMap,
    source_addr: Option<String>,
    host: &EmbeddedHost,
    webhook_registry: &RwLock<WebhookRegistry>,
) -> Result<(FlowRunId, Value), WebhookError> {
    // 1. Look up webhook registration (read lock released after clone)
    let flow_spec = {
        let registry = webhook_registry.read().unwrap();
        let registration = registry
            .get_by_path(path)
            .ok_or_else(|| WebhookError::PathNotFound(path.to_string()))?;
        registration.flow_spec.clone()
    };

    // 2. Build trigger input
    let body_value: Value = serde_json::from_slice(body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()));

    let headers_value = headers_to_json(headers);

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

    let trigger_input = TriggerInput {
        body: body_value,
        headers: headers_value,
        method: "POST".to_string(),
        path: path.to_string(),
        source_addr,
        received_at: now,
    };

    // 3. Submit flow with trigger input
    let trigger_value = serde_json::to_value(&trigger_input)?;
    let flow_run_id = host.submit_flow_with_trigger_input(flow_spec, trigger_value).await?;

    // 4. Generate and store trigger receipt
    let receipt = create_trigger_receipt(flow_run_id, &trigger_input);
    let receipt_value = serde_json::to_value(&receipt)?;
    host.store_trigger_receipt(flow_run_id, &receipt_value)?;

    tracing::info!(
        flow_run_id = %flow_run_id,
        webhook_path = %path,
        "webhook triggered flow run"
    );

    Ok((flow_run_id, receipt_value))
}

/// Convert HTTP headers to a flat JSON object.
fn headers_to_json(headers: &HeaderMap) -> Value {
    let mut map = serde_json::Map::new();
    for (name, value) in headers.iter() {
        if let Ok(v) = value.to_str() {
            map.insert(name.as_str().to_string(), Value::String(v.to_string()));
        }
    }
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_to_json_converts_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-custom", "value".parse().unwrap());

        let json = headers_to_json(&headers);
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj["content-type"], "application/json");
        assert_eq!(obj["x-custom"], "value");
    }

    #[test]
    fn headers_to_json_empty() {
        let headers = HeaderMap::new();
        let json = headers_to_json(&headers);
        assert_eq!(json, Value::Object(serde_json::Map::new()));
    }
}
