//! web.search — configurable web search WASM connector.
//!
//! Default provider: Brave Search API.
//! Provider URL configurable via KV store key "provider_url".
//! API key read from environment variable (configured in manifest).

wit_bindgen::generate!({
    world: "connector-world",
    path: "../../../crates/worldinterface-wasm/wit",
});

use exo::connector::{http, kv, logging};

struct WebSearchConnector;

const DEFAULT_PROVIDER_URL: &str = "https://api.search.brave.com/res/v1/web/search";
const DEFAULT_API_KEY_ENV: &str = "BRAVE_API_KEY";

impl exports::exo::connector::connector::Guest for WebSearchConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "web.search".to_string(),
            display_name: "Web Search".to_string(),
            description: "Searches the web via a configurable search API".to_string(),
            input_schema: Some(INPUT_SCHEMA.to_string()),
            output_schema: Some(OUTPUT_SCHEMA.to_string()),
            idempotent: true,
            side_effects: false,
        }
    }

    fn invoke(
        _ctx: exports::exo::connector::connector::InvocationContext,
        params: String,
    ) -> Result<String, String> {
        let parsed: serde_json::Value =
            serde_json::from_str(&params).map_err(|e| format!("invalid params JSON: {e}"))?;

        let query = parsed["query"].as_str().ok_or("missing required field 'query'")?;
        if query.trim().is_empty() {
            return Err("'query' must not be empty".into());
        }
        let max_results = parsed["max_results"].as_u64().unwrap_or(5);

        // Read provider config from KV (or use defaults)
        let provider_url =
            kv::get("provider_url").unwrap_or_else(|| DEFAULT_PROVIDER_URL.to_string());
        let api_key_env =
            kv::get("api_key_env").unwrap_or_else(|| DEFAULT_API_KEY_ENV.to_string());

        // Read API key from environment
        let api_key = std::env::var(&api_key_env).unwrap_or_default();
        if api_key.is_empty() {
            return Err(format!("API key not found: set environment variable '{api_key_env}'"));
        }

        // Build search URL
        let search_url = format!("{}?q={}&count={}", provider_url, urlencoded(query), max_results);

        logging::log(logging::LogLevel::Debug, &format!("searching: {query}"));

        let headers = vec![
            ("Accept".into(), "application/json".into()),
            ("X-Subscription-Token".into(), api_key),
        ];

        let response = http::request("GET", &search_url, &headers, None)
            .map_err(|e| format!("search request failed: {e}"))?;

        if response.status != 200 {
            return Err(format!(
                "search API returned status {}: {}",
                response.status,
                &response.body[..response.body.len().min(200)]
            ));
        }

        // Parse Brave Search API response format
        let body: serde_json::Value = serde_json::from_str(&response.body)
            .map_err(|e| format!("failed to parse search response: {e}"))?;

        let results: Vec<serde_json::Value> = body["web"]["results"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .take(max_results as usize)
            .map(|r| {
                serde_json::json!({
                    "title": r["title"].as_str().unwrap_or(""),
                    "url": r["url"].as_str().unwrap_or(""),
                    "snippet": r["description"].as_str().unwrap_or(""),
                })
            })
            .collect();

        Ok(serde_json::json!({
            "results": results,
            "query": query,
            "count": results.len(),
        })
        .to_string())
    }
}

/// Minimal percent-encoding for URL query parameters.
fn urlencoded(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for ch in s.chars() {
        match ch {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(ch),
            ' ' => out.push('+'),
            other => {
                for byte in other.to_string().as_bytes() {
                    out.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    out
}

const INPUT_SCHEMA: &str = r#"{"type":"object","required":["query"],"properties":{"query":{"type":"string","description":"Search query"},"max_results":{"type":"integer","default":5,"description":"Maximum results to return"}}}"#;

const OUTPUT_SCHEMA: &str = r#"{"type":"object","properties":{"results":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"url":{"type":"string"},"snippet":{"type":"string"}}}},"query":{"type":"string"},"count":{"type":"integer"}}}"#;

export!(WebSearchConnector);
