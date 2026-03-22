//! Host Function Demo — reference WASM connector exercising logging, crypto, and KV.

wit_bindgen::generate!({
    world: "connector-world",
    path: "../../../crates/worldinterface-wasm/wit",
});

use exo::connector::{crypto, kv, logging};

struct HostDemoConnector;

impl exports::exo::connector::connector::Guest for HostDemoConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "demo.host-functions".to_string(),
            display_name: "Host Function Demo".to_string(),
            description: "Demonstrates logging, crypto, and KV host functions.".to_string(),
            input_schema: Some(
                r#"{"type":"object","required":["message"],"properties":{"message":{"type":"string"}}}"#
                    .to_string(),
            ),
            output_schema: None,
            idempotent: true,
            side_effects: false,
        }
    }

    fn invoke(
        _ctx: exports::exo::connector::connector::InvocationContext,
        params: String,
    ) -> Result<String, String> {
        let input: serde_json::Value =
            serde_json::from_str(&params).map_err(|e| format!("invalid params: {e}"))?;

        let message = input
            .get("message")
            .and_then(|v| v.as_str())
            .ok_or("missing 'message' string field")?;

        // 1. Log via host logging interface
        logging::log(
            logging::LogLevel::Info,
            &format!("processing: {message}"),
        );

        // 2. Hash via host crypto interface
        let digest = crypto::sha256(message.as_bytes());

        // 3. Store in KV via host KV interface
        kv::set("last-message", message);
        let retrieved = kv::get("last-message");

        let result = serde_json::json!({
            "message": message,
            "sha256": digest,
            "stored": retrieved.as_deref() == Some(message),
        });

        serde_json::to_string(&result).map_err(|e| format!("serialization error: {e}"))
    }
}

export!(HostDemoConnector);
