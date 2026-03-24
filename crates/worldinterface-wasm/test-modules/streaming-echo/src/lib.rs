//! Streaming Echo test module — minimal WASM streaming connector for integration tests.

wit_bindgen::generate!({
    world: "streaming-connector-world",
    path: "../../wit",
});

struct StreamingEchoConnector;

impl exports::exo::connector::connector::Guest for StreamingEchoConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "test.streaming-echo".to_string(),
            display_name: "Test Streaming Echo".to_string(),
            description: "Streaming connector that echoes messages for testing".to_string(),
            input_schema: None,
            output_schema: None,
            idempotent: true,
            side_effects: false,
        }
    }

    fn invoke(
        _ctx: exports::exo::connector::connector::InvocationContext,
        params: String,
    ) -> Result<String, String> {
        // Echo: return params unchanged
        Ok(params)
    }
}

impl exports::exo::connector::streaming_connector::Guest for StreamingEchoConnector {
    fn stream_config() -> exports::exo::connector::streaming_connector::StreamSetup {
        exports::exo::connector::streaming_connector::StreamSetup {
            url: "ws://127.0.0.1:0/test".to_string(),
            headers: vec![],
            init_messages: vec!["hello".to_string()],
            heartbeat_interval_ms: 1000,
            heartbeat_payload: Some("{\"ping\":true}".to_string()),
        }
    }

    fn on_message(raw: String) -> Vec<exports::exo::connector::streaming_connector::StreamMessage> {
        vec![exports::exo::connector::streaming_connector::StreamMessage {
            source_identity: "test:echo:1".to_string(),
            content: raw,
            metadata: vec![],
        }]
    }
}

export!(StreamingEchoConnector);
