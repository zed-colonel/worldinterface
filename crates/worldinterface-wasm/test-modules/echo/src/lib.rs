//! Echo test module — minimal WASM connector that echoes params back.

wit_bindgen::generate!({
    world: "connector-world",
    path: "../../wit",
});

struct EchoConnector;

impl exports::exo::connector::connector::Guest for EchoConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "test.echo".to_string(),
            display_name: "Test Echo".to_string(),
            description: "Echoes input parameters back as output".to_string(),
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

export!(EchoConnector);
