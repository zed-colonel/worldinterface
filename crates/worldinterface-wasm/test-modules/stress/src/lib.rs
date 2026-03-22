//! Stress test module — exercises fuel exhaustion, memory limits, and timeouts.

wit_bindgen::generate!({
    world: "connector-world",
    path: "../../wit",
});

struct StressConnector;

impl exports::exo::connector::connector::Guest for StressConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "test.stress".to_string(),
            display_name: "Test Stress".to_string(),
            description: "Stress tests for metering safety".to_string(),
            input_schema: None,
            output_schema: None,
            idempotent: false,
            side_effects: false,
        }
    }

    fn invoke(
        _ctx: exports::exo::connector::connector::InvocationContext,
        params: String,
    ) -> Result<String, String> {
        if params.contains("\"action\":\"loop\"") || params.contains("\"action\": \"loop\"") {
            // Infinite loop — should be caught by fuel exhaustion or epoch deadline
            #[allow(clippy::empty_loop)]
            loop {}
        }

        if params.contains("\"action\":\"allocate\"")
            || params.contains("\"action\": \"allocate\"")
        {
            // Allocate excessive memory — should be caught by memory limit
            let mut vecs: Vec<Vec<u8>> = Vec::new();
            loop {
                // Allocate 1MB chunks until memory limit hit
                vecs.push(vec![0u8; 1_048_576]);
            }
        }

        Err(format!("unknown stress action in params: {params}"))
    }
}

export!(StressConnector);
