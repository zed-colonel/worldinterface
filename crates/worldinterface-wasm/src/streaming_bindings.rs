//! Generated bindings for the streaming-connector-world.
//!
//! Separate from connector.rs to avoid name collisions with ConnectorWorld.
//! Produces `StreamingConnectorWorld` with access to both `connector` and
//! `streaming-connector` exports.

wasmtime::component::bindgen!({
    world: "streaming-connector-world",
    path: "wit",
    imports: {
        "exo:connector/logging@0.1.0": trappable,
        "exo:connector/kv@0.1.0": trappable,
        "exo:connector/crypto@0.1.0": trappable,
        "exo:connector/process@0.1.0": trappable,
        "exo:connector/http@0.1.0": trappable,
        "exo:connector/websocket@0.1.0": trappable,
    },
});
