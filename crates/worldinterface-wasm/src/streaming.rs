//! Streaming lifecycle manager for WASM connectors.
//!
//! Manages host-side WebSocket connections for modules that export the
//! streaming-connector interface. The module provides configuration and
//! message parsing; this manager handles I/O, heartbeats, and reconnection.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use wasmtime::Store;
use worldinterface_core::streaming::{StreamMessage, StreamMessageHandler};

use crate::metering::EPOCH_INTERVAL_MS;
use crate::module_loader::StreamingModuleInfo;
use crate::state::WasmState;
use crate::streaming_bindings::StreamingConnectorWorld;

/// Internal: parsed stream-config result.
struct StreamConfig {
    url: String,
    headers: Vec<(String, String)>,
    init_messages: Vec<String>,
    heartbeat_interval_ms: u32,
    heartbeat_payload: Option<String>,
}

/// Manages the background WebSocket lifecycle for a streaming WASM connector.
pub struct StreamingLifecycleManager {
    /// Module info (runtime, component, manifest, policy).
    info: StreamingModuleInfo,
    /// Handler to receive parsed messages.
    handler: Arc<dyn StreamMessageHandler>,
    /// Shutdown signal.
    shutdown: Arc<AtomicBool>,
    /// Background task handle.
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl StreamingLifecycleManager {
    /// Create a new lifecycle manager. Does NOT start the background task yet.
    pub fn new(
        info: StreamingModuleInfo,
        handler: Arc<dyn StreamMessageHandler>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self { info, handler, shutdown, task_handle: None }
    }

    /// The connector name from the manifest.
    pub fn connector_name(&self) -> &str {
        &self.info.manifest.connector.name
    }

    /// Start the background lifecycle task.
    ///
    /// Calls stream-config() to get connection parameters, then spawns a
    /// tokio task that maintains the WebSocket connection.
    pub fn start(&mut self) -> Result<(), String> {
        // 1. Call stream-config() on a blocking thread
        let config = self.call_stream_config()?;

        tracing::info!(
            connector = %self.info.manifest.connector.name,
            url = %config.url,
            heartbeat_ms = config.heartbeat_interval_ms,
            "starting streaming lifecycle"
        );

        // 2. Spawn background task
        let info = StreamingModuleInfo {
            runtime: Arc::clone(&self.info.runtime),
            component: self.info.component.clone(),
            manifest: self.info.manifest.clone(),
            policy: Arc::clone(&self.info.policy),
            env_overrides: self.info.env_overrides.clone(),
        };
        let handler = Arc::clone(&self.handler);
        let shutdown = Arc::clone(&self.shutdown);

        let handle = tokio::spawn(async move {
            streaming_loop(info, config, handler, shutdown).await;
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// Call stream-config() on the WASM module.
    fn call_stream_config(&self) -> Result<StreamConfig, String> {
        let wasi_ctx = wasmtime_wasi::WasiCtxBuilder::new().build();
        let mut store = Store::new(
            self.info.runtime.engine(),
            WasmState {
                wasi_ctx,
                resource_table: wasmtime::component::ResourceTable::new(),
                policy: Arc::clone(&self.info.policy),
                resource_pool: Arc::clone(self.info.runtime.resource_pool()),
                module_name: self.info.manifest.connector.name.clone(),
            },
        );
        store.set_fuel(self.info.policy.max_fuel).map_err(|e| format!("fuel setup: {e}"))?;

        let epoch_ticks = self.info.policy.timeout.as_millis() as u64 / EPOCH_INTERVAL_MS;
        store.epoch_deadline_trap();
        store.set_epoch_deadline(epoch_ticks);

        let bindings = StreamingConnectorWorld::instantiate(
            &mut store,
            &self.info.component,
            self.info.runtime.linker(),
        )
        .map_err(|e| format!("streaming instantiation: {e}"))?;

        let setup = bindings
            .exo_connector_streaming_connector()
            .call_stream_config(&mut store)
            .map_err(|e| format!("stream-config call: {e}"))?;

        Ok(StreamConfig {
            url: setup.url,
            headers: setup.headers,
            init_messages: setup.init_messages,
            heartbeat_interval_ms: setup.heartbeat_interval_ms,
            heartbeat_payload: setup.heartbeat_payload,
        })
    }

    /// Request shutdown and wait for the background task to complete.
    pub async fn shutdown(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.task_handle {
            let _ = handle.await;
        }
    }
}

/// Background task: maintain WebSocket connection, dispatch messages.
async fn streaming_loop(
    info: StreamingModuleInfo,
    config: StreamConfig,
    handler: Arc<dyn StreamMessageHandler>,
    shutdown: Arc<AtomicBool>,
) {
    let connector_name = info.manifest.connector.name.clone();
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        if shutdown.load(Ordering::SeqCst) {
            tracing::info!(connector = %connector_name, "streaming shutdown requested");
            return;
        }

        // 1. Connect
        let stream = match connect_websocket(&config).await {
            Ok(stream) => {
                backoff = Duration::from_secs(1); // Reset backoff on success
                stream
            }
            Err(e) => {
                tracing::warn!(
                    connector = %connector_name,
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "streaming connect failed, retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        tracing::info!(connector = %connector_name, "streaming connected");

        let (mut write, mut read) = stream.split();

        // 2. Send init messages
        for msg in &config.init_messages {
            if let Err(e) = write.send(Message::Text(msg.clone().into())).await {
                tracing::warn!(
                    connector = %connector_name,
                    error = %e,
                    "init message send failed"
                );
                break;
            }
        }

        // 3. Start heartbeat + message loop
        let heartbeat_interval = if config.heartbeat_interval_ms > 0 {
            Some(Duration::from_millis(config.heartbeat_interval_ms as u64))
        } else {
            None
        };

        let mut heartbeat_timer = heartbeat_interval.map(tokio::time::interval);
        if let Some(ref mut timer) = heartbeat_timer {
            timer.tick().await; // Skip the first immediate tick
        }

        // Poll shutdown flag every 250ms so we can exit promptly even when
        // the WebSocket is idle and no heartbeat is configured.
        let mut shutdown_poll = tokio::time::interval(Duration::from_millis(250));
        shutdown_poll.tick().await; // Skip the first immediate tick

        loop {
            if shutdown.load(Ordering::SeqCst) {
                let _ = write.close().await;
                return;
            }

            tokio::select! {
                // Shutdown poll — check flag periodically
                _ = shutdown_poll.tick() => {
                    if shutdown.load(Ordering::SeqCst) {
                        let _ = write.close().await;
                        return;
                    }
                }

                // Heartbeat tick
                _ = async {
                    match heartbeat_timer.as_mut() {
                        Some(timer) => timer.tick().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(ref payload) = config.heartbeat_payload {
                        if let Err(e) = write.send(Message::Text(payload.clone().into())).await {
                            tracing::warn!(
                                connector = %connector_name,
                                error = %e,
                                "heartbeat send failed"
                            );
                            break; // Reconnect
                        }
                    }
                }

                // Incoming WebSocket frame
                frame = read.next() => {
                    match frame {
                        Some(Ok(Message::Text(raw))) => {
                            let raw_str = raw.to_string();
                            match call_on_message(&info, &raw_str) {
                                Ok(messages) if !messages.is_empty() => {
                                    if let Err(e) =
                                        handler.handle_messages(&connector_name, messages)
                                    {
                                        tracing::error!(
                                            connector = %connector_name,
                                            error = %e,
                                            "stream message handler failed"
                                        );
                                    }
                                }
                                Ok(_) => {} // Empty list — protocol frame, no app messages
                                Err(e) => {
                                    tracing::warn!(
                                        connector = %connector_name,
                                        error = %e,
                                        "on_message call failed"
                                    );
                                }
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            tracing::info!(
                                connector = %connector_name,
                                "streaming connection closed by remote"
                            );
                            break; // Reconnect
                        }
                        Some(Ok(_)) => {} // Ping/Pong/Binary — ignore
                        Some(Err(e)) => {
                            tracing::warn!(
                                connector = %connector_name,
                                error = %e,
                                "streaming read error"
                            );
                            break; // Reconnect
                        }
                        None => {
                            tracing::info!(
                                connector = %connector_name,
                                "streaming connection ended"
                            );
                            break; // Reconnect
                        }
                    }
                }
            }
        }
    }
}

/// Establish a WebSocket connection using the stream config.
async fn connect_websocket(
    config: &StreamConfig,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let mut request = config
        .url
        .clone()
        .into_client_request()
        .map_err(|e| format!("invalid WebSocket URL: {e}"))?;

    for (key, value) in &config.headers {
        if let (Ok(name), Ok(val)) = (
            key.parse::<tokio_tungstenite::tungstenite::http::HeaderName>(),
            value.parse::<tokio_tungstenite::tungstenite::http::HeaderValue>(),
        ) {
            request.headers_mut().insert(name, val);
        }
    }

    let (stream, _response) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| format!("WebSocket connect failed: {e}"))?;

    Ok(stream)
}

/// Call the WASM module's on_message() export.
///
/// Creates a fresh Store for each call (same pattern as invoke()).
fn call_on_message(info: &StreamingModuleInfo, raw: &str) -> Result<Vec<StreamMessage>, String> {
    let wasi_ctx = wasmtime_wasi::WasiCtxBuilder::new().build();
    let mut store = Store::new(
        info.runtime.engine(),
        WasmState {
            wasi_ctx,
            resource_table: wasmtime::component::ResourceTable::new(),
            policy: Arc::clone(&info.policy),
            resource_pool: Arc::clone(info.runtime.resource_pool()),
            module_name: info.manifest.connector.name.clone(),
        },
    );
    store.set_fuel(info.policy.max_fuel).map_err(|e| format!("fuel setup: {e}"))?;

    let epoch_ticks = info.policy.timeout.as_millis() as u64 / EPOCH_INTERVAL_MS;
    store.epoch_deadline_trap();
    store.set_epoch_deadline(epoch_ticks);

    let bindings =
        StreamingConnectorWorld::instantiate(&mut store, &info.component, info.runtime.linker())
            .map_err(|e| format!("streaming instantiation: {e}"))?;

    let results = bindings
        .exo_connector_streaming_connector()
        .call_on_message(&mut store, raw)
        .map_err(|e| format!("on_message call: {e}"))?;

    Ok(results
        .into_iter()
        .map(|m| StreamMessage {
            source_identity: m.source_identity,
            content: m.content,
            metadata: m.metadata.into_iter().collect(),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    type ReceivedMessages = Vec<(String, Vec<StreamMessage>)>;

    struct MockHandler {
        received: Arc<Mutex<ReceivedMessages>>,
    }

    impl StreamMessageHandler for MockHandler {
        fn handle_messages(
            &self,
            connector_name: &str,
            messages: Vec<StreamMessage>,
        ) -> Result<(), String> {
            self.received.lock().unwrap().push((connector_name.to_string(), messages));
            Ok(())
        }
    }

    // ── Sub-component tests (always available) ──

    #[test]
    fn stream_config_struct_fields() {
        let config = StreamConfig {
            url: "wss://gateway.discord.gg/?v=10".into(),
            headers: vec![("Authorization".into(), "Bot token".into())],
            init_messages: vec![r#"{"op":2}"#.into()],
            heartbeat_interval_ms: 41250,
            heartbeat_payload: Some(r#"{"op":1,"d":null}"#.into()),
        };
        assert_eq!(config.url, "wss://gateway.discord.gg/?v=10");
        assert_eq!(config.headers.len(), 1);
        assert_eq!(config.init_messages.len(), 1);
        assert_eq!(config.heartbeat_interval_ms, 41250);
        assert!(config.heartbeat_payload.is_some());
    }

    #[test]
    fn mock_handler_receives_messages() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let handler = MockHandler { received: received.clone() };
        let messages = vec![
            StreamMessage {
                source_identity: "discord:user:111".into(),
                content: "hello".into(),
                metadata: [("channel_id".into(), "ch-1".into())].into_iter().collect(),
            },
            StreamMessage {
                source_identity: "discord:user:222".into(),
                content: "world".into(),
                metadata: Default::default(),
            },
        ];
        handler.handle_messages("discord", messages).unwrap();

        let locked = received.lock().unwrap();
        assert_eq!(locked.len(), 1);
        assert_eq!(locked[0].0, "discord");
        assert_eq!(locked[0].1.len(), 2);
        assert_eq!(locked[0].1[0].content, "hello");
        assert_eq!(locked[0].1[1].content, "world");
    }

    #[tokio::test]
    async fn connect_websocket_rejects_invalid_url() {
        let config = StreamConfig {
            url: "not-a-url".into(),
            headers: vec![],
            init_messages: vec![],
            heartbeat_interval_ms: 0,
            heartbeat_payload: None,
        };
        let result = connect_websocket(&config).await;
        assert!(result.is_err());
    }

    #[test]
    fn lifecycle_manager_connector_name() {
        let dir = tempfile::tempdir().unwrap();
        let rt_config = crate::runtime::WasmRuntimeConfig {
            kv_store_dir: dir.path().join("kv"),
            ..Default::default()
        };
        let runtime = Arc::new(crate::runtime::WasmRuntime::new(rt_config).unwrap());

        let wat = r#"
            (component
                (core module $m
                    (memory (export "memory") 1)
                    (func (export "dummy"))
                )
            )
        "#;
        let component =
            wasmtime::component::Component::new(runtime.engine(), wat.as_bytes()).unwrap();

        let manifest = crate::manifest::ConnectorManifest {
            connector: crate::manifest::ManifestConnector {
                name: "test.stream".into(),
                version: "0.1.0".into(),
                description: "test".into(),
                streaming: true,
            },
            capabilities: Default::default(),
            resources: Default::default(),
        };

        let info = StreamingModuleInfo {
            runtime: Arc::clone(&runtime),
            component,
            manifest,
            policy: Arc::new(crate::policy::CapabilityPolicy::deny_all()),
            env_overrides: Default::default(),
        };

        let received = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn StreamMessageHandler> = Arc::new(MockHandler { received });
        let shutdown = Arc::new(AtomicBool::new(false));

        let manager = StreamingLifecycleManager::new(info, handler, shutdown);
        assert_eq!(manager.connector_name(), "test.stream");
    }

    #[tokio::test]
    async fn shutdown_without_start_is_safe() {
        // WasmRuntime contains reqwest::blocking::Client which panics if
        // dropped in async context. Create and drop on a blocking thread.
        let (manager, _dir) = tokio::task::spawn_blocking(|| {
            let dir = tempfile::tempdir().unwrap();
            let rt_config = crate::runtime::WasmRuntimeConfig {
                kv_store_dir: dir.path().join("kv"),
                ..Default::default()
            };
            let runtime = Arc::new(crate::runtime::WasmRuntime::new(rt_config).unwrap());

            let wat = r#"(component (core module $m (memory (export "memory") 1)))"#;
            let component =
                wasmtime::component::Component::new(runtime.engine(), wat.as_bytes()).unwrap();

            let manifest = crate::manifest::ConnectorManifest {
                connector: crate::manifest::ManifestConnector {
                    name: "test.shutdown".into(),
                    version: "0.1.0".into(),
                    description: "test".into(),
                    streaming: true,
                },
                capabilities: Default::default(),
                resources: Default::default(),
            };

            let info = StreamingModuleInfo {
                runtime,
                component,
                manifest,
                policy: Arc::new(crate::policy::CapabilityPolicy::deny_all()),
                env_overrides: Default::default(),
            };

            let received = Arc::new(Mutex::new(Vec::new()));
            let handler: Arc<dyn StreamMessageHandler> = Arc::new(MockHandler { received });
            let shutdown_flag = Arc::new(AtomicBool::new(false));

            let manager = StreamingLifecycleManager::new(info, handler, shutdown_flag);
            (manager, dir)
        })
        .await
        .unwrap();

        // Shutdown without starting should not panic
        manager.shutdown().await;
    }

    // ── E4S3-T5 through T10: Streaming lifecycle integration tests ──
    //
    // These tests use the streaming-echo WASM test module and a mock WebSocket
    // echo server to exercise the full streaming_loop flow.
    // Gated behind wasm-tests feature because they require compiled test modules.

    /// Spawn a WebSocket echo server on a random port. Returns the URL and
    /// a handle to abort the server when done.
    #[cfg(feature = "wasm-tests")]
    async fn spawn_ws_echo_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    let (mut write, mut read) = ws.split();
                    while let Some(Ok(msg)) = read.next().await {
                        if (msg.is_text() || msg.is_binary()) && write.send(msg).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        (format!("ws://127.0.0.1:{}", addr.port()), handle)
    }

    /// Load the streaming-echo test module and build StreamingModuleInfo.
    /// Requires compiled test modules (gated behind wasm-tests feature).
    #[cfg(feature = "wasm-tests")]
    fn load_streaming_echo_info(runtime: &Arc<crate::runtime::WasmRuntime>) -> StreamingModuleInfo {
        let compiled_dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-modules/compiled");
        let wasm_path = compiled_dir.join("streaming-echo.wasm");
        let manifest_path = compiled_dir.join("streaming-echo.connector.toml");

        let manifest_content = std::fs::read_to_string(&manifest_path)
            .expect("streaming-echo.connector.toml should exist");
        let manifest = crate::manifest::ConnectorManifest::from_toml(&manifest_content)
            .expect("streaming-echo manifest should parse");
        let policy = crate::policy::CapabilityPolicy::from_manifest(&manifest)
            .expect("policy should compile");

        let wasm_bytes = std::fs::read(&wasm_path).expect("streaming-echo.wasm should exist");
        let component = wasmtime::component::Component::new(runtime.engine(), &wasm_bytes)
            .expect("streaming-echo should compile");

        StreamingModuleInfo {
            runtime: Arc::clone(runtime),
            component,
            manifest,
            policy: Arc::new(policy),
            env_overrides: Default::default(),
        }
    }

    // ── E4S3-T5: Streaming lifecycle connects to mock WS server ──

    #[tokio::test]
    #[cfg(feature = "wasm-tests")]
    async fn streaming_lifecycle_connects() {
        let (ws_url, server_handle) = spawn_ws_echo_server().await;

        let (info, _dir, received, shutdown) = tokio::task::spawn_blocking(move || {
            let dir = tempfile::tempdir().unwrap();
            let rt_config = crate::runtime::WasmRuntimeConfig {
                kv_store_dir: dir.path().join("kv"),
                ..Default::default()
            };
            let runtime = Arc::new(crate::runtime::WasmRuntime::new(rt_config).unwrap());
            let mut info = load_streaming_echo_info(&runtime);
            // Override the URL to point at our mock server
            info.manifest.connector.name = "test.streaming-echo".into();
            let received = Arc::new(Mutex::new(Vec::<(String, Vec<StreamMessage>)>::new()));
            let shutdown = Arc::new(AtomicBool::new(false));
            (info, dir, received, shutdown)
        })
        .await
        .unwrap();

        // Build a StreamConfig that points at the mock server
        let config = StreamConfig {
            url: ws_url,
            headers: vec![],
            init_messages: vec![],
            heartbeat_interval_ms: 0,
            heartbeat_payload: None,
        };

        let handler: Arc<dyn StreamMessageHandler> =
            Arc::new(MockHandler { received: received.clone() });
        let shutdown_clone = Arc::clone(&shutdown);

        // Spawn streaming_loop — it should connect successfully
        let loop_handle = tokio::spawn(async move {
            streaming_loop(info, config, handler, shutdown_clone).await;
        });

        // Give it time to connect
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Signal shutdown
        shutdown.store(true, Ordering::SeqCst);
        let _ = tokio::time::timeout(Duration::from_secs(5), loop_handle).await;
        server_handle.abort();
        // If we got here without panic, connection succeeded
    }

    // ── E4S3-T6: Streaming lifecycle sends init messages ──

    #[tokio::test]
    #[cfg(feature = "wasm-tests")]
    async fn streaming_lifecycle_sends_init_messages() {
        // Spawn a WS server that records received messages
        let init_received = Arc::new(Mutex::new(Vec::<String>::new()));
        let init_received_clone = init_received.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (mut _write, mut read) = ws.split();
                while let Some(Ok(msg)) = read.next().await {
                    if let Message::Text(text) = msg {
                        init_received_clone.lock().unwrap().push(text.to_string());
                    }
                }
            }
        });

        let ws_url = format!("ws://127.0.0.1:{}", addr.port());

        let (_dir, shutdown) = {
            let dir = tempfile::tempdir().unwrap();
            let shutdown = Arc::new(AtomicBool::new(false));
            (dir, shutdown)
        };

        let (info, _dir2) = tokio::task::spawn_blocking(|| {
            let dir = tempfile::tempdir().unwrap();
            let rt_config = crate::runtime::WasmRuntimeConfig {
                kv_store_dir: dir.path().join("kv"),
                ..Default::default()
            };
            let runtime = Arc::new(crate::runtime::WasmRuntime::new(rt_config).unwrap());
            let info = load_streaming_echo_info(&runtime);
            (info, dir)
        })
        .await
        .unwrap();

        let config = StreamConfig {
            url: ws_url,
            headers: vec![],
            init_messages: vec!["init-msg-1".into(), "init-msg-2".into()],
            heartbeat_interval_ms: 0,
            heartbeat_payload: None,
        };

        let received = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn StreamMessageHandler> = Arc::new(MockHandler { received });
        let shutdown_clone = Arc::clone(&shutdown);

        let loop_handle = tokio::spawn(async move {
            streaming_loop(info, config, handler, shutdown_clone).await;
        });

        // Wait for init messages to arrive at server
        tokio::time::sleep(Duration::from_millis(300)).await;

        shutdown.store(true, Ordering::SeqCst);
        let _ = tokio::time::timeout(Duration::from_secs(5), loop_handle).await;
        server_handle.abort();

        let msgs = init_received.lock().unwrap();
        assert!(
            msgs.len() >= 2,
            "server should have received at least 2 init messages, got {}",
            msgs.len()
        );
        assert_eq!(msgs[0], "init-msg-1");
        assert_eq!(msgs[1], "init-msg-2");
    }

    // ── E4S3-T7: Streaming lifecycle sends heartbeats ──

    #[tokio::test]
    #[cfg(feature = "wasm-tests")]
    async fn streaming_lifecycle_heartbeat() {
        let heartbeats = Arc::new(Mutex::new(Vec::<String>::new()));
        let heartbeats_clone = heartbeats.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (_write, mut read) = ws.split();
                while let Some(Ok(msg)) = read.next().await {
                    if let Message::Text(text) = msg {
                        heartbeats_clone.lock().unwrap().push(text.to_string());
                    }
                }
            }
        });

        let ws_url = format!("ws://127.0.0.1:{}", addr.port());
        let shutdown = Arc::new(AtomicBool::new(false));

        let (info, _dir) = tokio::task::spawn_blocking(|| {
            let dir = tempfile::tempdir().unwrap();
            let rt_config = crate::runtime::WasmRuntimeConfig {
                kv_store_dir: dir.path().join("kv"),
                ..Default::default()
            };
            let runtime = Arc::new(crate::runtime::WasmRuntime::new(rt_config).unwrap());
            let info = load_streaming_echo_info(&runtime);
            (info, dir)
        })
        .await
        .unwrap();

        let config = StreamConfig {
            url: ws_url,
            headers: vec![],
            init_messages: vec![],
            heartbeat_interval_ms: 100, // Fast heartbeat for testing
            heartbeat_payload: Some(r#"{"ping":true}"#.into()),
        };

        let received = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn StreamMessageHandler> = Arc::new(MockHandler { received });
        let shutdown_clone = Arc::clone(&shutdown);

        let loop_handle = tokio::spawn(async move {
            streaming_loop(info, config, handler, shutdown_clone).await;
        });

        // Wait for at least 2 heartbeats (100ms interval, wait 350ms)
        tokio::time::sleep(Duration::from_millis(350)).await;

        shutdown.store(true, Ordering::SeqCst);
        let _ = tokio::time::timeout(Duration::from_secs(5), loop_handle).await;
        server_handle.abort();

        let hbs = heartbeats.lock().unwrap();
        assert!(hbs.len() >= 2, "should have received at least 2 heartbeats, got {}", hbs.len());
        assert!(hbs.iter().all(|h| h == r#"{"ping":true}"#));
    }

    // ── E4S3-T8: Streaming lifecycle reconnects on disconnect ──

    #[tokio::test]
    #[cfg(feature = "wasm-tests")]
    async fn streaming_lifecycle_reconnects_on_disconnect() {
        let connection_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let connection_count_clone = connection_count.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            // Accept connections — close each one immediately to force reconnect
            while let Ok((stream, _)) = listener.accept().await {
                connection_count_clone.fetch_add(1, Ordering::SeqCst);
                let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (mut write, _read) = ws.split();
                // Close immediately
                let _ = write.close().await;
            }
        });

        let ws_url = format!("ws://127.0.0.1:{}", addr.port());
        let shutdown = Arc::new(AtomicBool::new(false));

        let (info, _dir) = tokio::task::spawn_blocking(|| {
            let dir = tempfile::tempdir().unwrap();
            let rt_config = crate::runtime::WasmRuntimeConfig {
                kv_store_dir: dir.path().join("kv"),
                ..Default::default()
            };
            let runtime = Arc::new(crate::runtime::WasmRuntime::new(rt_config).unwrap());
            let info = load_streaming_echo_info(&runtime);
            (info, dir)
        })
        .await
        .unwrap();

        let config = StreamConfig {
            url: ws_url,
            headers: vec![],
            init_messages: vec![],
            heartbeat_interval_ms: 0,
            heartbeat_payload: None,
        };

        let received = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn StreamMessageHandler> = Arc::new(MockHandler { received });
        let shutdown_clone = Arc::clone(&shutdown);

        let loop_handle = tokio::spawn(async move {
            streaming_loop(info, config, handler, shutdown_clone).await;
        });

        // Wait for reconnection attempts (backoff starts at 1s, so wait ~2.5s)
        tokio::time::sleep(Duration::from_millis(2500)).await;

        shutdown.store(true, Ordering::SeqCst);
        let _ = tokio::time::timeout(Duration::from_secs(5), loop_handle).await;
        server_handle.abort();

        let count = connection_count.load(Ordering::SeqCst);
        assert!(
            count >= 2,
            "should have connected at least twice (initial + reconnect), got {count}"
        );
    }

    // ── E4S3-T9: Streaming lifecycle shutdown stops the loop ──

    #[tokio::test]
    #[cfg(feature = "wasm-tests")]
    async fn streaming_lifecycle_shutdown() {
        let (ws_url, server_handle) = spawn_ws_echo_server().await;
        let shutdown = Arc::new(AtomicBool::new(false));

        let (info, _dir) = tokio::task::spawn_blocking(|| {
            let dir = tempfile::tempdir().unwrap();
            let rt_config = crate::runtime::WasmRuntimeConfig {
                kv_store_dir: dir.path().join("kv"),
                ..Default::default()
            };
            let runtime = Arc::new(crate::runtime::WasmRuntime::new(rt_config).unwrap());
            let info = load_streaming_echo_info(&runtime);
            (info, dir)
        })
        .await
        .unwrap();

        let config = StreamConfig {
            url: ws_url,
            headers: vec![],
            init_messages: vec![],
            heartbeat_interval_ms: 0,
            heartbeat_payload: None,
        };

        let received = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn StreamMessageHandler> = Arc::new(MockHandler { received });
        let shutdown_clone = Arc::clone(&shutdown);

        let loop_handle = tokio::spawn(async move {
            streaming_loop(info, config, handler, shutdown_clone).await;
        });

        // Let it connect
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Signal shutdown
        shutdown.store(true, Ordering::SeqCst);

        // The loop should exit promptly (within 2s)
        let result = tokio::time::timeout(Duration::from_secs(2), loop_handle).await;
        assert!(result.is_ok(), "streaming loop should exit within 2s of shutdown signal");
        server_handle.abort();
    }

    // ── E4S3-T10: Streaming on_message forwards to handler ──

    #[tokio::test]
    #[cfg(feature = "wasm-tests")]
    async fn streaming_on_message_forwards_to_handler() {
        // Spawn a WS server that sends a message to the client after accepting
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (mut write, _read) = ws.split();
                // Wait a moment then send a message
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = write.send(Message::Text("hello from server".into())).await;
                // Keep connection open briefly
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });

        let ws_url = format!("ws://127.0.0.1:{}", addr.port());
        let shutdown = Arc::new(AtomicBool::new(false));

        let (info, _dir) = tokio::task::spawn_blocking(|| {
            let dir = tempfile::tempdir().unwrap();
            let rt_config = crate::runtime::WasmRuntimeConfig {
                kv_store_dir: dir.path().join("kv"),
                ..Default::default()
            };
            let runtime = Arc::new(crate::runtime::WasmRuntime::new(rt_config).unwrap());
            let info = load_streaming_echo_info(&runtime);
            (info, dir)
        })
        .await
        .unwrap();

        let config = StreamConfig {
            url: ws_url,
            headers: vec![],
            init_messages: vec![],
            heartbeat_interval_ms: 0,
            heartbeat_payload: None,
        };

        let received = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<dyn StreamMessageHandler> =
            Arc::new(MockHandler { received: received.clone() });
        let shutdown_clone = Arc::clone(&shutdown);

        let loop_handle = tokio::spawn(async move {
            streaming_loop(info, config, handler, shutdown_clone).await;
        });

        // Wait for the message to be processed through WASM on_message → handler
        tokio::time::sleep(Duration::from_millis(500)).await;

        shutdown.store(true, Ordering::SeqCst);
        let _ = tokio::time::timeout(Duration::from_secs(5), loop_handle).await;
        server_handle.abort();

        let msgs = received.lock().unwrap();
        assert!(!msgs.is_empty(), "handler should have received at least one message batch");
        // The streaming-echo module returns source_identity="test:echo:1" and content=raw
        assert_eq!(msgs[0].1[0].source_identity, "test:echo:1");
        assert_eq!(msgs[0].1[0].content, "hello from server");
    }
}
