//! EmbeddedHost — the primary programmatic API surface for WorldInterface.

use std::collections::HashMap;
use std::sync::Arc;

use actionqueue_core::ids::TaskId;
use actionqueue_runtime::engine::ActionQueueEngine;
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use worldinterface_connector::ConnectorRegistry;
use worldinterface_contextstore::ContextStore;
use worldinterface_contextstore::SqliteContextStore;
use worldinterface_coordinator::FlowHandler;
use worldinterface_core::descriptor::Descriptor;
use worldinterface_core::flowspec::FlowSpec;
use worldinterface_core::id::{trigger_input_node_id, FlowRunId};

use crate::config::HostConfig;
use crate::error::HostError;
use crate::helpers::{
    build_single_node_flowspec, persist_coordinator_map, restore_coordinator_map,
};
use crate::status::{
    derive_flow_run_status, extract_flow_outputs, FlowPhase, FlowRunStatus, FlowRunSummary,
};
use crate::tick::{tick_loop, Engine};

/// The engine is wrapped in `Option` so it can be taken out and dropped on a
/// blocking thread (AQ's internal tokio runtime panics if dropped from async context).
pub(crate) type EngineSlot = Arc<tokio::sync::Mutex<Option<Engine>>>;

/// Shared inner state accessible from both the public API and the tick loop.
pub(crate) struct HostInner {
    pub engine: EngineSlot,
    pub store: Arc<SqliteContextStore>,
    pub registry: Arc<ConnectorRegistry>,
    pub coordinator_map: Arc<std::sync::Mutex<HashMap<FlowRunId, TaskId>>>,
    pub compiler_config: worldinterface_flowspec::CompilerConfig,
    pub shutdown_tx: watch::Sender<bool>,
    /// Keeps the WASM runtime alive (epoch ticker, resource pool).
    /// Wrapped in Mutex<Option> so it can be taken out and dropped on a blocking
    /// thread during shutdown (reqwest::blocking::Client panics if dropped in async context).
    #[cfg(feature = "wasm")]
    pub wasm_runtime: std::sync::Mutex<Option<Arc<worldinterface_wasm::WasmRuntime>>>,
}

/// The embedded WorldInterface host.
///
/// Owns the AQ engine, ContextStore, and ConnectorRegistry. Provides the full
/// lifecycle API: flow submission, status queries, capability discovery, and
/// single-op invocation.
pub struct EmbeddedHost {
    pub(crate) inner: Arc<HostInner>,
    tick_handle: JoinHandle<()>,
}

impl EmbeddedHost {
    /// Start the host: validate config, bootstrap AQ engine, restore coordinator map,
    /// and launch the background tick loop.
    #[allow(unused_mut)]
    pub async fn start(
        config: HostConfig,
        mut registry: ConnectorRegistry,
    ) -> Result<Self, HostError> {
        config.validate()?;

        // Ensure data directories exist
        std::fs::create_dir_all(&config.aq_data_dir)?;
        if let Some(parent) = config.context_store_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        // ── Load WASM connectors (if configured) ──
        // WasmRuntime creation involves reqwest::blocking::Client, which builds
        // an internal tokio runtime. This must happen on a blocking thread to
        // avoid "Cannot create Runtime within async context" panic.
        #[cfg(feature = "wasm")]
        let (wasm_runtime, wasm_connectors) =
            if let Some(ref connectors_dir) = config.connectors_dir {
                let wasm_config = config.wasm_runtime_config();
                let dir = connectors_dir.clone();
                let (runtime, connectors) = tokio::task::spawn_blocking(move || {
                    let runtime = Arc::new(
                        worldinterface_wasm::WasmRuntime::new(wasm_config)
                            .map_err(|e| HostError::WasmInit(e.to_string()))?,
                    );
                    let connectors = worldinterface_wasm::load_modules_from_dir(&runtime, &dir)
                        .map_err(|e| HostError::WasmInit(e.to_string()))?;
                    Ok::<_, HostError>((runtime, connectors))
                })
                .await
                .map_err(|e| HostError::InternalError(format!("WASM loading join error: {e}")))??;
                (Some(runtime), connectors)
            } else {
                (None, Vec::new())
            };
        #[cfg(feature = "wasm")]
        {
            let wasm_count = wasm_connectors.len();
            for connector in wasm_connectors {
                use worldinterface_connector::traits::Connector as _;
                let name = connector.describe().name.clone();
                if registry.get(&name).is_some() {
                    return Err(HostError::DuplicateConnector(name));
                }
                registry.register(Arc::new(connector));
            }
            if let Some(ref connectors_dir) = config.connectors_dir {
                tracing::info!(
                    dir = %connectors_dir.display(),
                    count = wasm_count,
                    "WASM connectors loaded"
                );
            }
        }

        // Open ContextStore
        let store = Arc::new(SqliteContextStore::open(&config.context_store_path)?);

        // Restore coordinator map from ContextStore globals
        let coordinator_map = restore_coordinator_map(&store)?;
        let coordinator_map = Arc::new(std::sync::Mutex::new(coordinator_map));

        // Build the FlowHandler and bootstrap AQ engine
        let registry = Arc::new(registry);
        let handler = FlowHandler::new(
            Arc::clone(&registry),
            Arc::clone(&store),
            Arc::clone(&config.metrics),
        );
        let runtime_config = config.to_runtime_config();
        let aq_engine = ActionQueueEngine::new(runtime_config, handler);
        let bootstrapped = aq_engine.bootstrap()?;
        let engine: EngineSlot = Arc::new(tokio::sync::Mutex::new(Some(bootstrapped)));

        // Set up shutdown signaling
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Launch background tick loop
        let tick_engine = Arc::clone(&engine);
        let tick_map = Arc::clone(&coordinator_map);
        let tick_store = Arc::clone(&store);
        let tick_interval = config.tick_interval;
        let tick_metrics = Arc::clone(&config.metrics);
        let tick_handle = tokio::spawn(async move {
            tick_loop(tick_engine, tick_map, tick_store, tick_interval, shutdown_rx, tick_metrics)
                .await;
        });

        let inner = Arc::new(HostInner {
            engine,
            store,
            registry,
            coordinator_map,
            compiler_config: config.compiler_config,
            shutdown_tx,
            #[cfg(feature = "wasm")]
            wasm_runtime: std::sync::Mutex::new(wasm_runtime),
        });

        Ok(Self { inner, tick_handle })
    }

    /// Submit a FlowSpec for execution.
    ///
    /// Validates, compiles, and submits the Coordinator task to AQ. Returns
    /// the FlowRunId for subsequent status queries.
    pub async fn submit_flow(&self, spec: FlowSpec) -> Result<FlowRunId, HostError> {
        let flow_run_id = FlowRunId::new();
        tracing::info!(%flow_run_id, "submitting flow");
        let result = worldinterface_flowspec::compile_with_config(
            &spec,
            &self.inner.compiler_config,
            flow_run_id,
        )?;

        // Step 1: Insert into coordinator_map (brief std::sync::Mutex lock, no await)
        {
            let mut map = self.inner.coordinator_map.lock().unwrap();
            map.insert(flow_run_id, result.coordinator.id());
        }

        // Step 2: Persist to ContextStore (no mutex held)
        persist_coordinator_map(&self.inner.store, &self.inner.coordinator_map)?;

        // Step 3: Submit to AQ engine (tokio::sync::Mutex)
        {
            let mut guard = self.inner.engine.lock().await;
            let engine = guard
                .as_mut()
                .ok_or_else(|| HostError::InternalError("engine already shut down".into()))?;
            engine.submit_task(result.coordinator)?;
        }

        Ok(flow_run_id)
    }

    /// Submit a FlowSpec with trigger input data.
    ///
    /// Like `submit_flow`, but additionally writes `trigger_input` to ContextStore
    /// at `(flow_run_id, TRIGGER_INPUT_NODE_ID)` before submitting the Coordinator
    /// task to AQ. This ensures trigger data is durably available before any step
    /// can attempt to read it.
    pub async fn submit_flow_with_trigger_input(
        &self,
        spec: FlowSpec,
        trigger_input: Value,
    ) -> Result<FlowRunId, HostError> {
        let flow_run_id = FlowRunId::new();
        tracing::info!(%flow_run_id, trigger = true, "submitting flow with trigger input");
        let result = worldinterface_flowspec::compile_with_config(
            &spec,
            &self.inner.compiler_config,
            flow_run_id,
        )?;

        // Write trigger input to ContextStore BEFORE AQ submission.
        let trigger_node_id = trigger_input_node_id();
        self.inner.store.put(flow_run_id, trigger_node_id, &trigger_input)?;

        // Insert into coordinator_map (brief std::sync::Mutex lock, no await)
        {
            let mut map = self.inner.coordinator_map.lock().unwrap();
            map.insert(flow_run_id, result.coordinator.id());
        }

        // Persist to ContextStore (no mutex held)
        persist_coordinator_map(&self.inner.store, &self.inner.coordinator_map)?;

        // Submit to AQ engine (tokio::sync::Mutex)
        {
            let mut guard = self.inner.engine.lock().await;
            let engine = guard
                .as_mut()
                .ok_or_else(|| HostError::InternalError("engine already shut down".into()))?;
            engine.submit_task(result.coordinator)?;
        }

        Ok(flow_run_id)
    }

    /// Access the shared ContextStore.
    ///
    /// Used by the daemon for webhook registry persistence and trigger
    /// receipt storage. ContextStore operations are thread-safe.
    pub fn context_store(&self) -> &dyn ContextStore {
        self.inner.store.as_ref()
    }

    /// Store a trigger receipt in ContextStore globals.
    ///
    /// Receipts are stored under `receipt:trigger:<flow_run_id>` for later retrieval.
    pub fn store_trigger_receipt(
        &self,
        flow_run_id: FlowRunId,
        receipt: &Value,
    ) -> Result<(), HostError> {
        let key = format!("receipt:trigger:{}", flow_run_id);
        self.inner.store.upsert_global(&key, receipt)?;
        Ok(())
    }

    /// Query the status of a flow run.
    pub async fn run_status(&self, flow_run_id: FlowRunId) -> Result<FlowRunStatus, HostError> {
        let coordinator_task_id = {
            let map = self.inner.coordinator_map.lock().unwrap();
            match map.get(&flow_run_id) {
                Some(&tid) => tid,
                None => return Err(HostError::FlowRunNotFound(flow_run_id)),
            }
        };

        let guard = self.inner.engine.lock().await;
        let engine = guard
            .as_ref()
            .ok_or_else(|| HostError::InternalError("engine already shut down".into()))?;
        let status = derive_flow_run_status(
            engine.projection(),
            self.inner.store.as_ref(),
            flow_run_id,
            coordinator_task_id,
        );
        Ok(status)
    }

    /// List all connector capabilities available in the registry.
    pub fn list_capabilities(&self) -> Vec<Descriptor> {
        self.inner.registry.list_capabilities()
    }

    /// Describe a specific connector by name.
    pub fn describe(&self, connector_name: &str) -> Option<Descriptor> {
        self.inner.registry.describe(connector_name)
    }

    /// List all known flow runs with summary status.
    ///
    /// Returns a summary for each flow in the coordinator map. Terminal flows
    /// that have been pruned are not included.
    pub async fn list_runs(&self) -> Result<Vec<FlowRunSummary>, HostError> {
        let flow_run_ids: Vec<FlowRunId> = {
            let map = self.inner.coordinator_map.lock().unwrap();
            map.keys().copied().collect()
        };

        let mut summaries = Vec::with_capacity(flow_run_ids.len());
        for flow_run_id in flow_run_ids {
            match self.run_status(flow_run_id).await {
                Ok(status) => summaries.push(FlowRunSummary {
                    flow_run_id: status.flow_run_id,
                    phase: status.phase,
                    submitted_at: status.submitted_at,
                    last_updated_at: status.last_updated_at,
                }),
                Err(HostError::FlowRunNotFound(_)) => continue,
                Err(e) => return Err(e),
            }
        }

        summaries.sort_by(|a, b| b.submitted_at.cmp(&a.submitted_at));
        Ok(summaries)
    }

    /// Invoke a single connector operation.
    ///
    /// Creates an ephemeral 1-node FlowSpec and executes it through the full
    /// AQ path (never bypasses AQ — Invariant 1).
    pub async fn invoke_single(
        &self,
        connector_name: &str,
        params: Value,
    ) -> Result<Value, HostError> {
        // Verify connector exists before building the flow
        if self.inner.registry.get(connector_name).is_none() {
            return Err(HostError::ConnectorNotFound(connector_name.to_string()));
        }

        let spec = build_single_node_flowspec(connector_name, params);
        let node_id = spec.nodes[0].id;
        let flow_run_id = self.submit_flow(spec).await?;

        // Poll until terminal
        let status = self.poll_until_terminal(flow_run_id).await?;

        match status.phase {
            FlowPhase::Completed => {
                // Get the single node's output
                let outputs = extract_flow_outputs(self.inner.store.as_ref(), flow_run_id)?;
                outputs.get(&node_id).cloned().ok_or_else(|| {
                    HostError::InternalError(format!(
                        "flow completed but no output for node {node_id}"
                    ))
                })
            }
            FlowPhase::Failed => Err(HostError::FlowFailed {
                flow_run_id,
                error: status.error.unwrap_or_else(|| "unknown error".into()),
            }),
            FlowPhase::Canceled => Err(HostError::FlowCanceled(flow_run_id)),
            _ => Err(HostError::InternalError(format!(
                "unexpected terminal phase: {:?}",
                status.phase
            ))),
        }
    }

    /// Poll a flow run until it reaches a terminal state.
    async fn poll_until_terminal(
        &self,
        flow_run_id: FlowRunId,
    ) -> Result<FlowRunStatus, HostError> {
        loop {
            let status = self.run_status(flow_run_id).await?;
            match status.phase {
                FlowPhase::Completed | FlowPhase::Failed | FlowPhase::Canceled => {
                    return Ok(status);
                }
                _ => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            }
        }
    }

    /// Stop the tick loop and take the engine out for safe disposal.
    ///
    /// Takes the engine out of the `Option`, so when `HostInner` is later
    /// dropped in async context, the `Option` is `None` and no runtime panic occurs.
    async fn stop_and_take_engine(self) -> Result<Option<Engine>, HostError> {
        // Signal tick loop to stop
        let _ = self.inner.shutdown_tx.send(true);

        // Wait for tick loop task to exit
        if let Err(e) = self.tick_handle.await {
            tracing::warn!(error = %e, "tick loop task panicked during shutdown");
        }

        // Take the engine out of the slot
        let mut guard = self.inner.engine.lock().await;
        Ok(guard.take())
    }

    /// Shut down the host gracefully.
    ///
    /// Stops the tick loop. In-flight handlers complete in background.
    /// On next restart, incomplete tasks are recovered from the WAL.
    pub async fn shutdown(self) -> Result<(), HostError> {
        tracing::info!("shutting down host");

        // Take the WASM runtime out before stop_and_take_engine moves self.
        // reqwest::blocking::Client panics if dropped in async context.
        #[cfg(feature = "wasm")]
        let wasm_rt = self.inner.wasm_runtime.lock().unwrap().take();

        let engine = self.stop_and_take_engine().await?;

        // Drop engine (and WASM runtime) on a blocking thread — AQ's internal
        // runtime cannot be dropped from within an async context.
        tokio::task::spawn_blocking(move || {
            drop(engine);
            #[cfg(feature = "wasm")]
            drop(wasm_rt);
        })
        .await
        .map_err(|e| HostError::InternalError(format!("shutdown join error: {e}")))?;

        tracing::info!("host shut down");
        Ok(())
    }

    /// Count currently active (non-terminal) flow runs.
    ///
    /// The coordinator map only contains non-terminal flows (pruned on completion),
    /// so its length is a good approximation of active flows.
    pub fn active_flow_count(&self) -> usize {
        let map = self.inner.coordinator_map.lock().unwrap();
        map.len()
    }

    /// Drop the host without graceful shutdown — simulates a crash.
    ///
    /// Stops the tick loop, then drops the engine on a blocking thread.
    /// Data files are preserved for crash-resume testing.
    #[doc(hidden)]
    pub async fn crash_drop(self) {
        #[cfg(feature = "wasm")]
        let wasm_rt = self.inner.wasm_runtime.lock().unwrap().take();
        let engine = self.stop_and_take_engine().await.ok().flatten();
        tokio::task::spawn_blocking(move || {
            drop(engine);
            #[cfg(feature = "wasm")]
            drop(wasm_rt);
        })
        .await
        .ok();
    }
}
