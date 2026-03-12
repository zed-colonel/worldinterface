//! Daemon startup, serving, and shutdown.

use std::sync::{Arc, RwLock};

use wi_connector::connectors::default_registry;
use wi_host::EmbeddedHost;
use wi_http_trigger::WebhookRegistry;

use crate::config::DaemonConfig;
use crate::error::DaemonError;
use crate::metrics::{PrometheusMetricsRecorder, WiMetricsRegistry};
use crate::router::build_router;
use crate::state::AppState;

/// Run the daemon: bootstrap host, bind HTTP server, handle signals.
pub async fn run(config: DaemonConfig) -> Result<(), DaemonError> {
    tracing::info!("starting WorldInterface daemon");

    // Create Prometheus metrics registry
    let metrics_registry =
        Arc::new(WiMetricsRegistry::new().map_err(|e| {
            DaemonError::Config(format!("failed to create metrics registry: {}", e))
        })?);

    // Build host config with metrics recorder
    let mut host_config = config.to_host_config();
    host_config.metrics = Arc::new(PrometheusMetricsRecorder::new(Arc::clone(&metrics_registry)));

    let registry = default_registry();
    let host = EmbeddedHost::start(host_config, registry).await?;

    tracing::info!("host started successfully");

    // Load webhook registrations from ContextStore
    let webhook_registry = WebhookRegistry::load_from_store(host.context_store())
        .map_err(|e| DaemonError::Config(format!("failed to load webhooks: {}", e)))?;
    tracing::info!(count = webhook_registry.len(), "loaded webhook registrations");

    // Build application state and router
    let state = Arc::new(AppState {
        host,
        webhook_registry: RwLock::new(webhook_registry),
        metrics: metrics_registry,
    });
    let router = build_router(Arc::clone(&state));

    // Bind TCP listener
    let listener =
        tokio::net::TcpListener::bind(&config.bind_address).await.map_err(DaemonError::Bind)?;
    tracing::info!(address = %config.bind_address, "listening");

    // Serve with graceful shutdown on SIGINT/SIGTERM
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(DaemonError::Serve)?;

    tracing::info!("HTTP server stopped, shutting down host");

    // Shut down the embedded host
    match Arc::try_unwrap(state) {
        Ok(app_state) => {
            app_state.host.shutdown().await?;
        }
        Err(_) => {
            tracing::warn!("could not take ownership of host for graceful shutdown");
        }
    }

    tracing::info!("daemon shut down");
    Ok(())
}

/// Wait for SIGINT or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => { tracing::info!("received SIGINT"); }
            _ = sigterm.recv() => { tracing::info!("received SIGTERM"); }
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("failed to listen for ctrl-c");
        tracing::info!("received SIGINT");
    }
}
