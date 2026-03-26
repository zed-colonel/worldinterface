//! Host configuration.

use std::num::NonZeroUsize;
#[cfg(feature = "wasm")]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use worldinterface_core::metrics::{MetricsRecorder, NoopMetricsRecorder};
use worldinterface_flowspec::CompilerConfig;

use crate::error::HostError;

/// Configuration for the embedded WorldInterface host.
#[derive(Clone)]
pub struct HostConfig {
    /// Directory for AQ WAL and snapshot files.
    pub aq_data_dir: PathBuf,

    /// Path to the SQLite ContextStore database file.
    pub context_store_path: PathBuf,

    /// How often the background tick loop runs (drives AQ dispatch + coordinator resume).
    /// Default: 50ms.
    pub tick_interval: Duration,

    /// Maximum concurrent handler executions (maps to AQ's dispatch_concurrency).
    /// Default: 4.
    pub dispatch_concurrency: NonZeroUsize,

    /// AQ lease timeout in seconds. Must be long enough for the longest connector
    /// invocation. Default: 300 (5 minutes).
    pub lease_timeout_secs: u64,

    /// Compiler configuration for FlowSpec compilation.
    pub compiler_config: CompilerConfig,

    /// Graceful shutdown timeout. How long to wait for in-flight work to complete.
    /// Default: 30 seconds.
    pub shutdown_timeout: Duration,

    /// Metrics recorder for observability.
    /// Defaults to [`NoopMetricsRecorder`] (no-op) for embedded/test use.
    pub metrics: Arc<dyn MetricsRecorder>,

    /// Optional directory to scan for WASM connector modules.
    /// Each module is a pair: `{name}.wasm` + `{name}.connector.toml`.
    /// If `None`, no WASM connectors are loaded (existing behavior).
    pub connectors_dir: Option<PathBuf>,

    /// Whether to watch `connectors_dir` for new connector modules at runtime.
    /// Requires the `watcher` feature. Default: false.
    pub watch_connectors_dir: bool,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            aq_data_dir: PathBuf::from("data/aq"),
            context_store_path: PathBuf::from("data/context.db"),
            tick_interval: Duration::from_millis(50),
            dispatch_concurrency: NonZeroUsize::new(4).unwrap(),
            lease_timeout_secs: 300,
            compiler_config: CompilerConfig::default(),
            shutdown_timeout: Duration::from_secs(30),
            metrics: Arc::new(NoopMetricsRecorder),
            connectors_dir: None,
            watch_connectors_dir: false,
        }
    }
}

impl std::fmt::Debug for HostConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostConfig")
            .field("aq_data_dir", &self.aq_data_dir)
            .field("context_store_path", &self.context_store_path)
            .field("tick_interval", &self.tick_interval)
            .field("dispatch_concurrency", &self.dispatch_concurrency)
            .field("lease_timeout_secs", &self.lease_timeout_secs)
            .field("compiler_config", &self.compiler_config)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("connectors_dir", &self.connectors_dir)
            .field("watch_connectors_dir", &self.watch_connectors_dir)
            .finish()
    }
}

impl HostConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), HostError> {
        if self.aq_data_dir.as_os_str().is_empty() {
            return Err(HostError::InvalidConfig("aq_data_dir must be non-empty".into()));
        }
        if self.context_store_path.as_os_str().is_empty() {
            return Err(HostError::InvalidConfig("context_store_path must be non-empty".into()));
        }
        if self.tick_interval.is_zero() {
            return Err(HostError::InvalidConfig("tick_interval must be > 0".into()));
        }
        if self.lease_timeout_secs < 30 {
            return Err(HostError::InvalidConfig("lease_timeout_secs must be >= 30".into()));
        }
        if self.shutdown_timeout.is_zero() {
            return Err(HostError::InvalidConfig("shutdown_timeout must be > 0".into()));
        }
        if let Some(ref dir) = self.connectors_dir {
            if !dir.exists() {
                return Err(HostError::InvalidConfig(format!(
                    "connectors_dir does not exist: {}",
                    dir.display()
                )));
            }
        }
        Ok(())
    }

    /// Build a `WasmRuntimeConfig` for the WASM runtime.
    /// The KV store lives alongside the ContextStore database.
    #[cfg(feature = "wasm")]
    pub(crate) fn wasm_runtime_config(&self) -> worldinterface_wasm::runtime::WasmRuntimeConfig {
        worldinterface_wasm::runtime::WasmRuntimeConfig {
            kv_store_dir: self
                .context_store_path
                .parent()
                .unwrap_or(Path::new("."))
                .join("wasm-kv"),
            ..Default::default()
        }
    }

    /// Build a RuntimeConfig for ActionQueue from this HostConfig.
    pub(crate) fn to_runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig {
            data_dir: self.aq_data_dir.clone(),
            backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_secs(5) },
            dispatch_concurrency: self.dispatch_concurrency,
            lease_timeout_secs: self.lease_timeout_secs,
            tick_interval: self.tick_interval,
            snapshot_event_threshold: Some(10_000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        assert!(HostConfig::default().validate().is_ok());
    }

    #[test]
    fn rejects_zero_tick_interval() {
        let config = HostConfig { tick_interval: Duration::ZERO, ..Default::default() };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, HostError::InvalidConfig(_)));
    }

    #[test]
    fn rejects_low_lease_timeout() {
        let config = HostConfig { lease_timeout_secs: 10, ..Default::default() };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, HostError::InvalidConfig(_)));
    }

    #[test]
    fn rejects_zero_shutdown_timeout() {
        let config = HostConfig { shutdown_timeout: Duration::ZERO, ..Default::default() };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, HostError::InvalidConfig(_)));
    }

    // ── E2S4-T1: connectors_dir = None → default behavior ──

    #[test]
    fn connectors_dir_none_is_valid() {
        let config = HostConfig { connectors_dir: None, ..Default::default() };
        assert!(config.validate().is_ok());
    }

    // ── E2S4-T2: connectors_dir = Some(valid) → passes validation ──

    #[test]
    fn connectors_dir_valid_passes() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            HostConfig { connectors_dir: Some(dir.path().to_path_buf()), ..Default::default() };
        assert!(config.validate().is_ok());
    }

    // ── E2S4-T3: connectors_dir = Some(nonexistent) → InvalidConfig ──

    #[test]
    fn connectors_dir_nonexistent_rejected() {
        let config = HostConfig {
            connectors_dir: Some(PathBuf::from("/tmp/nonexistent-e2s4-test-dir")),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(err, HostError::InvalidConfig(ref msg) if msg.contains("connectors_dir")));
    }

    #[test]
    fn to_runtime_config_preserves_values() {
        let config = HostConfig {
            aq_data_dir: PathBuf::from("/tmp/aq"),
            tick_interval: Duration::from_millis(100),
            dispatch_concurrency: NonZeroUsize::new(8).unwrap(),
            lease_timeout_secs: 600,
            ..Default::default()
        };
        let rc = config.to_runtime_config();
        assert_eq!(rc.data_dir, PathBuf::from("/tmp/aq"));
        assert_eq!(rc.tick_interval, Duration::from_millis(100));
        assert_eq!(rc.dispatch_concurrency, NonZeroUsize::new(8).unwrap());
        assert_eq!(rc.lease_timeout_secs, 600);
    }

    // ── E5S3: watch_connectors_dir defaults to false ──

    #[test]
    fn watch_connectors_dir_defaults_to_false() {
        let config = HostConfig::default();
        assert!(!config.watch_connectors_dir);
    }
}
