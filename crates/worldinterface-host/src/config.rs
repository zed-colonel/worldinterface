//! Host configuration.

use std::num::NonZeroUsize;
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
        Ok(())
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
}
