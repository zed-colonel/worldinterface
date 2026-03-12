//! Daemon configuration.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use wi_host::HostConfig;

use crate::error::DaemonError;

/// Configuration for the WorldInterface daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    /// Socket address to bind the HTTP server.
    #[serde(default = "default_bind_address")]
    pub bind_address: String,

    /// Directory for AQ WAL and snapshot files.
    #[serde(default = "default_aq_data_dir")]
    pub aq_data_dir: PathBuf,

    /// Path to the SQLite ContextStore database file.
    #[serde(default = "default_context_store_path")]
    pub context_store_path: PathBuf,

    /// Tick interval in milliseconds.
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,

    /// Maximum concurrent handler executions.
    #[serde(default = "default_dispatch_concurrency")]
    pub dispatch_concurrency: usize,

    /// AQ lease timeout in seconds.
    #[serde(default = "default_lease_timeout_secs")]
    pub lease_timeout_secs: u64,

    /// Graceful shutdown timeout in seconds.
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
}

fn default_bind_address() -> String {
    "127.0.0.1:7800".to_string()
}
fn default_aq_data_dir() -> PathBuf {
    PathBuf::from("data/aq")
}
fn default_context_store_path() -> PathBuf {
    PathBuf::from("data/context.db")
}
fn default_tick_interval_ms() -> u64 {
    50
}
fn default_dispatch_concurrency() -> usize {
    4
}
fn default_lease_timeout_secs() -> u64 {
    300
}
fn default_shutdown_timeout_secs() -> u64 {
    30
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            aq_data_dir: default_aq_data_dir(),
            context_store_path: default_context_store_path(),
            tick_interval_ms: default_tick_interval_ms(),
            dispatch_concurrency: default_dispatch_concurrency(),
            lease_timeout_secs: default_lease_timeout_secs(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
        }
    }
}

impl DaemonConfig {
    /// Load configuration with precedence: env vars > config file > defaults.
    pub fn load(config_path: Option<&Path>) -> Result<Self, DaemonError> {
        let mut config = match config_path {
            Some(path) => {
                let contents = std::fs::read_to_string(path)
                    .map_err(|e| DaemonError::Config(format!("reading {}: {e}", path.display())))?;
                toml::from_str(&contents)?
            }
            None => Self::default(),
        };

        // Environment variable overrides
        if let Ok(val) = std::env::var("WI_BIND_ADDRESS") {
            config.bind_address = val;
        }
        if let Ok(val) = std::env::var("WI_AQ_DATA_DIR") {
            config.aq_data_dir = PathBuf::from(val);
        }
        if let Ok(val) = std::env::var("WI_CONTEXT_STORE_PATH") {
            config.context_store_path = PathBuf::from(val);
        }
        if let Ok(val) = std::env::var("WI_TICK_INTERVAL_MS") {
            config.tick_interval_ms = val
                .parse()
                .map_err(|e| DaemonError::Config(format!("WI_TICK_INTERVAL_MS: {e}")))?;
        }
        if let Ok(val) = std::env::var("WI_DISPATCH_CONCURRENCY") {
            config.dispatch_concurrency = val
                .parse()
                .map_err(|e| DaemonError::Config(format!("WI_DISPATCH_CONCURRENCY: {e}")))?;
        }
        if let Ok(val) = std::env::var("WI_LEASE_TIMEOUT_SECS") {
            config.lease_timeout_secs = val
                .parse()
                .map_err(|e| DaemonError::Config(format!("WI_LEASE_TIMEOUT_SECS: {e}")))?;
        }
        if let Ok(val) = std::env::var("WI_SHUTDOWN_TIMEOUT_SECS") {
            config.shutdown_timeout_secs = val
                .parse()
                .map_err(|e| DaemonError::Config(format!("WI_SHUTDOWN_TIMEOUT_SECS: {e}")))?;
        }

        Ok(config)
    }

    /// Convert to HostConfig for EmbeddedHost::start().
    pub fn to_host_config(&self) -> HostConfig {
        HostConfig {
            aq_data_dir: self.aq_data_dir.clone(),
            context_store_path: self.context_store_path.clone(),
            tick_interval: Duration::from_millis(self.tick_interval_ms),
            dispatch_concurrency: NonZeroUsize::new(self.dispatch_concurrency)
                .unwrap_or(NonZeroUsize::new(4).unwrap()),
            lease_timeout_secs: self.lease_timeout_secs,
            shutdown_timeout: Duration::from_secs(self.shutdown_timeout_secs),
            ..HostConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    // Env var tests must not run concurrently since they mutate process-global state.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn default_config_is_valid() {
        let config = DaemonConfig::default();
        assert_eq!(config.bind_address, "127.0.0.1:7800");
        assert_eq!(config.tick_interval_ms, 50);
        assert_eq!(config.dispatch_concurrency, 4);
    }

    #[test]
    fn to_host_config_preserves_values() {
        let config = DaemonConfig {
            tick_interval_ms: 25,
            dispatch_concurrency: 8,
            lease_timeout_secs: 600,
            ..Default::default()
        };
        let host_config = config.to_host_config();
        assert_eq!(host_config.tick_interval, Duration::from_millis(25));
        assert_eq!(host_config.dispatch_concurrency.get(), 8);
        assert_eq!(host_config.lease_timeout_secs, 600);
    }

    #[test]
    fn load_without_config_file() {
        let _lock = ENV_LOCK.lock().unwrap();
        let config = DaemonConfig::load(None).unwrap();
        assert_eq!(config.bind_address, "127.0.0.1:7800");
    }

    #[test]
    fn load_with_env_overrides() {
        let _lock = ENV_LOCK.lock().unwrap();

        let key = "WI_BIND_ADDRESS";
        let original = std::env::var(key).ok();

        std::env::set_var(key, "0.0.0.0:9999");
        let config = DaemonConfig::load(None).unwrap();
        assert_eq!(config.bind_address, "0.0.0.0:9999");

        match original {
            Some(val) => std::env::set_var(key, val),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn load_env_overrides_toml() {
        let _lock = ENV_LOCK.lock().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        std::fs::write(
            &path,
            r#"
bind_address = "127.0.0.1:1111"
tick_interval_ms = 10
"#,
        )
        .unwrap();

        let key = "WI_TICK_INTERVAL_MS";
        let original = std::env::var(key).ok();

        std::env::set_var(key, "99");
        let config = DaemonConfig::load(Some(&path)).unwrap();
        // Env var should override TOML value
        assert_eq!(config.tick_interval_ms, 99);
        // TOML value should still apply for non-overridden fields
        assert_eq!(config.bind_address, "127.0.0.1:1111");

        match original {
            Some(val) => std::env::set_var(key, val),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn load_from_toml_file() {
        let _lock = ENV_LOCK.lock().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        std::fs::write(
            &path,
            r#"
bind_address = "0.0.0.0:9000"
tick_interval_ms = 10
"#,
        )
        .unwrap();
        let config = DaemonConfig::load(Some(&path)).unwrap();
        assert_eq!(config.bind_address, "0.0.0.0:9000");
        assert_eq!(config.tick_interval_ms, 10);
        // Defaults for unspecified fields
        assert_eq!(config.dispatch_concurrency, 4);
    }
}
