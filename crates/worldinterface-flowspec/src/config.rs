//! Compiler configuration.

/// Configuration for the FlowSpec compiler.
#[derive(Debug, Clone)]
pub struct CompilerConfig {
    /// Default timeout for Step tasks (seconds). None = no timeout.
    pub default_step_timeout_secs: Option<u64>,
    /// Maximum attempts for Step tasks before terminal failure.
    pub default_step_max_attempts: u32,
    /// Timeout for the Coordinator task (seconds). None = no timeout.
    pub coordinator_timeout_secs: Option<u64>,
}

impl Default for CompilerConfig {
    fn default() -> Self {
        Self {
            default_step_timeout_secs: Some(300),
            default_step_max_attempts: 3,
            coordinator_timeout_secs: None,
        }
    }
}
