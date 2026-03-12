//! CLI error types.

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// HTTP request failed.
    #[error("request failed: {0}")]
    Request(reqwest::Error),

    /// Daemon connection refused.
    #[error("cannot connect to daemon at {url}: {source}")]
    ConnectionRefused { url: String, source: reqwest::Error },

    /// API returned an error response.
    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },

    /// File I/O error.
    #[error("file error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON parse error.
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    /// YAML parse error.
    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// Daemon error (for `wi serve`).
    #[error("{0}")]
    Daemon(#[from] wi_daemon::DaemonError),
}
