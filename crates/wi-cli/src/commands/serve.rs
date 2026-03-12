//! `wi serve` — start the daemon.

use std::path::PathBuf;

use clap::Args;
use tracing_subscriber::{fmt, EnvFilter};

use crate::error::CliError;

#[derive(Args)]
pub struct ServeArgs {
    /// Path to TOML config file.
    #[arg(long, short)]
    pub config: Option<PathBuf>,
}

/// Initialize the tracing subscriber.
///
/// Uses `RUST_LOG` env var for filtering. Defaults to `info` for WI crates,
/// `warn` for everything else.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "wi_core=info,wi_flowspec=info,wi_contextstore=info,wi_connector=info,\
             wi_coordinator=info,wi_host=info,wi_http_trigger=info,wi_daemon=info,wi_cli=info,\
             tower_http=info,warn",
        )
    });

    let _ = fmt().with_env_filter(filter).with_target(true).try_init();
}

pub async fn execute(args: ServeArgs) -> Result<(), CliError> {
    init_tracing();
    let config = wi_daemon::DaemonConfig::load(args.config.as_deref())?;
    wi_daemon::run(config).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // T-11: Tracing Initialization (Sprint 8)

    #[test]
    fn init_tracing_does_not_panic() {
        // init_tracing() uses try_init() so multiple calls are safe.
        init_tracing();
    }

    #[test]
    fn init_tracing_multiple_calls_safe() {
        // Verifies that calling init_tracing() twice does not panic.
        // This is the pattern for test-friendly tracing initialization (H-6).
        init_tracing();
        init_tracing();
    }
}
