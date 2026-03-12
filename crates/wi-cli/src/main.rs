mod client;
mod commands;
mod error;
mod output;

use clap::{Parser, Subcommand};

use crate::client::DaemonClient;

/// WorldInterface command-line tool.
#[derive(Parser)]
#[command(name = "wi", about = "WorldInterface command-line tool")]
struct Cli {
    /// Daemon URL.
    #[arg(long, global = true, env = "WI_DAEMON_URL", default_value = "http://127.0.0.1:7800")]
    daemon_url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the daemon (runs in foreground).
    Serve(commands::serve::ServeArgs),

    /// Flow operations.
    #[command(subcommand)]
    Flow(commands::flow::FlowCommand),

    /// Run inspection.
    #[command(subcommand)]
    Run(commands::run::RunCommand),

    /// Capability discovery.
    #[command(subcommand)]
    Capabilities(commands::capabilities::CapabilitiesCommand),

    /// Invoke a single connector operation.
    Invoke(commands::invoke::InvokeArgs),

    /// Webhook management.
    #[command(subcommand)]
    Webhook(commands::webhook::WebhookCommand),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), error::CliError> {
    match cli.command {
        Command::Serve(args) => {
            // Initialize tracing for serve mode
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info,wi_daemon=debug,wi_host=debug".into()),
                )
                .init();
            commands::serve::execute(args).await
        }
        Command::Flow(cmd) => {
            let client = DaemonClient::new(&cli.daemon_url);
            let args = commands::flow::FlowArgs { command: cmd };
            commands::flow::execute(args, &client).await
        }
        Command::Run(cmd) => {
            let client = DaemonClient::new(&cli.daemon_url);
            let args = commands::run::RunArgs { command: cmd };
            commands::run::execute(args, &client).await
        }
        Command::Capabilities(cmd) => {
            let client = DaemonClient::new(&cli.daemon_url);
            let args = commands::capabilities::CapabilitiesArgs { command: cmd };
            commands::capabilities::execute(args, &client).await
        }
        Command::Invoke(args) => {
            let client = DaemonClient::new(&cli.daemon_url);
            commands::invoke::execute(args, &client).await
        }
        Command::Webhook(cmd) => {
            let client = DaemonClient::new(&cli.daemon_url);
            let args = commands::webhook::WebhookArgs { command: cmd };
            commands::webhook::execute(args, &client).await
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn cli_parses_serve() {
        let cli = Cli::try_parse_from(["wi", "serve", "--config", "foo.toml"]).unwrap();
        assert!(
            matches!(cli.command, Command::Serve(ref args) if args.config.as_deref() == Some(std::path::Path::new("foo.toml")))
        );
    }

    #[test]
    fn cli_parses_serve_no_config() {
        let cli = Cli::try_parse_from(["wi", "serve"]).unwrap();
        assert!(matches!(cli.command, Command::Serve(ref args) if args.config.is_none()));
    }

    #[test]
    fn cli_parses_flow_submit() {
        let cli = Cli::try_parse_from(["wi", "flow", "submit", "flow.yaml", "--wait"]).unwrap();
        match cli.command {
            Command::Flow(commands::flow::FlowCommand::Submit(ref args)) => {
                assert_eq!(args.file, std::path::PathBuf::from("flow.yaml"));
                assert!(args.wait);
            }
            _ => panic!("expected Flow Submit"),
        }
    }

    #[test]
    fn cli_parses_flow_submit_ephemeral() {
        let cli =
            Cli::try_parse_from(["wi", "flow", "submit", "flow.json", "--ephemeral"]).unwrap();
        match cli.command {
            Command::Flow(commands::flow::FlowCommand::Submit(ref args)) => {
                assert!(args.ephemeral);
            }
            _ => panic!("expected Flow Submit"),
        }
    }

    #[test]
    fn cli_parses_run_list() {
        let cli = Cli::try_parse_from(["wi", "run", "list"]).unwrap();
        assert!(matches!(cli.command, Command::Run(commands::run::RunCommand::List)));
    }

    #[test]
    fn cli_parses_run_inspect() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let cli = Cli::try_parse_from(["wi", "run", "inspect", id]).unwrap();
        match cli.command {
            Command::Run(commands::run::RunCommand::Inspect(ref args)) => {
                assert_eq!(args.id.to_string(), id);
            }
            _ => panic!("expected Run Inspect"),
        }
    }

    #[test]
    fn cli_parses_capabilities_list() {
        let cli = Cli::try_parse_from(["wi", "capabilities", "list"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Capabilities(commands::capabilities::CapabilitiesCommand::List)
        ));
    }

    #[test]
    fn cli_parses_capabilities_describe() {
        let cli = Cli::try_parse_from(["wi", "capabilities", "describe", "delay"]).unwrap();
        match cli.command {
            Command::Capabilities(commands::capabilities::CapabilitiesCommand::Describe(
                ref args,
            )) => {
                assert_eq!(args.name, "delay");
            }
            _ => panic!("expected Capabilities Describe"),
        }
    }

    #[test]
    fn cli_parses_invoke() {
        let cli =
            Cli::try_parse_from(["wi", "invoke", "delay", "-p", r#"{"duration_ms":10}"#]).unwrap();
        match cli.command {
            Command::Invoke(ref args) => {
                assert_eq!(args.op, "delay");
                assert_eq!(args.params.as_deref(), Some(r#"{"duration_ms":10}"#));
            }
            _ => panic!("expected Invoke"),
        }
    }

    #[test]
    fn cli_global_daemon_url() {
        let cli =
            Cli::try_parse_from(["wi", "--daemon-url", "http://x:8000", "run", "list"]).unwrap();
        assert_eq!(cli.daemon_url, "http://x:8000");
    }

    #[test]
    fn cli_default_daemon_url() {
        let cli = Cli::try_parse_from(["wi", "run", "list"]).unwrap();
        assert_eq!(cli.daemon_url, "http://127.0.0.1:7800");
    }

    // T-12: CLI webhook command parsing

    #[test]
    fn cli_parses_webhook_register() {
        let cli =
            Cli::try_parse_from(["wi", "webhook", "register", "github/push", "flow.yaml"]).unwrap();
        match cli.command {
            Command::Webhook(commands::webhook::WebhookCommand::Register(ref args)) => {
                assert_eq!(args.path, "github/push");
                assert_eq!(args.file, std::path::PathBuf::from("flow.yaml"));
                assert!(args.description.is_none());
            }
            _ => panic!("expected Webhook Register"),
        }
    }

    #[test]
    fn cli_parses_webhook_register_with_desc() {
        let cli =
            Cli::try_parse_from(["wi", "webhook", "register", "path", "flow.yaml", "-d", "desc"])
                .unwrap();
        match cli.command {
            Command::Webhook(commands::webhook::WebhookCommand::Register(ref args)) => {
                assert_eq!(args.description.as_deref(), Some("desc"));
            }
            _ => panic!("expected Webhook Register"),
        }
    }

    #[test]
    fn cli_parses_webhook_list() {
        let cli = Cli::try_parse_from(["wi", "webhook", "list"]).unwrap();
        assert!(matches!(cli.command, Command::Webhook(commands::webhook::WebhookCommand::List)));
    }

    #[test]
    fn cli_parses_webhook_delete() {
        let cli = Cli::try_parse_from(["wi", "webhook", "delete", "some-uuid"]).unwrap();
        match cli.command {
            Command::Webhook(commands::webhook::WebhookCommand::Delete(ref args)) => {
                assert_eq!(args.id, "some-uuid");
            }
            _ => panic!("expected Webhook Delete"),
        }
    }
}
