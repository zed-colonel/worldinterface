//! `wi webhook` — webhook management operations.

use std::path::PathBuf;

use clap::{Args, Subcommand};
use wi_core::flowspec::FlowSpec;

use crate::client::DaemonClient;
use crate::error::CliError;
use crate::output;

#[derive(Args)]
pub struct WebhookArgs {
    #[command(subcommand)]
    pub command: WebhookCommand,
}

#[derive(Subcommand)]
pub enum WebhookCommand {
    /// Register a new webhook.
    Register(WebhookRegisterArgs),
    /// List all registered webhooks.
    List,
    /// Delete a webhook.
    Delete(WebhookDeleteArgs),
}

#[derive(Args)]
pub struct WebhookRegisterArgs {
    /// URL path for the webhook (e.g., "github/push").
    pub path: String,
    /// Path to FlowSpec file (YAML or JSON).
    pub file: PathBuf,
    /// Optional description.
    #[arg(long, short)]
    pub description: Option<String>,
}

#[derive(Args)]
pub struct WebhookDeleteArgs {
    /// Webhook ID to delete.
    pub id: String,
}

pub async fn execute(args: WebhookArgs, client: &DaemonClient) -> Result<(), CliError> {
    match args.command {
        WebhookCommand::Register(reg_args) => execute_register(reg_args, client).await,
        WebhookCommand::List => execute_list(client).await,
        WebhookCommand::Delete(del_args) => execute_delete(del_args, client).await,
    }
}

async fn execute_register(
    args: WebhookRegisterArgs,
    client: &DaemonClient,
) -> Result<(), CliError> {
    let contents = std::fs::read_to_string(&args.file)?;

    let spec: FlowSpec = if is_yaml_file(&args.file) {
        serde_yaml::from_str(&contents)?
    } else {
        serde_json::from_str(&contents)?
    };

    let resp = client.register_webhook(&args.path, spec, args.description).await?;
    println!("Webhook registered:");
    println!("  ID:         {}", resp.webhook_id);
    println!("  Path:       {}", resp.path);
    println!("  Invoke URL: {}", resp.invoke_url);

    Ok(())
}

async fn execute_list(client: &DaemonClient) -> Result<(), CliError> {
    let webhooks = client.list_webhooks().await?;
    println!("{}", output::format_webhook_list(&webhooks));
    Ok(())
}

async fn execute_delete(args: WebhookDeleteArgs, client: &DaemonClient) -> Result<(), CliError> {
    client.delete_webhook(&args.id).await?;
    println!("Webhook deleted: {}", args.id);
    Ok(())
}

fn is_yaml_file(path: &std::path::Path) -> bool {
    matches!(path.extension().and_then(|e| e.to_str()), Some("yaml" | "yml"))
}
