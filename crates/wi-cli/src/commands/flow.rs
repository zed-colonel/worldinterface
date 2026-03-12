//! `wi flow` — flow operations.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};
use wi_core::flowspec::FlowSpec;

use crate::client::DaemonClient;
use crate::error::CliError;
use crate::output;

#[derive(Args)]
pub struct FlowArgs {
    #[command(subcommand)]
    pub command: FlowCommand,
}

#[derive(Subcommand)]
pub enum FlowCommand {
    /// Submit a flow from a YAML or JSON file.
    Submit(FlowSubmitArgs),
}

#[derive(Args)]
pub struct FlowSubmitArgs {
    /// Path to FlowSpec file (YAML or JSON).
    pub file: PathBuf,

    /// Wait for completion and print result.
    #[arg(long, short)]
    pub wait: bool,

    /// Submit as ephemeral flow (bare FlowSpec, no wrapper).
    #[arg(long, short)]
    pub ephemeral: bool,
}

pub async fn execute(args: FlowArgs, client: &DaemonClient) -> Result<(), CliError> {
    match args.command {
        FlowCommand::Submit(submit_args) => execute_submit(submit_args, client).await,
    }
}

async fn execute_submit(args: FlowSubmitArgs, client: &DaemonClient) -> Result<(), CliError> {
    let contents = std::fs::read_to_string(&args.file)?;

    let spec: FlowSpec = if is_yaml_file(&args.file) {
        serde_yaml::from_str(&contents)?
    } else {
        serde_json::from_str(&contents)?
    };

    let flow_run_id = if args.ephemeral {
        client.submit_ephemeral_flow(spec).await?
    } else {
        client.submit_flow(spec).await?
    };
    println!("Flow submitted: {flow_run_id}");

    if args.wait {
        println!("Waiting for completion...");
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let status = client.get_run(flow_run_id).await?;
            match status.phase {
                wi_host::FlowPhase::Completed
                | wi_host::FlowPhase::Failed
                | wi_host::FlowPhase::Canceled => {
                    println!("{}", output::format_run_inspect(&status));
                    break;
                }
                _ => continue,
            }
        }
    }

    Ok(())
}

fn is_yaml_file(path: &std::path::Path) -> bool {
    matches!(path.extension().and_then(|e| e.to_str()), Some("yaml" | "yml"))
}
