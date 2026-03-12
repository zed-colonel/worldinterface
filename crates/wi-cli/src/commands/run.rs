//! `wi run` — run inspection.

use clap::{Args, Subcommand};
use wi_core::id::FlowRunId;

use crate::client::DaemonClient;
use crate::error::CliError;
use crate::output;

#[derive(Args)]
pub struct RunArgs {
    #[command(subcommand)]
    pub command: RunCommand,
}

#[derive(Subcommand)]
pub enum RunCommand {
    /// List all flow runs.
    List,

    /// Inspect a specific flow run.
    Inspect(RunInspectArgs),
}

#[derive(Args)]
pub struct RunInspectArgs {
    /// FlowRunId to inspect.
    pub id: FlowRunId,
}

pub async fn execute(args: RunArgs, client: &DaemonClient) -> Result<(), CliError> {
    match args.command {
        RunCommand::List => {
            let runs = client.list_runs().await?;
            println!("{}", output::format_run_list(&runs));
        }
        RunCommand::Inspect(inspect_args) => {
            let status = client.get_run(inspect_args.id).await?;
            println!("{}", output::format_run_inspect(&status));
        }
    }
    Ok(())
}
