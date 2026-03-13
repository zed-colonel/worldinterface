//! `wi capabilities` — connector discovery.

use clap::{Args, Subcommand};

use crate::client::DaemonClient;
use crate::error::CliError;
use crate::output;

#[derive(Args)]
pub struct CapabilitiesArgs {
    #[command(subcommand)]
    pub command: CapabilitiesCommand,
}

#[derive(Subcommand)]
pub enum CapabilitiesCommand {
    /// List all available connectors.
    List,

    /// Describe a specific connector.
    Describe(CapabilitiesDescribeArgs),
}

#[derive(Args)]
pub struct CapabilitiesDescribeArgs {
    /// Connector name.
    pub name: String,
}

pub async fn execute(args: CapabilitiesArgs, client: &DaemonClient) -> Result<(), CliError> {
    match args.command {
        CapabilitiesCommand::List => {
            let descriptors = client.list_capabilities().await?;
            println!("{}", output::format_capabilities_list(&descriptors));
        }
        CapabilitiesCommand::Describe(describe_args) => {
            let descriptor = client.describe_capability(&describe_args.name).await?;
            println!("{}", output::format_capability_describe(&descriptor));
        }
    }
    Ok(())
}
