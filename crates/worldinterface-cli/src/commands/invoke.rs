//! `wi invoke` — single-op invocation.

use std::path::PathBuf;

use clap::Args;
use serde_json::Value;

use crate::client::DaemonClient;
use crate::error::CliError;

#[derive(Args)]
pub struct InvokeArgs {
    /// Connector name.
    pub op: String,

    /// Parameters as inline JSON string.
    #[arg(long, short)]
    pub params: Option<String>,

    /// Parameters from a JSON file.
    #[arg(long, short = 'f')]
    pub params_file: Option<PathBuf>,
}

pub async fn execute(args: InvokeArgs, client: &DaemonClient) -> Result<(), CliError> {
    let params: Value = if let Some(ref json_str) = args.params {
        serde_json::from_str(json_str)?
    } else if let Some(ref path) = args.params_file {
        let contents = std::fs::read_to_string(path)?;
        serde_json::from_str(&contents)?
    } else {
        Value::Object(serde_json::Map::new())
    };

    let output = client.invoke(&args.op, params).await?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
