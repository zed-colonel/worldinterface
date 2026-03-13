//! HTTP client for the WorldInterface daemon API.

use serde::de::DeserializeOwned;
use serde_json::Value;
use worldinterface_core::descriptor::Descriptor;
use worldinterface_core::flowspec::FlowSpec;
use worldinterface_core::id::FlowRunId;
use worldinterface_daemon::SubmitFlowRequest;
use worldinterface_host::{FlowRunStatus, FlowRunSummary};

use crate::error::CliError;

/// Client for the WorldInterface daemon HTTP API.
pub struct DaemonClient {
    base_url: String,
    client: reqwest::Client,
}

#[derive(serde::Deserialize)]
struct ErrorBody {
    error: String,
}

#[derive(serde::Deserialize)]
struct ListCapabilitiesResponse {
    capabilities: Vec<Descriptor>,
}

#[derive(serde::Deserialize)]
struct ListRunsResponse {
    runs: Vec<FlowRunSummary>,
}

#[derive(serde::Deserialize)]
struct InvokeResponse {
    output: Value,
}

#[derive(serde::Serialize)]
struct RegisterWebhookBody {
    path: String,
    flow_spec: FlowSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct RegisterWebhookResponse {
    pub webhook_id: String,
    pub path: String,
    pub invoke_url: String,
}

#[derive(serde::Deserialize)]
struct ListWebhooksResponse {
    webhooks: Vec<WebhookSummary>,
}

#[derive(Debug, serde::Deserialize)]
pub struct WebhookSummary {
    pub id: String,
    pub path: String,
    #[allow(dead_code)]
    pub description: Option<String>,
    pub created_at: u64,
    pub invoke_url: String,
}

impl DaemonClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn list_capabilities(&self) -> Result<Vec<Descriptor>, CliError> {
        let resp: ListCapabilitiesResponse = self.get("/api/v1/capabilities").await?;
        Ok(resp.capabilities)
    }

    pub async fn describe_capability(&self, name: &str) -> Result<Descriptor, CliError> {
        self.get(&format!("/api/v1/capabilities/{}", name)).await
    }

    pub async fn submit_flow(&self, spec: FlowSpec) -> Result<FlowRunId, CliError> {
        let resp: worldinterface_daemon::SubmitFlowResponse =
            self.post("/api/v1/flows", &SubmitFlowRequest { spec }).await?;
        Ok(resp.flow_run_id)
    }

    pub async fn submit_ephemeral_flow(&self, spec: FlowSpec) -> Result<FlowRunId, CliError> {
        let resp: worldinterface_daemon::SubmitFlowResponse =
            self.post("/api/v1/flows/ephemeral", &spec).await?;
        Ok(resp.flow_run_id)
    }

    pub async fn list_runs(&self) -> Result<Vec<FlowRunSummary>, CliError> {
        let resp: ListRunsResponse = self.get("/api/v1/runs").await?;
        Ok(resp.runs)
    }

    pub async fn get_run(&self, id: FlowRunId) -> Result<FlowRunStatus, CliError> {
        self.get(&format!("/api/v1/runs/{}", id)).await
    }

    pub async fn invoke(&self, op: &str, params: Value) -> Result<Value, CliError> {
        let resp: InvokeResponse = self.post(&format!("/api/v1/invoke/{}", op), &params).await?;
        Ok(resp.output)
    }

    pub async fn register_webhook(
        &self,
        path: &str,
        spec: FlowSpec,
        description: Option<String>,
    ) -> Result<RegisterWebhookResponse, CliError> {
        self.post(
            "/api/v1/webhooks",
            &RegisterWebhookBody { path: path.to_string(), flow_spec: spec, description },
        )
        .await
    }

    pub async fn list_webhooks(&self) -> Result<Vec<WebhookSummary>, CliError> {
        let resp: ListWebhooksResponse = self.get("/api/v1/webhooks").await?;
        Ok(resp.webhooks)
    }

    pub async fn delete_webhook(&self, id: &str) -> Result<(), CliError> {
        let url = format!("{}/api/v1/webhooks/{}", self.base_url, id);
        let resp = self.client.delete(&url).send().await.map_err(|e| self.classify_error(e))?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let status_code = status.as_u16();
            let message = match resp.json::<ErrorBody>().await {
                Ok(body) => body.error,
                Err(_) => format!("HTTP {}", status_code),
            };
            Err(CliError::Api { status: status_code, message })
        }
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, CliError> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self.client.get(&url).send().await.map_err(|e| self.classify_error(e))?;
        self.handle_response(resp).await
    }

    async fn post<T: DeserializeOwned, B: serde::Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, CliError> {
        let url = format!("{}{}", self.base_url, path);
        let resp =
            self.client.post(&url).json(body).send().await.map_err(|e| self.classify_error(e))?;
        self.handle_response(resp).await
    }

    /// Classify a reqwest error, detecting connection refused specifically.
    fn classify_error(&self, err: reqwest::Error) -> CliError {
        if err.is_connect() {
            CliError::ConnectionRefused { url: self.base_url.clone(), source: err }
        } else {
            CliError::Request(err)
        }
    }

    async fn handle_response<T: DeserializeOwned>(
        &self,
        resp: reqwest::Response,
    ) -> Result<T, CliError> {
        let status = resp.status();
        if status.is_success() {
            Ok(resp.json().await.map_err(CliError::Request)?)
        } else {
            let status_code = status.as_u16();
            let message = match resp.json::<ErrorBody>().await {
                Ok(body) => body.error,
                Err(_) => format!("HTTP {}", status_code),
            };
            Err(CliError::Api { status: status_code, message })
        }
    }
}
