//! Webhook CRUD and invocation endpoints.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wi_core::flowspec::FlowSpec;
use wi_core::id::FlowRunId;
use wi_http_trigger::{validate_webhook_path, WebhookId, WebhookRegistration};

use crate::error::ApiError;
use crate::state::SharedState;

pub fn register_routes(router: axum::Router<SharedState>) -> axum::Router<SharedState> {
    router
        .route("/api/v1/webhooks", axum::routing::post(register_webhook))
        .route("/api/v1/webhooks", axum::routing::get(list_webhooks))
        .route("/api/v1/webhooks/:id", axum::routing::get(get_webhook).delete(delete_webhook))
        .route("/webhooks/*path", axum::routing::post(invoke_webhook))
}

// --- CRUD types ---

#[derive(Deserialize)]
pub struct RegisterWebhookRequest {
    pub path: String,
    pub flow_spec: FlowSpec,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Serialize)]
pub struct RegisterWebhookResponse {
    pub webhook_id: WebhookId,
    pub path: String,
    pub invoke_url: String,
}

#[derive(Serialize)]
pub struct ListWebhooksResponse {
    pub webhooks: Vec<WebhookSummary>,
}

#[derive(Serialize)]
pub struct WebhookSummary {
    pub id: WebhookId,
    pub path: String,
    pub description: Option<String>,
    pub created_at: u64,
    pub invoke_url: String,
}

#[derive(Serialize)]
pub struct WebhookInvokeResponse {
    pub flow_run_id: FlowRunId,
    pub receipt: Value,
}

// --- Handlers ---

async fn register_webhook(
    State(state): State<SharedState>,
    Json(request): Json<RegisterWebhookRequest>,
) -> Result<(StatusCode, Json<RegisterWebhookResponse>), ApiError> {
    validate_webhook_path(&request.path)?;

    // Validate FlowSpec — compile to verify it produces a valid AQ DAG.
    // Compilation errors map to BadRequest via the From<HostError> impl.
    request
        .flow_spec
        .validate()
        .map_err(|e| ApiError::BadRequest(format!("invalid FlowSpec: {}", e)))?;

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

    let registration = WebhookRegistration {
        id: WebhookId::new(),
        path: request.path.clone(),
        flow_spec: request.flow_spec,
        description: request.description,
        created_at: now,
    };

    let webhook_id = registration.id;
    {
        let mut registry = state.webhook_registry.write().unwrap();
        registry.register(registration, state.host.context_store())?;
    }

    Ok((
        StatusCode::CREATED,
        Json(RegisterWebhookResponse {
            webhook_id,
            path: request.path.clone(),
            invoke_url: format!("/webhooks/{}", request.path),
        }),
    ))
}

async fn list_webhooks(State(state): State<SharedState>) -> Json<ListWebhooksResponse> {
    let registry = state.webhook_registry.read().unwrap();
    let webhooks = registry
        .list()
        .into_iter()
        .map(|r| WebhookSummary {
            id: r.id,
            path: r.path.clone(),
            description: r.description.clone(),
            created_at: r.created_at,
            invoke_url: format!("/webhooks/{}", r.path),
        })
        .collect();
    Json(ListWebhooksResponse { webhooks })
}

async fn get_webhook(
    State(state): State<SharedState>,
    Path(id_str): Path<String>,
) -> Result<Json<WebhookRegistration>, ApiError> {
    let id: WebhookId = id_str
        .parse()
        .map_err(|_| ApiError::BadRequest(format!("invalid webhook ID: {}", id_str)))?;
    let registry = state.webhook_registry.read().unwrap();
    let path = registry
        .by_id_to_path(&id)
        .ok_or_else(|| ApiError::NotFound(format!("webhook not found: {}", id)))?;
    let registration = registry.get_by_path(path).expect("inconsistent registry");
    Ok(Json(registration.clone()))
}

async fn delete_webhook(
    State(state): State<SharedState>,
    Path(id_str): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id: WebhookId = id_str
        .parse()
        .map_err(|_| ApiError::BadRequest(format!("invalid webhook ID: {}", id_str)))?;
    {
        let mut registry = state.webhook_registry.write().unwrap();
        registry.remove(id, state.host.context_store())?;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn invoke_webhook(
    State(state): State<SharedState>,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<WebhookInvokeResponse>), ApiError> {
    // Record webhook invocation metric
    state.metrics.collectors().webhook_invocations_total.with_label_values(&[&path]).inc();

    match wi_http_trigger::handle_webhook(
        &path,
        &body,
        &headers,
        None, // skip source_addr extraction for v1.0-alpha (H-6)
        &state.host,
        &state.webhook_registry,
    )
    .await
    {
        Ok((flow_run_id, receipt)) => {
            Ok((StatusCode::ACCEPTED, Json(WebhookInvokeResponse { flow_run_id, receipt })))
        }
        Err(e) => {
            state.metrics.collectors().webhook_errors_total.inc();
            Err(ApiError::from(e))
        }
    }
}
