//! Flow submission endpoints.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use wi_core::flowspec::FlowSpec;
use wi_core::id::FlowRunId;

use crate::error::ApiError;
use crate::state::SharedState;

pub fn register_routes(router: axum::Router<SharedState>) -> axum::Router<SharedState> {
    router
        .route("/api/v1/flows", axum::routing::post(submit_flow))
        .route("/api/v1/flows/ephemeral", axum::routing::post(submit_ephemeral_flow))
}

#[derive(Serialize, Deserialize)]
pub struct SubmitFlowRequest {
    pub spec: FlowSpec,
}

#[derive(Serialize, Deserialize)]
pub struct SubmitFlowResponse {
    pub flow_run_id: FlowRunId,
}

async fn submit_flow(
    State(state): State<SharedState>,
    Json(request): Json<SubmitFlowRequest>,
) -> Result<(StatusCode, Json<SubmitFlowResponse>), ApiError> {
    let flow_run_id = state.host.submit_flow(request.spec).await?;
    Ok((StatusCode::ACCEPTED, Json(SubmitFlowResponse { flow_run_id })))
}

async fn submit_ephemeral_flow(
    State(state): State<SharedState>,
    Json(spec): Json<FlowSpec>,
) -> Result<(StatusCode, Json<SubmitFlowResponse>), ApiError> {
    let flow_run_id = state.host.submit_flow(spec).await?;
    Ok((StatusCode::ACCEPTED, Json(SubmitFlowResponse { flow_run_id })))
}
