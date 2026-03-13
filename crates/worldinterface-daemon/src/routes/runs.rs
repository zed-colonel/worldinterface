//! Run status endpoints.

use axum::extract::{Path, State};
use axum::Json;
use serde::Serialize;
use worldinterface_core::id::FlowRunId;
use worldinterface_host::{FlowRunStatus, FlowRunSummary};

use crate::error::ApiError;
use crate::state::SharedState;

pub fn register_routes(router: axum::Router<SharedState>) -> axum::Router<SharedState> {
    router
        .route("/api/v1/runs", axum::routing::get(list_runs))
        .route("/api/v1/runs/:id", axum::routing::get(get_run))
}

#[derive(Serialize)]
struct ListRunsResponse {
    runs: Vec<FlowRunSummary>,
}

async fn list_runs(State(state): State<SharedState>) -> Result<Json<ListRunsResponse>, ApiError> {
    let runs = state.host.list_runs().await?;
    Ok(Json(ListRunsResponse { runs }))
}

async fn get_run(
    State(state): State<SharedState>,
    Path(id_str): Path<String>,
) -> Result<Json<FlowRunStatus>, ApiError> {
    let id: FlowRunId = id_str
        .parse()
        .map_err(|_| ApiError::BadRequest(format!("invalid flow run ID: {}", id_str)))?;
    let status = state.host.run_status(id).await?;
    Ok(Json(status))
}
