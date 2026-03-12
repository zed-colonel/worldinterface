//! Single-op invocation endpoint.

use axum::extract::{Path, State};
use axum::Json;
use serde::Serialize;
use serde_json::Value;

use crate::error::ApiError;
use crate::state::SharedState;

pub fn register_routes(router: axum::Router<SharedState>) -> axum::Router<SharedState> {
    router.route("/api/v1/invoke/:op", axum::routing::post(invoke_single))
}

#[derive(Serialize)]
struct InvokeResponse {
    output: Value,
}

async fn invoke_single(
    State(state): State<SharedState>,
    Path(op): Path<String>,
    Json(params): Json<Value>,
) -> Result<Json<InvokeResponse>, ApiError> {
    let output = state.host.invoke_single(&op, params).await?;
    Ok(Json(InvokeResponse { output }))
}
