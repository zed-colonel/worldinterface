//! Capability discovery endpoints.

use axum::extract::{Path, State};
use axum::Json;
use serde::Serialize;
use wi_core::descriptor::Descriptor;

use crate::error::ApiError;
use crate::state::SharedState;

pub fn register_routes(router: axum::Router<SharedState>) -> axum::Router<SharedState> {
    router
        .route("/api/v1/capabilities", axum::routing::get(list_capabilities))
        .route("/api/v1/capabilities/:name", axum::routing::get(describe_capability))
}

#[derive(Serialize)]
struct ListCapabilitiesResponse {
    capabilities: Vec<Descriptor>,
}

async fn list_capabilities(State(state): State<SharedState>) -> Json<ListCapabilitiesResponse> {
    let capabilities = state.host.list_capabilities();
    Json(ListCapabilitiesResponse { capabilities })
}

async fn describe_capability(
    State(state): State<SharedState>,
    Path(name): Path<String>,
) -> Result<Json<Descriptor>, ApiError> {
    state
        .host
        .describe(&name)
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("connector '{}' not found", name)))
}
