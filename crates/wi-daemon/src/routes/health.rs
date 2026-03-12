//! Health check endpoints.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::state::SharedState;

pub fn register_routes(router: axum::Router<SharedState>) -> axum::Router<SharedState> {
    router.route("/healthz", axum::routing::get(healthz)).route("/ready", axum::routing::get(ready))
}

async fn healthz() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn ready() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ready" })))
}
