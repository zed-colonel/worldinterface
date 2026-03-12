//! `GET /metrics` — Prometheus text exposition endpoint.

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;

use crate::state::SharedState;

pub fn register_routes(router: axum::Router<SharedState>) -> axum::Router<SharedState> {
    router.route("/metrics", axum::routing::get(handle_metrics))
}

async fn handle_metrics(State(state): State<SharedState>) -> impl IntoResponse {
    // Update active flow gauge before encoding
    let active = state.host.active_flow_count();
    state.metrics.collectors().flow_runs_active.set(active as f64);

    match state.metrics.encode_text() {
        Ok(body) => {
            let mut response = body.into_response();
            if let Ok(value) = HeaderValue::from_str("text/plain; version=0.0.4; charset=utf-8") {
                response.headers_mut().insert(axum::http::header::CONTENT_TYPE, value);
            }
            response
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "metrics encoding failed").into_response(),
    }
}
