//! Router construction.

use tower_http::trace::TraceLayer;

use crate::routes;
use crate::state::SharedState;

/// Build the complete daemon router with all endpoint modules.
pub fn build_router(state: SharedState) -> axum::Router {
    let router = axum::Router::new();
    let router = routes::health::register_routes(router);
    let router = routes::capabilities::register_routes(router);
    let router = routes::flows::register_routes(router);
    let router = routes::runs::register_routes(router);
    let router = routes::invoke::register_routes(router);
    let router = routes::webhooks::register_routes(router);
    let router = routes::metrics::register_routes(router);
    router.with_state(state).layer(TraceLayer::new_for_http())
}
