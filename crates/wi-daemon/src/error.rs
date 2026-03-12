//! Error types for the daemon.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use wi_host::HostError;
use wi_http_trigger::WebhookError;

/// Top-level daemon errors (startup/shutdown).
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    /// Host startup or shutdown error.
    #[error("host error: {0}")]
    Host(#[from] HostError),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// TCP bind failed.
    #[error("failed to bind: {0}")]
    Bind(#[source] std::io::Error),

    /// HTTP server error.
    #[error("server error: {0}")]
    Serve(#[source] std::io::Error),

    /// I/O error (config file reading, etc.)
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// TOML parsing error.
    #[error("config parse error: {0}")]
    ConfigParse(#[from] toml::de::Error),
}

/// API-level errors that map to HTTP status codes.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Resource not found (404).
    #[error("{0}")]
    NotFound(String),

    /// Bad request — validation, compilation, or parse errors (400).
    #[error("{0}")]
    BadRequest(String),

    /// Conflict — duplicate resource (409).
    #[error("{0}")]
    Conflict(String),

    /// Internal server error (500).
    #[error("{0}")]
    Internal(String),
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };
        (status, Json(ErrorResponse { error: message })).into_response()
    }
}

impl From<HostError> for ApiError {
    fn from(err: HostError) -> Self {
        match err {
            HostError::FlowRunNotFound(_) => ApiError::NotFound(err.to_string()),
            HostError::ConnectorNotFound(_) => ApiError::NotFound(err.to_string()),
            HostError::InvalidConfig(_) => ApiError::BadRequest(err.to_string()),
            HostError::Compilation(_) => ApiError::BadRequest(err.to_string()),
            HostError::FlowFailed { .. } => ApiError::Internal(err.to_string()),
            HostError::FlowCanceled(_) => ApiError::Internal(err.to_string()),
            _ => ApiError::Internal(err.to_string()),
        }
    }
}

impl From<WebhookError> for ApiError {
    fn from(err: WebhookError) -> Self {
        match &err {
            WebhookError::PathAlreadyRegistered(_) => ApiError::Conflict(err.to_string()),
            WebhookError::WebhookNotFound(_) => ApiError::NotFound(err.to_string()),
            WebhookError::PathNotFound(_) => ApiError::NotFound(err.to_string()),
            WebhookError::InvalidPath(_) => ApiError::BadRequest(err.to_string()),
            _ => ApiError::Internal(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;
    use wi_core::id::FlowRunId;

    use super::*;

    #[test]
    fn host_not_found_maps_to_404() {
        let err: ApiError = HostError::FlowRunNotFound(FlowRunId::new()).into();
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    #[test]
    fn host_connector_not_found_maps_to_404() {
        let err: ApiError = HostError::ConnectorNotFound("foo".to_string()).into();
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    #[test]
    fn host_invalid_config_maps_to_400() {
        let err: ApiError = HostError::InvalidConfig("bad".to_string()).into();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[test]
    fn host_internal_error_maps_to_500() {
        let err: ApiError = HostError::InternalError("bug".to_string()).into();
        assert!(matches!(err, ApiError::Internal(_)));
    }

    #[test]
    fn api_error_serializes_to_json() {
        let err = ApiError::NotFound("gone".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn api_error_bad_request_status() {
        let err = ApiError::BadRequest("invalid".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn api_error_internal_status() {
        let err = ApiError::Internal("crash".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn api_error_conflict_status() {
        let err = ApiError::Conflict("duplicate".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    // T-13: WebhookError -> ApiError mapping

    #[test]
    fn webhook_path_registered_maps_to_409() {
        let err: ApiError = WebhookError::PathAlreadyRegistered("github/push".to_string()).into();
        assert!(matches!(err, ApiError::Conflict(_)));
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn webhook_not_found_maps_to_404() {
        let err: ApiError = WebhookError::WebhookNotFound(wi_http_trigger::WebhookId::new()).into();
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    #[test]
    fn webhook_path_not_found_maps_to_404() {
        let err: ApiError = WebhookError::PathNotFound("unknown".to_string()).into();
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    #[test]
    fn webhook_invalid_path_maps_to_400() {
        let err: ApiError = WebhookError::InvalidPath("bad".to_string()).into();
        assert!(matches!(err, ApiError::BadRequest(_)));
    }
}
