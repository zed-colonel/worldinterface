//! Webhook path validation.

use crate::error::WebhookError;

/// Validate a webhook path.
///
/// Rules:
/// - Must be non-empty
/// - Must not start or end with '/'
/// - Must contain only alphanumeric characters, hyphens, underscores, dots, and slashes
/// - Must not contain '..' (directory traversal)
/// - Maximum length: 256 characters
pub fn validate_webhook_path(path: &str) -> Result<(), WebhookError> {
    if path.is_empty() {
        return Err(WebhookError::InvalidPath("path must not be empty".into()));
    }
    if path.starts_with('/') || path.ends_with('/') {
        return Err(WebhookError::InvalidPath("path must not start or end with '/'".into()));
    }
    if path.contains("..") {
        return Err(WebhookError::InvalidPath("path must not contain '..'".into()));
    }
    if path.len() > 256 {
        return Err(WebhookError::InvalidPath("path must be 256 characters or fewer".into()));
    }
    let valid = path
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/');
    if !valid {
        return Err(WebhookError::InvalidPath(
            "path contains invalid characters (allowed: alphanumeric, -, _, ., /)".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_simple_path() {
        assert!(validate_webhook_path("github/push").is_ok());
    }

    #[test]
    fn valid_nested_path() {
        assert!(validate_webhook_path("org/repo/branch").is_ok());
    }

    #[test]
    fn valid_path_with_dots() {
        assert!(validate_webhook_path("api.v1.hook").is_ok());
    }

    #[test]
    fn valid_path_with_hyphens() {
        assert!(validate_webhook_path("my-webhook").is_ok());
    }

    #[test]
    fn valid_path_with_underscores() {
        assert!(validate_webhook_path("my_webhook").is_ok());
    }

    #[test]
    fn rejects_empty_path() {
        let err = validate_webhook_path("").unwrap_err();
        assert!(matches!(err, WebhookError::InvalidPath(_)));
    }

    #[test]
    fn rejects_leading_slash() {
        let err = validate_webhook_path("/github/push").unwrap_err();
        assert!(matches!(err, WebhookError::InvalidPath(_)));
    }

    #[test]
    fn rejects_trailing_slash() {
        let err = validate_webhook_path("github/push/").unwrap_err();
        assert!(matches!(err, WebhookError::InvalidPath(_)));
    }

    #[test]
    fn rejects_directory_traversal() {
        let err = validate_webhook_path("../secret").unwrap_err();
        assert!(matches!(err, WebhookError::InvalidPath(_)));
    }

    #[test]
    fn rejects_special_characters() {
        let err = validate_webhook_path("hook?q=1").unwrap_err();
        assert!(matches!(err, WebhookError::InvalidPath(_)));
    }

    #[test]
    fn rejects_too_long_path() {
        let long = "a".repeat(257);
        let err = validate_webhook_path(&long).unwrap_err();
        assert!(matches!(err, WebhookError::InvalidPath(_)));
    }

    #[test]
    fn accepts_max_length_path() {
        let max = "a".repeat(256);
        assert!(validate_webhook_path(&max).is_ok());
    }
}
