//! Webhook identity types.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a webhook registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WebhookId(Uuid);

impl Default for WebhookId {
    fn default() -> Self {
        Self::new()
    }
}

impl WebhookId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl From<Uuid> for WebhookId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for WebhookId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for WebhookId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse::<Uuid>()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_id_new_is_unique() {
        let a = WebhookId::new();
        let b = WebhookId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn webhook_id_display_and_parse() {
        let id = WebhookId::new();
        let s = id.to_string();
        let parsed: WebhookId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn webhook_id_serializes() {
        let id = WebhookId::new();
        let json = serde_json::to_string(&id).unwrap();
        let back: WebhookId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }
}
