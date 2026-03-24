//! Streaming connector types for persistent inbound I/O.
//!
//! These types define the boundary between the streaming lifecycle manager
//! (owned by WI) and the embedding application. WI calls the handler with
//! parsed messages; the handler decides what to do with them.

use std::collections::HashMap;

/// A message received from a streaming connector's `on_message()` export.
///
/// The WASM module parses protocol-specific frames into these generic messages.
/// The embedding application's `StreamMessageHandler` receives them.
#[derive(Debug, Clone)]
pub struct StreamMessage {
    /// External identity of the message source.
    /// Format: "{platform}:{type}:{id}" (e.g., "discord:user:123456789").
    pub source_identity: String,
    /// The message content (text).
    pub content: String,
    /// Platform-specific metadata (e.g., channel_id, guild_id, message_id).
    pub metadata: HashMap<String, String>,
}

/// Callback for handling messages received from streaming connectors.
///
/// The streaming lifecycle manager calls this with parsed messages from
/// WASM modules' `on_message()` exports. The embedding application decides
/// what to do with the messages — Exoskeleton writes them to the inbox;
/// a standalone WI deployment might trigger a workflow or log them.
///
/// Handler error type uses `String` for maximum flexibility — embedding
/// applications convert their domain errors to strings.
pub trait StreamMessageHandler: Send + Sync {
    /// Handle a batch of parsed messages from a streaming connector.
    ///
    /// Called on the Tool AQ's tokio runtime (blocking thread). Must not
    /// block for extended periods — the streaming lifecycle resumes
    /// listening after this returns.
    fn handle_messages(
        &self,
        connector_name: &str,
        messages: Vec<StreamMessage>,
    ) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    type ReceivedMessages = Vec<(String, Vec<StreamMessage>)>;

    struct MockHandler {
        received: Arc<Mutex<ReceivedMessages>>,
    }

    impl StreamMessageHandler for MockHandler {
        fn handle_messages(
            &self,
            connector_name: &str,
            messages: Vec<StreamMessage>,
        ) -> Result<(), String> {
            self.received.lock().unwrap().push((connector_name.to_string(), messages));
            Ok(())
        }
    }

    // ── E4S3-T1: StreamMessageHandler trait callable ──

    #[test]
    fn stream_message_handler_trait_callable() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let handler = MockHandler { received: received.clone() };

        let messages = vec![StreamMessage {
            source_identity: "discord:user:123".into(),
            content: "hello".into(),
            metadata: HashMap::new(),
        }];

        handler.handle_messages("discord", messages).unwrap();

        let locked = received.lock().unwrap();
        assert_eq!(locked.len(), 1);
        assert_eq!(locked[0].0, "discord");
        assert_eq!(locked[0].1.len(), 1);
    }

    // ── E4S3-T2: StreamMessage fields accessible ──

    #[test]
    fn stream_message_fields_accessible() {
        let mut metadata = HashMap::new();
        metadata.insert("channel_id".into(), "ch-100".into());
        metadata.insert("guild_id".into(), "guild-200".into());

        let msg = StreamMessage {
            source_identity: "discord:user:456789".into(),
            content: "what is the weather?".into(),
            metadata,
        };

        assert_eq!(msg.source_identity, "discord:user:456789");
        assert_eq!(msg.content, "what is the weather?");
        assert_eq!(msg.metadata.get("channel_id").unwrap(), "ch-100");
        assert_eq!(msg.metadata.get("guild_id").unwrap(), "guild-200");
    }
}
