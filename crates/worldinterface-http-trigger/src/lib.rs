//! Dynamic webhook ingress for WorldInterface.
//!
//! Provides HTTPS endpoints that trigger FlowRuns when called. Webhook
//! registration creates a dynamic route; incoming requests are atomically
//! appended to ActionQueue as trigger tasks.

pub mod error;
pub mod handler;
pub mod receipt;
pub mod registration;
pub mod registry;
pub mod trigger;
pub mod types;
pub mod validation;

pub use error::WebhookError;
pub use handler::handle_webhook;
pub use receipt::create_trigger_receipt;
pub use registration::WebhookRegistration;
pub use registry::WebhookRegistry;
pub use trigger::TriggerInput;
pub use types::WebhookId;
pub use validation::validate_webhook_path;
