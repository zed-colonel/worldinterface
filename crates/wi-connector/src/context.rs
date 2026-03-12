//! Invocation context and cancellation token for connector invocations.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use uuid::Uuid;
use wi_core::id::{FlowRunId, NodeId, StepRunId};

/// Context provided to every connector invocation.
///
/// Contains identity fields for idempotency (Invariant 3), tracing, and
/// cooperative cancellation.
#[derive(Debug, Clone)]
pub struct InvocationContext {
    /// Which flow run this invocation belongs to.
    pub flow_run_id: FlowRunId,
    /// Which node in the FlowSpec graph is being executed.
    pub node_id: NodeId,
    /// The step execution identity (deterministic from FlowRunId + NodeId).
    pub step_run_id: StepRunId,
    /// The AQ RunId — the idempotency key for this invocation.
    /// Stored as raw UUID to avoid coupling wi-connector to actionqueue-core.
    pub run_id: Uuid,
    /// The AQ AttemptId — unique per retry attempt. Used for logging, NOT
    /// for idempotency.
    pub attempt_id: Uuid,
    /// Which attempt this is (1-indexed). Useful for connectors that want
    /// to log retry information.
    pub attempt_number: u32,
    /// Cooperative cancellation signal. Connectors performing long operations
    /// should poll this at bounded intervals.
    pub cancellation: CancellationToken,
}

/// Cooperative cancellation signal for connector invocations.
///
/// Wraps an atomic flag that the runtime sets when a timeout or cancellation
/// is requested. Connectors performing long operations should check
/// `is_cancelled()` at bounded intervals.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self { cancelled: Arc::new(AtomicBool::new(false)) }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Construct from an external atomic flag. Used by the Step handler
    /// (Sprint 4) to bridge AQ's CancellationToken.
    pub fn from_flag(flag: Arc<AtomicBool>) -> Self {
        Self { cancelled: flag }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invocation_context_carries_all_fields() {
        let ctx = InvocationContext {
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            step_run_id: StepRunId::new(),
            run_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            attempt_number: 1,
            cancellation: CancellationToken::new(),
        };
        // All fields are accessible
        let _ = ctx.flow_run_id;
        let _ = ctx.node_id;
        let _ = ctx.step_run_id;
        let _ = ctx.run_id;
        let _ = ctx.attempt_id;
        assert_eq!(ctx.attempt_number, 1);
        assert!(!ctx.cancellation.is_cancelled());
    }

    #[test]
    fn cancellation_token_starts_uncancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn cancellation_token_cancel_sets_flag() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancellation_token_from_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        let token = CancellationToken::from_flag(Arc::clone(&flag));
        assert!(!token.is_cancelled());
        flag.store(true, Ordering::Release);
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancellation_token_is_clone() {
        let token = CancellationToken::new();
        let clone = token.clone();
        clone.cancel();
        assert!(token.is_cancelled());
    }
}
