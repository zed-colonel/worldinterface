//! Multiplexed handler — routes Coordinator and Step tasks.
//!
//! ActionQueue takes a single `ExecutorHandler`. This module provides
//! `FlowHandler`, which inspects the task payload to determine the task
//! type and routes to the appropriate execution path.

use std::sync::Arc;

use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use serde::Deserialize;
use wi_connector::ConnectorRegistry;
use wi_contextstore::ContextStore;
use wi_core::metrics::MetricsRecorder;
use wi_flowspec::payload::{CoordinatorPayload, StepPayload, TaskType};

/// Minimal struct for routing — only deserializes the task_type discriminator.
#[derive(Deserialize)]
struct PayloadPeek {
    task_type: TaskType,
}

/// The top-level handler registered with ActionQueue.
///
/// Routes execution based on the `TaskType` discriminator in the payload.
pub struct FlowHandler<S: ContextStore> {
    registry: Arc<ConnectorRegistry>,
    store: Arc<S>,
    metrics: Arc<dyn MetricsRecorder>,
}

impl<S: ContextStore> FlowHandler<S> {
    pub fn new(
        registry: Arc<ConnectorRegistry>,
        store: Arc<S>,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self { registry, store, metrics }
    }
}

impl<S: ContextStore + 'static> ExecutorHandler for FlowHandler<S> {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        // 1. Peek at the task_type discriminator
        let peek: PayloadPeek = match serde_json::from_slice(&ctx.input.payload) {
            Ok(p) => p,
            Err(e) => {
                return HandlerOutput::terminal_failure(format!(
                    "payload deserialization failed: {e}"
                ));
            }
        };

        // 2. Route based on task_type
        match peek.task_type {
            TaskType::Coordinator => {
                let payload: CoordinatorPayload = match serde_json::from_slice(&ctx.input.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        return HandlerOutput::terminal_failure(format!(
                            "coordinator payload deserialization failed: {e}"
                        ));
                    }
                };
                crate::coordinator::execute_coordinator(&ctx, &payload, self.store.as_ref())
            }
            TaskType::Step => {
                let payload: StepPayload = match serde_json::from_slice(&ctx.input.payload) {
                    Ok(p) => p,
                    Err(e) => {
                        return HandlerOutput::terminal_failure(format!(
                            "step payload deserialization failed: {e}"
                        ));
                    }
                };
                crate::step::execute_step(
                    &ctx,
                    &payload,
                    &self.registry,
                    &self.store,
                    &*self.metrics,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::{AttemptId, RunId};
    use actionqueue_core::task::safety::SafetyLevel;
    use actionqueue_executor_local::handler::{AttemptMetadata, CancellationContext, HandlerInput};
    use serde_json::json;
    use wi_connector::connectors::default_registry;
    use wi_contextstore::SqliteContextStore;
    use wi_core::flowspec::{ConnectorNode, NodeType};
    use wi_core::id::{FlowRunId, NodeId};
    use wi_core::metrics::NoopMetricsRecorder;
    use wi_flowspec::payload::{StepPayload, TaskType};

    use super::*;

    fn make_handler() -> (FlowHandler<SqliteContextStore>, Arc<SqliteContextStore>) {
        let store = Arc::new(SqliteContextStore::in_memory().unwrap());
        let registry = Arc::new(default_registry());
        let metrics: Arc<dyn MetricsRecorder> = Arc::new(NoopMetricsRecorder);
        let handler = FlowHandler::new(registry, Arc::clone(&store), metrics);
        (handler, store)
    }

    fn make_ctx_with_payload(payload_bytes: Vec<u8>) -> ExecutorContext {
        ExecutorContext {
            input: HandlerInput {
                run_id: RunId::new(),
                attempt_id: AttemptId::new(),
                payload: payload_bytes,
                metadata: AttemptMetadata {
                    max_attempts: 3,
                    attempt_number: 1,
                    timeout_secs: None,
                    safety_level: SafetyLevel::Idempotent,
                },
                cancellation_context: CancellationContext::new(),
            },
            submission: None,
            children: None,
        }
    }

    // T-1: Multiplexed Handler Routing

    #[test]
    fn routes_step_payload() {
        let (handler, store) = make_handler();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = StepPayload {
            task_type: TaskType::Step,
            flow_run_id: fr,
            node_id,
            node_type: NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 10}),
                idempotency_config: None,
            }),
            flow_params: None,
        };
        let bytes = serde_json::to_vec(&payload).unwrap();
        let ctx = make_ctx_with_payload(bytes);

        let result = handler.execute(ctx);
        assert!(matches!(result, HandlerOutput::Success { .. }));
        assert!(store.get(fr, node_id).unwrap().is_some());
    }

    #[test]
    fn routes_coordinator_payload() {
        let (handler, _store) = make_handler();

        // A coordinator with no children snapshot suspends on first dispatch
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        let spec = wi_core::flowspec::FlowSpec {
            id: None,
            name: None,
            nodes: vec![wi_core::flowspec::Node {
                id: node_id,
                label: None,
                node_type: NodeType::Connector(ConnectorNode {
                    connector: "delay".into(),
                    params: json!({"duration_ms": 10}),
                    idempotency_config: None,
                }),
            }],
            edges: vec![],
            params: None,
        };

        let task_id = wi_flowspec::id::derive_task_id(fr, node_id);
        let mut node_task_map = std::collections::HashMap::new();
        node_task_map.insert(node_id, task_id);

        let payload = wi_flowspec::payload::CoordinatorPayload {
            task_type: TaskType::Coordinator,
            flow_spec: spec,
            flow_run_id: fr,
            node_task_map,
            dependencies: std::collections::HashMap::new(),
        };
        let bytes = serde_json::to_vec(&payload).unwrap();

        // Need to provide a submission port for the coordinator
        let submission = Arc::new(TestSubmissionPort::new());
        let ctx = ExecutorContext {
            input: HandlerInput {
                run_id: RunId::new(),
                attempt_id: AttemptId::new(),
                payload: bytes,
                metadata: AttemptMetadata {
                    max_attempts: 1,
                    attempt_number: 1,
                    timeout_secs: None,
                    safety_level: SafetyLevel::Pure,
                },
                cancellation_context: CancellationContext::new(),
            },
            submission: Some(submission.clone()),
            children: None,
        };

        let result = handler.execute(ctx);
        // Coordinator should submit the step and suspend
        assert!(matches!(result, HandlerOutput::Suspended { .. }));
        assert_eq!(submission.submitted_count(), 1);
    }

    #[test]
    fn rejects_corrupt_payload() {
        let (handler, _) = make_handler();
        let ctx = make_ctx_with_payload(b"not valid json".to_vec());
        let result = handler.execute(ctx);
        assert!(matches!(result, HandlerOutput::TerminalFailure { .. }));
    }

    #[test]
    fn rejects_unknown_task_type() {
        let (handler, _) = make_handler();
        let ctx =
            make_ctx_with_payload(serde_json::to_vec(&json!({"task_type": "unknown"})).unwrap());
        let result = handler.execute(ctx);
        assert!(matches!(result, HandlerOutput::TerminalFailure { .. }));
    }

    /// Test submission port that records submissions.
    struct TestSubmissionPort {
        submissions: std::sync::Mutex<
            Vec<(actionqueue_core::task::task_spec::TaskSpec, Vec<actionqueue_core::ids::TaskId>)>,
        >,
    }

    impl TestSubmissionPort {
        fn new() -> Self {
            Self { submissions: std::sync::Mutex::new(Vec::new()) }
        }

        fn submitted_count(&self) -> usize {
            self.submissions.lock().unwrap().len()
        }
    }

    impl actionqueue_executor_local::handler::TaskSubmissionPort for TestSubmissionPort {
        fn submit(
            &self,
            task_spec: actionqueue_core::task::task_spec::TaskSpec,
            dependencies: Vec<actionqueue_core::ids::TaskId>,
        ) {
            self.submissions.lock().unwrap().push((task_spec, dependencies));
        }
    }
}
