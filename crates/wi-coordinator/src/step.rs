//! Step handler — executes a single FlowSpec node.
//!
//! Handles three node types:
//! - **Connector**: invokes a connector via `invoke_with_receipt()`
//! - **Transform**: executes a pure transform via `execute_transform()`
//! - **Branch**: evaluates a condition and writes a `BranchResult` to ContextStore

use std::sync::Arc;

use actionqueue_executor_local::handler::{ExecutorContext, HandlerOutput};
use serde::{Deserialize, Serialize};
use wi_connector::execute_transform;
use wi_connector::invoke_with_receipt;
use wi_connector::{CancellationToken, ConnectorError, ConnectorRegistry, InvocationContext};
use wi_contextstore::{AtomicWriter, ContextStore, ContextStoreError};
use wi_core::flowspec::branch::BranchNode;
use wi_core::flowspec::{ConnectorNode, NodeType, TransformNode};
use wi_core::id::NodeId;
use wi_core::metrics::MetricsRecorder;
use wi_flowspec::id::derive_step_run_id;
use wi_flowspec::payload::StepPayload;

use crate::error::{ResolveError, StepError};
use crate::resolve::resolve_params;

/// Result of branch evaluation, stored in ContextStore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchResult {
    /// True if the branch condition evaluated to true.
    pub taken: bool,
    /// The NodeId of the then-edge target.
    pub then_target: NodeId,
    /// The NodeId of the else-edge target (None if no else branch).
    pub else_target: Option<NodeId>,
}

/// Execute a step, returning a HandlerOutput.
///
/// This is the core step execution logic, factored out of the handler for testability.
pub fn execute_step<S: ContextStore>(
    ctx: &ExecutorContext,
    payload: &StepPayload,
    registry: &ConnectorRegistry,
    store: &Arc<S>,
    metrics: &dyn MetricsRecorder,
) -> HandlerOutput {
    match step_inner(ctx, payload, registry, store, metrics) {
        Ok(output) => output,
        Err(e) => map_step_error(e),
    }
}

fn step_inner<S: ContextStore>(
    ctx: &ExecutorContext,
    payload: &StepPayload,
    registry: &ConnectorRegistry,
    store: &Arc<S>,
    metrics: &dyn MetricsRecorder,
) -> Result<HandlerOutput, StepError> {
    let step_type = match &payload.node_type {
        NodeType::Connector(_) => "connector",
        NodeType::Transform(_) => "transform",
        NodeType::Branch(_) => "branch",
    };
    let _span = tracing::info_span!(
        "step_run",
        flow_run_id = %payload.flow_run_id,
        node_id = %payload.node_id,
        step_type = step_type,
    )
    .entered();

    match &payload.node_type {
        NodeType::Connector(connector_node) => {
            execute_connector_step(ctx, payload, connector_node, registry, store, metrics)
        }
        NodeType::Transform(transform_node) => {
            execute_transform_step(payload, transform_node, store, metrics)
        }
        NodeType::Branch(branch_node) => execute_branch_step(payload, branch_node, store, metrics),
    }
}

/// Execute a connector step with crash recovery and receipt generation.
fn execute_connector_step<S: ContextStore>(
    ctx: &ExecutorContext,
    payload: &StepPayload,
    connector_node: &ConnectorNode,
    registry: &ConnectorRegistry,
    store: &Arc<S>,
    metrics: &dyn MetricsRecorder,
) -> Result<HandlerOutput, StepError> {
    // Crash recovery: check if output already exists in ContextStore (Invariant 2 + 4)
    if let Some(_existing) = store.get(payload.flow_run_id, payload.node_id)? {
        tracing::info!(
            %payload.flow_run_id, %payload.node_id,
            "output already exists in ContextStore (idempotent retry), skipping connector invocation"
        );
        return Ok(HandlerOutput::success());
    }

    // Look up connector
    let connector = registry
        .get(&connector_node.connector)
        .ok_or_else(|| StepError::ConnectorNotFound { name: connector_node.connector.clone() })?;

    // Resolve parameters
    let resolved_params = resolve_params(
        &connector_node.params,
        payload.flow_run_id,
        payload.flow_params.as_ref(),
        store.as_ref(),
    )?;

    // Build InvocationContext
    let inv_ctx = build_invocation_context(ctx, payload);

    // Record connector invocation metric
    metrics.record_connector_invocation(&connector_node.connector);

    // Invoke with receipt (Invariant 6)
    let (result, receipt) = invoke_with_receipt(connector.as_ref(), &inv_ctx, &resolved_params);

    match result {
        Ok(output) => {
            // Write output to ContextStore via AtomicWriter (Invariant 2)
            let writer = AtomicWriter::new(Arc::clone(store));
            writer
                .write_and_complete(payload.flow_run_id, payload.node_id, &output, || Ok(()))
                .map_err(|e| match e {
                    wi_contextstore::AtomicWriteError::WriteFailed(store_err) => {
                        StepError::ContextStoreError(store_err)
                    }
                    wi_contextstore::AtomicWriteError::CompletionFailed(e) => {
                        StepError::ContextStoreError(ContextStoreError::StorageError(e.to_string()))
                    }
                })?;

            // Record ContextStore write metric
            metrics.record_contextstore_write();

            // Store receipt durably (Invariant 6)
            let receipt_key = format!("receipt:step:{}:{}", payload.flow_run_id, payload.node_id);
            if let Ok(receipt_value) = serde_json::to_value(&receipt) {
                let _ = store.upsert_global(&receipt_key, &receipt_value);
            }

            // Record success metrics
            metrics.record_step_completed(
                &connector_node.connector,
                receipt.duration_ms as f64 / 1000.0,
            );

            Ok(HandlerOutput::success())
        }
        Err(e) => {
            // Store failure receipt durably (Invariant 6 — receipts even on failure)
            // Best-effort: failure receipt storage should not mask the original error
            let receipt_key = format!("receipt:step:{}:{}", payload.flow_run_id, payload.node_id);
            if let Ok(receipt_value) = serde_json::to_value(&receipt) {
                let _ = store.upsert_global(&receipt_key, &receipt_value);
            }

            // Record failure metrics
            metrics.record_step_failed(&connector_node.connector);

            Err(StepError::ConnectorError(e))
        }
    }
}

/// Execute a transform step.
fn execute_transform_step<S: ContextStore>(
    payload: &StepPayload,
    transform_node: &TransformNode,
    store: &Arc<S>,
    metrics: &dyn MetricsRecorder,
) -> Result<HandlerOutput, StepError> {
    // Crash recovery: check if output already exists
    if store.get(payload.flow_run_id, payload.node_id)?.is_some() {
        tracing::info!(
            %payload.flow_run_id, %payload.node_id,
            "transform output already exists (idempotent retry), skipping"
        );
        return Ok(HandlerOutput::success());
    }

    // Resolve transform input (may contain template references)
    let resolved_input = resolve_params(
        &transform_node.input,
        payload.flow_run_id,
        payload.flow_params.as_ref(),
        store.as_ref(),
    )?;

    // Execute the transform
    let output = execute_transform(&transform_node.transform, &resolved_input)?;

    // Write to ContextStore
    store.put(payload.flow_run_id, payload.node_id, &output).or_else(|e| match e {
        ContextStoreError::AlreadyExists { .. } => Ok(()),
        other => Err(other),
    })?;

    // Record ContextStore write metric
    metrics.record_contextstore_write();

    Ok(HandlerOutput::success())
}

/// Execute a branch step.
fn execute_branch_step<S: ContextStore>(
    payload: &StepPayload,
    branch_node: &BranchNode,
    store: &Arc<S>,
    metrics: &dyn MetricsRecorder,
) -> Result<HandlerOutput, StepError> {
    // Crash recovery: check if branch result already exists
    if store.get(payload.flow_run_id, payload.node_id)?.is_some() {
        tracing::info!(
            %payload.flow_run_id, %payload.node_id,
            "branch result already exists (idempotent retry), skipping"
        );
        return Ok(HandlerOutput::success());
    }

    // Evaluate the branch condition
    let taken = crate::branch_eval::evaluate_branch(
        &branch_node.condition,
        payload.flow_run_id,
        payload.flow_params.as_ref(),
        store.as_ref(),
    )?;

    let branch_result = BranchResult {
        taken,
        then_target: branch_node.then_edge,
        else_target: branch_node.else_edge,
    };

    // Write BranchResult to ContextStore
    let value = serde_json::to_value(&branch_result).map_err(|e| {
        StepError::ContextStoreError(ContextStoreError::StorageError(e.to_string()))
    })?;

    store.put(payload.flow_run_id, payload.node_id, &value).or_else(|e| match e {
        ContextStoreError::AlreadyExists { .. } => Ok(()),
        other => Err(other),
    })?;

    // Record ContextStore write metric
    metrics.record_contextstore_write();

    Ok(HandlerOutput::success())
}

/// Build an InvocationContext from AQ's ExecutorContext and the StepPayload.
fn build_invocation_context(ctx: &ExecutorContext, payload: &StepPayload) -> InvocationContext {
    let step_run_id = derive_step_run_id(payload.flow_run_id, payload.node_id);

    // Bridge cancellation: share AQ's underlying AtomicBool with UI's token.
    // AQ's watchdog thread or dispatch loop can cancel at any time; this
    // gives UI connectors live visibility into that signal with zero overhead.
    let cancellation =
        CancellationToken::from_flag(ctx.input.cancellation_context.token().cancelled_flag());

    InvocationContext {
        flow_run_id: payload.flow_run_id,
        node_id: payload.node_id,
        step_run_id,
        run_id: *ctx.input.run_id.as_uuid(),
        attempt_id: *ctx.input.attempt_id.as_uuid(),
        attempt_number: ctx.input.metadata.attempt_number,
        cancellation,
    }
}

/// Map StepError to HandlerOutput.
fn map_step_error(err: StepError) -> HandlerOutput {
    match &err {
        StepError::ConnectorError(ConnectorError::Retryable(_)) => {
            HandlerOutput::retryable_failure(err.to_string())
        }
        StepError::ConnectorError(ConnectorError::Cancelled) => {
            HandlerOutput::retryable_failure(err.to_string())
        }
        StepError::ConnectorError(ConnectorError::Terminal(_))
        | StepError::ConnectorError(ConnectorError::InvalidParams(_)) => {
            HandlerOutput::terminal_failure(err.to_string())
        }
        StepError::ResolveFailed(ResolveError::NodeOutputNotFound { .. }) => {
            HandlerOutput::retryable_failure(err.to_string())
        }
        StepError::ResolveFailed(ResolveError::ContextStoreError(
            ContextStoreError::StorageError(_),
        )) => HandlerOutput::retryable_failure(err.to_string()),
        StepError::ContextStoreError(ContextStoreError::StorageError(_)) => {
            HandlerOutput::retryable_failure(err.to_string())
        }
        _ => HandlerOutput::terminal_failure(err.to_string()),
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
    use wi_core::flowspec::branch::{BranchCondition, ParamRef};
    use wi_core::flowspec::transform::TransformType;
    use wi_core::id::FlowRunId;
    use wi_core::metrics::NoopMetricsRecorder;

    use super::*;

    static NOOP_METRICS: NoopMetricsRecorder = NoopMetricsRecorder;

    fn make_store() -> Arc<SqliteContextStore> {
        Arc::new(SqliteContextStore::in_memory().unwrap())
    }

    fn make_executor_context() -> ExecutorContext {
        ExecutorContext {
            input: HandlerInput {
                run_id: RunId::new(),
                attempt_id: AttemptId::new(),
                payload: vec![],
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

    fn make_step_payload(
        flow_run_id: FlowRunId,
        node_id: NodeId,
        node_type: NodeType,
    ) -> StepPayload {
        StepPayload {
            task_type: wi_flowspec::payload::TaskType::Step,
            flow_run_id,
            node_id,
            node_type,
            flow_params: None,
        }
    }

    // T-2: Step Handler — Connector Invocation

    #[test]
    fn step_invokes_delay_connector() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 10}),
                idempotency_config: None,
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));
        assert!(store.get(fr, node_id).unwrap().is_some());
    }

    #[test]
    fn step_invokes_fs_write_connector() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "fs.write".into(),
                params: json!({
                    "path": file_path.to_str().unwrap(),
                    "content": "hello world"
                }),
                idempotency_config: None,
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "hello world");
        assert!(store.get(fr, node_id).unwrap().is_some());
    }

    #[test]
    fn step_connector_not_found() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "nonexistent".into(),
                params: json!({}),
                idempotency_config: None,
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::TerminalFailure { .. }));
    }

    #[test]
    fn step_connector_terminal_error() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        // Create file so "create" mode fails
        std::fs::write(&file_path, "existing").unwrap();

        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "fs.write".into(),
                params: json!({
                    "path": file_path.to_str().unwrap(),
                    "content": "new",
                    "mode": "create"
                }),
                idempotency_config: None,
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::TerminalFailure { .. }));
    }

    #[test]
    fn step_connector_invalid_params() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({}), // missing duration_ms
                idempotency_config: None,
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::TerminalFailure { .. }));
    }

    // T-3: Step Handler — Transform Execution

    #[test]
    fn step_executes_identity_transform() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let input_val = json!({"key": "value", "num": 42});
        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Transform(TransformNode {
                transform: TransformType::Identity,
                input: input_val.clone(),
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));
        let stored = store.get(fr, node_id).unwrap().unwrap();
        assert_eq!(stored, input_val);
    }

    #[test]
    fn step_executes_field_mapping() {
        use wi_core::flowspec::transform::{FieldMapping, FieldMappingSpec};

        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Transform(TransformNode {
                transform: TransformType::FieldMapping(FieldMappingSpec {
                    mappings: vec![FieldMapping { from: "a".into(), to: "b".into() }],
                }),
                input: json!({"a": 1}),
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));
        let stored = store.get(fr, node_id).unwrap().unwrap();
        assert_eq!(stored, json!({"b": 1}));
    }

    #[test]
    fn step_transform_error_is_terminal() {
        use wi_core::flowspec::transform::{FieldMapping, FieldMappingSpec};

        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Transform(TransformNode {
                transform: TransformType::FieldMapping(FieldMappingSpec {
                    mappings: vec![FieldMapping { from: "nonexistent".into(), to: "out".into() }],
                }),
                input: json!({"a": 1}),
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::TerminalFailure { .. }));
    }

    // T-4: Step Handler — Branch Evaluation

    #[test]
    fn step_evaluates_exists_true() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        let then_target = NodeId::new();

        let mut payload = make_step_payload(
            fr,
            node_id,
            NodeType::Branch(BranchNode {
                condition: BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                then_edge: then_target,
                else_edge: None,
            }),
        );
        payload.flow_params = Some(json!({"flag": "present"}));

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));

        let stored = store.get(fr, node_id).unwrap().unwrap();
        let branch_result: BranchResult = serde_json::from_value(stored).unwrap();
        assert!(branch_result.taken);
        assert_eq!(branch_result.then_target, then_target);
    }

    #[test]
    fn step_evaluates_exists_false() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        let then_target = NodeId::new();
        let else_target = NodeId::new();

        let mut payload = make_step_payload(
            fr,
            node_id,
            NodeType::Branch(BranchNode {
                condition: BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                then_edge: then_target,
                else_edge: Some(else_target),
            }),
        );
        payload.flow_params = Some(json!({"flag": null}));

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));

        let stored = store.get(fr, node_id).unwrap().unwrap();
        let branch_result: BranchResult = serde_json::from_value(stored).unwrap();
        assert!(!branch_result.taken);
    }

    #[test]
    fn step_evaluates_equals_true() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        let then_target = NodeId::new();

        let mut payload = make_step_payload(
            fr,
            node_id,
            NodeType::Branch(BranchNode {
                condition: BranchCondition::Equals {
                    left: ParamRef::FlowParam { path: "status".into() },
                    right: json!("ok"),
                },
                then_edge: then_target,
                else_edge: None,
            }),
        );
        payload.flow_params = Some(json!({"status": "ok"}));

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));

        let stored = store.get(fr, node_id).unwrap().unwrap();
        let branch_result: BranchResult = serde_json::from_value(stored).unwrap();
        assert!(branch_result.taken);
    }

    #[test]
    fn step_expression_not_implemented() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        let then_target = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Branch(BranchNode {
                condition: BranchCondition::Expression("1 + 1".into()),
                then_edge: then_target,
                else_edge: None,
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::TerminalFailure { .. }));
    }

    // T-5: Step Handler — Crash Recovery

    #[test]
    fn step_skips_invocation_if_output_exists() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        // Pre-write output
        store.put(fr, node_id, &json!({"pre": "existing"})).unwrap();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 10}),
                idempotency_config: None,
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));
        // Output should still be the pre-existing one
        let stored = store.get(fr, node_id).unwrap().unwrap();
        assert_eq!(stored, json!({"pre": "existing"}));
    }

    // T-11: InvocationContext Construction

    #[test]
    fn context_has_correct_ids() {
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 0}),
                idempotency_config: None,
            }),
        );

        let inv_ctx = build_invocation_context(&ctx, &payload);
        assert_eq!(inv_ctx.flow_run_id, fr);
        assert_eq!(inv_ctx.node_id, node_id);
        assert_eq!(inv_ctx.run_id, *ctx.input.run_id.as_uuid());
        assert_eq!(inv_ctx.attempt_id, *ctx.input.attempt_id.as_uuid());
        assert_eq!(inv_ctx.attempt_number, 1);
    }

    #[test]
    fn cancellation_token_bridges_correctly() {
        let ctx = {
            let c = make_executor_context();
            c.input.cancellation_context.cancel();
            c
        };

        let payload = make_step_payload(
            FlowRunId::new(),
            NodeId::new(),
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({}),
                idempotency_config: None,
            }),
        );

        let inv_ctx = build_invocation_context(&ctx, &payload);
        assert!(inv_ctx.cancellation.is_cancelled());
    }

    // T-10: Receipt Generation (Invariant 6)

    #[test]
    fn connector_step_produces_receipt() {
        // invoke_with_receipt always produces a receipt, even on success.
        // Verify by calling it directly with a real connector.
        let registry = default_registry();
        let connector = registry.get("delay").unwrap();
        let inv_ctx = InvocationContext {
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            step_run_id: wi_flowspec::id::derive_step_run_id(FlowRunId::new(), NodeId::new()),
            run_id: uuid::Uuid::new_v4(),
            attempt_id: uuid::Uuid::new_v4(),
            attempt_number: 1,
            cancellation: CancellationToken::new(),
        };
        let params = json!({"duration_ms": 10});
        let (result, receipt) = invoke_with_receipt(connector.as_ref(), &inv_ctx, &params);
        assert!(result.is_ok());
        assert_eq!(receipt.status, wi_core::receipt::ReceiptStatus::Success);
        assert_eq!(receipt.connector, "delay");
    }

    #[test]
    fn connector_step_produces_receipt_on_failure() {
        let registry = default_registry();
        let connector = registry.get("delay").unwrap();
        let inv_ctx = InvocationContext {
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            step_run_id: wi_flowspec::id::derive_step_run_id(FlowRunId::new(), NodeId::new()),
            run_id: uuid::Uuid::new_v4(),
            attempt_id: uuid::Uuid::new_v4(),
            attempt_number: 1,
            cancellation: CancellationToken::new(),
        };
        let params = json!({}); // missing duration_ms → InvalidParams
        let (result, receipt) = invoke_with_receipt(connector.as_ref(), &inv_ctx, &params);
        assert!(result.is_err());
        assert_eq!(receipt.status, wi_core::receipt::ReceiptStatus::Failure);
        assert_eq!(receipt.connector, "delay");
        assert!(receipt.error.is_some());
    }

    #[test]
    fn transform_step_does_not_produce_receipt() {
        // Transform steps write to ContextStore but do NOT call invoke_with_receipt.
        // Verify by executing a transform and confirming the output exists but
        // no receipt-related code path is taken (no connector invocation).
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Transform(TransformNode {
                transform: TransformType::Identity,
                input: json!({"key": "value"}),
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));
        // Output is in ContextStore (transform ran), but no receipt was produced
        // because the code path goes through execute_transform_step, not
        // execute_connector_step (which is the only path that calls invoke_with_receipt).
        assert!(store.get(fr, node_id).unwrap().is_some());
    }

    // T-4: Durable Receipt Storage (Sprint 8)

    #[test]
    fn connector_step_stores_receipt_on_success() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 10}),
                idempotency_config: None,
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));

        // Receipt should be stored in ContextStore globals
        let receipt_key = format!("receipt:step:{}:{}", fr, node_id);
        let receipt = store.get_global(&receipt_key).unwrap();
        assert!(receipt.is_some(), "receipt should be stored on success");
        let receipt = receipt.unwrap();
        assert_eq!(receipt.get("connector").and_then(|v| v.as_str()), Some("delay"));
        assert_eq!(receipt.get("status").and_then(|v| v.as_str()), Some("success"));
    }

    #[test]
    fn connector_step_stores_receipt_on_failure() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({}), // missing duration_ms → InvalidParams → failure
                idempotency_config: None,
            }),
        );

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::TerminalFailure { .. }));

        // Failure receipt should still be stored (best-effort)
        let receipt_key = format!("receipt:step:{}:{}", fr, node_id);
        let receipt = store.get_global(&receipt_key).unwrap();
        assert!(receipt.is_some(), "receipt should be stored even on failure");
        let receipt = receipt.unwrap();
        assert_eq!(receipt.get("status").and_then(|v| v.as_str()), Some("failure"));
        assert!(receipt.get("error").is_some(), "failure receipt should have error field");
    }

    #[test]
    fn receipt_has_correct_flow_run_id() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 10}),
                idempotency_config: None,
            }),
        );

        execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);

        let receipt_key = format!("receipt:step:{}:{}", fr, node_id);
        let receipt = store.get_global(&receipt_key).unwrap().unwrap();
        assert_eq!(
            receipt.get("flow_run_id").and_then(|v| v.as_str()),
            Some(fr.to_string()).as_deref()
        );
    }

    #[test]
    fn receipt_has_nonzero_duration() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 50}),
                idempotency_config: None,
            }),
        );

        execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);

        let receipt_key = format!("receipt:step:{}:{}", fr, node_id);
        let receipt = store.get_global(&receipt_key).unwrap().unwrap();
        let duration = receipt.get("duration_ms").and_then(|v| v.as_u64()).unwrap();
        assert!(duration >= 50, "duration should be >= 50ms, got {}ms", duration);
    }

    #[test]
    fn transform_step_does_not_store_receipt() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Transform(TransformNode {
                transform: TransformType::Identity,
                input: json!({"key": "value"}),
            }),
        );

        execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);

        // Transform steps should NOT store receipts (no boundary crossing)
        let receipt_key = format!("receipt:step:{}:{}", fr, node_id);
        let receipt = store.get_global(&receipt_key).unwrap();
        assert!(receipt.is_none(), "transform steps should not produce receipts");
    }

    #[test]
    fn branch_step_does_not_produce_receipt() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();
        let then_target = NodeId::new();

        let mut payload = make_step_payload(
            fr,
            node_id,
            NodeType::Branch(BranchNode {
                condition: BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                then_edge: then_target,
                else_edge: None,
            }),
        );
        payload.flow_params = Some(json!({"flag": true}));

        let result = execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        assert!(matches!(result, HandlerOutput::Success { .. }));
        // BranchResult is in ContextStore, but no receipt was produced
        // because branch evaluation goes through execute_branch_step, not
        // execute_connector_step.
        assert!(store.get(fr, node_id).unwrap().is_some());

        // Verify no receipt key exists (branch steps are not boundary crossings)
        let receipt_key = format!("receipt:step:{}:{}", fr, node_id);
        assert!(
            store.get_global(&receipt_key).unwrap().is_none(),
            "branch steps should not produce receipts"
        );
    }

    // T-10: Tracing Span Assertions (Sprint 8)

    use std::io;
    use std::sync::Mutex;

    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    /// Shared buffer writer for capturing tracing output in tests.
    struct BufWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Clone for BufWriter {
        fn clone(&self) -> Self {
            BufWriter(Arc::clone(&self.0))
        }
    }

    impl<'a> fmt::MakeWriter<'a> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Run a closure with a tracing subscriber that captures output, return the captured text.
    fn capture_tracing<F: FnOnce()>(f: F) -> String {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer = BufWriter(Arc::clone(&buf));
        let subscriber = tracing_subscriber::registry().with(
            fmt::layer()
                .with_writer(writer)
                .with_ansi(false)
                .with_target(false)
                .with_level(true)
                .with_span_events(fmt::format::FmtSpan::NEW | fmt::format::FmtSpan::CLOSE),
        );
        tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn step_run_span_has_flow_run_id_and_node_id() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 10}),
                idempotency_config: None,
            }),
        );

        let output = capture_tracing(|| {
            execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        });

        assert!(
            output.contains(&fr.to_string()),
            "tracing output should contain flow_run_id {}, got:\n{}",
            fr,
            output
        );
        assert!(
            output.contains(&node_id.to_string()),
            "tracing output should contain node_id {}, got:\n{}",
            node_id,
            output
        );
        assert!(
            output.contains("step_run"),
            "tracing output should contain step_run span name, got:\n{}",
            output
        );
    }

    #[test]
    fn step_run_span_has_step_type() {
        let store = make_store();
        let registry = default_registry();
        let ctx = make_executor_context();
        let fr = FlowRunId::new();
        let node_id = NodeId::new();

        let payload = make_step_payload(
            fr,
            node_id,
            NodeType::Transform(TransformNode {
                transform: TransformType::Identity,
                input: json!({"x": 1}),
            }),
        );

        let output = capture_tracing(|| {
            execute_step(&ctx, &payload, &registry, &store, &NOOP_METRICS);
        });

        assert!(
            output.contains("step_type"),
            "tracing output should contain step_type field, got:\n{}",
            output
        );
        assert!(
            output.contains("transform"),
            "tracing output should contain step_type=transform, got:\n{}",
            output
        );
    }
}
