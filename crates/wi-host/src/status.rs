//! Flow run status types and derivation from AQ projection state.

use std::collections::HashMap;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::RunState;
use actionqueue_storage::recovery::reducer::ReplayReducer;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wi_core::id::{FlowRunId, NodeId};
use wi_flowspec::payload::CoordinatorPayload;

/// Overall status of a flow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowRunStatus {
    pub flow_run_id: FlowRunId,
    pub phase: FlowPhase,
    pub steps: Vec<StepStatus>,
    pub outputs: Option<HashMap<NodeId, Value>>,
    pub error: Option<String>,
    /// Timestamp when the flow was submitted (unix epoch seconds).
    pub submitted_at: u64,
    /// Timestamp of the last state change (unix epoch seconds).
    pub last_updated_at: u64,
    /// Trigger receipt (present when the flow was started via webhook).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_receipt: Option<Value>,
}

/// High-level phase of a flow run's lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowPhase {
    Pending,
    Running,
    Completed,
    Failed,
    Canceled,
}

/// Status of an individual step within a flow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepStatus {
    pub node_id: NodeId,
    pub phase: StepPhase,
    pub output: Option<Value>,
    /// Receipt for this step's boundary crossing (if available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<Value>,
    /// Error message for failed steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Label from the FlowSpec node (if set).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Connector name (for connector nodes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector: Option<String>,
}

/// Lightweight summary of a flow run for list display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowRunSummary {
    pub flow_run_id: FlowRunId,
    pub phase: FlowPhase,
    pub submitted_at: u64,
    pub last_updated_at: u64,
}

/// Phase of an individual step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepPhase {
    Pending,
    Running,
    Completed,
    Failed,
    Excluded,
}

/// Derive the full FlowRunStatus from AQ projection + ContextStore.
pub(crate) fn derive_flow_run_status(
    projection: &ReplayReducer,
    store: &dyn wi_contextstore::ContextStore,
    flow_run_id: FlowRunId,
    coordinator_task_id: TaskId,
) -> FlowRunStatus {
    // Find the Coordinator's latest run
    let coordinator_run = find_latest_run(projection, coordinator_task_id);

    let flow_phase = match coordinator_run {
        None => {
            // Task exists but no run yet (pre-first-tick)
            if projection.get_task(&coordinator_task_id).is_some() {
                FlowPhase::Pending
            } else {
                // Should not happen if coordinator_task_id is valid
                FlowPhase::Pending
            }
        }
        Some(ref run) => match run.state() {
            RunState::Completed => FlowPhase::Completed,
            RunState::Failed => FlowPhase::Failed,
            RunState::Canceled => FlowPhase::Canceled,
            _ => FlowPhase::Running,
        },
    };

    // Collect step statuses by reading the coordinator payload for node→task mapping
    let (steps, outputs) = collect_step_info(projection, store, flow_run_id, coordinator_task_id);

    // Extract error if failed
    let error = if flow_phase == FlowPhase::Failed {
        coordinator_run.as_ref().and_then(|run| extract_last_error(projection, run.id()))
    } else {
        None
    };

    // Timestamps: submitted_at from TaskRecord, last_updated_at from run state
    let submitted_at =
        projection.get_task_record(&coordinator_task_id).map(|r| r.created_at()).unwrap_or(0);
    let last_updated_at =
        coordinator_run.as_ref().map(|r| r.last_state_change_at()).unwrap_or(submitted_at);

    // Look up trigger receipt (present for webhook-triggered flows)
    let trigger_receipt_key = format!("receipt:trigger:{}", flow_run_id);
    let trigger_receipt = store.get_global(&trigger_receipt_key).ok().flatten();

    FlowRunStatus {
        flow_run_id,
        phase: flow_phase,
        steps,
        outputs: if flow_phase == FlowPhase::Completed { Some(outputs) } else { None },
        error,
        submitted_at,
        last_updated_at,
        trigger_receipt,
    }
}

/// Find the latest RunInstance for a task.
pub(crate) fn find_latest_run(
    projection: &ReplayReducer,
    task_id: TaskId,
) -> Option<actionqueue_core::run::RunInstance> {
    projection.runs_for_task(task_id).max_by_key(|r| r.last_state_change_at()).cloned()
}

/// Collect step statuses and outputs from the AQ projection and ContextStore.
fn collect_step_info(
    projection: &ReplayReducer,
    store: &dyn wi_contextstore::ContextStore,
    flow_run_id: FlowRunId,
    coordinator_task_id: TaskId,
) -> (Vec<StepStatus>, HashMap<NodeId, Value>) {
    let mut steps = Vec::new();
    let mut outputs = HashMap::new();

    // Get the coordinator's payload to find node→task mapping
    let coordinator_spec = match projection.get_task(&coordinator_task_id) {
        Some(spec) => spec,
        None => return (steps, outputs),
    };

    let coordinator_payload: CoordinatorPayload =
        match serde_json::from_slice(coordinator_spec.payload()) {
            Ok(p) => p,
            Err(_) => return (steps, outputs),
        };

    // For each node in the flow, derive its step status
    for node in &coordinator_payload.flow_spec.nodes {
        let task_id = match coordinator_payload.node_task_map.get(&node.id) {
            Some(&tid) => tid,
            None => continue,
        };

        // Try to get the step's output from ContextStore
        let output = store.get(flow_run_id, node.id).ok().flatten();

        // Find the step's latest run
        let step_run = find_latest_run(projection, task_id);

        let step_phase = match step_run {
            None => {
                // Check if task exists — if not, it may have been excluded (branch not taken)
                if projection.get_task(&task_id).is_some() {
                    StepPhase::Pending
                } else {
                    // Not submitted → excluded (branch not taken)
                    // But also check if output exists (completed in prior run before crash)
                    if output.is_some() {
                        StepPhase::Completed
                    } else {
                        StepPhase::Excluded
                    }
                }
            }
            Some(ref run) => match run.state() {
                RunState::Completed => StepPhase::Completed,
                RunState::Failed => StepPhase::Failed,
                RunState::Canceled => {
                    // Check if this step was excluded via branch
                    // A canceled step task means the coordinator marked it as excluded
                    if output.is_some() {
                        StepPhase::Completed
                    } else {
                        // Check the step payload — if branch evaluated to exclude, this is Excluded
                        // For now, check via attempt output
                        match extract_step_exclusion(projection, run.id()) {
                            true => StepPhase::Excluded,
                            false => StepPhase::Failed,
                        }
                    }
                }
                _ => StepPhase::Running,
            },
        };

        if let Some(ref val) = output {
            outputs.insert(node.id, val.clone());
        }

        // Look up step receipt from ContextStore globals
        let receipt_key = format!("receipt:step:{}:{}", flow_run_id, node.id);
        let receipt = store.get_global(&receipt_key).ok().flatten();

        // Extract error from AQ attempt history for failed steps
        let step_error = if step_phase == StepPhase::Failed {
            step_run.as_ref().and_then(|run| extract_last_error(projection, run.id()))
        } else {
            None
        };

        // Extract label and connector from FlowSpec node
        let label = node.label.clone();
        let connector = match &node.node_type {
            wi_core::flowspec::NodeType::Connector(c) => Some(c.connector.clone()),
            _ => None,
        };

        steps.push(StepStatus {
            node_id: node.id,
            phase: step_phase,
            output,
            receipt,
            error: step_error,
            label,
            connector,
        });
    }

    (steps, outputs)
}

/// Check if a step run was excluded (branch not taken) by looking at its attempt output.
fn extract_step_exclusion(
    projection: &ReplayReducer,
    run_id: actionqueue_core::ids::RunId,
) -> bool {
    if let Some(history) = projection.get_attempt_history(&run_id) {
        for entry in history.iter().rev() {
            if let Some(output) = entry.output() {
                // The step handler writes "excluded" markers for branch-excluded steps
                if let Ok(payload) = serde_json::from_slice::<Value>(output) {
                    if payload.get("excluded").and_then(|v| v.as_bool()) == Some(true) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Extract the last error message from a run's attempt history.
fn extract_last_error(
    projection: &ReplayReducer,
    run_id: actionqueue_core::ids::RunId,
) -> Option<String> {
    let history = projection.get_attempt_history(&run_id)?;
    history.iter().rev().find_map(|a| a.error().map(|s| s.to_string()))
}

/// Extract all flow outputs from ContextStore for a completed flow.
pub(crate) fn extract_flow_outputs(
    store: &dyn wi_contextstore::ContextStore,
    flow_run_id: FlowRunId,
) -> Result<HashMap<NodeId, Value>, wi_contextstore::error::ContextStoreError> {
    let keys = store.list_keys(flow_run_id)?;
    let mut outputs = HashMap::new();
    for node_id in keys {
        if let Some(val) = store.get(flow_run_id, node_id)? {
            outputs.insert(node_id, val);
        }
    }
    Ok(outputs)
}
