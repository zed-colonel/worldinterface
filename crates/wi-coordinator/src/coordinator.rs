//! Coordinator handler — orchestrates a FlowRun's lifecycle.
//!
//! The Coordinator is a **stateless, multi-dispatch task**. Each time AQ
//! dispatches it, it reconstructs its world-view from:
//! - `CoordinatorPayload` (immutable, from the task payload)
//! - `ChildrenSnapshot` (fresh AQ state each dispatch)
//! - `ContextStore` (branch results)
//!
//! It uses `HandlerOutput::Suspended` to yield control between phases.

use std::collections::{HashMap, HashSet};

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_executor_local::handler::{ExecutorContext, HandlerOutput};
use serde_json::Value;
use wi_contextstore::ContextStore;
use wi_core::flowspec::{EdgeCondition, NodeType};
use wi_core::id::NodeId;
use wi_flowspec::payload::CoordinatorPayload;

use crate::error::CoordinatorError;
use crate::step::BranchResult;

/// Execute the Coordinator logic, returning a HandlerOutput.
pub fn execute_coordinator<S: ContextStore>(
    ctx: &ExecutorContext,
    payload: &CoordinatorPayload,
    store: &S,
) -> HandlerOutput {
    let _span = tracing::info_span!(
        "flow_run",
        flow_run_id = %payload.flow_run_id,
        node_count = payload.flow_spec.nodes.len(),
    )
    .entered();

    match coordinator_inner(ctx, payload, store) {
        Ok(output) => output,
        Err(e) => HandlerOutput::terminal_failure(e.to_string()),
    }
}

fn coordinator_inner<S: ContextStore>(
    ctx: &ExecutorContext,
    payload: &CoordinatorPayload,
    store: &S,
) -> Result<HandlerOutput, CoordinatorError> {
    let children = ctx.children.as_ref();

    // Build reverse map: TaskId -> NodeId
    let task_node_map: HashMap<TaskId, NodeId> =
        payload.node_task_map.iter().map(|(&node_id, &task_id)| (task_id, node_id)).collect();

    // Determine which steps have already been submitted
    let submitted: HashSet<TaskId> =
        children.map(|c| c.children().iter().map(|cs| cs.task_id()).collect()).unwrap_or_default();

    tracing::debug!(
        submitted_count = submitted.len(),
        children_count = children.map(|c| c.len()).unwrap_or(0),
        "coordinator dispatch"
    );

    // Read branch results from ContextStore for completed branch steps
    let branch_results = read_branch_results(payload, &task_node_map, children, store)?;
    // Determine the set of nodes that should NOT be submitted (non-taken branch paths)
    let excluded = compute_excluded_nodes(payload, &branch_results);

    // Submit eligible steps in topological waves.
    //
    // AQ's dependency gate enforces ordering: a step won't be promoted to
    // Ready until all its declared prerequisites complete. We leverage this
    // by passing filtered dependencies (excluding non-taken branch paths)
    // at submission time.
    //
    // Multi-pass: within a single Coordinator dispatch, we repeatedly find
    // steps whose dependencies have all been submitted (either previously
    // or in an earlier pass of this dispatch) and submit them. This lets a
    // linear A→B→C flow submit all three steps in one dispatch rather than
    // requiring a separate suspend/resume cycle per step.
    let newly_submitted = submit_eligible_steps(
        payload,
        &submitted,
        &branch_results,
        &excluded,
        &task_node_map,
        children,
        ctx.submission.as_deref(),
    );

    // Check for flow completion
    let all_submitted: HashSet<TaskId> =
        submitted.iter().chain(newly_submitted.iter()).copied().collect();

    // Expected steps = all nodes minus excluded
    let expected_tasks: HashSet<TaskId> = payload
        .node_task_map
        .iter()
        .filter(|(node_id, _)| !excluded.contains(node_id))
        .map(|(_, task_id)| *task_id)
        .collect();

    // Are all expected steps submitted?
    let all_expected_submitted = expected_tasks.iter().all(|t| all_submitted.contains(t));

    if all_expected_submitted {
        // Check if all submitted children are terminal
        if let Some(children_snap) = children {
            let all_terminal = expected_tasks.iter().all(|task_id| {
                children_snap.get(*task_id).map(|c| c.all_terminal()).unwrap_or(false)
            });

            if all_terminal {
                // Check for failures
                let any_failed = expected_tasks.iter().any(|task_id| {
                    children_snap
                        .get(*task_id)
                        .map(|c| {
                            c.run_states()
                                .iter()
                                .any(|(_, state)| matches!(state, RunState::Failed))
                        })
                        .unwrap_or(false)
                });

                if any_failed {
                    // Find the failed node for error reporting
                    let failed_node = expected_tasks.iter().find_map(|task_id| {
                        let child = children_snap.get(*task_id)?;
                        let has_failure = child
                            .run_states()
                            .iter()
                            .any(|(_, state)| matches!(state, RunState::Failed));
                        if has_failure {
                            Some(task_node_map[task_id])
                        } else {
                            None
                        }
                    });

                    return Err(CoordinatorError::StepFailed {
                        node_id: failed_node.unwrap_or_else(NodeId::new),
                        error: "step failed terminally".into(),
                    });
                }

                // All completed — gather flow outputs
                let outputs = gather_flow_outputs(payload, &excluded, store)?;
                let output_bytes = serde_json::to_vec(&outputs)?;
                return Ok(HandlerOutput::success_with_output(output_bytes));
            }
        }
    }

    // Not done yet — suspend and wait for progress
    Ok(HandlerOutput::suspended())
}

/// Read BranchResults from ContextStore for completed branch nodes.
fn read_branch_results<S: ContextStore>(
    payload: &CoordinatorPayload,
    _task_node_map: &HashMap<TaskId, NodeId>,
    children: Option<&actionqueue_executor_local::children::ChildrenSnapshot>,
    store: &S,
) -> Result<HashMap<NodeId, BranchResult>, CoordinatorError> {
    let mut results = HashMap::new();

    for node in &payload.flow_spec.nodes {
        if !matches!(node.node_type, NodeType::Branch(_)) {
            continue;
        }

        let task_id = payload.node_task_map[&node.id];

        // Check if this branch step has completed
        let completed = children
            .and_then(|c| c.get(task_id))
            .map(|c| c.run_states().iter().any(|(_, state)| matches!(state, RunState::Completed)))
            .unwrap_or(false);

        if !completed {
            continue;
        }

        // Read branch result from ContextStore
        if let Some(value) = store.get(payload.flow_run_id, node.id)? {
            let branch_result: BranchResult = serde_json::from_value(value)
                .map_err(CoordinatorError::PayloadDeserializationFailed)?;
            results.insert(node.id, branch_result);
        }
    }

    Ok(results)
}

/// Compute the set of nodes that should be excluded (non-taken branch paths).
fn compute_excluded_nodes(
    payload: &CoordinatorPayload,
    branch_results: &HashMap<NodeId, BranchResult>,
) -> HashSet<NodeId> {
    let mut excluded = HashSet::new();

    for result in branch_results.values() {
        if result.taken {
            // Then path taken — exclude else target and its downstream
            if let Some(else_target) = result.else_target {
                collect_downstream(payload, else_target, &mut excluded);
            }
        } else {
            // Else path taken (or no else) — exclude then target and its downstream
            collect_downstream(payload, result.then_target, &mut excluded);
            // If there's no else edge and the branch is not taken, the then-target
            // should be excluded but else_target (which doesn't exist) is fine
        }
    }

    excluded
}

/// Collect a node and all its downstream nodes (via edges) into the excluded set.
/// Stops at nodes that have other incoming edges from non-excluded nodes (merge points).
/// `start` is always excluded (it's the non-taken branch target).
fn collect_downstream(payload: &CoordinatorPayload, start: NodeId, excluded: &mut HashSet<NodeId>) {
    // Always exclude the start node (the non-taken branch target).
    if !excluded.insert(start) {
        return; // Already excluded.
    }
    // Seed queue with downstream nodes from start.
    let mut queue: Vec<NodeId> =
        payload.flow_spec.edges.iter().filter(|e| e.from == start).map(|e| e.to).collect();

    while let Some(node_id) = queue.pop() {
        // Check if this node has incoming edges from non-excluded nodes.
        // If so, it's a merge point and should NOT be excluded.
        let has_non_excluded_incoming = payload
            .flow_spec
            .edges
            .iter()
            .any(|e| e.to == node_id && !excluded.contains(&e.from) && e.from != node_id);

        if has_non_excluded_incoming {
            continue;
        }

        if excluded.insert(node_id) {
            for edge in &payload.flow_spec.edges {
                if edge.from == node_id {
                    queue.push(edge.to);
                }
            }
        }
    }
}

/// Submit eligible steps in topological waves, leveraging AQ's dependency gate.
///
/// Each wave finds steps whose dependencies have all been submitted (in a
/// previous Coordinator dispatch OR in an earlier wave of this dispatch),
/// submits them with filtered dependencies, and adds them to the submitted
/// set for the next wave.
///
/// Dependencies are filtered in two ways:
/// 1. **Excluded deps** (non-taken branch paths) are removed — those tasks
///    will never be submitted.
/// 2. **Already-completed deps** are removed — AQ's `gc_task()` may have
///    purged them from the DependencyGate's `satisfied` set, so declaring
///    a late dependency on them would block the dependent forever. The
///    Coordinator's own eligibility check already verified these completed.
///
/// The remaining deps (in-flight or not-yet-run tasks from the current wave)
/// are passed to AQ, which enforces ordering via its dependency gate.
///
/// Returns the set of TaskIds that were newly submitted in this dispatch.
fn submit_eligible_steps(
    payload: &CoordinatorPayload,
    previously_submitted: &HashSet<TaskId>,
    branch_results: &HashMap<NodeId, BranchResult>,
    excluded: &HashSet<NodeId>,
    task_node_map: &HashMap<TaskId, NodeId>,
    children: Option<&actionqueue_executor_local::children::ChildrenSnapshot>,
    submission: Option<&dyn actionqueue_executor_local::handler::TaskSubmissionPort>,
) -> HashSet<TaskId> {
    let mut all_submitted: HashSet<TaskId> = previously_submitted.clone();
    let mut newly_submitted = HashSet::new();

    loop {
        let mut wave = Vec::new();

        for (&node_id, &task_id) in &payload.node_task_map {
            // Skip if already submitted (previously or in an earlier wave)
            if all_submitted.contains(&task_id) {
                continue;
            }

            // Skip excluded nodes (non-taken branch paths)
            if excluded.contains(&node_id) {
                continue;
            }

            // Check if all dependencies are submitted or excluded
            let deps = payload.dependencies.get(&task_id).cloned().unwrap_or_default();
            let all_deps_satisfied = deps.iter().all(|dep_task_id| {
                if all_submitted.contains(dep_task_id) {
                    return true;
                }
                // Excluded deps are satisfied (they will never run)
                task_node_map.get(dep_task_id).map(|n| excluded.contains(n)).unwrap_or(false)
            });

            if !all_deps_satisfied {
                continue;
            }

            // For branch-gated nodes: verify the branch has been evaluated
            // and this node is on the taken path
            if !is_branch_gated(payload, node_id, branch_results, excluded) {
                continue;
            }

            // Filter deps for AQ submission: remove excluded and already-
            // completed tasks. AQ's gc_task() removes completed tasks from
            // the dependency gate's satisfied set; declaring a dependency on
            // a GC'd task would block the dependent permanently.
            let active_deps: Vec<TaskId> = deps
                .iter()
                .filter(|dep_id| {
                    // Remove excluded deps
                    if task_node_map.get(dep_id).map(|n| excluded.contains(n)).unwrap_or(false) {
                        return false;
                    }
                    // Remove deps that have already reached terminal state
                    // (they may have been GC'd from the dependency gate)
                    if let Some(snap) = children {
                        if let Some(child) = snap.get(**dep_id) {
                            if child.all_terminal() {
                                return false;
                            }
                        }
                    }
                    true
                })
                .copied()
                .collect();

            wave.push((task_id, node_id, active_deps));
        }

        if wave.is_empty() {
            break;
        }

        // Submit this wave
        if let Some(port) = submission {
            for &(task_id, node_id, ref active_deps) in &wave {
                let step_spec = rebuild_step_spec(payload, node_id, task_id);
                port.submit(step_spec, active_deps.clone());
            }
        }

        for (task_id, _, _) in &wave {
            all_submitted.insert(*task_id);
            newly_submitted.insert(*task_id);
        }
    }

    newly_submitted
}

/// Check if a node is eligible considering branch gating.
/// Returns true if the node should be submitted, false if it should wait.
fn is_branch_gated(
    payload: &CoordinatorPayload,
    node_id: NodeId,
    branch_results: &HashMap<NodeId, BranchResult>,
    excluded: &HashSet<NodeId>,
) -> bool {
    // Look at incoming edges to this node
    for edge in &payload.flow_spec.edges {
        if edge.to != node_id {
            continue;
        }

        // If the source is a branch node, check if this edge is the taken path
        if let Some(condition) = &edge.condition {
            let source_node = payload.flow_spec.nodes.iter().find(|n| n.id == edge.from);

            if let Some(node) = source_node {
                if matches!(node.node_type, NodeType::Branch(_)) {
                    // Branch must have been evaluated
                    if let Some(result) = branch_results.get(&node.id) {
                        let on_taken_path = match condition {
                            EdgeCondition::BranchTrue => result.taken,
                            EdgeCondition::BranchFalse => !result.taken,
                            EdgeCondition::Always => true,
                        };
                        if !on_taken_path {
                            return false;
                        }
                    } else {
                        // Branch not yet evaluated — can't submit
                        return false;
                    }
                }
            }
        }

        // If the source is excluded, this node shouldn't be submitted via this edge
        // (but it may have other incoming edges)
        if excluded.contains(&edge.from) {
            // Check if there's another non-excluded incoming edge
            let has_other = payload
                .flow_spec
                .edges
                .iter()
                .any(|e| e.to == node_id && e.from != edge.from && !excluded.contains(&e.from));
            if !has_other {
                return false;
            }
        }
    }

    true
}

/// Rebuild a step TaskSpec from the payload data.
fn rebuild_step_spec(
    payload: &CoordinatorPayload,
    node_id: NodeId,
    task_id: TaskId,
) -> actionqueue_core::task::task_spec::TaskSpec {
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::safety::SafetyLevel;
    use actionqueue_core::task::task_spec::TaskPayload;
    use wi_flowspec::payload::{StepPayload, TaskType, PAYLOAD_CONTENT_TYPE};

    let node = payload
        .flow_spec
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .expect("node_id must exist in flow_spec");

    let step_payload = StepPayload {
        task_type: TaskType::Step,
        flow_run_id: payload.flow_run_id,
        node_id,
        node_type: node.node_type.clone(),
        flow_params: payload.flow_spec.params.clone(),
    };
    let payload_bytes = serde_json::to_vec(&step_payload).expect("StepPayload serialization");

    let coordinator_task_id = wi_flowspec::id::derive_coordinator_task_id(payload.flow_run_id);

    let safety_level = match &node.node_type {
        NodeType::Connector(_) => SafetyLevel::Idempotent,
        NodeType::Transform(_) => SafetyLevel::Pure,
        NodeType::Branch(_) => SafetyLevel::Pure,
    };

    let mut constraints = TaskConstraints::default();
    constraints.set_safety_level(safety_level);

    let metadata = TaskMetadata::new(vec!["wi".into(), "step".into()], 0, node.label.clone());

    TaskSpec::new(
        task_id,
        TaskPayload::with_content_type(payload_bytes, PAYLOAD_CONTENT_TYPE),
        RunPolicy::Once,
        constraints,
        metadata,
    )
    .expect("TaskSpec construction")
    .with_parent(coordinator_task_id)
}

use actionqueue_core::task::task_spec::TaskSpec;

/// Gather outputs from terminal nodes (nodes with no outgoing edges) for the flow result.
fn gather_flow_outputs<S: ContextStore>(
    payload: &CoordinatorPayload,
    excluded: &HashSet<NodeId>,
    store: &S,
) -> Result<Value, CoordinatorError> {
    let mut outputs = serde_json::Map::new();

    for node in &payload.flow_spec.nodes {
        if excluded.contains(&node.id) {
            continue;
        }
        // Terminal node = no outgoing edges (among non-excluded nodes)
        let has_outgoing =
            payload.flow_spec.edges.iter().any(|e| e.from == node.id && !excluded.contains(&e.to));

        if has_outgoing {
            continue;
        }

        if let Some(value) = store.get(payload.flow_run_id, node.id)? {
            outputs.insert(node.id.as_ref().to_string(), value);
        }
    }

    Ok(Value::Object(outputs))
}
