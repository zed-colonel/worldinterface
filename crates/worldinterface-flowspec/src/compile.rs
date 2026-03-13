//! FlowSpec -> ActionQueue DAG compiler.
//!
//! The compiler is a pure function: `FlowSpec -> CompilationResult`. It does not
//! execute anything. The Coordinator handler (Sprint 4) submits the compiled steps
//! to AQ at runtime.

use std::collections::HashMap;

use actionqueue_core::ids::TaskId;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::safety::SafetyLevel;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use worldinterface_core::flowspec::topo;
use worldinterface_core::flowspec::{FlowSpec, NodeType};
use worldinterface_core::id::{FlowRunId, NodeId};

use crate::config::CompilerConfig;
use crate::error::CompilationError;
use crate::id::{derive_coordinator_task_id, derive_task_id};
use crate::payload::{CoordinatorPayload, StepPayload, TaskType, PAYLOAD_CONTENT_TYPE};

/// The complete output of compiling a FlowSpec.
#[derive(Debug, Clone)]
pub struct CompilationResult {
    /// The Coordinator task spec (root of the AQ hierarchy).
    pub coordinator: TaskSpec,
    /// Step task specs, one per FlowSpec node, in topological order.
    pub steps: Vec<TaskSpec>,
    /// Dependency declarations: step TaskId -> Vec of prerequisite TaskIds.
    pub dependencies: HashMap<TaskId, Vec<TaskId>>,
    /// Mapping from FlowSpec NodeId -> AQ TaskId for each step.
    pub node_task_map: HashMap<NodeId, TaskId>,
    /// The FlowRunId assigned to this compilation.
    pub flow_run_id: FlowRunId,
}

/// Compile a FlowSpec into an ActionQueue task hierarchy.
///
/// Uses `CompilerConfig::default()` and generates a fresh `FlowRunId`.
pub fn compile(spec: &FlowSpec) -> Result<CompilationResult, CompilationError> {
    compile_with_config(spec, &CompilerConfig::default(), FlowRunId::new())
}

/// Compile a FlowSpec with explicit configuration and FlowRunId.
///
/// The explicit `flow_run_id` parameter enables deterministic compilation
/// for testing and crash-resume scenarios.
pub fn compile_with_config(
    spec: &FlowSpec,
    config: &CompilerConfig,
    flow_run_id: FlowRunId,
) -> Result<CompilationResult, CompilationError> {
    // 1. Validate
    spec.validate()?;

    if spec.nodes.is_empty() {
        return Err(CompilationError::EmptyFlowSpec);
    }

    // 2. Compute topological order
    let topo_order = topo::topological_sort(spec).expect(
        "topological_sort failed after successful validation (cycle should have been caught by \
         validate)",
    );

    // 3. Build NodeId -> TaskId mapping (deterministic)
    let node_task_map: HashMap<NodeId, TaskId> =
        topo_order.iter().map(|&node_id| (node_id, derive_task_id(flow_run_id, node_id))).collect();

    // 4. Build dependency map from edges
    let mut dependencies: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
    for edge in &spec.edges {
        if let (Some(&to_task), Some(&from_task)) =
            (node_task_map.get(&edge.to), node_task_map.get(&edge.from))
        {
            dependencies.entry(to_task).or_default().push(from_task);
        }
    }

    // 5. Build node index for quick lookup
    let node_index: HashMap<NodeId, &worldinterface_core::flowspec::Node> =
        spec.nodes.iter().map(|n| (n.id, n)).collect();

    // 6. Generate Coordinator TaskSpec
    let coordinator_task_id = derive_coordinator_task_id(flow_run_id);

    let coordinator_payload = CoordinatorPayload {
        task_type: TaskType::Coordinator,
        flow_spec: spec.clone(),
        flow_run_id,
        node_task_map: node_task_map.clone(),
        dependencies: dependencies.clone(),
    };
    let coordinator_payload_bytes = serde_json::to_vec(&coordinator_payload)?;

    let mut coordinator_constraints = TaskConstraints::default();
    coordinator_constraints.set_safety_level(SafetyLevel::Pure);
    if let Some(timeout) = config.coordinator_timeout_secs {
        coordinator_constraints.set_timeout_secs(Some(timeout)).map_err(|e| {
            CompilationError::TaskSpecFailed {
                node_id: None,
                source: actionqueue_core::task::task_spec::TaskSpecError::InvalidConstraints(e),
            }
        })?;
    }

    let coordinator_metadata = TaskMetadata::new(
        vec!["wi".into(), "coordinator".into()],
        0,
        spec.name.as_ref().map(|n| format!("Coordinator for flow: {n}")),
    );

    let coordinator = TaskSpec::new(
        coordinator_task_id,
        TaskPayload::with_content_type(coordinator_payload_bytes, PAYLOAD_CONTENT_TYPE),
        RunPolicy::Once,
        coordinator_constraints,
        coordinator_metadata,
    )
    .map_err(|e| CompilationError::TaskSpecFailed { node_id: None, source: e })?;

    // 7. Generate Step TaskSpecs in topological order
    let mut steps = Vec::with_capacity(topo_order.len());

    for &node_id in &topo_order {
        let node = node_index[&node_id];
        let task_id = node_task_map[&node_id];

        let step_payload = StepPayload {
            task_type: TaskType::Step,
            flow_run_id,
            node_id,
            node_type: node.node_type.clone(),
            flow_params: spec.params.clone(),
        };
        let step_payload_bytes = serde_json::to_vec(&step_payload)?;

        let safety_level = match &node.node_type {
            NodeType::Connector(_) => SafetyLevel::Idempotent,
            NodeType::Transform(_) => SafetyLevel::Pure,
            NodeType::Branch(_) => SafetyLevel::Pure,
        };

        let mut step_constraints = TaskConstraints::new(
            config.default_step_max_attempts,
            config.default_step_timeout_secs,
            None,
        )
        .map_err(|e| CompilationError::TaskSpecFailed {
            node_id: Some(node_id),
            source: actionqueue_core::task::task_spec::TaskSpecError::InvalidConstraints(e),
        })?;
        step_constraints.set_safety_level(safety_level);

        let step_metadata =
            TaskMetadata::new(vec!["wi".into(), "step".into()], 0, node.label.clone());

        let step = TaskSpec::new(
            task_id,
            TaskPayload::with_content_type(step_payload_bytes, PAYLOAD_CONTENT_TYPE),
            RunPolicy::Once,
            step_constraints,
            step_metadata,
        )
        .map_err(|e| CompilationError::TaskSpecFailed { node_id: Some(node_id), source: e })?
        .with_parent(coordinator_task_id);

        steps.push(step);
    }

    Ok(CompilationResult { coordinator, steps, dependencies, node_task_map, flow_run_id })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;
    use worldinterface_core::flowspec::*;

    use super::*;

    fn connector_node(id: NodeId, name: &str) -> Node {
        Node {
            id,
            label: Some(name.into()),
            node_type: NodeType::Connector(ConnectorNode {
                connector: name.into(),
                params: json!({}),
                idempotency_config: None,
            }),
        }
    }

    fn transform_node(id: NodeId) -> Node {
        Node {
            id,
            label: None,
            node_type: NodeType::Transform(TransformNode {
                transform: TransformType::Identity,
                input: json!({}),
            }),
        }
    }

    fn branch_node_helper(id: NodeId, then_edge: NodeId, else_edge: Option<NodeId>) -> Node {
        Node {
            id,
            label: None,
            node_type: NodeType::Branch(BranchNode {
                condition: BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                then_edge,
                else_edge,
            }),
        }
    }

    fn edge(from: NodeId, to: NodeId) -> Edge {
        Edge { from, to, condition: None }
    }

    fn branch_edge(from: NodeId, to: NodeId, condition: EdgeCondition) -> Edge {
        Edge { from, to, condition: Some(condition) }
    }

    fn make_ids(n: usize) -> Vec<NodeId> {
        (0..n).map(|_| NodeId::new()).collect()
    }

    fn make_spec(nodes: Vec<Node>, edges: Vec<Edge>) -> FlowSpec {
        FlowSpec { id: None, name: None, nodes, edges, params: None }
    }

    fn make_spec_with_params(nodes: Vec<Node>, edges: Vec<Edge>) -> FlowSpec {
        FlowSpec {
            id: None,
            name: Some("test-flow".into()),
            nodes,
            edges,
            params: Some(json!({"timeout": 30})),
        }
    }

    // --- T-3: Compiler - Linear Flow ---

    #[test]
    fn compile_single_node() {
        let id = NodeId::new();
        let spec = make_spec(vec![connector_node(id, "delay")], vec![]);
        let result = compile(&spec).unwrap();
        assert_eq!(result.steps.len(), 1);
        assert!(result.dependencies.is_empty());
    }

    #[test]
    fn compile_linear_three_nodes() {
        let ids = make_ids(3);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let result = compile(&spec).unwrap();
        assert_eq!(result.steps.len(), 3);

        let task_a = result.node_task_map[&ids[0]];
        let task_b = result.node_task_map[&ids[1]];
        let task_c = result.node_task_map[&ids[2]];

        // B depends on A
        assert!(result.dependencies[&task_b].contains(&task_a));
        // C depends on B
        assert!(result.dependencies[&task_c].contains(&task_b));
        // A has no dependencies
        assert!(!result.dependencies.contains_key(&task_a));
    }

    #[test]
    fn compile_linear_five_nodes() {
        let ids = make_ids(5);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
                connector_node(ids[3], "d"),
                connector_node(ids[4], "e"),
            ],
            vec![
                edge(ids[0], ids[1]),
                edge(ids[1], ids[2]),
                edge(ids[2], ids[3]),
                edge(ids[3], ids[4]),
            ],
        );
        let result = compile(&spec).unwrap();
        assert_eq!(result.steps.len(), 5);

        for i in 1..5 {
            let task = result.node_task_map[&ids[i]];
            let prev = result.node_task_map[&ids[i - 1]];
            assert!(result.dependencies[&task].contains(&prev));
        }
    }

    #[test]
    fn compile_step_has_parent() {
        let ids = make_ids(2);
        let spec = make_spec(
            vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            vec![edge(ids[0], ids[1])],
        );
        let result = compile(&spec).unwrap();
        let coord_id = result.coordinator.id();
        for step in &result.steps {
            assert_eq!(step.parent_task_id(), Some(coord_id));
        }
    }

    #[test]
    fn compile_step_task_ids_unique() {
        let ids = make_ids(3);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let result = compile(&spec).unwrap();
        let mut task_ids: Vec<TaskId> = result.steps.iter().map(|s| s.id()).collect();
        task_ids.push(result.coordinator.id());
        task_ids.sort_by_key(|id| *id.as_uuid());
        task_ids.dedup_by_key(|id| *id.as_uuid());
        assert_eq!(task_ids.len(), 4); // 3 steps + 1 coordinator
    }

    #[test]
    fn compile_step_order_is_topological() {
        let ids = make_ids(3);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let result = compile(&spec).unwrap();
        let step_task_ids: Vec<TaskId> = result.steps.iter().map(|s| s.id()).collect();
        // Step 0 should be task for ids[0], etc.
        assert_eq!(step_task_ids[0], result.node_task_map[&ids[0]]);
        assert_eq!(step_task_ids[1], result.node_task_map[&ids[1]]);
        assert_eq!(step_task_ids[2], result.node_task_map[&ids[2]]);
    }

    // --- T-4: Compiler - Branch Flow ---

    #[test]
    fn compile_branch_simple() {
        let ids = make_ids(4);
        // A -> Branch -> B | C
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                branch_node_helper(ids[1], ids[2], Some(ids[3])),
                connector_node(ids[2], "b"),
                connector_node(ids[3], "c"),
            ],
            vec![
                edge(ids[0], ids[1]),
                branch_edge(ids[1], ids[2], EdgeCondition::BranchTrue),
                branch_edge(ids[1], ids[3], EdgeCondition::BranchFalse),
            ],
        );
        let result = compile(&spec).unwrap();
        assert_eq!(result.steps.len(), 4);

        let task_branch = result.node_task_map[&ids[1]];
        let task_b = result.node_task_map[&ids[2]];
        let task_c = result.node_task_map[&ids[3]];

        // Both B and C depend on Branch
        assert!(result.dependencies[&task_b].contains(&task_branch));
        assert!(result.dependencies[&task_c].contains(&task_branch));
    }

    #[test]
    fn compile_branch_then_only() {
        let ids = make_ids(3);
        // A -> Branch -> B (no else)
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                branch_node_helper(ids[1], ids[2], None),
                connector_node(ids[2], "b"),
            ],
            vec![edge(ids[0], ids[1]), branch_edge(ids[1], ids[2], EdgeCondition::BranchTrue)],
        );
        let result = compile(&spec).unwrap();
        assert_eq!(result.steps.len(), 3);

        let task_branch = result.node_task_map[&ids[1]];
        let task_b = result.node_task_map[&ids[2]];
        assert!(result.dependencies[&task_b].contains(&task_branch));
    }

    #[test]
    fn compile_branch_with_downstream() {
        let ids = make_ids(5);
        // A -> Branch -> B | C, B -> D, C -> D
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                branch_node_helper(ids[1], ids[2], Some(ids[3])),
                connector_node(ids[2], "b"),
                connector_node(ids[3], "c"),
                connector_node(ids[4], "d"),
            ],
            vec![
                edge(ids[0], ids[1]),
                branch_edge(ids[1], ids[2], EdgeCondition::BranchTrue),
                branch_edge(ids[1], ids[3], EdgeCondition::BranchFalse),
                edge(ids[2], ids[4]),
                edge(ids[3], ids[4]),
            ],
        );
        let result = compile(&spec).unwrap();
        assert_eq!(result.steps.len(), 5);

        let task_b = result.node_task_map[&ids[2]];
        let task_c = result.node_task_map[&ids[3]];
        let task_d = result.node_task_map[&ids[4]];

        // D depends on both B and C
        let d_deps = &result.dependencies[&task_d];
        assert!(d_deps.contains(&task_b));
        assert!(d_deps.contains(&task_c));
    }

    // --- T-6: Payload Inspection ---

    #[test]
    fn compiled_coordinator_payload_deserializes() {
        let ids = make_ids(2);
        let spec = make_spec_with_params(
            vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            vec![edge(ids[0], ids[1])],
        );
        let result = compile(&spec).unwrap();
        let payload_bytes = result.coordinator.payload();
        let payload: CoordinatorPayload = serde_json::from_slice(payload_bytes).unwrap();
        assert_eq!(payload.task_type, TaskType::Coordinator);
        assert_eq!(payload.flow_spec, spec);
        assert_eq!(payload.flow_run_id, result.flow_run_id);
    }

    #[test]
    fn compiled_step_payloads_deserialize() {
        let ids = make_ids(2);
        let spec = make_spec(
            vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            vec![edge(ids[0], ids[1])],
        );
        let result = compile(&spec).unwrap();
        for step in &result.steps {
            let payload: StepPayload = serde_json::from_slice(step.payload()).unwrap();
            assert_eq!(payload.task_type, TaskType::Step);
            assert_eq!(payload.flow_run_id, result.flow_run_id);
            // node_id should be in the node_task_map
            assert!(result.node_task_map.contains_key(&payload.node_id));
        }
    }

    #[test]
    fn compiled_step_flow_params_present() {
        let id = NodeId::new();
        let spec = make_spec_with_params(vec![connector_node(id, "a")], vec![]);
        let result = compile(&spec).unwrap();
        let payload: StepPayload = serde_json::from_slice(result.steps[0].payload()).unwrap();
        assert_eq!(payload.flow_params, Some(json!({"timeout": 30})));
    }

    // --- T-7: TaskSpec Validity ---

    #[test]
    fn compiled_task_specs_have_valid_ids() {
        let ids = make_ids(2);
        let spec = make_spec(
            vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            vec![edge(ids[0], ids[1])],
        );
        let result = compile(&spec).unwrap();
        assert!(!result.coordinator.id().is_nil());
        for step in &result.steps {
            assert!(!step.id().is_nil());
        }
    }

    #[test]
    fn compiled_task_specs_have_correct_run_policy() {
        let id = NodeId::new();
        let spec = make_spec(vec![connector_node(id, "a")], vec![]);
        let result = compile(&spec).unwrap();
        assert_eq!(*result.coordinator.run_policy(), RunPolicy::Once);
        for step in &result.steps {
            assert_eq!(*step.run_policy(), RunPolicy::Once);
        }
    }

    #[test]
    fn compiled_coordinator_has_no_parent() {
        let id = NodeId::new();
        let spec = make_spec(vec![connector_node(id, "a")], vec![]);
        let result = compile(&spec).unwrap();
        assert_eq!(result.coordinator.parent_task_id(), None);
    }

    #[test]
    fn compiled_steps_have_parent() {
        let ids = make_ids(3);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let result = compile(&spec).unwrap();
        let coord_id = result.coordinator.id();
        for step in &result.steps {
            assert_eq!(step.parent_task_id(), Some(coord_id));
        }
    }

    #[test]
    fn compiled_step_safety_levels() {
        let ids = make_ids(4);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "http.request"),
                transform_node(ids[1]),
                branch_node_helper(ids[2], ids[3], None),
                connector_node(ids[3], "fs.write"),
            ],
            vec![
                edge(ids[0], ids[1]),
                edge(ids[1], ids[2]),
                branch_edge(ids[2], ids[3], EdgeCondition::BranchTrue),
            ],
        );
        let result = compile(&spec).unwrap();

        // Build a map of node_id -> step for easy lookup
        let step_map: HashMap<NodeId, &TaskSpec> = result
            .steps
            .iter()
            .map(|s| {
                let payload: StepPayload = serde_json::from_slice(s.payload()).unwrap();
                (payload.node_id, s)
            })
            .collect();

        // Connector -> Idempotent
        assert_eq!(step_map[&ids[0]].constraints().safety_level(), SafetyLevel::Idempotent);
        // Transform -> Pure
        assert_eq!(step_map[&ids[1]].constraints().safety_level(), SafetyLevel::Pure);
        // Branch -> Pure
        assert_eq!(step_map[&ids[2]].constraints().safety_level(), SafetyLevel::Pure);
        // Connector -> Idempotent
        assert_eq!(step_map[&ids[3]].constraints().safety_level(), SafetyLevel::Idempotent);
    }

    #[test]
    fn compiled_step_constraints_match_config() {
        let id = NodeId::new();
        let spec = make_spec(vec![connector_node(id, "a")], vec![]);
        let config = CompilerConfig {
            default_step_timeout_secs: Some(60),
            default_step_max_attempts: 5,
            coordinator_timeout_secs: Some(3600),
        };
        let result = compile_with_config(&spec, &config, FlowRunId::new()).unwrap();

        let step = &result.steps[0];
        assert_eq!(step.constraints().timeout_secs(), Some(60));
        assert_eq!(step.constraints().max_attempts(), 5);

        assert_eq!(result.coordinator.constraints().timeout_secs(), Some(3600));
    }

    // --- T-8: Error Cases ---

    #[test]
    fn compile_rejects_invalid_spec() {
        let ids = make_ids(2);
        let spec = make_spec(
            vec![connector_node(ids[0], "a"), connector_node(ids[1], "b")],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[0])], // cycle
        );
        let err = compile(&spec).unwrap_err();
        assert!(matches!(err, CompilationError::ValidationFailed(_)));
    }

    #[test]
    fn compile_rejects_empty_spec() {
        let spec = make_spec(vec![], vec![]);
        let err = compile(&spec).unwrap_err();
        // Empty spec is caught by validation (NoNodes rule)
        assert!(matches!(err, CompilationError::ValidationFailed(_)));
    }

    #[test]
    fn compile_rejects_dangling_edge() {
        let id = NodeId::new();
        let phantom = NodeId::new();
        let spec = make_spec(vec![connector_node(id, "a")], vec![edge(id, phantom)]);
        let err = compile(&spec).unwrap_err();
        assert!(matches!(err, CompilationError::ValidationFailed(_)));
    }

    // --- T-9: Node-Task Mapping ---

    #[test]
    fn node_task_map_has_all_nodes() {
        let ids = make_ids(3);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let result = compile(&spec).unwrap();
        for &id in &ids {
            assert!(result.node_task_map.contains_key(&id));
        }
    }

    #[test]
    fn node_task_map_values_match_step_ids() {
        let ids = make_ids(3);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let result = compile(&spec).unwrap();
        let step_ids: std::collections::HashSet<TaskId> =
            result.steps.iter().map(|s| s.id()).collect();
        for task_id in result.node_task_map.values() {
            assert!(step_ids.contains(task_id));
        }
    }

    #[test]
    fn dependency_map_uses_task_ids_from_node_task_map() {
        let ids = make_ids(3);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let result = compile(&spec).unwrap();
        let valid_ids: std::collections::HashSet<TaskId> =
            result.node_task_map.values().copied().collect();
        for (task_id, deps) in &result.dependencies {
            assert!(valid_ids.contains(task_id));
            for dep in deps {
                assert!(valid_ids.contains(dep));
            }
        }
    }

    // --- T-10: Configuration ---

    #[test]
    fn custom_config_applied() {
        let id = NodeId::new();
        let spec = make_spec(vec![connector_node(id, "a")], vec![]);
        let config = CompilerConfig {
            default_step_timeout_secs: Some(120),
            default_step_max_attempts: 7,
            coordinator_timeout_secs: Some(7200),
        };
        let result = compile_with_config(&spec, &config, FlowRunId::new()).unwrap();
        assert_eq!(result.steps[0].constraints().timeout_secs(), Some(120));
        assert_eq!(result.steps[0].constraints().max_attempts(), 7);
        assert_eq!(result.coordinator.constraints().timeout_secs(), Some(7200));
    }

    #[test]
    fn default_config_produces_valid_specs() {
        let ids = make_ids(3);
        let spec = make_spec(
            vec![
                connector_node(ids[0], "a"),
                connector_node(ids[1], "b"),
                connector_node(ids[2], "c"),
            ],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let result = compile(&spec).unwrap();
        result.coordinator.validate().unwrap();
        for step in &result.steps {
            step.validate().unwrap();
        }
    }

    // --- T-11: Property-Based Tests ---

    fn linear_flow(n: usize) -> FlowSpec {
        let ids: Vec<NodeId> = (0..n).map(|_| NodeId::new()).collect();
        let nodes = ids.iter().map(|&id| connector_node(id, "test")).collect();
        let edges = ids.windows(2).map(|w| edge(w[0], w[1])).collect();
        FlowSpec { id: None, name: None, nodes, edges, params: None }
    }

    proptest! {
        #[test]
        fn compilation_is_deterministic(n in 1usize..=20) {
            let spec = linear_flow(n);
            let flow_run_id = FlowRunId::new();
            let config = CompilerConfig::default();
            let r1 = compile_with_config(&spec, &config, flow_run_id).unwrap();
            let r2 = compile_with_config(&spec, &config, flow_run_id).unwrap();
            // Same TaskIds
            prop_assert_eq!(r1.coordinator.id(), r2.coordinator.id());
            prop_assert_eq!(r1.flow_run_id, r2.flow_run_id);
            for (s1, s2) in r1.steps.iter().zip(r2.steps.iter()) {
                prop_assert_eq!(s1.id(), s2.id());
            }
        }

        #[test]
        fn compiled_dependencies_are_acyclic(n in 1usize..=20) {
            let spec = linear_flow(n);
            let result = compile(&spec).unwrap();
            // Verify no self-dependencies
            for (task_id, deps) in &result.dependencies {
                prop_assert!(!deps.contains(task_id), "task depends on itself");
            }
        }

        #[test]
        fn step_count_equals_node_count(n in 1usize..=20) {
            let spec = linear_flow(n);
            let result = compile(&spec).unwrap();
            prop_assert_eq!(result.steps.len(), spec.nodes.len());
        }
    }
}
