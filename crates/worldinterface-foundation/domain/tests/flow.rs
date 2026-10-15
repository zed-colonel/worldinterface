use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use worldinterface_domain::flow::*;
use worldinterface_protocol::*;

fn o(s: &str) -> OpaqueRef {
    s.to_string().try_into().unwrap()
}
fn resource(s: &str) -> ResourceSelector {
    ResourceSelector { namespace: o("fixture"), selector: o(s) }
}
#[derive(Deserialize)]
struct Snapshot {
    descriptors: Vec<ConnectorDescriptor>,
    schemas: BTreeMap<OpaqueRef, Schema>,
    submitting_authority: AuthorityUpperBound,
    run_authority: AuthorityUpperBound,
}
fn snapshot() -> Snapshot {
    serde_json::from_str(include_str!("../../../../conformance/wi-fabric-2/flow/context.json"))
        .unwrap()
}
fn context() -> CompilationContext {
    let s = snapshot();
    CompilationContext::new(s.descriptors, s.schemas, s.submitting_authority, s.run_authority)
        .unwrap()
}
fn bound() -> AuthorityUpperBound {
    snapshot().run_authority
}
fn policy() -> BoundaryPolicy {
    BoundaryPolicy {
        budget: EffectBudget { max_attempts: 3, timeout_millis: 100 },
        deadline: None,
        receipt_profile: o("receipt-1"),
        expected_outcome: None,
    }
}
fn literal(schema: &str, value: Value) -> PlanInput {
    PlanInput { schema: o(schema), source: InputSource::Literal { value } }
}
fn output_ref(node: &str, port: &str) -> OutputRef {
    OutputRef { node: o(node), port: o(port) }
}
fn transform(id: &str) -> PlanNode {
    PlanNode {
        id: o(id),
        dependencies: vec![],
        guards: BTreeMap::new(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::from([(
            o("value"),
            PlanOutput { schema: o("bool"), resources: vec![] },
        )]),
        authority: AuthorityUpperBound::default(),
        fan_out: FanOut::Expanded {},
        operation: PlanOperation::Transform {
            program: Program::closed(Expression::Literal { value: json!(true) }).unwrap(),
        },
    }
}
fn boundary(id: &str) -> PlanNode {
    PlanNode {
        id: o(id),
        dependencies: vec![],
        guards: BTreeMap::new(),
        inputs: BTreeMap::from([(o("input"), literal("bool", json!(true)))]),
        outputs: BTreeMap::from([(
            o("output"),
            PlanOutput { schema: o("bool"), resources: vec![] },
        )]),
        authority: bound(),
        fan_out: FanOut::Expanded {},
        operation: PlanOperation::Boundary {
            binding: Box::new(context().binding(&o("fixture"), &o("write")).unwrap()),
            resource: ResourceBinding::Fixed { resource: resource("a") },
            policy: policy(),
        },
    }
}
fn definition(nodes: Vec<PlanNode>) -> FlowDefinition {
    FlowDefinition {
        id: o("test"),
        version: 1,
        predecessor: None,
        body: FlowBody {
            entries: nodes
                .iter()
                .filter(|n| n.dependencies.is_empty())
                .map(|n| n.id.clone())
                .collect(),
            nodes,
            outputs: BTreeMap::new(),
            authority: bound(),
            causal_metadata: BTreeMap::new(),
        },
    }
}
fn rejects(d: &FlowDefinition, kind: ErrorKind) {
    let err = compile_flow(d, &context()).unwrap_err();
    assert_eq!(err.kind, kind, "{err}");
}
fn branch(id: &str) -> PlanNode {
    PlanNode {
        outputs: BTreeMap::new(),
        operation: PlanOperation::Branch {
            program: Program::closed(Expression::Literal { value: json!(true) }).unwrap(),
            when_true: o("yes"),
            when_false: o("no"),
        },
        ..transform(id)
    }
}
fn timer(id: &str) -> PlanNode {
    PlanNode {
        outputs: BTreeMap::new(),
        operation: PlanOperation::Timer {
            deadline: Deadline {
                inclusive_at: "2030-01-01T00:00:00.000000000Z".to_string().try_into().unwrap(),
                action: TimeoutAction::Fail,
            },
        },
        ..transform(id)
    }
}
fn signal(id: &str) -> PlanNode {
    PlanNode {
        operation: PlanOperation::Wait {
            signal: SignalWait {
                tenant: o("tenant"),
                namespace: o("events"),
                signal_kind: o("completed"),
                correlation: o("request-1"),
                accept_early: true,
                progress: SignalProgress::Once {},
                deadline: None,
            },
        },
        ..timer(id)
    }
}
fn submission() -> EffectSubmission {
    EffectSubmission {
        client_request_id: "00000000-0000-4000-8000-000000000001".to_string().try_into().unwrap(),
        requested_accountable_principal: None,
        submitted_by: None,
        purpose: Some(o("purpose")),
        causal_refs: vec![o("cause")],
        acting_capacity: None,
        information_use: None,
        effect_class: EffectClass::Mutate,
        resource: resource("a"),
        input: EffectInputRef {
            artifact: ArtifactRef {
                scope: o("tenant"),
                content: ContentHash::of(b"true"),
                size: 4,
            },
        },
        requested_authority_refs: vec![o("requested")],
        approval_attestations: vec![o("approval")],
        deadline: None,
        requested_receipt_profile: o("receipt-1"),
        budget: policy().budget,
        expected_outcome: None,
        delegation_credential: None,
    }
}

#[test]
fn flow_rejects_cycles() {
    let mut a = transform("a");
    let mut b = transform("b");
    a.dependencies = vec![o("b")];
    b.dependencies = vec![o("a")];
    rejects(&definition(vec![transform("entry"), a, b]), ErrorKind::Graph);
}
#[test]
fn flow_rejects_unreachable_nodes() {
    let mut d = definition(vec![transform("a"), transform("b")]);
    d.body.entries = vec![o("a")];
    rejects(&d, ErrorKind::Unreachable);
}
#[test]
fn flow_rejects_duplicate_ids_and_missing_references() {
    rejects(&definition(vec![transform("a"), transform("a")]), ErrorKind::Graph);
    let mut d = definition(vec![transform("a")]);
    d.body.nodes[0].dependencies = vec![o("missing")];
    d.body.entries = vec![o("missing")];
    rejects(&d, ErrorKind::Reference);
}
#[test]
fn flow_rejects_schema_mismatch_and_unavailable_outputs() {
    let mut n = boundary("b");
    n.inputs.insert(o("input"), literal("bool", json!(1)));
    rejects(&definition(vec![n]), ErrorKind::Schema);
    let mut n = boundary("b");
    n.inputs.insert(
        o("input"),
        PlanInput {
            schema: o("int"),
            source: InputSource::Output { source: output_ref("a", "value") },
        },
    );
    n.dependencies = vec![o("a")];
    rejects(&definition(vec![transform("a"), n]), ErrorKind::Schema);
    let mut n = boundary("b");
    n.inputs.insert(
        o("input"),
        PlanInput {
            schema: o("bool"),
            source: InputSource::Output { source: output_ref("a", "value") },
        },
    );
    rejects(&definition(vec![transform("a"), n]), ErrorKind::Reference);
}
#[test]
fn flow_rejects_hidden_io_and_unpinned_programs() {
    for access in ["filesystem", "network", "clock", "randomness", "secrets", "process"] {
        let mut n = transform("t");
        if let PlanOperation::Transform { program } = &mut n.operation {
            program.requested_access = vec![o(access)];
        }
        rejects(&definition(vec![n]), ErrorKind::HiddenIo);
    }
    let mut n = transform("t");
    if let PlanOperation::Transform { program } = &mut n.operation {
        program.evaluator = o("native-callback");
    }
    rejects(&definition(vec![n]), ErrorKind::HiddenIo);
    let mut n = transform("t");
    if let PlanOperation::Transform { program } = &mut n.operation {
        program.implementation_hash = ContentHash::of(b"wrong");
    }
    rejects(&definition(vec![n]), ErrorKind::HiddenIo);
}
#[test]
fn flow_rejects_authority_enlargement() {
    let mut n = boundary("b");
    if let PlanOperation::Boundary { resource: r, .. } = &mut n.operation {
        *r = ResourceBinding::Fixed { resource: resource("forbidden") };
    }
    rejects(&definition(vec![n]), ErrorKind::Authority);
    let mut d = definition(vec![boundary("b")]);
    d.body.authority.alternatives.pop();
    rejects(&d, ErrorKind::Authority);
    let mut n = transform("t");
    n.authority = bound();
    rejects(&definition(vec![n]), ErrorKind::Authority);
    let s = snapshot();
    let mut run = s.run_authority.clone();
    run.alternatives[0].resource = resource("forbidden");
    assert_eq!(
        CompilationContext::new(s.descriptors, s.schemas, s.submitting_authority, run)
            .unwrap_err()
            .kind,
        ErrorKind::Authority
    );
}
fn resource_producer(id: &str, which: &str) -> PlanNode {
    let mut n = transform(id);
    n.outputs = BTreeMap::from([(
        o("value"),
        PlanOutput { schema: o("resource"), resources: vec![resource(which)] },
    )]);
    n.operation = PlanOperation::Transform {
        program: Program::closed(Expression::Literal {
            value: serde_json::to_value(resource(which)).unwrap(),
        })
        .unwrap(),
    };
    n
}
#[test]
fn flow_rejects_ambiguous_resource_production() {
    let mut b = boundary("b");
    b.dependencies = vec![o("a"), o("c")];
    if let PlanOperation::Boundary { resource: r, .. } = &mut b.operation {
        *r = ResourceBinding::Produced {
            producers: vec![output_ref("a", "value"), output_ref("c", "value")],
        };
    }
    rejects(
        &definition(vec![resource_producer("a", "a"), resource_producer("c", "b"), b]),
        ErrorKind::Resource,
    );
}
#[test]
fn flow_rejects_unbounded_fan_out() {
    let mut n = transform("a");
    n.fan_out = FanOut::Dynamic { input: o("items") };
    rejects(&definition(vec![n]), ErrorKind::Bound);
}
#[test]
fn flow_rejects_invalid_waits() {
    rejects(&definition(vec![signal("a"), timer("b")]), ErrorKind::Wait);
    let mut n = signal("a");
    if let PlanOperation::Wait { signal } = &mut n.operation {
        signal.progress = SignalProgress::AfterSequence { sequence: 0 };
    }
    rejects(&definition(vec![n]), ErrorKind::Wait);
    let mut n = timer("a");
    if let PlanOperation::Timer { deadline } = &mut n.operation {
        deadline.inclusive_at = "2030-01-01T00:00:00.000000001Z".to_string().try_into().unwrap();
    }
    rejects(&definition(vec![n]), ErrorKind::Wait);
    let mut n = signal("a");
    n.authority = bound();
    rejects(&definition(vec![n]), ErrorKind::Wait);
    let mut d = definition(vec![signal("a"), signal("b")]);
    d.body.entries = vec![o("a")];
    d.body.nodes[1].dependencies = vec![o("a")];
    if let PlanOperation::Wait { signal } = &mut d.body.nodes[0].operation {
        signal.deadline = Some(Deadline {
            inclusive_at: "2030-01-01T00:00:00.000000000Z".to_string().try_into().unwrap(),
            action: TimeoutAction::Continue,
        });
    }
    if let PlanOperation::Wait { signal } = &mut d.body.nodes[1].operation {
        signal.progress = SignalProgress::AfterHandled { wait: o("a") };
    }
    rejects(&d, ErrorKind::Wait);
}
#[test]
fn flow_sequential_and_exclusive_waits_pass_repeat_progress_is_required() {
    let a = signal("a");
    let mut b = signal("b");
    b.dependencies = vec![o("a")];
    let mut d = definition(vec![a, b]);
    rejects(&d, ErrorKind::Wait);
    if let PlanOperation::Wait { signal } = &mut d.body.nodes[1].operation {
        signal.progress = SignalProgress::AfterSequence { sequence: 100 };
    }
    rejects(&d, ErrorKind::Wait); // prior wait can handle a sequence greater than 100.
    if let PlanOperation::Wait { signal } = &mut d.body.nodes[1].operation {
        signal.progress = SignalProgress::AfterHandled { wait: o("a") };
    }
    compile_flow(&d, &context()).unwrap();
    let mut c = signal("c");
    c.dependencies = vec![o("b")];
    if let PlanOperation::Wait { signal } = &mut c.operation {
        signal.progress = SignalProgress::AfterHandled { wait: o("a") };
    }
    d.body.nodes.push(c);
    rejects(&d, ErrorKind::Wait); // must advance past b as well.
    if let PlanOperation::Wait { signal } = &mut d.body.nodes[2].operation {
        signal.progress = SignalProgress::AfterHandled { wait: o("b") };
    }
    compile_flow(&d, &context()).unwrap();
    let mut fresh = definition(vec![signal("a"), signal("b")]);
    fresh.body.entries = vec![o("a")];
    fresh.body.nodes[1].dependencies = vec![o("a")];
    if let PlanOperation::Wait { signal } = &mut fresh.body.nodes[1].operation {
        signal.progress = SignalProgress::FreshGeneration { generation: o("two") };
    }
    compile_flow(&fresh, &context()).unwrap();
    if let PlanOperation::Wait { signal } = &mut fresh.body.nodes[0].operation {
        signal.progress = SignalProgress::FreshGeneration { generation: o("two") };
    }
    rejects(&fresh, ErrorKind::Wait);
    let mut a = timer("a");
    a.dependencies = vec![o("branch")];
    a.guards.insert(o("branch"), o("yes"));
    let mut b = timer("b");
    b.dependencies = vec![o("branch")];
    b.guards.insert(o("branch"), o("no"));
    compile_flow(&definition(vec![branch("branch"), a, b]), &context()).unwrap();
    let mut nodes = (0..12).map(|i| timer(&format!("t{i}"))).collect::<Vec<_>>();
    for i in 1..nodes.len() {
        nodes[i].dependencies = vec![nodes[i - 1].id.clone()];
    }
    compile_flow(&definition(nodes), &context()).unwrap();
}
#[test]
fn flow_direct_is_one_operation_and_preserves_admission_separately() {
    let s = submission();
    let selection = DirectOperationSelection { connector: o("fixture"), operation: o("write") };
    let p = compile_direct(&s, &selection, &context()).unwrap();
    assert_eq!(p.document().body.nodes.len(), 1);
    assert_eq!(p.direct_submission(), Some(&s));
    let mut flow_node = boundary("direct");
    flow_node.inputs.insert(
        o("input"),
        PlanInput {
            schema: o("bool"),
            source: InputSource::Artifact { reference: s.input.artifact.clone() },
        },
    );
    let d = definition(vec![flow_node]);
    let f = compile_flow(&d, &context()).unwrap();
    assert_eq!(p.document().body.nodes, f.document().body.nodes);
    assert_ne!(p.hash(), f.hash());
    let mut changed = s.clone();
    changed.client_request_id =
        "00000000-0000-4000-8000-000000000002".to_string().try_into().unwrap();
    changed.purpose = Some(o("other"));
    let q = compile_direct(&changed, &selection, &context()).unwrap();
    assert_eq!(p.hash(), q.hash());
    assert_eq!(q.direct_submission(), Some(&changed));
    let restored = restore_plan(p.canonical_bytes(), p.hash(), &context()).unwrap();
    assert!(restored.direct_submission().is_none());
}
#[test]
fn flow_independent_canonical_vector_and_plan_digest() {
    let bytes = include_bytes!("../../../../conformance/wi-fabric-2/flow/plan.canonical.json");
    let hash: ContentHash = include_str!("../../../../conformance/wi-fabric-2/flow/plan.sha256")
        .trim()
        .to_string()
        .try_into()
        .unwrap();
    let p = restore_plan(bytes, &hash, &context()).unwrap();
    assert_eq!(p.canonical_bytes(), bytes);
    assert_ne!(ContentHash::of(bytes), hash);
    let mut d = definition(vec![timer("timer")]);
    d.id = o("golden");
    d.body.authority = AuthorityUpperBound::default();
    let compiled = compile_flow(&d, &context()).unwrap();
    assert_eq!(compiled.canonical_bytes(), bytes);
    assert_eq!(compiled.hash(), &hash);
}
#[test]
fn flow_hash_stable_under_set_order_and_changes_with_semantics() {
    let mut b = boundary("b");
    b.dependencies = vec![o("a"), o("c")];
    let d = definition(vec![transform("a"), transform("c"), b]);
    let p = compile_flow(&d, &context()).unwrap();
    let mut reordered = d.clone();
    reordered.body.nodes.reverse();
    reordered.body.entries.reverse();
    reordered.body.nodes[0].dependencies.reverse();
    reordered.body.authority.alternatives.reverse();
    let q = compile_flow(&reordered, &context()).unwrap();
    assert_eq!(p.hash(), q.hash());
    let mut changed = d.clone();
    changed.body.causal_metadata.insert(o("cause"), o("changed"));
    assert_ne!(p.hash(), compile_flow(&changed, &context()).unwrap().hash());
    let mut changed = d.clone();
    if let PlanOperation::Boundary { policy, .. } = &mut changed.body.nodes[2].operation {
        policy.budget.max_attempts = 2;
    }
    assert_ne!(p.hash(), compile_flow(&changed, &context()).unwrap().hash());
    let mut changed = d;
    if let PlanOperation::Boundary { resource: r, .. } = &mut changed.body.nodes[2].operation {
        *r = ResourceBinding::Fixed { resource: resource("b") };
    }
    assert_ne!(p.hash(), compile_flow(&changed, &context()).unwrap().hash());
}
#[test]
fn flow_recovery_rejects_versions_duplicate_unknown_noncanonical_and_oversize() {
    let p = compile_flow(&definition(vec![transform("a")]), &context()).unwrap();
    let v: Value = serde_json::from_slice(p.canonical_bytes()).unwrap();
    for (path, value) in [
        (vec!["schema"], json!(2)),
        (vec!["plan", "version"], json!(2)),
        (vec!["unknown"], json!(true)),
        (vec!["plan", "unknown"], json!(true)),
    ] {
        let mut v = v.clone();
        let mut cursor = &mut v;
        for key in &path[..path.len() - 1] {
            cursor = &mut cursor[*key];
        }
        cursor[path[path.len() - 1]] = value;
        assert!(restore_plan(&canonical_data(&v).unwrap(), p.hash(), &context()).is_err());
    }
    assert!(restore_plan(&serde_json::to_vec_pretty(&v).unwrap(), p.hash(), &context()).is_err());
    let duplicate =
        String::from_utf8(p.canonical_bytes().to_vec()).unwrap().replacen("{", "{\"schema\":1,", 1);
    assert!(restore_plan(duplicate.as_bytes(), p.hash(), &context()).is_err());
    assert!(restore_plan(&vec![b' '; 65537], p.hash(), &context()).is_err());
    let mut e = PlanEnvelope::parse(p.canonical_bytes()).unwrap();
    e.plan.body.nodes[0].dependencies = vec![o("a")];
    e.plan.body.entries = vec![o("missing")];
    if let PlanOrigin::Flow { definition_digest, .. } = &mut e.plan.origin {
        *definition_digest = FlowDefinition {
            id: o("test"),
            version: 1,
            predecessor: None,
            body: e.plan.body.clone(),
        }
        .digest()
        .unwrap();
    }
    assert!(restore_plan(&e.canonical_bytes().unwrap(), &e.digest().unwrap(), &context()).is_err());
}
#[test]
fn flow_registry_descriptor_and_schema_drift_reject() {
    let d = definition(vec![boundary("b")]);
    let p = compile_flow(&d, &context()).unwrap();
    let mut s = snapshot();
    s.descriptors[0].implementation = ContentHash::of(b"upgrade");
    let c =
        CompilationContext::new(s.descriptors, s.schemas, s.submitting_authority, s.run_authority)
            .unwrap();
    assert!(restore_plan(p.canonical_bytes(), p.hash(), &c).is_err());
    let mut s = snapshot();
    s.schemas.insert(o("bool"), Schema::Integer {});
    let c =
        CompilationContext::new(s.descriptors, s.schemas, s.submitting_authority, s.run_authority)
            .unwrap();
    assert!(restore_plan(p.canonical_bytes(), p.hash(), &c).is_err());
    let mut d = d;
    if let PlanOperation::Boundary { binding, .. } = &mut d.body.nodes[0].operation {
        binding.descriptor.max_evidence_bytes = 2;
        binding.descriptor_hash = binding.descriptor.digest().unwrap();
    }
    rejects(&d, ErrorKind::Registry);
    let mut s = snapshot();
    s.descriptors[0].version = 2;
    assert!(CompilationContext::new(
        s.descriptors,
        s.schemas,
        s.submitting_authority,
        s.run_authority
    )
    .is_err());
}
#[test]
fn flow_paired_authority_never_forms_a_cross_product() {
    let s = snapshot();
    let mut second = s.run_authority.alternatives[1].clone();
    second.operation = o("read");
    let ceiling =
        AuthorityUpperBound { alternatives: vec![s.run_authority.alternatives[0].clone(), second] };
    let c = CompilationContext::new(s.descriptors, s.schemas, ceiling.clone(), ceiling.clone())
        .unwrap();
    let mut n = boundary("b");
    n.authority = ceiling.clone();
    if let PlanOperation::Boundary { resource: r, .. } = &mut n.operation {
        *r = ResourceBinding::Fixed { resource: resource("b") };
    }
    let mut d = definition(vec![n]);
    d.body.authority = ceiling;
    assert_eq!(compile_flow(&d, &c).unwrap_err().kind, ErrorKind::Authority);
}
#[test]
fn flow_pure_transform_and_branch_replay_have_identical_hashes() {
    let mut n = transform("t");
    n.inputs.insert(o("x"), literal("bool", json!(true)));
    n.operation = PlanOperation::Transform {
        program: Program::closed(Expression::And {
            left: Box::new(Expression::Input { port: o("x") }),
            right: Box::new(Expression::Not {
                value: Box::new(Expression::Literal { value: json!(false) }),
            }),
        })
        .unwrap(),
    };
    let mut b = branch("b");
    b.dependencies = vec![o("t")];
    b.inputs.insert(
        o("x"),
        PlanInput {
            schema: o("bool"),
            source: InputSource::Output { source: output_ref("t", "value") },
        },
    );
    b.operation = PlanOperation::Branch {
        program: Program::closed(Expression::Input { port: o("x") }).unwrap(),
        when_true: o("yes"),
        when_false: o("no"),
    };
    let p = compile_flow(&definition(vec![n, b]), &context()).unwrap();
    let inputs = BTreeMap::from([(o("x"), json!(true))]);
    let a = p.evaluate(&o("t"), &inputs).unwrap();
    assert_eq!(a.value, json!(true));
    assert_eq!(a, p.evaluate(&o("t"), &inputs).unwrap());
    let b = p.evaluate(&o("b"), &inputs).unwrap();
    assert_eq!(b.branch_outcome, Some(o("yes")));
    assert_eq!(b, p.evaluate(&o("b"), &inputs).unwrap());
    assert!(p.evaluate(&o("t"), &BTreeMap::from([(o("x"), json!(1))])).is_err());
}
fn exclusive_resources() -> FlowDefinition {
    let mut a = resource_producer("a", "a");
    a.dependencies = vec![o("branch")];
    a.guards.insert(o("branch"), o("yes"));
    let mut b = resource_producer("b", "b");
    b.dependencies = vec![o("branch")];
    b.guards.insert(o("branch"), o("no"));
    let mut c = boundary("c");
    c.dependencies = vec![o("a"), o("b")];
    if let PlanOperation::Boundary { resource: r, .. } = &mut c.operation {
        *r = ResourceBinding::Exclusive {
            branch: o("branch"),
            when_true: output_ref("a", "value"),
            when_false: output_ref("b", "value"),
        };
    }
    definition(vec![branch("branch"), a, b, c])
}
#[test]
fn flow_branch_resources_are_explicit_exclusive_and_authority_bounded() {
    let d = exclusive_resources();
    compile_flow(&d, &context()).unwrap();
    let mut changed = d.clone();
    if let PlanOperation::Boundary { resource: r, .. } = &mut changed.body.nodes[3].operation {
        *r = ResourceBinding::Produced { producers: vec![output_ref("a", "value")] };
    }
    rejects(&changed, ErrorKind::Reference);
    let mut changed = d.clone();
    changed.body.nodes[2].guards.insert(o("branch"), o("yes"));
    rejects(&changed, ErrorKind::Resource);
    let mut changed = d;
    changed.body.nodes[2] = {
        let mut b = resource_producer("b", "forbidden");
        b.dependencies = vec![o("branch")];
        b.guards.insert(o("branch"), o("no"));
        b
    };
    rejects(&changed, ErrorKind::Authority);
    let mut s = snapshot();
    let mut publish = s.descriptors[0].clone();
    publish.operation = o("publish");
    publish.effect_class = EffectClass::Publish;
    s.descriptors.push(publish);
    let c =
        CompilationContext::new(s.descriptors, s.schemas, s.submitting_authority, s.run_authority)
            .unwrap();
    let mut d = exclusive_resources();
    if let PlanOperation::Boundary { binding, .. } = &mut d.body.nodes[3].operation {
        *binding = Box::new(c.binding(&o("fixture"), &o("publish")).unwrap());
    }
    assert_eq!(compile_flow(&d, &c).unwrap_err().kind, ErrorKind::Authority);
}
#[test]
fn flow_branch_value_join_requires_exhaustive_selection() {
    let mut d = exclusive_resources();
    d.body.outputs.insert(
        o("chosen"),
        PlanInput {
            schema: o("resource"),
            source: InputSource::Exclusive {
                branch: o("branch"),
                when_true: output_ref("a", "value"),
                when_false: output_ref("b", "value"),
            },
        },
    );
    compile_flow(&d, &context()).unwrap();
    d.body.outputs.get_mut(&o("chosen")).unwrap().source =
        InputSource::Output { source: output_ref("a", "value") };
    rejects(&d, ErrorKind::Reference);
}
#[test]
fn flow_bounds_allow_64_operations_and_reject_immediately_excessive_limits() {
    let nodes = (0..64).map(|i| transform(&format!("t{i:02}"))).collect();
    let mut d = definition(nodes);
    compile_flow(&d, &context()).unwrap();
    d.body.nodes.push(transform("t64"));
    rejects(&d, ErrorKind::Bound);
    let mut d = definition(vec![transform("t")]);
    for i in 0..33 {
        d.body.nodes[0].inputs.insert(o(&format!("i{i}")), literal("bool", json!(true)));
    }
    rejects(&d, ErrorKind::Bound);
    let mut n = transform("t");
    let mut expr = Expression::Literal { value: json!(true) };
    for _ in 0..17 {
        expr = Expression::Not { value: Box::new(expr) };
    }
    n.operation = PlanOperation::Transform { program: Program::closed(expr).unwrap() };
    rejects(&definition(vec![n]), ErrorKind::Bound);
    let mut d = definition((0..40).map(|i| transform(&format!("n{i}"))).collect());
    for node in &mut d.body.nodes {
        for i in 0..32 {
            node.inputs.insert(o(&format!("i{i}")), literal("bool", json!(true)));
        }
    }
    assert!(compile_flow(&d, &context()).is_err());
}
#[test]
fn flow_revision_preserves_original_and_changes_definition_and_plan_identity() {
    let d = definition(vec![transform("t")]);
    let old = d.clone();
    let mut body = d.body.clone();
    body.causal_metadata.insert(o("changed"), o("value"));
    let next = d.revise(body).unwrap();
    assert_eq!(d, old);
    assert_eq!(next.predecessor, Some(d.digest().unwrap()));
    assert_eq!(next.version, 2);
    assert_ne!(d.digest().unwrap(), next.digest().unwrap());
    assert_ne!(
        compile_flow(&d, &context()).unwrap().hash(),
        compile_flow(&next, &context()).unwrap().hash()
    );
    let mut bad = d;
    bad.version = 2;
    rejects(&bad, ErrorKind::Version);
}
#[test]
fn flow_artifact_references_are_not_materialized_or_coerced() {
    let p = compile_direct(
        &submission(),
        &DirectOperationSelection { connector: o("fixture"), operation: o("write") },
        &context(),
    )
    .unwrap();
    assert!(matches!(
        p.document().body.nodes[0].inputs[&o("input")].source,
        InputSource::Artifact { .. }
    ));
    let mut n = transform("t");
    n.inputs.insert(
        o("x"),
        PlanInput {
            schema: o("bool"),
            source: InputSource::Artifact { reference: submission().input.artifact },
        },
    );
    n.operation = PlanOperation::Transform {
        program: Program::closed(Expression::Input { port: o("x") }).unwrap(),
    };
    let p = compile_flow(&definition(vec![n]), &context()).unwrap();
    assert!(p.evaluate(&o("t"), &BTreeMap::from([(o("x"), json!("true"))])).is_err());
    for schema in [
        json!({"type":"string","pattern":".*"}),
        json!({"type":"any"}),
        json!({"type":"integer","coerce":true}),
    ] {
        assert!(serde_json::from_value::<Schema>(schema).is_err());
    }
}
#[test]
fn flow_every_execution_field_is_hashed_and_recovery_revalidates() {
    let p = compile_flow(&exclusive_resources(), &context()).unwrap();
    let original: Value = serde_json::from_slice(p.canonical_bytes()).unwrap();
    fn leaves(v: &Value, path: Vec<String>, out: &mut Vec<Vec<String>>) {
        match v {
            Value::Object(m) if m.is_empty() => out.push(path),
            Value::Array(a) if a.is_empty() => out.push(path),
            Value::Object(m) => {
                for (k, v) in m {
                    let mut p = path.clone();
                    p.push(k.clone());
                    leaves(v, p, out)
                }
            }
            Value::Array(a) => {
                for (i, v) in a.iter().enumerate() {
                    let mut p = path.clone();
                    p.push(i.to_string());
                    leaves(v, p, out)
                }
            }
            _ => out.push(path),
        }
    }
    let mut paths = vec![];
    leaves(&original, vec![], &mut paths);
    assert!(paths.len() > 100);
    for path in paths {
        let mut value = original.clone();
        let mut cursor = &mut value;
        for key in &path {
            cursor = if cursor.is_array() {
                &mut cursor[key.parse::<usize>().unwrap()]
            } else {
                &mut cursor[key]
            };
        }
        *cursor = match cursor {
            Value::Bool(v) => json!(!*v),
            Value::Number(v) => json!(v.as_u64().unwrap() + 1),
            Value::String(v) => json!(format!("{v}-changed")),
            Value::Object(_) | Value::Array(_) => json!([true]),
            _ => unreachable!(),
        };
        let bytes = canonical_data(&value).unwrap();
        assert_ne!(bytes, p.canonical_bytes(), "{path:?}");
        assert!(restore_plan(&bytes, p.hash(), &context()).is_err(), "{path:?}");
        if let Ok(e) = PlanEnvelope::parse(&bytes) {
            assert_ne!(&e.digest().unwrap(), p.hash(), "{path:?}");
        }
    }
}

#[test]
fn flow_materialized_contracts_preserve_literals_and_resource_possibilities() {
    let d = exclusive_resources();
    let p = compile_flow(&d, &context()).unwrap();
    assert!(p.validate_inputs(&o("c"), &BTreeMap::from([(o("input"), json!(false))])).is_err());
    assert!(p
        .validate_outputs(
            &o("a"),
            &BTreeMap::from([(o("value"), serde_json::to_value(resource("b")).unwrap())])
        )
        .is_err());
    assert!(p
        .validate_outputs(
            &o("a"),
            &BTreeMap::from([(o("value"), serde_json::to_value(resource("a")).unwrap())])
        )
        .is_ok());
    assert!(p.validate_outputs(&o("a"), &BTreeMap::new()).is_err());
}
#[test]
fn flow_expression_projection_equality_and_boolean_composition_are_closed() {
    let mut s = snapshot();
    s.schemas.insert(
        o("object"),
        Schema::Object { properties: BTreeMap::from([(o("flag"), Schema::Boolean {})]) },
    );
    let c =
        CompilationContext::new(s.descriptors, s.schemas, s.submitting_authority, s.run_authority)
            .unwrap();
    let mut n = transform("t");
    n.inputs.insert(o("object"), literal("object", json!({"flag":true})));
    n.operation = PlanOperation::Transform {
        program: Program::closed(Expression::Or {
            left: Box::new(Expression::Equal {
                left: Box::new(Expression::Project {
                    value: Box::new(Expression::Input { port: o("object") }),
                    field: o("flag"),
                }),
                right: Box::new(Expression::Literal { value: json!(true) }),
            }),
            right: Box::new(Expression::Literal { value: json!(false) }),
        })
        .unwrap(),
    };
    let p = compile_flow(&definition(vec![n.clone()]), &c).unwrap();
    assert_eq!(
        p.evaluate(&o("t"), &BTreeMap::from([(o("object"), json!({"flag":true}))])).unwrap().value,
        json!(true)
    );
    if let PlanOperation::Transform { program } = &mut n.operation {
        *program = Program::closed(Expression::Project {
            value: Box::new(Expression::Input { port: o("object") }),
            field: o("unknown"),
        })
        .unwrap();
    }
    assert_eq!(compile_flow(&definition(vec![n]), &c).unwrap_err().kind, ErrorKind::Schema);
}
#[test]
fn flow_expression_depth_steps_and_schema_limits_are_enforced() {
    let mut expression = Expression::Literal { value: json!(true) };
    for _ in 0..MAX_EXPRESSION_DEPTH {
        expression = Expression::Not { value: Box::new(expression) };
    }
    let mut node = transform("t");
    node.operation = PlanOperation::Transform { program: Program::closed(expression).unwrap() };
    let p = compile_flow(&definition(vec![node]), &context()).unwrap();
    p.evaluate(&o("t"), &BTreeMap::new()).unwrap();
    fn tree(depth: u32) -> Expression {
        if depth == 0 {
            Expression::Literal { value: json!(true) }
        } else {
            Expression::And { left: Box::new(tree(depth - 1)), right: Box::new(tree(depth - 1)) }
        }
    }
    let mut node = transform("t");
    node.operation = PlanOperation::Transform { program: Program::closed(tree(8)).unwrap() };
    rejects(&definition(vec![node]), ErrorKind::Bound);
    let mut s = snapshot();
    s.schemas.insert(o("large"), Schema::String { max_bytes: 4097 });
    assert!(CompilationContext::new(
        s.descriptors,
        s.schemas,
        s.submitting_authority,
        s.run_authority
    )
    .is_err());
}
#[test]
fn flow_validated_version_revision_keeps_the_activated_value_immutable() {
    let d = definition(vec![transform("t")]);
    let (version, plan) = compile_flow_version(&d, &context()).unwrap();
    let mut next_body = d.body;
    next_body.causal_metadata.insert(o("revision"), o("two"));
    let (next, next_plan) = version.revise(next_body, &context()).unwrap();
    assert_eq!(version.definition().version, 1);
    assert_eq!(next.definition().version, 2);
    assert_eq!(next.definition().predecessor.as_ref(), Some(version.digest()));
    assert_ne!(plan.hash(), next_plan.hash());
    assert_eq!(version.digest(), &version.definition().digest().unwrap());
}

#[test]
fn flow_exact_envelope_byte_bound_is_inclusive() {
    let mut s = snapshot();
    s.schemas.insert(o("text"), Schema::String { max_bytes: 4096 });
    let c =
        CompilationContext::new(s.descriptors, s.schemas, s.submitting_authority, s.run_authority)
            .unwrap();
    let mut d = definition(vec![transform("t")]);
    let p = compile_flow(&d, &c).unwrap();
    let mut e = PlanEnvelope::parse(p.canonical_bytes()).unwrap();
    let mut count = 0;
    while canonical_collection(&e).unwrap().len() <= 65536 - 4096 {
        e.plan.body.nodes[0]
            .inputs
            .insert(o(&format!("chunk{count}")), literal("text", json!("x".repeat(4096))));
        count += 1;
    }
    e.plan.body.nodes[0].inputs.insert(o("filler"), literal("text", json!("")));
    let padding = 65536 - canonical_collection(&e).unwrap().len();
    assert!(padding <= 4096);
    e.plan.body.nodes[0].inputs.get_mut(&o("filler")).unwrap().source =
        InputSource::Literal { value: json!("x".repeat(padding)) };
    d.body = e.plan.body;
    let p = compile_flow(&d, &c).unwrap();
    assert_eq!(p.canonical_bytes().len(), 65536);
    if let InputSource::Literal { value } =
        &mut d.body.nodes[0].inputs.get_mut(&o("filler")).unwrap().source
    {
        *value = json!("x".repeat(padding + 1));
    }
    assert!(compile_flow(&d, &c).is_err());
}

// F-001: connector references and closed JSON validation do not inherit evaluator limits.
#[test]
fn flow_large_boundary_artifacts_preserve_schema_integrity_and_authority_checks() {
    let mut s = snapshot();
    s.schemas.insert(
        o("payload"),
        Schema::Array { items: Box::new(Schema::String { max_bytes: 4096 }), max_items: 32 },
    );
    s.descriptors[0].input_schema = o("payload");
    let c =
        CompilationContext::new(s.descriptors, s.schemas, s.submitting_authority, s.run_authority)
            .unwrap();
    let value = json!(vec!["x".repeat(4096); 32]);
    let bytes = serde_json::to_vec(&value).unwrap();
    assert!(bytes.len() > 65536);
    let mut s = submission();
    s.input.artifact.size = bytes.len() as u64;
    s.input.artifact.content = ContentHash::of(&bytes);
    let selection = DirectOperationSelection { connector: o("fixture"), operation: o("write") };
    let p = compile_direct(&s, &selection, &c).unwrap();
    assert_eq!(p.document().body.nodes.len(), 1);
    assert!(p.canonical_bytes().len() < MAX_VALUE_BYTES);
    p.verify_input_artifact(&o("direct"), &o("input"), &bytes).unwrap();
    p.validate_inputs(&o("direct"), &BTreeMap::from([(o("input"), value.clone())])).unwrap();
    assert!(!validate_value(&p.document().schemas[&o("payload")].schema, &value));
    let mut pure = transform("pure");
    pure.inputs.insert(
        o("x"),
        PlanInput {
            schema: o("payload"),
            source: InputSource::Artifact { reference: s.input.artifact.clone() },
        },
    );
    assert_eq!(
        compile_flow(&definition(vec![pure.clone()]), &c).unwrap_err().kind,
        ErrorKind::Bound
    );
    if let InputSource::Artifact { reference } = &mut pure.inputs.get_mut(&o("x")).unwrap().source {
        reference.size = 4;
    }
    let pure = compile_flow(&definition(vec![pure]), &c).unwrap();
    assert!(pure.evaluate(&o("pure"), &BTreeMap::from([(o("x"), value)])).is_err());
    assert!(p.verify_input_artifact(&o("direct"), &o("input"), &bytes[..bytes.len() - 1]).is_err());
    let mut corrupt = bytes.clone();
    corrupt[2] = b'y';
    assert!(p.verify_input_artifact(&o("direct"), &o("input"), &corrupt).is_err());
    let wrong = b"true";
    s.input.artifact.size = wrong.len() as u64;
    s.input.artifact.content = ContentHash::of(wrong);
    let p = compile_direct(&s, &selection, &c).unwrap();
    assert!(p.verify_input_artifact(&o("direct"), &o("input"), wrong).is_err());
    s.resource = resource("forbidden");
    assert_eq!(compile_direct(&s, &selection, &c).unwrap_err().kind, ErrorKind::Authority);
}

// F-001: non-JSON connector contracts remain references and cannot enter expressions.
#[test]
fn flow_opaque_connector_artifacts_are_pinned_bounded_and_never_evaluated() {
    let mut s = snapshot();
    let contract = Schema::Artifact {
        schema_hash: ContentHash::of(b"binary-contract-1"),
        max_bytes: MAX_BOUNDARY_ARTIFACT_BYTES,
    };
    s.schemas.insert(o("binary"), contract.clone());
    s.descriptors[0].input_schema = o("binary");
    s.descriptors[0].output_schema = o("binary");
    let c = CompilationContext::new(
        s.descriptors.clone(),
        s.schemas.clone(),
        s.submitting_authority.clone(),
        s.run_authority.clone(),
    )
    .unwrap();
    let bytes = vec![0xff; 20_000];
    let mut submission = submission();
    submission.input.artifact.size = bytes.len() as u64;
    submission.input.artifact.content = ContentHash::of(&bytes);
    let selection = DirectOperationSelection { connector: o("fixture"), operation: o("write") };
    let p = compile_direct(&submission, &selection, &c).unwrap();
    // Returning the contract requires the caller to validate it at the connector.
    assert_eq!(p.verify_input_artifact(&o("direct"), &o("input"), &bytes).unwrap(), &contract);
    assert!(p
        .validate_inputs(&o("direct"), &BTreeMap::from([(o("input"), json!(bytes))]))
        .is_err());
    let mut n = transform("t");
    n.inputs.insert(
        o("x"),
        PlanInput {
            schema: o("binary"),
            source: InputSource::Artifact { reference: submission.input.artifact.clone() },
        },
    );
    assert_eq!(compile_flow(&definition(vec![n.clone()]), &c).unwrap_err().kind, ErrorKind::Bound);
    if let InputSource::Artifact { reference } = &mut n.inputs.get_mut(&o("x")).unwrap().source {
        reference.size = 4;
    }
    assert_eq!(compile_flow(&definition(vec![n]), &c).unwrap_err().kind, ErrorKind::Schema);
    submission.input.artifact.size = MAX_BOUNDARY_ARTIFACT_BYTES;
    compile_direct(&submission, &selection, &c).unwrap();
    submission.input.artifact.size += 1;
    assert_eq!(compile_direct(&submission, &selection, &c).unwrap_err().kind, ErrorKind::Bound);
    s.schemas.insert(
        o("binary"),
        Schema::Artifact { schema_hash: ContentHash::of(b"binary-contract-1"), max_bytes: 19_999 },
    );
    let smaller =
        CompilationContext::new(s.descriptors, s.schemas, s.submitting_authority, s.run_authority)
            .unwrap();
    submission.input.artifact.size = 20_000;
    assert_eq!(
        compile_direct(&submission, &selection, &smaller).unwrap_err().kind,
        ErrorKind::Bound
    );
    assert!(restore_plan(p.canonical_bytes(), p.hash(), &smaller).is_err());
    for invalid in [
        Schema::Artifact { schema_hash: ContentHash::of(b"contract"), max_bytes: 0 },
        Schema::Artifact {
            schema_hash: ContentHash::of(b"contract"),
            max_bytes: MAX_BOUNDARY_ARTIFACT_BYTES + 1,
        },
        Schema::Array { items: Box::new(contract), max_items: 1 },
    ] {
        let mut s = snapshot();
        s.schemas.insert(o("invalid"), invalid);
        assert!(CompilationContext::new(
            s.descriptors,
            s.schemas,
            s.submitting_authority,
            s.run_authority
        )
        .is_err());
    }
}

// F-002: contradictory and missing guards on plain dependencies must reject.
#[test]
fn flow_dependency_guards_reject_dead_nodes_and_unguarded_consumers() {
    let mut m = transform("m");
    m.dependencies = vec![o("br")];
    m.guards.insert(o("br"), o("no"));
    let mut n = transform("n");
    n.dependencies = vec![o("m")];
    n.guards.insert(o("br"), o("yes"));
    rejects(&definition(vec![branch("br"), m.clone(), n.clone()]), ErrorKind::Unreachable);
    n.guards.clear();
    let mut consumer = transform("consumer");
    consumer.dependencies = vec![o("n")];
    consumer.inputs.insert(
        o("x"),
        PlanInput {
            schema: o("bool"),
            source: InputSource::Output { source: output_ref("n", "value") },
        },
    );
    rejects(&definition(vec![branch("br"), m.clone(), n.clone(), consumer]), ErrorKind::Reference);
    n.guards = m.guards.clone();
    compile_flow(&definition(vec![branch("br"), m, n]), &context()).unwrap();
}

// F-002: ordering cannot be inferred through nodes skipped on another branch.
#[test]
fn flow_dependency_guards_preserve_wait_ordering_and_explicit_value_joins() {
    let mut wait = signal("wait");
    wait.dependencies = vec![o("br")];
    wait.guards.insert(o("br"), o("yes"));
    let mut middle = transform("middle");
    middle.dependencies = vec![o("wait")];
    middle.guards.insert(o("br"), o("no"));
    let mut after = timer("after");
    after.dependencies = vec![o("middle")];
    after.guards = middle.guards.clone();
    rejects(
        &definition(vec![branch("br"), wait.clone(), middle.clone(), after.clone()]),
        ErrorKind::Unreachable,
    );
    middle.guards = wait.guards.clone();
    after.guards.clear();
    rejects(
        &definition(vec![branch("br"), wait.clone(), middle.clone(), after.clone()]),
        ErrorKind::Reference,
    );
    after.guards = wait.guards.clone();
    compile_flow(&definition(vec![branch("br"), wait, middle, after]), &context()).unwrap();
    let mut a = transform("a");
    a.dependencies = vec![o("br")];
    a.guards.insert(o("br"), o("yes"));
    let mut b = transform("b");
    b.dependencies = vec![o("br")];
    b.guards.insert(o("br"), o("no"));
    let mut join = transform("join");
    join.dependencies = vec![o("a"), o("b")];
    join.inputs.insert(
        o("x"),
        PlanInput {
            schema: o("bool"),
            source: InputSource::Exclusive {
                branch: o("br"),
                when_true: output_ref("a", "value"),
                when_false: output_ref("b", "value"),
            },
        },
    );
    join.operation = PlanOperation::Transform {
        program: Program::closed(Expression::Input { port: o("x") }).unwrap(),
    };
    compile_flow(&definition(vec![branch("br"), a, b, join]), &context()).unwrap();
}

// F-004: trace hashes have distinct, exact wi-json-1 domains.
#[test]
fn flow_evaluation_hashes_use_distinct_input_and_output_domains() {
    let mut n = transform("t");
    n.inputs.insert(o("x"), literal("bool", json!(true)));
    let p = compile_flow(&definition(vec![n]), &context()).unwrap();
    let inputs = BTreeMap::from([(o("x"), json!(true))]);
    let evaluation = p.evaluate(&o("t"), &inputs).unwrap();
    assert_eq!(evaluation.input_hash, ContentHash::of(br#"["wi-eval-input-1",{"x":true}]"#));
    assert_eq!(evaluation.output_hash, ContentHash::of(br#"["wi-eval-output-1",true]"#));
    assert_ne!(evaluation.input_hash, ContentHash::of(&canonical_data(&inputs).unwrap()));
    assert_ne!(evaluation.output_hash, ContentHash::of(b"true"));
    assert_ne!(evaluation.output_hash, ContentHash::of(br#"["wi-eval-input-1",true]"#));
}

// F-003: these bytes and hashes are constructed by stdlib-only Python, not Rust.
#[test]
fn flow_independent_direct_and_branch_boundary_vectors() {
    fn assert_vector(p: &ExecutionPlan, bytes: &[u8], hash: &str) {
        let hash: ContentHash = hash.trim().to_string().try_into().unwrap();
        assert_eq!(p.canonical_bytes(), bytes);
        assert_eq!(p.hash(), &hash);
        let restored = restore_plan(bytes, &hash, &context()).unwrap();
        assert_eq!(restored.canonical_bytes(), bytes);
    }
    let mut submission = submission();
    submission.deadline = Some("2030-01-01T00:00:00.000000000Z".to_string().try_into().unwrap());
    submission.expected_outcome = Some(o("written"));
    let direct = compile_direct(
        &submission,
        &DirectOperationSelection { connector: o("fixture"), operation: o("write") },
        &context(),
    )
    .unwrap();
    assert_vector(
        &direct,
        include_bytes!("../../../../conformance/wi-fabric-2/flow/direct.canonical.json"),
        include_str!("../../../../conformance/wi-fabric-2/flow/direct.sha256"),
    );
    let mut d = exclusive_resources();
    d.id = o("golden-branch");
    d.body.causal_metadata.insert(o("cause"), o("golden-cause"));
    if let PlanOperation::Boundary { policy, .. } = &mut d.body.nodes[3].operation {
        policy.deadline = submission.deadline;
        policy.expected_outcome = submission.expected_outcome;
    }
    let flow = compile_flow(&d, &context()).unwrap();
    assert_vector(
        &flow,
        include_bytes!("../../../../conformance/wi-fabric-2/flow/branch.canonical.json"),
        include_str!("../../../../conformance/wi-fabric-2/flow/branch.sha256"),
    );
}
