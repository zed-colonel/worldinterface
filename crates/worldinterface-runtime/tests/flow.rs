#[path = "support/child.rs"]
mod child_guard;
mod support {
    pub mod connectors;
    pub mod spi_harness;
}
use actionqueue_core::ids::TaskId;
use std::{collections::BTreeMap, sync::Arc};
use support::{connectors::*, spi_harness::host};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    flow::*,
    resource::{ResolutionFact, ResolverSnapshot, TargetDocument},
};
use worldinterface_protocol::*;
use worldinterface_runtime::{
    aq_adapter::QueueEngine,
    authority::{Authentication, TrustedClock},
    completion::EffectAttemptHandler,
    flow::{PlanConfiguration, PlanService},
    flow_node::FlowAttemptHandler,
    resolution::ResourceObserver,
    storage::StoreBundle,
    Result,
};
use worldinterface_storage::{flow::*, StoreProfile};
#[derive(Clone)]
struct Auth(OpaqueRef, PrincipalId);
impl Authentication for Auth {
    type Session = ();
    fn authenticate(&self, _: &()) -> Result<(OpaqueRef, PrincipalId)> {
        Ok((self.0.clone(), self.1.clone()))
    }
}
#[derive(Clone)]
struct Config(FlowPins);
impl worldinterface_runtime::artifact::ArtifactAuthorization for Config {
    type AuthenticatedContext = AuthenticatedCaller;
    fn authorize_read(&self, caller: &AuthenticatedCaller, artifact: &ArtifactRef) -> bool {
        &artifact.scope == caller.tenant()
    }
}
impl PlanConfiguration for Config {
    fn pins(&self, _: &AuthenticatedCaller) -> Result<FlowPins> {
        Ok(self.0.clone())
    }
}
#[derive(Clone)]
struct Resolver(ResolutionFact);
impl ResourceObserver for Resolver {
    fn observe(
        &self,
        _: &OpaqueRef,
        s: &EffectSubmission,
        _: &ResolverSnapshot,
        _: &Timestamp,
    ) -> Result<ResolutionFact> {
        if s.resource != self.0.selector {
            return Err("wrong selector".into());
        }
        Ok(self.0.clone())
    }
    fn compare_before_use(&self, _: &ResolutionFact) -> Result<TargetDocument> {
        Ok(self.0.document.clone())
    }
}
struct Fixture {
    _temp: tempfile::TempDir,
    bundle: Arc<StoreBundle>,
    connector: DeterministicConnector,
    snapshot: worldinterface_domain::authorization::AuthorizationSnapshot,
    submission: EffectSubmission,
    clock: Clock,
    config: Config,
    auth: Auth,
}
impl Fixture {
    fn new() -> Self {
        let temp = if let Some(parent) = std::env::var_os("WI_FLOW_FIXTURE_PARENT") {
            tempfile::tempdir_in(parent).unwrap()
        } else {
            tempfile::tempdir().unwrap()
        };
        let connector = DeterministicConnector::new(
            temp.path().join("provider"),
            IdempotencyClass::ExternallyKeyed,
            Mode::Commit,
        );
        let (submission, snapshot) = fixture(&connector.descriptor);
        let clock = Clock::new();
        let bundle = Arc::new(
            StoreBundle::initialize(
                &temp.path().join("bundle"),
                StoreProfile {
                    instance: InstanceId::generate(),
                    created_at: clock.now().unwrap(),
                    platform: false,
                },
            )
            .unwrap(),
        );
        let auth =
            Auth(snapshot.bindings[0].tenant.clone(), snapshot.bindings[0].principal.clone());
        for b in [b"input".as_slice(), b"resolution", b"grant"] {
            bundle.wi().publish(auth.0.clone(), b).unwrap();
        }
        let authority = AuthorityUpperBound {
            alternatives: vec![AuthorityAlternative {
                connector: connector.descriptor.connector.clone(),
                operation: connector.descriptor.operation.clone(),
                effect: submission.effect_class.clone(),
                resource: submission.resource.clone(),
            }],
        };
        let config = Config(FlowPins {
            descriptors: vec![connector.descriptor.clone()],
            schemas: BTreeMap::from([
                (
                    "input:1".to_string().try_into().unwrap(),
                    Schema::Artifact { schema_hash: ContentHash::of(b"input:1"), max_bytes: 4096 },
                ),
                (
                    "output:1".to_string().try_into().unwrap(),
                    Schema::Artifact { schema_hash: ContentHash::of(b"output:1"), max_bytes: 4096 },
                ),
                ("bool".to_string().try_into().unwrap(), Schema::Boolean {}),
                ("int".to_string().try_into().unwrap(), Schema::Integer {}),
            ]),
            submitting_authority: authority.clone(),
            run_authority: authority,
        });
        let mut f =
            Self { _temp: temp, bundle, connector, snapshot, submission, clock, config, auth };
        f.submission.budget.timeout_millis = 60_000;
        let submission = f.submission.clone();
        allow_demands(&mut f, &[submission]);
        f
    }
    fn handler(&self) -> FlowAttemptHandler<DeterministicConnector, Evidence, Resolver, Clock> {
        FlowAttemptHandler {
            effect: EffectAttemptHandler {
                bundle: self.bundle.clone(),
                port: self.connector.clone(),
                evidence: Evidence(self.snapshot.clone()),
                clock: self.clock.clone(),
            },
            resolver: Resolver(self.snapshot.facts[0].clone()),
            artifacts: Arc::new(self.config.clone()),
        }
    }
    fn service<'a>(
        &'a self,
        evidence: &'a Evidence,
    ) -> PlanService<'a, Auth, Evidence, Clock, Config> {
        PlanService {
            root: self.bundle.wi(),
            authentication: &self.auth,
            evidence,
            clock: &self.clock,
            configuration: &self.config,
        }
    }
    fn direct(&self) -> ExecutionPlan {
        compile_direct(&self.submission, &self.selection(), &self.config.0.context().unwrap())
            .unwrap()
    }
    fn selection(&self) -> DirectOperationSelection {
        DirectOperationSelection {
            connector: self.connector.descriptor.connector.clone(),
            operation: self.connector.descriptor.operation.clone(),
        }
    }
    fn run(&self, id: &OpaqueRef) -> FlowRun {
        self.bundle.wi().flow_run_snapshot(&self.auth.0, id).unwrap().runs[id].clone()
    }
    async fn drive<
        V: worldinterface_runtime::authority::EvidenceAuthority + Send + Sync + 'static,
    >(
        &self,
        queue: &mut QueueEngine<'_, FlowAttemptHandler<DeterministicConnector, V, Resolver, Clock>>,
        id: &OpaqueRef,
    ) {
        for _ in 0..3000 {
            let _ = queue.tick().await.unwrap();
            let r = self.run(id);
            let task: TaskId = r.coordinator_task.as_str().parse().unwrap();
            if r.outcome.is_some()
                && queue.engine().projection().runs_for_task(task).all(|r| r.state().is_terminal())
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!(
            "Flow did not settle: {:?}; AQ {:?}",
            self.run(id),
            queue
                .engine()
                .projection()
                .run_instances()
                .map(|r| (r.task_id(), r.state()))
                .collect::<Vec<_>>()
        );
    }
}
#[tokio::test]
async fn direct_and_flow_share_transactional_executor_and_receipts() {
    for flow in [false, true] {
        let f = Fixture::new();
        let evidence = Evidence(f.snapshot.clone());
        let service = f.service(&evidence);
        let mut queue = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        let id = if flow {
            let d = FlowDefinition {
                id: "one".to_string().try_into().unwrap(),
                version: 1,
                predecessor: None,
                body: f.direct().document().body.clone(),
            };
            service.activate_flow_version(&(), d.clone(), None).unwrap();
            service.submit_flow(&(), &d.id, f.submission.clone(), &mut queue).unwrap()
        } else {
            service.submit_direct(&(), f.submission.clone(), f.selection(), &mut queue).unwrap()
        };
        f.drive(&mut queue, &id).await;
        let r = f.run(&id);
        assert_eq!(r.outcome, Some(FlowOutcome::Succeeded));
        assert_eq!(r.results.len(), 1);
        assert_eq!(f.connector.commits(), 1);
        let result = r.results.values().next().unwrap();
        let effect = result.effect.as_ref().unwrap();
        assert_eq!(f.bundle.wi().effect_receipts(&f.auth.0, effect).unwrap().len(), 1);
        assert!(f.bundle.wi().effect_snapshot(&f.auth.0).unwrap().effects[effect].dispatches[0]
            .admission
            .as_ref()
            .unwrap()
            .child
            .is_some());
        assert!(r.proposal.is_some());
        assert_eq!(r.bindings.len(), 1);
        queue.shutdown().unwrap();
    }
}
#[tokio::test]
async fn tampered_plan_hash_snapshots_and_attribution_prevent_execution() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap();
    let r = f.run(&id);
    // Populate and reuse the validation cache before every mutation below.
    assert_eq!(r.restore().unwrap().canonical_bytes(), r.plan);
    assert_eq!(r.restore().unwrap().hash(), &r.plan_hash);
    for kind in ["bytes", "hash", "snapshot", "schemas", "authority", "principal", "definition"] {
        let mut bad = r.clone();
        match kind {
            "bytes" => bad.plan[0] ^= 1,
            "hash" => bad.plan_hash = ContentHash::of(b"tamper"),
            "snapshot" => bad.pins.descriptors[0].implementation = ContentHash::of(b"replacement"),
            "schemas" => bad.pins.schemas.clear(),
            "authority" => bad.pins.run_authority.alternatives.clear(),
            "principal" => bad.principal = PrincipalId::generate(),
            _ => {
                bad.origin = PlanOrigin::Flow {
                    id: "other".to_string().try_into().unwrap(),
                    version: 8,
                    predecessor: None,
                    definition_digest: ContentHash::of(b"other"),
                }
            }
        }
        // Reach plan/pin/identity validation rather than the admission digest.
        bad.admission_hash = bad.admission_digest().unwrap();
        if kind == "hash" {
            let e = bad.restore().unwrap_err();
            assert!(e.to_string().contains("plan bytes or digest mismatch"), "{e}");
        } else {
            assert!(bad.restore().is_err(), "{kind}");
        }
        assert_eq!(r.restore().unwrap().hash(), &r.plan_hash);
    }
    let mut state = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap();
    let bad = state.runs.get_mut(&id).unwrap();
    bad.plan_hash = ContentHash::of(b"wrong persisted expected hash");
    bad.admission_hash = bad.admission_digest().unwrap();
    let connection =
        rusqlite::Connection::open(f._temp.path().join("bundle/wi/flow.sqlite")).unwrap();
    connection
        .execute(
            "UPDATE flow_runs SET state=? WHERE tenant=? AND id=?",
            rusqlite::params![
                canonical_collection(&state.runs[&id]).unwrap(),
                f.auth.0.as_str(),
                id.as_str()
            ],
        )
        .unwrap();
    drop(connection);
    let _ = q.tick().await.unwrap();
    assert!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().quarantined[&id]
        .contains("plan bytes or digest mismatch"));
    assert_eq!(f.connector.commits(), 0);
    q.shutdown().unwrap();
}

fn pure_node(
    id: &str,
    dependencies: Vec<OpaqueRef>,
    value: serde_json::Value,
    schema: &str,
) -> PlanNode {
    PlanNode {
        id: id.to_string().try_into().unwrap(),
        dependencies,
        guards: BTreeMap::new(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::from([(
            "value".to_string().try_into().unwrap(),
            PlanOutput { schema: schema.to_string().try_into().unwrap(), resources: vec![] },
        )]),
        authority: AuthorityUpperBound::default(),
        fan_out: FanOut::Expanded {},
        operation: PlanOperation::Transform {
            program: Program::closed(Expression::Literal { value }).unwrap(),
        },
    }
}
fn branching() -> FlowDefinition {
    let branch: OpaqueRef = "branch".to_string().try_into().unwrap();
    let a: OpaqueRef = "a".to_string().try_into().unwrap();
    let b: OpaqueRef = "b".to_string().try_into().unwrap();
    let yes: OpaqueRef = "yes".to_string().try_into().unwrap();
    let no: OpaqueRef = "no".to_string().try_into().unwrap();
    let mut root = pure_node("branch", vec![], true.into(), "bool");
    root.outputs.clear();
    root.operation = PlanOperation::Branch {
        program: Program::closed(Expression::Literal { value: true.into() }).unwrap(),
        when_true: yes.clone(),
        when_false: no.clone(),
    };
    let mut left = pure_node("a", vec![branch.clone()], 7.into(), "int");
    left.guards.insert(branch.clone(), yes);
    let mut right = pure_node("b", vec![branch.clone()], 9.into(), "int");
    right.guards.insert(branch.clone(), no);
    let mut join = pure_node("join", vec![a.clone(), b.clone()], 0.into(), "int");
    let port: OpaqueRef = "input".to_string().try_into().unwrap();
    join.inputs.insert(
        port.clone(),
        PlanInput {
            schema: "int".to_string().try_into().unwrap(),
            source: InputSource::Exclusive {
                branch,
                when_true: OutputRef { node: a, port: "value".to_string().try_into().unwrap() },
                when_false: OutputRef { node: b, port: "value".to_string().try_into().unwrap() },
            },
        },
    );
    join.operation =
        PlanOperation::Transform { program: Program::closed(Expression::Input { port }).unwrap() };
    FlowDefinition {
        id: "branches".to_string().try_into().unwrap(),
        version: 1,
        predecessor: None,
        body: FlowBody {
            entries: vec![root.id.clone()],
            nodes: vec![root, left, right, join.clone()],
            outputs: BTreeMap::from([(
                "answer".to_string().try_into().unwrap(),
                PlanInput {
                    schema: "int".to_string().try_into().unwrap(),
                    source: InputSource::Output {
                        source: OutputRef {
                            node: join.id,
                            port: "value".to_string().try_into().unwrap(),
                        },
                    },
                },
            )]),
            authority: AuthorityUpperBound::default(),
            causal_metadata: BTreeMap::new(),
        },
    }
}
#[tokio::test]
async fn branch_replay_skips_without_receipts_and_exclusive_join_publishes_selected_output() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut d = branching();
    let mut boundary = f.direct().document().body.nodes[0].clone();
    boundary.id = "skipped-boundary".to_string().try_into().unwrap();
    boundary.dependencies = vec!["branch".to_string().try_into().unwrap()];
    boundary
        .guards
        .insert("branch".to_string().try_into().unwrap(), "no".to_string().try_into().unwrap());
    d.body.nodes.push(boundary);
    d.body.authority = f.config.0.run_authority.clone();
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    // Activation affects future submissions only.
    let mut revised = d.body.clone();
    revised.nodes[0].operation = PlanOperation::Branch {
        program: Program::closed(Expression::Literal { value: false.into() }).unwrap(),
        when_true: "yes".to_string().try_into().unwrap(),
        when_false: "no".to_string().try_into().unwrap(),
    };
    let next = d.revise(revised).unwrap();
    service.activate_flow_version(&(), next, Some(1)).unwrap();
    f.drive(&mut q, &id).await;
    let before = f.run(&id);
    assert_eq!(before.outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(
        before.results[&"b".to_string().try_into().unwrap()].disposition,
        NodeDisposition::Skipped
    );
    assert_eq!(f.bundle.wi().read_artifact(before.exports.values().next().unwrap()).unwrap(), b"7");
    assert!(f.bundle.wi().effect_snapshot(&f.auth.0).unwrap().effects.is_empty());
    assert_eq!(f.connector.commits(), 0);
    q.shutdown().unwrap();
    f.bundle.wi().repository(worldinterface_storage::StoreKind::Flow).unwrap().rebuild().unwrap();
    assert_eq!(f.run(&id), before);
    let reopened = StoreBundle::open(&f._temp.path().join("bundle")).unwrap();
    assert_eq!(reopened.wi().flow_snapshot(&f.auth.0).unwrap().runs[&id], before);
}
#[tokio::test]
async fn admission_readiness_retained_wake_restart_and_duplicates_converge() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap();
    // Reconcile coordinator, then bypass host reconciliation until the child waits.
    let _ = q.tick().await.unwrap();
    q.shutdown().unwrap();
    let mut engine = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: f.bundle.aq_path(),
            ..Default::default()
        },
        f.handler(),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    for _ in 0..1000 {
        let _ = engine.tick().await.unwrap();
        let r = f.run(&id);
        let awaiting = r.node_tasks.values().all(|t| {
            engine
                .projection()
                .runs_for_task(t.as_str().parse().unwrap())
                .any(|r| r.state() == actionqueue_core::run::RunState::Awaiting)
        });
        if awaiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(f.run(&id).bindings.is_empty());
    assert_eq!(f.connector.commits(), 0);
    engine.shutdown().unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    f.drive(&mut q, &id).await;
    let r = f.run(&id);
    assert_eq!(r.outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(f.connector.commits(), 1);
    let seq = q.engine().projection().signals().statistics().retained;
    for _ in 0..3 {
        let _ = q.tick().await.unwrap();
    }
    assert_eq!(q.engine().projection().signals().statistics().retained, seq);
    assert!(
        q.engine()
            .projection()
            .get_attempt_history(&r.bindings.values().next().unwrap().run.as_str().parse().unwrap())
            .unwrap()
            .len()
            >= 2
    );
    q.shutdown().unwrap();
}
#[test]
fn flow_authority_purity_version_conflicts_and_unsupported_profile_reject_before_admission() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut d = FlowDefinition {
        id: "reject".to_string().try_into().unwrap(),
        version: 1,
        predecessor: None,
        body: f.direct().document().body.clone(),
    };
    if let PlanOperation::Boundary { resource, .. } = &mut d.body.nodes[0].operation {
        *resource = ResourceBinding::Fixed {
            resource: ResourceSelector {
                namespace: "connector".to_string().try_into().unwrap(),
                selector: "widened".to_string().try_into().unwrap(),
            },
        };
    }
    assert_eq!(
        service
            .activate_flow_version(&(), d, None)
            .unwrap_err()
            .downcast_ref::<CompileError>()
            .unwrap()
            .kind,
        ErrorKind::Authority
    );
    let mut d = branching();
    if let PlanOperation::Transform { program } = &mut d.body.nodes[1].operation {
        program.requested_access.push("network".to_string().try_into().unwrap());
    }
    assert_eq!(
        service
            .activate_flow_version(&(), d, None)
            .unwrap_err()
            .downcast_ref::<CompileError>()
            .unwrap()
            .kind,
        ErrorKind::HiddenIo
    );
    let mut d = branching();
    d.version = 2;
    assert!(service.activate_flow_version(&(), d, None).is_err());
    let d = branching();
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut bad = d.clone();
    bad.body.nodes[0]
        .guards
        .insert("branch".to_string().try_into().unwrap(), "yes".to_string().try_into().unwrap());
    assert!(service.activate_flow_version(&(), bad, None).is_err());
    let mut timer = pure_node("timer", vec![], true.into(), "bool");
    timer.outputs.clear();
    timer.operation = PlanOperation::Timer {
        deadline: Deadline {
            inclusive_at: "2030-01-01T00:00:00.000000000Z".to_string().try_into().unwrap(),
            action: TimeoutAction::Continue,
        },
    };
    let d = FlowDefinition {
        id: "timer".to_string().try_into().unwrap(),
        version: 1,
        predecessor: None,
        body: FlowBody {
            entries: vec![timer.id.clone()],
            nodes: vec![timer],
            outputs: BTreeMap::new(),
            authority: AuthorityUpperBound::default(),
            causal_metadata: BTreeMap::new(),
        },
    };
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    assert!(service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).is_err());
    assert_eq!(q.engine().projection().task_count(), 0);
    q.shutdown().unwrap();
    assert_eq!(f.connector.commits(), 0);
}

#[tokio::test]
async fn revoked_stale_registry_and_schema_bindings_prevent_connector_invocation() {
    for kind in ["revoked", "stale", "registry", "schema"] {
        let f = Fixture::new();
        let evidence = Evidence(f.snapshot.clone());
        let service = f.service(&evidence);
        let mut handler = f.handler();
        match kind {
            "revoked" => handler.effect.evidence.0.grants[0].revoked = true,
            "stale" => handler.resolver.0.document.version = 2,
            "registry" => {
                handler.effect.port.descriptor.implementation = ContentHash::of(b"changed")
            }
            _ => { /* Wrong bytes pass content integrity but fail the installed schema. */ }
        }
        let mut q = QueueEngine::bootstrap(&f.bundle, handler, host()).unwrap();
        let mut submission = f.submission.clone();
        if kind == "schema" {
            submission.input.artifact =
                f.bundle.wi().publish(f.auth.0.clone(), b"invalid-input").unwrap();
        }
        let id = service.submit_direct(&(), submission, f.selection(), &mut q).unwrap();
        f.drive(&mut q, &id).await;
        assert_ne!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
        assert_eq!(f.connector.commits(), 0, "{kind}");
        assert_eq!(
            f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "{kind}"
        );
        assert!(f
            .bundle
            .wi()
            .effect_snapshot(&f.auth.0)
            .unwrap()
            .effects
            .values()
            .all(|e| e.attempt.is_none()));
        q.shutdown().unwrap();
    }
}
struct PretendNodes(FlowAttemptHandler<DeterministicConnector, Evidence, Resolver, Clock>);
impl actionqueue_executor_local::handler::ExecutorHandler for PretendNodes {
    fn execute(
        &self,
        ctx: actionqueue_executor_local::handler::ExecutorContext,
    ) -> actionqueue_core::disposition::AttemptDisposition {
        let payload: serde_json::Value = serde_json::from_slice(&ctx.input.payload).unwrap();
        if payload.get("node").is_some() {
            actionqueue_core::disposition::AttemptDisposition::complete(None)
        } else {
            self.0.execute(ctx)
        }
    }
}
#[tokio::test]
async fn aq_success_without_wi_acceptance_cannot_complete_flow() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut q = QueueEngine::bootstrap(&f.bundle, PretendNodes(f.handler()), host()).unwrap();
    let id = service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap();
    for _ in 0..2000 {
        let _ = q.tick().await.unwrap();
        if f.run(&id).outcome.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let r = f.run(&id);
    assert_eq!(r.outcome, Some(FlowOutcome::Failed));
    assert!(r.results.is_empty());
    for task in r.node_tasks.values() {
        assert_eq!(
            q.engine().projection().task_terminal_status(task.as_str().parse().unwrap()),
            Some(actionqueue_core::continuation::TaskTerminalStatus::Succeeded)
        );
    }
    assert_eq!(f.connector.commits(), 0);
    q.shutdown().unwrap();
}
#[tokio::test]
async fn completed_flow_backup_restore_and_missing_artifact_rejection() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap();
    f.drive(&mut q, &id).await;
    let run = f.run(&id);
    q.shutdown().unwrap();
    install_checkpoint(&f.bundle.aq_path());
    let tenant = f.auth.0.clone();
    let path = f._temp.path().join("bundle");
    let Fixture { _temp: temp, bundle, .. } = f;
    drop(bundle);
    let backup = temp.path().join("backup");
    let restored = temp.path().join("restored");
    StoreBundle::backup(&path, &backup).unwrap();
    StoreBundle::restore(&backup, &restored).unwrap();
    let reopened = StoreBundle::open(&restored).unwrap();
    assert_eq!(reopened.wi().flow_snapshot(&tenant).unwrap().runs[&id], run);
    let artifact = run.results.values().next().unwrap().outputs.values().next().unwrap();
    drop(reopened);
    // Removing a published output fails validation before either store executes.
    let artifacts = restored.join("wi/artifacts");
    fn remove_content(path: &std::path::Path, content: &str) -> bool {
        for e in std::fs::read_dir(path).unwrap() {
            let e = e.unwrap();
            if e.path().is_dir() {
                if remove_content(&e.path(), content) {
                    return true;
                }
            } else if e.file_name() == content {
                std::fs::remove_file(e.path()).unwrap();
                return true;
            }
        }
        false
    }
    assert!(remove_content(&artifacts, artifact.content.as_str()));
    assert!(StoreBundle::open(&restored).is_err());
}

// Exercise the supported offline snapshot writer; ordinary production capture is
// also exercised at the default threshold by the full-capacity proof.
fn install_checkpoint(path: &std::path::Path) {
    use actionqueue_storage::{
        recovery::bootstrap::recover_read_only,
        snapshot::{
            build::build_snapshot_from_projection,
            writer::{SnapshotFsWriter, SnapshotWriter},
        },
        store::{open_store, OpenOptions},
        wal::repair::RepairPolicy,
    };
    let session = open_store(path, OpenOptions::ReadWrite).unwrap();
    let recovered = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    let snapshot = build_snapshot_from_projection(&recovered.projection, 1).unwrap();
    let mut writer = SnapshotFsWriter::new(&session).unwrap();
    writer.write(&snapshot).unwrap();
    writer.close().unwrap();
    assert!(recover_read_only(&session, RepairPolicy::Strict).unwrap().snapshot_loaded);
}

#[tokio::test]
async fn checkpoint_tail_and_wal_only_recovery_preserve_pending_flow_and_branch_receipts() {
    use actionqueue_storage::{
        recovery::bootstrap::recover_read_only,
        store::{open_store, OpenOptions},
        wal::repair::RepairPolicy,
    };
    let mut f = Fixture::new();
    let mut direct_submission = f.submission.clone();
    direct_submission.client_request_id = "checkpoint:direct".to_string().try_into().unwrap();
    let branch_submission = f.submission.clone();
    allow_demands(&mut f, &[branch_submission, direct_submission.clone()]);
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = branching();
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let branch = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    // Establish real parent waits and child admission evidence, then checkpoint
    // while the Flow is unfinished. Recovery must retain those exact bindings.
    for _ in 0..1000 {
        let _ = q.tick().await.unwrap();
        let r = f.run(&branch);
        if !r.bindings.is_empty() {
            assert!(r.outcome.is_none());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let pending = f.run(&branch);
    assert!(!pending.bindings.is_empty());
    q.shutdown().unwrap();
    install_checkpoint(&f.bundle.aq_path());
    q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    // This admission is strictly after the checkpoint and must come from its WAL tail.
    let direct =
        f.service(&evidence).submit_direct(&(), direct_submission, f.selection(), &mut q).unwrap();
    q.shutdown().unwrap();
    let session = open_store(&f.bundle.aq_path(), OpenOptions::ReadOnly).unwrap();
    let from_checkpoint = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    assert!(from_checkpoint.snapshot_loaded);
    assert!(from_checkpoint.projection.latest_sequence() > from_checkpoint.snapshot_sequence);
    let digest = from_checkpoint.projection.projection_digest().unwrap();
    let snapshot_path = session.snapshot_path();
    let snapshot = std::fs::read(&snapshot_path).unwrap();
    drop(from_checkpoint);
    drop(session);
    // An incompatible checkpoint fails closed without modifying the store.
    std::fs::write(&snapshot_path, b"unsupported checkpoint").unwrap();
    let session = open_store(&f.bundle.aq_path(), OpenOptions::ReadOnly).unwrap();
    assert!(recover_read_only(&session, RepairPolicy::Strict).is_err());
    assert_eq!(std::fs::read(&snapshot_path).unwrap(), b"unsupported checkpoint");
    drop(session);
    // Physical truncation is distinguishable from incompatibility and may use WAL.
    std::fs::write(&snapshot_path, &snapshot[..snapshot.len() - 1]).unwrap();
    let session = open_store(&f.bundle.aq_path(), OpenOptions::ReadOnly).unwrap();
    let recovered = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    assert!(!recovered.snapshot_loaded);
    assert_eq!(recovered.projection.projection_digest().unwrap(), digest);
    drop(recovered);
    drop(session);
    std::fs::remove_file(&snapshot_path).unwrap();
    let session = open_store(&f.bundle.aq_path(), OpenOptions::ReadOnly).unwrap();
    let recovered = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    assert!(!recovered.snapshot_loaded);
    assert_eq!(recovered.projection.projection_digest().unwrap(), digest);
    drop(recovered);
    drop(session);
    std::fs::write(&snapshot_path, &snapshot).unwrap();
    let path = f._temp.path().join("bundle");
    f.bundle = Arc::new(StoreBundle::open(&path).unwrap());
    assert_eq!(f.run(&branch), pending);
    q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    f.drive(&mut q, &branch).await;
    f.drive(&mut q, &direct).await;
    let branch_run = f.run(&branch);
    let direct_run = f.run(&direct);
    assert_eq!(branch_run.outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(
        branch_run.results[&"branch".to_string().try_into().unwrap()]
            .branch
            .as_ref()
            .unwrap()
            .as_str(),
        "yes"
    );
    assert_eq!(direct_run.outcome, Some(FlowOutcome::Succeeded));
    let effect = direct_run.results.values().next().unwrap().effect.as_ref().unwrap();
    let receipts = f.bundle.wi().effect_receipts(&f.auth.0, effect).unwrap();
    assert_eq!(receipts.len(), 1);
    q.shutdown().unwrap();
    // Compare complete WI results and receipts through both recovery paths.
    for checkpoint in [true, false] {
        if !checkpoint {
            std::fs::remove_file(&snapshot_path).unwrap();
        }
        let reopened = StoreBundle::open(&path).unwrap();
        assert_eq!(
            reopened.wi().flow_run_snapshot(&f.auth.0, &branch).unwrap().runs[&branch],
            branch_run
        );
        assert_eq!(
            reopened.wi().flow_run_snapshot(&f.auth.0, &direct).unwrap().runs[&direct],
            direct_run
        );
        assert_eq!(reopened.wi().effect_receipts(&f.auth.0, effect).unwrap(), receipts);
    }
    // Strict validation rejects both CRC corruption and partial tail frames,
    // before WI journal recovery can mutate any source database.
    std::fs::write(&snapshot_path, &snapshot).unwrap();
    let session = open_store(&f.bundle.aq_path(), OpenOptions::ReadOnly).unwrap();
    let wal = session.wal_path();
    drop(session);
    let complete = std::fs::read(&wal).unwrap();
    let before = std::fs::read(path.join("wi/flow.sqlite")).unwrap();
    for partial in [false, true] {
        let mut damaged = complete.clone();
        if partial {
            damaged.extend_from_slice(b"partial tail");
        } else {
            *damaged.last_mut().unwrap() ^= 1;
        }
        std::fs::write(&wal, &damaged).unwrap();
        assert!(StoreBundle::open(&path).is_err());
        assert_eq!(std::fs::read(path.join("wi/flow.sqlite")).unwrap(), before);
        assert_eq!(std::fs::read(&wal).unwrap(), damaged);
    }
}

#[tokio::test]
async fn flow_cancellation_after_start_preserves_external_uncertainty() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Condvar, Mutex,
    };
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, cond) = &*self.0;
            *lock.lock().unwrap() = true;
            cond.notify_all();
        }
    }
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let entered = Arc::new(AtomicBool::new(false));
    let latch = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(latch.clone());
    let mut handler = f.handler();
    handler.effect.port.mode = Mode::Ambiguous;
    let hook_entered = entered.clone();
    let hook_latch = latch.clone();
    handler.effect.port.after_commit = Some(Arc::new(move || {
        hook_entered.store(true, Ordering::SeqCst);
        let (lock, cond) = &*hook_latch;
        let _guard = cond.wait_while(lock.lock().unwrap(), |ready| !*ready).unwrap();
    }));
    let mut q = QueueEngine::bootstrap(&f.bundle, handler, host()).unwrap();
    let id = service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap();
    for _ in 0..2000 {
        let _ = q.tick().await.unwrap();
        if entered.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(entered.load(Ordering::SeqCst));
    assert_eq!(f.connector.commits(), 1);
    q.cancel_task(f.run(&id).coordinator_task.as_str().parse().unwrap()).unwrap();
    // Reconciliation observes Running/Start before the connector returns.
    let _ = q.tick().await.unwrap();
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Uncertain));
    drop(release);
    for _ in 0..2000 {
        let _ = q.tick().await.unwrap();
        if f.bundle
            .wi()
            .effect_snapshot(&f.auth.0)
            .unwrap()
            .effects
            .values()
            .any(|e| !e.outcomes.is_empty())
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Uncertain));
    assert_eq!(f.connector.commits(), 1);
    assert!(f
        .bundle
        .wi()
        .effect_snapshot(&f.auth.0)
        .unwrap()
        .effects
        .values()
        .all(|e| e.outcomes.last().is_some_and(|o| o.outcome == ExecutionOutcome::Uncertain)));
    q.shutdown().unwrap();
}

#[test]
fn scoped_flow_request_dedup_and_foreign_inspection_preserve_original_admission() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap();
    let original = f.run(&id);
    assert_eq!(
        service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap(),
        id
    );
    assert_eq!(q.engine().projection().task_count(), 1);
    let mut conflicting = f.submission.clone();
    conflicting.purpose = Some("changed-attribution".to_string().try_into().unwrap());
    assert!(service.submit_direct(&(), conflicting, f.selection(), &mut q).is_err());
    assert_eq!(f.run(&id), original);
    let foreign = Auth("foreign".to_string().try_into().unwrap(), f.auth.1.clone());
    let other = PlanService {
        root: f.bundle.wi(),
        authentication: &foreign,
        evidence: &evidence,
        clock: &f.clock,
        configuration: &f.config,
    };
    assert!(other.inspect_run(&(), &id).is_err());
    assert_eq!(f.connector.commits(), 0);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn bounded_64_node_plan_commits_flat_children_and_rejects_65_before_admission() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let nodes: Vec<_> =
        (0..64).map(|i| pure_node(&format!("n{i:02}"), vec![], true.into(), "bool")).collect();
    let d = FlowDefinition {
        id: "bounded".to_string().try_into().unwrap(),
        version: 1,
        predecessor: None,
        body: FlowBody {
            entries: nodes.iter().map(|n| n.id.clone()).collect(),
            nodes,
            outputs: BTreeMap::new(),
            authority: AuthorityUpperBound::default(),
            causal_metadata: BTreeMap::new(),
        },
    };
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut too_large = d.clone();
    too_large.id = "excessive".to_string().try_into().unwrap();
    too_large.body.nodes.push(pure_node("excess", vec![], true.into(), "bool"));
    too_large.body.entries.push("excess".to_string().try_into().unwrap());
    assert_eq!(
        service
            .activate_flow_version(&(), too_large, None)
            .unwrap_err()
            .downcast_ref::<CompileError>()
            .unwrap()
            .kind,
        ErrorKind::Bound
    );
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    f.drive(&mut q, &id).await;
    let r = f.run(&id);
    assert_eq!(r.outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(r.bindings.len(), 64);
    assert_eq!(r.results.len(), 64);
    let parent: TaskId = r.coordinator_task.as_str().parse().unwrap();
    assert_eq!(
        q.engine().projection().parent_child_mappings().filter(|(_, p)| *p == parent).count(),
        64
    );
    let parent_run = r.parent_run.as_ref().unwrap().as_str().parse().unwrap();
    let committed: Vec<_> = q
        .engine()
        .projection()
        .get_attempt_history(&parent_run)
        .unwrap()
        .iter()
        .filter_map(|a| a.disposition.as_ref())
        .filter(|d| !d.disposition.child_admissions().is_empty())
        .collect();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].disposition.child_admissions().len(), 64);
    assert!(matches!(
        committed[0].disposition.outcome(),
        actionqueue_core::disposition::DispositionOutcome::Awaiting
    ));
    assert_eq!(f.connector.commits(), 0);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn skipped_boundary_has_no_effect_or_receipt_while_selected_boundary_completes() {
    let mut f = Fixture::new();
    // The host's exact-demand authority already covers the two deterministic
    // materializations. Flow still cannot enlarge either structural ceiling.
    let mut demands = vec![worldinterface_domain::ingress::digest(&f.submission).unwrap()];
    for node in ["selected", "skipped"] {
        let mut s = f.submission.clone();
        s.client_request_id = worldinterface_runtime::flow::node_request_id(
            &s.client_request_id,
            &node.to_string().try_into().unwrap(),
        )
        .unwrap();
        demands.push(worldinterface_domain::ingress::digest(&s).unwrap());
    }
    for b in &mut f.snapshot.bindings {
        b.principal_authority = demands.iter().cloned().collect();
        b.capacity_authority = b.principal_authority.clone();
    }
    f.snapshot.policy.platform = demands.iter().cloned().collect();
    f.snapshot.policy.resource = f.snapshot.policy.platform.clone();
    f.snapshot.policy.information_use = f.snapshot.policy.platform.clone();
    for g in &mut f.snapshot.grants {
        g.authority = demands.iter().cloned().collect();
    }
    let direct = f.direct();
    let mut a = direct.document().body.nodes[0].clone();
    let mut b = a.clone();
    let mut branch = pure_node("branch", vec![], true.into(), "bool");
    branch.outputs.clear();
    branch.operation = PlanOperation::Branch {
        program: Program::closed(Expression::Literal { value: true.into() }).unwrap(),
        when_true: "yes".to_string().try_into().unwrap(),
        when_false: "no".to_string().try_into().unwrap(),
    };
    a.id = "selected".to_string().try_into().unwrap();
    a.dependencies = vec![branch.id.clone()];
    a.guards.insert(branch.id.clone(), "yes".to_string().try_into().unwrap());
    b.id = "skipped".to_string().try_into().unwrap();
    b.dependencies = vec![branch.id.clone()];
    b.guards.insert(branch.id.clone(), "no".to_string().try_into().unwrap());
    let d = FlowDefinition {
        id: "boundary-branches".to_string().try_into().unwrap(),
        version: 1,
        predecessor: None,
        body: FlowBody {
            entries: vec![branch.id.clone()],
            nodes: vec![branch, a, b],
            outputs: BTreeMap::new(),
            authority: f.config.0.run_authority.clone(),
            causal_metadata: BTreeMap::new(),
        },
    };
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    f.drive(&mut q, &id).await;
    let r = f.run(&id);
    assert_eq!(r.outcome, Some(FlowOutcome::Succeeded));
    let skipped = &r.results[&"skipped".to_string().try_into().unwrap()];
    assert_eq!(skipped.disposition, NodeDisposition::Skipped);
    assert!(skipped.effect.is_none());
    let effects = f.bundle.wi().effect_snapshot(&f.auth.0).unwrap();
    assert_eq!(effects.effects.len(), 1);
    assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        f.bundle
            .wi()
            .effect_receipts(&f.auth.0, effects.effects.keys().next().unwrap())
            .unwrap()
            .len(),
        1
    );
    q.shutdown().unwrap();
}

#[tokio::test]
async fn flow_boundary_crash_child() {
    let Some(parent) = std::env::var_os("WI_FLOW_FIXTURE_PARENT") else { return };
    let parent = std::path::PathBuf::from(parent);
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut handler = f.handler();
    let ready = parent.join("ready");
    handler.effect.port.after_commit = Some(Arc::new(move || {
        std::fs::write(&ready, b"committed").unwrap();
        loop {
            std::thread::park_timeout(std::time::Duration::from_millis(10));
        }
    }));
    let mut q = QueueEngine::bootstrap(&f.bundle, handler, host()).unwrap();
    let id = service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap();
    std::fs::write(
        parent.join("fixture.json"),
        serde_json::to_vec(&(
            f._temp.path(),
            &f.snapshot,
            &f.config.0,
            &f.submission,
            f.clock.now().unwrap(),
            &id,
        ))
        .unwrap(),
    )
    .unwrap();
    f.drive(&mut q, &id).await;
    panic!("crash barrier not reached");
}
#[tokio::test]
async fn flow_child_start_restart_preserves_binding_and_never_reinvokes_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "flow_boundary_crash_child", "--nocapture"])
        .env("WI_FLOW_FIXTURE_PARENT", temp.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut child = child_guard::KillOnDrop(child);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while !temp.path().join("ready").exists() {
        assert!(child.try_wait().unwrap().is_none());
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let (path, snapshot, pins, submission, at, id): (
        std::path::PathBuf,
        worldinterface_domain::authorization::AuthorizationSnapshot,
        FlowPins,
        EffectSubmission,
        Timestamp,
        OpaqueRef,
    ) = serde_json::from_slice(&std::fs::read(temp.path().join("fixture.json")).unwrap()).unwrap();
    let connector = DeterministicConnector::new(
        path.join("provider"),
        IdempotencyClass::ExternallyKeyed,
        Mode::Commit,
    );
    let auth = Auth(snapshot.bindings[0].tenant.clone(), snapshot.bindings[0].principal.clone());
    let clock = Clock(Arc::new(std::sync::atomic::AtomicU64::new(at.epoch_millis().unwrap())));
    let f = Fixture {
        _temp: temp,
        bundle: Arc::new(StoreBundle::open(&path.join("bundle")).unwrap()),
        connector,
        snapshot,
        submission,
        clock,
        config: Config(pins),
        auth,
    };
    assert_eq!(f.connector.commits(), 1);
    let before = f.run(&id).bindings;
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Uncertain));
    assert_eq!(f.run(&id).bindings, before);
    assert_eq!(f.connector.commits(), 1);
    assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    q.shutdown().unwrap();
}

struct DeniedArtifacts(Config);
impl worldinterface_runtime::artifact::ArtifactAuthorization for DeniedArtifacts {
    type AuthenticatedContext = AuthenticatedCaller;
    fn authorize_read(&self, _: &AuthenticatedCaller, _: &ArtifactRef) -> bool {
        false
    }
}
impl PlanConfiguration for DeniedArtifacts {
    fn pins(&self, caller: &AuthenticatedCaller) -> Result<FlowPins> {
        self.0.pins(caller)
    }
}
#[tokio::test]
async fn host_artifact_authorization_is_required_at_submission_and_execution() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let denied = DeniedArtifacts(f.config.clone());
    let service = PlanService {
        root: f.bundle.wi(),
        authentication: &f.auth,
        evidence: &evidence,
        clock: &f.clock,
        configuration: &denied,
    };
    let mut handler = f.handler();
    handler.artifacts = Arc::new(DeniedArtifacts(f.config.clone()));
    let mut q = QueueEngine::bootstrap(&f.bundle, handler, host()).unwrap();
    assert!(service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).is_err());
    assert_eq!(q.engine().projection().task_count(), 0);
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Failed));
    assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(service.inspect_run(&(), &id).is_err());
    q.shutdown().unwrap();
}

#[tokio::test]
async fn maximum_length_flow_identities_have_bounded_aq_and_effect_keys() {
    assert_ne!(
        worldinterface_runtime::flow::node_request_id(
            &"a:node:b".to_string().try_into().unwrap(),
            &"c".to_string().try_into().unwrap()
        )
        .unwrap(),
        worldinterface_runtime::flow::node_request_id(
            &"a".to_string().try_into().unwrap(),
            &"b:node:c".to_string().try_into().unwrap()
        )
        .unwrap()
    );

    let mut f = Fixture::new();
    f.submission.client_request_id = "r".repeat(256).try_into().unwrap();
    let ids: Vec<OpaqueRef> =
        ['a', 'b'].into_iter().map(|c| c.to_string().repeat(256).try_into().unwrap()).collect();
    let mut demands = vec![worldinterface_domain::ingress::digest(&f.submission).unwrap()];
    for node in &ids {
        let mut s = f.submission.clone();
        s.client_request_id =
            worldinterface_runtime::flow::node_request_id(&s.client_request_id, node).unwrap();
        assert!(s.client_request_id.as_str().len() <= 256);
        demands.push(worldinterface_domain::ingress::digest(&s).unwrap());
    }
    for b in &mut f.snapshot.bindings {
        b.principal_authority = demands.iter().cloned().collect();
        b.capacity_authority = b.principal_authority.clone();
    }
    f.snapshot.policy.platform = demands.iter().cloned().collect();
    f.snapshot.policy.resource = f.snapshot.policy.platform.clone();
    f.snapshot.policy.information_use = f.snapshot.policy.platform.clone();
    for g in &mut f.snapshot.grants {
        g.authority = demands.iter().cloned().collect();
    }
    let plan = f.direct();
    let nodes: Vec<_> = ids
        .iter()
        .map(|id| {
            let mut n = plan.document().body.nodes[0].clone();
            n.id = id.clone();
            n
        })
        .collect();
    let d = FlowDefinition {
        id: "f".repeat(256).try_into().unwrap(),
        version: 1,
        predecessor: None,
        body: FlowBody {
            entries: ids,
            nodes,
            outputs: BTreeMap::new(),
            authority: f.config.0.run_authority.clone(),
            causal_metadata: BTreeMap::new(),
        },
    };
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(q.engine().projection().admissions().all(|a| a.key().as_str().len() <= 256));
    q.shutdown().unwrap();
}

fn allow_demands(f: &mut Fixture, submissions: &[EffectSubmission]) {
    let demands: std::collections::BTreeSet<_> =
        submissions.iter().map(|s| worldinterface_domain::ingress::digest(s).unwrap()).collect();
    for b in &mut f.snapshot.bindings {
        b.principal_authority = demands.clone();
        b.capacity_authority = demands.clone();
    }
    f.snapshot.policy.platform = demands.clone();
    f.snapshot.policy.resource = demands.clone();
    f.snapshot.policy.information_use = demands.clone();
    for g in &mut f.snapshot.grants {
        g.authority = demands.clone();
    }
}
fn boundary_definition(f: &Fixture, count: usize) -> FlowDefinition {
    let nodes: Vec<_> = (0..count)
        .map(|i| {
            let mut n = f.direct().document().body.nodes[0].clone();
            n.id = format!("n{i:02}").try_into().unwrap();
            n
        })
        .collect();
    FlowDefinition {
        id: "boundaries".to_string().try_into().unwrap(),
        version: 1,
        predecessor: None,
        body: FlowBody {
            entries: nodes.iter().map(|n| n.id.clone()).collect(),
            nodes,
            outputs: BTreeMap::new(),
            authority: f.config.0.run_authority.clone(),
            causal_metadata: BTreeMap::new(),
        },
    }
}
struct Latch(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
impl Latch {
    fn new() -> Self {
        Self(Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new())))
    }
    fn hook(&self, entered: Arc<std::sync::atomic::AtomicBool>) -> Arc<dyn Fn() + Send + Sync> {
        let latch = self.0.clone();
        Arc::new(move || {
            entered.store(true, std::sync::atomic::Ordering::SeqCst);
            let (lock, cond) = &*latch;
            drop(cond.wait_while(lock.lock().unwrap(), |ready| !*ready).unwrap());
        })
    }
}
impl Drop for Latch {
    fn drop(&mut self) {
        let (lock, cond) = &*self.0;
        *lock.lock().unwrap() = true;
        cond.notify_all();
    }
}
#[tokio::test]
async fn cancellation_before_start_and_failed_node_redelivery_cannot_invoke() {
    for target in ["parent", "child", "failed"] {
        let f = Fixture::new();
        let evidence = Evidence(f.snapshot.clone());
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let latch = Latch::new();
        let mut handler = f.handler();
        // Hosts may open a separate bundle object for the handler.
        handler.effect.bundle =
            Arc::new(StoreBundle::open(&f._temp.path().join("bundle")).unwrap());
        handler.effect.port.after_prepare = Some(latch.hook(entered.clone()));
        let mut q = QueueEngine::bootstrap(&f.bundle, handler, host()).unwrap();
        let id = f
            .service(&evidence)
            .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
            .unwrap();
        for _ in 0..2000 {
            let _ = q.tick().await.unwrap();
            if entered.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
        let run = f.run(&id);
        if target != "failed" {
            let task = if target == "parent" {
                &run.coordinator_task
            } else {
                run.node_tasks.values().next().unwrap()
            };
            q.cancel_task(task.as_str().parse().unwrap()).unwrap();
        } else {
            let node = run.node_tasks.keys().next().unwrap();
            let effect = f
                .bundle
                .wi()
                .effect_snapshot(&f.auth.0)
                .unwrap()
                .effects
                .keys()
                .next()
                .unwrap()
                .clone();
            f.bundle
                .wi()
                .flow_transact(
                    &f.auth.0,
                    f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().revision,
                    FlowCommand::RecordNodeResult {
                        run: id.clone(),
                        node: node.clone(),
                        result: NodeResult {
                            disposition: NodeDisposition::Failed,
                            effect: Some(effect),
                            values: BTreeMap::new(),
                            outputs: BTreeMap::new(),
                            branch: None,
                            program: None,
                            input_hash: ContentHash::of(b""),
                            output_hash: ContentHash::of(b""),
                        },
                    },
                )
                .unwrap();
        }
        let _ = q.tick().await.unwrap();
        drop(latch);
        f.drive(&mut q, &id).await;
        assert_eq!(f.connector.commits(), 0);
        assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(
            f.run(&id).outcome,
            Some(if target == "failed" { FlowOutcome::Failed } else { FlowOutcome::Canceled })
        );
        q.shutdown().unwrap();
    }
}
#[tokio::test]
async fn publication_failure_retries_durable_success_without_reinvocation() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    let db = rusqlite::Connection::open(f._temp.path().join("bundle/wi/flow.sqlite")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_result BEFORE INSERT ON flow_history WHEN CAST(NEW.entry AS TEXT) LIKE '%record_node_result%' BEGIN SELECT RAISE(ABORT,'injected journal contention'); END;").unwrap();
    for _ in 0..2000 {
        let _ = q.tick().await.unwrap();
        let retry = q.engine().projection().run_instances().any(|r| {
            q.engine()
                .projection()
                .get_attempt_history(&r.id())
                .into_iter()
                .flat_map(|history| history.iter())
                .any(|a| {
                    a.disposition.as_ref().is_some_and(|d| {
                    matches!(
                        d.disposition.outcome(),
                        actionqueue_core::disposition::DispositionOutcome::RetryableFailure { .. }
                    )
                })
                })
        });
        if retry {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert_eq!(f.connector.commits(), 1);
    assert!(f.run(&id).results.is_empty());
    assert!(f.run(&id).outcome.is_none());
    db.execute_batch("DROP TRIGGER fail_result").unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    q.shutdown().unwrap();
}
#[tokio::test]
async fn pre_start_journal_failure_is_retryable_and_then_succeeds() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    let db = rusqlite::Connection::open(f._temp.path().join("bundle/wi/flow.sqlite")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_proposal BEFORE INSERT ON flow_history WHEN CAST(NEW.entry AS TEXT) LIKE '%prepare_children%' BEGIN SELECT RAISE(ABORT,'temporary schema storage failure, foreign cache not authorized'); END;").unwrap();
    let mut retried = false;
    for _ in 0..2000 {
        let _ = q.tick().await.unwrap();
        retried = q.engine().projection().run_instances().any(|r| {
            q.engine()
                .projection()
                .get_attempt_history(&r.id())
                .into_iter()
                .flat_map(|history| history.iter())
                .any(|a| {
                    a.disposition.as_ref().is_some_and(|d| {
                    matches!(
                        d.disposition.outcome(),
                        actionqueue_core::disposition::DispositionOutcome::RetryableFailure { .. }
                    )
                })
                })
        });
        if retried {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(retried);
    assert_eq!(f.connector.commits(), 0);
    db.execute_batch("DROP TRIGGER fail_proposal").unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    q.shutdown().unwrap();
}
#[tokio::test]
async fn flow_owned_authorized_effect_cannot_enter_legacy_dispatch() {
    use worldinterface_domain::{
        agency::AuthenticatedCaller,
        effect::{EffectAction, EffectOwner},
    };
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let run = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    let node = f.run(&run).node_tasks.keys().next().unwrap().clone();
    let caller =
        AuthenticatedCaller::from_trusted_authentication(f.auth.0.clone(), f.auth.1.clone());
    let egress = worldinterface_egress::Egress::new(f.bundle.wi());
    let snapshot = worldinterface_runtime::authority::verify_snapshot(
        f.bundle.wi(),
        &evidence,
        &f.auth.0,
        &f.submission,
        &f.clock.now().unwrap(),
    )
    .unwrap();
    let id = egress
        .apply(
            &caller,
            f.clock.now().unwrap(),
            EffectAction::SubmitFlow {
                id: EffectIntentId::generate(),
                submission: Box::new(f.submission.clone()),
                snapshot: snapshot.clone(),
                owner: EffectOwner { flow: run, node },
            },
        )
        .unwrap();
    egress
        .apply(
            &caller,
            f.clock.now().unwrap(),
            EffectAction::Resolve { id: id.clone(), snapshot: snapshot.clone() },
        )
        .unwrap();
    egress
        .apply(
            &caller,
            f.clock.now().unwrap(),
            EffectAction::Authorize { id: id.clone(), snapshot: snapshot.clone() },
        )
        .unwrap();
    let service = worldinterface_runtime::egress::EffectService {
        root: f.bundle.wi(),
        authentication: &f.auth,
        evidence: &evidence,
        resolver: &Resolver(f.snapshot.facts[0].clone()),
        clock: &f.clock,
    };
    assert!(service.advance(&(), &id).unwrap_err().to_string().contains("Flow-owned"));
    assert!(worldinterface_runtime::admission::prepare(
        &f.bundle,
        &caller,
        f.clock.now().unwrap(),
        &id,
        snapshot
    )
    .unwrap_err()
    .to_string()
    .contains("Flow-owned"));
    assert_eq!(service.dispatch_pending(&(), &mut q, 100).unwrap().attempted, 0);
    assert_eq!(q.engine().projection().task_count(), 1);
    assert_eq!(f.connector.commits(), 0);
    assert!(egress.inspect(&caller, &id).unwrap().dispatch.is_none());
    q.shutdown().unwrap();
}
#[tokio::test]
async fn node_request_collision_uses_distinct_owned_effects_and_queue_keeps_progressing() {
    let mut f = Fixture::new();
    let d = boundary_definition(&f, 2);
    let mut direct = f.submission.clone();
    direct.client_request_id = worldinterface_runtime::flow::node_request_id(
        &direct.client_request_id,
        &d.body.nodes[0].id,
    )
    .unwrap();
    let mut submissions = vec![f.submission.clone(), direct.clone()];
    for n in &d.body.nodes {
        let mut s = f.submission.clone();
        s.client_request_id =
            worldinterface_runtime::flow::node_request_id(&s.client_request_id, &n.id).unwrap();
        submissions.push(s);
    }
    allow_demands(&mut f, &submissions);
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let direct_run = service.submit_direct(&(), direct, f.selection(), &mut q).unwrap();
    f.drive(&mut q, &direct_run).await;
    let flow = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    f.drive(&mut q, &flow).await;
    assert_eq!(
        f.run(&flow).outcome,
        Some(FlowOutcome::Succeeded),
        "{:?}",
        f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().quarantined
    );
    assert_eq!(f.connector.commits(), 3);
    assert_eq!(f.bundle.wi().effect_snapshot(&f.auth.0).unwrap().effects.len(), 3);
    assert!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().quarantined.is_empty());
    q.shutdown().unwrap();
}
#[tokio::test]
async fn damaged_run_is_quarantined_while_another_tenant_completes() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let bad = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    let mut state = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap();
    state.runs.get_mut(&bad).unwrap().plan_hash = ContentHash::of(b"wrong");
    let db = rusqlite::Connection::open(f._temp.path().join("bundle/wi/flow.sqlite")).unwrap();
    db.execute(
        "UPDATE flow_runs SET state=? WHERE tenant=? AND id=?",
        rusqlite::params![
            canonical_collection(&state.runs[&bad]).unwrap(),
            f.auth.0.as_str(),
            bad.as_str()
        ],
    )
    .unwrap();
    // A second tenant's pure plan needs no boundary authority and remains schedulable.
    let auth = Auth("tenant:healthy".to_string().try_into().unwrap(), f.auth.1.clone());
    let mut snap = f.snapshot.clone();
    for b in &mut snap.bindings {
        b.tenant = auth.0.clone();
    }
    let mut submission = f.submission.clone();
    submission.input.artifact = f.bundle.wi().publish(auth.0.clone(), b"input").unwrap();
    let mut v = serde_json::to_value(&snap).unwrap();
    fn replace_tenant(v: &mut serde_json::Value, old: &str, new: &str) {
        match v {
            serde_json::Value::String(s) if s == old => *s = new.into(),
            serde_json::Value::Array(a) => {
                for v in a {
                    replace_tenant(v, old, new);
                }
            }
            serde_json::Value::Object(m) => {
                for v in m.values_mut() {
                    replace_tenant(v, old, new);
                }
            }
            _ => {}
        }
    }
    replace_tenant(&mut v, f.auth.0.as_str(), auth.0.as_str());
    snap = serde_json::from_value(v).unwrap();
    for bytes in [b"input".as_slice(), b"resolution", b"grant"] {
        f.bundle.wi().publish(auth.0.clone(), bytes).unwrap();
    }
    let demand = worldinterface_domain::ingress::digest(&submission).unwrap();
    for b in &mut snap.bindings {
        b.principal_authority.insert(demand.clone());
        b.capacity_authority.insert(demand.clone());
    }
    let evidence2 = Evidence(snap);
    let service = PlanService {
        root: f.bundle.wi(),
        authentication: &auth,
        evidence: &evidence2,
        clock: &f.clock,
        configuration: &f.config,
    };
    let d = branching();
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let good = service.submit_flow(&(), &d.id, submission, &mut q).unwrap();
    for _ in 0..2000 {
        let _ = q.tick().await.unwrap();
        if f.bundle.wi().flow_snapshot(&auth.0).unwrap().runs[&good].outcome.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert_eq!(
        f.bundle.wi().flow_snapshot(&auth.0).unwrap().runs[&good].outcome,
        Some(FlowOutcome::Succeeded)
    );
    assert!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().quarantined.contains_key(&bad));
    assert_eq!(f.connector.commits(), 0);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn wi_admission_crash_child() {
    let Some(parent) = std::env::var_os("WI_ADMISSION_CRASH_PARENT") else { return };
    let parent = std::path::PathBuf::from(parent);
    let mut f = Fixture::new();
    let d = boundary_definition(&f, 3);
    let mut submissions = vec![f.submission.clone()];
    for n in &d.body.nodes {
        let mut s = f.submission.clone();
        s.client_request_id =
            worldinterface_runtime::flow::node_request_id(&s.client_request_id, &n.id).unwrap();
        submissions.push(s);
    }
    allow_demands(&mut f, &submissions);
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    std::fs::write(
        parent.join("fixture.json"),
        serde_json::to_vec(&(
            f._temp.path(),
            &f.snapshot,
            &f.config.0,
            &f.submission,
            f.clock.now().unwrap(),
            &id,
        ))
        .unwrap(),
    )
    .unwrap();
    f.drive(&mut q, &id).await;
    panic!("admission crash barrier not reached");
}
#[tokio::test]
async fn wi_proposal_aq_commit_and_binding_process_death_recover_stable_children() {
    for point in ["flow-proposal-committed", "flow-child-before-bind", "flow-child-bound"] {
        let temp = tempfile::tempdir().unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "wi_admission_crash_child", "--nocapture"])
            .env("WI_FLOW_FIXTURE_PARENT", temp.path())
            .env("WI_ADMISSION_CRASH_PARENT", temp.path())
            .env("WI_CRASH_POINT", point)
            .env("WI_CRASH_READY", temp.path().join("ready"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut child = child_guard::KillOnDrop(child);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !temp.path().join("ready").exists() {
            assert!(child.try_wait().unwrap().is_none(), "helper exited at {point}");
            assert!(std::time::Instant::now() < deadline, "barrier {point}");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let (path, snapshot, pins, submission, at, id): (
            std::path::PathBuf,
            worldinterface_domain::authorization::AuthorizationSnapshot,
            FlowPins,
            EffectSubmission,
            Timestamp,
            OpaqueRef,
        ) = serde_json::from_slice(&std::fs::read(temp.path().join("fixture.json")).unwrap())
            .unwrap();
        let connector = DeterministicConnector::new(
            path.join("provider"),
            IdempotencyClass::ExternallyKeyed,
            Mode::Commit,
        );
        let auth =
            Auth(snapshot.bindings[0].tenant.clone(), snapshot.bindings[0].principal.clone());
        let f = Fixture {
            _temp: temp,
            bundle: Arc::new(StoreBundle::open(&path.join("bundle")).unwrap()),
            connector,
            snapshot,
            submission,
            clock: Clock(Arc::new(std::sync::atomic::AtomicU64::new(at.epoch_millis().unwrap()))),
            config: Config(pins),
            auth,
        };
        let before = f.run(&id);
        assert!(before.proposal.is_some());
        assert_eq!(f.connector.commits(), 0);
        let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        let children = before
            .node_tasks
            .values()
            .filter(|task| {
                q.engine().projection().task_admission(task.as_str().parse().unwrap()).is_some()
            })
            .count();
        assert_eq!(children, if point == "flow-proposal-committed" { 0 } else { 3 });
        assert_eq!(before.bindings.len(), usize::from(point == "flow-child-bound"));
        if point == "flow-child-bound" {
            let node = before
                .node_tasks
                .keys()
                .find(|n| !before.bindings.contains_key(*n))
                .unwrap()
                .clone();
            let duplicate = before.bindings.values().next().unwrap().clone();
            let state = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap();
            let error = f
                .bundle
                .wi()
                .flow_transact(
                    &f.auth.0,
                    state.revision,
                    FlowCommand::BindChild { run: id.clone(), node, binding: duplicate },
                )
                .unwrap_err();
            assert!(error.to_string().contains("invalid Flow child binding"));
            assert_eq!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().revision, state.revision);
        }
        f.drive(&mut q, &id).await;
        let after = f.run(&id);
        assert_eq!(
            after.outcome,
            Some(FlowOutcome::Succeeded),
            "{point}: {:?}; results {:?}; effects {:?}; parent history {:?}",
            f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().quarantined,
            after.results,
            f.bundle.wi().effect_snapshot(&f.auth.0).unwrap(),
            q.engine()
                .projection()
                .get_attempt_history(&after.parent_run.as_ref().unwrap().as_str().parse().unwrap())
        );
        assert_eq!(after.node_tasks, before.node_tasks);
        assert_eq!(after.proposal, before.proposal);
        for (node, binding) in before.bindings {
            assert_eq!(after.bindings[&node], binding);
        }
        assert_eq!(after.bindings.len(), 3);
        assert_eq!(f.connector.commits(), 3);
        assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(q.engine().projection().signals().statistics().retained, 0);
        q.shutdown().unwrap();
    }
}
#[tokio::test]
async fn boundary_attempt_timeout_tracks_budget_in_seconds() {
    for budget in [1, 60_001, 120_000] {
        let mut f = Fixture::new();
        f.submission.budget.timeout_millis = budget;
        let submission = f.submission.clone();
        allow_demands(&mut f, &[submission]);
        let evidence = Evidence(f.snapshot.clone());
        let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        let id = f
            .service(&evidence)
            .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
            .unwrap();
        let run = f.run(&id);
        assert_eq!(
            q.engine()
                .projection()
                .task_admission(run.coordinator_task.as_str().parse().unwrap())
                .unwrap()
                .request()
                .task_spec()
                .constraints()
                .timeout_secs(),
            None,
            "the coordinator has no external operation budget"
        );
        f.drive(&mut q, &id).await;
        for task in f.run(&id).node_tasks.values() {
            assert_eq!(
                q.engine()
                    .projection()
                    .task_admission(task.as_str().parse().unwrap())
                    .unwrap()
                    .request()
                    .task_spec()
                    .constraints()
                    .timeout_secs(),
                Some(budget.div_ceil(1000).max(1))
            );
        }
        q.shutdown().unwrap();
    }
}
#[test]
fn sixty_four_boundary_nodes_reject_at_wi_plan_byte_bound_before_aq_admission() {
    let f = Fixture::new();
    let d = boundary_definition(&f, 64);
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let error = service.activate_flow_version(&(), d, None).unwrap_err();
    assert_eq!(error.downcast_ref::<CompileError>().unwrap().kind, ErrorKind::Bound);
    assert!(error.to_string().contains("record exceeds size bound"));
    assert_eq!(q.engine().projection().task_count(), 0);
    assert_eq!(f.connector.commits(), 0);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn full_signal_capacity_does_not_block_binding_or_flow_execution() {
    use actionqueue_core::{continuation::*, ids::*};
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let limits = actionqueue_core::limits::SignalLimits { identities: 1, ..Default::default() };
    let mut q =
        QueueEngine::bootstrap_with_signal_limits(&f.bundle, f.handler(), host(), limits).unwrap();
    // Fill the shared AQ budget before any Flow child is admitted.
    let request = |id: &str| {
        AdmitSignalRequest::new(
            SignalId::new(id).unwrap(),
            SignalNamespace::new("proof").unwrap(),
            SignalKind::new("capacity").unwrap(),
            Some(CorrelationId::new("capacity").unwrap()),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap()
    };
    // The host control path owns signal admission; bootstrap another session only
    // after releasing the engine, preserving the same persisted capacity usage.
    q.shutdown().unwrap();
    let mut engine = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: f.bundle.aq_path(),
            signal_limits: limits,
            ..Default::default()
        },
        f.handler(),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    engine.admit_signal(request("one")).unwrap();
    let e = engine.admit_signal(request("two")).unwrap_err();
    assert!(e.code() == "backpressure", "{e}");
    engine.shutdown().unwrap();
    q = QueueEngine::bootstrap_with_signal_limits(&f.bundle, f.handler(), host(), limits).unwrap();
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(f.connector.commits(), 1);
    assert_eq!(q.engine().projection().signals().statistics().retained, 1);
    q.shutdown().unwrap();
}
#[tokio::test]
async fn artifact_output_size_and_json_input_schema_are_enforced_on_actual_bytes() {
    for output in [true, false] {
        let mut f = Fixture::new();
        let schema = if output { "output:1" } else { "input:1" }.to_string().try_into().unwrap();
        f.config.0.schemas.insert(
            schema,
            if output {
                Schema::Artifact { max_bytes: 1, schema_hash: ContentHash::of(b"output:1") }
            } else {
                Schema::Object {
                    properties: BTreeMap::from([
                        (
                            "content".to_string().try_into().unwrap(),
                            Schema::String { max_bytes: 64 },
                        ),
                        (
                            "scope".to_string().try_into().unwrap(),
                            Schema::String { max_bytes: 256 },
                        ),
                        ("size".to_string().try_into().unwrap(), Schema::Integer {}),
                    ]),
                }
            },
        );
        let evidence = Evidence(f.snapshot.clone());
        let mut handler = f.handler();
        if output {
            handler.effect.port.outputs_override = Some(vec![b"true".to_vec()]);
        }
        let mut q = QueueEngine::bootstrap(&f.bundle, handler, host()).unwrap();
        let admitted =
            f.service(&evidence).submit_direct(&(), f.submission.clone(), f.selection(), &mut q);
        if !output {
            let error = admitted.unwrap_err();
            assert_eq!(error.downcast_ref::<CompileError>().unwrap().kind, ErrorKind::Schema);
            assert!(error.to_string().contains("artifact is not JSON"));
            assert_eq!(q.engine().projection().task_count(), 0);
            assert_eq!(f.connector.commits(), 0);
            q.shutdown().unwrap();
            continue;
        }
        let id = admitted.unwrap();
        f.drive(&mut q, &id).await;
        let r = f.run(&id);
        assert_ne!(r.outcome, Some(FlowOutcome::Succeeded));
        assert!(r.exports.is_empty());
        if output {
            assert_eq!(f.connector.commits(), 1);
            // A committed effect whose outputs cannot satisfy the plan cannot be hidden as Failed.
            assert_eq!(r.outcome, Some(FlowOutcome::Uncertain));
        } else {
            assert_eq!(f.connector.commits(), 0);
        }
        q.shutdown().unwrap();
    }
    // This descriptor profile has exactly one output port, so mixed artifact/JSON
    // boundary ports reject at compilation instead of skipping one port's schema.
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut d = boundary_definition(&f, 1);
    d.body.nodes[0].outputs.insert(
        "json".to_string().try_into().unwrap(),
        PlanOutput { schema: "bool".to_string().try_into().unwrap(), resources: vec![] },
    );
    assert_eq!(
        f.service(&evidence)
            .activate_flow_version(&(), d, None)
            .unwrap_err()
            .downcast_ref::<CompileError>()
            .unwrap()
            .kind,
        ErrorKind::Schema
    );
}
#[test]
fn runtime_compiler_rejects_connector_and_operation_authority_enlargement() {
    for connector in [true, false] {
        let f = Fixture::new();
        let mut config = f.config.clone();
        let mut descriptor = f.connector.descriptor.clone();
        if connector {
            descriptor.connector = "outside".to_string().try_into().unwrap();
        } else {
            descriptor.operation = "outside".to_string().try_into().unwrap();
        }
        config.0.descriptors.push(descriptor.clone());
        let context = config.0.context().unwrap();
        let mut d = boundary_definition(&f, 1);
        if let PlanOperation::Boundary { binding, .. } = &mut d.body.nodes[0].operation {
            *binding =
                Box::new(context.binding(&descriptor.connector, &descriptor.operation).unwrap());
        }
        let mut alternative = d.body.authority.alternatives[0].clone();
        alternative.connector = descriptor.connector;
        alternative.operation = descriptor.operation;
        d.body.authority.alternatives = vec![alternative.clone()];
        d.body.nodes[0].authority.alternatives = vec![alternative];
        let evidence = Evidence(f.snapshot.clone());
        let service = PlanService {
            root: f.bundle.wi(),
            authentication: &f.auth,
            evidence: &evidence,
            clock: &f.clock,
            configuration: &config,
        };
        let e = service.activate_flow_version(&(), d, None).unwrap_err();
        assert_eq!(e.downcast_ref::<CompileError>().unwrap().kind, ErrorKind::Authority);
        assert!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().runs.is_empty());
        assert_eq!(f.connector.commits(), 0);
    }
}
struct SessionAuth(Auth);
impl Authentication for SessionAuth {
    type Session = &'static str;
    fn authenticate(&self, session: &Self::Session) -> Result<(OpaqueRef, PrincipalId)> {
        if *session == "valid" {
            self.0.authenticate(&())
        } else {
            Err("forged session".into())
        }
    }
}
struct InvalidEvidence(Evidence, bool);
impl worldinterface_runtime::authority::EvidenceAuthority for InvalidEvidence {
    fn current_snapshot(
        &self,
        tenant: &OpaqueRef,
        submission: &EffectSubmission,
    ) -> Result<worldinterface_domain::authorization::AuthorizationSnapshot> {
        worldinterface_runtime::authority::EvidenceAuthority::current_snapshot(
            &self.0, tenant, submission,
        )
    }
    fn verify(&self, _: &OpaqueRef, _: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        if self.1 {
            Err("revoked evidence".into())
        } else {
            Ok(ContentHash::of(b"invalid document"))
        }
    }
}
#[test]
fn production_direct_and_flow_reject_forged_sessions_invalid_evidence_and_claims() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let auth = SessionAuth(f.auth.clone());
    let resolver = Resolver(f.snapshot.facts[0].clone());
    let service = worldinterface_runtime::egress::EffectService {
        root: f.bundle.wi(),
        authentication: &auth,
        evidence: &evidence,
        resolver: &resolver,
        clock: &f.clock,
    };
    let plans = PlanService {
        root: f.bundle.wi(),
        authentication: &auth,
        evidence: &evidence,
        clock: &f.clock,
        configuration: &f.config,
    };
    let d = boundary_definition(&f, 1);
    plans.activate_flow_version(&"valid", d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    assert!(service
        .submit_direct(&"forged", f.submission.clone(), f.selection(), &f.config, &mut q)
        .unwrap_err()
        .to_string()
        .contains("forged session"));
    assert!(plans
        .submit_flow(&"forged", &d.id, f.submission.clone(), &mut q)
        .unwrap_err()
        .to_string()
        .contains("forged session"));
    for revoked in [false, true] {
        let bad = InvalidEvidence(Evidence(f.snapshot.clone()), revoked);
        let service = worldinterface_runtime::egress::EffectService {
            root: f.bundle.wi(),
            authentication: &auth,
            evidence: &bad,
            resolver: &resolver,
            clock: &f.clock,
        };
        let plans = PlanService {
            root: f.bundle.wi(),
            authentication: &auth,
            evidence: &bad,
            clock: &f.clock,
            configuration: &f.config,
        };
        assert!(service
            .submit_direct(&"valid", f.submission.clone(), f.selection(), &f.config, &mut q)
            .is_err());
        assert!(plans.submit_flow(&"valid", &d.id, f.submission.clone(), &mut q).is_err());
    }
    for field in ["principal", "actor", "capacity"] {
        let mut forged = f.submission.clone();
        match field {
            "principal" => forged.requested_accountable_principal = Some(PrincipalId::generate()),
            "actor" => forged.submitted_by = Some(ActorId::generate()),
            _ => {
                let mut capacity = f.snapshot.bindings[0].capacity.clone();
                capacity.assignment_ref = "forged-capacity".to_string().try_into().unwrap();
                forged.acting_capacity = Some(capacity);
            }
        }
        assert!(
            service
                .submit_direct(&"valid", forged.clone(), f.selection(), &f.config, &mut q)
                .is_err(),
            "{field}"
        );
        assert!(plans.submit_flow(&"valid", &d.id, forged, &mut q).is_err(), "{field}");
    }
    assert_eq!(q.engine().projection().task_count(), 0);
    assert!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().runs.is_empty());
    // Untrusted authority claims and a changed request cannot replace the authenticated admission.
    let id = service
        .submit_direct(&"valid", f.submission.clone(), f.selection(), &f.config, &mut q)
        .unwrap();
    let before = f.run(&id);
    let mut conflict = f.submission.clone();
    conflict.purpose = Some("forged-purpose".to_string().try_into().unwrap());
    assert!(service.submit_direct(&"valid", conflict, f.selection(), &f.config, &mut q).is_err());
    assert_eq!(f.run(&id), before);
    assert_eq!(q.engine().projection().task_count(), 1);
    assert_eq!(f.connector.commits(), 0);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn flow_journal_rejects_invalid_artifacts_and_preserves_finished_admission() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    let initial = f.run(&id);
    for foreign in [false, true] {
        let mut bad = initial.clone();
        if foreign {
            bad.submission.input.artifact.scope = "foreign".to_string().try_into().unwrap();
        } else {
            bad.submission.input.artifact.content = ContentHash::of(b"missing");
        }
        bad.admission_hash = bad.admission_digest().unwrap();
        let rev = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().revision;
        let e = f
            .bundle
            .wi()
            .flow_transact(&f.auth.0, rev, FlowCommand::AdmitRun { run: Box::new(bad) })
            .unwrap_err();
        assert!(
            !e.to_string().contains("Flow run conflict"),
            "must reject artifact before journaling: {e}"
        );
        assert_eq!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().revision, rev);
    }
    f.drive(&mut q, &id).await;
    let finished = f.run(&id);
    let rev = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().revision;
    assert_eq!(
        f.bundle
            .wi()
            .flow_transact(&f.auth.0, 0, FlowCommand::AdmitRun { run: Box::new(initial) })
            .unwrap()
            .runs[&id],
        finished
    );
    let node = finished.node_tasks.keys().next().unwrap().clone();
    for command in [
        FlowCommand::BindChild {
            run: id.clone(),
            node: node.clone(),
            binding: finished.bindings[&node].clone(),
        },
        FlowCommand::WakeDelivered { run: id.clone(), node: node.clone(), sequence: 1 },
        FlowCommand::RecordNodeResult {
            run: id.clone(),
            node: node.clone(),
            result: finished.results[&node].clone(),
        },
    ] {
        assert!(f
            .bundle
            .wi()
            .flow_transact(&f.auth.0, rev, command)
            .unwrap_err()
            .to_string()
            .contains("Flow run finished"));
    }
    // Missing outputs/exports fail before they can poison append-only history.
    let mut result = finished.results[&node].clone();
    result.outputs.values_mut().next().unwrap().content = ContentHash::of(b"missing-output");
    assert!(!f
        .bundle
        .wi()
        .flow_transact(
            &f.auth.0,
            rev,
            FlowCommand::RecordNodeResult { run: id.clone(), node, result }
        )
        .unwrap_err()
        .to_string()
        .contains("Flow run finished"));
    let missing = ArtifactRef {
        scope: f.auth.0.clone(),
        content: ContentHash::of(b"missing-export"),
        size: 14,
    };
    assert!(!f
        .bundle
        .wi()
        .flow_transact(
            &f.auth.0,
            rev,
            FlowCommand::Finish {
                run: id,
                outcome: FlowOutcome::Succeeded,
                exports: BTreeMap::from([("bad".to_string().try_into().unwrap(), missing)])
            }
        )
        .unwrap_err()
        .to_string()
        .contains("Flow outcome conflict"));
    assert_eq!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().revision, rev);
    q.shutdown().unwrap();
}
// Issue exact-demand credentials per submission; large policy whitelists are
// deliberately absent from both the host and the persisted admission pins.
#[derive(Clone)]
struct CapacityEvidence(Evidence);
impl worldinterface_runtime::authority::EvidenceAuthority for CapacityEvidence {
    fn current_snapshot(
        &self,
        tenant: &OpaqueRef,
        submission: &EffectSubmission,
    ) -> Result<worldinterface_domain::authorization::AuthorizationSnapshot> {
        let mut snapshot = self.0.current_snapshot(tenant, submission)?;
        let demand: std::collections::BTreeSet<ContentHash> =
            [worldinterface_domain::ingress::digest(submission)?].into();
        for b in &mut snapshot.bindings {
            b.principal_authority = demand.clone();
            b.capacity_authority = demand.clone();
        }
        snapshot.policy.platform = demand.clone();
        snapshot.policy.resource = demand.clone();
        snapshot.policy.information_use = demand.clone();
        for g in &mut snapshot.grants {
            g.authority = demand.clone();
        }
        Ok(snapshot)
    }
    fn verify(
        &self,
        tenant: &OpaqueRef,
        evidence: &EvidenceRef,
        at: &Timestamp,
    ) -> Result<ContentHash> {
        self.0.verify(tenant, evidence, at)
    }
}

fn next_count_test_run(f: &Fixture, template: &FlowRun, label: &str) -> FlowRun {
    let mut r = template.clone();
    r.id = label.to_string().try_into().unwrap();
    r.submission.client_request_id = label.to_string().try_into().unwrap();
    let p = compile_direct(&r.submission, &f.selection(), &r.pins.context().unwrap()).unwrap();
    r.plan = p.canonical_bytes().to_vec();
    r.plan_hash = p.hash().clone();
    r.origin = p.document().origin.clone();
    r.coordinator_task = TaskId::new().to_string().try_into().unwrap();
    for task in r.node_tasks.values_mut() {
        *task = TaskId::new().to_string().try_into().unwrap();
    }
    r.admission_hash = r.admission_digest().unwrap();
    r
}

#[tokio::test]
async fn flow_retained_count_rollback_replay_and_tampering() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    q.shutdown().unwrap();
    let root = f.bundle.wi();
    let original = root.flow_catalog(&f.auth.0).unwrap();
    assert_eq!(original.retained_run_count, 1);
    let r = next_count_test_run(&f, &f.run(&id), "count:next");
    let command = FlowCommand::AdmitRun { run: Box::new(r.clone()) };
    let path = f._temp.path().join("bundle/wi/flow.sqlite");
    let c = rusqlite::Connection::open(&path).unwrap();
    c.execute_batch("CREATE TRIGGER count_failure BEFORE UPDATE ON flow_state BEGIN SELECT RAISE(ABORT,'count rollback'); END;").unwrap();
    assert!(root.flow_update(&f.auth.0, original.revision, command.clone()).is_err());
    assert_eq!(root.flow_catalog(&f.auth.0).unwrap(), original);
    assert!(!root.flow_snapshot(&f.auth.0).unwrap().runs.contains_key(&r.id));
    c.execute_batch("DROP TRIGGER count_failure").unwrap();
    // The failed transaction did not reserve an identity or advance the count.
    let admitted = root.flow_update(&f.auth.0, original.revision, command.clone()).unwrap();
    assert_eq!(admitted.retained_run_count, 2);
    assert_eq!(root.flow_update(&f.auth.0, original.revision, command).unwrap(), admitted);
    let mut changed = r.clone();
    changed.wait_id = "changed-wait".to_string().try_into().unwrap();
    changed.admission_hash = changed.admission_digest().unwrap();
    assert!(root
        .flow_update(&f.auth.0, admitted.revision, FlowCommand::AdmitRun { run: Box::new(changed) })
        .is_err());
    let canceled = root
        .flow_update(
            &f.auth.0,
            admitted.revision,
            FlowCommand::CancelRequested { run: r.id.clone() },
        )
        .unwrap();
    let finished = root
        .flow_update(
            &f.auth.0,
            canceled.revision,
            FlowCommand::Finish {
                run: r.id.clone(),
                outcome: FlowOutcome::Canceled,
                exports: BTreeMap::new(),
            },
        )
        .unwrap();
    assert_eq!(finished.retained_run_count, 2);
    let before = root.flow_snapshot(&f.auth.0).unwrap();
    root.repository(worldinterface_storage::StoreKind::Flow).unwrap().rebuild().unwrap();
    assert_eq!(root.flow_snapshot(&f.auth.0).unwrap(), before);
    // Foundation open verifies its complete journal independently of AQ bindings.
    let wi_path = f._temp.path().join("bundle/wi");
    let reopened = worldinterface_storage::StoreRoot::open_existing(&wi_path).unwrap();
    assert_eq!(reopened.flow_snapshot(&f.auth.0).unwrap(), before);
    drop(reopened);
    let mut catalog = root.flow_catalog(&f.auth.0).unwrap();
    catalog.retained_run_count -= 1;
    c.execute(
        "UPDATE flow_state SET state=? WHERE tenant=?",
        rusqlite::params![canonical_collection(&catalog).unwrap(), f.auth.0.as_str()],
    )
    .unwrap();
    drop(c);
    let bytes = std::fs::read(&path).unwrap();
    assert!(worldinterface_storage::StoreRoot::open_existing(&wi_path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[tokio::test]
async fn concurrent_flow_admissions_serialize_at_retained_capacity() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    q.shutdown().unwrap();
    let wi_path = f._temp.path().join("bundle/wi");
    let handles: Vec<_> = (0..2)
        .map(|_| worldinterface_storage::StoreRoot::open_existing(&wi_path).unwrap())
        .collect();
    let template = f.run(&id);
    let runs = [
        next_count_test_run(&f, &template, "race:a"),
        next_count_test_run(&f, &template, "race:b"),
    ];
    // Isolate the final-slot race with a synthetic catalog count. The capacity
    // proof below separately uses all 10,000 real production admissions.
    let mut catalog = f.bundle.wi().flow_catalog(&f.auth.0).unwrap();
    catalog.retained_run_count = MAX_FLOW_RUNS - 1;
    let c = rusqlite::Connection::open(wi_path.join("flow.sqlite")).unwrap();
    c.execute(
        "UPDATE flow_state SET state=? WHERE tenant=?",
        rusqlite::params![canonical_collection(&catalog).unwrap(), f.auth.0.as_str()],
    )
    .unwrap();
    drop(c);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let threads: Vec<_> = handles
        .into_iter()
        .zip(runs)
        .map(|(root, run)| {
            let tenant = f.auth.0.clone();
            let barrier = barrier.clone();
            let revision = catalog.revision;
            std::thread::spawn(move || {
                barrier.wait();
                root.flow_update(&tenant, revision, FlowCommand::AdmitRun { run: Box::new(run) })
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(results.iter().find_map(|r| r.as_ref().err()).unwrap().is::<FlowCapacity>());
    let state = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap();
    assert_eq!(state.retained_run_count, MAX_FLOW_RUNS);
    assert_eq!(state.revision, catalog.revision + 1);
    assert_eq!(state.runs.len(), 2);
}

#[tokio::test]
async fn flow_capacity_is_enforced_without_losing_existing_history() {
    let started = std::time::Instant::now();
    let mut submitting = std::time::Duration::ZERO;
    let mut canceling = std::time::Duration::ZERO;
    let mut reconciling = std::time::Duration::ZERO;
    let mut last_batch = (started, submitting, canceling, reconciling);
    let mut f = Fixture::new();
    let evidence = CapacityEvidence(Evidence(f.snapshot.clone()));
    let handler = |f: &Fixture| {
        let h = f.handler();
        FlowAttemptHandler {
            effect: EffectAttemptHandler {
                bundle: f.bundle.clone(),
                port: h.effect.port,
                evidence: evidence.clone(),
                clock: h.effect.clock,
            },
            resolver: h.resolver,
            artifacts: h.artifacts,
        }
    };
    let mut q = QueueEngine::bootstrap(&f.bundle, handler(&f), host()).unwrap();
    let mut successes = Vec::new();
    let mut canceled = None;
    let mut last_submission = f.submission.clone();
    // All 10,000 admissions go through the authenticated production facade.
    // Mix completed external effects with host cancellation; retain every run,
    // receipt and request identity in the same bundle throughout.
    for i in 1..=MAX_FLOW_RUNS {
        let mut submission = f.submission.clone();
        submission.client_request_id = format!("direct-capacity:{i}").try_into().unwrap();
        let resolver = Resolver(f.snapshot.facts[0].clone());
        let service = worldinterface_runtime::egress::EffectService {
            root: f.bundle.wi(),
            authentication: &f.auth,
            evidence: &evidence,
            resolver: &resolver,
            clock: &f.clock,
        };
        let before_submit = std::time::Instant::now();
        let id = service
            .submit_direct(&(), submission.clone(), f.selection(), &f.config, &mut q)
            .unwrap();
        submitting += before_submit.elapsed();
        if [1, 256, 257, 5000, 9999, 10000].contains(&i) {
            // Leave the final admitted run unfinished across a full WI/AQ reopen.
            if i == MAX_FLOW_RUNS {
                eprintln!("capacity paired reopen starting at {:?}", started.elapsed());
                q.shutdown().unwrap();
                let session = actionqueue_storage::store::open_store(
                    &f.bundle.aq_path(),
                    actionqueue_storage::store::OpenOptions::ReadOnly,
                )
                .unwrap();
                assert!(
                    session.snapshot_path().is_file(),
                    "default automatic checkpoint must be present at capacity"
                );
                drop(session);
                f.bundle = Arc::new(StoreBundle::open(&f._temp.path().join("bundle")).unwrap());
                eprintln!("capacity paired store validation finished at {:?}", started.elapsed());
                q = QueueEngine::bootstrap(&f.bundle, handler(&f), host()).unwrap();
                eprintln!("capacity paired reopen finished at {:?}", started.elapsed());
            }
            f.drive(&mut q, &id).await;
            let run = f.run(&id);
            let parent: TaskId = run.coordinator_task.as_str().parse().unwrap();
            assert_eq!(
                run.outcome,
                Some(FlowOutcome::Succeeded),
                "Direct request {i}: parent history {:?}; Flow results {:?}",
                q.engine()
                    .projection()
                    .get_attempt_history(&q.engine().projection().run_ids_for_task(parent)[0]),
                run.results
            );
            assert_eq!(run.results.len(), 1);
            let effect = run.results.values().next().unwrap().effect.as_ref().unwrap();
            assert_eq!(f.bundle.wi().effect_receipts(&f.auth.0, effect).unwrap().len(), 1);
            successes.push(run);
        } else {
            let before_cancel = std::time::Instant::now();
            let run = f.run(&id);
            q.cancel_task(run.coordinator_task.as_str().parse().unwrap()).unwrap();
            canceling += before_cancel.elapsed();
            if canceled.is_none() {
                canceled = Some(id.clone());
            }
        }
        // Reconcile bounded batches through the real scheduling path.
        if i % 100 == 0 {
            let before_tick = std::time::Instant::now();
            let _ = q.tick().await.unwrap();
            reconciling += before_tick.elapsed();
        }
        last_submission = submission;
        if i == 257 {
            q.shutdown().unwrap();
            f.bundle = Arc::new(StoreBundle::open(&f._temp.path().join("bundle")).unwrap());
            q = QueueEngine::bootstrap(&f.bundle, handler(&f), host()).unwrap();
        }
        if i % 1000 == 0 {
            eprintln!(
                "production Direct capacity: {i}/{} at {:?} (submit {submitting:?}, cancel {canceling:?}, tick {reconciling:?})",
                MAX_FLOW_RUNS,
                started.elapsed()
            );
            eprintln!(
                "capacity batch {}: wall {:?}, submit {:?}, cancel {:?}, tick {:?}",
                i / 1000,
                last_batch.0.elapsed(),
                submitting - last_batch.1,
                canceling - last_batch.2,
                reconciling - last_batch.3
            );
            last_batch = (std::time::Instant::now(), submitting, canceling, reconciling);
        }
    }
    assert_eq!(f.bundle.wi().flow_catalog(&f.auth.0).unwrap().retained_run_count, MAX_FLOW_RUNS);
    assert_eq!(f.connector.commits(), successes.len());
    let before = f.bundle.wi().flow_catalog(&f.auth.0).unwrap().revision;
    let resolver = Resolver(f.snapshot.facts[0].clone());
    let service = worldinterface_runtime::egress::EffectService {
        root: f.bundle.wi(),
        authentication: &f.auth,
        evidence: &evidence,
        resolver: &resolver,
        clock: &f.clock,
    };
    let last = service
        .submit_direct(&(), last_submission.clone(), f.selection(), &f.config, &mut q)
        .unwrap();
    assert_eq!(last, successes.last().unwrap().id);
    let mut conflict = last_submission.clone();
    conflict.budget.timeout_millis += 1;
    assert!(service
        .submit_direct(&(), conflict, f.selection(), &f.config, &mut q)
        .unwrap_err()
        .to_string()
        .contains("Flow request conflict"));
    last_submission.client_request_id = "capacity:overflow".to_string().try_into().unwrap();
    let error =
        service.submit_direct(&(), last_submission, f.selection(), &f.config, &mut q).unwrap_err();
    assert!(error.is::<FlowCapacity>(), "{error}");
    assert_eq!(f.bundle.wi().flow_catalog(&f.auth.0).unwrap().revision, before);
    assert!(f.bundle.wi().flow_pending_snapshot(&f.auth.0).unwrap().runs.is_empty());
    let state = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap();
    assert_eq!(state.retained_run_count, MAX_FLOW_RUNS);
    assert_eq!(state.runs.len(), MAX_FLOW_RUNS);
    assert_eq!(state.requests.len(), MAX_FLOW_RUNS);
    drop(state);
    // Rebuild while the engine is quiescent: all AQ tasks are terminal and no
    // handler can write. The full paired reopen above already verified 10,000
    // retained admissions with the final request pending. Keep its live AQ
    // projection to verify production idempotency after rebuilding WI history.
    {
        eprintln!("capacity history rebuild starting at {:?}", started.elapsed());
        let _guard = f.bundle.wi().flow_dispatch_guard().unwrap();
        f.bundle
            .wi()
            .repository(worldinterface_storage::StoreKind::Flow)
            .unwrap()
            .rebuild()
            .unwrap();
        eprintln!("capacity history rebuild finished at {:?}", started.elapsed());
    }
    assert_eq!(f.bundle.wi().flow_catalog(&f.auth.0).unwrap().retained_run_count, MAX_FLOW_RUNS);
    for run in &successes {
        assert_eq!(&f.run(&run.id), run);
    }
    assert_eq!(f.run(canceled.as_ref().unwrap()).outcome, Some(FlowOutcome::Canceled));
    let canceled_run = f.run(canceled.as_ref().unwrap());
    let resolver = Resolver(f.snapshot.facts[0].clone());
    let service = worldinterface_runtime::egress::EffectService {
        root: f.bundle.wi(),
        authentication: &f.auth,
        evidence: &evidence,
        resolver: &resolver,
        clock: &f.clock,
    };
    for run in successes.iter().chain(std::iter::once(&canceled_run)) {
        assert_eq!(
            service
                .submit_direct(&(), run.submission.clone(), f.selection(), &f.config, &mut q)
                .unwrap(),
            run.id
        );
    }
    assert_eq!(q.engine().projection().admissions().count(), MAX_FLOW_RUNS + successes.len());
    assert_eq!(
        f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst) as usize,
        successes.len()
    );
    q.shutdown().unwrap();
}

#[tokio::test]
async fn forged_grant_and_approval_claims_do_not_authorize_production_direct_or_flow() {
    for flow in [false, true] {
        for approval in [false, true] {
            let mut f = Fixture::new();
            if approval {
                f.submission
                    .approval_attestations
                    .push("forged-approval".to_string().try_into().unwrap());
            } else {
                f.submission
                    .requested_authority_refs
                    .push("forged-grant".to_string().try_into().unwrap());
            }
            let submission = f.submission.clone();
            allow_demands(&mut f, &[submission]);
            let evidence = Evidence(f.snapshot.clone());
            let resolver = Resolver(f.snapshot.facts[0].clone());
            let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
            let id = if flow {
                let d = boundary_definition(&f, 1);
                let service = f.service(&evidence);
                service.activate_flow_version(&(), d.clone(), None).unwrap();
                service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap()
            } else {
                let service = worldinterface_runtime::egress::EffectService {
                    root: f.bundle.wi(),
                    authentication: &f.auth,
                    evidence: &evidence,
                    resolver: &resolver,
                    clock: &f.clock,
                };
                service
                    .submit_direct(&(), f.submission.clone(), f.selection(), &f.config, &mut q)
                    .unwrap()
            };
            f.drive(&mut q, &id).await;
            assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Denied));
            assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            q.shutdown().unwrap();
        }
    }
}

struct FinishGate<H> {
    inner: H,
    hook: Arc<dyn Fn() + Send + Sync>,
}
impl<H: actionqueue_executor_local::handler::ExecutorHandler>
    actionqueue_executor_local::handler::ExecutorHandler for FinishGate<H>
{
    fn execute(
        &self,
        ctx: actionqueue_executor_local::handler::ExecutorContext,
    ) -> actionqueue_core::disposition::AttemptDisposition {
        let payload: serde_json::Value = serde_json::from_slice(&ctx.input.payload).unwrap();
        if payload.get("node").is_none() && ctx.input.resume_context.is_some() {
            (self.hook)();
        }
        self.inner.execute(ctx)
    }
}
#[tokio::test]
async fn journal_finish_rejects_substituted_exports_and_stale_definition_admission() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = branching();
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let latch = Latch::new();
    let handler = FinishGate { inner: f.handler(), hook: latch.hook(entered.clone()) };
    let mut q = QueueEngine::bootstrap(&f.bundle, handler, host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    let initial = f.run(&id);
    let next = d.revise(d.body.clone()).unwrap();
    service.activate_flow_version(&(), next, Some(1)).unwrap();
    let mut stale = initial.clone();
    stale.id = uuid::Uuid::new_v4().to_string().try_into().unwrap();
    stale.submission.client_request_id = "stale".to_string().try_into().unwrap();
    stale.admission_hash = stale.admission_digest().unwrap();
    let revision = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().revision;
    assert!(f
        .bundle
        .wi()
        .flow_transact(&f.auth.0, revision, FlowCommand::AdmitRun { run: Box::new(stale) })
        .unwrap_err()
        .to_string()
        .contains("inactive Flow definition"));
    for _ in 0..2000 {
        let _ = q.tick().await.unwrap();
        if entered.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
    let state = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap();
    let r = &state.runs[&id];
    assert!(r.outcome.is_none());
    assert_eq!(r.results.len(), 4);
    let wrong = f.bundle.wi().publish(f.auth.0.clone(), b"9").unwrap();
    assert!(f
        .bundle
        .wi()
        .flow_transact(
            &f.auth.0,
            state.revision,
            FlowCommand::Finish {
                run: id.clone(),
                outcome: FlowOutcome::Succeeded,
                exports: BTreeMap::from([("answer".to_string().try_into().unwrap(), wrong)])
            }
        )
        .unwrap_err()
        .to_string()
        .contains("export value mismatch"));
    let node = r.bindings.keys().next().unwrap().clone();
    let mut binding = r.bindings[&node].clone();
    binding.request.content = ContentHash::of(b"missing-binding");
    assert!(!f
        .bundle
        .wi()
        .flow_transact(
            &f.auth.0,
            state.revision,
            FlowCommand::BindChild { run: id.clone(), node, binding }
        )
        .unwrap_err()
        .to_string()
        .contains("binding conflict"));
    assert_eq!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().revision, state.revision);
    drop(latch);
    for _ in 0..2000 {
        let _ = q.tick().await.unwrap();
        if f.run(&id).outcome.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    q.shutdown().unwrap();
}

#[tokio::test]
async fn outcome_recording_error_marks_node_uncertain_and_blocks_dependents() {
    let mut f = Fixture::new();
    let mut d = boundary_definition(&f, 2);
    d.body.nodes[1].dependencies = vec![d.body.nodes[0].id.clone()];
    d.body.entries.truncate(1);
    let mut submissions = vec![f.submission.clone()];
    for n in &d.body.nodes {
        let mut s = f.submission.clone();
        s.client_request_id =
            worldinterface_runtime::flow::node_request_id(&s.client_request_id, &n.id).unwrap();
        submissions.push(s);
    }
    allow_demands(&mut f, &submissions);
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut handler = f.handler();
    let path = f._temp.path().join("bundle/wi/fabric.sqlite");
    handler.effect.port.after_invoke = Some(Arc::new(move || {
        rusqlite::Connection::open(&path).unwrap().execute_batch("CREATE TRIGGER fail_flow_outcome BEFORE INSERT ON effect_history WHEN CAST(NEW.entry AS TEXT) LIKE '%\"operation\":\"outcome\"%' BEGIN SELECT RAISE(ABORT,'injected outcome recording error'); END;").unwrap();
    }));
    let mut q = QueueEngine::bootstrap(&f.bundle, handler, host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    f.drive(&mut q, &id).await;
    let r = f.run(&id);
    assert_eq!(r.outcome, Some(FlowOutcome::Uncertain));
    assert_eq!(r.results[&d.body.nodes[0].id].disposition, NodeDisposition::Uncertain);
    assert!(!r.results.contains_key(&d.body.nodes[1].id));
    assert_eq!(
        q.engine()
            .projection()
            .task_terminal_status(r.node_tasks[&d.body.nodes[1].id].as_str().parse().unwrap()),
        Some(actionqueue_core::continuation::TaskTerminalStatus::Canceled)
    );
    assert_eq!(f.connector.commits(), 1);
    assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn rejected_aq_cancel_does_not_mutate_flow_and_authorized_execution_continues() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    let before = f.run(&id);
    q.set_host_control_context(None);
    assert!(q.cancel_task(before.coordinator_task.as_str().parse().unwrap()).is_err());
    assert_eq!(f.run(&id), before);
    q.set_host_control_context(Some(host()));
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(f.connector.commits(), 1);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn exported_json_schema_validates_artifact_bytes_instead_of_reference_metadata() {
    for valid in [false, true] {
        let mut f = Fixture::new();
        let mut d = branching();
        let artifact = if valid {
            f.bundle.wi().publish(f.auth.0.clone(), b"true").unwrap()
        } else {
            f.submission.input.artifact.clone()
        };
        let schema: OpaqueRef = "export".to_string().try_into().unwrap();
        f.config.0.schemas.insert(
            schema.clone(),
            if valid {
                Schema::Boolean {}
            } else {
                Schema::Object {
                    properties: BTreeMap::from([
                        (
                            "content".to_string().try_into().unwrap(),
                            Schema::String { max_bytes: 64 },
                        ),
                        (
                            "scope".to_string().try_into().unwrap(),
                            Schema::String { max_bytes: 256 },
                        ),
                        ("size".to_string().try_into().unwrap(), Schema::Integer {}),
                    ]),
                }
            },
        );
        d.body.outputs = BTreeMap::from([(
            "export".to_string().try_into().unwrap(),
            PlanInput { schema, source: InputSource::Artifact { reference: artifact.clone() } },
        )]);
        let evidence = Evidence(f.snapshot.clone());
        let service = f.service(&evidence);
        service.activate_flow_version(&(), d.clone(), None).unwrap();
        let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
        f.drive(&mut q, &id).await;
        let r = f.run(&id);
        if valid {
            assert_eq!(r.outcome, Some(FlowOutcome::Succeeded));
            assert_eq!(r.exports.values().next(), Some(&artifact));
        } else {
            assert_eq!(r.outcome, Some(FlowOutcome::Failed));
            assert!(r.exports.is_empty());
        }
        assert_eq!(f.connector.invocation_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        q.shutdown().unwrap();
    }
}

#[tokio::test]
async fn temporary_binding_artifact_io_failure_retries_without_quarantine() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    for _ in 0..2000 {
        let _ = q.tick().await.unwrap();
        if !f.run(&id).bindings.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let run = f.run(&id);
    let binding = run.bindings.values().next().expect("bound child");
    let path = f._temp.path().join("bundle/wi/artifacts").join(binding.request.content.as_str());
    let hidden = f._temp.path().join("temporarily-unavailable-request");
    std::fs::rename(&path, &hidden).unwrap();
    let _ = q.tick().await.unwrap();
    assert!(f.bundle.wi().flow_run_snapshot(&f.auth.0, &id).unwrap().quarantined.is_empty());
    std::fs::rename(&hidden, &path).unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(f.connector.commits(), 1);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn quarantine_control_failure_surfaces_and_retries_after_host_repair() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = f
        .service(&evidence)
        .submit_direct(&(), f.submission.clone(), f.selection(), &mut q)
        .unwrap();
    let mut run = f.run(&id);
    run.plan_hash = ContentHash::of(b"damaged");
    let db = rusqlite::Connection::open(f._temp.path().join("bundle/wi/flow.sqlite")).unwrap();
    db.execute(
        "UPDATE flow_runs SET state=? WHERE tenant=? AND id=?",
        rusqlite::params![canonical_collection(&run).unwrap(), f.auth.0.as_str(), id.as_str()],
    )
    .unwrap();
    q.set_host_control_context(None);
    assert!(q.tick().await.is_err());
    assert!(f.bundle.wi().flow_run_snapshot(&f.auth.0, &id).unwrap().quarantined.contains_key(&id));
    q.set_host_control_context(Some(host()));
    let _ = q.tick().await.unwrap();
    let task = run.coordinator_task.as_str().parse().unwrap();
    assert!(q
        .engine()
        .projection()
        .runs_for_task(task)
        .all(|r| r.state() == actionqueue_core::run::RunState::Canceled));
    assert_eq!(f.connector.commits(), 0);
    q.shutdown().unwrap();
}
