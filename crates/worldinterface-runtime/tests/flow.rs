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
        Self { _temp: temp, bundle, connector, snapshot, submission, clock, config, auth }
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
        self.bundle.wi().flow_snapshot(&self.auth.0).unwrap().runs[id].clone()
    }
    async fn drive(
        &self,
        queue: &mut QueueEngine<
            '_,
            FlowAttemptHandler<DeterministicConnector, Evidence, Resolver, Clock>,
        >,
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
    for kind in ["bytes", "hash", "snapshot", "principal", "definition"] {
        let mut bad = r.clone();
        match kind {
            "bytes" => bad.plan[0] ^= 1,
            "hash" => bad.plan_hash = ContentHash::of(b"tamper"),
            "snapshot" => bad.pins.descriptors[0].implementation = ContentHash::of(b"replacement"),
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
        assert!(bad.restore().is_err(), "{kind}");
    }
    let mut state = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap();
    state.runs.get_mut(&id).unwrap().plan_hash = ContentHash::of(b"wrong persisted expected hash");
    let connection =
        rusqlite::Connection::open(f._temp.path().join("bundle/wi/flow.sqlite")).unwrap();
    connection
        .execute(
            "UPDATE flow_state SET state=? WHERE tenant=?",
            rusqlite::params![canonical_collection(&state).unwrap(), f.auth.0.as_str()],
        )
        .unwrap();
    drop(connection);
    assert!(q.tick().await.is_err());
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
    let d = branching();
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
    assert!(service.activate_flow_version(&(), d, None).is_err());
    let mut d = branching();
    if let PlanOperation::Branch { program, .. } = &mut d.body.nodes[0].operation {
        program.requested_access.push("network".to_string().try_into().unwrap());
    }
    assert!(service.activate_flow_version(&(), d, None).is_err());
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
    assert!(service.activate_flow_version(&(), too_large, None).is_err());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    f.drive(&mut q, &id).await;
    let r = f.run(&id);
    assert_eq!(r.outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(r.bindings.len(), 64);
    assert_eq!(r.results.len(), 64);
    let parent: TaskId = r.coordinator_task.as_str().parse().unwrap();
    assert!(q.engine().projection().parent_child_mappings().all(|(_, p)| p == parent));
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
        s.client_request_id =
            format!("{}:node:{node}", s.client_request_id.as_str()).try_into().unwrap();
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
