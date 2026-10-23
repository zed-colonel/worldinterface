#![allow(dead_code)]
use super::connectors::*;
use actionqueue_core::ids::TaskId;
use std::{collections::BTreeMap, sync::Arc};
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
pub(crate) struct Auth(pub OpaqueRef, pub PrincipalId);
impl Authentication for Auth {
    type Session = ();
    fn authenticate(&self, _: &()) -> Result<(OpaqueRef, PrincipalId)> {
        Ok((self.0.clone(), self.1.clone()))
    }
}
#[derive(Clone)]
pub(crate) struct Config(pub FlowPins);
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
pub(crate) struct Resolver(pub ResolutionFact);
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
pub(crate) struct Fixture {
    pub(crate) _temp: tempfile::TempDir,
    pub(crate) bundle: Arc<StoreBundle>,
    pub(crate) connector: DeterministicConnector,
    pub(crate) snapshot: worldinterface_domain::authorization::AuthorizationSnapshot,
    pub(crate) submission: EffectSubmission,
    pub(crate) clock: Clock,
    pub(crate) config: Config,
    pub(crate) auth: Auth,
}
impl Fixture {
    pub(crate) fn new() -> Self {
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
    pub(crate) fn handler(
        &self,
    ) -> FlowAttemptHandler<DeterministicConnector, Evidence, Resolver, Clock> {
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
    pub(crate) fn service<'a>(
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
    pub(crate) fn direct(&self) -> ExecutionPlan {
        compile_direct(&self.submission, &self.selection(), &self.config.0.context().unwrap())
            .unwrap()
    }
    pub(crate) fn selection(&self) -> DirectOperationSelection {
        DirectOperationSelection {
            connector: self.connector.descriptor.connector.clone(),
            operation: self.connector.descriptor.operation.clone(),
        }
    }
    pub(crate) fn run(&self, id: &OpaqueRef) -> FlowRun {
        self.bundle.wi().flow_run_snapshot(&self.auth.0, id).unwrap().runs[id].clone()
    }
    pub(crate) async fn drive<
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
pub(crate) fn pure_node(
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
pub(crate) fn allow_demands(f: &mut Fixture, submissions: &[EffectSubmission]) {
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
