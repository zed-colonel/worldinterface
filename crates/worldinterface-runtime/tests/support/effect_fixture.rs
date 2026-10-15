use actionqueue_core::{
    bounded::OpaqueRef as AqRef,
    causal::ControlMutationContext,
    control::{ControlScope, HostControlContext},
    disposition::AttemptDisposition,
    ids::RunId,
};
use actionqueue_executor_local::handler::{CancellationToken, ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
use std::sync::Arc;
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::{AuthorizationSnapshot, VerifiedSnapshot},
    effect::{CommitObservation, EffectAction},
};
use worldinterface_egress::Egress;
use worldinterface_protocol::*;
use worldinterface_runtime::{
    admission,
    aq_adapter::QueueEngine,
    authority::{EvidenceAuthority, TrustedClock},
    completion::{
        BoundExecution, EffectAttemptHandler, EffectExecutionPort, EvidenceBytes, PortObservation,
    },
    run_discovery,
    storage::StoreBundle,
    Result,
};

pub(crate) fn at() -> Timestamp {
    "2026-09-21T00:00:00.000000000Z".to_owned().try_into().unwrap()
}
pub(crate) fn fixture() -> (EffectSubmission, AuthorizationSnapshot) {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/egress/fixture.json"
    ))
    .unwrap();
    (
        serde_json::from_value(data["submission"].clone()).unwrap(),
        serde_json::from_value(data["snapshot"].clone()).unwrap(),
    )
}
pub(crate) fn caller(snapshot: &AuthorizationSnapshot) -> AuthenticatedCaller {
    AuthenticatedCaller::from_trusted_authentication(
        snapshot.bindings[0].tenant.clone(),
        snapshot.bindings[0].principal.clone(),
    )
}
pub(crate) fn verified(snapshot: &AuthorizationSnapshot) -> VerifiedSnapshot {
    VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap()
}
pub(crate) fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(AqRef::new("trusted-host").unwrap()),
    }
}
struct Noop;
impl ExecutorHandler for Noop {
    fn execute(&self, _: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
pub(crate) fn authorize(
    bundle: &StoreBundle,
    submission: EffectSubmission,
    snapshot: &AuthorizationSnapshot,
) -> EffectIntentId {
    let actor = caller(snapshot);
    for name in [
        "input",
        "executable",
        "dependency",
        "configuration",
        "profile-definition",
        "adapter",
        "resolution",
        "grant",
        "fixture-review",
        "worker",
    ] {
        bundle.wi().publish(actor.tenant().clone(), name.as_bytes()).unwrap();
    }
    let egress = Egress::new(bundle.wi());
    let id = egress
        .apply(
            &actor,
            at(),
            EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot: verified(snapshot),
                link: None,
            },
        )
        .unwrap();
    for action in [
        EffectAction::Resolve { id: id.clone(), snapshot: verified(snapshot) },
        EffectAction::Authorize { id: id.clone(), snapshot: verified(snapshot) },
    ] {
        egress.apply(&actor, at(), action).unwrap();
    }
    let view = egress.inspect(&actor, &id).unwrap();
    assert_eq!(view.phase, worldinterface_domain::Phase::Authorized, "{:?}", view.authorization);
    id
}
struct Clock;
impl TrustedClock for Clock {
    fn now(&self) -> Result<Timestamp> {
        Ok(at())
    }
}
struct Evidence(AuthorizationSnapshot);
impl EvidenceAuthority for Evidence {
    fn current_snapshot(
        &self,
        _: &OpaqueRef,
        _: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        Ok(self.0.clone())
    }
    fn verify(&self, _: &OpaqueRef, reference: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        Ok(if reference == &self.0.facts[0].evidence {
            self.0.facts[0].attested_document.clone()
        } else {
            ContentHash::of(b"verified")
        })
    }
}
struct FixturePort(Vec<(OpaqueRef, Vec<u8>)>);
impl EffectExecutionPort for FixturePort {
    fn invoke(&self, bound: BoundExecution, _: &CancellationToken) -> Result<PortObservation> {
        assert_eq!(bound.intent, bound.resolved.intent_id);
        Ok(PortObservation {
            commit: CommitObservation::ConfirmedCommitted,
            outcome: ExecutionOutcome::Succeeded,
            outputs: vec![b"output".to_vec()],
            evidence: self
                .0
                .iter()
                .map(|(schema, bytes)| EvidenceBytes {
                    schema: schema.clone(),
                    bytes: bytes.clone(),
                })
                .collect(),
            boundary_record: Some(EvidenceBytes {
                schema: "fixture-boundary:1".to_owned().try_into()?,
                bytes: b"provider assertion".to_vec(),
            }),
        })
    }
}
pub(crate) fn complete(
    bundle: Arc<StoreBundle>,
    id: &EffectIntentId,
    snapshot: &AuthorizationSnapshot,
    mut evidence: Vec<(OpaqueRef, Vec<u8>)>,
) -> RunId {
    evidence.push(("fixture-executor:1".to_owned().try_into().unwrap(), b"executed".to_vec()));
    let actor = caller(snapshot);
    let mut queue = QueueEngine::bootstrap(&bundle, Noop, host()).unwrap();
    admission::prepare(&bundle, &actor, at(), id, verified(snapshot)).unwrap();
    admission::deliver(&mut queue, &actor, at(), id, verified(snapshot)).unwrap();
    assert!(run_discovery::discover(&queue, &actor, at(), id, 8).unwrap().complete);
    let run: RunId = bundle.wi().effect_snapshot(actor.tenant()).unwrap().effects[id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .observed_runs[0]
        .as_str()
        .parse()
        .unwrap();
    queue.shutdown().unwrap();
    let handler = EffectAttemptHandler {
        bundle: bundle.clone(),
        port: FixturePort(evidence),
        evidence: Evidence(snapshot.clone()),
        clock: Clock,
    };
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(20), aq.run_until_idle())
            .await
            .unwrap()
            .unwrap();
    });
    assert_eq!(
        aq.projection().get_run_state(&run),
        Some(&actionqueue_core::run::state::RunState::Completed)
    );
    aq.shutdown().unwrap();
    run
}
