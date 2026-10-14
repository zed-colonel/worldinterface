use actionqueue_core::{
    bounded::OpaqueRef as AqOpaqueRef,
    causal::ControlMutationContext,
    control::{ControlScope, HostControlContext},
    disposition::AttemptDisposition,
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
};
use worldinterface_domain::{
    authorization::{
        approval_binding, Approval, AuthorizationSnapshot, Credential, Enforcement, InputClass,
        OrdinaryQualification, Reason, VerifiedSnapshot,
    },
    effect::{CommitObservation, EffectAction},
    ingress::digest,
    resource::{ResolutionFact, ResolverSnapshot, Target, TargetDocument},
    Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::{
    ActorId, ArtifactRef, ContentHash, Decision, EffectIntentId, EffectSubmission, EvidenceRef,
    InformationUseEnvelope, InstanceId, OpaqueRef, PrincipalId, RedistributionPolicy, Timestamp,
};
use worldinterface_runtime::{
    admission,
    aq_adapter::QueueEngine,
    authority::{caller, Authentication, EvidenceAuthority, TrustedClock},
    egress::EffectService,
    resolution::ResourceObserver,
    storage::StoreBundle,
    Result,
};
use worldinterface_storage::{StoreProfile, StoreRoot};

fn at() -> Timestamp {
    "2026-09-21T00:00:00.000000000Z".to_owned().try_into().unwrap()
}
fn fixture() -> (EffectSubmission, AuthorizationSnapshot) {
    let data: serde_json::Value =
        serde_json::from_str(include_str!("../../../conformance/wi-fabric-2/egress/fixture.json"))
            .unwrap();
    (
        serde_json::from_value(data["submission"].clone()).unwrap(),
        serde_json::from_value(data["snapshot"].clone()).unwrap(),
    )
}
fn publish(root: &StoreRoot) {
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
        "generated-executable",
    ] {
        root.publish("tenant:fixture".to_owned().try_into().unwrap(), name.as_bytes()).unwrap();
    }
}
struct Auth {
    principal: PrincipalId,
}
impl Authentication for Auth {
    type Session = &'static str;
    fn authenticate(&self, session: &Self::Session) -> Result<(OpaqueRef, PrincipalId)> {
        match *session {
            "good" => Ok(("tenant:fixture".to_owned().try_into()?, self.principal.clone())),
            "cross" => Ok(("tenant:other".to_owned().try_into()?, self.principal.clone())),
            "other_principal" => Ok((
                "tenant:fixture".to_owned().try_into()?,
                "42345678-1234-4234-8234-123456789abc".to_owned().try_into()?,
            )),
            _ => Err("invalid session".into()),
        }
    }
}
struct Clock {
    expired: Cell<bool>,
}
impl TrustedClock for Clock {
    fn now(&self) -> Result<Timestamp> {
        if self.expired.get() {
            Ok("2026-09-23T00:00:00.000000000Z".to_owned().try_into()?)
        } else {
            Ok(at())
        }
    }
}
struct ExpireAtDeliveryClock {
    calls: Cell<usize>,
    expire_on: usize,
}
impl TrustedClock for ExpireAtDeliveryClock {
    fn now(&self) -> Result<Timestamp> {
        let calls = self.calls.get() + 1;
        self.calls.set(calls);
        if calls >= self.expire_on {
            Ok("2026-09-23T00:00:00.000000000Z".to_owned().try_into()?)
        } else {
            Ok(at())
        }
    }
}
struct Evidence {
    snapshot: AuthorizationSnapshot,
    revoked: Cell<bool>,
    invalid: Cell<bool>,
}
impl EvidenceAuthority for Evidence {
    fn current_snapshot(
        &self,
        _: &OpaqueRef,
        _: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        Ok(self.snapshot.clone())
    }
    fn verify(&self, _: &OpaqueRef, reference: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        if self.revoked.get() {
            return Err("revoked authority evidence".into());
        }
        if reference == &self.snapshot.facts[0].evidence {
            if self.invalid.get() {
                Ok(ContentHash::of(b"wrong document"))
            } else {
                Ok(self.snapshot.facts[0].attested_document.clone())
            }
        } else {
            Ok(ContentHash::of(b"verified"))
        }
    }
}
/// Advances trusted time while the host refreshes authority, after dispatch sampled it.
struct ExpiringEvidence<'a> {
    base: &'a Evidence,
    clock: &'a Clock,
    calls: Cell<usize>,
    expire_on: usize,
}
impl EvidenceAuthority for ExpiringEvidence<'_> {
    fn current_snapshot(
        &self,
        tenant: &OpaqueRef,
        submission: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        let calls = self.calls.get() + 1;
        self.calls.set(calls);
        if calls == self.expire_on {
            self.clock.expired.set(true);
        }
        self.base.current_snapshot(tenant, submission)
    }
    fn verify(
        &self,
        tenant: &OpaqueRef,
        reference: &EvidenceRef,
        at: &Timestamp,
    ) -> Result<ContentHash> {
        self.base.verify(tenant, reference, at)
    }
}
struct Observer<'a> {
    root: &'a StoreRoot,
    fact: ResolutionFact,
    changed: Cell<bool>,
    observed_after_intent: Cell<bool>,
}
impl ResourceObserver for Observer<'_> {
    fn observe(
        &self,
        tenant: &OpaqueRef,
        _: &EffectSubmission,
        _: &ResolverSnapshot,
        _: &Timestamp,
    ) -> Result<ResolutionFact> {
        self.observed_after_intent.set(!self.root.effect_snapshot(tenant)?.effects.is_empty());
        if self.changed.get() {
            Err("ambiguous live target".into())
        } else {
            Ok(self.fact.clone())
        }
    }
    fn compare_before_use(&self, _: &ResolutionFact) -> Result<TargetDocument> {
        let mut document = self.fact.document.clone();
        if self.changed.get() {
            match &mut document.target {
                Target::Filesystem { symlink_chain, .. } => {
                    *symlink_chain = ContentHash::of(b"replaced link")
                }
                Target::Network { addresses, redirects, .. } => {
                    addresses.insert("169.254.169.254".to_owned().try_into()?);
                    redirects.push("redirect:metadata".to_owned().try_into()?);
                }
                Target::Communication { audience, .. } => {
                    audience.insert(PrincipalId::generate());
                }
                _ => document.version += 1,
            }
        }
        Ok(document)
    }
}
struct Complete;
impl ExecutorHandler for Complete {
    fn execute(&self, _: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(AqOpaqueRef::new("trusted-host").unwrap()),
    }
}

fn opaque(value: &str) -> OpaqueRef {
    value.to_owned().try_into().unwrap()
}
fn allow_submission(snapshot: &mut AuthorizationSnapshot, submission: &EffectSubmission) {
    let authority = BTreeSet::from([digest(submission).unwrap()]);
    snapshot.bindings[0].principal_authority = authority.clone();
    snapshot.bindings[0].capacity_authority = authority.clone();
    snapshot.policy.platform = authority.clone();
    snapshot.policy.resource = authority.clone();
    snapshot.policy.information_use = authority.clone();
    snapshot.grants[0].authority = authority;
}
fn qualify(snapshot: &mut AuthorizationSnapshot) {
    let worker = snapshot.worker.as_ref().unwrap();
    snapshot.qualification = Some(OrdinaryQualification {
        realization: digest(worker).unwrap(),
        tenant: snapshot.bindings[0].tenant.clone(),
        subject: snapshot.bindings[0].principal.clone(),
        provider: worker.provider.clone(),
        profile: worker.profile.clone(),
        operations: BTreeSet::from([opaque("start")]),
        aq_version: worker.aq_version.clone(),
        wi_version: worker.wi_version.clone(),
        expires_at: snapshot.policy.expires_at.clone(),
        revoked: false,
        quarantined: false,
        evidence: snapshot.fixture.as_ref().map_or_else(
            || snapshot.qualification.as_ref().unwrap().evidence.clone(),
            |q| q.evidence.clone(),
        ),
    });
    snapshot.fixture = None;
}
fn ordinary_fixture() -> (EffectSubmission, AuthorizationSnapshot) {
    let (submission, mut snapshot) = fixture();
    qualify(&mut snapshot);
    (submission, snapshot)
}
/// A trusted authority store which can change between evaluation and delivery.
struct CurrentEvidence(RefCell<AuthorizationSnapshot>);
impl EvidenceAuthority for CurrentEvidence {
    fn current_snapshot(
        &self,
        _: &OpaqueRef,
        _: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        Ok(self.0.borrow().clone())
    }
    fn verify(&self, _: &OpaqueRef, reference: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        let snapshot = self.0.borrow();
        Ok(snapshot
            .facts
            .iter()
            .find(|f| &f.evidence == reference)
            .map_or_else(|| ContentHash::of(b"verified"), |f| f.attested_document.clone()))
    }
}
type Service<'a> = EffectService<'a, Auth, CurrentEvidence, Observer<'a>, Clock>;
fn with_effect(
    submission: EffectSubmission,
    snapshot: AuthorizationSnapshot,
    check: impl FnOnce(
        &StoreBundle,
        &Service<'_>,
        &CurrentEvidence,
        &mut QueueEngine<'_, Complete>,
        &EffectIntentId,
    ),
) {
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("bundle"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    publish(bundle.wi());
    let auth = Auth { principal: snapshot.bindings[0].principal.clone() };
    let observer = Observer {
        root: bundle.wi(),
        fact: snapshot.facts[0].clone(),
        changed: Cell::new(false),
        observed_after_intent: Cell::new(false),
    };
    let evidence = CurrentEvidence(RefCell::new(snapshot));
    let clock = Clock { expired: Cell::new(false) };
    let service = EffectService {
        root: bundle.wi(),
        authentication: &auth,
        evidence: &evidence,
        resolver: &observer,
        clock: &clock,
    };
    let id = service.submit(&"good", submission.clone()).unwrap();
    let state = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
    assert_eq!(state.effects[&id].phase, Phase::Submitted);
    assert_eq!(state.effects[&id].intent.submission, submission);
    assert!(state.effects[&id].dispatches.is_empty());
    assert!(state.uses.is_empty());
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    assert_eq!(service.advance(&"good", &id).unwrap().phase, Phase::Resolving);
    assert!(observer.observed_after_intent.get());
    check(&bundle, &service, &evidence, &mut queue, &id);
    queue.shutdown().unwrap();
}
fn assert_denied(
    bundle: &StoreBundle,
    service: &Service<'_>,
    queue: &mut QueueEngine<'_, Complete>,
    id: &EffectIntentId,
    expected: &[Reason],
) {
    let before = queue.engine().projection().projection_digest().unwrap();
    let view = service.advance(&"good", id).unwrap();
    assert_eq!(view.phase, Phase::Denied, "{:?}", view.authorization);
    let explanation = service.explain(&"good", id).unwrap();
    assert_eq!(explanation.decision, Decision::Deny);
    assert_eq!(explanation.reasons, expected);
    assert_eq!(service.dispatch_pending(&"good", queue, 1).unwrap().attempted, 0);
    assert_eq!(queue.engine().projection().projection_digest().unwrap(), before);
    let state = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
    assert!(state.uses.is_empty());
    assert!(state.effects[id].dispatches.is_empty());
}

#[test]
fn wp009_runtime_exact_qualification_admits_once_without_execution_success() {
    let (submission, snapshot) = ordinary_fixture();
    assert_eq!(
        snapshot.qualification.as_ref().unwrap().realization,
        digest(snapshot.worker.as_ref().unwrap()).unwrap()
    );
    with_effect(submission, snapshot, |bundle, service, _, queue, id| {
        assert_eq!(service.advance(&"good", id).unwrap().phase, Phase::Authorized);
        assert_eq!(service.explain(&"good", id).unwrap().decision, Decision::Allow);
        assert_eq!(service.dispatch_pending(&"good", queue, 1).unwrap().acknowledged, 1);
        assert_eq!(service.dispatch_pending(&"good", queue, 1).unwrap().attempted, 0);
        assert_eq!(queue.engine().projection().task_count(), 1);
        let state = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
        assert_eq!(state.uses.values().sum::<u32>(), 1);
        let effect = &state.effects[id];
        let binding = effect.dispatches[0].admission.as_ref().unwrap();
        assert!(binding.acknowledged_sequence.is_some());
        assert!(binding.observed_runs.is_empty());
        assert_eq!(
            admission::load_request(bundle.wi(), binding).unwrap().task_spec().id().to_string(),
            binding.task.as_str()
        );
        assert_eq!(effect.phase, Phase::DispatchPending);
        assert_eq!(effect.commit, CommitObservation::NotAttempted);
        assert!(effect.outcomes.is_empty());
    });
}

#[test]
fn wp010_runtime_generated_executable_denied_despite_exact_approval_and_purpose() {
    for purpose in ["self-initiated:generated", "candidate:generated", "reviewed:generated"] {
        let (mut submission, mut snapshot) = ordinary_fixture();
        submission.purpose = Some(opaque(purpose));
        allow_submission(&mut snapshot, &submission);
        snapshot.worker.as_mut().unwrap().reviewed_executable = false;
        if purpose != "reviewed:generated" {
            snapshot.worker.as_mut().unwrap().manifest.executable = ArtifactRef {
                scope: opaque("tenant:fixture"),
                content: ContentHash::of(b"generated-executable"),
                size: b"generated-executable".len() as u64,
            };
        }
        qualify(&mut snapshot);
        snapshot.policy.approval_required = true;
        snapshot.policy.approvers = BTreeSet::from([PrincipalId::generate()]);
        with_effect(submission, snapshot, |bundle, service, evidence, queue, id| {
            let state = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
            let effect = &state.effects[id];
            let mut snapshot = evidence.0.borrow_mut();
            let exact_binding = approval_binding(
                &effect.intent,
                &effect.agency,
                effect.resolution.as_ref().unwrap(),
                &snapshot,
            )
            .unwrap();
            let approval = Approval {
                id: opaque("approval:review"),
                version: 1,
                approver: snapshot.policy.approvers.iter().next().unwrap().clone(),
                exact_binding,
                expires_at: snapshot.policy.expires_at.clone(),
                revoked: false,
                max_uses: 1,
                evidence: snapshot.grants[0].evidence.clone(),
            };
            snapshot.approvals.push(approval);
            drop(snapshot);
            assert_denied(bundle, service, queue, id, &[Reason::Executable]);
        });
    }
}

#[test]
fn wp011_runtime_protected_input_and_credentials_cannot_admit() {
    for case in ["protected", "production", "wrong_run", "expired", "wrong_subject", "widened"] {
        let (submission, mut snapshot) = ordinary_fixture();
        let principal = snapshot.bindings[0].principal.clone();
        let worker = snapshot.worker.as_mut().unwrap();
        let expected = if case == "protected" {
            worker.input_class = InputClass::Protected;
            Reason::ProtectedInput
        } else {
            worker.credential = if case == "production" {
                Credential::Production
            } else {
                Credential::Test {
                    run: if case == "wrong_run" { opaque("run:other") } else { worker.run.clone() },
                    subject: if case == "wrong_subject" {
                        PrincipalId::generate()
                    } else {
                        principal
                    },
                    expires_at: if case == "expired" {
                        at()
                    } else {
                        "2026-09-21T00:05:00.000000000Z".to_owned().try_into().unwrap()
                    },
                    authority: if case == "widened" {
                        BTreeSet::from([digest(&submission).unwrap(), ContentHash::of(b"extra")])
                    } else {
                        BTreeSet::from([digest(&submission).unwrap()])
                    },
                }
            };
            Reason::Credential
        };
        qualify(&mut snapshot);
        let reasons =
            if case == "expired" { vec![expected, Reason::Expired] } else { vec![expected] };
        with_effect(submission, snapshot, |bundle, service, _, queue, id| {
            assert_denied(bundle, service, queue, id, &reasons)
        });
    }
}

#[test]
fn wp011_runtime_test_credential_is_exact_short_lived_and_inspection_is_scoped() {
    let (submission, mut snapshot) = ordinary_fixture();
    let expiry: Timestamp = "2026-09-21T00:05:00.000000000Z".to_owned().try_into().unwrap();
    let worker = snapshot.worker.as_mut().unwrap();
    worker.credential = Credential::Test {
        run: worker.run.clone(),
        subject: snapshot.bindings[0].principal.clone(),
        expires_at: expiry.clone(),
        authority: BTreeSet::from([digest(&submission).unwrap()]),
    };
    qualify(&mut snapshot);
    with_effect(submission, snapshot, |bundle, service, _, queue, id| {
        assert_eq!(service.advance(&"good", id).unwrap().phase, Phase::Authorized);
        assert_eq!(service.explain(&"good", id).unwrap().expires_at, expiry);
        assert!(service.explain(&"cross", id).is_err());
        assert!(service.explain(&"forged", id).is_err());
        assert!(service.explain(&"other_principal", id).is_err());
        assert!(service.advance(&"other_principal", id).is_err());
        let before = queue.engine().projection().projection_digest().unwrap();
        assert_eq!(service.dispatch_pending(&"other_principal", queue, 1).unwrap().attempted, 0);
        assert_eq!(queue.engine().projection().projection_digest().unwrap(), before);
        assert_eq!(service.dispatch_pending(&"good", queue, 1).unwrap().acknowledged, 1);
        assert_eq!(
            bundle
                .wi()
                .effect_snapshot(&opaque("tenant:fixture"))
                .unwrap()
                .uses
                .values()
                .sum::<u32>(),
            1
        );
    });
}

#[test]
fn wp012_runtime_each_mandatory_control_denies_without_downgrade() {
    for name in ["cpu", "memory", "mount", "network"] {
        for defect in ["missing", "unknown", "unsupported", "advertised", "applied", "observed"] {
            let (submission, mut snapshot) = ordinary_fixture();
            let controls = &mut snapshot.worker.as_mut().unwrap().controls;
            if defect == "missing" {
                controls.remove(&opaque(name));
            } else {
                let control = controls.get_mut(&opaque(name)).unwrap();
                match defect {
                    "unknown" => control.enforcement = Enforcement::Unknown,
                    "unsupported" => control.enforcement = Enforcement::Unsupported,
                    "advertised" => control.advertised = opaque("unbounded"),
                    "applied" => control.applied = opaque("unbounded"),
                    "observed" => control.observed = opaque("unbounded"),
                    _ => unreachable!(),
                }
            }
            qualify(&mut snapshot);
            with_effect(submission, snapshot, |bundle, service, _, queue, id| {
                assert_denied(bundle, service, queue, id, &[Reason::MandatoryControl(opaque(name))])
            });
        }
    }
    let (submission, mut snapshot) = ordinary_fixture();
    snapshot.worker.as_mut().unwrap().mandatory.insert(opaque("unknown-control"));
    qualify(&mut snapshot);
    with_effect(submission, snapshot, |bundle, service, _, queue, id| {
        assert_denied(
            bundle,
            service,
            queue,
            id,
            &[Reason::MandatoryControl(opaque("unknown-control"))],
        )
    });
}

#[test]
fn wp013_runtime_ordinary_unsupported_operations_are_unavailable() {
    for operation in ["suspend", "checkpoint", "resume", "command"] {
        let (submission, mut snapshot) = ordinary_fixture();
        snapshot.worker.as_mut().unwrap().operation = opaque(operation);
        if let Target::Worker { operation: target, .. } = &mut snapshot.facts[0].document.target {
            *target = opaque(operation);
        }
        snapshot.facts[0].attested_document = digest(&snapshot.facts[0].document).unwrap();
        snapshot.policy.targets = BTreeSet::from([snapshot.facts[0].attested_document.clone()]);
        // The realization and exact target are valid; the qualification permits only start.
        qualify(&mut snapshot);
        with_effect(submission, snapshot, |bundle, service, _, queue, id| {
            assert_denied(bundle, service, queue, id, &[Reason::Unavailable]);
            let state = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
            assert_eq!(state.effects.len(), 1);
            assert_eq!(
                state.effects[id]
                    .authorization_inputs
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation,
                opaque(operation)
            );
        });
    }
}

fn drift_worker(snapshot: &mut AuthorizationSnapshot, field: &str, root: &StoreRoot) {
    let worker = snapshot.worker.as_mut().unwrap();
    match field {
        "adapter" => {
            worker.manifest.adapter =
                root.publish(opaque("tenant:fixture"), b"new adapter").unwrap()
        }
        "provider" => worker.provider = opaque("provider:changed"),
        "profile" => worker.profile = opaque("profile:changed"),
        "executable" => {
            worker.manifest.executable =
                root.publish(opaque("tenant:fixture"), b"new executable").unwrap()
        }
        "configuration" => {
            worker.manifest.configuration =
                root.publish(opaque("tenant:fixture"), b"new configuration").unwrap()
        }
        "input" => worker.input = root.publish(opaque("tenant:fixture"), b"new input").unwrap(),
        _ => unreachable!(),
    }
}

#[test]
fn wp014_runtime_drift_blocks_initial_and_prepared_delivery() {
    for field in ["provider", "profile", "executable", "configuration", "input", "adapter"] {
        for prepared in [false, true] {
            let (submission, snapshot) = ordinary_fixture();
            with_effect(submission, snapshot, |bundle, service, evidence, queue, id| {
                assert_eq!(service.advance(&"good", id).unwrap().phase, Phase::Authorized);
                let before_binding = if prepared {
                    Some(
                        admission::prepare(
                            bundle,
                            &caller(service.authentication, &"good").unwrap(),
                            at(),
                            id,
                            VerifiedSnapshot::from_trusted_verifier(evidence.0.borrow().clone())
                                .unwrap(),
                        )
                        .unwrap(),
                    )
                } else {
                    None
                };
                drift_worker(&mut evidence.0.borrow_mut(), field, bundle.wi());
                let before = queue.engine().projection().projection_digest().unwrap();
                let state_before = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
                let report = service.dispatch_pending(&"good", queue, 1).unwrap();
                assert_eq!(report.attempted, 1, "{field}/{prepared}");
                assert_eq!(report.failed, 1);
                assert_eq!(report.acknowledged, 0);
                assert_eq!(queue.engine().projection().projection_digest().unwrap(), before);
                let state = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
                assert_eq!(state, state_before);
                assert_eq!(state.uses.values().sum::<u32>(), u32::from(prepared));
                if let Some(binding) = before_binding {
                    assert_eq!(state.effects[id].dispatches[0].admission.as_ref(), Some(&binding));
                    let subject = caller(service.authentication, &"good").unwrap();
                    assert!(admission::deliver(
                        queue,
                        &subject,
                        at(),
                        id,
                        VerifiedSnapshot::from_trusted_verifier(evidence.0.borrow().clone())
                            .unwrap()
                    )
                    .unwrap_err()
                    .to_string()
                    .contains("pending admission authorization drift"));
                } else {
                    assert_eq!(service.advance(&"good", id).unwrap().phase, Phase::Denied);
                    assert_eq!(
                        service.explain(&"good", id).unwrap().reasons,
                        vec![Reason::ProfileDrift]
                    );
                }
            });
        }
    }
}

#[test]
fn wp014_runtime_explicit_reevaluation_cannot_rebind_persisted_request() {
    for prepared in [false, true] {
        let (submission, snapshot) = ordinary_fixture();
        with_effect(submission, snapshot, |bundle, service, evidence, queue, id| {
            assert_eq!(service.advance(&"good", id).unwrap().phase, Phase::Authorized);
            let original = service.explain(&"good", id).unwrap();
            let binding = if prepared {
                Some(
                    admission::prepare(
                        bundle,
                        &caller(service.authentication, &"good").unwrap(),
                        at(),
                        id,
                        VerifiedSnapshot::from_trusted_verifier(evidence.0.borrow().clone())
                            .unwrap(),
                    )
                    .unwrap(),
                )
            } else {
                None
            };
            // A new valid qualification can change the realization before preparation.
            // The operation and resolved target stay exact; provider/operation changes need replacement.
            for field in ["profile", "executable", "configuration", "adapter"] {
                drift_worker(&mut evidence.0.borrow_mut(), field, bundle.wi());
            }
            qualify(&mut evidence.0.borrow_mut());
            let before = queue.engine().projection().projection_digest().unwrap();
            assert_eq!(service.dispatch_pending(&"good", queue, 1).unwrap().failed, 1);
            assert_eq!(queue.engine().projection().projection_digest().unwrap(), before);
            if let Some(binding) = binding {
                let subject = caller(service.authentication, &"good").unwrap();
                assert!(worldinterface_egress::Egress::new(bundle.wi())
                    .apply(
                        &subject,
                        at(),
                        worldinterface_domain::effect::EffectAction::Authorize {
                            id: id.clone(),
                            snapshot: VerifiedSnapshot::from_trusted_verifier(
                                evidence.0.borrow().clone()
                            )
                            .unwrap()
                        }
                    )
                    .unwrap_err()
                    .to_string()
                    .contains("pending admission authorization drift"));
                let state = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
                assert_eq!(state.effects[id].dispatches[0].admission.as_ref(), Some(&binding));
                assert_eq!(state.uses.values().sum::<u32>(), 1);
            } else {
                assert_eq!(service.advance(&"good", id).unwrap().phase, Phase::Authorized);
                assert_ne!(
                    service.explain(&"good", id).unwrap().exact_binding,
                    original.exact_binding
                );
                assert_eq!(service.dispatch_pending(&"good", queue, 1).unwrap().acknowledged, 1);
                assert_eq!(queue.engine().projection().task_count(), 1);
                assert_eq!(
                    bundle
                        .wi()
                        .effect_snapshot(&opaque("tenant:fixture"))
                        .unwrap()
                        .uses
                        .values()
                        .sum::<u32>(),
                    1
                );
            }
        });
    }
}

#[test]
fn caller_claims_cannot_override_authenticated_agency_or_grants() {
    for claim in ["actor", "accountable", "capacity", "grant", "approval"] {
        let (mut submission, mut snapshot) = ordinary_fixture();
        match claim {
            "actor" => submission.submitted_by = Some(ActorId::generate()),
            "accountable" => {
                submission.requested_accountable_principal = Some(PrincipalId::generate())
            }
            "capacity" => {
                let mut capacity = snapshot.bindings[0].capacity.clone();
                capacity.assignment_ref = opaque("capacity:forged");
                submission.acting_capacity = Some(capacity);
            }
            "grant" => submission.requested_authority_refs.push(opaque("grant:forged")),
            "approval" => submission.approval_attestations.push(opaque("approval:forged")),
            _ => unreachable!(),
        }
        allow_submission(&mut snapshot, &submission);
        if claim == "grant" || claim == "approval" {
            with_effect(submission, snapshot, |bundle, service, _, queue, id| {
                assert_denied(
                    bundle,
                    service,
                    queue,
                    id,
                    &[if claim == "grant" { Reason::Grant } else { Reason::Approval }],
                )
            });
        } else {
            let temp = tempfile::tempdir().unwrap();
            let bundle = StoreBundle::initialize(
                &temp.path().join("bundle"),
                StoreProfile {
                    instance: InstanceId::generate(),
                    created_at: at(),
                    platform: false,
                },
            )
            .unwrap();
            publish(bundle.wi());
            let auth = Auth { principal: snapshot.bindings[0].principal.clone() };
            let evidence = CurrentEvidence(RefCell::new(snapshot.clone()));
            let observer = Observer {
                root: bundle.wi(),
                fact: snapshot.facts[0].clone(),
                changed: Cell::new(false),
                observed_after_intent: Cell::new(false),
            };
            let clock = Clock { expired: Cell::new(false) };
            let service = EffectService {
                root: bundle.wi(),
                authentication: &auth,
                evidence: &evidence,
                resolver: &observer,
                clock: &clock,
            };
            let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
            let before = queue.engine().projection().projection_digest().unwrap();
            assert!(service
                .submit(&"good", submission)
                .unwrap_err()
                .to_string()
                .contains("agency assertion or capacity mismatch"));
            let state = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
            assert!(state.effects.is_empty());
            assert!(state.uses.is_empty());
            assert!(!observer.observed_after_intent.get());
            assert_eq!(service.dispatch_pending(&"good", &mut queue, 1).unwrap().attempted, 0);
            assert_eq!(queue.engine().projection().projection_digest().unwrap(), before);
            queue.shutdown().unwrap();
        }
    }
}

#[test]
fn self_initiated_and_external_requests_follow_the_same_authorization_path() {
    for purpose in [None, Some(opaque("self-initiated:fixture"))] {
        for allowed in [false, true] {
            let (mut submission, mut snapshot) = ordinary_fixture();
            submission.purpose = purpose.clone();
            allow_submission(&mut snapshot, &submission);
            if !allowed {
                snapshot.policy.platform.clear();
            }
            with_effect(submission, snapshot, |bundle, service, _, queue, id| {
                if allowed {
                    assert_eq!(service.advance(&"good", id).unwrap().phase, Phase::Authorized);
                    let explanation = service.explain(&"good", id).unwrap();
                    assert_eq!(explanation.reasons, vec![Reason::ExactAuthority]);
                    assert_eq!(
                        explanation.trace.iter().map(|s| s.stage.as_str()).collect::<Vec<_>>(),
                        vec!["agency", "platform", "resource", "information_use", "grant:fixture"]
                    );
                    assert_eq!(
                        service.dispatch_pending(&"good", queue, 1).unwrap().acknowledged,
                        1
                    );
                } else {
                    assert_denied(bundle, service, queue, id, &[Reason::ExactAuthority]);
                }
            });
        }
    }
}

#[test]
fn production_facade_authenticates_verifies_and_rechecks_live_target() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("a"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    let other = StoreBundle::initialize(
        &temp.path().join("b"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    publish(bundle.wi());
    let (submission, snapshot) = fixture();
    let auth = Auth { principal: snapshot.bindings[0].principal.clone() };
    let evidence = Evidence {
        snapshot: snapshot.clone(),
        revoked: Cell::new(false),
        invalid: Cell::new(false),
    };
    let observer = Observer {
        root: bundle.wi(),
        fact: snapshot.facts[0].clone(),
        changed: Cell::new(false),
        observed_after_intent: Cell::new(false),
    };
    let clock = Clock { expired: Cell::new(false) };
    let service = EffectService {
        root: bundle.wi(),
        authentication: &auth,
        evidence: &evidence,
        resolver: &observer,
        clock: &clock,
    };
    assert!(service.submit(&"forged", submission.clone()).is_err());
    assert!(bundle.wi().effect_snapshot(&snapshot.bindings[0].tenant).unwrap().effects.is_empty());
    evidence.invalid.set(true);
    assert!(service.submit(&"good", submission.clone()).is_err());
    evidence.invalid.set(false);
    evidence.revoked.set(true);
    assert!(service.submit(&"good", submission.clone()).is_err());
    evidence.revoked.set(false);
    let id = service.submit(&"good", submission.clone()).unwrap();
    assert!(service.advance(&"cross", &id).is_err());
    assert!(service.explain(&"forged", &id).is_err());
    let mut conflict = submission.clone();
    conflict.budget.timeout_millis += 1;
    assert!(service.submit(&"good", conflict).is_err());
    assert_eq!(service.submit(&"good", submission.clone()).unwrap(), id);
    let mut wrong_queue = QueueEngine::bootstrap(&other, Complete, host()).unwrap();
    assert!(service.dispatch_pending(&"good", &mut wrong_queue, 1).is_err());
    assert_eq!(wrong_queue.engine().projection().task_count(), 0);
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    assert_eq!(service.advance(&"good", &id).unwrap().phase, Phase::Resolving);
    assert!(observer.observed_after_intent.get());
    assert_eq!(service.advance(&"good", &id).unwrap().phase, Phase::Authorized);
    observer.changed.set(true);
    assert_eq!(service.dispatch_pending(&"good", &mut queue, 1).unwrap().acknowledged, 0);
    assert_eq!(queue.engine().projection().task_count(), 0);
    observer.changed.set(false);
    clock.expired.set(true);
    assert_eq!(service.dispatch_pending(&"good", &mut queue, 1).unwrap().acknowledged, 0);
    assert_eq!(queue.engine().projection().task_count(), 0);
    clock.expired.set(false);
    assert_eq!(service.dispatch_pending(&"good", &mut queue, 1).unwrap().acknowledged, 1);
    assert_eq!(queue.engine().projection().task_count(), 1);
    let progress = service.discover_runs(&"good", &id, &queue, 8).unwrap();
    assert!(progress.complete);
    let view = Egress::new(bundle.wi()).inspect(&caller(&auth, &"good").unwrap(), &id).unwrap();
    let admission = view.dispatch.unwrap().admission.unwrap();
    let run = admission.observed_runs.first().expect("AQ run observed").clone();
    let principal = caller(&auth, &"good").unwrap();
    let start = EffectAction::StartProduction {
        id: id.clone(),
        snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        run: run.clone(),
        attempt: "attempt:production".to_owned().try_into().unwrap(),
    };
    assert!(Egress::new(bundle.wi())
        .apply(
            &principal,
            at(),
            EffectAction::StartProduction {
                id: id.clone(),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
                run: "forged-run".to_owned().try_into().unwrap(),
                attempt: "attempt:forged".to_owned().try_into().unwrap(),
            }
        )
        .is_err());
    assert!(Egress::new(bundle.wi())
        .apply(
            &principal,
            at(),
            EffectAction::Start {
                id: id.clone(),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
                attempt: "attempt:legacy".to_owned().try_into().unwrap(),
            }
        )
        .is_err());
    Egress::new(bundle.wi()).apply(&principal, at(), start.clone()).unwrap();
    assert!(Egress::new(bundle.wi()).apply(&principal, at(), start).is_err());
    queue.shutdown().unwrap();
    wrong_queue.shutdown().unwrap();
}

#[test]
fn expiry_during_refresh_blocks_initial_duplicate_and_later_batch_admission() {
    for case in ["initial", "duplicate", "batch", "preparation", "authorization_commit"] {
        let temp = tempfile::tempdir().unwrap();
        let bundle = StoreBundle::initialize(
            &temp.path().join("bundle"),
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap();
        publish(bundle.wi());
        let (submission, mut snapshot) = fixture();
        let second_submission = if case == "batch" {
            let mut second = submission.clone();
            second.client_request_id = "fixture-request-2".to_owned().try_into().unwrap();
            let authority = worldinterface_domain::ingress::digest(&second).unwrap();
            snapshot.bindings[0].principal_authority.insert(authority.clone());
            snapshot.bindings[0].capacity_authority.insert(authority.clone());
            snapshot.policy.platform.insert(authority.clone());
            snapshot.policy.resource.insert(authority.clone());
            snapshot.policy.information_use.insert(authority.clone());
            snapshot.grants[0].authority.insert(authority);
            Some(second)
        } else {
            None
        };
        let auth = Auth { principal: snapshot.bindings[0].principal.clone() };
        let evidence = Evidence {
            snapshot: snapshot.clone(),
            revoked: Cell::new(false),
            invalid: Cell::new(false),
        };
        let observer = Observer {
            root: bundle.wi(),
            fact: snapshot.facts[0].clone(),
            changed: Cell::new(false),
            observed_after_intent: Cell::new(false),
        };
        let clock = Clock { expired: Cell::new(false) };
        let service = EffectService {
            root: bundle.wi(),
            authentication: &auth,
            evidence: &evidence,
            resolver: &observer,
            clock: &clock,
        };
        let id = service.submit(&"good", submission.clone()).unwrap();
        assert_eq!(service.advance(&"good", &id).unwrap().phase, Phase::Resolving);
        assert_eq!(service.advance(&"good", &id).unwrap().phase, Phase::Authorized);
        let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
        if let Some(second) = second_submission {
            let second_id = service.submit(&"good", second).unwrap();
            assert_eq!(service.advance(&"good", &second_id).unwrap().phase, Phase::Resolving);
            let second_view = service.advance(&"good", &second_id).unwrap();
            assert_eq!(second_view.phase, Phase::Authorized, "{:?}", second_view.authorization);
        }
        let prepared = if case == "duplicate" {
            let subject = caller(&auth, &"good").unwrap();
            let binding = admission::prepare(
                &bundle,
                &subject,
                at(),
                &id,
                VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
            )
            .unwrap();
            let request = admission::load_request(bundle.wi(), &binding).unwrap();
            assert!(queue.ensure_task(request).unwrap().is_created());
            Some(binding)
        } else {
            None
        };
        let before_aq = queue.engine().projection().projection_digest().unwrap();
        let expiring = ExpiringEvidence {
            base: &evidence,
            clock: &clock,
            calls: Cell::new(0),
            expire_on: if case == "batch" { 2 } else { 1 },
        };
        let expiring_service = EffectService {
            root: bundle.wi(),
            authentication: &auth,
            evidence: &expiring,
            resolver: &observer,
            clock: &clock,
        };
        let report = if case == "preparation" || case == "authorization_commit" {
            let late_clock = ExpireAtDeliveryClock {
                calls: Cell::new(0),
                expire_on: if case == "preparation" { 3 } else { 4 },
            };
            EffectService {
                root: bundle.wi(),
                authentication: &auth,
                evidence: &evidence,
                resolver: &observer,
                clock: &late_clock,
            }
            .dispatch_pending(&"good", &mut queue, 2)
            .unwrap()
        } else {
            expiring_service.dispatch_pending(&"good", &mut queue, 2).unwrap()
        };
        assert_eq!(report.attempted, if case == "batch" { 2 } else { 1 }, "{case}");
        assert_eq!(report.acknowledged, if case == "batch" { 1 } else { 0 }, "{case}");
        assert_eq!(report.failed, 1, "{case}");
        assert_eq!(
            queue.engine().projection().task_count(),
            if case == "batch" || case == "duplicate" { 1 } else { 0 },
            "{case}"
        );
        let state = bundle.wi().effect_snapshot(&snapshot.bindings[0].tenant).unwrap();
        assert_eq!(
            state.uses.values().sum::<u32>(),
            if case == "initial" { 0 } else { 1 },
            "{case}"
        );
        if case == "preparation" || case == "authorization_commit" {
            assert_eq!(queue.engine().projection().projection_digest().unwrap(), before_aq);
            let admission = state.effects[&id].dispatches[0].admission.as_ref().unwrap();
            assert!(admission.acknowledged_sequence.is_none());
            admission::load_request(bundle.wi(), admission).unwrap();
        }
        if let Some(binding) = prepared {
            assert_eq!(queue.engine().projection().projection_digest().unwrap(), before_aq);
            let admission = state.effects[&id].dispatches[0].admission.as_ref().unwrap();
            assert_eq!(admission, &binding);
            assert!(admission.acknowledged_sequence.is_none());
            assert_eq!(
                admission::load_request(bundle.wi(), admission)
                    .unwrap()
                    .task_spec()
                    .id()
                    .to_string(),
                binding.task.as_str()
            );
        }
        queue.shutdown().unwrap();
    }
}

#[test]
fn live_ambiguity_after_intent_never_reaches_aq() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("bundle"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    publish(bundle.wi());
    let (mut submission, snapshot) = fixture();
    submission.purpose = Some("self-initiated:fixture".to_owned().try_into().unwrap());
    let auth = Auth { principal: snapshot.bindings[0].principal.clone() };
    let evidence = Evidence {
        snapshot: snapshot.clone(),
        revoked: Cell::new(false),
        invalid: Cell::new(false),
    };
    let observer = Observer {
        root: bundle.wi(),
        fact: snapshot.facts[0].clone(),
        changed: Cell::new(true),
        observed_after_intent: Cell::new(false),
    };
    let clock = Clock { expired: Cell::new(false) };
    let service = EffectService {
        root: bundle.wi(),
        authentication: &auth,
        evidence: &evidence,
        resolver: &observer,
        clock: &clock,
    };
    let id = service.submit(&"good", submission).unwrap();
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    assert_ne!(service.advance(&"good", &id).unwrap().phase, Phase::Authorized);
    assert!(observer.observed_after_intent.get());
    assert_eq!(service.dispatch_pending(&"good", &mut queue, 1).unwrap().attempted, 0);
    assert_eq!(queue.engine().projection().task_count(), 0);
    queue.shutdown().unwrap();
}

#[test]
fn production_facade_rejects_resource_boundaries_before_admission() {
    use std::collections::BTreeSet;
    for case in [
        "path_traversal",
        "symlink_replacement",
        "private_dns",
        "redirect",
        "dns_rebinding",
        "audience",
        "audience_widening",
        "audience_replacement",
        "selector_alias_ambiguity",
        "registry_ambiguity",
        "tenant_ambiguity",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let bundle = StoreBundle::initialize(
            &temp.path().join("bundle"),
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap();
        publish(bundle.wi());
        let (mut submission, mut snapshot) = fixture();
        snapshot.worker = None;
        snapshot.fixture = None;
        let namespace = match case {
            "path_traversal" | "symlink_replacement" => "filesystem",
            "private_dns" | "redirect" | "dns_rebinding" => "network",
            "audience" | "audience_widening" | "audience_replacement" => "communication",
            _ => "worker",
        };
        submission.resource.namespace = namespace.to_owned().try_into().unwrap();
        snapshot.registry.entries[0].namespace = namespace.to_owned().try_into().unwrap();
        snapshot.facts[0].selector = submission.resource.clone();
        snapshot.facts[0].document.target = match case {
            "path_traversal" | "symlink_replacement" => Target::Filesystem {
                root: "root:fixture".to_owned().try_into().unwrap(),
                components: vec![if case == "path_traversal" { ".." } else { "safe" }.into()],
                symlink_chain: ContentHash::of(b"original link"),
            },
            "private_dns" | "redirect" | "dns_rebinding" => Target::Network {
                scheme: "https".to_owned().try_into().unwrap(),
                host: "example.org".to_owned().try_into().unwrap(),
                port: 443,
                addresses: BTreeSet::from([if case == "private_dns" {
                    "169.254.169.254"
                } else {
                    "93.184.215.14"
                }
                .to_owned()
                .try_into()
                .unwrap()]),
                redirects: if case == "redirect" {
                    vec!["redirect:metadata".to_owned().try_into().unwrap()]
                } else {
                    vec![]
                },
                path: "/".into(),
            },
            "audience" | "audience_widening" | "audience_replacement" => Target::Communication {
                account: "account:fixture".to_owned().try_into().unwrap(),
                sender: snapshot.bindings[0].principal.clone(),
                channel: "channel:fixture".to_owned().try_into().unwrap(),
                thread: None,
                audience: if case == "audience" {
                    BTreeSet::new()
                } else {
                    BTreeSet::from([snapshot.bindings[0].principal.clone()])
                },
                visibility: "private".to_owned().try_into().unwrap(),
            },
            _ => snapshot.facts[0].document.target.clone(),
        };
        if case == "tenant_ambiguity" {
            snapshot.facts[0].document.tenant = "tenant:other".to_owned().try_into().unwrap();
        }
        if case == "registry_ambiguity" {
            snapshot.registry.entries.push(snapshot.registry.entries[0].clone());
        }
        if case == "audience_widening" || case == "audience_replacement" {
            submission.purpose = Some(opaque("communication:fixture"));
            submission.information_use = Some(InformationUseEnvelope {
                purpose_refs: vec![opaque("communication:fixture")],
                allowed_audiences: vec![snapshot.bindings[0].principal.clone()],
                retention_policy_ref: None,
                disclosure_policy_ref: None,
                derivation_policy_ref: None,
                redistribution: RedistributionPolicy::NamedAudiences,
                expiration: None,
                source_obligation_refs: vec![],
            });
            if case == "audience_widening" {
                if let Target::Communication { audience, .. } =
                    &mut snapshot.facts[0].document.target
                {
                    audience.insert(PrincipalId::generate());
                }
            }
        }
        snapshot.facts[0].registry =
            worldinterface_domain::ingress::digest(&snapshot.registry).unwrap();
        snapshot.facts[0].attested_document =
            worldinterface_domain::ingress::digest(&snapshot.facts[0].document).unwrap();
        snapshot.policy.targets = BTreeSet::from([snapshot.facts[0].attested_document.clone()]);
        // Alias ambiguity has two exact attestations for the same submitted selector.
        if case == "selector_alias_ambiguity" {
            snapshot.facts.push(snapshot.facts[0].clone());
        }
        allow_submission(&mut snapshot, &submission);
        let auth = Auth { principal: snapshot.bindings[0].principal.clone() };
        let evidence = Evidence {
            snapshot: snapshot.clone(),
            revoked: Cell::new(false),
            invalid: Cell::new(false),
        };
        let observer = Observer {
            root: bundle.wi(),
            fact: snapshot.facts[0].clone(),
            changed: Cell::new(false),
            observed_after_intent: Cell::new(false),
        };
        let clock = Clock { expired: Cell::new(false) };
        let service = EffectService {
            root: bundle.wi(),
            authentication: &auth,
            evidence: &evidence,
            resolver: &observer,
            clock: &clock,
        };
        let id = service.submit(&"good", submission).unwrap();
        let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
        let before = queue.engine().projection().projection_digest().unwrap();
        let first = service.advance(&"good", &id).unwrap();
        if case == "symlink_replacement"
            || case == "dns_rebinding"
            || case == "audience_replacement"
        {
            assert_eq!(first.phase, Phase::Resolving, "{case}");
            assert_eq!(service.advance(&"good", &id).unwrap().phase, Phase::Authorized, "{case}");
            observer.changed.set(true);
            assert!(service.advance(&"good", &id).is_err(), "{case}");
        } else {
            assert_eq!(first.phase, Phase::Failed, "{case}");
            if case == "selector_alias_ambiguity" {
                assert_eq!(
                    first.resolution_failure.as_deref(),
                    Some("missing or ambiguous target")
                );
            }
            if case == "audience_widening" {
                assert_eq!(first.resolution_failure.as_deref(), Some("audience widening"));
            }
            assert_ne!(service.advance(&"good", &id).unwrap().phase, Phase::Authorized, "{case}");
        }
        assert_eq!(
            service.dispatch_pending(&"good", &mut queue, 1).unwrap().acknowledged,
            0,
            "{case}"
        );
        assert_eq!(queue.engine().projection().task_count(), 0, "{case}");
        assert_eq!(queue.engine().projection().projection_digest().unwrap(), before, "{case}");
        let state = bundle.wi().effect_snapshot(&opaque("tenant:fixture")).unwrap();
        assert!(state.uses.is_empty(), "{case}");
        assert!(state.effects[&id].dispatches.is_empty(), "{case}");
        queue.shutdown().unwrap();
    }
}
