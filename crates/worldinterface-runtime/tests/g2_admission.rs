use worldinterface_runtime::connector::{
    Connector as EffectExecutionPort, PreparedInvocation as BoundExecution,
};
#[path = "support/child.rs"]
mod child_guard;
#[path = "support/spi_fixture.rs"]
mod spi_fixture;
use actionqueue_core::{
    admission::{canonical::CanonicalAdmissionV2, EnsureTaskRequest},
    bounded::OpaqueRef as AqOpaqueRef,
    causal::ControlMutationContext,
    control::{ControlScope, HostControlContext},
    disposition::AttemptDisposition,
    ids::{AdmissionKey, CorrelationId, TaskId, TraceId},
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_executor_local::handler::{
    AttemptMetadata, CancellationContext, ExecutorContext, ExecutorHandler, HandlerInput,
};
use child_guard::KillOnDrop;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::{AuthorizationSnapshot, VerifiedSnapshot},
    effect::EffectAction,
    Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::{
    ContentHash, EffectSubmission, EvidenceRef, InstanceId, OpaqueRef, Timestamp,
};
use worldinterface_runtime::aq_adapter::QueueEngine;
use worldinterface_runtime::{
    admission,
    authority::{EvidenceAuthority, TrustedClock},
    completion::{EffectAttemptHandler, EvidenceBytes, PortObservation},
    run_discovery,
    storage::StoreBundle,
    Result,
};
use worldinterface_storage::StoreProfile;

fn at() -> Timestamp {
    "2026-09-21T00:00:00.000000000Z".to_owned().try_into().unwrap()
}
fn fixture() -> (EffectSubmission, AuthorizationSnapshot) {
    let data: serde_json::Value =
        serde_json::from_str(include_str!("../../../conformance/wi-fabric-2/egress/fixture.json"))
            .unwrap();
    (
        serde_json::from_value(data["submission"].clone()).unwrap(),
        spi_fixture::pin(serde_json::from_value(data["snapshot"].clone()).unwrap()),
    )
}
fn verified(s: &AuthorizationSnapshot) -> VerifiedSnapshot {
    VerifiedSnapshot::from_trusted_verifier(s.clone()).unwrap()
}
fn caller(s: &AuthorizationSnapshot) -> AuthenticatedCaller {
    AuthenticatedCaller::from_trusted_authentication(
        s.bindings[0].tenant.clone(),
        s.bindings[0].principal.clone(),
    )
}
fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(AqOpaqueRef::new("trusted-host").unwrap()),
    }
}
struct Complete;
impl ExecutorHandler for Complete {
    fn execute(&self, _: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
struct SafeClock;
impl TrustedClock for SafeClock {
    fn now(&self) -> Result<Timestamp> {
        Ok(at())
    }
}
struct SafeEvidence(AuthorizationSnapshot);
impl EvidenceAuthority for SafeEvidence {
    fn current_snapshot(
        &self,
        _: &OpaqueRef,
        _: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        Ok(self.0.clone())
    }
    fn verify(&self, _: &OpaqueRef, reference: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        if reference == &self.0.facts[0].evidence {
            Ok(self.0.facts[0].attested_document.clone())
        } else {
            Ok(ContentHash::of(b"verified"))
        }
    }
}
struct MockUncertainPort(Arc<AtomicUsize>);
impl EffectExecutionPort for MockUncertainPort {
    fn descriptor(&self) -> &worldinterface_protocol::ConnectorDescriptor {
        spi_fixture::descriptor()
    }
    fn resolve(
        &self,
        r: worldinterface_runtime::connector::ResolveRequest,
    ) -> Result<worldinterface_runtime::connector::ResolveObservation> {
        Ok(worldinterface_runtime::connector::ResolveObservation { resource: r.resource })
    }
    fn prepare(
        &self,
        _: &worldinterface_runtime::connector::AuthorizedInvocation,
    ) -> worldinterface_runtime::connector::PrepareOutcome {
        worldinterface_runtime::connector::PrepareOutcome::Ready
    }
    fn reconcile(
        &self,
        _: worldinterface_runtime::connector::ReconcileRequest,
    ) -> Result<worldinterface_runtime::connector::ReconcileOutcome> {
        Ok(worldinterface_runtime::connector::ReconcileOutcome::Unknown(vec![]))
    }
    fn cancel(
        &self,
        _: worldinterface_runtime::connector::ReconcileRequest,
    ) -> Result<worldinterface_runtime::connector::CancelObservation> {
        Ok(worldinterface_runtime::connector::CancelObservation::Unsupported)
    }

    fn invoke(
        &self,
        _: BoundExecution,
        _: &actionqueue_executor_local::handler::CancellationToken,
    ) -> Result<PortObservation> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(PortObservation {
            commit: worldinterface_domain::effect::CommitObservation::Unknown,
            outcome: worldinterface_protocol::ExecutionOutcome::Uncertain,
            outputs: vec![],
            evidence: vec![EvidenceBytes {
                schema: "mock-port:1".to_string().try_into()?,
                bytes: b"provider commit unknown".to_vec(),
            }],
            boundary_record: None,
        })
    }
}
#[test]
fn ab_wi_002_003_004_persist_retry_ack_and_discover() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("bundle"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    let (submission, snapshot) = fixture();
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
        bundle
            .wi()
            .publish("tenant:fixture".to_owned().try_into().unwrap(), name.as_bytes())
            .unwrap();
    }
    let subject = caller(&snapshot);
    let egress = Egress::new(bundle.wi());
    let id = egress
        .apply(
            &subject,
            at(),
            EffectAction::Submit {
                id: worldinterface_protocol::EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot: verified(&snapshot),
                link: None,
            },
        )
        .unwrap();
    assert_eq!(egress.inspect(&subject, &id).unwrap().phase, Phase::Submitted);
    egress
        .apply(
            &subject,
            at(),
            EffectAction::Resolve { id: id.clone(), snapshot: verified(&snapshot) },
        )
        .unwrap();
    egress
        .apply(
            &subject,
            at(),
            EffectAction::Authorize { id: id.clone(), snapshot: verified(&snapshot) },
        )
        .unwrap();
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    let prepared = std::thread::scope(|scope| {
        let source = &bundle;
        let actor = &subject;
        let effect = &id;
        let jobs: Vec<_> = (0..8)
            .map(|_| {
                let snapshot = verified(&snapshot);
                scope.spawn(move || admission::prepare(source, actor, at(), effect, snapshot))
            })
            .collect();
        jobs.into_iter().map(|job| job.join().unwrap().unwrap()).collect::<Vec<_>>()
    });
    assert!(prepared.iter().all(|item| item == &prepared[0]));
    let binding = prepared[0].clone();
    let request = admission::load_request(bundle.wi(), &binding).unwrap();
    assert_eq!(request.task_spec().constraints().max_attempts(), 3);
    assert_eq!(
        request.task_spec().constraints().safety_level(),
        actionqueue_core::task::safety::SafetyLevel::Idempotent
    );
    assert_eq!(request.task_spec().id().to_string(), binding.task.as_str());
    assert_eq!(
        hex::encode(request.digest().unwrap().hash().bytes()),
        binding.canonical_digest.as_str()
    );
    assert_eq!(
        admission::prepare(&bundle, &subject, at(), &id, verified(&snapshot)).unwrap(),
        binding
    );
    let prepared = bundle.wi().effect_snapshot(subject.tenant()).unwrap();
    assert_eq!(prepared.uses.values().sum::<u32>(), 1);
    // AQ commit without WI acknowledgment: retry uses the persisted request and returns AlreadyExists.
    let first = queue.ensure_task(request.clone()).unwrap();
    assert!(first.is_created());
    // These conflict mutations are relative to the newly approved request bytes.
    for (field, value) in [
        ("max_attempts", serde_json::json!(4)),
        ("safety_level", serde_json::json!("Transactional")),
    ] {
        let mut value_request = serde_json::to_value(&request).unwrap();
        assert_ne!(value_request["task_spec"]["constraints"][field], value);
        value_request["task_spec"]["constraints"][field] = value;
        let mutated: EnsureTaskRequest = serde_json::from_value(value_request).unwrap();
        assert_ne!(mutated.digest().unwrap(), request.digest().unwrap());
        let before = queue.engine().projection().projection_digest().unwrap();
        let error = queue.ensure_task(mutated).unwrap_err();
        assert!(matches!(
            error,
            actionqueue_runtime::control::ServiceError::Admission(
                actionqueue_runtime::admission::AdmissionError::Rejected(
                    actionqueue_core::admission::AdmissionRejection::Conflict { .. }
                )
            )
        ));
        assert_eq!(queue.engine().projection().projection_digest().unwrap(), before);
    }
    let changed = EnsureTaskRequest::new(
        request.admission_key().clone(),
        TaskSpec::new(
            TaskId::new(),
            request.task_spec().task_payload().clone(),
            request.task_spec().run_policy().clone(),
            request.task_spec().constraints().clone(),
            request.task_spec().metadata().clone(),
        )
        .unwrap(),
        request.dependencies().to_vec(),
        request.causal_context().clone(),
        request.control_context().cloned(),
    )
    .unwrap();
    let unchanged = queue.engine().projection().projection_digest().unwrap();
    assert!(queue.ensure_task(changed).is_err());
    assert_eq!(queue.engine().projection().projection_digest().unwrap(), unchanged);
    let mut revoked = snapshot.clone();
    revoked.grants[0].revoked = true;
    assert!(admission::deliver(&mut queue, &subject, at(), &id, verified(&revoked)).is_err());
    assert_eq!(queue.engine().projection().projection_digest().unwrap(), unchanged);
    assert!(bundle.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .acknowledged_sequence
        .is_none());
    // AQ must reauthorize the identical persisted request even after its first
    // commit. WI authorization remains valid throughout this host-control cut.
    queue.set_host_control_context(None);
    let aq_before = queue.engine().projection().projection_digest().unwrap();
    let denied = admission::deliver(&mut queue, &subject, at(), &id, verified(&snapshot))
        .expect_err("AQ must reauthorize the unchanged request");
    assert!(matches!(
        denied.downcast_ref::<actionqueue_runtime::control::ServiceError>(),
        Some(actionqueue_runtime::control::ServiceError::Authorization(_))
    ));
    assert_eq!(queue.engine().projection().projection_digest().unwrap(), aq_before);
    assert_eq!(queue.engine().projection().task_count(), 1);
    assert!(bundle.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .acknowledged_sequence
        .is_none());
    queue.set_host_control_context(Some(host()));
    let retry = std::thread::scope(|scope| {
        let source = &bundle;
        let actor = &subject;
        let effect = &id;
        let fresh = verified(&snapshot);
        let competing = scope.spawn(move || admission::prepare(source, actor, at(), effect, fresh));
        let result =
            admission::deliver(&mut queue, &subject, at(), &id, verified(&snapshot)).unwrap();
        assert_eq!(competing.join().unwrap().unwrap(), binding);
        result
    });
    assert!(!retry.is_created());
    assert_eq!(first.sequence(), retry.sequence());
    assert_eq!(first.task_id(), retry.task_id());
    let state = bundle.wi().effect_snapshot(subject.tenant()).unwrap();
    assert_eq!(state.uses.values().sum::<u32>(), 1);
    assert_eq!(
        state.effects[&id].dispatches[0].admission.as_ref().unwrap().acknowledged_sequence,
        Some(first.sequence())
    );
    // The exact effect query still has to make progress when many visible runs
    // share its correlation. Force the effect task beyond the first AQ page.
    let correlation = CorrelationId::new(format!("wi/effect/{}", id.as_str())).unwrap();
    for n in 1..=33_u64 {
        let foreign_id: TaskId = format!("00000000-0000-4000-8000-{n:012x}").parse().unwrap();
        let foreign = EnsureTaskRequest::new(
            AdmissionKey::new(format!("unrelated/{n}")).unwrap(),
            TaskSpec::new(
                foreign_id,
                TaskPayload::new(vec![n as u8]),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .unwrap(),
            vec![],
            actionqueue_core::causal::CausalContext::new(
                TraceId::new(format!("unrelated/{n}")).unwrap(),
                correlation.clone(),
            ),
            None,
        )
        .unwrap();
        queue.ensure_task(foreign).unwrap();
    }
    let first_page = run_discovery::discover(&queue, &subject, at(), &id, 1).unwrap();
    assert!(!first_page.complete);
    let cursor = bundle.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .discovery_cursor
        .clone();
    assert!(cursor.is_some());
    queue.shutdown().unwrap();
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    // A queue revision invalidates AQ's cursor. The persisted cursor is cleared
    // and the next bounded call restarts without losing already observed runs.
    let extra = EnsureTaskRequest::new(
        AdmissionKey::new("unrelated/stale").unwrap(),
        TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(vec![0]),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap(),
        vec![],
        actionqueue_core::causal::CausalContext::new(
            TraceId::new("unrelated/stale").unwrap(),
            correlation,
        ),
        None,
    )
    .unwrap();
    queue.ensure_task(extra).unwrap();
    assert!(!run_discovery::discover(&queue, &subject, at(), &id, 1).unwrap().complete);
    assert!(bundle.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .discovery_cursor
        .is_none());
    let mut complete = false;
    // Even acknowledged tasks require current host authorization for run inspection.
    let before_inspection = bundle.wi().effect_snapshot(subject.tenant()).unwrap();
    let before_projection = queue.engine().projection().projection_digest().unwrap();
    queue.set_host_control_context(None);
    assert!(run_discovery::discover(&queue, &subject, at(), &id, 1).is_err());
    assert_eq!(bundle.wi().effect_snapshot(subject.tenant()).unwrap(), before_inspection);
    assert_eq!(queue.engine().projection().projection_digest().unwrap(), before_projection);
    queue.set_host_control_context(Some(host()));
    for _ in 0..4 {
        complete = run_discovery::discover(&queue, &subject, at(), &id, 1).unwrap().complete;
        if complete {
            break;
        }
    }
    assert!(complete);
    let state = bundle.wi().effect_snapshot(subject.tenant()).unwrap();
    assert_eq!(state.effects[&id].dispatches[0].admission.as_ref().unwrap().observed_runs.len(), 1);
    assert_eq!(state.effects[&id].phase, Phase::DispatchPending);
    let run = state.effects[&id].dispatches[0].admission.as_ref().unwrap().observed_runs[0]
        .as_str()
        .parse()
        .unwrap();
    queue.shutdown().unwrap();
    let bundle = Arc::new(bundle);
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: bundle.clone(),
        port: MockUncertainPort(calls.clone()),
        evidence: SafeEvidence(snapshot.clone()),
        clock: SafeClock,
    };
    let make_context = || ExecutorContext {
        input: HandlerInput {
            resume_context: None,
            causal_context: None,
            run_id: run,
            attempt_id: actionqueue_core::ids::AttemptId::new(),
            payload: request.task_spec().task_payload().bytes().to_vec(),
            metadata: AttemptMetadata {
                failure_attempt_count: 0,
                max_attempts: 3,
                attempt_number: 1,
                timeout_secs: Some(60),
                safety_level: actionqueue_core::task::safety::SafetyLevel::Idempotent,
            },
            cancellation_context: CancellationContext::new(),
        },
        children: None,
    };
    let mut forged_run = make_context();
    forged_run.input.run_id = actionqueue_core::ids::RunId::new();
    assert!(handler.execute_bound(&forged_run).is_err());
    let mut forged_payload = make_context();
    forged_payload.input.payload = b"forged task payload".to_vec();
    assert!(handler.execute_bound(&forged_payload).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    handler.execute_bound(&make_context()).unwrap();
    handler.execute_bound(&make_context()).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let receipts = bundle.wi().effect_receipts(subject.tenant(), &id).unwrap();
    assert_eq!(receipts.len(), 1);
    let crypto = tempfile::tempdir().unwrap();
    let key = crypto.path().join("key.pem");
    let public = crypto.path().join("public.pem");
    let input = crypto.path().join("input.bin");
    let signature = crypto.path().join("signature.bin");
    let openssl = |args: &[&std::ffi::OsStr]| {
        assert!(std::process::Command::new("openssl").args(args).status().unwrap().success());
    };
    openssl(&[
        "genpkey".as_ref(),
        "-algorithm".as_ref(),
        "Ed25519".as_ref(),
        "-out".as_ref(),
        key.as_os_str(),
    ]);
    openssl(&[
        "pkey".as_ref(),
        "-in".as_ref(),
        key.as_os_str(),
        "-pubout".as_ref(),
        "-out".as_ref(),
        public.as_os_str(),
    ]);
    let public_artifact =
        bundle.wi().publish(subject.tenant().clone(), &std::fs::read(&public).unwrap()).unwrap();
    let mut signing = worldinterface_protocol::receipt::SigningProofV1 {
        version: 1,
        receipt_digest: receipts[0].1.clone(),
        suite: "ed25519:1".to_owned().try_into().unwrap(),
        purpose: "receipt-provenance".to_owned().try_into().unwrap(),
        signed_at: at(),
        key_state: EvidenceRef {
            artifact: public_artifact,
            schema: "ed25519-public-key:1".to_owned().try_into().unwrap(),
        },
        signature: worldinterface_protocol::ArtifactRef {
            scope: subject.tenant().clone(),
            content: ContentHash::of(b"placeholder"),
            size: 11,
        },
    };
    std::fs::write(&input, signing.signing_input().unwrap()).unwrap();
    openssl(&[
        "pkeyutl".as_ref(),
        "-sign".as_ref(),
        "-inkey".as_ref(),
        key.as_os_str(),
        "-rawin".as_ref(),
        "-in".as_ref(),
        input.as_os_str(),
        "-out".as_ref(),
        signature.as_os_str(),
    ]);
    openssl(&[
        "pkeyutl".as_ref(),
        "-verify".as_ref(),
        "-pubin".as_ref(),
        "-inkey".as_ref(),
        public.as_os_str(),
        "-rawin".as_ref(),
        "-in".as_ref(),
        input.as_os_str(),
        "-sigfile".as_ref(),
        signature.as_os_str(),
    ]);
    signing.signature =
        bundle.wi().publish(subject.tenant().clone(), &std::fs::read(&signature).unwrap()).unwrap();
    bundle.wi().append_receipt_proof(subject.tenant(), &receipts[0].0, &signing).unwrap();
    let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        subject.tenant().clone(),
        subject.principal().clone(),
        worldinterface_domain::receipt::DisclosureProfile::Full,
    );
    assert_eq!(bundle.wi().receipt_proofs(&access, &receipts[0].0).unwrap().len(), 1);
    assert_eq!(bundle.wi().effect_receipts(subject.tenant(), &id).unwrap()[0].1, receipts[0].1);
    let proof = worldinterface_protocol::receipt::ProofBundleV1 {
        version: 1,
        kind: worldinterface_protocol::receipt::ProofBundleKind::Trial,
        candidate: worldinterface_protocol::CandidateId::generate(),
        records: vec![receipts[0].1.clone()],
        evidence: vec![],
    };
    let bundle_id: OpaqueRef = "proof:fixture".to_owned().try_into().unwrap();
    assert!(bundle
        .wi()
        .append_proof_bundle(&"tenant:other".to_owned().try_into().unwrap(), &bundle_id, &proof)
        .is_err());
    bundle.wi().append_proof_bundle(subject.tenant(), &bundle_id, &proof).unwrap();
    assert!(bundle
        .wi()
        .append_diagnostic_reference(
            &"tenant:other".to_owned().try_into().unwrap(),
            true,
            &"trace:foreign".to_owned().try_into().unwrap(),
            &receipts[0].0,
            &"short".to_owned().try_into().unwrap()
        )
        .is_err());
    bundle
        .wi()
        .append_diagnostic_reference(
            subject.tenant(),
            true,
            &"trace:fixture".to_owned().try_into().unwrap(),
            &receipts[0].0,
            &"short".to_owned().try_into().unwrap(),
        )
        .unwrap();
    drop(handler);
    drop(bundle);
    let reopened = StoreBundle::open(&temp.path().join("bundle")).unwrap();
    let recorded =
        &reopened.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches[0];
    assert_eq!(recorded.admission.as_ref().unwrap().canonical_digest, binding.canonical_digest);
    assert!(recorded.admission.as_ref().unwrap().acknowledged_sequence.is_some());
    assert_eq!(
        recorded.admission.as_ref().unwrap().observed_runs,
        state.effects[&id].dispatches[0].admission.as_ref().unwrap().observed_runs
    );
}

#[test]
fn ab_wi_003_public_v2_vectors_and_digest_field_boundaries() {
    let v1: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/wi-fabric-2/integration/aq-admission-v1-vector.json"
    ))
    .unwrap();
    let v2: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/wi-fabric-2/integration/aq-admission-v2-vector.json"
    ))
    .unwrap();
    for policy in ["Required", "Detached"] {
        let mut request = v1["request"].clone();
        request["task_spec"]["child_lifecycle_policy"] = policy.into();
        let parsed: EnsureTaskRequest = serde_json::from_value(request.clone()).unwrap();
        let canonical = CanonicalAdmissionV2::new(&parsed).unwrap();
        assert_eq!(hex::encode(canonical.bytes()), v2[policy]["canonical_hex"]);
        assert_eq!(hex::encode(parsed.digest().unwrap().hash().bytes()), v2[policy]["sha256"]);
        let baseline = parsed.digest().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let bundle = StoreBundle::initialize(
            &temp.path().join("bundle"),
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap();
        let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
        let mut admissible = request.clone();
        admissible["task_spec"].as_object_mut().unwrap().remove("tenant_id");
        admissible["task_spec"].as_object_mut().unwrap().remove("parent_task_id");
        admissible["task_spec"].as_object_mut().unwrap().remove("child_lifecycle_policy");
        admissible["dependencies"] = serde_json::json!([]);
        let admissible_json = admissible;
        let admissible: EnsureTaskRequest =
            serde_json::from_value(admissible_json.clone()).unwrap();
        let runtime_digest = admissible.digest().unwrap();
        let admitted = queue.ensure_task(admissible).unwrap();
        assert!(admitted.is_created());
        let mutations: Vec<(&str, serde_json::Value)> = vec![
            ("task_spec.id", serde_json::json!("00000000-0000-0000-0000-000000000009")),
            ("task_spec.payload.bytes", serde_json::json!([0, 1, 254])),
            ("task_spec.payload.content_type", serde_json::json!("application/json")),
            ("task_spec.run_policy.Repeat.count", serde_json::json!(3)),
            ("task_spec.run_policy.Repeat.interval_secs", serde_json::json!(61)),
            ("task_spec.constraints.max_attempts", serde_json::json!(4)),
            ("task_spec.constraints.timeout_secs", serde_json::json!(31)),
            ("task_spec.constraints.concurrency_key", serde_json::json!("other-lock")),
            (
                "task_spec.constraints.concurrency_key_hold_policy",
                serde_json::json!("HoldDuringRetry"),
            ),
            ("task_spec.constraints.safety_level", serde_json::json!("Idempotent")),
            ("task_spec.constraints.required_executor_traits", serde_json::json!(["cpu"])),
            ("task_spec.metadata.priority", serde_json::json!(-6)),
            ("task_spec.metadata.description", serde_json::json!("different")),
            ("task_spec.metadata.tags", serde_json::json!(["other"])),
            ("task_spec.parent_task_id", serde_json::json!("00000000-0000-0000-0000-000000000009")),
            ("task_spec.tenant_id", serde_json::json!("00000000-0000-0000-0000-000000000009")),
            ("dependencies", serde_json::json!(["00000000-0000-0000-0000-000000000009"])),
            ("causal_context.trace_id", serde_json::json!("changed")),
            ("causal_context.correlation_id", serde_json::json!("changed")),
            (
                "causal_context.causation.parent_task_id",
                serde_json::json!("00000000-0000-0000-0000-000000000009"),
            ),
            (
                "causal_context.causation.parent_run_id",
                serde_json::json!("00000000-0000-0000-0000-000000000009"),
            ),
            (
                "causal_context.causation.parent_attempt_id",
                serde_json::json!("00000000-0000-0000-0000-000000000009"),
            ),
            ("causal_context.causation.external_ref", serde_json::json!("changed")),
            ("causal_context.submitting_principal_ref", serde_json::json!("changed")),
            ("causal_context.requesting_actor_ref", serde_json::json!("changed")),
            ("causal_context.purpose_ref", serde_json::json!("changed")),
            ("causal_context.authorization_ref", serde_json::json!("new-auth")),
            ("causal_context.identity_context_ref", serde_json::json!("changed")),
            ("causal_context.signed_statement_ref", serde_json::json!("changed")),
            ("causal_context.proof_context_ref", serde_json::json!("changed")),
            ("causal_context.origin_ref", serde_json::json!("changed")),
        ];
        for (path, replacement) in mutations {
            let mut changed = admissible_json.clone();
            let mut node = &mut changed;
            let mut parts = path.split('.').peekable();
            while let Some(part) = parts.next() {
                if parts.peek().is_some() {
                    node = &mut node[part];
                } else {
                    node[part] = replacement.clone();
                }
            }
            let parsed: EnsureTaskRequest = serde_json::from_value(changed).unwrap();
            assert_ne!(parsed.digest().unwrap(), runtime_digest, "{path}");
            let before = queue.engine().projection().projection_digest().unwrap();
            if path == "task_spec.tenant_id" {
                // An approved AB-WI-003 exception: AQ checks the host tenant scope
                // before looking up the same-key request for a digest conflict.
                assert!(
                    matches!(
                        queue.ensure_task(parsed),
                        Err(actionqueue_runtime::control::ServiceError::Authorization(
                            actionqueue_core::control::ControlError::Scope
                        ))
                    ),
                    "{path}"
                );
            } else {
                assert!(
                    matches!(
                        queue.ensure_task(parsed),
                        Err(actionqueue_runtime::control::ServiceError::Admission(
                            actionqueue_runtime::admission::AdmissionError::Rejected(
                                actionqueue_core::admission::AdmissionRejection::Conflict { .. }
                            )
                        ))
                    ),
                    "{path}"
                );
            }
            assert_eq!(queue.engine().projection().projection_digest().unwrap(), before, "{path}");
        }
        let mut excluded = request.clone();
        excluded["admission_key"] = serde_json::json!("changed-key");
        excluded["control_context"] = serde_json::json!({"caller_ref":"changed","host_session_ref":null,"request_id":null,"reason_code":null});
        // Key is excluded. AQ's control context is host attribution and is separately authorized.
        let parsed: EnsureTaskRequest = serde_json::from_value(excluded).unwrap();
        assert_eq!(parsed.digest().unwrap(), baseline);

        let mut reordered = request.clone();
        reordered["task_spec"]["metadata"]["tags"] = serde_json::json!(["a", "z", "a"]);
        let parsed: EnsureTaskRequest = serde_json::from_value(reordered).unwrap();
        assert_eq!(parsed.digest().unwrap(), baseline, "metadata tags are normalized");
    }
}

#[test]
fn ab_wi_003_required_and_detached_same_key_conflict() {
    let v1: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/wi-fabric-2/integration/aq-admission-v1-vector.json"
    ))
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("bundle"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    let parent: TaskId = "00000000-0000-0000-0000-000000000002".parse().unwrap();
    let parent_request = EnsureTaskRequest::new(
        AdmissionKey::new("parent").unwrap(),
        TaskSpec::new(
            parent,
            TaskPayload::new(vec![1]),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap(),
        vec![],
        actionqueue_core::causal::CausalContext::new(
            TraceId::new("parent").unwrap(),
            CorrelationId::new("parent").unwrap(),
        ),
        None,
    )
    .unwrap();
    queue.ensure_task(parent_request).unwrap();
    let mut child = v1["request"].clone();
    child["task_spec"].as_object_mut().unwrap().remove("tenant_id");
    child["dependencies"] = serde_json::json!([]);
    child["task_spec"]["child_lifecycle_policy"] = "Required".into();
    let required: EnsureTaskRequest = serde_json::from_value(child.clone()).unwrap();
    assert!(queue.ensure_task(required.clone()).unwrap().is_created());
    let before = queue.engine().projection().projection_digest().unwrap();
    child["task_spec"]["child_lifecycle_policy"] = "Detached".into();
    let detached: EnsureTaskRequest = serde_json::from_value(child).unwrap();
    assert_ne!(required.digest().unwrap(), detached.digest().unwrap());
    assert!(matches!(
        queue.ensure_task(detached),
        Err(actionqueue_runtime::control::ServiceError::Admission(
            actionqueue_runtime::admission::AdmissionError::Rejected(
                actionqueue_core::admission::AdmissionRejection::Conflict { .. }
            )
        ))
    ));
    assert_eq!(queue.engine().projection().projection_digest().unwrap(), before);
    queue.shutdown().unwrap();
}

#[test]
fn wrong_queue_store_cannot_acknowledge_or_discover() {
    let temp = tempfile::tempdir().unwrap();
    let profile =
        || StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false };
    let source = StoreBundle::initialize(&temp.path().join("source"), profile()).unwrap();
    let other = StoreBundle::initialize(&temp.path().join("other"), profile()).unwrap();
    let (submission, snapshot) = fixture();
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
        source
            .wi()
            .publish("tenant:fixture".to_owned().try_into().unwrap(), name.as_bytes())
            .unwrap();
    }
    let subject = caller(&snapshot);
    let egress = Egress::new(source.wi());
    let id = egress
        .apply(
            &subject,
            at(),
            EffectAction::Submit {
                id: worldinterface_protocol::EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot: verified(&snapshot),
                link: None,
            },
        )
        .unwrap();
    egress
        .apply(
            &subject,
            at(),
            EffectAction::Resolve { id: id.clone(), snapshot: verified(&snapshot) },
        )
        .unwrap();
    egress
        .apply(
            &subject,
            at(),
            EffectAction::Authorize { id: id.clone(), snapshot: verified(&snapshot) },
        )
        .unwrap();
    let binding = admission::prepare(&source, &subject, at(), &id, verified(&snapshot)).unwrap();
    let mut wrong = QueueEngine::bootstrap(&other, Complete, host()).unwrap();
    assert_ne!(binding.queue_store.as_str(), wrong.store_id().to_string());
    assert!(admission::deliver(&mut wrong, &subject, at(), &id, verified(&snapshot)).is_err());
    assert!(run_discovery::discover(&wrong, &subject, at(), &id, 1).is_err());
    assert_eq!(wrong.engine().projection().task_count(), 0);
    assert!(source.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .acknowledged_sequence
        .is_none());
    // Public admission acts on this handle's own AQ store. Even if the same
    // request is admitted there, WI cannot acknowledge or discover it through
    // a handle bound to another bundle.
    let foreign_request = admission::load_request(source.wi(), &binding).unwrap();
    assert!(wrong.ensure_task(foreign_request).unwrap().is_created());
    assert_eq!(wrong.engine().projection().task_count(), 1);
    assert!(admission::deliver(&mut wrong, &subject, at(), &id, verified(&snapshot)).is_err());
    assert!(run_discovery::discover(&wrong, &subject, at(), &id, 1).is_err());
    assert!(source.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .acknowledged_sequence
        .is_none());
    wrong.shutdown().unwrap();
}

#[test]
fn ab_wi_002_subprocess_crash_cuts() {
    const CUT: &str = "WI_G2_CRASH_CUT";
    const PATH: &str = "WI_G2_CRASH_PATH";
    if let Ok(cut) = std::env::var(CUT) {
        let root = std::path::PathBuf::from(std::env::var(PATH).unwrap());
        let bundle = StoreBundle::initialize(
            &root,
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap();
        let (submission, snapshot) = fixture();
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
            bundle
                .wi()
                .publish("tenant:fixture".to_owned().try_into().unwrap(), name.as_bytes())
                .unwrap();
        }
        let subject = caller(&snapshot);
        let egress = Egress::new(bundle.wi());
        let id = egress
            .apply(
                &subject,
                at(),
                EffectAction::Submit {
                    id: worldinterface_protocol::EffectIntentId::generate(),
                    submission: Box::new(submission),
                    snapshot: verified(&snapshot),
                    link: None,
                },
            )
            .unwrap();
        if cut == "after-intent" {
            std::process::exit(0);
        }
        egress
            .apply(
                &subject,
                at(),
                EffectAction::Resolve { id: id.clone(), snapshot: verified(&snapshot) },
            )
            .unwrap();
        egress
            .apply(
                &subject,
                at(),
                EffectAction::Authorize { id: id.clone(), snapshot: verified(&snapshot) },
            )
            .unwrap();
        let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
        let admission =
            admission::prepare(&bundle, &subject, at(), &id, verified(&snapshot)).unwrap();
        queue.ensure_task(admission::load_request(bundle.wi(), &admission).unwrap()).unwrap();
        // A hard process exit skips engine shutdown and WI acknowledgment.
        std::process::exit(0);
    }
    for cut in ["after-intent", "after-aq"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("bundle");
        let stderr = temp.path().join("child-stderr");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "ab_wi_002_subprocess_crash_cuts", "--nocapture"])
            .env(CUT, cut)
            .env(PATH, &root)
            .stdout(std::process::Stdio::null())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .spawn()
            .map(KillOnDrop)
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "admission crash helper timed out at {cut}"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        };
        assert!(status.success(), "{}", std::fs::read_to_string(stderr).unwrap());
        let bundle = StoreBundle::open(&root).unwrap();
        let (_, snapshot) = fixture();
        let subject = caller(&snapshot);
        let state = bundle.wi().effect_snapshot(subject.tenant()).unwrap();
        let id = state.effects.keys().next().unwrap().clone();
        if cut == "after-intent" {
            assert_eq!(state.effects[&id].phase, Phase::Submitted);
            let egress = Egress::new(bundle.wi());
            egress
                .apply(
                    &subject,
                    at(),
                    EffectAction::Resolve { id: id.clone(), snapshot: verified(&snapshot) },
                )
                .unwrap();
            egress
                .apply(
                    &subject,
                    at(),
                    EffectAction::Authorize { id: id.clone(), snapshot: verified(&snapshot) },
                )
                .unwrap();
        }
        let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
        let admission =
            admission::prepare(&bundle, &subject, at(), &id, verified(&snapshot)).unwrap();
        let result =
            admission::deliver(&mut queue, &subject, at(), &id, verified(&snapshot)).unwrap();
        assert_eq!(result.is_created(), cut == "after-intent");
        assert_eq!(queue.engine().projection().task_count(), 1);
        assert_eq!(
            queue.engine().projection().tasks().next().unwrap().id().to_string(),
            admission.task.as_str()
        );
        queue.shutdown().unwrap();
        assert_eq!(
            bundle.wi().effect_snapshot(subject.tenant()).unwrap().uses.values().sum::<u32>(),
            1
        );
    }
}

#[test]
fn persisted_old_policy_request_is_retried_without_rewriting() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("bundle"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    let (submission, snapshot) = fixture();
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
        bundle
            .wi()
            .publish("tenant:fixture".to_owned().try_into().unwrap(), name.as_bytes())
            .unwrap();
    }
    let subject = caller(&snapshot);
    let egress = Egress::new(bundle.wi());
    let id = egress
        .apply(
            &subject,
            at(),
            EffectAction::Submit {
                id: worldinterface_protocol::EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot: verified(&snapshot),
                link: None,
            },
        )
        .unwrap();
    assert_eq!(egress.inspect(&subject, &id).unwrap().phase, Phase::Submitted);
    egress
        .apply(
            &subject,
            at(),
            EffectAction::Resolve { id: id.clone(), snapshot: verified(&snapshot) },
        )
        .unwrap();
    egress
        .apply(
            &subject,
            at(),
            EffectAction::Authorize { id: id.clone(), snapshot: verified(&snapshot) },
        )
        .unwrap();

    let state = bundle.wi().effect_snapshot(subject.tenant()).unwrap();
    let effect = &state.effects[&id];
    let execution = worldinterface_domain::ingress::digest(&(
        &effect.intent,
        &effect.resolution,
        effect.authorization.as_ref().unwrap(),
        verified(&snapshot).facts(),
        0u32,
    ))
    .unwrap();
    let task = TaskId::new();
    let key = AdmissionKey::new(format!(
        "wi-effect:{}",
        worldinterface_domain::ingress::digest(&(subject.tenant(), &id, 0u32)).unwrap().as_str()
    ))
    .unwrap();
    let mut constraints = TaskConstraints::new(1, Some(30), None).unwrap();
    constraints.set_safety_level(actionqueue_core::task::safety::SafetyLevel::Transactional);
    let spec = TaskSpec::new(
        task,
        worldinterface_runtime::aq_adapter::TaskInputLocator::new(
            effect.intent.submission.input.artifact.clone(),
        )
        .payload()
        .unwrap(),
        RunPolicy::Once,
        constraints,
        TaskMetadata::default(),
    )
    .unwrap();
    let causal = actionqueue_core::causal::CausalContext::new(
        TraceId::new(format!("wi/effect/{}", id.as_str())).unwrap(),
        CorrelationId::new(format!("wi/effect/{}", id.as_str())).unwrap(),
    )
    .with_authorization_ref(
        AqOpaqueRef::new(effect.authorization.as_ref().unwrap().exact_binding.as_str()).unwrap(),
    )
    .with_submitting_principal_ref(AqOpaqueRef::new(subject.principal().as_str()).unwrap());
    let request = EnsureTaskRequest::new(key, spec, vec![], causal, None).unwrap();
    let bytes = serde_json::to_vec(&request).unwrap();
    let old = worldinterface_domain::effect::ProductionAdmission {
        child: None,
        version: 1,
        queue_store: bundle.aq_store_id().unwrap().to_string().try_into().unwrap(),
        task: task.to_string().try_into().unwrap(),
        admission_key: request.admission_key().as_str().to_owned().try_into().unwrap(),
        execution,
        request_artifact: bundle.wi().publish(subject.tenant().clone(), &bytes).unwrap(),
        request_hash: ContentHash::of(&bytes),
        canonical_version: request.digest().unwrap().canonical_version(),
        canonical_digest: hex::encode(request.digest().unwrap().hash().bytes()).try_into().unwrap(),
        acknowledged_sequence: None,
        observed_runs: vec![],
        discovery_cursor: None,
    };
    egress
        .apply(
            &subject,
            at(),
            EffectAction::PrepareProduction {
                id: id.clone(),
                snapshot: verified(&snapshot),
                admission: Box::new(old.clone()),
            },
        )
        .unwrap();
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    let created = queue.ensure_task(request.clone()).unwrap();
    queue.shutdown().unwrap();
    drop(bundle);
    let bundle = StoreBundle::open(&temp.path().join("bundle")).unwrap();
    assert_eq!(admission::prepare(&bundle, &subject, at(), &id, verified(&snapshot)).unwrap(), old);
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    let recovered =
        admission::deliver(&mut queue, &subject, at(), &id, verified(&snapshot)).unwrap();
    assert!(!recovered.is_created());
    assert_eq!(created.sequence(), recovered.sequence());
    let persisted = bundle.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches
        [0]
    .admission
    .as_ref()
    .unwrap()
    .clone();
    assert_eq!(bundle.wi().read_artifact(&persisted.request_artifact).unwrap(), bytes);
    assert_eq!(persisted.canonical_digest, old.canonical_digest);
    assert_eq!(persisted.admission_key, old.admission_key);
    assert_eq!(persisted.task, old.task);
    let loaded = admission::load_request(bundle.wi(), &persisted).unwrap();
    assert_eq!(loaded.task_spec().constraints().max_attempts(), 1);
    assert_eq!(
        loaded.task_spec().constraints().safety_level(),
        actionqueue_core::task::safety::SafetyLevel::Transactional
    );
    queue.shutdown().unwrap();
}
