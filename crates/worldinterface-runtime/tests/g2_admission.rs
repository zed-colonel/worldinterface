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
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::{AuthorizationSnapshot, VerifiedSnapshot},
    effect::EffectAction,
    Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::{EffectSubmission, InstanceId, Timestamp};
use worldinterface_runtime::aq_adapter::QueueEngine;
use worldinterface_runtime::{admission, run_discovery, storage::StoreBundle};
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
        serde_json::from_value(data["snapshot"].clone()).unwrap(),
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
    let first = queue.engine_mut().ensure_task(request.clone()).unwrap();
    assert!(first.is_created());
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
    assert!(queue.engine_mut().ensure_task(changed).is_err());
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
        queue.engine_mut().ensure_task(foreign).unwrap();
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
    queue.engine_mut().ensure_task(extra).unwrap();
    assert!(!run_discovery::discover(&queue, &subject, at(), &id, 1).unwrap().complete);
    assert!(bundle.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .discovery_cursor
        .is_none());
    let mut complete = false;
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
    queue.shutdown().unwrap();
    drop(bundle);
    let reopened = StoreBundle::open(&temp.path().join("bundle")).unwrap();
    let recorded =
        &reopened.wi().effect_snapshot(subject.tenant()).unwrap().effects[&id].dispatches[0];
    assert_eq!(recorded.admission.as_ref().unwrap().canonical_digest, binding.canonical_digest);
    assert!(recorded.admission.as_ref().unwrap().acknowledged_sequence.is_some());
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
        let admitted = queue.engine_mut().ensure_task(admissible).unwrap();
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
                        queue.engine_mut().ensure_task(parsed),
                        Err(actionqueue_runtime::control::ServiceError::Authorization(
                            actionqueue_core::control::ControlError::Scope
                        ))
                    ),
                    "{path}"
                );
            } else {
                assert!(
                    matches!(
                        queue.engine_mut().ensure_task(parsed),
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
    queue.engine_mut().ensure_task(parent_request).unwrap();
    let mut child = v1["request"].clone();
    child["task_spec"].as_object_mut().unwrap().remove("tenant_id");
    child["dependencies"] = serde_json::json!([]);
    child["task_spec"]["child_lifecycle_policy"] = "Required".into();
    let required: EnsureTaskRequest = serde_json::from_value(child.clone()).unwrap();
    assert!(queue.engine_mut().ensure_task(required.clone()).unwrap().is_created());
    let before = queue.engine().projection().projection_digest().unwrap();
    child["task_spec"]["child_lifecycle_policy"] = "Detached".into();
    let detached: EnsureTaskRequest = serde_json::from_value(child).unwrap();
    assert_ne!(required.digest().unwrap(), detached.digest().unwrap());
    assert!(matches!(
        queue.engine_mut().ensure_task(detached),
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
        queue
            .engine_mut()
            .ensure_task(admission::load_request(bundle.wi(), &admission).unwrap())
            .unwrap();
        // A hard process exit skips engine shutdown and WI acknowledgment.
        std::process::exit(0);
    }
    for cut in ["after-intent", "after-aq"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("bundle");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "ab_wi_002_subprocess_crash_cuts", "--nocapture"])
            .env(CUT, cut)
            .env(PATH, &root)
            .output()
            .unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
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
