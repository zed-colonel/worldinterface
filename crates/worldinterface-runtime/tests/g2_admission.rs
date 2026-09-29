use actionqueue_core::{
    admission::{canonical::CanonicalAdmissionV2, EnsureTaskRequest},
    bounded::OpaqueRef as AqOpaqueRef,
    causal::ControlMutationContext,
    control::{ControlScope, HostControlContext},
    disposition::AttemptDisposition,
    ids::TaskId,
    task::task_spec::TaskSpec,
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::{AuthorizationSnapshot, VerifiedSnapshot},
    effect::EffectAction,
    Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::{EffectSubmission, InstanceId, Timestamp};
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
    let store =
        actionqueue_storage::store::StoreManifest::read(&bundle.aq_path()).unwrap().store_id;
    let binding =
        admission::prepare(bundle.wi(), &subject, at(), &id, verified(&snapshot), store).unwrap();
    let request = admission::load_request(bundle.wi(), &binding).unwrap();
    assert_eq!(request.task_spec().id().to_string(), binding.task.as_str());
    assert_eq!(
        hex::encode(request.digest().unwrap().hash().bytes()),
        binding.canonical_digest.as_str()
    );
    assert_eq!(
        admission::prepare(bundle.wi(), &subject, at(), &id, verified(&snapshot), store).unwrap(),
        binding
    );
    let prepared = bundle.wi().effect_snapshot(subject.tenant()).unwrap();
    assert_eq!(prepared.uses.values().sum::<u32>(), 1);
    let mut engine = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        Complete,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    // AQ commit without WI acknowledgment: retry uses the persisted request and returns AlreadyExists.
    let first = engine.ensure_task(request.clone()).unwrap();
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
    let unchanged = engine.projection().projection_digest().unwrap();
    assert!(engine.ensure_task(changed).is_err());
    assert_eq!(engine.projection().projection_digest().unwrap(), unchanged);
    let retry = admission::deliver(
        bundle.wi(),
        &subject,
        at(),
        &id,
        verified(&snapshot),
        store,
        &mut engine,
    )
    .unwrap();
    assert!(!retry.is_created());
    assert_eq!(first.sequence(), retry.sequence());
    assert_eq!(first.task_id(), retry.task_id());
    let state = bundle.wi().effect_snapshot(subject.tenant()).unwrap();
    assert_eq!(state.uses.values().sum::<u32>(), 1);
    assert_eq!(
        state.effects[&id].dispatches[0].admission.as_ref().unwrap().acknowledged_sequence,
        Some(first.sequence())
    );
    run_discovery::discover(bundle.wi(), &subject, at(), &id, store, &engine, 8).unwrap();
    let state = bundle.wi().effect_snapshot(subject.tenant()).unwrap();
    assert_eq!(state.effects[&id].dispatches[0].admission.as_ref().unwrap().observed_runs.len(), 1);
    assert_eq!(state.effects[&id].phase, Phase::DispatchPending);
    engine.shutdown().unwrap();
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
            let mut changed = request.clone();
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
            assert_ne!(parsed.digest().unwrap(), baseline, "{path}");
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
        let store =
            actionqueue_storage::store::StoreManifest::read(&bundle.aq_path()).unwrap().store_id;
        let admission =
            admission::prepare(bundle.wi(), &subject, at(), &id, verified(&snapshot), store)
                .unwrap();
        let mut engine = ActionQueueEngine::new(
            RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
            Complete,
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        engine.ensure_task(admission::load_request(bundle.wi(), &admission).unwrap()).unwrap();
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
        let store =
            actionqueue_storage::store::StoreManifest::read(&bundle.aq_path()).unwrap().store_id;
        let admission =
            admission::prepare(bundle.wi(), &subject, at(), &id, verified(&snapshot), store)
                .unwrap();
        let mut engine = ActionQueueEngine::new(
            RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
            Complete,
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        let result = admission::deliver(
            bundle.wi(),
            &subject,
            at(),
            &id,
            verified(&snapshot),
            store,
            &mut engine,
        )
        .unwrap();
        assert_eq!(result.is_created(), cut == "after-intent");
        assert_eq!(engine.projection().task_count(), 1);
        assert_eq!(
            engine.projection().tasks().next().unwrap().id().to_string(),
            admission.task.as_str()
        );
        engine.shutdown().unwrap();
        assert_eq!(
            bundle.wi().effect_snapshot(subject.tenant()).unwrap().uses.values().sum::<u32>(),
            1
        );
    }
}
