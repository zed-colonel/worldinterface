use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};
use worldinterface_protocol::ContentHash;
use worldinterface_worker::{
    artifacts::*, controls::*, credential::*, lifecycle::*, mapping::*, ports::*, profile::*, *,
};

struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
fn ready<F: Future>(future: F) -> F::Output {
    let waker: Waker = Waker::from(Arc::new(Noop));
    let mut ctx = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut ctx) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("scripted provider pending"),
    }
}
fn hash(s: &str) -> ContentHash {
    ContentHash::of(s.as_bytes())
}
fn request(op: Operation) -> WorkerRequestV1 {
    WorkerRequestV1 {
        schema: 1,
        profile: ProfileBinding::process_proof(),
        operation: op,
        provider: "provider-opaque".into(),
        run: "run-opaque".into(),
        request: "request-opaque".into(),
        input_digest: hash("coding-input"),
        controls_digest: hash("controls"),
    }
}
#[test]
fn wp_001_profile_and_binding_drift() {
    let profile_source =
        include_bytes!("../../../../contracts/sources/wi-fabric-2-worker-execution-profile.yaml");
    assert_eq!(ContentHash::of(profile_source).as_str(), DEFINITION_SHA256);
    let r = request(Operation::Start);
    r.validate().unwrap();
    let mut weak = r.clone();
    weak.profile.independent_worker_vm = true;
    assert!(weak.validate().is_err());
    weak = r.clone();
    weak.profile.definition_sha256 = hash("weakened").as_str().into();
    assert!(weak.validate().is_err());
    let d = hash("request-content");
    let resolved = ResolvedWorkerV1 {
        schema: 1,
        request_digest: d.clone(),
        provider: r.provider.clone(),
        run: r.run.clone(),
        adapter_digest: hash("adapter"),
        executable_digest: hash("executable"),
        dependencies_digest: hash("dependencies"),
        configuration_digest: hash("configuration"),
        authorization_digest: hash("authorization"),
        controls_digest: r.controls_digest.clone(),
    };
    resolved.validate_for(&r, &d).unwrap();
    assert!(resolved.validate_for(&r, &hash("changed-content")).is_err());
    let mut changed = resolved;
    changed.configuration_digest = hash("changed-config");
    assert_ne!(encode(&changed).unwrap(), encode(&r).unwrap());
}
#[test]
fn wp_003_independent_vm_cannot_inherit_enclosing_kata() {
    let a = Assurance {
        mediation: Mediation::Embedded,
        enclosing_instance: Some("kata-instance".into()),
        independent_worker_vm_required: true,
        hostile_code_containment_required: false,
    };
    assert!(a.validate().is_err());
}
#[test]
fn wp_008_mediation_does_not_elevate_containment() {
    for mediation in [Mediation::Embedded, Mediation::Delegated, Mediation::External] {
        let a = Assurance {
            mediation,
            enclosing_instance: Some("kata-instance".into()),
            independent_worker_vm_required: false,
            hostile_code_containment_required: true,
        };
        assert!(a.validate().is_err());
    }
}
struct Scripted;
impl WorkerPort for Scripted {
    fn submit<'a>(&'a self, r: &'a WorkerRequestV1) -> PortFuture<'a, Acknowledgment> {
        Box::pin(async move {
            Ok(if r.operation.available() {
                Acknowledgment::Accepted
            } else {
                Acknowledgment::Unavailable
            })
        })
    }
    fn observe<'a>(&'a self, provider: &'a str, run: &'a str) -> PortFuture<'a, Vec<Observation>> {
        Box::pin(async move {
            Ok(vec![Observation {
                provider: provider.into(),
                run: run.into(),
                event: "observed".into(),
                sequence: 1,
                kind: ObservationKind::Running,
            }])
        })
    }
    fn read_artifact<'a>(&'a self, reference: &'a str) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async move { Ok(reference.as_bytes().to_vec()) })
    }
}
#[test]
fn wp_004_opaque_consumer_artifacts_and_controls() {
    let p = Scripted;
    let r = request(Operation::Start);
    assert_eq!(ready(p.submit(&r)).unwrap(), Acknowledgment::Accepted);
    let obs = ready(p.observe(&r.provider, &r.run)).unwrap();
    assert_eq!(obs[0].run, r.run);
    let bytes = ready(p.read_artifact("coding-result")).unwrap();
    let export = ExportV1 {
        reference: "coding-result".into(),
        relative_name: "out/result.txt".into(),
        digest: ContentHash::of(&bytes),
        size: bytes.len() as u64,
        required_verifiers: vec!["integrity".into()],
        passed_verifiers: vec!["integrity".into()],
    };
    let manifest = ExportManifestV1 {
        schema: 1,
        provider: r.provider.clone(),
        run: r.run.clone(),
        owner: "principal".into(),
        exports: vec![export.clone()],
    };
    manifest.validate(&r.provider, &r.run, "principal", &["coding-result".into()]).unwrap();
    verify_content(&export, &bytes).unwrap();
    assert!(verify_content(&export, b"wrong").is_err());
    let mut noncoding = manifest;
    noncoding.exports[0].reference = "image-result".into();
    noncoding.exports[0].relative_name = "out/image.png".into();
    noncoding.validate(&r.provider, &r.run, "principal", &["image-result".into()]).unwrap();
    noncoding.exports[0].relative_name = "../escape".into();
    assert!(noncoding
        .validate(&r.provider, &r.run, "principal", &["image-result".into()])
        .is_err());
    let finite = |kind| Control { kind, mandatory: true, state: ControlState::Supported(100) };
    let mut controls = ControlEvidenceV1 {
        schema: 1,
        provider: r.provider,
        run: r.run,
        requested: vec![finite(ControlKind::WallTimeMs)],
        advertised: vec![],
        applied: vec![finite(ControlKind::WallTimeMs), finite(ControlKind::OutputBytes)],
        observed: vec![],
    };
    controls.validate().unwrap();
    controls.requested.push(Control {
        kind: ControlKind::Network,
        mandatory: true,
        state: ControlState::Unknown,
    });
    assert!(controls.validate().is_err());
    controls.requested.pop();
    controls.applied.push(Control {
        kind: ControlKind::Cpu,
        mandatory: false,
        state: ControlState::Supported(100),
    });
    assert!(controls.validate().is_err());
}
#[test]
fn wp_005_lifecycle_and_unavailable_operations() {
    let p = Scripted;
    let mut life = Lifecycle::default();
    for op in [
        Operation::Provision,
        Operation::Start,
        Operation::Inspect,
        Operation::Terminate,
        Operation::CollectArtifacts,
        Operation::Release,
    ] {
        assert!(op.semantics().is_ok());
    }
    for op in [Operation::Command, Operation::Checkpoint, Operation::Suspend, Operation::Resume] {
        assert_eq!(ready(p.submit(&request(op))).unwrap(), Acknowledgment::Unavailable);
        assert!(op.semantics().is_err());
        assert_eq!(life.phase, Phase::New);
    }
    for (n, fact) in [
        (1, Fact::Accepted(Operation::Provision)),
        (2, Fact::Provisioned),
        (3, Fact::StartIntent),
        (4, Fact::Accepted(Operation::Start)),
        (5, Fact::Running),
        (6, Fact::StopRequested),
        (7, Fact::Exited),
        (8, Fact::ArtifactsPublished),
        (9, Fact::Accepted(Operation::Release)),
        (10, Fact::Released),
    ] {
        life.apply(n, &format!("event-{n}"), fact).unwrap();
    }
    assert_eq!(life.phase, Phase::Released);
    assert!(life.apply(9, "reordered", Fact::Running).is_err());
    life.apply(11, "uncertain", Fact::Uncertain).unwrap();
    assert_eq!(life.phase, Phase::Quarantined);
}
#[test]
fn wp_006_aq_free_portable_contract_and_strict_codec() {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/worker/vectors.json"
    ))
    .unwrap();
    let golden = vectors["golden_request"].as_str().unwrap().as_bytes();
    assert_eq!(ContentHash::of(golden).as_str(), vectors["request_sha256"].as_str().unwrap());
    decode::<WorkerRequestV1>(golden).unwrap().validate().unwrap();
    for vector in vectors["reject"].as_array().unwrap() {
        let bytes = vector["bytes"].as_str().unwrap().as_bytes();
        assert!(
            decode::<WorkerRequestV1>(bytes).and_then(|r| r.validate()).is_err(),
            "{}",
            vector["name"]
        );
    }
    let r = request(Operation::Inspect);
    let encoded = encode(&r).unwrap();
    assert_eq!(decode::<WorkerRequestV1>(&encoded).unwrap(), r);
    assert!(decode::<WorkerRequestV1>(br#"{"schema":1,"schema":1}"#).is_err());
    assert!(decode::<WorkerRequestV1>(&vec![b' '; MAX_ARTIFACT_BYTES + 1]).is_err());
    let f = frame(&encoded).unwrap();
    assert_eq!(unframe(&f).unwrap(), encoded);
    assert!(unframe(&f[..f.len() - 1]).is_err());
    let cred = CredentialBindingV1 {
        schema: 1,
        reference: "credential-ref".into(),
        issuer: "wi".into(),
        audience: "agent-host".into(),
        principal: "principal".into(),
        provider: r.provider.clone(),
        run: r.run.clone(),
        operations: vec![Operation::Inspect],
        artifact_scope: vec!["result".into()],
        authorization_digest: hash("auth"),
        expires_unix_ms: 100,
        revocation_generation: 2,
    };
    cred.permits(&r.provider, &r.run, Operation::Inspect, Some("result"), &hash("auth"), 99, 2)
        .unwrap();
    assert!(cred
        .permits(&r.provider, "other", Operation::Inspect, None, &hash("auth"), 99, 2)
        .is_err());
    assert!(cred
        .permits(&r.provider, &r.run, Operation::Inspect, None, &hash("auth"), 100, 2)
        .is_err());
}
