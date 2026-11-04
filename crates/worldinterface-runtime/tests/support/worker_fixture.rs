#![allow(dead_code)]
use super::{
    connectors,
    flow_fixture::{Auth, Config, Resolver},
    spi_harness::host,
};
#[path = "worker_approval_anchor.rs"]
mod anchor;
#[path = "worker_build.rs"]
mod build;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use worldinterface_domain::{
    authorization::*, flow::*, ingress::digest, resource::Target, worker::*,
};
use worldinterface_protocol::*;
use worldinterface_runtime::{
    aq_adapter::QueueEngine,
    authority::{EvidenceAuthority, TrustedClock},
    completion::EffectAttemptHandler,
    egress::EffectService,
    flow::node_request_id,
    flow_node::FlowAttemptHandler,
    storage::StoreBundle,
    worker::{admission::WorkerEffect, connector::*, evaluation::*},
    worker_process::*,
    Result,
};
use worldinterface_storage::{
    flow::{FlowOutcome, FlowPins},
    StoreProfile,
};
use worldinterface_worker::{
    controls::*,
    mapping::{Operation, WorkerRequestV1},
    profile::ProfileBinding,
};
#[derive(Clone)]
pub struct Approval {
    expected: OperatorApprovalReceipt,
    packet: ContentHash,
    fixture: ContentHash,
    helper: ContentHash,
}
impl TrustedApprovals for Approval {
    fn accepted(&self, r: &OperatorApprovalReceipt, now: u64) -> Result<bool> {
        Ok(r == &self.expected && r.manifest == self.packet && now < r.expires_unix_ms)
    }
}
impl TrustedWorkerBuild for Approval {
    fn verify_build(
        &self,
        packet: &ContentHash,
        fixture: &ContentHash,
        helper: &ContentHash,
    ) -> Result<()> {
        if packet != &self.packet || fixture != &self.fixture || helper != &self.helper {
            return Err("unverified test-host build".into());
        }
        Ok(())
    }
}
pub fn evaluation() -> Arc<EvaluationVerifier<Approval>> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let bytes =
        std::fs::read(source.join("conformance/wi-fabric-2/worker/evaluation-approval.json"))
            .unwrap();
    let expected = anchor::ACCEPTED_RECEIPT_SHA256.expect(
        "operator acceptance of the completed WIP-10 packet is required before live evaluation",
    );
    assert_eq!(ContentHash::of(&bytes).as_str(), expected, "approval provenance drift");
    let receipt: OperatorApprovalReceipt = serde_json::from_slice(&bytes).unwrap();
    let manifest: EvaluationManifest = serde_json::from_slice(
        &std::fs::read(source.join("conformance/wi-fabric-2/worker/evaluation-manifest.json"))
            .unwrap(),
    )
    .unwrap();
    let environment: serde_json::Value = serde_json::from_slice(
        &std::fs::read(source.join("conformance/wi-fabric-2/worker/evaluation-environment.json"))
            .unwrap(),
    )
    .unwrap();
    for (path, expected) in environment["support_binary_sha256"].as_object().unwrap() {
        assert_eq!(
            ContentHash::of(&std::fs::read(path).unwrap()).as_str(),
            expected.as_str().unwrap(),
            "test support binary drift"
        );
    }
    let packet = manifest.digest().unwrap();
    assert_eq!(receipt.manifest, packet);
    let built = build::current();
    assert_eq!(built.packet, packet);
    let (fixture, helper) = (built.fixture_digest.clone(), built.helper_digest.clone());
    let trusted = Approval { expected: receipt.clone(), packet, fixture, helper };
    Arc::new(EvaluationVerifier { manifest, receipt, trusted, source_root: source })
}
#[derive(Clone)]
pub struct HostEvidence(pub AuthorizationSnapshot);
impl EvidenceAuthority for HostEvidence {
    fn current_snapshot(
        &self,
        tenant: &OpaqueRef,
        s: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        if tenant != &self.0.bindings[0].tenant
            || s.input.artifact != self.0.worker.as_ref().unwrap().input
        {
            return Err("worker host snapshot scope".into());
        }
        Ok(self.0.clone())
    }
    fn verify(&self, tenant: &OpaqueRef, e: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        if &e.artifact.scope != tenant {
            return Err("cross-tenant host evidence".into());
        }
        if e == &self.0.facts[0].evidence {
            return Ok(self.0.facts[0].attested_document.clone());
        }
        if self.0.grants.iter().any(|g| g.evidence == *e)
            || self.0.worker.as_ref().is_some_and(|w| w.evidence == *e)
            || self.0.fixture.as_ref().is_some_and(|q| q.evidence == *e)
        {
            return Ok(e.artifact.content.clone());
        }
        Err("unregistered host evidence".into())
    }
}
#[derive(Clone)]
pub struct HostClock;
impl Clock for HostClock {
    fn unix_ms(&self) -> u64 {
        SystemClock.unix_ms()
    }
}
impl TrustedClock for HostClock {
    fn now(&self) -> Result<Timestamp> {
        Ok(Timestamp::from_epoch_millis(self.unix_ms())?)
    }
}
/// A retry must retain the original agency facts, including their expiry.
/// The generic connector fixture uses a fresh wall clock for each construction.
pub fn admission_fixture(
    descriptor: &ConnectorDescriptor,
    created_at: &Timestamp,
) -> (EffectSubmission, AuthorizationSnapshot) {
    let (submission, mut snapshot) = connectors::fixture(descriptor);
    let expires =
        Timestamp::from_epoch_millis(created_at.epoch_millis().unwrap() + 86_400_000).unwrap();
    snapshot.policy.expires_at = expires.clone();
    for binding in &mut snapshot.bindings {
        binding.expires_at = expires.clone();
    }
    for grant in &mut snapshot.grants {
        grant.expires_at = expires.clone();
    }
    for fact in &mut snapshot.facts {
        fact.valid_until = expires.clone();
    }
    (submission, snapshot)
}
pub type Evidence = EvaluationAuthority<HostEvidence, Approval, HostClock>;
pub struct Harness {
    pub temp: tempfile::TempDir,
    pub bundle: Arc<StoreBundle>,
    pub runtime: Arc<WorkerRuntime<Approval>>,
    pub evaluation: Arc<EvaluationVerifier<Approval>>,
    pub task: Task,
    pub exports: Vec<Export>,
    pub case: String,
    pub controls: ControlEvidenceV1,
    pub auth: Auth,
    pub run: String,
}
impl Harness {
    pub fn new(task: Task, case: &str, wall: u64, output: u64) -> Self {
        let temp = tempfile::tempdir().unwrap();
        Self::at(temp, task, case, wall, output)
    }
    pub fn at(temp: tempfile::TempDir, task: Task, case: &str, wall: u64, output: u64) -> Self {
        let evaluation = evaluation();
        let bundle = Arc::new(
            StoreBundle::initialize(
                &temp.path().join("bundle"),
                StoreProfile {
                    instance: InstanceId::generate(),
                    created_at: HostClock.now().unwrap(),
                    platform: false,
                },
            )
            .unwrap(),
        );
        let auth = Auth(
            "tenant:fixture".to_string().try_into().unwrap(),
            "12345678-1234-4234-8234-123456789abc".to_string().try_into().unwrap(),
        );
        let root = temp.path().join("process");
        std::fs::create_dir(&root).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let runtime = Arc::new(
            WorkerRuntime::install(
                bundle.clone(),
                HostScope { tenant: auth.0.as_str().into(), principal: auth.1.as_str().into() },
                &root,
                &build::current().fixture,
                &build::current().helper,
                evaluation.clone(),
                Arc::new(HostClock),
                1,
            )
            .unwrap(),
        );
        let applied = vec![
            Control {
                kind: ControlKind::WallTimeMs,
                mandatory: true,
                state: ControlState::Supported(wall),
            },
            Control {
                kind: ControlKind::OutputBytes,
                mandatory: true,
                state: ControlState::Supported(output),
            },
        ];
        let controls = ControlEvidenceV1 {
            schema: 1,
            provider: "local-proof".into(),
            run: "run-proof".into(),
            requested: applied.clone(),
            advertised: applied.clone(),
            applied,
            observed: vec![],
        };
        Self {
            temp,
            bundle,
            runtime,
            evaluation,
            task,
            exports: vec![],
            case: case.into(),
            controls,
            auth,
            run: "run-proof".into(),
        }
    }
    pub fn binding(&self) -> WorkerBinding {
        self.bundle
            .wi()
            .worker_snapshot(
                &self.auth.0,
                &"local-proof".to_string().try_into().unwrap(),
                &self.run.clone().try_into().unwrap(),
            )
            .unwrap()
            .unwrap()
            .binding
    }
    pub fn state(&self) -> WorkerState {
        let b = self.binding();
        self.bundle.wi().worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap()
    }
    pub fn demand(&self, op: Operation) -> (EffectSubmission, AuthorizationSnapshot, Config) {
        let descriptor = descriptor(op).unwrap();
        let (mut submission, mut s) =
            admission_fixture(&descriptor, &self.bundle.wi().profile().created_at);
        let request = WorkerRequestV1 {
            schema: 1,
            profile: ProfileBinding::process_proof(),
            operation: op,
            provider: "local-proof".into(),
            run: self.run.clone(),
            request: format!("local-proof:{}:{}", self.run, op.name()),
            input_digest: AuthorizedRun::input_digest(&[]).unwrap(),
            controls_digest: AuthorizedRun::controls_digest(&self.controls).unwrap(),
        };
        let effect = WorkerEffect {
            version: 1,
            case: self.case.clone(),
            worker: request.clone(),
            task: self.task.clone(),
            controls: self.controls.clone(),
            inputs: vec![],
            exports: self.exports.clone(),
        };
        let bytes = canonical_data(&effect).unwrap();
        submission.input.artifact = self.bundle.wi().publish(self.auth.0.clone(), &bytes).unwrap();
        submission.client_request_id =
            format!("worker-flow-{}-{}", self.run, op.name()).try_into().unwrap();
        submission.effect_class = descriptor.effect_class.clone();
        submission.resource.namespace = descriptor.resource_namespace.clone();
        submission.budget.timeout_millis = 60_000;
        for bytes in [b"resolution".as_slice(), b"grant", b"worker", b"fixture-review"] {
            self.bundle.wi().publish(self.auth.0.clone(), bytes).unwrap();
        }
        let raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/wi-fabric-2/egress/fixture.json"
        ))
        .unwrap();
        let mut w: WorkerRealization =
            serde_json::from_value(raw["snapshot"]["worker"].clone()).unwrap();
        w.aq_version = dependencies_digest();
        w.wi_version = self.evaluation.manifest.digest().unwrap();
        w.profile = "process-managed-proof-1".to_string().try_into().unwrap();
        w.provider = "local-proof".to_string().try_into().unwrap();
        w.run = self.run.clone().try_into().unwrap();
        w.operation = op.name().to_string().try_into().unwrap();
        w.input = submission.input.artifact.clone();
        w.environment = self.evaluation.manifest.environment.clone();
        w.exports = digest(&effect.exports).unwrap();
        w.manifest.executable =
            self.bundle.wi().publish(self.auth.0.clone(), &build::current().fixture_bytes).unwrap();
        w.manifest.configuration = self
            .bundle
            .wi()
            .publish(
                self.auth.0.clone(),
                &worldinterface_worker::encode(&(&self.task, &effect.exports)).unwrap(),
            )
            .unwrap();
        // Publish exact recipe-bound adapter/dependency bytes, with the same canonical identities the adapter verifies.
        let adapter = adapter_bytes();
        w.manifest.adapter = self.bundle.wi().publish(self.auth.0.clone(), &adapter).unwrap();
        w.manifest.dependencies = vec![self
            .bundle
            .wi()
            .publish(self.auth.0.clone(), include_bytes!("../../Cargo.lock"))
            .unwrap()];
        w.manifest.profile_definition = self
            .bundle
            .wi()
            .publish(
                self.auth.0.clone(),
                include_bytes!(
                    "../../../../contracts/sources/wi-fabric-2-worker-execution-profile.yaml"
                ),
            )
            .unwrap();
        let wall = self
            .controls
            .applied
            .iter()
            .find_map(|c| {
                if c.kind == ControlKind::WallTimeMs {
                    if let ControlState::Supported(v) = c.state {
                        Some(v)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap();
        let output = self
            .controls
            .applied
            .iter()
            .find_map(|c| {
                if c.kind == ControlKind::OutputBytes {
                    if let ControlState::Supported(v) = c.state {
                        Some(v)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap();
        w.installation = Some(ProcessControlInstallation {
            version: 1,
            wall_ms: wall,
            output_bytes: output,
            configuration: w.manifest.configuration.content.clone(),
            adapter: w.manifest.adapter.content.clone(),
        });
        w.mandatory = ["wall_time_ms", "output_bytes"]
            .into_iter()
            .map(|s| s.to_string().try_into().unwrap())
            .collect();
        w.controls = BTreeMap::new();
        for (name, value) in [("wall_time_ms", wall), ("output_bytes", output)] {
            let v: OpaqueRef = value.to_string().try_into().unwrap();
            w.controls.insert(
                name.to_string().try_into().unwrap(),
                RequiredControl {
                    requested: v.clone(),
                    advertised: v.clone(),
                    applied: v,
                    observed: "not-observed".to_string().try_into().unwrap(),
                    enforcement: Enforcement::Enforced,
                },
            );
        }
        s.facts[0].selector = submission.resource.clone();
        s.facts[0].document.target = Target::Worker {
            provider: w.provider.clone(),
            run: w.run.clone(),
            resource: w.resource.clone(),
            operation: w.operation.clone(),
        };
        s.facts[0].attested_document = digest(&s.facts[0].document).unwrap();
        s.policy.targets = BTreeSet::from([s.facts[0].attested_document.clone()]);
        let mut qualification: FixtureQualification =
            serde_json::from_value(raw["snapshot"]["fixture"].clone()).unwrap();
        qualification.realization = digest(&w).unwrap();
        qualification.run = w.run.clone();
        qualification.operations = [w.operation.clone()].into();
        qualification.expires_at = s.policy.expires_at.clone();
        s.worker = Some(w);
        s.fixture = Some(qualification);
        s.required_outputs = vec![self
            .bundle
            .wi()
            .publish(self.auth.0.clone(), &acknowledgement(&request).unwrap())
            .unwrap()];
        let mut child = submission.clone();
        child.client_request_id = node_request_id(
            &submission.client_request_id,
            &"direct".to_string().try_into().unwrap(),
        )
        .unwrap();
        let demands: BTreeSet<_> = [digest(&submission).unwrap(), digest(&child).unwrap()].into();
        for b in &mut s.bindings {
            b.principal_authority = demands.clone();
            b.capacity_authority = demands.clone();
        }
        s.policy.platform = demands.clone();
        s.policy.resource = demands.clone();
        s.policy.information_use = demands.clone();
        for g in &mut s.grants {
            g.authority = demands.clone();
            g.max_uses = 100;
        }
        let authority = AuthorityUpperBound {
            alternatives: vec![AuthorityAlternative {
                connector: descriptor.connector.clone(),
                operation: descriptor.operation.clone(),
                effect: submission.effect_class.clone(),
                resource: submission.resource.clone(),
            }],
        };
        let config = Config(FlowPins {
            descriptors: vec![descriptor],
            schemas: BTreeMap::from([
                (
                    "wi-worker-effect:1".to_string().try_into().unwrap(),
                    Schema::Artifact {
                        schema_hash: ContentHash::of(b"wi-worker-effect:1"),
                        max_bytes: 65536,
                    },
                ),
                (
                    "wi-worker-result:1".to_string().try_into().unwrap(),
                    Schema::Artifact {
                        schema_hash: ContentHash::of(b"wi-worker-result:1"),
                        max_bytes: 65536,
                    },
                ),
            ]),
            submitting_authority: authority.clone(),
            run_authority: authority,
        });
        (submission, s, config)
    }
    pub async fn operation(&self, op: Operation) {
        let (submission, snapshot, config) = self.demand(op);
        let evidence = Evidence {
            base: HostEvidence(snapshot.clone()),
            evaluation: self.evaluation.clone(),
            clock: HostClock,
        };
        let resolver = Resolver(snapshot.facts[0].clone());
        let connector = self.runtime.connector(op).unwrap();
        let handler = FlowAttemptHandler {
            effect: EffectAttemptHandler {
                bundle: self.bundle.clone(),
                port: connector,
                evidence: evidence.clone(),
                clock: HostClock,
            },
            resolver: resolver.clone(),
            artifacts: Arc::new(config.clone()),
        };
        let mut queue = QueueEngine::bootstrap(&self.bundle, handler, host()).unwrap();
        let service = EffectService {
            root: self.bundle.wi(),
            authentication: &self.auth,
            evidence: &evidence,
            resolver: &resolver,
            clock: &HostClock,
        };
        let selection = DirectOperationSelection {
            connector: descriptor(op).unwrap().connector,
            operation: op.name().to_string().try_into().unwrap(),
        };
        let id = service.submit_direct(&(), submission, selection, &config, &mut queue).unwrap();
        let duplicate = service
            .submit_direct(
                &(),
                self.demand(op).0,
                DirectOperationSelection {
                    connector: descriptor(op).unwrap().connector,
                    operation: op.name().to_string().try_into().unwrap(),
                },
                &config,
                &mut queue,
            )
            .unwrap();
        assert_eq!(id, duplicate, "duplicate admission changed run identity");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let _ = queue.tick().await.unwrap();
            let state = self.bundle.wi().flow_run_snapshot(&self.auth.0, &id).unwrap();
            let run = &state.runs[&id];
            if run.outcome.is_some() {
                assert_eq!(run.outcome, Some(FlowOutcome::Succeeded), "{run:?}");
                break;
            }
            assert!(Instant::now() < deadline, "worker Flow deadline: {run:?}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        queue.shutdown().unwrap();
    }
    pub async fn finish(&self) -> CleanupReport {
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            if let Some(report) = self.runtime.cleanup_report(&self.binding()).unwrap() {
                self.runtime.poll(&self.binding()).unwrap();
                return report;
            }
            assert!(Instant::now() < deadline, "worker cleanup deadline");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
fn adapter_bytes() -> Vec<u8> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/worker_process");
    let mut out = Vec::new();
    for name in [
        "mod.rs",
        "admission.rs",
        "artifact.rs",
        "bootstrap.rs",
        "fixture.rs",
        "launch.rs",
        "ownership.rs",
        "journal.rs",
        "supervision.rs",
        "assets/sample-rust-lib.txt",
        "assets/sample-rust-Cargo.txt",
    ] {
        out.extend(std::fs::read(root.join(name)).unwrap());
    }
    out
}

/// Warm the verified compilation cache before crash-child observation deadlines.
/// This only builds reviewed source; it does not launch a worker or grant approval.
pub fn prebuild() {
    let _ = build::current();
}
