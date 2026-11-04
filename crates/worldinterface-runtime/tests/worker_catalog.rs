#![cfg(target_os = "linux")]
mod support {
    pub mod connectors;
    pub mod continuation_fixture;
    pub mod flow_fixture;
    pub mod spi_harness;
    pub mod worker_fixture;
}
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use worldinterface_capability::process_catalog::*;
use worldinterface_domain::{agency::AuthenticatedCaller, worker::*};
use worldinterface_protocol::*;
use worldinterface_runtime::{
    worker::{catalog::*, connector::descriptor, evaluation::*},
    worker_process::Task,
    Result,
};
use worldinterface_storage::{StoreKind, StoreProfile, StoreRoot};
use worldinterface_worker::mapping::Operation;

struct Trust {
    registration: ProcessRegistrationRecord,
    receipt: OperatorApprovalReceipt,
    revoked: AtomicBool,
    qualified: AtomicBool,
}
impl TrustedApprovals for Trust {
    fn registered(&self, record: &ProcessRegistrationRecord, _: u64) -> Result<bool> {
        Ok(record == &self.registration)
    }
    fn accepted(&self, r: &OperatorApprovalReceipt, now: u64) -> Result<bool> {
        Ok(r == &self.receipt && now < r.expires_unix_ms && !self.revoked.load(Ordering::SeqCst))
    }
    fn qualified(&self, _: &OrdinaryQualificationRecord, _: u64) -> Result<bool> {
        Ok(self.qualified.load(Ordering::SeqCst))
    }
}
fn fixture() -> (tempfile::TempDir, EvaluationVerifier<Trust>, ProcessCatalogBinding) {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("source.rs"), b"non-live catalog test").unwrap();
    let manifest = EvaluationManifest {
        version: 1,
        profile: "process-managed-proof-1".into(),
        environment: ContentHash::of(b"environment"),
        files: BTreeMap::from([("source.rs".into(), ContentHash::of(b"non-live catalog test"))]),
        tasks: BTreeSet::from([ContentHash::of(&canonical_data(&Task::Probe).unwrap())]),
        cases: BTreeSet::from(["WP-007".into(), "WP-032".into()]),
        operations: PROCESS_OPERATIONS.iter().map(|s| s.to_string()).collect(),
        max_wall_ms: 1000,
        max_output_bytes: 65536,
        nonsensitive_only: true,
        production_credentials: false,
        ordinary_use: false,
    };
    let packet = manifest.digest().unwrap();
    let receipt = OperatorApprovalReceipt {
        decision: "non-live-test-only".into(),
        operator: "test-authority".into(),
        manifest: packet.clone(),
        expires_unix_ms: 200,
    };
    let hash = ContentHash::of(b"measurement");
    let binding = ProcessCatalogBinding {
        version: 1,
        profile: manifest.profile.clone().try_into().unwrap(),
        profile_definition: hash.clone(),
        descriptor: hash.clone(),
        adapter: hash.clone(),
        executable: hash.clone(),
        helper: hash.clone(),
        dependencies: vec![hash.clone()],
        configuration: hash.clone(),
        policy: hash.clone(),
        controls: hash.clone(),
        aq: hash.clone(),
        wi: hash.clone(),
        environment: manifest.environment.clone(),
        fixture: packet,
        inputs: hash.clone(),
        exports: hash,
    };
    let verifier = EvaluationVerifier {
        manifest,
        receipt: receipt.clone(),
        trusted: Trust {
            registration: registration(&binding),
            receipt,
            revoked: AtomicBool::new(false),
            qualified: AtomicBool::new(false),
        },
        source_root: temp.path().into(),
    };
    (temp, verifier, binding)
}
fn registration(binding: &ProcessCatalogBinding) -> ProcessRegistrationRecord {
    ProcessRegistrationRecord::Evaluation {
        binding: binding.clone(),
        approval: binding.fixture.clone(),
    }
}
fn create(path: &std::path::Path) -> StoreRoot {
    StoreRoot::create_empty(
        path,
        StoreProfile {
            instance: InstanceId::generate(),
            created_at: Timestamp::from_epoch_millis(1).unwrap(),
            platform: false,
        },
    )
    .unwrap()
}
fn worker() -> WorkerBinding {
    WorkerBinding {
        tenant: "tenant:catalog".to_string().try_into().unwrap(),
        principal: PrincipalId::generate(),
        provider: "provider".to_string().try_into().unwrap(),
        run: "run".to_string().try_into().unwrap(),
        request: "start".to_string().try_into().unwrap(),
        realization: ContentHash::of(b"realization"),
    }
}
fn launch(binding: WorkerBinding, catalog: ContentHash) -> WorkerLaunchRecord {
    WorkerLaunchRecord {
        version: 1,
        binding,
        catalog,
        implementation: descriptor(Operation::Start).unwrap().implementation,
        connectors: worldinterface_runtime::worker::admission::OPERATIONS
            .iter()
            .flat_map(|op| {
                worldinterface_capability::ConnectorRegistry::new(vec![descriptor(*op).unwrap()])
                    .unwrap()
                    .bindings()
                    .unwrap()
            })
            .collect(),
        operations: b"[]".to_vec(),
    }
}
fn quarantine(root: &StoreRoot, binding: &WorkerBinding, catalog: ContentHash) {
    root.quarantine_worker_provider(WorkerProviderQuarantine {
        version: 1,
        tenant: binding.tenant.clone(),
        provider: binding.provider.clone(),
        revision: 1,
        affected: vec![catalog],
        reason: "operator-quarantine".to_string().try_into().unwrap(),
        evidence: ContentHash::of(b"incident"),
        attributed_to: binding.principal.clone(),
    })
    .unwrap();
}
#[test]
fn catalog_registration_is_exact_bounded_and_inspection_grants_no_authority() {
    let (_temp, v, binding) = fixture();
    let profile = inspect_profile().unwrap();
    assert_eq!(
        profile.operations.iter().map(OpaqueRef::as_str).collect::<Vec<_>>(),
        PROCESS_OPERATIONS
    );
    for op in [Operation::Command, Operation::Checkpoint, Operation::Suspend, Operation::Resume] {
        assert!(descriptor(op).is_err());
    }
    assert_eq!(profile.denial_reasons[0].as_str(), "ordinary-use-disabled");
    verify_registration(&registration(&binding), &binding, &v, 100).unwrap();
    for field in [
        "profile",
        "profile_definition",
        "descriptor",
        "adapter",
        "executable",
        "helper",
        "dependencies",
        "configuration",
        "policy",
        "controls",
        "aq",
        "wi",
        "environment",
        "fixture",
        "inputs",
        "exports",
    ] {
        let mut value = serde_json::to_value(&binding).unwrap();
        value[field] = if field == "profile" {
            serde_json::json!("other-profile")
        } else if field == "dependencies" {
            serde_json::json!([ContentHash::of(b"drift")])
        } else {
            serde_json::json!(ContentHash::of(b"drift"))
        };
        let current = serde_json::from_value(value).unwrap();
        assert!(
            verify_registration(&registration(&binding), &current, &v, 100).is_err(),
            "{field}"
        );
    }
    assert!(verify_registration(&registration(&binding), &binding, &v, 200).is_err());
    let mut fabricated = binding.clone();
    fabricated.policy = ContentHash::of(b"fabricated");
    assert!(verify_registration(&registration(&fabricated), &fabricated, &v, 100).is_err());
    v.trusted.revoked.store(true, Ordering::SeqCst);
    assert!(verify_registration(&registration(&binding), &binding, &v, 100).is_err());
    assert!(serde_json::from_str::<ProcessRegistrationRecord>("{}").is_err());
}
#[test]
fn catalog_ordinary_records_require_provenance_and_never_enable_or_self_promote() {
    let (_temp, v, binding) = fixture();
    let q = OrdinaryQualificationRecord {
        version: 1,
        binding: binding.clone(),
        producer_proof: ContentHash::of(b"producer"),
        fixtures: ContentHash::of(b"fixtures"),
        results: ContentHash::of(b"results"),
        reviewer_evidence: ContentHash::of(b"review"),
        operator_decision: "decision".to_string().try_into().unwrap(),
        valid_from_ms: 50,
        expires_ms: 150,
        revoked: false,
    };
    let record = ProcessRegistrationRecord::Ordinary { qualification: q.clone() };
    assert!(verify_registration(&record, &binding, &v, 100)
        .unwrap_err_string()
        .contains("qualification"));
    v.trusted.qualified.store(true, Ordering::SeqCst);
    assert!(verify_registration(&record, &binding, &v, 100)
        .unwrap_err_string()
        .contains("ordinary process use disabled"));
    for mode in ["version", "expired", "future", "revoked", "mismatch", "malformed"] {
        let mut changed = q.clone();
        match mode {
            "version" => changed.version = 2,
            "expired" => changed.expires_ms = 99,
            "future" => changed.valid_from_ms = 110,
            "revoked" => changed.revoked = true,
            "mismatch" => changed.binding.executable = ContentHash::of(b"other"),
            _ => changed.expires_ms = 0,
        }
        assert!(
            verify_registration(
                &ProcessRegistrationRecord::Ordinary { qualification: changed },
                &binding,
                &v,
                100
            )
            .is_err(),
            "{mode}"
        );
    }
}
trait ErrorString {
    fn unwrap_err_string(self) -> String;
}
impl<T> ErrorString for Result<T> {
    fn unwrap_err_string(self) -> String {
        self.err().expect("expected denial").to_string()
    }
}
#[test]
fn catalog_quarantine_fences_stale_admission_and_survives_cleanup_replacement_rebuild_and_reopen() {
    let (temp, v, binding) = fixture();
    let path = temp.path().join("store");
    let root = Arc::new(create(&path));
    let worker = worker();
    let registration = verify_registration(&registration(&binding), &binding, &v, 100).unwrap();
    let digest = authorize_admission(&root, &worker, &registration).unwrap();
    assert!(root.reserve_catalog_start(launch(worker.clone(), digest.clone()), 2).unwrap());
    let calls = AtomicBool::new(false);
    root.fence_worker_dispatch(&worker, &digest, || {
        calls.store(true, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    assert!(calls.swap(false, Ordering::SeqCst));
    quarantine(&root, &worker, digest.clone());
    assert!(root
        .fence_worker_dispatch(&worker, &digest, || {
            calls.store(true, Ordering::SeqCst);
            Ok(())
        })
        .is_err());
    assert!(!calls.load(Ordering::SeqCst));
    assert!(root
        .fence_worker_publication(&worker, || {
            calls.store(true, Ordering::SeqCst);
            Ok(())
        })
        .is_err());
    assert!(!calls.load(Ordering::SeqCst));
    assert!(authorize_admission(&root, &worker, &registration).is_err());
    let mut replacement = worker.clone();
    replacement.run = "new-run".to_string().try_into().unwrap();
    replacement.request = "new-start".to_string().try_into().unwrap();
    replacement.realization = ContentHash::of(b"replacement-registration");
    assert!(root
        .reserve_catalog_start(launch(replacement.clone(), ContentHash::of(b"new-registration")), 2)
        .is_err());
    let state =
        root.worker_snapshot(&worker.tenant, &worker.provider, &worker.run).unwrap().unwrap();
    root.record_worker_transition(&state, WorkerFact::Quarantined).unwrap();
    let state =
        root.worker_snapshot(&worker.tenant, &worker.provider, &worker.run).unwrap().unwrap();
    root.confirm_worker_cleanup(&state).unwrap();
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    assert!(root.worker_provider_quarantine(&worker.tenant, &worker.provider).unwrap().is_some());
    assert!(root.reserve_start(replacement, 2).is_err());
    assert_eq!(root.worker_launch(&worker).unwrap().unwrap().catalog, digest);
}
#[test]
fn catalog_recovery_is_exact_to_caller_binding_operation_and_current_policy() {
    let (temp, _, binding) = fixture();
    let root = create(&temp.path().join("store"));
    let worker = worker();
    let launch = launch(worker.clone(), binding.digest().unwrap());
    let implementation = launch.implementation.clone();
    root.reserve_catalog_start(launch, 1).unwrap();
    quarantine(&root, &worker, binding.digest().unwrap());
    let policy = RecoveryPolicy::new(
        worldinterface_runtime::worker_process::HostScope {
            tenant: worker.tenant.as_str().into(),
            principal: worker.principal.as_str().into(),
        },
        implementation.clone(),
        vec![implementation.clone()],
        ContentHash::of(b"recovery-policy"),
    )
    .unwrap();
    let caller = AuthenticatedCaller::from_trusted_authentication(
        worker.tenant.clone(),
        worker.principal.clone(),
    );
    for op in ["inspect", "terminate", "reconcile"] {
        authorize_recovery(&root, &caller, &worker, op, &implementation, &policy).unwrap();
    }
    for op in ["provision", "start", "collect_artifacts", "release", "command"] {
        assert!(authorize_recovery(&root, &caller, &worker, op, &implementation, &policy).is_err());
    }
    for mode in ["tenant", "caller", "run", "binding", "implementation"] {
        let mut b = worker.clone();
        let mut current = implementation.clone();
        let c = match mode {
            "tenant" => AuthenticatedCaller::from_trusted_authentication(
                "other-tenant".to_string().try_into().unwrap(),
                worker.principal.clone(),
            ),
            "caller" => AuthenticatedCaller::from_trusted_authentication(
                worker.tenant.clone(),
                PrincipalId::generate(),
            ),
            "run" => {
                b.run = "other-run".to_string().try_into().unwrap();
                caller.clone()
            }
            "binding" => {
                b.realization = ContentHash::of(b"other");
                caller.clone()
            }
            _ => {
                current = ContentHash::of(b"changed-recovery-implementation");
                caller.clone()
            }
        };
        assert!(
            authorize_recovery(&root, &c, &b, "terminate", &current, &policy).is_err(),
            "{mode}"
        );
    }
}

#[tokio::test]
async fn wp_007_exact_operator_packet_runs_only_supported_bounded_evaluation_through_wi_aq() {
    let h = support::worker_fixture::Harness::new(Task::Probe, "WP-007", 1500, 65536);
    h.operation(Operation::Start).await;
    h.operation(Operation::Inspect).await;
    assert!(h.finish().await.cleanup_confirmed);
    h.operation(Operation::CollectArtifacts).await;
    h.operation(Operation::Release).await;
    assert!(!h.state().capacity_reserved);
    assert_eq!(inspect_profile().unwrap().registration.as_str(), "evaluation-only");
}
#[tokio::test]
async fn wp_032_drift_denies_new_admissions_before_allocation_through_real_wi_aq() {
    use worldinterface_domain::ingress::digest;
    use worldinterface_storage::flow::FlowOutcome;
    let mut h = support::worker_fixture::Harness::new(Task::Probe, "WP-032", 1500, 65536);
    h.operation(Operation::Start).await;
    assert!(h.finish().await.cleanup_confirmed);
    h.operation(Operation::CollectArtifacts).await;
    h.operation(Operation::Release).await;
    h.rejects_helper_drift();
    let original = h.binding();
    let count = || std::fs::read_dir(h.root.join("process")).unwrap().count();
    let before = count();
    for field in [
        "profile",
        "profile_definition",
        "adapter",
        "executable",
        "dependencies",
        "configuration",
        "policy",
        "controls",
        "aq",
        "wi",
        "environment",
        "inputs",
        "exports",
    ] {
        h.run = format!("drift-{field}");
        let mut snapshot = h.demand(Operation::Start).1;
        if field == "policy" {
            snapshot.policy.revision = "changed-policy".to_string().try_into().unwrap();
        } else {
            let w = snapshot.worker.as_mut().unwrap();
            let changed = ContentHash::of(b"independent-current-drift");
            match field {
                "profile" => w.profile = "other-profile".to_string().try_into().unwrap(),
                "profile_definition" => w.manifest.profile_definition.content = changed,
                "adapter" => w.manifest.adapter.content = changed,
                "executable" => w.manifest.executable.content = changed,
                "dependencies" => w.manifest.dependencies.push(w.manifest.dependencies[0].clone()),
                "configuration" => w.manifest.configuration.content = changed,
                "controls" => w.installation.as_mut().unwrap().wall_ms += 1,
                "aq" => w.aq_version = changed,
                "wi" => w.wi_version = changed,
                "environment" => w.environment = changed,
                "inputs" => w.input.content = changed,
                _ => w.exports = changed,
            }
            // A fabricated updated fixture assertion cannot make changed runtime
            // facts match the source-bound host registration.
            snapshot.fixture.as_mut().unwrap().realization = digest(w).unwrap();
        }
        let result = h.operation_snapshot(Operation::Start, snapshot).await;
        assert!(matches!(result, FlowOutcome::Denied | FlowOutcome::Failed), "{field}: {result:?}");
        assert!(
            h.bundle
                .wi()
                .worker_snapshot(&h.auth.0, &original.provider, &h.run.clone().try_into().unwrap())
                .unwrap()
                .is_none(),
            "{field}"
        );
        assert_eq!(count(), before, "{field} allocated a workspace or ownership journal");
    }
}
#[tokio::test]
async fn wp_032_quarantine_revocation_and_recovery_only_reopen_keep_inspect_terminate_and_aq_reconcile(
) {
    use worldinterface_storage::flow::FlowOutcome;
    let mut h =
        support::worker_fixture::Harness::new(Task::Sleep { millis: 5000 }, "WP-032", 10000, 65536);
    h.ambiguous_start = true;
    h.operation_expect(Operation::Start, FlowOutcome::Uncertain).await;
    let binding = h.binding();
    let catalog = h.bundle.wi().worker_launch(&binding).unwrap().unwrap().catalog;
    quarantine(h.bundle.wi(), &binding, catalog);
    h.evaluation.trusted.revoked.store(true, Ordering::SeqCst);
    h.recovery = true;
    h.operation(Operation::Inspect).await;
    h.operation(Operation::Terminate).await;
    assert!(h.state().stop_requested && h.state().capacity_reserved);
    assert!(!h.state().cleanup_confirmed, "stop acknowledgement fabricated cleanup");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if h.runtime.recovery_evidence(&binding).unwrap().is_some() {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    h.bundle.wi().repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    h = h.reopen_recovery_only();
    h.operation(Operation::Inspect).await;
    h.operation(Operation::Terminate).await;
    assert!(h.runtime.recovery_evidence(&binding).unwrap().unwrap().cleanup_confirmed);
    h.runtime.poll(&binding).unwrap();
    h.reconcile_start().await;
    assert!(h.state().quarantined && h.state().capacity_reserved);
    // Recovery observes cleanup without silently releasing capacity or admission quarantine.
    let mut replacement = binding.clone();
    replacement.run = "replacement".to_string().try_into().unwrap();
    replacement.request = "replacement-start".to_string().try_into().unwrap();
    assert!(h.bundle.wi().reserve_start(replacement, 1).is_err());
    assert!(h
        .bundle
        .wi()
        .worker_provider_quarantine(&binding.tenant, &binding.provider)
        .unwrap()
        .is_some());
}

#[test]
fn catalog_quarantine_serializes_with_dispatch_and_fences_every_later_dispatch() {
    let (temp, _, binding) = fixture();
    let root = Arc::new(create(&temp.path().join("store")));
    let worker = worker();
    let digest = binding.digest().unwrap();
    root.reserve_catalog_start(launch(worker.clone(), digest.clone()), 1).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel();
    let dispatch = {
        let root = root.clone();
        let worker = worker.clone();
        let digest = digest.clone();
        std::thread::spawn(move || {
            root.fence_worker_dispatch(&worker, &digest, || {
                entered_tx.send(()).unwrap();
                finish_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
                Ok(())
            })
            .unwrap()
        })
    };
    entered_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let incident = {
        let root = root.clone();
        let worker = worker.clone();
        let digest = digest.clone();
        std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            quarantine(&root, &worker, digest);
        })
    };
    started_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    finish_tx.send(()).unwrap();
    dispatch.join().unwrap();
    incident.join().unwrap();
    let dispatched = AtomicBool::new(false);
    assert!(root
        .fence_worker_dispatch(&worker, &digest, || {
            dispatched.store(true, Ordering::SeqCst);
            Ok(())
        })
        .is_err());
    assert!(!dispatched.load(Ordering::SeqCst));
}

#[test]
fn catalog_recovery_only_construction_requires_no_approval_or_executable_files() {
    use worldinterface_runtime::{
        storage::StoreBundle,
        worker::connector::{RecoveryOnly, WorkerRuntime},
        worker_process::{HostScope, SystemClock},
    };
    let temp = tempfile::tempdir().unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let worker = worker();
    let scope = HostScope {
        tenant: worker.tenant.as_str().into(),
        principal: worker.principal.as_str().into(),
    };
    let implementation = descriptor(Operation::Start).unwrap().implementation;
    let policy = RecoveryPolicy::new(
        scope.clone(),
        implementation.clone(),
        vec![implementation],
        ContentHash::of(b"recovery-policy"),
    )
    .unwrap();
    let bundle = Arc::new(
        StoreBundle::initialize(
            &temp.path().join("bundle"),
            StoreProfile {
                instance: InstanceId::generate(),
                created_at: Timestamp::from_epoch_millis(1).unwrap(),
                platform: false,
            },
        )
        .unwrap(),
    );
    let before = std::fs::read_dir(temp.path()).unwrap().count();
    let runtime = WorkerRuntime::<RecoveryOnly>::install_recovery(
        bundle,
        scope,
        temp.path(),
        policy,
        Arc::new(SystemClock),
    )
    .unwrap();
    assert!(
        runtime.recovery_evidence(&worker).is_err(),
        "absent original binding gained recovery authority"
    );
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), before);
}
