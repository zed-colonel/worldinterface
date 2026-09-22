use actionqueue_core::{
    admission::EnsureTaskRequest,
    bounded::OpaqueRef,
    causal::{CausalContext, CausationLink, ControlMutationContext},
    control::{ControlScope, HostControlContext},
    data_ref::DataRef,
    disposition::{AttemptDisposition, ChildAdmission, DispositionOutcome, DispositionParts},
    ids::{AdmissionKey, TaskId},
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{ChildLifecyclePolicy, TaskPayload, TaskSpec},
    },
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
use std::sync::{Arc, Mutex};
use worldinterface_protocol::{ArtifactRef, ContentHash, InstanceId};
use worldinterface_runtime::{aq_adapter::*, artifact::*, storage::StoreBundle};
use worldinterface_storage::{StoreProfile, StoreRoot};

fn profile() -> StoreProfile {
    StoreProfile {
        instance: InstanceId::generate(),
        created_at: "2026-09-19T00:00:00.000000000Z".to_string().try_into().unwrap(),
        platform: false,
    }
}
fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("authenticated-host").unwrap()),
    }
}
fn spec(id: TaskId, payload: TaskPayload) -> TaskSpec {
    TaskSpec::new(id, payload, RunPolicy::Once, TaskConstraints::default(), TaskMetadata::default())
        .unwrap()
}
struct Policy;
struct AuthenticatedSession {
    scope: String,
}
impl ArtifactAuthorization for Policy {
    type AuthenticatedContext = AuthenticatedSession;
    fn authorize_read(&self, c: &AuthenticatedSession, a: &ArtifactRef) -> bool {
        c.scope == a.scope.as_str()
    }
}

#[test]
fn ab_wi_016_authorization_and_integrity() {
    let t = tempfile::tempdir().unwrap();
    let root = StoreRoot::create_empty(t.path().join("wi"), profile()).unwrap();
    let reference =
        root.publish("tenant-a".to_string().try_into().unwrap(), b"private bytes").unwrap();
    let resolver = ArtifactResolver::new(&root, &Policy);
    let session = AuthenticatedSession { scope: "tenant-a".into() };
    assert_eq!(resolver.read(&session, &reference).unwrap(), b"private bytes");
    let denied = AuthenticatedSession { scope: "tenant-b".into() };
    assert_eq!(
        resolver.read(&denied, &reference).unwrap_err().to_string(),
        "artifact access denied"
    );
    let mut wrong = reference.clone();
    wrong.size += 1;
    assert!(resolver.read(&session, &wrong).is_err());
    wrong = reference.clone();
    wrong.content = ContentHash::of(b"wrong");
    assert!(resolver.read(&session, &wrong).is_err());
    let path = t.path().join("wi/artifacts").join(reference.content.as_str());
    std::fs::write(&path, b"corrupt bytes").unwrap();
    assert!(resolver.read(&session, &reference).is_err());
    std::fs::remove_file(path).unwrap();
    assert!(resolver.read(&session, &reference).is_err());
    // Denial remains identical even when no blob exists, proving authorization first.
    assert_eq!(
        resolver.read(&denied, &reference).unwrap_err().to_string(),
        "artifact access denied"
    );
}

struct Handler {
    parent: TaskId,
    child: TaskId,
    locator: TaskInputLocator,
    seen: Arc<Mutex<Vec<CausalContext>>>,
}
impl ExecutorHandler for Handler {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        self.seen.lock().unwrap().push(ctx.input.causal_context.clone().unwrap());
        if ctx.input.payload == b"parent" && ctx.input.resume_context.is_none() {
            let child = ChildAdmission::new(AdmissionKey::new("child").unwrap(),
                spec(self.child, self.locator.payload().unwrap()).with_parent_policy(self.parent, ChildLifecyclePolicy::Detached),
                vec![], child_override(br#"{"correlation_id":"fork","requesting_actor_ref":"child-actor","origin_ref":"child-origin"}"#).unwrap()).unwrap();
            AttemptDisposition::new(
                DispositionOutcome::Awaiting,
                DispositionParts {
                    wait: Some(
                        actionqueue_core::continuation::WaitSpec::children(
                            actionqueue_core::ids::WaitId::new(),
                            vec![self.child],
                            actionqueue_core::continuation::ChildWaitPolicy::AllTerminal,
                            None,
                        )
                        .unwrap(),
                    ),
                    child_admissions: vec![child],
                    ..Default::default()
                },
            )
            .unwrap()
        } else if ctx.input.payload == b"parent" {
            AttemptDisposition::complete(None)
        } else {
            let locator = TaskInputLocator::decode(&ctx.input.payload).unwrap();
            assert_eq!(locator, self.locator);
            // The actual output DataRef API accepts the external reference without
            // fetching bytes. This fixture intentionally has no blob on the AQ host.
            AttemptDisposition::complete(Some(DataRef::External(locator.external().unwrap())))
        }
    }
}
struct Complete;
impl ExecutorHandler for Complete {
    fn execute(&self, _: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
#[tokio::test]
async fn ab_wi_015_016_real_child_execution_replay_and_foreign_provenance() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("bundle");
    let bundle = StoreBundle::initialize(&path, profile()).unwrap();
    let reference =
        bundle.wi().publish("tenant-a".to_string().try_into().unwrap(), b"large input").unwrap();
    let locator = TaskInputLocator::new(reference.clone());
    assert!(DataRef::External(locator.external().unwrap()).verify_bytes(b"bad").is_err());
    let parent = TaskId::new();
    let child = TaskId::new();
    let seen = Arc::new(Mutex::new(vec![]));
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: bundle.aq_path(),
            snapshot_event_threshold: Some(1),
            ..Default::default()
        },
        Handler { parent, child, locator, seen: seen.clone() },
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let context = causal_context("trace", "correlation")
        .unwrap()
        .with_authorization_ref(OpaqueRef::new("attribution-only").unwrap())
        .with_submitting_principal_ref(OpaqueRef::new("principal-attribution").unwrap())
        .with_purpose_ref(OpaqueRef::new("purpose").unwrap())
        .with_identity_context_ref(OpaqueRef::new("identity-context").unwrap())
        .with_signed_statement_ref(OpaqueRef::new("statement").unwrap())
        .with_proof_context_ref(OpaqueRef::new("proof").unwrap());
    let request = EnsureTaskRequest::new(
        AdmissionKey::new("stable-parent").unwrap(),
        spec(parent, TaskPayload::new(b"parent".to_vec())),
        vec![],
        context.clone(),
        None,
    )
    .unwrap();
    aq.set_control_context(None);
    let unchanged = aq.projection().projection_digest().unwrap();
    assert!(aq.ensure_task(request.clone()).is_err(), "attribution cannot authorize admission");
    assert_eq!(aq.projection().projection_digest().unwrap(), unchanged);
    aq.set_control_context(Some(host()));
    aq.ensure_task(request.clone()).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(15), aq.run_until_idle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(aq.projection().task_count(), 2);
    let run = aq.projection().run_ids_for_task(parent)[0];
    let attempt = aq.projection().get_attempt_history(&run).unwrap()[0].attempt_id();
    let expected = context
        .with_correlation_id(actionqueue_core::ids::CorrelationId::new("fork").unwrap())
        .with_requesting_actor_ref(OpaqueRef::new("child-actor").unwrap())
        .with_origin_ref(OpaqueRef::new("child-origin").unwrap())
        .with_causation(CausationLink::new(Some(parent), Some(run), Some(attempt), None).unwrap());
    assert_eq!(
        aq.projection().task_admission(child).unwrap().request().causal_context(),
        &expected
    );
    assert!(seen.lock().unwrap().contains(&expected));
    let before = aq.projection().projection_digest().unwrap();
    aq.shutdown().unwrap();
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        Complete,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert_eq!(aq.projection().projection_digest().unwrap(), before);
    assert!(!aq.ensure_task(request).unwrap().is_created());
    aq.shutdown().unwrap();
    let refs = QueueReferences::open(&bundle.aq_path()).unwrap();
    let claim = QueueProvenance {
        store: refs.store_id(),
        task: parent,
        run: Some(run),
        attempt: Some(attempt),
    };
    assert_eq!(refs.causation(&claim).unwrap().parent_task_id(), Some(parent));
    let mut forged = claim.clone();
    forged.task = TaskId::new();
    assert!(refs.causation(&forged).is_err());
    forged = claim.clone();
    forged.run = Some(actionqueue_core::ids::RunId::new());
    assert!(refs.causation(&forged).is_err());
    forged = claim.clone();
    forged.attempt = Some(actionqueue_core::ids::AttemptId::new());
    assert!(refs.causation(&forged).is_err());
    let other = StoreBundle::initialize(&t.path().join("other"), profile()).unwrap();
    let other_refs = QueueReferences::open(&other.aq_path()).unwrap();
    let foreign = other_refs.causation(&claim).unwrap();
    assert!(
        foreign.parent_task_id().is_none()
            && foreign.parent_run_id().is_none()
            && foreign.parent_attempt_id().is_none()
    );
    assert!(foreign.external_ref().unwrap().expose().contains(&parent.to_string()));
    assert!(StoreBundle::backup(&path, &t.path().join("busy")).is_err());
    drop(refs);
    drop(bundle);
    StoreBundle::backup(&path, &t.path().join("backup")).unwrap();
    StoreBundle::restore(&t.path().join("backup"), &t.path().join("restored")).unwrap();
    let restored = StoreBundle::open(&t.path().join("restored")).unwrap();
    assert_eq!(restored.wi().read_artifact(&reference).unwrap(), b"large input");
    let aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: restored.aq_path(), ..Default::default() },
        Complete,
    )
    .bootstrap()
    .unwrap();
    assert_eq!(aq.projection().projection_digest().unwrap(), before);
    aq.shutdown().unwrap();
}

#[test]
fn ab_wi_015_bounds_and_unsupported_overrides_reject_without_truncation() {
    assert!(causal_context(&"x".repeat(10000), "c").is_err());
    assert!(child_override(br#"{"authorization_ref":"forged"}"#).is_err());
    assert!(child_override(br#"{"trace_id":"forged"}"#).is_err());
    assert!(child_override(br#"{"origin_ref":"a","origin_ref":"b"}"#).is_err());
    let value = serde_json::json!({"origin_ref":"x".repeat(10000)});
    assert!(child_override(&serde_json::to_vec(&value).unwrap()).is_err());
    let artifact = ArtifactRef {
        scope: "tenant".to_string().try_into().unwrap(),
        content: ContentHash::of(b"test"),
        size: 4,
    };
    let locator = TaskInputLocator::new(artifact);
    let payload = locator.payload().unwrap();
    let golden: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../conformance/wi-fabric-2/integration/locator.json"
    ))
    .unwrap();
    assert_eq!(
        std::str::from_utf8(payload.bytes()).unwrap(),
        golden["canonical"].as_str().unwrap()
    );
    assert_eq!(ContentHash::of(payload.bytes()).as_str(), golden["sha256"].as_str().unwrap());
    assert_eq!(TaskInputLocator::decode(payload.bytes()).unwrap(), locator);
    let mut v: serde_json::Value = serde_json::from_slice(payload.bytes()).unwrap();
    v["version"] = 2.into();
    assert!(TaskInputLocator::decode(&serde_json::to_vec(&v).unwrap()).is_err());
}

#[test]
fn ab_wi_001_store_formats_fresh_lineage_and_profile_rejection() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let bundle = StoreBundle::initialize(&path, profile()).unwrap();
    let aq_path = bundle.aq_path();
    let manifest = actionqueue_storage::store::StoreManifest::read(&aq_path).unwrap();
    assert_eq!(
        (
            manifest.manifest_schema,
            manifest.wal_format,
            manifest.snapshot_schema,
            manifest.projection_version
        ),
        (1, 1, 9, 9)
    );
    assert!(manifest.features.is_empty());
    drop(bundle);
    let original = std::fs::read(aq_path.join("manifest.json")).unwrap();
    for field in ["manifest_schema", "wal_format", "snapshot_schema", "projection_version"] {
        let mut v: serde_json::Value = serde_json::from_slice(&original).unwrap();
        v[field] = 999.into();
        let bad = serde_json::to_vec(&v).unwrap();
        std::fs::write(aq_path.join("manifest.json"), &bad).unwrap();
        assert!(StoreBundle::open(&path).is_err());
        assert_eq!(std::fs::read(aq_path.join("manifest.json")).unwrap(), bad);
    }
    let mut unsupported: serde_json::Value = serde_json::from_slice(&original).unwrap();
    unsupported["features"] = serde_json::json!(["workflow"]);
    std::fs::write(aq_path.join("manifest.json"), serde_json::to_vec(&unsupported).unwrap())
        .unwrap();
    assert!(StoreBundle::open(&path).is_err());
    std::fs::write(aq_path.join("manifest.json"), &original).unwrap();
    let other = StoreBundle::initialize(&t.path().join("other"), profile()).unwrap();
    let wal = std::fs::read(other.aq_path().join("wal/actionqueue.wal")).unwrap();
    std::fs::write(aq_path.join("wal/actionqueue.wal"), &wal).unwrap();
    assert!(StoreBundle::open(&path).is_err());
    assert_eq!(std::fs::read(aq_path.join("wal/actionqueue.wal")).unwrap(), wal);
}

#[test]
fn backup_retains_both_store_locks_and_rejects_missing_references() {
    use actionqueue_storage::store::{open_store, OpenOptions};
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("bundle");
    let bundle = StoreBundle::initialize(&path, profile()).unwrap();
    let reference =
        bundle.wi().publish("tenant".to_string().try_into().unwrap(), b"required").unwrap();
    drop(bundle);
    let writer = open_store(&path.join("aq"), OpenOptions::ReadWrite).unwrap();
    assert!(StoreBundle::backup(&path, &t.path().join("aq-busy")).is_err());
    drop(writer);
    let wi = StoreRoot::open_existing(path.join("wi")).unwrap();
    assert!(StoreBundle::backup(&path, &t.path().join("wi-busy")).is_err());
    drop(wi);
    let wi = StoreRoot::open_maintenance(path.join("wi")).unwrap();
    assert!(wi.publish("tenant".to_string().try_into().unwrap(), b"forbidden").is_err());
    drop(wi);
    StoreBundle::backup(&path, &t.path().join("backup")).unwrap();
    let blob = t.path().join("backup/wi/artifacts").join(reference.content.as_str());
    std::fs::remove_file(blob).unwrap();
    assert!(StoreBundle::restore(&t.path().join("backup"), &t.path().join("restore")).is_err());
    assert!(!t.path().join("restore").exists());
}

#[test]
fn ab_wi_016_nested_destinations_leave_source_unchanged() {
    use std::{collections::BTreeMap, fs, path::Path};
    fn snapshot(root: &Path) -> BTreeMap<std::path::PathBuf, Option<Vec<u8>>> {
        fn visit(
            root: &Path,
            path: &Path,
            entries: &mut BTreeMap<std::path::PathBuf, Option<Vec<u8>>>,
        ) {
            for entry in fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                let key = path.strip_prefix(root).unwrap().to_path_buf();
                if path.is_dir() {
                    entries.insert(key, None);
                    visit(root, &path, entries);
                } else {
                    entries.insert(key, Some(fs::read(path).unwrap()));
                }
            }
        }
        let mut entries = BTreeMap::new();
        visit(root, root, &mut entries);
        entries
    }
    let t = tempfile::tempdir().unwrap();
    let source = t.path().join("source");
    let backup = t.path().join("backup");
    let bundle = StoreBundle::initialize(&source, profile()).unwrap();
    let reference =
        bundle.wi().publish("tenant".to_string().try_into().unwrap(), b"retained").unwrap();
    drop(bundle);
    StoreBundle::backup(&source, &backup).unwrap();
    let before = snapshot(&backup);
    let mut destinations = vec![backup.join("nested"), backup.join("wi/artifacts/nested")];
    #[cfg(unix)]
    {
        let alias = t.path().join("alias");
        std::os::unix::fs::symlink(&backup, &alias).unwrap();
        destinations.push(alias.join("nested"));
        destinations.push(alias.join("wi/artifacts/nested"));
    }
    for destination in destinations {
        for operation in [StoreBundle::backup, StoreBundle::restore] {
            assert_eq!(
                operation(&backup, &destination).unwrap_err().to_string(),
                "destination inside source"
            );
            assert_eq!(
                snapshot(&backup),
                before,
                "rejection must leave no staging or source changes"
            );
            let reopened = StoreBundle::open(&backup).unwrap();
            assert_eq!(reopened.wi().read_artifact(&reference).unwrap(), b"retained");
        }
    }
    let restored = t.path().join("restored");
    StoreBundle::restore(&backup, &restored).unwrap();
    assert_eq!(snapshot(&backup), before);
    assert_eq!(
        StoreBundle::open(&restored).unwrap().wi().read_artifact(&reference).unwrap(),
        b"retained"
    );
}

#[test]
fn ab_wi_016_single_component_relative_destinations() {
    use std::{path::Path, process::Command};
    const CHILD: &str = "WI_RELATIVE_DESTINATION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let source = Path::new("source");
        let bundle = StoreBundle::initialize(source, profile()).unwrap();
        let reference =
            bundle.wi().publish("tenant".to_string().try_into().unwrap(), b"relative").unwrap();
        drop(bundle);
        StoreBundle::backup(source, Path::new("backup")).unwrap();
        StoreBundle::restore(Path::new("backup"), Path::new("restored")).unwrap();
        for path in ["source", "backup", "restored"] {
            assert_eq!(
                StoreBundle::open(Path::new(path)).unwrap().wi().read_artifact(&reference).unwrap(),
                b"relative"
            );
        }
        return;
    }
    // Set cwd only in the child: parallel tests must never change process-global cwd.
    let t = tempfile::tempdir().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "ab_wi_016_single_component_relative_destinations", "--nocapture"])
        .env(CHILD, "1")
        .current_dir(t.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
