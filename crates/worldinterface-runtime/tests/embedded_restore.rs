//! AB-WI-019: WI-owned independent AQ fixture substitutes for EXO cognition.
#[path = "../../worldinterface-foundation/domain/tests/support/control_plane.rs"]
#[allow(dead_code)]
mod setup;
use actionqueue_core::{
    admission::EnsureTaskRequest,
    disposition::AttemptDisposition,
    ids::{AdmissionKey, TaskId},
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
use actionqueue_storage::store::{self, OpenOptions};
use setup::*;
use std::{fs, path::Path};
use worldinterface_domain::control_plane::Repository;
use worldinterface_protocol::{control_plane::*, InstanceId};
use worldinterface_runtime::{aq_adapter::causal_context, storage::StoreBundle};
use worldinterface_storage::StoreProfile;
struct Complete;
impl ExecutorHandler for Complete {
    fn execute(&self, _: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
fn host() -> actionqueue_core::control::HostControlContext {
    use actionqueue_core::{bounded::OpaqueRef, causal::ControlMutationContext, control::*};
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("independent-wi-fixture").unwrap()),
    }
}
async fn populate(path: &Path) -> (uuid::Uuid, TaskId) {
    let mut engine = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: path.into(),
            snapshot_event_threshold: Some(1),
            ..Default::default()
        },
        Complete,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let id = TaskId::new();
    engine
        .ensure_task(
            EnsureTaskRequest::new(
                AdmissionKey::new("one-durable-task").unwrap(),
                TaskSpec::new(
                    id,
                    TaskPayload::new(b"non-sensitive".to_vec()),
                    RunPolicy::Once,
                    TaskConstraints::default(),
                    TaskMetadata::default(),
                )
                .unwrap(),
                vec![],
                causal_context("restore", "restore").unwrap(),
                None,
            )
            .unwrap(),
        )
        .unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), engine.run_until_idle())
        .await
        .unwrap()
        .unwrap();
    let identity = store::StoreManifest::read(path).unwrap().store_id;
    engine.shutdown().unwrap();
    (identity, id)
}
#[tokio::test]
async fn ab_wi_019_two_independent_stores_restore_complete_wal_artifacts_and_control_history() {
    let t = tempfile::tempdir().unwrap();
    let source = t.path().join("bundle");
    let substitute = t.path().join("wi-owned-exo-substitute");
    let b = StoreBundle::initialize(
        &source,
        StoreProfile { instance: InstanceId::generate(), created_at: time(), platform: false },
    )
    .unwrap();
    let (_, permit, request, snapshot, _) = issue_fixture(false, Profile::Embedded);
    b.wi().bootstrap_control_plane(bundle(&snapshot, Profile::Embedded), now()).unwrap();
    b.wi().register_control_permit(permit).unwrap();
    b.wi().control_plane().execute(&caller(&snapshot), &request, now()).unwrap();
    let artifact = b
        .wi()
        .publish(
            snapshot.bindings[0].tenant.clone(),
            b"published result survives without process workspace",
        )
        .unwrap();
    let worker = worldinterface_domain::worker::WorkerBinding {
        tenant: snapshot.bindings[0].tenant.clone(),
        principal: snapshot.bindings[0].principal.clone(),
        provider: o("restore-provider"),
        run: o("restore-run"),
        request: o("restore-start"),
        realization: worldinterface_protocol::ContentHash::of(b"restore-owned-worker"),
    };
    b.wi().reserve_start(worker.clone(), 1).unwrap();
    worldinterface_runtime::worker::observation::commit(
        b.wi(),
        &worker,
        worldinterface_domain::continuation::EventRoute {
            tenant: worker.tenant.clone(),
            audience: worker.principal.clone(),
            kind: worldinterface_domain::continuation::EventKind::WorkerObservation,
            correlation: worker.event_correlation().unwrap(),
            generation: None,
        },
        worldinterface_worker::ports::Observation {
            provider: worker.provider.as_str().into(),
            run: worker.run.as_str().into(),
            event: "accepted-offline-fixture".into(),
            sequence: 1,
            kind: worldinterface_worker::ports::ObservationKind::Accepted,
        },
    )
    .unwrap();
    let pending =
        b.wi().signal_obligations(worldinterface_storage::StoreKind::Fabric, None, 64).unwrap();
    assert_eq!(pending.len(), 1);
    assert!(pending[0].sequence.is_none());
    let state = b.wi().control_plane_snapshot().unwrap();
    let profile = b.wi().profile().clone();
    drop(b);
    let independent =
        store::open_store(&substitute, OpenOptions::Initialize { features: vec![] }).unwrap();
    drop(independent);
    let (first, first_task) = populate(&source.join("aq")).await;
    let (second, second_task) = populate(&substitute).await;
    assert_ne!(first, second);
    let writer = store::open_store(&source.join("aq"), OpenOptions::ReadWrite).unwrap();
    assert!(store::open_store(&source.join("aq"), OpenOptions::ReadWrite).is_err());
    // The second independent writer remains available; no shared WAL or transaction.
    let other = store::open_store(&substitute, OpenOptions::ReadWrite).unwrap();
    assert!(StoreBundle::backup(&source, &t.path().join("busy")).is_err());
    assert!(!t.path().join("busy").exists());
    drop(writer);
    drop(other);
    StoreBundle::backup(&source, &t.path().join("backup")).unwrap();
    store::backup_store(&substitute, &t.path().join("backup-second")).unwrap();
    StoreBundle::restore(&t.path().join("backup"), &t.path().join("restored")).unwrap();
    store::restore_store(&t.path().join("backup-second"), &t.path().join("restored-second"))
        .unwrap();
    let restored = StoreBundle::open(&t.path().join("restored")).unwrap();
    assert_eq!(restored.wi().profile(), &profile);
    assert_eq!(restored.wi().control_plane_snapshot().unwrap(), state);
    assert_eq!(
        restored
            .wi()
            .signal_obligations(worldinterface_storage::StoreKind::Fabric, None, 64)
            .unwrap(),
        pending
    );
    restored.wi().read_artifact(pending[0].event.artifact.as_ref().unwrap()).unwrap();
    assert_eq!(
        restored.wi().read_artifact(&artifact).unwrap(),
        b"published result survives without process workspace"
    );
    for (path, identity, task) in [
        (t.path().join("restored/aq"), first, first_task),
        (t.path().join("restored-second"), second, second_task),
    ] {
        let original = if identity == first { source.join("aq") } else { substitute.clone() };
        assert_eq!(
            fs::read(path.join("wal/actionqueue.wal")).unwrap(),
            fs::read(original.join("wal/actionqueue.wal")).unwrap()
        );
        let engine = ActionQueueEngine::new(
            RuntimeConfig { data_dir: path, ..Default::default() },
            Complete,
        )
        .bootstrap()
        .unwrap();
        assert_eq!(store::StoreManifest::read(&original).unwrap().store_id, identity);
        assert!(engine.projection().task_admission(task).is_some());
        engine.shutdown().unwrap();
    }
}
#[tokio::test]
async fn ab_wi_019_invalid_restore_never_publishes_partial_destination() {
    for damage in ["snapshot-only", "artifact", "wi-profile", "aq-profile", "identity"] {
        let t = tempfile::tempdir().unwrap();
        let source = t.path().join("source");
        let b = StoreBundle::initialize(
            &source,
            StoreProfile { instance: InstanceId::generate(), created_at: time(), platform: false },
        )
        .unwrap();
        let a = b.wi().publish(o("tenant"), b"required").unwrap();
        drop(b);
        if damage == "snapshot-only" {
            let _ = populate(&source.join("aq")).await;
        }
        let backup = t.path().join("backup");
        StoreBundle::backup(&source, &backup).unwrap();
        match damage {
            "snapshot-only" => {
                assert!(
                    backup.join("aq/snapshots/snapshot.bin").is_file(),
                    "real AQ snapshot required"
                );
                fs::remove_file(backup.join("aq/wal/actionqueue.wal")).unwrap();
            }
            "artifact" => {
                fs::remove_file(backup.join("wi/artifacts").join(a.content.as_str())).unwrap()
            }
            "wi-profile" => {
                let path = backup.join("wi/manifest.json");
                let mut v: serde_json::Value =
                    serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                v["feature_profile"] = "incompatible".into();
                fs::write(path, serde_json::to_vec(&v).unwrap()).unwrap();
            }
            "aq-profile" => {
                let path = backup.join("aq/manifest.json");
                let mut v: serde_json::Value =
                    serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                v["wal_format"] = 999.into();
                fs::write(path, serde_json::to_vec(&v).unwrap()).unwrap();
            }
            _ => {
                let path = backup.join("manifest.json");
                let mut v: serde_json::Value =
                    serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                v["aq"] = uuid::Uuid::new_v4().to_string().into();
                fs::write(path, serde_json::to_vec(&v).unwrap()).unwrap();
            }
        }
        let dest = t.path().join("restore");
        assert!(StoreBundle::restore(&backup, &dest).is_err(), "{damage}");
        assert!(!dest.exists(), "partial {damage}");
    }
}
