use worldinterface_domain::{continuation::*, worker::*};
use worldinterface_protocol::*;
use worldinterface_storage::{StoreKind, StoreProfile, StoreRoot};
fn binding() -> WorkerBinding {
    WorkerBinding {
        tenant: "tenant".to_string().try_into().unwrap(),
        principal: PrincipalId::generate(),
        provider: "provider".to_string().try_into().unwrap(),
        run: "run".to_string().try_into().unwrap(),
        request: "request".to_string().try_into().unwrap(),
        realization: ContentHash::of(b"exact realization"),
    }
}
fn create(path: &std::path::Path) -> StoreRoot {
    StoreRoot::create_empty(
        path,
        StoreProfile {
            instance: InstanceId::generate(),
            created_at: "2026-11-02T00:00:00.000000000Z".to_string().try_into().unwrap(),
            platform: false,
        },
    )
    .unwrap()
}
fn state(root: &StoreRoot, b: &WorkerBinding) -> WorkerState {
    root.worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap()
}
#[test]
fn immutable_start_capacity_stop_and_cleanup_survive_rebuild_backup_and_restart() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let b = binding();
    assert!(root.reserve_start(b.clone(), 1).unwrap());
    assert!(!root.reserve_start(b.clone(), 1).unwrap());
    let mut changed = b.clone();
    changed.realization = ContentHash::of(b"changed");
    assert!(root.reserve_start(changed, 1).is_err());
    let mut second = b.clone();
    second.run = "second".to_string().try_into().unwrap();
    second.request = "second-request".to_string().try_into().unwrap();
    assert!(root.reserve_start(second.clone(), 1).is_err());
    let old = state(&root, &b);
    root.request_worker_stop(&old).unwrap();
    assert!(root.record_worker_transition(&old, WorkerFact::StartAccepted).is_err());
    assert!(state(&root, &b).capacity_reserved);
    assert!(!state(&root, &b).exited);
    root.record_worker_transition(&state(&root, &b), WorkerFact::Quarantined).unwrap();
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    assert!(state(&root, &b).stop_requested);
    assert!(root.reserve_start(second.clone(), 1).is_err());
    drop(root);
    let backup = t.path().join("backup");
    StoreRoot::backup_offline(&path, &backup).unwrap();
    let root = StoreRoot::open_existing(&backup).unwrap();
    let pending = state(&root, &b);
    assert!(pending.quarantined);
    assert!(pending.capacity_reserved);
    root.confirm_worker_cleanup(&pending).unwrap();
    assert!(!state(&root, &b).capacity_reserved);
    assert!(root.reserve_start(second, 1).unwrap());
    assert!(!root.reserve_start(b.clone(), 1).unwrap());
    assert!(root.record_worker_transition(&state(&root, &b), WorkerFact::Running).is_err());
}
fn observe(
    root: &StoreRoot,
    b: &WorkerBinding,
    seq: u64,
    fact: WorkerFact,
) -> std::result::Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let source: OpaqueRef = format!("event-{seq}").try_into().unwrap();
    let digest = ContentHash::of(format!("fact-{fact:?}").as_bytes());
    let event = DurableEvent::new(
        EventRoute {
            tenant: b.tenant.clone(),
            audience: b.principal.clone(),
            kind: EventKind::WorkerObservation,
            correlation: b.run.clone(),
            generation: None,
        },
        source.clone(),
        state(root, b).revision + 1,
        digest.clone(),
        None,
    )
    .unwrap();
    root.record_worker_observation(
        b.tenant.clone(),
        b.provider.clone(),
        b.run.clone(),
        WorkerObservation { event: source, provider_sequence: seq, fact, digest },
        event,
    )
}
#[test]
fn observations_preserve_gaps_conflicts_and_terminal_facts_with_atomic_outbox() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let b = binding();
    root.reserve_start(b.clone(), 1).unwrap();
    observe(&root, &b, 2, WorkerFact::Running).unwrap();
    assert_eq!(state(&root, &b).provider_sequence, 0);
    assert!(!state(&root, &b).running);
    observe(&root, &b, 1, WorkerFact::StartAccepted).unwrap();
    assert_eq!(state(&root, &b).provider_sequence, 2);
    assert!(state(&root, &b).running);
    assert!(!observe(&root, &b, 1, WorkerFact::StartAccepted).unwrap());
    assert!(observe(&root, &b, 1, WorkerFact::Exited).is_err());
    let c = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
    c.execute_batch("CREATE TRIGGER fail_worker_signal BEFORE INSERT ON signal_outbox BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    let before = state(&root, &b);
    assert!(observe(&root, &b, 3, WorkerFact::Exited).is_err());
    assert_eq!(state(&root, &b), before);
    c.execute_batch("DROP TRIGGER fail_worker_signal").unwrap();
    drop(c);
    observe(&root, &b, 3, WorkerFact::Exited).unwrap();
    observe(&root, &b, 4, WorkerFact::Running).unwrap();
    assert!(!state(&root, &b).running);
    assert!(state(&root, &b).exited);
    assert!(state(&root, &b).capacity_reserved);
    assert!(observe(&root, &b, 5, WorkerFact::CleanupConfirmed).is_err());
    let signals = root.pending_signals(StoreKind::Fabric, None, 100).unwrap();
    assert_eq!(signals.len(), 4);
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(root.pending_signals(StoreKind::Fabric, None, 100).unwrap(), signals);
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(state(&root, &b).provider_sequence, 4);
    drop(root);
    let c = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
    c.execute("UPDATE worker_runs SET reserved=0", []).unwrap();
    drop(c);
    assert!(StoreRoot::open_existing(&path).is_err());
}
#[test]
fn worker_provider_keys_bind_exact_original_operations_and_replay_acknowledgements() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let b = binding();
    root.reserve_start(b.clone(), 1).unwrap();
    let operation = WorkerOperation {
        key: "wi-key-1:original".to_string().try_into().unwrap(),
        intent: EffectIntentId::generate(),
        operation: "start".to_string().try_into().unwrap(),
        canonical_request: ContentHash::of(b"exact canonical operation"),
        acknowledged: false,
    };
    assert!(root.acknowledge_worker_operation(b.clone(), operation.clone()).is_err());
    assert!(root.begin_worker_operation(b.clone(), operation.clone()).unwrap());
    assert!(!root.begin_worker_operation(b.clone(), operation.clone()).unwrap());
    let mut conflict = operation.clone();
    conflict.canonical_request = ContentHash::of(b"changed");
    assert!(root.begin_worker_operation(b.clone(), conflict).is_err());
    root.acknowledge_worker_operation(b.clone(), operation.clone()).unwrap();
    assert!(!root.begin_worker_operation(b.clone(), operation.clone()).unwrap());
    let before = state(&root, &b);
    assert!(before.operations[&operation.key].acknowledged);
    root.acknowledge_worker_operation(b.clone(), operation.clone()).unwrap();
    assert_eq!(state(&root, &b), before);
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    drop(root);
    let reopened = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(state(&reopened, &b), before);
    assert!(!reopened.begin_worker_operation(b, operation).unwrap());
}
