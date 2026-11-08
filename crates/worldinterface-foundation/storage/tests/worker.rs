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
#[test]
fn retired_worker_inspection_is_bounded_immutable_and_survives_restore() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let b = binding();
    let evidence = br#"{"cleanup_confirmed":true,"controls":null}"#.to_vec();
    root.reserve_start(b.clone(), 1).unwrap();
    assert!(root.retain_worker_inspection(b.clone(), evidence.clone()).is_err());
    root.record_worker_transition(&state(&root, &b), WorkerFact::NoProcess).unwrap();
    root.confirm_worker_cleanup(&state(&root, &b)).unwrap();
    assert!(root.retain_worker_inspection(b.clone(), vec![]).is_err());
    assert!(root.retain_worker_inspection(b.clone(), vec![0; 16 * 1024 + 1]).is_err());
    let mut foreign = b.clone();
    foreign.principal = PrincipalId::generate();
    assert!(root.retain_worker_inspection(foreign, evidence.clone()).is_err());
    root.retain_worker_inspection(b.clone(), evidence.clone()).unwrap();
    let expected = state(&root, &b);
    root.retain_worker_inspection(b.clone(), evidence.clone()).unwrap();
    assert_eq!(state(&root, &b), expected);
    assert!(root.retain_worker_inspection(b.clone(), b"changed".to_vec()).is_err());
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(state(&root, &b), expected);
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(state(&root, &b).inspection_evidence, evidence);
    drop(root);
    let backup = t.path().join("backup");
    StoreRoot::backup_offline(&path, &backup).unwrap();
    let root = StoreRoot::open_existing(&backup).unwrap();
    assert_eq!(state(&root, &b), expected);
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
            correlation: b.event_correlation().unwrap(),
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
fn mixed_worker_and_intake_journals_preserve_global_stream_order_on_restore() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let b = binding();
    let route = EventRoute {
        tenant: b.tenant.clone(),
        audience: b.principal.clone(),
        kind: EventKind::WorkerObservation,
        correlation: b.event_correlation().unwrap(),
        generation: None,
    };
    let early = DurableEvent::new(
        route.clone(),
        "before-reservation".to_string().try_into().unwrap(),
        1,
        ContentHash::of(b"early"),
        None,
    )
    .unwrap();
    root.record_event(early.clone()).unwrap();
    root.reserve_start(b.clone(), 1).unwrap();
    observe(&root, &b, 1, WorkerFact::StartAccepted).unwrap();
    let late = DurableEvent::new(
        route,
        "after-reservation".to_string().try_into().unwrap(),
        state(&root, &b).revision + 1,
        ContentHash::of(b"late"),
        None,
    )
    .unwrap();
    root.record_event(late).unwrap();
    let expected = root.signal_obligations(StoreKind::Fabric, None, 100).unwrap();
    let old_state = state(&root, &b);
    assert!(root
        .record_event(
            DurableEvent::new(
                early.route,
                "regression".to_string().try_into().unwrap(),
                1,
                ContentHash::of(b"regression"),
                None
            )
            .unwrap()
        )
        .is_err());
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(root.signal_obligations(StoreKind::Fabric, None, 100).unwrap(), expected);
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(state(&root, &b), old_state);
    drop(root);
    let backup = t.path().join("backup");
    StoreRoot::backup_offline(&path, &backup).unwrap();
    let root = StoreRoot::open_existing(&backup).unwrap();
    assert_eq!(root.signal_obligations(StoreKind::Fabric, None, 100).unwrap(), expected);
}
#[test]
fn concurrent_worker_observations_allocate_revisions_with_operation_acknowledgement() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let b = binding();
    root.reserve_start(b.clone(), 1).unwrap();
    let operation = WorkerOperation {
        key: "wi-key-1:start".to_string().try_into().unwrap(),
        intent: EffectIntentId::generate(),
        operation: "start".to_string().try_into().unwrap(),
        canonical_request: ContentHash::of(b"start"),
        acknowledged: false,
    };
    root.begin_worker_operation(b.clone(), operation.clone()).unwrap();
    let route = EventRoute {
        tenant: b.tenant.clone(),
        audience: b.principal.clone(),
        kind: EventKind::WorkerObservation,
        correlation: b.event_correlation().unwrap(),
        generation: None,
    };
    let observations: Vec<_> = (1..=8)
        .map(|seq| {
            let bytes = canonical_data(&("provisioned", seq)).unwrap();
            let artifact = root.publish(b.tenant.clone(), &bytes).unwrap();
            (
                WorkerObservation {
                    event: format!("event-{seq}").try_into().unwrap(),
                    provider_sequence: seq,
                    fact: WorkerFact::Provisioned,
                    digest: artifact.content.clone(),
                },
                artifact,
            )
        })
        .collect();
    let before_failure = state(&root, &b);
    let c = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
    c.execute_batch("CREATE TRIGGER fail_worker_signal BEFORE INSERT ON signal_outbox BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    let (observation, artifact) = &observations[0];
    assert!(root
        .commit_worker_observation(&b, observation.clone(), route.clone(), artifact.clone())
        .is_err());
    assert_eq!(state(&root, &b), before_failure);
    assert!(root.pending_signals(StoreKind::Fabric, None, 100).unwrap().is_empty());
    c.execute_batch("DROP TRIGGER fail_worker_signal").unwrap();
    drop(c);
    let barrier = std::sync::Barrier::new(9);
    std::thread::scope(|scope| {
        for (observation, artifact) in &observations {
            scope.spawn(|| {
                barrier.wait();
                root.commit_worker_observation(
                    &b,
                    observation.clone(),
                    route.clone(),
                    artifact.clone(),
                )
                .unwrap();
            });
        }
        scope.spawn(|| {
            barrier.wait();
            root.acknowledge_worker_operation(b.clone(), operation.clone()).unwrap();
        });
    });
    let before = state(&root, &b);
    assert_eq!(before.provider_sequence, 8);
    assert_eq!(before.revision, 11);
    assert!(before.operations[&operation.key].acknowledged);
    assert!(before.capacity_reserved);
    let signals = root.pending_signals(StoreKind::Fabric, None, 100).unwrap();
    assert_eq!(signals.len(), 8);
    let revisions: std::collections::BTreeSet<_> =
        signals.iter().map(|s| s.event.revision).collect();
    assert_eq!(revisions.len(), 8);
    assert!(revisions.iter().all(|r| *r > 2 && *r <= before.revision));
    for (observation, artifact) in &observations {
        let id = root
            .commit_worker_observation(&b, observation.clone(), route.clone(), artifact.clone())
            .unwrap();
        assert_eq!(signals.iter().filter(|s| s.event.id == id).count(), 1);
    }
    let (observation, artifact) = &observations[0];
    let mut wrong_route = route.clone();
    wrong_route.audience = PrincipalId::generate();
    assert!(root
        .commit_worker_observation(&b, observation.clone(), wrong_route, artifact.clone())
        .is_err());
    let mut conflict = observation.clone();
    conflict.fact = WorkerFact::Exited;
    assert!(root.commit_worker_observation(&b, conflict, route, artifact.clone()).is_err());
    assert_eq!(state(&root, &b), before);
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    drop(root);
    let reopened = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(state(&reopened, &b), before);
    assert_eq!(reopened.pending_signals(StoreKind::Fabric, None, 100).unwrap(), signals);
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
#[test]
fn worker_provider_run_streams_do_not_collide_or_cross_wake() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let first = binding();
    let mut second = first.clone();
    second.provider = "other-provider".to_string().try_into().unwrap();
    second.request = "other-start".to_string().try_into().unwrap();
    second.realization = ContentHash::of(b"other exact realization");
    assert_ne!(first.event_correlation().unwrap(), second.event_correlation().unwrap());
    for b in [&first, &second] {
        root.reserve_start(b.clone(), 2).unwrap();
        let source: OpaqueRef = format!("{}:event-1", b.provider.as_str()).try_into().unwrap();
        let digest = ContentHash::of(b"accepted");
        let event = DurableEvent::new(
            EventRoute {
                tenant: b.tenant.clone(),
                audience: b.principal.clone(),
                kind: EventKind::WorkerObservation,
                correlation: b.event_correlation().unwrap(),
                generation: None,
            },
            source.clone(),
            2,
            digest.clone(),
            None,
        )
        .unwrap();
        let observation = WorkerObservation {
            event: source,
            provider_sequence: 1,
            fact: WorkerFact::StartAccepted,
            digest,
        };
        if b == &second {
            let mut wrong = event.clone();
            wrong.route.correlation = first.event_correlation().unwrap();
            assert!(root
                .record_worker_observation(
                    b.tenant.clone(),
                    b.provider.clone(),
                    b.run.clone(),
                    observation.clone(),
                    wrong
                )
                .is_err());
        }
        root.record_worker_observation(
            b.tenant.clone(),
            b.provider.clone(),
            b.run.clone(),
            observation,
            event,
        )
        .unwrap();
        assert!(state(&root, b).accepted);
    }
    let signals = root.pending_signals(StoreKind::Fabric, None, 100).unwrap();
    assert_eq!(signals.len(), 2);
    assert_ne!(signals[0].event.route, signals[1].event.route);
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(root.pending_signals(StoreKind::Fabric, None, 100).unwrap(), signals);
}
