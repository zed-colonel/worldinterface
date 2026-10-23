use worldinterface_domain::flow::*;
use worldinterface_protocol::*;
use worldinterface_storage::{flow::*, StoreKind, StoreProfile, StoreRoot};
fn tenant() -> OpaqueRef {
    "tenant".to_string().try_into().unwrap()
}
fn root(path: &std::path::Path) -> StoreRoot {
    StoreRoot::create_empty(
        path,
        StoreProfile {
            instance: InstanceId::generate(),
            created_at: "2026-10-15T00:00:00.000000000Z".to_string().try_into().unwrap(),
            platform: false,
        },
    )
    .unwrap()
}
fn definition() -> FlowDefinition {
    let envelope = PlanEnvelope::parse(
        include_bytes!("../../../../conformance/wi-fabric-2/flow/branch.canonical.json")
            .trim_ascii_end(),
    )
    .unwrap();
    FlowDefinition {
        id: "stored".to_string().try_into().unwrap(),
        version: 1,
        predecessor: None,
        body: envelope.plan.body,
    }
}
#[test]
fn flow_versions_predecessors_revision_rollback_and_rebuild() {
    let t = tempfile::tempdir().unwrap();
    let root = root(&t.path().join("root"));
    let tenant = tenant();
    let d = definition();
    let s = root
        .flow_transact(&tenant, 0, FlowCommand::InsertVersion { definition: d.clone() })
        .unwrap();
    assert_eq!(s.revision, 1);
    assert_eq!(
        root.flow_transact(&tenant, 0, FlowCommand::InsertVersion { definition: d.clone() })
            .unwrap(),
        s
    );
    let mut changed = d.clone();
    changed.body.causal_metadata.clear();
    assert!(root
        .flow_transact(&tenant, 1, FlowCommand::InsertVersion { definition: changed })
        .is_err());
    let mut bad = d.revise(d.body.clone()).unwrap();
    bad.predecessor = Some(ContentHash::of(b"wrong"));
    assert!(root
        .flow_transact(&tenant, 1, FlowCommand::InsertVersion { definition: bad })
        .is_err());
    assert_eq!(root.flow_snapshot(&tenant).unwrap(), s);
    let next = d.revise(d.body.clone()).unwrap();
    assert!(root
        .flow_transact(&tenant, 0, FlowCommand::InsertVersion { definition: next.clone() })
        .is_err());
    assert_eq!(root.flow_snapshot(&tenant).unwrap(), s);
    let s = root
        .flow_transact(&tenant, 1, FlowCommand::InsertVersion { definition: next.clone() })
        .unwrap();
    assert!(root
        .flow_transact(
            &tenant,
            s.revision,
            FlowCommand::ActivateVersion { id: d.id.clone(), version: 2, previous: Some(1) }
        )
        .is_err());
    let s = root
        .flow_transact(
            &tenant,
            s.revision,
            FlowCommand::ActivateVersion { id: d.id.clone(), version: 1, previous: None },
        )
        .unwrap();
    assert!(root
        .flow_transact(
            &tenant,
            s.revision,
            FlowCommand::ActivateVersion { id: d.id.clone(), version: 1, previous: Some(0) }
        )
        .unwrap_err()
        .to_string()
        .contains("invalid Flow activation precondition"));
    assert!(root
        .flow_transact(
            &tenant,
            s.revision,
            FlowCommand::ActivateVersion { id: d.id.clone(), version: 1, previous: Some(99) }
        )
        .unwrap_err()
        .to_string()
        .contains("precondition conflict"));
    let s = root
        .flow_transact(
            &tenant,
            s.revision,
            FlowCommand::ActivateVersion { id: d.id, version: 2, previous: Some(1) },
        )
        .unwrap();
    let c = rusqlite::Connection::open(t.path().join("root/flow.sqlite")).unwrap();
    c.execute_batch("CREATE TRIGGER inject_flow_failure BEFORE UPDATE ON flow_state BEGIN SELECT RAISE(ABORT,'injected state write failure'); END;").unwrap();
    assert!(root
        .flow_transact(
            &tenant,
            s.revision,
            FlowCommand::ActivateVersion { id: next.id.clone(), version: 1, previous: Some(2) }
        )
        .is_err());
    assert_eq!(root.flow_snapshot(&tenant).unwrap(), s);
    c.execute_batch("DROP TRIGGER inject_flow_failure").unwrap();
    drop(c);
    root.repository(StoreKind::Flow).unwrap().rebuild().unwrap();
    assert_eq!(root.flow_snapshot(&tenant).unwrap(), s);
    drop(root);
    let reopened = StoreRoot::open_existing(t.path().join("root")).unwrap();
    assert_eq!(reopened.flow_snapshot(&tenant).unwrap(), s);
}
#[test]
fn flow_projection_tampering_and_old_profiles_reject_without_writes() {
    let t = tempfile::tempdir().unwrap();
    let p = t.path().join("root");
    let root = root(&p);
    root.flow_transact(&tenant(), 0, FlowCommand::InsertVersion { definition: definition() })
        .unwrap();
    drop(root);
    let c = rusqlite::Connection::open(p.join("flow.sqlite")).unwrap();
    c.execute("UPDATE flow_state SET state=?", [b"{}".as_slice()]).unwrap();
    drop(c);
    let before = std::fs::read(p.join("flow.sqlite")).unwrap();
    assert!(StoreRoot::open_existing(&p).is_err());
    assert_eq!(std::fs::read(p.join("flow.sqlite")).unwrap(), before);
    for previous in ["local-receipt-3", "local-flow-5", "local-flow-6"] {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("root");
        drop(root_at(&p));
        let m = p.join("manifest.json");
        let bytes = std::fs::read(&m).unwrap();
        let mut v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        v["feature_profile"] = previous.into();
        std::fs::write(&m, serde_json::to_vec(&v).unwrap()).unwrap();
        let before = std::fs::read(p.join("flow.sqlite")).unwrap();
        assert!(StoreRoot::open_existing(&p).is_err());
        assert_eq!(std::fs::read(p.join("flow.sqlite")).unwrap(), before);
    }
}
fn root_at(path: &std::path::Path) -> StoreRoot {
    root(path)
}

#[test]
fn concurrent_identical_publications_converge_without_duplicate_journal_transitions() {
    let t = tempfile::tempdir().unwrap();
    let root = std::sync::Arc::new(root(&t.path().join("root")));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let root = root.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                root.publish(tenant(), b"same output").unwrap()
            })
        })
        .collect();
    let refs: Vec<_> = workers.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(refs.iter().all(|r| r == &refs[0]));
    let c = rusqlite::Connection::open(t.path().join("root/context.sqlite")).unwrap();
    let count: u64 =
        c.query_row("SELECT count(*) FROM publication_history", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 3);
    drop(c);
    drop(root);
    StoreRoot::open_existing(t.path().join("root")).unwrap();
}

#[test]
fn separate_store_handles_share_the_dispatch_fence() {
    let dir = tempfile::tempdir().unwrap();
    let first = std::sync::Arc::new(root(dir.path()));
    let second = StoreRoot::open_existing(dir.path()).unwrap();
    let guard = first.flow_dispatch_guard().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _guard = second.flow_dispatch_guard().unwrap();
        acquired_tx.send(()).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(acquired_rx.recv_timeout(std::time::Duration::from_millis(100)).is_err());
    drop(guard);
    acquired_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    thread.join().unwrap();
}
