use worldinterface_domain::continuation::*;
use worldinterface_protocol::*;
use worldinterface_storage::{StoreKind, StoreProfile, StoreRoot};
fn event() -> DurableEvent {
    DurableEvent::new(
        EventRoute {
            tenant: "tenant".to_string().try_into().unwrap(),
            audience: PrincipalId::generate(),
            kind: EventKind::ProviderCallback,
            correlation: "effect".to_string().try_into().unwrap(),
            generation: None,
        },
        "source".to_string().try_into().unwrap(),
        1,
        ContentHash::of(b"source"),
        None,
    )
    .unwrap()
}
/// F-023: the callback fence is per effect and route, transactional and replayed.
#[test]
fn callback_revisions_are_scoped_atomic_and_validated_against_history() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = StoreRoot::create_empty(
        &path,
        StoreProfile {
            instance: InstanceId::generate(),
            created_at: "2026-10-23T00:00:00.000000000Z".to_string().try_into().unwrap(),
            platform: false,
        },
    )
    .unwrap();
    let base = event();
    let at = |route: EventRoute, source: OpaqueRef, revision| {
        DurableEvent::new(route, source, revision, base.digest.clone(), None).unwrap()
    };
    let first = at(base.route.clone(), base.source.clone(), 5);
    root.record_event(first.clone()).unwrap();
    // Every route dimension and the effect source delimit independent streams.
    let mut routes = vec![];
    let mut route = base.route.clone();
    route.tenant = "other-tenant".to_string().try_into().unwrap();
    routes.push(route);
    let mut route = base.route.clone();
    route.audience = PrincipalId::generate();
    routes.push(route);
    let mut route = base.route.clone();
    route.correlation = "other-stream".to_string().try_into().unwrap();
    routes.push(route);
    let mut route = base.route.clone();
    route.generation = Some("generation-2".to_string().try_into().unwrap());
    routes.push(route);
    for route in routes {
        root.record_event(at(route, base.source.clone(), 1)).unwrap();
    }
    root.record_event(at(base.route.clone(), "other-effect".to_string().try_into().unwrap(), 1))
        .unwrap();
    let high_water = root.event_high_water(StoreKind::Fabric).unwrap();
    let pending = root.pending_signals(StoreKind::Fabric, None, 100).unwrap();
    for revision in [1, 3, 4] {
        let stale = at(base.route.clone(), base.source.clone(), revision);
        assert!(root.record_event(stale.clone()).is_err());
        assert!(root.authoritative_event(StoreKind::Fabric, &stale.id).is_err());
        assert!(root.signal_obligation(StoreKind::Fabric, &stale.id).is_err());
    }
    assert_eq!(root.event_high_water(StoreKind::Fabric).unwrap(), high_water);
    assert_eq!(root.pending_signals(StoreKind::Fabric, None, 100).unwrap(), pending);
    // A failed outbox write cannot advance the callback fence or leave a source.
    let c = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
    c.execute_batch("CREATE TRIGGER fail_signal BEFORE INSERT ON signal_outbox BEGIN SELECT RAISE(ABORT,'injected signal write'); END;").unwrap();
    let failed = at(base.route.clone(), base.source.clone(), 9);
    assert!(root.record_event(failed.clone()).is_err());
    assert!(root.authoritative_event(StoreKind::Fabric, &failed.id).is_err());
    assert_eq!(root.event_high_water(StoreKind::Fabric).unwrap(), high_water);
    c.execute_batch("DROP TRIGGER fail_signal").unwrap();
    drop(c);
    let next = at(base.route.clone(), base.source.clone(), 6);
    root.record_event(next.clone()).unwrap();
    root.record_event(first.clone()).unwrap();
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    let high_water = root.event_high_water(StoreKind::Fabric).unwrap();
    assert!(root.record_event(at(base.route.clone(), base.source.clone(), 4)).is_err());
    root.record_event(first.clone()).unwrap();
    root.record_event(next).unwrap();
    let mut conflict = first;
    conflict.digest = ContentHash::of(b"changed-result");
    assert!(root.record_event(conflict).is_err());
    assert_eq!(root.event_high_water(StoreKind::Fabric).unwrap(), high_water);
    drop(root);
    let c = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
    c.execute("UPDATE callback_revisions SET revision=1", []).unwrap();
    drop(c);
    let before = std::fs::read(path.join("fabric.sqlite")).unwrap();
    assert!(StoreRoot::open_existing(&path).is_err());
    assert_eq!(std::fs::read(path.join("fabric.sqlite")).unwrap(), before);
}

#[test]
fn signal_outbox_replay_preserves_frozen_requests_acknowledgments_and_rejects_projection_tampering()
{
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = StoreRoot::create_empty(
        &path,
        StoreProfile {
            instance: InstanceId::generate(),
            created_at: "2026-10-23T00:00:00.000000000Z".to_string().try_into().unwrap(),
            platform: false,
        },
    )
    .unwrap();
    let e = event();
    root.record_event(e.clone()).unwrap();
    root.record_event(e.clone()).unwrap();
    let mut conflicting = e.clone();
    conflicting.digest = ContentHash::of(b"other");
    assert!(root.record_event(conflicting).is_err());
    let request = b"frozen AQ-neutral bytes".to_vec();
    root.prepare_signal_request(StoreKind::Fabric, e.id.clone(), request.clone()).unwrap();
    assert!(root
        .prepare_signal_request(StoreKind::Fabric, e.id.clone(), b"replacement".to_vec())
        .is_err());
    let frozen = root.signal_obligation(StoreKind::Fabric, &e.id).unwrap();
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(root.signal_obligation(StoreKind::Fabric, &e.id).unwrap(), frozen);
    // F-020: diagnostics are bounded AQ classifications, including new codes.
    for code in ["not_found", "already_terminal", "future_rejection"] {
        root.record_signal_rejection(StoreKind::Fabric, e.id.clone(), code.into()).unwrap();
        let rejected = root.signal_obligation(StoreKind::Fabric, &e.id).unwrap();
        assert_eq!(rejected.rejection.as_deref(), Some(code));
        assert!(rejected.sequence.is_none());
        root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        assert_eq!(root.signal_obligation(StoreKind::Fabric, &e.id).unwrap(), rejected);
    }
    for code in [String::new(), "x".repeat(65), "request:secret".into()] {
        assert!(root.record_signal_rejection(StoreKind::Fabric, e.id.clone(), code).is_err());
    }
    root.acknowledge_signal(StoreKind::Fabric, e.id.clone(), ContentHash::of(&request), 7).unwrap();
    root.acknowledge_signal(StoreKind::Fabric, e.id.clone(), ContentHash::of(&request), 7).unwrap();
    assert!(root
        .acknowledge_signal(StoreKind::Fabric, e.id.clone(), ContentHash::of(&request), 8)
        .is_err());
    assert!(root.pending_signals(StoreKind::Fabric, None, 10).unwrap().is_empty());
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(root.signal_obligation(StoreKind::Fabric, &e.id).unwrap().sequence, Some(7));
    drop(root);
    drop(StoreRoot::open_existing(&path).unwrap());
    let c = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
    c.execute("UPDATE signal_outbox SET sequence=99", []).unwrap();
    drop(c);
    assert!(StoreRoot::open_existing(&path).is_err());
}
