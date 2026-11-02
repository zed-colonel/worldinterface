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
