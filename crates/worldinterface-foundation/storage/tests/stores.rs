use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    sync::{Arc, Barrier},
};
use worldinterface_domain::{Action, Event, Machine, Phase};
use worldinterface_protocol::*;
use worldinterface_storage::*;
fn at() -> Timestamp {
    "2026-09-19T23:00:00.000000000Z".to_string().try_into().unwrap()
}
fn opaque(s: &str) -> OpaqueRef {
    s.to_string().try_into().unwrap()
}
fn profile(platform: bool) -> StoreProfile {
    StoreProfile { instance: InstanceId::generate(), created_at: at(), platform }
}
fn vectors() -> Vec<Envelope> {
    let v: Vec<serde_json::Value> = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../conformance/wi-fabric-2/domain/canonical-vectors.json"
        ))
        .unwrap(),
    )
    .unwrap();
    v.into_iter()
        .map(|v| Envelope::parse(&serde_json::to_vec(&v["envelope"]).unwrap()).unwrap())
        .collect()
}
fn submission() -> Envelope {
    vectors().remove(0)
}
fn scope() -> AdmissionScope {
    AdmissionScope {
        deployment_tenant: opaque("deployment:tenant"),
        authenticated_principal: "12345678-1234-4234-8234-123456789abc"
            .to_string()
            .try_into()
            .unwrap(),
    }
}
fn event(id: &EntityId, action: Action) -> Event {
    Event { entity: id.clone(), machine: Machine::Effect, action, at: at() }
}
fn outbox(id: &EntityId) -> OutboxEntry {
    OutboxEntry {
        key: opaque(id.as_str()),
        entity: id.clone(),
        payload: opaque("payload:1"),
        delivered: false,
    }
}
fn admit(
    repo: &Repository<'_>,
    scope: AdmissionScope,
    s: Envelope,
    id: &EntityId,
) -> worldinterface_storage::Result<EntityId> {
    repo.admit(scope, s, id.clone(), event(id, Action::Submit), outbox(id))
}
fn snapshot(path: &Path) -> BTreeMap<String, Vec<u8>> {
    fn collect(root: &Path, path: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let e = entry.unwrap();
            if e.path().is_dir() {
                collect(root, &e.path(), out);
            } else {
                out.insert(
                    e.path().strip_prefix(root).unwrap().to_str().unwrap().into(),
                    fs::read(e.path()).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    collect(path, path, &mut out);
    out
}
#[test]
fn fresh_stores_all_records_query_reopen_and_deterministic_rebuild() {
    for platform in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("root");
        let root = StoreRoot::create_empty(&p, profile(platform)).unwrap();
        assert_eq!(root.repository(StoreKind::Platform).is_ok(), platform);
        let all = vectors();
        let id = EntityId::generate();
        for kind in [StoreKind::Fabric, StoreKind::Flow, StoreKind::Context] {
            let repo = root.repository(kind).unwrap();
            let mut b = Batch::new(id.clone(), Machine::Effect, 0);
            b.records = all.clone();
            b.events.push(event(&id, Action::Submit));
            b.outbox.push(outbox(&id));
            repo.append(&b).unwrap();
            let mut b = Batch::new(id.clone(), Machine::Effect, 1);
            b.events.push(event(&id, Action::Resolve));
            b.acknowledgments.push(outbox(&id).key);
            repo.append(&b).unwrap();
            assert_eq!(repo.records(&id).unwrap(), all);
            assert_eq!(repo.projection(&id).unwrap().unwrap().state.phase, Phase::Resolving);
            let first = repo.history(&id, 0, 1).unwrap();
            assert_eq!(first.len(), 1);
            assert_eq!(repo.history(&id, first[0].cursor, 1).unwrap().len(), 1);
            assert!(repo.outbox(true, None, 10).unwrap().is_empty());
            assert_eq!(repo.outbox(false, None, 10).unwrap().len(), 1);
            let before = snapshot(&p);
            repo.rebuild().unwrap();
            assert_eq!(repo.records(&id).unwrap(), all);
            assert_eq!(repo.history(&id, 0, 100).unwrap().len(), 2);
            assert_eq!(before.keys().collect::<Vec<_>>(), snapshot(&p).keys().collect::<Vec<_>>());
            assert!(repo.append(&b).is_err());
        }
        drop(root);
        let before = snapshot(&p);
        drop(StoreRoot::open_existing(&p).unwrap());
        assert_eq!(before, snapshot(&p));
    }
}
#[test]
fn identical_and_conflicting_admission_scope_and_restart_stability() {
    let t = tempfile::tempdir().unwrap();
    let p = t.path().join("root");
    let root = StoreRoot::create_empty(&p, profile(false)).unwrap();
    let id = EntityId::generate();
    let repo = root.repository(StoreKind::Fabric).unwrap();
    let s = submission();
    assert_eq!(admit(&repo, scope(), s.clone(), &id).unwrap(), id);
    assert_eq!(admit(&repo, scope(), s.clone(), &EntityId::generate()).unwrap(), id);
    let mut changed = s.clone();
    if let Record::EffectSubmission(ref mut s) = changed.record {
        s.requested_accountable_principal = Some(PrincipalId::generate());
    }
    let before = snapshot(&p);
    assert!(admit(&repo, scope(), changed, &EntityId::generate()).is_err());
    assert_eq!(before, snapshot(&p));
    let mut other = scope();
    other.authenticated_principal = PrincipalId::generate();
    let id2 = EntityId::generate();
    assert_eq!(admit(&repo, other, s.clone(), &id2).unwrap(), id2);
    let mut other = scope();
    other.deployment_tenant = opaque("other-tenant");
    let id3 = EntityId::generate();
    assert_eq!(admit(&repo, other, s.clone(), &id3).unwrap(), id3);
    let ingress = vectors().remove(1);
    let id4 = EntityId::generate();
    assert_eq!(admit(&repo, scope(), ingress, &id4).unwrap(), id4);
    drop(root);
    let root = StoreRoot::open_existing(&p).unwrap();
    let repo = root.repository(StoreKind::Fabric).unwrap();
    assert_eq!(admit(&repo, scope(), s, &EntityId::generate()).unwrap(), id);
    assert_eq!(repo.history(&id, 0, 10).unwrap().len(), 1);
}
#[test]
fn concurrent_equal_admissions_and_revision_writers_have_one_winner() {
    let t = tempfile::tempdir().unwrap();
    let p = t.path().join("root");
    drop(StoreRoot::create_empty(&p, profile(false)).unwrap());
    let barrier = Arc::new(Barrier::new(8));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let p = p.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                let root = StoreRoot::open_existing(&p).unwrap();
                let repo = root.repository(StoreKind::Fabric).unwrap();
                b.wait();
                admit(&repo, scope(), submission(), &EntityId::generate()).unwrap()
            })
        })
        .collect();
    let ids: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(ids.iter().all(|id| id == &ids[0]));
    let barrier = Arc::new(Barrier::new(8));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let p = p.clone();
            let id = ids[0].clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                let root = StoreRoot::open_existing(&p).unwrap();
                let repo = root.repository(StoreKind::Fabric).unwrap();
                let mut batch = Batch::new(id.clone(), Machine::Effect, 1);
                batch.events.push(event(&id, Action::Resolve));
                b.wait();
                repo.append(&batch).is_ok()
            })
        })
        .collect();
    assert_eq!(handles.into_iter().map(|h| usize::from(h.join().unwrap())).sum::<usize>(), 1);
    drop(StoreRoot::open_existing(p).unwrap());
}
#[test]
fn admission_rolls_back_when_outbox_key_conflicts() {
    let t = tempfile::tempdir().unwrap();
    let root = StoreRoot::create_empty(t.path().join("root"), profile(false)).unwrap();
    let repo = root.repository(StoreKind::Fabric).unwrap();
    let id = EntityId::generate();
    admit(&repo, scope(), submission(), &id).unwrap();
    let next = EntityId::generate();
    let mut o = outbox(&id);
    o.entity = next.clone();
    let mut s = scope();
    s.deployment_tenant = opaque("another");
    assert!(repo
        .admit(s.clone(), submission(), next.clone(), event(&next, Action::Submit), o)
        .is_err());
    assert!(repo.history(&next, 0, 10).unwrap().is_empty());
    assert!(repo.projection(&next).unwrap().is_none());
    assert!(repo.records(&next).unwrap().is_empty());
    assert_eq!(admit(&repo, s, submission(), &next).unwrap(), next);
}
#[test]
fn unsupported_incomplete_precontract_and_corrupt_stores_reject_without_changes() {
    for damage in [
        "schema",
        "profile",
        "instance",
        "store_schema",
        "store_missing",
        "precontract",
        "history",
        "projection",
        "sql_schema",
    ] {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("root");
        let root = StoreRoot::create_empty(&p, profile(false)).unwrap();
        let repo = root.repository(StoreKind::Fabric).unwrap();
        let id = EntityId::generate();
        admit(&repo, scope(), submission(), &id).unwrap();
        drop(root);
        match damage {
            "schema" | "profile" => {
                let f = p.join("manifest.json");
                let mut v: serde_json::Value =
                    serde_json::from_slice(&fs::read(&f).unwrap()).unwrap();
                if damage == "schema" {
                    v["schema"] = 999.into();
                } else {
                    v["canonical_profile"] = "unsupported".into();
                }
                fs::write(f, serde_json::to_vec(&v).unwrap()).unwrap();
            }
            "instance" => {
                let f = p.join("flow.json");
                let mut v: serde_json::Value =
                    serde_json::from_slice(&fs::read(&f).unwrap()).unwrap();
                v["root"]["profile"]["instance"] = InstanceId::generate().as_str().into();
                fs::write(f, serde_json::to_vec(&v).unwrap()).unwrap();
            }
            "store_missing" => fs::remove_file(p.join("context.sqlite")).unwrap(),
            "precontract" => fs::remove_file(p.join("manifest.json")).unwrap(),
            _ => {
                let c = rusqlite::Connection::open(p.join("fabric.sqlite")).unwrap();
                match damage {
                    "store_schema" => c.execute_batch("PRAGMA user_version=999").unwrap(),
                    "history" => c
                        .execute_batch(
                            "DROP TRIGGER history_no_update; UPDATE history SET digest='bad'",
                        )
                        .unwrap(),
                    "projection" => c.execute_batch("UPDATE projections SET revision=19").unwrap(),
                    _ => c.execute_batch("DROP TRIGGER history_no_delete").unwrap(),
                }
            }
        }
        let before = snapshot(&p);
        assert!(StoreRoot::open_existing(&p).is_err(), "{damage}");
        assert_eq!(before, snapshot(&p), "{damage}");
        assert!(StoreRoot::create_empty(&p, profile(false)).is_err());
        assert_eq!(before, snapshot(&p));
    }
}
#[test]
fn artifacts_scope_integrity_offline_exclusion_and_complete_restore() {
    let t = tempfile::tempdir().unwrap();
    let p = t.path().join("root");
    let backup = t.path().join("backup");
    let restored = t.path().join("restored");
    let root = StoreRoot::create_empty(&p, profile(true)).unwrap();
    let id = EntityId::generate();
    admit(&root.repository(StoreKind::Fabric).unwrap(), scope(), submission(), &id).unwrap();
    let reference = root.publish(opaque("tenant-a"), b"exact bytes\0\n").unwrap();
    let mut foreign = reference.clone();
    foreign.scope = opaque("tenant-b");
    assert!(root.read_artifact(&foreign).is_err());
    assert!(StoreRoot::backup_offline(&p, &backup).is_err());
    assert!(!backup.exists());
    drop(root);
    let before = snapshot(&p);
    StoreRoot::backup_offline(&p, &backup).unwrap();
    assert_eq!(before, snapshot(&p));
    assert_eq!(before, snapshot(&backup));
    StoreRoot::restore_offline(&backup, &restored).unwrap();
    let root = StoreRoot::open_existing(&restored).unwrap();
    assert_eq!(root.read_artifact(&reference).unwrap(), b"exact bytes\0\n");
    assert_eq!(
        root.repository(StoreKind::Fabric).unwrap().records(&id).unwrap(),
        vec![submission()]
    );
    assert!(root.repository(StoreKind::Platform).is_ok());
    drop(root);
    fs::write(backup.join("artifacts").join(reference.content.as_str()), b"corrupt").unwrap();
    let bad = t.path().join("bad-restore");
    assert!(StoreRoot::restore_offline(&backup, &bad).is_err());
    assert!(!bad.exists());
    fs::remove_file(restored.join("artifacts").join(reference.content.as_str())).unwrap();
    let before = snapshot(&restored);
    assert!(StoreRoot::open_existing(&restored).is_err());
    assert_eq!(before, snapshot(&restored));
}
#[test]
fn projections_can_be_repaired_from_history_without_provider_calls() {
    let t = tempfile::tempdir().unwrap();
    let p = t.path().join("root");
    let root = StoreRoot::create_empty(&p, profile(false)).unwrap();
    let repo = root.repository(StoreKind::Fabric).unwrap();
    let id = EntityId::generate();
    admit(&repo, scope(), submission(), &id).unwrap();
    let c = rusqlite::Connection::open(p.join("fabric.sqlite")).unwrap();
    c.execute_batch(
        "DELETE FROM projections; DELETE FROM records; DELETE FROM admissions; DELETE FROM outbox;",
    )
    .unwrap();
    drop(c);
    repo.rebuild().unwrap();
    assert_eq!(repo.projection(&id).unwrap().unwrap().state.phase, Phase::Submitted);
    assert_eq!(repo.outbox(true, None, 10).unwrap().len(), 1);
    assert_eq!(admit(&repo, scope(), submission(), &EntityId::generate()).unwrap(), id);
    drop(root);
    drop(StoreRoot::open_existing(p).unwrap());
}

#[test]
fn backup_cannot_recursively_copy_itself() {
    let t = tempfile::tempdir().unwrap();
    let p = t.path().join("root");
    drop(StoreRoot::create_empty(&p, profile(false)).unwrap());
    let before = snapshot(&p);
    assert!(StoreRoot::backup_offline(&p, p.join("backup")).is_err());
    assert_eq!(before, snapshot(&p));
}

// F-001: a valid database from a different lineage or logical store is not this store.
fn assert_identity_rejected_unchanged(path: &Path, temp: &Path) {
    let before = snapshot(path);
    let error = StoreRoot::open_existing(path).err().expect("identity must reject");
    assert!(error.to_string().contains("database identity"), "{error}");
    assert_eq!(before, snapshot(path));
    for restore in [false, true] {
        let destination = temp.join(if restore { "bad-restore" } else { "bad-backup" });
        let result = if restore {
            StoreRoot::restore_offline(path, &destination)
        } else {
            StoreRoot::backup_offline(path, &destination)
        };
        assert!(result.is_err());
        assert!(!destination.exists());
        assert_eq!(before, snapshot(path));
    }
}

#[test]
fn database_swaps_reject_open_backup_and_restore_without_changes() {
    let kinds = [StoreKind::Fabric, StoreKind::Flow, StoreKind::Context, StoreKind::Platform];
    for foreign_instance in [false, true] {
        for source_kind in kinds {
            for destination_kind in kinds {
                if !foreign_instance && source_kind == destination_kind {
                    continue;
                }
                let t = tempfile::tempdir().unwrap();
                let source = t.path().join("source");
                let destination = t.path().join("destination");
                let root = StoreRoot::create_empty(&source, profile(true)).unwrap();
                let id = EntityId::generate();
                admit(&root.repository(source_kind).unwrap(), scope(), submission(), &id).unwrap();
                drop(root);
                if foreign_instance {
                    drop(StoreRoot::create_empty(&destination, profile(true)).unwrap());
                } else {
                    StoreRoot::backup_offline(&source, &destination).unwrap();
                }
                fs::copy(
                    source.join(format!("{}.sqlite", source_kind.name())),
                    destination.join(format!("{}.sqlite", destination_kind.name())),
                )
                .unwrap();
                assert_identity_rejected_unchanged(&destination, t.path());
            }
        }
    }
}

#[test]
fn database_identity_is_immutable_and_binds_every_manifest_field() {
    for field in [
        "contract",
        "schema",
        "canonical_profile",
        "feature_profile",
        "profile",
        "stores",
        "complete",
        "kind",
        "missing",
    ] {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("root");
        drop(StoreRoot::create_empty(&p, profile(true)).unwrap());
        let c = rusqlite::Connection::open(p.join("fabric.sqlite")).unwrap();
        let before = snapshot(&p);
        for sql in [
            "UPDATE store_identity SET manifest=x'00'",
            "DELETE FROM store_identity",
            "INSERT OR REPLACE INTO store_identity VALUES(1,x'00')",
            "INSERT INTO store_identity VALUES(2,x'00')",
        ] {
            assert!(c.execute_batch(sql).is_err(), "{sql}");
        }
        assert_eq!(before, snapshot(&p));
        // Simulate damaged identity bytes while preserving the expected schema/triggers.
        let trigger_name =
            if field == "missing" { "identity_no_delete" } else { "identity_no_update" };
        let trigger: String = c
            .query_row("SELECT sql FROM sqlite_master WHERE name=?1", [trigger_name], |r| r.get(0))
            .unwrap();
        c.execute_batch(&format!("DROP TRIGGER {trigger_name}")).unwrap();
        if field == "missing" {
            c.execute("DELETE FROM store_identity", []).unwrap();
        } else {
            let bytes: Vec<u8> =
                c.query_row("SELECT manifest FROM store_identity", [], |r| r.get(0)).unwrap();
            let mut identity: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            if field == "kind" {
                identity["kind"] = "flow".into();
            } else {
                identity["root"].as_object_mut().unwrap().remove(field);
            }
            c.execute(
                "UPDATE store_identity SET manifest=?1",
                [canonical_data(&identity).unwrap()],
            )
            .unwrap();
        }
        c.execute_batch(&trigger).unwrap();
        drop(c);
        assert_identity_rejected_unchanged(&p, t.path());
    }
}
