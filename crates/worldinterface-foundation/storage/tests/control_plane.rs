#[path = "../../domain/tests/support/control_plane.rs"]
#[allow(dead_code)]
mod support;
use std::{
    cell::Cell,
    fs,
    sync::{Arc, Barrier},
};
use support::*;
use worldinterface_domain::control_plane::Repository as _;
use worldinterface_domain::{agency::*, authorization::*, control_plane as cp};
use worldinterface_protocol::{control_plane::*, *};
use worldinterface_storage::{StoreKind, StoreProfile, StoreRoot};

fn create(path: &std::path::Path) -> StoreRoot {
    StoreRoot::create_empty(
        path,
        StoreProfile { instance: InstanceId::generate(), created_at: time(), platform: false },
    )
    .unwrap()
}
struct Auth(AuthenticatedCaller);
impl cp::Authentication for Auth {
    type Session = ();
    fn authenticate(&self, _: &()) -> cp::Result<AuthenticatedCaller> {
        Ok(self.0.clone())
    }
}
struct Clock(u64);
impl cp::TrustedClock for Clock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}
#[derive(Default)]
struct Vault {
    calls: Cell<u32>,
    fail: bool,
}
impl cp::Custody for Vault {
    fn deliver(
        &self,
        _: &OpaqueRef,
        _: &CredentialMetadata,
    ) -> std::result::Result<(), CustodyReason> {
        self.calls.set(self.calls.get() + 1);
        if self.fail {
            Err(CustodyReason::Uncertain)
        } else {
            Ok(())
        }
    }
    fn revoke(&self, _: &ObjectRef, _: u64, _: u64) -> std::result::Result<(), CustodyReason> {
        if self.fail {
            Err(CustodyReason::Uncertain)
        } else {
            Ok(())
        }
    }
}
fn initialize_issue(root: &StoreRoot, worker: bool) -> (cp::Request, AuthorizationSnapshot) {
    let (_, permit, request, snapshot, _) = issue_fixture(worker, Profile::Embedded);
    root.bootstrap_control_plane(bundle(&snapshot, Profile::Embedded), now()).unwrap();
    root.register_control_permit(permit).unwrap();
    (request, snapshot)
}
fn admin_fixture() -> (cp::Bootstrap, cp::Request, AuthorizationSnapshot) {
    let (_, mut snapshot) = fixture(false);
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let read = cp::read_demand(&reference(&scope, ObjectKind::Resource, "readable")).unwrap();
    let mut base = snapshot.policy.platform.clone();
    base.insert(read);
    set_authority(&mut snapshot, &base);
    let mut grant = snapshot.grants[0].clone();
    grant.version = 2;
    grant.max_uses = 5;
    let cmd = cp::AdminCommand {
        request: id("revision"),
        reference: reference(&scope, ObjectKind::Grant, grant.id.as_str()),
        expected_revision: 1,
        content: cp::Content::Grant(grant),
        claims: IdentityClaims::default(),
        approvals: vec![],
    };
    let mut authority = snapshot.policy.platform.clone();
    authority.insert(cp::admin_demand(&cmd).unwrap());
    set_authority(&mut snapshot, &authority);
    (
        bundle(&snapshot, Profile::Embedded),
        request("revision", cp::Operation::Admin { command: Box::new(cmd) }),
        snapshot,
    )
}
#[test]
fn control_journal_replays_attribution_retries_and_stable_grant_use_across_restore() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let (boot, req, snapshot) = admin_fixture();
    root.bootstrap_control_plane(boot.clone(), now()).unwrap();
    let repo = root.control_plane();
    let (out, fresh) = repo.execute(&caller(&snapshot), &req, now()).unwrap();
    assert!(fresh);
    let cp::Outcome::Revision(r) = out else { panic!() };
    assert_eq!(r.revision, 2);
    assert_eq!(r.attribution.requester, snapshot.bindings[0].principal);
    let (again, fresh) = repo.execute(&caller(&snapshot), &req, now()).unwrap();
    assert!(!fresh);
    assert_eq!(again, cp::Outcome::Revision(r));
    let mut conflict = req.clone();
    if let cp::Operation::Admin { command } = &mut conflict.operation {
        command.expected_revision = 0;
    }
    assert_eq!(repo.execute(&caller(&snapshot), &conflict, now()), Err(cp::Error::Conflict));
    let reference = reference(
        &Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None },
        ObjectKind::Resource,
        "readable",
    );
    root.register_control_object(ObjectView {
        reference: reference.clone(),
        revision: 1,
        content: ContentHash::of(b"safe"),
        artifact: None,
    })
    .unwrap();
    for n in 0..4 {
        repo.execute(
            &caller(&snapshot),
            &request(&format!("read-{n}"), cp::Operation::Get { reference: reference.clone() }),
            now(),
        )
        .unwrap();
    }
    assert!(repo
        .execute(&caller(&snapshot), &request("exhausted", cp::Operation::Get { reference }), now())
        .is_err());
    let state = root.control_plane_snapshot().unwrap();
    assert_eq!(state.uses.values().copied().collect::<Vec<_>>(), vec![5]);
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(root.control_plane_snapshot().unwrap(), state);
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(root.control_plane_snapshot().unwrap(), state);
    assert!(root.bootstrap_control_plane(boot, now()).is_err());
    drop(root);
    let backup = t.path().join("backup");
    StoreRoot::backup_offline(&path, &backup).unwrap();
    let restored = t.path().join("restored");
    StoreRoot::restore_offline(&backup, &restored).unwrap();
    let root = StoreRoot::open_existing(&restored).unwrap();
    assert_eq!(root.control_plane_snapshot().unwrap(), state);
}
#[test]
fn control_concurrent_expected_revisions_have_one_atomic_winner() {
    let t = tempfile::tempdir().unwrap();
    let root = Arc::new(create(&t.path().join("root")));
    let (boot, req, snapshot) = admin_fixture();
    root.bootstrap_control_plane(boot, now()).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    std::thread::scope(|scope| {
        let mut jobs = vec![];
        for n in 0..2 {
            let root = root.clone();
            let barrier = barrier.clone();
            let mut req = req.clone();
            let caller = caller(&snapshot);
            req.id = id(&format!("concurrent-{n}"));
            if let cp::Operation::Admin { command } = &mut req.operation {
                command.request = req.id.clone();
            }
            jobs.push(scope.spawn(move || {
                barrier.wait();
                root.control_plane().execute(&caller, &req, now())
            }));
        }
        barrier.wait();
        assert_eq!(jobs.into_iter().map(|j| j.join().unwrap()).filter(|r| r.is_ok()).count(), 1);
    });
    assert_eq!(root.control_plane_snapshot().unwrap().uses.values().sum::<u32>(), 1);
}
#[test]
fn control_issuance_intent_is_durable_and_retry_never_repeats_custody() {
    for worker in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("root");
        let root = create(&path);
        let (req, snapshot) = initialize_issue(&root, worker);
        let vault = Vault::default();
        let service =
            cp::Service::new(root.control_plane(), Auth(caller(&snapshot)), Clock(now()), vault);
        let first = service.execute(&(), &req).unwrap();
        let cp::Outcome::Credential(m) = &first else { panic!() };
        assert_eq!(m.status, IssuanceStatus::Delivered);
        assert_eq!(service.execute(&(), &req).unwrap(), first);
        assert_eq!(root.control_plane_snapshot().unwrap().uses.values().sum::<u32>(), 1);
        root.withdraw_control_permit(m.permit.clone()).unwrap();
        assert!(service.execute(&(), &req).is_err());
        let state = root.control_plane_snapshot().unwrap();
        root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        assert_eq!(root.control_plane_snapshot().unwrap(), state);
        drop(service);
        drop(root);
        let root = StoreRoot::open_existing(&path).unwrap();
        assert_eq!(root.control_plane_snapshot().unwrap(), state);
    }
}
#[test]
fn control_crash_between_intent_and_delivery_and_backend_uncertainty_fail_closed() {
    for crash in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("root");
        let root = create(&path);
        let (req, snapshot) = initialize_issue(&root, false);
        if crash {
            root.control_plane().execute(&caller(&snapshot), &req, now()).unwrap();
            drop(root);
            let root = StoreRoot::open_existing(&path).unwrap();
            let vault = Vault::default();
            let (cp::Outcome::Credential(m), fresh) =
                root.control_plane().execute(&caller(&snapshot), &req, now()).unwrap()
            else {
                panic!()
            };
            assert!(!fresh);
            let m =
                root.control_plane().deliver(&m.issuance, &Clock(now()), &vault, fresh).unwrap();
            assert_eq!(m.status, IssuanceStatus::Unresolved);
            assert_eq!(vault.calls.get(), 0);
        } else {
            let service = cp::Service::new(
                root.control_plane(),
                Auth(caller(&snapshot)),
                Clock(now()),
                Vault { fail: true, ..Default::default() },
            );
            let cp::Outcome::Credential(m) = service.execute(&(), &req).unwrap() else { panic!() };
            assert_eq!(m.status, IssuanceStatus::Unresolved);
            assert_eq!(m.reason, Some(CustodyReason::Uncertain));
            assert_eq!(service.execute(&(), &req).unwrap(), cp::Outcome::Credential(m));
        }
    }
}
fn rotate_request(state: &cp::State, snapshot: &AuthorizationSnapshot) -> cp::AdminCommand {
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let reference = reference(&scope, ObjectKind::Secret, "secret");
    let cp::Content::Secret(mut secret) = state.current(&reference).unwrap().content.clone() else {
        panic!()
    };
    secret.version += 1;
    secret.generation += 1;
    cp::AdminCommand {
        request: id("rotate"),
        reference,
        expected_revision: 1,
        content: cp::Content::Secret(secret),
        claims: IdentityClaims::default(),
        approvals: vec![],
    }
}
#[test]
fn control_rotation_revocation_and_stale_intents_block_delivery() {
    let (state, _, _, mut snapshot, _) = issue_fixture(false, Profile::Embedded);
    let cmd = rotate_request(&state, &snapshot);
    let mut authority = snapshot.policy.platform.clone();
    authority.insert(cp::admin_demand(&cmd).unwrap());
    set_authority(&mut snapshot, &authority);
    // Rebuild permit after assigning the exact administrative permission.
    let boot = bundle(&snapshot, Profile::Embedded);
    let state = cp::bootstrap(&cp::State::default(), &boot, now()).unwrap();
    let (submission, _) = fixture(false);
    let intent = intent(&submission, &snapshot);
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let permit = cp::prepare_permit(
        &state,
        &caller(&snapshot),
        &intent,
        &VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        cp::PermitInput {
            reference: reference(&scope, ObjectKind::Authorization, "permit"),
            secret: reference(&scope, ObjectKind::Secret, "secret"),
            recipient: o("recipient:trusted"),
            run: o("run:fixture"),
            run_expires_at_ms: now() + 180_000,
            class: CredentialClass::Test,
        },
        now(),
    )
    .unwrap();
    let mut binding = permit.binding().clone();
    binding.expires_at_ms = now() + 120_000;
    let issue = request(
        "issue",
        cp::Operation::Issue {
            request: Box::new(CredentialRequest {
                request: id("issue"),
                permit: reference(&scope, ObjectKind::Authorization, "permit"),
                binding,
            }),
        },
    );
    for failure in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let root = create(&t.path().join("root"));
        root.bootstrap_control_plane(boot.clone(), now()).unwrap();
        root.register_control_permit(permit.clone()).unwrap();
        let (cp::Outcome::Credential(m), fresh) =
            root.control_plane().execute(&caller(&snapshot), &issue, now()).unwrap()
        else {
            panic!()
        };
        assert!(fresh);
        let service = cp::Service::new(
            root.control_plane(),
            Auth(caller(&snapshot)),
            Clock(now()),
            Vault { fail: failure, ..Default::default() },
        );
        let result = service.execute(
            &(),
            &request("rotate", cp::Operation::Admin { command: Box::new(cmd.clone()) }),
        );
        assert_eq!(result.is_ok(), !failure);
        let vault = Vault::default();
        let m = root.control_plane().deliver(&m.issuance, &Clock(now()), &vault, true).unwrap();
        assert_eq!(m.status, IssuanceStatus::Rejected);
        assert_eq!(m.reason, Some(CustodyReason::Stale));
        assert_eq!(vault.calls.get(), 0);
        assert!(root.control_plane().execute(&caller(&snapshot), &issue, now()).is_err());
        root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    }
}
#[test]
fn control_projection_tampering_and_incomplete_commits_are_detected_or_rolled_back() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let (boot, req, snapshot) = admin_fixture();
    root.bootstrap_control_plane(boot, now()).unwrap();
    let before = root.control_plane_snapshot().unwrap();
    let db = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
    db.execute_batch("CREATE TRIGGER reject_write BEFORE UPDATE ON control_state BEGIN SELECT RAISE(ABORT,'injected projection failure'); END;").unwrap();
    assert!(root.control_plane().execute(&caller(&snapshot), &req, now()).is_err());
    assert_eq!(root.control_plane_snapshot().unwrap(), before);
    db.execute_batch("DROP TRIGGER reject_write;").unwrap();
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(root.control_plane_snapshot().unwrap(), before);
    db.execute(
        "UPDATE control_state SET state=?",
        [canonical_collection(&cp::State::default()).unwrap()],
    )
    .unwrap();
    drop(db);
    assert!(StoreRoot::open_existing(&path).is_err());
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    drop(root);
    assert!(StoreRoot::open_existing(&path).is_ok());
}
#[test]
fn control_custody_secret_and_hostile_errors_never_enter_serialized_or_durable_paths() {
    const SENTINEL: &str = "WI_SECRET_SENTINEL_987654321";
    struct HostileVault;
    impl cp::Custody for HostileVault {
        fn deliver(
            &self,
            _: &OpaqueRef,
            _: &CredentialMetadata,
        ) -> std::result::Result<(), CustodyReason> {
            let provider_error =
                format!("Authorization: Bearer {SENTINEL}; provider token={SENTINEL}");
            assert!(provider_error.contains(SENTINEL));
            Err(CustodyReason::Uncertain)
        }
        fn revoke(&self, _: &ObjectRef, _: u64, _: u64) -> std::result::Result<(), CustodyReason> {
            Err(CustodyReason::Uncertain)
        }
    }
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("root");
    let root = create(&path);
    let (req, snapshot) = initialize_issue(&root, false);
    let service =
        cp::Service::new(root.control_plane(), Auth(caller(&snapshot)), Clock(now()), HostileVault);
    let out = service.execute(&(), &req).unwrap();
    let serial = canonical_data(&out).unwrap();
    assert!(!String::from_utf8_lossy(&serial).contains(SENTINEL));
    assert!(!format!("{out:?}").contains(SENTINEL));
    // Real artifact publication and exports carry only the returned safe metadata.
    let artifact = root.publish(snapshot.bindings[0].tenant.clone(), &serial).unwrap();
    assert_eq!(root.read_artifact(&artifact).unwrap(), serial);
    // Exercise every existing record family's real typed serializer and artifact
    // export path with custody-safe references to the returned metadata.
    let vectors: Vec<serde_json::Value> = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/domain/canonical-vectors.json"
    ))
    .unwrap();
    fn bind_artifacts(v: &mut serde_json::Value, safe: &ArtifactRef) {
        match v {
            serde_json::Value::Object(map)
                if map.len() == 3
                    && map.contains_key("scope")
                    && map.contains_key("content")
                    && map.contains_key("size") =>
            {
                *v = serde_json::to_value(safe).unwrap();
            }
            serde_json::Value::Object(map) => {
                for value in map.values_mut() {
                    bind_artifacts(value, safe);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    bind_artifacts(value, safe);
                }
            }
            _ => {}
        }
    }
    for vector in vectors {
        let mut v = vector["envelope"].clone();
        bind_artifacts(&mut v, &artifact);
        let record: Envelope = serde_json::from_value(v).unwrap();
        let bytes = record.canonical_bytes().unwrap();
        let exported = root.publish(snapshot.bindings[0].tenant.clone(), &bytes).unwrap();
        assert_eq!(root.read_artifact(&exported).unwrap(), bytes);
        assert!(!String::from_utf8_lossy(&bytes).contains(SENTINEL));
    }
    let plan_value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/flow/direct.canonical.json"
    ))
    .unwrap();
    let plan: worldinterface_protocol::plan::PlanDocument =
        serde_json::from_value(plan_value["plan"].clone()).unwrap();
    let bytes = canonical_data(&plan).unwrap();
    root.publish(snapshot.bindings[0].tenant.clone(), &bytes).unwrap();
    let state = root.control_plane_snapshot().unwrap();
    assert!(!String::from_utf8_lossy(&canonical_collection(&state).unwrap()).contains(SENTINEL));
    let c = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
    let mut q = c.prepare("SELECT entry FROM control_history").unwrap();
    for b in q.query_map([], |r| r.get::<_, Vec<u8>>(0)).unwrap() {
        assert!(!String::from_utf8_lossy(&b.unwrap()).contains(SENTINEL));
    }
    fn scan(path: &std::path::Path) {
        for e in fs::read_dir(path).unwrap() {
            let path = e.unwrap().path();
            if path.is_dir() {
                scan(&path)
            } else {
                assert!(
                    !fs::read(&path)
                        .unwrap()
                        .windows(SENTINEL.len())
                        .any(|w| w == SENTINEL.as_bytes()),
                    "{}",
                    path.display()
                );
            }
        }
    }
    scan(&path);
    drop(q);
    drop(c);
    drop(service);
    drop(root);
    let backup = t.path().join("export");
    StoreRoot::backup_offline(&path, &backup).unwrap();
    scan(&backup);
}
