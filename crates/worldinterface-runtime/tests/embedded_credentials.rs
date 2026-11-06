//! F-007: trusted custody simulation and real process death; no worker executable.
#[path = "support/child.rs"]
mod child;
#[path = "../../worldinterface-foundation/domain/tests/support/control_plane.rs"]
#[allow(dead_code)]
mod setup;
use setup::*;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use worldinterface_domain::control_plane::{self as cp, Repository};
use worldinterface_protocol::{control_plane::*, *};
use worldinterface_storage::{StoreProfile, StoreRoot};

struct Clock;
impl cp::TrustedClock for Clock {
    fn now_ms(&self) -> u64 {
        now()
    }
}
struct Vault {
    path: PathBuf,
    fenced: AtomicBool,
    calls: AtomicU32,
    invalidations: AtomicU32,
    uncertain: bool,
}
impl Vault {
    fn new(path: &Path) -> Self {
        Self {
            path: path.into(),
            fenced: AtomicBool::new(true),
            calls: AtomicU32::new(0),
            invalidations: AtomicU32::new(0),
            uncertain: false,
        }
    }
}
impl cp::Custody for Vault {
    fn fence_unattempted(
        &self,
        m: &CredentialMetadata,
    ) -> std::result::Result<ContentHash, CustodyReason> {
        if !self.fenced.load(Ordering::SeqCst) || self.uncertain {
            return Err(CustodyReason::Uncertain);
        }
        // The backend tombstone also prevents a late claim in the original store.
        fs::write(self.path.join(m.issuance.as_str()), b"fenced-and-invalidated")
            .map_err(|_| CustodyReason::Uncertain)?;
        cp::unattempted_fencing_binding(m).map_err(|_| CustodyReason::Uncertain)
    }

    fn deliver(
        &self,
        _: &OpaqueRef,
        m: &CredentialMetadata,
    ) -> std::result::Result<(), CustodyReason> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path.join(m.issuance.as_str()))
            .map_err(|_| CustodyReason::Rejected)?;
        if self.uncertain {
            Err(CustodyReason::Uncertain)
        } else {
            Ok(())
        }
    }
    fn deliver_attempt(
        &self,
        r: &OpaqueRef,
        m: &CredentialMetadata,
        a: &CredentialDeliveryAttempt,
    ) -> std::result::Result<(), CustodyReason> {
        assert_eq!(a.issuance, m.issuance);
        assert_eq!(a.metadata, worldinterface_domain::ingress::digest(m).unwrap());
        self.deliver(r, m)
    }
    fn revoke(&self, _: &ObjectRef, _: u64, _: u64) -> std::result::Result<(), CustodyReason> {
        Ok(())
    }
    fn fence(
        &self,
        a: &CredentialDeliveryAttempt,
    ) -> std::result::Result<ContentHash, CustodyReason> {
        if !self.fenced.load(Ordering::SeqCst) {
            return Err(CustodyReason::Uncertain);
        }
        cp::fencing_binding(a).map_err(|_| CustodyReason::Uncertain)
    }
    fn invalidate(
        &self,
        m: &CredentialMetadata,
        a: &CredentialDeliveryAttempt,
    ) -> std::result::Result<(), CustodyReason> {
        assert_eq!(m.issuance, a.issuance);
        self.invalidations.fetch_add(1, Ordering::SeqCst);
        if self.uncertain {
            Err(CustodyReason::Uncertain)
        } else {
            Ok(())
        }
    }
}
fn root(path: &Path) -> StoreRoot {
    StoreRoot::create_empty(
        path,
        StoreProfile { instance: InstanceId::generate(), created_at: time(), platform: false },
    )
    .unwrap()
}
fn initialize(root: &StoreRoot) -> cp::Request {
    let (_, permit, request, snapshot, _) = issue_fixture(false, Profile::Embedded);
    root.bootstrap_control_plane(bundle(&snapshot, Profile::Embedded), now()).unwrap();
    root.register_control_permit(permit).unwrap();
    request
}
fn issue(root: &StoreRoot, request: &cp::Request, vault: &Vault) -> CredentialMetadata {
    let (_, _, _, snapshot, _) = issue_fixture(false, Profile::Embedded);
    let (outcome, fresh) =
        root.control_plane().execute(&caller(&snapshot), request, now()).unwrap();
    let cp::Outcome::Credential(m) = outcome else { panic!() };
    root.control_plane().deliver(&m.issuance, &Clock, vault, fresh).unwrap()
}
#[test]
fn embedded_credential_crash_child() {
    let Some(path) = std::env::var_os("WI_EMBEDDED_CREDENTIAL_CHILD") else { return };
    let path = PathBuf::from(path);
    let root = StoreRoot::open_existing(path.join("root")).unwrap();
    let request: cp::Request =
        serde_json::from_slice(&fs::read(path.join("request.json")).unwrap()).unwrap();
    let _ = issue(&root, &request, &Vault::new(&path));
}
#[test]
fn f007_crash_cuts_reconcile_exact_ownership_without_redelivery() {
    for cut in [
        "control-intent-committed",
        "control-attempt-committed",
        "control-custody-returned",
        "control-delivery-committed",
    ] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path();
        let r = root(&path.join("root"));
        let request = initialize(&r);
        drop(r);
        fs::write(path.join("request.json"), serde_json::to_vec(&request).unwrap()).unwrap();
        let ready = path.join("ready");
        let mut process = child::KillOnDrop(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "embedded_credential_crash_child", "--nocapture"])
                .env("WI_EMBEDDED_CREDENTIAL_CHILD", path)
                .env("WI_CRASH_POINT", cut)
                .env("WI_CRASH_READY", &ready)
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        while !ready.exists() {
            assert!(process.try_wait().unwrap().is_none(), "child exited at {cut}");
            assert!(Instant::now() < deadline, "barrier {cut}");
            std::thread::sleep(Duration::from_millis(5));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let r = StoreRoot::open_existing(path.join("root")).unwrap();
        let before = r.control_plane_snapshot().unwrap();
        let m = before.issuances.values().next().unwrap().clone();
        let attempted = cut != "control-intent-committed";
        assert_eq!(before.delivery_attempts.contains_key(&m.issuance), attempted);
        let vault = Vault::new(path);
        vault.fenced.store(false, Ordering::SeqCst);
        if m.status == IssuanceStatus::Pending {
            assert_eq!(
                r.reconcile_control_delivery(&m.issuance, &vault),
                Err(cp::Error::Unresolved)
            );
            assert_eq!(r.control_plane_snapshot().unwrap(), before);
        }
        vault.fenced.store(true, Ordering::SeqCst);
        let recovered = r.reconcile_control_delivery(&m.issuance, &vault).unwrap();
        assert_eq!(
            recovered.status,
            match cut {
                "control-intent-committed" => IssuanceStatus::Unresolved,
                "control-delivery-committed" => IssuanceStatus::Delivered,
                _ => IssuanceStatus::Unresolved,
            }
        );
        assert_eq!(issue(&r, &request, &vault), recovered);
        assert_eq!(vault.calls.load(Ordering::SeqCst), 0);
        let stable = r.control_plane_snapshot().unwrap();
        assert_eq!(r.reconcile_control_delivery(&m.issuance, &vault).unwrap(), recovered);
        assert_eq!(r.control_plane_snapshot().unwrap(), stable);
        r.repository(worldinterface_storage::StoreKind::Fabric).unwrap().rebuild().unwrap();
        assert_eq!(r.control_plane_snapshot().unwrap(), stable);
        drop(r);
        let restored = path.join("restore");
        StoreRoot::restore_offline(path.join("root"), &restored).unwrap();
        assert_eq!(
            StoreRoot::open_existing(restored).unwrap().control_plane_snapshot().unwrap(),
            stable
        );
    }
}
#[test]
fn f007_restored_pending_attempt_requires_custody_fencing_and_uncertainty_stays_unresolved() {
    let t = tempfile::tempdir().unwrap();
    let r = root(&t.path().join("root"));
    let request = initialize(&r);
    let (_, _, _, snapshot, _) = issue_fixture(false, Profile::Embedded);
    let (cp::Outcome::Credential(m), _) =
        r.control_plane().execute(&caller(&snapshot), &request, now()).unwrap()
    else {
        panic!()
    };
    // Custody uncertainty is a completed attempt whose result remains unresolved.
    let mut vault = Vault::new(t.path());
    vault.uncertain = true;
    let m = r.control_plane().deliver(&m.issuance, &Clock, &vault, true).unwrap();
    assert_eq!(m.status, IssuanceStatus::Unresolved);
    let state = r.control_plane_snapshot().unwrap();
    drop(r);
    StoreRoot::restore_offline(t.path().join("root"), t.path().join("restored")).unwrap();
    let r = StoreRoot::open_existing(t.path().join("restored")).unwrap();
    vault.fenced.store(false, Ordering::SeqCst);
    assert_eq!(r.reconcile_control_delivery(&m.issuance, &vault), Err(cp::Error::Unresolved));
    assert_eq!(r.control_plane_snapshot().unwrap(), state);
    vault.fenced.store(true, Ordering::SeqCst);
    let result = r.reconcile_control_delivery(&m.issuance, &vault).unwrap();
    assert_eq!(result.status, IssuanceStatus::Unresolved);
    assert!(!r.control_plane_snapshot().unwrap().delivery_reconciliations[&m.issuance].invalidated);
    assert_eq!(issue(&r, &request, &vault).status, IssuanceStatus::Unresolved);
    assert_eq!(vault.calls.load(Ordering::SeqCst), 1);
}
#[test]
fn f007_live_delivery_excludes_recovery_and_concurrent_retry() {
    use std::sync::Barrier;
    struct Blocking {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
        calls: AtomicU32,
    }
    impl cp::Custody for Blocking {
        fn deliver(
            &self,
            _: &OpaqueRef,
            _: &CredentialMetadata,
        ) -> std::result::Result<(), CustodyReason> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.wait();
            self.release.wait();
            Ok(())
        }
        fn revoke(&self, _: &ObjectRef, _: u64, _: u64) -> std::result::Result<(), CustodyReason> {
            Ok(())
        }
    }
    let t = tempfile::tempdir().unwrap();
    let r = root(&t.path().join("root"));
    let request = initialize(&r);
    let (_, _, _, snapshot, _) = issue_fixture(false, Profile::Embedded);
    let (cp::Outcome::Credential(m), fresh) =
        r.control_plane().execute(&caller(&snapshot), &request, now()).unwrap()
    else {
        panic!()
    };
    let vault = Blocking {
        entered: Arc::new(Barrier::new(2)),
        release: Arc::new(Barrier::new(2)),
        calls: AtomicU32::new(0),
    };
    std::thread::scope(|s| {
        let running =
            s.spawn(|| r.control_plane().deliver(&m.issuance, &Clock, &vault, fresh).unwrap());
        vault.entered.wait();
        let retry = r.control_plane().deliver(&m.issuance, &Clock, &vault, false).unwrap();
        assert_eq!(retry.status, IssuanceStatus::Pending);
        assert_eq!(
            r.reconcile_control_delivery(&m.issuance, &Vault::new(t.path())),
            Err(cp::Error::Unresolved)
        );
        vault.release.wait();
        assert_eq!(running.join().unwrap().status, IssuanceStatus::Delivered);
    });
    assert_eq!(vault.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn f007_expiry_rotation_and_recovery_preserve_the_original_credential_generation() {
    let t = tempfile::tempdir().unwrap();
    let r = root(&t.path().join("root"));
    let request = initialize(&r);
    let vault = Vault::new(t.path());
    let metadata = issue(&r, &request, &vault);
    let state = r.control_plane_snapshot().unwrap();
    assert_eq!(cp::active_credential(&state, &metadata.issuance, now()).unwrap(), metadata);
    assert!(
        cp::active_credential(&state, &metadata.issuance, metadata.binding.expires_at_ms).is_err()
    );
    let secret = state.current(&metadata.binding.secret).unwrap();
    let cp::Content::Secret(mut next) = secret.content.clone() else { panic!() };
    next.version += 1;
    next.generation += 1;
    let command = cp::AdminCommand {
        request: id("rotate-generation"),
        reference: secret.reference.clone(),
        expected_revision: secret.revision,
        content: cp::Content::Secret(next),
        claims: Default::default(),
        approvals: vec![],
    };
    let (_, _, _, snapshot, _) = issue_fixture(false, Profile::Embedded);
    let caller = caller(&snapshot);
    let authorizer =
        worldinterface_domain::agency::AuthenticatedCaller::from_trusted_authentication(
            snapshot.bindings[0].tenant.clone(),
            snapshot.policy.authorizer.clone(),
        );
    r.authorize_control_administration(
        &authorizer,
        &caller,
        command.clone(),
        now() + 120000,
        now(),
    )
    .unwrap();
    let (cp::Outcome::Revision(revision), fresh) = r
        .control_plane()
        .execute(
            &caller,
            &cp::Request {
                id: command.request.clone(),
                operation: cp::Operation::Admin { command: Box::new(command) },
            },
            now(),
        )
        .unwrap()
    else {
        panic!()
    };
    r.control_plane().revoke(&revision, &vault, fresh).unwrap();
    let rotated = r.control_plane_snapshot().unwrap();
    assert!(cp::active_credential(&rotated, &metadata.issuance, now()).is_err());
    assert_eq!(
        rotated.delivery_attempts[&metadata.issuance],
        state.delivery_attempts[&metadata.issuance]
    );
    assert_eq!(r.reconcile_control_delivery(&metadata.issuance, &vault).unwrap(), metadata);
    assert_eq!(vault.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn f007_restored_unattempted_intent_requires_global_fencing_of_late_original_claims() {
    let t = tempfile::tempdir().unwrap();
    let original = t.path().join("original");
    let restored = t.path().join("restored");
    let r = root(&original);
    let request = initialize(&r);
    let (_, _, _, snapshot, _) = issue_fixture(false, Profile::Embedded);
    let (cp::Outcome::Credential(metadata), fresh) =
        r.control_plane().execute(&caller(&snapshot), &request, now()).unwrap()
    else {
        panic!()
    };
    assert!(fresh);
    drop(r);
    StoreRoot::restore_offline(&original, &restored).unwrap();
    let original = StoreRoot::open_existing(&original).unwrap();
    let restored = StoreRoot::open_existing(&restored).unwrap();
    let vault = Vault::new(t.path());
    vault.fenced.store(false, Ordering::SeqCst);
    assert_eq!(
        restored.reconcile_control_delivery(&metadata.issuance, &vault),
        Err(cp::Error::Unresolved)
    );
    assert_eq!(
        restored.control_plane_snapshot().unwrap().issuances[&metadata.issuance].status,
        IssuanceStatus::Pending
    );
    vault.fenced.store(true, Ordering::SeqCst);
    let reconciled = restored.reconcile_control_delivery(&metadata.issuance, &vault).unwrap();
    assert_eq!(reconciled.status, IssuanceStatus::Unresolved);
    // The original store is independent and can still claim locally. Custody's
    // issuance tombstone must prevent that claim from delivering a capability.
    let late = original.control_plane().deliver(&metadata.issuance, &Clock, &vault, true).unwrap();
    assert_eq!(late.status, IssuanceStatus::Rejected);
    assert!(cp::active_credential(
        &original.control_plane_snapshot().unwrap(),
        &metadata.issuance,
        now()
    )
    .is_err());
    let state = restored.control_plane_snapshot().unwrap();
    assert_eq!(
        restored.reconcile_control_delivery(&metadata.issuance, &vault).unwrap(),
        reconciled
    );
    assert_eq!(restored.control_plane_snapshot().unwrap(), state);
    restored.repository(worldinterface_storage::StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(restored.control_plane_snapshot().unwrap(), state);
}
