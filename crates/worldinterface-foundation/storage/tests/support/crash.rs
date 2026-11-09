//! Test-only subprocess barriers: production builds contain no environment fault switches.
use crate::*;
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::{AuthorizationSnapshot, VerifiedSnapshot},
    effect::{
        validate_command, CommitObservation, EffectAction, OutcomeEvidence, ValidatedEffectCommand,
        VerifiedOutcome,
    },
    ingress::digest,
    ports::EffectAcknowledgment,
};
use worldinterface_domain::{Action, Event, Machine};
use worldinterface_protocol::*;
struct KillOnDrop(std::process::Child);
impl std::ops::Deref for KillOnDrop {
    type Target = std::process::Child;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for KillOnDrop {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub(crate) fn checkpoint(name: &str) {
    if std::env::var("WI_CRASH_POINT").as_deref() == Ok(name) {
        fs::write(std::env::var_os("WI_CRASH_READY").unwrap(), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
}
pub(crate) fn prepare(c: &rusqlite::Connection) {
    if std::env::var_os("WI_CRASH_ROOT").is_some() {
        c.execute_batch("PRAGMA cache_size=1; PRAGMA cache_spill=ON;").unwrap();
    }
}
fn profile() -> StoreProfile {
    StoreProfile {
        instance: InstanceId::generate(),
        created_at: "2026-09-19T00:00:00.000000000Z".to_string().try_into().unwrap(),
        platform: false,
    }
}
fn submission() -> Envelope {
    let v: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../../../conformance/wi-fabric-2/domain/canonical-vectors.json"
    ))
    .unwrap();
    Envelope::parse(&serde_json::to_vec(&v[0]["envelope"]).unwrap()).unwrap()
}
fn submit(root: &StoreRoot) -> EntityId {
    let id: EntityId = "12345678-1234-4234-8234-123456789abc".to_string().try_into().unwrap();
    root.repository(StoreKind::Fabric)
        .unwrap()
        .admit(
            AdmissionScope {
                deployment_tenant: "tenant".to_string().try_into().unwrap(),
                authenticated_principal: "12345678-1234-4234-8234-123456789def"
                    .to_string()
                    .try_into()
                    .unwrap(),
            },
            submission(),
            id.clone(),
            Event {
                entity: id.clone(),
                machine: Machine::Effect,
                action: Action::Submit,
                at: profile().created_at,
            },
            OutboxEntry {
                key: "stable".to_string().try_into().unwrap(),
                entity: id,
                payload: "input".to_string().try_into().unwrap(),
                delivered: false,
            },
        )
        .unwrap()
}
#[test]
fn child() {
    let Some(path) = std::env::var_os("WI_CRASH_ROOT") else {
        return;
    };
    let path = Path::new(&path);
    let point = std::env::var("WI_CRASH_POINT").unwrap();
    if point.starts_with("initialization") {
        let _ = StoreRoot::create_empty(path, profile());
    } else if point.starts_with("backup-") {
        StoreRoot::backup_offline(path, path.with_extension("backup")).unwrap();
    } else {
        let root = StoreRoot::open_existing(path).unwrap();
        if point.starts_with("publication-") {
            if point == "publication-identified" {
                root.stage_publication(
                    "publication:1".to_string().try_into().unwrap(),
                    "tenant".to_string().try_into().unwrap(),
                    b"bytes",
                )
                .unwrap();
            } else {
                root.publish_staged(&"publication:1".to_string().try_into().unwrap(), b"bytes")
                    .unwrap();
            }
        } else if point == "artifact-durable" {
            root.publish("tenant".to_string().try_into().unwrap(), b"bytes").unwrap();
        } else if point.starts_with("fabric-receipt-") {
            let (command, id, execution, temporal) = fabric_receipt_inputs(&root, path);
            root.effect_transact_with_receipt(&command, id, &execution, temporal).unwrap();
        } else {
            submit(&root);
        }
    }
    panic!("child did not reach requested barrier");
}

fn fabric_fixture() -> (EffectSubmission, AuthorizationSnapshot) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../conformance/wi-fabric-2/egress/fixture.json"
    ))
    .unwrap();
    (
        serde_json::from_value(fixture["submission"].clone()).unwrap(),
        serde_json::from_value(fixture["snapshot"].clone()).unwrap(),
    )
}

fn fabric_caller(p: &AuthorizationSnapshot) -> AuthenticatedCaller {
    AuthenticatedCaller::from_trusted_authentication(
        p.bindings[0].tenant.clone(),
        p.bindings[0].principal.clone(),
    )
}

fn fabric_apply(root: &StoreRoot, p: &AuthorizationSnapshot, action: EffectAction) {
    let caller = fabric_caller(p);
    let command = validate_command(
        &root.effect_snapshot(caller.tenant()).unwrap(),
        &caller,
        profile().created_at,
        action,
    )
    .unwrap();
    root.effect_transact(&command).unwrap();
}

fn prepare_fabric_receipt(root: &StoreRoot, path: &Path) -> (OpaqueRef, ReceiptId) {
    let (submission, p) = fabric_fixture();
    let tenant = p.bindings[0].tenant.clone();
    for name in [
        "input",
        "executable",
        "dependency",
        "configuration",
        "profile-definition",
        "adapter",
        "resolution",
        "grant",
        "fixture-review",
        "worker",
    ] {
        root.publish(tenant.clone(), name.as_bytes()).unwrap();
    }
    let id = EffectIntentId::generate();
    let snapshot = || VerifiedSnapshot::from_trusted_verifier(p.clone()).unwrap();
    fabric_apply(
        root,
        &p,
        EffectAction::Submit {
            id: id.clone(),
            submission: Box::new(submission),
            snapshot: snapshot(),
            link: None,
        },
    );
    fabric_apply(root, &p, EffectAction::Resolve { id: id.clone(), snapshot: snapshot() });
    fabric_apply(root, &p, EffectAction::Authorize { id: id.clone(), snapshot: snapshot() });
    fabric_apply(root, &p, EffectAction::Prepare { id: id.clone(), snapshot: snapshot() });
    let state = root.effect_snapshot(&tenant).unwrap();
    let request = &state.effects[&id].dispatches[0].request;
    let ack = EffectAcknowledgment {
        task: request.task.clone(),
        admission_key: request.admission_key.clone(),
        request_digest: digest(request).unwrap(),
        durable_sequence: 1,
    };
    fabric_apply(root, &p, EffectAction::Acknowledge { id: id.clone(), acknowledgment: ack });
    fabric_apply(
        root,
        &p,
        EffectAction::Start {
            id,
            snapshot: snapshot(),
            attempt: "attempt:1".to_string().try_into().unwrap(),
        },
    );
    let receipt_id = ReceiptId::generate();
    fs::write(path.with_extension("receipt-id"), receipt_id.as_str()).unwrap();
    (tenant, receipt_id)
}

fn fabric_receipt_inputs(
    root: &StoreRoot,
    path: &Path,
) -> (ValidatedEffectCommand, ReceiptId, ExecutionRecord, TemporalProvenance) {
    let (_, p) = fabric_fixture();
    let caller = fabric_caller(&p);
    let state = root.effect_snapshot(caller.tenant()).unwrap();
    let effect = state.effects.values().next().unwrap();
    let id = effect.intent.id.clone();
    let outcome = OutcomeEvidence {
        classification: None,
        reconciliation_admission: None,
        intent: id.clone(),
        attempt: "attempt:1".to_string().try_into().unwrap(),
        connector_semantics: p.registry.entries[0].semantics.clone(),
        commit: CommitObservation::Unknown,
        outcome: ExecutionOutcome::Uncertain,
        evidence: vec![p.grants[0].evidence.clone()],
        required_outputs: p.required_outputs.clone(),
        durable_outputs: vec![],
        boundary_record: None,
    };
    let command = validate_command(
        &state,
        &caller,
        profile().created_at,
        EffectAction::Outcome {
            id: id.clone(),
            evidence: VerifiedOutcome::from_trusted_verifier(outcome.clone()),
        },
    )
    .unwrap();
    let execution = ExecutionRecord {
        classification: None,
        connector_execution: None,
        intent_id: id,
        attempt: outcome.attempt,
        outcome: outcome.outcome,
        evidence: outcome.evidence,
        recorded_at: profile().created_at,
    };
    let time = TimeEvidence {
        at: profile().created_at,
        source: "fixture".to_string().try_into().unwrap(),
        assurance: "fixture".to_string().try_into().unwrap(),
        uncertainty_nanos: 0,
    };
    let temporal = TemporalProvenance {
        occurred_at: None,
        observed_at: None,
        received_at: time.clone(),
        admitted_at: time,
        dispatched_at: None,
        committed_at: None,
        reconciled_at: None,
        sequence: None,
    };
    let receipt_id =
        fs::read_to_string(path.with_extension("receipt-id")).unwrap().try_into().unwrap();
    (command, receipt_id, execution, temporal)
}

#[test]
fn killed_fabric_receipt_transaction_recovers_all_or_none_and_retries() {
    for point in [
        "fabric-receipt-history",
        "fabric-receipt-state",
        "fabric-receipt-outbox",
        "fabric-receipt-index",
        "fabric-receipt-committed",
    ] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("root");
        let root = StoreRoot::create_empty(&path, profile()).unwrap();
        let (tenant, receipt_id) = prepare_fabric_receipt(&root, &path);
        let (command, _, execution, temporal) = fabric_receipt_inputs(&root, &path);
        let before = root.effect_snapshot(&tenant).unwrap();
        drop(root);
        kill_at(&path, point);
        let root = StoreRoot::open_existing(&path).unwrap();
        let committed = point == "fabric-receipt-committed";
        let after = root.effect_snapshot(&tenant).unwrap();
        let c = root.connect(StoreKind::Fabric).unwrap();
        let count: u64 =
            c.query_row("SELECT count(*) FROM receipt_history", [], |r| r.get(0)).unwrap();
        let indexed: u64 =
            c.query_row("SELECT count(*) FROM receipt_index", [], |r| r.get(0)).unwrap();
        let histories: u64 =
            c.query_row("SELECT count(*) FROM effect_history", [], |r| r.get(0)).unwrap();
        assert_eq!(
            (count, indexed, histories),
            (u64::from(committed), u64::from(committed), before.revision + u64::from(committed)),
            "{point}"
        );
        assert_eq!(after.revision, before.revision + u64::from(committed));
        assert_eq!(after.effects.values().next().unwrap().outcomes.len(), usize::from(committed));
        root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        let digest = root
            .effect_transact_with_receipt(
                &command,
                receipt_id.clone(),
                &execution,
                temporal.clone(),
            )
            .unwrap();
        assert_eq!(
            root.effect_transact_with_receipt(&command, receipt_id, &execution, temporal).unwrap(),
            digest
        );
        drop(root);
        StoreRoot::backup_offline(&path, t.path().join("backup")).unwrap();
        StoreRoot::restore_offline(t.path().join("backup"), t.path().join("restored")).unwrap();
        StoreRoot::open_existing(t.path().join("restored")).unwrap();
    }
}
#[test]
fn killed_publication_resumes_from_each_durable_boundary() {
    for point in ["publication-identified", "publication-durable", "publication-availability"] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("root");
        let root = StoreRoot::create_empty(&path, profile()).unwrap();
        if point != "publication-identified" {
            root.stage_publication(
                "publication:1".to_string().try_into().unwrap(),
                "tenant".to_string().try_into().unwrap(),
                b"bytes",
            )
            .unwrap();
        }
        drop(root);
        kill_at(&path, point);
        let root = StoreRoot::open_existing(&path).unwrap();
        let reference = root
            .publish_staged(&"publication:1".to_string().try_into().unwrap(), b"bytes")
            .unwrap();
        assert_eq!(root.read_artifact(&reference).unwrap(), b"bytes");
        drop(root);
        StoreRoot::open_existing(&path).unwrap();
    }
}
fn kill_at(path: &Path, point: &str) {
    kill_at_with_preload(path, point, None);
}
fn kill_at_with_preload(path: &Path, point: &str, preload: Option<&Path>) {
    let ready = path.with_extension("ready");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "crash::child", "--nocapture"])
        .env("WI_CRASH_ROOT", path)
        .env("WI_CRASH_POINT", point)
        .env("WI_CRASH_READY", &ready)
        .stdout(Stdio::null());
    if let Some(preload) = preload {
        command.env("LD_PRELOAD", preload);
    }
    let mut child = KillOnDrop(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.exists() && Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("child exited before {point}: {status}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let reached = ready.exists();
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(reached, "barrier {point} timed out");
}
#[test]
fn killed_transactions_artifacts_and_publication_recover_atomically() {
    for point in [
        "initialization-store",
        "initialization-publish",
        "after-records",
        "after-outbox",
        "after-commit",
        "artifact-durable",
        "backup-publish",
        "backup-renamed",
    ] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("root");
        if !point.starts_with("initialization") {
            drop(StoreRoot::create_empty(&path, profile()).unwrap());
        }
        kill_at(&path, point);
        if point.starts_with("initialization") {
            assert!(StoreRoot::open_existing(&path).is_err());
            continue;
        }
        if point == "backup-publish" {
            assert!(!path.with_extension("backup").exists());
            StoreRoot::backup_offline(&path, path.with_extension("backup")).unwrap();
        }
        if point.starts_with("backup-") {
            drop(StoreRoot::open_existing(path.with_extension("backup")).unwrap());
        }
        if point == "after-outbox" {
            let journal = fs::read(path.join("fabric.sqlite-journal")).unwrap();
            assert_eq!(
                &journal[..8],
                &[0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7],
                "must exercise a hot journal"
            );
            let manifest_path = path.join("manifest.json");
            let original = fs::read(&manifest_path).unwrap();
            let mut bad: serde_json::Value = serde_json::from_slice(&original).unwrap();
            bad["schema"] = 999.into();
            fs::write(&manifest_path, serde_json::to_vec(&bad).unwrap()).unwrap();
            let db = fs::read(path.join("fabric.sqlite")).unwrap();
            assert!(StoreRoot::open_existing(&path).is_err());
            assert_eq!(fs::read(path.join("fabric.sqlite-journal")).unwrap(), journal);
            assert_eq!(fs::read(path.join("fabric.sqlite")).unwrap(), db);
            fs::write(&manifest_path, original).unwrap();
        }
        let root = StoreRoot::open_existing(&path).unwrap();
        if point.starts_with("after-") {
            let c = root.connect(StoreKind::Fabric).unwrap();
            let count = |table: &str| {
                c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get::<_, i64>(0))
                    .unwrap()
            };
            let expected = if point == "after-commit" { 1 } else { 0 };
            for table in ["records", "outbox", "history", "admissions", "projections"] {
                assert_eq!(count(table), expected, "{point}/{table}");
            }
        }
        if point == "artifact-durable" {
            let a = ArtifactRef {
                scope: "tenant".to_string().try_into().unwrap(),
                content: ContentHash::of(b"bytes"),
                size: 5,
            };
            assert!(root.read_artifact(&a).is_err());
            assert_eq!(root.publish(a.scope.clone(), b"bytes").unwrap(), a);
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn killed_journal_creation_and_partial_headers_reopen_and_retry() {
    let build = tempfile::tempdir().unwrap();
    let library = build.path().join("journal-fault.so");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/journal-fault.c");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(source)
        .args(["-ldl", "-o"])
        .arg(&library)
        .status()
        .unwrap()
        .success());
    for size in [0, 1, 4, 7, 8, 28, 511] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("root");
        drop(StoreRoot::create_empty(&path, profile()).unwrap());
        let original = fs::read(path.join("fabric.sqlite")).unwrap();
        let point = if size == 0 {
            "journal-created".to_string()
        } else {
            format!("journal-header-{size}")
        };
        kill_at_with_preload(&path, &point, Some(&library));
        let journal = path.join("fabric.sqlite-journal");
        assert_eq!(fs::metadata(&journal).unwrap().len(), size);
        assert_eq!(fs::read(path.join("fabric.sqlite")).unwrap(), original);
        let root = StoreRoot::open_existing(&path).unwrap();
        assert!(!journal.exists());
        let count: i64 = root
            .connect(StoreKind::Fabric)
            .unwrap()
            .query_row("SELECT count(*) FROM outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        let id = submit(&root);
        assert_eq!(submit(&root), id);
        drop(root);
        let root = StoreRoot::open_existing(&path).unwrap();
        let outbox = root.repository(StoreKind::Fabric).unwrap().outbox(true, None, 10).unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].entity, id);
    }
}

#[test]
fn malformed_journals_and_invalid_recovery_copies_leave_original_untouched() {
    for damage in ["foreign-magic", "truncated-hot", "identity", "schema", "history", "artifact"] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("root");
        let root = StoreRoot::create_empty(&path, profile()).unwrap();
        submit(&root);
        let artifact = root.publish("tenant".to_string().try_into().unwrap(), b"bytes").unwrap();
        let c = root.connect(StoreKind::Fabric).unwrap();
        // Corrupt persisted contents without changing the expected schema/triggers.
        c.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false).unwrap();
        match damage {
            "identity" => {
                c.execute("UPDATE store_identity SET manifest=x'00'", []).unwrap();
            }
            "schema" => {
                c.execute_batch("PRAGMA user_version=999").unwrap();
            }
            "history" => {
                c.execute("DELETE FROM history", []).unwrap();
            }
            "artifact" => {
                fs::write(path.join("artifacts").join(artifact.content.as_str()), b"bad").unwrap();
            }
            _ => {}
        }
        drop(c);
        drop(root);
        let journal = path.join("fabric.sqlite-journal");
        let bytes: &[u8] = match damage {
            "foreign-magic" => b"not a SQLite journal",
            "truncated-hot" => &[0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7],
            _ => &[],
        };
        fs::write(&journal, bytes).unwrap();
        let db = fs::read(path.join("fabric.sqlite")).unwrap();
        assert!(StoreRoot::open_existing(&path).is_err(), "{damage}");
        assert_eq!(fs::read(&journal).unwrap(), bytes, "{damage}");
        assert_eq!(fs::read(path.join("fabric.sqlite")).unwrap(), db, "{damage}");
    }
}
