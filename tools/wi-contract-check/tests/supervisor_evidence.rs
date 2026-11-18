use serde_json::{json, Value};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};
use wi_contract_check::{
    hash, supervisor_evidence as evidence,
    worker_campaign::{self, Outcome},
};
fn digest(v: &Value) -> String {
    hash(serde_json::to_vec(v).unwrap())
}
fn receipt_digest(v: &Value) -> String {
    let mut bytes = b"WI-FABRIC-2\0wi-json-1\0record\0".to_vec();
    bytes.extend(serde_json::to_vec(v).unwrap());
    hash(bytes)
}
fn put(path: &Path, bytes: &[u8]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
fn observation(case: &str, scenario: &str) -> Value {
    let operation = match case {
        "WP-035" | "WP-036" => "stop",
        "WP-039" => "recover",
        _ => "replace",
    };
    let command = json!({"version":1,"key":"stable-key","request":{"version":1,"operation":operation,"audience":"operator","authority_generation":"a1","policy_revision":"p1","instance":"i","generation":"g1","target":"target","image":"image","deadline_ms":15000,"max_evidence_bytes":1024}});
    let authorization = json!({"tenant":"tenant","principal":"principal","audience":"operator","authority_generation":"a1","policy_revision":"p1"});
    let observation = json!({"state":"completed","evidence":[1,2]});
    let inspected = json!({"version":1,"command":command,"tenant":"tenant","principal":"principal","observation":observation});
    let execution = json!({"provider_key":"stable-key","operation_identity":"original-operation"});
    let receipt = json!({"contract":"WI-FABRIC-2","canonical_profile":"wi-json-1","schema":1,"record":{"kind":"boundary_receipt","body":{"id":"receipt-1","body":{"direction":"egress","body":{"intent_id":"intent-1","outcome":"uncertain","connector_execution":execution}}}}});
    let c = json!({"intent":"intent-1","tenant":"tenant","execution":execution,"status":"confirmed_committed","commit":"confirmed_committed","observations":1,"evidence":["retained-inspect"],"receipts":["reconciliation-receipt"]});
    let receipt_ids = json!([["receipt-1", receipt_digest(&receipt), 1]]);
    json!({"version":1,"kind":"wi-supervisor-observation-v1","case":case,"scenario":scenario,"command":command,"command_digest":digest(&command),"authorization":authorization,
        "backend_command":{"version":1,"command":command,"command_digest":digest(&command),"tenant":"tenant","principal":"principal"},
        "backend_result":{"version":1,"key":"stable-key","command_digest":digest(&command),"observation":observation},"inspect_record":inspected,
        "wi":{"intent":{"id":"intent-1","stable_idempotency_identity":"original-operation"},"original_receipts":receipt_ids,"reconciled_receipts":receipt_ids,"receipt_envelopes":[receipt],"case":c,"case_after_retry":c,"phase":"reconciled"},
        "activation_count":1,"guest_stopped":true,"wi_state_removed":true,"supervisor_restarted":true,"records":{},"build":{}})
}
fn fixture(base: &Path, root: &Path) -> (Value, Vec<Outcome>) {
    put(&root.join("contracts/sources/wi-fabric-2-worker-execution-profile.yaml"), b"profile");
    put(
        &root.join("crates/worldinterface-runtime/tests/support/supervisor_fixture.rs"),
        b"fixture",
    );
    let mut outcomes: Vec<Outcome> = vec![];
    for (case, scenario, test) in evidence::scenarios() {
        let mut v = observation(case, scenario);
        v["build"] = json!({"packet":"packet", "profile_definition":hash(b"profile"),"fixture_source":hash(b"fixture")});
        let prefix = format!("supervisor-records-{case}-{scenario}");
        let files = [
            (format!("{prefix}/backend-command"),digest(&v["command"]).into_bytes()),
            (format!("{prefix}/config.json"),serde_json::to_vec(&json!({"request":v["command"]["request"],"tenant":v["authorization"]["tenant"],"principal":v["authorization"]["principal"]})).unwrap()),
            (
                format!("{prefix}/backend-record.json"),
                serde_json::to_vec(&v["backend_command"]).unwrap(),
            ),
            (
                format!("{prefix}/backend-result.json"),
                serde_json::to_vec(&v["backend_result"]).unwrap(),
            ),
            (format!("{prefix}/activations"), b"1".to_vec()),
            (
                format!("{prefix}/journal/{}.json", hash(b"stable-key")),
                serde_json::to_vec(
                    &json!({"checksum":digest(&v["inspect_record"]),"record":v["inspect_record"]}),
                )
                .unwrap(),
            ),
        ];
        for (name, bytes) in files {
            put(&base.join(&name), &bytes);
            v["records"][name] = json!(hash(bytes));
        }
        let bytes = serde_json::to_vec(&v).unwrap();
        let name = format!("supervisor-{case}-{scenario}.json");
        put(&base.join(&name), &bytes);
        let marker = format!("WI-SUPERVISOR-OBSERVATION {name} {}\n", hash(&bytes));
        if let Some(outcome) = outcomes.iter_mut().find(|o| o.reference.test == test) {
            let path = base.join(&outcome.log);
            let mut log = fs::read_to_string(&path).unwrap();
            log.push_str(&marker);
            put(&path, log.as_bytes());
            outcome.log_sha256 = hash(log);
        } else {
            let log = format!("{case}.log");
            put(&base.join(&log), marker.as_bytes());
            outcomes.push(Outcome {
                reference: wi_contract_check::proof::TestRef {
                    manifest: "crates/worldinterface-runtime/Cargo.toml".into(),
                    package: "worldinterface-runtime".into(),
                    test: test.into(),
                },
                passed: true,
                executable_sha256: hash(b"executable"),
                log,
                log_sha256: hash(marker),
            });
        }
    }
    (
        json!({"evaluation_packet":{"manifest":"packet","acceptance_policy":wi_contract_check::evaluation::POLICY},"supervisor_observations":evidence::bindings(base,&outcomes)}),
        outcomes,
    )
}
#[test]
fn supervisor_observations_reject_contradictions_and_incomplete_wi_evidence() {
    let original = observation("WP-036", "restart");
    evidence::validate_observation(&original, "WP-036", "restart").unwrap();
    for pointer in [
        "/command/key",
        "/command_digest",
        "/authorization/principal",
        "/backend_result/command_digest",
        "/inspect_record/observation/state",
        "/wi/case/status",
        "/wi/case_after_retry/observations",
        "/wi/original_receipts/0/1",
        "/wi/receipt_envelopes/0/record/body/body/body/outcome",
        "/activation_count",
        "/wi_state_removed",
        "/case",
        "/scenario",
    ] {
        let mut v = original.clone();
        *v.pointer_mut(pointer).unwrap() = json!("substituted");
        assert!(evidence::validate_observation(&v, "WP-036", "restart").is_err(), "{pointer}");
    }
    for field in ["original_receipts", "receipt_envelopes"] {
        let mut v = original.clone();
        v["wi"][field] = json!([]);
        assert!(evidence::validate_observation(&v, "WP-036", "restart").is_err());
    }
}
#[test]
fn supervisor_campaign_rejects_missing_substituted_unowned_and_unbound_retained_records() {
    let root = tempfile::tempdir().unwrap();
    let base = tempfile::tempdir().unwrap();
    let (receipt, outcomes) = fixture(base.path(), root.path());
    evidence::validate(root.path(), &receipt, base.path(), &outcomes).unwrap();
    for path in [
        "supervisor-WP-036-restart.json",
        "supervisor-records-WP-036-restart/backend-result.json",
        "WP-036.log",
    ] {
        let p = base.path().join(path);
        let original = fs::read(&p).unwrap();
        put(&p, b"substituted");
        assert!(
            evidence::validate(root.path(), &receipt, base.path(), &outcomes).is_err(),
            "{path}"
        );
        fs::remove_file(&p).unwrap();
        assert!(
            evidence::validate(root.path(), &receipt, base.path(), &outcomes).is_err(),
            "{path}"
        );
        put(&p, &original);
    }
    let extra = base.path().join("supervisor-unowned.json");
    put(&extra, b"{}");
    assert!(evidence::validate(root.path(), &receipt, base.path(), &outcomes).is_err());
    fs::remove_file(extra).unwrap();
    for field in ["executable_sha256", "log_sha256", "owner"] {
        let mut changed = receipt.clone();
        changed["supervisor_observations"]["supervisor-WP-036-restart.json"][field] =
            json!("substituted");
        assert!(evidence::validate(root.path(), &changed, base.path(), &outcomes).is_err());
    }
    let mut unowned = outcomes.clone();
    unowned.retain(|o| !o.reference.test.starts_with("wp_036"));
    assert!(evidence::validate(root.path(), &receipt, base.path(), &unowned).is_err());
    let mut packet = receipt.clone();
    packet["evaluation_packet"]["manifest"] = json!("other packet");
    assert!(evidence::validate(root.path(), &packet, base.path(), &outcomes).is_err());
    evidence::validate(root.path(), &receipt, base.path(), &outcomes).unwrap();
    worker_campaign::validate_claims(&worker_campaign::claims()).unwrap();
}
