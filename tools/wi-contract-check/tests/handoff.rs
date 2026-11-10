//! Non-live publication regressions. Campaign execution remains separately required.
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tempfile::TempDir;
use wi_contract_check::{handoff, read, worker_campaign};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}
fn write(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}
fn fixture() -> TempDir {
    let temp = TempDir::new().unwrap();
    let source = root();
    let descriptor = handoff::descriptor(&source).unwrap();
    let mut paths: Vec<String> =
        descriptor["identities"].as_object().unwrap().keys().cloned().collect();
    paths.push(handoff::DESCRIPTOR.into());
    paths.push("conformance/wi-fabric-2/proof/cases.json".into());
    for gate in descriptor["gates"].as_array().unwrap() {
        paths.push(format!(
            "conformance/wi-fabric-2/proof/gates/{}.json",
            gate["id"].as_str().unwrap()
        ));
    }
    for name in paths {
        let dest = temp.path().join(&name);
        fs::create_dir_all(dest.parent().unwrap()).unwrap();
        fs::copy(source.join(name), dest).unwrap();
    }
    temp
}

#[test]
fn wp_034_publication_rejects_registry_gate_mapping_and_claim_drift() {
    let temp = fixture();
    let value = read(&temp.path().join(handoff::DESCRIPTOR)).unwrap();
    handoff::validate(temp.path(), &value).unwrap();
    for pointer in [
        "/cases/0/tests/0/test",
        "/cases/0/owning_item",
        "/gates/0/pass_criteria/0",
        "/gates/0/effective_on",
        "/execution_claims/milestone_closed",
        "/execution_claims/consumer_task_acceptance",
        "/controls/unknown",
        "/consumer/P1-05",
    ] {
        let mut changed = value.clone();
        *changed.pointer_mut(pointer).unwrap() = json!("substituted");
        assert!(handoff::validate(temp.path(), &changed).is_err(), "{pointer}");
    }
    for key in ["cases", "gates"] {
        let mut changed = value.clone();
        changed[key].as_array_mut().unwrap().pop();
        assert!(handoff::validate(temp.path(), &changed).is_err());
        let extra = changed[key][0].clone();
        changed[key].as_array_mut().unwrap().extend([extra.clone(), extra]);
        assert!(handoff::validate(temp.path(), &changed).is_err());
    }
    let mut missing = value.clone();
    missing["cases"][0]["tests"] = json!([]);
    assert!(handoff::validate(temp.path(), &missing).is_err());
    let registry_path = temp.path().join("conformance/wi-fabric-2/proof/cases.json");
    let mut registry = read(&registry_path).unwrap();
    registry["cases"][0]["tests"][0]["test"] = json!("changed-authoritative-test");
    write(&registry_path, &registry);
    assert!(handoff::validate(temp.path(), &value).is_err());
    fs::remove_file(temp.path().join(handoff::DESCRIPTOR)).unwrap();
    assert!(!handoff::check(temp.path()).unwrap().is_empty());
}

#[test]
fn wp_034_publication_rejects_stale_profile_fixture_lock_and_recipe_pins() {
    let temp = fixture();
    let value = read(&temp.path().join(handoff::DESCRIPTOR)).unwrap();
    for path in [
        "contracts/sources/wi-fabric-2-worker-execution-profile.yaml",
        "crates/worldinterface-runtime/src/bin/wi-process-fixture.rs",
        "crates/worldinterface-runtime/Cargo.lock",
        "scripts/build-wi-worker-evaluation.sh",
        "scripts/wi_worker_evaluation.py",
    ] {
        let path = temp.path().join(path);
        let original = fs::read(&path).unwrap();
        fs::write(&path, b"substituted").unwrap();
        assert!(handoff::validate(temp.path(), &value).is_err());
        fs::write(path, original).unwrap();
    }
    let mut changed = value.clone();
    changed["aq"]["runtime_release"]["tag"] = json!("v0.2.0");
    assert!(handoff::validate(temp.path(), &changed).is_err());
    let packet = json!({"fixture":"non-live-packet"});
    let record = wi_contract_check::evaluation::packet_record(&packet).unwrap();
    wi_contract_check::evaluation::validate_record(&record, &packet).unwrap();
    for changed in [
        json!({"manifest":record["manifest"]}),
        json!({"manifest":"substituted","acceptance_policy":wi_contract_check::evaluation::POLICY}),
        json!({"manifest":record["manifest"],"acceptance_policy":"receipt-expiry"}),
    ] {
        assert!(wi_contract_check::evaluation::validate_record(&changed, &packet).is_err());
    }
    assert!(
        wi_contract_check::evaluation::validate_record(&record, &json!({"changed":true})).is_err()
    );
    handoff::validate(temp.path(), &value).unwrap();
}

#[test]
fn wp_034_publication_requires_complete_gate_state_and_valid_descriptor() {
    let temp = fixture();
    let descriptor = read(&temp.path().join(handoff::DESCRIPTOR)).unwrap();
    let states: Value = descriptor["gates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| (g["id"].as_str().unwrap().to_string(), g["status"].clone()))
        .collect();
    worker_campaign::validate_gates(temp.path(), &states).unwrap();
    for id in ["WI-WORKER-G4", "WI-EMBEDDED-WORKER-PROOF-1", "WI-AQ-G1"] {
        let mut mixed = states.clone();
        mixed[id] = json!("open");
        assert!(worker_campaign::validate_gates(temp.path(), &mixed).is_err());
    }
    let mut substituted = states.clone();
    substituted.as_object_mut().unwrap().remove("WI-AQ-G1");
    substituted["unknown-gate"] = json!("closed");
    assert!(worker_campaign::validate_gates(temp.path(), &substituted).is_err());
    fs::remove_file(temp.path().join(handoff::DESCRIPTOR)).unwrap();
    assert!(worker_campaign::validate_gates(temp.path(), &states).is_err());
    let mut prepublication = states;
    prepublication["WI-WORKER-G4"] = json!("open");
    prepublication["WI-EMBEDDED-WORKER-PROOF-1"] = json!("open");
    worker_campaign::validate_gates(temp.path(), &prepublication).unwrap();
    // Regenerating both representations cannot waive the all-closed rule.
    let path = temp.path().join("conformance/wi-fabric-2/proof/gates/WI-WORKER-G4.json");
    let mut gate = read(&path).unwrap();
    gate["status"] = json!("open");
    write(&path, &gate);
    let open = handoff::descriptor(temp.path()).unwrap();
    assert!(handoff::validate(temp.path(), &open).is_err());
}

#[test]
fn wp_034_publication_binds_external_evidence_and_never_grants_reviewed_closure() {
    let source = fixture();
    let evidence = TempDir::new().unwrap();
    for name in ["coding.json", "transform.json", "seam.json"] {
        write(&evidence.path().join(name), &json!({"fixture":"non-live"}));
    }
    let receipt = json!({"after":{"commit":"fixture-commit","tree":"fixture-tree"},
        "claims":worker_campaign::claims(),"cases":{"WP-034":true}});
    let publication = handoff::publication(source.path(), &receipt, evidence.path()).unwrap();
    handoff::validate_publication(source.path(), &publication, &receipt, evidence.path()).unwrap();
    for pointer in [
        "/descriptor_sha256",
        "/producer/commit",
        "/campaign_sha256",
        "/evidence/producer_execution/0/fixture",
        "/claims/milestone_closed",
        "/reviewed_closure/established_by_this_execution",
    ] {
        let mut changed = publication.clone();
        *changed.pointer_mut(pointer).unwrap() = json!("substituted");
        assert!(handoff::validate_publication(source.path(), &changed, &receipt, evidence.path())
            .is_err());
    }
    write(&evidence.path().join("seam.json"), &json!({"substituted":true}));
    assert!(handoff::validate_publication(source.path(), &publication, &receipt, evidence.path())
        .is_err());
    fs::remove_file(evidence.path().join("coding.json")).unwrap();
    assert!(handoff::publication(source.path(), &receipt, evidence.path()).is_err());
    assert!(handoff::verify(source.path(), evidence.path()).is_err());
}
