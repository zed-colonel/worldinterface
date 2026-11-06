use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use wi_contract_check::{
    proof::{self, Registry, TestRef},
    worker_campaign as campaign,
};
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn reference(test: &str) -> TestRef {
    TestRef { manifest: "fixture".into(), package: "fixture".into(), test: test.into() }
}
#[test]
fn wp_033_inflated_claims_cannot_convert_worker_success_to_consumer_acceptance() {
    campaign::validate_claims(&campaign::claims()).unwrap();
    for field in [
        "consumer_task_acceptance",
        "ordinary_use",
        "parent_acceptance",
        "g5",
        "remote_parity",
        "release",
        "strong_isolation",
        "milestone_closed",
    ] {
        let mut claims = campaign::claims();
        claims[field] = json!(true);
        assert!(campaign::validate_claims(&claims).is_err(), "{field}");
    }
    let mut claims = campaign::claims();
    claims["eligible_milestone"] = json!("WI-FABRIC-2");
    assert!(campaign::validate_claims(&claims).is_err());
}
#[test]
fn wp_034_handoff_rejects_substituted_pins_missing_evidence_and_semantic_completion() {
    let expected = json!({"producer":{"commit":"a","tree":"b","sources":{"profile":"p","fixture":"f"}},"tests":["exact-test"],"observations":{"coding":"digest"},"claims":campaign::claims()});
    campaign::validate_contract(&expected, &expected).unwrap();
    for pointer in [
        "/producer/commit",
        "/producer/tree",
        "/producer/sources/profile",
        "/producer/sources/fixture",
        "/tests/0",
        "/observations/coding",
        "/claims/worker_completion",
        "/claims/consumer_task_acceptance",
    ] {
        let mut candidate = expected.clone();
        *candidate.pointer_mut(pointer).unwrap() = json!("substituted");
        assert!(campaign::validate_contract(&candidate, &expected).is_err(), "{pointer}");
    }
    let mut candidate = expected.clone();
    candidate.as_object_mut().unwrap().remove("tests");
    assert!(campaign::validate_contract(&candidate, &expected).is_err());
}
#[test]
fn campaign_dirty_source_changing_or_stale_generation_is_rejected() {
    let before = json!({"clean":true,"commit":"a","tree":"b","sources":{"lock":"1"}});
    campaign::validate_generation(&before, &before).unwrap();
    for pointer in ["/clean", "/commit", "/tree", "/sources/lock"] {
        let mut after = before.clone();
        *after.pointer_mut(pointer).unwrap() = json!("changed");
        assert!(campaign::validate_generation(&before, &after).is_err());
    }
    assert!(
        campaign::validate_generation(&json!({"clean":false}), &json!({"clean":false})).is_err()
    );
}
#[test]
fn campaign_missing_ignored_ambiguous_and_recursive_references_are_rejected() {
    let (mut registry, _) = proof::load(&root()).unwrap();
    let mut harnesses = registry
        .cases
        .iter()
        .flat_map(|c| c.tests.iter().cloned().map(|t| (t, vec![PathBuf::from("fixture")])))
        .collect::<BTreeMap<_, _>>();
    campaign::select(&registry, &harnesses).unwrap();
    let t = registry.cases[0].tests[0].clone();
    harnesses.remove(&t);
    assert!(campaign::select(&registry, &harnesses).is_err());
    harnesses.insert(t.clone(), vec![]);
    assert!(campaign::select(&registry, &harnesses).is_err());
    harnesses.insert(t.clone(), vec!["a".into(), "b".into()]);
    assert!(campaign::select(&registry, &harnesses).is_err());
    harnesses.insert(t, vec!["a".into()]);
    registry.cases[0].tests.push(reference(campaign::ENTRY));
    assert!(campaign::select(&registry, &harnesses).is_err());
}
#[test]
fn campaign_real_child_failure_zero_ignored_missing_and_altered_logs_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("fixture.rs");
    let executable = temp.path().join("fixture");
    fs::write(&source,"#[test] fn good() {}\n#[test] fn bad() {panic!(\"child failed\")}\n#[test] #[ignore] fn ignored() {}\n").unwrap();
    assert!(Command::new("timeout")
        .args(["60s", "rustc", "--test"])
        .arg(&source)
        .arg("-o")
        .arg(&executable)
        .status()
        .unwrap()
        .success());
    let (mut registry, _) = proof::load(&root()).unwrap();
    registry.cases.truncate(1);
    registry.cases[0].tests = vec![reference("good")];
    let good = campaign::execute(temp.path(), reference("good"), &executable, temp.path()).unwrap();
    campaign::validate_results(&registry, std::slice::from_ref(&good), temp.path()).unwrap();
    assert!(campaign::validate_results(&registry, &[], temp.path()).is_err());
    for name in ["bad", "ignored", "missing"] {
        assert!(
            !campaign::execute(temp.path(), reference(name), &executable, temp.path())
                .unwrap()
                .passed
        );
    }
    assert!(
        campaign::validate_results(&registry, &[good.clone(), good.clone()], temp.path()).is_err()
    );
    fs::write(temp.path().join(&good.log), "altered").unwrap();
    assert!(campaign::validate_results(&registry, &[good], temp.path()).is_err());
}
#[test]
fn campaign_coverage_requires_every_embedded_obligation_and_preserves_scope_tests() {
    let (registry, _) = proof::load(&root()).unwrap();
    let index =
        registry.cases.iter().flat_map(|c| c.tests.iter().cloned().map(|t| (t, 1))).collect();
    campaign::validate_coverage(&root(), &registry, &index).unwrap();
    let missing = Registry { version: registry.version, cases: vec![] };
    assert!(campaign::validate_coverage(&root(), &missing, &index).is_err());
    assert!(campaign::validate_coverage(&root(), &registry, &BTreeMap::new()).is_err());
}
