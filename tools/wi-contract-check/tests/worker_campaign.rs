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
    let temp = tempfile::tempdir().unwrap();
    let observation = |seam| {
        let controls = ["requested", "advertised", "applied", "observed"]
            .into_iter()
            .map(|key| {
                (
                    key.to_string(),
                    json!([
                        {"kind":"wall_time_ms","state":{"supported":1500}},
                        {"kind":"output_bytes","state":{"supported":65536}},
                        {"kind":"memory_bytes","state":"unsupported"}
                    ]),
                )
            })
            .chain([
                ("schema".into(), json!(1)),
                ("provider".into(), json!("fixture-provider")),
                ("run".into(), json!("fixture-run")),
            ])
            .collect::<serde_json::Map<_, _>>();
        json!({"schema":1,"evidence":{
            "binding":{"tenant":"fixture-tenant","principal":"fixture-principal",
                "provider":"fixture-provider","run":"fixture-run","request":"r","realization":"v"},
            "seam_simulation":seam,"controls":if seam {serde_json::Value::Null} else {json!(controls)},
            "cleanup":{"exit_confirmed":true,"exit_success":true,"cleanup_confirmed":true,
                "retained_output_bytes":12,"term_signals":0,"kill_signals":0,"descendants_observed":0}},
            "build":{"packet":"fixture-packet","fixture":"fixture-binary","helper":"helper-binary"},
            "completed":true,"capacity_reserved":false,"receipt_count":6})
    };
    let mut observations = serde_json::Map::new();
    for (name, seam) in [("coding.json", false), ("transform.json", false), ("seam.json", true)] {
        let value = observation(seam);
        campaign::validate_observation(&value, seam).unwrap();
        let bytes = serde_json::to_vec(&value).unwrap();
        observations.insert(name.into(), json!(wi_contract_check::hash(&bytes)));
        fs::write(temp.path().join(name), bytes).unwrap();
    }
    let receipt = json!({"version":2,
        "after":{"commit":"fixture-commit","tree":"fixture-tree",
            "sources":{"profile":"fixture-profile","fixture":"fixture-source"}},
        "outcomes":[{"reference":reference("exact-test"),"passed":true}],
        "runtime_suite_outcomes":[],"embedded_observations":observations,
        "cases":{"WP-034":true},"gates":{"WI-WORKER-G4":"open"},"claims":campaign::claims()});
    let expected = campaign::handoff_candidate(&receipt, temp.path()).unwrap();
    campaign::validate_handoff_payload(&expected, &receipt, temp.path()).unwrap();
    for pointer in [
        "/producer/commit",
        "/producer/tree",
        "/producer/sources/profile",
        "/producer/sources/fixture",
        "/tests/0",
        "/observations/coding.json",
        "/producer_execution/0/build/fixture",
        "/producer_execution/2/evidence/controls",
        "/claims/worker_completion",
        "/claims/consumer_task_acceptance",
    ] {
        let mut candidate = expected.clone();
        *candidate.pointer_mut(pointer).unwrap() = json!("substituted");
        assert!(
            campaign::validate_handoff_payload(&candidate, &receipt, temp.path()).is_err(),
            "{pointer}"
        );
    }
    for field in ["tests", "producer_execution", "observations", "runtime_suite_tests"] {
        let mut candidate = expected.clone();
        candidate.as_object_mut().unwrap().remove(field);
        assert!(
            campaign::validate_handoff_payload(&candidate, &receipt, temp.path()).is_err(),
            "{field}"
        );
    }
    // Embedded evidence is loaded by generation, not copied from the candidate.
    fs::remove_file(temp.path().join("seam.json")).unwrap();
    assert!(campaign::validate_handoff_payload(&expected, &receipt, temp.path()).is_err());
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

#[test]
fn campaign_parallel_harnesses_retain_failures_and_reject_substituted_binary_pins() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("fixture.rs");
    let left = temp.path().join("left");
    let right = temp.path().join("right");
    fs::write(
        &source,
        r#"
#[test] fn bad() { panic!("retain this failure"); }
#[test] fn replace() {
    let exe = std::env::current_exe().unwrap();
    let replacement = exe.with_extension("replacement");
    std::fs::write(&replacement, b"substituted during execution").unwrap();
    std::fs::rename(replacement, exe).unwrap();
}
#[test] fn peer() {
    let base = std::path::PathBuf::from(std::env::var_os("WI_WORKER_CAMPAIGN_EVIDENCE").unwrap());
    let exe = std::env::current_exe().unwrap();
    let name = exe.file_name().unwrap().to_str().unwrap();
    std::fs::write(base.join(format!("{name}.ready")), b"ready").unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !(base.join("left.ready").exists() && base.join("right.ready").exists()) {
        assert!(std::time::Instant::now() < deadline, "harnesses did not overlap");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
"#,
    )
    .unwrap();
    assert!(Command::new("timeout")
        .args(["60s", "rustc", "--test"])
        .arg(&source)
        .arg("-o")
        .arg(&left)
        .status()
        .unwrap()
        .success());
    fs::copy(&left, &right).unwrap();
    let mut selected = BTreeMap::new();
    for (package, executable) in [("left", &left), ("right", &right)] {
        for test in ["bad", "peer"] {
            let mut t = reference(test);
            t.package = package.into();
            selected.insert(t, executable.clone());
        }
    }
    let outcomes = campaign::execute_selected(temp.path(), &selected, temp.path()).unwrap();
    assert_eq!(outcomes.len(), 4);
    for outcome in &outcomes {
        assert_eq!(outcome.passed, outcome.reference.test == "peer");
        assert!(temp.path().join(&outcome.log).is_file());
    }
    campaign::validate_executables(&outcomes, &selected).unwrap();
    let mut conflicting = outcomes.clone();
    conflicting[1].executable_sha256 = "0".repeat(64);
    assert!(campaign::validate_executables(&conflicting, &selected).is_err());
    fs::write(&right, b"substituted").unwrap();
    assert!(campaign::validate_executables(&outcomes, &selected).is_err());
    let replaced = BTreeMap::from([(reference("replace"), left)]);
    assert!(campaign::execute_selected(temp.path(), &replaced, temp.path()).is_err());
}

#[test]
fn campaign_runtime_remainder_runs_unregistered_tests_without_repeating_registered_or_entry() {
    let reference = |name: &str| TestRef {
        manifest: "crates/worldinterface-runtime/Cargo.toml".into(),
        package: "worldinterface-runtime".into(),
        test: name.into(),
    };
    let mut harnesses = BTreeMap::new();
    for name in ["registered", "unregistered", campaign::ENTRY] {
        harnesses.insert(reference(name), vec![PathBuf::from(name)]);
    }
    let registered = BTreeMap::from([(reference("registered"), PathBuf::from("registered"))]);
    let rest = campaign::runtime_remainder(&harnesses, &registered).unwrap();
    assert_eq!(
        rest,
        BTreeMap::from([(reference("unregistered"), vec![PathBuf::from("unregistered")])])
    );
    harnesses.get_mut(&reference("unregistered")).unwrap().push("ambiguous".into());
    assert_eq!(
        campaign::runtime_remainder(&harnesses, &registered).unwrap()[&reference("unregistered")]
            .len(),
        2
    );
    harnesses.insert(reference("unregistered"), vec![]);
    assert!(campaign::runtime_remainder(&harnesses, &registered).is_err());
}

#[test]
fn campaign_shared_runtime_support_tests_retain_each_harness_and_reject_omission_or_substitution() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("fixture.rs");
    fs::write(&source, "#[test] fn shared() {}\n").unwrap();
    let left = temp.path().join("left");
    let right = temp.path().join("right");
    assert!(Command::new("timeout")
        .args(["60s", "rustc", "--test"])
        .arg(source)
        .arg("-o")
        .arg(&left)
        .status()
        .unwrap()
        .success());
    fs::copy(&left, &right).unwrap();
    let reference = reference("shared");
    let expected = BTreeMap::from([(reference.clone(), vec![left.clone(), right.clone()])]);
    let first = campaign::execute(temp.path(), reference.clone(), &left, temp.path()).unwrap();
    let second = campaign::execute(temp.path(), reference, &right, temp.path()).unwrap();
    assert_ne!(first.log, second.log);
    let outcomes = vec![first.clone(), second];
    campaign::validate_runtime_results(&expected, &outcomes, temp.path()).unwrap();
    assert!(campaign::validate_runtime_results(&expected, &[first.clone()], temp.path()).is_err());
    assert!(campaign::validate_runtime_results(&expected, &[first.clone(), first], temp.path())
        .is_err());
    fs::write(right, b"substituted binary").unwrap();
    assert!(campaign::validate_runtime_results(&expected, &outcomes, temp.path()).is_err());
}
