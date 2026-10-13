//! Validation fixtures and executable discovery regressions, without recursive suite execution.
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::TempDir;
use wi_contract_check::{
    contracts, files, hash,
    proof::{self, Gate, Registry, TestIndex, TestRef},
    read, refresh,
};
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}
fn fixture() -> TempDir {
    let t = TempDir::new().unwrap();
    for directory in
        ["contracts", "conformance", "docs/adr", "docs/work-items", "docs/release", ".github"]
    {
        for path in files(&root().join(directory)).unwrap() {
            let dest = t.path().join(path.strip_prefix(root()).unwrap());
            fs::create_dir_all(dest.parent().unwrap()).unwrap();
            fs::copy(path, dest).unwrap();
        }
    }
    fs::copy(root().join("CONTRIBUTING.md"), t.path().join("CONTRIBUTING.md")).unwrap();
    t
}
fn records() -> (Registry, Vec<Gate>, TestIndex) {
    let (r, g) = proof::load(&root()).unwrap();
    let index = r.cases.iter().flat_map(|c| c.tests.iter().cloned().map(|t| (t, 1))).collect();
    (r, g, index)
}
fn rejects(rule: &str, edit: impl FnOnce(&mut Registry, &mut Vec<Gate>, &mut TestIndex)) {
    let (mut r, mut g, mut index) = records();
    edit(&mut r, &mut g, &mut index);
    let f = proof::validate(&root(), &r, &g, &index).unwrap();
    assert!(f.iter().any(|f| f.rule == rule), "expected {rule}: {f:?}");
}
#[test]
fn baseline_records_have_precise_g1_coverage_and_no_source_result_claims() {
    let (r, g, index) = records();
    assert!(proof::validate(&root(), &r, &g, &index).unwrap().is_empty());
    assert_eq!(r.cases.len(), 55);
    assert_eq!(r.cases.iter().filter(|c| c.status == "implemented").count(), 11);
    assert_eq!(g.iter().filter(|g| g.status == "closed").count(), 2);
    assert_eq!(r.cases.iter().find(|c| c.id == "WP-002").unwrap().tests.len(), 3);
    assert_eq!(r.cases.iter().find(|c| c.id == "WP-007").unwrap().tests.len(), 3);
}
#[test]
fn missing_ignored_misplaced_and_ambiguous_tests_reject() {
    rejects("PROOF-TEST", |r, _, i| {
        i.remove(&r.cases[0].tests[0]);
    });
    rejects("PROOF-TEST", |r, _, _| r.cases[0].tests[0].package = "wrong-package".into());
    rejects("PROOF-TEST", |r, _, _| r.cases[0].tests[0].manifest = "Cargo.toml".into());
    rejects("PROOF-TEST", |r, _, _| r.cases[0].tests[0].test = "missing".into());
    rejects("PROOF-TEST", |r, _, i| {
        i.insert(r.cases[0].tests[0].clone(), 2);
    });
    rejects("PROOF-TEST", |r, _, _| {
        r.cases[0].tests.clear();
    });
}
#[test]
fn unknown_duplicate_omitted_case_digest_and_reallocation_reject() {
    rejects("PROOF-CASE", |r, _, _| r.cases[0].id = "WP-035".into());
    rejects("PROOF-CASE", |r, _, _| r.cases.push(r.cases[0].clone()));
    rejects("PROOF-CASE", |r, _, _| {
        r.cases.pop();
    });
    rejects("PROOF-DIGEST", |r, _, _| r.cases[0].source_record_sha256 = "0".repeat(64));
    rejects("PROOF-ALLOCATION", |r, _, _| {
        r.cases.iter_mut().find(|c| c.id == "AB-WI-019").unwrap().gate = "WI-AQ-G1".into()
    });
    rejects("PROOF-ALLOCATION", |r, _, _| {
        r.cases.iter_mut().find(|c| c.id == "AB-WI-021").unwrap().gate = "WI-AQ-G3".into()
    });
}
#[test]
fn coverage_omissions_and_scope_disagreements_reject() {
    rejects("PROOF-COVERAGE", |r, _, _| r.cases[0].coverage_refs.clear());
    for id in ["WP-002", "WP-007", "AB-WI-016"] {
        rejects("PROOF-COVERAGE", |r, _, _| {
            r.cases.iter_mut().find(|c| c.id == id).unwrap().tests.pop();
        });
    }
    let t = fixture();
    let (r, g, i) = records();
    let path = t.path().join("conformance/wi-fabric-2/worker/coverage.json");
    let mut v = read(&path).unwrap();
    v["cases"][0]["test"] = json!("different");
    fs::write(path, serde_json::to_vec(&v).unwrap()).unwrap();
    assert!(proof::validate(t.path(), &r, &g, &i)
        .unwrap()
        .iter()
        .any(|f| f.rule == "PROOF-COVERAGE"));
}
#[test]
fn gate_source_prerequisites_required_cases_bindings_and_cycles_reject() {
    rejects("PROOF-GATE-SOURCE", |_, g, _| {
        g[0].requires.pop();
    });
    rejects("PROOF-GATE-SOURCE", |_, g, _| g[0].pass_criteria = json!(["weaker"]));
    rejects("PROOF-GATE-SOURCE", |_, g, _| g[0].source_record_sha256 = "0".repeat(64));
    rejects("PROOF-GATE-CASES", |_, g, _| g[0].required_cases.clear());
    rejects("PROOF-BINDING", |_, g, _| g[0].requirement_bindings.clear());
    rejects("PROOF-GATE", |_, g, _| {
        g.pop();
    });
    rejects("PROOF-CYCLE", |_, g, _| {
        let id = g[0].id.clone();
        g[0].requires.push(json!({"id":id,"kind":"checkpoint","state":"passed"}));
    });
}
#[test]
fn closed_gate_with_open_case_or_prerequisite_rejects() {
    rejects("PROOF-CLOSED-CASE", |r, _, _| {
        r.cases[0].status = "planned".into();
        r.cases[0].tests.clear();
    });
    rejects("PROOF-CLOSED-PREREQUISITE", |_, g, _| {
        g.iter_mut().find(|g| g.id == "WI-WORKER-G3").unwrap().status = "closed".into()
    });
}
#[test]
fn authoritative_export_and_matrix_byte_tampering_rejects() {
    for (path, rule) in [
        (proof::EXPORT, "PROOF-EXPORT"),
        ("contracts/sources/wi-fabric-2-worker-execution-acceptance-matrix.yaml", "PROOF-SOURCE"),
    ] {
        let t = fixture();
        let p = t.path().join(path);
        let mut bytes = fs::read(&p).unwrap();
        bytes.push(b'\n');
        fs::write(p, bytes).unwrap();
        let (r, g, i) = records();
        assert!(proof::validate(t.path(), &r, &g, &i).unwrap().iter().any(|f| f.rule == rule));
    }
}
#[test]
fn imported_adr_approvals_and_queue_pins_cannot_be_tampered() {
    for id in ["WI-ADR-011", "WI-ADR-014", "WI-ADR-015", "WI-ADR-018"] {
        let t = fixture();
        let path = t.path().join(format!("docs/adr/approvals/{id}.json"));
        let bytes = fs::read(&path).unwrap();
        let mut v: Value = serde_json::from_slice(&bytes).unwrap();
        v["approval"]["rationale"] = json!("substituted rationale");
        fs::write(&path, serde_json::to_vec(&v).unwrap()).unwrap();
        assert!(!contracts::adr_coverage(t.path(), id, "", "merge", true).unwrap());
        assert!(contracts::docs(t.path())
            .unwrap()
            .iter()
            .any(|f| f.rule == "ADR-APPROVAL" && f.item == id));
        fs::write(&path, bytes).unwrap();
        let p = t.path().join("docs/adr/queue.json");
        let mut q = read(&p).unwrap();
        let r = q.as_array_mut().unwrap().iter_mut().find(|r| r["id"] == id).unwrap();
        r["approval_sha256"] = json!("0".repeat(64));
        r["decision_packet_digest"] = json!("0".repeat(64));
        fs::write(p, serde_json::to_vec(&q).unwrap()).unwrap();
        assert!(contracts::docs(t.path())
            .unwrap()
            .iter()
            .any(|f| f.rule == "ADR-APPROVAL" && f.item == id));
    }
}
#[test]
fn compiled_discovery_excludes_ignored_unreachable_disabled_and_unselected_tests() {
    let t = TempDir::new().unwrap();
    let write = |p: &str, s: &str| {
        let p = t.path().join(p);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, s).unwrap();
    };
    write(
        "Cargo.toml",
        r#"[workspace]
members=[".","excluded"]
default-members=["."]
[package]
name="proof-discovery-fixture"
version="0.1.0"
edition="2021"
[features]
hidden=[]
[[test]]
name="not_run"
path="not_run.rs"
test=false
"#,
    );
    write(
        "src/lib.rs",
        r#"#[test] fn eligible() {}
#[test] #[ignore] fn ignored() {}
#[test] #[cfg_attr(all(),ignore)] fn conditional_ignore() {}
#[test] #[cfg(feature="hidden")] fn feature_disabled() {}
#[cfg(test)] mod nested;
#[test] fn ambiguous() {}
"#,
    );
    write("src/nested.rs", "#[test] fn reachable() {}\n");
    write("src/orphan.rs", "#[test] fn unreachable() {}\n");
    write("tests/second.rs", "#[test] fn ambiguous() {}\n");
    write("not_run.rs", "#[test] fn target_disabled() {}\n");
    write(
        "excluded/Cargo.toml",
        "[package]\nname='excluded-fixture'\nversion='0.1.0'\nedition='2021'\n",
    );
    write("excluded/src/lib.rs", "#[test] fn excluded() {}\n");
    let out = Command::new("cargo")
        .current_dir(t.path())
        .args(["generate-lockfile", "--offline"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let index = proof::discover_configuration(t.path(), "Cargo.toml", &[]).unwrap();
    let test = |name: &str| TestRef {
        manifest: "Cargo.toml".into(),
        package: "proof-discovery-fixture".into(),
        test: name.into(),
    };
    assert_eq!(index.get(&test("eligible")), Some(&1));
    assert_eq!(index.get(&test("nested::reachable")), Some(&1));
    assert_eq!(index.get(&test("ambiguous")), Some(&2));
    for name in
        ["ignored", "conditional_ignore", "feature_disabled", "unreachable", "target_disabled"]
    {
        assert!(!index.contains_key(&test(name)), "{name}");
    }
    assert!(!index.keys().any(|t| t.package == "excluded-fixture"));
    let all =
        proof::discover_configuration(t.path(), "Cargo.toml", &["--workspace", "--all-features"])
            .unwrap();
    assert!(all.contains_key(&test("feature_disabled")));
    assert!(all.keys().any(|t| t.package == "excluded-fixture"));
}
#[test]
fn hash_refresh_is_deterministic_and_preserves_every_semantic_field() {
    let first = refresh::prepare(&root()).unwrap();
    let second = refresh::prepare(&root()).unwrap();
    assert_eq!(first, second);
    for (path, value) in first {
        assert!(refresh::hash_only_change(&read(&root().join(path)).unwrap(), &value));
    }
    let before = json!({"families":[{"owner":"WI-10","destination":"specialist","surfaces":[{"path":"README.md","source_sha256":"old","treatment":"remove"}],"items":[{"id":"a","source_sha256":"old","registration_sites":["x"]}]}]});
    let mut after = before.clone();
    after["families"][0]["surfaces"][0]["source_sha256"] = json!(hash("new"));
    assert!(refresh::hash_only_change(&before, &after));
    for field in ["owner", "destination", "treatment", "registration_sites", "path", "id"] {
        let mut after = before.clone();
        let family = &mut after["families"][0];
        match field {
            "owner" | "destination" => family[field] = json!("changed"),
            "treatment" | "path" => family["surfaces"][0][field] = json!("changed"),
            _ => family["items"][0][field] = json!("changed"),
        };
        assert!(!refresh::hash_only_change(&before, &after), "{field}");
    }
}

#[test]
fn scope_digest_and_closed_gate_adr_status_disagreement_reject() {
    let t = fixture();
    let (r, g, i) = records();
    let p = t.path().join("conformance/wi-fabric-2/integration/fixtures.json");
    let mut v = read(&p).unwrap();
    v["cases"]["AB-WI-001"]["source_record_sha256"] = json!("0".repeat(64));
    fs::write(p, serde_json::to_vec(&v).unwrap()).unwrap();
    assert!(proof::validate(t.path(), &r, &g, &i)
        .unwrap()
        .iter()
        .any(|f| f.rule == "PROOF-COVERAGE"));
    let p = t.path().join("docs/adr/queue.json");
    let mut q = read(&p).unwrap();
    q[15]["status"] = json!("queued");
    fs::write(p, serde_json::to_vec(&q).unwrap()).unwrap();
    assert!(proof::validate(t.path(), &r, &g, &i).unwrap().iter().any(|f| f.rule == "PROOF-ADR"));
}
#[test]
fn hash_refresh_round_trip_changes_only_hash_bytes_and_rejects_structure() {
    let original = r#"[ { "id": "a", "source_sha256": "0000000000000000000000000000000000000000000000000000000000000000", "treatment": "retain" } ]
"#;
    let mut v: Value = serde_json::from_str(original).unwrap();
    v[0]["source_sha256"] = json!("1".repeat(64));
    let once = refresh::render_hashes(original, &v).unwrap();
    let twice = refresh::render_hashes(&once, &v).unwrap();
    assert_eq!(once, twice);
    assert_eq!(once, original.replace(&"0".repeat(64), &"1".repeat(64)));
    v[0]["treatment"] = json!("delete");
    assert!(refresh::render_hashes(original, &v).is_err());
    v[0].as_object_mut().unwrap().remove("source_sha256");
    assert!(!refresh::hash_only_change(&serde_json::from_str(original).unwrap(), &v));
}
