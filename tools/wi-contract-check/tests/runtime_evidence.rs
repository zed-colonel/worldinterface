//! Synthetic validator fixtures, never integration evidence.
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
};
use wi_contract_check::{evidence, hash, read};
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn put(base: &Path, name: &str, value: &Value) -> Value {
    let bytes = serde_json::to_vec(value).unwrap();
    fs::write(base.join(name), &bytes).unwrap();
    json!({"path":name,"sha256":hash(bytes)})
}
fn fixture(base: &Path) -> Value {
    let m = read(&root().join("conformance/wi-fabric-2/manifest.yaml")).unwrap();
    let case = m["cases"].as_array().unwrap().iter().find(|c| c["id"] == "AB-WI-001").unwrap();
    let packages: Vec<_> = ["core", "storage", "runtime", "engine", "workflow", "executor-local"]
        .into_iter()
        .map(|p| json!({"name":format!("actionqueue-{p}"),"version":"0.2.0","features":[]}))
        .collect();
    let log = put(base, "log.json", &json!({"synthetic":"command completed"}));
    let command = json!({"argv":["synthetic-validator-fixture"],"exit_code":0,"log":log});
    let mut receipt = json!({"schema_version":1,"kind":"runtime-conformance","phase":"candidate","case_id":"AB-WI-001","status":"passed","producer":"synthetic-implementer","consumer_revision":"b".repeat(40),"upstream":m["upstream_target"],"source_record_sha256":case["source_record_sha256"],"fixture_manifest_sha256":m["fixture_manifest_sha256"],"integration_fixture_sha256":m["integration_fixture_sha256"],"executable_case_sha256":hash(fs::read(root().join(case["test_source"].as_str().unwrap())).unwrap()),"resolved_packages_sha256":hash(serde_json::to_vec(&packages).unwrap()),"toolchain":"synthetic fixture","environment_sha256":"d".repeat(64),"test_run_id":"synthetic-1","command":command});
    let build = json!({"upstream":receipt["upstream"],"consumer_revision":receipt["consumer_revision"],"status":"passed","resolved_packages_sha256":receipt["resolved_packages_sha256"],"packages":packages,"command":command});
    receipt["upstream_build_receipt"] = put(base, "build.json", &build);
    let result = json!({"case_id":receipt["case_id"],"consumer_revision":receipt["consumer_revision"],"source_record_sha256":receipt["source_record_sha256"],"test_run_id":receipt["test_run_id"],"tests_passed":1,"status":"passed"});
    receipt["results"] = json!([put(base, "result.json", &result)]);
    let mut review = receipt.clone();
    review["reviewer"] = "synthetic-independent-reviewer".into();
    receipt["independent_review"] = json!({"reviewer":"synthetic-independent-reviewer","receipt":put(base,"review.json",&review)});
    receipt
}
#[test]
fn imported_source_binding_and_review_artifacts_are_required() {
    let t = tempfile::tempdir().unwrap();
    let r = fixture(t.path());
    evidence::runtime(&root(), &r, t.path()).unwrap();
    for key in [
        "source_record_sha256",
        "executable_case_sha256",
        "integration_fixture_sha256",
        "resolved_packages_sha256",
        "consumer_revision",
        "test_run_id",
    ] {
        let mut bad = r.clone();
        bad[key] =
            if key == "consumer_revision" { json!("c".repeat(40)) } else { json!("c".repeat(64)) };
        assert!(evidence::runtime(&root(), &bad, t.path()).is_err(), "{key}");
    }
    let mut bad = r.clone();
    bad["upstream"]["implementation_commit"] = "97c9dc26c19c697dbfb204ed503e82c5f053394f".into();
    assert!(evidence::runtime(&root(), &bad, t.path()).is_err());
    fs::write(t.path().join("log.json"), b"altered").unwrap();
    assert!(evidence::runtime(&root(), &r, t.path()).is_err());
}
#[test]
fn empty_build_results_and_self_review_cannot_pass() {
    for change in ["empty-build", "empty-results", "self-review", "wrong-review", "old-package"] {
        let t = tempfile::tempdir().unwrap();
        let mut r = fixture(t.path());
        match change {
            "empty-build" | "old-package" => {
                let mut b = read(&t.path().join("build.json")).unwrap();
                if change == "empty-build" {
                    b["packages"] = json!([]);
                } else {
                    b["packages"][0]["version"] = "0.1.0".into();
                }
                r["upstream_build_receipt"] = put(t.path(), "build.json", &b);
            }
            "empty-results" => {
                let mut result = read(&t.path().join("result.json")).unwrap();
                result["tests_passed"] = 0.into();
                r["results"] = json!([put(t.path(), "result.json", &result)]);
            }
            "self-review" => r["producer"] = r["independent_review"]["reviewer"].clone(),
            _ => {
                let mut review = read(&t.path().join("review.json")).unwrap();
                review["command"]["argv"] = json!(["wrong-command"]);
                r["independent_review"]["receipt"] = put(t.path(), "review.json", &review);
            }
        }
        assert!(evidence::runtime(&root(), &r, t.path()).is_err(), "{change}");
    }
}
