//! Synthetic mutation tests of the development checker. None execute WI/AQ conformance.
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};
use tempfile::TempDir;
use wi_contract_check::{contracts, evidence, files, hash, inventory, policy, read};
fn milestones(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}
fn write(root: &Path, p: &str, s: &str) {
    let p = root.join(p);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, s).unwrap()
}
fn data(root: &Path, p: &str, v: &Value) {
    write(root, p, &serde_json::to_string_pretty(v).unwrap())
}
fn copy_tree(src: &Path, dst: &Path) {
    for p in files(src).unwrap() {
        let rel = p.strip_prefix(src).unwrap();
        let to = dst.join(rel);
        fs::create_dir_all(to.parent().unwrap()).unwrap();
        fs::copy(p, to).unwrap();
    }
}
fn contract_repo() -> TempDir {
    let t = TempDir::new().unwrap();
    for d in [
        "contracts",
        "crates/worldinterface-runtime",
        "conformance",
        "fixtures",
        "docs/adr",
        "docs/work-items",
        "docs/release",
        ".github",
    ] {
        copy_tree(&root().join(d), &t.path().join(d))
    }
    fs::copy(root().join("CONTRIBUTING.md"), t.path().join("CONTRIBUTING.md")).unwrap();
    t
}
fn tiny_repo() -> TempDir {
    let t = TempDir::new().unwrap();
    write(t.path(), "Cargo.toml", "[workspace]\nmembers=['crates/a']\n");
    write(t.path(), "crates/a/Cargo.toml", "[package]\nname='a'\nversion='0.1.0'\n");
    write(t.path(), "crates/a/src/lib.rs", "pub struct Safe;");
    write(t.path(), "fixtures/old.rs", "pub struct Old;");
    t
}
fn pol() -> Value {
    read(&root().join("contracts/forbidden-symbols.yaml")).unwrap()
}
#[test]
fn inventory_parses_ids_nested_conditional_and_private_types() {
    let source = r#"define_id!(EffectId, "doc"); #[cfg(feature="x")] mod outer { struct Private; mod nested { enum E { X } } } impl Connector for Unregistered {}"#;
    let (items, _) = inventory::scan_source("crates/a/src/lib.rs", source).unwrap();
    assert!(items.iter().any(|x| x.kind == "generated-id" && x.name == "EffectId"));
    assert!(items.iter().any(|x| x.name == "Private" && !x.conditions.is_empty()));
    assert!(items.iter().any(|x| x.name == "E" && x.id.contains("outer::nested")));
    assert!(items.iter().any(|x| x.kind == "connector" && x.registration_sites.is_empty()));
}
#[test]
fn inventory_rejects_unknown_declaration_macros() {
    assert!(inventory::scan_source("x.rs", "make_types!();").is_err());
    assert!(inventory::scan_source("x.rs", "define_id!(123, true);").is_err());
}
#[test]
fn inventory_is_complete_and_deterministic() {
    let a = inventory::generate(&root()).unwrap();
    let b = inventory::generate(&root()).unwrap();
    assert_eq!(a, b);
    for (k, n) in [
        ("workspace-crate", 10),
        ("auxiliary-crate", 11),
        ("foundation-crate", 7),
        ("connector", 28), // WIP-10 adds the embedded WorkerConnector.
        ("wasm-manifest", 15),
        ("generated-id", 4),
    ] {
        assert_eq!(a.iter().filter(|x| x.kind == k).count(), n, "{k}")
    }
    assert_eq!(
        a.iter()
            .filter(|x| x.kind == "registration" && x.id.contains("::default_registry::"))
            .count(),
        21
    );
    assert_eq!(
        a.iter()
            .filter(|x| x.kind == "connector"
                && x.path == "crates/worldinterface-runtime/src/worker/connector.rs"
                && x.name == "WorkerConnector < S >")
            .count(),
        1
    );
    assert!(a.iter().any(|x| x.name == "code_fuzzy"));
    assert!(a.iter().any(|x| x.path.ends_with("semantic/rust/analyzer.rs")));
    assert!(inventory::check(&root()).unwrap().is_empty());
}
#[test]
fn dispositions_reject_missing_duplicate_stale_and_incomplete() {
    let (items, _) = inventory::scan_source("x.rs", "pub struct X;").unwrap();
    let ds:Vec<_>=items.iter().map(|i|json!({"id":i.id,"disposition":"rebuild","target":"protocol","source":"WI-02","rationale":"new protocol"})).collect();
    assert!(inventory::check_dispositions(&items, &json!(ds)).is_empty());
    let mut missing = ds.clone();
    missing.pop();
    assert!(!inventory::check_dispositions(&items, &json!(missing)).is_empty());
    let mut dup = ds.clone();
    dup.push(ds[0].clone());
    assert!(!inventory::check_dispositions(&items, &json!(dup)).is_empty());
    let mut stale = ds.clone();
    stale[0]["id"] = json!("stale");
    assert!(!inventory::check_dispositions(&items, &json!(stale)).is_empty());
    let mut bad = ds.clone();
    bad[0]["rationale"] = json!("");
    assert!(!inventory::check_dispositions(&items, &json!(bad)).is_empty());
}
#[test]
fn all_deletion_rules_activate_only_at_approved_milestones() {
    let p = pol();
    let cases = [
        ("CB-RECEIPT-MODEL", "crates/worldinterface-core/src/receipt.rs", "struct Receipt;", 5),
        ("CB-RECEIPT-WRITER", "crates/a/src/lib.rs", "mod receipt_gen;", 6),
        ("CB-CONNECTOR-SPI", "crates/a/src/lib.rs", "struct InvocationContext;", 6),
        ("CB-FLOW", "crates/a/src/lib.rs", "struct FlowSpec;", 7),
        ("CB-SIGNAL", "crates/a/src/lib.rs", "struct SignalRegistry;", 8),
        (
            "CB-DELAY",
            "crates/worldinterface-connector/src/connectors/delay.rs",
            "struct DelayConnector;",
            8,
        ),
        ("CB-CODING", "crates/a/src/lib.rs", "mod code_test;", 10),
        (
            "CB-CODING-HELPERS",
            "crates/worldinterface-connector/src/connectors/code_common.rs",
            "fn helper() {}",
            10,
        ),
        ("CB-COMPAT-PRINCIPAL", "crates/a/src/lib.rs", "struct AnonymousPrincipal;", 4),
        ("CB-FIRE-FORGET", "crates/a/src/lib.rs", "struct TaskSubmissionPort;", 7),
        (
            "CB-CODING-SEMANTIC",
            "crates/worldinterface-connector/src/semantic/types.rs",
            "struct Symbol;",
            10,
        ),
    ];
    assert_eq!(cases.len(), p["rules"].as_array().unwrap().len());
    for (id, path, src, at) in cases {
        for milestone in 1..=14 {
            let f = policy::symbol_findings(path, src, &p, &milestones(&[milestone])).unwrap();
            assert_eq!(f.iter().any(|x| x.rule == id), milestone == at, "{id} at {milestone}")
        }
    }
}
#[test]
fn milestones_require_controller_digest_unique_known_ids_and_wi01_entry() {
    let v = json!({"version":1,"reached":["WI-01","WI-02"]});
    let bytes = serde_json::to_vec(&v).unwrap();
    assert!(policy::validate_state(&v, None, &bytes, true).is_err());
    assert!(policy::validate_state(&v, Some("wrong"), &bytes, true).is_err());
    assert!(policy::validate_state(&v, None, &bytes, false).is_err());
    assert_eq!(
        policy::validate_state(&v, Some(&hash(&bytes)), &bytes, true).unwrap(),
        milestones(&[1, 2])
    );
    for reached in
        [json!([]), json!(["WI-02"]), json!(["WI-01", "WI-01"]), json!(["WI-01", "WI-99"])]
    {
        let v = json!({"version":1,"reached":reached});
        let b = serde_json::to_vec(&v).unwrap();
        assert!(policy::validate_state(&v, Some(&hash(&b)), &b, true).is_err());
    }
}
#[test]
fn domain_semantics_rejected_opaque_strings_permitted() {
    assert!(!policy::symbol_findings(
        "crates/a/src/lib.rs",
        "struct Vessel;",
        &pol(),
        &milestones(&[1])
    )
    .unwrap()
    .is_empty());
    assert!(policy::symbol_findings(
        "crates/a/src/lib.rs",
        r#"struct OpaqueRef(String); const EXAMPLE: &str="ref://exo/Vessel/opaque";"#,
        &pol(),
        &milestones(&[1])
    )
    .unwrap()
    .is_empty());
    assert!(!policy::symbol_findings(
        "crates/a/src/protocol/mod.rs",
        "use crate::runtime::Effect;",
        &pol(),
        &milestones(&[1])
    )
    .unwrap()
    .is_empty());
}
#[test]
fn only_exact_test_cfg_is_excluded() {
    let t=policy::production_tokens("// SignalRegistry\n#[cfg(test)] mod tests {struct SignalRegistry;} #[cfg(feature=\"legacy\")] struct DelayConnector;").unwrap();
    assert!(!t.contains("SignalRegistry"));
    assert!(t.contains("DelayConnector"));
}
#[test]
fn includes_and_module_paths_cannot_reach_archives() {
    let t = tiny_repo();
    let root = t.path().canonicalize().unwrap();
    let p = root.join("crates/a/src/lib.rs");
    for s in [
        r#"include!("../../../fixtures/old.rs");"#,
        r#"const S:&str=include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/old.rs"));"#,
        r#"const B:&[u8]=include_bytes!("../../../fixtures/old.rs");"#,
        r#"#[path="../../../fixtures/old.rs"] mod old;"#,
        r#"#[cfg_attr(feature="old",path="../../../fixtures/old.rs")] mod old;"#,
        r#"include!(concat!(env!("OUT_DIR"),"/old.rs"));"#,
        r#"macro_rules! old {()=>{include!("../../../fixtures/old.rs");}}"#,
    ] {
        assert!(!policy::source_routes(&root, &p, s).unwrap().is_empty(), "{s}");
    }
    write(&root, "crates/a/src/safe.rs", "struct Safe;");
    assert!(policy::source_routes(&root, &p, r#"include!("safe.rs");"#).unwrap().is_empty());
}
#[test]
fn cargo_archive_targets_paths_and_build_scripts_rejected() {
    for suffix in [
        "[dependencies]\nold={path='../../fixtures'}\n",
        "[lib]\npath='../../fixtures/old.rs'\n",
        "",
    ] {
        let t = tiny_repo();
        write(t.path(), "fixtures/Cargo.toml", "[package]\nname='old'\nversion='0.1.0'\n");
        write(
            t.path(),
            "crates/a/Cargo.toml",
            &format!("[package]\nname='a'\nversion='0.1.0'\n{suffix}"),
        );
        if suffix.is_empty() {
            write(t.path(), "crates/a/build.rs", "fn main() {}")
        }
        assert!(!policy::dependencies(t.path(), &pol()).unwrap().is_empty());
    }
}
#[test]
fn renamed_and_transitive_dependencies_do_not_hide_domain_leakage() {
    let t = tiny_repo();
    write(t.path(),"crates/a/Cargo.toml","[package]\nname='worldinterface-protocol'\nversion='0.1.0'\n[dependencies]\nhelper={path='../helper'}\n");
    write(t.path(),"crates/helper/Cargo.toml","[package]\nname='helper'\nversion='0.1.0'\n[dependencies]\nalias={package='exoskeleton',version='=1.0.0'}\n");
    assert!(policy::dependencies(t.path(), &pol()).unwrap().iter().any(|x| x.rule == "DEP-DOMAIN"));
    write(
        t.path(),
        "crates/helper/Cargo.toml",
        "[package]\nname='worldinterface-runtime'\nversion='0.1.0'\n",
    );
    assert!(policy::dependencies(t.path(), &pol())
        .unwrap()
        .iter()
        .any(|x| x.item == "worldinterface-runtime"));
}
#[test]
fn optional_identity_adapter_allowed_but_default_enabled_is_mandatory() {
    let t = tiny_repo();
    let base="[package]\nname='identity-adapter'\nversion='0.1.0'\n[dependencies]\nidentity={package='entityid',version='=1.0.0',optional=true}\n";
    write(t.path(), "crates/a/Cargo.toml", base);
    assert!(policy::dependencies(t.path(), &pol()).unwrap().is_empty());
    write(
        t.path(),
        "crates/a/Cargo.toml",
        &format!(
            "{base}[features]\ndefault=['identity-profile']\nidentity-profile=['dep:identity']\n"
        ),
    );
    assert!(policy::dependencies(t.path(), &pol()).unwrap().iter().any(|x| x.rule == "DEP-DOMAIN"));
}
#[test]
fn archive_workspace_patches_are_rejected() {
    let t = tiny_repo();
    write(
        t.path(),
        "Cargo.toml",
        "[workspace]\nmembers=['crates/a']\n[patch.crates-io]\nold={path='fixtures'}\n",
    );
    assert!(policy::dependencies(t.path(), &pol())
        .unwrap()
        .iter()
        .any(|x| x.rule == "ARCHIVE-WORKSPACE"));
}
#[test]
fn contracts_match_all_invariants_and_preserve_sources() {
    assert!(contracts::contracts(&root()).unwrap().is_empty());
    assert_eq!(
        contracts::invariants_from_source(
            &fs::read_to_string(
                root().join("contracts/sources/worldinterface-v2-foundational-design.md")
            )
            .unwrap()
        )
        .len(),
        59
    );
}
#[test]
fn altered_invariant_body_is_rejected() {
    let t = contract_repo();
    let p = "contracts/wi-fabric-2.yaml";
    let mut v = read(&t.path().join(p)).unwrap();
    v["invariants"][58]["text"] = json!("weakened");
    data(t.path(), p, &v);
    assert!(contracts::contracts(t.path())
        .unwrap()
        .iter()
        .any(|x| x.rule == "CONTRACT-INVARIANTS"));
}
#[test]
fn stale_import_hash_is_rejected() {
    let t = contract_repo();
    write(t.path(), "contracts/aq-assumptions.yaml", "status: accepted\n");
    assert!(contracts::provenance(t.path()).unwrap().iter().any(|x| x.rule == "SOURCE-HASH"));
}
#[test]
fn fixture_tampering_and_unindexed_files_are_rejected() {
    let t = contract_repo();
    write(t.path(), "fixtures/pre-wi-fabric-2/source/Cargo.lock", "altered");
    write(t.path(), "fixtures/pre-wi-fabric-2/extra.json", "{}");
    let f = contracts::fixtures(t.path()).unwrap();
    assert!(f.iter().any(|x| x.rule == "FIXTURE-HASH"));
    assert!(f.iter().any(|x| x.rule == "FIXTURE-UNINDEXED"));
}
#[test]
fn exo_observations_cannot_inherit_frozen_execution_revision() {
    let t = contract_repo();
    let p = "fixtures/pre-wi-fabric-2/manifest.json";
    let mut m = read(&t.path().join(p)).unwrap();
    let x = m["files"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|x| x["category"] == "exo-coding")
        .unwrap();
    x["execution_revision"] = json!("1bcbb757a7885ce1f911ac948bfc2269c9209075");
    data(t.path(), p, &m);
    assert!(contracts::fixtures(t.path()).unwrap().iter().any(|x| x.rule == "FIXTURE-PROVENANCE"));
}
#[test]
fn fresh_store_skeleton_has_no_runtime_passes() {
    assert!(contracts::conformance(&root()).unwrap().is_empty());
    let t = contract_repo();
    let p = "conformance/wi-fabric-2/manifest.yaml";
    let mut m = read(&t.path().join(p)).unwrap();
    m["store_scenarios"][2]["unchanged_on_rejection"] = json!(false);
    data(t.path(), p, &m);
    assert!(contracts::conformance(t.path())
        .unwrap()
        .iter()
        .any(|x| x.rule == "CONFORMANCE-STORE"));
    m["cases"][0]["result"] = json!("passed");
    data(t.path(), p, &m);
    assert!(contracts::conformance(t.path()).is_err());
}
#[test]
fn adr_owner_and_deadline_cannot_drift() {
    let t = contract_repo();
    assert!(
        contracts::docs(t.path()).unwrap().is_empty(),
        "{:?}",
        contracts::docs(t.path()).unwrap()
    );
    let p = "docs/adr/queue.json";
    let mut q = read(&t.path().join(p)).unwrap();
    q[15]["owner"] = json!("unassigned");
    q[0]["due"] = json!("WI-14");
    data(t.path(), p, &q);
    assert_eq!(
        contracts::docs(t.path()).unwrap().iter().filter(|x| x.rule == "ADR-QUEUE").count(),
        2
    );
}
#[test]
fn mirrored_adr_approval_cannot_change_identity_or_coverage() {
    let t = contract_repo();
    let p = "docs/adr/queue.json";
    let mut q = read(&t.path().join(p)).unwrap();
    q[7]["approval_id"] = json!("invented-approval");
    q[9]["approval_id"] = json!("invented-approval");
    data(t.path(), p, &q);
    let findings = contracts::docs(t.path()).unwrap();
    assert!(findings.iter().any(|f| f.rule == "ADR-APPROVAL" && f.item == "WI-ADR-008"));
    assert!(findings.iter().any(|f| f.rule == "ADR-APPROVAL" && f.item == "WI-ADR-010"));
}
#[test]
fn wi02_drafts_require_acceptance_and_maturity() {
    let t = contract_repo();
    let p = "docs/work-items/wi-02.json";
    let mut q = read(&t.path().join(p)).unwrap();
    q[3]["maturity"] = json!("independent");
    q[0]["acceptance"] = json!([]);
    data(t.path(), p, &q);
    assert_eq!(
        contracts::docs(t.path()).unwrap().iter().filter(|x| x.rule == "WI02-DRAFT").count(),
        2
    );
}
fn runtime_receipt() -> Value {
    let m = read(&root().join("conformance/wi-fabric-2/manifest.yaml")).unwrap();
    let release = read(&root().join("contracts/aq-integration-target.json")).unwrap()
        ["runtime_release"]
        .clone();
    let artifact = json!({"path":"synthetic.json","sha256":"a".repeat(64)});
    json!({"producer":"synthetic-producer","executable_case_sha256":"e".repeat(64),"integration_fixture_sha256":m["integration_fixture_sha256"],"resolved_packages_sha256":"f".repeat(64),"schema_version":1,"kind":"runtime-conformance","phase":"candidate","case_id":"WI-SKELETON-W1","status":"passed","consumer_revision":"b".repeat(40),"upstream":m["upstream_target"],"runtime_release":release,"upstream_build_receipt":artifact,"source_record_sha256":"c".repeat(64),"fixture_manifest_sha256":m["fixture_manifest_sha256"],"toolchain":"synthetic checker fixture only","environment_sha256":"d".repeat(64),"test_run_id":"synthetic-checker-test","command":{"argv":["synthetic"],"exit_code":0,"log":artifact},"results":[artifact],"independent_review":{"reviewer":"synthetic-reviewer","receipt":artifact}})
}
#[test]
fn execution_schema_requires_each_identity_and_result() {
    let schema = read(&root().join("conformance/wi-fabric-2/evidence.schema.json")).unwrap();
    let v = runtime_receipt();
    assert!(evidence::validate(&schema, &v, "test").is_ok());
    for field in schema["required"].as_array().unwrap() {
        let mut missing = v.clone();
        missing.as_object_mut().unwrap().remove(field.as_str().unwrap());
        assert!(evidence::validate(&schema, &missing, "test").is_err(), "{field}")
    }
    let mut empty = v.clone();
    empty["results"] = json!([]);
    assert!(evidence::validate(&schema, &empty, "test").is_err());
}
#[test]
fn runtime_receipt_rejects_planned_cases_and_historical_upstream() {
    let v = runtime_receipt();
    assert!(evidence::runtime(&root(), &v, &root())
        .unwrap_err()
        .to_string()
        .contains("unimplemented"));
    let t = contract_repo();
    let p = "conformance/wi-fabric-2/manifest.yaml";
    let mut m = read(&t.path().join(p)).unwrap();
    m["cases"][0]["status"] = json!("implemented");
    data(t.path(), p, &m);
    let mut hist = v.clone();
    hist["upstream"]["implementation_commit"] = json!("97c9dc26c19c697dbfb204ed503e82c5f053394f");
    assert!(evidence::runtime(t.path(), &hist, t.path())
        .unwrap_err()
        .to_string()
        .contains("imported target"));
}
#[test]
fn publication_preparation_is_not_completion() {
    let t = TempDir::new().unwrap();
    write(t.path(), "observation.json", "{\"synthetic_checker_fixture\":true}");
    let a = json!({"path":"observation.json","sha256":hash(fs::read(t.path().join("observation.json")).unwrap())});
    let cmd = json!({"argv":["synthetic"],"exit_code":0,"log":a});
    let prep = json!({"schema_version":1,"kind":"publication-preparation","source_revision":"b".repeat(40),"baseline_revision":"c".repeat(40),"artifacts":[a],"commands":[cmd],"toolchain":"synthetic","environment_sha256":"d".repeat(64),"verification_receipts":[a],"publication_status":"not-performed","owner":"operator"});
    assert!(evidence::publication(&root(), &prep, t.path()).is_ok());
    data(t.path(), "preparation.json", &prep);
    let prep_ref = json!({"path":"preparation.json","sha256":hash(fs::read(t.path().join("preparation.json")).unwrap())});
    let mut done = json!({"schema_version":1,"kind":"publication-completion","preparation":prep_ref,"source_revision":"b".repeat(40),"operator":"synthetic","action":"synthetic","script":a,"destination":"synthetic","observed_at":"synthetic","observed_publication_state":a,"command":cmd});
    assert!(evidence::publication(&root(), &done, t.path()).is_ok());
    done["source_revision"] = json!("e".repeat(40));
    assert!(evidence::publication(&root(), &done, t.path()).is_err());
    let mut false_done = prep.clone();
    false_done["kind"] = json!("publication-completion");
    assert!(evidence::publication(&root(), &false_done, t.path()).is_err());
}
#[test]
fn schema_validator_rejects_unknown_schema_keywords() {
    assert!(evidence::validate(&json!({"unsupported":true}), &json!({}), "schema").is_err());
}

#[test]
fn locked_registry_facade_cannot_hide_transitive_domain_dependency() {
    let t = tiny_repo();
    write(
        t.path(),
        "crates/a/Cargo.toml",
        "[package]\nname='a'\nversion='0.1.0'\n[dependencies]\nfacade='=1.0.0'\n",
    );
    write(t.path(),"Cargo.lock","version=4\n[[package]]\nname='facade'\nversion='1.0.0'\ndependencies=['exoskeleton 1.0.0']\n[[package]]\nname='exoskeleton'\nversion='1.0.0'\n");
    assert!(policy::dependencies(t.path(), &pol())
        .unwrap()
        .iter()
        .any(|x| x.rule == "DEP-LOCK-REACHABILITY"));
}
#[test]
fn production_cannot_include_excluded_test_sources() {
    let t = tiny_repo();
    write(t.path(), "crates/a/tests/old.rs", "pub struct SignalRegistry;");
    let r = t.path().canonicalize().unwrap();
    assert!(!policy::source_routes(
        &r,
        &r.join("crates/a/src/lib.rs"),
        r#"#[path="../tests/old.rs"] mod old;"#
    )
    .unwrap()
    .is_empty());
}
#[test]
fn statement_macros_cannot_silently_introduce_types() {
    assert!(inventory::scan_source("x.rs", "fn f() { make_types!(); }").is_err());
}
#[test]
fn frozen_tag_and_copied_git_blobs_match() {
    assert!(contracts::baseline(&root()).unwrap().is_empty());
}

#[test]
fn legacy_connector_wire_names_activate_with_coding_catalog_gate() {
    for name in ["code.read", "code.apply_patch", "code.impls", "repo.locate", "repo.context"] {
        let src = format!("fn f() {{ invoke(\"{name}\"); }}");
        assert!(policy::symbol_findings("crates/a/src/lib.rs", &src, &pol(), &milestones(&[9]))
            .unwrap()
            .is_empty());
        assert!(policy::symbol_findings("crates/a/src/lib.rs", &src, &pol(), &milestones(&[10]))
            .unwrap()
            .iter()
            .any(|f| f.rule == "CB-CODING"));
    }
}

#[test]
fn required_parent_features_make_indirect_identity_dependency_mandatory() {
    let t = tiny_repo();
    write(t.path(),"crates/a/Cargo.toml","[package]\nname='a'\nversion='0.1.0'\n[dependencies]\na_quiet={path='../helper'}\nz_active={path='../helper',features=['portable']}\n");
    write(t.path(),"crates/helper/Cargo.toml","[package]\nname='helper'\nversion='0.1.0'\n[dependencies]\nidentity={package='entityid',version='=1.0.0',optional=true}\n[features]\nportable=['identity/sign']\n");
    assert!(policy::dependencies(t.path(), &pol()).unwrap().iter().any(|f| f.rule == "DEP-DOMAIN"));
}

#[test]
fn independent_controller_milestones_activate_only_their_own_rules() {
    // F-001: Appendix A admits WI-04 and WI-05 independently after WI-02.
    for ids in [vec![1, 2, 4], vec![5, 1, 2], vec![1, 2, 5, 4]] {
        let v = json!({"version":1,"reached":ids.iter().map(|n| format!("WI-{n:02}")).collect::<Vec<_>>()});
        let bytes = serde_json::to_vec(&v).unwrap();
        let approved = policy::validate_state(&v, Some(&hash(&bytes)), &bytes, true).unwrap();
        assert_eq!(approved, milestones(&ids));
        for (id, path, source, at) in [
            ("CB-COMPAT-PRINCIPAL", "crates/a/src/lib.rs", "struct AnonymousPrincipal;", 4),
            ("CB-RECEIPT-MODEL", "crates/worldinterface-core/src/receipt.rs", "struct Receipt;", 5),
            ("CB-RECEIPT-WRITER", "crates/a/src/lib.rs", "mod receipt_gen;", 6),
        ] {
            let findings = policy::symbol_findings(path, source, &pol(), &approved).unwrap();
            assert_eq!(findings.iter().any(|f| f.rule == id), ids.contains(&at));
        }
    }
}

#[test]
fn nested_non_rs_source_routes_cannot_bypass_policy_or_inventory() {
    // F-002: rustc accepts this route; both development checks must reject it.
    for route in [r#"#[path="extra.inc"] mod extra;"#, r#"include!("extra.inc");"#] {
        let t = tiny_repo();
        data(t.path(), "contracts/forbidden-symbols.yaml", &pol());
        write(t.path(), "crates/a/src/lib.rs", route);
        write(t.path(), "crates/a/src/extra.inc", r#"include!("../../../fixtures/old.rs");"#);
        write(t.path(), "fixtures/old.rs", "pub struct Vessel;");
        assert!(policy::check_at(t.path(), &milestones(&[1]))
            .unwrap()
            .iter()
            .any(|f| f.rule == "ARCHIVE-INCLUSION"));
        let err = inventory::generate(t.path()).unwrap_err().to_string();
        assert!(err.contains("inventory source coverage rejected"), "{err}");
    }
}

#[test]
fn cargo_targets_outside_scanned_sources_fail_inventory_and_policy() {
    for target in ["src/entry.inc", "../../unscanned.rs", "src/target/entry.rs"] {
        let t = tiny_repo();
        write(t.path(), &format!("crates/a/{target}"), "pub struct Vessel;");
        write(
            t.path(),
            "crates/a/Cargo.toml",
            &format!("[package]\nname='a'\nversion='0.1.0'\n[lib]\npath='{target}'\n"),
        );
        assert!(policy::dependencies(t.path(), &pol())
            .unwrap()
            .iter()
            .any(|f| f.rule == "ARCHIVE-TARGET"));
        assert!(inventory::generate(t.path()).is_err());
    }
}

#[test]
fn scanned_module_cycles_are_bounded_and_all_contents_are_checked() {
    let t = tiny_repo();
    data(t.path(), "contracts/forbidden-symbols.yaml", &pol());
    write(t.path(), "crates/a/src/lib.rs", r#"#[path="extra.rs"] mod extra;"#);
    write(t.path(), "crates/a/src/extra.rs", r#"#[path="lib.rs"] mod back; pub struct Vessel;"#);
    let findings = policy::check_at(t.path(), &milestones(&[1])).unwrap();
    assert!(findings.iter().any(|f| f.rule == "DOMAIN-EXO" && f.path.ends_with("extra.rs")));
    let items = inventory::generate(t.path()).unwrap();
    assert_eq!(items.iter().filter(|i| i.name == "Vessel").count(), 1);
    write(t.path(), "crates/a/src/extra.rs", r#"#[path="../../../fixtures/old.rs"] mod old;"#);
    assert!(policy::check_at(t.path(), &milestones(&[1]))
        .unwrap()
        .iter()
        .any(|f| f.rule == "ARCHIVE-INCLUSION" && f.path.ends_with("extra.rs")));
    assert!(inventory::generate(t.path()).is_err());
}

#[test]
fn contextual_module_paths_fail_closed_and_resource_includes_remain_supported() {
    let t = tiny_repo();
    let r = t.path().canonicalize().unwrap();
    let p = r.join("crates/a/src/lib.rs");
    write(&r, "crates/a/src/safe.rs", "pub struct Safe;");
    for source in
        [r#"mod nested { #[path="safe.rs"] mod another; }"#, r#"#[path="safe.rs"] mod nested {}"#]
    {
        assert!(!policy::source_routes(&r, &p, source).unwrap().is_empty());
    }
    write(&r, "crates/a/src/resource.txt", "data");
    assert!(policy::source_routes(&r, &p,
        r#"const S:&str=include_str!("resource.txt"); const B:&[u8]=include_bytes!("resource.txt");"#
    ).unwrap().is_empty());
}

#[test]
fn optional_exo_dependencies_are_unconditionally_rejected_outside_foundation() {
    // F-003: features, aliases and target/workspace inheritance cannot invert WI -> EXO.
    for declaration in [
        "[dependencies]\nexo={package='exoskeleton',version='=1.0.0',optional=true}\n[features]\nextra=['dep:exo']\n",
        "[target.'cfg(unix)'.dependencies]\nexo={package='exoskeleton',version='=1.0.0',optional=true}\n",
        "[dependencies]\nexo={workspace=true,optional=true}\n",
    ] {
        let t = tiny_repo();
        write(t.path(), "Cargo.toml", "[workspace]\nmembers=['crates/a']\n[workspace.dependencies]\nexo={package='exoskeleton',version='=1.0.0'}\n");
        write(t.path(), "crates/a/Cargo.toml", &format!(
            "[package]\nname='a'\nversion='0.1.0'\n{declaration}"));
        assert!(policy::dependencies(t.path(), &pol()).unwrap().iter()
            .any(|f| f.rule == "DEP-DOMAIN" && f.item == "exoskeleton"));
    }
}

#[test]
fn optional_local_and_locked_transitive_exo_routes_are_rejected() {
    let t = tiny_repo();
    write(t.path(), "crates/a/Cargo.toml", "[package]\nname='a'\nversion='0.1.0'\n[dependencies]\nhelper={path='../helper',optional=true}\n");
    write(t.path(), "crates/helper/Cargo.toml", "[package]\nname='helper'\nversion='0.1.0'\n[dependencies]\nexo={path='../renamed',optional=true}\n");
    write(
        t.path(),
        "crates/renamed/Cargo.toml",
        "[package]\nname='exoskeleton'\nversion='1.0.0'\n",
    );
    assert!(policy::dependencies(t.path(), &pol())
        .unwrap()
        .iter()
        .any(|f| f.rule == "DEP-DOMAIN" && f.item == "exoskeleton"));
    write(t.path(), "crates/helper/Cargo.toml", "[package]\nname='helper'\nversion='0.1.0'\n[dependencies]\nfacade={version='=1.0.0',optional=true}\n");
    for (leaf, rejected) in [("exoskeleton", true), ("entityid", false)] {
        write(t.path(), "Cargo.lock", &format!("version=4\n[[package]]\nname='facade'\nversion='1.0.0'\ndependencies=['{leaf} 1.0.0']\n[[package]]\nname='{leaf}'\nversion='1.0.0'\n"));
        assert_eq!(
            policy::dependencies(t.path(), &pol())
                .unwrap()
                .iter()
                .any(|f| f.rule == "DEP-LOCK-REACHABILITY" && f.item == leaf),
            rejected
        );
    }
}

#[test]
fn adr_clause_approval_cannot_be_promoted_or_rewritten() {
    let t = contract_repo();
    let mut q = read(&t.path().join("docs/adr/queue.json")).unwrap();
    q[15]["status"] = json!("queued");
    q[15]["coverage"] = json!("clauses");
    data(t.path(), "docs/adr/queue.json", &q);
    let findings = contracts::docs(t.path()).unwrap();
    assert!(findings.iter().any(|f| f.rule == "ADR-QUEUE"));
    assert!(findings.iter().any(|f| f.rule == "ADR-APPROVAL"));
    let p = "docs/adr/approvals/WI-ADR-016.json";
    let mut a = read(&t.path().join(p)).unwrap();
    a["decision"]["retainedObligations"] = json!("");
    data(t.path(), p, &a);
    assert!(contracts::docs(t.path()).unwrap().iter().any(|f| f.rule == "ADR-APPROVAL"));
}
#[test]
fn isolated_workspace_uses_its_own_transitive_lock() {
    let t = tiny_repo();
    write(t.path(), "crates/foundation/Cargo.toml", "[workspace]\nmembers=['protocol']\n");
    write(
        t.path(),
        "crates/foundation/protocol/Cargo.toml",
        "[package]\nname='worldinterface-protocol'\nversion='0.2.0'\n[dependencies]\nfacade='1'\n",
    );
    write(t.path(), "crates/foundation/protocol/src/lib.rs", "pub struct Safe;");
    write(t.path(), "crates/foundation/Cargo.lock", "version=4\n[[package]]\nname='facade'\nversion='1.0.0'\ndependencies=['exoskeleton 1.0.0']\n[[package]]\nname='exoskeleton'\nversion='1.0.0'\n");
    assert!(policy::dependencies(t.path(), &pol())
        .unwrap()
        .iter()
        .any(|f| f.rule == "DEP-LOCK-REACHABILITY"));
}

#[test]
fn path_dependency_resolves_its_own_workspace_inheritance() {
    let t = tiny_repo();
    write(
        t.path(),
        "Cargo.toml",
        "[workspace]\nmembers=['crates/a']\n[workspace.dependencies]\nserde='1'\n",
    );
    write(
        t.path(),
        "crates/a/Cargo.toml",
        "[package]\nname='a'\nversion='1.0.0'\n[dependencies]\nserde.workspace=true\n",
    );
    write(t.path(), "examples/separate/Cargo.toml", "[workspace]\n[package]\nname='separate'\nversion='1.0.0'\n[dependencies]\na={path='../../crates/a'}\n");
    write(t.path(), "examples/separate/src/lib.rs", "pub struct Safe;");
    assert!(policy::dependencies(t.path(), &pol()).unwrap().is_empty());
}

#[test]
fn ingress_approvals_bind_full_coverage_ids_revision_and_exact_proposal() {
    assert!(
        contracts::docs(&root()).unwrap().is_empty(),
        "{:?}",
        contracts::docs(&root()).unwrap()
    );
    for id in ["WI-ADR-012", "WI-ADR-013"] {
        assert!(contracts::adr_coverage(&root(), id, "wi/WI-03/domain", "merge", true).unwrap());
        for pointer in [
            "/bindingRevision",
            "/decision/coverage",
            "/approval/id",
            "/sourceRunDigest",
            "/decision/bindingDigest",
        ] {
            let t = contract_repo();
            let path = format!("docs/adr/approvals/{id}.json");
            let mut a = read(&t.path().join(&path)).unwrap();
            *a.pointer_mut(pointer).unwrap() = json!("tampered");
            data(t.path(), &path, &a);
            assert!(
                !contracts::adr_coverage(t.path(), id, "wi/WI-03/domain", "merge", true).unwrap()
            );
            assert!(
                contracts::docs(t.path()).unwrap().iter().any(|f| f.rule == "ADR-APPROVAL"),
                "{id} {pointer}"
            );
        }
    }
    let t = contract_repo();
    write(t.path(), "docs/adr/approvals/ingress-proposal.md", "rewritten policy");
    assert!(contracts::docs(t.path()).unwrap().iter().any(|f| f.rule == "ADR-APPROVAL"));
}

#[test]
fn egress_approvals_bind_full_operator_decisions_without_advancing_gates() {
    assert!(contracts::docs(&root()).unwrap().is_empty());
    for id in ["WI-ADR-004", "WI-ADR-005", "WI-ADR-006", "WI-ADR-007"] {
        assert!(contracts::adr_coverage(&root(), id, "wi/WI-04/domain", "merge", true).unwrap());
        for pointer in [
            "/bindingRevision",
            "/decision/coverage",
            "/decision/proposal",
            "/approval/id",
            "/approval/outcome",
        ] {
            let t = contract_repo();
            let path = format!("docs/adr/approvals/{id}.json");
            let mut record = read(&t.path().join(&path)).unwrap();
            *record.pointer_mut(pointer).unwrap() = json!("tampered");
            data(t.path(), &path, &record);
            assert!(
                !contracts::adr_coverage(t.path(), id, "wi/WI-04/domain", "merge", true).unwrap()
            );
            assert!(contracts::docs(t.path()).unwrap().iter().any(|f| f.rule == "ADR-APPROVAL"));
        }
    }
    assert!(!contracts::adr_coverage(&root(), "WI-WORKER-G1", "wi/WI-04/domain", "merge", true)
        .unwrap());
}

#[test]
fn receipt_and_connector_approvals_bind_full_records_without_granting_runtime() {
    for id in ["WI-ADR-008", "WI-ADR-010"] {
        assert!(contracts::adr_coverage(&root(), id, "wi/WI-04/domain", "merge", true).unwrap());
        for pointer in ["/decision/proposal", "/decision/retainedObligations", "/approval/id"] {
            let t = contract_repo();
            let path = format!("docs/adr/approvals/{id}.json");
            let mut record = read(&t.path().join(&path)).unwrap();
            *record.pointer_mut(pointer).unwrap() = json!("tampered");
            data(t.path(), &path, &record);
            assert!(
                !contracts::adr_coverage(t.path(), id, "wi/WI-04/domain", "merge", true).unwrap()
            );
            assert!(contracts::docs(t.path()).unwrap().iter().any(|f| f.rule == "ADR-APPROVAL"));
        }
    }
    assert!(!contracts::adr_coverage(&root(), "WI-WORKER-G1", "wi/WI-04/domain", "merge", true)
        .unwrap());
}
