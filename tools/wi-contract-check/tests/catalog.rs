//! Synthetic definition review only. No runtime qualification or authority is granted.
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};
use tempfile::TempDir;
use wi_contract_check::{catalog::*, contracts, files, hash, inventory, policy, read};
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}
fn load<T: serde::de::DeserializeOwned>(path: &str) -> T {
    serde_json::from_value(read(&root().join(path)).unwrap()).unwrap()
}
fn plan() -> Disposition {
    load("inventory/connector-disposition.json")
}
fn sandbox() -> Sandbox {
    load("contracts/wi10-sandbox-disposition.json")
}
fn items() -> Vec<inventory::Item> {
    load("inventory/current.json")
}
fn copy(src: &Path, dst: &Path) {
    for p in files(src).unwrap() {
        let to = dst.join(p.strip_prefix(src).unwrap());
        fs::create_dir_all(to.parent().unwrap()).unwrap();
        fs::copy(p, to).unwrap();
    }
}
fn request() -> Request {
    let admission = review();
    let mut binding = sandbox().mapping.selected;
    binding.descriptor = hash(serde_json::to_vec(&admission).unwrap());
    Request {
        admission,
        binding,
        operation: Operation::Start,
        evaluation_only: false,
        authorized: true,
        mandatory_controls_enforced: true,
        resource_bounds_enforced: true,
        non_sensitive_inputs: true,
        reviewed_executable_and_inputs: true,
        requires_independent_vm: false,
        quarantined: false,
    }
}
fn qualification() -> Qualification {
    Qualification {
        binding: request().binding,
        results: hash("synthetic results"),
        reviewer_evidence: hash("independent-review-assertion"),
        independent_review: true,
        passed: true,
        revoked: false,
        synthetic: false,
    }
}
fn approval() -> EvaluationApproval {
    EvaluationApproval {
        binding: request().binding,
        approval_id: "synthetic-exact-fixture-approval".into(),
        reviewed_fixtures: true,
        non_sensitive_environment: true,
        revoked: false,
    }
}
fn review() -> AdmissionReview {
    let mut review = AdmissionReview {
        version: 1,
        operation: "process.execute".into(),
        justified_criteria: [(
            "trust_boundary".into(),
            "bounded exported result crosses worker boundary".into(),
        )]
        .into(),
        frequency: "one reviewed test fixture per run".into(),
        mediation_cost: "accountable exported result justifies boundary cost".into(),
        appendix_answers: QUESTIONS
            .iter()
            .map(|k| (k.to_string(), format!("synthetic reviewed answer for {k}")))
            .collect(),
        descriptor: DESCRIPTORS
            .iter()
            .map(|k| (k.to_string(), format!("synthetic reviewed declaration for {k}")))
            .collect(),
        arbitrary_deployment_shell: false,
    };
    for (key, value) in [
        ("time_limit", "1000"),
        ("memory_limit", "1048576"),
        ("output_limit", "65536"),
        ("isolation", "process-managed-proof-1"),
        ("effect_class", "bounded-process"),
        ("secrets", "no-production-secrets"),
    ] {
        review.descriptor.insert(key.into(), value.into());
    }
    review
}
#[test]
fn inventory_mutations_fail() {
    let root = root();
    let items = items();
    let base = plan();
    assert!(validate_disposition(&root, &items, &base).unwrap().is_empty());
    for kind in ["connector", "wasm-manifest", "registration", "source-module"] {
        let i = items
            .iter()
            .find(|i| i.kind == kind && (kind != "source-module" || i.path.contains("code_common")))
            .unwrap();
        let index =
            base.families.iter().position(|g| g.items.iter().any(|r| r.id == i.id)).unwrap();
        for mutation in 0..5 {
            let mut d = base.clone();
            let g = &mut d.families[index];
            let n = g.items.iter().position(|r| r.id == i.id).unwrap();
            match mutation {
                0 => {
                    g.items.remove(n);
                }
                1 => {
                    g.items.push(g.items[n].clone());
                }
                2 => g.items[n].source_sha256 = hash("stale"),
                3 => g.items[n].id = "stale-id".into(),
                _ => g.items[n].registration_sites.push("unreviewed-dynamic-site".into()),
            }
            assert!(
                validate_disposition(&root, &items, &d)
                    .unwrap()
                    .iter()
                    .any(|f| f.rule == "CAT-INVENTORY"),
                "{kind} mutation {mutation}"
            );
        }
    }
    let mut d = base.clone();
    d.families.remove(0);
    assert!(!validate_disposition(&root, &items, &d).unwrap().is_empty());
    let mut d = base.clone();
    d.families.push(d.families[0].clone());
    assert!(!validate_disposition(&root, &items, &d).unwrap().is_empty());
    let mut d = base;
    let g = d.families.iter_mut().find(|g| g.id == "code-common").unwrap();
    g.destination = Destination::Infrastructure;
    assert!(!validate_disposition(&root, &items, &d).unwrap().is_empty());
    let (new, _) = inventory::scan_source(
        "crates/worldinterface-connector/src/connectors/new_helper.rs",
        "fn private_helper() {} impl Connector for NewConnector {}",
    )
    .unwrap();
    let mut changed = items;
    changed.extend(new);
    assert!(validate_disposition(&root, &changed, &d)
        .unwrap()
        .iter()
        .any(|f| f.rule == "CAT-INVENTORY"));
}
#[test]
fn surface_mutations_fail() {
    let root = root();
    let items = items();
    let base = plan();
    for mutation in 0..5 {
        let mut d = base.clone();
        let g = d.families.iter_mut().find(|g| g.id == "code-grep").unwrap();
        match mutation {
            0 => {
                g.surfaces.pop();
            }
            1 => g.surfaces.push(g.surfaces[0].clone()),
            2 => g.surfaces[0].source_sha256 = hash("stale"),
            3 => g.replacement_requirements.clear(),
            _ => g.removal_claimed = true,
        }
        assert!(!validate_disposition(&root, &items, &d).unwrap().is_empty());
    }
    assert!(validate_forbidden_coverage(&root, &base).unwrap().is_empty());
    let mut d = base;
    for g in &mut d.families {
        g.surfaces.retain(|s| !s.path.ends_with("code_common.rs"));
    }
    assert!(!validate_forbidden_coverage(&root, &d).unwrap().is_empty());
}
#[test]
fn removal_requires_exact_independent_evidence() {
    let q = qualification();
    let b = q.binding.clone();
    let expected = ReplacementExpectation {
        revision: "a".repeat(40),
        artifact_digest: hash("replacement"),
        binding: b.clone(),
        fixture_results: q.results.clone(),
        wi06_acceptance: hash("WI-06 exact receipt"),
        wi09_acceptance: Some(hash("WI-09 exact receipt")),
    };
    let evidence = Replacement {
        revision: expected.revision.clone(),
        artifact_digest: expected.artifact_digest.clone(),
        binding: b,
        qualification: q,
        fixture_results: expected.fixture_results.clone(),
        wi06_acceptance: expected.wi06_acceptance.clone(),
        wi09_acceptance: expected.wi09_acceptance.clone(),
    };
    assert!(!assess_removal(&expected, None));
    assert!(assess_removal(&expected, Some(&evidence)));
    for mutation in 0..13 {
        let mut e = evidence.clone();
        match mutation {
            0 => e.revision = "b".repeat(40),
            1 => e.artifact_digest = hash("wrong"),
            2 => e.binding.configuration = hash("drift"),
            3 => e.fixture_results = hash("wrong"),
            4 => e.wi06_acceptance.clear(),
            5 => e.wi09_acceptance = None,
            6 => e.qualification.synthetic = true,
            7 => e.qualification.revoked = true,
            8 => e.qualification.independent_review = false,
            9 => e.qualification.passed = false,
            10 => e.qualification.reviewer_evidence.clear(),
            11 => e.qualification.binding.adapter = hash("drift"),
            _ => e.qualification.results = hash("unrelated-report"),
        }
        assert!(!assess_removal(&expected, Some(&e)), "mutation {mutation}");
    }
    let mut d = plan();
    d.families[0].replacement = Some(evidence);
    assert!(validate_disposition(&root(), &items(), &d)
        .unwrap()
        .iter()
        .any(|f| f.rule == "CAT-REMOVAL"));
    assert_eq!(
        read(&root().join("contracts/milestones.json")).unwrap()["reached"],
        json!(["WI-01"])
    );
}
#[test]
fn admission_checklist_is_complete() {
    let base = review();
    assert!(validate_admission(&base));
    for key in DESCRIPTORS {
        let mut r = base.clone();
        r.descriptor.remove(*key);
        assert!(!validate_admission(&r), "missing {key}");
        let mut r = base.clone();
        r.descriptor.insert(key.to_string(), " ".into());
        assert!(!validate_admission(&r));
    }
    for key in QUESTIONS {
        let mut r = base.clone();
        r.appendix_answers.remove(*key);
        assert!(!validate_admission(&r));
    }
    for mutation in 0..7 {
        let mut r = base.clone();
        match mutation {
            0 => r.justified_criteria.clear(),
            1 => r.frequency.clear(),
            2 => r.mediation_cost.clear(),
            3 => r.arbitrary_deployment_shell = true,
            4 => r.operation = "shell.exec".into(),
            5 => {
                r.descriptor.insert("metadata".into(), "anything".into());
            }
            _ => {
                r.justified_criteria.insert("agent-wants-it".into(), "convenient".into());
            }
        }
        assert!(!validate_admission(&r));
    }
}
#[test]
fn catalog_mutations_fail() {
    let base: Catalog = load("contracts/wi10-catalog.json");
    assert!(validate_catalog(&base).is_empty());
    for mutation in 0..8 {
        let mut c = base.clone();
        match mutation {
            0 => {
                c.candidates.pop();
            }
            1 => c.candidates.push(c.candidates[0].clone()),
            2 => c.candidates[0].status = "admitted".into(),
            3 => c.runtime_pass_asserted = true,
            4 => c.denied.clear(),
            5 => {
                c.admission_criteria.pop();
            }
            6 => {
                c.descriptor_requirements.pop();
            }
            _ => c.appendix_questions.clear(),
        }
        assert!(!validate_catalog(&c).is_empty());
    }
    for path in [
        "contracts/wi10-catalog.json",
        "contracts/wi10-sandbox-disposition.json",
        "inventory/connector-disposition.json",
        "contracts/wi10-obligations.json",
    ] {
        let mut v = read(&root().join(path)).unwrap();
        v["unreviewed_extension"] = json!(true);
        let rejects = match path {
            "contracts/wi10-catalog.json" => serde_json::from_value::<Catalog>(v).is_err(),
            "inventory/connector-disposition.json" => {
                serde_json::from_value::<Disposition>(v).is_err()
            }
            "contracts/wi10-obligations.json" => serde_json::from_value::<Obligations>(v).is_err(),
            _ => serde_json::from_value::<Sandbox>(v).is_err(),
        };
        assert!(rejects);
    }
}
#[test]
fn wp007_unqualified_and_evaluation_table() {
    use AdmissionDecision::*;
    let mut r = request();
    let a = approval();
    assert_eq!(evaluate_registration(&r, None, None), Denied);
    assert_eq!(evaluate_registration(&r, None, Some(&a)), Denied);
    r.evaluation_only = true;
    assert_eq!(evaluate_registration(&r, None, None), Denied);
    assert_eq!(evaluate_registration(&r, None, Some(&a)), EvaluationOnly);
    for mutation in 0..8 {
        let mut changed = r.clone();
        match mutation {
            0 => changed.authorized = false,
            1 => changed.mandatory_controls_enforced = false,
            2 => changed.resource_bounds_enforced = false,
            3 => changed.non_sensitive_inputs = false,
            4 => changed.reviewed_executable_and_inputs = false,
            5 => changed.requires_independent_vm = true,
            6 => changed.binding.fixtures = hash("different-fixture"),
            _ => changed.quarantined = true,
        }
        assert_eq!(evaluate_registration(&changed, None, Some(&a)), Denied, "mutation {mutation}");
    }
    for mutation in 0..5 {
        let mut a = a.clone();
        match mutation {
            0 => a.approval_id.clear(),
            1 => a.revoked = true,
            2 => a.reviewed_fixtures = false,
            3 => a.non_sensitive_environment = false,
            _ => a.binding.environment = hash("different-environment"),
        }
        assert_eq!(evaluate_registration(&r, None, Some(&a)), Denied);
    }
    // Evaluation success returns a value; it cannot mutate qualification or promote.
    let before = serde_json::to_value(&a).unwrap();
    assert_eq!(evaluate_registration(&r, None, Some(&a)), EvaluationOnly);
    r.evaluation_only = false;
    assert_eq!(evaluate_registration(&r, None, Some(&a)), Denied);
    assert_eq!(serde_json::to_value(&a).unwrap(), before);
}
#[test]
fn qualification_drift_and_revocation() {
    use AdmissionDecision::*;
    let r = request();
    let q = qualification();
    assert_eq!(evaluate_registration(&r, Some(&q), None), Qualified);
    for field in [
        "descriptor",
        "profile",
        "adapter",
        "executable",
        "dependencies",
        "configuration",
        "environment",
        "fixtures",
    ] {
        let mut v = serde_json::to_value(&r).unwrap();
        v["binding"][field] = json!(hash("changed"));
        let changed: Request = serde_json::from_value(v).unwrap();
        assert_eq!(evaluate_registration(&changed, Some(&q), None), Denied, "{field}");
    }
    for mutation in 0..6 {
        let mut q = q.clone();
        match mutation {
            0 => q.revoked = true,
            1 => q.synthetic = true,
            2 => q.independent_review = false,
            3 => q.passed = false,
            4 => q.results.clear(),
            _ => q.reviewer_evidence.clear(),
        }
        assert_eq!(evaluate_registration(&r, Some(&q), None), Denied);
    }
}
#[test]
fn quarantine_and_discovery() {
    use AdmissionDecision::*;
    let mut r = request();
    r.quarantined = true;
    let q = qualification();
    assert_eq!(evaluate_registration(&r, Some(&q), None), Denied);
    for operation in [Operation::Inspect, Operation::Terminate, Operation::Reconcile] {
        r.operation = operation;
        r.authorized = true;
        assert_eq!(evaluate_registration(&r, None, None), AuthorizedCleanup);
        r.authorized = false;
        assert_eq!(evaluate_registration(&r, None, None), Denied);
    }
    r.authorized = true;
    let support = inspect_supported(&r, Some(&q), None);
    assert!(support.iter().all(|s| !s.grants_authority));
    assert!(support
        .iter()
        .filter(|s| matches!(
            s.operation,
            Operation::Start
                | Operation::Collect
                | Operation::Command
                | Operation::Checkpoint
                | Operation::Suspend
                | Operation::Resume
        ))
        .all(|s| !s.available));
    assert!(support
        .iter()
        .filter(|s| matches!(
            s.operation,
            Operation::Inspect | Operation::Terminate | Operation::Reconcile
        ))
        .all(|s| s.available));
    r.quarantined = false;
    r.authorized = false;
    assert!(inspect_supported(&r, Some(&q), None)
        .iter()
        .all(|s| !s.available && !s.grants_authority));
}
#[test]
fn sandbox_and_mapping_mutations_fail() {
    let base = sandbox();
    assert!(validate_sandbox(&base).is_empty());
    assert_ne!(base.mapping.requested_controls, base.mapping.advertised_controls);
    assert_ne!(base.mapping.applied_controls, base.mapping.observed_controls);
    for mutation in 0..13 {
        let mut s = base.clone();
        match mutation {
            0 => s.package_destination = "worldinterface-dependency".into(),
            1 => s.replacement_fixture.clear(),
            2 => s.unavailable_assurances.clear(),
            3 => s.verifiers[0].external_io = true,
            4 => s.verifiers[2].executes_tests = true,
            5 => s.verifiers[0].max_input_bytes = u64::MAX,
            6 => s.mapping.image = ImageApplicability::Unknown,
            7 => s.mapping.image = ImageApplicability::Known(hash("fake-image")),
            8 => s.mapping.provider_ref = "/host/bin/tool".into(),
            9 => s.mapping.run_ref = "pid:1234".into(),
            10 => s.mapping.evidence_ref.max_bytes = u64::MAX,
            11 => s.mapping.observed_controls.push(Control::WallTimeMs(0)),
            _ => s.mapping.selected.configuration = hash("substituted-manifest"),
        }
        assert!(!validate_sandbox(&s).is_empty(), "mutation {mutation}");
    }
    let mut v = serde_json::to_value(&base).unwrap();
    v["mapping"]["metadata"] = json!({"host_pid":7});
    assert!(serde_json::from_value::<Sandbox>(v).is_err());
}
#[test]
fn production_cannot_reach_domain_tooling() {
    let t = TempDir::new().unwrap();
    fs::create_dir_all(t.path().join("crates/a/src")).unwrap();
    fs::write(t.path().join("Cargo.toml"), "[workspace]\nmembers=['crates/a']\n").unwrap();
    fs::write(t.path().join("crates/a/Cargo.toml"), "[package]\nname='a'\nversion='0.1.0'\n")
        .unwrap();
    fs::write(t.path().join("crates/a/src/lib.rs"), "pub struct Safe;").unwrap();
    copy(&root().join("contracts"), &t.path().join("contracts"));
    copy(&root().join("tools/wi-contract-check"), &t.path().join("tools/wi-contract-check"));
    assert!(policy::check_at(t.path(), &BTreeSet::new()).unwrap().is_empty());
    fs::write(t.path().join("crates/a/Cargo.toml"),"[package]\nname='a'\nversion='0.1.0'\n[dependencies]\nwi-contract-check={path='../../tools/wi-contract-check',optional=true}\n").unwrap();
    assert!(!policy::check_at(t.path(), &BTreeSet::new()).unwrap().is_empty());
    fs::write(t.path().join("crates/a/Cargo.toml"), "[package]\nname='a'\nversion='0.1.0'\n")
        .unwrap();
    fs::write(
        t.path().join("crates/a/src/lib.rs"),
        "include!(\"../../../tools/wi-contract-check/src/catalog.rs\");",
    )
    .unwrap();
    assert!(!policy::check_at(t.path(), &BTreeSet::new()).unwrap().is_empty());
    fs::write(
        t.path().join("crates/a/src/lib.rs"),
        "const PLAN: &str = include_str!(\"../../../contracts/wi10-catalog.json\");",
    )
    .unwrap();
    assert!(!policy::check_at(t.path(), &BTreeSet::new()).unwrap().is_empty());
}
#[test]
fn adr_clause_coverage_cannot_expand() {
    assert!(
        contracts::adr_coverage(&root(), "WI-ADR-017", "wi/WI-10/domain", "merge", true).unwrap()
    );
    assert!(
        contracts::adr_coverage(&root(), "WI-ADR-016", "wi/WI-10/domain", "start", false).unwrap()
    );
    assert!(
        !contracts::adr_coverage(&root(), "WI-ADR-016", "wi/WI-10/domain", "start", true).unwrap()
    );
    assert!(
        !contracts::adr_coverage(&root(), "WI-ADR-016", "wi/WI-09/domain", "merge", false).unwrap()
    );
    assert!(
        !contracts::adr_coverage(&root(), "WI-ADR-016", "wi/WI-10/domain", "merge", false).unwrap()
    );
    let t = TempDir::new().unwrap();
    copy(&root().join("docs/adr"), &t.path().join("docs/adr"));
    let path = t.path().join("docs/adr/approvals.json");
    let mut v: Value = read(&path).unwrap();
    v["records"][0]["decision"]["coverage"] = json!("full");
    fs::write(path, serde_json::to_vec(&v).unwrap()).unwrap();
    assert!(
        !contracts::adr_coverage(t.path(), "WI-ADR-016", "wi/WI-10/domain", "start", true).unwrap()
    );
}
#[test]
fn obligations_preserve_pending_evidence() {
    let mut o: Obligations = load("contracts/wi10-obligations.json");
    assert!(validate_obligations(&root(), &o).unwrap().is_empty());
    o.assumptions[0].status = "resolved".into();
    assert!(!validate_obligations(&root(), &o).unwrap().is_empty());
    o.assumptions[0].status = "pending".into();
    o.obligations[0].tests = vec!["nonexistent".into()];
    assert!(!validate_obligations(&root(), &o).unwrap().is_empty());
}
#[test]
fn orphan_helpers_stay_forbidden_only_at_controller_milestone() {
    let p = read(&root().join("contracts/forbidden-symbols.yaml")).unwrap();
    for name in ["code_common", "code_fuzzy", "gitignore_check"] {
        let path = format!("crates/worldinterface-connector/src/connectors/{name}.rs");
        let anonymous_helper = "pub fn helper_without_old_name() {}";
        assert!(policy::symbol_findings(&path, anonymous_helper, &p, &BTreeSet::new())
            .unwrap()
            .is_empty());
        assert!(policy::symbol_findings(&path, anonymous_helper, &p, &[10].into())
            .unwrap()
            .iter()
            .any(|f| f.rule == "CB-CODING-HELPERS"));
    }
}
#[test]
fn contradictory_or_substituted_descriptors_block_registration() {
    let base = review();
    for (key, value) in [
        ("time_limit", "unbounded"),
        ("memory_limit", "0"),
        ("output_limit", "-1"),
        ("isolation", "independent-worker-vm"),
        ("secrets", "production-credentials"),
        ("effect_class", "pure-transform"),
    ] {
        let mut review = base.clone();
        review.descriptor.insert(key.into(), value.into());
        assert!(!validate_admission(&review));
    }
    let q = qualification();
    let mut r = request();
    r.admission.descriptor.insert("resolver".into(), "substituted resolver".into());
    assert_eq!(evaluate_registration(&r, Some(&q), None), AdmissionDecision::Denied);
    r.binding.descriptor = hash(serde_json::to_vec(&r.admission).unwrap());
    assert_eq!(evaluate_registration(&r, Some(&q), None), AdmissionDecision::Denied);
    r.evaluation_only = true;
    assert_eq!(evaluate_registration(&r, None, Some(&approval())), AdmissionDecision::Denied);
}
#[test]
fn control_provenance_is_typed_bounded_and_content_bound() {
    let base = sandbox();
    let mut s = base.clone();
    // A producer's advertised maximum does not replace the requested value.
    assert_eq!(s.mapping.requested_controls[0], Control::WallTimeMs(1000));
    assert_eq!(s.mapping.advertised_controls[0], Control::WallTimeMs(2000));
    assert!(s.mapping.observed_controls.is_empty());
    s.mapping.applied_controls[0] = Control::WallTimeMs(2000);
    assert!(!validate_sandbox(&s).is_empty());
    let mut s = base.clone();
    s.mapping.evidence_ref.max_bytes = 1;
    assert!(!validate_sandbox(&s).is_empty());
    let mut s = base.clone();
    s.mapping.resolved_effect_manifest.max_bytes = 1;
    assert!(!validate_sandbox(&s).is_empty());
    let mut v = serde_json::to_value(&base).unwrap();
    v["mapping"]["observed_controls"] = json!([{"control":"independent-vm","value":true}]);
    assert!(serde_json::from_value::<Sandbox>(v).is_err());
}
