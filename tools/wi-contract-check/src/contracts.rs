use crate::{array, evidence, files, finding, hash, read, relative, string, Finding, Result};
use serde_json::{json, Value};
use std::{collections::BTreeSet, fs, path::Path};
const PLAN: &str = "contracts/sources/wi-fabric-2-foundational-implementation-plan.md";
const DESIGN: &str = "contracts/sources/worldinterface-v2-foundational-design.md";
pub fn provenance(root: &Path) -> Result<Vec<Finding>> {
    let mut f = vec![];
    let v = read(&root.join("contracts/provenance.json"))?;
    for (path, record) in v["imports"].as_object().ok_or("imports missing")? {
        if hash(fs::read(root.join(path))?) != string(record, "sha256")? {
            f.push(finding(
                "SOURCE-HASH",
                path,
                path,
                string(record, "source")?,
                "imported source bytes differ from pinned normative artifact",
            ))
        }
    }
    Ok(f)
}
pub fn fixtures(root: &Path) -> Result<Vec<Finding>> {
    let mut f = vec![];
    let m = read(&root.join("fixtures/pre-wi-fabric-2/manifest.json"))?;
    let mut seen = BTreeSet::new();
    let mut categories = BTreeSet::new();
    for x in array(&m, "files")? {
        let p = string(x, "path")?;
        categories.insert(string(x, "category")?);
        if !p.starts_with("fixtures/pre-wi-fabric-2/")
            || p.contains("..")
            || !seen.insert(p.to_string())
        {
            f.push(finding(
                "FIXTURE-PATH",
                p,
                p,
                "WI-01 §1.5",
                "duplicate or escaping fixture path",
            ));
            continue;
        }
        if hash(fs::read(root.join(p))?) != string(x, "sha256")? {
            f.push(finding(
                "FIXTURE-HASH",
                p,
                p,
                "WI-01 §1.5",
                "fixture tampered; restore bytes or create a new immutable version",
            ))
        }
        let class = string(x, "classification")?;
        if !["copied-source", "observed-historical", "provenance-index", "interpretation"]
            .contains(&class)
            || x["fixture_version"] != 1
            || x["source"].as_str().unwrap_or("").is_empty()
            || class == "copied-source" && x["source_revision"].as_str().is_none()
            || x["category"] == "exo-coding" && !x["execution_revision"].is_null()
        {
            f.push(finding(
                "FIXTURE-PROVENANCE",
                p,
                p,
                "accepted WI-01 evidence design",
                "invalid classification or unsupported execution revision attribution",
            ))
        }
    }
    for c in [
        "dependency-lock",
        "toolchain",
        "flowspec",
        "compiled-dag",
        "receipt",
        "contextstore",
        "webhook",
        "stream",
        "wasm-manifest",
        "wasm-policy",
        "signal-ordering",
        "recovery",
        "historical-measurement",
        "descriptors",
        "exo-coding",
    ] {
        if !categories.contains(c) {
            f.push(finding(
                "FIXTURE-COVERAGE",
                "fixtures/pre-wi-fabric-2/manifest.json",
                c,
                "WI-01 §1.5",
                "missing evidence category",
            ))
        }
    }
    for p in files(&root.join("fixtures/pre-wi-fabric-2"))? {
        let p = relative(root, &p);
        if !seen.contains(&p)
            && p != "fixtures/pre-wi-fabric-2/manifest.json"
            && p != "fixtures/pre-wi-fabric-2/README.md"
        {
            f.push(finding("FIXTURE-UNINDEXED", &p, &p, "WI-01 §1.5", "unindexed fixture bytes"))
        }
    }
    if m["baseline_commit"] != "f48df82e98203fa94c86eecae60425205c3e00e9"
        || m["runtime_imports_allowed"] != false
    {
        f.push(finding(
            "FIXTURE-BASELINE",
            "fixtures/pre-wi-fabric-2/manifest.json",
            "baseline",
            "WI-01",
            "wrong frozen baseline or active archive",
        ))
    }
    Ok(f)
}
pub fn invariants_from_source(source: &str) -> Vec<Value> {
    let mut out = vec![];
    let lines: Vec<_> = source.lines().collect();
    let mut n = 0;
    while n < lines.len() {
        if let Some(heading) = lines[n].strip_prefix("### W") {
            if let Some((num, title)) = heading.split_once(". ") {
                if num.parse::<u32>().is_ok() {
                    let id = format!("W{num}");
                    n += 1;
                    let start = n;
                    while n < lines.len() && !lines[n].starts_with("##") {
                        n += 1
                    }
                    out.push(json!({"id":id,"title":title,"text":lines[start..n].join("\n").trim(),"source":format!("{DESIGN}#{id}")}));
                    continue;
                }
            }
        }
        n += 1;
    }
    out
}
pub fn contracts(root: &Path) -> Result<Vec<Finding>> {
    let mut f = provenance(root)?;
    let wi = read(&root.join("contracts/wi-fabric-2.yaml"))?;
    let stack = read(&root.join("contracts/stack.yaml"))?;
    let expected = invariants_from_source(&fs::read_to_string(root.join(DESIGN))?);
    if expected.len() != 59 || wi["invariants"] != json!(expected) {
        f.push(finding(
            "CONTRACT-INVARIANTS",
            "contracts/wi-fabric-2.yaml",
            "W1-W59",
            DESIGN,
            "invariants must exactly preserve normative headings and bodies",
        ))
    }
    if wi["contract"] != "WI-FABRIC-2"
        || wi["stack"] != stack["contract_revision"]
        || wi["dependency_direction"] != stack["dependency_direction"]
        || wi["removed_concepts"] != stack["removed_target_paths"]["WorldInterface"]
    {
        f.push(finding(
            "CONTRACT-OWNERSHIP",
            "contracts/wi-fabric-2.yaml",
            "stack",
            PLAN,
            "contract IDs, removed concepts or dependency direction disagree with canonical stack",
        ))
    }
    let expected_store = json!({"contract":"WI-FABRIC-2","fabric_schema":1,"context_schema":1,"flow_schema":1,"platform_schema":1,"platform_optional":true,"pre_contract_migration":false});
    if wi["fresh_store"] != expected_store {
        f.push(finding(
            "CONTRACT-FRESH-STORE",
            "contracts/wi-fabric-2.yaml",
            "fresh-store",
            PLAN,
            "preserve fresh schema 1 and reject pre-contract migration",
        ))
    }
    let lock = read(&root.join("contracts/aq-baseline.lock.json"))?;
    let pin = read(&root.join("contracts/aq-integration-target.json"))?;
    if pin["target"]["archive_sha256"] != lock["archive_sha256"]
        || pin["target"]["source_tree_sha256"] != lock["source_tree_sha256"]
        || pin["target"]["conformance_revision"] != lock["conformance_package_revision"]
        || pin["execution_status"] != "not-run"
        || pin["actual_checker_aq_dependencies"] != json!([])
    {
        f.push(finding(
            "CONTRACT-UPSTREAM",
            "contracts/aq-integration-target.json",
            "AQ-CONT-1",
            "accepted design; imported AQ lock",
            "target binding and actual execution identities must remain distinct",
        ))
    }
    // Ledger statuses are preserved by its imported-byte digest, including pending consumer evidence.
    read(&root.join("contracts/aq-assumptions.yaml"))?;
    Ok(f)
}
pub fn conformance(root: &Path) -> Result<Vec<Finding>> {
    let path = "conformance/wi-fabric-2/manifest.yaml";
    let m = read(&root.join(path))?;
    evidence::validate(
        &read(&root.join("conformance/wi-fabric-2/manifest.schema.json"))?,
        &m,
        "manifest",
    )?;
    let mut f = vec![];
    let wi = read(&root.join("contracts/wi-fabric-2.yaml"))?;
    if m["upstream_target"] != read(&root.join("contracts/aq-integration-target.json"))?["target"]
        || m["package_revision"] != wi["conformance_revision"]
        || m["fixture_manifest_sha256"]
            != hash(fs::read(root.join(string(&m, "fixture_manifest")?))?)
    {
        f.push(finding(
            "CONFORMANCE-PINS",
            path,
            "manifest",
            PLAN,
            "stale upstream, package revision or fixture digest",
        ))
    }
    let mut ids = BTreeSet::new();
    let mut inv = BTreeSet::new();
    for c in array(&m, "cases")? {
        if !ids.insert(string(c, "id")?) || c["status"] != "unimplemented" || !c["result"].is_null()
        {
            f.push(finding(
                "CONFORMANCE-PLANNED",
                path,
                string(c, "id")?,
                "WI-01 scope",
                "skeleton must not claim runtime implementation or results",
            ))
        }
        for i in array(c, "invariants")? {
            inv.insert(i.as_str().ok_or("invalid invariant")?.to_string());
        }
    }
    if inv != (1..=59).map(|n| format!("W{n}")).collect() {
        f.push(finding(
            "CONFORMANCE-COVERAGE",
            path,
            "W1-W59",
            DESIGN,
            "coverage skeleton must cover precisely W1-W59",
        ))
    }
    let store_path = root.join(string(&m, "store_fixture")?);
    let store = read(&store_path)?;
    if hash(fs::read(&store_path)?) != string(&m, "store_fixture_sha256")?
        || store["runtime_executed"] != false
        || store["classification"] != "planned-scenario-input"
        || store["fixture_version"] != 1
    {
        f.push(finding(
            "CONFORMANCE-STORE-FIXTURE",
            path,
            "store-inputs",
            "implementation plan §2.2",
            "tampered or misclassified planned fresh-store inputs",
        ));
    }
    let inputs = array(&store, "scenarios")?;
    if inputs.len() != 4
        || inputs[0]["files"] != json!({})
        || inputs[1]["files"]["manifest.json"]["contract"] != "WI-FABRIC-2"
        || inputs[1]["files"]["manifest.json"]["fabric_schema"] != 1
        || !inputs[2]["files"]["manifest.json"].is_null()
        || inputs[2]["files"].as_object().is_none_or(|x| x.is_empty())
        || inputs[3]["files"]["manifest.json"]["fabric_schema"] != 999
    {
        f.push(finding(
            "CONFORMANCE-STORE-FIXTURE",
            path,
            "store-inputs",
            "implementation plan §2.2",
            "fresh-store inputs no longer exercise empty/valid/pre-contract/unknown cases",
        ));
    }
    let scenarios = array(&m, "store_scenarios")?;
    for (id, expected, unchanged) in [
        ("STORE-EMPTY", "initialize", false),
        ("STORE-VALID", "open", false),
        ("STORE-PRE-CONTRACT", "reject", true),
        ("STORE-UNKNOWN", "reject", true),
    ] {
        let matches: Vec<_> = scenarios.iter().filter(|s| s["id"] == id).collect();
        if matches.len() != 1
            || matches[0]["expected"] != expected
            || matches[0]["unchanged_on_rejection"] != unchanged
        {
            f.push(finding(
                "CONFORMANCE-STORE",
                path,
                id,
                "implementation plan §2.2",
                "missing, duplicate or altered fresh-store scenario",
            ))
        }
    }
    if scenarios.len() != 4 {
        f.push(finding(
            "CONFORMANCE-STORE",
            path,
            "store-scenarios",
            "WI-01",
            "unexpected scenario; scope change requires review",
        ))
    }
    Ok(f)
}
pub fn docs(root: &Path) -> Result<Vec<Finding>> {
    let plan = fs::read_to_string(root.join(PLAN))?;
    let queue = read(&root.join("docs/adr/queue.json"))?;
    let records = queue.as_array().ok_or("ADR queue missing")?;
    let mut f = vec![];
    let expected: Vec<_> = plan
        .lines()
        .filter(|l| l.starts_with("| `WI-ADR-"))
        .map(|l| l.split('|').map(str::trim).collect::<Vec<_>>())
        .collect();
    if expected.len() != 20 || records.len() != 20 {
        return Err("ADR queue must contain 20 source decisions".into());
    }
    for (row, r) in expected.iter().zip(records) {
        let id = row[1].trim_matches('`');
        if r["id"] != id
            || r["decision"] != row[2]
            || r["due"] != row[3]
            || r["owner"] != "operator"
            || r["status"] != "queued"
        {
            f.push(finding(
                "ADR-QUEUE",
                "docs/adr/queue.json",
                id,
                "implementation plan §4; accepted owner decision",
                "preserve subject, deadline and operator ownership; stub is not acceptance",
            ))
        }
        let stub = fs::read_to_string(root.join(format!("docs/adr/{id}.md")))?;
        if !stub.contains(row[2])
            || !stub.contains(row[3])
            || !stub.contains("Owner: operator")
            || !stub.contains("Status: queued")
        {
            f.push(finding(
                "ADR-STUB",
                "docs/adr/queue.json",
                id,
                "implementation plan §4",
                "stub disagrees with queue",
            ))
        }
    }
    let issues = read(&root.join("docs/work-items/wi-02.json"))?;
    let rows = issues.as_array().ok_or("WI-02 drafts missing")?;
    if rows.len() != 4 {
        return Err("four WI-02 issue drafts required".into());
    }
    for (i, r) in rows.iter().enumerate() {
        if r["id"] != format!("WI-02-{:02}", i + 1)
            || r["status"] != "draft-not-filed"
            || r["maturity"] != if i == 3 { "assumption-bound" } else { "independent" }
            || r["acceptance"].as_array().is_none_or(|a| a.len() < 2)
            || r["adrs"].as_array().is_none_or(Vec::is_empty)
            || r["invariants"].as_array().is_none_or(Vec::is_empty)
        {
            f.push(finding(
                "WI02-DRAFT",
                "docs/work-items/wi-02.json",
                &format!("WI-02-{:02}", i + 1),
                "implementation plan WI-02",
                "draft requires maturity, ADRs, invariants and concrete acceptance",
            ))
        }
    }
    for p in [
        "CONTRIBUTING.md",
        ".github/pull_request_template.md",
        ".github/ISSUE_TEMPLATE/work-item.md",
    ] {
        let text = fs::read_to_string(root.join(p))?;
        for word in ["maturity", "invariants", "evidence"] {
            if !text.to_lowercase().contains(word) {
                f.push(finding(
                    "REVIEW-TEMPLATE",
                    p,
                    word,
                    "implementation plan §2.3",
                    "missing required review field",
                ))
            }
        }
    }
    let upstream = fs::read_to_string(root.join(".github/ISSUE_TEMPLATE/upstream-requirement.md"))?;
    for word in ["semantic need", "failure scenario", "conformance case", "affected WI", "AQ"] {
        if !upstream.contains(word) {
            f.push(finding(
                "UPSTREAM-TEMPLATE",
                ".github/ISSUE_TEMPLATE/upstream-requirement.md",
                word,
                "implementation plan §3.4",
                "missing upstream requirement field",
            ))
        }
    }
    let prep = read(&root.join("docs/release/preparation.schema.json"))?;
    let done = read(&root.join("docs/release/completion.schema.json"))?;
    if prep["properties"]["kind"]["const"] != "publication-preparation"
        || prep["properties"]["publication_status"]["const"] != "not-performed"
        || done["properties"]["kind"]["const"] != "publication-completion"
        || !array(&done, "required")?.contains(&json!("preparation"))
    {
        return Err("publication preparation and completion must remain separate".into());
    }
    Ok(f)
}

/// Validate local frozen Git evidence. This is not remote publication evidence.
pub fn baseline(root: &Path) -> Result<Vec<Finding>> {
    use std::process::Command;
    let m = read(&root.join("fixtures/pre-wi-fabric-2/manifest.json"))?;
    let revision = string(&m, "baseline_commit")?;
    let tag = string(&m, "baseline_tag")?;
    let output = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", &format!("refs/tags/{tag}^{{}}")])
        .output()?;
    let mut f = vec![];
    if !output.status.success() || String::from_utf8_lossy(&output.stdout).trim() != revision {
        f.push(finding(
            "BASELINE-TAG",
            "fixtures/pre-wi-fabric-2/manifest.json",
            tag,
            "implementation plan WI-01",
            "local frozen tag absent or points to wrong revision; do not infer remote publication",
        ))
    }
    for item in array(&m, "files")? {
        if let Some(source) = item["source"].as_str().and_then(|s| s.strip_prefix("git:")) {
            let output = Command::new("git").current_dir(root).args(["show", source]).output()?;
            if !output.status.success() || hash(output.stdout) != string(item, "sha256")? {
                f.push(finding(
                    "BASELINE-SOURCE",
                    string(item, "path")?,
                    source,
                    "implementation plan §1.5",
                    "copied source does not match frozen Git blob",
                ))
            }
        }
    }
    Ok(f)
}
