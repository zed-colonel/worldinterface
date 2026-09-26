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
        || !["not-run", "candidate-evidence-external"]
            .contains(&pin["execution_status"].as_str().unwrap_or(""))
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
        let baseline = array(&m, "assigned_aq_baseline_cases")?.contains(&c["id"]);
        if baseline {
            let original = read(
                &root.join("contracts/sources/aq-baseline-integration-acceptance-matrix.yaml"),
            )?;
            let source = array(&original, "cases")?
                .iter()
                .find(|r| r["id"] == c["id"])
                .ok_or("unknown source case")?;
            if c["source_record_sha256"] != hash(serde_json::to_vec(source)?)
                || !root.join(string(c, "test_source")?).is_file()
            {
                return Err("baseline source/test binding mismatch".into());
            }
        }
        if !ids.insert(string(c, "id")?)
            || (!baseline && c["status"] != "unimplemented")
            || !c["result"].is_null()
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
    let assigned: BTreeSet<_> =
        array(&m, "assigned_aq_baseline_cases")?.iter().map(|v| v.as_str().unwrap_or("")).collect();
    if assigned != BTreeSet::from(["AB-WI-001", "AB-WI-015", "AB-WI-016"])
        || !assigned.iter().all(|id| ids.contains(id))
    {
        return Err("assigned G1 cases must be registered exactly".into());
    }
    if m["integration_fixture_sha256"]
        != hash(fs::read(root.join(string(&m, "integration_fixture")?))?)
    {
        return Err("integration fixture changed".into());
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
/// Recorded operator guidance, pinned independently from mutable queue status.
/// Scoped acceptance never satisfies a full-checkpoint request.
pub fn adr_coverage(root: &Path, id: &str, scope: &str, phase: &str, full: bool) -> Result<bool> {
    let ingress_pin = match id {
        "WI-ADR-012" => Some("2be10eb17b6770127cdb8fb12b655243230d73da8884044a866a633a6d51ef8b"),
        "WI-ADR-013" => Some("5d6f0c215398cbaa01bce0bfd1639874736a2419a1fc75246a7471e9f8af9bcb"),
        _ => None,
    };
    if let Some(pin) = ingress_pin {
        let bytes = fs::read(root.join(format!("docs/adr/approvals/{id}.json")))?;
        return Ok(hash(&bytes) == pin
            && hash(&fs::read(root.join("docs/adr/approvals/ingress-proposal.md"))?)
                == "752a0ef945c6146feb8de94a4474d93d97dc19ba2e3571a2058b10218b077616");
    }
    let bytes = fs::read(root.join("docs/adr/approvals.json"))?;
    if hash(&bytes) != "80d06ce11cfdf319d7611c3d8bc093c086e29ab68d482fdff200cc5fa5156c90" {
        return Ok(false);
    }
    let v: Value = serde_json::from_slice(&bytes)?;
    let Some(record) =
        v["records"].as_array().and_then(|rs| rs.iter().find(|r| r["checkpoint"] == id))
    else {
        return Ok(false);
    };
    if record["approval"]["outcome"] != "accepted" {
        return Ok(false);
    }
    if record["decision"]["coverage"] == "full" {
        return Ok(true);
    }
    Ok(!full
        && record["decision"]["consumers"]
            .as_array()
            .is_some_and(|cs| cs.iter().any(|c| c["sliceId"] == scope && c["phase"] == phase)))
}
fn adr_status_matches(root: &Path, id: &str, r: &Value) -> Result<bool> {
    let (status, approval, packet) = match id {
        "WI-ADR-016" => (
            "accepted",
            "10a13b9c-cbfc-4469-9e03-9011adbdd507",
            "e8b380e95823b73ad67cb0adf5c7cb1b681cbca4d3557df837f6f22001455aca",
        ),
        "WI-ADR-017" => (
            "accepted",
            "01f46a00-49ae-4244-ae29-ea95cc6413c3",
            "0ed0ac6f51edbcd7f66f5c2f2affaff97ad7000aefb3e761639e550e380d3f4a",
        ),
        _ => return Ok(r["status"] == "queued" && r.get("approval_id").is_none()),
    };
    Ok(r["status"] == status
        && r["approval_id"] == approval
        && r["decision_packet_digest"] == packet
        && adr_coverage(root, id, "wi/WI-10/domain", "start", id == "WI-ADR-017")?)
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
        let approved = match id {
            "WI-ADR-001" => Some((
                "accepted",
                "full",
                "ffc652dc4aeb0af4708da435a3836d48e4f34bccd0759cd5df10bb5fcb83e099",
            )),
            "WI-ADR-002" => Some((
                "accepted",
                "full",
                "90fc2c3a245ecc3e2f007a284aa54b82a24858409e7b8b8353643bd138cfb326",
            )),
            "WI-ADR-003" => Some((
                "accepted",
                "full",
                "1912ba6bcb31f0c4a32053fb5d6b0156458db1d5496ca92018881afee241de18",
            )),
            "WI-ADR-013" => Some((
                "accepted",
                "full",
                "5d6f0c215398cbaa01bce0bfd1639874736a2419a1fc75246a7471e9f8af9bcb",
            )),
            "WI-ADR-012" => Some((
                "accepted",
                "full",
                "2be10eb17b6770127cdb8fb12b655243230d73da8884044a866a633a6d51ef8b",
            )),
            "WI-ADR-016" => Some((
                "accepted",
                "full",
                "b25fea0d6571e51bfbd60d370a2c5fad7b30b664b05ba80c425ac2616cf7d9cf",
            )),
            _ => None,
        };
        let status = approved.map_or("queued", |(status, _, _)| status);
        if let Some((_, coverage, digest)) = approved {
            let path = if id == "WI-ADR-016" {
                "docs/adr/approvals/WI-ADR-016-full.json".to_string()
            } else {
                format!("docs/adr/approvals/{id}.json")
            };
            let bytes = fs::read(root.join(&path))?;
            let approval: Value = serde_json::from_slice(&bytes)?;
            let historical_intact = id != "WI-ADR-016"
                || hash(&fs::read(root.join("docs/adr/approvals/WI-ADR-016.json"))?)
                    == "a32ba0a9c38340cfa3be18e9e6fa19b20172fb4d33e42b55d81307c9a84fcca8";
            if !historical_intact
                || hash(&bytes) != digest
                || r["approval_sha256"] != digest
                || r["approval_file"] != path
                || r["coverage"] != coverage
                || approval["checkpoint"] != id
                || approval["decision"]["coverage"] != coverage
                || approval["approval"]["outcome"] != "accepted"
                || r["approval_id"] != approval["approval"]["id"]
            {
                f.push(finding("ADR-APPROVAL", &path, id, "supplied operator approvals, binding revision 4", "approval provenance or coverage changed; full and clause acceptance are distinct"));
            }
        }
        if r["id"] != id
            || r["decision"] != row[2]
            || r["due"] != row[3]
            || r["owner"] != "operator"
            || if matches!(
                id,
                "WI-ADR-001" | "WI-ADR-002" | "WI-ADR-003" | "WI-ADR-012" | "WI-ADR-013"
            ) {
                r["status"] != status
            } else {
                !adr_status_matches(root, id, r)?
            }
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
            || !stub.contains(&format!("Status: {}\n", r["status"].as_str().unwrap_or("invalid")))
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
    if hash(&fs::read(root.join("docs/adr/approvals/ingress-proposal.md"))?)
        != "752a0ef945c6146feb8de94a4474d93d97dc19ba2e3571a2058b10218b077616"
    {
        f.push(finding(
            "ADR-APPROVAL",
            "docs/adr/approvals/ingress-proposal.md",
            "WI-ADR-012/013",
            "bound operator proposal",
            "approved proposal bytes changed",
        ));
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
