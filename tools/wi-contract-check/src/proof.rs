//! Off-map proof records. Discovery establishes test eligibility, never test passage.
use crate::{array, contracts, finding, hash, read, string, Finding, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

pub const BASE: &str = "conformance/wi-fabric-2/proof";
pub const EXPORT: &str = "conformance/wi-fabric-2/proof/wi-retired-map-gates-2026-10-13.json";
pub const EXPORT_SHA256: &str = "a092d50054dc125772498cdb268d709cf4a2800d427c7ea07e6ceb7e2c37c3e4";
const WORKER: &str = "contracts/sources/wi-fabric-2-worker-execution-acceptance-matrix.yaml";
const AQ: &str = "contracts/sources/aq-baseline-integration-acceptance-matrix.yaml";
const FOUNDATION: &str = "crates/worldinterface-foundation/Cargo.toml";
const RUNTIME: &str = "crates/worldinterface-runtime/Cargo.toml";
const CHECKER: &str = "tools/wi-contract-check/Cargo.toml";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct TestRef {
    pub manifest: String,
    pub package: String,
    pub test: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub id: String,
    pub source_matrix: String,
    pub source_record_sha256: String,
    pub gate: String,
    pub owning_item: String,
    pub status: String,
    pub coverage_refs: Vec<String>,
    pub tests: Vec<TestRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registry {
    pub version: u32,
    pub cases: Vec<Case>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gate {
    pub version: u32,
    pub id: String,
    pub source_record_sha256: String,
    pub pass_criteria: Value,
    pub requires: Vec<Value>,
    pub required_cases: Vec<String>,
    pub closing_item: String,
    pub status: String,
    pub effective_on: String,
    pub historical_evidence: Vec<String>,
    pub requirement_bindings: Vec<Value>,
}
/// Counts eligible harness occurrences. A duplicate name is ambiguous evidence.
pub type TestIndex = BTreeMap<TestRef, usize>;
/// Executable identities retained for the campaign; duplicate references stay ambiguous.
pub type HarnessIndex = BTreeMap<TestRef, Vec<PathBuf>>;

/// serde_json's default map is key ordered, including recursively nested maps.
pub fn source_digest(record: &Value) -> Result<String> {
    Ok(hash(serde_json::to_vec(record)?))
}
fn cargo(root: &Path, args: &[&str]) -> Result<String> {
    let out = Command::new("timeout")
        .args(["--kill-after=10s", "1800s", "cargo", "--offline"])
        .current_dir(root)
        .args(args)
        .output()?;
    if !out.status.success() {
        return Err(
            format!("cargo {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr)).into()
        );
    }
    Ok(String::from_utf8(out.stdout)?)
}
fn listing(executable: &Path, ignored: bool) -> Result<BTreeSet<String>> {
    let mut cmd = Command::new(executable);
    cmd.args(["--list", "--format", "terse"]);
    if ignored {
        cmd.arg("--ignored");
    }
    let out = cmd.output()?;
    if !out.status.success() {
        return Err(format!("test listing failed: {}", executable.display()).into());
    }
    Ok(String::from_utf8(out.stdout)?
        .lines()
        .filter_map(|l| l.strip_suffix(": test").map(str::to_owned))
        .collect())
}
/// Inspect only routes reachable from the compiled target, with exact module names.
/// Compilation/listing decides cfg eligibility; syntax supplies the required test form.
fn source_tests(path: &Path) -> Result<BTreeSet<String>> {
    fn walk(
        path: &Path,
        prefix: &str,
        dir: &Path,
        seen: &mut BTreeSet<PathBuf>,
        out: &mut BTreeSet<String>,
    ) -> Result<()> {
        let path = path.canonicalize()?;
        if !seen.insert(path.clone()) {
            return Err("cyclic Rust module route in proof test discovery".into());
        }
        let ast = syn::parse_file(&fs::read_to_string(&path)?)?;
        items(&ast.items, prefix, dir, seen, out)?;
        seen.remove(&path);
        Ok(())
    }
    fn items(
        nodes: &[syn::Item],
        prefix: &str,
        dir: &Path,
        seen: &mut BTreeSet<PathBuf>,
        out: &mut BTreeSet<String>,
    ) -> Result<()> {
        for node in nodes {
            match node {
                syn::Item::Fn(f)
                    if f.attrs.iter().any(|a| {
                        let segments: Vec<_> =
                            a.path().segments.iter().map(|s| s.ident.to_string()).collect();
                        segments == ["test"] || segments == ["tokio", "test"]
                    }) =>
                {
                    out.insert(format!("{prefix}{}", f.sig.ident));
                }
                syn::Item::Mod(m) => {
                    let name = m.ident.to_string();
                    let prefix = format!("{prefix}{name}::");
                    if let Some((_, nodes)) = &m.content {
                        items(nodes, &prefix, &dir.join(&name), seen, out)?;
                    } else {
                        let explicit = m.attrs.iter().find_map(|a| {
                            if !a.path().is_ident("path") {
                                return None;
                            }
                            if let syn::Meta::NameValue(v) = &a.meta {
                                if let syn::Expr::Lit(l) = &v.value {
                                    if let syn::Lit::Str(s) = &l.lit {
                                        return Some(s.value());
                                    }
                                }
                            }
                            None
                        });
                        let module = explicit.map(|p| dir.join(p)).unwrap_or_else(|| {
                            let file = dir.join(format!("{name}.rs"));
                            if file.is_file() {
                                file
                            } else {
                                dir.join(&name).join("mod.rs")
                            }
                        });
                        // A disabled module need not exist. It cannot appear in the harness.
                        if module.is_file() {
                            let next = if module.file_name().is_some_and(|n| n == "mod.rs") {
                                module.parent().unwrap().to_owned()
                            } else {
                                module.with_extension("")
                            };
                            walk(&module, &prefix, &next, seen, out)?;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    let mut out = BTreeSet::new();
    walk(path, "", path.parent().ok_or("target parent missing")?, &mut BTreeSet::new(), &mut out)?;
    Ok(out)
}
/// Uses the adopted test configurations. --no-run avoids recursively running this suite.
pub fn discover(root: &Path) -> Result<TestIndex> {
    let mut index = TestIndex::new();
    for (manifest, flags) in
        [(FOUNDATION, vec!["--workspace", "--all-features"]), (RUNTIME, vec![]), (CHECKER, vec![])]
    {
        index.extend(discover_configuration(root, manifest, &flags)?);
    }
    Ok(index)
}
/// Public for small executable Cargo fixtures used by discovery regression tests.
pub fn discover_configuration(root: &Path, manifest: &str, flags: &[&str]) -> Result<TestIndex> {
    Ok(discover_harnesses(root, manifest, flags)?
        .into_iter()
        .map(|(t, paths)| (t, paths.len()))
        .collect())
}
pub fn discover_harnesses(root: &Path, manifest: &str, flags: &[&str]) -> Result<HarnessIndex> {
    let mut args =
        vec!["metadata", "--locked", "--manifest-path", manifest, "--format-version", "1"];
    if flags.contains(&"--all-features") {
        args.push("--all-features");
    }
    let metadata: Value = serde_json::from_str(&cargo(root, &args)?)?;
    let selected = if flags.contains(&"--workspace") {
        "workspace_members"
    } else {
        "workspace_default_members"
    };
    let members = array(&metadata, selected)?;
    let packages: BTreeMap<_, _> = array(&metadata, "packages")?
        .iter()
        .filter(|p| members.contains(&p["id"]))
        .map(|p| (p["id"].as_str().unwrap_or("").to_owned(), p))
        .collect();
    let mut args =
        vec!["test", "--locked", "--manifest-path", manifest, "--no-run", "--message-format=json"];
    args.extend(flags);
    // runtime-test delegates these harnesses to the campaign. Explicit selection
    // builds test=false targets without making Cargo execute them a second time.
    let campaign_owned = manifest == RUNTIME;
    if campaign_owned {
        args.extend(["--lib", "--test", "*"]);
        validate_runtime_ownership(&packages)?;
    }
    let output = cargo(root, &args)?;
    let mut index = HarnessIndex::new();
    let mut executables = BTreeSet::new();
    for line in output.lines() {
        let a: Value = serde_json::from_str(line)?;
        if a["reason"] != "compiler-artifact" || a["profile"]["test"] != true {
            continue;
        }
        let Some(executable) = a["executable"].as_str() else {
            continue;
        };
        let Some(package) = packages.get(a["package_id"].as_str().unwrap_or("")) else {
            continue;
        };
        if !executables.insert(executable.to_owned()) {
            continue;
        }
        let declared = array(package, "targets")?.iter().any(|t| {
            (t["test"] == true
                || (campaign_owned && t["kind"] == json!(["test"]))
                || (campaign_owned && t["kind"] == json!(["lib"])))
                && t["name"] == a["target"]["name"]
                && t["src_path"] == a["target"]["src_path"]
        });
        if !declared {
            continue;
        }
        let syntax = source_tests(Path::new(string(&a["target"], "src_path")?))?;
        let ignored = listing(Path::new(executable), true)?;
        for test in listing(Path::new(executable), false)?.difference(&ignored) {
            if syntax.contains(test) {
                index
                    .entry(TestRef {
                        manifest: manifest.into(),
                        package: string(package, "name")?.into(),
                        test: test.clone(),
                    })
                    .or_default()
                    .push(PathBuf::from(executable));
            }
        }
    }
    if campaign_owned {
        let entry = TestRef {
            manifest: RUNTIME.into(),
            package: "worldinterface-runtime".into(),
            test: crate::worker_campaign::ENTRY.into(),
        };
        if index.get(&entry).map(Vec::len) != Some(1) {
            return Err("missing, ignored or ambiguous adopted campaign entry point".into());
        }
    }
    Ok(index)
}

/// Prevent a new auto-discovered target from silently executing twice, and keep
/// the campaign mandatory in the adopted default Cargo invocation.
fn validate_runtime_ownership(packages: &BTreeMap<String, &Value>) -> Result<()> {
    let package = packages
        .values()
        .find(|p| p["name"] == "worldinterface-runtime")
        .ok_or("missing runtime package")?;
    let mut entry = false;
    for target in array(package, "targets")? {
        if target["kind"] == json!(["test"]) {
            let is_entry = target["name"] == "worker_campaign";
            if target["test"] != is_entry || target.get("required-features").is_some() {
                return Err("runtime harness must have exactly one campaign owner".into());
            }
            entry |= is_entry;
        } else if target["kind"] == json!(["lib"]) && target["test"] != false {
            return Err("runtime unit tests must be owned by the campaign".into());
        }
    }
    if !entry {
        return Err("missing adopted runtime campaign entry point".into());
    }
    Ok(())
}

fn allocation(id: &str, source: &Value) -> Result<(String, String)> {
    let n: usize = id.rsplit('-').next().ok_or("case number missing")?.parse()?;
    let (gate, item) = if id.starts_with("WP-") {
        let item = match n {
            1..=8 => 1,
            9..=14 => 2,
            15..=16 => 4,
            17..=26 => 10,
            27 | 28 | 31 => 11,
            29 | 30 => 14,
            32 => 12,
            33 | 34 => 15,
            _ => return Err("out-of-scope WP case".into()),
        };
        (string(source, "checkpoint")?.to_owned(), item)
    } else {
        let item = match n {
            1 | 15 | 16 => 1,
            2..=4 => 2,
            5 | 10 | 13 => 7,
            6 | 7 | 9 | 22 => 3,
            8 | 11 => 8,
            12 | 14 | 20 | 23 => 5,
            19 => 14,
            21 => 10,
            _ => return Err("out-of-scope AB case".into()),
        };
        (
            match n {
                19 => "WI-WORKER-G4",
                21 => "WI-WORKER-G3",
                _ => string(source, "capability_gate")?,
            }
            .to_owned(),
            item,
        )
    };
    Ok((gate, format!("WIP-{item:02}")))
}
fn coverage(root: &Path, id: &str) -> Result<(Vec<String>, BTreeSet<TestRef>)> {
    let mut refs = vec![];
    let mut tests = BTreeSet::new();
    let mut add = |path: &str, manifest: &str, package: &str, names: Vec<String>| {
        if !refs.iter().any(|r| r == path) {
            refs.push(path.to_owned());
        }
        for test in names {
            tests.insert(TestRef { manifest: manifest.into(), package: package.into(), test });
        }
    };
    let worker_path = "conformance/wi-fabric-2/worker/coverage.json";
    for c in array(&read(&root.join(worker_path))?, "cases")? {
        if c["id"] == id {
            add(worker_path, FOUNDATION, string(c, "package")?, vec![string(c, "test")?.into()]);
        }
    }
    for (path, case, manifest, package, field) in [
        (
            "conformance/wi-fabric-2/domain/coverage.json",
            "WP-002",
            FOUNDATION,
            "worldinterface-protocol",
            "checks",
        ),
        ("contracts/wi10-obligations.json", "WP-007", CHECKER, "wi-contract-check", "tests"),
    ] {
        if id == case {
            let v = read(&root.join(path))?;
            let ob = array(&v, "obligations")?
                .iter()
                .find(|o| o["id"] == id)
                .ok_or("missing case coverage obligation")?;
            add(
                path,
                manifest,
                package,
                array(ob, field)?
                    .iter()
                    .map(|t| t.as_str().map(str::to_owned).ok_or("invalid coverage test"))
                    .collect::<std::result::Result<_, _>>()?,
            );
        }
    }
    for path in [
        "conformance/wi-fabric-2/egress/coverage.json",
        "conformance/wi-fabric-2/receipts/coverage.json",
    ] {
        let v = read(&root.join(path))?;
        if let Some(names) = v["assigned_cases"].get(id) {
            let names = if let Some(s) = names.as_str() {
                vec![s.to_owned()]
            } else {
                names
                    .as_array()
                    .ok_or("invalid coverage names")?
                    .iter()
                    .map(|s| s.as_str().map(str::to_owned).ok_or("invalid test"))
                    .collect::<std::result::Result<_, _>>()?
            };
            for name in names {
                let ob = array(&v, "obligations")?
                    .iter()
                    .find(|o| o["test"] == name)
                    .ok_or("assigned test lacks obligation")?;
                let package = string(ob, "package")?;
                let package = if package.starts_with("worldinterface-") {
                    package.to_owned()
                } else {
                    format!("worldinterface-{package}")
                };
                add(path, FOUNDATION, &package, vec![name]);
            }
        }
    }
    let path = "conformance/wi-fabric-2/integration/fixtures.json";
    let v = read(&root.join(path))?;
    if let Some(c) = v["cases"].get(id) {
        add(
            path,
            RUNTIME,
            "worldinterface-runtime",
            array(c, "required_tests")?
                .iter()
                .map(|s| s.as_str().map(str::to_owned).ok_or("invalid test"))
                .collect::<std::result::Result<_, _>>()?,
        );
    }
    let path = "conformance/wi-fabric-2/completion/coverage.json";
    if root.join(path).is_file() {
        let v = read(&root.join(path))?;
        if let Some(names) = v["assigned_cases"].get(id) {
            add(
                path,
                RUNTIME,
                "worldinterface-runtime",
                names
                    .as_array()
                    .ok_or("invalid completion case")?
                    .iter()
                    .map(|n| n.as_str().map(str::to_owned).ok_or("invalid completion test"))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            );
        }
    }
    let path = "conformance/wi-fabric-2/connectors/coverage.json";
    let v = read(&root.join(path))?;
    if let Some(names) = v["assigned_cases"].get(id) {
        add(path, RUNTIME, "worldinterface-runtime", serde_json::from_value(names.clone())?);
    }
    let path = "conformance/wi-fabric-2/flow/integration-coverage.json";
    let v = read(&root.join(path))?;
    if let Some(names) = v["assigned_cases"].get(id) {
        add(path, RUNTIME, "worldinterface-runtime", serde_json::from_value(names.clone())?);
    }
    let path = "conformance/wi-fabric-2/continuations/coverage.json";
    let v = read(&root.join(path))?;
    if let Some(names) = v["assigned_cases"].get(id) {
        add(path, RUNTIME, "worldinterface-runtime", serde_json::from_value(names.clone())?);
    }
    let path = "conformance/wi-fabric-2/worker/lifecycle-coverage.json";
    let v = read(&root.join(path))?;
    if let Some(names) = v["assigned_cases"].get(id) {
        add(path, RUNTIME, "worldinterface-runtime", serde_json::from_value(names.clone())?);
    }
    let path = "conformance/wi-fabric-2/worker/artifact-seam-coverage.json";
    let v = read(&root.join(path))?;
    if let Some(names) = v["assigned_cases"].get(id) {
        add(path, RUNTIME, "worldinterface-runtime", serde_json::from_value(names.clone())?);
    }
    let path = "conformance/wi-fabric-2/worker/catalog-coverage.json";
    let v = read(&root.join(path))?;
    if let Some(names) = v["assigned_cases"].get(id) {
        add(path, RUNTIME, "worldinterface-runtime", serde_json::from_value(names.clone())?);
    }
    let path = "conformance/wi-fabric-2/worker/embedded-coverage.json";
    let v = read(&root.join(path))?;
    if let Some(references) = v["assigned_cases"].get(id) {
        let references: Vec<TestRef> = serde_json::from_value(references.clone())?;
        refs.push(path.to_string());
        tests.extend(references);
    }
    let path = "conformance/wi-fabric-2/worker/campaign-coverage.json";
    let v = read(&root.join(path))?;
    if let Some(references) = v["assigned_cases"].get(id) {
        refs.push(path.to_string());
        tests.extend(serde_json::from_value::<Vec<TestRef>>(references.clone())?);
    }
    refs.sort();
    Ok((refs, tests))
}
fn binding(root: &Path, requirement: &Value) -> Result<Value> {
    let id = string(requirement, "id")?;
    Ok(match id {
        "STACK-PLAN-ACCEPTED" => {
            json!({"kind":"plan-import","plan_version":"9212604c-4873-4cd5-b492-1ee3141b8b0b","date":"2026-10-13","historical_acceptance":"2026-10-01"})
        }
        "AQ-BASELINE-ACCEPTED" => {
            json!({"kind":"published-aq","historical_submission":"7b060900","historical_acceptance":"2026-09-21","contract":"contracts/aq-integration-target.json","record":"published_release","check":"runtime-aq-graph"})
        }
        "wi/WI-02/domain" => {
            json!({"kind":"historical-verified-slice","revisions":["9cbdb13","d8e1a49"]})
        }
        "wi/WI-09/domain" => json!({"kind":"historical-verified-slice","revisions":["d5c4ad1"]}),
        "wi/WI-10/domain" => json!({"kind":"historical-verified-slice","revisions":["f364bad"]}),
        "wi/WI-04/domain" => json!({"kind":"historical-verified-slice","revisions":["9e92972"]}),
        "wi/WI-05/domain" => json!({"kind":"historical-verified-slice","revisions":["afa130d"]}),
        id if id.starts_with("WI-ADR-") => {
            let q = read(&root.join("docs/adr/queue.json"))?;
            let a = q
                .as_array()
                .ok_or("ADR queue missing")?
                .iter()
                .find(|a| a["id"] == id)
                .ok_or("ADR missing")?;
            json!({"kind":"accepted-adr","approval_file":a["approval_file"],"approval_sha256":a["approval_sha256"]})
        }
        id if requirement["kind"] == "slice" => {
            let items = match id {
                "wi/WI-02/integration" => vec!["WIP-01"],
                "wi/WI-03/integration" => vec!["WIP-03"],
                "wi/WI-04/integration" => vec!["WIP-02"],
                "wi/WI-05/integration" => vec!["WIP-04"],
                "wi/WI-06/integration" => vec!["WIP-05"],
                "wi/WI-07/integration" => vec!["WIP-07"],
                "wi/WI-08/integration" => vec!["WIP-08"],
                "wi/WI-09/process-provider" => vec!["WIP-09", "WIP-10", "WIP-11"],
                "wi/WI-10/process-catalog" => vec!["WIP-12"],
                "wi/WI-11/embedded-runtime" => vec!["WIP-14"],
                "wi/WI-13/worker-conformance" => vec!["WIP-15"],
                "wi/WI-14/worker-handoff" => vec!["WIP-15", "WIP-16"],
                _ => return Err(format!("unmapped slice {id}").into()),
            };
            json!({"kind":"reviewed-item-scope","covering_items":items,"effective_on":"reviewed-merge-with-adopted-checks"})
        }
        _ => json!({"kind":"closed-gate","record":format!("{BASE}/gates/{id}.json")}),
    })
}
pub fn prerequisite_cycles(gates: &[Gate]) -> bool {
    fn visit<'a>(
        id: &'a str,
        gates: &'a [Gate],
        active: &mut BTreeSet<&'a str>,
        done: &mut BTreeSet<&'a str>,
    ) -> bool {
        if done.contains(id) {
            return false;
        }
        if !active.insert(id) {
            return true;
        }
        if let Some(g) = gates.iter().find(|g| g.id == id) {
            for r in &g.requires {
                if let Some(next) = r["id"].as_str() {
                    if gates.iter().any(|g| g.id == next) && visit(next, gates, active, done) {
                        return true;
                    }
                }
            }
        }
        active.remove(id);
        done.insert(id);
        false
    }
    gates.iter().any(|g| visit(&g.id, gates, &mut BTreeSet::new(), &mut BTreeSet::new()))
}
/// Pure validation against a separately discovered index; mutation fixtures never run test suites.
pub fn validate(
    root: &Path,
    registry: &Registry,
    gates: &[Gate],
    tests: &TestIndex,
) -> Result<Vec<Finding>> {
    let mut f = vec![];
    let mut reject = |rule: &str, id: &str, message: &str| {
        f.push(finding(
            rule,
            BASE,
            id,
            "WI-WORKER-PROOF-1; pinned source matrices and retired map",
            message,
        ))
    };
    if registry.version != 1 {
        reject("PROOF-VERSION", "cases", "version 1 required");
    }
    if hash(fs::read(root.join(EXPORT))?) != EXPORT_SHA256 {
        reject("PROOF-EXPORT", EXPORT, "authoritative export bytes changed");
    }
    let export = read(&root.join(EXPORT))?;
    let mut sources = BTreeMap::new();
    for (path, pin) in [
        (WORKER, "9c11d92d056b0411ed4af00a8f076182d95c500acd6e360f42a3a0bdd8aec2eb"),
        (AQ, "3ed55cff70cbc603e6a44591b0f73aabf58b19d81cff28cf95baa08429ad6f4a"),
    ] {
        if hash(fs::read(root.join(path))?) != pin {
            reject(
                "PROOF-SOURCE",
                path,
                "normative matrix bytes and result flags must remain unchanged",
            );
        }
        for source in array(&read(&root.join(path))?, "cases")? {
            let id = string(source, "id")?;
            if let Ok((gate, item)) = allocation(id, source) {
                sources.insert(id.to_owned(), (path, source.clone(), gate, item));
            }
        }
    }
    let mut seen = BTreeSet::new();
    for c in &registry.cases {
        if !seen.insert(c.id.clone()) {
            reject("PROOF-CASE", &c.id, "duplicate case");
        }
        let Some((matrix, source, gate, item)) = sources.get(&c.id) else {
            reject("PROOF-CASE", &c.id, "unknown or out-of-scope case");
            continue;
        };
        if c.source_matrix != *matrix || c.source_record_sha256 != source_digest(source)? {
            reject("PROOF-DIGEST", &c.id, "source-record identity/digest mismatch");
        }
        if c.gate != *gate || c.owning_item != *item {
            reject("PROOF-ALLOCATION", &c.id, "gate or owning item differs from plan allocation");
        }
        if !["planned", "implemented"].contains(&c.status.as_str()) {
            reject("PROOF-STATUS", &c.id, "invalid case status");
        }
        let fixtures = read(&root.join("conformance/wi-fabric-2/integration/fixtures.json"))?;
        if let Some(covered_case) = fixtures["cases"].get(&c.id) {
            if covered_case["source_record_sha256"] != source_digest(source)? {
                reject(
                    "PROOF-COVERAGE",
                    &c.id,
                    "scope fixture source-record digest differs from normative matrix",
                );
            }
        }
        let (refs, covered) = coverage(root, &c.id)?;
        if c.coverage_refs != refs {
            reject(
                "PROOF-COVERAGE",
                &c.id,
                "coverage references differ from authoritative scope files",
            );
        }
        let unique: BTreeSet<_> = c.tests.iter().cloned().collect();
        if unique.len() != c.tests.len() {
            reject("PROOF-TEST", &c.id, "duplicate test reference");
        }
        if c.status == "implemented" {
            if c.tests.is_empty() {
                reject("PROOF-TEST", &c.id, "implemented case requires executable test evidence");
            }
            if !covered.is_subset(&unique) {
                reject("PROOF-COVERAGE", &c.id, "required scope tests omitted or misplaced");
            }
            for t in &c.tests {
                if tests.get(t) != Some(&1) {
                    reject("PROOF-TEST", &c.id, "test missing, ignored, ambiguous, unreachable, disabled or outside adopted package/configuration");
                }
            }
        } else if !c.tests.is_empty() {
            reject("PROOF-STATUS", &c.id, "planned cases must not claim executable evidence");
        }
    }
    if seen != sources.keys().cloned().collect() {
        reject("PROOF-CASE", "registry", "register exactly all 55 in-scope cases");
    }
    let source_gates = array(&export, "checkpoints")?;
    let expected_ids: BTreeSet<_> =
        source_gates.iter().map(|g| string(g, "id")).collect::<Result<_>>()?;
    let mut ids = BTreeSet::new();
    for g in gates {
        if !ids.insert(g.id.as_str()) {
            reject("PROOF-GATE", &g.id, "duplicate gate");
        }
        let Some(source) = source_gates.iter().find(|s| s["id"] == g.id) else {
            reject("PROOF-GATE", &g.id, "unknown gate");
            continue;
        };
        let closing = match g.id.as_str() {
            "WI-WORKER-G1" | "WI-AQ-G1" => "WIP-01",
            "WI-WORKER-G2" | "WI-AQ-G2" => "WIP-04",
            "WI-WORKER-G3" => "WIP-10",
            "WI-WORKER-G4" | "WI-EMBEDDED-WORKER-PROOF-1" => "WIP-16",
            "WI-AQ-G3" | "WI-EMBEDDED-CORE-1" => "WIP-08",
            "WI-AQ-G4" => "WIP-07",
            _ => return Err("unknown exported gate".into()),
        };
        let historical: Vec<&str> = match g.id.as_str() {
            "WI-WORKER-G1" => vec!["264d791e", "b0f3e948"],
            "WI-AQ-G1" => vec!["df7bc4a3"],
            "WI-AQ-G2" => vec!["7cdecc06", "7ed4b60e"],
            "WI-AQ-G3" => vec!["300d033f"],
            _ => vec![],
        };
        if g.version != 1
            || g.closing_item != closing
            || g.effective_on != "reviewed-merge-with-adopted-checks"
            || g.historical_evidence != historical
            || !["open", "closed"].contains(&g.status.as_str())
        {
            reject(
                "PROOF-GATE",
                &g.id,
                "version, status, closing item, merge effectiveness or historical evidence changed",
            );
        }
        if g.source_record_sha256 != source_digest(source)?
            || g.pass_criteria != source["pass_criteria"]
            || json!(g.requires) != source["requires"]
        {
            reject(
                "PROOF-GATE-SOURCE",
                &g.id,
                "gate criteria or complete prerequisites differ from authoritative export",
            );
        }
        let required: BTreeSet<String> = sources
            .iter()
            .filter(|(id, (_, _, gate, _))| match g.id.as_str() {
                "WI-EMBEDDED-WORKER-PROOF-1" => true,
                "WI-EMBEDDED-CORE-1" => {
                    id.starts_with("AB-") && !["AB-WI-019", "AB-WI-021"].contains(&id.as_str())
                }
                _ => *gate == g.id,
            })
            .map(|(id, _)| id.clone())
            .collect();
        if g.required_cases != required.iter().cloned().collect::<Vec<_>>() {
            reject(
                "PROOF-GATE-CASES",
                &g.id,
                "required cases must match independently derived source/plan allocation",
            );
        }
        let expected_bindings: Vec<_> = array(source, "requires")?
            .iter()
            .map(|r| Ok(json!({"requirement":r,"binding":binding(root,r)?})))
            .collect::<Result<_>>()?;
        if g.requirement_bindings != expected_bindings {
            reject("PROOF-BINDING", &g.id, "off-map requirement binding missing or changed");
        }
        if g.status == "closed" {
            for id in &required {
                if !registry.cases.iter().any(|c| c.id == *id && c.status == "implemented") {
                    reject("PROOF-CLOSED-CASE", &g.id, "closed gate has an open case");
                }
            }
            for r in array(source, "requires")? {
                let id = string(r, "id")?;
                if expected_ids.contains(id)
                    && !gates.iter().any(|g| g.id == id && g.status == "closed")
                {
                    reject(
                        "PROOF-CLOSED-PREREQUISITE",
                        &g.id,
                        "closed gate has an open prerequisite",
                    );
                }
                let queue = read(&root.join("docs/adr/queue.json"))?;
                let accepted = queue.as_array().is_some_and(|q| {
                    q.iter().any(|a| {
                        a["id"] == id && a["status"] == "accepted" && a["coverage"] == "full"
                    })
                });
                if id.starts_with("WI-ADR-")
                    && (!accepted || !contracts::adr_coverage(root, id, "", "merge", true)?)
                {
                    reject("PROOF-ADR", &g.id, "closed gate requires pinned full ADR approval");
                }
            }
        }
    }
    if ids != expected_ids {
        reject("PROOF-GATE", "registry", "exactly ten exported gates required");
    }
    if prerequisite_cycles(gates) {
        reject("PROOF-CYCLE", "gates", "prerequisite cycle");
    }
    Ok(f)
}
pub fn load(root: &Path) -> Result<(Registry, Vec<Gate>)> {
    let registry = serde_json::from_value(read(&root.join(format!("{BASE}/cases.json")))?)?;
    let mut gates = vec![];
    for path in crate::files(&root.join(format!("{BASE}/gates")))? {
        let gate: Gate = serde_json::from_value(read(&path)?)?;
        if path != root.join(format!("{BASE}/gates/{}.json", gate.id)) {
            return Err("misplaced gate record".into());
        }
        gates.push(gate);
    }
    Ok((registry, gates))
}
pub fn check(root: &Path) -> Result<Vec<Finding>> {
    let (registry, gates) = load(root)?;
    let tests = discover(root)?;
    let mut findings = validate(root, &registry, &gates, &tests)?;
    findings.extend(validate_completion(root, &tests)?);
    findings.extend(validate_connectors(root, &tests)?);
    findings.extend(validate_flow(root, &tests)?);
    findings.extend(validate_control_plane(root, &tests)?);
    findings.extend(validate_flow_integration(root, &tests)?);
    findings.extend(validate_continuations(root, &tests)?);
    findings.extend(validate_worker_lifecycle(root, &tests)?);
    findings.extend(validate_worker_catalog(root, &tests)?);
    findings.extend(validate_embedded(root, &tests)?);
    if let Err(e) = crate::worker_campaign::validate_coverage(root, &registry, &tests) {
        findings.push(finding(
            "PROOF-CAMPAIGN",
            crate::worker_campaign::COVERAGE,
            "coverage",
            "WIP-15",
            &e.to_string(),
        ));
    }
    Ok(findings)
}

/// The whole WI-05 parent exit gate needs executable evidence beyond its WP cases.
pub fn validate_completion(root: &Path, tests: &TestIndex) -> Result<Vec<Finding>> {
    let path = "conformance/wi-fabric-2/completion/coverage.json";
    let v = read(&root.join(path))?;
    let obligations: BTreeMap<String, Vec<TestRef>> =
        serde_json::from_value(v["required_obligations"].clone())?;
    let required = BTreeSet::from([
        "canonical-replay",
        "authoritative-source-resource",
        "publication-before-completion",
        "crash-convergence",
        "redacted-inspection",
        "signature-provenance",
        "campaign-noninterference",
        "no-success-quality-claims",
        "material-differences",
    ]);
    let mut findings = Vec::new();
    if v["scope"] != "wi/WI-05/integration"
        || obligations.keys().map(String::as_str).collect::<BTreeSet<_>>() != required
    {
        findings.push(finding(
            "PROOF-WI05",
            path,
            "obligations",
            "WI-05 required tests",
            "complete WI-05 obligation matrix required",
        ));
    }
    for (obligation, references) in obligations {
        if references.is_empty() || references.iter().any(|t| tests.get(t) != Some(&1)) {
            findings.push(finding(
                "PROOF-WI05",
                path,
                &obligation,
                "WI-05 required tests",
                "test missing, ignored, ambiguous or outside adopted configuration",
            ));
        }
    }
    Ok(findings)
}

/// WI-06 producer-core evidence remains separate from full-parent adapter obligations.
pub fn validate_connectors(root: &Path, tests: &TestIndex) -> Result<Vec<Finding>> {
    let path = "conformance/wi-fabric-2/connectors/coverage.json";
    let v = read(&root.join(path))?;
    let obligations: BTreeMap<String, Vec<TestRef>> =
        serde_json::from_value(v["required_obligations"].clone())?;
    let required = BTreeSet::from([
        "descriptor-conformance",
        "stable-provider-key",
        "executor-classification",
        "external-key-crash",
        "nonrepeatable-ambiguity",
        "exact-envelope",
        "bounded-reconciliation",
        "reconciliation-crash",
        "replay-and-inspection",
    ]);
    let deferred: BTreeSet<String> = serde_json::from_value(v["deferred_full_parent"].clone())?;
    let mut findings = Vec::new();
    if v["version"] != 1
        || v["scope"] != "wi/WI-06/integration:embedded-proof"
        || obligations.keys().map(String::as_str).collect::<BTreeSet<_>>() != required
        || deferred
            != BTreeSet::from([
                "filesystem-reference".into(),
                "http-reference".into(),
                "semantic-communication-reference".into(),
                "wasm-alignment".into(),
                "legacy-deletion".into(),
                "worker-supervisor-integration".into(),
            ])
    {
        findings.push(finding(
            "PROOF-WI06",
            path,
            "scope",
            "WIP-05 embedded-proof",
            "complete core and deferred-parent obligation matrix required",
        ));
    }
    for (obligation, references) in obligations {
        if references.is_empty()
            || references.iter().any(|t| {
                t.manifest != RUNTIME
                    || t.package != "worldinterface-runtime"
                    || tests.get(t) != Some(&1)
            })
        {
            findings.push(finding(
                "PROOF-WI06",
                path,
                &obligation,
                "WIP-05 core required tests",
                "exact non-ignored runtime test required",
            ));
        }
    }
    Ok(findings)
}

/// Complete pure WI-07 domain proof. No acceptance cases belong to WIP-06.
pub fn validate_flow(root: &Path, tests: &TestIndex) -> Result<Vec<Finding>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Coverage {
        version: u32,
        scope: String,
        upstream_conformance: bool,
        runtime_conformance: bool,
        assigned_cases: BTreeMap<String, Value>,
        required_obligations: BTreeMap<String, Vec<TestRef>>,
    }
    let path = "conformance/wi-fabric-2/flow/coverage.json";
    let coverage: Coverage = serde_json::from_value(read(&root.join(path))?)?;
    let required = BTreeSet::from([
        "cycles",
        "unreachable",
        "graph-identities",
        "schema-compatibility",
        "closed-purity",
        "authority-ceilings",
        "ambiguous-resources",
        "bounded-fan-out",
        "wait-rejection",
        "wait-sequencing",
        "direct-equivalence",
        "canonical-vector",
        "hash-normalization",
        "strict-recovery",
        "snapshot-drift",
        "paired-authority",
        "deterministic-replay",
        "branch-resource-scope",
        "branch-value-availability",
        "profile-bounds",
        "immutable-revision",
        "artifact-and-schema-profile",
        "execution-field-tampering",
        "registry-compatibility",
    ]);
    let mut findings = Vec::new();
    if coverage.version != 1
        || coverage.scope != "wi/WI-07/domain"
        || coverage.upstream_conformance
        || coverage.runtime_conformance
        || !coverage.assigned_cases.is_empty()
        || coverage.required_obligations.keys().map(String::as_str).collect::<BTreeSet<_>>()
            != required
    {
        findings.push(finding(
            "PROOF-WI07",
            path,
            "scope",
            "WIP-06 / WI-ADR-014",
            "complete pure-domain obligation matrix required; no runtime or case claim",
        ));
    }
    for (obligation, references) in coverage.required_obligations {
        if references.is_empty()
            || references.iter().collect::<BTreeSet<_>>().len() != references.len()
            || references.iter().any(|t| {
                t.manifest != FOUNDATION
                    || ![
                        "worldinterface-domain",
                        "worldinterface-protocol",
                        "worldinterface-capability",
                    ]
                    .contains(&t.package.as_str())
                    || tests.get(t) != Some(&1)
            })
        {
            findings.push(finding(
                "PROOF-WI07",
                path,
                &obligation,
                "WIP-06 required tests",
                "exact non-ignored unambiguous foundation test required",
            ));
        }
    }
    Ok(findings)
}

pub fn validate_control_plane(root: &Path, tests: &TestIndex) -> Result<Vec<Finding>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Coverage {
        version: u32,
        scope: String,
        upstream_conformance: bool,
        runtime_conformance: bool,
        assigned_cases: BTreeMap<String, Value>,
        required_obligations: BTreeMap<String, Vec<TestRef>>,
    }
    let path = "conformance/wi-fabric-2/control-plane/coverage.json";
    let coverage: Coverage = serde_json::from_value(read(&root.join(path))?)?;
    let required = BTreeSet::from([
        "explicit-profile",
        "closed-wire",
        "server-owned-agency",
        "tenant-organization-scoping",
        "no-ambient-roles",
        "attributed-administration",
        "durable-revisions-use-accounting",
        "concurrent-administration",
        "exact-credential-scope",
        "fresh-authorization-expiry",
        "durable-issuance",
        "custody-uncertainty",
        "rotation-revocation",
        "replay-integrity",
        "secret-exclusion",
    ]);
    let mut findings = Vec::new();
    if coverage.version != 1
        || coverage.scope != "wi/WI-11/domain"
        || coverage.upstream_conformance
        || coverage.runtime_conformance
        || !coverage.assigned_cases.is_empty()
        || coverage.required_obligations.keys().map(String::as_str).collect::<BTreeSet<_>>()
            != required
    {
        findings.push(finding(
            "PROOF-WI11",
            path,
            "scope",
            "WIP-13 / WI-ADR-018",
            "complete pure-domain obligation matrix required; no runtime or case claim",
        ));
    }
    for (obligation, references) in coverage.required_obligations {
        if references.is_empty()
            || references.iter().collect::<BTreeSet<_>>().len() != references.len()
            || references.iter().any(|t| {
                t.manifest != FOUNDATION
                    || ![
                        "worldinterface-domain",
                        "worldinterface-protocol",
                        "worldinterface-storage",
                    ]
                    .contains(&t.package.as_str())
                    || tests.get(t) != Some(&1)
            })
        {
            findings.push(finding(
                "PROOF-WI11",
                path,
                &obligation,
                "WIP-13 required tests",
                "exact non-ignored unambiguous foundation test required",
            ));
        }
    }
    Ok(findings)
}

/// WI-07 runtime evidence is separate from the sealed pure compiler proof.
pub fn validate_flow_integration(root: &Path, tests: &TestIndex) -> Result<Vec<Finding>> {
    let path = "conformance/wi-fabric-2/flow/integration-coverage.json";
    let v = read(&root.join(path))?;
    let obligations: BTreeMap<String, Vec<TestRef>> =
        serde_json::from_value(v["required_obligations"].clone())?;
    let required = BTreeSet::from([
        "shared-execution",
        "pinned-admission",
        "immutable-versions",
        "authority-and-purity",
        "bounded-children",
        "compound-recovery",
        "binding-readiness",
        "domain-acceptance",
        "branch-receipts",
        "storage-replay-and-artifacts",
        "deadline-race",
        "production-wait-profile",
    ]);
    let assigned: BTreeMap<String, Vec<String>> =
        serde_json::from_value(v["assigned_cases"].clone())?;
    let mut findings = Vec::new();
    if v["version"] != 1
        || v["scope"] != "wi/WI-07/integration:embedded-proof"
        || v["upstream_conformance"] != false
        || v["runtime_profile"] != "aq-0.3.0-wip07-flat-required-children"
        || obligations.keys().map(String::as_str).collect::<BTreeSet<_>>() != required
        || assigned.keys().map(String::as_str).collect::<BTreeSet<_>>()
            != BTreeSet::from(["AB-WI-005", "AB-WI-010", "AB-WI-013"])
    {
        findings.push(finding(
            "PROOF-WI07-RUNTIME",
            path,
            "scope",
            "WIP-07",
            "complete runtime obligation matrix and exact owned cases required",
        ));
    }
    for (obligation, refs) in &obligations {
        if refs.is_empty()
            || refs.iter().collect::<BTreeSet<_>>().len() != refs.len()
            || refs.iter().any(|t| {
                !((t.manifest == RUNTIME && t.package == "worldinterface-runtime")
                    || (t.manifest == FOUNDATION && t.package == "worldinterface-storage"))
                    || tests.get(t) != Some(&1)
            })
        {
            findings.push(finding(
                "PROOF-WI07-RUNTIME",
                path,
                obligation,
                "WIP-07 required tests",
                "exact non-ignored unambiguous adopted runtime/storage test required",
            ));
        }
    }
    for (case, names) in assigned {
        let fixtures = read(&root.join("conformance/wi-fabric-2/integration/fixtures.json"))?;
        if names.is_empty()
            || serde_json::to_value(&names)? != fixtures["cases"][&case]["required_tests"]
            || names.iter().any(|name| {
                !obligations.values().flatten().any(|t| t.test == *name && t.manifest == RUNTIME)
            })
        {
            findings.push(finding("PROOF-WI07-RUNTIME", path, &case, "WIP-07 case matrix", "case evidence must agree with authoritative integration fixtures and runtime obligations"));
        }
    }
    Ok(findings)
}

/// Complete WIP-08 obligations with exact adopted, non-ignored test identities.
pub fn validate_continuations(root: &Path, tests: &TestIndex) -> Result<Vec<Finding>> {
    let path = "conformance/wi-fabric-2/continuations/coverage.json";
    let v = read(&root.join(path))?;
    let obligations: BTreeMap<String, Vec<TestRef>> =
        serde_json::from_value(v["required_obligations"].clone())?;
    let assigned: BTreeMap<String, Vec<String>> =
        serde_json::from_value(v["assigned_cases"].clone())?;
    let required = BTreeSet::from([
        "source-outbox-atomicity",
        "early-late-duplicate-fanout",
        "stable-admission-backpressure",
        "cursor-and-generation",
        "physical-attempt-release",
        "resume-idempotency",
        "races",
        "process-death-and-paired-restore",
        "projection-replay",
        "current-authorization",
    ]);
    let mut f = Vec::new();
    if v["version"] != 1
        || v["scope"] != "wi/WI-08/integration:embedded-proof"
        || v["upstream_conformance"] != false
        || v["runtime_profile"] != "aq-0.3.0-durable-continuations-1"
        || obligations.keys().map(String::as_str).collect::<BTreeSet<_>>() != required
        || assigned.keys().map(String::as_str).collect::<BTreeSet<_>>()
            != BTreeSet::from(["AB-WI-008", "AB-WI-011"])
    {
        f.push(finding(
            "PROOF-WI08",
            path,
            "scope",
            "WIP-08",
            "complete continuation obligations and exact owned cases required",
        ));
    }
    for (name, refs) in &obligations {
        if refs.is_empty()
            || refs.iter().collect::<BTreeSet<_>>().len() != refs.len()
            || refs.iter().any(|t| {
                !((t.manifest == RUNTIME && t.package == "worldinterface-runtime")
                    || (t.manifest == FOUNDATION && t.package == "worldinterface-storage"))
                    || tests.get(t) != Some(&1)
            })
        {
            f.push(finding(
                "PROOF-WI08",
                path,
                name,
                "WIP-08",
                "exact non-ignored adopted runtime/storage tests required",
            ));
        }
    }
    for producer in [
        "provider_callback_commits_an_authoritative_record_and_outbox_without_waiter",
        "worker_intake_conflicts_scope_and_sequence_are_durable",
        "ingress_ready_and_acknowledged_commit_source_outbox_atomically_and_wake_flows",
        "reconciliation_commits_source_outbox_atomically_and_wakes_flow",
        "flow_child_commits_source_outbox_atomically_and_wakes_flow",
    ] {
        if !obligations.get("source-outbox-atomicity").is_some_and(|refs| {
            refs.iter().any(|t| {
                t.manifest == RUNTIME && t.package == "worldinterface-runtime" && t.test == producer
            })
        }) {
            f.push(finding(
                "PROOF-WI08",
                path,
                producer,
                "WIP-08 all source producers",
                "each producer requires its exact source/outbox regression",
            ));
        }
    }
    let fixtures = read(&root.join("conformance/wi-fabric-2/integration/fixtures.json"))?;
    for (id, names) in assigned {
        if names.is_empty()
            || serde_json::to_value(&names)? != fixtures["cases"][&id]["required_tests"]
            || names.iter().any(|n| {
                !obligations.values().flatten().any(|t| t.test == *n && t.manifest == RUNTIME)
            })
        {
            f.push(finding(
                "PROOF-WI08",
                path,
                &id,
                "WIP-08",
                "case fixtures and continuation evidence must agree",
            ));
        }
    }
    Ok(f)
}

/// WIP-10 maps every lifecycle criterion to an adopted, non-ignored test.
/// Runtime passage and approval authenticity remain separate evidence boundaries.
pub fn validate_worker_lifecycle(root: &Path, tests: &TestIndex) -> Result<Vec<Finding>> {
    let path = "conformance/wi-fabric-2/worker/lifecycle-coverage.json";
    let v = read(&root.join(path))?;
    let assigned: BTreeMap<String, Vec<String>> =
        serde_json::from_value(v["assigned_cases"].clone())?;
    let obligations: BTreeMap<String, Vec<TestRef>> =
        serde_json::from_value(v["required_obligations"].clone())?;
    let required: BTreeSet<String> = (17..=26)
        .map(|n| format!("WP-{n:03}"))
        .chain(std::iter::once("AB-WI-021".into()))
        .collect();
    let mut findings = Vec::new();
    if v["version"] != 1
        || v["scope"] != "wi/WI-09/process-provider:lifecycle"
        || v["runtime_pass_asserted"] != false
        || assigned.keys().cloned().collect::<BTreeSet<_>>() != required
    {
        findings.push(finding(
            "PROOF-WI10",
            path,
            "matrix",
            "WIP-10",
            "exact lifecycle case coverage and candidate evidence profile required",
        ));
    }
    let expected_obligations = required
        .into_iter()
        .chain(["approval-authenticity".into(), "storage-recovery".into()])
        .collect::<BTreeSet<_>>();
    if obligations.keys().cloned().collect::<BTreeSet<_>>() != expected_obligations {
        findings.push(finding(
            "PROOF-WI10",
            path,
            "obligations",
            "WIP-10",
            "lifecycle, approval and recovery obligations cannot be omitted",
        ));
    }
    for (case, names) in assigned {
        if names.is_empty()
            || names.iter().any(|name| {
                !obligations.values().flatten().any(|t| {
                    t.test == *name
                        && t.manifest == RUNTIME
                        && t.package == "worldinterface-runtime"
                        && tests.get(t) == Some(&1)
                })
            })
        {
            findings.push(finding(
                "PROOF-WI10",
                path,
                &case,
                "WIP-10",
                "each criterion requires exact reachable non-ignored adopted runtime tests",
            ));
        }
    }
    for (obligation, refs) in obligations {
        if refs.is_empty() || refs.iter().any(|t| tests.get(t) != Some(&1)) {
            findings.push(finding(
                "PROOF-WI10",
                path,
                &obligation,
                "WIP-10",
                "obligation tests must be adopted and non-ignored",
            ));
        }
    }
    Ok(findings)
}

/// Catalog claims require exact adopted, non-ignored runtime references.
pub fn validate_worker_catalog(root: &Path, tests: &TestIndex) -> Result<Vec<Finding>> {
    let path = "conformance/wi-fabric-2/worker/catalog-coverage.json";
    let v = read(&root.join(path))?;
    let assigned: BTreeMap<String, Vec<String>> =
        serde_json::from_value(v["assigned_cases"].clone())?;
    let obligations: BTreeMap<String, Vec<TestRef>> =
        serde_json::from_value(v["required_obligations"].clone())?;
    let expected = BTreeSet::from([
        "registration",
        "qualification",
        "drift",
        "quarantine",
        "recovery",
        "live-wi-aq",
    ]);
    let invalid = v["version"] != 1
        || v["scope"] != "wi/WI-10/process-catalog"
        || v["runtime_pass_asserted"] != false
        || assigned.keys().map(String::as_str).collect::<BTreeSet<_>>()
            != BTreeSet::from(["WP-007", "WP-032"])
        || obligations.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
        || obligations.values().any(|refs| {
            refs.is_empty()
                || refs.iter().any(|t| {
                    t.manifest != RUNTIME
                        || t.package != "worldinterface-runtime"
                        || tests.get(t) != Some(&1)
                })
        })
        || assigned.values().any(|names| {
            names.is_empty()
                || names.iter().any(|name| {
                    !obligations
                        .values()
                        .flatten()
                        .any(|t| t.test == *name && tests.get(t) == Some(&1))
                })
        });
    Ok(if invalid {
        vec![finding(
            "PROOF-WI12",
            path,
            "matrix",
            "WIP-12",
            "exact catalog cases and executable obligations required",
        )]
    } else {
        vec![]
    })
}

/// WIP-14 proof registrations and the complete WI-owned consumer import boundary.
pub fn validate_embedded(root: &Path, tests: &TestIndex) -> Result<Vec<Finding>> {
    let path = "conformance/wi-fabric-2/worker/embedded-coverage.json";
    let value = read(&root.join(path))?;
    let assigned: BTreeMap<String, Vec<TestRef>> =
        serde_json::from_value(value["assigned_cases"].clone())?;
    let obligations: BTreeMap<String, Vec<TestRef>> =
        serde_json::from_value(value["required_obligations"].clone())?;
    let expected = BTreeSet::from([
        "composition",
        "consumer-boundary",
        "scoped-disclosure",
        "shared-use-accounting",
        "interrupted-delivery",
        "offline-restore",
        "restart-quarantine",
    ]);
    let mut findings = Vec::new();
    if value["version"] != 1
        || value["scope"] != "wi/WI-11/embedded-runtime"
        || value["runtime_pass_asserted"] != false
        || value["second_store"]
            != "WI-owned independent AQ fixture; substitute for EXO cognition only"
        || assigned.keys().map(String::as_str).collect::<BTreeSet<_>>()
            != BTreeSet::from(["WP-029", "WP-030", "AB-WI-019"])
        || obligations.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
        || assigned.values().chain(obligations.values()).any(|refs| {
            refs.is_empty()
                || refs.iter().any(|t| {
                    ![FOUNDATION, RUNTIME].contains(&t.manifest.as_str())
                        || tests.get(t) != Some(&1)
                })
        })
    {
        findings.push(finding(
            "PROOF-WI14",
            path,
            "coverage",
            "WIP-14",
            "exact non-ignored embedded cases and obligations required",
        ));
    }
    let sources: Vec<String> = serde_json::from_value(value["consumer_sources"].clone())?;
    let expected_sources = [
        "crates/worldinterface-runtime/tests/embedded_worker.rs",
        "crates/worldinterface-runtime/tests/support/embedded_consumer.rs",
        "crates/worldinterface-runtime/tests/worker_campaign_cases.rs",
    ];
    if sources.iter().map(String::as_str).collect::<BTreeSet<_>>() != expected_sources.into() {
        findings.push(finding(
            "PROOF-WI14",
            path,
            "consumer_sources",
            "WP-030",
            "entry and complete helper source list required",
        ));
    }
    for source in sources {
        let code = std::fs::read_to_string(root.join(&source))?;
        for violation in
            embedded_consumer_violations(&code, source.ends_with("embedded_consumer.rs"))?
        {
            findings.push(finding(
                "PROOF-WI14",
                &source,
                &violation,
                "WP-030",
                "consumer must use application services",
            ));
        }
    }
    Ok(findings)
}
fn embedded_consumer_violations(code: &str, helper: bool) -> Result<Vec<String>> {
    use quote::ToTokens;
    use syn::visit::{self, Visit};
    struct Boundary {
        helper: bool,
        violations: Vec<String>,
    }
    impl<'ast> Visit<'ast> for Boundary {
        fn visit_path(&mut self, path: &'ast syn::Path) {
            let names = path.segments.iter().map(|s| s.ident.to_string()).collect::<Vec<_>>();
            let text = names.join("::");
            if (text.starts_with("worldinterface_runtime::")
                && !text.starts_with("worldinterface_runtime::embedded::"))
                || text.starts_with("worldinterface_storage::")
                || text.starts_with("actionqueue")
                || text.starts_with("std::fs")
                || text.starts_with("std::process")
                || self.helper && (text.starts_with("setup::") || text.starts_with("super::"))
                || names.iter().any(|n| {
                    [
                        "StoreRoot",
                        "WorkerRuntime",
                        "QueueEngine",
                        "PlanService",
                        "Repository",
                        "Service",
                        "Egress",
                    ]
                    .contains(&n.as_str())
                })
            {
                self.violations.push(text);
            }
            visit::visit_path(self, path);
        }
        fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
            // UseTree does not contain syn::Path nodes. Inspect only import
            // syntax; quoted scanner strings and comments never count as imports.
            let text = item.tree.to_token_stream().to_string().replace(' ', "");
            if (text.starts_with("worldinterface_runtime::")
                && !text.starts_with("worldinterface_runtime::embedded::"))
                || text.starts_with("worldinterface_storage::")
                || text.starts_with("actionqueue")
                || text.starts_with("std::fs")
                || text.starts_with("std::process")
                || text.starts_with("std::{") && (text.contains("fs") || text.contains("process"))
                || self.helper && (text.starts_with("super::") || text.starts_with("support::"))
            {
                self.violations.push(text);
            }
            visit::visit_item_use(self, item);
        }
        fn visit_expr_field(&mut self, field: &'ast syn::ExprField) {
            if let syn::Member::Named(name) = &field.member {
                let name = name.to_string();
                if ["queue", "bundle", "runtime", "port"].contains(&name.as_str())
                    || self.helper && name == "host"
                {
                    self.violations.push(name);
                }
            }
            visit::visit_expr_field(self, field);
        }
    }
    let file = syn::parse_file(code)?;
    let mut boundary = Boundary { helper, violations: vec![] };
    boundary.visit_file(&file);
    Ok(boundary.violations)
}
#[cfg(test)]
mod embedded_boundary_tests {
    use super::*;
    #[test]
    fn embedded_boundary_scans_helper_imports_and_fields_but_not_comment_or_string_mentions() {
        assert!(
            embedded_consumer_violations("use std::{fs, time::Instant};", true).unwrap().len() == 1
        );
        assert!(
            embedded_consumer_violations("use worldinterface_runtime::storage::StoreBundle;", true)
                .unwrap()
                .len()
                == 1
        );
        assert!(!embedded_consumer_violations("fn bypass(app: A) { let x=app.bundle; }", true)
            .unwrap()
            .is_empty());
        assert!(embedded_consumer_violations(
            "// std::fs\nfn valid(){let text=\"worldinterface_runtime::storage\";}",
            true
        )
        .unwrap()
        .is_empty());
    }
}
