//! WI-owned campaign orchestration. Discovery is eligibility; only executed exact
//! harness results and bound embedded observations establish campaign passage.
use crate::{
    array, hash,
    proof::{self, HarnessIndex, Registry, TestRef},
    read, string, Result,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
    time::Instant,
};

pub const COVERAGE: &str = "conformance/wi-fabric-2/worker/campaign-coverage.json";
pub const ENTRY: &str = "wi_owned_worker_acceptance_campaign";
pub const PARALLELISM: usize = 8;
const RUNTIME: &str = "crates/worldinterface-runtime/Cargo.toml";
const PACKET: &str = "conformance/wi-fabric-2/worker/evaluation-manifest.json";
const APPROVAL: &str = "conformance/wi-fabric-2/worker/evaluation-approval.json";
const LOCKS: [&str; 3] = [
    "crates/worldinterface-foundation/Cargo.lock",
    "crates/worldinterface-runtime/Cargo.lock",
    "tools/wi-contract-check/Cargo.lock",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outcome {
    pub reference: TestRef,
    pub executable_sha256: String,
    pub log: String,
    pub log_sha256: String,
    pub passed: bool,
}
fn output(root: &Path, program: &str, args: &[&str]) -> Result<String> {
    let out = Command::new(program).args(args).current_dir(root).output()?;
    if !out.status.success() {
        return Err(format!("{program} failed: {}", String::from_utf8_lossy(&out.stderr)).into());
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}
fn digest_file(root: &Path, path: &str) -> Result<String> {
    Ok(hash(fs::read(root.join(path))?))
}
fn pins(root: &Path) -> Result<Value> {
    let (registry, gates) = proof::load(root)?;
    let mut sources = BTreeMap::new();
    for path in LOCKS.into_iter().chain([
        PACKET,
        APPROVAL,
        COVERAGE,
        "conformance/wi-fabric-2/proof/cases.json",
        "contracts/aq-integration-target.json",
        "contracts/sources/wi-fabric-2-worker-execution-profile.yaml",
        "contracts/sources/wi-fabric-2-worker-execution-acceptance-matrix.yaml",
        "contracts/sources/aq-baseline-integration-acceptance-matrix.yaml",
    ]) {
        sources.insert(path.to_string(), digest_file(root, path)?);
    }
    for gate in &gates {
        let path = format!("conformance/wi-fabric-2/proof/gates/{}.json", gate.id);
        sources.insert(path.clone(), digest_file(root, &path)?);
    }
    for path in [crate::handoff::DESCRIPTOR, crate::handoff::DOCUMENT] {
        if root.join(path).exists() {
            sources.insert(path.to_string(), digest_file(root, path)?);
        }
    }
    let lock = read(&root.join(LOCKS[1]))?;
    let target = read(&root.join("contracts/aq-integration-target.json"))?;
    let mut aq = BTreeMap::new();
    for p in array(&lock, "package")? {
        let name = string(p, "name")?;
        if name.starts_with("actionqueue-") {
            let release = &target["runtime_release"]["crates"][name];
            if p["version"] != "0.3.0"
                || p["version"] != release["version"]
                || p["checksum"] != release["checksum"]
                || p["source"] != "registry+https://github.com/rust-lang/crates.io-index"
            {
                return Err("campaign requires published crates.io AQ 0.3.0 checksums".into());
            }
            aq.insert(
                name.to_string(),
                json!({"version":p["version"],"checksum":p["checksum"],"source":p["source"]}),
            );
        }
    }
    if aq.len() != 6 || registry.cases.len() != 55 {
        return Err("incomplete AQ graph or registry".into());
    }
    Ok(json!({"commit":output(root,"git", &["rev-parse","HEAD"])? ,
        "tree":output(root,"git", &["rev-parse","HEAD^{tree}"])? ,
        "clean":output(root,"git", &["status","--porcelain=v1","--untracked-files=all"])?.is_empty(),
        "sources":sources,"aq":aq,"historical_baseline":target["published_release"],
        "toolchain":output(root,"rustc", &["--version","--verbose"])? ,
        "cargo":output(root,"cargo", &["--version"])?}))
}
pub fn claims() -> Value {
    json!({"eligible_milestone":"WI-EMBEDDED-WORKER-PROOF-1", "milestone_closed":false,
        "worker_completion":"producer observation only", "wi_receipt":"provenance and execution evidence only",
        "consumer_task_acceptance":false, "ordinary_use":false, "parent_acceptance":false,
        "g5":false, "remote_parity":false, "release":false, "strong_isolation":false})
}
pub fn validate_claims(value: &Value) -> Result<()> {
    if *value != claims() {
        return Err(
            "only the partial producer milestone is eligible; completion is nonsemantic".into()
        );
    }
    Ok(())
}
pub fn validate_generation(before: &Value, after: &Value) -> Result<()> {
    if before["clean"] != true || after["clean"] != true || before != after {
        return Err("campaign requires one unchanged clean committed generation".into());
    }
    Ok(())
}
pub fn select(registry: &Registry, harnesses: &HarnessIndex) -> Result<BTreeMap<TestRef, PathBuf>> {
    let mut selected = BTreeMap::new();
    for c in &registry.cases {
        if c.status != "implemented" || c.tests.is_empty() {
            return Err(format!("unimplemented campaign case {}", c.id).into());
        }
        for t in &c.tests {
            if t.test == ENTRY {
                return Err("recursive campaign reference".into());
            }
            let paths = harnesses
                .get(t)
                .ok_or_else(|| format!("missing/ignored campaign test {}", t.test))?;
            if paths.len() != 1 {
                return Err(format!("ambiguous campaign test {}", t.test).into());
            }
            selected.insert(t.clone(), paths[0].clone());
        }
    }
    Ok(selected)
}
/// A successful child that executes zero tests, or ignores its test, cannot pass.
pub fn exact_pass(success: bool, log: &str) -> bool {
    success
        && log
            .lines()
            .any(|l| l.starts_with("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;"))
}
fn gate_states(root: &Path) -> Result<Value> {
    Ok(proof::load(root)?.1.iter().map(|g| (g.id.clone(), json!(g.status))).collect())
}
pub fn validate_gates(root: &Path, gates: &Value) -> Result<()> {
    let object = gates.as_object().ok_or("missing gate states")?;
    let final_state =
        gates["WI-WORKER-G4"] == "closed" && gates["WI-EMBEDDED-WORKER-PROOF-1"] == "closed";
    let expected = [
        "WI-AQ-G1",
        "WI-AQ-G2",
        "WI-AQ-G3",
        "WI-AQ-G4",
        "WI-WORKER-G1",
        "WI-WORKER-G2",
        "WI-WORKER-G3",
        "WI-WORKER-G4",
        "WI-EMBEDDED-CORE-1",
        "WI-EMBEDDED-WORKER-PROOF-1",
    ];
    if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected.into()
        || object.iter().any(|(id, status)| {
            *status
                != if !final_state
                    && ["WI-WORKER-G4", "WI-EMBEDDED-WORKER-PROOF-1"].contains(&id.as_str())
                {
                    "open"
                } else {
                    "closed"
                }
        })
    {
        return Err(
            "campaign requires eight closed/two open or ten closed gates; mixed states rejected"
                .into(),
        );
    }
    if final_state {
        crate::handoff::validate(root, &read(&root.join(crate::handoff::DESCRIPTOR))?)?;
    }
    Ok(())
}
/// Static coverage remains definition-review evidence, never a runtime pass claim.
pub fn validate_coverage(root: &Path, registry: &Registry, index: &proof::TestIndex) -> Result<()> {
    let contract = read(&root.join("conformance/wi-fabric-2/worker/handoff-contract.json"))?;
    if contract["version"] != 1
        || contract["kind"] != "wi-producer-evidence-v1"
        || contract["publication_owner"] != "WIP-16"
        || contract["executable_contract_owner"] != "WIP-15"
    {
        return Err("handoff contract version or ownership drift".into());
    }
    validate_claims(&contract["completion_semantics"])?;
    let v = read(&root.join(COVERAGE))?;
    if v["version"] != 1 || v["runtime_pass_asserted"] != false {
        return Err("invalid campaign coverage".into());
    }
    let expected = [
        "coding",
        "non-coding",
        "seam-simulation",
        "admission-denials",
        "acknowledgment-loss-duplication",
        "cancellation",
        "bounds",
        "credentials",
        "artifacts",
        "recovery",
        "catalog",
        "nonsemantic-completion",
        "pinned-handoff",
    ];
    let obligations =
        v["embedded_obligations"].as_object().ok_or("missing embedded obligations")?;
    if obligations.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected.into() {
        return Err("incomplete embedded obligations".into());
    }
    let all: BTreeSet<_> = registry.cases.iter().flat_map(|c| c.tests.iter().cloned()).collect();
    for refs in obligations.values() {
        let refs: Vec<TestRef> = serde_json::from_value(refs.clone())?;
        if refs.is_empty() || refs.iter().any(|t| index.get(t) != Some(&1) || !all.contains(t)) {
            return Err("missing, ignored or unregistered campaign obligation".into());
        }
    }
    Ok(())
}
fn approval(root: &Path) -> Result<Value> {
    let packet = read(&root.join(PACKET))?;
    for (path, digest) in packet["files"].as_object().ok_or("packet files missing")? {
        if json!(digest_file(root, path)?) != *digest {
            return Err(format!("packet source drift: {path}").into());
        }
    }
    let receipt = read(&root.join(APPROVAL))?;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
    validate_approval(&receipt, &packet, now)?;
    Ok(receipt)
}
pub fn validate_approval(receipt: &Value, packet: &Value, now: u128) -> Result<()> {
    if receipt["manifest"] != hash(serde_json::to_vec(&packet)?) {
        return Err("renewed operator acceptance of the concrete evaluation packet required".into());
    }
    if receipt["expires_unix_ms"].as_u64().is_none_or(|expiry| u128::from(expiry) <= now) {
        return Err("evaluation approval expired; renewed exact-packet acceptance required".into());
    }
    Ok(())
}
fn outside(root: &Path, directory: &Path) -> Result<PathBuf> {
    let path = directory.canonicalize()?;
    if path.starts_with(root.canonicalize()?)
        || std::env::var_os("CARGO_TARGET_DIR").is_some_and(|p| path.starts_with(PathBuf::from(p)))
    {
        return Err("campaign evidence must be outside source and build cache".into());
    }
    Ok(path)
}
fn local(base: &Path, name: &str) -> Result<PathBuf> {
    let path = Path::new(name);
    if path.components().count() != 1
        || !matches!(path.components().next(), Some(std::path::Component::Normal(_)))
    {
        return Err("invalid evidence path".into());
    }
    let path = base.join(path);
    if fs::symlink_metadata(&path)?.file_type().is_symlink() {
        return Err("symlink evidence rejected".into());
    }
    Ok(path)
}
fn discover(root: &Path) -> Result<HarnessIndex> {
    let mut harnesses = HarnessIndex::new();
    for (manifest, flags) in [
        ("crates/worldinterface-foundation/Cargo.toml", vec!["--workspace", "--all-features"]),
        (RUNTIME, vec![]),
        ("tools/wi-contract-check/Cargo.toml", vec![]),
    ] {
        harnesses.extend(proof::discover_harnesses(root, manifest, &flags)?);
    }
    Ok(harnesses)
}
pub fn run(root: &Path, directory: &Path) -> Result<()> {
    let started = Instant::now();
    if std::env::var_os("WI_WORKER_CAMPAIGN_CHILD").is_some() {
        return Err("recursive campaign execution rejected".into());
    }
    fs::create_dir_all(directory)?;
    let directory = outside(root, directory)?;
    if fs::read_dir(&directory)?.next().is_some() {
        return Err("campaign requires an empty evidence directory".into());
    }
    let before = pins(root)?;
    validate_generation(&before, &before)?;
    let acceptance = approval(root)?;
    let gates = gate_states(root)?;
    validate_gates(root, &gates)?;
    let (registry, gate_records) = proof::load(root)?;
    let discovery_started = Instant::now();
    let harnesses = discover(root)?;
    let discovery_seconds = discovery_started.elapsed().as_secs_f64();
    let index = harnesses.iter().map(|(t, p)| (t.clone(), p.len())).collect();
    if !proof::validate(root, &registry, &gate_records, &index)?.is_empty() {
        return Err("campaign registry validation failed".into());
    }
    validate_coverage(root, &registry, &index)?;
    let selected = select(&registry, &harnesses)?;
    // The campaign is also the sole owner of non-registered runtime tests. Do
    // not lose suite coverage when removing Cargo's duplicate default execution.
    let runtime_suite = runtime_remainder(&harnesses, &selected)?;
    // Share workers across both sets so a long runtime test can overlap the
    // registered cases. Receipt validation still checks each set independently.
    let execution_started = Instant::now();
    let all_outcomes = execute_harnesses(
        root,
        selected.iter().map(|(r, p)| (r.clone(), p.clone())).chain(
            runtime_suite
                .iter()
                .flat_map(|(r, paths)| paths.iter().map(move |p| (r.clone(), p.clone()))),
        ),
        &directory,
    )?;
    let execution_seconds = execution_started.elapsed().as_secs_f64();
    let (outcomes, runtime_suite_outcomes): (Vec<_>, Vec<_>) =
        all_outcomes.into_iter().partition(|o| selected.contains_key(&o.reference));
    let mut observations = BTreeMap::new();
    for name in ["coding.json", "transform.json", "seam.json"] {
        observations.insert(name.to_string(), digest_file(&directory, name).ok());
    }
    let after = pins(root)?;
    let cases: BTreeMap<_, _> = registry
        .cases
        .iter()
        .map(|c| {
            (
                c.id.clone(),
                json!(c
                    .tests
                    .iter()
                    .all(|t| outcomes.iter().any(|o| o.reference == *t && o.passed))),
            )
        })
        .collect();
    let receipt = json!({"version":2,"before":before,"after":after,"os":{
        "kernel":output(root,"uname", &["-srv"])? ,"architecture":output(root,"uname", &["-m"])? ,
        "release":fs::read_to_string("/etc/os-release")?},"execution_identity":output(root,"id",&[])? ,
        "evaluation_approval":acceptance,"outcomes":outcomes,"runtime_suite_outcomes":runtime_suite_outcomes,"cases":cases,"embedded_observations":observations,
        "gates":gates,"claims":claims(),"limitations":limitations(),
        "campaign_parallelism":PARALLELISM});
    fs::write(directory.join("receipt.json"), serde_json::to_vec_pretty(&receipt)?)?;
    let validation_started = Instant::now();
    // Reuse this run's compiled listings. Independent receipt checking rediscovers
    // the current binaries, but generation must build/list each configuration once.
    validate_receipt_with_harnesses(root, &receipt, &directory, &harnesses)?;
    let handoff = handoff_candidate(&receipt, &directory)?;
    fs::write(directory.join("handoff-candidate.json"), serde_json::to_vec_pretty(&handoff)?)?;
    // The complete receipt is validated above; compare persisted candidate bytes
    // without redoing discovery or executable measurement.
    validate_contract(&read(&directory.join("handoff-candidate.json"))?, &handoff)?;
    if root.join(crate::handoff::DESCRIPTOR).exists() {
        let publication = crate::handoff::publication(root, &receipt, &directory)?;
        fs::write(directory.join("handoff.json"), serde_json::to_vec_pretty(&publication)?)?;
        crate::handoff::validate_publication(
            root,
            &read(&directory.join("handoff.json"))?,
            &receipt,
            &directory,
        )?;
    }
    let timing = json!({"discovery_build_and_listing_seconds":discovery_seconds,
        "campaign_execution_seconds":execution_seconds,"execution_phases":1,
        "receipt_validation_seconds":validation_started.elapsed().as_secs_f64(),
        "total_seconds":started.elapsed().as_secs_f64(),"discovery_passes":1,
        "registered_tests":outcomes.len(),"additional_runtime_tests":runtime_suite_outcomes.len()});
    fs::write(directory.join("timing.json"), serde_json::to_vec_pretty(&timing)?)?;
    eprintln!("campaign timing: {timing}");
    Ok(())
}
/// Every eligible runtime test absent from the registry still runs exactly once.
pub fn runtime_remainder(
    harnesses: &HarnessIndex,
    registered: &BTreeMap<TestRef, PathBuf>,
) -> Result<HarnessIndex> {
    let mut remainder = BTreeMap::new();
    for (reference, paths) in harnesses {
        if reference.manifest != RUNTIME
            || reference.test == ENTRY
            || registered.contains_key(reference)
        {
            continue;
        }
        if paths.is_empty() {
            return Err("missing additional runtime harness".into());
        }
        remainder.insert(reference.clone(), paths.clone());
    }
    Ok(remainder)
}
/// Independent harnesses have private stores and child processes. Bound their
/// concurrency and retain every individual outcome. Tests from the same harness
/// can overlap; measure each executable once before and after the entire phase.
pub fn execute_selected(
    root: &Path,
    selected: &BTreeMap<TestRef, PathBuf>,
    directory: &Path,
) -> Result<Vec<Outcome>> {
    execute_harnesses(root, selected.iter().map(|(r, p)| (r.clone(), p.clone())), directory)
}
fn execute_harnesses(
    root: &Path,
    selected: impl Iterator<Item = (TestRef, PathBuf)>,
    directory: &Path,
) -> Result<Vec<Outcome>> {
    let selected = selected.collect::<VecDeque<_>>();
    let mut digests = BTreeMap::new();
    for (_, executable) in &selected {
        if !digests.contains_key(executable) {
            digests.insert(executable.clone(), hash(fs::read(executable)?));
        }
    }
    let pending = Mutex::new(selected);
    let batches = std::thread::scope(|scope| {
        let workers = (0..PARALLELISM)
            .map(|_| {
                scope.spawn(|| {
                    let mut batches = Vec::new();
                    loop {
                        let Some((reference, executable)) = pending.lock().unwrap().pop_front()
                        else {
                            break;
                        };
                        let outcome = execute_bound(
                            root,
                            reference,
                            &executable,
                            directory,
                            &digests[&executable],
                        );
                        if let Ok(outcome) = &outcome {
                            eprintln!(
                                "campaign {}: {}",
                                outcome.reference.test,
                                if outcome.passed { "passed" } else { "FAILED" }
                            );
                        }
                        batches.push(outcome.map_err(|e| e.to_string()));
                    }
                    batches
                })
            })
            .collect::<Vec<_>>();
        workers.into_iter().map(|w| w.join()).collect::<Vec<_>>()
    });
    for (executable, digest) in digests {
        if digest != hash(fs::read(executable)?) {
            return Err("campaign harness changed during execution".into());
        }
    }
    let mut outcomes = Vec::new();
    for worker in batches {
        for batch in worker.map_err(|_| "campaign worker panicked")? {
            outcomes.push(batch.map_err(|e| format!("campaign harness failed: {e}"))?);
        }
    }
    outcomes.sort_by(|a, b| (&a.reference, &a.log).cmp(&(&b.reference, &b.log)));
    Ok(outcomes)
}
pub fn execute(
    root: &Path,
    reference: TestRef,
    executable: &Path,
    directory: &Path,
) -> Result<Outcome> {
    let digest = hash(fs::read(executable)?);
    let outcome = execute_bound(root, reference, executable, directory, &digest)?;
    if digest != hash(fs::read(executable)?) {
        return Err("campaign harness changed during execution".into());
    }
    Ok(outcome)
}
fn log_name(reference: &TestRef, executable: &Path) -> Result<String> {
    Ok(format!("{}.log", hash(serde_json::to_vec(&(reference, executable))?)))
}
fn execute_bound(
    root: &Path,
    reference: TestRef,
    executable: &Path,
    directory: &Path,
    executable_sha256: &str,
) -> Result<Outcome> {
    let name = log_name(&reference, executable)?;
    let out = Command::new("timeout")
        .args(["--kill-after=10s", "600s"])
        .arg(executable)
        .args([&reference.test, "--exact", "--nocapture", "--test-threads=1"])
        .current_dir(root)
        .env("WI_WORKER_CAMPAIGN_CHILD", "1")
        .env("WI_WORKER_CAMPAIGN_EVIDENCE", directory)
        .env("PROPTEST_DISABLE_FAILURE_PERSISTENCE", "1")
        .output()?;
    let mut log = out.stdout;
    log.extend_from_slice(&out.stderr);
    let passed = exact_pass(out.status.success(), &String::from_utf8_lossy(&log));
    fs::write(directory.join(&name), &log)?;
    Ok(Outcome {
        reference,
        executable_sha256: executable_sha256.to_owned(),
        log: name,
        log_sha256: hash(&log),
        passed,
    })
}
pub fn limitations() -> Value {
    json!([
        "same OS identity and co-residency",
        "CPU, memory, process count, network and filesystem containment unavailable",
        "no independent VM or hostile-code qualification",
        "seam simulation only; no remote provider qualification",
        "worker exit and WI receipt do not establish consumer task acceptance",
        "reviewed milestone closure is effective only on reviewed merge with adopted checks; execution grants no closure"
    ])
}
fn keys(value: &Value, expected: &[&str]) -> Result<()> {
    if value
        .as_object()
        .ok_or("object required")?
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != expected.iter().copied().collect()
    {
        return Err("incomplete or unknown evidence fields".into());
    }
    Ok(())
}
pub fn validate_receipt(root: &Path, receipt: &Value, base: &Path) -> Result<()> {
    validate_receipt_with_harnesses(root, receipt, base, &discover(root)?)
}
fn validate_receipt_with_harnesses(
    root: &Path,
    receipt: &Value,
    base: &Path,
    harnesses: &HarnessIndex,
) -> Result<()> {
    keys(
        receipt,
        &[
            "version",
            "before",
            "after",
            "os",
            "execution_identity",
            "evaluation_approval",
            "outcomes",
            "runtime_suite_outcomes",
            "cases",
            "embedded_observations",
            "gates",
            "claims",
            "limitations",
            "campaign_parallelism",
        ],
    )?;
    if receipt["version"] != 2 || receipt["campaign_parallelism"] != PARALLELISM {
        return Err("unsupported campaign receipt".into());
    }
    validate_generation(&receipt["before"], &receipt["after"])?;
    if receipt["after"] != pins(root)? {
        return Err("stale campaign source/build pins".into());
    }
    validate_claims(&receipt["claims"])?;
    validate_gates(root, &receipt["gates"])?;
    if receipt["gates"] != gate_states(root)?
        || receipt["limitations"] != limitations()
        || receipt["evaluation_approval"] != approval(root)?
    {
        return Err("campaign scope drift".into());
    }
    for value in [
        &receipt["execution_identity"],
        &receipt["os"]["kernel"],
        &receipt["os"]["architecture"],
        &receipt["os"]["release"],
    ] {
        if value.as_str().is_none_or(str::is_empty) {
            return Err("missing execution environment".into());
        }
    }
    let (registry, _) = proof::load(root)?;
    let outcomes: Vec<Outcome> = serde_json::from_value(receipt["outcomes"].clone())?;
    validate_results(&registry, &outcomes, base)?;
    let selected = select(&registry, harnesses)?;
    let runtime_suite = runtime_remainder(harnesses, &selected)?;
    let additional: Vec<Outcome> =
        serde_json::from_value(receipt["runtime_suite_outcomes"].clone())?;
    validate_runtime_results(&runtime_suite, &additional, base)?;
    validate_executables(&outcomes, &selected)?;
    let cases = receipt["cases"].as_object().ok_or("missing case outcomes")?;
    if cases.len() != 55
        || registry
            .cases
            .iter()
            .any(|c| c.status != "implemented" || cases.get(&c.id) != Some(&json!(true)))
    {
        return Err("incomplete case aggregation".into());
    }
    let observations =
        receipt["embedded_observations"].as_object().ok_or("missing embedded evidence")?;
    if observations.len() != 3 {
        return Err("coding, transform and seam evidence required".into());
    }
    for name in ["coding.json", "transform.json", "seam.json"] {
        let bytes = fs::read(local(base, name)?)?;
        if observations.get(name) != Some(&json!(hash(&bytes))) {
            return Err("substituted embedded evidence".into());
        }
        let owner = if name == "seam.json" {
            "wp_034_embedded_seam_handoff_retains_nonsemantic_completion_and_unknown_controls"
        } else {
            "wp_033_embedded_completion_is_producer_evidence_for_partial_milestone_only"
        };
        let outcome = outcomes
            .iter()
            .find(|o| o.reference.test == owner && o.reference.manifest == RUNTIME)
            .ok_or("missing embedded observation producer")?;
        let log = fs::read_to_string(local(base, &outcome.log)?)?;
        if !log
            .lines()
            .any(|line| line.contains(&format!("WI-WORKER-OBSERVATION {name} {}", hash(&bytes))))
        {
            return Err("embedded observation disagrees with executed test log".into());
        }
        let observation: Value = serde_json::from_slice(&bytes)?;
        validate_observation(&observation, name == "seam.json")?;
        if observation["build"]["packet"] != receipt["evaluation_approval"]["manifest"]
            || observation["build"]["profile_definition"]
                != hash(fs::read(
                    root.join("contracts/sources/wi-fabric-2-worker-execution-profile.yaml"),
                )?)
        {
            return Err("unbound fixture build or profile definition".into());
        }
        if observation["build"]["seam_implementation"]
            != digest_file(root, "crates/worldinterface-runtime/tests/support/worker_seam.rs")?
        {
            return Err("substituted seam implementation".into());
        }
        for field in ["fixture", "helper", "seam_implementation"] {
            if observation["build"][field]
                .as_str()
                .is_none_or(|s| s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                return Err("missing build digest".into());
            }
        }
    }
    Ok(())
}
pub fn validate_executables(
    outcomes: &[Outcome],
    selected: &BTreeMap<TestRef, PathBuf>,
) -> Result<()> {
    let mut digests = BTreeMap::new();
    for outcome in outcomes {
        let executable = selected.get(&outcome.reference).ok_or("unknown campaign executable")?;
        if !digests.contains_key(executable) {
            digests.insert(executable.clone(), hash(fs::read(executable)?));
        }
        if outcome.executable_sha256 != digests[executable] {
            return Err("substituted compiled harness identity".into());
        }
    }
    Ok(())
}
pub fn validate_results(registry: &Registry, outcomes: &[Outcome], base: &Path) -> Result<()> {
    let expected: BTreeSet<_> =
        registry.cases.iter().flat_map(|c| c.tests.iter().cloned()).collect();
    validate_outcomes(&expected, outcomes, base)
}
fn validate_outcomes(
    expected: &BTreeSet<TestRef>,
    outcomes: &[Outcome],
    base: &Path,
) -> Result<()> {
    let actual: BTreeSet<_> = outcomes.iter().map(|o| o.reference.clone()).collect();
    if &actual != expected || actual.len() != outcomes.len() {
        return Err("incomplete or duplicate individual outcomes".into());
    }
    validate_logs(outcomes, base)
}
/// Shared support tests can have the same reference in several runtime harnesses.
/// Bind each occurrence to its current binary and unique per-harness log, retaining
/// the registry's stricter one-harness rule for registered acceptance references.
pub fn validate_runtime_results(
    expected: &HarnessIndex,
    outcomes: &[Outcome],
    base: &Path,
) -> Result<()> {
    let mut digests = BTreeMap::new();
    let mut expected_keys = BTreeSet::new();
    for (reference, paths) in expected {
        for path in paths {
            if !digests.contains_key(path) {
                digests.insert(path.clone(), hash(fs::read(path)?));
            }
            if !expected_keys.insert((
                reference.clone(),
                digests[path].clone(),
                log_name(reference, path)?,
            )) {
                return Err("duplicate runtime harness".into());
            }
        }
    }
    let actual: BTreeSet<_> = outcomes
        .iter()
        .map(|o| (o.reference.clone(), o.executable_sha256.clone(), o.log.clone()))
        .collect();
    if expected_keys != actual || actual.len() != outcomes.len() {
        return Err("missing, duplicate or substituted additional runtime outcome".into());
    }
    validate_logs(outcomes, base)
}
fn validate_logs(outcomes: &[Outcome], base: &Path) -> Result<()> {
    for o in outcomes {
        let bytes = fs::read(local(base, &o.log)?)?;
        if !o.passed
            || o.executable_sha256.len() != 64
            || hash(&bytes) != o.log_sha256
            || !exact_pass(true, &String::from_utf8_lossy(&bytes))
        {
            return Err(format!("failed or substituted test evidence {}", o.reference.test).into());
        }
    }
    Ok(())
}
pub fn validate_observation(v: &Value, seam: bool) -> Result<()> {
    keys(v, &["schema", "evidence", "build", "completed", "capacity_reserved", "receipt_count"])?;
    keys(&v["evidence"], &["binding", "seam_simulation", "controls", "cleanup"])?;
    keys(
        &v["evidence"]["binding"],
        &["tenant", "principal", "provider", "run", "request", "realization"],
    )?;
    keys(
        &v["evidence"]["cleanup"],
        &[
            "exit_confirmed",
            "exit_success",
            "cleanup_confirmed",
            "retained_output_bytes",
            "term_signals",
            "kill_signals",
            "descendants_observed",
        ],
    )?;
    if v["evidence"]["cleanup"]["exit_success"] != true
        || v["evidence"]["cleanup"]["retained_output_bytes"].as_u64().is_none_or(|n| n > 65536)
    {
        return Err("execution failure or output bound exceeded".into());
    }
    if v["schema"] != 1
        || v["evidence"]["seam_simulation"] != seam
        || v["completed"] != true
        || v["capacity_reserved"] != false
        || v["receipt_count"].as_u64().unwrap_or(0) < 5
        || v["evidence"]["cleanup"]["cleanup_confirmed"] != true
        || v["evidence"]["cleanup"]["exit_confirmed"] != true
    {
        return Err("incomplete embedded execution/cleanup evidence".into());
    }
    let controls = &v["evidence"]["controls"];
    if seam {
        if !controls.is_null() {
            return Err("seam must not claim process controls".into());
        }
    } else {
        if controls["schema"] != 1
            || controls["provider"] != v["evidence"]["binding"]["provider"]
            || controls["run"] != v["evidence"]["binding"]["run"]
        {
            return Err("unbound applied controls".into());
        }
        for field in ["requested", "advertised", "applied", "observed"] {
            let list = array(controls, field)?;
            for (kind, max) in [("wall_time_ms", 15000), ("output_bytes", 65536)] {
                let amount = list
                    .iter()
                    .find(|c| c["kind"] == kind)
                    .and_then(|c| c["state"]["supported"].as_u64())
                    .ok_or("missing finite applied/observed limit")?;
                if amount == 0 || amount > max {
                    return Err("evaluation limit exceeded".into());
                }
            }
            if list.iter().any(|c| {
                !["wall_time_ms", "output_bytes"].contains(&c["kind"].as_str().unwrap_or(""))
                    && c["state"].get("supported").is_some()
            }) {
                return Err("inflated containment claim".into());
            }
        }
    }
    Ok(())
}
/// Candidate is generated from execution, with the producer commit resolved at run
/// time. WIP-16 publishes the final handoff; this function grants no gate closure.
pub fn candidate(root: &Path, receipt: &Value, base: &Path) -> Result<Value> {
    validate_receipt(root, receipt, base)?;
    handoff_candidate(receipt, base)
}
/// Generate the contract payload from receipt evidence. Callers granting passage
/// must first validate the receipt; this function also supports non-live fixtures.
pub fn handoff_candidate(receipt: &Value, base: &Path) -> Result<Value> {
    Ok(json!({"version":1,"kind":"wi-producer-evidence-v1", "producer":receipt["after"],
        "campaign_sha256":hash(serde_json::to_vec(receipt)?),"cases":receipt["cases"],
        "tests":receipt["outcomes"],"runtime_suite_tests":receipt["runtime_suite_outcomes"],"observations":receipt["embedded_observations"],
        "gates":receipt["gates"],"claims":claims(),
        "producer_execution": [read(&local(base,"coding.json")?)?,read(&local(base,"transform.json")?)?,read(&local(base,"seam.json")?)?],"limitations":limitations(),
        "supported_operations":["provision","start","inspect","terminate","collect_artifacts","release"],
        "unavailable_operations":["pause","resume","checkpoint","remote-provider","daemon","python"],
        "consumer_acceptance":"requires independent application validation; worker exit and WI receipt are insufficient"}))
}
pub fn validate_handoff(root: &Path, handoff: &Value, receipt: &Value, base: &Path) -> Result<()> {
    validate_receipt(root, receipt, base)?;
    validate_handoff_payload(handoff, receipt, base)
}
/// Shared generated-payload check, independently testable with a fixture receipt.
/// validate_handoff also verifies the receipt's live source and executable pins.
pub fn validate_handoff_payload(handoff: &Value, receipt: &Value, base: &Path) -> Result<()> {
    validate_contract(handoff, &handoff_candidate(receipt, base)?)
}
pub fn validate_contract(handoff: &Value, expected: &Value) -> Result<()> {
    validate_claims(&handoff["claims"])?;
    if handoff != expected {
        return Err("handoff substitution, incomplete pins or semantic completion claim".into());
    }
    Ok(())
}
