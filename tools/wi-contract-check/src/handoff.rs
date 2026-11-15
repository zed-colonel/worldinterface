//! Publication consistency is definition evidence. Only a validated campaign
//! supplies execution evidence; reviewed merge remains a separate authority.
use crate::{files, finding, hash, proof, read, relative, worker_campaign, Finding, Result};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::Path};

pub const DESCRIPTOR: &str = "conformance/wi-fabric-2/proof/handoff.json";
pub const DOCUMENT: &str = "docs/evidence/WI-EMBEDDED-WORKER-PROOF-1.md";

/// Keep the human-readable gate/case/test appendix in the same generation too.
pub fn mapping_markdown(root: &Path) -> Result<String> {
    let (registry, gates) = proof::load_worker(root)?;
    let mut text = String::from("\n| Gate | Pass criteria | Required cases |\n|---|---|---|\n");
    for gate in gates {
        let criteria: Vec<String> = serde_json::from_value(gate.pass_criteria)?;
        text.push_str(&format!(
            "| {} | {} | {} |\n",
            gate.id,
            criteria.join("<br>"),
            gate.required_cases.join(", ")
        ));
    }
    text.push_str("\n| Case | Exact executable references |\n|---|---|\n");
    for case in registry.cases {
        let mut references = vec![];
        for test in case.tests {
            let manifest = match test.manifest.as_str() {
                crate::proof::FOUNDATION => "F",
                crate::proof::RUNTIME => "R",
                crate::proof::CHECKER => "C",
                _ => return Err("unknown handoff test manifest".into()),
            };
            references.push(format!("`{manifest} · {} · {}`", test.package, test.test));
        }
        text.push_str(&format!("| {} | {} |\n", case.id, references.join("<br>")));
    }
    Ok(text)
}

pub const SUPERVISOR_DOCUMENT: &str = "docs/evidence/WI-WORKER-G5.md";
pub fn supervisor_descriptor(root: &Path) -> Result<Value> {
    let (registry, gates) = proof::load(root)?;
    let gate =
        gates.into_iter().find(|g| g.id == "WI-WORKER-G5").ok_or("missing supervisor gate")?;
    let cases: Vec<_> = registry.cases.into_iter().filter(|c| c.gate == "WI-WORKER-G5").collect();
    Ok(json!({
        "version":1,"owner":"WSP-04","implementation_owner":"WSP-03",
        "available_since":"0.2.0-alpha.2",
        "scope":"authenticated interface and durable reconciliation only",
        "gate":gate,"cases":cases,"document":SUPERVISOR_DOCUMENT,
        "effective_on":"reviewed-merge-with-adopted-checks","execution_grants_closure":false,
        "interfaces":["worldinterface_worker::supervisor::{SupervisorHandler,Command,Inspect,Record,Session,Grant}",
            "worldinterface_runtime::supervisor::{Server,ChannelPolicy,PrivateChannel,Client,SupervisorConnector}"],
        "evidence":{
            "schema":"wi-supervisor-observation-v1","producer":"bounded external producer fixture; separate process and independently retained operator records; same OS identity",
            "binding":["case and crash scenario","stable command key and digest","server-owned authorization scope","durable backend result","authenticated inspect record","original WI intent and unchanged receipt","idempotent reconciliation","one activation","exact test outcome, executable/log hashes, source generation and evaluation packet"],
            "files":"supervisor-WP-NNN-SCENARIO.json; retained operator records outside guest and StoreBundle; all three WP-038 cuts are separate observations"
        },
        "bounds":{"wall_ms":15000,"frame_bytes":65536,"backend_evidence_bytes":4096,"observation_bytes":65536},
        "claims":{"authenticated_interface":true,"durable_reconciliation":true,"kata_lifecycle_qualified":false,"isolation_qualified":false,"consumer_task_accepted":false},
        "consumer":{
            "owner":"Exoskeleton",
            "obligations":["actual Kata stop/recreate","storage durability","image compatibility","deployment controls","isolation qualification"],
            "implementation":"implement SupervisorHandler in an operator-owned supervisor outside the target lifetime; stable-key dispatch and authoritative inspect survive target loss; never give a WorkerPort lifecycle authority"
        }
    }))
}

/// No commit or packet digest enters this descriptor. The packet hashes
/// it, and the external execution bundle resolves all concrete generation pins.
pub fn descriptor(root: &Path) -> Result<Value> {
    let (registry, gates) = proof::load_worker(root)?;
    let aq = read(&root.join("contracts/aq-integration-target.json"))?;
    let manifest: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("crates/worldinterface-runtime/Cargo.toml"),
    )?)?;
    let adapter_version =
        manifest["package"]["version"].as_str().ok_or("missing adapter version")?;
    let release_notes = format!("docs/release/{adapter_version}.md");
    let mut identities = BTreeMap::new();
    for path in [
        "rust-toolchain.toml",
        "docs/release/package-tests.json",
        release_notes.as_str(),
        "scripts/release.py",
        "scripts/wi_release.py",
        "scripts/wi_release_packages.py",
        "scripts/check-packages.py",
        "scripts/check-consumer.py",
        "examples/downstream-consumer/Cargo.toml",
        "examples/downstream-consumer/src/main.rs",
        "crates/worldinterface-foundation/Cargo.lock",
        "crates/worldinterface-runtime/Cargo.lock",
        "tools/wi-contract-check/Cargo.lock",
        "contracts/aq-integration-target.json",
        "contracts/sources/wi-fabric-2-worker-execution-profile.yaml",
        "contracts/sources/wi-fabric-2-worker-execution-profile.md",
        "contracts/sources/wi-fabric-2-worker-boundary-assumption-ledger.yaml",
        "contracts/sources/wi-fabric-2-worker-execution-acceptance-matrix.yaml",
        "contracts/sources/aq-baseline-integration-acceptance-matrix.yaml",
        "crates/worldinterface-runtime/Cargo.toml",
        "crates/worldinterface-runtime/tests/support/worker_build.rs",
        "crates/worldinterface-runtime/tests/support/worker_fixture.rs",
        "crates/worldinterface-runtime/tests/support/worker_seam.rs",
        "scripts/build-wi-worker-evaluation.sh",
        "scripts/wi_worker_evaluation.py",
        "conformance/wi-fabric-2/worker/evaluation-environment.json",
        "conformance/wi-fabric-2/worker/handoff-contract.json",
        "conformance/wi-fabric-2/worker/campaign-coverage.json",
        DOCUMENT,
        SUPERVISOR_DOCUMENT,
        "docs/work-items/WSP-04.md",
        "docs/work-items/WSP-05.md",
        "conformance/wi-fabric-2/worker/supervisor-coverage.json",
        "crates/worldinterface-runtime/tests/worker_supervisor.rs",
        "crates/worldinterface-runtime/tests/support/supervisor_fixture.rs",
        "crates/worldinterface-runtime/tests/support/supervisor_harness.rs",
        "crates/worldinterface-foundation/worker/src/supervisor/mod.rs",
    ] {
        identities.insert(path.to_owned(), hash(fs::read(root.join(path))?));
    }
    for directory in [
        "crates/worldinterface-runtime/src/worker_process",
        "crates/worldinterface-runtime/src/supervisor",
        "crates/worldinterface-runtime/src/bin",
        "fixtures/pre-wi-fabric-2/exo/repos/sample-rust",
    ] {
        for path in files(&root.join(directory))? {
            identities.insert(relative(root, &path), hash(fs::read(path)?));
        }
    }
    Ok(json!({
        "version":1, "kind":"wi-embedded-worker-handoff-descriptor-v1",
        "milestone":"WI-EMBEDDED-WORKER-PROOF-1", "maturity":"integration-only",
        "publication_owner":"WIP-16", "executable_contract_owner":"WIP-15",
        "revision_binding":"containing committed revision; resolve commit and tree externally on unchanged clean HEAD",
        "reviewed_closure":{
            "gates":["WI-WORKER-G4","WI-EMBEDDED-WORKER-PROOF-1"],
            "effective_on":"reviewed-merge-with-adopted-checks",
            "execution_grants_closure":false
        },
        "supervisor":supervisor_descriptor(root)?,
        "execution_claims":worker_campaign::claims(),
        "toolchain":"Rust 1.89.0; locked, offline Cargo; Linux x86_64",
        "aq":{"historical_target":aq["target"], "historical_release":aq["published_release"],
            "runtime_release":aq["runtime_release"],
            "interpretation":"execution uses crates.io 0.3.0; 0.2.0 is historical baseline identity only"},
        "profile":{"provider":"process-managed-proof-1", "execution":"WI-WORKER-EXECUTION-1",
            "adapter":format!("worldinterface-runtime {adapter_version} worker_process; source digests in identities")},
        "identities":identities,
        "cases":registry.cases, "gates":gates,
        "supported_operations":["provision","start","inspect","terminate","collect_artifacts","release"],
        "unavailable_operations":["pause","resume","checkpoint","remote-provider","daemon","python"],
        "controls":{
            "requested":"per-run finite limits; declarations do not establish enforcement",
            "advertised":"process wall_time_ms and output_bytes only; other containment unsupported",
            "applied":"source-bound executable and inputs, cleared environment, descriptor allowlist, wall timer and retained-output bounds",
            "observed":"separate per-run control and cleanup evidence; retained bytes, signals, descendant and exit measurements",
            "unknown":"seam simulation controls are null; recovery without live observations returns unknown",
            "missing":["cpu","memory","process_count","filesystem","network","independent_vm"]
        },
        "bounds":{"evaluation_wall_ms":15000,"evaluation_output_bytes":65536,
            "inputs":1,"input_bytes":1048576,"exports":4,"export_bytes":4194304,
            "flow_nodes":64,"flow_ports":32,"aq_initial_runs":64,"aq_dependencies":64,"aq_children":64,
            "aq_emitted_signals":32,"aq_hierarchy_depth":8,"waits_per_run":1,"aq_payload_bytes":65536},
        "risks":worker_campaign::limitations(),
        "release":{
            "package_version":adapter_version,
            "store_compatibility":{
                "previous_version":"0.2.0-alpha.1",
                "previous_store_requires_recreation":true,
                "current_store_readable_by_previous":false,
                "migration_supported":false,"rollback_compatible":false,
                "schema_changes":["effect_instances","effect_owner","effect_requests","worker_reserved","flow_pending_global"],
                "warning":"An alpha.1 store must be recreated under alpha.2. A store opened by alpha.2 cannot be read by alpha.1. No migration or rollback compatibility is provided."
            },
            "packaged_descriptor":"release/handoff.json",
            "packaged_gates":"release/gates/*.json; byte-equal authoritative reviewed records",
            "repository_lock":"release/repository-Cargo.lock; dependencies_digest() preserves repository provenance",
            "packaged_lock":"Cargo.lock; normalized archive digest recorded separately in external consumer-handoff.json",
            "consumer_handoff":"external release consumer-handoff.json binds source commit, descriptor, archives, both lock identities, measured worker digests and build environment; operator relays exact bytes and manifest SHA-256 through the published map record to EXV-11/EXV-27 as a downstream input",
            "worker_recipe":format!("canonicalize build root, CARGO_HOME and CARGO_TARGET_DIR before invoking Cargo; replace inherited RUSTFLAGS and CARGO_ENCODED_RUSTFLAGS with --remap-path-prefix for build root=/wi/build, target directory=/wi/target, each CARGO_HOME/registry/src/* root=/wi/registry, then sysroot/lib/rustlib/src/rust=/rustc/<commit-hash from rustc -vV> (ASCII 31 separators); cargo +1.89.0 install --locked --version '={adapter_version}' --target x86_64-unknown-linux-gnu --profile release --bin wi-process-fixture --bin wi-process-reaper --root INSTALL_ROOT worldinterface-runtime"),
            "digest_scope":"registry, staging and sysroot source paths remapped; recorded compiler, target, profile, environment and linker/system only",
            "procedure":"docs/release/publication-staging.md"
        },
        "consumer":{
            "P1-01":"pin descriptor, producer commit/tree, locks, AQ identities, profile, adapter, fixture/recipe and execution evidence; independently admit reviewed media executable/configuration/inputs",
            "P1-02":"use authenticated embedded services and scoped artifact import/read; validate one real media work order and its provenance receipts",
            "P1-03":"validate lost start acknowledgement, duplicate/early callback, interruption, cancellation, failed export, reconciliation and cleanup for the consumer workload",
            "P1-04":"compile the consumer render graph within Flow bounds and validate AQ durability, outbox ordering and restore",
            "P1-05":"validate media/task acceptance independently; retain capacity until confirmed cleanup, including cancellation and uncertain start",
            "interface":"EmbeddedHost::install/application; Application submission, Flow, advance, inspection, receipts, artifacts and reconciliation",
            "media_gap":"current process adapter accepts typed reviewed fixtures; arbitrary FFmpeg arguments and execution require further consumer integration and approval",
            "reviewed_fixture_registration":"exact host-owned catalog binding in a reviewed commit; same committed packet rule, no separate approval record; evaluation never registers itself",
            "ordinary_use":"disabled; reviewed fixture registration does not enable general workloads",
            "remote":"no remote or two-host qualification; Mac inference gateway remains consumer work"
        },
        "reproduction":{
            "adopted_checks":".craftingtable/checks.json; ct-check --declared ID for all ten IDs on committed HEAD",
            "prepare":["cargo run --locked --manifest-path tools/wi-contract-check/Cargo.toml -- hashes refresh",
                "cargo run --locked --manifest-path tools/wi-contract-check/Cargo.toml -- handoff generate > conformance/wi-fabric-2/proof/handoff.json",
                "cp conformance/wi-fabric-2/proof/handoff.json crates/worldinterface-runtime/release/handoff.json",
                "cargo run --locked --manifest-path tools/wi-contract-check/Cargo.toml -- hashes refresh",
                "python3 scripts/refresh-wi-worker-evaluation.py","scripts/build-wi-worker-evaluation.sh"],
            "execute":"scripts/worker-proof-evidence.sh EMPTY_EXTERNAL_DIRECTORY",
            "verify":"cargo run --locked --offline --manifest-path tools/wi-contract-check/Cargo.toml -- handoff verify DIRECTORY"
        },
        "execution_evidence":{
            "packet":"conformance/wi-fabric-2/worker/evaluation-manifest.json",
            "acceptance_policy":crate::evaluation::POLICY,
            "acceptance_rule":"2026-11-09 operator decision: complete manifest must match current source bytes and HEAD before bounded candidate execution; reviewed merge with adopted checks establishes durable acceptance, without receipt, anchor or calendar expiry",
            "historical_provenance":["conformance/wi-fabric-2/worker/evaluation-approval.json","crates/worldinterface-runtime/tests/support/worker_approval_anchor.rs"],
            "external_files":["receipt.json","handoff-candidate.json","handoff.json","coding.json","transform.json","seam.json","timing.json"],
            "required":"fresh complete registered and runtime-remainder passage, exact logs and executable digests, current clean commit/tree, OS/identity, exact committed packet, fixture/helper build and observed controls",
            "historical_evidence":"WIP-15 results are provenance only; never substitute for this revision"
        },
        "reviewer_references":[
            "docs/work-items/WIP-15-verification.md: predecessor reviews and cold campaign",
            "CraftingTable design 11e3aabb-001b-4e23-a675-1450f334de68: accepted WIP-16 design",
            "CraftingTable implementation 5f81f4d2-2cf6-4cb6-9e0f-8ea8f5dee8ab: candidate preparation; independent exact-head review and integration remain external"
        ]
    }))
}

pub fn validate(root: &Path, value: &Value) -> Result<()> {
    if *value != descriptor(root)? {
        return Err(
            "handoff disagrees with registry, gates, source identities or permitted claims".into(),
        );
    }
    validate_package_data(root)?;
    let (registry, gates) = proof::load_worker(root)?;
    if registry.cases.len() != 55
        || registry.cases.iter().any(|c| c.status != "implemented" || c.tests.is_empty())
        || gates.len() != 10
        || gates
            .iter()
            .any(|g| g.status != "closed" || g.effective_on != "reviewed-merge-with-adopted-checks")
    {
        return Err(
            "published handoff requires all 55 cases and ten reviewed-merge gate records".into()
        );
    }
    if !fs::read_to_string(root.join(DOCUMENT))?.ends_with(&mapping_markdown(root)?) {
        return Err("evidence document gate/case/test mapping disagrees with registry".into());
    }
    Ok(())
}

/// Package-local provenance and test support must retain authoritative bytes.
pub fn validate_package_data(root: &Path) -> Result<()> {
    let inventory = read(&root.join("docs/release/package-tests.json"))?;
    for (dest, source) in inventory["byte_copies"].as_object().ok_or("missing package copies")? {
        let source = source.as_str().ok_or("invalid package copy source")?;
        if fs::read(root.join(dest))? != fs::read(root.join(source))? {
            return Err(format!("stale package data: {dest}").into());
        }
    }
    Ok(())
}

pub fn check(root: &Path) -> Result<Vec<Finding>> {
    Ok(match read(&root.join(DESCRIPTOR)).and_then(|v| validate(root, &v)) {
        Ok(()) => vec![],
        Err(e) => {
            vec![finding("HANDOFF-CONSISTENCY", DESCRIPTOR, "WP-034", "WIP-16", &e.to_string())]
        }
    })
}

/// Construct a publication payload after receipt validation. Pure construction is
/// also used by negative fixtures; it never grants passage or reviewed closure.
pub fn publication(root: &Path, receipt: &Value, base: &Path) -> Result<Value> {
    validate(root, &read(&root.join(DESCRIPTOR))?)?;
    let candidate = worker_campaign::handoff_candidate(receipt, base)?;
    Ok(json!({"version":1,"kind":"wi-embedded-worker-execution-handoff-v1",
        "descriptor_sha256":hash(fs::read(root.join(DESCRIPTOR))?),
        "producer":receipt["after"], "campaign_sha256":hash(serde_json::to_vec(receipt)?),
        "evidence":candidate, "reviewed_closure":{
            "effective_on":"reviewed-merge-with-adopted-checks",
            "established_by_this_execution":false}, "claims":worker_campaign::claims()}))
}

pub fn validate_publication(
    root: &Path,
    value: &Value,
    receipt: &Value,
    base: &Path,
) -> Result<()> {
    if *value != publication(root, receipt, base)? {
        return Err(
            "substituted publication evidence, descriptor, revision or closure claim".into()
        );
    }
    Ok(())
}

pub fn verify(root: &Path, base: &Path) -> Result<()> {
    // Reuse the complete live generation, log, observation and executable checks.
    let receipt = read(&base.join("receipt.json"))?;
    worker_campaign::validate_handoff(
        root,
        &read(&base.join("handoff-candidate.json"))?,
        &receipt,
        base,
    )?;
    validate_publication(root, &read(&base.join("handoff.json"))?, &receipt, base)
}
