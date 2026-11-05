//! Offline WI-10 definition review. These types are tooling records, not a wire API.
//! No function here registers, authorizes, launches, deletes, or advances a milestone.
use crate::{files, finding, hash, inventory, read, relative, Finding, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

pub const CRITERIA: &[&str] = &[
    "trust_boundary",
    "durable_consequence",
    "ambiguity",
    "sensitive_authority",
    "independent_evidence",
    "semantic_governance",
];
pub const DESCRIPTORS: &[&str] = &[
    "effect_class",
    "resource_grammar",
    "resolver",
    "operation_schema",
    "input_artifact_profile",
    "output_artifact_profile",
    "idempotency_class",
    "provider_key_mapping",
    "commit_evidence",
    "reconciliation",
    "retry_boundaries",
    "isolation",
    "secrets",
    "time_limit",
    "memory_limit",
    "output_limit",
    "receipt_evidence_profile",
    "privacy",
    "retention",
];
pub const QUESTIONS: &[&str] = &[
    "trust_boundary",
    "accountable_principal",
    "canonical_resource",
    "shared_durable_sensitive_external",
    "ambiguous_commit",
    "idempotency_reconciliation",
    "semantic_governance",
    "sandbox_alternative",
    "useful_receipt",
    "policy_grammar",
    "isolation",
    "historical_versions",
    "external_identifier_binding",
    "historical_key_purpose",
    "proof_meaning",
    "distinct_roles",
    "disclosure_completeness",
    "optional_adapter",
];
pub const TARGETS: &[&str] = &[
    "filesystem.read",
    "filesystem.write",
    "filesystem.enumerate",
    "http.request",
    "artifact.import",
    "artifact.export",
    "process.execute",
    "message.send",
    "sandbox.provision",
    "sandbox.inspect",
    "sandbox.release",
    "worker.start",
    "worker.inspect",
    "worker.collect",
    "worker.terminate",
    "candidate.build",
    "runtime.activate",
    "runtime.rollback",
    "runtime.checkpoint",
    "principal.register",
    "principal.bind",
    "identity.issue-delegation",
    "identity.rotate",
    "identity.revoke",
    "artifact.publish",
    "information.disclose",
];
const SOURCE: &str = "implementation plan §1.4, §10.2–10.3, WI-10; WI-ADR-017; WP-007";
fn fail(f: &mut Vec<Finding>, rule: &str, item: &str, message: &str) {
    f.push(finding(rule, "contracts/wi10-*.json", item, SOURCE, message));
}
fn exact(values: &[String], required: &[&str]) -> bool {
    values.len() == required.len()
        && values.iter().map(String::as_str).collect::<BTreeSet<_>>()
            == required.iter().copied().collect()
}
fn digest(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
fn nonempty(s: &str) -> bool {
    !s.trim().is_empty()
}
fn revision(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryRef {
    pub id: String,
    pub source_sha256: String,
    pub registration_sites: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Destination {
    Specialist,
    AnalysisVerifier,
    CanonicalCapability,
    BoundedProcess,
    SandboxWorker,
    Ingress,
    DeterministicTransform,
    AdmissionReview,
    Wi08Deletion,
    Infrastructure,
    TestEvidence,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Surface {
    pub path: String,
    pub source_sha256: String,
    pub treatment: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Family {
    pub id: String,
    pub destination: Destination,
    pub owner: String,
    pub source: String,
    pub items: Vec<InventoryRef>,
    pub surfaces: Vec<Surface>,
    pub replacement_requirements: Vec<String>,
    pub removal_claimed: bool,
    pub replacement: Option<Replacement>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Disposition {
    pub version: u32,
    pub runtime_pass_asserted: bool,
    pub families: Vec<Family>,
}
/// Classification covers private helpers, all manifest locations and dynamic registrations.
/// Shared integration surfaces retain their own group; no guessed runtime call graph.
pub fn family_for(item: &inventory::Item) -> String {
    let p = &item.path;
    if p.contains("/semantic/") {
        return "semantic-analysis".into();
    }
    if let Some(tail) = p.strip_prefix("crates/worldinterface-connector/src/connectors/") {
        if tail != "mod.rs" {
            return tail.trim_end_matches(".rs").replace('_', "-");
        }
    }
    if p.ends_with("/signal.rs") || p.ends_with("/signal_registry.rs") {
        return "signal-machinery".into();
    }
    if item.kind == "wasm-manifest" {
        return format!("wasm:{}", item.name);
    }
    if p.contains("worldinterface-http-trigger/") {
        return "webhook-ingress".into();
    }
    if p.contains("worldinterface-wasm/") || p.starts_with("examples/wasm-connectors/") {
        return "wasm-support".into();
    }
    if p.contains("worldinterface-connector/") || item.kind == "registration" {
        return "registry-and-shared-support".into();
    }
    "fabric-support".into()
}
pub fn destination_for(id: &str) -> Destination {
    use Destination::*;
    match id {
        "semantic-analysis" | "code-symbol" | "code-read-symbol" | "code-references"
        | "code-impls" => AnalysisVerifier,
        "fs-read" | "fs-write" | "http-request" => CanonicalCapability,
        "shell-exec" => BoundedProcess,
        "sandbox-exec" => SandboxWorker,
        "delay" | "signal-await" | "signal-emit" | "signal-machinery" => Wi08Deletion,
        "webhook-ingress" | "wasm:test.streaming-echo" => Ingress,
        "wasm:json.validate" | "wasm:test.echo" => DeterministicTransform,
        "wasm:test.stress" => TestEvidence,
        s if s.starts_with("code-") || s.starts_with("repo-") || s == "gitignore-check" => {
            Specialist
        }
        s if s.starts_with("wasm:") => AdmissionReview,
        _ => Infrastructure,
    }
}
/// All source/schema/test/example/manifest locations are included, plus documentation
/// references (including root Markdown) and Cargo dependency declarations. Shared
/// files require selective edits. Frozen imports/fixtures remain outside this scan.
fn surface_sources(root: &Path) -> Result<BTreeMap<String, String>> {
    let mut sources = BTreeMap::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext.eq_ignore_ascii_case("md")) {
            if entry.file_type()?.is_symlink() {
                return Err(format!("symlink requires explicit review: {}", path.display()).into());
            }
            if entry.file_type()?.is_file() {
                sources.insert(relative(root, &path), fs::read_to_string(path)?);
            }
        }
    }
    for dir in ["crates", "examples", "docs"] {
        for p in files(&root.join(dir))? {
            let rel = relative(root, &p);
            if rel.starts_with("docs/adr/") || rel == "docs/connector-development.md" {
                continue;
            }
            if let Ok(text) = fs::read_to_string(&p) {
                sources.insert(rel, text);
            }
        }
    }
    Ok(sources)
}

fn surface_paths(
    root: &Path,
    items: &[&inventory::Item],
    family: &str,
    sources: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    let mut paths: BTreeSet<String> = items.iter().map(|i| i.path.clone()).collect();
    let needle = family.replace('-', "_");
    let dotted = needle.replace('_', ".");
    let names: Vec<_> = items.iter().filter(|i| i.kind == "connector").map(|i| &i.name).collect();
    for (rel, text) in sources {
        if text.contains(&needle)
            || text.contains(&dotted)
            || names.iter().any(|name| text.contains(name.as_str()))
        {
            paths.insert(rel.clone());
        }
    }
    // Removing a helper can orphan dependencies; declarations are review surfaces,
    // not a claim that every dependency in these shared files can be deleted.
    paths.insert("Cargo.toml".into());
    paths.insert("Cargo.lock".into());
    for item in items {
        let parts: Vec<_> = item.path.split('/').collect();
        for n in 1..parts.len() {
            let manifest = format!("{}/Cargo.toml", parts[..n].join("/"));
            if root.join(&manifest).exists() {
                paths.insert(manifest);
            }
        }
    }
    paths
}
pub fn validate_disposition(
    root: &Path,
    items: &[inventory::Item],
    plan: &Disposition,
) -> Result<Vec<Finding>> {
    let mut f = vec![];
    if plan.version != 1 || plan.runtime_pass_asserted {
        fail(&mut f, "CAT-VERSION", "disposition", "version 1 offline records required");
    }
    let sources = surface_sources(root)?;
    let mut groups: BTreeMap<String, Vec<&inventory::Item>> = BTreeMap::new();
    for i in items {
        // WI-09 adds target worker contracts; this WI-10 legacy connector
        // removal plan inventories only the preexisting connector surfaces.
        if i.path.starts_with("crates/worldinterface-foundation/worker/") {
            continue;
        }
        groups.entry(family_for(i)).or_default().push(i);
    }
    let mut seen = BTreeSet::new();
    for family in &plan.families {
        let id = &family.id;
        if !seen.insert(id.clone()) || !groups.contains_key(id) {
            fail(&mut f, "CAT-INVENTORY", id, "duplicate or stale family");
            continue;
        }
        let expected = &groups[id];
        let expected_refs: Vec<_> = expected
            .iter()
            .map(|i| InventoryRef {
                id: i.id.clone(),
                source_sha256: i.source_sha256.clone(),
                registration_sites: i.registration_sites.clone(),
            })
            .collect();
        if family.items != expected_refs || family.destination != destination_for(id) {
            fail(
                &mut f,
                "CAT-INVENTORY",
                id,
                "missing, duplicate, stale or misclassified inventory/helper/manifest reference",
            );
        }
        if !nonempty(&family.source)
            || family.owner
                != if family.destination == Destination::Wi08Deletion { "WI-08" } else { "WI-10" }
        {
            fail(&mut f, "CAT-DISPOSITION", id, "source and unchanged removal ownership required");
        }
        let paths = surface_paths(root, expected, id, &sources);
        let actual: BTreeSet<_> = family.surfaces.iter().map(|s| s.path.clone()).collect();
        if actual != paths || family.surfaces.len() != paths.len() {
            fail(
                &mut f,
                "CAT-SURFACE",
                id,
                &format!(
                    "incomplete or duplicate removal surfaces: missing {:?}; unexpected {:?}",
                    paths.difference(&actual).collect::<Vec<_>>(),
                    actual.difference(&paths).collect::<Vec<_>>()
                ),
            );
        }
        for s in &family.surfaces {
            if !paths.contains(&s.path) {
                continue;
            }
            if hash(fs::read(root.join(&s.path))?) != s.source_sha256 || !nonempty(&s.treatment) {
                fail(
                    &mut f,
                    "CAT-SURFACE",
                    id,
                    "stale surface digest or missing selective deletion treatment",
                );
            }
        }
        if !exact(
            &family.replacement_requirements,
            &[
                "exact-revision-and-artifacts",
                "independent-qualification",
                "replacement-fixtures",
                "WI-06-acceptance",
                "WI-09-acceptance-if-worker",
            ],
        ) {
            fail(&mut f, "CAT-REMOVAL", id, "replacement requirements may not be weakened");
        }
        // This slice has no trusted runtime evidence ingestion/admission path.
        // Structural plans pass with blocked readiness. Any readiness claim must wait
        // for integration to supply independently verified evidence, never local fixtures.
        if family.removal_claimed || family.replacement.is_some() {
            fail(
                &mut f,
                "CAT-REMOVAL",
                id,
                "removal blocked: no verified replacement evidence in this domain slice",
            );
        }
    }
    for id in groups.keys() {
        if !seen.contains(id) {
            fail(&mut f, "CAT-INVENTORY", id, "unclassified family");
        }
    }
    Ok(f)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Candidate {
    pub operation: String,
    pub status: String,
    pub owner_scope: String,
    pub rationale: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub version: u32,
    pub runtime_pass_asserted: bool,
    pub admission_criteria: Vec<String>,
    pub descriptor_requirements: Vec<String>,
    pub appendix_questions: Vec<String>,
    pub candidates: Vec<Candidate>,
    pub denied: Vec<String>,
}
pub fn validate_catalog(c: &Catalog) -> Vec<Finding> {
    let mut f = vec![];
    if c.version != 1
        || c.runtime_pass_asserted
        || !exact(&c.admission_criteria, CRITERIA)
        || !exact(&c.descriptor_requirements, DESCRIPTORS)
        || !exact(&c.appendix_questions, QUESTIONS)
    {
        fail(
            &mut f,
            "CAT-ADMISSION",
            "checklist",
            "complete versioned admission, descriptor and Appendix A checklist required",
        );
    }
    if !exact(&c.candidates.iter().map(|c| c.operation.clone()).collect::<Vec<_>>(), TARGETS) {
        fail(&mut f, "CAT-VOCABULARY", "catalog", "missing, duplicate or unreviewed operation");
    }
    for entry in &c.candidates {
        let status = if entry.operation == "filesystem.enumerate" { "deferred" } else { "pending" };
        if entry.status != status || !nonempty(&entry.owner_scope) || !nonempty(&entry.rationale) {
            fail(&mut f, "CAT-ADMISSION", &entry.operation, "actual admission remains pending; frequency, cost and complete descriptor review precede admission");
        }
    }
    if !exact(
        &c.denied,
        &[
            "arbitrary-deployment-shell",
            "coding-specific-fabric-vocabulary",
            "unqualified-ordinary-use",
            "implicit-hostile-code-or-vm-assurance",
        ],
    ) {
        fail(&mut f, "CAT-EXCLUSIONS", "catalog", "mandatory exclusions required");
    }
    f
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionReview {
    pub version: u32,
    pub operation: String,
    pub justified_criteria: BTreeMap<String, String>,
    pub frequency: String,
    pub mediation_cost: String,
    pub appendix_answers: BTreeMap<String, String>,
    pub descriptor: BTreeMap<String, String>,
    pub arbitrary_deployment_shell: bool,
}
pub fn validate_admission(r: &AdmissionReview) -> bool {
    r.version == 1
        && TARGETS.contains(&r.operation.as_str())
        && r.operation != "filesystem.enumerate"
        && !r.arbitrary_deployment_shell
        && !r.justified_criteria.is_empty()
        && r.justified_criteria.iter().all(|(k, v)| CRITERIA.contains(&k.as_str()) && nonempty(v))
        && nonempty(&r.frequency)
        && nonempty(&r.mediation_cost)
        && exact(&r.appendix_answers.keys().cloned().collect::<Vec<_>>(), QUESTIONS)
        && r.appendix_answers.values().all(|s| nonempty(s))
        && exact(&r.descriptor.keys().cloned().collect::<Vec<_>>(), DESCRIPTORS)
        && r.descriptor.values().all(|s| nonempty(s))
        && ["time_limit", "memory_limit", "output_limit"]
            .iter()
            .all(|k| r.descriptor.get(*k).is_some_and(|s| s.parse::<u64>().is_ok_and(|n| n > 0)))
        && (r.operation != "process.execute"
            || (r.descriptor.get("isolation").map(String::as_str)
                == Some("process-managed-proof-1")
                && r.descriptor.get("effect_class").map(String::as_str) == Some("bounded-process")
                && r.descriptor.get("secrets").map(String::as_str)
                    == Some("no-production-secrets")))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub descriptor: String,
    pub profile: String,
    pub adapter: String,
    pub executable: String,
    pub dependencies: String,
    pub configuration: String,
    pub environment: String,
    pub fixtures: String,
}
impl Binding {
    pub fn valid(&self) -> bool {
        [
            &self.descriptor,
            &self.profile,
            &self.adapter,
            &self.executable,
            &self.dependencies,
            &self.configuration,
            &self.environment,
            &self.fixtures,
        ]
        .iter()
        .all(|s| digest(s))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Qualification {
    pub binding: Binding,
    pub results: String,
    pub reviewer_evidence: String,
    pub independent_review: bool,
    pub passed: bool,
    pub revoked: bool,
    pub synthetic: bool,
}
impl Qualification {
    pub fn applies(&self, b: &Binding) -> bool {
        b.valid()
            && self.binding == *b
            && digest(&self.results)
            && digest(&self.reviewer_evidence)
            && self.independent_review
            && self.passed
            && !self.revoked
            && !self.synthetic
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationApproval {
    pub binding: Binding,
    pub approval_id: String,
    pub reviewed_fixtures: bool,
    pub non_sensitive_environment: bool,
    pub revoked: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Operation {
    Start,
    Inspect,
    Terminate,
    Reconcile,
    Collect,
    Command,
    Checkpoint,
    Suspend,
    Resume,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub admission: AdmissionReview,
    pub binding: Binding,
    pub operation: Operation,
    pub evaluation_only: bool,
    pub authorized: bool,
    pub mandatory_controls_enforced: bool,
    pub resource_bounds_enforced: bool,
    pub non_sensitive_inputs: bool,
    pub reviewed_executable_and_inputs: bool,
    pub requires_independent_vm: bool,
    pub quarantined: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AdmissionDecision {
    Denied,
    Unavailable,
    EvaluationOnly,
    Qualified,
    AuthorizedCleanup,
}
/// Evaluates *supplied evidence assertions*. Even Qualified is an offline decision,
/// never authentication of the assertions or a grant. Callers cannot self-promote.
pub fn evaluate_registration(
    r: &Request,
    q: Option<&Qualification>,
    approval: Option<&EvaluationApproval>,
) -> AdmissionDecision {
    use AdmissionDecision::*;
    if matches!(
        r.operation,
        Operation::Command | Operation::Checkpoint | Operation::Suspend | Operation::Resume
    ) {
        return Unavailable;
    }
    if !r.authorized
        || !r.binding.valid()
        || !r.mandatory_controls_enforced
        || !r.resource_bounds_enforced
    {
        return Denied;
    }
    if matches!(r.operation, Operation::Inspect | Operation::Terminate | Operation::Reconcile) {
        return AuthorizedCleanup;
    }
    if !validate_admission(&r.admission)
        || r.admission.operation != "process.execute"
        || r.binding.descriptor
            != hash(serde_json::to_vec(&r.admission).expect("serializable admission"))
        || r.quarantined
        || !r.non_sensitive_inputs
        || !r.reviewed_executable_and_inputs
        || r.requires_independent_vm
    {
        return Denied;
    }
    if r.evaluation_only {
        return if approval.is_some_and(|a| {
            a.binding == r.binding
                && nonempty(&a.approval_id)
                && a.reviewed_fixtures
                && a.non_sensitive_environment
                && !a.revoked
        }) {
            EvaluationOnly
        } else {
            Denied
        };
    }
    if q.is_some_and(|q| q.applies(&r.binding)) {
        Qualified
    } else {
        Denied
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Support {
    pub operation: Operation,
    pub available: bool,
    pub reason: AdmissionDecision,
    pub grants_authority: bool,
}
pub fn inspect_supported(
    r: &Request,
    q: Option<&Qualification>,
    a: Option<&EvaluationApproval>,
) -> Vec<Support> {
    [
        Operation::Start,
        Operation::Inspect,
        Operation::Terminate,
        Operation::Reconcile,
        Operation::Collect,
        Operation::Command,
        Operation::Checkpoint,
        Operation::Suspend,
        Operation::Resume,
    ]
    .into_iter()
    .map(|operation| {
        let mut request = r.clone();
        request.operation = operation.clone();
        let reason = evaluate_registration(&request, q, a);
        Support {
            operation,
            available: !matches!(
                reason,
                AdmissionDecision::Denied | AdmissionDecision::Unavailable
            ),
            reason,
            grants_authority: false,
        }
    })
    .collect()
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Replacement {
    pub revision: String,
    pub artifact_digest: String,
    pub binding: Binding,
    pub qualification: Qualification,
    pub fixture_results: String,
    pub wi06_acceptance: String,
    pub wi09_acceptance: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplacementExpectation {
    pub revision: String,
    pub artifact_digest: String,
    pub binding: Binding,
    pub fixture_results: String,
    pub wi06_acceptance: String,
    pub wi09_acceptance: Option<String>,
}
/// The expectation must come from independent replacement review. This domain
/// command has no such trust root and never reports executable removal readiness.
pub fn assess_removal(expected: &ReplacementExpectation, supplied: Option<&Replacement>) -> bool {
    supplied.is_some_and(|e| {
        revision(&e.revision)
            && e.revision == expected.revision
            && digest(&e.artifact_digest)
            && e.artifact_digest == expected.artifact_digest
            && e.binding == expected.binding
            && e.qualification.applies(&expected.binding)
            && digest(&e.fixture_results)
            && e.fixture_results == expected.fixture_results
            && e.qualification.results == e.fixture_results
            && digest(&e.wi06_acceptance)
            && e.wi06_acceptance == expected.wi06_acceptance
            && e.wi09_acceptance == expected.wi09_acceptance
            && e.wi09_acceptance.as_ref().is_none_or(|s| digest(s))
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "digest", deny_unknown_fields)]
pub enum ImageApplicability {
    Known(String),
    NotApplicable,
    Unknown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub schema: String,
    pub version: u32,
    pub digest: String,
    pub max_bytes: u64,
}
/// Typed control values are kept separately by provenance; observed absence is
/// unknown, not inferred from requested or advertised values.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "control", content = "value", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Control {
    WallTimeMs(u64),
    OutputBytes(u64),
    ReviewedInputs(String),
    AllowlistedEnvironment(String),
}
impl Control {
    fn key(&self) -> &'static str {
        match self {
            Self::WallTimeMs(_) => "time",
            Self::OutputBytes(_) => "output",
            Self::ReviewedInputs(_) => "inputs",
            Self::AllowlistedEnvironment(_) => "environment",
        }
    }
    fn valid(&self) -> bool {
        match self {
            Self::WallTimeMs(n) | Self::OutputBytes(n) => *n > 0,
            Self::ReviewedInputs(s) | Self::AllowlistedEnvironment(s) => digest(s),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessMapping {
    pub version: u32,
    pub requested_profile_selector: String,
    pub effect_input_ref: ArtifactRef,
    pub resolved_effect_manifest: ArtifactRef,
    pub evidence_ref: ArtifactRef,
    pub selected: Binding,
    pub image: ImageApplicability,
    pub provider_ref: String,
    pub run_ref: String,
    pub resource_ref: String,
    pub enclosing_instance_evidence: Option<ArtifactRef>,
    pub requested_controls: Vec<Control>,
    pub advertised_controls: Vec<Control>,
    pub applied_controls: Vec<Control>,
    pub observed_controls: Vec<Control>,
}
pub fn validate_mapping(m: &ProcessMapping) -> bool {
    fn artifact(a: &ArtifactRef, schema: &str) -> bool {
        a.schema == schema
            && a.version == 1
            && digest(&a.digest)
            && a.max_bytes > 0
            && a.max_bytes <= 1_048_576
    }
    fn opaque(s: &str, prefix: &str) -> bool {
        s.strip_prefix(prefix).is_some_and(|tail| {
            !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
    }
    m.version == 1
        && m.requested_profile_selector == "worker-profile:process-managed-proof-1"
        && artifact(&m.effect_input_ref, "EffectInputRef/reviewed-inputs")
        && artifact(&m.resolved_effect_manifest, "ResolvedEffect/typed-immutable-process-manifest")
        && artifact(&m.evidence_ref, "EvidenceRef/process-controls")
        && m.resolved_effect_manifest.digest
            == hash(serde_json::to_vec(&m.selected).expect("serializable binding"))
        && m.evidence_ref.digest
            == hash(
                serde_json::to_vec(&[
                    &m.requested_controls,
                    &m.advertised_controls,
                    &m.applied_controls,
                    &m.observed_controls,
                ])
                .expect("serializable controls"),
            )
        && serde_json::to_vec(&m.selected).expect("serializable binding").len() as u64
            <= m.resolved_effect_manifest.max_bytes
        && serde_json::to_vec(&[
            &m.requested_controls,
            &m.advertised_controls,
            &m.applied_controls,
            &m.observed_controls,
        ])
        .expect("serializable controls")
        .len() as u64
            <= m.evidence_ref.max_bytes
        && m.selected.valid()
        && m.image == ImageApplicability::NotApplicable
        && opaque(&m.provider_ref, "provider:")
        && opaque(&m.run_ref, "run:")
        && opaque(&m.resource_ref, "resource:")
        && m.enclosing_instance_evidence
            .as_ref()
            .is_none_or(|a| artifact(a, "EvidenceRef/enclosing-instance"))
        && [
            &m.requested_controls,
            &m.advertised_controls,
            &m.applied_controls,
            &m.observed_controls,
        ]
        .iter()
        .all(|v| {
            v.len() <= 32
                && v.iter().all(Control::valid)
                && v.iter().map(Control::key).collect::<BTreeSet<_>>().len() == v.len()
        })
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verifier {
    pub id: String,
    pub inputs: Vec<String>,
    pub output: String,
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
    pub external_io: bool,
    pub executes_tests: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sandbox {
    pub version: u32,
    pub runtime_pass_asserted: bool,
    pub package_destination: String,
    pub contents: Vec<String>,
    pub replacement_fixture: Vec<String>,
    pub unavailable_assurances: Vec<String>,
    pub verifiers: Vec<Verifier>,
    pub mapping: ProcessMapping,
}
pub fn validate_sandbox(s: &Sandbox) -> Vec<Finding> {
    let mut f = vec![];
    if s.version != 1
        || s.runtime_pass_asserted
        || s.package_destination
            != "standalone-specialist-package-outside-worldinterface-dependencies"
        || !exact(
            &s.contents,
            &[
                "search-read",
                "edit-write-patch",
                "semantic-analysis",
                "repository-context",
                "test-execution",
                "git-diff-ignore",
                "artifact-export",
            ],
        )
        || !exact(
            &s.replacement_fixture,
            &[
                "search",
                "edit",
                "test",
                "artifact-export",
                "no-fabric-coding-connectors",
                "reviewed-executable-and-dependencies",
                "non-sensitive-inputs",
                "independent-results-review",
            ],
        )
        || !exact(
            &s.unavailable_assurances,
            &[
                "hostile-code",
                "independent-worker-vm",
                "kata-qualification",
                "filesystem-confinement",
                "network-confinement",
                "cpu-memory-process-count-unless-enforced",
            ],
        )
        || !validate_mapping(&s.mapping)
    {
        fail(&mut f, "CAT-SANDBOX", "disposition", "standalone destination, complete replacement fixture, explicit limitations and ADR-016 mapping required");
    }
    let contracts = [
        ("changed-path-scope", vec!["changed-path-manifest", "authorized-path-scope"]),
        ("artifact-schema", vec!["artifact-bytes", "versioned-schema"]),
        ("test-report", vec!["exported-test-report", "expected-suite-manifest"]),
        ("secret-scan", vec!["exported-artifact-bytes", "reviewed-secret-rules"]),
        ("patch-metadata", vec!["patch-manifest", "base-and-result-digests"]),
    ];
    if !exact(
        &s.verifiers.iter().map(|v| v.id.clone()).collect::<Vec<_>>(),
        &contracts.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
    ) {
        fail(&mut f, "CAT-VERIFIER", "verifiers", "five independent verifier contracts required");
    }
    for v in &s.verifiers {
        if contracts
            .iter()
            .find(|(k, _)| *k == v.id)
            .is_none_or(|(_, inputs)| !exact(&v.inputs, inputs))
            || v.external_io
            || v.executes_tests
            || v.max_input_bytes == 0
            || v.max_input_bytes > 16_777_216
            || v.max_output_bytes == 0
            || v.max_output_bytes > 1_048_576
            || v.output != "bounded-findings-with-input-digests-and-verifier-version"
        {
            fail(&mut f, "CAT-VERIFIER", &v.id, "verifiers consume bounded explicit artifacts only; no hidden test execution or external I/O");
        }
    }
    f
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assumption {
    pub id: String,
    pub applicability: String,
    pub status: String,
    pub evidence_owner: String,
    pub retained_evidence: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Obligation {
    pub id: String,
    pub source: String,
    pub tests: Vec<String>,
    pub follow_on: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Obligations {
    pub version: u32,
    pub runtime_pass_asserted: bool,
    pub source_digests: BTreeMap<String, String>,
    pub obligations: Vec<Obligation>,
    pub assumptions: Vec<Assumption>,
    pub predecessor_receipt: String,
    pub predecessor_revision: String,
    pub policy_revision: u32,
    pub scope_binding_digest: String,
}
pub fn validate_obligations(root: &Path, o: &Obligations) -> Result<Vec<Finding>> {
    let mut f = vec![];
    if o.version != 1
        || o.runtime_pass_asserted
        || o.policy_revision != 1
        || o.scope_binding_digest
            != "51563f70c37bd7ca696f22833ec81d37acfa4918ba380dec679198c0a4f36cff"
        || o.predecessor_receipt != "648a94bc-0aae-48bb-b7f4-b1cd879f7b94"
        || o.predecessor_revision != "8f2783963701dc60988f367ed53484a6ec3a7b00"
        || !exact(
            &o.obligations.iter().map(|x| x.id.clone()).collect::<Vec<_>>(),
            &[
                "inventory",
                "deletion-plan",
                "admission",
                "WP-007",
                "sandbox",
                "isolation",
                "ADR-coverage",
            ],
        )
    {
        fail(
            &mut f,
            "CAT-OBLIGATIONS",
            "scope",
            "preserve domain scope, predecessor evidence and per-obligation traceability",
        );
    }
    let tests = fs::read_to_string(root.join("tools/wi-contract-check/tests/catalog.rs"))?;
    for ob in &o.obligations {
        if !nonempty(&ob.source)
            || !nonempty(&ob.follow_on)
            || ob.tests.is_empty()
            || ob.tests.iter().any(|t| !tests.contains(&format!("fn {t}(")))
        {
            fail(
                &mut f,
                "CAT-OBLIGATIONS",
                &ob.id,
                "each obligation requires executable tests, source and retained follow-on work",
            );
        }
    }
    let required: Vec<_> = (1..=8).map(|n| format!("WP-A{n:02}")).collect();
    if !exact(
        &o.assumptions.iter().map(|a| a.id.clone()).collect::<Vec<_>>(),
        &required.iter().map(String::as_str).collect::<Vec<_>>(),
    ) || o.assumptions.iter().any(|a| {
        a.status != "pending"
            || !nonempty(&a.applicability)
            || !nonempty(&a.evidence_owner)
            || !nonempty(&a.retained_evidence)
    }) {
        fail(
            &mut f,
            "CAT-ASSUMPTIONS",
            "WP-A01–WP-A08",
            "retain applicability and unresolved runtime evidence",
        );
    }
    for (name, expected) in [
        ("worker-assumptions", "65bd7ca0b4fc1c4d116157b9ee05ac575e3da6bba0618dc35284303c5cd92d05"),
        ("aq-assumptions", "943d9f263afeee5c56cc6c65a3c6dfca6fca8c871ff070aed2cd0c77f1437415"),
        ("WI-ADR-016", "cafcb369dfeb14acbd6407ffd740da96d1d4de8f642243a1704128b05c89acc4"),
        ("WI-ADR-017", "0ed0ac6f51edbcd7f66f5c2f2affaff97ad7000aefb3e761639e550e380d3f4a"),
    ] {
        if o.source_digests.get(name).map(String::as_str) != Some(expected) {
            fail(&mut f, "CAT-SOURCE", name, "adopted evidence digest changed");
        }
    }
    Ok(f)
}
/// WI-10 rule hits must all have deletion-plan surfaces, without enabling the rules.
pub fn validate_forbidden_coverage(root: &Path, d: &Disposition) -> Result<Vec<Finding>> {
    let policy = read(&root.join("contracts/forbidden-symbols.yaml"))?;
    let mut f = vec![];
    let covered: BTreeSet<_> =
        d.families.iter().flat_map(|g| g.surfaces.iter().map(|s| s.path.as_str())).collect();
    for rule in policy["rules"].as_array().ok_or("missing forbidden rules")? {
        if rule["activate_at"] != "WI-10" {
            continue;
        }
        let re = regex::Regex::new(rule["pattern"].as_str().ok_or("missing pattern")?)?;
        for prefix in rule["paths"].as_array().ok_or("missing paths")? {
            let prefix = prefix.as_str().ok_or("invalid path")?;
            let base = root.join(prefix);
            let paths = if base.is_file() { vec![base] } else { files(&base)? };
            for path in paths {
                if path.extension().is_none_or(|s| s != "rs") {
                    continue;
                }
                if re.is_match(&fs::read_to_string(&path)?)
                    && !covered.contains(relative(root, &path).as_str())
                {
                    fail(
                        &mut f,
                        "CAT-FORBIDDEN-COVERAGE",
                        &relative(root, &path),
                        "WI-10 forbidden-symbol hit lacks a deletion-plan surface",
                    );
                }
            }
        }
    }
    Ok(f)
}
pub fn check(root: &Path) -> Result<Vec<Finding>> {
    let d: Disposition =
        serde_json::from_value(read(&root.join("inventory/connector-disposition.json"))?)?;
    let c: Catalog = serde_json::from_value(read(&root.join("contracts/wi10-catalog.json"))?)?;
    let s: Sandbox =
        serde_json::from_value(read(&root.join("contracts/wi10-sandbox-disposition.json"))?)?;
    let o: Obligations =
        serde_json::from_value(read(&root.join("contracts/wi10-obligations.json"))?)?;
    let items = inventory::generate(root)?;
    let mut f = validate_disposition(root, &items, &d)?;
    f.extend(validate_catalog(&c));
    let process: ProcessCatalogContract =
        serde_json::from_value(read(&root.join("contracts/worker-process-catalog.json"))?)?;
    f.extend(validate_process_catalog(&process));
    f.extend(validate_sandbox(&s));
    f.extend(validate_obligations(root, &o)?);
    f.extend(validate_forbidden_coverage(root, &d)?);
    Ok(f)
}

/// WIP-12 runtime contract; the historical WI-10 inventory remains definition-only.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessCatalogContract {
    pub version: u32,
    pub scope: String,
    pub operations: Vec<String>,
    pub records: Vec<String>,
    pub binding_fields: Vec<String>,
    pub recovery_operations: Vec<String>,
    pub quarantine_scope: String,
    pub store_profile: String,
    pub ordinary_use: bool,
    pub self_promotion: bool,
    pub runtime_pass_asserted: bool,
}
pub fn validate_process_catalog(c: &ProcessCatalogContract) -> Vec<Finding> {
    if c.version != 1
        || c.scope != "wi/WI-10/process-catalog"
        || !exact(
            &c.operations,
            &["provision", "start", "inspect", "terminate", "collect_artifacts", "release"],
        )
        || !exact(&c.records, &["evaluation", "ordinary"])
        || !exact(
            &c.binding_fields,
            &[
                "profile",
                "profile_definition",
                "descriptor",
                "adapter",
                "executable",
                "helper",
                "dependencies",
                "configuration",
                "policy",
                "controls",
                "aq",
                "wi",
                "environment",
                "fixture",
                "inputs",
                "exports",
            ],
        )
        || !exact(&c.recovery_operations, &["inspect", "terminate", "reconcile"])
        || c.quarantine_scope != "tenant-provider"
        || c.store_profile != "local-control-plane-15"
        || c.ordinary_use
        || c.self_promotion
        || c.runtime_pass_asserted
    {
        vec![finding(
            "CATALOG-PROCESS",
            "contracts/worker-process-catalog.json",
            "contract",
            "WIP-12",
            "exact registration, drift, quarantine and recovery boundaries required",
        )]
    } else {
        vec![]
    }
}

#[cfg(test)]
mod surface_tests {
    use super::*;

    #[test]
    fn root_documentation_is_discovered_without_scanning_frozen_evidence() {
        let temp = tempfile::TempDir::new().unwrap();
        for path in [
            "README.md",
            "CONNECTORS.MD",
            "docs/current.md",
            "docs/adr/historical.md",
            "docs/connector-development.md",
            "contracts/sources/README.md",
            "fixtures/pre-wi-fabric-2/README.md",
            "plans/historical.md",
        ] {
            let path = temp.path().join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "shell.exec sandbox.exec").unwrap();
        }
        let sources = surface_sources(temp.path()).unwrap();
        assert_eq!(
            sources.keys().map(String::as_str).collect::<Vec<_>>(),
            ["CONNECTORS.MD", "README.md", "docs/current.md"]
        );
        for family in ["shell-exec", "sandbox-exec"] {
            let paths = surface_paths(temp.path(), &[], family, &sources);
            assert!(paths.contains("README.md"));
            assert!(paths.contains("CONNECTORS.MD"));
        }
    }
}
