//! AQ-neutral Flow journal. All methods are trusted-host persistence interfaces.
use crate::{Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use worldinterface_domain::{
    agency::AgencyRecord,
    continuation::{ResumeAcceptance, WaitCheckpoint},
    flow::*,
};
use worldinterface_protocol::{
    canonical_collection, strict_collection, ArtifactRef, ConnectorDescriptor, ContentHash,
    EffectIntentId, EffectSubmission, OpaqueRef, PrincipalId,
};

/// Deterministic Flow journal/admission validation failure.
#[derive(Debug)]
pub struct FlowIntegrity(pub String);
impl std::fmt::Display for FlowIntegrity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for FlowIntegrity {}
fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(FlowIntegrity(message.into()))
}
#[derive(Debug)]
pub struct FlowRevisionConflict;
impl std::fmt::Display for FlowRevisionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Flow revision conflict")
    }
}
impl std::error::Error for FlowRevisionConflict {}
#[derive(Debug)]
pub enum FlowCapacity {
    Runs,
    Versions,
    Bytes,
}
impl std::fmt::Display for FlowCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Runs => "Flow run capacity",
            Self::Versions => "Flow version capacity",
            Self::Bytes => "Flow state byte capacity",
        })
    }
}
impl std::error::Error for FlowCapacity {}

/// Match the existing per-tenant effect capacity. Runs retain complete history.
pub const MAX_FLOW_RUNS: usize = 10_000;
pub const MAX_FLOW_VERSIONS: usize = 256;
/// Bounds each run and the definition catalog separately, never all retained runs.
pub const MAX_FLOW_STATE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const SCHEMA: &str = "
CREATE TABLE flow_history(seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, revision INTEGER NOT NULL, entry BLOB NOT NULL, digest TEXT NOT NULL, UNIQUE(tenant,revision));
CREATE TRIGGER flow_no_update BEFORE UPDATE ON flow_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER flow_no_delete BEFORE DELETE ON flow_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE flow_state(tenant TEXT PRIMARY KEY, state BLOB NOT NULL);
CREATE TABLE flow_runs(tenant TEXT NOT NULL, id TEXT NOT NULL, request TEXT NOT NULL, terminal INTEGER NOT NULL, quarantine TEXT, state BLOB NOT NULL, PRIMARY KEY(tenant,id), UNIQUE(tenant,request));
CREATE INDEX flow_pending ON flow_runs(tenant,terminal);
CREATE TABLE flow_tasks(tenant TEXT NOT NULL, task TEXT NOT NULL, run TEXT NOT NULL, PRIMARY KEY(tenant,task));
CREATE INDEX flow_task_lookup ON flow_tasks(task);
";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowPins {
    pub descriptors: Vec<ConnectorDescriptor>,
    pub schemas: BTreeMap<OpaqueRef, Schema>,
    pub submitting_authority: AuthorityUpperBound,
    pub run_authority: AuthorityUpperBound,
}
impl FlowPins {
    pub fn context(&self) -> Result<CompilationContext> {
        Ok(CompilationContext::new(
            self.descriptors.clone(),
            self.schemas.clone(),
            self.submitting_authority.clone(),
            self.run_authority.clone(),
        )?)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildBinding {
    pub version: u32,
    pub queue_store: OpaqueRef,
    pub parent_task: OpaqueRef,
    pub parent_run: OpaqueRef,
    pub producing_attempt: OpaqueRef,
    pub disposition_hash: ContentHash,
    pub admission_sequence: u64,
    pub request: ArtifactRef,
    pub run: OpaqueRef,
    /// Legacy wake evidence. New deadline-based binding checks leave this empty.
    pub wake_request: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeDisposition {
    Succeeded,
    Skipped,
    Denied,
    Failed,
    Canceled,
    Uncertain,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeResult {
    pub disposition: NodeDisposition,
    pub values: BTreeMap<OpaqueRef, Value>,
    pub outputs: BTreeMap<OpaqueRef, ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<OpaqueRef>,
    pub input_hash: ContentHash,
    pub output_hash: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program: Option<Program>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<EffectIntentId>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowOutcome {
    Succeeded,
    Denied,
    Failed,
    Canceled,
    Uncertain,
}
// Plans are canonical UTF-8 JSON; serialized coordinator requests are UTF-8
// JSON too. Keeping their exact bytes in a string avoids parsing thousands of
// integer values each time a run projection or admission history entry is read.
fn serialize_json_bytes<S: serde::Serializer>(
    bytes: &[u8],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(std::str::from_utf8(bytes).map_err(serde::ser::Error::custom)?)
}
fn deserialize_json_bytes<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<u8>, D::Error> {
    Ok(String::deserialize(deserializer)?.into_bytes())
}
/// Reuse only a sealed compiler result for identical bytes, expected hash and
/// complete compilation pins. Run attribution is checked separately on every
/// restore. This cache holds no execution, authorization or branch state.
fn restore_pinned_plan(
    bytes: &[u8],
    expected: &ContentHash,
    pins: &FlowPins,
) -> Result<ExecutionPlan> {
    use std::{
        collections::VecDeque,
        sync::{Mutex, OnceLock},
    };
    type Plans = VecDeque<(ExecutionPlan, FlowPins, usize)>;
    static PLANS: OnceLock<Mutex<Plans>> = OnceLock::new();
    const MAX_ENTRIES: usize = 16;
    const MAX_ENCODED_BYTES: usize = 8 * 1024 * 1024;
    let cache = PLANS.get_or_init(|| Mutex::new(VecDeque::new()));
    let matches = |(p, saved, _): &(ExecutionPlan, FlowPins, usize)| {
        p.hash() == expected && p.canonical_bytes() == bytes && saved == pins
    };
    if let Ok(plans) = cache.lock() {
        if let Some((p, _, _)) = plans.iter().find(|entry| matches(entry)) {
            return Ok(p.clone());
        }
    }
    // A miss, changed pin, poisoned cache or oversized entry always takes the
    // ordinary compiler path. Cache availability never establishes validity.
    let plan = restore_plan(bytes, expected, &pins.context()?)?;
    let Ok(encoded) = canonical_collection(pins) else { return Ok(plan) };
    let cost = bytes.len().saturating_add(encoded.len());
    if cost > MAX_ENCODED_BYTES {
        return Ok(plan);
    }
    if let Ok(mut plans) = cache.lock() {
        if plans.iter().any(matches) {
            return Ok(plan);
        }
        while plans.len() >= MAX_ENTRIES
            || plans.iter().map(|entry| entry.2).sum::<usize>() + cost > MAX_ENCODED_BYTES
        {
            plans.pop_front();
        }
        plans.push_back((plan.clone(), pins.clone(), cost));
    }
    Ok(plan)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowRun {
    pub id: OpaqueRef,
    pub tenant: OpaqueRef,
    pub principal: PrincipalId,
    pub agency: AgencyRecord,
    pub submission: EffectSubmission,
    #[serde(serialize_with = "serialize_json_bytes", deserialize_with = "deserialize_json_bytes")]
    pub plan: Vec<u8>,
    pub plan_hash: ContentHash,
    pub admission_hash: ContentHash,
    pub pins: FlowPins,
    pub origin: PlanOrigin,
    pub queue_store: OpaqueRef,
    #[serde(serialize_with = "serialize_json_bytes", deserialize_with = "deserialize_json_bytes")]
    pub coordinator_request: Vec<u8>,
    pub coordinator_task: OpaqueRef,
    pub node_tasks: BTreeMap<OpaqueRef, OpaqueRef>,
    pub wait_id: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<Vec<u8>>,
    #[serde(default)]
    pub cancel_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run: Option<OpaqueRef>,
    pub bindings: BTreeMap<OpaqueRef, ChildBinding>,
    #[serde(default)]
    pub wait_cursors: BTreeMap<OpaqueRef, u64>,
    #[serde(default)]
    pub waits: BTreeMap<OpaqueRef, WaitCheckpoint>,
    #[serde(default)]
    pub accepted_resumes: BTreeMap<String, ResumeAcceptance>,
    pub wakes_delivered: BTreeMap<OpaqueRef, u64>,
    pub results: BTreeMap<OpaqueRef, NodeResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<FlowOutcome>,
    pub exports: BTreeMap<OpaqueRef, ArtifactRef>,
}

#[cfg(test)]
#[test]
fn canonical_json_byte_storage_preserves_exact_bytes_and_hashes() {
    let bytes = canonical_collection(&("é界\n\"\\", [1, 2, 3])).unwrap();
    let encoded = serialize_json_bytes(&bytes, serde_json::value::Serializer).unwrap();
    assert_eq!(encoded.as_str().unwrap().as_bytes(), bytes);
    let decoded = deserialize_json_bytes(encoded).unwrap();
    assert_eq!(decoded, bytes);
    assert_eq!(ContentHash::of(&decoded), ContentHash::of(&bytes));
    assert!(serialize_json_bytes(&[0xff], serde_json::value::Serializer).is_err());
    assert!(deserialize_json_bytes(serde_json::json!([123, 125])).is_err());
}
impl FlowRun {
    pub fn admission_digest(&self) -> Result<ContentHash> {
        Ok(ContentHash::of(
            &canonical_collection(&(
                "wi-flow-admission-1",
                &self.id,
                &self.tenant,
                &self.principal,
                &self.agency,
                &self.submission,
                &self.plan_hash,
                &self.pins,
                &self.origin,
                &self.queue_store,
                &self.coordinator_request,
                &self.coordinator_task,
                &self.node_tasks,
                &self.wait_id,
            ))
            .map_err(invalid)?,
        ))
    }
    pub fn restore(&self) -> Result<ExecutionPlan> {
        if self.admission_digest()? != self.admission_hash {
            return Err(invalid("Flow admission hash mismatch"));
        }
        let p = restore_pinned_plan(&self.plan, &self.plan_hash, &self.pins)?;
        if p.document().origin != self.origin
            || self.agency.binding.tenant != self.tenant
            || self.agency.binding.principal != self.principal
            || self.node_tasks.keys().ne(p.document().body.nodes.iter().map(|n| &n.id))
        {
            return Err(invalid("Flow admission identity mismatch"));
        }
        Ok(p)
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowState {
    pub revision: u64,
    /// Retained admissions across all run outcomes, reconstructed from history.
    pub retained_run_count: usize,
    pub versions: BTreeMap<OpaqueRef, BTreeMap<u64, FlowDefinition>>,
    pub active: BTreeMap<OpaqueRef, u64>,
    #[serde(default)]
    pub activation_preconditions: BTreeMap<OpaqueRef, u64>,
    #[serde(default)]
    pub quarantined: BTreeMap<OpaqueRef, String>,
    pub runs: BTreeMap<OpaqueRef, FlowRun>,
    pub requests: BTreeMap<String, OpaqueRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FlowCommand {
    RebindCheckpoint {
        run: OpaqueRef,
        node: OpaqueRef,
        expected: Vec<u8>,
        checkpoint: Vec<u8>,
    },
    SetWaitCursor {
        run: OpaqueRef,
        node: OpaqueRef,
        sequence: u64,
    },
    PrepareWait {
        run: OpaqueRef,
        wait: WaitCheckpoint,
    },
    AcceptResume {
        run: OpaqueRef,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node: Option<OpaqueRef>,
        acceptance: ResumeAcceptance,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<NodeResult>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_wait: Option<WaitCheckpoint>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finish: Option<(FlowOutcome, BTreeMap<OpaqueRef, ArtifactRef>)>,
    },
    CancelRequested {
        run: OpaqueRef,
    },
    QuarantineRun {
        run: OpaqueRef,
        diagnostic: String,
    },
    InsertVersion {
        definition: FlowDefinition,
    },
    ActivateVersion {
        id: OpaqueRef,
        version: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        previous: Option<u64>,
    },
    AdmitRun {
        run: Box<FlowRun>,
    },
    BindCoordinator {
        run: OpaqueRef,
        parent_run: OpaqueRef,
    },
    PrepareChildren {
        run: OpaqueRef,
        parent_run: OpaqueRef,
        proposal: Vec<u8>,
    },
    BindChild {
        run: OpaqueRef,
        node: OpaqueRef,
        binding: ChildBinding,
    },
    WakeDelivered {
        run: OpaqueRef,
        node: OpaqueRef,
        sequence: u64,
    },
    RecordNodeResult {
        run: OpaqueRef,
        node: OpaqueRef,
        result: NodeResult,
    },
    Finish {
        run: OpaqueRef,
        outcome: FlowOutcome,
        exports: BTreeMap<OpaqueRef, ArtifactRef>,
    },
}
pub fn resume_key(a: &ResumeAcceptance) -> Result<String> {
    Ok(String::from_utf8(
        canonical_collection(&(&a.queue_store, &a.run, a.context_id)).map_err(invalid)?,
    )?)
}
fn request_key(run: &FlowRun) -> Result<String> {
    Ok(String::from_utf8(
        canonical_collection(&(&run.principal, &run.submission.client_request_id))
            .map_err(invalid)?,
    )?)
}
fn reduce(old: &FlowState, tenant: &OpaqueRef, cmd: &FlowCommand) -> Result<FlowState> {
    let mut s = old.clone();
    if let FlowCommand::AcceptResume { run, node, acceptance, result, next_wait, finish } = cmd {
        let r = old.runs.get(run).ok_or_else(|| invalid("unknown resume Flow"))?;
        let key = resume_key(acceptance)?;
        if let Some(previous) = r.accepted_resumes.get(&key) {
            if previous != acceptance {
                return Err(invalid("conflicting resume context"));
            }
            return Ok(s);
        }
        if acceptance.queue_store != r.queue_store
            || acceptance.context_id == 0
            || acceptance.disposition.is_empty()
            || acceptance.disposition.len() > 65536
            || node.as_ref().map_or(r.parent_run.as_ref(), |n| r.bindings.get(n).map(|b| &b.run))
                != Some(&acceptance.run)
            || r.cancel_requested
            || r.outcome.is_some()
        {
            return Err(invalid("invalid resume binding"));
        }
        if let Some(node) = node {
            if !r.waits.contains_key(node) {
                return Err(invalid("resume without prepared wait"));
            }
        }
        if let Some(result) = result {
            s = reduce(
                &s,
                tenant,
                &FlowCommand::RecordNodeResult {
                    run: run.clone(),
                    node: node.clone().ok_or_else(|| invalid("result without node"))?,
                    result: result.clone(),
                },
            )?;
        }
        if let Some((outcome, exports)) = finish {
            s = reduce(
                &s,
                tenant,
                &FlowCommand::Finish {
                    run: run.clone(),
                    outcome: outcome.clone(),
                    exports: exports.clone(),
                },
            )?;
        }
        if let Some(wait) = next_wait {
            if node.as_ref() != Some(&wait.node)
                || wait.queue_store != r.queue_store
                || wait.run != acceptance.run
                || wait.plan_hash != r.plan_hash
                || result.is_some()
                || finish.is_some()
            {
                return Err(invalid("invalid replacement wait"));
            }
            s.runs.get_mut(run).unwrap().waits.insert(wait.node.clone(), wait.clone());
        }
        s.runs.get_mut(run).unwrap().accepted_resumes.insert(key, acceptance.clone());
        s.revision = old.revision + 1;
        return Ok(s);
    }
    match cmd {
        FlowCommand::QuarantineRun { run, diagnostic } => {
            if !s.runs.contains_key(run) || diagnostic.len() > 1024 {
                return Err(invalid("invalid Flow quarantine"));
            }
            s.quarantined.entry(run.clone()).or_insert_with(|| diagnostic.clone());
        }
        FlowCommand::InsertVersion { definition: d } => {
            if s.versions.values().map(BTreeMap::len).sum::<usize>() >= MAX_FLOW_VERSIONS
                && s.versions.get(&d.id).and_then(|v| v.get(&d.version)).is_none()
            {
                return Err(FlowCapacity::Versions.into());
            }
            let versions = s.versions.entry(d.id.clone()).or_default();
            if let Some(v) = versions.get(&d.version) {
                if v == d {
                    return Ok(s);
                }
                return Err(invalid("immutable Flow version conflict"));
            }
            let previous = versions.last_key_value();
            if d.version != previous.map_or(1, |(v, _)| v + 1)
                || d.predecessor
                    != previous.map(|(_, v)| v.digest()).transpose().map_err(invalid)?
            {
                return Err(invalid("Flow predecessor mismatch"));
            }
            versions.insert(d.version, d.clone());
        }
        FlowCommand::ActivateVersion { id, version, previous } => {
            if *previous == Some(0) {
                return Err(invalid("invalid Flow activation precondition"));
            }
            if s.versions.get(id).and_then(|v| v.get(version)).is_none() {
                return Err(invalid("unknown Flow version"));
            }
            if s.active.get(id) == Some(version) {
                if s.activation_preconditions.get(id) != Some(&previous.unwrap_or(0)) {
                    return Err(invalid("Flow activation precondition conflict"));
                }
                return Ok(s);
            }
            if s.active.get(id) != previous.as_ref() {
                return Err(invalid("Flow activation conflict"));
            }
            s.active.insert(id.clone(), *version);
            s.activation_preconditions.insert(id.clone(), previous.unwrap_or(0));
        }
        FlowCommand::AdmitRun { run: r } => {
            let p = r.restore()?;
            if let Some(existing) = s.runs.get(&r.id) {
                if existing.admission_hash == r.admission_hash {
                    return Ok(s);
                }
                return Err(invalid("Flow run conflict"));
            }
            if &r.tenant != tenant
                || r.cancel_requested
                || r.proposal.is_some()
                || r.parent_run.is_some()
                || !r.bindings.is_empty()
                || !r.results.is_empty()
                || !r.exports.is_empty()
                || r.outcome.is_some()
                || !r.wakes_delivered.is_empty()
                || !r.wait_cursors.is_empty()
                || !r.waits.is_empty()
                || !r.accepted_resumes.is_empty()
            {
                return Err(invalid("invalid initial Flow run"));
            }
            if let PlanOrigin::Flow { id, version, definition_digest, .. } = &r.origin {
                if s.active.get(id) != Some(version) {
                    return Err(invalid("inactive Flow definition"));
                }
                let d = s
                    .versions
                    .get(id)
                    .and_then(|v| v.get(version))
                    .ok_or_else(|| invalid("unregistered Flow definition"))?;
                if &d.digest().map_err(invalid)? != definition_digest
                    || compile_flow(d, &r.pins.context()?)?.hash() != &r.plan_hash
                {
                    return Err(invalid("Flow definition pin mismatch"));
                }
            } else {
                let PlanOperation::Boundary { binding, .. } = &p.document().body.nodes[0].operation
                else {
                    return Err(invalid("invalid Direct plan"));
                };
                if compile_direct(
                    &r.submission,
                    &DirectOperationSelection {
                        connector: binding.descriptor.connector.clone(),
                        operation: binding.descriptor.operation.clone(),
                    },
                    &r.pins.context()?,
                )?
                .hash()
                    != &r.plan_hash
                {
                    return Err(invalid("Direct attribution/plan mismatch"));
                }
            }
            let key = request_key(r)?;
            if s.requests.contains_key(&key) {
                return Err(invalid("Flow request conflict"));
            }
            if s.retained_run_count >= MAX_FLOW_RUNS {
                return Err(FlowCapacity::Runs.into());
            }
            s.retained_run_count += 1;
            s.requests.insert(key, r.id.clone());
            s.runs.insert(r.id.clone(), *r.clone());
        }
        _ => {
            let id = match cmd {
                FlowCommand::RebindCheckpoint { run, .. }
                | FlowCommand::SetWaitCursor { run, .. }
                | FlowCommand::PrepareWait { run, .. }
                | FlowCommand::CancelRequested { run }
                | FlowCommand::BindCoordinator { run, .. }
                | FlowCommand::PrepareChildren { run, .. }
                | FlowCommand::BindChild { run, .. }
                | FlowCommand::WakeDelivered { run, .. }
                | FlowCommand::RecordNodeResult { run, .. }
                | FlowCommand::Finish { run, .. } => run,
                _ => unreachable!(),
            };
            let r = s.runs.get_mut(id).ok_or_else(|| invalid("unknown Flow run"))?;
            // Admission validates the immutable plan in full. These commands
            // cannot change it. Check attribution here; execution and recovery
            // recompile against the expected hash through FlowRun::restore.
            if r.admission_digest()? != r.admission_hash {
                return Err(invalid("Flow admission hash mismatch"));
            }
            if r.outcome.is_some() && !matches!(cmd, FlowCommand::Finish { .. }) {
                return Err(invalid("Flow run finished"));
            }
            match cmd {
                FlowCommand::RebindCheckpoint { node, expected, checkpoint, .. } => {
                    if r.results.contains_key(node)
                        || checkpoint.is_empty()
                        || checkpoint.len() > 16384
                    {
                        return Err(invalid("invalid checkpoint rebind"));
                    }
                    let w = r
                        .waits
                        .get_mut(node)
                        .ok_or_else(|| invalid("missing checkpoint to rebind"))?;
                    if w.checkpoint == *checkpoint {
                        return Ok(s);
                    }
                    if w.checkpoint != *expected {
                        return Err(invalid("checkpoint rebind conflict"));
                    }
                    w.checkpoint = checkpoint.clone();
                }
                FlowCommand::SetWaitCursor { node, sequence, .. } => {
                    if !r.bindings.contains_key(node) || r.results.contains_key(node) {
                        return Err(invalid("unbound wait cursor"));
                    }
                    if let Some(old) = r.wait_cursors.get(node) {
                        if old != sequence {
                            return Err(invalid("wait cursor conflict"));
                        }
                        return Ok(s);
                    }
                    r.wait_cursors.insert(node.clone(), *sequence);
                }
                FlowCommand::PrepareWait { wait, .. } => {
                    if let Some(old) = r.waits.get(&wait.node) {
                        if old == wait {
                            return Ok(s);
                        }
                        return Err(invalid("wait checkpoint conflict"));
                    }
                    if r.cancel_requested
                        || wait.queue_store != r.queue_store
                        || wait.plan_hash != r.plan_hash
                        || r.bindings.get(&wait.node).is_none_or(|b| b.run != wait.run)
                        || wait
                            .route
                            .as_ref()
                            .is_some_and(|v| v.tenant != r.tenant || v.audience != r.principal)
                        || wait.wait.is_empty()
                        || wait.wait.len() > 16384
                        || wait.checkpoint.is_empty()
                        || wait.checkpoint.len() > 16384
                        || r.results.contains_key(&wait.node)
                    {
                        return Err(invalid("invalid wait checkpoint binding"));
                    }
                    r.waits.insert(wait.node.clone(), wait.clone());
                }
                FlowCommand::CancelRequested { .. } => {
                    r.cancel_requested = true;
                }
                FlowCommand::BindCoordinator { parent_run, .. } => {
                    if let Some(old) = &r.parent_run {
                        if old == parent_run {
                            return Ok(s);
                        }
                        return Err(invalid("coordinator run conflict"));
                    }
                    r.parent_run = Some(parent_run.clone());
                }
                FlowCommand::PrepareChildren { parent_run, proposal, .. } => {
                    if let Some(old) = &r.proposal {
                        if old == proposal && r.parent_run.as_ref() == Some(parent_run) {
                            return Ok(s);
                        }
                        return Err(invalid("Flow proposal conflict"));
                    }
                    if r.parent_run.as_ref() != Some(parent_run) {
                        return Err(invalid("unverified coordinator run"));
                    }
                    r.proposal = Some(proposal.clone());
                }
                FlowCommand::BindChild { node, binding, .. } => {
                    if let Some(old) = r.bindings.get(node) {
                        if old == binding {
                            return Ok(s);
                        }
                        return Err(invalid("Flow binding conflict"));
                    }
                    if r.bindings.values().any(|b| b.run == binding.run)
                        || binding.version != 1
                        || binding.queue_store != r.queue_store
                        || binding.parent_task != r.coordinator_task
                        || Some(&binding.parent_run) != r.parent_run.as_ref()
                        || binding.admission_sequence == 0
                        || !r.node_tasks.contains_key(node)
                        || &binding.request.scope != tenant
                        || r.proposal.is_none()
                    {
                        return Err(invalid("invalid Flow child binding"));
                    }
                    r.bindings.insert(node.clone(), binding.clone());
                }
                FlowCommand::WakeDelivered { node, sequence, .. } => {
                    if !r.bindings.contains_key(node) || *sequence == 0 {
                        return Err(invalid("unbound Flow wake"));
                    }
                    if let Some(old) = r.wakes_delivered.get(node) {
                        if old == sequence {
                            return Ok(s);
                        }
                        return Err(invalid("Flow wake conflict"));
                    }
                    r.wakes_delivered.insert(node.clone(), *sequence);
                }
                FlowCommand::RecordNodeResult { node, result, .. } => {
                    if let Some(old) = r.results.get(node) {
                        if old == result {
                            return Ok(s);
                        }
                        return Err(invalid("Flow result conflict"));
                    }
                    if !r.bindings.contains_key(node)
                        || result.outputs.values().any(|v| &v.scope != tenant)
                    {
                        return Err(invalid("unbound Flow result"));
                    }
                    r.results.insert(node.clone(), result.clone());
                }
                FlowCommand::Finish { outcome, exports, .. } => {
                    if let Some(old) = &r.outcome {
                        if old == outcome && &r.exports == exports {
                            return Ok(s);
                        }
                        return Err(invalid("Flow outcome conflict"));
                    }
                    if *outcome == FlowOutcome::Succeeded
                        && (exports.keys().ne(r.restore()?.document().body.outputs.keys())
                            || r.results.len() != r.node_tasks.len()
                            || r.results.values().any(|v| {
                                !matches!(
                                    v.disposition,
                                    NodeDisposition::Succeeded | NodeDisposition::Skipped
                                )
                            }))
                    {
                        return Err(invalid("AQ closure is not Flow success"));
                    }
                    if *outcome == FlowOutcome::Succeeded {
                        for (name, input) in &r.restore()?.document().body.outputs {
                            let source = match &input.source {
                                InputSource::Output { source } => Some(source),
                                InputSource::Exclusive { branch, when_true, when_false } => {
                                    let plan = r.restore()?;
                                    let branch_node = plan
                                        .document()
                                        .body
                                        .nodes
                                        .iter()
                                        .find(|n| &n.id == branch)
                                        .ok_or_else(|| invalid("unknown export branch"))?;
                                    let PlanOperation::Branch {
                                        when_true: yes,
                                        when_false: no,
                                        ..
                                    } = &branch_node.operation
                                    else {
                                        return Err(invalid("invalid export branch"));
                                    };
                                    let selected =
                                        r.results.get(branch).and_then(|v| v.branch.as_ref());
                                    if selected == Some(yes) {
                                        Some(when_true)
                                    } else if selected == Some(no) {
                                        Some(when_false)
                                    } else {
                                        return Err(invalid("unsettled export branch"));
                                    }
                                }
                                InputSource::Artifact { reference } => {
                                    if exports.get(name) != Some(reference) {
                                        return Err(invalid("Flow export value mismatch"));
                                    }
                                    None
                                }
                                InputSource::Literal { value } => {
                                    let expected = ContentHash::of(
                                        &worldinterface_protocol::canonical_data(value)
                                            .map_err(invalid)?,
                                    );
                                    if exports.get(name).is_none_or(|a| a.content != expected) {
                                        return Err(invalid("Flow export value mismatch"));
                                    }
                                    None
                                }
                            };
                            if let Some(source) = source {
                                if exports.get(name)
                                    != r.results
                                        .get(&source.node)
                                        .and_then(|v| v.outputs.get(&source.port))
                                {
                                    return Err(invalid("Flow export value mismatch"));
                                }
                            }
                        }
                    }
                    if exports.values().any(|v| &v.scope != tenant) {
                        return Err(invalid("foreign Flow export"));
                    }
                    r.outcome = Some(outcome.clone());
                    r.exports = exports.clone();
                }
                _ => unreachable!(),
            }
        }
    }
    if s != *old {
        s.revision += 1;
    }
    Ok(s)
}
#[derive(Serialize, Deserialize)]
struct Entry {
    tenant: OpaqueRef,
    expected: u64,
    command: FlowCommand,
}
fn decode<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    Ok(serde_json::from_value(strict_collection(b).map_err(invalid)?)?)
}
fn catalog(c: &Connection, tenant: &OpaqueRef) -> Result<FlowState> {
    let b: Option<Vec<u8>> = c
        .query_row("SELECT state FROM flow_state WHERE tenant=?", [tenant.as_str()], |r| r.get(0))
        .optional()?;
    b.map_or_else(|| Ok(FlowState::default()), |b| decode(&b))
}
// Load only the requested run(s). Finished history never enters a scheduling write.
fn view(
    c: &Connection,
    tenant: &OpaqueRef,
    id: Option<&OpaqueRef>,
    pending: bool,
) -> Result<FlowState> {
    let mut s = catalog(c, tenant)?;
    let sql = if id.is_some() {
        "SELECT state,quarantine FROM flow_runs WHERE tenant=?1 AND id=?2"
    } else if pending {
        // BTreeMap supplies deterministic order. Force the unfinished-run
        // index so SQLite cannot trade that filter for a tenant-wide sort scan.
        "SELECT state,quarantine FROM flow_runs INDEXED BY flow_pending WHERE tenant=?1 AND terminal=0 AND ?2 IS NULL"
    } else {
        "SELECT state,quarantine FROM flow_runs WHERE tenant=?1 AND ?2 IS NULL ORDER BY id"
    };
    let mut q = c.prepare(sql)?;
    for row in q.query_map(params![tenant.as_str(), id.map(OpaqueRef::as_str)], |r| {
        Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<String>>(1)?))
    })? {
        let (bytes, quarantine) = row?;
        let r: FlowRun = decode(&bytes)?;
        s.requests.insert(request_key(&r)?, r.id.clone());
        if let Some(diagnostic) = quarantine {
            s.quarantined.insert(r.id.clone(), diagnostic);
        }
        s.runs.insert(r.id.clone(), r);
    }
    Ok(s)
}
fn read_view(
    root: &StoreRoot,
    tenant: &OpaqueRef,
    run: Option<&OpaqueRef>,
    pending: bool,
) -> Result<FlowState> {
    let c = root.connect(StoreKind::Flow)?;
    let tx = c.unchecked_transaction()?;
    let s = view(&tx, tenant, run, pending)?;
    tx.commit()?;
    Ok(s)
}
fn command_view(c: &Connection, tenant: &OpaqueRef, cmd: &FlowCommand) -> Result<FlowState> {
    let id = match cmd {
        FlowCommand::InsertVersion { .. } | FlowCommand::ActivateVersion { .. } => {
            return catalog(c, tenant)
        }
        FlowCommand::AdmitRun { run } => &run.id,
        FlowCommand::RebindCheckpoint { run, .. }
        | FlowCommand::SetWaitCursor { run, .. }
        | FlowCommand::PrepareWait { run, .. }
        | FlowCommand::AcceptResume { run, .. }
        | FlowCommand::CancelRequested { run }
        | FlowCommand::QuarantineRun { run, .. }
        | FlowCommand::BindCoordinator { run, .. }
        | FlowCommand::PrepareChildren { run, .. }
        | FlowCommand::BindChild { run, .. }
        | FlowCommand::WakeDelivered { run, .. }
        | FlowCommand::RecordNodeResult { run, .. }
        | FlowCommand::Finish { run, .. } => run,
    };
    let mut s = view(c, tenant, Some(id), false)?;
    if let FlowCommand::AdmitRun { run } = cmd {
        let key = request_key(run)?;
        let winner: Option<String> = c
            .query_row(
                "SELECT id FROM flow_runs WHERE tenant=? AND request=?",
                params![tenant.as_str(), key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(winner) = winner {
            s.requests.insert(key, winner.try_into().map_err(invalid)?);
        }
    }
    Ok(s)
}
fn state_bytes<T: Serialize>(state: &T) -> Result<Vec<u8>> {
    let bytes = canonical_collection(state).map_err(|_| FlowCapacity::Bytes)?;
    if bytes.len() > MAX_FLOW_STATE_BYTES {
        return Err(FlowCapacity::Bytes.into());
    }
    Ok(bytes)
}
fn write(c: &Connection, tenant: &OpaqueRef, s: &FlowState) -> Result<()> {
    for r in s.runs.values() {
        c.execute("INSERT INTO flow_runs VALUES(?,?,?,?,?,?) ON CONFLICT(tenant,id) DO UPDATE SET terminal=excluded.terminal,quarantine=excluded.quarantine,state=excluded.state", params![tenant.as_str(),r.id.as_str(),request_key(r)?,r.outcome.is_some(),s.quarantined.get(&r.id),state_bytes(r)?])?;
        for task in std::iter::once(&r.coordinator_task).chain(r.node_tasks.values()) {
            c.execute(
                "INSERT OR IGNORE INTO flow_tasks VALUES(?,?,?)",
                params![tenant.as_str(), task.as_str(), r.id.as_str()],
            )?;
        }
    }
    let mut metadata = s.clone();
    metadata.runs.clear();
    metadata.requests.clear();
    metadata.quarantined.clear();
    c.execute(
        "INSERT INTO flow_state VALUES(?,?) ON CONFLICT(tenant) DO UPDATE SET state=excluded.state",
        params![tenant.as_str(), state_bytes(&metadata)?],
    )?;
    Ok(())
}
pub(crate) fn replay(source: &Connection, target: &Connection) -> Result<()> {
    let mut q =
        source.prepare("SELECT tenant,revision,entry,digest FROM flow_history ORDER BY seq")?;
    for row in q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, u64>(1)?,
            r.get::<_, Vec<u8>>(2)?,
            r.get::<_, String>(3)?,
        ))
    })? {
        let (tenant, rev, b, hash) = row?;
        let e: Entry = decode(&b)?;
        let old = command_view(target, &e.tenant, &e.command)?;
        let new = reduce(&old, &e.tenant, &e.command)?;
        if e.tenant.as_str() != tenant
            || e.expected != old.revision
            || new.revision != rev
            || rev != e.expected + 1
            || canonical_collection(&e).map_err(invalid)? != b
            || ContentHash::of(&b).as_str() != hash
        {
            return Err(invalid("corrupt Flow history"));
        }
        write(target, &e.tenant, &new)?;
        crate::continuation::flow_events(target, &e.tenant, &old, &new)?;
    }
    Ok(())
}
pub struct FlowDispatchGuard<'a> {
    root: &'a StoreRoot,
    _lock: std::fs::File,
}
impl StoreRoot {
    /// Fences Start against cancellation and Finish across all handles and
    /// processes opening this bundle. Each guard owns a distinct file descriptor;
    /// the shared root lock only excludes offline maintenance.
    pub fn flow_dispatch_guard(&self) -> Result<FlowDispatchGuard<'_>> {
        let lock = std::fs::File::open(self.path.join("flow-dispatch.lock"))?;
        fs2::FileExt::lock_exclusive(&lock)?;
        Ok(FlowDispatchGuard { root: self, _lock: lock })
    }
    fn validate_flow_command_artifacts(&self, tenant: &OpaqueRef, cmd: &FlowCommand) -> Result<()> {
        let check = |a: &ArtifactRef| -> Result<()> {
            if &a.scope != tenant {
                return Err(invalid("foreign Flow artifact"));
            }
            self.read_artifact(a)?;
            Ok(())
        };
        match cmd {
            FlowCommand::AdmitRun { run } => {
                check(&run.submission.input.artifact)?;
                for n in &run.restore()?.document().body.nodes {
                    for input in n.inputs.values() {
                        if let InputSource::Artifact { reference } = &input.source {
                            check(reference)?;
                        }
                    }
                }
            }
            FlowCommand::BindChild { binding, .. } => check(&binding.request)?,
            FlowCommand::PrepareWait { wait, .. }
            | FlowCommand::AcceptResume { next_wait: Some(wait), .. } => check(&wait.artifact)?,
            FlowCommand::RecordNodeResult { result, .. }
            | FlowCommand::AcceptResume { result: Some(result), .. } => {
                for a in result.outputs.values() {
                    check(a)?;
                }
            }
            FlowCommand::Finish { exports, .. } => {
                for a in exports.values() {
                    check(a)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub fn flow_snapshot(&self, tenant: &OpaqueRef) -> Result<FlowState> {
        read_view(self, tenant, None, false)
    }
    /// Definition catalog and revision, without retained run history.
    pub fn flow_catalog(&self, tenant: &OpaqueRef) -> Result<FlowState> {
        catalog(&self.connect(StoreKind::Flow)?, tenant)
    }
    pub fn flow_run_snapshot(&self, tenant: &OpaqueRef, run: &OpaqueRef) -> Result<FlowState> {
        read_view(self, tenant, Some(run), false)
    }
    pub fn flow_pending_snapshot(&self, tenant: &OpaqueRef) -> Result<FlowState> {
        read_view(self, tenant, None, true)
    }
    pub fn flow_request(
        &self,
        tenant: &OpaqueRef,
        principal: &PrincipalId,
        request: &worldinterface_protocol::ClientRequestId,
    ) -> Result<Option<FlowRun>> {
        let c = self.connect(StoreKind::Flow)?;
        let key = String::from_utf8(canonical_collection(&(principal, request)).map_err(invalid)?)?;
        let bytes: Option<Vec<u8>> = c
            .query_row(
                "SELECT state FROM flow_runs WHERE tenant=? AND request=?",
                params![tenant.as_str(), key],
                |r| r.get(0),
            )
            .optional()?;
        bytes.map(|b| decode(&b)).transpose()
    }
    pub fn flow_task(&self, task: &str) -> Result<Option<(OpaqueRef, OpaqueRef)>> {
        let c = self.connect(StoreKind::Flow)?;
        let row: Option<(String, String)> = c
            .query_row("SELECT tenant,run FROM flow_tasks WHERE task=?", [task], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;
        row.map(|(t, r)| Ok((t.try_into().map_err(invalid)?, r.try_into().map_err(invalid)?)))
            .transpose()
    }
    pub fn flow_tenants(&self) -> Result<Vec<OpaqueRef>> {
        let c = self.connect(StoreKind::Flow)?;
        let mut q = c.prepare("SELECT tenant FROM flow_state ORDER BY tenant")?;
        let rows = q.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|r| r?.try_into().map_err(invalid)).collect()
    }
    pub fn flow_transact(
        &self,
        tenant: &OpaqueRef,
        expected: u64,
        command: FlowCommand,
    ) -> Result<FlowState> {
        let guard = self.flow_dispatch_guard()?;
        self.flow_transact_guarded(&guard, tenant, expected, command)
    }
    /// Apply a command and return only its catalog and affected run.
    pub fn flow_update(
        &self,
        tenant: &OpaqueRef,
        expected: u64,
        command: FlowCommand,
    ) -> Result<FlowState> {
        let guard = self.flow_dispatch_guard()?;
        self.flow_update_guarded(&guard, tenant, expected, command)
    }
    pub fn flow_transact_guarded(
        &self,
        guard: &FlowDispatchGuard<'_>,
        tenant: &OpaqueRef,
        expected: u64,
        command: FlowCommand,
    ) -> Result<FlowState> {
        self.flow_update_guarded(guard, tenant, expected, command)?;
        self.flow_snapshot(tenant)
    }
    /// Caller holds this root's dispatch guard across reading outcome evidence
    /// and writing Finish, preventing a concurrent Start from being hidden.
    pub fn flow_update_guarded(
        &self,
        guard: &FlowDispatchGuard<'_>,
        tenant: &OpaqueRef,
        expected: u64,
        command: FlowCommand,
    ) -> Result<FlowState> {
        if !std::ptr::eq(self, guard.root) {
            return Err(invalid("foreign Flow dispatch guard"));
        }
        #[cfg(all(feature = "crash-points", not(test)))]
        if matches!(command, FlowCommand::BindChild { .. }) {
            crate::crashpoint::checkpoint("flow-child-before-bind");
        }
        self.validate_flow_command_artifacts(tenant, &command)?;
        let mut c = self.connect(StoreKind::Flow)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old = command_view(&tx, tenant, &command)?;
        let new = reduce(&old, tenant, &command)?;
        if new == old {
            return Ok(old);
        }
        if old.revision != expected {
            return Err(FlowRevisionConflict.into());
        }
        let e = Entry { tenant: tenant.clone(), expected, command };
        let b = canonical_collection(&e).map_err(invalid)?;
        tx.execute(
            "INSERT INTO flow_history(tenant,revision,entry,digest) VALUES(?,?,?,?)",
            params![tenant.as_str(), new.revision, &b, ContentHash::of(&b).as_str()],
        )?;
        write(&tx, tenant, &new)?;
        crate::continuation::flow_events(&tx, tenant, &old, &new)?;
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("flow-before-commit");
        tx.commit()?;
        #[cfg(all(feature = "crash-points", not(test)))]
        match &e.command {
            FlowCommand::PrepareChildren { .. } => {
                crate::crashpoint::checkpoint("flow-proposal-committed")
            }
            FlowCommand::BindCoordinator { .. } => {
                crate::crashpoint::checkpoint("flow-coordinator-bound")
            }
            FlowCommand::BindChild { .. } => crate::crashpoint::checkpoint("flow-child-bound"),
            FlowCommand::PrepareWait { .. } => {
                crate::crashpoint::checkpoint("continuation-wait-prepared")
            }
            FlowCommand::AcceptResume { .. } => {
                crate::crashpoint::checkpoint("continuation-resume-accepted")
            }
            _ => {}
        }
        Ok(new)
    }
    pub(crate) fn validate_flow_artifacts(&self) -> Result<()> {
        let c = Connection::open_with_flags(
            self.db_path(StoreKind::Flow),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let mut q = c.prepare("SELECT state FROM flow_runs")?;
        for b in q.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
            let r: FlowRun = decode(&b?)?;
            {
                let p = r.restore()?;
                self.read_artifact(&r.submission.input.artifact)?;
                for n in &p.document().body.nodes {
                    for input in n.inputs.values() {
                        if let InputSource::Artifact { reference } = &input.source {
                            if reference.scope != r.tenant {
                                return Err(invalid("foreign Flow artifact"));
                            }
                            self.read_artifact(reference)?;
                        }
                    }
                }
                for w in r.waits.values() {
                    self.read_artifact(&w.artifact)?;
                }
                for b in r.bindings.values() {
                    self.read_artifact(&b.request)?;
                }
                for a in
                    r.results.values().flat_map(|v| v.outputs.values()).chain(r.exports.values())
                {
                    self.read_artifact(a)?;
                }
            }
        }
        Ok(())
    }
}
