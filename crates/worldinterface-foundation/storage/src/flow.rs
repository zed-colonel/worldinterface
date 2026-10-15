//! AQ-neutral Flow journal. All methods are trusted-host persistence interfaces.
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use worldinterface_domain::{agency::AgencyRecord, flow::*};
use worldinterface_protocol::{
    canonical_collection, strict_collection, ArtifactRef, ConnectorDescriptor, ContentHash,
    EffectIntentId, EffectSubmission, OpaqueRef, PrincipalId,
};

/// Bounded embedded profile: retain up to 256 runs and 64 MiB per tenant.
/// A full tenant requires an offline archive/new bundle before new admission.
pub const MAX_FLOW_RUNS: usize = 256;
pub const MAX_FLOW_STATE_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const SCHEMA: &str = "
CREATE TABLE flow_history(seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, revision INTEGER NOT NULL, entry BLOB NOT NULL, digest TEXT NOT NULL, UNIQUE(tenant,revision));
CREATE TRIGGER flow_no_update BEFORE UPDATE ON flow_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER flow_no_delete BEFORE DELETE ON flow_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE flow_state(tenant TEXT PRIMARY KEY, state BLOB NOT NULL);
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowRun {
    pub id: OpaqueRef,
    pub tenant: OpaqueRef,
    pub principal: PrincipalId,
    pub agency: AgencyRecord,
    pub submission: EffectSubmission,
    pub plan: Vec<u8>,
    pub plan_hash: ContentHash,
    pub admission_hash: ContentHash,
    pub pins: FlowPins,
    pub origin: PlanOrigin,
    pub queue_store: OpaqueRef,
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
    pub wakes_delivered: BTreeMap<OpaqueRef, u64>,
    pub results: BTreeMap<OpaqueRef, NodeResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<FlowOutcome>,
    pub exports: BTreeMap<OpaqueRef, ArtifactRef>,
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
        let p = restore_plan(&self.plan, &self.plan_hash, &self.pins.context()?)?;
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
fn request_key(run: &FlowRun) -> Result<String> {
    Ok(String::from_utf8(
        canonical_collection(&(&run.principal, &run.submission.client_request_id))
            .map_err(invalid)?,
    )?)
}
fn reduce(old: &FlowState, tenant: &OpaqueRef, cmd: &FlowCommand) -> Result<FlowState> {
    let mut s = old.clone();
    match cmd {
        FlowCommand::QuarantineRun { run, diagnostic } => {
            if !s.runs.contains_key(run) || diagnostic.len() > 1024 {
                return Err(invalid("invalid Flow quarantine"));
            }
            s.quarantined.entry(run.clone()).or_insert_with(|| diagnostic.clone());
        }
        FlowCommand::InsertVersion { definition: d } => {
            if s.versions.values().map(BTreeMap::len).sum::<usize>() >= MAX_FLOW_RUNS
                && s.versions.get(&d.id).and_then(|v| v.get(&d.version)).is_none()
            {
                return Err(invalid("Flow version capacity"));
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
            if s.runs.len() >= MAX_FLOW_RUNS {
                return Err(invalid("Flow run capacity"));
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
            {
                return Err(invalid("invalid initial Flow run"));
            }
            if p.document().body.nodes.iter().any(|n| {
                matches!(n.operation, PlanOperation::Wait { .. } | PlanOperation::Timer { .. })
            }) {
                return Err(invalid("unsupported WIP-07 execution profile"));
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
            s.requests.insert(key, r.id.clone());
            s.runs.insert(r.id.clone(), *r.clone());
        }
        _ => {
            let id = match cmd {
                FlowCommand::CancelRequested { run }
                | FlowCommand::BindCoordinator { run, .. }
                | FlowCommand::PrepareChildren { run, .. }
                | FlowCommand::BindChild { run, .. }
                | FlowCommand::WakeDelivered { run, .. }
                | FlowCommand::RecordNodeResult { run, .. }
                | FlowCommand::Finish { run, .. } => run,
                _ => unreachable!(),
            };
            let r = s.runs.get_mut(id).ok_or_else(|| invalid("unknown Flow run"))?;
            r.restore()?;
            if r.outcome.is_some() && !matches!(cmd, FlowCommand::Finish { .. }) {
                return Err(invalid("Flow run finished"));
            }
            match cmd {
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
    if canonical_collection(&s).map_err(invalid)?.len() > MAX_FLOW_STATE_BYTES {
        return Err(invalid("Flow state byte capacity"));
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
fn snapshot(c: &Connection, tenant: &OpaqueRef) -> Result<FlowState> {
    let b: Option<Vec<u8>> = c
        .query_row("SELECT state FROM flow_state WHERE tenant=?", [tenant.as_str()], |r| r.get(0))
        .optional()?;
    b.map_or_else(|| Ok(FlowState::default()), |b| decode(&b))
}
fn write(c: &Connection, tenant: &OpaqueRef, s: &FlowState) -> Result<()> {
    c.execute(
        "INSERT INTO flow_state VALUES(?,?) ON CONFLICT(tenant) DO UPDATE SET state=excluded.state",
        params![tenant.as_str(), canonical_collection(s).map_err(invalid)?],
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
        let old = snapshot(target, &e.tenant)?;
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
    }
    Ok(())
}
pub struct FlowDispatchGuard<'a> {
    root: &'a StoreRoot,
    _guard: std::sync::MutexGuard<'a, ()>,
}
impl StoreRoot {
    /// Serializes Start with Flow cancellation, quarantine, result and Finish writes.
    /// StoreRoot holds the exclusive bundle lock, so all writers share this guard.
    pub fn flow_dispatch_guard(&self) -> Result<FlowDispatchGuard<'_>> {
        Ok(FlowDispatchGuard {
            root: self,
            _guard: self
                .flow_dispatch
                .lock()
                .map_err(|_| invalid("Flow dispatch lock poisoned"))?,
        })
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
            FlowCommand::RecordNodeResult { result, .. } => {
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
        snapshot(&self.connect(StoreKind::Flow)?, tenant)
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
    /// Caller holds this root's dispatch guard across reading outcome evidence
    /// and writing Finish, preventing a concurrent Start from being hidden.
    pub fn flow_transact_guarded(
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
        let old = snapshot(&tx, tenant)?;
        let new = reduce(&old, tenant, &command)?;
        if new == old {
            return Ok(old);
        }
        if old.revision != expected {
            return Err(invalid("Flow revision conflict"));
        }
        let e = Entry { tenant: tenant.clone(), expected, command };
        let b = canonical_collection(&e).map_err(invalid)?;
        tx.execute(
            "INSERT INTO flow_history(tenant,revision,entry,digest) VALUES(?,?,?,?)",
            params![tenant.as_str(), new.revision, &b, ContentHash::of(&b).as_str()],
        )?;
        write(&tx, tenant, &new)?;
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
            _ => {}
        }
        Ok(new)
    }
    pub(crate) fn validate_flow_artifacts(&self) -> Result<()> {
        let c = Connection::open_with_flags(
            self.db_path(StoreKind::Flow),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let mut q = c.prepare("SELECT state FROM flow_state")?;
        for b in q.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
            let s: FlowState = decode(&b?)?;
            for r in s.runs.values() {
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
