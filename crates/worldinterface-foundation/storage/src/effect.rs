//! Atomic, versioned effect journal and derived source/dispatch outbox.
//! The root is trusted local storage, never an authenticated public API.
use crate::{invalid, Batch, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use worldinterface_domain::effect::{replay_command, EffectState, ValidatedEffectCommand};
use worldinterface_protocol::*;

/// Optimistic journal conflict or SQLite writer contention. Callers must
/// re-snapshot and revalidate the original effect fence before a bounded retry.
pub fn is_effect_contention(error: &(dyn std::error::Error + 'static)) -> bool {
    error.is::<EffectJournalConflict>()
        || matches!(error.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(code, _))
                if matches!(code.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked))
}
/// A validated command was prepared against a different journal snapshot.
#[derive(Debug)]
pub struct EffectJournalConflict;
impl std::fmt::Display for EffectJournalConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("effect journal snapshot changed")
    }
}
impl std::error::Error for EffectJournalConflict {}

fn replay_validated(
    state: &EffectState,
    command: &ValidatedEffectCommand,
) -> Result<(EffectState, EffectIntentId)> {
    if &worldinterface_domain::effect::state_digest(state).map_err(invalid)?
        != command.base_digest()
    {
        return Err(EffectJournalConflict.into());
    }
    replay_command(state, command.tenant(), command.bytes()).map_err(invalid)
}

/// Prepared under the Fabric writer lock; the receipt is still domain validated.
pub struct EffectReceiptCommand {
    pub command: ValidatedEffectCommand,
    pub execution: ExecutionRecord,
    pub temporal: TemporalProvenance,
}

pub(crate) const SCHEMA: &str = "
CREATE TABLE effect_history(seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, revision INTEGER NOT NULL, entry BLOB NOT NULL, digest TEXT NOT NULL, UNIQUE(tenant,revision));
CREATE INDEX effect_history_intent ON effect_history(tenant,json_extract(CAST(entry AS TEXT),'$.action.id'),revision);
CREATE TRIGGER effect_no_update BEFORE UPDATE ON effect_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER effect_no_delete BEFORE DELETE ON effect_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE effect_run_index(run TEXT PRIMARY KEY, tenant TEXT NOT NULL, intent TEXT NOT NULL, task TEXT NOT NULL);
CREATE TABLE effect_state(tenant TEXT PRIMARY KEY, state BLOB NOT NULL);
CREATE TABLE effect_dispatch_fault_history(seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, intent TEXT NOT NULL, run TEXT NOT NULL, code TEXT NOT NULL, digest TEXT NOT NULL, UNIQUE(tenant,intent,run,code));
CREATE TRIGGER dispatch_fault_no_update BEFORE UPDATE ON effect_dispatch_fault_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER dispatch_fault_no_delete BEFORE DELETE ON effect_dispatch_fault_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE effect_outbox(tenant TEXT NOT NULL, intent TEXT NOT NULL, dispatch BLOB NOT NULL, PRIMARY KEY(tenant,intent));
";
fn snapshot(c: &Connection, tenant: &OpaqueRef) -> Result<EffectState> {
    let b: Option<Vec<u8>> = c
        .query_row("SELECT state FROM effect_state WHERE tenant=?", [tenant.as_str()], |r| r.get(0))
        .optional()?;
    b.map_or_else(
        || Ok(EffectState::default()),
        |b| Ok(serde_json::from_value(strict_collection(&b).map_err(invalid)?)?),
    )
}
pub(crate) fn snapshot_for_replay(c: &Connection, tenant: &OpaqueRef) -> Result<EffectState> {
    snapshot(c, tenant)
}
// Convergence diagnostics survive reconciliation and later dispatches. Bind them
// to the immutable receipt's source revision, not the effect's newest outcome.
fn receipt_binds_convergence_fault(
    c: &Connection,
    tenant: &OpaqueRef,
    id: &EffectIntentId,
    run: &OpaqueRef,
    outcome: &ExecutionOutcome,
) -> Result<bool> {
    let mut q = c.prepare(
        "SELECT source_revision,execution FROM receipt_history WHERE tenant=? AND intent=?",
    )?;
    for row in q.query_map(params![tenant.as_str(), id.as_str()], |r| {
        Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?))
    })? {
        let (revision, bytes) = row?;
        let execution: ExecutionRecord =
            serde_json::from_value(strict_collection(&bytes).map_err(invalid)?)?;
        if &execution.outcome != outcome {
            continue;
        }
        let source = snapshot_at_revision(c, tenant, revision)?;
        let effect =
            source.effects.get(id).ok_or_else(|| invalid("fault receipt without effect"))?;
        if effect
            .outcomes
            .last()
            .is_some_and(|o| &o.outcome == outcome && o.attempt == execution.attempt)
            && effect
                .dispatches
                .last()
                .and_then(|d| d.admission.as_ref())
                .is_some_and(|a| a.observed_runs.contains(run))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn snapshot_at_revision(
    c: &Connection,
    tenant: &OpaqueRef,
    revision: u64,
) -> Result<EffectState> {
    let mut state = EffectState::default();
    let mut query = c.prepare(
        "SELECT revision,entry FROM effect_history WHERE tenant=? AND revision<=? ORDER BY revision",
    )?;
    let entries = query.query_map(params![tenant.as_str(), revision], |row| {
        Ok((row.get::<_, u64>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    for entry in entries {
        let (number, bytes) = entry?;
        if number != state.revision + 1 {
            return Err(invalid("effect history gap"));
        }
        state = replay_command(&state, tenant, &bytes).map_err(invalid)?.0;
    }
    if state.revision != revision {
        return Err(invalid("missing receipt source revision"));
    }
    Ok(state)
}
fn write(c: &Connection, tenant: &OpaqueRef, s: &EffectState, fault: Option<&str>) -> Result<()> {
    write_projection(c, tenant, s, fault, false)
}
fn write_projection(
    c: &Connection,
    tenant: &OpaqueRef,
    s: &EffectState,
    fault: Option<&str>,
    receipt: bool,
) -> Result<()> {
    let _ = receipt;
    c.execute("INSERT INTO effect_state VALUES(?,?) ON CONFLICT(tenant) DO UPDATE SET state=excluded.state",params![tenant.as_str(),canonical_collection(s).map_err(invalid)?])?;
    if fault == Some("state") {
        return Err(invalid("injected effect state"));
    }
    if receipt {
        #[cfg(test)]
        crate::crash::checkpoint("fabric-receipt-state");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("fabric-receipt-state");
    }
    for (id, e) in &s.effects {
        // An empty list is the durable non-dispatch-ready source outbox. Queue delivery cannot read
        // it as a request. All operation requests/acks remain in the authoritative journal.
        c.execute("INSERT INTO effect_outbox VALUES(?,?,?) ON CONFLICT(tenant,intent) DO UPDATE SET dispatch=excluded.dispatch",params![tenant.as_str(),id.as_str(),canonical_data(&e.dispatches).map_err(invalid)?])?;
        for admission in e.dispatches.iter().filter_map(|d| d.admission.as_ref()) {
            for run in &admission.observed_runs {
                c.execute(
                    "INSERT OR IGNORE INTO effect_run_index VALUES(?,?,?,?)",
                    params![run.as_str(), tenant.as_str(), id.as_str(), admission.task.as_str()],
                )?;
                let binding: (String, String, String) = c.query_row(
                    "SELECT tenant,intent,task FROM effect_run_index WHERE run=?",
                    [run.as_str()],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?;
                if binding
                    != (
                        tenant.as_str().to_owned(),
                        id.as_str().to_owned(),
                        admission.task.as_str().to_owned(),
                    )
                {
                    return Err(invalid("conflicting effect run binding"));
                }
            }
        }
    }
    if fault == Some("outbox") {
        return Err(invalid("injected effect outbox"));
    }
    Ok(())
}
fn transact(
    c: &mut Connection,
    command: &ValidatedEffectCommand,
    fault: Option<&str>,
) -> Result<EffectIntentId> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let tenant = command.tenant();
    let old = snapshot(&tx, tenant)?;
    let (new, id) = replay_validated(&old, command)?;
    if !old.effects.contains_key(&id) {
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM projections WHERE entity=?)",
            [id.as_str()],
            |r| r.get(0),
        )?;
        if exists {
            return Err(invalid("intent identity belongs to historical repository"));
        }
    }
    if new.revision != old.revision {
        tx.execute(
            "INSERT INTO effect_history(tenant,revision,entry,digest) VALUES(?,?,?,?)",
            params![
                tenant.as_str(),
                new.revision,
                command.bytes(),
                ContentHash::of(command.bytes()).as_str()
            ],
        )?;
        if fault == Some("history") {
            return Err(invalid("injected effect history"));
        }
        write(&tx, tenant, &new, fault)?;
    }
    tx.commit()?;
    #[cfg(any(test, feature = "crash-points"))]
    if strict_json(command.bytes()).map_err(invalid)?["action"]["operation"].as_str()
        == Some("start_production")
    {
        #[cfg(test)]
        crate::crash::checkpoint("fabric-start-committed");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("fabric-start-committed");
    }
    Ok(id)
}
/// A trusted-host diagnostic. It does not fabricate a domain outcome or receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchFault {
    pub tenant: OpaqueRef,
    pub intent: EffectIntentId,
    pub run: OpaqueRef,
    pub code: String,
}

/// Phase observations from this effect's authoritative journal, not a tenant clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectExecutionTimes {
    pub received: Timestamp,
    pub admitted: Timestamp,
    pub dispatched: Timestamp,
    pub run: Option<OpaqueRef>,
    pub attempt: OpaqueRef,
}

fn execution_times(
    c: &Connection,
    tenant: &OpaqueRef,
    id: &EffectIntentId,
) -> Result<EffectExecutionTimes> {
    let mut q = c.prepare("SELECT entry FROM effect_history WHERE tenant=? AND json_extract(CAST(entry AS TEXT),'$.action.id')=? ORDER BY revision")?;
    let mut received = None;
    let mut admitted = None;
    let mut dispatched = None;
    let mut run = None;
    let mut attempt = None;
    for row in q.query_map(params![tenant.as_str(), id.as_str()], |r| r.get::<_, Vec<u8>>(0))? {
        let entry = strict_json(&row?).map_err(invalid)?;
        let action = &entry["action"];
        let at: Timestamp = serde_json::from_value(entry["at"].clone())?;
        match action["operation"].as_str() {
            Some("submit") => received = Some(at),
            Some("prepare" | "prepare_production") => {
                admitted = None;
                dispatched = None;
                run = None;
                attempt = None;
            }
            Some("acknowledge" | "acknowledge_production") => admitted = Some(at),
            Some("start" | "start_production") => {
                dispatched = Some(at);
                attempt = Some(serde_json::from_value(action["attempt"].clone())?);
                run = action.get("run").map(|v| serde_json::from_value(v.clone())).transpose()?;
            }
            _ => {}
        }
    }
    Ok(EffectExecutionTimes {
        received: received.ok_or_else(|| invalid("missing submission timestamp"))?,
        admitted: admitted.ok_or_else(|| invalid("missing admission timestamp"))?,
        dispatched: dispatched.ok_or_else(|| invalid("missing Start timestamp"))?,
        run,
        attempt: attempt.ok_or_else(|| invalid("missing Start attempt"))?,
    })
}

impl StoreRoot {
    /// Trusted-host lookup from the journal-derived run index. This is a binding,
    /// not authorization; hosts must still check the admission and principal.
    pub fn effect_run_binding(
        &self,
        run: &OpaqueRef,
    ) -> Result<Option<(OpaqueRef, EffectIntentId)>> {
        let c = self.connect(StoreKind::Fabric)?;
        let binding: Option<(String, String)> = c
            .query_row(
                "SELECT tenant,intent FROM effect_run_index WHERE run=?",
                [run.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        binding
            .map(|(tenant, id)| {
                Ok((tenant.try_into().map_err(invalid)?, id.try_into().map_err(invalid)?))
            })
            .transpose()
    }

    /// Trusted-host lookup of the immutable receipt's source dispatch and attempt.
    pub fn effect_snapshot_at_revision(
        &self,
        tenant: &OpaqueRef,
        revision: u64,
    ) -> Result<EffectState> {
        snapshot_at_revision(&self.connect(StoreKind::Fabric)?, tenant, revision)
    }

    /// Trusted-host lookup; the last Start belongs to the current dispatch.
    pub fn effect_execution_times(
        &self,
        tenant: &OpaqueRef,
        id: &EffectIntentId,
    ) -> Result<EffectExecutionTimes> {
        let c = self.connect(StoreKind::Fabric)?;
        execution_times(&c, tenant, id)
    }

    /// Trusted-host diagnostic for a refused dispatch. No provider ran under this
    /// dispatch's Start fence. A later authorized attempt may still proceed.
    pub fn record_dispatch_fault(
        &self,
        tenant: &OpaqueRef,
        id: &EffectIntentId,
        run: &OpaqueRef,
        code: &str,
    ) -> Result<()> {
        if ![
            "wi.effect.pre_start_error",
            "wi.effect.pre_start_exhausted",
            "wi.effect.pre_start_canceled",
        ]
        .contains(&code)
        {
            return Err(invalid("unsupported dispatch fault code"));
        }
        let mut c = self.connect(StoreKind::Fabric)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = snapshot(&tx, tenant)?;
        let effect = state.effects.get(id).ok_or_else(|| invalid("fault without effect"))?;
        if effect.phase != worldinterface_domain::Phase::DispatchPending
            || effect.attempt.is_some()
            || effect
                .dispatches
                .last()
                .and_then(|d| d.admission.as_ref())
                .is_none_or(|a| !a.observed_runs.contains(run))
        {
            return Err(invalid("dispatch fault source mismatch"));
        }
        let digest = ContentHash::of(&canonical_data(&(tenant, id, run, code)).map_err(invalid)?);
        tx.execute("INSERT OR IGNORE INTO effect_dispatch_fault_history(tenant,intent,run,code,digest) VALUES(?,?,?,?,?)",
            params![tenant.as_str(), id.as_str(), run.as_str(), code, digest.as_str()])?;
        tx.commit()?;
        Ok(())
    }

    /// Trusted-host enumeration; authenticate and authorize before disclosure.
    pub fn dispatch_faults(
        &self,
        tenant: &OpaqueRef,
        id: &EffectIntentId,
    ) -> Result<Vec<DispatchFault>> {
        let c = self.connect(StoreKind::Fabric)?;
        let mut q = c.prepare("SELECT run,code FROM effect_dispatch_fault_history WHERE tenant=? AND intent=? ORDER BY seq")?;
        let result = q
            .query_map(params![tenant.as_str(), id.as_str()], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .map(|row| {
                let (run, code) = row?;
                Ok(DispatchFault {
                    tenant: tenant.clone(),
                    intent: id.clone(),
                    run: run.try_into().map_err(invalid)?,
                    code,
                })
            })
            .collect();
        result
    }

    /// Trusted-host append. A cross-store diagnostic. It never changes an outcome or receipt.
    pub fn record_convergence_fault(
        &self,
        tenant: &OpaqueRef,
        id: &EffectIntentId,
        run: &OpaqueRef,
        fabric: &ExecutionOutcome,
        queue: &str,
    ) -> Result<()> {
        if !["completed", "failed", "canceled"].contains(&queue) {
            return Err(invalid("invalid AQ terminal state"));
        }
        let state = self.effect_snapshot(tenant)?;
        let effect = state.effects.get(id).ok_or_else(|| invalid("fault without effect"))?;
        if effect.outcomes.last().is_none_or(|o| &o.outcome != fabric)
            || effect
                .dispatches
                .last()
                .and_then(|d| d.admission.as_ref())
                .is_none_or(|a| !a.observed_runs.contains(run))
        {
            return Err(invalid("fault source binding"));
        }
        let bytes = canonical_data(&(tenant, id, run, fabric, queue)).map_err(invalid)?;
        let c = self.connect(StoreKind::Fabric)?;
        c.execute("INSERT OR IGNORE INTO convergence_fault_history(tenant,intent,run,fabric,queue,digest) VALUES(?,?,?,?,?,?)",
            params![tenant.as_str(), id.as_str(), run.as_str(),
                serde_json::to_string(fabric)?, queue, ContentHash::of(&bytes).as_str()])?;
        Ok(())
    }
    /// Trusted-host enumeration; never expose the tenant list to remote callers.
    pub fn effect_tenants(&self) -> Result<Vec<OpaqueRef>> {
        let c = self.connect(StoreKind::Fabric)?;
        let mut q = c.prepare("SELECT tenant FROM effect_state ORDER BY tenant")?;
        let tenants = q
            .query_map([], |r| r.get::<_, String>(0))?
            .map(|r| r?.try_into().map_err(invalid))
            .collect();
        tenants
    }
    /// Trusted-host lookup; the host must authorize the tenant and effect principal.
    pub fn effect_snapshot(&self, tenant: &OpaqueRef) -> Result<EffectState> {
        snapshot(&self.connect(StoreKind::Fabric)?, tenant)
    }
    pub fn effect_transact(&self, command: &ValidatedEffectCommand) -> Result<EffectIntentId> {
        let state = self.effect_snapshot(command.tenant())?;
        let (new, id) = replay_validated(&state, command)?;
        let aggregate = new.effects.get(&id).ok_or_else(|| invalid("missing effect"))?;
        if aggregate.outcomes.len() != state.effects.get(&id).map_or(0, |e| e.outcomes.len()) {
            return Err(invalid("outcome requires atomic receipt transaction"));
        }
        for reference in worldinterface_domain::effect::required_artifacts(aggregate) {
            self.read_artifact(reference)?;
        }
        transact(&mut self.connect(StoreKind::Fabric)?, command, None)
    }
    /// Commits a local outcome, receipt and effect outbox in one Fabric transaction.
    /// This is a domain seam; it does not acknowledge an AQ attempt.
    pub fn effect_transact_with_receipt(
        &self,
        command: &ValidatedEffectCommand,
        receipt_id: ReceiptId,
        execution: &ExecutionRecord,
        temporal: TemporalProvenance,
    ) -> Result<ContentHash> {
        let mut c = self.connect(StoreKind::Fabric)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let tenant = command.tenant();
        if let Some(digest) =
            crate::receipt::exact_retry(&tx, tenant, command, &receipt_id, execution, &temporal)?
        {
            return Ok(digest);
        }
        let state = snapshot(&tx, tenant)?;
        let digest = self.write_receipt(&tx, &state, command, receipt_id, execution, temporal)?;
        tx.commit()?;
        #[cfg(test)]
        crate::crash::checkpoint("fabric-receipt-committed");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("fabric-receipt-committed");
        Ok(digest)
    }
    /// Trusted-host completion seam. Snapshot, phase times, fence validation and
    /// command preparation all run under BEGIN IMMEDIATE. The callback must not
    /// perform I/O or re-enter storage. Publish artifacts before calling this.
    /// None means the caller's attempt fence no longer holds; nothing is written.
    pub fn effect_transact_receipt_with(
        &self,
        tenant: &OpaqueRef,
        id: &EffectIntentId,
        attempt: &OpaqueRef,
        run: &OpaqueRef,
        prepare: impl FnOnce(
            &EffectState,
            &EffectExecutionTimes,
        ) -> Result<Option<EffectReceiptCommand>>,
    ) -> Result<Option<ContentHash>> {
        let mut c = self.connect(StoreKind::Fabric)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = snapshot(&tx, tenant)?;
        let effect = state.effects.get(id).ok_or_else(|| invalid("missing completion effect"))?;
        if effect.phase != worldinterface_domain::Phase::Running
            || effect.attempt.as_ref() != Some(attempt)
            || effect.outcomes.iter().any(|o| &o.attempt == attempt)
            || effect
                .dispatches
                .last()
                .and_then(|d| d.admission.as_ref())
                .is_none_or(|a| !a.observed_runs.contains(run))
        {
            return Ok(None);
        }
        let phases = execution_times(&tx, tenant, id)?;
        if &phases.attempt != attempt || phases.run.as_ref() != Some(run) {
            return Ok(None);
        }
        let Some(prepared) = prepare(&state, &phases)? else { return Ok(None) };
        if prepared.command.tenant() != tenant
            || &prepared.execution.intent_id != id
            || &prepared.execution.attempt != attempt
        {
            return Err(invalid("completion tenant mismatch"));
        }
        let digest = self.write_receipt(
            &tx,
            &state,
            &prepared.command,
            ReceiptId::generate(),
            &prepared.execution,
            prepared.temporal,
        )?;
        tx.commit()?;
        #[cfg(test)]
        crate::crash::checkpoint("fabric-receipt-committed");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("fabric-receipt-committed");
        Ok(Some(digest))
    }

    fn write_receipt(
        &self,
        c: &Connection,
        old: &EffectState,
        command: &ValidatedEffectCommand,
        receipt_id: ReceiptId,
        execution: &ExecutionRecord,
        temporal: TemporalProvenance,
    ) -> Result<ContentHash> {
        let tenant = command.tenant();
        let (new, id) = replay_validated(old, command)?;
        if new.revision == old.revision {
            return Err(invalid("receipt command already applied; inspect immutable receipt"));
        }
        let aggregate = new.effects.get(&id).ok_or_else(|| invalid("missing effect"))?;
        let receipt = worldinterface_domain::receipt::prepare_egress_receipt(
            aggregate,
            new.revision,
            receipt_id,
            execution,
            temporal,
        )
        .map_err(invalid)?;
        let Record::BoundaryReceipt(ref r) = receipt.envelope().record else { unreachable!() };
        let ReceiptBody::Egress(ref body) = r.body else { unreachable!() };
        for reference in &body.output_refs {
            self.read_artifact(reference)?;
        }
        for reference in &r.evidence {
            if reference.artifact.scope != *tenant {
                return Err(invalid("receipt evidence scope"));
            }
            self.read_artifact(&reference.artifact)?;
        }
        for reference in worldinterface_domain::effect::required_artifacts(aggregate) {
            self.read_artifact(reference)?;
        }
        c.execute(
            "INSERT INTO effect_history(tenant,revision,entry,digest) VALUES(?,?,?,?)",
            params![
                tenant.as_str(),
                new.revision,
                command.bytes(),
                ContentHash::of(command.bytes()).as_str()
            ],
        )?;
        #[cfg(test)]
        crate::crash::checkpoint("fabric-receipt-history");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("fabric-receipt-history");
        write_projection(c, tenant, &new, None, true)?;
        #[cfg(test)]
        crate::crash::checkpoint("fabric-receipt-outbox");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("fabric-receipt-outbox");
        let digest = crate::receipt::insert(c, tenant, &receipt, execution)?;
        #[cfg(test)]
        crate::crash::checkpoint("fabric-receipt-index");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("fabric-receipt-index");
        Ok(digest)
    }
}
impl StoreRoot {
    pub(crate) fn validate_effect_artifacts(&self) -> Result<()> {
        let c = Connection::open_with_flags(
            self.db_path(StoreKind::Fabric),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let mut q = c.prepare("SELECT tenant FROM effect_state")?;
        let tenants =
            q.query_map([], |r| r.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        for tenant in tenants {
            let s = snapshot(&c, &tenant.try_into().map_err(invalid)?)?;
            for e in s.effects.values() {
                for reference in worldinterface_domain::effect::required_artifacts(e) {
                    self.read_artifact(reference)?;
                }
            }
        }
        let mut q =
            c.prepare("SELECT tenant,intent,run,code,digest FROM effect_dispatch_fault_history")?;
        for row in q.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ))
        })? {
            let (tenant, id, run, code, digest) = row?;
            let tenant: OpaqueRef = tenant.try_into().map_err(invalid)?;
            let id: EffectIntentId = id.try_into().map_err(invalid)?;
            let run: OpaqueRef = run.try_into().map_err(invalid)?;
            let state = snapshot(&c, &tenant)?;
            let effect =
                state.effects.get(&id).ok_or_else(|| invalid("dispatch fault without effect"))?;
            if ![
                "wi.effect.pre_start_error",
                "wi.effect.pre_start_exhausted",
                "wi.effect.pre_start_canceled",
            ]
            .contains(&code.as_str())
                || ContentHash::of(&canonical_data(&(&tenant, &id, &run, &code)).map_err(invalid)?)
                    .as_str()
                    != digest
                || !effect.dispatches.iter().any(|d| {
                    d.admission.as_ref().is_some_and(|a| {
                        a.acknowledged_sequence.is_some() && a.observed_runs.contains(&run)
                    })
                })
            {
                return Err(invalid("dispatch fault source or digest mismatch"));
            }
        }
        let mut q = c.prepare(
            "SELECT tenant,intent,run,fabric,queue,digest FROM convergence_fault_history",
        )?;
        let rows = q.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
            ))
        })?;
        for row in rows {
            let (tenant, intent, run, fabric, queue, digest) = row?;
            let tenant: OpaqueRef = tenant.try_into().map_err(invalid)?;
            let intent: EffectIntentId = intent.try_into().map_err(invalid)?;
            let run: OpaqueRef = run.try_into().map_err(invalid)?;
            let outcome: ExecutionOutcome = serde_json::from_str(&fabric)?;
            if !["completed", "failed", "canceled"].contains(&queue.as_str())
                || ContentHash::of(
                    &canonical_data(&(&tenant, &intent, &run, &outcome, &queue))
                        .map_err(invalid)?,
                )
                .as_str()
                    != digest
            {
                return Err(invalid("convergence fault record corrupt"));
            }
            if !receipt_binds_convergence_fault(&c, &tenant, &intent, &run, &outcome)? {
                return Err(invalid("convergence fault source mismatch"));
            }
        }
        Ok(())
    }
}
pub(crate) fn replay(source: &Connection, target: &Connection) -> Result<()> {
    let mut q =
        source.prepare("SELECT tenant,revision,entry,digest FROM effect_history ORDER BY seq")?;
    let rows = q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, u64>(1)?,
            r.get::<_, Vec<u8>>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    for row in rows {
        let (tenant, revision, bytes, hash) = row?;
        if ContentHash::of(&bytes).as_str() != hash {
            return Err(invalid("corrupt effect journal digest"));
        }
        let tenant: OpaqueRef = tenant.try_into().map_err(invalid)?;
        let old = snapshot(target, &tenant)?;
        let (new, _) = replay_command(&old, &tenant, &bytes).map_err(invalid)?;
        if new.revision != revision || old.revision.checked_add(1) != Some(revision) {
            return Err(invalid("effect journal revision"));
        }
        write(target, &tenant, &new, None)?;
    }
    Ok(())
}
pub(crate) fn reject_generic(c: &Connection, batch: &Batch) -> Result<()> {
    let exists: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM effect_outbox WHERE intent=?)",
        [batch.entity.as_str()],
        |r| r.get(0),
    )?;
    if exists {
        return Err(invalid("generic mutation of guarded effect"));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn phase_times_seek_one_intent_and_preserve_dispatch_and_tenant_boundaries() {
        // S-023: completion must not scan/deserialize the entire tenant journal
        // while holding the shared Fabric writer lock.
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(SCHEMA).unwrap();
        let tenant: OpaqueRef = "tenant-a".to_owned().try_into().unwrap();
        let id = EffectIntentId::generate();
        let tx = c.unchecked_transaction().unwrap();
        for revision in 1..=2000 {
            let entry = serde_json::json!({"action": {"id": EffectIntentId::generate(), "operation": "submit"}});
            let bytes = canonical_data(&entry).unwrap();
            tx.execute(
                "INSERT INTO effect_history(tenant,revision,entry,digest) VALUES(?,?,?,?)",
                params![tenant.as_str(), revision, &bytes, ContentHash::of(&bytes).as_str()],
            )
            .unwrap();
        }
        let times: Vec<Timestamp> = (0..6)
            .map(|n| format!("2026-09-21T00:00:0{n}.000000000Z").try_into().unwrap())
            .collect();
        for (n, operation) in [
            "submit",
            "acknowledge_production",
            "start_production",
            "prepare_production",
            "acknowledge_production",
            "start_production",
        ]
        .into_iter()
        .enumerate()
        {
            for scope in [tenant.as_str(), "tenant-b"] {
                let entry = serde_json::json!({
                    "at": times[n],
                    "action": {"id": id, "operation": operation, "attempt": if scope == tenant.as_str() { format!("attempt-{n}") } else { format!("foreign-attempt-{n}") }, "run": format!("run-{n}")}
                });
                let bytes = canonical_data(&entry).unwrap();
                tx.execute(
                    "INSERT INTO effect_history(tenant,revision,entry,digest) VALUES(?,?,?,?)",
                    params![scope, 2001 + n, &bytes, ContentHash::of(&bytes).as_str()],
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
        let phases = execution_times(&c, &tenant, &id).unwrap();
        assert_eq!(phases.received, times[0]);
        assert_eq!(phases.admitted, times[4]);
        assert_eq!(phases.dispatched, times[5]);
        assert_eq!(phases.attempt.as_str(), "attempt-5");
        assert_eq!(phases.run.unwrap().as_str(), "run-5");
        let mut q = c.prepare("EXPLAIN QUERY PLAN SELECT entry FROM effect_history WHERE tenant=? AND json_extract(CAST(entry AS TEXT),'$.action.id')=? ORDER BY revision").unwrap();
        let plan = q
            .query_map(params![tenant.as_str(), id.as_str()], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            plan.iter().any(|s| s.contains("effect_history_intent (tenant=? AND <expr>=?)")),
            "{plan:?}"
        );
        assert!(!plan.iter().any(|s| s.contains("SCAN") || s.contains("TEMP B-TREE")), "{plan:?}");
    }

    #[test]
    fn missing_phase_index_rejects_interim_schema_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("wi");
        let root = StoreRoot::create_empty(
            &path,
            crate::StoreProfile {
                instance: InstanceId::generate(),
                created_at: "2026-09-21T00:00:00.000000000Z".to_owned().try_into().unwrap(),
                platform: false,
            },
        )
        .unwrap();
        let fabric = root.db_path(StoreKind::Fabric);
        root.connect(StoreKind::Fabric)
            .unwrap()
            .execute_batch("DROP INDEX effect_history_intent")
            .unwrap();
        drop(root);
        let before = std::fs::read(&fabric).unwrap();
        for _ in 0..2 {
            let error = StoreRoot::open_existing(&path).err().expect("incompatible interim layout");
            assert_eq!(error.to_string(), "unsupported schema");
            assert_eq!(std::fs::read(&fabric).unwrap(), before);
        }
    }

    #[test]
    fn effect_journal_faults_roll_back_every_projection() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/wi-fabric-2/egress/fixture.json"
        ))
        .unwrap();
        let submission: EffectSubmission =
            serde_json::from_value(fixture["submission"].clone()).unwrap();
        let snapshot: worldinterface_domain::authorization::AuthorizationSnapshot =
            serde_json::from_value(fixture["snapshot"].clone()).unwrap();
        let caller =
            worldinterface_domain::agency::AuthenticatedCaller::from_trusted_authentication(
                snapshot.bindings[0].tenant.clone(),
                snapshot.bindings[0].principal.clone(),
            );
        let verified =
            worldinterface_domain::authorization::VerifiedSnapshot::from_trusted_verifier(snapshot)
                .unwrap();
        let at: Timestamp = "2026-09-21T00:00:00.000000000Z".to_owned().try_into().unwrap();
        let id = EffectIntentId::generate();
        let mut c = Connection::open_in_memory().unwrap();
        c.execute_batch(SCHEMA).unwrap();
        c.execute_batch(crate::repository::SCHEMA).unwrap();
        let actions = vec![
            worldinterface_domain::effect::EffectAction::Submit {
                id: id.clone(),
                submission: Box::new(submission),
                snapshot: verified.clone(),
                link: None,
            },
            worldinterface_domain::effect::EffectAction::Resolve {
                id: id.clone(),
                snapshot: verified.clone(),
            },
            worldinterface_domain::effect::EffectAction::Authorize {
                id: id.clone(),
                snapshot: verified.clone(),
            },
            worldinterface_domain::effect::EffectAction::Prepare {
                id: id.clone(),
                snapshot: verified,
            },
        ];
        for action in actions {
            let old = snapshot_state(&c, &caller);
            let cmd =
                worldinterface_domain::effect::validate_command(&old, &caller, at.clone(), action)
                    .unwrap();
            for fault in ["history", "state", "outbox"] {
                assert!(transact(&mut c, &cmd, Some(fault)).is_err());
                assert_eq!(snapshot_state(&c, &caller), old);
                let count: u64 =
                    c.query_row("SELECT count(*) FROM effect_history", [], |r| r.get(0)).unwrap();
                assert_eq!(count, old.revision);
            }
            transact(&mut c, &cmd, None).unwrap();
            let conflict = transact(&mut c, &cmd, None).unwrap_err();
            assert!(conflict.is::<EffectJournalConflict>());
            assert!(is_effect_contention(conflict.as_ref()));
            assert!(!is_effect_contention(invalid("stale or unsupported effect journal").as_ref()));
        }
        let old = snapshot_state(&c, &caller);
        let request = &old.effects[&id].dispatches[0].request;
        let ack = worldinterface_domain::ports::EffectAcknowledgment {
            task: request.task.clone(),
            admission_key: request.admission_key.clone(),
            request_digest: worldinterface_domain::ingress::digest(request).unwrap(),
            durable_sequence: 1,
        };
        let cmd = worldinterface_domain::effect::validate_command(
            &old,
            &caller,
            at,
            worldinterface_domain::effect::EffectAction::Acknowledge { id, acknowledgment: ack },
        )
        .unwrap();
        for fault in ["history", "state", "outbox"] {
            assert!(transact(&mut c, &cmd, Some(fault)).is_err());
            assert_eq!(snapshot_state(&c, &caller), old);
        }
        transact(&mut c, &cmd, None).unwrap();
    }
    fn snapshot_state(
        c: &Connection,
        caller: &worldinterface_domain::agency::AuthenticatedCaller,
    ) -> EffectState {
        snapshot(c, caller.tenant()).unwrap()
    }
}
