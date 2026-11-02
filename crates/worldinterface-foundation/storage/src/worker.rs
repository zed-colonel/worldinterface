//! Worker journal and reservation projection, owned by Fabric transactions.
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use worldinterface_domain::{continuation::DurableEvent, worker::*};
use worldinterface_protocol::{canonical_data, strict_json, ContentHash, OpaqueRef};

pub(crate) const SCHEMA: &str = "
CREATE TABLE worker_history(seq INTEGER PRIMARY KEY, command BLOB NOT NULL, digest TEXT NOT NULL);
CREATE TRIGGER worker_no_update BEFORE UPDATE ON worker_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER worker_no_delete BEFORE DELETE ON worker_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE worker_runs(tenant TEXT NOT NULL, provider TEXT NOT NULL, run TEXT NOT NULL, request TEXT NOT NULL, reserved INTEGER NOT NULL, state BLOB NOT NULL, PRIMARY KEY(tenant,provider,run), UNIQUE(tenant,request));
CREATE TABLE worker_limits(tenant TEXT PRIMARY KEY, slots INTEGER NOT NULL);
";
#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Operation {
        binding: WorkerBinding,
        entry: WorkerOperation,
    },
    Reserve {
        binding: WorkerBinding,
        slots: u32,
    },
    Transition {
        tenant: OpaqueRef,
        provider: OpaqueRef,
        run: OpaqueRef,
        revision: u64,
        fact: WorkerFact,
    },
    Observe {
        tenant: OpaqueRef,
        provider: OpaqueRef,
        run: OpaqueRef,
        observation: WorkerObservation,
        event: Box<DurableEvent>,
    },
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_value(strict_json(bytes).map_err(invalid)?)?)
}
fn snapshot(
    c: &Connection,
    tenant: &OpaqueRef,
    provider: &OpaqueRef,
    run: &OpaqueRef,
) -> Result<Option<WorkerState>> {
    let bytes: Option<Vec<u8>> = c
        .query_row(
            "SELECT state FROM worker_runs WHERE tenant=? AND provider=? AND run=?",
            params![tenant.as_str(), provider.as_str(), run.as_str()],
            |r| r.get(0),
        )
        .optional()?;
    bytes.map(|b| decode(&b)).transpose()
}
fn save(c: &Connection, s: &WorkerState) -> Result<()> {
    let b = &s.binding;
    c.execute("INSERT INTO worker_runs VALUES(?,?,?,?,?,?) ON CONFLICT(tenant,provider,run) DO UPDATE SET reserved=excluded.reserved,state=excluded.state",params![b.tenant.as_str(), b.provider.as_str(), b.run.as_str(), b.request.as_str(), s.capacity_reserved,canonical_data(s).map_err(invalid)?])?;
    Ok(())
}
fn apply(c: &Connection, command: &Command) -> Result<bool> {
    match command {
        Command::Operation { binding, entry: operation } => {
            let mut state = snapshot(c, &binding.tenant, &binding.provider, &binding.run)?
                .ok_or_else(|| invalid("worker absent"))?;
            if state.binding != *binding {
                return Err(invalid("worker operation realization drift"));
            }
            if let Some(old) = state.operations.get(&operation.key) {
                let mut expected = old.clone();
                expected.acknowledged = operation.acknowledged;
                if expected != *operation {
                    return Err(invalid("worker provider key conflict"));
                }
                if old == operation || old.acknowledged && !operation.acknowledged {
                    return Ok(false);
                }
            } else if operation.acknowledged || state.operations.len() >= 256 {
                return Err(invalid("worker operation missing intent or bound"));
            }
            state.operations.insert(operation.key.clone(), operation.clone());
            state.revision =
                state.revision.checked_add(1).ok_or_else(|| invalid("worker revision overflow"))?;
            save(c, &state)?;
        }
        Command::Reserve { binding, slots } => {
            if *slots == 0 || *slots > 1024 {
                return Err(invalid("worker capacity bound"));
            }
            if let Some(old) = snapshot(c, &binding.tenant, &binding.provider, &binding.run)? {
                if old.binding != *binding {
                    return Err(invalid("worker realization conflict"));
                }
                return Ok(false);
            }
            let previous: Option<u32> = c
                .query_row(
                    "SELECT slots FROM worker_limits WHERE tenant=?",
                    [binding.tenant.as_str()],
                    |r| r.get(0),
                )
                .optional()?;
            if previous.is_some_and(|n| n != *slots) {
                return Err(invalid("worker capacity policy drift"));
            }
            let count: u32 = c.query_row(
                "SELECT count(*) FROM worker_runs WHERE tenant=? AND reserved=1",
                [binding.tenant.as_str()],
                |r| r.get(0),
            )?;
            if count >= *slots {
                return Err(invalid("worker capacity exhausted"));
            }
            c.execute(
                "INSERT OR IGNORE INTO worker_limits VALUES(?,?)",
                params![binding.tenant.as_str(), slots],
            )?;
            save(c, &WorkerState::start_intent(binding.clone()))?;
        }
        Command::Transition { tenant, provider, run, revision, fact } => {
            let mut state =
                snapshot(c, tenant, provider, run)?.ok_or_else(|| invalid("worker absent"))?;
            if state.revision != *revision {
                return Err(invalid("worker revision conflict"));
            }
            state.transition(*fact).map_err(invalid)?;
            state.revision =
                state.revision.checked_add(1).ok_or_else(|| invalid("worker revision overflow"))?;
            save(c, &state)?;
        }
        Command::Observe { tenant, provider, run, observation, event } => {
            let mut state =
                snapshot(c, tenant, provider, run)?.ok_or_else(|| invalid("worker absent"))?;
            if event.route.correlation != *run
                || event.route.generation.is_some()
                || event.route.tenant != *tenant
                || event.route.audience != state.binding.principal
                || event.route.kind
                    != worldinterface_domain::continuation::EventKind::WorkerObservation
                || event.digest != observation.digest
                || event.source != observation.event
            {
                return Err(invalid("worker observation route or content mismatch"));
            }
            if !state.observe(observation.clone()).map_err(invalid)? {
                return Ok(false);
            }
            state.revision =
                state.revision.checked_add(1).ok_or_else(|| invalid("worker revision overflow"))?;
            // WI commit order is separate from provider sequence retained in the aggregate.
            if event.revision != state.revision {
                return Err(invalid("worker commit revision conflict"));
            }
            save(c, &state)?;
            crate::continuation::insert(c, event)?;
        }
    }
    Ok(true)
}
fn transact(root: &StoreRoot, cmd: &Command) -> Result<bool> {
    let mut c = root.connect(StoreKind::Fabric)?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = apply(&tx, cmd)?;
    if changed {
        let bytes = canonical_data(cmd).map_err(invalid)?;
        tx.execute(
            "INSERT INTO worker_history(command,digest) VALUES(?,?)",
            params![&bytes, ContentHash::of(&bytes).as_str()],
        )?;
    }
    let boundary = match cmd {
        Command::Operation { entry, .. } if entry.acknowledged => "worker-operation-before-ack",
        Command::Operation { .. } => "worker-operation-intent-before-commit",
        Command::Reserve { .. } => "worker-intent-before-commit",
        Command::Observe { .. } => "worker-event-before-commit",
        Command::Transition { fact: WorkerFact::StopRequested, .. } => {
            "worker-cancel-before-commit"
        }
        Command::Transition { fact: WorkerFact::CleanupConfirmed, .. } => {
            "worker-cleanup-before-commit"
        }
        _ => "worker-transition-before-commit",
    };
    worker_boundary(boundary);
    tx.commit()?;
    worker_boundary(match boundary {
        "worker-operation-before-ack" => "worker-operation-acknowledged",
        "worker-operation-intent-before-commit" => "worker-operation-intent-committed",
        "worker-intent-before-commit" => "worker-intent-committed",
        "worker-event-before-commit" => "worker-event-committed",
        "worker-cancel-before-commit" => "worker-cancel-committed",
        "worker-cleanup-before-commit" => "worker-cleanup-committed",
        _ => "worker-transition-committed",
    });
    Ok(changed)
}
impl StoreRoot {
    pub fn committed_worker_event(
        &self,
        tenant: &OpaqueRef,
        source: &OpaqueRef,
    ) -> Result<Option<OpaqueRef>> {
        let c = self.connect(StoreKind::Fabric)?;
        let id: Option<String> = c
            .query_row(
                "SELECT id FROM worker_event_sequences WHERE tenant=? AND source=?",
                params![tenant.as_str(), source.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        id.map(|s| s.try_into().map_err(invalid)).transpose()
    }

    pub fn begin_worker_operation(
        &self,
        binding: WorkerBinding,
        operation: WorkerOperation,
    ) -> Result<bool> {
        if operation.acknowledged {
            return Err(invalid("new worker operation cannot assert acknowledgement"));
        }
        transact(self, &Command::Operation { binding, entry: operation })
    }
    pub fn acknowledge_worker_operation(
        &self,
        binding: WorkerBinding,
        mut operation: WorkerOperation,
    ) -> Result<()> {
        operation.acknowledged = true;
        transact(self, &Command::Operation { binding, entry: operation })?;
        Ok(())
    }
    /// True only for the transaction that created the immutable start intent.
    /// False authorizes reconciliation of the original run, never another spawn.
    pub fn reserve_start(&self, binding: WorkerBinding, slots: u32) -> Result<bool> {
        transact(self, &Command::Reserve { binding, slots })
    }
    pub fn worker_snapshot(
        &self,
        tenant: &OpaqueRef,
        provider: &OpaqueRef,
        run: &OpaqueRef,
    ) -> Result<Option<WorkerState>> {
        snapshot(&self.connect(StoreKind::Fabric)?, tenant, provider, run)
    }
    pub fn record_worker_transition(&self, state: &WorkerState, fact: WorkerFact) -> Result<()> {
        transact(
            self,
            &Command::Transition {
                tenant: state.binding.tenant.clone(),
                provider: state.binding.provider.clone(),
                run: state.binding.run.clone(),
                revision: state.revision,
                fact,
            },
        )?;
        Ok(())
    }
    pub fn request_worker_stop(&self, state: &WorkerState) -> Result<()> {
        self.record_worker_transition(state, WorkerFact::StopRequested)
    }
    /// Host must verify private cleanup evidence before invoking this trusted API.
    pub fn confirm_worker_cleanup(&self, state: &WorkerState) -> Result<()> {
        self.record_worker_transition(state, WorkerFact::CleanupConfirmed)
    }
    pub fn record_worker_observation(
        &self,
        tenant: OpaqueRef,
        provider: OpaqueRef,
        run: OpaqueRef,
        observation: WorkerObservation,
        event: DurableEvent,
    ) -> Result<bool> {
        transact(
            self,
            &Command::Observe { tenant, provider, run, observation, event: Box::new(event) },
        )
    }
    pub fn unfinished_workers(
        &self,
        after: Option<(&str, &str, &str)>,
        limit: u32,
    ) -> Result<Vec<WorkerState>> {
        if limit == 0 || limit > 1000 {
            return Err(invalid("worker enumeration bound"));
        }
        let c = self.connect(StoreKind::Fabric)?;
        let mut q = c.prepare("SELECT state FROM worker_runs WHERE reserved=1 AND (tenant,provider,run)>(?,?,?) ORDER BY tenant,provider,run LIMIT ?")?;
        let (tenant, provider, run) = after.unwrap_or(("", "", ""));
        let rows =
            q.query_map(params![tenant, provider, run, limit], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|r| decode(&r?)).collect()
    }
}
pub(crate) fn replay(source: &Connection, target: &Connection) -> Result<()> {
    let mut q = source.prepare("SELECT command,digest FROM worker_history ORDER BY seq")?;
    let rows = q.query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (bytes, hash) = row?;
        let cmd: Command = decode(&bytes)?;
        if ContentHash::of(&bytes).as_str() != hash
            || canonical_data(&cmd).map_err(invalid)? != bytes
            || !apply(target, &cmd)?
        {
            return Err(invalid("corrupt worker journal"));
        }
    }
    Ok(())
}

/// Debug verification seam; production storage compiles this to a no-op.
pub fn worker_boundary(name: &str) {
    #[cfg(all(feature = "crash-points", not(test)))]
    crate::crashpoint::checkpoint(name);
    #[cfg(any(not(feature = "crash-points"), test))]
    let _ = name;
}
