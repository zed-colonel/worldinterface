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
CREATE TABLE worker_snapshots(identity TEXT PRIMARY KEY, content TEXT NOT NULL, bytes BLOB NOT NULL);
CREATE TRIGGER worker_snapshot_no_update BEFORE UPDATE ON worker_snapshots BEGIN SELECT RAISE(ABORT,'immutable'); END;
CREATE TRIGGER worker_snapshot_no_delete BEFORE DELETE ON worker_snapshots BEGIN SELECT RAISE(ABORT,'immutable'); END;
CREATE TABLE worker_launches(tenant TEXT NOT NULL, provider TEXT NOT NULL, run TEXT NOT NULL, record BLOB NOT NULL, PRIMARY KEY(tenant,provider,run));
CREATE TABLE worker_providers(tenant TEXT NOT NULL, provider TEXT NOT NULL, record BLOB NOT NULL, PRIMARY KEY(tenant,provider));
CREATE TABLE worker_limits(tenant TEXT PRIMARY KEY, slots INTEGER NOT NULL);
";
#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Quarantine {
        record: WorkerProviderQuarantine,
    },
    Policy {
        binding: WorkerBinding,
        policy: WorkerArtifactPolicy,
    },
    Collection {
        binding: WorkerBinding,
        collection: WorkerCollection,
        events: Vec<DurableEvent>,
    },
    Operation {
        binding: WorkerBinding,
        entry: WorkerOperation,
    },
    Reserve {
        binding: WorkerBinding,
        slots: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        launch: Option<WorkerLaunchRecord>,
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
        Command::Quarantine { record } => {
            if record.version != 1
                || record.revision != 1
                || record.affected.is_empty()
                || record.affected.len() > 32
            {
                return Err(invalid("provider quarantine bounds"));
            }
            if let Some(old) = provider_quarantine(c, &record.tenant, &record.provider)? {
                if old == *record {
                    return Ok(false);
                }
                return Err(invalid("provider quarantine is irreversible"));
            }
            c.execute(
                "INSERT INTO worker_providers VALUES(?,?,?)",
                params![
                    record.tenant.as_str(),
                    record.provider.as_str(),
                    canonical_data(record).map_err(invalid)?
                ],
            )?;
        }
        Command::Policy { binding, policy } => {
            let mut state = snapshot(c, &binding.tenant, &binding.provider, &binding.run)?
                .ok_or_else(|| invalid("worker absent"))?;
            if state.binding != *binding {
                return Err(invalid("policy binding mismatch"));
            }
            crate::worker_collection::validate_policy(policy)?;
            if state.artifact_policy == [policy.clone()] {
                return Ok(false);
            }
            if !state.artifact_policy.is_empty() || state.accepted {
                return Err(invalid("immutable worker policy"));
            }
            state.artifact_policy.push(policy.clone());
            state.revision += 1;
            save(c, &state)?;
        }
        Command::Collection { binding, collection, events } => {
            let mut state = snapshot(c, &binding.tenant, &binding.provider, &binding.run)?
                .ok_or_else(|| invalid("worker absent"))?;
            if state.binding != *binding {
                return Err(invalid("collection binding mismatch"));
            }
            if state.collection == *collection {
                return Ok(false);
            }
            if matches!(collection, WorkerCollection::Prepared(_) | WorkerCollection::Published(_))
                && provider_quarantine(c, &binding.tenant, &binding.provider)?.is_some()
            {
                return Err(invalid("provider quarantine forbids new export publication"));
            }
            crate::worker_collection::advance(&mut state, collection, events)?;
            state.revision += 1;
            save(c, &state)?;
            for event in events {
                crate::continuation::insert(c, event)?;
            }
        }
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
        Command::Reserve { binding, slots, launch } => {
            if provider_quarantine(c, &binding.tenant, &binding.provider)?.is_some() {
                return Err(invalid("provider quarantined"));
            }
            if let Some(launch) = launch {
                if launch.version != 1
                    || launch.binding != *binding
                    || launch.operations.is_empty()
                    || launch.operations.len() > 65536
                    || launch.connectors.len() != 6
                    || launch.connectors.iter().any(|b| b.validate().is_err())
                {
                    return Err(invalid("worker launch binding or bound"));
                }
                if let Some(old) = launch_record(c, binding)? {
                    if old != *launch {
                        return Err(invalid("immutable worker launch"));
                    }
                }
            }
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
            if let Some(launch) = launch {
                c.execute(
                    "INSERT INTO worker_launches VALUES(?,?,?,?)",
                    params![
                        binding.tenant.as_str(),
                        binding.provider.as_str(),
                        binding.run.as_str(),
                        canonical_data(launch).map_err(invalid)?
                    ],
                )?;
            }
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
            if event.route.correlation != state.binding.event_correlation().map_err(invalid)?
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
        Command::Collection { collection: WorkerCollection::Prepared(_), .. } => {
            "worker-collection-before-prepare"
        }
        Command::Collection { collection: WorkerCollection::Published(_), .. } => {
            "worker-collection-before-publish"
        }
        Command::Collection { collection: WorkerCollection::Quarantined(_), .. } => {
            "worker-collection-before-quarantine"
        }
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
        "worker-collection-before-prepare" => "worker-collection-prepared",
        "worker-collection-before-publish" => "worker-collection-published",
        "worker-collection-before-quarantine" => "worker-collection-quarantined",
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
    pub fn bind_worker_artifact_policy(
        &self,
        binding: WorkerBinding,
        policy: WorkerArtifactPolicy,
    ) -> Result<()> {
        transact(self, &Command::Policy { binding, policy })?;
        Ok(())
    }
    pub fn record_worker_collection(
        &self,
        binding: WorkerBinding,
        collection: WorkerCollection,
        events: Vec<DurableEvent>,
    ) -> Result<()> {
        if let WorkerCollection::Published(p) = &collection {
            let publications = p
                .exports
                .iter()
                .map(|(r, a)| (r.as_str(), a))
                .chain(std::iter::once(("wi:manifest", &p.manifest)));
            for (reference, artifact) in publications {
                let id = p.prepared.publication_identity(&binding, reference).map_err(invalid)?;
                if !self.publication(&id)?.is_some_and(|entry| {
                    entry.artifact == *artifact
                        && entry.state
                            == worldinterface_domain::publication::PublicationState::Available
                }) {
                    return Err(invalid("collection publication not available"));
                }
                self.read_artifact(artifact)?;
            }
        }
        transact(self, &Command::Collection { binding, collection, events })?;
        Ok(())
    }
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
        transact(self, &Command::Reserve { binding, slots, launch: None })
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

fn provider_quarantine(
    c: &Connection,
    tenant: &OpaqueRef,
    provider: &OpaqueRef,
) -> Result<Option<WorkerProviderQuarantine>> {
    let bytes: Option<Vec<u8>> = c
        .query_row(
            "SELECT record FROM worker_providers WHERE tenant=? AND provider=?",
            params![tenant.as_str(), provider.as_str()],
            |r| r.get(0),
        )
        .optional()?;
    bytes.map(|b| decode(&b)).transpose()
}
fn launch_record(c: &Connection, b: &WorkerBinding) -> Result<Option<WorkerLaunchRecord>> {
    let bytes: Option<Vec<u8>> = c
        .query_row(
            "SELECT record FROM worker_launches WHERE tenant=? AND provider=? AND run=?",
            params![b.tenant.as_str(), b.provider.as_str(), b.run.as_str()],
            |r| r.get(0),
        )
        .optional()?;
    let record: Option<WorkerLaunchRecord> = bytes.map(|b| decode(&b)).transpose()?;
    if record.as_ref().is_some_and(|r| r.binding != *b) {
        return Err(invalid("recovery realization mismatch"));
    }
    Ok(record)
}
impl StoreRoot {
    pub fn quarantine_worker_provider(&self, record: WorkerProviderQuarantine) -> Result<()> {
        transact(self, &Command::Quarantine { record })?;
        Ok(())
    }
    pub fn worker_provider_quarantine(
        &self,
        tenant: &OpaqueRef,
        provider: &OpaqueRef,
    ) -> Result<Option<WorkerProviderQuarantine>> {
        provider_quarantine(&self.connect(StoreKind::Fabric)?, tenant, provider)
    }
    pub fn worker_launch(&self, binding: &WorkerBinding) -> Result<Option<WorkerLaunchRecord>> {
        launch_record(&self.connect(StoreKind::Fabric)?, binding)
    }
    pub fn reserve_catalog_start(&self, launch: WorkerLaunchRecord, slots: u32) -> Result<bool> {
        transact(
            self,
            &Command::Reserve { binding: launch.binding.clone(), slots, launch: Some(launch) },
        )
    }
    /// Serialize the last admission check and dispatch against provider quarantine.
    /// The callback must not re-enter Fabric storage; it may only dispatch already validated work.
    pub fn fence_worker_dispatch<T>(
        &self,
        binding: &WorkerBinding,
        catalog: &ContentHash,
        dispatch: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let mut c = self.connect(StoreKind::Fabric)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if provider_quarantine(&tx, &binding.tenant, &binding.provider)?.is_some()
            || launch_record(&tx, binding)?.is_none_or(|r| r.catalog != *catalog)
        {
            return Err(invalid("stale catalog admission or provider quarantine"));
        }
        let result = dispatch()?;
        tx.commit()?;
        Ok(result)
    }

    /// Termination retains the exact launch binding even during provider quarantine.
    /// The trusted callback may only submit termination, never new work or exports,
    /// and must not re-enter Fabric storage.
    pub fn fence_worker_termination<T>(
        &self,
        binding: &WorkerBinding,
        catalog: &ContentHash,
        terminate: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let mut c = self.connect(StoreKind::Fabric)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if launch_record(&tx, binding)?.is_none_or(|r| r.catalog != *catalog) {
            return Err(invalid("stale termination catalog binding"));
        }
        let result = terminate()?;
        tx.commit()?;
        Ok(result)
    }
}

impl StoreRoot {
    /// Order ContextStore publication against Fabric provider quarantine. The
    /// callback may access ContextStore, but must not write Fabric state.
    pub fn fence_worker_publication<T>(
        &self,
        binding: &WorkerBinding,
        publish: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let mut c = self.connect(StoreKind::Fabric)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state = snapshot(&tx, &binding.tenant, &binding.provider, &binding.run)?
            .ok_or_else(|| invalid("worker absent"))?;
        if state.binding != *binding
            || state.quarantined
            || state.stop_requested
            || provider_quarantine(&tx, &binding.tenant, &binding.provider)?.is_some()
        {
            return Err(invalid("worker publication quarantined"));
        }
        let result = publish()?;
        tx.commit()?;
        Ok(result)
    }
}
