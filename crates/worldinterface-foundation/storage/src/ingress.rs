//! Fabric-owned atomic ingress journal. This low-level repository is a trusted local
//! persistence interface, not an authenticated API. The ingress facade governs disclosure.
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use worldinterface_domain::ingress::{apply_ingress, IngressCommand, IngressResult, IngressState};
use worldinterface_protocol::{
    canonical_collection, canonical_data, strict_collection, ContentHash, OpaqueRef,
};

pub(crate) const SCHEMA: &str = "
CREATE TABLE ingress_history(seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, revision INTEGER NOT NULL, entry BLOB NOT NULL, digest TEXT NOT NULL, UNIQUE(tenant,revision));
CREATE TRIGGER ingress_no_update BEFORE UPDATE ON ingress_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER ingress_no_delete BEFORE DELETE ON ingress_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE ingress_state(tenant TEXT PRIMARY KEY, state BLOB NOT NULL);
";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    tenant: OpaqueRef,
    expected: u64,
    at_ms: u64,
    command: IngressCommand,
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_value(strict_collection(bytes).map_err(invalid)?)?)
}
fn snapshot(c: &Connection, tenant: &OpaqueRef) -> Result<IngressState> {
    let bytes: Option<Vec<u8>> = c
        .query_row("SELECT state FROM ingress_state WHERE tenant=?", [tenant.as_str()], |r| {
            r.get(0)
        })
        .optional()?;
    bytes.map_or_else(|| Ok(IngressState::default()), |b| decode(&b))
}
fn apply(c: &Connection, e: &Entry) -> Result<(IngressState, IngressResult)> {
    let old = snapshot(c, &e.tenant)?;
    if old.revision != e.expected {
        return Err(invalid("ingress revision conflict"));
    }
    let (new, result) = apply_ingress(&old, &e.tenant, e.at_ms, &e.command).map_err(invalid)?;
    Ok((new, result))
}
fn write_state(c: &Connection, tenant: &OpaqueRef, s: &IngressState) -> Result<()> {
    c.execute("INSERT INTO ingress_state VALUES(?,?) ON CONFLICT(tenant) DO UPDATE SET state=excluded.state",params![tenant.as_str(),canonical_collection(s).map_err(invalid)?])?;
    Ok(())
}
fn record(c: &Connection, e: &Entry, fault: Option<&str>) -> Result<(IngressState, IngressResult)> {
    let (new, result) = apply(c, e)?;
    if new.revision != e.expected {
        let bytes = canonical_data(e).map_err(invalid)?;
        c.execute(
            "INSERT INTO ingress_history(tenant,revision,entry,digest) VALUES(?,?,?,?)",
            params![e.tenant.as_str(), new.revision, bytes, ContentHash::of(&bytes).as_str()],
        )?;
        if fault == Some("history") {
            return Err(invalid("injected after ingress history"));
        }
        write_state(c, &e.tenant, &new)?;
        if fault == Some("projection") {
            return Err(invalid("injected after ingress projection"));
        }
    }
    Ok((new, result))
}
fn transact(
    c: &mut Connection,
    tenant: &OpaqueRef,
    expected: Option<u64>,
    now: u64,
    command: &IngressCommand,
    fault: Option<&str>,
    fresh: bool,
) -> Result<IngressResult> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let state = snapshot(&tx, tenant)?;
    let revision = state.revision;
    let now = if fresh { now.max(state.last_at_ms) } else { now };
    if expected.is_some_and(|v| v != revision) {
        return Err(invalid("ingress revision conflict"));
    }
    let e =
        Entry { tenant: tenant.clone(), expected: revision, at_ms: now, command: command.clone() };
    let (_, result) = record(&tx, &e, fault)?;
    tx.commit()?;
    Ok(result)
}
fn transact_dispatch(
    c: &mut Connection,
    tenant: &OpaqueRef,
    now: u64,
    command: &IngressCommand,
    fault: Option<&str>,
) -> Result<IngressResult> {
    let id = match command {
        IngressCommand::RecordQueueTask { id, .. }
        | IngressCommand::RecordQueueSignal { id, .. }
        | IngressCommand::RecordQueueWakeRetired { id, .. }
        | IngressCommand::RecoverQueue { id, .. }
        | IngressCommand::RecordAcknowledgmentSignal { id, .. }
        | IngressCommand::Expire { id } => id,
        _ => return Err(invalid("command is not dispatch progress")),
    };
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let state = snapshot(&tx, tenant)?;
    let now = now.max(state.last_at_ms);
    let e = Entry {
        tenant: tenant.clone(),
        expected: state.revision,
        at_ms: now,
        command: command.clone(),
    };
    let (new, result) = record(&tx, &e, fault)?;
    if new.revision != state.revision && new.queue.contains_key(id) {
        // Retain the existing replay vocabulary. No cursor-only row is written
        // for a no-op or a fenced stale completion, and both writes roll back
        // together if either the progress or cursor cannot be committed. An
        // acknowledgment or expiration can also belong to a non-queued delivery;
        // that progress commits without changing the queue's scan position.
        let cursor = Entry {
            tenant: tenant.clone(),
            expected: new.revision,
            at_ms: now,
            command: IngressCommand::AdvanceDispatchCursor { id: id.clone() },
        };
        record(&tx, &cursor, fault.and_then(|f| f.strip_prefix("cursor-")))?;
    }
    tx.commit()?;
    Ok(result)
}
impl StoreRoot {
    /// Tenant inventory for cross-store reference validation during bundle reopen.
    pub fn ingress_tenants(&self) -> Result<Vec<OpaqueRef>> {
        let c = self.connect(StoreKind::Fabric)?;
        let mut q = c.prepare("SELECT tenant FROM ingress_state ORDER BY tenant")?;
        let rows = q.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|row| row?.try_into().map_err(invalid)).collect()
    }
    pub fn ingress_snapshot(&self, tenant: &OpaqueRef) -> Result<IngressState> {
        snapshot(&self.connect(StoreKind::Fabric)?, tenant)
    }
    pub fn ingress_transact(
        &self,
        tenant: &OpaqueRef,
        expected: Option<u64>,
        now: u64,
        command: &IngressCommand,
    ) -> Result<IngressResult> {
        transact(&mut self.connect(StoreKind::Fabric)?, tenant, expected, now, command, None, false)
    }
    /// Records a completion after external I/O using the journal clock observed under
    /// the same write lock. The caller's timestamp cannot reverse concurrent writes.
    pub fn ingress_transact_fresh(
        &self,
        tenant: &OpaqueRef,
        now: u64,
        command: &IngressCommand,
    ) -> Result<IngressResult> {
        transact(&mut self.connect(StoreKind::Fabric)?, tenant, None, now, command, None, true)
    }
    /// Commits dispatch progress and, for queue entries, its restart scan position
    /// together. Non-queued progress preserves that position. Idle inspection
    /// remains process-local and never enters the replayed journal.
    pub fn ingress_transact_dispatch(
        &self,
        tenant: &OpaqueRef,
        now: u64,
        command: &IngressCommand,
    ) -> Result<IngressResult> {
        transact_dispatch(&mut self.connect(StoreKind::Fabric)?, tenant, now, command, None)
    }
    pub fn ingress_transact_fresh_expected(
        &self,
        tenant: &OpaqueRef,
        expected: u64,
        now: u64,
        command: &IngressCommand,
    ) -> Result<IngressResult> {
        transact(
            &mut self.connect(StoreKind::Fabric)?,
            tenant,
            Some(expected),
            now,
            command,
            None,
            true,
        )
    }
}
pub(crate) fn replay(source: &Connection, target: &Connection) -> Result<()> {
    let mut q =
        source.prepare("SELECT tenant,revision,entry,digest FROM ingress_history ORDER BY seq")?;
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
        let e: Entry = decode(&bytes)?;
        if canonical_data(&e).map_err(invalid)? != bytes
            || ContentHash::of(&bytes).as_str() != hash
            || e.tenant.as_str() != tenant
            || e.expected.checked_add(1) != Some(revision)
        {
            return Err(invalid("corrupt ingress history"));
        }
        let (s, _) = apply(target, &e)?;
        if s.revision != revision {
            return Err(invalid("nonmutating ingress history"));
        }
        write_state(target, &e.tenant, &s)?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use worldinterface_protocol::*;
    #[test]
    fn nonqueued_dispatch_progress_preserves_cursor_and_replays_atomically() {
        let binding: IngressBinding = serde_json::from_str(include_str!(
            "../../../../conformance/wi-fabric-2/ingress/binding.json"
        ))
        .unwrap();
        let tenant = binding.profile.scope.tenant.clone();
        let id = DeliveryId::generate();
        let queued = DeliveryId::generate();
        let mut initial = IngressState::default();
        initial.queue.insert(
            queued.clone(),
            worldinterface_domain::ingress::QueueObligation {
                version: 1,
                delivery: queued.clone(),
                task_request: b"task".to_vec(),
                signal_request: b"signal".to_vec(),
                task_digest: ContentHash::of(b"task"),
                signal_digest: ContentHash::of(b"signal"),
                task_admitted: false,
                signal_sequence: None,
                retired_wake_sequence: None,
                generation: 0,
                prior: vec![],
            },
        );
        initial.dispatch_cursor = Some(queued);
        initial.deliveries.insert(
            id.clone(),
            ObservationDelivery {
                id: id.clone(),
                observation: ObservationId::generate(),
                binding: BindingVersion { id: binding.id, revision: binding.revision },
                subscriber: binding.subscriber,
                policy: binding.policy,
                expires_at_ms: 1000,
                state: DeliveryState::Pending,
                attempts: vec![],
                acknowledgment: None,
                token_invalidated: false,
                wake_key: "wake".to_owned().try_into().unwrap(),
                locator: "locator".to_owned().try_into().unwrap(),
            },
        );
        initial.acknowledgments_to_signal.insert(
            id.clone(),
            worldinterface_domain::ingress::AcknowledgmentSignal {
                version: 1,
                delivery: id.clone(),
                request: b"ack".to_vec(),
                digest: ContentHash::of(b"ack"),
                sequence: None,
            },
        );
        // Both commands are valid without a queue entry. An unrelated queued
        // cursor must survive their success, failure, duplicate and replay paths.
        for command in [
            IngressCommand::RecordAcknowledgmentSignal { id: id.clone(), sequence: 1 },
            IngressCommand::Expire { id: id.clone() },
        ] {
            for fault in ["history", "projection"] {
                let mut c = Connection::open_in_memory().unwrap();
                c.execute_batch(SCHEMA).unwrap();
                write_state(&c, &tenant, &initial).unwrap();
                assert!(transact_dispatch(&mut c, &tenant, 1000, &command, Some(fault)).is_err());
                assert_eq!(snapshot(&c, &tenant).unwrap(), initial);
                assert_eq!(
                    c.query_row("SELECT count(*) FROM ingress_history", [], |r| r.get::<_, u64>(0))
                        .unwrap(),
                    0
                );
                transact_dispatch(&mut c, &tenant, 1000, &command, None).unwrap();
                let committed = snapshot(&c, &tenant).unwrap();
                assert_eq!(committed.dispatch_cursor, initial.dispatch_cursor);
                assert_eq!(committed.queue, initial.queue);
                assert_eq!(committed.revision, 1);
                match &command {
                    IngressCommand::RecordAcknowledgmentSignal { .. } => {
                        assert_eq!(committed.acknowledgments_to_signal[&id].sequence, Some(1));
                    }
                    IngressCommand::Expire { .. } => {
                        assert_eq!(committed.deliveries[&id].state, DeliveryState::Expired);
                        assert!(committed.deliveries[&id].token_invalidated);
                    }
                    _ => unreachable!(),
                }
                transact_dispatch(&mut c, &tenant, 2000, &command, None).unwrap();
                assert_eq!(snapshot(&c, &tenant).unwrap(), committed);
                assert_eq!(
                    c.query_row("SELECT count(*) FROM ingress_history", [], |r| r.get::<_, u64>(0))
                        .unwrap(),
                    1
                );
                let replayed = Connection::open_in_memory().unwrap();
                replayed.execute_batch(SCHEMA).unwrap();
                write_state(&replayed, &tenant, &initial).unwrap();
                replay(&c, &replayed).unwrap();
                assert_eq!(snapshot(&replayed, &tenant).unwrap(), committed);
            }
        }
    }
    #[test]
    fn retired_wake_and_cursor_roll_back_and_replay_atomically() {
        let binding: IngressBinding = serde_json::from_str(include_str!(
            "../../../../conformance/wi-fabric-2/ingress/binding.json"
        ))
        .unwrap();
        let tenant = binding.profile.scope.tenant.clone();
        let id = DeliveryId::generate();
        let queued = id.clone();
        let mut initial = IngressState::default();
        initial.queue.insert(
            queued.clone(),
            worldinterface_domain::ingress::QueueObligation {
                version: 1,
                delivery: queued.clone(),
                task_request: b"task".to_vec(),
                signal_request: b"signal".to_vec(),
                task_digest: ContentHash::of(b"task"),
                signal_digest: ContentHash::of(b"signal"),
                task_admitted: false,
                signal_sequence: None,
                retired_wake_sequence: None,
                generation: 0,
                prior: vec![],
            },
        );
        initial.deliveries.insert(
            id.clone(),
            ObservationDelivery {
                id: id.clone(),
                observation: ObservationId::generate(),
                binding: BindingVersion { id: binding.id, revision: binding.revision },
                subscriber: binding.subscriber,
                policy: binding.policy,
                expires_at_ms: 1000,
                state: DeliveryState::Pending,
                attempts: vec![],
                acknowledgment: None,
                token_invalidated: false,
                wake_key: "wake".to_owned().try_into().unwrap(),
                locator: "locator".to_owned().try_into().unwrap(),
            },
        );
        let command = IngressCommand::RecordQueueWakeRetired {
            id: id.clone(),
            generation: 0,
            signal_digest: ContentHash::of(b"signal"),
            sequence: 7,
        };
        for fault in ["history", "projection", "cursor-history", "cursor-projection"] {
            let mut c = Connection::open_in_memory().unwrap();
            c.execute_batch(SCHEMA).unwrap();
            write_state(&c, &tenant, &initial).unwrap();
            assert!(transact_dispatch(&mut c, &tenant, 999, &command, Some(fault)).is_err());
            assert_eq!(snapshot(&c, &tenant).unwrap(), initial);
            assert_eq!(
                c.query_row("SELECT count(*) FROM ingress_history", [], |r| r.get::<_, u64>(0))
                    .unwrap(),
                0
            );
            transact_dispatch(&mut c, &tenant, 999, &command, None).unwrap();
            let committed = snapshot(&c, &tenant).unwrap();
            assert_eq!(committed.dispatch_cursor, Some(id.clone()));
            assert_eq!(committed.queue[&id].signal_request, initial.queue[&id].signal_request);
            assert_eq!(committed.revision, 2);
            assert_eq!(committed.deliveries[&id].state, DeliveryState::DeadLetter);
            assert_eq!(committed.queue[&id].retired_wake_sequence, Some(7));
            transact_dispatch(&mut c, &tenant, 2000, &command, None).unwrap();
            assert_eq!(snapshot(&c, &tenant).unwrap(), committed);
            assert_eq!(
                c.query_row("SELECT count(*) FROM ingress_history", [], |r| r.get::<_, u64>(0))
                    .unwrap(),
                2
            );
            let replayed = Connection::open_in_memory().unwrap();
            replayed.execute_batch(SCHEMA).unwrap();
            write_state(&replayed, &tenant, &initial).unwrap();
            replay(&c, &replayed).unwrap();
            assert_eq!(snapshot(&replayed, &tenant).unwrap(), committed);
        }
    }
    #[test]
    fn dispatch_progress_and_cursor_roll_back_together_without_idle_writes() {
        let tenant: OpaqueRef = "tenant".to_owned().try_into().unwrap();
        let id = DeliveryId::generate();
        let task_digest = ContentHash::of(b"task");
        let mut initial = IngressState::default();
        initial.queue.insert(
            id.clone(),
            worldinterface_domain::ingress::QueueObligation {
                version: 1,
                delivery: id.clone(),
                task_request: b"task".to_vec(),
                signal_request: b"signal".to_vec(),
                task_digest: task_digest.clone(),
                signal_digest: ContentHash::of(b"signal"),
                task_admitted: false,
                signal_sequence: None,
                retired_wake_sequence: None,
                generation: 0,
                prior: vec![],
            },
        );
        let other = DeliveryId::generate();
        let mut other_queue = initial.queue[&id].clone();
        other_queue.delivery = other.clone();
        initial.queue.insert(other.clone(), other_queue);
        let command = IngressCommand::RecordQueueTask {
            id: id.clone(),
            generation: 0,
            task_digest: task_digest.clone(),
        };
        for fault in ["history", "projection", "cursor-history", "cursor-projection"] {
            let mut c = Connection::open_in_memory().unwrap();
            c.execute_batch(SCHEMA).unwrap();
            write_state(&c, &tenant, &initial).unwrap();
            assert!(transact_dispatch(&mut c, &tenant, 1000, &command, Some(fault)).is_err());
            assert_eq!(snapshot(&c, &tenant).unwrap(), initial);
            assert_eq!(
                c.query_row("SELECT count(*) FROM ingress_history", [], |r| r.get::<_, u64>(0))
                    .unwrap(),
                0
            );
            transact_dispatch(&mut c, &tenant, 1000, &command, None).unwrap();
            let committed = snapshot(&c, &tenant).unwrap();
            assert!(committed.queue[&id].task_admitted);
            assert_eq!(committed.dispatch_cursor, Some(id.clone()));
            assert_eq!(committed.revision, 2);
            transact(
                &mut c,
                &tenant,
                None,
                1000,
                &IngressCommand::AdvanceDispatchCursor { id: other.clone() },
                None,
                true,
            )
            .unwrap();
            let committed = snapshot(&c, &tenant).unwrap();
            // Neither duplicate nor generation-fenced completions write a cursor.
            transact_dispatch(&mut c, &tenant, 2000, &command, None).unwrap();
            let stale = IngressCommand::RecordQueueTask {
                id: id.clone(),
                generation: 1,
                task_digest: task_digest.clone(),
            };
            assert_eq!(
                transact_dispatch(&mut c, &tenant, 2000, &stale, None).unwrap(),
                IngressResult::StaleQueueGeneration
            );
            assert_eq!(snapshot(&c, &tenant).unwrap(), committed);
            assert_eq!(
                c.query_row("SELECT count(*) FROM ingress_history", [], |r| r.get::<_, u64>(0))
                    .unwrap(),
                3
            );
            // Existing command bytes replay to the identical projection.
            let replayed = Connection::open_in_memory().unwrap();
            replayed.execute_batch(SCHEMA).unwrap();
            write_state(&replayed, &tenant, &initial).unwrap();
            replay(&c, &replayed).unwrap();
            assert_eq!(snapshot(&replayed, &tenant).unwrap(), committed);
        }
    }
    #[test]
    fn ingress_journal_and_projection_faults_roll_back_together() {
        // Exercise a real binding transaction through each physical write boundary.
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/wi-fabric-2/ingress/binding.json"
        ))
        .unwrap();
        let binding: IngressBinding = serde_json::from_value(fixture).unwrap();
        for fault in ["history", "projection"] {
            let mut c = Connection::open_in_memory().unwrap();
            c.execute_batch(crate::repository::SCHEMA).unwrap();
            c.execute_batch(SCHEMA).unwrap();
            c.execute_batch(crate::effect::SCHEMA).unwrap();
            let tenant = binding.profile.scope.tenant.clone();
            let command = IngressCommand::Bind { binding: Box::new(binding.clone()) };
            assert!(transact(&mut c, &tenant, None, 1000, &command, Some(fault), false).is_err());
            assert_eq!(snapshot(&c, &tenant).unwrap(), IngressState::default());
            assert_eq!(
                c.query_row("SELECT count(*) FROM ingress_history", [], |r| r.get::<_, u64>(0))
                    .unwrap(),
                0
            );
            transact(&mut c, &tenant, None, 1000, &command, None, false).unwrap();
            crate::repository::validate_database(&c).unwrap();
        }
    }
}
