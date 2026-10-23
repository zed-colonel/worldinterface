//! Trusted persistence. Source hooks run inside their owner's SQLite transaction.
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use worldinterface_domain::continuation::*;
use worldinterface_protocol::{canonical_data, strict_json, ContentHash, OpaqueRef};

pub(crate) const SCHEMA: &str = "
CREATE TABLE continuation_history(seq INTEGER PRIMARY KEY, entry BLOB NOT NULL, digest TEXT NOT NULL);
CREATE TRIGGER continuation_no_update BEFORE UPDATE ON continuation_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER continuation_no_delete BEFORE DELETE ON continuation_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE event_sources(id TEXT PRIMARY KEY, canonical BLOB NOT NULL, digest TEXT NOT NULL);
CREATE TRIGGER event_source_no_update BEFORE UPDATE ON event_sources BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER event_source_no_delete BEFORE DELETE ON event_sources BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE signal_outbox(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, event BLOB NOT NULL, request BLOB, digest TEXT, sequence INTEGER, rejection TEXT);
CREATE TABLE worker_event_sequences(stream TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT NOT NULL, PRIMARY KEY(stream,sequence));
CREATE INDEX signal_pending ON signal_outbox(id) WHERE sequence IS NULL;
";
#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Record { event: DurableEvent },
    Prepare { id: OpaqueRef, request: Vec<u8> },
    Rejected { id: OpaqueRef, code: String },
    Acknowledge { id: OpaqueRef, digest: ContentHash, sequence: u64 },
}
fn decode<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    Ok(serde_json::from_value(strict_json(b).map_err(invalid)?)?)
}
pub(crate) fn insert(c: &Connection, event: &DurableEvent) -> Result<()> {
    event.validate().map_err(invalid)?;
    let bytes = canonical_data(event).map_err(invalid)?;
    let source: Option<(Vec<u8>, String)> = c
        .query_row(
            "SELECT canonical,digest FROM event_sources WHERE id=?",
            [event.id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((b, d)) = source {
        if b != bytes || d != ContentHash::of(&bytes).as_str() {
            return Err(invalid("authoritative event conflict"));
        }
    } else {
        c.execute(
            "INSERT INTO event_sources VALUES(?,?,?)",
            params![event.id.as_str(), &bytes, ContentHash::of(&bytes).as_str()],
        )?;
    }
    let old: Option<Vec<u8>> = c
        .query_row("SELECT event FROM signal_outbox WHERE id=?", [event.id.as_str()], |r| r.get(0))
        .optional()?;
    if let Some(old) = old {
        if old != bytes {
            return Err(invalid("conflicting event identity"));
        }
        return Ok(());
    }
    if event.route.kind == EventKind::WorkerObservation {
        let stream = event.route.correlation_id().map_err(invalid)?;
        let last: Option<u64> = c.query_row(
            "SELECT max(sequence) FROM worker_event_sequences WHERE stream=?",
            [&stream],
            |r| r.get(0),
        )?;
        if last.is_some_and(|v| event.revision <= v) {
            return Err(invalid("worker observation sequence conflicts or regresses"));
        }
        c.execute(
            "INSERT INTO worker_event_sequences VALUES(?,?,?)",
            params![stream, event.revision, event.id.as_str()],
        )?;
    }
    c.execute(
        "INSERT INTO signal_outbox(id,tenant,event) VALUES(?,?,?)",
        params![event.id.as_str(), event.route.tenant.as_str(), bytes],
    )?;
    Ok(())
}
fn get(c: &Connection, id: &OpaqueRef) -> Result<SignalObligation> {
    let (e, r, d, sequence, rejection) = c.query_row(
        "SELECT event,request,digest,sequence,rejection FROM signal_outbox WHERE id=?",
        [id.as_str()],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<u64>>(3)?,
                r.get::<_, Option<String>>(4)?,
            ))
        },
    )?;
    Ok(SignalObligation {
        event: decode(&e)?,
        request: r,
        request_digest: d.map(|d| d.try_into().map_err(invalid)).transpose()?,
        sequence,
        rejection,
    })
}
fn apply(c: &Connection, cmd: &Command) -> Result<bool> {
    match cmd {
        Command::Record { event } => {
            let existed: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM signal_outbox WHERE id=?)",
                [event.id.as_str()],
                |r| r.get(0),
            )?;
            insert(c, event)?;
            Ok(!existed)
        }
        Command::Prepare { id, request } => {
            if request.is_empty() || request.len() > 16384 {
                return Err(invalid("signal request bound"));
            }
            let old = get(c, id)?;
            if let Some(bytes) = old.request {
                if bytes != *request {
                    return Err(invalid("frozen signal request conflict"));
                }
                return Ok(false);
            }
            c.execute(
                "UPDATE signal_outbox SET request=?,digest=? WHERE id=?",
                params![request, ContentHash::of(request).as_str(), id.as_str()],
            )?;
            Ok(true)
        }
        Command::Rejected { id, code } => {
            if ![
                "backpressure",
                "forbidden",
                "invalid_request",
                "conflict",
                "storage_unavailable",
                "signal_committed_recovery_required",
            ]
            .contains(&code.as_str())
            {
                return Err(invalid("unknown signal diagnostic"));
            }
            let old = get(c, id)?;
            if old.sequence.is_some() {
                return Err(invalid("diagnostic after acknowledgment"));
            }
            if old.rejection.as_ref() == Some(code) {
                return Ok(false);
            }
            c.execute(
                "UPDATE signal_outbox SET rejection=? WHERE id=?",
                params![code, id.as_str()],
            )?;
            Ok(true)
        }
        Command::Acknowledge { id, digest, sequence } => {
            let old = get(c, id)?;
            if *sequence == 0 || old.request_digest.as_ref() != Some(digest) {
                return Err(invalid("signal acknowledgment mismatch"));
            }
            if let Some(old) = old.sequence {
                if old != *sequence {
                    return Err(invalid("signal sequence conflict"));
                }
                return Ok(false);
            }
            c.execute(
                "UPDATE signal_outbox SET sequence=?,rejection=NULL WHERE id=?",
                params![sequence, id.as_str()],
            )?;
            Ok(true)
        }
    }
}
fn transact(root: &StoreRoot, kind: StoreKind, cmd: Command) -> Result<()> {
    let mut c = root.connect(kind)?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if apply(&tx, &cmd)? {
        let b = canonical_data(&cmd).map_err(invalid)?;
        tx.execute(
            "INSERT INTO continuation_history(entry,digest) VALUES(?,?)",
            params![b, ContentHash::of(&b).as_str()],
        )?;
    }
    #[cfg(all(feature = "crash-points", not(test)))]
    crate::crashpoint::checkpoint("continuation-before-source-commit");
    tx.commit()?;
    #[cfg(all(feature = "crash-points", not(test)))]
    crate::crashpoint::checkpoint("continuation-source-committed");
    Ok(())
}
impl StoreRoot {
    pub fn record_event(&self, event: DurableEvent) -> Result<()> {
        if let Some(a) = &event.artifact {
            self.read_artifact(a)?;
        }
        if !matches!(event.route.kind, EventKind::ProviderCallback | EventKind::WorkerObservation) {
            return Err(invalid("owner events require owner transaction"));
        }
        transact(self, StoreKind::Fabric, Command::Record { event })
    }
    pub fn authoritative_event(&self, kind: StoreKind, id: &OpaqueRef) -> Result<DurableEvent> {
        let c = self.connect(kind)?;
        let (b, d): (Vec<u8>, String) = c.query_row(
            "SELECT canonical,digest FROM event_sources WHERE id=?",
            [id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let event: DurableEvent = decode(&b)?;
        event.validate().map_err(invalid)?;
        if event.id != *id
            || ContentHash::of(&b).as_str() != d
            || canonical_data(&event).map_err(invalid)? != b
        {
            return Err(invalid("corrupt authoritative event"));
        }
        Ok(event)
    }
    pub fn signal_obligation(&self, kind: StoreKind, id: &OpaqueRef) -> Result<SignalObligation> {
        get(&self.connect(kind)?, id)
    }
    pub fn pending_signals(
        &self,
        kind: StoreKind,
        after: Option<&OpaqueRef>,
        limit: u32,
    ) -> Result<Vec<SignalObligation>> {
        if limit == 0 || limit > 1000 {
            return Err(invalid("signal scan limit must be 1..1000"));
        }
        let c = self.connect(kind)?;
        let mut q = c.prepare("SELECT id FROM signal_outbox INDEXED BY signal_pending WHERE sequence IS NULL AND id>? ORDER BY id LIMIT ?")?;
        let ids = q.query_map(params![after.map_or("", OpaqueRef::as_str), limit], |r| {
            r.get::<_, String>(0)
        })?;
        ids.map(|id| get(&c, &id?.try_into().map_err(invalid)?)).collect()
    }
    pub fn signal_obligations(
        &self,
        kind: StoreKind,
        after: Option<&OpaqueRef>,
        limit: u32,
    ) -> Result<Vec<SignalObligation>> {
        if limit == 0 || limit > 1000 {
            return Err(invalid("signal scan limit must be 1..1000"));
        }
        let c = self.connect(kind)?;
        let mut q = c.prepare("SELECT id FROM signal_outbox WHERE id>? ORDER BY id LIMIT ?")?;
        let ids = q.query_map(params![after.map_or("", OpaqueRef::as_str), limit], |r| {
            r.get::<_, String>(0)
        })?;
        ids.map(|id| get(&c, &id?.try_into().map_err(invalid)?)).collect()
    }
    pub fn prepare_signal_request(
        &self,
        kind: StoreKind,
        id: OpaqueRef,
        request: Vec<u8>,
    ) -> Result<()> {
        transact(self, kind, Command::Prepare { id, request })
    }
    pub fn record_signal_rejection(
        &self,
        kind: StoreKind,
        id: OpaqueRef,
        code: String,
    ) -> Result<()> {
        transact(self, kind, Command::Rejected { id, code })
    }
    pub fn acknowledge_signal(
        &self,
        kind: StoreKind,
        id: OpaqueRef,
        digest: ContentHash,
        sequence: u64,
    ) -> Result<()> {
        transact(self, kind, Command::Acknowledge { id, digest, sequence })
    }
    pub(crate) fn validate_continuation_artifacts(&self) -> Result<()> {
        for kind in [StoreKind::Fabric, StoreKind::Flow] {
            let c = Connection::open_with_flags(
                self.db_path(kind),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            let mut q = c.prepare("SELECT event FROM signal_outbox")?;
            for b in q.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
                let e: DurableEvent = decode(&b?)?;
                if let Some(a) = e.artifact {
                    self.read_artifact(&a)?;
                }
            }
        }
        Ok(())
    }
}
pub(crate) fn replay(source: &Connection, target: &Connection) -> Result<()> {
    let mut q = source.prepare("SELECT entry,digest FROM continuation_history ORDER BY seq")?;
    for r in q.query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?)))? {
        let (b, d) = r?;
        let cmd: Command = decode(&b)?;
        if ContentHash::of(&b).as_str() != d
            || canonical_data(&cmd).map_err(invalid)? != b
            || !apply(target, &cmd)?
        {
            return Err(invalid("corrupt continuation history"));
        }
    }
    Ok(())
}

pub(crate) fn ingress_events(
    c: &Connection,
    tenant: &OpaqueRef,
    old: &worldinterface_domain::ingress::IngressState,
    new: &worldinterface_domain::ingress::IngressState,
) -> Result<()> {
    for (id, delivery) in &new.deliveries {
        let kind = if !old.deliveries.contains_key(id) {
            Some(EventKind::IngressReady)
        } else if delivery.acknowledgment.is_some() && old.deliveries[id].acknowledgment.is_none() {
            Some(EventKind::IngressAcknowledged)
        } else {
            None
        };
        if let Some(kind) = kind {
            let source: OpaqueRef = id.as_str().to_string().try_into().map_err(invalid)?;
            let route = EventRoute {
                tenant: tenant.clone(),
                audience: delivery.subscriber.clone(),
                kind,
                correlation: source.clone(),
                generation: None,
            };
            insert(
                c,
                &DurableEvent::new(
                    route,
                    source,
                    new.revision,
                    ContentHash::of(&canonical_data(delivery).map_err(invalid)?),
                    None,
                )
                .map_err(invalid)?,
            )?;
        }
    }
    Ok(())
}
pub(crate) fn effect_events(
    c: &Connection,
    tenant: &OpaqueRef,
    old: &worldinterface_domain::effect::EffectState,
    new: &worldinterface_domain::effect::EffectState,
) -> Result<()> {
    for (id, effect) in &new.effects {
        for case in &effect.reconciliation_cases {
            let count = old
                .effects
                .get(id)
                .and_then(|e| e.reconciliation_cases.iter().find(|v| v.id == case.id))
                .map_or(0, |v| v.receipts.len());
            for receipt in case.receipts.iter().skip(count) {
                let route = EventRoute {
                    tenant: tenant.clone(),
                    audience: effect.agency.binding.principal.clone(),
                    kind: EventKind::Reconciliation,
                    correlation: case.id.clone(),
                    generation: None,
                };
                insert(
                    c,
                    &DurableEvent::new(
                        route,
                        case.id.clone(),
                        receipt.source_revision,
                        ContentHash::of(&canonical_data(receipt).map_err(invalid)?),
                        None,
                    )
                    .map_err(invalid)?,
                )?;
            }
        }
    }
    Ok(())
}
pub(crate) fn flow_events(
    c: &Connection,
    tenant: &OpaqueRef,
    old: &crate::flow::FlowState,
    new: &crate::flow::FlowState,
) -> Result<()> {
    for (id, run) in &new.runs {
        for (node, result) in &run.results {
            if old.runs.get(id).is_some_and(|r| r.results.contains_key(node)) {
                continue;
            }
            let source =
                worldinterface_domain::continuation::flow_child_ref(id, node).map_err(invalid)?;
            let route = EventRoute {
                tenant: tenant.clone(),
                audience: run.principal.clone(),
                kind: EventKind::FlowChild,
                correlation: source.clone(),
                generation: None,
            };
            insert(
                c,
                &DurableEvent::new(
                    route,
                    source,
                    new.revision,
                    ContentHash::of(&canonical_data(result).map_err(invalid)?),
                    None,
                )
                .map_err(invalid)?,
            )?;
        }
    }
    Ok(())
}

/// Development crash seam immediately after AQ commit and before WI acknowledgment.
pub fn continuation_signal_admitted() {
    #[cfg(all(feature = "crash-points", not(test)))]
    crate::crashpoint::checkpoint("continuation-signal-admitted");
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_event_and_signal_obligation_roll_back_together() {
        let mut c = Connection::open_in_memory().unwrap();
        c.execute_batch(SCHEMA).unwrap();
        let event = DurableEvent::new(
            EventRoute {
                tenant: "tenant".to_string().try_into().unwrap(),
                audience: worldinterface_protocol::PrincipalId::generate(),
                kind: EventKind::ProviderCallback,
                correlation: "effect".to_string().try_into().unwrap(),
                generation: None,
            },
            "source".to_string().try_into().unwrap(),
            1,
            ContentHash::of(b"source"),
            None,
        )
        .unwrap();
        {
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate).unwrap();
            insert(&tx, &event).unwrap();
            assert_eq!(
                tx.query_row("SELECT count(*) FROM event_sources", [], |r| r.get::<_, u32>(0))
                    .unwrap(),
                1
            );
            // Dropping this uncommitted source transaction models a failed owner write.
        }
        for table in ["event_sources", "signal_outbox", "continuation_history"] {
            assert_eq!(
                c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get::<_, u32>(0))
                    .unwrap(),
                0
            );
        }
    }
}
