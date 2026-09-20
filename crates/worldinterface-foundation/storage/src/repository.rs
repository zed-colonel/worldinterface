use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use worldinterface_domain::{evolve, Event, Machine, State};
use worldinterface_protocol::{
    canonical_data, strict_json, ClientRequestId, ContentHash, EntityId, Envelope, OpaqueRef,
    PrincipalId, Record,
};

pub(crate) const SCHEMA: &str = "
PRAGMA user_version=1; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
CREATE TABLE store_identity(singleton INTEGER PRIMARY KEY CHECK(singleton=1), manifest BLOB NOT NULL);
CREATE TRIGGER identity_no_insert BEFORE INSERT ON store_identity WHEN EXISTS(SELECT 1 FROM store_identity) BEGIN SELECT RAISE(ABORT,'immutable identity'); END;
CREATE TRIGGER identity_no_update BEFORE UPDATE ON store_identity BEGIN SELECT RAISE(ABORT,'immutable identity'); END;
CREATE TRIGGER identity_no_delete BEFORE DELETE ON store_identity BEGIN SELECT RAISE(ABORT,'immutable identity'); END;
CREATE TABLE history(seq INTEGER PRIMARY KEY, entity TEXT NOT NULL, revision INTEGER NOT NULL, batch BLOB NOT NULL, digest TEXT NOT NULL, UNIQUE(entity,revision));
CREATE TRIGGER history_no_update BEFORE UPDATE ON history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER history_no_delete BEFORE DELETE ON history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE projections(entity TEXT PRIMARY KEY, revision INTEGER NOT NULL, state BLOB NOT NULL);
CREATE TABLE records(seq INTEGER PRIMARY KEY, entity TEXT NOT NULL, digest TEXT NOT NULL, canonical BLOB NOT NULL);
CREATE TABLE admissions(scope TEXT NOT NULL, principal TEXT NOT NULL, class TEXT NOT NULL, request TEXT NOT NULL, digest TEXT NOT NULL, entity TEXT NOT NULL, PRIMARY KEY(scope,principal,class,request));
CREATE TABLE outbox(key TEXT PRIMARY KEY, entity TEXT NOT NULL, payload TEXT NOT NULL, delivered INTEGER NOT NULL CHECK(delivered IN (0,1)));
CREATE TABLE artifacts(scope TEXT NOT NULL, content TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY(scope,content));
";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionScope {
    pub deployment_tenant: OpaqueRef,
    pub authenticated_principal: PrincipalId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    pub scope: AdmissionScope,
    pub class: String,
    pub request: ClientRequestId,
    pub digest: ContentHash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxEntry {
    pub key: OpaqueRef,
    pub entity: EntityId,
    pub payload: OpaqueRef,
    pub delivered: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub entity: EntityId,
    pub machine: Machine,
    pub expected_revision: u64,
    pub events: Vec<Event>,
    pub records: Vec<Envelope>,
    pub outbox: Vec<OutboxEntry>,
    pub acknowledgments: Vec<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<Admission>,
}
impl Batch {
    pub fn new(entity: EntityId, machine: Machine, expected_revision: u64) -> Self {
        Self {
            entity,
            machine,
            expected_revision,
            events: vec![],
            records: vec![],
            outbox: vec![],
            acknowledgments: vec![],
            admission: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Projection {
    pub revision: u64,
    pub state: State,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryEntry {
    pub cursor: u64,
    pub batch: Batch,
}
pub struct Repository<'a> {
    root: &'a StoreRoot,
    kind: StoreKind,
}
impl StoreRoot {
    pub fn repository(&self, kind: StoreKind) -> Result<Repository<'_>> {
        drop(self.connect(kind)?);
        Ok(Repository { root: self, kind })
    }
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_value(strict_json(bytes).map_err(invalid)?)?)
}
fn projection(c: &Connection, entity: &EntityId) -> Result<Option<Projection>> {
    let row: Option<(u64, Vec<u8>)> = c
        .query_row(
            "SELECT revision,state FROM projections WHERE entity=?",
            [entity.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    row.map(|(revision, bytes)| Ok(Projection { revision, state: decode(&bytes)? })).transpose()
}
fn submission_key(e: &Envelope) -> Result<(&str, &ClientRequestId)> {
    match &e.record {
        Record::EffectSubmission(s) => Ok(("effect", &s.client_request_id)),
        Record::IngressBindingSubmission(s) => Ok(("ingress_binding", &s.client_request_id)),
        Record::CommissioningSubmission(s) => Ok(("commissioning", &s.client_request_id)),
        _ => Err(invalid("admission requires caller submission")),
    }
}
fn apply(c: &Connection, b: &Batch, fault: Option<&str>) -> Result<()> {
    if b.events.is_empty()
        && b.records.is_empty()
        && b.outbox.is_empty()
        && b.acknowledgments.is_empty()
    {
        return Err(invalid("empty batch"));
    }
    let previous = projection(c, &b.entity)?;
    if previous.as_ref().map_or(0, |p| p.revision) != b.expected_revision {
        return Err(invalid("revision conflict"));
    }
    let mut state = previous.map_or_else(|| State::empty(b.entity.clone(), b.machine), |p| p.state);
    if state.machine != b.machine {
        return Err(invalid("machine conflict"));
    }
    for e in &b.events {
        state = evolve(&state, e).map_err(|e| invalid(format!("transition: {e:?}")))?;
    }
    for e in &b.records {
        c.execute(
            "INSERT INTO records(entity,digest,canonical) VALUES(?,?,?)",
            params![
                b.entity.as_str(),
                e.digest().map_err(invalid)?.as_str(),
                e.canonical_bytes().map_err(invalid)?
            ],
        )?;
    }
    if fault == Some("records") {
        return Err(invalid("injected after records"));
    }
    if let Some(a) = &b.admission {
        let submission =
            b.records.first().ok_or_else(|| invalid("missing admission submission"))?;
        let (class, request) = submission_key(submission)?;
        if class != a.class
            || request != &a.request
            || submission.digest().map_err(invalid)? != a.digest
            || b.expected_revision != 0
        {
            return Err(invalid("admission does not bind first submission"));
        }
        c.execute(
            "INSERT INTO admissions VALUES(?,?,?,?,?,?)",
            params![
                a.scope.deployment_tenant.as_str(),
                a.scope.authenticated_principal.as_str(),
                a.class,
                a.request.as_str(),
                a.digest.as_str(),
                b.entity.as_str()
            ],
        )?;
    }
    for o in &b.outbox {
        if o.entity != b.entity || o.delivered {
            return Err(invalid("outbox must belong to source and begin pending"));
        }
        c.execute(
            "INSERT INTO outbox VALUES(?,?,?,0)",
            params![o.key.as_str(), o.entity.as_str(), o.payload.as_str()],
        )?;
    }
    for key in &b.acknowledgments {
        if c.execute(
            "UPDATE outbox SET delivered=1 WHERE key=? AND entity=? AND delivered=0",
            params![key.as_str(), b.entity.as_str()],
        )? != 1
        {
            return Err(invalid("unknown or already acknowledged outbox"));
        }
    }
    if fault == Some("outbox") {
        return Err(invalid("injected after outbox"));
    }
    let revision =
        b.expected_revision.checked_add(1).ok_or_else(|| invalid("revision overflow"))?;
    c.execute("INSERT INTO projections VALUES(?,?,?) ON CONFLICT(entity) DO UPDATE SET revision=excluded.revision,state=excluded.state",params![b.entity.as_str(),revision,canonical_data(&state).map_err(invalid)?])?;
    Ok(())
}
fn append(c: &mut Connection, b: &Batch, fault: Option<&str>) -> Result<()> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    append_in(&tx, b, fault)?;
    tx.commit()?;
    Ok(())
}
fn append_in(c: &Connection, b: &Batch, fault: Option<&str>) -> Result<()> {
    let bytes = canonical_data(b).map_err(invalid)?;
    apply(c, b, fault)?;
    c.execute(
        "INSERT INTO history(entity,revision,batch,digest) VALUES(?,?,?,?)",
        params![
            b.entity.as_str(),
            b.expected_revision + 1,
            bytes,
            ContentHash::of(&bytes).as_str()
        ],
    )?;
    if fault == Some("history") {
        return Err(invalid("injected before commit"));
    }
    Ok(())
}
impl Repository<'_> {
    pub fn append(&self, batch: &Batch) -> Result<()> {
        append(&mut self.root.connect(self.kind)?, batch, None)
    }
    /// Caller supplies trusted transport scope separately. No scope is taken from claims.
    pub fn admit(
        &self,
        scope: AdmissionScope,
        submission: Envelope,
        entity: EntityId,
        event: Event,
        outbox: OutboxEntry,
    ) -> Result<EntityId> {
        let (class, request) = submission_key(&submission)?;
        let digest = submission.digest().map_err(invalid)?;
        let mut c = self.root.connect(self.kind)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String,String)>=tx.query_row("SELECT digest,entity FROM admissions WHERE scope=? AND principal=? AND class=? AND request=?",params![scope.deployment_tenant.as_str(),scope.authenticated_principal.as_str(),class,request.as_str()],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((old, id)) = existing {
            if old != digest.as_str() {
                return Err(invalid("conflicting client request"));
            }
            return id.try_into().map_err(invalid);
        }
        let mut batch = Batch::new(entity.clone(), event.machine, 0);
        batch.admission =
            Some(Admission { scope, class: class.into(), request: request.clone(), digest });
        batch.records.push(submission);
        batch.events.push(event);
        batch.outbox.push(outbox);
        append_in(&tx, &batch, None)?;
        tx.commit()?;
        Ok(entity)
    }
    pub fn projection(&self, entity: &EntityId) -> Result<Option<Projection>> {
        projection(&self.root.connect(self.kind)?, entity)
    }
    pub fn history(&self, entity: &EntityId, cursor: u64, limit: u32) -> Result<Vec<HistoryEntry>> {
        if limit == 0 || limit > 1000 {
            return Err(invalid("history limit must be 1..1000"));
        }
        let c = self.root.connect(self.kind)?;
        let mut q = c.prepare(
            "SELECT seq,batch FROM history WHERE entity=? AND seq>? ORDER BY seq LIMIT ?",
        )?;
        let rows = q.query_map(params![entity.as_str(), cursor, limit], |r| {
            Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        rows.map(|r| {
            let (cursor, bytes) = r?;
            Ok(HistoryEntry { cursor, batch: decode(&bytes)? })
        })
        .collect()
    }
    pub fn records(&self, entity: &EntityId) -> Result<Vec<Envelope>> {
        let c = self.root.connect(self.kind)?;
        let mut q = c.prepare("SELECT canonical FROM records WHERE entity=? ORDER BY seq")?;
        let rows = q.query_map([entity.as_str()], |r| r.get::<_, Vec<u8>>(0))?;
        rows.map(|r| Envelope::parse(&r?).map_err(invalid)).collect()
    }
    pub fn outbox(
        &self,
        pending_only: bool,
        after: Option<&OpaqueRef>,
        limit: u32,
    ) -> Result<Vec<OutboxEntry>> {
        if limit == 0 || limit > 1000 {
            return Err(invalid("outbox limit must be 1..1000"));
        }
        let c = self.root.connect(self.kind)?;
        let mut q=c.prepare("SELECT key,entity,payload,delivered FROM outbox WHERE (?=0 OR delivered=0) AND key>? ORDER BY key LIMIT ?")?;
        let rows =
            q.query_map(params![pending_only, after.map_or("", OpaqueRef::as_str), limit], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, bool>(3)?,
                ))
            })?;
        rows.map(|r| {
            let (key, entity, payload, delivered) = r?;
            Ok(OutboxEntry {
                key: key.try_into().map_err(invalid)?,
                entity: entity.try_into().map_err(invalid)?,
                payload: payload.try_into().map_err(invalid)?,
                delivered,
            })
        })
        .collect()
    }
    pub fn rebuild(&self) -> Result<()> {
        let mut c = self.root.connect(self.kind)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch("DELETE FROM projections; DELETE FROM records; DELETE FROM admissions; DELETE FROM outbox; DELETE FROM ingress_state;")?;
        replay_into(&tx, &tx)?;
        crate::ingress::replay(&tx, &tx)?;
        tx.commit()?;
        Ok(())
    }
}
fn replay_into(source: &Connection, target: &Connection) -> Result<()> {
    let mut q = source.prepare("SELECT entity,revision,batch,digest FROM history ORDER BY seq")?;
    let rows = q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, u64>(1)?,
            r.get::<_, Vec<u8>>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    for r in rows {
        let (entity, revision, bytes, digest) = r?;
        if ContentHash::of(&bytes).as_str() != digest {
            return Err(invalid("corrupt history digest"));
        }
        let b: Batch = decode(&bytes)?;
        if canonical_data(&b).map_err(invalid)? != bytes
            || b.entity.as_str() != entity
            || b.expected_revision.checked_add(1) != Some(revision)
        {
            return Err(invalid("corrupt history envelope"));
        }
        apply(target, &b, None)?;
    }
    Ok(())
}
pub(crate) fn validate_database(c: &Connection) -> Result<()> {
    // Validate under one SQLite read snapshot. Replay never calls authorization or providers.
    c.execute_batch("BEGIN")?;
    let expected = Connection::open_in_memory()?;
    expected.execute_batch(SCHEMA)?;
    expected.execute_batch(crate::ingress::SCHEMA)?;
    replay_into(c, &expected)?;
    crate::ingress::replay(c, &expected)?;
    for query in ["SELECT tenant,state FROM ingress_state ORDER BY tenant", "SELECT entity,revision,state FROM projections ORDER BY entity", "SELECT entity,digest,canonical FROM records ORDER BY seq", "SELECT scope,principal,class,request,digest,entity FROM admissions ORDER BY scope,principal,class,request", "SELECT key,entity,payload,delivered FROM outbox ORDER BY key"] {
  fn values(c: &Connection, sql: &str) -> Result<Vec<Vec<rusqlite::types::Value>>> {
   let mut q=c.prepare(sql)?; let n=q.column_count();
   let rows=q.query_map([],|r|(0..n).map(|i|r.get(i)).collect::<rusqlite::Result<Vec<_>>>())?;
   Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
  }
  if values(c,query)? != values(&expected,query)? { return Err(invalid("projection differs from authoritative history")); }
 }
    // Schema SQL (including append-only triggers and unique indexes) must match exactly.
    let sql =
        "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name";
    let schema = |c: &Connection| -> Result<Vec<(String, String, String)>> {
        let mut q = c.prepare(sql)?;
        let rows = q.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    };
    if schema(c)? != schema(&expected)? {
        return Err(invalid("unsupported schema"));
    }
    c.execute_batch("COMMIT")?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_transaction_fault_rolls_back_records_outbox_history_and_projection() {
        for fault in ["records", "outbox", "history"] {
            let mut c = Connection::open_in_memory().unwrap();
            c.execute_batch(SCHEMA).unwrap();
            c.execute_batch(crate::ingress::SCHEMA).unwrap();
            let entity = EntityId::generate();
            let mut b = Batch::new(entity.clone(), Machine::Effect, 0);
            b.events.push(Event {
                entity: entity.clone(),
                machine: Machine::Effect,
                action: worldinterface_domain::Action::Submit,
                at: "2026-09-19T00:00:00.000000000Z".to_string().try_into().unwrap(),
            });
            b.records.push(Envelope::new(Record::TemporalProvenance(
                worldinterface_protocol::TemporalProvenance {
                    occurred_at: None,
                    observed_at: None,
                    received_at: worldinterface_protocol::TimeEvidence {
                        at: b.events[0].at.clone(),
                        source: "clock".to_string().try_into().unwrap(),
                        assurance: "local".to_string().try_into().unwrap(),
                        uncertainty_nanos: 0,
                    },
                    admitted_at: worldinterface_protocol::TimeEvidence {
                        at: b.events[0].at.clone(),
                        source: "clock".to_string().try_into().unwrap(),
                        assurance: "local".to_string().try_into().unwrap(),
                        uncertainty_nanos: 0,
                    },
                    dispatched_at: None,
                    committed_at: None,
                    reconciled_at: None,
                    sequence: None,
                },
            )));
            b.outbox.push(OutboxEntry {
                key: "key".to_string().try_into().unwrap(),
                entity,
                payload: "payload".to_string().try_into().unwrap(),
                delivered: false,
            });
            assert!(append(&mut c, &b, Some(fault)).is_err());
            for table in ["history", "records", "projections", "outbox", "admissions"] {
                assert_eq!(
                    c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                        .get::<_, u32>(0))
                        .unwrap(),
                    0
                );
            }
            append(&mut c, &b, None).unwrap();
            validate_database(&c).unwrap();
        }
    }
}
