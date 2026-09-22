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
fn transact(
    c: &mut Connection,
    tenant: &OpaqueRef,
    expected: Option<u64>,
    now: u64,
    command: &IngressCommand,
    fault: Option<&str>,
) -> Result<IngressResult> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let revision = snapshot(&tx, tenant)?.revision;
    if expected.is_some_and(|v| v != revision) {
        return Err(invalid("ingress revision conflict"));
    }
    let e =
        Entry { tenant: tenant.clone(), expected: revision, at_ms: now, command: command.clone() };
    let (new, result) = apply(&tx, &e)?;
    if new.revision != revision {
        let bytes = canonical_data(&e).map_err(invalid)?;
        tx.execute(
            "INSERT INTO ingress_history(tenant,revision,entry,digest) VALUES(?,?,?,?)",
            params![tenant.as_str(), new.revision, bytes, ContentHash::of(&bytes).as_str()],
        )?;
        if fault == Some("history") {
            return Err(invalid("injected after ingress history"));
        }
        write_state(&tx, tenant, &new)?;
        if fault == Some("projection") {
            return Err(invalid("injected after ingress projection"));
        }
    }
    tx.commit()?;
    Ok(result)
}
impl StoreRoot {
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
        transact(&mut self.connect(StoreKind::Fabric)?, tenant, expected, now, command, None)
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
            assert!(transact(&mut c, &tenant, None, 1000, &command, Some(fault)).is_err());
            assert_eq!(snapshot(&c, &tenant).unwrap(), IngressState::default());
            assert_eq!(
                c.query_row("SELECT count(*) FROM ingress_history", [], |r| r.get::<_, u64>(0))
                    .unwrap(),
                0
            );
            transact(&mut c, &tenant, None, 1000, &command, None).unwrap();
            crate::repository::validate_database(&c).unwrap();
        }
    }
}
