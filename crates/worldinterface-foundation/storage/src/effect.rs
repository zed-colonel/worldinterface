//! Atomic, versioned effect journal and derived source/dispatch outbox.
//! The root is trusted local storage, never an authenticated public API.
use crate::{invalid, Batch, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use worldinterface_domain::effect::{replay_command, EffectState, ValidatedEffectCommand};
use worldinterface_protocol::*;
pub(crate) const SCHEMA: &str = "
CREATE TABLE effect_history(seq INTEGER PRIMARY KEY, tenant TEXT NOT NULL, revision INTEGER NOT NULL, entry BLOB NOT NULL, digest TEXT NOT NULL, UNIQUE(tenant,revision));
CREATE TRIGGER effect_no_update BEFORE UPDATE ON effect_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER effect_no_delete BEFORE DELETE ON effect_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE effect_state(tenant TEXT PRIMARY KEY, state BLOB NOT NULL);
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
fn write(c: &Connection, tenant: &OpaqueRef, s: &EffectState, fault: Option<&str>) -> Result<()> {
    c.execute("INSERT INTO effect_state VALUES(?,?) ON CONFLICT(tenant) DO UPDATE SET state=excluded.state",params![tenant.as_str(),canonical_collection(s).map_err(invalid)?])?;
    if fault == Some("state") {
        return Err(invalid("injected effect state"));
    }
    for (id, e) in &s.effects {
        // An empty list is the durable non-dispatch-ready source outbox. Queue delivery cannot read
        // it as a request. All operation requests/acks remain in the authoritative journal.
        c.execute("INSERT INTO effect_outbox VALUES(?,?,?) ON CONFLICT(tenant,intent) DO UPDATE SET dispatch=excluded.dispatch",params![tenant.as_str(),id.as_str(),canonical_data(&e.dispatches).map_err(invalid)?])?;
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
    let (new, id) = replay_command(&old, tenant, command.bytes()).map_err(invalid)?;
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
    Ok(id)
}
impl StoreRoot {
    pub fn effect_snapshot(&self, tenant: &OpaqueRef) -> Result<EffectState> {
        snapshot(&self.connect(StoreKind::Fabric)?, tenant)
    }
    pub fn effect_transact(&self, command: &ValidatedEffectCommand) -> Result<EffectIntentId> {
        let state = self.effect_snapshot(command.tenant())?;
        let (new, id) =
            replay_command(&state, command.tenant(), command.bytes()).map_err(invalid)?;
        for reference in worldinterface_domain::effect::required_artifacts(&new.effects[&id]) {
            self.read_artifact(reference)?;
        }
        transact(&mut self.connect(StoreKind::Fabric)?, command, None)
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
