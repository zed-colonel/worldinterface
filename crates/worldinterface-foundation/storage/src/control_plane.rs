//! Fabric control-plane journal. Only Service authenticates public operations.
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use worldinterface_domain::{agency::AuthenticatedCaller, control_plane as cp};
use worldinterface_protocol::{control_plane::*, *};

pub(crate) const SCHEMA: &str = "
CREATE TABLE control_history(seq INTEGER PRIMARY KEY, revision INTEGER NOT NULL UNIQUE, entry BLOB NOT NULL, digest TEXT NOT NULL);
CREATE TRIGGER control_no_update BEFORE UPDATE ON control_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TRIGGER control_no_delete BEFORE DELETE ON control_history BEGIN SELECT RAISE(ABORT,'append only'); END;
CREATE TABLE control_state(singleton INTEGER PRIMARY KEY CHECK(singleton=1), state BLOB NOT NULL);
CREATE TABLE control_entries(collection TEXT NOT NULL, key TEXT NOT NULL, value BLOB NOT NULL, PRIMARY KEY(collection,key));
";
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Event {
    Bootstrap {
        command: cp::Bootstrap,
        now: u64,
    },
    Execute {
        tenant: OpaqueRef,
        principal: PrincipalId,
        request: cp::Request,
        now: u64,
    },
    AuthorizeAdministration {
        tenant: OpaqueRef,
        authorizer: PrincipalId,
        requester: PrincipalId,
        command: Box<cp::AdminCommand>,
        expires_at_ms: u64,
        now: u64,
    },
    Object {
        object: ObjectView,
    },
    Permit {
        permit: Box<cp::Permit>,
    },
    WithdrawPermit {
        reference: ObjectRef,
    },
    Delivery {
        id: ContentHash,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<CustodyReason>,
    },
    Revocation {
        secret: ObjectRef,
        version: u64,
        generation: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<CustodyReason>,
    },
}
fn decode<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    Ok(serde_json::from_value(strict_collection(b).map_err(invalid)?)?)
}
fn read(c: &Connection) -> Result<cp::State> {
    let b: Option<Vec<u8>> = c
        .query_row("SELECT state FROM control_state WHERE singleton=1", [], |r| r.get(0))
        .optional()?;
    let Some(b) = b else { return Ok(cp::State::default()) };
    let mut value: serde_json::Value = decode(&b)?;
    let header = value.as_object_mut().ok_or_else(|| invalid("control header"))?;
    for (field, array) in COLLECTIONS {
        header.insert(
            field.to_string(),
            if *array { serde_json::json!([]) } else { serde_json::json!({}) },
        );
    }
    let mut q =
        c.prepare("SELECT collection,key,value FROM control_entries ORDER BY collection,key")?;
    let rows = q.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, Vec<u8>>(2)?))
    })?;
    for row in rows {
        let (field, key, bytes) = row?;
        let entry = decode(&bytes)?;
        let array = COLLECTIONS
            .iter()
            .find(|(f, _)| *f == field)
            .ok_or_else(|| invalid("control collection"))?
            .1;
        if array {
            let items = value[&field].as_array_mut().ok_or_else(|| invalid("control array"))?;
            if key != format!("{:020}", items.len()) {
                return Err(invalid("control sequence"));
            }
            items.push(entry);
        } else {
            value[&field].as_object_mut().ok_or_else(|| invalid("control map"))?.insert(key, entry);
        }
    }
    Ok(serde_json::from_value(value)?)
}

// Each journal fact and projected entry retains the normal encoding bound. The
// aggregate has no single-blob ceiling; reads never append entries. Keys provide
// deterministic replay order and mutations update only the changed entries.
const COLLECTIONS: &[(&str, bool)] = &[
    ("history", true),
    ("objects", true),
    ("permits", true),
    ("issuances", false),
    ("revocations", false),
    ("uses", false),
    ("requests", false),
    ("administration_authorizations", false),
];

type ProjectionEntries = std::collections::BTreeMap<(String, String), Vec<u8>>;
fn entries(s: &cp::State) -> Result<(serde_json::Value, ProjectionEntries)> {
    let mut header = serde_json::to_value(s)?;
    let mut entries = std::collections::BTreeMap::new();
    for (field, array) in COLLECTIONS {
        let value = header.as_object_mut().ok_or_else(|| invalid("control header"))?.remove(*field);
        match value {
            Some(serde_json::Value::Array(items)) if *array => {
                for (index, item) in items.into_iter().enumerate() {
                    entries.insert(
                        (field.to_string(), format!("{index:020}")),
                        canonical_collection(&item).map_err(invalid)?,
                    );
                }
            }
            Some(serde_json::Value::Object(items)) if !*array => {
                for (key, item) in items {
                    entries.insert(
                        (field.to_string(), key),
                        canonical_collection(&item).map_err(invalid)?,
                    );
                }
            }
            None if !*array => {}
            _ => return Err(invalid("control collection type")),
        }
    }
    Ok((header, entries))
}
fn evolve(old: &cp::State, event: &Event) -> cp::Result<cp::State> {
    match event {
        Event::Bootstrap { command, now } => cp::bootstrap(old, command, *now),
        Event::Execute { tenant, principal, request, now } => cp::apply(
            old,
            &AuthenticatedCaller::from_trusted_authentication(tenant.clone(), principal.clone()),
            request,
            *now,
        )
        .map(|v| v.0),
        Event::AuthorizeAdministration {
            tenant,
            authorizer,
            requester,
            command,
            expires_at_ms,
            now,
        } => cp::authorize_administration(
            old,
            &AuthenticatedCaller::from_trusted_authentication(tenant.clone(), authorizer.clone()),
            &AuthenticatedCaller::from_trusted_authentication(tenant.clone(), requester.clone()),
            command,
            *expires_at_ms,
            *now,
        ),
        Event::Object { object } => cp::register_object(old, object),
        Event::Permit { permit } => cp::register_permit(old, permit),
        Event::WithdrawPermit { reference } => cp::withdraw_permit(old, reference),
        Event::Delivery { id, reason } => cp::finish_delivery(old, id, reason.map_or(Ok(()), Err)),
        Event::Revocation { secret, version, generation, reason } => {
            cp::finish_revocation(old, secret, *version, *generation, reason.map_or(Ok(()), Err))
        }
    }
}
fn write(c: &Connection, old: &cp::State, s: &cp::State) -> Result<()> {
    let (header, entries) = entries(s)?;
    let (_, previous) = self::entries(old)?;
    c.execute("INSERT INTO control_state VALUES(1,?) ON CONFLICT(singleton) DO UPDATE SET state=excluded.state", [canonical_collection(&header).map_err(invalid)?])?;
    for (field, key) in previous.keys().filter(|k| !entries.contains_key(*k)) {
        c.execute("DELETE FROM control_entries WHERE collection=? AND key=?", params![field, key])?;
    }
    for ((field, key), bytes) in &entries {
        if previous.get(&(field.clone(), key.clone())) != Some(bytes) {
            c.execute("INSERT INTO control_entries VALUES(?,?,?) ON CONFLICT(collection,key) DO UPDATE SET value=excluded.value", params![field, key, bytes])?;
        }
    }
    Ok(())
}
fn record(c: &Connection, old: &cp::State, event: &Event) -> Result<cp::State> {
    let new = evolve(old, event).map_err(|e| invalid(format!("control plane {e:?}")))?;
    if new == *old {
        return Ok(new);
    }
    let bytes = canonical_collection(event).map_err(invalid)?;
    c.execute(
        "INSERT INTO control_history(revision,entry,digest) VALUES(?,?,?)",
        params![new.revision, &bytes, ContentHash::of(&bytes).as_str()],
    )?;
    write(c, old, &new)?;
    Ok(new)
}
pub(crate) fn replay(source: &Connection, target: &Connection) -> Result<()> {
    let mut q = source.prepare("SELECT revision,entry,digest FROM control_history ORDER BY seq")?;
    let rows = q.query_map([], |r| {
        Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, String>(2)?))
    })?;
    let mut state = cp::State::default();
    for row in rows {
        let (revision, bytes, digest) = row?;
        let event: Event = decode(&bytes)?;
        if ContentHash::of(&bytes).as_str() != digest
            || canonical_collection(&event).map_err(invalid)? != bytes
        {
            return Err(invalid("control history integrity"));
        }
        state = evolve(&state, &event).map_err(|e| invalid(format!("control replay {e:?}")))?;
        if state.revision != revision {
            return Err(invalid("control revision integrity"));
        }
    }
    if state.revision > 0 {
        write(target, &cp::State::default(), &state)?;
    }
    Ok(())
}

/// Trusted-host persistence only. Never return this adapter from Service.
pub struct ControlPlaneRepository<'a> {
    root: &'a StoreRoot,
}
impl StoreRoot {
    pub fn control_plane(&self) -> ControlPlaneRepository<'_> {
        ControlPlaneRepository { root: self }
    }
    pub fn control_plane_snapshot(&self) -> Result<cp::State> {
        read(&self.connect(StoreKind::Fabric)?)
    }
    fn control_host_event(&self, event: Event) -> Result<()> {
        let mut c = self.connect(StoreKind::Fabric)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        record(&tx, &read(&tx)?, &event)?;
        tx.commit()?;
        Ok(())
    }
    pub fn bootstrap_control_plane(&self, command: cp::Bootstrap, now: u64) -> Result<()> {
        self.control_host_event(Event::Bootstrap { command, now })
    }
    /// Trusted host authenticates both contexts. Never expose as a caller route.
    pub fn authorize_control_administration(
        &self,
        authorizer: &AuthenticatedCaller,
        requester: &AuthenticatedCaller,
        command: cp::AdminCommand,
        expires_at_ms: u64,
        now: u64,
    ) -> Result<()> {
        if authorizer.tenant() != requester.tenant() {
            return Err(invalid("control authorization tenant"));
        }
        self.control_host_event(Event::AuthorizeAdministration {
            tenant: requester.tenant().clone(),
            authorizer: authorizer.principal().clone(),
            requester: requester.principal().clone(),
            command: Box::new(command),
            expires_at_ms,
            now,
        })
    }
    pub fn register_control_object(&self, object: ObjectView) -> Result<()> {
        if let Some(artifact) = &object.artifact {
            self.read_artifact(artifact)?;
        }
        self.control_host_event(Event::Object { object })
    }
    pub fn withdraw_control_permit(&self, reference: ObjectRef) -> Result<()> {
        self.control_host_event(Event::WithdrawPermit { reference })
    }
    pub fn register_control_permit(&self, permit: cp::Permit) -> Result<()> {
        self.control_host_event(Event::Permit { permit: Box::new(permit) })
    }
}
fn storage(_: impl std::fmt::Display) -> cp::Error {
    cp::Error::Storage
}
impl cp::Repository for ControlPlaneRepository<'_> {
    fn execute(
        &self,
        caller: &AuthenticatedCaller,
        request: &cp::Request,
        now: u64,
    ) -> cp::Result<(cp::Outcome, bool)> {
        let mut c = self.root.connect(StoreKind::Fabric).map_err(storage)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(storage)?;
        let old = read(&tx).map_err(storage)?;
        let (new, outcome) = cp::apply(&old, caller, request, now)?;
        let fresh = new.revision != old.revision;
        if fresh {
            record(
                &tx,
                &old,
                &Event::Execute {
                    tenant: caller.tenant().clone(),
                    principal: caller.principal().clone(),
                    request: request.clone(),
                    now,
                },
            )
            .map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok((outcome, fresh))
    }
    fn deliver(
        &self,
        id: &ContentHash,
        clock: &impl cp::TrustedClock,
        custody: &impl cp::Custody,
        fresh: bool,
    ) -> cp::Result<CredentialMetadata> {
        let mut c = self.root.connect(StoreKind::Fabric).map_err(storage)?;
        // Serializes delivery against all administration and revocation. Intent
        // was committed by execute, and a process crash cannot retry custody.
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(storage)?;
        let old = read(&tx).map_err(storage)?;
        let m = old.issuances.get(id).cloned().ok_or(cp::Error::Unavailable)?;
        if m.status != IssuanceStatus::Pending {
            return Ok(m);
        }
        // Only the execution that committed intent owns the one delivery attempt.
        // A duplicate may arrive before it; observing Pending cannot cancel it.
        // After interruption Pending requires explicit host reconciliation, never
        // automatic custody replay.
        if !fresh {
            return Ok(m);
        }
        let result = match cp::delivery_reference(&old, id, clock.now_ms()) {
            Ok(reference) => custody.deliver(&reference, &m),
            Err(_) => Err(CustodyReason::Stale),
        };
        let new = record(&tx, &old, &Event::Delivery { id: id.clone(), reason: result.err() })
            .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(new.issuances[id].clone())
    }
    fn revoke(
        &self,
        revision: &cp::Revision,
        custody: &impl cp::Custody,
        fresh: bool,
    ) -> cp::Result<()> {
        let cp::Content::Secret(secret) = &revision.content else {
            return Err(cp::Error::Invalid);
        };
        let mut c = self.root.connect(StoreKind::Fabric).map_err(storage)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(storage)?;
        let old = read(&tx).map_err(storage)?;
        let key = worldinterface_domain::ingress::digest(&(
            &revision.reference,
            secret.version,
            secret.generation,
        ))
        .map_err(storage)?;
        match old.revocations.get(&key) {
            Some(IssuanceStatus::Delivered) => return Ok(()),
            Some(IssuanceStatus::Pending) => {}
            _ => return Err(cp::Error::Unresolved),
        }
        if !fresh {
            return Err(cp::Error::Unresolved);
        }
        let result = custody.revoke(&revision.reference, secret.version, secret.generation);
        record(
            &tx,
            &old,
            &Event::Revocation {
                secret: revision.reference.clone(),
                version: secret.version,
                generation: secret.generation,
                reason: result.err(),
            },
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        result.map_err(|_| cp::Error::Unresolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn control_projection_entries_do_not_share_a_16_mib_ceiling() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/wi-fabric-2/egress/fixture.json"
        ))
        .unwrap();
        let mut b: worldinterface_domain::agency::Binding =
            serde_json::from_value(fixture["snapshot"]["bindings"][0].clone()).unwrap();
        b.principal_authority = (0u32..256).map(|n| ContentHash::of(&n.to_be_bytes())).collect();
        b.capacity_authority = b.principal_authority.clone();
        let reference = ObjectRef {
            scope: Scope { tenant: b.tenant.clone(), organization: None },
            kind: ObjectKind::Binding,
            id: b.id.clone(),
        };
        let revision = cp::Revision {
            schema: 1,
            reference,
            revision: 1,
            previous: None,
            content_digest: ContentHash::of(b"content"),
            content: cp::Content::Binding(b.clone()),
            attribution: cp::Attribution {
                requester: b.principal.clone(),
                actor: b.actor,
                accountable: b.accountable.clone(),
                capacity: b.capacity,
                authorizer: b.accountable,
                approvals: vec![],
                at_ms: 0,
                request: "test".to_string().try_into().unwrap(),
                command: ContentHash::of(b"test"),
                bootstrap: false,
                authorization: None,
            },
        };
        let mut state = cp::State::default();
        state.history = vec![revision; 520];
        assert!(canonical_collection(&state).is_err());
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(SCHEMA).unwrap();
        write(&c, &cp::State::default(), &state).unwrap();
        assert_eq!(read(&c).unwrap(), state);
        let mut next = state.clone();
        next.uses.insert(ContentHash::of(b"reservation"), 1);
        write(&c, &state, &next).unwrap();
        assert_eq!(read(&c).unwrap(), next);
    }
}
