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
    ReconcileUnattempted {
        id: ContentHash,
        fenced: ContentHash,
    },
    ClaimDelivery {
        attempt: CredentialDeliveryAttempt,
    },
    ReconcileDelivery {
        evidence: DeliveryReconciliation,
    },
    ReserveEffect {
        operation: ContentHash,
        limits: Vec<worldinterface_domain::authorization::UseLimit>,
    },
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
    ("delivery_attempts", false),
    ("delivery_reconciliations", false),
    ("effect_reservations", false),
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
        Event::ReconcileUnattempted { id, fenced } => cp::reconcile_unattempted(old, id, fenced),
        Event::ClaimDelivery { attempt } => cp::claim_delivery(old, attempt),
        Event::ReconcileDelivery { evidence } => cp::reconcile_delivery(old, evidence),
        Event::ReserveEffect { operation, limits } => cp::reserve_effect(old, operation, limits),
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
    /// Final provider fence, called while the owning worker transaction excludes
    /// administration. Uses only the logical dispatch's existing reservation.
    pub fn check_control_execution(
        &self,
        tenant: &OpaqueRef,
        principal: &PrincipalId,
        intent: &EffectIntentId,
        dispatch: usize,
        facts: &worldinterface_domain::authorization::AuthorizationSnapshot,
        now: u64,
    ) -> Result<()> {
        let mut state = self.control_plane_snapshot()?;
        if state.configuration.is_none() {
            return Ok(());
        }
        let operation = ContentHash::of(
            &canonical_data(&("wi-effect-use-v1", tenant, intent, dispatch)).map_err(invalid)?,
        );
        if let Some(limits) = state.effect_reservations.get(&operation).cloned() {
            for limit in limits {
                if let Some(n) = state.uses.get_mut(&limit.key) {
                    *n = n.saturating_sub(1);
                }
            }
        }
        let mut current = facts.clone();
        state
            .execution_authority(
                &AuthenticatedCaller::from_trusted_authentication(
                    tenant.clone(),
                    principal.clone(),
                ),
                now,
                &mut current,
            )
            .map_err(|_| invalid("managed dispatch authority unavailable"))?;
        if current.bindings != facts.bindings
            || current.policy != facts.policy
            || current.grants != facts.grants
        {
            return Err(invalid("managed dispatch authority drift"));
        }
        Ok(())
    }
    /// Exclusive ownership for embedded composition. Retain for the host lifetime.
    /// This fences local compositions only; custody fences restored copies.
    pub fn claim_embedded_host(&self) -> Result<std::fs::File> {
        let lock = std::fs::File::open(self.path.join("embedded-host.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        Ok(lock)
    }
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
    /// Trusted recovery only. A restored filesystem cannot establish abandonment;
    /// require custody to fence the exact original owner before changing status.
    pub fn reconcile_control_delivery(
        &self,
        id: &ContentHash,
        custody: &impl cp::Custody,
    ) -> cp::Result<CredentialMetadata> {
        let lock = std::fs::File::open(self.path.join("control-delivery.lock")).map_err(storage)?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| cp::Error::Unresolved)?;
        let mut c = self.connect(StoreKind::Fabric).map_err(storage)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(storage)?;
        let old = read(&tx).map_err(storage)?;
        let m = old.issuances.get(id).cloned().ok_or(cp::Error::Unavailable)?;
        if !matches!(m.status, IssuanceStatus::Pending | IssuanceStatus::Unresolved) {
            return Ok(m);
        }
        let Some(attempt) = old.delivery_attempts.get(id) else {
            if m.status == IssuanceStatus::Unresolved {
                return Ok(m);
            }
            // A restored unattempted intent may have progressed in its original
            // store. Local locks cannot fence that store or a future claim.
            let fenced = custody.fence_unattempted(&m).map_err(|_| cp::Error::Unresolved)?;
            if fenced != cp::unattempted_fencing_binding(&m)? {
                return Err(cp::Error::Unresolved);
            }
            let new = record(&tx, &old, &Event::ReconcileUnattempted { id: id.clone(), fenced })
                .map_err(storage)?;
            tx.commit().map_err(storage)?;
            return Ok(new.issuances[id].clone());
        };
        if old.delivery_reconciliations.get(id).is_some_and(|r| r.invalidated) {
            return Ok(m);
        }
        let fenced = custody.fence(attempt).map_err(|_| cp::Error::Unresolved)?;
        if fenced != cp::fencing_binding(attempt)? {
            return Err(cp::Error::Unresolved);
        }
        // Journal uncertainty before the invalidation call. A second crash can
        // only repeat idempotent invalidation, never delivery or promotion.
        let mut evidence =
            DeliveryReconciliation { attempt: attempt.clone(), fenced, invalidated: false };
        let _uncertain =
            record(&tx, &old, &Event::ReconcileDelivery { evidence: evidence.clone() })
                .map_err(storage)?;
        tx.commit().map_err(storage)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(storage)?;
        evidence.invalidated = custody.invalidate(&m, attempt).is_ok();
        let new = record(&tx, &read(&tx).map_err(storage)?, &Event::ReconcileDelivery { evidence })
            .map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(new.issuances[id].clone())
    }
}

/// Called inside the effect's Fabric transaction. A separate journal records
/// shared uses, but there is no second transaction or cross-WAL dependency.
pub(crate) fn reserve_effect(
    c: &Connection,
    tenant: &OpaqueRef,
    old: &worldinterface_domain::effect::EffectState,
    new: &worldinterface_domain::effect::EffectState,
    id: &EffectIntentId,
    command: &[u8],
) -> Result<()> {
    let control = read(c)?;
    if control.configuration.is_none() {
        return Ok(());
    }
    if control.configuration.as_ref().unwrap().status.tenant != *tenant {
        return Err(invalid("managed tenant mismatch"));
    }
    let effect = &new.effects[id];
    let action = strict_json(command).map_err(invalid)?;
    let phase = action["action"]["operation"].as_str().unwrap_or("");
    if matches!(phase, "prepare_production" | "start_production" | "authorize") {
        let facts = effect
            .authorization_inputs
            .as_ref()
            .ok_or_else(|| invalid("managed effect authority absent"))?;
        let caller = AuthenticatedCaller::from_trusted_authentication(
            tenant.clone(),
            effect.agency.binding.principal.clone(),
        );
        let mut managed = facts.clone();
        // Do not drop a grant already reserved by this logical dispatch.
        let mut current = control.clone();
        let operation = ContentHash::of(
            &canonical_data(&("wi-effect-use-v1", tenant, id, effect.dispatches.len()))
                .map_err(invalid)?,
        );
        if let Some(limits) = control.effect_reservations.get(&operation) {
            for limit in limits {
                if let Some(n) = current.uses.get_mut(&limit.key) {
                    *n = n.saturating_sub(1);
                }
            }
        }
        let at: Timestamp = serde_json::from_value(action["at"].clone())?;
        current
            .execution_authority(&caller, at.epoch_millis().map_err(invalid)?, &mut managed)
            .map_err(|_| invalid("managed authority unavailable"))?;
        if managed.bindings != facts.bindings
            || managed.policy != facts.policy
            || managed.grants != facts.grants
        {
            return Err(invalid("managed authority drift"));
        }
    }
    if new.uses == old.uses {
        return Ok(());
    }
    let limits = effect
        .authorization
        .as_ref()
        .ok_or_else(|| invalid("missing shared reservation"))?
        .reservations
        .clone();
    let operation = ContentHash::of(
        &canonical_data(&("wi-effect-use-v1", tenant, id, effect.dispatches.len()))
            .map_err(invalid)?,
    );
    record(c, &control, &Event::ReserveEffect { operation, limits })?;
    Ok(())
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
        #[cfg(all(feature = "crash-points", not(test)))]
        if fresh && matches!(outcome, cp::Outcome::Credential(_)) {
            crate::crashpoint::checkpoint("control-intent-committed");
        }
        Ok((outcome, fresh))
    }
    fn deliver(
        &self,
        id: &ContentHash,
        clock: &impl cp::TrustedClock,
        custody: &impl cp::Custody,
        fresh: bool,
    ) -> cp::Result<CredentialMetadata> {
        if !fresh {
            return read(&self.root.connect(StoreKind::Fabric).map_err(storage)?)
                .map_err(storage)?
                .issuances
                .get(id)
                .cloned()
                .ok_or(cp::Error::Unavailable);
        }
        let lock =
            std::fs::File::open(self.root.path.join("control-delivery.lock")).map_err(storage)?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| cp::Error::Unresolved)?;
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
        if old.delivery_attempts.contains_key(id) {
            return Ok(m);
        }
        let attempt = CredentialDeliveryAttempt {
            issuance: id.clone(),
            owner: InstanceId::generate().as_str().to_string().try_into().map_err(storage)?,
            metadata: worldinterface_domain::ingress::digest(&m).map_err(storage)?,
            claimed_at_ms: clock.now_ms(),
        };
        record(&tx, &old, &Event::ClaimDelivery { attempt }).map_err(storage)?;
        tx.commit().map_err(storage)?;
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("control-attempt-committed");
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(storage)?;
        let old = read(&tx).map_err(storage)?;
        let result = match cp::delivery_reference(&old, id, clock.now_ms()) {
            Ok(reference) => custody.deliver_attempt(&reference, &m, &old.delivery_attempts[id]),
            Err(_) => Err(CustodyReason::Stale),
        };
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("control-custody-returned");
        let new = record(&tx, &old, &Event::Delivery { id: id.clone(), reason: result.err() })
            .map_err(storage)?;
        tx.commit().map_err(storage)?;
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("control-delivery-committed");
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
#[path = "../../domain/tests/support/control_plane.rs"]
#[allow(dead_code)]
mod control_fixture;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_effect_and_credential_reservations_have_one_atomic_last_use() {
        use super::control_fixture::*;
        use cp::Repository;
        let t = tempfile::tempdir().unwrap();
        let root = StoreRoot::create_empty(
            t.path().join("root"),
            crate::StoreProfile {
                instance: InstanceId::generate(),
                created_at: time(),
                platform: false,
            },
        )
        .unwrap();
        let (_, permit, request, snapshot, _) = issue_fixture(false, Profile::Embedded);
        root.bootstrap_control_plane(bundle(&snapshot, Profile::Embedded), now()).unwrap();
        root.register_control_permit(permit).unwrap();
        let grant = &snapshot.grants[0];
        let key = worldinterface_domain::ingress::digest(&(
            "grant",
            &grant.tenant,
            &grant.issuer,
            &grant.id,
        ))
        .unwrap();
        let limits = vec![worldinterface_domain::authorization::UseLimit {
            key: key.clone(),
            max_uses: grant.max_uses,
        }];
        for n in 0..grant.max_uses - 1 {
            root.control_host_event(Event::ReserveEffect {
                operation: ContentHash::of(&n.to_be_bytes()),
                limits: limits.clone(),
            })
            .unwrap();
        }
        let barrier = std::sync::Barrier::new(2);
        let (effect, credential) = std::thread::scope(|threads| {
            let effect = threads.spawn(|| {
                barrier.wait();
                root.control_host_event(Event::ReserveEffect {
                    operation: ContentHash::of(b"last-effect"),
                    limits: limits.clone(),
                })
                .is_ok()
            });
            let credential = threads.spawn(|| {
                barrier.wait();
                root.control_plane().execute(&caller(&snapshot), &request, now()).is_ok()
            });
            (effect.join().unwrap(), credential.join().unwrap())
        });
        assert_ne!(effect, credential, "only one logical operation can own the remaining use");
        let state = root.control_plane_snapshot().unwrap();
        assert_eq!(state.uses[&key], grant.max_uses);
        if effect {
            root.control_host_event(Event::ReserveEffect {
                operation: ContentHash::of(b"last-effect"),
                limits,
            })
            .unwrap();
            assert_eq!(root.control_plane_snapshot().unwrap(), state);
        }
        root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        assert_eq!(root.control_plane_snapshot().unwrap(), state);
    }
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
