//! Fabric-owned immutable receipt index. Construction is domain guarded.
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use worldinterface_domain::ingress::digest as domain_digest;
use worldinterface_domain::receipt::ValidatedReceipt;
use worldinterface_domain::receipt::{DisclosureProfile, ReceiptAccess, ReceiptView};
use worldinterface_protocol::{
    ContentHash, EffectIntentId, Envelope, OpaqueRef, ReceiptBody, ReceiptId, Record,
};

pub(crate) fn insert(
    c: &Connection,
    tenant: &OpaqueRef,
    receipt: &ValidatedReceipt,
) -> Result<ContentHash> {
    let envelope = receipt.envelope();
    let Record::BoundaryReceipt(body) = &envelope.record else {
        return Err(invalid("expected boundary receipt"));
    };
    let ReceiptBody::Egress(effect) = &body.body else {
        return Err(invalid("expected egress receipt"));
    };
    if effect.intent_id != *receipt.source_intent() {
        return Err(invalid("receipt intent mismatch"));
    }
    let bytes = envelope.canonical_bytes().map_err(invalid)?;
    Envelope::parse_receipt(&bytes).map_err(invalid)?;
    let digest = envelope.digest().map_err(invalid)?;
    let existing: Option<(String, Vec<u8>)> = c
        .query_row(
            "SELECT digest,canonical FROM receipt_history WHERE id=?",
            [body.id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((old_digest, old_bytes)) = existing {
        if old_digest == digest.as_str() && old_bytes == bytes {
            return Ok(digest);
        }
        return Err(invalid("conflicting receipt identity"));
    }
    c.execute("INSERT INTO receipt_history(id,tenant,intent,source_revision,canonical,digest) VALUES(?,?,?,?,?,?)", params![body.id.as_str(), tenant.as_str(), effect.intent_id.as_str(), receipt.source_revision(), bytes, digest.as_str()])?;
    c.execute(
        "INSERT INTO receipt_index VALUES(?,?,?,?)",
        params![body.id.as_str(), digest.as_str(), tenant.as_str(), effect.intent_id.as_str()],
    )?;
    Ok(digest)
}
impl StoreRoot {
    pub fn inspect_receipt(
        &self,
        access: &ReceiptAccess,
        id: &ReceiptId,
    ) -> Result<Option<ReceiptView>> {
        let c = self.connect(StoreKind::Fabric)?;
        let bytes: Option<Vec<u8>> = c.query_row("SELECT h.canonical FROM receipt_history h JOIN receipt_index i ON i.id=h.id WHERE h.id=? AND i.tenant=?", params![id.as_str(), access.tenant().as_str()], |r| r.get(0)).optional()?;
        bytes
            .map(|b| {
                let envelope = Envelope::parse_receipt(&b).map_err(invalid)?;
                let Record::BoundaryReceipt(ref receipt) = envelope.record else { unreachable!() };
                let direction = match receipt.body {
                    ReceiptBody::Ingress(_) => "ingress",
                    ReceiptBody::Egress(_) => "egress",
                };
                let full = access.disclosure() == DisclosureProfile::Full;
                Ok(ReceiptView {
                    id: receipt.id.clone(),
                    direction: direction.to_string().try_into().map_err(invalid)?,
                    digest: if full { Some(envelope.digest().map_err(invalid)?) } else { None },
                    canonical: if full { Some(envelope) } else { None },
                })
            })
            .transpose()
    }
    pub(crate) fn validate_receipt_artifacts(&self) -> Result<()> {
        let c = Connection::open_with_flags(
            self.db_path(StoreKind::Fabric),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let mut q = c.prepare("SELECT id,tenant,intent,canonical,digest FROM receipt_history")?;
        let rows = q.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Vec<u8>>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        for row in rows {
            let (id, tenant, intent, bytes, digest) = row?;
            let e = Envelope::parse_receipt(&bytes).map_err(invalid)?;
            if e.digest().map_err(invalid)?.as_str() != digest {
                return Err(invalid("receipt digest mismatch"));
            }
            let Record::BoundaryReceipt(r) = e.record else { unreachable!() };
            if r.id.as_str() != id {
                return Err(invalid("receipt identity mismatch"));
            }
            let ReceiptBody::Egress(body) = r.body else {
                return Err(invalid("unsupported receipt direction in Fabric"));
            };
            if body.intent_id.as_str() != intent {
                return Err(invalid("receipt intent mismatch"));
            }
            let state = self.effect_snapshot(&tenant.try_into().map_err(invalid)?)?;
            let effect = state
                .effects
                .get(&body.intent_id)
                .ok_or_else(|| invalid("receipt without authoritative intent"))?;
            let resolution =
                effect.resolution.as_ref().ok_or_else(|| invalid("receipt without resolution"))?;
            let authorization = effect
                .authorization
                .as_ref()
                .ok_or_else(|| invalid("receipt without authorization"))?;
            let inputs = effect
                .authorization_inputs
                .as_ref()
                .ok_or_else(|| invalid("receipt without authorization inputs"))?;
            let provider =
                inputs.worker.as_ref().ok_or_else(|| invalid("receipt without exact provider"))?;
            if body.agency_digest != domain_digest(&effect.agency).map_err(invalid)?
                || body.resolved_effect_digest
                    != domain_digest(&resolution.resolved).map_err(invalid)?
                || body.authorization_digest != domain_digest(authorization).map_err(invalid)?
                || body.executor != provider.provider
                || r.versions.resolver != resolution.resolved.resolver_version
                || r.versions.capability_descriptor != resolution.descriptor
                || r.versions.policy != inputs.policy.revision
                || r.versions.connector_profile != resolution.resolved.registry_snapshot
                || r.versions.provider_profile != domain_digest(provider).map_err(invalid)?
                || !effect.outcomes.iter().any(|o| {
                    o.attempt == body.attempt
                        && o.outcome == body.outcome
                        && o.durable_outputs == body.output_refs
                })
            {
                return Err(invalid("receipt differs from authoritative effect evidence"));
            }
            for reference in &body.output_refs {
                self.read_artifact(reference)?;
            }
        }
        Ok(())
    }
}
pub(crate) fn replay(source: &Connection, target: &Connection) -> Result<()> {
    let mut q = source.prepare("SELECT id,tenant,intent,source_revision,canonical,digest FROM receipt_history ORDER BY seq")?;
    let rows = q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, u64>(3)?,
            r.get::<_, Vec<u8>>(4)?,
            r.get::<_, String>(5)?,
        ))
    })?;
    for row in rows {
        let (id, tenant, intent, revision, bytes, digest) = row?;
        let e = Envelope::parse_receipt(&bytes).map_err(invalid)?;
        if e.digest().map_err(invalid)?.as_str() != digest {
            return Err(invalid("receipt journal digest"));
        }
        let Record::BoundaryReceipt(r) = &e.record else { unreachable!() };
        let ReceiptBody::Egress(body) = &r.body else {
            return Err(invalid("unsupported receipt direction"));
        };
        if r.id.as_str() != id || body.intent_id.as_str() != intent || revision == 0 {
            return Err(invalid("receipt journal references"));
        }
        let tenant: OpaqueRef = tenant.try_into().map_err(invalid)?;
        let state = crate::effect::snapshot_for_replay(target, &tenant)?;
        let effect: &EffectIntentId = &body.intent_id;
        if state.revision < revision || !state.effects.contains_key(effect) {
            return Err(invalid("receipt source revision"));
        }
        target.execute(
            "INSERT INTO receipt_index VALUES(?,?,?,?)",
            params![id, digest, tenant.as_str(), intent],
        )?;
    }
    Ok(())
}
