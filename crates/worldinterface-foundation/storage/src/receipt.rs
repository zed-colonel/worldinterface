//! Fabric-owned immutable receipt index. Construction is domain guarded.
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use worldinterface_domain::receipt::ValidatedReceipt;
use worldinterface_domain::receipt::{DisclosureProfile, ReceiptAccess, ReceiptView};
use worldinterface_protocol::{
    canonical_data, strict_collection, ContentHash, EffectIntentId, Envelope, ExecutionRecord,
    OpaqueRef, ReceiptBody, ReceiptId, Record, TemporalProvenance,
};

type RetryRow = (String, u64, Vec<u8>, String, Vec<u8>, String);
type ExistingRow = (String, String, u64, String, Vec<u8>, Vec<u8>);

/// A committed identity is an exact retry only when the original journal command,
/// execution assertion, and temporal evidence all match. This runs before the
/// stale-state check, so a lost response can recover without another transition.
pub(crate) fn exact_retry(
    c: &Connection,
    tenant: &OpaqueRef,
    command: &worldinterface_domain::effect::ValidatedEffectCommand,
    id: &ReceiptId,
    execution: &ExecutionRecord,
    temporal: &TemporalProvenance,
) -> Result<Option<ContentHash>> {
    let row: Option<RetryRow> = c.query_row(
        "SELECT tenant,source_revision,canonical,digest,execution,execution_digest FROM receipt_history WHERE id=?",
        [id.as_str()],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
    ).optional()?;
    let Some((old_tenant, revision, bytes, digest, execution_bytes, execution_digest)) = row else {
        return Ok(None);
    };
    let original: Option<Vec<u8>> = c
        .query_row(
            "SELECT entry FROM effect_history WHERE tenant=? AND revision=?",
            params![tenant.as_str(), revision],
            |r| r.get(0),
        )
        .optional()?;
    let envelope = Envelope::parse_receipt(&bytes).map_err(invalid)?;
    let Record::BoundaryReceipt(ref receipt) = envelope.record else { unreachable!() };
    if old_tenant != tenant.as_str()
        || original.as_deref() != Some(command.bytes())
        || execution_bytes != canonical_data(execution).map_err(invalid)?
        || execution_digest != ContentHash::of(&execution_bytes).as_str()
        || receipt.temporal != *temporal
        || envelope.digest().map_err(invalid)?.as_str() != digest
    {
        return Err(invalid("conflicting receipt retry identity"));
    }
    Ok(Some(digest.try_into().map_err(invalid)?))
}

pub(crate) fn insert(
    c: &Connection,
    tenant: &OpaqueRef,
    receipt: &ValidatedReceipt,
    execution: &ExecutionRecord,
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
    let execution_bytes = canonical_data(execution).map_err(invalid)?;
    let existing: Option<ExistingRow> = c
        .query_row(
            "SELECT tenant,intent,source_revision,digest,canonical,execution FROM receipt_history WHERE id=?",
            [body.id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        )
        .optional()?;
    if let Some((old_tenant, old_intent, old_revision, old_digest, old_bytes, old_execution)) =
        existing
    {
        if old_tenant == tenant.as_str()
            && old_intent == effect.intent_id.as_str()
            && old_revision == receipt.source_revision()
            && old_digest == digest.as_str()
            && old_bytes == bytes
            && old_execution == execution_bytes
        {
            return Ok(digest);
        }
        return Err(invalid("conflicting receipt identity"));
    }
    c.execute("INSERT INTO receipt_history(id,tenant,intent,source_revision,canonical,digest,execution,execution_digest) VALUES(?,?,?,?,?,?,?,?)", params![body.id.as_str(), tenant.as_str(), effect.intent_id.as_str(), receipt.source_revision(), bytes, digest.as_str(), execution_bytes, ContentHash::of(&execution_bytes).as_str()])?;
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
                let (direction, intent_id, observation_id) = match &receipt.body {
                    ReceiptBody::Ingress(body) => {
                        ("ingress", None, Some(body.observation_id.clone()))
                    }
                    ReceiptBody::Egress(body) => ("egress", Some(body.intent_id.clone()), None),
                };
                let full = access.disclosure() == DisclosureProfile::Full;
                Ok(ReceiptView {
                    id: receipt.id.clone(),
                    direction: direction.to_string().try_into().map_err(invalid)?,
                    digest: Some(envelope.digest().map_err(invalid)?),
                    causal_refs: receipt.causal_refs.clone(),
                    intent_id,
                    observation_id,
                    evidence_hashes: if access.disclosure() == DisclosureProfile::Summary {
                        Vec::new()
                    } else {
                        receipt.evidence.iter().map(|e| e.artifact.content.clone()).collect()
                    },
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
        let mut q = c.prepare("SELECT id,tenant,intent,source_revision,canonical,digest,execution,execution_digest FROM receipt_history")?;
        let rows = q.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, u64>(3)?,
                r.get::<_, Vec<u8>>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Vec<u8>>(6)?,
                r.get::<_, String>(7)?,
            ))
        })?;
        for row in rows {
            let (id, tenant, intent, revision, bytes, digest, execution_bytes, execution_digest) =
                row?;
            if execution_bytes.is_empty()
                || execution_digest != ContentHash::of(&execution_bytes).as_str()
            {
                return Err(invalid("receipt without execution binding"));
            }
            let execution: ExecutionRecord =
                serde_json::from_value(strict_collection(&execution_bytes).map_err(invalid)?)?;
            if canonical_data(&execution).map_err(invalid)? != execution_bytes {
                return Err(invalid("noncanonical execution binding"));
            }
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
            let tenant: OpaqueRef = tenant.try_into().map_err(invalid)?;
            let state = crate::effect::snapshot_at_revision(&c, &tenant, revision)?;
            let effect = state
                .effects
                .get(&body.intent_id)
                .ok_or_else(|| invalid("receipt without authoritative intent"))?;
            let outcome =
                effect.outcomes.last().ok_or_else(|| invalid("receipt without outcome"))?;
            if execution.intent_id != body.intent_id
                || execution.attempt != body.attempt
                || execution.outcome != body.outcome
                || execution.evidence.iter().any(|reference| !outcome.evidence.contains(reference))
            {
                return Err(invalid("receipt execution binding mismatch"));
            }
            let expected = worldinterface_domain::receipt::prepare_egress_receipt(
                effect,
                revision,
                r.id.clone(),
                &execution,
                r.temporal.clone(),
            )
            .map_err(invalid)?;
            if expected.envelope().canonical_bytes().map_err(invalid)? != bytes {
                return Err(invalid("receipt differs from authoritative effect evidence"));
            }
            for reference in &body.output_refs {
                self.read_artifact(reference)?;
            }
            for reference in &r.evidence {
                if reference.artifact.scope != tenant {
                    return Err(invalid("receipt evidence scope"));
                }
                self.read_artifact(&reference.artifact)?;
            }
        }
        Ok(())
    }
}
pub(crate) fn replay(source: &Connection, target: &Connection) -> Result<()> {
    let mut q = source.prepare("SELECT id,tenant,intent,source_revision,canonical,digest,execution,execution_digest FROM receipt_history ORDER BY seq")?;
    let rows = q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, u64>(3)?,
            r.get::<_, Vec<u8>>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, Vec<u8>>(6)?,
            r.get::<_, String>(7)?,
        ))
    })?;
    for row in rows {
        let (id, tenant, intent, revision, bytes, digest, execution_bytes, execution_digest) = row?;
        let e = Envelope::parse_receipt(&bytes).map_err(invalid)?;
        if e.digest().map_err(invalid)?.as_str() != digest
            || execution_bytes.is_empty()
            || ContentHash::of(&execution_bytes).as_str() != execution_digest
        {
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
        let execution: ExecutionRecord =
            serde_json::from_value(strict_collection(&execution_bytes).map_err(invalid)?)?;
        if canonical_data(&execution).map_err(invalid)? != execution_bytes {
            return Err(invalid("noncanonical receipt execution"));
        }
        let original = crate::effect::snapshot_at_revision(source, &tenant, revision)?;
        let source_effect = original
            .effects
            .get(&body.intent_id)
            .ok_or_else(|| invalid("receipt without source effect"))?;
        let expected = worldinterface_domain::receipt::prepare_egress_receipt(
            source_effect,
            revision,
            r.id.clone(),
            &execution,
            r.temporal.clone(),
        )
        .map_err(invalid)?;
        if expected.envelope().canonical_bytes().map_err(invalid)? != bytes {
            return Err(invalid("receipt journal differs from source revision"));
        }
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
