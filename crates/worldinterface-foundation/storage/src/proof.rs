//! Detached proof records and reference-only diagnostic streams.
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OptionalExtension};
use worldinterface_domain::receipt::{DisclosureProfile, ReceiptAccess};
use worldinterface_protocol::receipt::{ProofBundleV1, SigningProofV1};
use worldinterface_protocol::{
    canonical_data, strict_json, ContentHash, EvidenceRef, OpaqueRef, ReceiptId,
};

fn receipt_digest(
    c: &Connection,
    tenant: &OpaqueRef,
    id: &ReceiptId,
) -> Result<Option<ContentHash>> {
    let digest: Option<String> = c.query_row(
        "SELECT digest FROM receipt_index WHERE tenant=? AND id=? UNION ALL SELECT digest FROM ingress_receipt_index WHERE tenant=? AND id=?",
        params![tenant.as_str(), id.as_str(), tenant.as_str(), id.as_str()], |r| r.get(0)
    ).optional()?;
    digest.map(|v| v.try_into().map_err(invalid)).transpose()
}
impl StoreRoot {
    /// The signing provider and key lifecycle are external. This stores its immutable
    /// provenance assertion only, without changing the boundary receipt.
    /// Trusted-host append; the host must authorize this tenant and signing provider.
    pub fn append_receipt_proof(
        &self,
        tenant: &OpaqueRef,
        receipt: &ReceiptId,
        proof: &SigningProofV1,
    ) -> Result<ContentHash> {
        proof.signing_input().map_err(invalid)?;
        if proof.key_state.artifact.scope != *tenant || proof.signature.scope != *tenant {
            return Err(invalid("cross-tenant signing proof"));
        }
        self.read_artifact(&proof.key_state.artifact)?;
        self.read_artifact(&proof.signature)?;
        let c = self.connect(StoreKind::Fabric)?;
        if receipt_digest(&c, tenant, receipt)?.as_ref() != Some(&proof.receipt_digest) {
            return Err(invalid("signing proof has no matching tenant receipt"));
        }
        let bytes = canonical_data(proof).map_err(invalid)?;
        let digest = ContentHash::of(&bytes);
        c.execute(
            "INSERT OR IGNORE INTO receipt_proofs(tenant,receipt,digest,canonical) VALUES(?,?,?,?)",
            params![tenant.as_str(), receipt.as_str(), digest.as_str(), bytes],
        )?;
        Ok(digest)
    }
    pub fn receipt_proofs(
        &self,
        access: &ReceiptAccess,
        receipt: &ReceiptId,
    ) -> Result<Vec<(ContentHash, SigningProofV1)>> {
        if access.disclosure() != DisclosureProfile::Full
            || self.inspect_receipt(access, receipt)?.is_none()
        {
            return Err(invalid("receipt proof disclosure denied"));
        }
        let c = self.connect(StoreKind::Fabric)?;
        let mut q = c.prepare(
            "SELECT digest,canonical FROM receipt_proofs WHERE tenant=? AND receipt=? ORDER BY seq",
        )?;
        let rows = q.query_map(params![access.tenant().as_str(), receipt.as_str()], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|(digest, bytes)| {
                Ok((
                    digest.try_into().map_err(invalid)?,
                    serde_json::from_value(strict_json(&bytes).map_err(invalid)?)?,
                ))
            })
            .collect()
    }
    /// Trusted-host append; authorizing a receipt does not authorize a bundle append.
    pub fn append_proof_bundle(
        &self,
        tenant: &OpaqueRef,
        id: &OpaqueRef,
        bundle: &ProofBundleV1,
    ) -> Result<ContentHash> {
        bundle.validate().map_err(invalid)?;
        let mut connection = self.connect(StoreKind::Fabric)?;
        let c = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for digest in &bundle.records {
            let exists: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM receipt_index WHERE tenant=? AND digest=? UNION ALL SELECT 1 FROM ingress_receipt_index WHERE tenant=? AND digest=?)",
                params![tenant.as_str(), digest.as_str(), tenant.as_str(), digest.as_str()], |r| r.get(0))?;
            if !exists {
                return Err(invalid("proof bundle references foreign or absent receipt"));
            }
        }
        for reference in &bundle.evidence {
            if reference.artifact.scope != *tenant {
                return Err(invalid("cross-tenant proof evidence"));
            }
            self.read_artifact(&reference.artifact)?;
        }
        let bytes = canonical_data(bundle).map_err(invalid)?;
        let digest = ContentHash::of(&bytes);
        let old: Option<(String, String, Vec<u8>)> = c
            .query_row(
                "SELECT tenant,digest,canonical FROM proof_bundles WHERE tenant=? AND id=?",
                params![tenant.as_str(), id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((old_tenant, old_digest, old_bytes)) = old {
            if old_tenant != tenant.as_str() || old_digest != digest.as_str() || old_bytes != bytes
            {
                return Err(invalid("conflicting proof bundle identity"));
            }
            return Ok(digest);
        }
        c.execute(
            "INSERT INTO proof_bundles(tenant,id,digest,canonical) VALUES(?,?,?,?)",
            params![tenant.as_str(), id.as_str(), digest.as_str(), bytes],
        )?;
        c.commit()?;
        Ok(digest)
    }
    /// Receipt ownership and full disclosure are necessary. Additional evidence
    /// requires a separate trusted-host grant for every returned reference.
    pub fn inspect_proof_bundle(
        &self,
        access: &ReceiptAccess,
        id: &OpaqueRef,
        mut evidence_allowed: impl FnMut(&EvidenceRef) -> Result<bool>,
    ) -> Result<Option<ProofBundleV1>> {
        if access.disclosure() != DisclosureProfile::Full {
            return Err(invalid("proof bundle disclosure denied"));
        }
        let c = self.connect(StoreKind::Fabric)?;
        let bytes: Option<Vec<u8>> = c
            .query_row(
                "SELECT canonical FROM proof_bundles WHERE tenant=? AND id=?",
                params![access.tenant().as_str(), id.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        bytes
            .map(|b| {
                let bundle: ProofBundleV1 =
                    serde_json::from_value(strict_json(&b).map_err(invalid)?)?;
                bundle.validate().map_err(invalid)?;
                for receipt in self.proof_bundle_receipts(access.tenant(), &bundle)? {
                    if self.inspect_receipt(access, &receipt)?.is_none() {
                        return Err(invalid("proof bundle member disclosure denied"));
                    }
                }
                for reference in &bundle.evidence {
                    if !evidence_allowed(reference)? {
                        return Err(invalid("proof bundle evidence disclosure denied"));
                    }
                }
                Ok(bundle)
            })
            .transpose()
    }
    /// Trusted-host lookup; public callers must authorize every returned receipt.
    pub fn proof_bundle_receipts(
        &self,
        tenant: &OpaqueRef,
        bundle: &ProofBundleV1,
    ) -> Result<Vec<ReceiptId>> {
        let c = self.connect(StoreKind::Fabric)?;
        bundle.records.iter().map(|digest| {
            let id: String = c.query_row(
                "SELECT id FROM receipt_index WHERE tenant=? AND digest=? UNION ALL SELECT id FROM ingress_receipt_index WHERE tenant=? AND digest=?",
                params![tenant.as_str(), digest.as_str(), tenant.as_str(), digest.as_str()], |r| r.get(0))?;
            id.try_into().map_err(invalid)
        }).collect()
    }
    /// Trace and telemetry contain references only and have explicit retention classes.
    pub fn append_diagnostic_reference(
        &self,
        tenant: &OpaqueRef,
        trace: bool,
        id: &OpaqueRef,
        receipt: &ReceiptId,
        retention: &OpaqueRef,
    ) -> Result<ContentHash> {
        if !["short", "standard", "long"].contains(&retention.as_str()) {
            return Err(invalid("unknown diagnostic retention"));
        }
        let fabric = self.connect(StoreKind::Fabric)?;
        if receipt_digest(&fabric, tenant, receipt)?.is_none() {
            return Err(invalid("diagnostic references foreign or absent receipt"));
        }
        let bytes = canonical_data(&(tenant, id, receipt, retention)).map_err(invalid)?;
        let digest = ContentHash::of(&bytes);
        let flow = self.connect(StoreKind::Flow)?;
        let table = if trace { "workflow_trace" } else { "telemetry" };
        let sql =
            format!("INSERT INTO {table}(tenant,id,receipt,retention,digest) VALUES(?,?,?,?,?)");
        flow.execute(
            &sql,
            params![
                tenant.as_str(),
                id.as_str(),
                receipt.as_str(),
                retention.as_str(),
                digest.as_str()
            ],
        )?;
        Ok(digest)
    }
}

pub(crate) fn validate(root: &StoreRoot) -> Result<()> {
    let c = Connection::open_with_flags(
        root.db_path(StoreKind::Fabric),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut q =
        c.prepare("SELECT tenant,receipt,digest,canonical FROM receipt_proofs ORDER BY seq")?;
    let rows = q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, Vec<u8>>(3)?,
        ))
    })?;
    for row in rows {
        let (tenant, receipt, digest, bytes) = row?;
        let tenant: OpaqueRef = tenant.try_into().map_err(invalid)?;
        let receipt: ReceiptId = receipt.try_into().map_err(invalid)?;
        let proof: SigningProofV1 = serde_json::from_value(strict_json(&bytes).map_err(invalid)?)?;
        proof.signing_input().map_err(invalid)?;
        if ContentHash::of(&bytes).as_str() != digest
            || canonical_data(&proof).map_err(invalid)? != bytes
            || receipt_digest(&c, &tenant, &receipt)?.as_ref() != Some(&proof.receipt_digest)
            || proof.key_state.artifact.scope != tenant
            || proof.signature.scope != tenant
        {
            return Err(invalid("invalid detached receipt proof"));
        }
        root.read_artifact(&proof.key_state.artifact)?;
        root.read_artifact(&proof.signature)?;
    }
    let mut q = c.prepare("SELECT tenant,digest,canonical FROM proof_bundles ORDER BY seq")?;
    let rows = q.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, Vec<u8>>(2)?))
    })?;
    for row in rows {
        let (tenant, digest, bytes) = row?;
        let tenant: OpaqueRef = tenant.try_into().map_err(invalid)?;
        let bundle: ProofBundleV1 = serde_json::from_value(strict_json(&bytes).map_err(invalid)?)?;
        bundle.validate().map_err(invalid)?;
        if ContentHash::of(&bytes).as_str() != digest
            || canonical_data(&bundle).map_err(invalid)? != bytes
        {
            return Err(invalid("invalid proof bundle bytes"));
        }
        for reference in &bundle.records {
            let exists: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM receipt_index WHERE tenant=? AND digest=? UNION ALL SELECT 1 FROM ingress_receipt_index WHERE tenant=? AND digest=?)",
                params![tenant.as_str(), reference.as_str(), tenant.as_str(), reference.as_str()], |r| r.get(0))?;
            if !exists {
                return Err(invalid("proof bundle references foreign receipt"));
            }
        }
        for reference in &bundle.evidence {
            if reference.artifact.scope != tenant {
                return Err(invalid("cross-tenant proof bundle"));
            }
            root.read_artifact(&reference.artifact)?;
        }
    }
    let flow = Connection::open_with_flags(
        root.db_path(StoreKind::Flow),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    for table in ["workflow_trace", "telemetry"] {
        let mut q = flow.prepare(&format!(
            "SELECT tenant,id,receipt,retention,digest FROM {table} ORDER BY seq"
        ))?;
        let rows = q.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        for row in rows {
            let (tenant, id, receipt, retention, digest) = row?;
            let tenant: OpaqueRef = tenant.try_into().map_err(invalid)?;
            let id: OpaqueRef = id.try_into().map_err(invalid)?;
            let receipt: ReceiptId = receipt.try_into().map_err(invalid)?;
            let retention: OpaqueRef = retention.try_into().map_err(invalid)?;
            if !["short", "standard", "long"].contains(&retention.as_str())
                || receipt_digest(&c, &tenant, &receipt)?.is_none()
                || ContentHash::of(
                    &canonical_data(&(&tenant, &id, &receipt, &retention)).map_err(invalid)?,
                )
                .as_str()
                    != digest
            {
                return Err(invalid("invalid diagnostic reference"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proof_and_diagnostic_ids_are_unique_within_each_tenant() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(crate::repository::SCHEMA).unwrap();
        for tenant in ["first", "second"] {
            c.execute(
                "INSERT INTO proof_bundles(tenant,id,digest,canonical) VALUES(?,?,?,?)",
                params![tenant, "same-id", "digest", b"fixture".as_slice()],
            )
            .unwrap();
            for table in ["workflow_trace", "telemetry"] {
                let sql = format!(
                    "INSERT INTO {table}(tenant,id,receipt,retention,digest) VALUES(?,?,?,?,?)"
                );
                c.execute(&sql, params![tenant, "same-id", "receipt", "short", "digest"]).unwrap();
                assert!(c
                    .execute(&sql, params![tenant, "same-id", "receipt", "short", "digest"])
                    .is_err());
            }
            assert!(c
                .execute(
                    "INSERT INTO proof_bundles(tenant,id,digest,canonical) VALUES(?,?,?,?)",
                    params![tenant, "same-id", "digest", b"fixture".as_slice()]
                )
                .is_err());
        }
    }
}
