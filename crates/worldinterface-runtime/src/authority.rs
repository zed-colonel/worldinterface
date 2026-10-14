//! Trusted host authority boundaries. Public submissions contain no authority snapshots.
use crate::Result;
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::{AuthorizationSnapshot, VerifiedSnapshot},
};
use worldinterface_protocol::{
    ContentHash, EffectSubmission, EvidenceRef, OpaqueRef, PrincipalId, Timestamp,
};
use worldinterface_storage::StoreRoot;

/// Implemented by authenticated host composition, never by request deserialization.
pub trait Authentication {
    type Session;
    fn authenticate(&self, session: &Self::Session) -> Result<(OpaqueRef, PrincipalId)>;
}
/// Host-owned time source; timestamps are never accepted from an effect request.
/// Completion samples it while holding the Fabric writer lock. Implementations
/// must return promptly and must not re-enter storage or contact a provider.
pub trait TrustedClock {
    fn now(&self) -> Result<Timestamp>;
}
/// Verifies current signed/attested records and revocation against trusted authority stores.
pub trait EvidenceAuthority {
    fn current_snapshot(
        &self,
        tenant: &OpaqueRef,
        submission: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot>;
    fn verify(
        &self,
        tenant: &OpaqueRef,
        evidence: &EvidenceRef,
        at: &Timestamp,
    ) -> Result<ContentHash>;
}
pub fn caller<A: Authentication>(auth: &A, session: &A::Session) -> Result<AuthenticatedCaller> {
    let (tenant, principal) = auth.authenticate(session)?;
    Ok(AuthenticatedCaller::from_trusted_authentication(tenant, principal))
}
/// Closed artifact and provenance check before the pure domain reducer sees facts.
pub fn verify_snapshot(
    root: &StoreRoot,
    authority: &impl EvidenceAuthority,
    tenant: &OpaqueRef,
    submission: &EffectSubmission,
    at: &Timestamp,
) -> Result<VerifiedSnapshot> {
    let snapshot = authority.current_snapshot(tenant, submission)?;
    for reference in snapshot
        .bindings
        .iter()
        .flat_map(|b| b.delegations.iter().map(|d| &d.evidence))
        .chain(snapshot.grants.iter().map(|g| &g.evidence))
        .chain(snapshot.approvals.iter().map(|a| &a.evidence))
        .chain(snapshot.facts.iter().map(|f| &f.evidence))
        .chain(snapshot.worker.iter().map(|w| &w.evidence))
        .chain(snapshot.fixture.iter().map(|q| &q.evidence))
        .chain(snapshot.qualification.iter().map(|q| &q.evidence))
    {
        if &reference.artifact.scope != tenant {
            return Err("cross-tenant authority evidence".into());
        }
        root.read_artifact(&reference.artifact)?;
        authority.verify(tenant, reference, at)?;
    }
    for binding in &snapshot.bindings {
        if let Some(attestation) = &binding.capacity.attestation_ref {
            if &attestation.artifact.scope != tenant {
                return Err("cross-tenant capacity evidence".into());
            }
            root.read_artifact(&attestation.artifact)?;
            authority.verify(tenant, attestation, at)?;
        }
    }
    for fact in &snapshot.facts {
        if authority.verify(tenant, &fact.evidence, at)? != fact.attested_document {
            return Err("resolution evidence does not attest exact document".into());
        }
    }
    Ok(VerifiedSnapshot::from_trusted_verifier(snapshot)?)
}
