//! Server-owned agency. These records describe verified server facts, never caller grants.
use crate::ingress::digest;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use worldinterface_protocol::*;
pub type Result<T> = std::result::Result<T, String>;

/// Closed exact-demand grammar. Each hash identifies a complete canonical submission.
/// An empty set grants nothing; there are no wildcard or executable predicates.
pub type Authority = BTreeSet<ContentHash>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub id: OpaqueRef,
    pub version: u64,
    pub tenant: OpaqueRef,
    pub principal: PrincipalId,
    pub actor: ActorId,
    pub accountable: PrincipalId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sponsor: Option<PrincipalId>,
    pub capacity: ActingCapacityRef,
    pub principal_authority: Authority,
    pub capacity_authority: Authority,
    pub expires_at: Timestamp,
    pub revoked: bool,
    pub delegations: Vec<Delegation>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delegation {
    pub id: OpaqueRef,
    pub version: u64,
    pub issuer: PrincipalId,
    pub subject: PrincipalId,
    pub tenant: OpaqueRef,
    pub authority: Authority,
    pub expires_at: Timestamp,
    pub revoked: bool,
    pub evidence: EvidenceRef,
}
/// No Deserialize implementation: only trusted host composition can authenticate.
#[derive(Clone, Debug)]
pub struct AuthenticatedCaller {
    pub(crate) tenant: OpaqueRef,
    pub(crate) principal: PrincipalId,
}
impl AuthenticatedCaller {
    /// Authentication adapter boundary. Runtime host composition authenticates sessions.
    pub fn from_trusted_authentication(tenant: OpaqueRef, principal: PrincipalId) -> Self {
        Self { tenant, principal }
    }
    pub fn tenant(&self) -> &OpaqueRef {
        &self.tenant
    }
    pub fn principal(&self) -> &PrincipalId {
        &self.principal
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgencyRecord {
    pub binding: Binding,
    pub context: AgencyContext,
    pub effective: Authority,
}
/// Optional assertions only constrain already resolved facts.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityClaims {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<ActorId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accountable: Option<PrincipalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<ActingCapacityRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation: Option<EvidenceRef>,
}

pub fn resolve_binding(
    caller: &AuthenticatedCaller,
    claims: &IdentityClaims,
    bindings: &[Binding],
    now: &Timestamp,
) -> Result<(Binding, Authority)> {
    if bindings.len() > 256 {
        return Err("binding snapshot bound".into());
    }
    let matches: Vec<_> = bindings
        .iter()
        .filter(|b| {
            b.tenant == caller.tenant
                && b.principal == caller.principal
                && !b.revoked
                && b.expires_at > *now
        })
        .collect();
    if matches.len() != 1 {
        return Err("missing or ambiguous current binding".into());
    }
    let b = matches[0];
    if b.version == 0
        || b.delegations.len() > 16
        || b.principal_authority.len() > 256
        || !b.capacity_authority.is_subset(&b.principal_authority)
        || b.capacity.principal_ref != b.accountable
        || claims.actor.as_ref().is_some_and(|v| v != &b.actor)
        || claims.accountable.as_ref().is_some_and(|v| v != &b.accountable)
        || claims.capacity.as_ref().is_some_and(|v| v != &b.capacity)
        || b.capacity.attestation_ref.as_ref().is_some_and(|v| v.artifact.scope != caller.tenant)
    {
        return Err("agency assertion or capacity mismatch".into());
    }
    let mut effective = b.capacity_authority.clone();
    let mut current = b.accountable.clone();
    let mut visited = BTreeSet::from([current.clone()]);
    let mut expires = b.expires_at.clone();
    let mut ids = BTreeSet::new();
    for d in &b.delegations {
        if d.issuer != current
            || !visited.insert(d.subject.clone())
            || !ids.insert(d.id.clone())
            || d.tenant != caller.tenant
            || d.evidence.artifact.scope != caller.tenant
            || d.revoked
            || d.version == 0
            || d.expires_at <= *now
            || d.expires_at > expires
            || !d.authority.is_subset(&effective)
        {
            return Err("invalid or widened delegation".into());
        }
        current = d.subject.clone();
        expires = d.expires_at.clone();
        effective = d.authority.clone();
    }
    if current != caller.principal
        || claims
            .delegation
            .as_ref()
            .is_some_and(|e| b.delegations.last().is_none_or(|d| &d.evidence != e))
    {
        return Err("delegation endpoint or credential mismatch".into());
    }
    digest(b)?;
    Ok((b.clone(), effective))
}

pub fn resolve_agency(
    caller: &AuthenticatedCaller,
    s: &EffectSubmission,
    bindings: &[Binding],
    now: &Timestamp,
) -> Result<AgencyRecord> {
    let (b, effective) = resolve_binding(
        caller,
        &IdentityClaims {
            actor: s.submitted_by.clone(),
            accountable: s.requested_accountable_principal.clone(),
            capacity: s.acting_capacity.clone(),
            delegation: s.delegation_credential.clone(),
        },
        bindings,
        now,
    )?;
    // Size/canonical validation applies even to trusted in-memory facts.
    digest(&b)?;
    Ok(AgencyRecord {
        binding: b.clone(),
        effective,
        context: AgencyContext {
            accountable_principal: b.accountable.clone(),
            requester: b.actor.clone(),
            delegation_chain: b.delegations.iter().map(|d| d.id.clone()).collect(),
            submitted_by: Some(b.actor.clone()),
            purpose: s.purpose.clone(),
            causal_refs: s.causal_refs.clone(),
            acting_capacity: Some(b.capacity.clone()),
        },
    })
}
