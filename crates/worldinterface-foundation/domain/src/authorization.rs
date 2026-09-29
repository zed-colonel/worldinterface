//! Closed exact-demand authorization over verified, immutable server snapshots.
use crate::agency::Result;
use crate::{agency::*, ingress::digest, resource::*};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use worldinterface_protocol::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub id: OpaqueRef,
    pub version: u64,
    pub tenant: OpaqueRef,
    pub issuer: PrincipalId,
    pub subject: PrincipalId,
    pub authority: Authority,
    pub expires_at: Timestamp,
    pub revoked: bool,
    pub max_uses: u32,
    pub evidence: EvidenceRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Approval {
    pub id: OpaqueRef,
    pub version: u64,
    pub approver: PrincipalId,
    pub exact_binding: ContentHash,
    pub expires_at: Timestamp,
    pub revoked: bool,
    pub max_uses: u32,
    pub evidence: EvidenceRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub version: u32,
    pub revision: OpaqueRef,
    pub authorizer: PrincipalId,
    pub grant_issuers: BTreeSet<PrincipalId>,
    pub approvers: BTreeSet<PrincipalId>,
    pub platform: Authority,
    pub resource: Authority,
    pub targets: BTreeSet<ContentHash>,
    pub information_use: Authority,
    pub approval_required: bool,
    pub separation_of_duties: bool,
    pub retry_after_noncommit: bool,
    pub expires_at: Timestamp,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Enforcement {
    Enforced,
    Unsupported,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequiredControl {
    pub requested: OpaqueRef,
    pub advertised: OpaqueRef,
    pub applied: OpaqueRef,
    pub observed: OpaqueRef,
    pub enforcement: Enforcement,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputClass {
    NonSensitive,
    Protected,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Credential {
    None,
    Test { run: OpaqueRef, subject: PrincipalId, expires_at: Timestamp, authority: Authority },
    Production,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRealization {
    pub profile: OpaqueRef,
    pub provider: OpaqueRef,
    pub run: OpaqueRef,
    pub resource: OpaqueRef,
    pub operation: OpaqueRef,
    pub manifest: ImmutableArtifactManifest,
    pub image: ImageApplicability,
    pub environment: ContentHash,
    pub evidence: EvidenceRef,
    pub aq_version: ContentHash,
    pub wi_version: ContentHash,
    pub input: ArtifactRef,
    pub exports: ContentHash,
    pub controls: BTreeMap<OpaqueRef, RequiredControl>,
    pub mandatory: BTreeSet<OpaqueRef>,
    pub reviewed_executable: bool,
    pub input_class: InputClass,
    pub credential: Credential,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureQualification {
    pub realization: ContentHash,
    pub tenant: OpaqueRef,
    pub subject: PrincipalId,
    pub run: OpaqueRef,
    pub operations: BTreeSet<OpaqueRef>,
    pub expires_at: Timestamp,
    pub revoked: bool,
    pub evidence: EvidenceRef,
}
/// Independent ordinary-use qualification. Its evidence is verified by runtime authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrdinaryQualification {
    pub realization: ContentHash,
    pub tenant: OpaqueRef,
    pub subject: PrincipalId,
    pub provider: OpaqueRef,
    pub profile: OpaqueRef,
    pub operations: BTreeSet<OpaqueRef>,
    pub aq_version: ContentHash,
    pub wi_version: ContentHash,
    pub expires_at: Timestamp,
    pub revoked: bool,
    pub quarantined: bool,
    pub evidence: EvidenceRef,
}
/// Serializable journal facts, NOT a wire authentication/verification context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationSnapshot {
    pub bindings: Vec<Binding>,
    pub registry: ResolverSnapshot,
    pub facts: Vec<ResolutionFact>,
    pub policy: Policy,
    pub grants: Vec<Grant>,
    pub approvals: Vec<Approval>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<WorkerRealization>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixture: Option<FixtureQualification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualification: Option<OrdinaryQualification>,
    pub required_outputs: Vec<ArtifactRef>,
}
/// Not caller-deserializable. A trusted composition verifies provenance before construction.
#[derive(Clone, Debug)]
pub struct VerifiedSnapshot(pub(crate) AuthorizationSnapshot);
impl VerifiedSnapshot {
    /// Trusted evidence boundary; runtime verifies provenance before construction.
    pub fn from_trusted_verifier(snapshot: AuthorizationSnapshot) -> Result<Self> {
        digest(&snapshot)?;
        if snapshot.bindings.len() > 256
            || snapshot.grants.len() > 64
            || snapshot.approvals.len() > 64
        {
            return Err("snapshot bound".into());
        }
        Ok(Self(snapshot))
    }
    pub fn facts(&self) -> &AuthorizationSnapshot {
        &self.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    ExactAuthority,
    Agency,
    Resource,
    Policy,
    Grant,
    Approval,
    Expired,
    Unavailable,
    FixtureOnly,
    Executable,
    ProtectedInput,
    Credential,
    ProfileDrift,
    MandatoryControl(OpaqueRef),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attenuation {
    pub stage: OpaqueRef,
    pub remaining: Authority,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationEvaluation {
    pub decision: Decision,
    pub reasons: Vec<Reason>,
    pub trace: Vec<Attenuation>,
    pub exact_binding: ContentHash,
    pub snapshot_digest: ContentHash,
    pub expires_at: Timestamp,
    pub reservations: Vec<UseLimit>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UseLimit {
    pub key: ContentHash,
    pub max_uses: u32,
}
/// Approval binds everything except approvals themselves, avoiding a recursive signature.
pub fn approval_binding(
    intent: &EffectIntent,
    agency: &AgencyRecord,
    resolution: &ResolutionRecord,
    snapshot: &AuthorizationSnapshot,
) -> Result<ContentHash> {
    let mut base = snapshot.clone();
    base.approvals.clear();
    digest(&(intent, agency, resolution, base))
}
fn worker_reason(
    w: &WorkerRealization,
    q: Option<&FixtureQualification>,
    ordinary: Option<&OrdinaryQualification>,
    intent: &EffectIntent,
    agency: &AgencyRecord,
    resolution: &ResolutionRecord,
    now: &Timestamp,
) -> Result<Option<Reason>> {
    if !w.reviewed_executable {
        return Ok(Some(Reason::Executable));
    }
    if ordinary.is_none() && w.input_class != InputClass::NonSensitive {
        return Ok(Some(Reason::ProtectedInput));
    }
    let demand = digest(&intent.submission)?;
    match &w.credential {
        Credential::None => {}
        Credential::Test { run, subject, expires_at, authority }
            if run == &w.run
                && subject == &agency.binding.principal
                && expires_at > now
                && authority == &BTreeSet::from([demand]) => {}
        Credential::Production if ordinary.is_some() => {}
        _ => return Ok(Some(Reason::Credential)),
    }
    if w.mandatory.is_empty() || w.mandatory.len() > 32 || w.controls.len() > 32 {
        return Ok(Some(Reason::Policy));
    }
    for required in &w.mandatory {
        if !["cpu", "memory", "mount", "network"].contains(&required.as_str())
            || w.controls.get(required).is_none_or(|c| {
                c.enforcement != Enforcement::Enforced
                    || c.requested != c.advertised
                    || c.requested != c.applied
                    || c.requested != c.observed
            })
        {
            return Ok(Some(Reason::MandatoryControl(required.clone())));
        }
    }
    if let Some(qualified) = ordinary {
        if q.is_some()
            || qualified.realization != digest(w)?
            || qualified.tenant != agency.binding.tenant
            || qualified.subject != agency.binding.principal
            || qualified.provider != w.provider
            || qualified.profile != w.profile
            || qualified.aq_version != w.aq_version
            || qualified.wi_version != w.wi_version
            || qualified.evidence.artifact.scope != qualified.tenant
            || qualified.revoked
            || qualified.quarantined
            || qualified.expires_at <= *now
            || !qualified.operations.contains(&w.operation)
            || w.input != intent.submission.input.artifact
            || matches!(w.image, ImageApplicability::Unknown)
            || !matches!(&resolution.fact.document.target, Target::Worker { provider, run, resource, operation }
                if provider == &w.provider && run == &w.run && resource == &w.resource && operation == &w.operation)
        {
            return Ok(Some(Reason::ProfileDrift));
        }
        return Ok(None);
    }
    let Some(q) = q else {
        return Ok(Some(Reason::FixtureOnly));
    };
    if !q.operations.contains(&w.operation) {
        return Ok(Some(Reason::Unavailable));
    }
    if q.realization != digest(w)?
        || q.tenant != agency.binding.tenant
        || q.subject != agency.binding.principal
        || q.run != w.run
        || q.revoked
        || q.expires_at <= *now
        || q.evidence.artifact.scope != q.tenant
        || w.input != intent.submission.input.artifact
        || w.input.scope != q.tenant
        || w.evidence.artifact.scope != q.tenant
        || matches!(w.image, ImageApplicability::Unknown)
    {
        return Ok(Some(Reason::ProfileDrift));
    }
    if !matches!(&resolution.fact.document.target, Target::Worker { provider, run, resource, operation } if provider == &w.provider && run == &w.run && resource == &w.resource && operation == &w.operation)
    {
        return Ok(Some(Reason::ProfileDrift));
    }
    Ok(None)
}
pub fn authorize(
    intent: &EffectIntent,
    agency: &AgencyRecord,
    resolution: &ResolutionRecord,
    verified: &VerifiedSnapshot,
    now: &Timestamp,
) -> Result<AuthorizationEvaluation> {
    let s = &verified.0;
    let base_binding = approval_binding(intent, agency, resolution, s)?;
    let mut a = AuthorizationEvaluation {
        decision: Decision::Deny,
        reasons: vec![],
        trace: vec![],
        exact_binding: digest(&(intent, agency, resolution, s))?,
        snapshot_digest: digest(s)?,
        expires_at: s.policy.expires_at.clone(),
        reservations: vec![],
    };
    let mut reasons = Vec::new();
    let caller = AuthenticatedCaller::from_trusted_authentication(
        agency.binding.tenant.clone(),
        agency.binding.principal.clone(),
    );
    if resolve_agency(&caller, &intent.submission, &s.bindings, now).as_ref() != Ok(agency) {
        reasons.push(Reason::Agency);
    }
    if resolve_resource(intent, &agency.binding.tenant, &s.registry, &s.facts, now).as_ref()
        != Ok(resolution)
    {
        reasons.push(Reason::Resource);
    }
    let p = &s.policy;
    if !p.targets.contains(&digest(&resolution.fact.document)?) {
        reasons.push(Reason::Resource);
    }
    if p.version != 1
        || p.expires_at <= *now
        || intent.submission.budget.max_attempts == 0
        || intent.submission.budget.max_attempts > 100
        || intent.submission.budget.timeout_millis == 0
    {
        reasons.push(Reason::Policy);
    }
    let mut effective = agency.effective.clone();
    for (stage, allowed) in [
        ("agency", &agency.effective),
        ("platform", &p.platform),
        ("resource", &p.resource),
        ("information_use", &p.information_use),
    ] {
        effective = effective.intersection(allowed).cloned().collect();
        a.trace.push(Attenuation {
            stage: stage.to_owned().try_into()?,
            remaining: effective.clone(),
        });
    }
    let demand = digest(&intent.submission)?;
    let mut ids = BTreeSet::new();
    if s.grants.is_empty() {
        reasons.push(Reason::Grant);
    }
    for g in &s.grants {
        if !ids.insert(g.id.clone())
            || g.version == 0
            || g.revoked
            || g.expires_at <= *now
            || g.tenant != agency.binding.tenant
            || g.evidence.artifact.scope != g.tenant
            || g.subject != agency.binding.principal
            || !p.grant_issuers.contains(&g.issuer)
            || g.max_uses == 0
            || g.max_uses > 10000
        {
            reasons.push(Reason::Grant);
        }
        effective = effective.intersection(&g.authority).cloned().collect();
        a.trace.push(Attenuation { stage: g.id.clone(), remaining: effective.clone() });
        a.expires_at = a.expires_at.min(g.expires_at.clone());
        // Stable grant identity across revisions prevents renewed documents resetting use counts.
        a.reservations.push(UseLimit {
            key: digest(&("grant", &g.tenant, &g.issuer, &g.id))?,
            max_uses: g.max_uses,
        });
    }
    if !effective.contains(&demand) {
        reasons.push(Reason::ExactAuthority);
    }
    for requested in &intent.submission.requested_authority_refs {
        if !ids.contains(requested) {
            reasons.push(Reason::Grant);
        }
    }
    a.expires_at = a
        .expires_at
        .min(agency.binding.expires_at.clone())
        .min(resolution.fact.valid_until.clone());
    for d in &agency.binding.delegations {
        a.expires_at = a.expires_at.min(d.expires_at.clone());
    }
    if let Some(deadline) = &intent.submission.deadline {
        a.expires_at = a.expires_at.min(deadline.clone());
    }
    if let Some(u) = &intent.submission.information_use {
        if let Some(expiry) = &u.expiration {
            a.expires_at = a.expires_at.min(expiry.clone());
        }
        if intent
            .submission
            .purpose
            .as_ref()
            .is_none_or(|purpose| !u.purpose_refs.contains(purpose))
        {
            reasons.push(Reason::Policy);
        }
    }
    if matches!(resolution.fact.document.target, Target::Worker { .. }) {
        match &s.worker {
            Some(w) => {
                if let Some(r) = worker_reason(
                    w,
                    s.fixture.as_ref(),
                    s.qualification.as_ref(),
                    intent,
                    agency,
                    resolution,
                    now,
                )? {
                    reasons.push(r);
                }
                if let Credential::Test { expires_at, .. } = &w.credential {
                    a.expires_at = a.expires_at.min(expires_at.clone());
                }
                if let Some(q) = &s.fixture {
                    a.expires_at = a.expires_at.min(q.expires_at.clone());
                }
                if let Some(q) = &s.qualification {
                    a.expires_at = a.expires_at.min(q.expires_at.clone());
                }
            }
            None => reasons.push(Reason::FixtureOnly),
        }
    } else if s.worker.is_some() || s.fixture.is_some() || s.qualification.is_some() {
        reasons.push(Reason::ProfileDrift);
    }
    let mut approval_ids = BTreeSet::new();
    for approval in &s.approvals {
        if !approval_ids.insert(approval.id.clone())
            || approval.version == 0
            || approval.revoked
            || approval.expires_at <= *now
            || approval.exact_binding != base_binding
            || !p.approvers.contains(&approval.approver)
            || approval.max_uses == 0
            || approval.max_uses > 10000
            || approval.evidence.artifact.scope != agency.binding.tenant
            || p.separation_of_duties
                && (approval.approver == agency.binding.principal
                    || approval.approver == agency.binding.accountable
                    || Some(&approval.approver) == agency.binding.sponsor.as_ref()
                    || approval.approver == p.authorizer)
        {
            reasons.push(Reason::Approval);
        }
        a.expires_at = a.expires_at.min(approval.expires_at.clone());
        a.reservations.push(UseLimit {
            key: digest(&("approval", &agency.binding.tenant, &approval.approver, &approval.id))?,
            max_uses: approval.max_uses,
        });
    }
    if intent.submission.approval_attestations.iter().any(|id| !approval_ids.contains(id)) {
        reasons.push(Reason::Approval);
    }
    if a.expires_at <= *now {
        reasons.push(Reason::Expired);
    }
    if reasons.is_empty() {
        a.decision = if p.approval_required && s.approvals.is_empty() {
            reasons.push(Reason::Approval);
            Decision::NeedsApproval
        } else {
            Decision::Allow
        };
    }
    if reasons.is_empty() {
        reasons.push(Reason::ExactAuthority);
    }
    reasons.truncate(64);
    a.reasons = reasons;
    Ok(a)
}
