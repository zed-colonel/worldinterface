//! A receipt is prepared only from a coherent authoritative effect aggregate.
use crate::{
    effect::{inspect_effect, EffectAggregate},
    ingress::digest,
};
use serde::{Deserialize, Serialize};
use worldinterface_protocol::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DisclosureProfile {
    Summary,
    Full,
}

/// The caller's access decision is supplied by the trusted composition layer.
/// This type is not constructible from receipt claims or serialized input.
#[derive(Clone, Debug)]
pub struct ReceiptAccess {
    tenant: OpaqueRef,
    principal: PrincipalId,
    disclosure: DisclosureProfile,
}
impl ReceiptAccess {
    pub fn from_trusted_authorizer(
        tenant: OpaqueRef,
        principal: PrincipalId,
        disclosure: DisclosureProfile,
    ) -> Self {
        Self { tenant, principal, disclosure }
    }
    pub fn tenant(&self) -> &OpaqueRef {
        &self.tenant
    }
    pub fn principal(&self) -> &PrincipalId {
        &self.principal
    }
    pub fn disclosure(&self) -> DisclosureProfile {
        self.disclosure
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptView {
    pub id: ReceiptId,
    pub direction: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<ContentHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical: Option<Envelope>,
}

#[derive(Clone, Debug)]
pub struct ValidatedReceipt {
    envelope: Envelope,
    source_revision: u64,
    source_intent: EffectIntentId,
}
impl ValidatedReceipt {
    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }
    pub fn source_revision(&self) -> u64 {
        self.source_revision
    }
    pub fn source_intent(&self) -> &EffectIntentId {
        &self.source_intent
    }
}

pub fn prepare_egress_receipt(
    source: &EffectAggregate,
    source_revision: u64,
    id: ReceiptId,
    execution: &ExecutionRecord,
    temporal: TemporalProvenance,
) -> Result<ValidatedReceipt> {
    inspect_effect(source)?;
    let resolution = source.resolution.as_ref().ok_or("missing resolution")?;
    let authorization = source.authorization.as_ref().ok_or("missing authorization")?;
    let inputs = source.authorization_inputs.as_ref().ok_or("missing authorization inputs")?;
    let outcome = source.outcomes.last().ok_or("missing outcome")?;
    if authorization.decision != Decision::Allow
        || resolution.resolved.intent_id != source.intent.id
        || execution.intent_id != source.intent.id
        || execution.attempt != outcome.attempt
        || execution.outcome != outcome.outcome
        || source.attempt.as_ref() != Some(&execution.attempt)
        || source.intent.agency != source.agency.context
        || inputs.policy.revision.as_str().is_empty()
    {
        return Err("receipt source references disagree".into());
    }
    let profile = source.intent.submission.requested_receipt_profile.clone();
    if profile.as_str() != receipt::BOUNDARY_PROFILE
        && profile.as_str() != receipt::DEVELOPMENTAL_PROFILE
    {
        return Err("unsupported receipt profile".into());
    }
    if matches!(outcome.outcome, ExecutionOutcome::Succeeded)
        && (outcome.required_outputs != outcome.durable_outputs
            || outcome.boundary_record.is_none())
    {
        return Err("successful receipt lacks published output evidence".into());
    }
    let provider_profile = inputs.worker.as_ref().ok_or("missing exact provider profile")?;
    let versions = BoundaryVersions {
        resolver: resolution.resolved.resolver_version.clone(),
        capability_descriptor: resolution.descriptor.clone(),
        policy: inputs.policy.revision.clone(),
        grant_versions: inputs.grants.iter().map(digest).collect::<Result<Vec<_>>>()?,
        connector_profile: resolution.resolved.registry_snapshot.clone(),
        provider_profile: digest(provider_profile)?,
    };
    let body = EgressReceipt {
        intent_id: source.intent.id.clone(),
        agency_digest: digest(&source.agency)?,
        authorization_digest: digest(authorization)?,
        resolved_effect_digest: digest(&resolution.resolved)?,
        attempt: execution.attempt.clone(),
        executor: provider_profile.provider.clone(),
        outcome: outcome.outcome.clone(),
        output_refs: outcome.durable_outputs.clone(),
    };
    // The execution report is an assertion, not an authority for new references.
    // A report may omit evidence; the durable outcome remains the source of truth.
    if execution.evidence.iter().any(|reference| !outcome.evidence.contains(reference)) {
        return Err("execution evidence is not bound to the authoritative outcome".into());
    }
    let mut evidence = outcome.evidence.clone();
    if let Some(reference) = &outcome.boundary_record {
        evidence.push(reference.clone());
    }
    let receipt = BoundaryReceipt {
        id,
        profile,
        versions,
        body: ReceiptBody::Egress(body),
        temporal,
        evidence,
        causal_refs: source.intent.submission.causal_refs.clone(),
    };
    receipt::validate_receipt(&receipt)?;
    let envelope = Envelope::new(Record::BoundaryReceipt(receipt));
    envelope.canonical_bytes()?;
    Ok(ValidatedReceipt { envelope, source_revision, source_intent: source.intent.id.clone() })
}
