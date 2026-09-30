//! Bounded descriptive comparison. Campaign labels confer no authority.
use crate::{
    effect::{CommitObservation, EffectAggregate},
    ingress::digest,
};
use serde::{Deserialize, Serialize};
use worldinterface_protocol::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignArmV1 {
    pub version: u32,
    pub campaign: OpaqueRef,
    pub arm: OpaqueRef,
    pub declared_interventions: Vec<OpaqueRef>,
}
impl CampaignArmV1 {
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 || self.declared_interventions.len() > 32 {
            return Err("unbounded campaign attribution".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevelopmentalSnapshotV1 {
    pub version: u32,
    pub intent: EffectIntentId,
    pub campaign: Option<CampaignArmV1>,
    pub agency: ContentHash,
    pub resource: Option<CanonicalResourceRef>,
    pub policy: Option<OpaqueRef>,
    pub grants: Vec<ContentHash>,
    pub approvals: Vec<ContentHash>,
    pub information_use: Option<ContentHash>,
    pub connector: Option<OpaqueRef>,
    pub environment: Option<ContentHash>,
    pub input: ArtifactRef,
    pub outputs: Vec<ArtifactRef>,
    pub attempt: Option<OpaqueRef>,
    pub dispatch_key: Option<OpaqueRef>,
    pub receipt_profile: OpaqueRef,
    pub submitted_at: Timestamp,
    pub outcome: Option<ExecutionOutcome>,
    pub commit: CommitObservation,
    pub reconciliation_history: Vec<ReconciliationResult>,
    pub uncertainty: bool,
    pub gaps: Vec<OpaqueRef>,
}
pub fn inspect(
    source: &EffectAggregate,
    campaign: Option<CampaignArmV1>,
) -> Result<DevelopmentalSnapshotV1> {
    if let Some(c) = &campaign {
        c.validate()?;
    }
    let mut gaps = vec![];
    if source.resolution.is_none() {
        gaps.push("resolution-missing".to_string().try_into()?);
    }
    if source.authorization_inputs.is_none() {
        gaps.push("authorization-snapshot-missing".to_string().try_into()?);
    }
    if source.outcomes.is_empty() {
        gaps.push("outcome-missing".to_string().try_into()?);
    }
    if matches!(
        source.commit,
        crate::effect::CommitObservation::Possible | crate::effect::CommitObservation::Unknown
    ) && source.reconciliations.is_empty()
    {
        gaps.push("reconciliation-missing".to_string().try_into()?);
    }
    if source.intent.submission.causal_refs.len() > 64
        || source.outcomes.len() > 64
        || source.reconciliations.len() > 64
    {
        return Err("causal inspection bound".into());
    }
    let inputs = source.authorization_inputs.as_ref();
    let outcome = source.outcomes.last();
    Ok(DevelopmentalSnapshotV1 {
        version: 1,
        intent: source.intent.id.clone(),
        campaign,
        agency: digest(&source.agency)?,
        resource: source.resolution.as_ref().map(|r| r.resolved.resource.clone()),
        policy: inputs.map(|i| i.policy.revision.clone()),
        grants: inputs
            .map(|i| i.grants.iter().map(digest).collect::<Result<Vec<_>>>())
            .transpose()?
            .unwrap_or_default(),
        approvals: inputs
            .map(|i| i.approvals.iter().map(digest).collect::<Result<Vec<_>>>())
            .transpose()?
            .unwrap_or_default(),
        information_use: source
            .intent
            .submission
            .information_use
            .as_ref()
            .map(digest)
            .transpose()?,
        connector: source.resolution.as_ref().map(|r| r.resolved.connector.clone()),
        environment: inputs.and_then(|i| i.worker.as_ref().map(|w| w.environment.clone())),
        input: source.intent.submission.input.artifact.clone(),
        outputs: outcome.map(|o| o.durable_outputs.clone()).unwrap_or_default(),
        attempt: outcome.map(|o| o.attempt.clone()).or_else(|| source.attempt.clone()),
        dispatch_key: source.dispatches.last().map(|d| d.request.admission_key.clone()),
        receipt_profile: source.intent.submission.requested_receipt_profile.clone(),
        submitted_at: source.intent.submitted_at.clone(),
        outcome: outcome.map(|o| o.outcome.clone()),
        commit: source.commit.clone(),
        reconciliation_history: source.reconciliations.iter().map(|r| r.result.clone()).collect(),
        uncertainty: source.commit == crate::effect::CommitObservation::Possible
            || source.commit == crate::effect::CommitObservation::Unknown
            || outcome.is_some_and(|o| o.outcome == ExecutionOutcome::Uncertain),
        gaps,
    })
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Comparison {
    Match,
    Difference,
    Unknown,
}
fn compare<T: PartialEq>(left: Option<&T>, right: Option<&T>) -> Comparison {
    match (left, right) {
        (Some(a), Some(b)) if a == b => Comparison::Match,
        (Some(_), Some(_)) => Comparison::Difference,
        _ => Comparison::Unknown,
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevelopmentalComparisonV1 {
    pub version: u32,
    pub fields: Vec<(OpaqueRef, Comparison)>,
    pub declared_interventions: Vec<OpaqueRef>,
    pub confounds: Vec<OpaqueRef>,
}
pub fn compare_arms(
    a: &DevelopmentalSnapshotV1,
    b: &DevelopmentalSnapshotV1,
) -> Result<DevelopmentalComparisonV1> {
    if a.version != 1 || b.version != 1 {
        return Err("unsupported comparison version".into());
    }
    let fields: Vec<(OpaqueRef, Comparison)> = vec![
        ("agency", compare(Some(&a.agency), Some(&b.agency))),
        ("resource", compare(a.resource.as_ref(), b.resource.as_ref())),
        ("policy", compare(a.policy.as_ref(), b.policy.as_ref())),
        (
            "grants",
            compare(a.policy.as_ref().map(|_| &a.grants), b.policy.as_ref().map(|_| &b.grants)),
        ),
        (
            "approvals",
            compare(
                a.policy.as_ref().map(|_| &a.approvals),
                b.policy.as_ref().map(|_| &b.approvals),
            ),
        ),
        ("information_use", compare(a.information_use.as_ref(), b.information_use.as_ref())),
        ("connector", compare(a.connector.as_ref(), b.connector.as_ref())),
        ("environment", compare(a.environment.as_ref(), b.environment.as_ref())),
        ("input", compare(Some(&a.input), Some(&b.input))),
        ("attempt", compare(a.attempt.as_ref(), b.attempt.as_ref())),
        ("dispatch_key", compare(a.dispatch_key.as_ref(), b.dispatch_key.as_ref())),
        ("outcome", compare(a.outcome.as_ref(), b.outcome.as_ref())),
        ("commit", compare(Some(&a.commit), Some(&b.commit))),
        (
            "reconciliation_history",
            compare(Some(&a.reconciliation_history), Some(&b.reconciliation_history)),
        ),
        (
            "outputs",
            compare(a.outcome.as_ref().map(|_| &a.outputs), b.outcome.as_ref().map(|_| &b.outputs)),
        ),
        ("receipt_profile", compare(Some(&a.receipt_profile), Some(&b.receipt_profile))),
        ("timing", compare(Some(&a.submitted_at), Some(&b.submitted_at))),
        ("uncertainty", compare(Some(&a.uncertainty), Some(&b.uncertainty))),
    ]
    .into_iter()
    .map(|(name, result)| Ok((name.to_string().try_into()?, result)))
    .collect::<Result<Vec<_>>>()?;
    let declared_interventions = a
        .campaign
        .iter()
        .chain(&b.campaign)
        .flat_map(|c| c.declared_interventions.clone())
        .collect();
    let confounds = fields
        .iter()
        .filter(|(_, state)| *state != Comparison::Match)
        .map(|(name, _)| name.clone())
        .collect();
    Ok(DevelopmentalComparisonV1 { version: 1, fields, declared_interventions, confounds })
}
