//! Evidence-bound effect journal. Coarse phases are derived; they confer no authority.
use crate::agency::Result;
use crate::{agency::*, authorization::*, ingress::digest, ports::*, resource::*, Phase};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use worldinterface_protocol::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitObservation {
    NotAttempted,
    ProvenImpossible,
    Possible,
    Unknown,
    ConfirmedCommitted,
    ConfirmedNotCommitted,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeEvidence {
    pub intent: EffectIntentId,
    pub attempt: OpaqueRef,
    pub connector_semantics: OpaqueRef,
    pub commit: CommitObservation,
    pub outcome: ExecutionOutcome,
    pub evidence: Vec<EvidenceRef>,
    pub required_outputs: Vec<ArtifactRef>,
    pub durable_outputs: Vec<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary_record: Option<EvidenceRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationEvidence {
    pub intent: EffectIntentId,
    pub attempt: OpaqueRef,
    pub connector_semantics: OpaqueRef,
    pub result: ReconciliationResult,
    pub evidence: Vec<EvidenceRef>,
}
#[derive(Clone, Debug)]
pub struct VerifiedOutcome(OutcomeEvidence);
impl VerifiedOutcome {
    pub fn from_trusted_verifier(e: OutcomeEvidence) -> Self {
        Self(e)
    }
}
#[derive(Clone, Debug)]
pub struct VerifiedReconciliation(ReconciliationEvidence);
impl VerifiedReconciliation {
    pub fn from_trusted_verifier(e: ReconciliationEvidence) -> Self {
        Self(e)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkKind {
    Replacement,
    Compensation,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentLink {
    pub original: EffectIntentId,
    pub kind: LinkKind,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dispatch {
    pub request: EffectAdmissionRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<ProductionAdmission>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledgment: Option<EffectAcknowledgment>,
}
/// AQ-neutral, immutable record of the complete request published as a scoped artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductionAdmission {
    pub version: u32,
    pub queue_store: OpaqueRef,
    pub task: OpaqueRef,
    pub admission_key: OpaqueRef,
    pub execution: ContentHash,
    pub request_artifact: ArtifactRef,
    pub request_hash: ContentHash,
    pub canonical_version: u32,
    pub canonical_digest: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_sequence: Option<u64>,
    #[serde(default)]
    pub observed_runs: Vec<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_cursor: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectAggregate {
    pub intent: EffectIntent,
    pub agency: AgencyRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<IntentLink>,
    pub phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<ResolutionRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_failure: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<AuthorizationEvaluation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization_inputs: Option<AuthorizationSnapshot>,
    pub dispatches: Vec<Dispatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<OpaqueRef>,
    pub outcomes: Vec<OutcomeEvidence>,
    pub reconciliations: Vec<ReconciliationEvidence>,
    pub commit: CommitObservation,
    pub canceled_before_dispatch: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectState {
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_at: Option<Timestamp>,
    pub effects: BTreeMap<EffectIntentId, EffectAggregate>,
    pub requests: BTreeMap<ContentHash, EffectIntentId>,
    pub uses: BTreeMap<ContentHash, u32>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectView {
    pub intent: EffectIntent,
    pub agency: AgencyRecord,
    pub resolution: Option<ResolutionRecord>,
    pub authorizer: Option<PrincipalId>,
    pub approvers: Vec<PrincipalId>,
    pub phase: Phase,
    pub commit: CommitObservation,
    pub resolution_failure: Option<String>,
    pub authorization: Option<AuthorizationEvaluation>,
    pub outcome: Option<OutcomeEvidence>,
    pub reconciliation: Option<ReconciliationEvidence>,
    pub dispatch: Option<Dispatch>,
    pub link: Option<IntentLink>,
}
pub fn inspect_effect(e: &EffectAggregate) -> Result<EffectView> {
    if matches!(
        e.phase,
        Phase::Authorized | Phase::DispatchPending | Phase::Running | Phase::Succeeded
    ) && (e.resolution.is_none()
        || e.authorization.as_ref().is_none_or(|a| a.decision != Decision::Allow))
        || e.phase == Phase::Succeeded
            && e.outcomes.last().is_none_or(|o| o.outcome != ExecutionOutcome::Succeeded)
        || e.phase == Phase::Reconciled && e.reconciliations.is_empty()
        || e.phase == Phase::Running && e.attempt.is_none()
    {
        return Err("inconsistent mandatory effect evidence".into());
    }
    if e.intent.agency != e.agency.context
        || e.resolution.as_ref().is_some_and(|r| {
            r.resolved.intent_id != e.intent.id
                || r.resolved.normalized_parameters != e.intent.submission.input
                || r.fact.document.reference(r.fact.observed_version.clone()).as_ref()
                    != Ok(&r.resolved.resource)
        })
        || e.phase == Phase::Succeeded
            && (e.commit != CommitObservation::ConfirmedCommitted
                || e.outcomes.last().is_none_or(|o| {
                    o.boundary_record.is_none()
                        || o.required_outputs != o.durable_outputs
                        || o.evidence.is_empty()
                }))
        || e.phase == Phase::Failed
            && e.resolution_failure.is_none()
            && e.outcomes.last().is_none_or(|o| o.outcome != ExecutionOutcome::FailedBeforeCommit)
        || e.phase == Phase::Canceled
            && !e.canceled_before_dispatch
            && e.outcomes.last().is_none_or(|o| o.outcome != ExecutionOutcome::CanceledBeforeCommit)
        || e.phase == Phase::Uncertain
            && (e.outcomes.is_empty()
                || !matches!(
                    e.commit,
                    CommitObservation::Possible
                        | CommitObservation::Unknown
                        | CommitObservation::ConfirmedCommitted
                ))
        || e.phase == Phase::Reconciled
            && e.reconciliations.last().is_none_or(|r| {
                !matches!(
                    (&r.result, &e.commit),
                    (
                        ReconciliationResult::ConfirmedCommitted,
                        CommitObservation::ConfirmedCommitted
                    ) | (
                        ReconciliationResult::ConfirmedNotCommitted,
                        CommitObservation::ConfirmedNotCommitted
                    )
                )
            })
    {
        return Err("contradictory effect evidence".into());
    }
    for d in &e.dispatches {
        if d.request.intent != e.intent.id
            || d.admission.as_ref().is_some_and(|a| {
                a.version != 1
                    || a.canonical_version != 2
                    || a.execution != d.request.execution
                    || a.admission_key != d.request.admission_key
                    || a.request_artifact.scope != e.agency.binding.tenant
                    || a.request_artifact.content != a.request_hash
                    || a.acknowledged_sequence == Some(0)
                    || a.observed_runs.len() > 10000
                    || a.observed_runs.windows(2).any(|w| w[0] >= w[1])
                    || !a.observed_runs.is_empty() && a.acknowledged_sequence.is_none()
            })
            || d.acknowledgment.as_ref().is_some_and(|a| {
                a.task != d.request.task
                    || a.admission_key != d.request.admission_key
                    || digest(&d.request).as_ref() != Ok(&a.request_digest)
                    || a.durable_sequence == 0
            })
        {
            return Err("inconsistent admission evidence".into());
        }
    }
    Ok(EffectView {
        intent: e.intent.clone(),
        agency: e.agency.clone(),
        resolution: e.resolution.clone(),
        authorizer: e.authorization_inputs.as_ref().map(|s| s.policy.authorizer.clone()),
        approvers: e
            .authorization_inputs
            .as_ref()
            .map_or_else(Vec::new, |s| s.approvals.iter().map(|a| a.approver.clone()).collect()),
        phase: e.phase,
        commit: e.commit.clone(),
        resolution_failure: e.resolution_failure.clone(),
        authorization: e.authorization.clone(),
        outcome: e.outcomes.last().cloned(),
        reconciliation: e.reconciliations.last().cloned(),
        dispatch: e.dispatches.last().cloned(),
        link: e.link.clone(),
    })
}
/// Trusted actions intentionally do not implement Deserialize.
#[derive(Clone, Debug)]
pub enum EffectAction {
    Submit {
        id: EffectIntentId,
        submission: Box<EffectSubmission>,
        snapshot: VerifiedSnapshot,
        link: Option<IntentLink>,
    },
    Resolve {
        id: EffectIntentId,
        snapshot: VerifiedSnapshot,
    },
    Authorize {
        id: EffectIntentId,
        snapshot: VerifiedSnapshot,
    },
    Prepare {
        id: EffectIntentId,
        snapshot: VerifiedSnapshot,
    },
    PrepareProduction {
        id: EffectIntentId,
        snapshot: VerifiedSnapshot,
        admission: Box<ProductionAdmission>,
    },
    AcknowledgeProduction {
        id: EffectIntentId,
        sequence: u64,
    },
    ObserveRun {
        id: EffectIntentId,
        run: OpaqueRef,
    },
    AdvanceDiscovery {
        id: EffectIntentId,
        previous: Option<String>,
        next: Option<String>,
    },
    Acknowledge {
        id: EffectIntentId,
        acknowledgment: EffectAcknowledgment,
    },
    Start {
        id: EffectIntentId,
        snapshot: VerifiedSnapshot,
        attempt: OpaqueRef,
    },
    StartProduction {
        id: EffectIntentId,
        snapshot: VerifiedSnapshot,
        run: OpaqueRef,
        attempt: OpaqueRef,
    },
    Cancel {
        id: EffectIntentId,
    },
    Outcome {
        id: EffectIntentId,
        evidence: VerifiedOutcome,
    },
    Reconcile {
        id: EffectIntentId,
        evidence: VerifiedReconciliation,
    },
    PermitRetry {
        id: EffectIntentId,
        snapshot: VerifiedSnapshot,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum JournalAction {
    Submit {
        id: EffectIntentId,
        submission: Box<EffectSubmission>,
        snapshot: AuthorizationSnapshot,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        link: Option<IntentLink>,
    },
    Resolve {
        id: EffectIntentId,
        snapshot: AuthorizationSnapshot,
    },
    Authorize {
        id: EffectIntentId,
        snapshot: AuthorizationSnapshot,
    },
    Prepare {
        id: EffectIntentId,
        snapshot: AuthorizationSnapshot,
    },
    PrepareProduction {
        id: EffectIntentId,
        snapshot: AuthorizationSnapshot,
        admission: Box<ProductionAdmission>,
    },
    AcknowledgeProduction {
        id: EffectIntentId,
        sequence: u64,
    },
    ObserveRun {
        id: EffectIntentId,
        run: OpaqueRef,
    },
    AdvanceDiscovery {
        id: EffectIntentId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        previous: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next: Option<String>,
    },
    Acknowledge {
        id: EffectIntentId,
        acknowledgment: EffectAcknowledgment,
    },
    Start {
        id: EffectIntentId,
        snapshot: AuthorizationSnapshot,
        attempt: OpaqueRef,
    },
    StartProduction {
        id: EffectIntentId,
        snapshot: AuthorizationSnapshot,
        run: OpaqueRef,
        attempt: OpaqueRef,
    },
    Cancel {
        id: EffectIntentId,
    },
    Outcome {
        id: EffectIntentId,
        evidence: OutcomeEvidence,
    },
    Reconcile {
        id: EffectIntentId,
        evidence: ReconciliationEvidence,
    },
    PermitRetry {
        id: EffectIntentId,
        snapshot: AuthorizationSnapshot,
    },
}
impl From<EffectAction> for JournalAction {
    fn from(a: EffectAction) -> Self {
        match a {
            EffectAction::Submit { id, submission, snapshot, link } => {
                Self::Submit { id, submission, snapshot: snapshot.0, link }
            }
            EffectAction::Resolve { id, snapshot } => Self::Resolve { id, snapshot: snapshot.0 },
            EffectAction::Authorize { id, snapshot } => {
                Self::Authorize { id, snapshot: snapshot.0 }
            }
            EffectAction::Prepare { id, snapshot } => Self::Prepare { id, snapshot: snapshot.0 },
            EffectAction::PrepareProduction { id, snapshot, admission } => {
                Self::PrepareProduction { id, snapshot: snapshot.0, admission }
            }
            EffectAction::AcknowledgeProduction { id, sequence } => {
                Self::AcknowledgeProduction { id, sequence }
            }
            EffectAction::ObserveRun { id, run } => Self::ObserveRun { id, run },
            EffectAction::AdvanceDiscovery { id, previous, next } => {
                Self::AdvanceDiscovery { id, previous, next }
            }
            EffectAction::Start { id, snapshot, attempt } => {
                Self::Start { id, snapshot: snapshot.0, attempt }
            }
            EffectAction::StartProduction { id, snapshot, run, attempt } => {
                Self::StartProduction { id, snapshot: snapshot.0, run, attempt }
            }
            EffectAction::Acknowledge { id, acknowledgment } => {
                Self::Acknowledge { id, acknowledgment }
            }
            EffectAction::Cancel { id } => Self::Cancel { id },
            EffectAction::Outcome { id, evidence } => Self::Outcome { id, evidence: evidence.0 },
            EffectAction::Reconcile { id, evidence } => {
                Self::Reconcile { id, evidence: evidence.0 }
            }
            EffectAction::PermitRetry { id, snapshot } => {
                Self::PermitRetry { id, snapshot: snapshot.0 }
            }
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    version: u32,
    before: ContentHash,
    tenant: OpaqueRef,
    principal: PrincipalId,
    at: Timestamp,
    action: JournalAction,
}
#[derive(Clone, Debug)]
pub struct ValidatedEffectCommand {
    bytes: Vec<u8>,
    tenant: OpaqueRef,
    before: ContentHash,
}
impl ValidatedEffectCommand {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Digest of the exact journal state against which this command was validated.
    pub fn base_digest(&self) -> &ContentHash {
        &self.before
    }
    pub fn tenant(&self) -> &OpaqueRef {
        &self.tenant
    }
}
pub fn validate_command(
    state: &EffectState,
    caller: &AuthenticatedCaller,
    at: Timestamp,
    action: EffectAction,
) -> Result<ValidatedEffectCommand> {
    let entry = Entry {
        version: 1,
        before: state_digest(state)?,
        tenant: caller.tenant.clone(),
        principal: caller.principal.clone(),
        at,
        action: action.into(),
    };
    apply(state, &entry)?;
    Ok(ValidatedEffectCommand {
        bytes: canonical_data(&entry)?,
        tenant: caller.tenant.clone(),
        before: entry.before,
    })
}
pub fn state_digest(s: &EffectState) -> Result<ContentHash> {
    Ok(ContentHash::of(&canonical_collection(s)?))
}
/// Persistence replay only. Never reconstructs an executable command or recontacts a provider.
pub fn replay_command(
    state: &EffectState,
    tenant: &OpaqueRef,
    bytes: &[u8],
) -> Result<(EffectState, EffectIntentId)> {
    let e: Entry = serde_json::from_value(strict_json(bytes)?).map_err(|e| e.to_string())?;
    if &e.tenant != tenant || canonical_data(&e)? != bytes {
        return Err("noncanonical or cross-tenant journal".into());
    }
    apply(state, &e)
}
fn current(
    e: &EffectAggregate,
    snapshot: &AuthorizationSnapshot,
    now: &Timestamp,
) -> Result<AuthorizationEvaluation> {
    let r = e.resolution.as_ref().ok_or("unresolved effect")?;
    let a = authorize(&e.intent, &e.agency, r, &VerifiedSnapshot(snapshot.clone()), now)?;
    if a.decision != Decision::Allow || e.authorization.as_ref() != Some(&a) {
        return Err("authorization drift: explicit reevaluation required".into());
    }
    Ok(a)
}
fn reserve(uses: &mut BTreeMap<ContentHash, u32>, a: &AuthorizationEvaluation) -> Result<()> {
    for limit in &a.reservations {
        let n = uses.entry(limit.key.clone()).or_default();
        if *n >= limit.max_uses {
            return Err("authorization use exhausted".into());
        }
        *n += 1;
    }
    Ok(())
}
fn check_evidence(refs: &[EvidenceRef], tenant: &OpaqueRef) -> Result<()> {
    if refs.is_empty()
        || refs.len() > 64
        || refs.iter().any(|r| &r.artifact.scope != tenant || r.artifact.size == 0)
    {
        return Err("missing or wrong-scope evidence".into());
    }
    Ok(())
}
fn apply(state: &EffectState, entry: &Entry) -> Result<(EffectState, EffectIntentId)> {
    if entry.version != 1
        || entry.before != state_digest(state)?
        || state.last_at.as_ref().is_some_and(|t| t > &entry.at)
    {
        return Err("stale or unsupported effect journal".into());
    }
    let caller = AuthenticatedCaller::from_trusted_authentication(
        entry.tenant.clone(),
        entry.principal.clone(),
    );
    let now = &entry.at;
    let mut s = state.clone();
    if let JournalAction::Submit { id, submission, snapshot, link } = &entry.action {
        let key =
            digest(&(&entry.tenant, &entry.principal, "effect", &submission.client_request_id))?;
        if let Some(old) = s.requests.get(&key) {
            let old_effect = &s.effects[old];
            if old_effect.intent.submission != **submission || old_effect.link != *link {
                return Err("conflicting client request".into());
            }
            return Ok((state.clone(), old.clone()));
        }
        if s.effects.contains_key(id)
            || s.effects.len() >= 10000
            || submission.input.artifact.scope != entry.tenant
        {
            return Err("intent identity, capacity or input scope".into());
        }
        let agency = resolve_agency(&caller, submission, &snapshot.bindings, now)?;
        if let Some(link) = link {
            let original = s.effects.get(&link.original).ok_or("unknown original intent")?;
            if original.agency.binding.principal != entry.principal || link.original == *id {
                return Err("invalid linked effect".into());
            }
            if link.kind == LinkKind::Compensation
                && !matches!(
                    original.commit,
                    CommitObservation::Possible
                        | CommitObservation::Unknown
                        | CommitObservation::ConfirmedCommitted
                )
            {
                return Err("compensation without possible commit".into());
            }
        }
        let intent = EffectIntent {
            id: id.clone(),
            submission: *submission.clone(),
            agency: agency.context.clone(),
            stable_idempotency_identity: format!("effect:{}", key.as_str()).try_into()?,
            submitted_at: now.clone(),
        };
        s.effects.insert(
            id.clone(),
            EffectAggregate {
                intent,
                agency,
                link: link.clone(),
                phase: Phase::Submitted,
                resolution: None,
                resolution_failure: None,
                authorization: None,
                authorization_inputs: None,
                dispatches: vec![],
                attempt: None,
                outcomes: vec![],
                reconciliations: vec![],
                commit: CommitObservation::NotAttempted,
                canceled_before_dispatch: false,
            },
        );
        s.requests.insert(key, id.clone());
        s.revision = s.revision.checked_add(1).ok_or("revision overflow")?;
        s.last_at = Some(now.clone());
        return Ok((s, id.clone()));
    }
    let id = match &entry.action {
        JournalAction::Resolve { id, .. }
        | JournalAction::Authorize { id, .. }
        | JournalAction::Prepare { id, .. }
        | JournalAction::PrepareProduction { id, .. }
        | JournalAction::AcknowledgeProduction { id, .. }
        | JournalAction::ObserveRun { id, .. }
        | JournalAction::AdvanceDiscovery { id, .. }
        | JournalAction::Acknowledge { id, .. }
        | JournalAction::Start { id, .. }
        | JournalAction::StartProduction { id, .. }
        | JournalAction::Cancel { id }
        | JournalAction::Outcome { id, .. }
        | JournalAction::Reconcile { id, .. }
        | JournalAction::PermitRetry { id, .. } => id,
        _ => unreachable!(),
    };
    let e = s.effects.get_mut(id).ok_or("effect unavailable")?;
    if e.agency.binding.tenant != entry.tenant || e.agency.binding.principal != entry.principal {
        return Err("effect unavailable".into());
    }
    match &entry.action {
        JournalAction::Resolve { snapshot, .. } => {
            if e.phase != Phase::Submitted {
                return Err("resolution phase".into());
            }
            match resolve_resource(
                &e.intent,
                &entry.tenant,
                &snapshot.registry,
                &snapshot.facts,
                now,
            ) {
                Ok(r) => {
                    e.resolution = Some(r);
                    e.phase = Phase::Resolving;
                }
                Err(reason) => {
                    e.resolution_failure = Some(reason);
                    e.phase = Phase::Failed;
                    e.commit = CommitObservation::ProvenImpossible;
                }
            }
        }
        JournalAction::Authorize { snapshot, .. } => {
            if !matches!(
                e.phase,
                Phase::Resolving
                    | Phase::AwaitingApproval
                    | Phase::Authorized
                    | Phase::DispatchPending
            ) {
                return Err("authorization phase".into());
            }
            // The immutable intent's agency may not silently rebind. Changed bindings need replacement.
            let a = authorize(
                &e.intent,
                &e.agency,
                e.resolution.as_ref().ok_or("unresolved")?,
                &VerifiedSnapshot(snapshot.clone()),
                now,
            )?;
            if e.phase == Phase::DispatchPending {
                if a.decision != Decision::Allow || e.authorization.as_ref() != Some(&a) {
                    return Err("pending admission authorization drift".into());
                }
                return Ok((state.clone(), id.clone()));
            }
            e.phase = match a.decision {
                Decision::Allow => Phase::Authorized,
                Decision::Deny => Phase::Denied,
                Decision::NeedsApproval => Phase::AwaitingApproval,
            };
            e.authorization = Some(a);
            e.authorization_inputs = Some(snapshot.clone());
        }
        JournalAction::Prepare { snapshot, .. }
        | JournalAction::PrepareProduction { snapshot, .. } => {
            current(e, snapshot, now)?;
            if e.phase == Phase::DispatchPending {
                if let JournalAction::PrepareProduction { admission, .. } = &entry.action {
                    if e.dispatches.last().and_then(|d| d.admission.as_ref())
                        != Some(admission.as_ref())
                    {
                        return Err("admission binding conflict".into());
                    }
                }
                return Ok((state.clone(), id.clone()));
            }
            if e.phase != Phase::Authorized {
                return Err("dispatch preparation phase".into());
            }
            let a = e.authorization.as_ref().ok_or("no authorization")?;
            reserve(&mut s.uses, a)?;
            let index = u32::try_from(e.dispatches.len()).map_err(|_| "operation overflow")?;
            if index >= e.intent.submission.budget.max_attempts {
                return Err("attempt budget".into());
            }
            let identity = digest(&(&entry.tenant, id, index))?;
            let request = EffectAdmissionRequest {
                version: 1,
                task: format!("wi-task:{}", identity.as_str()).try_into()?,
                admission_key: format!("wi-effect:{}", identity.as_str()).try_into()?,
                intent: id.clone(),
                operation_index: index,
                submission: digest(&e.intent.submission)?,
                agency: digest(&e.agency)?,
                resolution: digest(&e.resolution)?,
                authorization: digest(a)?,
                execution: digest(&(&e.intent, &e.resolution, a, snapshot, index))?,
            };
            let admission =
                if let JournalAction::PrepareProduction { admission, .. } = &entry.action {
                    if admission.version != 1
                        || admission.canonical_version != 2
                        || admission.request_artifact.scope != entry.tenant
                        || admission.request_artifact.size == 0
                        || admission.request_artifact.content != admission.request_hash
                        || admission.execution != request.execution
                        || admission.admission_key != request.admission_key
                        || admission.acknowledged_sequence.is_some()
                        || !admission.observed_runs.is_empty()
                        || admission.discovery_cursor.is_some()
                    {
                        return Err("invalid production admission binding".into());
                    }
                    Some(*admission.clone())
                } else {
                    None
                };
            e.dispatches.push(Dispatch { request, admission, acknowledgment: None });
            e.phase = Phase::DispatchPending;
        }
        JournalAction::AcknowledgeProduction { sequence, .. } => {
            let d = e.dispatches.last_mut().ok_or("missing production admission")?;
            let a = d.admission.as_mut().ok_or("missing production binding")?;
            if e.phase != Phase::DispatchPending || *sequence == 0 {
                return Err("invalid production acknowledgment".into());
            }
            if let Some(old) = a.acknowledged_sequence {
                if old != *sequence {
                    return Err("production acknowledgment conflict".into());
                }
                return Ok((state.clone(), id.clone()));
            }
            a.acknowledged_sequence = Some(*sequence);
        }
        JournalAction::ObserveRun { run, .. } => {
            let d = e.dispatches.last_mut().ok_or("missing production admission")?;
            let a = d.admission.as_mut().ok_or("missing production binding")?;
            if a.acknowledged_sequence.is_none() {
                return Err("unacknowledged admission".into());
            }
            if a.observed_runs.contains(run) {
                return Ok((state.clone(), id.clone()));
            }
            if a.observed_runs.len() >= 10000 {
                return Err("run mapping limit".into());
            }
            a.observed_runs.push(run.clone());
            a.observed_runs.sort();
        }
        JournalAction::AdvanceDiscovery { previous, next, .. } => {
            let a = e
                .dispatches
                .last_mut()
                .and_then(|d| d.admission.as_mut())
                .ok_or("missing production admission")?;
            if a.acknowledged_sequence.is_none()
                || a.discovery_cursor != *previous
                || next.as_ref().is_some_and(|v| v.len() > 4096)
            {
                return Err("discovery cursor conflict".into());
            }
            a.discovery_cursor = next.clone();
        }
        JournalAction::Acknowledge { acknowledgment, .. } => {
            if e.phase != Phase::DispatchPending {
                return Err("acknowledgment phase".into());
            }
            let d = e.dispatches.last_mut().ok_or("missing prepared request")?;
            if acknowledgment.task != d.request.task
                || acknowledgment.admission_key != d.request.admission_key
                || acknowledgment.request_digest != digest(&d.request)?
                || acknowledgment.durable_sequence == 0
                || acknowledgment.durable_sequence > 9_007_199_254_740_991
            {
                return Err("acknowledgment binding".into());
            }
            if let Some(old) = &d.acknowledgment {
                if old != acknowledgment {
                    return Err("acknowledgment conflict".into());
                }
                return Ok((state.clone(), id.clone()));
            }
            d.acknowledgment = Some(acknowledgment.clone());
        }
        JournalAction::Start { snapshot, attempt, .. }
        | JournalAction::StartProduction { snapshot, attempt, .. } => {
            current(e, snapshot, now)?;
            let acknowledged = match &entry.action {
                JournalAction::StartProduction { run, .. } => {
                    e.dispatches.last().is_some_and(|d| {
                        d.admission.as_ref().is_some_and(|a| {
                            a.acknowledged_sequence.is_some() && a.observed_runs.contains(run)
                        })
                    })
                }
                _ => e
                    .dispatches
                    .last()
                    .is_some_and(|d| d.admission.is_none() && d.acknowledgment.is_some()),
            };
            if e.phase != Phase::DispatchPending
                || !acknowledged
                || e.outcomes.iter().any(|o| &o.attempt == attempt)
            {
                return Err("execution start evidence".into());
            }
            e.attempt = Some(attempt.clone());
            e.phase = Phase::Running;
            // Once handed to a connector, failure without provider evidence is uncertainty.
            e.commit = CommitObservation::Possible;
        }
        JournalAction::Cancel { .. } => {
            if !matches!(
                e.phase,
                Phase::Submitted | Phase::Resolving | Phase::AwaitingApproval | Phase::Authorized
            ) {
                return Err("cannot infer noncommit from queue cancellation".into());
            }
            e.phase = Phase::Canceled;
            e.canceled_before_dispatch = true;
            e.commit = CommitObservation::NotAttempted;
        }
        JournalAction::Outcome { evidence: o, .. } => {
            if e.phase != Phase::Running
                || o.intent != *id
                || Some(&o.attempt) != e.attempt.as_ref()
                || e.resolution
                    .as_ref()
                    .is_none_or(|r| r.resolved.idempotency_semantics != o.connector_semantics)
            {
                return Err("outcome binding or phase".into());
            }
            check_evidence(&o.evidence, &entry.tenant)?;
            if e.authorization_inputs
                .as_ref()
                .is_none_or(|s| s.required_outputs != o.required_outputs)
            {
                return Err("required output manifest mismatch".into());
            }
            if o.required_outputs.len() > 64
                || o.durable_outputs.len() > 64
                || o.required_outputs
                    .iter()
                    .chain(&o.durable_outputs)
                    .any(|r| r.scope != entry.tenant)
            {
                return Err("output scope or bound".into());
            }
            e.phase = match (&o.commit, &o.outcome) {
                (CommitObservation::ConfirmedCommitted, ExecutionOutcome::Succeeded)
                    if o.boundary_record.as_ref().is_some_and(|r| {
                        r.artifact.scope == entry.tenant && r.artifact.size > 0
                    }) && o.required_outputs == o.durable_outputs =>
                {
                    Phase::Succeeded
                }
                (
                    CommitObservation::ProvenImpossible | CommitObservation::ConfirmedNotCommitted,
                    ExecutionOutcome::FailedBeforeCommit,
                ) => Phase::Failed,
                (
                    CommitObservation::ProvenImpossible | CommitObservation::ConfirmedNotCommitted,
                    ExecutionOutcome::CanceledBeforeCommit,
                ) => Phase::Canceled,
                (
                    CommitObservation::Possible
                    | CommitObservation::Unknown
                    | CommitObservation::ConfirmedCommitted,
                    ExecutionOutcome::Uncertain,
                ) => Phase::Uncertain,
                _ => return Err("contradictory commit/outcome or incomplete publication".into()),
            };
            e.commit = o.commit.clone();
            e.outcomes.push(o.clone());
        }
        JournalAction::Reconcile { evidence: r, .. } => {
            if e.phase != Phase::Uncertain
                || r.intent != *id
                || Some(&r.attempt) != e.attempt.as_ref()
                || e.resolution
                    .as_ref()
                    .is_none_or(|e| e.resolved.idempotency_semantics != r.connector_semantics)
            {
                return Err("reconciliation binding or phase".into());
            }
            check_evidence(&r.evidence, &entry.tenant)?;
            if e.commit == CommitObservation::ConfirmedCommitted
                && r.result != ReconciliationResult::ConfirmedCommitted
            {
                return Err("contradicts confirmed commit".into());
            }
            e.phase = match r.result {
                ReconciliationResult::ConfirmedCommitted
                | ReconciliationResult::ConfirmedNotCommitted => Phase::Reconciled,
                _ => Phase::Uncertain,
            };
            e.commit = match r.result {
                ReconciliationResult::ConfirmedCommitted => CommitObservation::ConfirmedCommitted,
                ReconciliationResult::ConfirmedNotCommitted => {
                    CommitObservation::ConfirmedNotCommitted
                }
                _ => CommitObservation::Unknown,
            };
            e.reconciliations.push(r.clone());
        }
        JournalAction::PermitRetry { snapshot, .. } => {
            if e.phase != Phase::Reconciled
                || e.reconciliations
                    .last()
                    .is_none_or(|r| r.result != ReconciliationResult::ConfirmedNotCommitted)
                || !snapshot.policy.retry_after_noncommit
            {
                return Err("retry requires provider noncommit and explicit policy".into());
            }
            let a = authorize(
                &e.intent,
                &e.agency,
                e.resolution.as_ref().ok_or("missing resolution")?,
                &VerifiedSnapshot(snapshot.clone()),
                now,
            )?;
            if a.decision != Decision::Allow {
                return Err("retry lacks current exact authorization".into());
            }
            e.authorization = Some(a);
            e.authorization_inputs = Some(snapshot.clone());
            e.phase = Phase::Authorized;
            e.attempt = None;
            e.commit = CommitObservation::NotAttempted;
        }
        _ => unreachable!(),
    }
    inspect_effect(e)?;
    s.revision = s.revision.checked_add(1).ok_or("revision overflow")?;
    s.last_at = Some(now.clone());
    Ok((s, id.clone()))
}

/// Artifacts whose durable availability is required by the recorded facts. Requested
/// output manifests are not outputs; only publication evidence makes them required here.
pub fn required_artifacts(e: &EffectAggregate) -> Vec<&ArtifactRef> {
    let mut refs = vec![&e.intent.submission.input.artifact];
    refs.extend(e.agency.binding.capacity.attestation_ref.iter().map(|a| &a.artifact));
    refs.extend(e.agency.binding.delegations.iter().map(|d| &d.evidence.artifact));
    if let Some(r) = &e.resolution {
        refs.push(&r.fact.evidence.artifact);
    }
    if let Some(s) = &e.authorization_inputs {
        refs.extend(s.grants.iter().map(|g| &g.evidence.artifact));
        refs.extend(s.approvals.iter().map(|a| &a.evidence.artifact));
        if let Some(w) = &s.worker {
            refs.extend([
                &w.input,
                &w.evidence.artifact,
                &w.manifest.executable,
                &w.manifest.adapter,
                &w.manifest.configuration,
                &w.manifest.profile_definition,
            ]);
            refs.extend(&w.manifest.dependencies);
        }
        if let Some(q) = &s.fixture {
            refs.push(&q.evidence.artifact);
        }
        if let Some(q) = &s.qualification {
            refs.push(&q.evidence.artifact);
        }
    }
    for o in &e.outcomes {
        refs.extend(&o.durable_outputs);
        refs.extend(o.evidence.iter().map(|e| &e.artifact));
        refs.extend(o.boundary_record.iter().map(|e| &e.artifact));
    }
    for r in &e.reconciliations {
        refs.extend(r.evidence.iter().map(|e| &e.artifact));
    }
    for d in &e.dispatches {
        refs.extend(d.admission.as_ref().map(|a| &a.request_artifact));
    }
    refs
}
