//! Evidence-bound effect journal. Coarse phases are derived; they confer no authority.
use crate::agency::Result;
use crate::{agency::*, authorization::*, ingress::digest, ports::*, resource::*, Phase};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use worldinterface_protocol::*;

pub const MAX_OUTCOME_OUTPUTS: usize = 64;
pub const MAX_OUTCOME_EVIDENCE: usize = 64;
/// Local completion profile: outputs, evidence and boundary bytes together.
pub const MAX_OUTCOME_BYTES: u64 = 16 * 1024 * 1024;

/// Shared by domain reference validation and the runtime's pre-publication check.
pub fn validate_outcome_bounds(
    output_count: usize,
    evidence_count: usize,
    sizes: impl IntoIterator<Item = u64>,
) -> Result<()> {
    if output_count > MAX_OUTCOME_OUTPUTS || evidence_count > MAX_OUTCOME_EVIDENCE {
        return Err("outcome artifact count bound".into());
    }
    let mut total = 0u64;
    for size in sizes {
        total = total.checked_add(size).ok_or("outcome artifact byte bound")?;
        if total > MAX_OUTCOME_BYTES {
            return Err("outcome artifact byte bound".into());
        }
    }
    Ok(())
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<ExecutorClassification>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciliation_admission: Option<Vec<u8>>,
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
/// Verified transactional-child origin; never a top-level acknowledgment substitute.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildAdmissionProvenance {
    pub admission_key: String,
    pub version: u32,
    pub flow: OpaqueRef,
    pub node: OpaqueRef,
    pub parent_task: OpaqueRef,
    pub parent_run: OpaqueRef,
    pub producing_attempt: OpaqueRef,
    pub disposition_hash: ContentHash,
}
/// AQ-neutral, immutable record of the complete request published as a scoped artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductionAdmission {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child: Option<ChildAdmissionProvenance>,
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
pub struct EffectOwner {
    pub flow: OpaqueRef,
    pub node: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectAggregate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<EffectOwner>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reconciliation_cases: Vec<crate::reconciliation::ReconciliationCase>,
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

/// Deterministic request identity for one dispatch. This computes bindings only;
/// authorization, reservation and phase validation remain in command validation.
pub fn admission_request(
    tenant: &OpaqueRef,
    effect: &EffectAggregate,
    snapshot: &AuthorizationSnapshot,
    index: u32,
) -> Result<EffectAdmissionRequest> {
    let id = &effect.intent.id;
    let authorization = effect.authorization.as_ref().ok_or("no authorization")?;
    let identity = digest(&(tenant, id, index))?;
    Ok(EffectAdmissionRequest {
        version: 1,
        task: format!("wi-task:{}", identity.as_str()).try_into()?,
        admission_key: format!("wi-effect:{}", identity.as_str()).try_into()?,
        intent: id.clone(),
        operation_index: index,
        submission: digest(&effect.intent.submission)?,
        agency: digest(&effect.agency)?,
        resolution: digest(&effect.resolution)?,
        authorization: digest(authorization)?,
        execution: digest(&(&effect.intent, &effect.resolution, authorization, snapshot, index))?,
    })
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
            && e.outcomes.last().is_none_or(|o| {
                !matches!(
                    o.outcome,
                    ExecutionOutcome::FailedBeforeCommit | ExecutionOutcome::TerminalFailure
                )
            })
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
                    || a.child.as_ref().is_some_and(|c| c.version != 1)
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
    SubmitFlow {
        id: EffectIntentId,
        submission: Box<EffectSubmission>,
        snapshot: VerifiedSnapshot,
        owner: EffectOwner,
    },
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
    ReconciliationCase {
        id: EffectIntentId,
        case: OpaqueRef,
        expected: u32,
        action: crate::reconciliation::VerifiedCaseAction,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        owner: Option<EffectOwner>,
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
    ReconciliationCase {
        id: EffectIntentId,
        case: OpaqueRef,
        expected: u32,
        action: crate::reconciliation::CaseAction,
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
                Self::Submit { id, submission, snapshot: snapshot.0, link, owner: None }
            }
            EffectAction::SubmitFlow { id, submission, snapshot, owner } => Self::Submit {
                id,
                submission,
                snapshot: snapshot.0,
                link: None,
                owner: Some(owner),
            },
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
            EffectAction::ReconciliationCase { id, case, expected, action } => {
                Self::ReconciliationCase { id, case, expected, action: action.into_action() }
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
    let action: JournalAction = action.into();
    let version = match &action {
        JournalAction::ReconciliationCase { .. } => 2,
        JournalAction::Outcome { evidence, .. }
            if evidence.classification.is_some() || evidence.reconciliation_admission.is_some() =>
        {
            2
        }
        _ => 1,
    };
    let entry = Entry {
        version,
        before: state_digest(state)?,
        tenant: caller.tenant.clone(),
        principal: caller.principal.clone(),
        at,
        action,
    };
    apply(state, &entry)?;
    Ok(ValidatedEffectCommand {
        bytes: canonical_data(&entry)?,
        tenant: caller.tenant.clone(),
        before: entry.before,
    })
}
pub fn state_digest(s: &EffectState) -> Result<ContentHash> {
    worldinterface_protocol::collection_digest(s)
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
        || refs.len() > MAX_OUTCOME_EVIDENCE
        || refs.iter().any(|r| &r.artifact.scope != tenant || r.artifact.size == 0)
    {
        return Err("missing or wrong-scope evidence".into());
    }
    Ok(())
}
/// Validates an outcome's reference structure, scope, size and commit claims.
/// This does not authorize execution or assert that any artifact is published.
/// Runtimes can check content-derived references before publishing bytes; the
/// journal repeats these rules after checking the current attempt and manifest.
pub fn validate_outcome_publication(tenant: &OpaqueRef, o: &OutcomeEvidence) -> Result<Phase> {
    if o.reconciliation_admission.as_ref().is_some_and(|b| {
        o.outcome != ExecutionOutcome::Uncertain || b.is_empty() || b.len() > 16384
    }) {
        return Err("reconciliation admission bytes require uncertainty".into());
    }
    check_evidence(&o.evidence, tenant)?;
    validate_outcome_bounds(
        o.durable_outputs.len(),
        o.evidence.len(),
        o.durable_outputs
            .iter()
            .map(|r| r.size)
            .chain(o.evidence.iter().chain(&o.boundary_record).map(|r| r.artifact.size)),
    )?;
    if o.required_outputs.len() > MAX_OUTCOME_OUTPUTS
        || o.required_outputs.iter().chain(&o.durable_outputs).any(|r| r.scope != *tenant)
    {
        return Err("output scope or bound".into());
    }
    let phase = match (&o.commit, &o.outcome) {
        (
            CommitObservation::ConfirmedNotCommitted | CommitObservation::ConfirmedCommitted,
            ExecutionOutcome::TerminalFailure,
        ) => Phase::Failed,
        (CommitObservation::ConfirmedCommitted, ExecutionOutcome::Succeeded)
            if o.boundary_record
                .as_ref()
                .is_some_and(|r| r.artifact.scope == *tenant && r.artifact.size > 0)
                && o.required_outputs == o.durable_outputs =>
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
    Ok(phase)
}

fn apply(state: &EffectState, entry: &Entry) -> Result<(EffectState, EffectIntentId)> {
    if ![1, 2].contains(&entry.version)
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
    if let JournalAction::Submit { id, submission, snapshot, link, owner } = &entry.action {
        let key = if let Some(owner) = owner {
            digest(&(&entry.tenant, &entry.principal, "flow-effect", owner))?
        } else {
            digest(&(&entry.tenant, &entry.principal, "effect", &submission.client_request_id))?
        };
        if let Some(old) = s.requests.get(&key) {
            let old_effect = &s.effects[old];
            if old_effect.intent.submission != **submission
                || old_effect.link != *link
                || old_effect.owner != *owner
            {
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
                owner: owner.clone(),
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
                reconciliation_cases: vec![],
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
        | JournalAction::ReconciliationCase { id, .. }
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
            if matches!(entry.action, JournalAction::Prepare { .. })
                && e.resolution.as_ref().is_some_and(|r| r.resolved.connector_execution.is_some())
            {
                return Err("connector-bound effects require production admission".into());
            }
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
            let request = admission_request(&entry.tenant, e, snapshot, index)?;
            if let JournalAction::PrepareProduction { admission, .. } = &entry.action {
                if e.owner.as_ref().map(|o| (&o.flow, &o.node))
                    != admission.child.as_ref().map(|c| (&c.flow, &c.node))
                {
                    return Err("effect ownership/admission mismatch".into());
                }
                if let Some(child) = &admission.child {
                    if child.version != 1 {
                        return Err("unsupported child admission provenance".into());
                    }
                    if child.admission_key.len() != 73
                        || !child.admission_key.starts_with("child/v1/")
                        || !child.admission_key[9..].bytes().all(|b| b.is_ascii_hexdigit())
                    {
                        return Err("invalid scoped child admission key".into());
                    }
                }
            }
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
            if e.phase != Phase::DispatchPending {
                return Err("discovery phase".into());
            }
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
            if previous == next {
                return Ok((state.clone(), id.clone()));
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
            if e.authorization_inputs
                .as_ref()
                .is_none_or(|s| s.required_outputs != o.required_outputs)
            {
                return Err("required output manifest mismatch".into());
            }
            e.phase = validate_outcome_publication(&entry.tenant, o)?;
            e.commit = o.commit.clone();
            e.outcomes.push(o.clone());
            if e.phase == Phase::Uncertain {
                if let Some(case) = crate::reconciliation::ReconciliationCase::from_uncertain(
                    e,
                    s.revision + 1,
                    now,
                )? {
                    e.reconciliation_cases.push(case);
                }
            }
        }
        JournalAction::Reconcile { evidence: r, .. } => {
            if !e.reconciliation_cases.is_empty()
                || e.phase != Phase::Uncertain
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
        JournalAction::ReconciliationCase { case, expected, action, .. } => {
            use crate::reconciliation::{CaseAction, CaseStatus};
            let case = e
                .reconciliation_cases
                .iter_mut()
                .find(|c| &c.id == case)
                .ok_or("missing reconciliation case")?;
            if case.revision != *expected {
                return Err("stale reconciliation case".into());
            }
            match action {
                CaseAction::PrepareAdmission { request } => {
                    if request.is_empty()
                        || request.len() > 16384
                        || case.admission_request.as_ref().is_some_and(|old| old != request)
                    {
                        return Err("reconciliation admission conflict".into());
                    }
                    if case.admission_request.as_ref() == Some(request) {
                        return Ok((state.clone(), id.clone()));
                    }
                    case.admission_request = Some(request.clone());
                    case.revision += 1;
                }
                CaseAction::Acknowledge { sequence, run } => {
                    if *sequence == 0
                        || case.admission_request.is_none()
                        || case.admission_sequence.is_some_and(|old| old != *sequence)
                        || case
                            .aq_run
                            .as_ref()
                            .zip(run.as_ref())
                            .is_some_and(|(old, new)| old != new)
                    {
                        return Err("reconciliation acknowledgment conflict".into());
                    }
                    if case.admission_sequence == Some(*sequence)
                        && run.as_ref().is_none_or(|r| case.aq_run.as_ref() == Some(r))
                    {
                        return Ok((state.clone(), id.clone()));
                    }
                    case.admission_sequence = Some(*sequence);
                    if run.is_some() {
                        case.aq_run = run.clone();
                    }
                    case.revision += 1;
                }
                CaseAction::Start => case.start(*expected, now.epoch_millis()?)?,
                CaseAction::Observe { result, evidence } => {
                    case.observe(*expected, *result, evidence.clone(), now, s.revision + 1)?;
                    let result = match case.status {
                        CaseStatus::ConfirmedCommitted => {
                            Some(ReconciliationResult::ConfirmedCommitted)
                        }
                        CaseStatus::ConfirmedNotCommitted => {
                            Some(ReconciliationResult::ConfirmedNotCommitted)
                        }
                        CaseStatus::Escalated => Some(ReconciliationResult::Escalated),
                        _ => None,
                    };
                    if let Some(result) = result {
                        let reconciled = matches!(
                            result,
                            ReconciliationResult::ConfirmedCommitted
                                | ReconciliationResult::ConfirmedNotCommitted
                        );
                        if e.attempt.as_ref() != Some(&case.attempt) || e.phase != Phase::Uncertain
                        {
                            return Err("reconciliation effect fence".into());
                        }
                        e.reconciliations.push(ReconciliationEvidence {
                            intent: id.clone(),
                            attempt: case.attempt.clone(),
                            connector_semantics: e
                                .resolution
                                .as_ref()
                                .ok_or("no resolution")?
                                .resolved
                                .idempotency_semantics
                                .clone(),
                            result,
                            evidence: evidence.clone(),
                        });
                        e.commit = case.commit.clone();
                        if reconciled {
                            e.phase = Phase::Reconciled;
                        }
                    }
                }
            }
        }
        JournalAction::PermitRetry { snapshot, .. } => {
            let proven_noncommit = (e.phase == Phase::Reconciled
                && e.reconciliations
                    .last()
                    .is_some_and(|r| r.result == ReconciliationResult::ConfirmedNotCommitted))
                || (e.phase == Phase::Failed
                    && e.commit == CommitObservation::ConfirmedNotCommitted);
            if !proven_noncommit
                || e.dispatches.len() >= e.intent.submission.budget.max_attempts as usize
                || e.resolution
                    .as_ref()
                    .and_then(|r| r.resolved.connector_execution.as_ref())
                    .is_some_and(|x| !x.binding.descriptor.retry_after_noncommit)
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
    for c in &e.reconciliation_cases {
        refs.extend(c.evidence.iter().map(|e| &e.artifact));
    }
    for d in &e.dispatches {
        refs.extend(d.admission.as_ref().map(|a| &a.request_artifact));
    }
    refs
}
