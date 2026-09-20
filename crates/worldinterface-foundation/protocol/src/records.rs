use crate::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub scope: OpaqueRef,
    pub content: ContentHash,
    pub size: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRef {
    pub artifact: ArtifactRef,
    pub schema: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectInputRef {
    pub artifact: ArtifactRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSelector {
    pub namespace: OpaqueRef,
    pub selector: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalResourceRef {
    pub namespace: OpaqueRef,
    pub resource: OpaqueRef,
    pub version: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActingCapacityRef {
    pub principal_ref: PrincipalId,
    pub assignment_ref: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_ref: Option<PrincipalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_ref: Option<EvidenceRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RedistributionPolicy {
    Prohibited,
    NamedAudiences,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InformationUseEnvelope {
    pub purpose_refs: Vec<OpaqueRef>,
    pub allowed_audiences: Vec<PrincipalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_policy_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disclosure_policy_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derivation_policy_ref: Option<OpaqueRef>,
    pub redistribution: RedistributionPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration: Option<Timestamp>,
    pub source_obligation_refs: Vec<OpaqueRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeEvidence {
    pub at: Timestamp,
    pub source: OpaqueRef,
    pub assurance: OpaqueRef,
    pub uncertainty_nanos: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalProvenance {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurred_at: Option<TimeEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<TimeEvidence>,
    pub received_at: TimeEvidence,
    pub admitted_at: TimeEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<TimeEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<TimeEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciled_at: Option<TimeEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<OpaqueRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectBudget {
    pub max_attempts: u32,
    pub timeout_millis: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundaryVersions {
    pub resolver: OpaqueRef,
    pub capability_descriptor: ContentHash,
    pub policy: OpaqueRef,
    pub grant_versions: Vec<ContentHash>,
    pub connector_profile: ContentHash,
    pub provider_profile: ContentHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectSubmission {
    pub client_request_id: ClientRequestId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_accountable_principal: Option<PrincipalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_by: Option<ActorId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<OpaqueRef>,
    pub causal_refs: Vec<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_capacity: Option<ActingCapacityRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub information_use: Option<InformationUseEnvelope>,
    pub effect_class: EffectClass,
    pub resource: ResourceSelector,
    pub input: EffectInputRef,
    pub requested_authority_refs: Vec<OpaqueRef>,
    pub approval_attestations: Vec<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    pub requested_receipt_profile: OpaqueRef,
    pub budget: EffectBudget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_outcome: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_credential: Option<EvidenceRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectClass {
    Read,
    Mutate,
    Publish,
    Control,
    Lifecycle,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressBindingSubmission {
    pub client_request_id: ClientRequestId,
    pub source: ResourceSelector,
    pub subscriber: OpaqueRef,
    pub event_scope: OpaqueRef,
    pub delivery_policy: OpaqueRef,
    pub requested_authority_refs: Vec<OpaqueRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgencyContext {
    pub accountable_principal: PrincipalId,
    pub requester: ActorId,
    pub delegation_chain: Vec<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_by: Option<ActorId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<OpaqueRef>,
    pub causal_refs: Vec<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_capacity: Option<ActingCapacityRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectIntent {
    pub id: EffectIntentId,
    pub submission: EffectSubmission,
    pub agency: AgencyContext,
    pub stable_idempotency_identity: OpaqueRef,
    pub submitted_at: Timestamp,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedEffect {
    pub intent_id: EffectIntentId,
    pub effect_class: EffectClass,
    pub resource: CanonicalResourceRef,
    pub normalized_parameters: EffectInputRef,
    pub resolver_version: OpaqueRef,
    pub registry_snapshot: ContentHash,
    pub connector: OpaqueRef,
    pub idempotency_semantics: OpaqueRef,
    pub sensitivity: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Decision {
    Allow,
    Deny,
    NeedsApproval,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationDecision {
    pub id: AuthorizationId,
    pub intent_id: EffectIntentId,
    pub resolved_effect_digest: ContentHash,
    pub agency_digest: ContentHash,
    pub policy_version: OpaqueRef,
    pub resolver_version: OpaqueRef,
    pub grant_refs: Vec<OpaqueRef>,
    pub approval_refs: Vec<OpaqueRef>,
    pub constraints: ArtifactRef,
    pub versions: BoundaryVersions,
    pub decision: Decision,
    pub expires_at: Timestamp,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundaryObservation {
    pub id: ObservationId,
    pub binding_id: IngressBindingId,
    pub source_connector: OpaqueRef,
    pub canonical_source: CanonicalResourceRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_event_id: Option<OpaqueRef>,
    pub deduplication_key: OpaqueRef,
    pub asserted_sender: OpaqueRef,
    pub asserted_audience: Vec<OpaqueRef>,
    pub authentication_assurance: OpaqueRef,
    pub content_ref: ArtifactRef,
    pub attachment_refs: Vec<ArtifactRef>,
    pub temporal: TemporalProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub information_use: Option<InformationUseEnvelope>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionOutcome {
    Succeeded,
    FailedBeforeCommit,
    CanceledBeforeCommit,
    Uncertain,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRecord {
    pub intent_id: EffectIntentId,
    pub attempt: OpaqueRef,
    pub outcome: ExecutionOutcome,
    pub evidence: Vec<EvidenceRef>,
    pub recorded_at: Timestamp,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationRecord {
    pub id: ReconciliationId,
    pub intent_id: EffectIntentId,
    pub result: ReconciliationResult,
    pub evidence: Vec<EvidenceRef>,
    pub recorded_at: Timestamp,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReconciliationResult {
    ConfirmedCommitted,
    ConfirmedNotCommitted,
    StillUncertain,
    Escalated,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressReceipt {
    pub observation_id: ObservationId,
    pub binding_id: IngressBindingId,
    pub source: CanonicalResourceRef,
    pub observation_digest: ContentHash,
    pub validation: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EgressReceipt {
    pub intent_id: EffectIntentId,
    pub agency_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub resolved_effect_digest: ContentHash,
    pub attempt: OpaqueRef,
    pub executor: OpaqueRef,
    pub outcome: ExecutionOutcome,
    pub output_refs: Vec<ArtifactRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "direction", content = "body", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReceiptBody {
    Ingress(IngressReceipt),
    Egress(EgressReceipt),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundaryReceipt {
    pub id: ReceiptId,
    pub profile: OpaqueRef,
    pub versions: BoundaryVersions,
    pub body: ReceiptBody,
    pub temporal: TemporalProvenance,
    pub evidence: Vec<EvidenceRef>,
    pub causal_refs: Vec<OpaqueRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommissioningSubmission {
    pub client_request_id: ClientRequestId,
    pub accountable_principal: PrincipalId,
    pub genesis_ref: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_capacity_ref: Option<ActingCapacityRef>,
    pub requested_operations: Vec<EffectSubmission>,
    pub disclosure_policy_ref: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateArtifact {
    pub candidate_id: CandidateId,
    pub component_class: OpaqueRef,
    pub subject_ref: OpaqueRef,
    pub source_revision_ref: ArtifactRef,
    pub build_artifact_ref: ArtifactRef,
    pub build_provenance_ref: EvidenceRef,
    pub evaluation_plan_ref: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LifecycleOperation {
    Activate,
    Rollback,
    Quarantine,
    Restore,
    Migration,
    Succession,
    Fork,
    Retire,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleRecord {
    pub candidate_id: CandidateId,
    pub operation: LifecycleOperation,
    pub intent_id: EffectIntentId,
    pub source_lineage: OpaqueRef,
    pub target: OpaqueRef,
    pub authorizer: AuthorizationId,
    pub receipt: ReceiptId,
    pub recorded_at: Timestamp,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateVerification {
    pub candidate_id: CandidateId,
    pub report: EvidenceRef,
    pub verifier: PrincipalId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateAcceptance {
    pub candidate_id: CandidateId,
    pub decision_ref: OpaqueRef,
    pub accountable_principal: PrincipalId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentProvenance {
    pub component: OpaqueRef,
    pub source: ArtifactRef,
    pub build: ArtifactRef,
    pub toolchain: ArtifactRef,
    pub builder: PrincipalId,
    pub inputs: Vec<ArtifactRef>,
    pub outputs: Vec<ArtifactRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImmutableArtifactManifest {
    pub executable: ArtifactRef,
    pub dependencies: Vec<ArtifactRef>,
    pub configuration: ArtifactRef,
    pub profile_definition: ArtifactRef,
    pub adapter: ArtifactRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ImageApplicability {
    Known(ContentHash),
    NotApplicable,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderKind {
    Process,
    Image,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlValues {
    pub cpu_millis: u32,
    pub memory_bytes: u64,
    pub network: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlEvidence {
    pub version: u32,
    pub provider_kind: ProviderKind,
    pub provider_ref: OpaqueRef,
    pub run_ref: OpaqueRef,
    pub resource_ref: OpaqueRef,
    pub profile: OpaqueRef,
    pub realization: ArtifactRef,
    pub image: ImageApplicability,
    pub requested: ControlValues,
    pub advertised: ControlValues,
    pub applied: ControlValues,
    pub observed: ControlValues,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing_instance: Option<EvidenceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing_source: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing_trust: Option<OpaqueRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "snake_case", deny_unknown_fields)]
pub enum Record {
    IngressBinding(Box<IngressBinding>),
    ObservationDelivery(Box<ObservationDelivery>),
    DeliveryAttempt(DeliveryAttempt),
    DeliveryAcknowledgment(DeliveryAcknowledgment),
    IngressAdmission(Box<IngressAdmission>),
    EffectSubmission(EffectSubmission),
    IngressBindingSubmission(IngressBindingSubmission),
    AgencyContext(AgencyContext),
    EffectIntent(Box<EffectIntent>),
    ResolvedEffect(ResolvedEffect),
    AuthorizationDecision(AuthorizationDecision),
    BoundaryObservation(BoundaryObservation),
    ExecutionRecord(ExecutionRecord),
    ReconciliationRecord(ReconciliationRecord),
    BoundaryReceipt(BoundaryReceipt),
    ActingCapacityRef(ActingCapacityRef),
    InformationUseEnvelope(InformationUseEnvelope),
    TemporalProvenance(TemporalProvenance),
    CommissioningSubmission(CommissioningSubmission),
    CandidateArtifact(CandidateArtifact),
    CandidateVerification(CandidateVerification),
    CandidateAcceptance(CandidateAcceptance),
    LifecycleRecord(LifecycleRecord),
    ComponentProvenance(ComponentProvenance),
    ImmutableArtifactManifest(ImmutableArtifactManifest),
    ControlEvidence(ControlEvidence),
}
impl Record {
    pub fn validate(&self) -> Result<()> {
        if let Self::ControlEvidence(e) = self {
            if e.version != 1
                || (e.provider_kind == ProviderKind::Process
                    && e.image != ImageApplicability::NotApplicable)
                || (e.provider_kind == ProviderKind::Image
                    && e.image == ImageApplicability::NotApplicable)
            {
                return Err("invalid process/image applicability".into());
            }
            if e.enclosing_instance.is_some() != e.enclosing_source.is_some()
                || e.enclosing_instance.is_some() != e.enclosing_trust.is_some()
            {
                return Err("enclosing evidence requires source and trust".into());
            }
        }
        Ok(())
    }
}
