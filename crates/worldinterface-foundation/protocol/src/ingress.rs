//! Inactive ingress domain vocabulary. Millisecond inputs are explicit UTC epoch times.
//! Assertions and evidence references never confer authority.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderScope {
    pub tenant: OpaqueRef,
    pub account: OpaqueRef,
    pub source: CanonicalResourceRef,
    pub namespace: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum IdentityStrategy {
    StableId,
    StreamPosition,
    /// Explicit bounded approximation: same producer content in the same producer-time bucket.
    ContentWindow {
        window_ms: u64,
    },
    NonDeduplicable,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderProfile {
    pub attachment_policy: AttachmentPolicy,
    pub version: OpaqueRef,
    pub scope: ProviderScope,
    pub identity: IdentityStrategy,
    pub replay_window_ms: u64,
    pub max_body_bytes: u32,
    pub max_attachments: u32,
    pub capacity: u32,
    pub first_offset: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPolicy {
    pub version: OpaqueRef,
    pub max_attempts: u32,
    pub acknowledgment_timeout_ms: u64,
    pub backoff_ms: u64,
    pub retention_ms: u64,
    pub physical_erasure: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingStatus {
    Active,
    Suspended,
    Revoked,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressBinding {
    pub id: IngressBindingId,
    pub revision: u64,
    pub principal: PrincipalId,
    pub submission: IngressBindingSubmission,
    pub subscriber: PrincipalId,
    pub profile: ProviderProfile,
    pub policy: DeliveryPolicy,
    pub resolver_version: OpaqueRef,
    pub authorization: EvidenceRef,
    pub authentication_requirement: OpaqueRef,
    pub expires_at_ms: u64,
    pub status: BindingStatus,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingVersion {
    pub id: IngressBindingId,
    pub revision: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernedAttachment {
    pub artifact: ArtifactRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine: Option<OpaqueRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ObservationProfile {
    Ordinary,
    Commissioning { evidence: EvidenceRef },
    RoleStewardship { capacity: ActingCapacityRef, evidence: EvidenceRef },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEvent {
    pub scope: ProviderScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    pub produced_at_ms: u64,
    pub asserted_sender: OpaqueRef,
    pub asserted_audience: Vec<OpaqueRef>,
    pub content: ArtifactRef,
    pub attachments: Vec<GovernedAttachment>,
    pub information_use: InformationUseEnvelope,
    pub profile: ObservationProfile,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransportProvenance {
    pub transport: OpaqueRef,
    pub authenticated_peer: OpaqueRef,
    pub assurance: OpaqueRef,
    pub exact_bytes_digest: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_from: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderingEvidence {
    pub arrival: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    pub contiguous_next: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap: Option<(u64, u64)>,
    pub out_of_order: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressAdmission {
    pub identity: ContentHash,
    pub producer_digest: ContentHash,
    pub event: ProviderEvent,
    pub profile: ProviderProfile,
    pub provenance: TransportProvenance,
    pub observation: BoundaryObservation,
    pub receipt: BoundaryReceipt,
    pub bindings: Vec<BindingVersion>,
    pub deliveries: Vec<DeliveryId>,
    pub ordering: OrderingEvidence,
    pub admitted_at_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Pending,
    Acknowledged,
    DeadLetter,
    Expired,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Issued,
    WakeSucceeded,
    WakeFailed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryAttempt {
    pub number: u32,
    pub issued_at_ms: u64,
    pub next_attempt_ms: u64,
    pub token_digest: ContentHash,
    pub status: AttemptStatus,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryAcknowledgment {
    pub delivery: DeliveryId,
    pub subscriber: PrincipalId,
    pub acknowledged_at_ms: u64,
    pub attempt: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationDelivery {
    pub id: DeliveryId,
    pub observation: ObservationId,
    pub binding: BindingVersion,
    pub subscriber: PrincipalId,
    pub policy: DeliveryPolicy,
    pub expires_at_ms: u64,
    pub state: DeliveryState,
    pub attempts: Vec<DeliveryAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledgment: Option<DeliveryAcknowledgment>,
    pub token_invalidated: bool,
    pub wake_key: OpaqueRef,
    pub locator: OpaqueRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AttachmentPolicy {
    GovernedReferences,
    QuarantineAll { policy: OpaqueRef },
}
