//! Assumption-bound internal ports, pending WI-AQ-G1. References are not grants.
//! Task payload locators and admission identities must be persisted before calling.
use worldinterface_protocol::{ContentHash, OpaqueRef};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueAdmission {
    pub task: OpaqueRef,
    pub admission_key: OpaqueRef,
    pub canonical_request: ContentHash,
    pub payload_locator: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionResult {
    Created(OpaqueRef),
    AlreadyExists(OpaqueRef),
    Conflict,
    UnknownAcknowledgment,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueueStatus {
    Missing,
    Pending,
    Running,
    Terminal,
}
pub trait QueueAdmissionPort {
    fn ensure(&mut self, request: &QueueAdmission) -> Result<AdmissionResult, String>;
}
pub trait QueueInspectionPort {
    fn inspect(&self, task: &OpaqueRef) -> Result<QueueStatus, String>;
}
pub trait QueueWakePort {
    fn wake(&mut self, key: &OpaqueRef, payload_locator: &OpaqueRef) -> Result<(), String>;
}

/// WI-04 fixture admission seam. Implementations belong in tests; this is not an AQ adapter.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectAdmissionRequest {
    pub version: u32,
    pub task: OpaqueRef,
    pub admission_key: OpaqueRef,
    pub intent: worldinterface_protocol::EffectIntentId,
    pub operation_index: u32,
    pub submission: ContentHash,
    pub agency: ContentHash,
    pub resolution: ContentHash,
    pub authorization: ContentHash,
    pub execution: ContentHash,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectAcknowledgment {
    pub task: OpaqueRef,
    pub admission_key: OpaqueRef,
    pub request_digest: ContentHash,
    pub durable_sequence: u64,
}
pub trait TestEffectAdmissionPort {
    /// Retry MUST receive the identical persisted request, including stable task identity.
    /// None means acknowledgment unknown, not evidence of non-admission or non-commit.
    fn ensure_effect(
        &mut self,
        request: &EffectAdmissionRequest,
    ) -> Result<Option<EffectAcknowledgment>, String>;
}
