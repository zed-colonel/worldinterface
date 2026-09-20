//! Assumption-bound internal ports, pending WI-AQ-G1. References are not grants.
//! Task payload locators and admission identities must be persisted before calling.
use worldinterface_protocol::{ContentHash, OpaqueRef};

/// Domain planning summary only, not a durable AQ EnsureTaskRequest.
/// G2 must persist the full stable request (spec, dependencies, causal context,
/// admission key and task identity) before submission; this type cannot do so.
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
