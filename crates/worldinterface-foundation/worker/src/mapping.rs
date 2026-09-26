use crate::{nonempty, profile::ProfileBinding, Result};
use serde::{Deserialize, Serialize};
use worldinterface_capability::{CapabilityDescriptor, Idempotency, Reconciliation};
use worldinterface_protocol::ContentHash;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Provision,
    Start,
    Inspect,
    Terminate,
    CollectArtifacts,
    Release,
    Command,
    Checkpoint,
    Suspend,
    Resume,
}
impl Operation {
    pub fn available(self) -> bool {
        !matches!(self, Self::Command | Self::Checkpoint | Self::Suspend | Self::Resume)
    }
    pub fn semantics(self) -> Result<(Idempotency, Reconciliation)> {
        if !self.available() {
            return Err("operation unavailable".into());
        }
        Ok(match self {
            Self::Inspect => (Idempotency::ReadOnly, Reconciliation::QueryByKey),
            Self::CollectArtifacts => (Idempotency::ProviderKey, Reconciliation::ObserveVersion),
            _ => (Idempotency::ProviderKey, Reconciliation::QueryByKey),
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Provision => "provision",
            Self::Start => "start",
            Self::Inspect => "inspect",
            Self::Terminate => "terminate",
            Self::CollectArtifacts => "collect_artifacts",
            Self::Release => "release",
            Self::Command => "command",
            Self::Checkpoint => "checkpoint",
            Self::Suspend => "suspend",
            Self::Resume => "resume",
        }
    }
}
pub fn validate_descriptor(op: Operation, descriptor: &CapabilityDescriptor) -> Result<()> {
    let (idempotency, reconciliation) = op.semantics()?;
    if descriptor.operation.as_str() != op.name()
        || descriptor.idempotency != idempotency
        || descriptor.reconciliation != reconciliation
    {
        return Err("worker capability semantics differ".into());
    }
    Ok(())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRequestV1 {
    pub schema: u32,
    pub profile: ProfileBinding,
    pub operation: Operation,
    pub provider: String,
    pub run: String,
    pub request: String,
    pub input_digest: ContentHash,
    pub controls_digest: ContentHash,
}
impl WorkerRequestV1 {
    pub fn validate(&self) -> Result<()> {
        if self.schema != 1 {
            return Err("unsupported worker request".into());
        }
        self.profile.validate()?;
        self.operation.semantics()?;
        for s in [&self.provider, &self.run, &self.request] {
            nonempty(s)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedWorkerV1 {
    pub schema: u32,
    pub request_digest: ContentHash,
    pub provider: String,
    pub run: String,
    pub adapter_digest: ContentHash,
    pub executable_digest: ContentHash,
    pub dependencies_digest: ContentHash,
    pub configuration_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub controls_digest: ContentHash,
}
impl ResolvedWorkerV1 {
    pub fn validate_for(&self, request: &WorkerRequestV1, digest: &ContentHash) -> Result<()> {
        if self.schema != 1
            || &self.request_digest != digest
            || self.provider != request.provider
            || self.run != request.run
            || self.controls_digest != request.controls_digest
        {
            return Err("resolved worker binding drift".into());
        }
        Ok(())
    }
}
