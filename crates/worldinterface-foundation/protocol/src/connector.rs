//! Versioned connector contract. Legacy capability declarations are not SPI registrations.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyClass {
    ReadOnly,
    NaturallyIdempotent,
    ExternallyKeyed,
    Compensatable,
    NonRepeatable,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderKeyMapping {
    pub version: u32,
    pub provider_scope: OpaqueRef,
    pub parameter: OpaqueRef,
}
impl ProviderKeyMapping {
    /// Deliberately excludes queue, dispatch generation and attempt identity.
    pub fn derive(
        &self,
        tenant: &OpaqueRef,
        operation: &OpaqueRef,
        identity: &OpaqueRef,
    ) -> Result<OpaqueRef> {
        if self.version != 1 {
            return Err("unsupported provider key mapping".into());
        }
        let bytes = canonical_data(&(
            "wi-provider-key:1",
            tenant,
            &self.provider_scope,
            operation,
            identity,
        ))?;
        format!("wi-key-1:{}", ContentHash::of(&bytes).as_str()).try_into()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationPolicy {
    pub max_observations: u32,
    pub backoff_millis: u64,
    pub timeout_millis: u64,
    pub automatic: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorDescriptor {
    pub version: u32,
    pub connector: OpaqueRef,
    pub implementation: ContentHash,
    pub operation: OpaqueRef,
    pub effect_class: EffectClass,
    pub resource_namespace: OpaqueRef,
    pub resource_grammar: OpaqueRef,
    pub input_schema: OpaqueRef,
    pub output_schema: OpaqueRef,
    pub isolation_profile: OpaqueRef,
    /// Broker references only. Values never belong in this contract or evidence.
    pub secret_refs: Vec<OpaqueRef>,
    pub idempotency: IdempotencyClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_key: Option<ProviderKeyMapping>,
    pub retry_after_noncommit: bool,
    pub observes_commit: bool,
    pub reconciliation: ReconciliationPolicy,
    pub cancellation_supported: bool,
    pub evidence_schema: OpaqueRef,
    pub max_evidence_bytes: u64,
}
impl ConnectorDescriptor {
    pub fn digest(&self) -> Result<ContentHash> {
        Ok(ContentHash::of(&canonical_data(self)?))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorBinding {
    pub version: u32,
    pub descriptor: ConnectorDescriptor,
    pub descriptor_hash: ContentHash,
    pub registry_digest: ContentHash,
}
impl ConnectorBinding {
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 || self.descriptor.digest()? != self.descriptor_hash {
            return Err("invalid connector binding".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorExecution {
    pub binding: ConnectorBinding,
    pub operation_identity: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_key: Option<OpaqueRef>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorClassification {
    PreDispatchFailure,
    ConfirmedNotCommitted,
    Committed,
    TerminalFailure,
    AmbiguousCommit,
}
