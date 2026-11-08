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
    /// Provider-side request field/header that receives the derived key. This is
    /// transport metadata, not a key namespace: renaming the provider parameter
    /// must preserve replay identity. provider_scope names the namespace. The
    /// pinned descriptor hashes this field so an adapter cannot silently change
    /// where it places the key.
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
        crate::data_digest(self)
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

pub fn validate_connector_descriptor(d: &ConnectorDescriptor) -> Result<()> {
    if d.version != 1
        || d.secret_refs.len() > 32
        || d.max_evidence_bytes == 0
        || d.max_evidence_bytes > 1024 * 1024
        || d.reconciliation.max_observations == 0
        || d.reconciliation.max_observations > 32
        || d.reconciliation.timeout_millis == 0
        || d.reconciliation.timeout_millis > 86_400_000
        || d.reconciliation.backoff_millis == 0
        || d.reconciliation.backoff_millis > d.reconciliation.timeout_millis
    {
        return Err("unsupported or unbounded connector declaration".into());
    }
    if (d.effect_class != EffectClass::Read
        && matches!(d.idempotency, IdempotencyClass::ReadOnly | IdempotencyClass::Unknown))
        || (d.idempotency == IdempotencyClass::ExternallyKeyed && d.provider_key.is_none())
        || (d.provider_key.is_some() && d.idempotency != IdempotencyClass::ExternallyKeyed)
        || d.provider_key.as_ref().is_some_and(|k| k.version != 1)
        || d.reconciliation.automatic && !d.observes_commit
    {
        return Err("contradictory connector declaration".into());
    }
    let mut refs = std::collections::BTreeSet::new();
    if d.secret_refs.iter().any(|r| !refs.insert(r)) {
        return Err("duplicate secret reference".into());
    }
    Ok(())
}

/// Validate and normalize a snapshot using the original WIP-05 digest format.
pub fn validate_registry_snapshot(descriptors: &mut [ConnectorDescriptor]) -> Result<ContentHash> {
    if descriptors.is_empty() || descriptors.len() > 32 {
        return Err("registry bound".into());
    }
    for d in descriptors.iter() {
        validate_connector_descriptor(d)?;
    }
    descriptors.sort_by(|a, b| (&a.connector, &a.operation).cmp(&(&b.connector, &b.operation)));
    if descriptors
        .windows(2)
        .any(|w| w[0].connector == w[1].connector && w[0].operation == w[1].operation)
    {
        return Err("duplicate connector operation".into());
    }
    crate::data_digest(&("wi-connector-registry:1", descriptors))
}
