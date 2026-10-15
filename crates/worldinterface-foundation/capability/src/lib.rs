//! Structural declarations, never policy evaluation or execution.
use serde::{Deserialize, Serialize};
use worldinterface_protocol::{CanonicalResourceRef, ContentHash, EffectClass, OpaqueRef};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Idempotency {
    ReadOnly,
    ProviderKey,
    CompareAndSwap,
    NonIdempotent,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Reconciliation {
    QueryByKey,
    ObserveVersion,
    ManualOnly,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityDescriptor {
    pub name: OpaqueRef,
    pub version: ContentHash,
    pub effect_class: EffectClass,
    pub operation: OpaqueRef,
    pub resource_namespace: OpaqueRef,
    pub idempotency: Idempotency,
    pub reconciliation: Reconciliation,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceScope {
    pub resources: Vec<CanonicalResourceRef>,
    pub operations: Vec<OpaqueRef>,
}
impl ResourceScope {
    /// Set inclusion only; this structural predicate grants no executable permission.
    pub fn attenuates(&self, parent: &Self) -> bool {
        self.resources.iter().all(|r| parent.resources.contains(r))
            && self.operations.iter().all(|o| parent.operations.contains(o))
    }
}

pub use worldinterface_protocol::{ConnectorBinding, ConnectorDescriptor, IdempotencyClass};
/// Validate declarations before accepting an executable implementation.
pub fn validate_connector(d: &ConnectorDescriptor) -> Result<(), String> {
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
/// Canonical immutable snapshot. No mutable replacement or fallback selection.
#[derive(Clone, Debug)]
pub struct ConnectorRegistry {
    descriptors: Vec<ConnectorDescriptor>,
    digest: ContentHash,
}
impl ConnectorRegistry {
    pub fn new(mut descriptors: Vec<ConnectorDescriptor>) -> Result<Self, String> {
        if descriptors.is_empty() || descriptors.len() > 32 {
            return Err("registry bound".into());
        }
        for d in &descriptors {
            validate_connector(d)?;
        }
        descriptors.sort_by(|a, b| (&a.connector, &a.operation).cmp(&(&b.connector, &b.operation)));
        if descriptors
            .windows(2)
            .any(|w| w[0].connector == w[1].connector && w[0].operation == w[1].operation)
        {
            return Err("duplicate connector operation".into());
        }
        let digest = ContentHash::of(&worldinterface_protocol::canonical_data(&(
            "wi-connector-registry:1",
            &descriptors,
        ))?);
        Ok(Self { descriptors, digest })
    }
    pub fn digest(&self) -> &ContentHash {
        &self.digest
    }
    pub fn bindings(&self) -> Result<Vec<ConnectorBinding>, String> {
        self.descriptors
            .iter()
            .map(|d| {
                Ok(ConnectorBinding {
                    version: 1,
                    descriptor: d.clone(),
                    descriptor_hash: d.digest()?,
                    registry_digest: self.digest.clone(),
                })
            })
            .collect()
    }
}
