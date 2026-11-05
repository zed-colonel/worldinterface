//! Structural declarations, never policy evaluation or execution.
pub mod process_catalog;
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
    worldinterface_protocol::validate_connector_descriptor(d)
}
/// Canonical immutable snapshot. No mutable replacement or fallback selection.
#[derive(Clone, Debug)]
pub struct ConnectorRegistry {
    descriptors: Vec<ConnectorDescriptor>,
    digest: ContentHash,
}
impl ConnectorRegistry {
    pub fn new(mut descriptors: Vec<ConnectorDescriptor>) -> Result<Self, String> {
        let digest = worldinterface_protocol::validate_registry_snapshot(&mut descriptors)?;
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
