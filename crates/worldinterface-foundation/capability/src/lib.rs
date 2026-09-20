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
