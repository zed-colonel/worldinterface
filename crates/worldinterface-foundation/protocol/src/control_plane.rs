//! Closed control-plane wire contracts. No transport identity or credential bytes.
use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Embedded,
    LocalDevelopment,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileStatus {
    pub profile: Profile,
    pub non_production: bool,
    pub tenant: OpaqueRef,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    Principal,
    Binding,
    Policy,
    Grant,
    Resource,
    Flow,
    Worker,
    Signal,
    Receipt,
    Evidence,
    Artifact,
    Audit,
    Secret,
    Authorization,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub tenant: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization: Option<PrincipalId>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    pub scope: Scope,
    pub kind: ObjectKind,
    pub id: OpaqueRef,
}

/// Metadata and an optional scoped artifact reference; never inline object bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectView {
    pub reference: ObjectRef,
    pub revision: u64,
    pub content: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<ArtifactRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub binding: ContentHash,
    pub after: OpaqueRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub kind: ObjectKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_filter: Option<OpaqueRef>,
    pub limit: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    pub objects: Vec<ObjectView>,
    pub visible_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<Cursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Audience {
    Worker,
    Connector,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialClass {
    Test,
    Production,
}

/// Every dimension is mandatory. Revisions are digests of attributed envelopes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialScope {
    pub scope: Scope,
    pub principal: PrincipalId,
    pub audience: Audience,
    pub recipient: OpaqueRef,
    pub provider: OpaqueRef,
    pub operation: OpaqueRef,
    pub resource: CanonicalResourceRef,
    pub run: OpaqueRef,
    pub authorization: ContentHash,
    pub revisions: BTreeMap<OpaqueRef, ContentHash>,
    pub class: CredentialClass,
    pub secret: ObjectRef,
    pub secret_version: u64,
    pub generation: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialRequest {
    pub request: ClientRequestId,
    pub permit: ObjectRef,
    pub binding: CredentialScope,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CustodyReason {
    Unavailable,
    Rejected,
    Uncertain,
    Stale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssuanceStatus {
    Pending,
    Delivered,
    Unresolved,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialMetadata {
    pub issuance: ContentHash,
    pub permit: ObjectRef,
    pub binding: CredentialScope,
    pub status: IssuanceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<CustodyReason>,
}
