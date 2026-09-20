use crate::Result;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PrincipalId(String);
impl PrincipalId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for PrincipalId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<PrincipalId> for String {
    fn from(v: PrincipalId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ActorId(String);
impl ActorId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ActorId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<ActorId> for String {
    fn from(v: ActorId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IngressBindingId(String);
impl IngressBindingId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for IngressBindingId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<IngressBindingId> for String {
    fn from(v: IngressBindingId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObservationId(String);
impl ObservationId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ObservationId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<ObservationId> for String {
    fn from(v: ObservationId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DeliveryId(String);
impl DeliveryId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for DeliveryId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<DeliveryId> for String {
    fn from(v: DeliveryId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EffectIntentId(String);
impl EffectIntentId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for EffectIntentId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<EffectIntentId> for String {
    fn from(v: EffectIntentId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AuthorizationId(String);
impl AuthorizationId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for AuthorizationId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<AuthorizationId> for String {
    fn from(v: AuthorizationId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ReceiptId(String);
impl ReceiptId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ReceiptId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<ReceiptId> for String {
    fn from(v: ReceiptId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ReconciliationId(String);
impl ReconciliationId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ReconciliationId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<ReconciliationId> for String {
    fn from(v: ReconciliationId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PlanId(String);
impl PlanId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for PlanId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<PlanId> for String {
    fn from(v: PlanId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct WorkerId(String);
impl WorkerId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for WorkerId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<WorkerId> for String {
    fn from(v: WorkerId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SandboxId(String);
impl SandboxId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for SandboxId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<SandboxId> for String {
    fn from(v: SandboxId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProofId(String);
impl ProofId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ProofId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<ProofId> for String {
    fn from(v: ProofId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CandidateId(String);
impl CandidateId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for CandidateId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<CandidateId> for String {
    fn from(v: CandidateId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EntityId(String);
impl EntityId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for EntityId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<EntityId> for String {
    fn from(v: EntityId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InstanceId(String);
impl InstanceId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for InstanceId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let id = uuid::Uuid::parse_str(&s).map_err(|e| e.to_string())?;
        if id.get_version_num() != 4
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != s
        {
            return Err("expected canonical UUIDv4".into());
        }
        Ok(Self(s))
    }
}
impl From<InstanceId> for String {
    fn from(v: InstanceId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OpaqueRef(String);
impl OpaqueRef {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for OpaqueRef {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        if s.is_empty()
            || s.len() > 256
            || !s.bytes().all(|c| c.is_ascii_alphanumeric() || b"-_: .".contains(&c))
            || s.contains(' ')
        {
            return Err("invalid bounded opaque reference".into());
        }
        Ok(Self(s))
    }
}
impl From<OpaqueRef> for String {
    fn from(v: OpaqueRef) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ClientRequestId(String);
impl ClientRequestId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ClientRequestId {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        if s.is_empty()
            || s.len() > 256
            || !s.bytes().all(|c| c.is_ascii_alphanumeric() || b"-_: .".contains(&c))
            || s.contains(' ')
        {
            return Err("invalid bounded opaque reference".into());
        }
        Ok(Self(s))
    }
}
impl From<ClientRequestId> for String {
    fn from(v: ClientRequestId) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ContentHash(String);
impl ContentHash {
    pub fn of(bytes: &[u8]) -> Self {
        use sha2::{Digest, Sha256};
        Self(format!("{:x}", Sha256::digest(bytes)))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ContentHash {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return Err("invalid SHA-256".into());
        }
        Ok(Self(s))
    }
}
impl From<ContentHash> for String {
    fn from(v: ContentHash) -> Self {
        v.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Timestamp(String);
impl Timestamp {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for Timestamp {
    type Error = String;
    fn try_from(s: String) -> Result<Self> {
        let t = chrono::DateTime::parse_from_rfc3339(&s).map_err(|e| e.to_string())?;
        if s.len() != 30
            || t.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string() != s
            || t.offset().local_minus_utc() != 0
            || s[17..19] == *"60"
        {
            return Err("expected UTC timestamp with nine fractional digits".into());
        }
        Ok(Self(s))
    }
}
impl From<Timestamp> for String {
    fn from(v: Timestamp) -> Self {
        v.0
    }
}

impl Timestamp {
    pub fn epoch_millis(&self) -> Result<u64> {
        let time =
            chrono::DateTime::parse_from_rfc3339(self.as_str()).map_err(|e| e.to_string())?;
        u64::try_from(time.timestamp_millis()).map_err(|_| "pre-epoch timestamp".into())
    }
}
