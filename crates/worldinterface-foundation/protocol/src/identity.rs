use crate::Result;
use serde::{Deserialize, Serialize};

macro_rules! define_id {
    ($name:ident, uuid) => {
        define_id!($name, opaque_type);
        impl $name {
            pub fn generate() -> Self {
                Self(uuid::Uuid::new_v4().to_string())
            }
        }
        impl TryFrom<String> for $name {
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
    };
    ($name:ident, opaque) => {
        define_id!($name, opaque_type);
        impl TryFrom<String> for $name {
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
    };
    ($name:ident, opaque_type) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);
        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl From<$name> for String {
            fn from(v: $name) -> Self {
                v.0
            }
        }
    };
}
define_id!(PrincipalId, uuid);
define_id!(ActorId, uuid);
define_id!(IngressBindingId, uuid);
define_id!(ObservationId, uuid);
define_id!(DeliveryId, uuid);
define_id!(EffectIntentId, uuid);
define_id!(AuthorizationId, uuid);
define_id!(ReceiptId, uuid);
define_id!(ReconciliationId, uuid);
define_id!(PlanId, uuid);
define_id!(WorkerId, uuid);
define_id!(SandboxId, uuid);
define_id!(ProofId, uuid);
define_id!(CandidateId, uuid);
define_id!(EntityId, uuid);
define_id!(InstanceId, uuid);
define_id!(OpaqueRef, opaque);
define_id!(ClientRequestId, opaque);

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
    pub fn from_epoch_millis(millis: u64) -> Result<Self> {
        let millis = i64::try_from(millis).map_err(|_| "timestamp overflow")?;
        let time = chrono::DateTime::from_timestamp_millis(millis).ok_or("timestamp overflow")?;
        time.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string().try_into()
    }
    pub fn epoch_millis(&self) -> Result<u64> {
        let time =
            chrono::DateTime::parse_from_rfc3339(self.as_str()).map_err(|e| e.to_string())?;
        u64::try_from(time.timestamp_millis()).map_err(|_| "pre-epoch timestamp".into())
    }
}
