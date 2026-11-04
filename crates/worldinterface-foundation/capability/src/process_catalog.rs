//! Bounded catalog declarations. Decoding these records never grants execution authority.
use serde::{Deserialize, Serialize};
use worldinterface_protocol::{canonical_data, ContentHash, OpaqueRef};

pub const PROCESS_OPERATIONS: [&str; 6] =
    ["provision", "start", "inspect", "terminate", "collect_artifacts", "release"];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessCatalogBinding {
    pub version: u32,
    pub profile: OpaqueRef,
    pub profile_definition: ContentHash,
    pub descriptor: ContentHash,
    pub adapter: ContentHash,
    pub executable: ContentHash,
    pub helper: ContentHash,
    pub dependencies: Vec<ContentHash>,
    pub configuration: ContentHash,
    pub policy: ContentHash,
    pub controls: ContentHash,
    pub aq: ContentHash,
    pub wi: ContentHash,
    pub environment: ContentHash,
    pub fixture: ContentHash,
    pub inputs: ContentHash,
    pub exports: ContentHash,
}
impl ProcessCatalogBinding {
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 || self.dependencies.is_empty() || self.dependencies.len() > 32 {
            return Err("catalog binding version or dependency bound".into());
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<ContentHash, String> {
        self.validate()?;
        Ok(ContentHash::of(&canonical_data(self)?))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrdinaryQualificationRecord {
    pub version: u32,
    pub binding: ProcessCatalogBinding,
    pub producer_proof: ContentHash,
    pub fixtures: ContentHash,
    pub results: ContentHash,
    pub reviewer_evidence: ContentHash,
    pub operator_decision: OpaqueRef,
    pub valid_from_ms: u64,
    pub expires_ms: u64,
    pub revoked: bool,
}
impl OrdinaryQualificationRecord {
    pub fn validate(&self) -> Result<(), String> {
        self.binding.validate()?;
        if self.version != 1 || self.valid_from_ms >= self.expires_ms {
            return Err("qualification version or validity".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessRegistrationRecord {
    Evaluation { binding: ProcessCatalogBinding, approval: ContentHash },
    Ordinary { qualification: OrdinaryQualificationRecord },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessProfileInspection {
    pub profile: OpaqueRef,
    pub operations: Vec<OpaqueRef>,
    pub mandatory_controls: Vec<OpaqueRef>,
    pub registration: OpaqueRef,
    pub denial_reasons: Vec<OpaqueRef>,
}
