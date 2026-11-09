//! Typed evidence assertions, with explicit gaps and bounded completeness claims.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum EvidenceClass {
    SelfAssertion,
    Authorization,
    Executor,
    Provider,
    Transparency,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassifiedEvidenceV1 {
    pub version: u32,
    pub class: EvidenceClass,
    pub reference: EvidenceRef,
    pub source: OpaqueRef,
    pub verified: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceGapV1 {
    pub expected_class: EvidenceClass,
    pub reason: OpaqueRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletenessClaimV1 {
    pub version: u32,
    pub scope: OpaqueRef,
    pub from: Timestamp,
    pub through: Timestamp,
    pub log_policy: EvidenceRef,
    pub evidence: Vec<ClassifiedEvidenceV1>,
    pub gaps: Vec<EvidenceGapV1>,
}
impl CompletenessClaimV1 {
    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.from > self.through
            || self.evidence.len() > 64
            || self.gaps.len() > 64
        {
            return Err("invalid completeness scope or bound".into());
        }
        if self.evidence.iter().any(|e| e.version != 1) {
            return Err("unsupported evidence class version".into());
        }
        Ok(())
    }
}
