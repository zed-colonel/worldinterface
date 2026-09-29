//! Versioned receipt admission and separate proof material. Proofs never alter receipts.
use crate::*;
use serde::{Deserialize, Serialize};

pub const BOUNDARY_PROFILE: &str = "boundary:1";
pub const DEVELOPMENTAL_PROFILE: &str = "developmental:1";

pub fn validate_receipt(receipt: &BoundaryReceipt) -> Result<()> {
    if ![BOUNDARY_PROFILE, DEVELOPMENTAL_PROFILE].contains(&receipt.profile.as_str()) {
        return Err("unsupported receipt profile".into());
    }
    if receipt.evidence.len() > 64 || receipt.causal_refs.len() > 64 {
        return Err("receipt reference bound".into());
    }
    if let ReceiptBody::Egress(body) = &receipt.body {
        if body.output_refs.len() > 64 {
            return Err("invalid egress output references".into());
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SigningProofV1 {
    pub version: u32,
    pub receipt_digest: ContentHash,
    pub suite: OpaqueRef,
    pub purpose: OpaqueRef,
    pub signed_at: Timestamp,
    pub key_state: EvidenceRef,
    pub signature: ArtifactRef,
}
impl SigningProofV1 {
    pub fn signing_input(&self) -> Result<Vec<u8>> {
        if self.version != 1 || self.suite.as_str() != "ed25519:1" {
            return Err("unsupported signing proof".into());
        }
        let mut input = b"WI-FABRIC-2\0wi-json-1\0receipt-signing-proof-1\0".to_vec();
        input.extend(canonical_data(&(
            &self.receipt_digest,
            &self.suite,
            &self.purpose,
            &self.signed_at,
            &self.key_state,
        ))?);
        Ok(input)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProofBundleKind {
    Candidate,
    Build,
    Trial,
    Activation,
    Rollback,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProofBundleV1 {
    pub version: u32,
    pub kind: ProofBundleKind,
    pub candidate: CandidateId,
    pub records: Vec<ContentHash>,
    pub evidence: Vec<EvidenceRef>,
}
impl ProofBundleV1 {
    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.records.is_empty()
            || self.records.len() > 64
            || self.evidence.len() > 64
        {
            return Err("unsupported or unbounded proof bundle".into());
        }
        Ok(())
    }
}
