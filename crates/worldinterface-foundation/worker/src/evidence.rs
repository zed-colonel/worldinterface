//! Incomplete observations are serializable even when admission is denied.
use crate::{
    controls::{Control, ControlEvidenceV1, ControlKind, ControlState},
    nonempty,
    profile::ProfileBinding,
    Result,
};
use serde::{Deserialize, Serialize};
use worldinterface_protocol::{ArtifactRef, ContentHash, EvidenceRef, OpaqueRef, PrincipalId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactWorkerBindingV1 {
    pub profile: ProfileBinding,
    pub adapter: ArtifactRef,
    pub executable: ArtifactRef,
    pub dependencies: Vec<ArtifactRef>,
    pub configuration: ArtifactRef,
    pub inputs: Vec<ArtifactRef>,
    pub principal: PrincipalId,
    pub run: OpaqueRef,
    pub resource: OpaqueRef,
    pub authorization: EvidenceRef,
    pub grant: EvidenceRef,
    pub artifact_scope: OpaqueRef,
    pub aq_revision: ContentHash,
    pub wi_revision: ContentHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum VerificationStatus {
    Verified,
    Unverified,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnclosingInstanceEvidenceV1 {
    pub source: OpaqueRef,
    pub trust: OpaqueRef,
    pub status: VerificationStatus,
    pub evidence: EvidenceRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerEvidenceReportV1 {
    pub schema: u32,
    pub binding: ExactWorkerBindingV1,
    pub controls: ControlEvidenceV1,
    pub missing: Vec<ControlKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing: Option<EnclosingInstanceEvidenceV1>,
    pub fixture_only: bool,
}
impl WorkerEvidenceReportV1 {
    /// Checks structure without suppressing a rejected or incomplete observation.
    pub fn validate(&self) -> Result<()> {
        if self.schema != 1
            || self.binding.profile.validate().is_err()
            || self.binding.dependencies.len() > 64
            || self.binding.inputs.len() > 64
            || self.missing.len() > 16
            || self.controls.schema != 1
            || self.controls.run != self.binding.run.as_str()
        {
            return Err("worker evidence binding or bound invalid".into());
        }
        nonempty(&self.controls.provider)?;
        nonempty(&self.controls.run)?;
        for a in [&self.binding.adapter, &self.binding.executable, &self.binding.configuration] {
            if a.scope != self.binding.artifact_scope {
                return Err("cross-scope worker artifact".into());
            }
        }
        if self
            .binding
            .dependencies
            .iter()
            .chain(&self.binding.inputs)
            .any(|a| a.scope != self.binding.artifact_scope)
            || self.binding.authorization.artifact.scope != self.binding.artifact_scope
            || self.binding.grant.artifact.scope != self.binding.artifact_scope
        {
            return Err("cross-scope worker evidence".into());
        }
        for list in [
            &self.controls.requested,
            &self.controls.advertised,
            &self.controls.applied,
            &self.controls.observed,
        ] {
            if list.len() > 16 {
                return Err("control bound".into());
            }
            let mut seen = std::collections::BTreeSet::new();
            for c in list {
                if !seen.insert(format!("{:?}", c.kind)) {
                    return Err("duplicate control".into());
                }
            }
        }
        let mut missing = std::collections::BTreeSet::new();
        for kind in &self.missing {
            if !missing.insert(format!("{:?}", kind)) {
                return Err("duplicate missing control".into());
            }
            if self
                .controls
                .applied
                .iter()
                .any(|c| c.kind == *kind && matches!(c.state, ControlState::Supported(_)))
            {
                return Err("missing control cannot be supported".into());
            }
        }
        Ok(())
    }
    /// Fixture eligibility is scoped to this exact definition; it never admits an ordinary provider.
    pub fn fixture_eligible(&self) -> Result<bool> {
        self.validate()?;
        if !self.fixture_only {
            return Ok(false);
        }
        Ok(self.controls.validate().is_ok())
    }
    pub fn mandatory_gaps(&self) -> Result<Vec<Control>> {
        self.validate()?;
        Ok(self.controls.requested.iter().filter(|c| c.mandatory &&
            !self.controls.applied.iter().any(|a| a.kind == c.kind && matches!((&c.state, &a.state),
                (ControlState::Supported(requested), ControlState::Supported(applied)) if applied <= requested))
        ).cloned().collect())
    }
}
