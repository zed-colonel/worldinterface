use crate::{nonempty, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    WallTimeMs,
    OutputBytes,
    Cpu,
    Memory,
    ProcessCount,
    Network,
    Filesystem,
    IndependentVm,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlState {
    Supported(u64),
    Unsupported,
    Unknown,
    NotApplicable,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Control {
    pub kind: ControlKind,
    pub mandatory: bool,
    pub state: ControlState,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlEvidenceV1 {
    pub schema: u32,
    pub provider: String,
    pub run: String,
    pub requested: Vec<Control>,
    pub advertised: Vec<Control>,
    pub applied: Vec<Control>,
    pub observed: Vec<Control>,
}
impl ControlEvidenceV1 {
    pub fn validate(&self) -> Result<()> {
        if self.schema != 1 {
            return Err("unsupported control schema".into());
        }
        nonempty(&self.provider)?;
        nonempty(&self.run)?;
        for list in [&self.requested, &self.advertised, &self.applied, &self.observed] {
            if list.len() > 16 {
                return Err("too many controls".into());
            }
            let mut seen = std::collections::BTreeSet::new();
            for c in list {
                if !seen.insert(format!("{:?}", c.kind)) {
                    return Err("duplicate control".into());
                }
                if let ControlState::Supported(n) = c.state {
                    if n == 0 || n > 1_000_000_000_000 {
                        return Err("invalid control limit".into());
                    }
                }
                if matches!(c.state, ControlState::Supported(_))
                    && !matches!(c.kind, ControlKind::WallTimeMs | ControlKind::OutputBytes)
                {
                    return Err("control lacks process-profile enforcement evidence".into());
                }
            }
        }
        for kind in [ControlKind::WallTimeMs, ControlKind::OutputBytes] {
            if !self
                .applied
                .iter()
                .any(|c| c.kind == kind && matches!(c.state, ControlState::Supported(_)))
            {
                return Err("required finite control missing".into());
            }
        }
        for c in &self.requested {
            let applied = self.applied.iter().find(|a| a.kind == c.kind);
            match (&c.state, applied.map(|a| &a.state)) {
                (ControlState::Supported(requested), Some(ControlState::Supported(actual)))
                    if actual <= requested => {}
                (ControlState::Supported(_), Some(ControlState::Supported(_))) => {
                    return Err("applied control exceeds requested limit".into());
                }
                _ if c.mandatory => return Err("mandatory control unavailable".into()),
                _ => {}
            }
        }
        Ok(())
    }
}
