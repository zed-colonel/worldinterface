use crate::{mapping::Operation, nonempty, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    New,
    ProvisionAccepted,
    Provisioned,
    StartIntent,
    StartAccepted,
    Running,
    StopRequested,
    Exited,
    ArtifactsPublished,
    ReleaseAccepted,
    Released,
    Quarantined,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fact {
    Accepted(Operation),
    Provisioned,
    StartIntent,
    Running,
    StopRequested,
    Exited,
    ArtifactsPublished,
    Released,
    Uncertain,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lifecycle {
    pub phase: Phase,
    pub sequence: u64,
    pub last_event: Option<String>,
}
impl Default for Lifecycle {
    fn default() -> Self {
        Self { phase: Phase::New, sequence: 0, last_event: None }
    }
}
impl Lifecycle {
    pub fn apply(&mut self, sequence: u64, event: &str, fact: Fact) -> Result<()> {
        nonempty(event)?;
        if self.last_event.as_deref() == Some(event) && self.sequence == sequence {
            return Ok(());
        }
        if sequence != self.sequence + 1 {
            return Err("reordered or duplicate worker observation".into());
        }
        let next = match (self.phase, fact) {
            (_, Fact::Uncertain) => Phase::Quarantined,
            (Phase::New, Fact::Accepted(Operation::Provision)) => Phase::ProvisionAccepted,
            (Phase::ProvisionAccepted, Fact::Provisioned) => Phase::Provisioned,
            (Phase::Provisioned, Fact::StartIntent) => Phase::StartIntent,
            (Phase::StartIntent, Fact::Accepted(Operation::Start)) => Phase::StartAccepted,
            (Phase::StartAccepted, Fact::Running) => Phase::Running,
            (Phase::Running, Fact::StopRequested) => Phase::StopRequested,
            (Phase::StopRequested, Fact::Exited) => Phase::Exited,
            (Phase::Exited, Fact::ArtifactsPublished) => Phase::ArtifactsPublished,
            (Phase::ArtifactsPublished, Fact::Accepted(Operation::Release)) => {
                Phase::ReleaseAccepted
            }
            (Phase::ReleaseAccepted, Fact::Released) => Phase::Released,
            _ => return Err("invalid worker lifecycle transition".into()),
        };
        self.phase = next;
        self.sequence = sequence;
        self.last_event = Some(event.into());
        Ok(())
    }
}
