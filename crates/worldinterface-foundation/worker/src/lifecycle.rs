use crate::{mapping::Operation, nonempty, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use worldinterface_protocol::ContentHash;

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
    /// Reconciliation proved absence; it does not assert a confirmed exit.
    NoProcess,
    ArtifactsPublished,
    ReleaseAccepted,
    Released,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconciliationOutcome {
    OwnedProcess,
    NoProcess,
}
/// Provider observation bound to a separately authorized WI recovery decision.
/// The caller must obtain the expected values from trusted WI state, not the worker.
#[derive(Clone, Copy)]
pub struct ReconciliationBinding<'a> {
    pub provider: &'a str,
    pub run: &'a str,
    pub authorization_digest: &'a ContentHash,
    pub ownership_evidence_digest: &'a ContentHash,
    pub outcome: ReconciliationOutcome,
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum AcceptedEvent {
    Fact(Fact),
    Reconciled {
        provider: String,
        run: String,
        authorization_digest: ContentHash,
        ownership_evidence_digest: ContentHash,
        outcome: ReconciliationOutcome,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lifecycle {
    pub phase: Phase,
    pub quarantined: bool,
    pub reconciled: bool,
    pub sequence: u64,
    pub last_event: Option<String>,
    // Event identities remain bound to their facts after later observations arrive.
    accepted: BTreeMap<String, (u64, AcceptedEvent)>,
}
impl Default for Lifecycle {
    fn default() -> Self {
        Self {
            phase: Phase::New,
            quarantined: false,
            reconciled: false,
            sequence: 0,
            last_event: None,
            accepted: BTreeMap::new(),
        }
    }
}
impl Lifecycle {
    pub fn apply(&mut self, sequence: u64, event: &str, fact: Fact) -> Result<()> {
        nonempty(event)?;
        if let Some(previous) = self.accepted.get(event) {
            return if *previous == (sequence, AcceptedEvent::Fact(fact)) {
                Ok(())
            } else {
                Err("conflicting worker event identity".into())
            };
        }
        if self.sequence.checked_add(1) != Some(sequence) {
            return Err("reordered or duplicate worker observation".into());
        }
        if self.quarantined
            && (self.reconciled
                && !matches!(
                    fact,
                    Fact::Uncertain
                        | Fact::StopRequested
                        | Fact::Exited
                        | Fact::Accepted(Operation::Release)
                        | Fact::Released
                )
                || !self.reconciled && fact != Fact::Uncertain)
        {
            return Err("quarantined worker requires verified cleanup".into());
        }
        let next = match (self.phase, fact) {
            (phase, Fact::Uncertain) if phase != Phase::Released => phase,
            (Phase::New, Fact::Accepted(Operation::Provision)) => Phase::ProvisionAccepted,
            (Phase::ProvisionAccepted, Fact::Provisioned) => Phase::Provisioned,
            (Phase::Provisioned, Fact::StartIntent) => Phase::StartIntent,
            (Phase::StartIntent, Fact::Accepted(Operation::Start)) => Phase::StartAccepted,
            (Phase::StartAccepted, Fact::Running) => Phase::Running,
            (Phase::Running, Fact::StopRequested) => Phase::StopRequested,
            (Phase::StartIntent | Phase::StartAccepted, Fact::StopRequested)
                if self.quarantined && self.reconciled =>
            {
                Phase::StopRequested
            }
            (
                Phase::StartIntent | Phase::StartAccepted | Phase::Running | Phase::StopRequested,
                Fact::Exited,
            ) => Phase::Exited,
            (Phase::Exited, Fact::ArtifactsPublished) if !self.quarantined => {
                Phase::ArtifactsPublished
            }
            (Phase::Exited | Phase::NoProcess, Fact::Accepted(Operation::Release))
                if self.quarantined && self.reconciled =>
            {
                Phase::ReleaseAccepted
            }
            (Phase::ArtifactsPublished, Fact::Accepted(Operation::Release)) => {
                Phase::ReleaseAccepted
            }
            (Phase::ReleaseAccepted, Fact::Released) => Phase::Released,
            _ => return Err("invalid worker lifecycle transition".into()),
        };
        self.phase = next;
        if fact == Fact::Uncertain {
            self.quarantined = true;
            self.reconciled = false;
        }
        self.sequence = sequence;
        self.last_event = Some(event.into());
        self.accepted.insert(event.into(), (sequence, AcceptedEvent::Fact(fact)));
        Ok(())
    }

    /// Record an authorized ownership reconciliation. An absent process is a
    /// confirmed cleanup outcome, not a fabricated worker exit observation.
    pub fn reconcile(
        &mut self,
        sequence: u64,
        event: &str,
        observed: &ReconciliationBinding<'_>,
        expected: &ReconciliationBinding<'_>,
    ) -> Result<()> {
        nonempty(event)?;
        for value in [observed.provider, observed.run, expected.provider, expected.run] {
            nonempty(value)?;
        }
        if observed.provider != expected.provider
            || observed.run != expected.run
            || observed.authorization_digest != expected.authorization_digest
            || observed.ownership_evidence_digest != expected.ownership_evidence_digest
            || observed.outcome != expected.outcome
        {
            return Err("unverified worker reconciliation".into());
        }
        let accepted = AcceptedEvent::Reconciled {
            provider: observed.provider.into(),
            run: observed.run.into(),
            authorization_digest: observed.authorization_digest.clone(),
            ownership_evidence_digest: observed.ownership_evidence_digest.clone(),
            outcome: observed.outcome,
        };
        if let Some(previous) = self.accepted.get(event) {
            return if *previous == (sequence, accepted) {
                Ok(())
            } else {
                Err("conflicting worker event identity".into())
            };
        }
        if !self.quarantined
            || self.reconciled
            || self.sequence.checked_add(1) != Some(sequence)
            || (observed.outcome == ReconciliationOutcome::OwnedProcess
                && !matches!(
                    self.phase,
                    Phase::StartIntent
                        | Phase::StartAccepted
                        | Phase::Running
                        | Phase::StopRequested
                ))
        {
            return Err("unverified worker reconciliation".into());
        }
        if observed.outcome == ReconciliationOutcome::NoProcess && self.phase != Phase::Exited {
            self.phase = Phase::NoProcess;
        }
        self.reconciled = true;
        self.sequence = sequence;
        self.last_event = Some(event.into());
        self.accepted.insert(event.into(), (sequence, accepted));
        Ok(())
    }
}
