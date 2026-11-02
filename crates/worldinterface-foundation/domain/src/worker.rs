//! Durable worker ownership facts. AQ task ownership never releases these reservations.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use worldinterface_protocol::{ContentHash, EffectIntentId, OpaqueRef, PrincipalId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerBinding {
    pub tenant: OpaqueRef,
    pub principal: PrincipalId,
    pub provider: OpaqueRef,
    pub run: OpaqueRef,
    pub request: OpaqueRef,
    /// Exact request, executable, configuration, control, input and evaluation bindings.
    pub realization: ContentHash,
}
impl WorkerBinding {
    /// Providers may reuse a run name; a notification must bind both dimensions.
    pub fn event_correlation(&self) -> Result<OpaqueRef, String> {
        format!(
            "wi-worker-run:{}",
            ContentHash::of(&worldinterface_protocol::canonical_data(&(
                "wi-worker-stream-1",
                &self.provider,
                &self.run
            ))?)
            .as_str()
        )
        .try_into()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerFact {
    Provisioned,
    ArtifactsPublished,
    Released,
    StartAccepted,
    Running,
    StopRequested,
    Exited,
    Completed,
    Quarantined,
    CleanupConfirmed,
    NoProcess,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerObservation {
    pub event: OpaqueRef,
    pub provider_sequence: u64,
    pub fact: WorkerFact,
    pub digest: ContentHash,
}
/// The provider key is the exact WIP-05 execution key, independent of AQ attempt identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerOperation {
    pub key: OpaqueRef,
    pub intent: EffectIntentId,
    pub operation: OpaqueRef,
    pub canonical_request: ContentHash,
    pub acknowledged: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerState {
    pub binding: WorkerBinding,
    pub revision: u64,
    pub accepted: bool,
    pub running: bool,
    pub stop_requested: bool,
    pub exited: bool,
    pub no_process: bool,
    pub completed: bool,
    pub quarantined: bool,
    pub cleanup_confirmed: bool,
    pub capacity_reserved: bool,
    /// Highest contiguous provider sequence applied, distinct from WI revision.
    pub provider_sequence: u64,
    pub observations: BTreeMap<u64, WorkerObservation>,
    pub operations: BTreeMap<OpaqueRef, WorkerOperation>,
}
impl WorkerState {
    pub fn start_intent(binding: WorkerBinding) -> Self {
        Self {
            binding,
            revision: 1,
            accepted: false,
            running: false,
            stop_requested: false,
            exited: false,
            no_process: false,
            completed: false,
            quarantined: false,
            cleanup_confirmed: false,
            capacity_reserved: true,
            provider_sequence: 0,
            observations: BTreeMap::new(),
            operations: BTreeMap::new(),
        }
    }
    pub fn transition(&mut self, fact: WorkerFact) -> Result<(), String> {
        match fact {
            WorkerFact::Provisioned if !self.cleanup_confirmed => {}
            WorkerFact::ArtifactsPublished if self.exited && !self.quarantined => {}
            WorkerFact::Released if self.cleanup_confirmed => {}
            WorkerFact::StartAccepted
                if !self.cleanup_confirmed
                    && !self.quarantined
                    && !self.exited
                    && !self.stop_requested =>
            {
                self.accepted = true
            }
            WorkerFact::Running
                if self.accepted && !self.stop_requested && !self.exited && !self.quarantined =>
            {
                self.running = true
            }
            WorkerFact::StopRequested if !self.cleanup_confirmed => self.stop_requested = true,
            WorkerFact::Exited if !self.cleanup_confirmed => {
                self.exited = true;
                self.running = false;
            }
            WorkerFact::Completed if self.exited && !self.quarantined && !self.stop_requested => {
                self.completed = true
            }
            WorkerFact::Quarantined if !self.cleanup_confirmed => {
                self.quarantined = true;
                self.stop_requested = true;
            }
            // Trusted cleanup evidence may prove no process was ever launched.
            WorkerFact::NoProcess if !self.accepted && !self.running => {
                self.no_process = true;
            }
            WorkerFact::CleanupConfirmed if self.exited || self.quarantined || self.no_process => {
                self.cleanup_confirmed = true;
                self.running = false;
                self.capacity_reserved = false;
            }
            _ => return Err("invalid or terminal worker transition".into()),
        }
        Ok(())
    }
    /// Keep gaps inspectable. Reordering never manufactures missing observations.
    pub fn observe(&mut self, observation: WorkerObservation) -> Result<bool, String> {
        if matches!(observation.fact, WorkerFact::CleanupConfirmed | WorkerFact::NoProcess) {
            return Err("provider cannot release ownership capacity".into());
        }
        if observation.provider_sequence == 0 || self.observations.len() >= 4096 {
            return Err("worker observation bound".into());
        }
        if let Some(old) = self.observations.get(&observation.provider_sequence) {
            return if old == &observation {
                Ok(false)
            } else {
                Err("worker sequence conflict".into())
            };
        }
        if self.observations.values().any(|o| o.event == observation.event) {
            return Err("worker event identity conflict".into());
        }
        self.observations.insert(observation.provider_sequence, observation);
        while let Some(next) = self.observations.get(&(self.provider_sequence + 1)).cloned() {
            // Late facts remain evidence; they cannot revive terminal or quarantined work.
            if self.transition(next.fact).is_err()
                && !self.exited
                && !self.quarantined
                && !self.cleanup_confirmed
            {
                self.quarantined = true;
                self.stop_requested = true;
            }
            self.provider_sequence += 1;
        }
        Ok(true)
    }
}
