//! Internal pure domain foundation. No composition root or production AQ adapter.
pub mod developmental;
pub mod ports;
pub mod publication;
pub mod receipt;
use serde::{Deserialize, Serialize};
use worldinterface_protocol::{EntityId, Timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Machine {
    Observation,
    Delivery,
    Effect,
    Execution,
    Reconciliation,
    Proof,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Empty,
    Observed,
    Validated,
    Quarantined,
    Stored,
    Pending,
    InFlight,
    Acknowledged,
    Accepted,
    Rejected,
    Submitted,
    Resolving,
    AwaitingApproval,
    Authorized,
    Denied,
    DispatchPending,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Uncertain,
    Reconciling,
    Reconciled,
    ConfirmedCommitted,
    ConfirmedNotCommitted,
    StillUncertain,
    Escalated,
    Draft,
    Published,
    PublicationFailed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Observe,
    Validate,
    Quarantine,
    Store,
    Enqueue,
    Send,
    RetryDelivery,
    Acknowledge,
    ApplicationAccept,
    ApplicationReject,
    Submit,
    Resolve,
    NeedApproval,
    Authorize,
    Deny,
    Dispatch,
    Start,
    Succeed,
    FailBeforeCommit,
    CancelBeforeCommit,
    CommitUnknown,
    Reconcile,
    ConfirmCommitted,
    ConfirmNotCommitted,
    RemainUncertain,
    Escalate,
    PermitRetry,
    DraftProof,
    PublishProof,
    FailPublication,
    RetryPublication,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub entity: EntityId,
    pub machine: Machine,
    pub phase: Phase,
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_at: Option<Timestamp>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub entity: EntityId,
    pub machine: Machine,
    pub action: Action,
    pub at: Timestamp,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransitionError {
    WrongEntity,
    TimeReversed,
    IllegalTransition,
    RevisionOverflow,
}
impl State {
    pub fn empty(entity: EntityId, machine: Machine) -> Self {
        Self { entity, machine, phase: Phase::Empty, revision: 0, last_at: None }
    }
}
pub fn evolve(state: &State, event: &Event) -> Result<State, TransitionError> {
    if state.entity != event.entity || state.machine != event.machine {
        return Err(TransitionError::WrongEntity);
    }
    if state.last_at.as_ref().is_some_and(|t| t > &event.at) {
        return Err(TransitionError::TimeReversed);
    }
    use Action::*;
    use Machine::*;
    use Phase as P;
    let phase = match (state.machine, state.phase, event.action) {
        (Observation, P::Empty, Observe) => P::Observed,
        (Observation, P::Observed, Validate) => P::Validated,
        (Observation, P::Observed, Quarantine) => P::Quarantined,
        (Observation, P::Validated, Store) => P::Stored,
        (Delivery, P::Empty, Enqueue) => P::Pending,
        (Delivery, P::Pending, Send) => P::InFlight,
        (Delivery, P::InFlight, RetryDelivery) => P::Pending,
        (Delivery, P::InFlight, Acknowledge) => P::Acknowledged,
        (Delivery, P::Acknowledged, ApplicationAccept) => P::Accepted,
        (Delivery, P::Acknowledged, ApplicationReject) => P::Rejected,
        (Effect, P::Empty, Submit) => P::Submitted,
        (Effect, P::Submitted, Resolve) => P::Resolving,
        (Effect, P::Resolving, NeedApproval) => P::AwaitingApproval,
        (Effect, P::Resolving | P::AwaitingApproval, Authorize) => P::Authorized,
        (Effect, P::Resolving | P::AwaitingApproval, Deny) => P::Denied,
        (Effect, P::Authorized, Dispatch) => P::DispatchPending,
        (Effect, P::DispatchPending, Start) => P::Running,
        (Effect | Execution, P::Running, Succeed) => P::Succeeded,
        (Effect | Execution, P::Running, FailBeforeCommit) => P::Failed,
        (
            Effect,
            P::Submitted | P::Resolving | P::AwaitingApproval | P::Authorized | P::DispatchPending,
            CancelBeforeCommit,
        ) => P::Canceled,
        (Execution, P::Empty, Start) => P::Running,
        (Execution, P::Running, CancelBeforeCommit) => P::Canceled,
        (Effect | Execution, P::Running, CommitUnknown) => P::Uncertain,
        (Effect, P::Uncertain, Reconcile) => P::Reconciling,
        (Effect, P::Reconciling, ConfirmCommitted | ConfirmNotCommitted) => P::Reconciled,
        (Effect, P::Reconciling, RemainUncertain | Escalate) => P::Uncertain,
        (Reconciliation, P::Empty, CommitUnknown) => P::Uncertain,
        (Reconciliation, P::Uncertain | P::StillUncertain, Reconcile) => P::Reconciling,
        (Reconciliation, P::Reconciling, ConfirmCommitted) => P::ConfirmedCommitted,
        (Reconciliation, P::Reconciling, ConfirmNotCommitted) => P::ConfirmedNotCommitted,
        (Reconciliation, P::Reconciling, RemainUncertain) => P::StillUncertain,
        (Reconciliation, P::Reconciling, Escalate) => P::Escalated,
        (Reconciliation, P::ConfirmedNotCommitted, PermitRetry) => P::Pending,
        (Proof, P::Empty, DraftProof) => P::Draft,
        (Proof, P::Draft, PublishProof) => P::Published,
        (Proof, P::Draft, FailPublication) => P::PublicationFailed,
        (Proof, P::PublicationFailed, RetryPublication) => P::Draft,
        _ => return Err(TransitionError::IllegalTransition),
    };
    Ok(State {
        phase,
        revision: state.revision.checked_add(1).ok_or(TransitionError::RevisionOverflow)?,
        last_at: Some(event.at.clone()),
        ..state.clone()
    })
}
pub fn replay(
    entity: EntityId,
    machine: Machine,
    events: &[Event],
) -> Result<State, TransitionError> {
    events.iter().try_fold(State::empty(entity, machine), |s, e| evolve(&s, e))
}

pub mod agency;
pub mod authorization;
pub mod effect;
pub mod ingress;
pub mod resource;

pub mod reconciliation;

pub mod flow;

pub mod continuation;

pub mod worker;
