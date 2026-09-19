use proptest::prelude::*;
use serde_json::Value;
use worldinterface_domain::*;
use worldinterface_protocol::{EntityId, Timestamp};
fn model() -> Value {
    serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../conformance/wi-fabric-2/domain/transitions.json"
        ))
        .unwrap(),
    )
    .unwrap()
}
fn decode<T: serde::de::DeserializeOwned>(s: &str) -> T {
    serde_json::from_value(Value::String(s.into())).unwrap()
}
fn timestamp() -> Timestamp {
    "2026-09-19T23:00:00.000000000Z".to_string().try_into().unwrap()
}
fn actions() -> Vec<Action> {
    "observe validate quarantine store enqueue send retry_delivery acknowledge application_accept application_reject submit resolve need_approval authorize deny dispatch start succeed fail_before_commit cancel_before_commit commit_unknown reconcile confirm_committed confirm_not_committed remain_uncertain escalate permit_retry draft_proof publish_proof fail_publication retry_publication".split_whitespace().map(decode).collect()
}
fn phases() -> Vec<Phase> {
    "empty observed validated quarantined stored pending in_flight acknowledged accepted rejected submitted resolving awaiting_approval authorized denied dispatch_pending running succeeded failed canceled uncertain reconciling reconciled confirmed_committed confirmed_not_committed still_uncertain escalated draft published publication_failed".split_whitespace().map(decode).collect()
}
fn model_next(m: &Value, machine: Machine, phase: Phase, action: Action) -> Option<Phase> {
    let key = serde_json::to_value(machine).unwrap();
    m[key.as_str().unwrap()]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| {
            row[0] == serde_json::to_value(phase).unwrap()
                && row[1] == serde_json::to_value(action).unwrap()
        })
        .map(|row| serde_json::from_value(row[2].clone()).unwrap())
}
#[test]
fn exhaustive_legal_and_illegal_transition_matrix() {
    let m = model();
    for machine in [
        Machine::Observation,
        Machine::Delivery,
        Machine::Effect,
        Machine::Execution,
        Machine::Reconciliation,
        Machine::Proof,
    ] {
        for phase in phases() {
            for action in actions() {
                let id = EntityId::generate();
                let mut state = State::empty(id.clone(), machine);
                state.phase = phase;
                let event = Event { entity: id, machine, action, at: timestamp() };
                let result = evolve(&state, &event);
                assert_eq!(
                    result.as_ref().ok().map(|s| s.phase),
                    model_next(&m, machine, phase, action),
                    "{machine:?}/{phase:?}/{action:?}"
                );
                if let Ok(s) = result {
                    assert_eq!(s.revision, 1);
                    assert_eq!(s.last_at, Some(timestamp()));
                }
            }
        }
    }
}
#[test]
fn generated_sequences_match_independent_model_and_replay() {
    let config =
        ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() };
    let mut runner = proptest::test_runner::TestRunner::new(config);
    runner
        .run(
            &(0usize..6, prop::collection::vec(0usize..128, 1..100)),
            |(machine_index, choices)| {
                let machine = [
                    Machine::Observation,
                    Machine::Delivery,
                    Machine::Effect,
                    Machine::Execution,
                    Machine::Reconciliation,
                    Machine::Proof,
                ][machine_index];
                let m = model();
                let id = EntityId::generate();
                let mut state = State::empty(id.clone(), machine);
                let mut history = vec![];
                let mut predicted = Phase::Empty;
                for choice in choices {
                    let all = actions();
                    let legal: Vec<_> = all
                        .iter()
                        .copied()
                        .filter(|a| model_next(&m, machine, predicted, *a).is_some())
                        .collect();
                    let action = if choice % 2 == 0 && !legal.is_empty() {
                        legal[choice % legal.len()]
                    } else {
                        all[choice % all.len()]
                    };
                    let event = Event { entity: id.clone(), machine, action, at: timestamp() };
                    let before = state.clone();
                    match model_next(&m, machine, predicted, action) {
                        Some(next) => {
                            state = evolve(&state, &event).unwrap();
                            predicted = next;
                            history.push(event);
                        }
                        None => {
                            prop_assert!(evolve(&state, &event).is_err());
                            prop_assert_eq!(&state, &before);
                        }
                    }
                    prop_assert_eq!(state.phase, predicted);
                    prop_assert_eq!(&replay(id.clone(), machine, &history).unwrap(), &state);
                }
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn identity_clock_and_revision_are_explicit_inputs() {
    let id = EntityId::generate();
    let mut s = State::empty(id.clone(), Machine::Effect);
    let mut e = Event {
        entity: EntityId::generate(),
        machine: Machine::Effect,
        action: Action::Submit,
        at: timestamp(),
    };
    assert_eq!(evolve(&s, &e), Err(TransitionError::WrongEntity));
    e.entity = id;
    s.last_at = Some("2026-09-20T23:00:00.000000000Z".to_string().try_into().unwrap());
    assert_eq!(evolve(&s, &e), Err(TransitionError::TimeReversed));
    s.last_at = None;
    s.revision = u64::MAX;
    assert_eq!(evolve(&s, &e), Err(TransitionError::RevisionOverflow));
}
#[test]
fn queue_ports_have_only_test_composition() {
    use worldinterface_domain::ports::*;
    use worldinterface_protocol::{ContentHash, OpaqueRef};
    struct Mock {
        request: Option<QueueAdmission>,
    }
    impl QueueAdmissionPort for Mock {
        fn ensure(&mut self, r: &QueueAdmission) -> Result<AdmissionResult, String> {
            if let Some(old) = &self.request {
                return Ok(if old == r {
                    AdmissionResult::AlreadyExists(r.task.clone())
                } else {
                    AdmissionResult::Conflict
                });
            }
            self.request = Some(r.clone());
            Ok(AdmissionResult::Created(r.task.clone()))
        }
    }
    impl QueueInspectionPort for Mock {
        fn inspect(&self, _: &OpaqueRef) -> Result<QueueStatus, String> {
            Ok(if self.request.is_some() { QueueStatus::Pending } else { QueueStatus::Missing })
        }
    }
    impl QueueWakePort for Mock {
        fn wake(&mut self, _: &OpaqueRef, _: &OpaqueRef) -> Result<(), String> {
            Ok(())
        }
    }
    let opaque = || "opaque".to_string().try_into().unwrap();
    let r = QueueAdmission {
        task: opaque(),
        admission_key: opaque(),
        canonical_request: ContentHash::of(b"request"),
        payload_locator: opaque(),
    };
    let mut mock = Mock { request: None };
    assert!(matches!(mock.ensure(&r).unwrap(), AdmissionResult::Created(_)));
    assert!(matches!(mock.ensure(&r).unwrap(), AdmissionResult::AlreadyExists(_)));
    assert_eq!(mock.inspect(&r.task).unwrap(), QueueStatus::Pending);
    mock.wake(&r.admission_key, &r.payload_locator).unwrap();
}
