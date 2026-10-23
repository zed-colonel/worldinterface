#![allow(dead_code, unused_imports)]
use super::{connectors::*, flow_fixture::*};
use actionqueue_core::run::RunState;
use std::collections::BTreeMap;
use worldinterface_domain::{agency::AuthenticatedCaller, continuation::*, flow::*};
use worldinterface_protocol::*;
use worldinterface_runtime::{aq_adapter::QueueEngine, continuation::*, Result};
use worldinterface_storage::{flow::*, StoreKind};

pub(crate) fn r(s: &str) -> OpaqueRef {
    s.to_string().try_into().unwrap()
}
pub(crate) struct Policy(pub EventRoute);
impl EventAuthorization for Policy {
    fn authorize(
        &self,
        c: &AuthenticatedCaller,
        kind: EventKind,
        _: &OpaqueRef,
    ) -> Result<EventRoute> {
        if c.tenant() != &self.0.tenant || kind != self.0.kind {
            return Err("event authority denied".into());
        }
        Ok(self.0.clone())
    }
    fn authorize_read(&self, c: &AuthenticatedCaller, e: &DurableEvent) -> bool {
        c.tenant() == &e.route.tenant && c.principal() == &e.route.audience
    }
}
pub(crate) fn event_route(f: &Fixture) -> EventRoute {
    EventRoute {
        tenant: f.auth.0.clone(),
        audience: f.auth.1.clone(),
        kind: EventKind::WorkerObservation,
        correlation: r("worker-stream"),
        generation: None,
    }
}
pub(crate) fn emit(f: &Fixture, sequence: u64) -> OpaqueRef {
    EventIntake { root: f.bundle.wi(), authentication: &f.auth, policy: &Policy(event_route(f)) }
        .worker_observation(
            &(),
            worldinterface_worker::ports::Observation {
                provider: "worker-fixture".into(),
                run: "worker-run".into(),
                event: format!("event-{sequence}"),
                sequence,
                kind: worldinterface_worker::ports::ObservationKind::Exited,
            },
        )
        .unwrap()
}
pub(crate) fn definition(
    f: &Fixture,
    accept_early: bool,
    progress: SignalProgress,
    deadline: Option<Deadline>,
) -> FlowDefinition {
    let mut n = pure_node("wait", vec![], true.into(), "bool");
    n.outputs.clear();
    n.operation = PlanOperation::Wait {
        signal: SignalWait {
            tenant: f.auth.0.clone(),
            namespace: r(NAMESPACE),
            signal_kind: r("worker_observation"),
            correlation: r("worker-stream"),
            accept_early,
            progress,
            deadline,
        },
    };
    FlowDefinition {
        id: r("continuation"),
        version: 1,
        predecessor: None,
        body: FlowBody {
            entries: vec![n.id.clone()],
            nodes: vec![n],
            outputs: BTreeMap::new(),
            authority: AuthorityUpperBound::default(),
            causal_metadata: BTreeMap::new(),
        },
    }
}
pub(crate) async fn until_wait(
    f: &Fixture,
    q: &mut QueueEngine<
        '_,
        worldinterface_runtime::flow_node::FlowAttemptHandler<
            DeterministicConnector,
            Evidence,
            Resolver,
            Clock,
        >,
    >,
    id: &OpaqueRef,
) {
    for _ in 0..1000 {
        let _ = q.tick().await.unwrap();
        let run = f.run(id);
        if run.waits.contains_key(&r("wait"))
            && q.engine()
                .projection()
                .runs_for_task(run.node_tasks[&r("wait")].as_str().parse().unwrap())
                .any(|v| v.state() == RunState::Awaiting)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    panic!("wait did not establish: {:?}", f.run(id));
}

pub(crate) fn assert_redeliveries(
    f: &Fixture,
    q: &QueueEngine<
        '_,
        worldinterface_runtime::flow_node::FlowAttemptHandler<
            DeterministicConnector,
            Evidence,
            Resolver,
            Clock,
        >,
    >,
    id: &OpaqueRef,
) {
    use actionqueue_executor_local::handler::{
        AttemptMetadata, CancellationContext, ExecutorContext, HandlerInput,
    };
    let run = f.run(id);
    let handler = f.handler();
    for a in run.accepted_resumes.values() {
        let aq_run = a.run.as_str().parse().unwrap();
        let p = q.engine().projection();
        let context = p
            .get_attempt_history(&aq_run)
            .unwrap()
            .iter()
            .filter_map(|attempt| p.attempt_resume(aq_run, attempt.attempt_id()))
            .find(|c| c.context_id.0 == a.context_id)
            .unwrap();
        let request = if run.parent_run.as_ref() == Some(&a.run) {
            run.coordinator_request.clone()
        } else {
            f.bundle
                .wi()
                .read_artifact(&run.bindings.values().find(|b| b.run == a.run).unwrap().request)
                .unwrap()
        };
        let request: actionqueue_core::admission::EnsureTaskRequest =
            serde_json::from_slice(&request).unwrap();
        for number in [1, 2, 3] {
            let ctx = ExecutorContext {
                input: HandlerInput {
                    resume_context: Some(context.clone()),
                    causal_context: None,
                    run_id: aq_run,
                    attempt_id: actionqueue_core::ids::AttemptId::new(),
                    payload: request.task_spec().task_payload().bytes().to_vec(),
                    metadata: AttemptMetadata {
                        failure_attempt_count: number - 1,
                        max_attempts: 3,
                        attempt_number: number,
                        timeout_secs: None,
                        safety_level: actionqueue_core::task::safety::SafetyLevel::Idempotent,
                    },
                    cancellation_context: CancellationContext::new(),
                },
                children: None,
            };
            assert_eq!(
                serde_json::to_vec(&handler.execute_bound(&ctx).unwrap()).unwrap(),
                a.disposition
            );
        }
    }
    assert_eq!(f.run(id), run);
}
