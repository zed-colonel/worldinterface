mod support {
    pub mod connectors;
    pub mod continuation_fixture;
    pub mod flow_fixture;
    pub mod spi_harness;
}
use actionqueue_core::run::RunState;
use std::collections::BTreeMap;
use support::{connectors::*, flow_fixture::*, spi_harness::host};
use worldinterface_domain::{continuation::*, flow::*};
use worldinterface_runtime::{aq_adapter::QueueEngine, continuation::*};
use worldinterface_storage::{flow::*, StoreKind};

use support::continuation_fixture::*;
#[tokio::test]
async fn ab_wi_008_early_late_duplicate_and_fanout_use_durable_records() {
    for early in [true, false] {
        let f = Fixture::new();
        let evidence = Evidence(f.snapshot.clone());
        let service = f.service(&evidence);
        let d = definition(&f, true, SignalProgress::Once {}, None);
        service.activate_flow_version(&(), d.clone(), None).unwrap();
        let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        let first = if early {
            let id = emit(&f, 1);
            q.drain_signals().unwrap();
            Some(id)
        } else {
            None
        };
        let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
        if !early {
            until_wait(&f, &mut q, &id).await;
        }
        let event = emit(&f, 1);
        if let Some(first) = first {
            assert_eq!(first, event);
        }
        f.drive(&mut q, &id).await;
        let run = f.run(&id);
        assert_eq!(run.outcome, Some(FlowOutcome::Succeeded));
        assert_eq!(
            run.accepted_resumes.values().filter(|a| a.event.as_ref() == Some(&event)).count(),
            1
        );
        assert_eq!(f.connector.commits(), 0);
        let o = f.bundle.wi().signal_obligation(StoreKind::Fabric, &event).unwrap();
        assert_eq!(q.drain_signals().unwrap().acknowledged, 0);
        assert!(o.sequence.is_some());
        // A second authorized run sees the same retained event; signals are not consumed.
        let mut submission = f.submission.clone();
        submission.client_request_id = uuid::Uuid::new_v4().to_string().try_into().unwrap();
        let second = service.submit_flow(&(), &d.id, submission, &mut q).unwrap();
        f.drive(&mut q, &second).await;
        assert_eq!(f.run(&second).outcome, Some(FlowOutcome::Succeeded));
        q.shutdown().unwrap();
    }
}
#[tokio::test]
async fn ab_wi_011_awaiting_releases_attempt_and_unrelated_work_progresses() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    until_wait(&f, &mut q, &id).await;
    let run = f.run(&id);
    let child = q
        .engine()
        .projection()
        .runs_for_task(run.node_tasks[&r("wait")].as_str().parse().unwrap())
        .next()
        .unwrap();
    assert_eq!(child.state(), RunState::Awaiting);
    assert!(child.current_attempt_id().is_none());
    let mut submission = f.submission.clone();
    submission.client_request_id = uuid::Uuid::new_v4().to_string().try_into().unwrap();
    // A pure unrelated Flow completes while the signal wait remains unresolved.
    let node = pure_node("pure", vec![], true.into(), "bool");
    let pure = FlowDefinition {
        id: r("unrelated"),
        version: 1,
        predecessor: None,
        body: FlowBody {
            entries: vec![node.id.clone()],
            nodes: vec![node],
            outputs: BTreeMap::new(),
            authority: AuthorityUpperBound::default(),
            causal_metadata: BTreeMap::new(),
        },
    };
    service.activate_flow_version(&(), pure.clone(), None).unwrap();
    let other = service.submit_flow(&(), &pure.id, submission, &mut q).unwrap();
    f.drive(&mut q, &other).await;
    assert!(f.run(&id).outcome.is_none());
    emit(&f, 1);
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    q.shutdown().unwrap();
}
#[tokio::test]
async fn eligibility_uses_signal_high_water_and_after_handled_advances_actual_sequence() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut d = definition(&f, false, SignalProgress::Once {}, None);
    let mut second = d.body.nodes[0].clone();
    second.id = r("second");
    second.dependencies = vec![r("wait")];
    if let PlanOperation::Wait { signal } = &mut second.operation {
        signal.accept_early = true;
        signal.progress = SignalProgress::AfterHandled { wait: r("wait") };
    }
    d.body.nodes.push(second);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let old = emit(&f, 1);
    q.drain_signals().unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    until_wait(&f, &mut q, &id).await;
    let cursor = f.run(&id).waits[&r("wait")].after_sequence.unwrap();
    assert_eq!(
        cursor,
        f.bundle.wi().signal_obligation(StoreKind::Fabric, &old).unwrap().sequence.unwrap()
    );
    let first = emit(&f, 2);
    for _ in 0..1000 {
        let _ = q.tick().await.unwrap();
        if f.run(&id).waits.contains_key(&r("second")) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let run = f.run(&id);
    assert_eq!(run.results.len(), 1);
    assert_eq!(
        run.waits[&r("second")].after_sequence,
        f.bundle.wi().signal_obligation(StoreKind::Fabric, &first).unwrap().sequence
    );
    emit(&f, 3);
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).results.len(), 2);
    q.shutdown().unwrap();
}
#[tokio::test]
async fn outbox_backpressure_retains_identity_and_pending_obligations() {
    let f = Fixture::new();
    let event = emit(&f, 1);
    let limits = actionqueue_core::limits::SignalLimits { identities: 0, ..Default::default() };
    let mut q =
        QueueEngine::bootstrap_with_signal_limits(&f.bundle, f.handler(), host(), limits).unwrap();
    let status = q.drain_signals().unwrap();
    assert_eq!(status.pending, vec![(event.clone(), "backpressure".into())]);
    let frozen = f.bundle.wi().signal_obligation(StoreKind::Fabric, &event).unwrap();
    assert!(frozen.sequence.is_none());
    assert_eq!(q.drain_signals().unwrap(), status);
    q.shutdown().unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    q.drain_signals().unwrap();
    let delivered = f.bundle.wi().signal_obligation(StoreKind::Fabric, &event).unwrap();
    assert_eq!(delivered.request, frozen.request);
    assert!(delivered.sequence.is_some());
    q.shutdown().unwrap();
}
#[test]
fn worker_intake_conflicts_scope_and_sequence_are_durable() {
    let f = Fixture::new();
    let id = emit(&f, 1);
    assert_eq!(emit(&f, 1), id);
    let intake = EventIntake {
        root: f.bundle.wi(),
        authentication: &f.auth,
        policy: &Policy(event_route(&f)),
    };
    assert!(intake
        .worker_observation(
            &(),
            worldinterface_worker::ports::Observation {
                provider: "worker-fixture".into(),
                run: "worker-run".into(),
                event: "event-1".into(),
                sequence: 1,
                kind: worldinterface_worker::ports::ObservationKind::Running
            }
        )
        .is_err());
    let foreign = Auth(r("foreign"), f.auth.1.clone());
    assert!(EventIntake {
        root: f.bundle.wi(),
        authentication: &foreign,
        policy: &Policy(event_route(&f))
    }
    .worker_observation(
        &(),
        worldinterface_worker::ports::Observation {
            provider: "worker-fixture".into(),
            run: "worker-run".into(),
            event: "event-2".into(),
            sequence: 2,
            kind: worldinterface_worker::ports::ObservationKind::Exited
        }
    )
    .is_err());
    f.bundle.wi().repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(f.bundle.wi().pending_signals(StoreKind::Fabric, None, 10).unwrap().len(), 1);
    let pending = f.bundle.wi().signal_obligation(StoreKind::Fabric, &id).unwrap();
    let source = f._temp.path().join("bundle");
    let backup = f._temp.path().join("pending-backup");
    let restored = f._temp.path().join("pending-restored");
    drop(f.bundle);
    worldinterface_runtime::storage::StoreBundle::backup(&source, &backup).unwrap();
    worldinterface_runtime::storage::StoreBundle::restore(&backup, &restored).unwrap();
    let restored = worldinterface_runtime::storage::StoreBundle::open(&restored).unwrap();
    assert_eq!(restored.wi().signal_obligation(StoreKind::Fabric, &id).unwrap(), pending);
    restored.wi().repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(restored.wi().signal_obligation(StoreKind::Fabric, &id).unwrap(), pending);
}

#[tokio::test]
async fn signal_deadline_cancel_and_late_event_orders_preserve_committed_outcomes() {
    for (early, action, timer) in [
        (true, TimeoutAction::Fail, false),
        (false, TimeoutAction::Fail, false),
        (false, TimeoutAction::Continue, false),
        (false, TimeoutAction::Fail, true),
        (false, TimeoutAction::Continue, true),
    ] {
        let f = Fixture::new();
        let evidence = Evidence(f.snapshot.clone());
        let service = f.service(&evidence);
        let deadline = Deadline {
            inclusive_at: "2000-01-01T00:00:00.000000000Z".to_string().try_into().unwrap(),
            action: action.clone(),
        };
        let mut d = definition(&f, true, SignalProgress::Once {}, Some(deadline.clone()));
        if timer {
            d.body.nodes[0].operation = PlanOperation::Timer { deadline };
        }
        service.activate_flow_version(&(), d.clone(), None).unwrap();
        let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        if early {
            emit(&f, 1);
            q.drain_signals().unwrap();
        }
        let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
        f.drive(&mut q, &id).await;
        let before = f.run(&id);
        assert_eq!(
            before.outcome,
            Some(if early || action == TimeoutAction::Continue {
                FlowOutcome::Succeeded
            } else {
                FlowOutcome::Failed
            })
        );
        emit(&f, 2);
        q.drain_signals().unwrap();
        assert_eq!(f.run(&id), before);
        assert_redeliveries(&f, &q, &id);
        q.shutdown().unwrap();
    }
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    until_wait(&f, &mut q, &id).await;
    q.cancel_task(f.run(&id).coordinator_task.as_str().parse().unwrap()).unwrap();
    emit(&f, 1);
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Canceled));
    assert!(f.run(&id).results.is_empty());
    q.shutdown().unwrap();
}
#[tokio::test]
async fn early_control_wakes_reestablish_child_and_parent_waits() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    until_wait(&f, &mut q, &id).await;
    let run = f.run(&id);
    let child: actionqueue_core::ids::RunId =
        run.bindings[&r("wait")].run.as_str().parse().unwrap();
    let wait: actionqueue_core::continuation::WaitSpec =
        serde_json::from_slice(&run.waits[&r("wait")].wait).unwrap();
    q.resolve_wait(child, wait.wait_id()).unwrap();
    let parent: actionqueue_core::ids::RunId =
        run.parent_run.as_ref().unwrap().as_str().parse().unwrap();
    q.resolve_wait(parent, run.wait_id.as_str().parse().unwrap()).unwrap();
    for _ in 0..1000 {
        let _ = q.tick().await.unwrap();
        if f.run(&id).accepted_resumes.len() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let run = f.run(&id);
    assert!(run.results.is_empty());
    assert!(run.outcome.is_none());
    assert_eq!(run.accepted_resumes.len(), 2);
    emit(&f, 1);
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert_redeliveries(&f, &q, &id);
    q.shutdown().unwrap();
}
#[tokio::test]
async fn resume_redelivery_reproduces_result_and_conflicting_context_rejects() {
    use actionqueue_executor_local::handler::{
        AttemptMetadata, CancellationContext, ExecutorContext, HandlerInput,
    };
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    emit(&f, 1);
    f.drive(&mut q, &id).await;
    let run = f.run(&id);
    let child: actionqueue_core::ids::RunId =
        run.bindings[&r("wait")].run.as_str().parse().unwrap();
    let projection = q.engine().projection();
    let history = projection.get_attempt_history(&child).unwrap();
    let context = history
        .iter()
        .filter_map(|a| projection.attempt_resume(child, a.attempt_id()))
        .find(|c| matches!(c.wake, actionqueue_core::continuation::WakeReason::Signal { .. }))
        .unwrap();
    let request: actionqueue_core::admission::EnsureTaskRequest = serde_json::from_slice(
        &f.bundle.wi().read_artifact(&run.bindings[&r("wait")].request).unwrap(),
    )
    .unwrap();
    let mut ctx = ExecutorContext {
        input: HandlerInput {
            resume_context: Some(context),
            causal_context: None,
            run_id: child,
            attempt_id: actionqueue_core::ids::AttemptId::new(),
            payload: request.task_spec().task_payload().bytes().to_vec(),
            metadata: AttemptMetadata {
                failure_attempt_count: 0,
                max_attempts: 3,
                attempt_number: 1,
                timeout_secs: None,
                safety_level: actionqueue_core::task::safety::SafetyLevel::Idempotent,
            },
            cancellation_context: CancellationContext::new(),
        },
        children: None,
    };
    let before = f.run(&id);
    let handler = f.handler();
    for count in [1, 2, 3] {
        ctx.input.metadata.attempt_number = count;
        assert!(matches!(
            handler.execute_bound(&ctx).unwrap().outcome(),
            actionqueue_core::disposition::DispositionOutcome::Complete
        ));
    }
    assert_eq!(f.run(&id), before);
    ctx.input.resume_context.as_mut().unwrap().resumed_at += 1;
    assert!(handler.execute_bound(&ctx).is_err());
    assert_eq!(f.run(&id), before);
    q.shutdown().unwrap();
}
#[tokio::test]
async fn provider_callback_commits_an_authoritative_record_and_outbox_without_waiter() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let run = service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap();
    f.drive(&mut q, &run).await;
    let effect = f.run(&run).results.values().next().unwrap().effect.clone().unwrap();
    let policy = Policy(EventRoute {
        kind: EventKind::ProviderCallback,
        correlation: effect.as_str().to_string().try_into().unwrap(),
        ..event_route(&f)
    });
    let intake = EventIntake { root: f.bundle.wi(), authentication: &f.auth, policy: &policy };
    let artifact = f.bundle.wi().publish(f.auth.0.clone(), b"provider-result").unwrap();
    let id = intake.provider_callback(&(), &effect, 1, artifact.clone()).unwrap();
    assert_eq!(intake.provider_callback(&(), &effect, 1, artifact).unwrap(), id);
    assert!(f.bundle.wi().signal_obligation(StoreKind::Fabric, &id).unwrap().sequence.is_none());
    let changed = f.bundle.wi().publish(f.auth.0.clone(), b"conflicting-result").unwrap();
    assert!(intake.provider_callback(&(), &effect, 1, changed).is_err());
    q.drain_signals().unwrap();
    assert!(f.bundle.wi().signal_obligation(StoreKind::Fabric, &id).unwrap().sequence.is_some());
    q.shutdown().unwrap();
}

struct SuspendWait<H> {
    inner: H,
    once: std::sync::atomic::AtomicBool,
}
impl<H: actionqueue_executor_local::handler::ExecutorHandler>
    actionqueue_executor_local::handler::ExecutorHandler for SuspendWait<H>
{
    fn execute(
        &self,
        ctx: actionqueue_executor_local::handler::ExecutorContext,
    ) -> actionqueue_core::disposition::AttemptDisposition {
        let d = self.inner.execute(ctx);
        if d.wait().and_then(|w| w.filter()).is_some_and(|f| f.namespace.as_str() == NAMESPACE)
            && !self.once.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            actionqueue_core::disposition::AttemptDisposition::suspended(
                d.checkpoint().cloned(),
                None,
            )
        } else {
            d
        }
    }
}
#[tokio::test]
async fn administrative_resume_reestablishes_wait_and_all_wake_forms_redeliver_idempotently() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(
        &f.bundle,
        SuspendWait { inner: f.handler(), once: std::sync::atomic::AtomicBool::new(false) },
        host(),
    )
    .unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    let mut child = None;
    for _ in 0..1000 {
        let _ = q.tick().await.unwrap();
        let run = f.run(&id);
        if let Some(binding) = run.bindings.get(&r("wait")) {
            let aq_run = binding.run.as_str().parse().unwrap();
            if q.engine().projection().get_run_state(&aq_run) == Some(&RunState::Suspended) {
                child = Some(aq_run);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    q.resume_run(child.expect("suspended child")).unwrap();
    for _ in 0..1000 {
        let _ = q.tick().await.unwrap();
        if f.run(&id).accepted_resumes.len() == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(f.run(&id).results.is_empty());
    q.shutdown().unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    emit(&f, 1);
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert_redeliveries(&f, &q, &id);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn current_authority_and_scoped_routes_are_required_at_every_wake() {
    let mut f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let d = definition(&f, true, SignalProgress::Once {}, None);
    let service = f.service(&evidence);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    until_wait(&f, &mut q, &id).await;
    for other_tenant in [false, true] {
        let mut route = event_route(&f);
        if other_tenant {
            route.tenant = r("foreign")
        } else {
            route.audience = worldinterface_protocol::PrincipalId::generate()
        };
        let event = DurableEvent::new(
            route,
            r("forged-reference"),
            1,
            worldinterface_protocol::ContentHash::of(b"foreign"),
            None,
        )
        .unwrap();
        f.bundle.wi().record_event(event).unwrap();
        q.drain_signals().unwrap();
    }
    for _ in 0..5 {
        let _ = q.tick().await.unwrap();
    }
    assert!(f.run(&id).results.is_empty());
    assert!(f.run(&id).accepted_resumes.is_empty());
    q.shutdown().unwrap();
    f.snapshot.bindings[0].revoked = true;
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    emit(&f, 1);
    f.drive(&mut q, &id).await;
    assert_ne!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert!(f.run(&id).results.is_empty());
    q.shutdown().unwrap();
}
#[tokio::test]
async fn altered_checkpoint_cannot_authorize_wait_progress() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    until_wait(&f, &mut q, &id).await;
    let state = f.bundle.wi().flow_run_snapshot(&f.auth.0, &id).unwrap();
    let w = &state.runs[&id].waits[&r("wait")];
    let mut checkpoint: actionqueue_core::continuation::CheckpointRef =
        serde_json::from_slice(&w.checkpoint).unwrap();
    checkpoint.checkpoint_id = actionqueue_core::ids::CheckpointId::new();
    f.bundle
        .wi()
        .flow_update(
            &f.auth.0,
            state.revision,
            worldinterface_storage::flow::FlowCommand::RebindCheckpoint {
                run: id.clone(),
                node: r("wait"),
                expected: w.checkpoint.clone(),
                checkpoint: serde_json::to_vec(&checkpoint).unwrap(),
            },
        )
        .unwrap();
    // The authoritative WI checkpoint change cannot rewrite the already established AQ wait.
    emit(&f, 1);
    for _ in 0..100 {
        let _ = q.tick().await.unwrap();
        let state = f.bundle.wi().flow_run_snapshot(&f.auth.0, &id).unwrap();
        if state.quarantined.contains_key(&id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let state = f.bundle.wi().flow_run_snapshot(&f.auth.0, &id).unwrap();
    assert!(state.quarantined.contains_key(&id));
    assert!(state.runs[&id].results.is_empty());
    assert!(state.runs[&id].accepted_resumes.is_empty());
    assert_ne!(state.runs[&id].outcome, Some(FlowOutcome::Succeeded));
    q.shutdown().unwrap();
}

#[tokio::test]
async fn fresh_generation_is_scoped_and_inherited_through_after_handled() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut d = definition(&f, true, SignalProgress::FreshGeneration { generation: r("g1") }, None);
    let mut second = d.body.nodes[0].clone();
    second.id = r("second");
    second.dependencies = vec![r("wait")];
    if let PlanOperation::Wait { signal } = &mut second.operation {
        signal.progress = SignalProgress::AfterHandled { wait: r("wait") };
    }
    let mut third = d.body.nodes[0].clone();
    third.id = r("third");
    third.dependencies = vec![r("second")];
    if let PlanOperation::Wait { signal } = &mut third.operation {
        signal.progress = SignalProgress::FreshGeneration { generation: r("g2") };
    }
    d.body.nodes.extend([second, third]);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    for (generation, seq) in [("g1", 1), ("g1", 2), ("g2", 1)] {
        let route = EventRoute { generation: Some(r(generation)), ..event_route(&f) };
        EventIntake { root: f.bundle.wi(), authentication: &f.auth, policy: &Policy(route) }
            .worker_observation(
                &(),
                worldinterface_worker::ports::Observation {
                    provider: "fixture".into(),
                    run: "run".into(),
                    event: format!("event-{seq}"),
                    sequence: seq,
                    kind: worldinterface_worker::ports::ObservationKind::Running,
                },
            )
            .unwrap();
    }
    q.drain_signals().unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    f.drive(&mut q, &id).await;
    let run = f.run(&id);
    assert_eq!(run.outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(run.waits[&r("second")].route.as_ref().unwrap().generation, Some(r("g1")));
    assert_eq!(run.waits[&r("third")].route.as_ref().unwrap().generation, Some(r("g2")));
    q.shutdown().unwrap();
}
