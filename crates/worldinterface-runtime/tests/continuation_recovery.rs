mod support {
    pub mod connectors;
    pub mod continuation_fixture;
    pub mod flow_fixture;
    pub mod spi_harness;
}
use actionqueue_core::{disposition::*, limits::SignalLimits};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use std::sync::atomic::{AtomicBool, Ordering};
use support::{
    connectors::*,
    continuation_fixture::*,
    flow_fixture::*,
    spi_harness::{host, Noop},
};
use worldinterface_domain::{continuation::*, flow::*};
use worldinterface_protocol::*;
use worldinterface_runtime::{
    aq_adapter::QueueEngine, continuation::*, inspection::ContinuationInspector,
};
use worldinterface_storage::{flow::*, StoreKind};

#[tokio::test]
async fn admitted_unacknowledged_wake_with_backlog_recovers_independently_of_scan() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    until_wait(&f, &mut q, &id).await;
    q.shutdown().unwrap();
    let event = emit(&f, 1);
    let mut backlog = 0;
    for i in 0..10000 {
        let e = DurableEvent::new(
            EventRoute {
                kind: EventKind::ProviderCallback,
                correlation: r("unrelated"),
                ..event_route(&f)
            },
            r(&format!("backlog-{i}")),
            1,
            ContentHash::of(b"backlog"),
            None,
        )
        .unwrap();
        if e.id < event {
            f.bundle.wi().record_event(e).unwrap();
            backlog += 1;
            if backlog == 300 {
                break;
            }
        }
    }
    assert_eq!(backlog, 300);
    let e = f.bundle.wi().authoritative_event(StoreKind::Fabric, &event).unwrap();
    let request = signal_request(&e).unwrap();
    f.bundle
        .wi()
        .prepare_signal_request(
            StoreKind::Fabric,
            event.clone(),
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap();
    let mut aq = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: f.bundle.aq_path(),
            ..Default::default()
        },
        Noop,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let sequence = aq.admit_signal(request).unwrap().sequence().get();
    aq.shutdown().unwrap(); // The cross-store cut: AQ committed, WI not acknowledged.
    assert!(f.bundle.wi().signal_obligation(StoreKind::Fabric, &event).unwrap().sequence.is_none());
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(
        f.bundle.wi().signal_obligation(StoreKind::Fabric, &event).unwrap().sequence,
        Some(sequence)
    );
    assert_redeliveries(&f, &q, &id);
    q.shutdown().unwrap();
    for kind in [StoreKind::Fabric, StoreKind::Flow] {
        f.bundle.wi().repository(kind).unwrap().rebuild().unwrap();
    }
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    q.shutdown().unwrap();
}

#[tokio::test]
async fn non_early_wait_rejects_preexisting_outbox_events_after_backpressure_and_rebuild() {
    for backpressured in [false, true] {
        let f = Fixture::new();
        let old = emit(&f, 1);
        let position = f.bundle.wi().event_position(StoreKind::Fabric, &old).unwrap();
        let evidence = Evidence(f.snapshot.clone());
        let service = f.service(&evidence);
        let d = definition(&f, false, SignalProgress::Once {}, None);
        service.activate_flow_version(&(), d.clone(), None).unwrap();
        let limits = if backpressured {
            SignalLimits { identities: 0, ..Default::default() }
        } else {
            SignalLimits::default()
        };
        let mut q =
            QueueEngine::bootstrap_with_signal_limits(&f.bundle, f.handler(), host(), limits)
                .unwrap();
        if backpressured {
            assert!(!q.drain_signals().unwrap().pending.is_empty());
        }
        let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
        until_wait(&f, &mut q, &id).await;
        assert!(f.run(&id).wait_cursors[&r("wait")].event_position >= position);
        q.shutdown().unwrap();
        for kind in [StoreKind::Fabric, StoreKind::Flow] {
            f.bundle.wi().repository(kind).unwrap().rebuild().unwrap();
        }
        assert_eq!(f.bundle.wi().event_position(StoreKind::Fabric, &old).unwrap(), position);
        let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        for _ in 0..50 {
            let _ = q.tick().await.unwrap();
            tokio::task::yield_now().await;
        }
        assert!(f.run(&id).results.is_empty());
        assert!(f.run(&id).outcome.is_none());
        let fresh = emit(&f, 2);
        f.drive(&mut q, &id).await;
        let run = f.run(&id);
        assert_eq!(run.outcome, Some(FlowOutcome::Succeeded));
        assert!(run.accepted_resumes.values().any(|a| a.event.as_ref() == Some(&fresh)));
        assert_redeliveries(&f, &q, &id);
        q.shutdown().unwrap();
    }
}

struct RevokeAfterAccept<H> {
    normal: H,
    revoked: H,
    coordinator: bool,
    cut: AtomicBool,
}
impl<H: ExecutorHandler> ExecutorHandler for RevokeAfterAccept<H> {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let p: serde_json::Value = serde_json::from_slice(&ctx.input.payload).unwrap();
        let target = p["node"].is_null() == self.coordinator && ctx.input.resume_context.is_some();
        if target && self.cut.load(Ordering::SeqCst) {
            return self.revoked.execute(ctx);
        }
        let d = self.normal.execute(ctx);
        if target
            && matches!(d.outcome(), DispositionOutcome::Complete)
            && !self.cut.swap(true, Ordering::SeqCst)
        {
            return AttemptDisposition::retryable_failure(
                actionqueue_core::bounded::BoundedError {
                    code: actionqueue_core::bounded::BoundedCode::new(
                        "test.accepted_before_aq_commit",
                    )
                    .unwrap(),
                    message: actionqueue_core::bounded::BoundedMessage::new(
                        "redeliver accepted context with revoked authority",
                    )
                    .unwrap(),
                },
            );
        }
        d
    }
}
#[tokio::test]
async fn accepted_child_and_coordinator_replay_after_authority_revocation() {
    for coordinator in [false, true] {
        let mut f = Fixture::new();
        let evidence = Evidence(f.snapshot.clone());
        let normal = f.handler();
        f.snapshot.bindings[0].revoked = true;
        let revoked = f.handler();
        f.snapshot.bindings[0].revoked = false;
        let service = f.service(&evidence);
        let d = definition(&f, true, SignalProgress::Once {}, None);
        service.activate_flow_version(&(), d.clone(), None).unwrap();
        let mut q = QueueEngine::bootstrap(
            &f.bundle,
            RevokeAfterAccept { normal, revoked, coordinator, cut: AtomicBool::new(false) },
            host(),
        )
        .unwrap();
        let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
        emit(&f, 1);
        // WI may Finish before AQ has committed the retried coordinator result.
        for _ in 0..1500 {
            let _ = q.tick().await.unwrap();
            let run = f.run(&id);
            if run.outcome == Some(FlowOutcome::Succeeded)
                && run.parent_run.as_ref().is_some_and(|r| {
                    q.engine().projection().get_run_state(&r.as_str().parse().unwrap())
                        == Some(&actionqueue_core::run::RunState::Completed)
                })
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let run = f.run(&id);
        assert_eq!(run.outcome, Some(FlowOutcome::Succeeded));
        for aq_run in run.bindings.values().map(|b| &b.run).chain(run.parent_run.iter()) {
            assert_eq!(
                q.engine().projection().get_run_state(&aq_run.as_str().parse().unwrap()),
                Some(&actionqueue_core::run::RunState::Completed)
            );
        }
        q.shutdown().unwrap();
        f.snapshot.bindings[0].revoked = true;
        let q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        assert_redeliveries(&f, &q, &id);
        q.shutdown().unwrap();
    }
}

#[tokio::test]
async fn unaccepted_coordinator_finish_requires_authority_after_child_acceptance() {
    let mut f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let normal = f.handler();
    f.snapshot.bindings[0].revoked = true;
    let revoked = f.handler();
    f.snapshot.bindings[0].revoked = false;
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(
        &f.bundle,
        RevokeAfterAccept { normal, revoked, coordinator: true, cut: AtomicBool::new(true) },
        host(),
    )
    .unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    emit(&f, 1);
    f.drive(&mut q, &id).await;
    let run = f.run(&id);
    assert_eq!(run.results[&r("wait")].disposition, NodeDisposition::Succeeded);
    assert_eq!(run.outcome, Some(FlowOutcome::Failed));
    assert!(!run.accepted_resumes.values().any(|a| Some(&a.run) == run.parent_run.as_ref()));
    q.shutdown().unwrap();
    f.bundle.wi().repository(StoreKind::Flow).unwrap().rebuild().unwrap();
    assert_eq!(f.run(&id), run);
}

#[tokio::test]
async fn continuation_inspection_authorizes_and_reports_pending_and_accepted_chain() {
    let f = Fixture::new();
    let event = emit(&f, 1);
    let policy = Policy(event_route(&f));
    let inspector =
        ContinuationInspector { root: f.bundle.wi(), authentication: &f.auth, policy: &policy };
    f.bundle
        .wi()
        .record_signal_rejection(StoreKind::Fabric, event.clone(), "backpressure".into())
        .unwrap();
    let pending = inspector.event(&(), StoreKind::Fabric, &event).unwrap();
    assert_eq!(pending.signal.rejection.as_deref(), Some("backpressure"));
    assert!(pending.waits.is_empty() && pending.accepted.is_empty());
    for auth in
        [Auth(r("foreign"), f.auth.1.clone()), Auth(f.auth.0.clone(), PrincipalId::generate())]
    {
        let denied =
            ContinuationInspector { root: f.bundle.wi(), authentication: &auth, policy: &policy };
        assert_eq!(
            denied.event(&(), StoreKind::Fabric, &event).unwrap_err().to_string(),
            "event disclosure denied"
        );
        assert_eq!(
            denied.event(&(), StoreKind::Fabric, &r("missing")).unwrap_err().to_string(),
            "event disclosure denied"
        );
    }
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    f.drive(&mut q, &id).await;
    let view = inspector.event(&(), StoreKind::Fabric, &event).unwrap();
    assert!(view.signal.sequence.is_some() && view.signal.rejection.is_none());
    assert_eq!(view.waits.len(), 1);
    assert_eq!(view.accepted.len(), 1);
    assert_eq!(view.accepted[0].event.as_ref(), Some(&event));
    q.shutdown().unwrap();
}

#[tokio::test]
async fn accept_resume_validates_exports_even_with_other_artifacts() {
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
    let foreign = f.bundle.wi().publish(r("foreign"), b"export").unwrap();
    for combined in [false, true] {
        let state = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap();
        let cmd = FlowCommand::AcceptResume {
            run: id.clone(),
            node: Some(r("wait")),
            acceptance: run.accepted_resumes.values().next().unwrap().clone(),
            result: combined.then(|| run.results[&r("wait")].clone()),
            next_wait: combined.then(|| Box::new(run.waits[&r("wait")].clone())),
            finish: Some((
                FlowOutcome::Succeeded,
                std::collections::BTreeMap::from([(r("export"), foreign.clone())]),
            )),
        };
        assert_eq!(
            f.bundle.wi().flow_update(&f.auth.0, state.revision, cmd).unwrap_err().to_string(),
            "foreign Flow artifact"
        );
    }
    q.shutdown().unwrap();
}

#[test]
fn signal_capacity_alarm_uses_resident_statistics_across_restart() {
    let f = Fixture::new();
    let limits = SignalLimits { identities: 2, ..Default::default() };
    let mut q =
        QueueEngine::bootstrap_with_signal_limits(&f.bundle, f.handler(), host(), limits).unwrap();
    assert!(!q.signal_capacity().alarm);
    emit(&f, 1);
    q.drain_signals().unwrap();
    assert!(!q.signal_capacity().alarm);
    emit(&f, 2);
    q.drain_signals().unwrap();
    let full = q.signal_capacity();
    assert!(full.alarm && full.resident_bytes > 0);
    assert_eq!(full.resident_identities, 2);
    q.shutdown().unwrap();
    let q =
        QueueEngine::bootstrap_with_signal_limits(&f.bundle, f.handler(), host(), limits).unwrap();
    assert_eq!(q.signal_capacity(), full);
    q.shutdown().unwrap();
}

struct HoldFinish<H> {
    inner: H,
    hold: bool,
}
impl<H: ExecutorHandler> ExecutorHandler for HoldFinish<H> {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let p: serde_json::Value = serde_json::from_slice(&ctx.input.payload).unwrap();
        if self.hold && p["node"].is_null() && ctx.input.resume_context.is_some() {
            return AttemptDisposition::suspended(None, None);
        }
        self.inner.execute(ctx)
    }
}
#[tokio::test]
async fn signal_cancel_acceptance_and_deadline_winners_replay_across_paired_reopen() {
    for order in
        ["cancel-signal", "signal-cancel", "accepted-cancel", "deadline-signal", "signal-deadline"]
    {
        let mut f = Fixture::new();
        let evidence = Evidence(f.snapshot.clone());
        let service = f.service(&evidence);
        let deadline = order.contains("deadline").then(|| Deadline {
            inclusive_at: "2000-01-01T00:00:00.000000000Z".to_string().try_into().unwrap(),
            action: TimeoutAction::Fail,
        });
        let d = definition(&f, true, SignalProgress::Once {}, deadline);
        service.activate_flow_version(&(), d.clone(), None).unwrap();
        let mut q = QueueEngine::bootstrap(
            &f.bundle,
            HoldFinish { inner: f.handler(), hold: order == "accepted-cancel" },
            host(),
        )
        .unwrap();
        if order == "signal-deadline" {
            emit(&f, 1);
            q.drain_signals().unwrap();
        }
        let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
        if order.contains("deadline") {
            f.drive(&mut q, &id).await;
            assert_eq!(
                f.run(&id).outcome,
                Some(if order == "signal-deadline" {
                    FlowOutcome::Succeeded
                } else {
                    FlowOutcome::Failed
                })
            );
            emit(&f, 2);
        } else {
            until_wait(&f, &mut q, &id).await;
            if order != "cancel-signal" {
                emit(&f, 1);
                q.drain_signals().unwrap();
            }
            if order == "accepted-cancel" {
                for _ in 0..1000 {
                    let _ = q.tick().await.unwrap();
                    if !f.run(&id).results.is_empty() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                assert_eq!(f.run(&id).results[&r("wait")].disposition, NodeDisposition::Succeeded);
            }
            q.cancel_task(f.run(&id).coordinator_task.as_str().parse().unwrap()).unwrap();
            if order == "cancel-signal" {
                emit(&f, 1);
            }
            f.drive(&mut q, &id).await;
            assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Canceled));
        }
        q.drain_signals().unwrap();
        let before = f.run(&id);
        q.shutdown().unwrap();
        f.bundle = std::sync::Arc::new(
            worldinterface_runtime::storage::StoreBundle::open(&f._temp.path().join("bundle"))
                .unwrap(),
        );
        for kind in [StoreKind::Fabric, StoreKind::Flow] {
            f.bundle.wi().repository(kind).unwrap().rebuild().unwrap();
        }
        let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        for _ in 0..5 {
            let _ = q.tick().await.unwrap();
        }
        assert_redeliveries(&f, &q, &id);
        assert_eq!(f.run(&id), before, "{order}");
        q.shutdown().unwrap();
    }
}
