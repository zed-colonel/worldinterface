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

// Observe inside the executor, before control returns to tick's next outbox
// drain. This distinguishes handler repair from eventual scan acknowledgment.
struct CheckWakeAcknowledgment<H> {
    inner: H,
    bundle: std::sync::Arc<worldinterface_runtime::storage::StoreBundle>,
    event: OpaqueRef,
    sequence: u64,
    observed: std::sync::Arc<AtomicBool>,
}
impl<H: ExecutorHandler> ExecutorHandler for CheckWakeAcknowledgment<H> {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let payload: serde_json::Value = serde_json::from_slice(&ctx.input.payload).unwrap();
        let target = payload["node"] == "wait" && ctx.input.resume_context.is_some();
        if target {
            assert!(
                self.bundle
                    .wi()
                    .signal_obligation(StoreKind::Fabric, &self.event)
                    .unwrap()
                    .sequence
                    .is_none(),
                "scan reached the target before the handler"
            );
        }
        let disposition = self.inner.execute(ctx);
        if target {
            assert!(matches!(disposition.outcome(), DispositionOutcome::Complete));
            assert_eq!(
                self.bundle
                    .wi()
                    .signal_obligation(StoreKind::Fabric, &self.event)
                    .unwrap()
                    .sequence,
                Some(self.sequence),
                "wake handler must repair acknowledgment"
            );
            self.observed.store(true, Ordering::SeqCst);
        }
        disposition
    }
}

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
    let observed = std::sync::Arc::new(AtomicBool::new(false));
    let handler = CheckWakeAcknowledgment {
        inner: f.handler(),
        bundle: f.bundle.clone(),
        event: event.clone(),
        sequence,
        observed: observed.clone(),
    };
    let mut q = QueueEngine::bootstrap(&f.bundle, handler, host()).unwrap();
    f.drive(&mut q, &id).await;
    assert!(observed.load(Ordering::SeqCst));
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

    // Event access alone must not disclose another principal's run. Exercise
    // the trait default, independently of the fixture's event audience check
    // and the explicit operator override below (F-017).
    struct OpenEventReads;
    impl EventAuthorization for OpenEventReads {
        fn authorize(
            &self,
            _: &worldinterface_domain::agency::AuthenticatedCaller,
            _: EventKind,
            _: &OpaqueRef,
        ) -> worldinterface_runtime::Result<EventRoute> {
            unreachable!("inspection never authorizes event production")
        }
        fn authorize_read(
            &self,
            _: &worldinterface_domain::agency::AuthenticatedCaller,
            _: &DurableEvent,
        ) -> bool {
            true
        }
    }
    let reader = Auth(f.auth.0.clone(), PrincipalId::generate());
    for (auth, own) in [(&f.auth, true), (&reader, false)] {
        let disclosed = ContinuationInspector {
            root: f.bundle.wi(),
            authentication: auth,
            policy: &OpenEventReads,
        }
        .event(&(), StoreKind::Fabric, &event)
        .unwrap();
        assert_eq!(disclosed.signal, view.signal);
        assert_eq!(disclosed.waits.len(), usize::from(own));
        assert_eq!(disclosed.accepted.len(), usize::from(own));
    }

    struct OperatorPolicy {
        event: Policy,
        run: Option<OpaqueRef>,
    }
    impl EventAuthorization for OperatorPolicy {
        fn authorize(
            &self,
            c: &worldinterface_domain::agency::AuthenticatedCaller,
            kind: EventKind,
            source: &OpaqueRef,
        ) -> worldinterface_runtime::Result<EventRoute> {
            self.event.authorize(c, kind, source)
        }
        fn authorize_read(
            &self,
            _: &worldinterface_domain::agency::AuthenticatedCaller,
            _: &DurableEvent,
        ) -> bool {
            true
        }
        fn authorize_run_read(
            &self,
            _: &worldinterface_domain::agency::AuthenticatedCaller,
            run: &FlowRun,
        ) -> bool {
            self.run.as_ref() == Some(&run.id)
        }
    }
    let operator = Auth(f.auth.0.clone(), PrincipalId::generate());
    for allowed in [false, true] {
        let policy =
            OperatorPolicy { event: Policy(event_route(&f)), run: allowed.then_some(id.clone()) };
        let inspector = ContinuationInspector {
            root: f.bundle.wi(),
            authentication: &operator,
            policy: &policy,
        };
        let operator_view = inspector.event(&(), StoreKind::Fabric, &event).unwrap();
        assert_eq!(operator_view.waits.len(), usize::from(allowed));
        assert_eq!(operator_view.accepted.len(), usize::from(allowed));
        if allowed {
            assert_eq!(operator_view.waits, view.waits);
            assert_eq!(operator_view.accepted, view.accepted);
        }
        // Even a permissive host policy cannot cross the authenticated tenant.
        let foreign = Auth(r("foreign"), operator.1.clone());
        assert!(ContinuationInspector {
            root: f.bundle.wi(),
            authentication: &foreign,
            policy: &policy
        }
        .event(&(), StoreKind::Fabric, &event)
        .is_err());
    }
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

#[tokio::test]
async fn journal_only_admission_failure_retries_on_an_otherwise_idle_live_engine() {
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    // Establish an idle watermark before AdmitRun. Removing only the WI
    // journal position must make the recovery assertion below fail (F-016).
    let _ = q.tick().await.unwrap();
    let aq_position = q.engine().projection().latest_sequence();
    let journal_position = f.bundle.wi().flow_journal_position().unwrap();
    // Temporarily unavailable host context rejects AQ admission before any
    // WAL write, after the WI admission/outbox has already committed.
    q.set_host_control_context(None);
    assert!(service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).is_err());
    let run = f
        .bundle
        .wi()
        .flow_request(&f.auth.0, &f.auth.1, &f.submission.client_request_id)
        .unwrap()
        .unwrap();
    let parent = run.coordinator_task.as_str().parse().unwrap();
    assert!(f.bundle.wi().flow_journal_position().unwrap() > journal_position);
    let committed_position = f.bundle.wi().flow_journal_position().unwrap();
    assert_eq!(q.engine().projection().latest_sequence(), aq_position);
    assert!(q.engine().projection().task_admission(parent).is_none());
    // Reconciliation itself fails during this tick too, leaving retries enabled,
    // rather than caching the new journal position after a transient failure.
    let _ = q.tick().await.unwrap();
    assert!(q.engine().projection().task_admission(parent).is_none());
    assert_eq!(q.engine().projection().latest_sequence(), aq_position);
    assert_eq!(f.bundle.wi().flow_journal_position().unwrap(), committed_position);
    assert!(!f.bundle.wi().flow_snapshot(&f.auth.0).unwrap().quarantined.contains_key(&run.id));
    q.set_host_control_context(Some(host()));
    // No resubmit, signal, other admission or reopen can invalidate the
    // watermark: the following tick alone must finish the durable admission.
    let _ = q.tick().await.unwrap();
    let admitted = q.engine().projection().task_admission(parent).expect("admission retried");
    let expected: actionqueue_core::admission::EnsureTaskRequest =
        serde_json::from_slice(&run.coordinator_request).unwrap();
    assert_eq!(admitted.digest(), &expected.digest().unwrap());
    until_wait(&f, &mut q, &run.id).await;
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

/// F-012: idle scheduling must not repeat a full reconciliation per waiting
/// Flow. The 250 ms average budget leaves ample headroom over an empty tick
/// while catching the reviewed 500+ ms cost at 100 waits. The active phase
/// qualifies the documented supported waiting workload (F-018).
#[tokio::test]
async fn idle_ticks_skip_unchanged_waiting_flows_and_reconcile_changes_after_restart() {
    use actionqueue_core::run::RunState;
    use std::time::{Duration, Instant};
    let mut f = Fixture::new();
    let submissions: Vec<_> = (0..101)
        .map(|i| {
            let mut s = f.submission.clone();
            s.client_request_id = format!("idle-scale:{i}").try_into().unwrap();
            s
        })
        .collect();
    allow_demands(&mut f, &submissions);
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let ids: Vec<_> = submissions[..100]
        .iter()
        .map(|s| service.submit_flow(&(), &d.id, s.clone(), &mut q).unwrap())
        .collect();
    for _ in 0..1500 {
        let _ = q.tick().await.unwrap();
        let state = f.bundle.wi().flow_pending_snapshot(&f.auth.0).unwrap();
        if state.runs.len() == 100
            && state.runs.values().all(|r| {
                r.waits.len() == 1
                    && r.bindings.values().all(|b| {
                        q.engine().projection().get_run_state(&b.run.as_str().parse().unwrap())
                            == Some(&RunState::Awaiting)
                    })
            })
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        q.engine().projection().run_instances().filter(|r| r.state() == RunState::Awaiting).count(),
        200
    );
    // Complete the final disposable watermark pass before measurement.
    for _ in 0..2 {
        let _ = q.tick().await.unwrap();
    }
    let started = Instant::now();
    for _ in 0..8 {
        let tick = q.tick().await.unwrap();
        assert_eq!(tick.dispatched, 0);
    }
    let average = started.elapsed() / 8;
    eprintln!("100 waiting Flows: average idle tick {average:?}");
    assert!(average < Duration::from_millis(250), "idle tick: {average:?}");

    // Each unrelated source/outbox commit advances AQ without satisfying any
    // Flow. Unlike idle ticks, every measured tick must reconcile all waits.
    let mut traffic_route = event_route(&f);
    traffic_route.correlation = r("scale-traffic");
    let traffic_policy = Policy(traffic_route);
    let intake =
        EventIntake { root: f.bundle.wi(), authentication: &f.auth, policy: &traffic_policy };
    let mut active = Vec::new();
    for sequence in 1..=8 {
        let event = intake
            .worker_observation(
                &(),
                worldinterface_worker::ports::Observation {
                    provider: "scale-traffic".into(),
                    run: "unrelated".into(),
                    event: format!("traffic-{sequence}"),
                    sequence,
                    kind: worldinterface_worker::ports::ObservationKind::Exited,
                },
            )
            .unwrap();
        let started = Instant::now();
        assert_eq!(q.tick().await.unwrap().dispatched, 0);
        active.push(started.elapsed());
        assert!(f
            .bundle
            .wi()
            .signal_obligation(StoreKind::Fabric, &event)
            .unwrap()
            .sequence
            .is_some());
        assert_eq!(
            q.engine()
                .projection()
                .run_instances()
                .filter(|r| r.state() == RunState::Awaiting)
                .count(),
            200
        );
    }
    let average = active.iter().sum::<Duration>() / active.len() as u32;
    let maximum = *active.iter().max().unwrap();
    eprintln!(
        "100 waiting Flows under unrelated traffic: average tick {average:?}, maximum {maximum:?}"
    );
    assert!(average < Duration::from_millis(500), "active average tick: {average:?}");
    assert!(maximum < Duration::from_secs(2), "active maximum tick: {maximum:?}");

    // WI and AQ mutations both invalidate the disposable watermark. A newly
    // admitted unrelated Flow must progress while all 100 waits stay unresolved.
    let mut pure = d.clone();
    pure.id = r("idle-unrelated");
    pure.body.nodes = vec![pure_node("value", vec![], true.into(), "bool")];
    pure.body.entries = vec![r("value")];
    service.activate_flow_version(&(), pure.clone(), None).unwrap();
    let unrelated = service.submit_flow(&(), &pure.id, submissions[100].clone(), &mut q).unwrap();
    f.drive(&mut q, &unrelated).await;
    assert_eq!(f.run(&unrelated).outcome, Some(FlowOutcome::Succeeded));
    let canceled = ids.last().unwrap();
    q.cancel_task(f.run(canceled).coordinator_task.as_str().parse().unwrap()).unwrap();
    f.drive(&mut q, canceled).await;
    assert_eq!(f.run(canceled).outcome, Some(FlowOutcome::Canceled));
    q.shutdown().unwrap();

    // No local watermark survives reopen. AQ-owned waits still wake from their
    // authoritative records, with the canceled run retaining its outcome.
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    emit(&f, 1);
    f.drive(&mut q, &ids[0]).await;
    assert_eq!(f.run(&ids[0]).outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(f.run(canceled).outcome, Some(FlowOutcome::Canceled));
    q.shutdown().unwrap();
}
