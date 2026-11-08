mod support {
    pub mod connectors;
    pub mod spi_harness;
}
use support::{connectors::*, spi_harness::*};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::VerifiedSnapshot,
    effect::EffectAction,
    reconciliation::{CaseAction, CaseStatus, VerifiedCaseAction},
    Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::*;
use worldinterface_runtime::{authority::TrustedClock, storage::StoreBundle};

#[test]
fn connector_bound_effects_reject_nonproduction_preparation() {
    let h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Unknown);
    let state = h.bundle.wi().effect_snapshot(h.caller().tenant()).unwrap();
    let error = worldinterface_domain::effect::validate_command(
        &state,
        &h.caller(),
        h.clock.now().unwrap(),
        EffectAction::Prepare {
            id: h.id.clone(),
            snapshot: VerifiedSnapshot::from_trusted_verifier(h.snapshot.clone()).unwrap(),
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("require production admission"));
}

#[tokio::test]
async fn reconciliation_waits_when_dispatched_before_acknowledgment() {
    use worldinterface_runtime::{
        aq_adapter::QueueEngine,
        reconciliation::{self, ReconciliationHandler},
    };
    let h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Ambiguous);
    h.execute().await;
    let handler = ReconciliationHandler {
        bundle: h.bundle.clone(),
        connector: h.connector.clone(),
        clock: h.clock.clone(),
    };
    let mut q = QueueEngine::bootstrap(&h.bundle, handler, host()).unwrap();
    let request: actionqueue_core::admission::EnsureTaskRequest =
        serde_json::from_slice(&h.case().admission_request.unwrap()).unwrap();
    let admission = q.ensure_task(request).unwrap();
    let run = q.engine().projection().run_ids_for_task(admission.task_id())[0];
    for _ in 0..20 {
        let _ = q.tick().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert!(h.case().aq_run.is_none());
    assert!(!q.engine().projection().get_run_state(&run).unwrap().is_terminal());
    assert_eq!(h.case().observations, 0);
    reconciliation::deliver(&mut q, &h.caller(), &h.clock, &h.id).unwrap();
    h.clock.advance(2000);
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    for _ in 0..100 {
        let _ = q.tick().await.unwrap();
        if h.case().status.terminal() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    assert_eq!(h.case().status, CaseStatus::ConfirmedCommitted);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn reconciliation_confirmations_preserve_original_receipt_and_stable_key() {
    for committed in [true, false] {
        let h = Harness::new(
            IdempotencyClass::ExternallyKeyed,
            if committed { Mode::Ambiguous } else { Mode::Unknown },
        );
        h.execute().await;
        let original = h.bundle.wi().effect_receipts(h.caller().tenant(), &h.id).unwrap();
        let key = h.case().execution.provider_key.clone();
        // An authoritative absence query is permitted only after uncertainty is durable.
        let mut h = h;
        if !committed {
            h.connector.mode = Mode::NonCommit;
        }
        h.reconcile().await;
        assert_eq!(
            h.case().status,
            if committed {
                CaseStatus::ConfirmedCommitted
            } else {
                CaseStatus::ConfirmedNotCommitted
            }
        );
        assert_eq!(h.effect().phase, Phase::Reconciled);
        assert_eq!(h.case().execution.provider_key, key);
        assert_eq!(h.bundle.wi().effect_receipts(h.caller().tenant(), &h.id).unwrap(), original);
        assert_eq!(h.connector.commits(), usize::from(committed));
        assert_eq!(h.case().receipts.len(), 1);
        let count = h.case().observations;
        h.reconcile().await;
        assert_eq!(h.case().observations, count);
        let case = h.case();
        assert!(h
            .bundle
            .wi()
            .reconciliation_apply(
                &h.caller(),
                h.clock.now().unwrap(),
                &case,
                VerifiedCaseAction::from_trusted_runtime(CaseAction::Observe {
                    result: CaseStatus::ConfirmedNotCommitted,
                    evidence: case.evidence[..1].to_vec()
                })
            )
            .is_err());
        let access = AuthenticatedCaller::from_trusted_authentication(
            "different-tenant".to_string().try_into().unwrap(),
            h.caller().principal().clone(),
        );
        assert!(h.bundle.wi().reconciliation_cases(&access, &h.id).is_err());
        let stranger = AuthenticatedCaller::from_trusted_authentication(
            h.caller().tenant().clone(),
            PrincipalId::generate(),
        );
        assert!(h.bundle.wi().reconciliation_cases(&stranger, &h.id).is_err());
    }
}
#[tokio::test]
async fn reconciliation_retry_requires_policy_budget_and_fresh_exact_authorization() {
    let mut h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Unknown);
    h.execute().await;
    h.connector.mode = Mode::NonCommit;
    h.reconcile().await;
    let key = h.case().execution.provider_key.clone();
    let caller = h.caller();
    let retry = |s| EffectAction::PermitRetry {
        id: h.id.clone(),
        snapshot: VerifiedSnapshot::from_trusted_verifier(s).unwrap(),
    };
    let mut denied = h.snapshot.clone();
    denied.policy.retry_after_noncommit = false;
    assert!(Egress::new(h.bundle.wi())
        .apply(&caller, h.clock.now().unwrap(), retry(denied))
        .is_err());
    let mut expired = h.snapshot.clone();
    expired.grants[0].revoked = true;
    assert!(Egress::new(h.bundle.wi())
        .apply(&caller, h.clock.now().unwrap(), retry(expired))
        .is_err());
    Egress::new(h.bundle.wi())
        .apply(&caller, h.clock.now().unwrap(), retry(h.snapshot.clone()))
        .unwrap();
    assert_eq!(h.effect().phase, Phase::Authorized);
    assert_eq!(
        h.effect().resolution.unwrap().resolved.connector_execution.unwrap().provider_key,
        key
    );
    let verified = || VerifiedSnapshot::from_trusted_verifier(h.snapshot.clone()).unwrap();
    let mut queue =
        worldinterface_runtime::aq_adapter::QueueEngine::bootstrap(&h.bundle, Noop, host())
            .unwrap();
    worldinterface_runtime::admission::prepare(
        &h.bundle,
        &caller,
        h.clock.now().unwrap(),
        &h.id,
        verified(),
    )
    .unwrap();
    worldinterface_runtime::admission::deliver(
        &mut queue,
        &caller,
        h.clock.now().unwrap(),
        &h.id,
        verified(),
    )
    .unwrap();
    worldinterface_runtime::run_discovery::discover(
        &queue,
        &caller,
        h.clock.now().unwrap(),
        &h.id,
        8,
    )
    .unwrap();
    h.run = h.effect().dispatches.last().unwrap().admission.as_ref().unwrap().observed_runs[0]
        .as_str()
        .parse()
        .unwrap();
    queue.shutdown().unwrap();
    h.connector.mode = Mode::Commit;
    h.execute().await;
    assert_eq!(h.effect().phase, Phase::Succeeded);
    assert_eq!(h.connector.commits(), 1);
    assert_eq!(
        h.effect().resolution.unwrap().resolved.connector_execution.unwrap().provider_key,
        key
    );
}
#[tokio::test]
async fn reconciliation_bounded_backoff_deadline_and_escalation() {
    for mode in [Mode::Unknown, Mode::Escalate] {
        let h = Harness::new(IdempotencyClass::NonRepeatable, mode);
        h.execute().await;
        let mut draft = h.case();
        let mut pending = draft.clone();
        assert!(pending
            .observe(
                pending.revision,
                CaseStatus::Pending,
                h.effect().outcomes[0].evidence.clone(),
                &h.clock.now().unwrap(),
                99
            )
            .is_err());
        draft.start(draft.revision, h.clock.now().unwrap().epoch_millis().unwrap()).unwrap();
        assert!(draft
            .start(draft.revision, h.clock.now().unwrap().epoch_millis().unwrap())
            .is_err());
        h.reconcile().await;
        let c = h.case();
        assert_eq!(
            c.status,
            if mode == Mode::Unknown { CaseStatus::StillUnknown } else { CaseStatus::Escalated }
        );
        assert!(c.observations <= 3);
        assert_eq!(h.effect().phase, Phase::Uncertain);
        assert!(!c.evidence.is_empty());
        assert_eq!(h.connector.commits(), 0);
        let revision = h.bundle.wi().effect_snapshot(h.caller().tenant()).unwrap().revision;
        h.reconcile().await;
        assert_eq!(h.bundle.wi().effect_snapshot(h.caller().tenant()).unwrap().revision, revision);
    }
    let h = Harness::new(IdempotencyClass::NonRepeatable, Mode::Unknown);
    h.execute().await;
    h.clock.advance(121_000);
    h.reconcile().await;
    assert_eq!(h.case().status, CaseStatus::StillUnknown);
    assert_eq!(h.case().observations, 0);
}
#[tokio::test]
async fn reconciliation_reopen_backup_and_projection_rebuild_preserve_case() {
    let h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Ambiguous);
    h.execute().await;
    h.reconcile().await;
    let case = h.case();
    let path = h.temp.path().join("bundle");
    // The source journal is authoritative; reopening validates the reconstructed projection.
    let snapshot = h.effect();
    let caller = h.caller();
    let id = h.id.clone();
    drop(h.bundle);
    let reopened = StoreBundle::open(&path).unwrap();
    assert_eq!(reopened.wi().reconciliation_cases(&caller, &id).unwrap()[0], case);
    assert_eq!(reopened.wi().effect_snapshot(caller.tenant()).unwrap().effects[&id], snapshot);
    drop(reopened);
    let backup = h.temp.path().join("backup");
    let restored = h.temp.path().join("restored");
    StoreBundle::backup(&path, &backup).unwrap();
    StoreBundle::restore(&backup, &restored).unwrap();
    let restored = StoreBundle::open(&restored).unwrap();
    assert_eq!(restored.wi().reconciliation_cases(&caller, &id).unwrap()[0], case);
}

#[tokio::test]
async fn reconciliation_rejects_copied_payload_on_another_aq_run() {
    use worldinterface_runtime::{
        aq_adapter::QueueEngine,
        reconciliation::{self, ReconciliationHandler},
    };
    let h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Ambiguous);
    h.execute().await;
    let mut queue = QueueEngine::bootstrap(&h.bundle, Noop, host()).unwrap();
    reconciliation::deliver(&mut queue, &h.caller(), &h.clock, &h.id).unwrap();
    queue.shutdown().unwrap();
    let case = h.case();
    assert!(case.aq_run.is_some());
    let mut copied = h.context();
    let request: actionqueue_core::admission::EnsureTaskRequest =
        serde_json::from_slice(case.admission_request.as_ref().unwrap()).unwrap();
    copied.input.payload = request.task_spec().task_payload().bytes().to_vec();
    copied.input.causal_context = Some(request.causal_context().clone());
    let handler = ReconciliationHandler {
        bundle: h.bundle.clone(),
        connector: h.connector.clone(),
        clock: h.clock.clone(),
    };
    assert!(handler.execute_bound(&copied).unwrap_err().to_string().contains("task binding"));
    assert_eq!(h.case(), case);
    assert_eq!(h.case().observations, 0);
    h.reconcile().await;
    assert_eq!(h.case().status, CaseStatus::ConfirmedCommitted);
    assert_eq!(h.connector.commits(), 1);
}

/// F-001: a principal cannot forge lifecycle, observation or admission authority.
#[tokio::test]
async fn reconciliation_principal_cannot_forge_case_transitions_or_enable_retry() {
    let h = Harness::new(IdempotencyClass::NonRepeatable, Mode::Ambiguous);
    h.execute().await;
    let case = h.case();
    let evidence = h.effect().outcomes[0].evidence.clone();
    for action in [
        CaseAction::Start,
        CaseAction::Observe {
            result: CaseStatus::ConfirmedNotCommitted,
            evidence: evidence.clone(),
        },
        CaseAction::Observe { result: CaseStatus::ConfirmedCommitted, evidence: evidence.clone() },
        CaseAction::Observe { result: CaseStatus::Escalated, evidence: evidence.clone() },
        CaseAction::Observe { result: CaseStatus::StillUnknown, evidence },
        CaseAction::Acknowledge {
            sequence: 1,
            run: Some("forged-run".to_string().try_into().unwrap()),
        },
        CaseAction::PrepareAdmission { request: b"forged-request".to_vec() },
    ] {
        // Raw CaseAction cannot inhabit EffectAction (compile-fail domain test).
        // Even an erroneously attested principal request is refused by Egress.
        let error = Egress::new(h.bundle.wi())
            .apply(
                &h.caller(),
                h.clock.now().unwrap(),
                EffectAction::ReconciliationCase {
                    id: h.id.clone(),
                    case: case.id.clone(),
                    expected: case.revision,
                    action: VerifiedCaseAction::from_trusted_runtime(action),
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("trusted runtime path"));
        assert_eq!(h.case(), case);
        assert!(Egress::new(h.bundle.wi())
            .apply(
                &h.caller(),
                h.clock.now().unwrap(),
                EffectAction::PermitRetry {
                    id: h.id.clone(),
                    snapshot: VerifiedSnapshot::from_trusted_verifier(h.snapshot.clone()).unwrap(),
                },
            )
            .is_err());
    }
    h.execute().await;
    assert_eq!(h.connector.commits(), 1);
    h.reconcile().await;
    assert_eq!(h.case().status, CaseStatus::ConfirmedCommitted);
    assert_eq!(h.connector.commits(), 1);
}

/// F-002: transient host-clock failure retries the admitted observation run;
/// a binding violation still produces terminal failure without spending budget.
#[tokio::test]
async fn reconciliation_retries_transient_clock_and_rejects_binding_errors() {
    use actionqueue_core::disposition::DispositionOutcome;
    use actionqueue_executor_local::handler::ExecutorHandler;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use worldinterface_runtime::{
        aq_adapter::QueueEngine,
        reconciliation::{self, ReconciliationHandler},
        Result,
    };
    struct FlakyClock {
        failed: Arc<AtomicBool>,
        clock: Clock,
    }
    impl TrustedClock for FlakyClock {
        fn now(&self) -> Result<Timestamp> {
            if !self.failed.swap(true, Ordering::SeqCst) {
                return Err("temporary clock failure".into());
            }
            self.clock.now()
        }
    }
    let h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Ambiguous);
    h.execute().await;
    let mut queue = QueueEngine::bootstrap(&h.bundle, Noop, host()).unwrap();
    reconciliation::deliver(&mut queue, &h.caller(), &h.clock, &h.id).unwrap();
    queue.shutdown().unwrap();
    let case = h.case();
    let request: actionqueue_core::admission::EnsureTaskRequest =
        serde_json::from_slice(case.admission_request.as_ref().unwrap()).unwrap();
    let context = |run| {
        let mut ctx = h.context();
        ctx.input.payload = request.task_spec().task_payload().bytes().to_vec();
        ctx.input.causal_context = Some(request.causal_context().clone());
        ctx.input.run_id = run;
        ctx
    };
    let handler = ReconciliationHandler {
        bundle: h.bundle.clone(),
        connector: h.connector.clone(),
        clock: FlakyClock { failed: Arc::new(AtomicBool::new(false)), clock: h.clock.clone() },
    };
    assert!(matches!(
        handler.execute(context(h.run)).outcome(),
        DispositionOutcome::TerminalFailure { .. }
    ));
    let run = case.aq_run.as_ref().unwrap().as_str().parse().unwrap();
    h.reconcile_with_clock(handler.clock).await;
    let queue = QueueEngine::bootstrap(&h.bundle, Noop, host()).unwrap();
    let observed_run = queue.engine().projection().run_instances().find(|r| r.id() == run).unwrap();
    assert_eq!(observed_run.failure_attempt_count(), 1);
    assert_eq!(observed_run.attempt_count(), 2);
    assert_eq!(observed_run.state(), actionqueue_core::run::state::RunState::Completed);
    queue.shutdown().unwrap();
    assert_eq!(h.case().observations, 1);
    assert_eq!(h.case().status, CaseStatus::ConfirmedCommitted);
    assert_eq!(h.connector.commits(), 1);
}
