mod support {
    pub mod connectors;
    pub mod spi_harness;
}
use support::{connectors::*, spi_harness::*};
use worldinterface_domain::{effect::CommitObservation, Phase};
use worldinterface_protocol::*;
use worldinterface_runtime::{completion::PortObservation, connector::*};

#[test]
fn spi_registry_classes_keys_and_invalid_contracts() {
    for class in [
        IdempotencyClass::ReadOnly,
        IdempotencyClass::NaturallyIdempotent,
        IdempotencyClass::ExternallyKeyed,
        IdempotencyClass::Compensatable,
        IdempotencyClass::NonRepeatable,
        IdempotencyClass::Unknown,
    ] {
        let mut d = DeterministicConnector::new("unused".into(), class, Mode::Commit).descriptor;
        if matches!(class, IdempotencyClass::ReadOnly | IdempotencyClass::Unknown) {
            assert!(validate_connector(&d).is_err());
            d.effect_class = EffectClass::Read;
        }
        validate_connector(&d).unwrap();
        let registry = ConnectorRegistry::new(vec![d.clone()]).unwrap();
        assert_eq!(registry.digest(), ConnectorRegistry::new(vec![d.clone()]).unwrap().digest());
        if class == IdempotencyClass::ExternallyKeyed {
            let mapping = d.provider_key.clone().unwrap();
            let tenant: OpaqueRef = "tenant".to_string().try_into().unwrap();
            let identity: OpaqueRef = "effect:stable".to_string().try_into().unwrap();
            assert_eq!(
                mapping.derive(&tenant, &d.operation, &identity).unwrap(),
                mapping.derive(&tenant, &d.operation, &identity).unwrap()
            );
            assert_ne!(
                mapping.derive(&tenant, &d.operation, &identity).unwrap(),
                mapping
                    .derive(
                        &"other-tenant".to_string().try_into().unwrap(),
                        &d.operation,
                        &identity
                    )
                    .unwrap()
            );
            let mut renamed = mapping.clone();
            renamed.parameter = "renamed-header".to_string().try_into().unwrap();
            assert_eq!(
                mapping.derive(&tenant, &d.operation, &identity).unwrap(),
                renamed.derive(&tenant, &d.operation, &identity).unwrap()
            );
            let mut moved = d.clone();
            moved.provider_key = Some(renamed);
            assert_ne!(d.digest().unwrap(), moved.digest().unwrap());
            d.provider_key = None;
            assert!(validate_connector(&d).is_err());
        }
        d.max_evidence_bytes = 0;
        assert!(validate_connector(&d).is_err());
    }
    let descriptor =
        DeterministicConnector::new("unused".into(), IdempotencyClass::NonRepeatable, Mode::Commit)
            .descriptor;
    let mut value = serde_json::to_value(&descriptor).unwrap();
    value.as_object_mut().unwrap().remove("input_schema");
    assert!(serde_json::from_value::<ConnectorDescriptor>(value).is_err());
    let observation = |commit, outcome| PortObservation {
        commit,
        outcome,
        outputs: vec![],
        evidence: vec![],
        boundary_record: None,
    };
    for (commit, outcome, class) in [
        (
            CommitObservation::ProvenImpossible,
            ExecutionOutcome::FailedBeforeCommit,
            ExecutorClassification::PreDispatchFailure,
        ),
        (
            CommitObservation::ConfirmedNotCommitted,
            ExecutionOutcome::FailedBeforeCommit,
            ExecutorClassification::ConfirmedNotCommitted,
        ),
        (
            CommitObservation::ConfirmedCommitted,
            ExecutionOutcome::Succeeded,
            ExecutorClassification::Committed,
        ),
        (
            CommitObservation::ConfirmedNotCommitted,
            ExecutionOutcome::TerminalFailure,
            ExecutorClassification::TerminalFailure,
        ),
        (
            CommitObservation::Unknown,
            ExecutionOutcome::Uncertain,
            ExecutorClassification::AmbiguousCommit,
        ),
    ] {
        assert_eq!(observation(commit, outcome).classification(), class);
    }
}
#[tokio::test]
async fn spi_executor_classifies_and_rejects_drift_scope_and_prepare() {
    for (mode, phase, commits) in [
        (Mode::Commit, Phase::Succeeded, 1),
        (Mode::NonCommit, Phase::Failed, 0),
        (Mode::Terminal, Phase::Failed, 0),
        (Mode::TerminalUnknown, Phase::Uncertain, 0),
        (Mode::Ambiguous, Phase::Uncertain, 1),
        (Mode::PrepareReject, Phase::Failed, 0),
    ] {
        let h = Harness::new(IdempotencyClass::ExternallyKeyed, mode);
        h.execute().await;
        assert_eq!(h.effect().phase, phase, "{mode:?}");
        assert_eq!(h.connector.commits(), commits);
        if phase == Phase::Uncertain {
            assert!(!h.case().admission_request.as_ref().unwrap().is_empty());
        }
    }
    let transient = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::PrepareTransient);
    transient.execute().await;
    assert_eq!(transient.connector.commits(), 0);
    assert_eq!(transient.effect().phase, Phase::DispatchPending);
    assert!(transient.effect().attempt.is_none());
    for drift in [false, true] {
        let mut h = Harness::new(IdempotencyClass::NaturallyIdempotent, Mode::Widen);
        if drift {
            h.connector.mode = Mode::Commit;
            h.connector.descriptor.implementation = ContentHash::of(b"replacement");
        }
        h.execute().await;
        assert_eq!(h.connector.commits(), 0);
        assert_eq!(h.effect().phase, Phase::DispatchPending);
    }
}
#[tokio::test]
async fn spi_naturally_idempotent_and_nonrepeatable_are_real_connectors() {
    for class in [IdempotencyClass::NaturallyIdempotent, IdempotencyClass::NonRepeatable] {
        let h = Harness::new(class, Mode::Ambiguous);
        h.execute().await;
        h.execute().await;
        assert_eq!(h.connector.commits(), 1);
        assert_eq!(h.effect().phase, Phase::Uncertain);
        h.reconcile().await;
        assert_eq!(
            h.case().status,
            worldinterface_domain::reconciliation::CaseStatus::ConfirmedCommitted
        );
        assert_eq!(h.connector.commits(), 1);
        let serialized = serde_json::to_string(&h.effect()).unwrap();
        assert!(!serialized.contains("provider-secret-value"));
        assert_eq!(
            h.effect()
                .resolution
                .unwrap()
                .resolved
                .connector_execution
                .unwrap()
                .binding
                .descriptor
                .secret_refs[0]
                .as_str(),
            "broker-ref:provider"
        );
    }
}

#[test]
fn spi_cancellation_before_and_after_dispatch_preserves_commit_truth() {
    use actionqueue_executor_local::handler::ExecutorHandler;
    use std::sync::Arc;
    for after in [false, true] {
        let mut h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Commit);
        let ctx = h.context();
        let token = ctx.input.cancellation_context.token().clone();
        let cancel: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            token.cancel();
        });
        if after {
            h.connector.after_commit = Some(cancel);
        } else {
            h.connector.after_prepare = Some(cancel);
        }
        let _ = h.handler().execute(ctx);
        assert_eq!(h.connector.commits(), usize::from(after));
        assert_eq!(h.effect().phase, if after { Phase::Uncertain } else { Phase::Canceled });
    }
}

#[test]
fn spi_preparation_cannot_substitute_current_exact_authority() {
    use actionqueue_executor_local::handler::ExecutorHandler;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use worldinterface_domain::authorization::AuthorizationSnapshot;
    use worldinterface_runtime::{
        authority::EvidenceAuthority, completion::EffectAttemptHandler, Result,
    };
    struct Revocation {
        snapshot: AuthorizationSnapshot,
        revoked: Arc<AtomicBool>,
    }
    impl EvidenceAuthority for Revocation {
        fn current_snapshot(
            &self,
            _: &OpaqueRef,
            _: &EffectSubmission,
        ) -> Result<AuthorizationSnapshot> {
            let mut s = self.snapshot.clone();
            s.grants[0].revoked = self.revoked.load(Ordering::SeqCst);
            Ok(s)
        }
        fn verify(&self, _: &OpaqueRef, e: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
            Ok(if e == &self.snapshot.facts[0].evidence {
                self.snapshot.facts[0].attested_document.clone()
            } else {
                ContentHash::of(b"verified")
            })
        }
    }
    let mut h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Commit);
    let revoked = Arc::new(AtomicBool::new(false));
    let changed = revoked.clone();
    h.connector.after_prepare = Some(Arc::new(move || {
        changed.store(true, Ordering::SeqCst);
    }));
    let handler = EffectAttemptHandler {
        bundle: h.bundle.clone(),
        port: h.connector.clone(),
        evidence: Revocation { snapshot: h.snapshot.clone(), revoked },
        clock: h.clock.clone(),
    };
    let result = handler.execute(h.context());
    assert!(matches!(
        result.outcome(),
        actionqueue_core::disposition::DispositionOutcome::RetryableFailure { .. }
    ));
    assert_eq!(h.connector.commits(), 0);
    assert!(h.effect().attempt.is_none());
}

/// F-003: meaningful cancellation is a connector phase; a request, unsupported
/// cancellation or transport error never serves as evidence of noncommit.
#[test]
fn spi_cancel_phase_records_observation_and_preserves_commit_truth() {
    use actionqueue_executor_local::handler::ExecutorHandler;
    use std::sync::{atomic::Ordering, Arc};
    for (supported, cancel, mode, expected_commit, expected_phase) in [
        (
            false,
            Some(CancelObservation::ConfirmedNotCommitted),
            Mode::Unknown,
            CommitObservation::Unknown,
            Phase::Uncertain,
        ),
        (
            true,
            Some(CancelObservation::Requested),
            Mode::Unknown,
            CommitObservation::Unknown,
            Phase::Uncertain,
        ),
        (
            true,
            Some(CancelObservation::Unsupported),
            Mode::Unknown,
            CommitObservation::Unknown,
            Phase::Uncertain,
        ),
        (true, None, Mode::Unknown, CommitObservation::Unknown, Phase::Uncertain),
        (
            true,
            Some(CancelObservation::ConfirmedNotCommitted),
            Mode::Unknown,
            CommitObservation::ConfirmedNotCommitted,
            Phase::Canceled,
        ),
        (
            true,
            Some(CancelObservation::ConfirmedNotCommitted),
            Mode::Commit,
            CommitObservation::ConfirmedCommitted,
            Phase::Succeeded,
        ),
    ] {
        let mut h = Harness::with_cancellation(IdempotencyClass::NonRepeatable, mode, supported);
        h.connector.cancel_observation = cancel;
        let ctx = h.context();
        let token = ctx.input.cancellation_context.token().clone();
        h.connector.after_invoke = Some(Arc::new(move || {
            token.cancel();
        }));
        let _ = h.handler().execute(ctx);
        assert_eq!(h.connector.cancel_calls.load(Ordering::SeqCst), u64::from(supported));
        assert_eq!(h.effect().commit, expected_commit);
        assert_eq!(h.effect().phase, expected_phase);
        assert_eq!(h.connector.commits(), usize::from(mode == Mode::Commit));
        let outcome = h.effect().outcomes.remove(0);
        let cancellation =
            outcome.evidence.iter().find(|e| e.schema.as_str() == "wi.connector-cancel:1");
        assert_eq!(cancellation.is_some(), supported);
        if let Some(e) = cancellation {
            assert!(e.artifact.size > 0);
            assert_eq!(&e.artifact.scope, h.caller().tenant());
        }
        let _ = h.handler().execute(h.context());
        assert_eq!(h.connector.cancel_calls.load(Ordering::SeqCst), u64::from(supported));
        assert_eq!(h.connector.commits(), usize::from(mode == Mode::Commit));
    }
    // Before dispatch there is no external operation to cancel.
    let mut h = Harness::with_cancellation(IdempotencyClass::NonRepeatable, Mode::Commit, true);
    let ctx = h.context();
    let token = ctx.input.cancellation_context.token().clone();
    h.connector.after_prepare = Some(Arc::new(move || {
        token.cancel();
    }));
    let _ = h.handler().execute(ctx);
    assert_eq!(h.effect().phase, Phase::Canceled);
    assert_eq!(h.connector.cancel_calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.connector.commits(), 0);
}

/// F-004: terminal provider failure and provider commit are independent facts.
#[tokio::test]
async fn spi_terminal_failure_after_commit_is_visible_and_cannot_reinvoke() {
    use actionqueue_core::disposition::DispositionOutcome;
    use actionqueue_executor_local::handler::ExecutorHandler;
    use worldinterface_domain::{
        authorization::VerifiedSnapshot,
        effect::EffectAction,
        receipt::{DisclosureProfile, ReceiptAccess},
    };
    use worldinterface_egress::Egress;
    use worldinterface_runtime::{authority::TrustedClock, storage::StoreBundle};
    let h = Harness::new(IdempotencyClass::NonRepeatable, Mode::TerminalCommitted);
    let disposition = h.handler().execute(h.context());
    assert!(
        matches!(disposition.outcome(), DispositionOutcome::TerminalFailure { error } if error.code.as_str() == "wi.effect.terminal_provider_failure_committed")
    );
    let view = Egress::new(h.bundle.wi()).inspect(&h.caller(), &h.id).unwrap();
    assert_eq!(view.phase, Phase::Failed);
    assert_eq!(view.commit, CommitObservation::ConfirmedCommitted);
    assert_eq!(view.outcome.unwrap().classification, Some(ExecutorClassification::TerminalFailure));
    let receipts = h.bundle.wi().effect_receipts(h.caller().tenant(), &h.id).unwrap();
    assert_eq!(receipts.len(), 1);
    let receipt = h
        .bundle
        .wi()
        .inspect_receipt(
            &ReceiptAccess::from_trusted_authorizer(
                h.caller().tenant().clone(),
                h.caller().principal().clone(),
                DisclosureProfile::Full,
            ),
            &receipts[0].0,
        )
        .unwrap()
        .unwrap();
    let Record::BoundaryReceipt(receipt) = receipt.canonical.unwrap().record else {
        panic!("boundary receipt")
    };
    assert!(receipt.temporal.committed_at.is_some());
    let ReceiptBody::Egress(body) = receipt.body else { panic!("egress receipt") };
    assert_eq!(body.outcome, ExecutionOutcome::TerminalFailure);
    assert_eq!(body.classification, Some(ExecutorClassification::TerminalFailure));
    let verified = || VerifiedSnapshot::from_trusted_verifier(h.snapshot.clone()).unwrap();
    for action in [
        EffectAction::PermitRetry { id: h.id.clone(), snapshot: verified() },
        EffectAction::StartProduction {
            id: h.id.clone(),
            snapshot: verified(),
            run: h.run.to_string().try_into().unwrap(),
            attempt: "new-attempt".to_string().try_into().unwrap(),
        },
    ] {
        assert!(Egress::new(h.bundle.wi())
            .apply(&h.caller(), h.clock.now().unwrap(), action)
            .is_err());
    }
    assert_eq!(h.handler().execute(h.context()), disposition);
    h.execute().await;
    assert_eq!(h.connector.commits(), 1);
    assert_eq!(h.bundle.wi().effect_receipts(h.caller().tenant(), &h.id).unwrap(), receipts);
    let reopened = StoreBundle::open(&h.temp.path().join("bundle")).unwrap();
    assert!(reopened.convergence_faults().is_empty());
    assert_eq!(
        reopened.wi().effect_snapshot(h.caller().tenant()).unwrap().effects[&h.id].commit,
        CommitObservation::ConfirmedCommitted
    );
}
