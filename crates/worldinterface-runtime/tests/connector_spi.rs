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
