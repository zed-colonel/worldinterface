mod support {
    pub mod connectors;
    pub mod continuation_fixture;
    pub mod flow_fixture;
    pub mod spi_harness;
}
use support::{connectors::*, continuation_fixture::*, flow_fixture::*, spi_harness::host};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    continuation::*,
    flow::*,
    ingress::{IngressCommand, IngressResult},
    reconciliation::{CaseAction, CaseStatus, VerifiedCaseAction},
};
use worldinterface_protocol::*;
use worldinterface_runtime::{
    aq_adapter::QueueEngine, authority::TrustedClock, ingress::IngressRuntime,
};
use worldinterface_storage::{flow::FlowOutcome, StoreKind};

fn fault(f: &Fixture, enable: bool) {
    let c = rusqlite::Connection::open(f._temp.path().join("bundle/wi/fabric.sqlite")).unwrap();
    c.execute_batch(if enable {
        "CREATE TRIGGER fail_signal BEFORE INSERT ON signal_outbox BEGIN SELECT RAISE(ABORT,'injected signal write'); END;"
    } else { "DROP TRIGGER fail_signal;" }).unwrap();
}
fn events(
    f: &Fixture,
    kind: EventKind,
) -> Vec<worldinterface_domain::continuation::SignalObligation> {
    f.bundle
        .wi()
        .signal_obligations(StoreKind::Fabric, None, 1000)
        .unwrap()
        .into_iter()
        .filter(|o| o.event.route.kind == kind)
        .collect()
}
fn rebuild(f: &Fixture, kind: EventKind) -> worldinterface_domain::continuation::SignalObligation {
    let before = events(f, kind);
    assert_eq!(before.len(), 1);
    let position = f.bundle.wi().event_position(StoreKind::Fabric, &before[0].event.id).unwrap();
    f.bundle.wi().repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(events(f, kind), before);
    assert_eq!(
        f.bundle.wi().authoritative_event(StoreKind::Fabric, &before[0].event.id).unwrap(),
        before[0].event
    );
    assert_eq!(
        f.bundle.wi().event_position(StoreKind::Fabric, &before[0].event.id).unwrap(),
        position
    );
    before.into_iter().next().unwrap()
}
async fn wake(f: &Fixture, event: &DurableEvent) {
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut d = definition(f, true, SignalProgress::Once {}, None);
    d.id = r(event.route.kind.name());
    if let PlanOperation::Wait { signal } = &mut d.body.nodes[0].operation {
        signal.signal_kind = r(event.route.kind.name());
        signal.correlation = event.route.correlation.clone();
    }
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let mut submission = f.submission.clone();
    submission.client_request_id = uuid::Uuid::new_v4().to_string().try_into().unwrap();
    let id = service.submit_flow(&(), &d.id, submission, &mut q).unwrap();
    f.drive(&mut q, &id).await;
    assert_eq!(f.run(&id).outcome, Some(FlowOutcome::Succeeded));
    assert_eq!(
        f.run(&id)
            .accepted_resumes
            .values()
            .filter(|a| a.event.as_ref() == Some(&event.id))
            .count(),
        1
    );
    assert_redeliveries(f, &q, &id);
    q.shutdown().unwrap();
}

#[tokio::test]
async fn ingress_ready_and_acknowledged_commit_source_outbox_atomically_and_wake_flows() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use worldinterface_ingress::{transport::ProviderCredentials, AuthenticatedScope};
    let f = Fixture::new();
    let now = f.clock.now().unwrap().epoch_millis().unwrap();
    let mut binding: IngressBinding =
        serde_json::from_str(include_str!("../../../conformance/wi-fabric-2/ingress/binding.json"))
            .unwrap();
    binding.profile.scope.tenant = f.auth.0.clone();
    binding.principal = f.auth.1.clone();
    binding.subscriber = f.auth.1.clone();
    binding.authorization.artifact =
        f.bundle.wi().publish(f.auth.0.clone(), b"authorization").unwrap();
    binding.expires_at_ms = now + 1_000_000;
    binding.policy.retention_ms = 1_000_000;
    binding.policy.acknowledgment_timeout_ms = 60_000;
    f.bundle
        .wi()
        .ingress_transact(
            &f.auth.0,
            None,
            now,
            &IngressCommand::Bind { binding: Box::new(binding.clone()) },
        )
        .unwrap();
    let event = ProviderEvent {
        scope: binding.profile.scope.clone(),
        event_id: Some(r("source-event")),
        offset: Some(0),
        produced_at_ms: now,
        asserted_sender: r("sender"),
        asserted_audience: vec![],
        content: f
            .bundle
            .wi()
            .publish(
                worldinterface_domain::ingress::provider_artifact_scope(&binding.profile.scope)
                    .unwrap(),
                b"message",
            )
            .unwrap(),
        attachments: vec![],
        profile: ObservationProfile::Ordinary,
        information_use: InformationUseEnvelope {
            purpose_refs: vec![],
            allowed_audiences: vec![f.auth.1.clone()],
            retention_policy_ref: None,
            disclosure_policy_ref: None,
            derivation_policy_ref: None,
            redistribution: RedistributionPolicy::Prohibited,
            expiration: None,
            source_obligation_refs: vec![],
        },
    };
    let body = canonical_data(&event).unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    mac.update(&body);
    let signature = mac.finalize().into_bytes();
    let credentials = ProviderCredentials::new(binding.profile.clone(), vec![7; 32]).unwrap();
    let service = IngressRuntime::new(f.bundle.wi(), f.auth.0.clone());
    let before = f.bundle.wi().ingress_snapshot(&f.auth.0).unwrap();
    fault(&f, true);
    assert!(service.accept_http(&credentials, &body, &signature, now).is_err());
    assert_eq!(f.bundle.wi().ingress_snapshot(&f.auth.0).unwrap(), before);
    assert_eq!(f.bundle.wi().event_high_water(StoreKind::Fabric).unwrap(), 0);
    assert!(events(&f, EventKind::IngressReady).is_empty());
    fault(&f, false);
    let IngressResult::Admitted { deliveries, .. } =
        service.accept_http(&credentials, &body, &signature, now).unwrap()
    else {
        panic!("admission");
    };
    let ready = rebuild(&f, EventKind::IngressReady);
    assert!(ready.sequence.is_none()); // Source/outbox exists before any waiter or AQ admission.
    assert_eq!(ready.event.source.as_str(), deliveries[0].as_str());
    assert!(matches!(
        service.accept_http(&credentials, &body, &signature, now).unwrap(),
        IngressResult::Admitted { duplicate: true, .. }
    ));
    wake(&f, &ready.event).await;
    let subscriber = AuthenticatedScope { tenant: f.auth.0.clone(), principal: f.auth.1.clone() };
    let issued = service.claim(&subscriber, &deliveries[0], now).unwrap().unwrap();
    let before = f.bundle.wi().ingress_snapshot(&f.auth.0).unwrap();
    let source_position = f.bundle.wi().event_high_water(StoreKind::Fabric).unwrap();
    fault(&f, true);
    assert!(service.acknowledge(&subscriber, &deliveries[0], &issued.token, now).is_err());
    assert_eq!(f.bundle.wi().ingress_snapshot(&f.auth.0).unwrap(), before);
    assert_eq!(f.bundle.wi().event_high_water(StoreKind::Fabric).unwrap(), source_position);
    assert!(events(&f, EventKind::IngressAcknowledged).is_empty());
    fault(&f, false);
    let ack = service.acknowledge(&subscriber, &deliveries[0], &issued.token, now).unwrap();
    let acknowledged = rebuild(&f, EventKind::IngressAcknowledged);
    assert!(acknowledged.sequence.is_none());
    assert_eq!(service.acknowledge(&subscriber, &deliveries[0], &issued.token, now).unwrap(), ack);
    wake(&f, &acknowledged.event).await;
    let ready = rebuild(&f, EventKind::IngressReady);
    assert!(ready.sequence.is_some());
    assert!(rebuild(&f, EventKind::IngressAcknowledged).sequence.is_some());
    let path = f._temp.path().join("bundle");
    drop(f.bundle);
    let reopened = worldinterface_runtime::storage::StoreBundle::open(&path).unwrap();
    assert_eq!(reopened.wi().signal_obligation(StoreKind::Fabric, &ready.event.id).unwrap(), ready);
}

#[tokio::test]
async fn reconciliation_commits_source_outbox_atomically_and_wakes_flow() {
    let mut f = Fixture::new();
    f.connector.mode = Mode::Ambiguous;
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_direct(&(), f.submission.clone(), f.selection(), &mut q).unwrap();
    f.drive(&mut q, &id).await;
    q.shutdown().unwrap();
    let intent = f.run(&id).results.values().next().unwrap().effect.clone().unwrap();
    let caller =
        AuthenticatedCaller::from_trusted_authentication(f.auth.0.clone(), f.auth.1.clone());
    let case = f.bundle.wi().reconciliation_cases(&caller, &intent).unwrap()[0].clone();
    f.bundle
        .wi()
        .reconciliation_apply(
            &caller,
            f.clock.now().unwrap(),
            &case,
            VerifiedCaseAction::from_trusted_runtime(CaseAction::Start),
        )
        .unwrap();
    let case = f.bundle.wi().reconciliation_cases(&caller, &intent).unwrap()[0].clone();
    let proof = EvidenceRef {
        artifact: f.bundle.wi().publish(f.auth.0.clone(), b"provider-confirmation").unwrap(),
        schema: r("provider-query:1"),
    };
    let action = VerifiedCaseAction::from_trusted_runtime(CaseAction::Observe {
        result: CaseStatus::ConfirmedCommitted,
        evidence: vec![proof],
    });
    let before = f.bundle.wi().effect_snapshot(&f.auth.0).unwrap();
    let source_position = f.bundle.wi().event_high_water(StoreKind::Fabric).unwrap();
    fault(&f, true);
    assert!(f
        .bundle
        .wi()
        .reconciliation_apply(&caller, f.clock.now().unwrap(), &case, action.clone())
        .is_err());
    assert_eq!(f.bundle.wi().effect_snapshot(&f.auth.0).unwrap(), before);
    assert_eq!(f.bundle.wi().event_high_water(StoreKind::Fabric).unwrap(), source_position);
    assert!(events(&f, EventKind::Reconciliation).is_empty());
    fault(&f, false);
    f.bundle.wi().reconciliation_apply(&caller, f.clock.now().unwrap(), &case, action).unwrap();
    let signal = rebuild(&f, EventKind::Reconciliation);
    assert!(signal.sequence.is_none());
    let receipt =
        f.bundle.wi().reconciliation_cases(&caller, &intent).unwrap()[0].receipts[0].clone();
    assert_eq!(signal.event.source, case.id);
    assert_eq!(signal.event.digest, ContentHash::of(&canonical_data(&receipt).unwrap()));
    wake(&f, &signal.event).await;
    assert!(rebuild(&f, EventKind::Reconciliation).sequence.is_some());
    let path = f._temp.path().join("bundle");
    drop(f.bundle);
    let reopened = worldinterface_runtime::storage::StoreBundle::open(&path).unwrap();
    assert_eq!(
        reopened.wi().authoritative_event(StoreKind::Fabric, &signal.event.id).unwrap(),
        signal.event
    );
}

#[tokio::test]
async fn flow_child_commits_source_outbox_atomically_and_wakes_flow() {
    use worldinterface_storage::flow::{FlowCommand, NodeDisposition, NodeResult};
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    until_wait(&f, &mut q, &id).await;
    q.shutdown().unwrap();
    let result = NodeResult {
        disposition: NodeDisposition::Succeeded,
        values: Default::default(),
        outputs: Default::default(),
        branch: None,
        input_hash: ContentHash::of(b""),
        output_hash: ContentHash::of(b""),
        program: None,
        effect: None,
    };
    let before = f.bundle.wi().flow_snapshot(&f.auth.0).unwrap();
    let cmd =
        FlowCommand::RecordNodeResult { run: id.clone(), node: r("wait"), result: result.clone() };
    let c = rusqlite::Connection::open(f._temp.path().join("bundle/wi/flow.sqlite")).unwrap();
    c.execute_batch("CREATE TRIGGER fail_signal BEFORE INSERT ON signal_outbox BEGIN SELECT RAISE(ABORT,'injected signal write'); END;").unwrap();
    assert!(f.bundle.wi().flow_update(&f.auth.0, before.revision, cmd.clone()).is_err());
    assert_eq!(f.bundle.wi().flow_snapshot(&f.auth.0).unwrap(), before);
    assert_eq!(f.bundle.wi().event_high_water(StoreKind::Flow).unwrap(), 0);
    c.execute_batch("DROP TRIGGER fail_signal").unwrap();
    drop(c);
    f.bundle.wi().flow_update(&f.auth.0, before.revision, cmd).unwrap();
    let pending = f.bundle.wi().signal_obligations(StoreKind::Flow, None, 10).unwrap();
    assert_eq!(pending.len(), 1);
    let signal = &pending[0];
    assert!(signal.sequence.is_none());
    assert_eq!(signal.event.route.kind, EventKind::FlowChild);
    assert_eq!(signal.event.source, flow_child_ref(&id, &r("wait")).unwrap());
    assert_eq!(signal.event.digest, ContentHash::of(&canonical_data(&result).unwrap()));
    f.bundle.wi().repository(StoreKind::Flow).unwrap().rebuild().unwrap();
    assert_eq!(
        f.bundle.wi().signal_obligation(StoreKind::Flow, &signal.event.id).unwrap(),
        *signal
    );
    wake(&f, &signal.event).await;
    let delivered = f.bundle.wi().signal_obligation(StoreKind::Flow, &signal.event.id).unwrap();
    assert!(delivered.sequence.is_some());
    f.bundle.wi().repository(StoreKind::Flow).unwrap().rebuild().unwrap();
    assert_eq!(
        f.bundle.wi().signal_obligation(StoreKind::Flow, &signal.event.id).unwrap(),
        delivered
    );
}
