#![cfg(target_os = "linux")]
mod support {
    pub mod child;
    pub mod connectors;
    pub mod embedded_consumer;
    pub mod embedded_setup;
    pub mod flow_fixture;
    pub mod spi_harness;
    pub mod worker_fixture;
}
use support::{embedded_consumer as consumer, embedded_setup as setup};
use worldinterface_protocol::control_plane::*;
use worldinterface_runtime::embedded::FlowOutcome;
#[tokio::test]
async fn unpollable_and_foreign_workers_do_not_abort_embedded_advancement() {
    let s = setup::RecoverySetup::new();
    s.reserve_unpollable_workers();
    let flow = s.persist_unprojected_flow();
    let mut app = s.host.application().unwrap();
    for _ in 0..20 {
        app.advance(&setup::Session::Valid).await.unwrap();
    }
    s.assert_flow_projected(&flow);
    s.assert_unpollable_quarantines();
}

#[tokio::test]
async fn wp_029_embedded_coding_and_transform_use_one_engine_and_application_services() {
    for coding in [false, true] {
        let s = setup::Setup::live(coding);
        let mut app = s.host.application().unwrap();
        let mut flows = vec![];
        for index in [0, 1, 2] {
            let (submission, selection) = s.demands[index].clone();
            flows.push(
                consumer::operation(
                    &mut app,
                    &setup::Session::Valid,
                    submission,
                    selection,
                    &s.worker,
                    &s.provider,
                    &s.run,
                )
                .await,
            );
        }
        consumer::exited(&mut app, &setup::Session::Valid, &s.worker, &s.provider, &s.run).await;
        let (submission, selection) = s.demands[4].clone();
        flows.push(
            consumer::operation(
                &mut app,
                &setup::Session::Valid,
                submission,
                selection,
                &s.worker,
                &s.provider,
                &s.run,
            )
            .await,
        );
        consumer::published(
            &app,
            &setup::Session::Valid,
            &s.worker,
            &s.provider,
            &s.run,
            &s.expected,
        );
        // Exercise the sixth supported operation on this same engine after
        // publication; termination cannot revoke an already committed export.
        let (submission, selection) = s.demands[3].clone();
        flows.push(
            consumer::operation(
                &mut app,
                &setup::Session::Valid,
                submission,
                selection,
                &s.worker,
                &s.provider,
                &s.run,
            )
            .await,
        );
        let (submission, selection) = s.demands[5].clone();
        flows.push(
            consumer::operation(
                &mut app,
                &setup::Session::Valid,
                submission,
                selection,
                &s.worker,
                &s.provider,
                &s.run,
            )
            .await,
        );
        assert!(
            !app.inspect_worker(&setup::Session::Valid, &s.worker, &s.provider, &s.run)
                .unwrap()
                .capacity_reserved
        );
        // Grant the unpredictable receipt/Flow identities through separately trusted
        // administration after execution, then disclose through the consumer API.
        s.disclose();
        consumer::receipts(&app, &setup::Session::Valid, &s.receipts());
        for id in flows {
            let reference = ObjectRef { scope: s.scope.clone(), kind: ObjectKind::Flow, id };
            assert!(app
                .inspect_flow(&setup::Session::Valid, &reference)
                .unwrap()
                .outcome
                .is_some());
        }
        consumer::published(
            &app,
            &setup::Session::Valid,
            &s.worker,
            &s.provider,
            &s.run,
            &s.expected,
        );
        app.shutdown().unwrap();
    }
}
#[tokio::test]
async fn wp_030_embedded_forgery_scope_and_unauthorized_controls_fail_closed() {
    let s = setup::Setup::live_case(false, "WP-030");
    let mut app = s.host.application().unwrap();
    for session in [setup::Session::ForeignTenant, setup::Session::ForeignPrincipal] {
        let (submission, selection) = s.demands[1].clone();
        assert!(app.submit(&session, submission, selection).is_err());
        consumer::denied(&app, &session, &s.worker);
    }
    let mut foreign = s.worker.clone();
    foreign.scope.tenant = "foreign".to_string().try_into().unwrap();
    consumer::denied(&app, &setup::Session::Valid, &foreign);
    foreign = s.worker.clone();
    foreign.scope.organization = Some(worldinterface_protocol::PrincipalId::generate());
    consumer::denied(&app, &setup::Session::Valid, &foreign);
    // Operation selection cannot forge the descriptor, resource or authority.
    let (mut submission, mut selection) = s.demands[1].clone();
    selection.operation = "kill".to_string().try_into().unwrap();
    assert!(app.submit(&setup::Session::Valid, submission.clone(), selection).is_err());
    submission.submitted_by = Some(worldinterface_protocol::ActorId::generate());
    let selection = s.demands[1].1.clone();
    assert!(app.submit(&setup::Session::Valid, submission, selection).is_err());
    for (reference, _) in &s.expected {
        consumer::denied(&app, &setup::Session::ForeignTenant, reference);
    }
    app.shutdown().unwrap();
}
// boundary scanner start
#[test]
fn wp_030_consumer_and_helper_imports_stay_inside_application_boundary() {
    let sources = [
        include_str!("support/embedded_consumer.rs"),
        include_str!("embedded_worker.rs"),
        include_str!("worker_campaign_cases.rs"),
    ];
    // The entry point delegates trusted setup. Consumer helpers have no imports or
    // access paths to stores, process adapters, AQ, bootstrap or host internals.
    for (index, source) in sources.iter().enumerate() {
        // Exclude only this scanner, whose forbidden-path literals would match
        // themselves. Keep every consumer test before and after this function.
        let consumer = if index == 1 {
            let (before, scanner) = source.split_once("\n// boundary scanner start\n").unwrap();
            let (_, after) = scanner.split_once("\n// boundary scanner end\n").unwrap();
            format!("{before}\n{after}")
        } else {
            source.to_string()
        };
        let code = consumer
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        for forbidden in [
            "worldinterface_runtime::worker",
            "worldinterface_runtime::aq_",
            "worldinterface_runtime::storage",
            "worldinterface_storage::",
            "std::fs",
            "std::process",
            ".queue",
            ".bundle",
        ] {
            assert!(!code.contains(forbidden), "forbidden consumer path {forbidden}");
        }
        if index == 0 {
            for forbidden in ["setup::", ".host", "bootstrap", "WorkerRuntime", "StoreRoot"] {
                assert!(!code.contains(forbidden), "helper escapes boundary: {forbidden}");
            }
        }
    }
}
// boundary scanner end

#[test]
fn wp_030_recovery_only_facade_enforces_disclosure_revocation_and_exclusive_ownership() {
    let s = setup::RecoverySetup::new();
    let app = s.host.application().unwrap();
    assert_eq!(app.read_artifact(&setup::Session::Valid, &s.artifact).unwrap(), b"scoped artifact");
    for session in [setup::Session::ForeignTenant, setup::Session::ForeignPrincipal] {
        consumer::denied(&app, &session, &s.artifact);
        assert!(app.read_artifact(&session, &s.artifact).is_err());
    }
    let mut reference = s.artifact.clone();
    reference.scope.organization = Some(worldinterface_protocol::PrincipalId::generate());
    consumer::denied(&app, &setup::Session::Valid, &reference);
    s.writer_exclusion();
    s.revoke();
    consumer::denied(&app, &setup::Session::Valid, &s.artifact);
    assert!(app.read_artifact(&setup::Session::Valid, &s.artifact).is_err());
    app.shutdown().unwrap();
}

#[tokio::test]
async fn wp_029_restored_publication_is_readable_without_original_workspace_and_keeps_capacity() {
    let s = setup::Setup::live(false);
    let mut app = s.host.application().unwrap();
    let (submission, selection) = s.demands[1].clone();
    consumer::operation(
        &mut app,
        &setup::Session::Valid,
        submission,
        selection,
        &s.worker,
        &s.provider,
        &s.run,
    )
    .await;
    consumer::exited(&mut app, &setup::Session::Valid, &s.worker, &s.provider, &s.run).await;
    let (submission, selection) = s.demands[4].clone();
    consumer::operation(
        &mut app,
        &setup::Session::Valid,
        submission,
        selection,
        &s.worker,
        &s.provider,
        &s.run,
    )
    .await;
    app.shutdown().unwrap();
    let s = s.restored();
    let mut app = s.host.application().unwrap();
    app.advance(&setup::Session::Valid).await.unwrap();
    let worker = consumer::published(
        &app,
        &setup::Session::Valid,
        &s.worker,
        &s.provider,
        &s.run,
        &s.expected,
    );
    let evidence = app
        .inspect_worker_evidence(&setup::Session::Valid, &s.worker, &s.provider, &s.run)
        .unwrap();
    assert!(evidence.controls.is_none() && evidence.cleanup.is_none());
    assert!(worker.quarantined && worker.capacity_reserved && !worker.cleanup_confirmed);
    app.shutdown().unwrap();
}
#[tokio::test]
async fn embedded_worker_crash_child() {
    setup::crash_child().await;
}
#[test]
fn wp_029_embedded_crash_recovery_quarantines_without_replacement() {
    setup::crash_recovery();
}

#[tokio::test]
async fn wp_030_catalog_quarantine_and_revocation_fence_queued_execution() {
    for revoked in [false, true] {
        let s = setup::Setup::live_case(false, "WP-030");
        let finished = s.flow_observer();
        let mut app = s.host.application().unwrap();
        if !revoked {
            s.quarantine();
        }
        let (submission, selection) = s.demands[1].clone();
        let id = app.submit(&setup::Session::Valid, submission, selection).unwrap();
        if revoked {
            s.revoke();
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !finished(&id) {
            app.advance(&setup::Session::Valid).await.unwrap();
            assert!(std::time::Instant::now() < deadline, "queued execution did not terminate");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // The trusted authorizer installs exact read disclosure after the rejected
        // execution; this does not resubmit its immutable request or wake its task.
        s.disclose();
        let reference = ObjectRef { scope: s.scope.clone(), kind: ObjectKind::Flow, id };
        let flow = app.inspect_flow(&setup::Session::Valid, &reference).unwrap();
        assert!(matches!(flow.outcome, Some(FlowOutcome::Denied | FlowOutcome::Failed)));
        assert!(app
            .inspect_worker(&setup::Session::Valid, &s.worker, &s.provider, &s.run)
            .is_err());
        app.shutdown().unwrap();
    }
}

#[test]
fn wp_030_recovery_reconstructs_scoped_metadata_before_returning_application() {
    let setup = setup::RecoverySetup::new();
    let reference = setup.persist_unprojected_flow();
    let application = setup.host.application().unwrap();
    setup.assert_flow_projected(&reference);
    consumer::denied(&application, &setup::Session::Valid, &reference);
    application.shutdown().unwrap();
}
