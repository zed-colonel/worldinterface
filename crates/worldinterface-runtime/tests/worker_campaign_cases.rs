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

#[test]
fn campaign_unknown_controls_publish_once_with_exact_observation_digest() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("seam.json");
    let value = serde_json::json!({"evidence":{"seam_simulation":true,"controls":null}});
    let digest = setup::publish_campaign_observation(&path, &value).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(), value);
    assert_eq!(worldinterface_protocol::ContentHash::of(&bytes), digest);
    let inflated = serde_json::json!({"evidence":{"controls":{"supported":true}}});
    assert!(setup::publish_campaign_observation(&path, &inflated).is_err());
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[tokio::test]
async fn wp_033_embedded_completion_is_producer_evidence_for_partial_milestone_only() {
    for coding in [false, true] {
        let s = setup::Setup::live_case(coding, "WP-033");
        let evidence = complete(&s, false).await;
        setup::campaign_observation(
            if coding { "coding.json" } else { "transform.json" },
            &s,
            evidence,
        );
    }
}
#[tokio::test]
async fn wp_034_embedded_seam_handoff_retains_nonsemantic_completion_and_unknown_controls() {
    let s = setup::Setup::seam();
    let evidence = complete(&s, true).await;
    assert!(evidence.seam_simulation);
    assert!(evidence.controls.is_none(), "seam cannot attest process controls");
    setup::campaign_observation("seam.json", &s, evidence);
}
async fn complete(
    s: &setup::Setup,
    seam: bool,
) -> worldinterface_runtime::embedded::WorkerEvidence {
    let mut app = s.host.application().unwrap();
    for session in [setup::Session::ForeignTenant, setup::Session::ForeignPrincipal] {
        consumer::denied(&app, &session, &s.worker);
        assert!(app.inspect_worker_evidence(&session, &s.worker, &s.provider, &s.run).is_err());
        let (submission, selection) = s.demands[1].clone();
        assert!(app.submit(&session, submission, selection).is_err());
    }
    for index in [0, 1, 2] {
        let (submission, selection) = s.demands[index].clone();
        if seam && index == 1 {
            let id =
                app.submit(&setup::Session::Valid, submission.clone(), selection.clone()).unwrap();
            assert_eq!(app.submit(&setup::Session::Valid, submission, selection).unwrap(), id);
            consumer::exited(&mut app, &setup::Session::Valid, &s.worker, &s.provider, &s.run)
                .await;
            for _ in 0..12 {
                app.advance(&setup::Session::Valid).await.unwrap();
            }
        } else {
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
        }
    }
    consumer::exited(&mut app, &setup::Session::Valid, &s.worker, &s.provider, &s.run).await;
    for index in [4, 3, 5] {
        let (submission, selection) = s.demands[index].clone();
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
    }
    let state = consumer::published(
        &app,
        &setup::Session::Valid,
        &s.worker,
        &s.provider,
        &s.run,
        &s.expected,
    );
    assert!(!state.capacity_reserved && state.completed);
    s.disclose();
    if seam {
        let start = state.operations.values().find(|o| o.operation.as_str() == "start").unwrap();
        app.reconcile_effect(&setup::Session::Valid, &start.intent).unwrap();
        for _ in 0..30 {
            app.advance(&setup::Session::Valid).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(app
            .inspect_worker(&setup::Session::Valid, &s.worker, &s.provider, &s.run)
            .unwrap()
            .operations
            .values()
            .any(|o| o.intent == start.intent && o.acknowledged));
    }
    consumer::receipts(&app, &setup::Session::Valid, &s.receipts());
    let evidence = app
        .inspect_worker_evidence(&setup::Session::Valid, &s.worker, &s.provider, &s.run)
        .unwrap();
    assert!(evidence.cleanup.as_ref().unwrap().cleanup_confirmed);
    if !evidence.seam_simulation {
        evidence.controls.as_ref().unwrap().validate().unwrap();
    }
    let mut foreign = s.worker.clone();
    foreign.scope.organization = Some(worldinterface_protocol::PrincipalId::generate());
    assert!(app
        .inspect_worker_evidence(&setup::Session::Valid, &foreign, &s.provider, &s.run)
        .is_err());
    app.shutdown().unwrap();
    evidence
}

#[tokio::test]
async fn campaign_embedded_cancellation_and_output_bounds_keep_capacity_until_cleanup() {
    for cancellation in [false, true] {
        let s = setup::Setup::bounded(cancellation);
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
        assert!(
            app.inspect_worker(&setup::Session::Valid, &s.worker, &s.provider, &s.run)
                .unwrap()
                .capacity_reserved
        );
        if cancellation {
            let (submission, selection) = s.demands[3].clone();
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
        }
        consumer::exited(&mut app, &setup::Session::Valid, &s.worker, &s.provider, &s.run).await;
        let evidence = app
            .inspect_worker_evidence(&setup::Session::Valid, &s.worker, &s.provider, &s.run)
            .unwrap();
        let cleanup = evidence.cleanup.unwrap();
        assert!(cleanup.cleanup_confirmed && cleanup.retained_output_bytes <= 65536);
        let state =
            app.inspect_worker(&setup::Session::Valid, &s.worker, &s.provider, &s.run).unwrap();
        assert!(
            state.capacity_reserved,
            "cleanup alone cannot release artifact/publication capacity"
        );
        app.shutdown().unwrap();
    }
}
