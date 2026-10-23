//! Kill real WI/AQ producers at committed boundaries; reopen the paired stores.
#[path = "support/child.rs"]
mod child_guard;
mod support {
    pub mod connectors;
    pub mod continuation_fixture;
    pub mod flow_fixture;
    pub mod spi_harness;
}
use actionqueue_core::disposition::{AttemptDisposition, DispositionOutcome};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use support::{connectors::*, continuation_fixture::*, flow_fixture::*, spi_harness::host};
use worldinterface_domain::flow::SignalProgress;
use worldinterface_protocol::*;
use worldinterface_runtime::{aq_adapter::QueueEngine, storage::StoreBundle};
use worldinterface_storage::{flow::FlowOutcome, StoreKind};
struct BeforeCommit<H>(H);
impl<H: ExecutorHandler> ExecutorHandler for BeforeCommit<H> {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let resumed = ctx.input.resume_context.is_some();
        let d = self.0.execute(ctx);
        if resumed
            && matches!(d.outcome(), DispositionOutcome::Complete)
            && std::env::var("WI_CONTINUATION_CUT").as_deref() == Ok("before-aq-attempt-commit")
        {
            std::fs::write(std::env::var_os("WI_CRASH_READY").unwrap(), b"ready").unwrap();
            loop {
                std::thread::park_timeout(Duration::from_millis(10));
            }
        }
        d
    }
}
#[tokio::test]
async fn continuation_crash_child() {
    let Some(parent) = std::env::var_os("WI_CONTINUATION_PARENT") else {
        return;
    };
    let parent = std::path::PathBuf::from(parent);
    let f = Fixture::new();
    let evidence = Evidence(f.snapshot.clone());
    let service = f.service(&evidence);
    let d = definition(&f, true, SignalProgress::Once {}, None);
    service.activate_flow_version(&(), d.clone(), None).unwrap();
    let mut q = QueueEngine::bootstrap(&f.bundle, BeforeCommit(f.handler()), host()).unwrap();
    let id = service.submit_flow(&(), &d.id, f.submission.clone(), &mut q).unwrap();
    use worldinterface_runtime::authority::TrustedClock;
    std::fs::write(
        parent.join("fixture.json"),
        serde_json::to_vec(&(
            f._temp.path(),
            &f.snapshot,
            &f.config.0,
            &f.submission,
            f.clock.now().unwrap(),
            &id,
        ))
        .unwrap(),
    )
    .unwrap();
    let cut = std::env::var("WI_CONTINUATION_CUT").unwrap();
    if cut == "continuation-wait-prepared" {
        for _ in 0..2000 {
            let _ = q.tick().await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    } else {
        emit(&f, 1);
        for _ in 0..2000 {
            let _ = q.tick().await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    panic!("continuation crash cut was not reached");
}
#[tokio::test]
async fn continuation_process_death_replays_source_signal_wait_and_resume_commits() {
    for point in [
        "continuation-before-source-commit",
        "continuation-source-committed",
        "continuation-signal-admitted",
        "continuation-wait-prepared",
        "continuation-resume-accepted",
        "before-aq-attempt-commit",
    ] {
        let t = tempfile::tempdir().unwrap();
        let ready = t.path().join("ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "continuation_crash_child", "--nocapture"])
            .env("WI_CONTINUATION_PARENT", t.path())
            .env("WI_FLOW_FIXTURE_PARENT", t.path())
            .env("WI_CONTINUATION_CUT", point)
            .env("WI_CRASH_POINT", point)
            .env("WI_CRASH_READY", &ready)
            .stdout(std::process::Stdio::null())
            .spawn()
            .map(child_guard::KillOnDrop)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(45);
        while !ready.exists() && Instant::now() < deadline {
            assert!(child.try_wait().unwrap().is_none(), "helper exited at {point}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let reached = ready.exists();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(reached, "cut not reached: {point}");
        let (path, snapshot, pins, submission, at, id): (
            std::path::PathBuf,
            worldinterface_domain::authorization::AuthorizationSnapshot,
            worldinterface_storage::flow::FlowPins,
            EffectSubmission,
            Timestamp,
            OpaqueRef,
        ) = serde_json::from_slice(&std::fs::read(t.path().join("fixture.json")).unwrap()).unwrap();
        let clock = Clock::new();
        clock.0.store(at.epoch_millis().unwrap(), std::sync::atomic::Ordering::SeqCst);
        let auth =
            Auth(snapshot.bindings[0].tenant.clone(), snapshot.bindings[0].principal.clone());
        let f = Fixture {
            _temp: tempfile::tempdir().unwrap(),
            bundle: Arc::new(StoreBundle::open(&path.join("bundle")).unwrap()),
            connector: DeterministicConnector::new(
                path.join("provider"),
                IdempotencyClass::ExternallyKeyed,
                Mode::Commit,
            ),
            snapshot,
            submission,
            clock,
            config: Config(pins),
            auth,
        };
        let mut q = QueueEngine::bootstrap(&f.bundle, f.handler(), host()).unwrap();
        let event = emit(&f, 1);
        f.drive(&mut q, &id).await;
        let run = f.run(&id);
        assert_eq!(run.outcome, Some(FlowOutcome::Succeeded), "{point}");
        assert_eq!(
            run.accepted_resumes.values().filter(|a| a.event.as_ref() == Some(&event)).count(),
            1,
            "{point}"
        );
        q.shutdown().unwrap();
        f.bundle.wi().repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        f.bundle.wi().repository(StoreKind::Flow).unwrap().rebuild().unwrap();
        assert_eq!(f.run(&id), run);
        drop(f);
        let bundle = path.join("bundle");
        let backup = t.path().join("backup");
        let restored = t.path().join("restored");
        StoreBundle::backup(&bundle, &backup).unwrap();
        StoreBundle::restore(&backup, &restored).unwrap();
        let restored = StoreBundle::open(&restored).unwrap();
        assert_eq!(
            restored.wi().flow_run_snapshot(&run.tenant, &run.id).unwrap().runs[&run.id],
            run
        );
    }
}
