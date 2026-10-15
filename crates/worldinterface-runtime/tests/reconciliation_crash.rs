mod support {
    pub mod connectors;
    pub mod spi_harness;
}
use actionqueue_core::{disposition::AttemptDisposition, ids::RunId};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};
use support::{connectors::*, spi_harness::*};
use worldinterface_protocol::*;
use worldinterface_runtime::{
    aq_adapter::QueueEngine, authority::TrustedClock, reconciliation::ReconciliationHandler,
    storage::StoreBundle,
};
#[derive(Serialize, Deserialize)]
struct Metadata {
    bundle: PathBuf,
    db: PathBuf,
    snapshot: worldinterface_domain::authorization::AuthorizationSnapshot,
    id: EffectIntentId,
    run: String,
    now: u64,
}
fn barrier() {
    std::fs::write(std::env::var_os("WI_SPI_READY").unwrap(), b"ready").unwrap();
    loop {
        std::thread::park();
    }
}
struct BeforeDisposition<H>(H);
impl<H: ExecutorHandler> ExecutorHandler for BeforeDisposition<H> {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let result = self.0.execute(ctx);
        barrier();
        result
    }
}
#[test]
fn reconciliation_crash_child() {
    let Ok(point) = std::env::var("WI_SPI_CHILD") else { return };
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Ambiguous);
    let metadata = Metadata {
        bundle: h.temp.path().join("bundle"),
        db: h.connector.db.clone(),
        snapshot: h.snapshot.clone(),
        id: h.id.clone(),
        run: h.run.to_string(),
        now: h.clock.now().unwrap().epoch_millis().unwrap(),
    };
    std::fs::write(
        std::env::var_os("WI_SPI_METADATA").unwrap(),
        serde_json::to_vec(&metadata).unwrap(),
    )
    .unwrap();
    if point == "provider-commit" {
        h.connector.after_commit = Some(Arc::new(barrier));
    }
    if point.starts_with("fabric-") {
        std::env::set_var("WI_CRASH_POINT", &point);
    }
    runtime.block_on(h.execute());
    if point == "admission-ack" {
        let mut queue = QueueEngine::bootstrap(&h.bundle, Noop, host()).unwrap();
        let bytes = h.case().admission_request.unwrap();
        let request = serde_json::from_slice(&bytes).unwrap();
        let _ = queue.ensure_task(request).unwrap();
        barrier();
    }
    if point.starts_with("reconciliation-") && point != "reconciliation-disposition" {
        std::env::set_var("WI_CRASH_POINT", &point);
        runtime.block_on(h.reconcile());
    }
    if point == "reconciliation-disposition" {
        let mut queue = QueueEngine::bootstrap(&h.bundle, Noop, host()).unwrap();
        worldinterface_runtime::reconciliation::deliver(&mut queue, &h.caller(), &h.clock, &h.id)
            .unwrap();
        queue.shutdown().unwrap();
        let handler = BeforeDisposition(ReconciliationHandler {
            bundle: h.bundle.clone(),
            connector: h.connector.clone(),
            clock: h.clock.clone(),
        });
        let mut engine = actionqueue_runtime::engine::ActionQueueEngine::new(
            actionqueue_runtime::config::RuntimeConfig {
                data_dir: h.bundle.aq_path(),
                ..Default::default()
            },
            handler,
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        runtime.block_on(async {
            let _ = engine.run_until_idle().await.unwrap();
        });
    }
    panic!("missed crash barrier {point}");
}
struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn spi_provider_and_reconciliation_crash_cuts_never_duplicate_commit() {
    for point in [
        "provider-commit",
        "fabric-receipt-history",
        "fabric-receipt-committed",
        "admission-ack",
        "reconciliation-evidence-published",
        "reconciliation-transition-committed",
        "reconciliation-disposition",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("ready");
        let metadata = temp.path().join("metadata.json");
        let log = std::fs::File::create(temp.path().join("child.log")).unwrap();
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "reconciliation_crash_child", "--nocapture"])
                .env("WI_SPI_CHILD", point)
                .env("WI_SPI_READY", &ready)
                .env("WI_CRASH_READY", &ready)
                .env("WI_SPI_METADATA", &metadata)
                .env("TMPDIR", temp.path())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        let start = Instant::now();
        while !ready.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "{point}: {}",
                std::fs::read_to_string(temp.path().join("child.log")).unwrap()
            );
            assert!(start.elapsed() < Duration::from_secs(30), "barrier timeout {point}");
            std::thread::sleep(Duration::from_millis(10));
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let m: Metadata = serde_json::from_slice(&std::fs::read(metadata).unwrap()).unwrap();
        let connector =
            DeterministicConnector::new(m.db, IdempotencyClass::ExternallyKeyed, Mode::Ambiguous);
        assert_eq!(connector.commits(), 1, "{point}");
        let h = Harness {
            temp: tempfile::tempdir_in(temp.path()).unwrap(),
            bundle: Arc::new(StoreBundle::open(&m.bundle).unwrap()),
            connector,
            snapshot: m.snapshot,
            id: m.id,
            run: m.run.parse::<RunId>().unwrap(),
            clock: Clock(Arc::new(AtomicU64::new(m.now + 2000))),
        };
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            h.execute().await;
            h.reconcile().await;
        });
        assert_eq!(h.connector.commits(), 1, "{point}");
        assert_eq!(
            h.case().status,
            worldinterface_domain::reconciliation::CaseStatus::ConfirmedCommitted,
            "{point}"
        );
        assert_eq!(h.effect().outcomes.len(), 1);
        assert_eq!(h.effect().outcomes[0].outcome, ExecutionOutcome::Uncertain);
        let case = h.case();
        let sequence = case.admission_sequence;
        runtime.block_on(h.reconcile());
        assert_eq!(h.case().admission_sequence, sequence);
        drop(h.bundle);
        let reopened = StoreBundle::open(&m.bundle).unwrap();
        assert_eq!(reopened.wi().reconciliation_cases(&worldinterface_domain::agency::AuthenticatedCaller::from_trusted_authentication(case.tenant.clone(),h.snapshot.bindings[0].principal.clone()),&case.intent).unwrap()[0],case);
    }
}
