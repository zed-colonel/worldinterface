#![allow(dead_code)]
use super::{
    connectors::*,
    spi_harness::{admit, host, submit, AqClock, Noop},
};
use actionqueue_core::ids::RunId;
use actionqueue_executor_local::handler::ExecutorHandler;
use actionqueue_runtime::{
    config::{BackoffStrategyConfig, RuntimeConfig},
    engine::{ActionQueueEngine, BootstrappedEngine},
};
use std::{collections::BTreeMap, sync::Arc, time::Instant};
use worldinterface_domain::{
    agency::AuthenticatedCaller, authorization::AuthorizationSnapshot,
    reconciliation::ReconciliationCase,
};
use worldinterface_protocol::*;
use worldinterface_runtime::{
    aq_adapter::QueueEngine,
    authority::TrustedClock,
    completion::EffectAttemptHandler,
    reconciliation::{self, ReconciliationHandler},
    storage::StoreBundle,
};
pub struct Harness {
    pub bundle: Arc<StoreBundle>,
    pub connector: Arc<worldinterface_runtime::supervisor::SupervisorConnector>,
    pub snapshot: AuthorizationSnapshot,
    pub id: EffectIntentId,
    pub run: RunId,
    pub clock: Clock,
}
impl Harness {
    pub fn new(
        root: &std::path::Path,
        connector: std::sync::Arc<worldinterface_runtime::supervisor::SupervisorConnector>,
        submission: EffectSubmission,
        snapshot: AuthorizationSnapshot,
    ) -> Self {
        let clock = Clock::new();
        let bundle = Arc::new(
            StoreBundle::initialize(
                &root.join("bundle"),
                worldinterface_storage::StoreProfile {
                    instance: InstanceId::generate(),
                    created_at: clock.now().unwrap(),
                    platform: false,
                },
            )
            .unwrap(),
        );
        let caller = AuthenticatedCaller::from_trusted_authentication(
            snapshot.bindings[0].tenant.clone(),
            snapshot.bindings[0].principal.clone(),
        );
        let input = std::fs::read(root.join("request.json")).unwrap();
        bundle.wi().publish(caller.tenant().clone(), &input).unwrap();
        let id = submit(bundle.wi(), &clock, submission, &snapshot);
        let run = admit(&bundle, &clock, &snapshot, &id);
        Self { bundle, connector, snapshot, id, run, clock }
    }
    pub fn caller(&self) -> AuthenticatedCaller {
        AuthenticatedCaller::from_trusted_authentication(
            self.snapshot.bindings[0].tenant.clone(),
            self.snapshot.bindings[0].principal.clone(),
        )
    }
    pub fn effect(&self) -> worldinterface_domain::effect::EffectAggregate {
        self.bundle.wi().effect_snapshot(self.caller().tenant()).unwrap().effects[&self.id].clone()
    }
    pub fn case(&self) -> ReconciliationCase {
        self.bundle
            .wi()
            .reconciliation_cases(&self.caller(), &self.id)
            .unwrap()
            .last()
            .unwrap()
            .clone()
    }
    pub fn handler(
        &self,
    ) -> EffectAttemptHandler<
        Arc<worldinterface_runtime::supervisor::SupervisorConnector>,
        Evidence,
        Clock,
    > {
        EffectAttemptHandler {
            bundle: self.bundle.clone(),
            port: self.connector.clone(),
            evidence: Evidence(self.snapshot.clone()),
            clock: self.clock.clone(),
        }
    }
    fn advance_to(&self, seconds: u64) {
        let current = self.clock.now().unwrap().epoch_millis().unwrap();
        self.clock.advance((seconds * 1000).saturating_sub(current));
    }
    fn engine<H: ExecutorHandler + 'static>(&self, handler: H) -> BootstrappedEngine<H, AqClock> {
        ActionQueueEngine::new(
            RuntimeConfig {
                data_dir: self.bundle.aq_path(),
                tick_interval: std::time::Duration::from_millis(1),
                backoff_strategy: BackoffStrategyConfig::Fixed {
                    interval: std::time::Duration::from_secs(1),
                },
                ..Default::default()
            },
            handler,
        )
        .bootstrap_with_clock(AqClock(self.clock.clone()))
        .unwrap()
        .with_host(host())
    }
    fn release_timer(&self, run: &actionqueue_core::run::run_instance::RunInstance) {
        match run.state() {
            actionqueue_core::run::state::RunState::Scheduled => {
                self.advance_to(run.scheduled_at())
            }
            actionqueue_core::run::state::RunState::RetryWait => {
                self.advance_to(run.last_state_change_at() + 1)
            }
            _ => {}
        }
    }
    pub async fn execute(&self) {
        let mut engine = self.engine(self.handler());
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        while Instant::now() < deadline {
            let _ = engine.tick().await.unwrap();
            if engine.projection().get_run_state(&self.run).is_some_and(|s| s.is_terminal()) {
                if self.effect().phase == worldinterface_domain::Phase::Succeeded {
                    assert_eq!(
                        engine.projection().get_run_state(&self.run),
                        Some(&actionqueue_core::run::state::RunState::Completed)
                    );
                }
                engine.shutdown().unwrap();
                return;
            }
            // Release only an actual AQ timer, once. Never consume virtual
            // seconds merely because the executor has not picked up work yet.
            for run in engine.projection().run_instances().filter(|r| r.id() == self.run) {
                self.release_timer(run);
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!(
            "effect did not settle: {:?}, WI {:?}",
            engine.projection().get_run_state(&self.run),
            self.effect().phase
        );
    }
    pub async fn reconcile(&self) {
        let mut queue = QueueEngine::bootstrap(&self.bundle, Noop, host()).unwrap();
        reconciliation::deliver(&mut queue, &self.caller(), &self.clock, &self.id).unwrap();
        queue.shutdown().unwrap();
        let handler = ReconciliationHandler {
            bundle: self.bundle.clone(),
            connector: self.connector.clone(),
            clock: self.clock.clone(),
        };
        let mut engine = self.engine(handler);
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        let mut waits = BTreeMap::new();
        while Instant::now() < deadline {
            let _ = engine.tick().await.unwrap();
            if self.case().status.terminal()
                && engine
                    .projection()
                    .run_instances()
                    .filter(|r| r.id() != self.run)
                    .all(|r| r.state().is_terminal())
            {
                // Allow the handler's returned disposition to be committed by AQ.
                for _ in 0..5 {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    let _ = engine.tick().await.unwrap();
                }
                engine.shutdown().unwrap();
                return;
            }
            for run in engine.projection().run_instances().filter(|r| r.id() != self.run) {
                self.release_timer(run);
                if run.state() == actionqueue_core::run::state::RunState::Awaiting {
                    if let Some(wait) = engine.projection().waits().active(run.id()) {
                        // The supervisor drives inspect on a real thread. Pace
                        // each distinct polling wait in real time so a busy
                        // backend cannot exhaust the virtual policy budget.
                        let first_seen =
                            waits.entry(wait.spec.wait_id()).or_insert_with(Instant::now);
                        if first_seen.elapsed() >= std::time::Duration::from_secs(1) {
                            self.advance_to(wait.spec.deadline().unwrap().at);
                        }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!(
            "reconciliation did not settle: {:?}; AQ {:?}",
            self.case().status,
            engine.projection().run_instances().map(|r| (r.id(), r.state())).collect::<Vec<_>>()
        );
    }
}
