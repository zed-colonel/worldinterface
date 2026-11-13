#![allow(dead_code)]
use super::connectors::*;
use actionqueue_core::{
    bounded::OpaqueRef as AqRef,
    causal::ControlMutationContext,
    control::{ControlScope, HostControlContext},
    disposition::AttemptDisposition,
    ids::RunId,
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{
    config::{BackoffStrategyConfig, RuntimeConfig},
    engine::ActionQueueEngine,
};
use std::{sync::Arc, time::Instant};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::{AuthorizationSnapshot, VerifiedSnapshot},
    effect::EffectAction,
    reconciliation::ReconciliationCase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::*;
use worldinterface_runtime::{
    admission,
    aq_adapter::QueueEngine,
    authority::TrustedClock,
    completion::EffectAttemptHandler,
    reconciliation::{self, ReconciliationHandler},
    run_discovery,
    storage::StoreBundle,
};
pub fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(AqRef::new("host").unwrap()),
    }
}
pub struct Noop;
impl ExecutorHandler for Noop {
    fn execute(&self, _: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
/// This fixture crosses real process and socket boundaries. Advancing a fake
/// second per AQ tick can expire a command before Submit, or exhaust a case
/// while an authenticated inspect is still outstanding under host load.
#[derive(Clone)]
pub struct AqClock {
    epoch_ms: u64,
    started: Instant,
}
impl AqClock {
    pub fn new() -> Self {
        Self { epoch_ms: super::supervisor_fixture::now(), started: Instant::now() }
    }
}
impl TrustedClock for AqClock {
    fn now(&self) -> worldinterface_runtime::Result<Timestamp> {
        Ok(Timestamp::from_epoch_millis(self.epoch_ms + self.started.elapsed().as_millis() as u64)?)
    }
}
impl actionqueue_core::time::clock::Clock for AqClock {
    fn now(&self) -> u64 {
        TrustedClock::now(self).unwrap().epoch_millis().unwrap() / 1000
    }
}
pub struct Harness {
    pub bundle: Arc<StoreBundle>,
    pub connector: Arc<worldinterface_runtime::supervisor::SupervisorConnector>,
    pub snapshot: AuthorizationSnapshot,
    pub id: EffectIntentId,
    pub run: RunId,
    pub clock: AqClock,
}
impl Harness {
    pub fn new(
        root: &std::path::Path,
        connector: std::sync::Arc<worldinterface_runtime::supervisor::SupervisorConnector>,
        submission: EffectSubmission,
        snapshot: AuthorizationSnapshot,
    ) -> Self {
        let clock = AqClock::new();
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
        for bytes in [b"resolution".as_slice(), b"grant"] {
            bundle.wi().publish(caller.tenant().clone(), bytes).unwrap();
        }
        let input = std::fs::read(root.join("request.json")).unwrap();
        bundle.wi().publish(caller.tenant().clone(), &input).unwrap();
        let egress = Egress::new(bundle.wi());
        let verified = || VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap();
        let id = egress
            .apply(
                &caller,
                clock.now().unwrap(),
                EffectAction::Submit {
                    id: EffectIntentId::generate(),
                    submission: Box::new(submission),
                    snapshot: verified(),
                    link: None,
                },
            )
            .unwrap();
        for action in [
            EffectAction::Resolve { id: id.clone(), snapshot: verified() },
            EffectAction::Authorize { id: id.clone(), snapshot: verified() },
        ] {
            egress.apply(&caller, clock.now().unwrap(), action).unwrap();
        }
        let mut queue = QueueEngine::bootstrap(&bundle, Noop, host()).unwrap();
        admission::prepare(&bundle, &caller, clock.now().unwrap(), &id, verified()).unwrap();
        admission::deliver(&mut queue, &caller, clock.now().unwrap(), &id, verified()).unwrap();
        run_discovery::discover(&queue, &caller, clock.now().unwrap(), &id, 8).unwrap();
        let state = bundle.wi().effect_snapshot(caller.tenant()).unwrap();
        let run = state.effects[&id].dispatches[0].admission.as_ref().unwrap().observed_runs[0]
            .as_str()
            .parse()
            .unwrap();
        queue.shutdown().unwrap();
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
    pub fn context(&self) -> ExecutorContext {
        use actionqueue_executor_local::handler::{
            AttemptMetadata, CancellationContext, HandlerInput,
        };
        let effect = self.effect();
        let admission = effect.dispatches[0].admission.as_ref().unwrap();
        let request =
            worldinterface_runtime::admission::load_request(self.bundle.wi(), admission).unwrap();
        ExecutorContext {
            children: None,
            input: HandlerInput {
                resume_context: None,
                causal_context: Some(request.causal_context().clone()),
                run_id: self.run,
                attempt_id: actionqueue_core::ids::AttemptId::new(),
                payload: request.task_spec().task_payload().bytes().to_vec(),
                metadata: AttemptMetadata {
                    failure_attempt_count: 0,
                    max_attempts: 3,
                    attempt_number: 1,
                    timeout_secs: Some(30),
                    safety_level: actionqueue_core::task::safety::SafetyLevel::Idempotent,
                },
                cancellation_context: CancellationContext::new(),
            },
        }
    }
    pub fn handler(
        &self,
    ) -> EffectAttemptHandler<
        Arc<worldinterface_runtime::supervisor::SupervisorConnector>,
        Evidence,
        AqClock,
    > {
        EffectAttemptHandler {
            bundle: self.bundle.clone(),
            port: self.connector.clone(),
            evidence: Evidence(self.snapshot.clone()),
            clock: self.clock.clone(),
        }
    }
    pub async fn execute(&self) {
        let mut engine = ActionQueueEngine::new(
            RuntimeConfig {
                data_dir: self.bundle.aq_path(),
                tick_interval: std::time::Duration::from_millis(1),
                backoff_strategy: BackoffStrategyConfig::Fixed {
                    interval: std::time::Duration::from_secs(1),
                },
                ..Default::default()
            },
            self.handler(),
        )
        .bootstrap_with_clock(self.clock.clone())
        .unwrap()
        .with_host(host());
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
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!(
            "effect did not settle: {:?}, WI {:?}",
            engine.projection().get_run_state(&self.run),
            self.effect().phase
        );
    }
    pub async fn reconcile(&self) {
        self.reconcile_with_clock(self.clock.clone()).await;
    }
    pub async fn reconcile_with_clock<C: TrustedClock + Send + Sync + 'static>(&self, clock: C) {
        let mut queue = QueueEngine::bootstrap(&self.bundle, Noop, host()).unwrap();
        reconciliation::deliver(&mut queue, &self.caller(), &self.clock, &self.id).unwrap();
        queue.shutdown().unwrap();
        let handler = ReconciliationHandler {
            bundle: self.bundle.clone(),
            connector: self.connector.clone(),
            clock,
        };
        let mut engine = ActionQueueEngine::new(
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
        .bootstrap_with_clock(self.clock.clone())
        .unwrap()
        .with_host(host());
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
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
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!(
            "reconciliation did not settle: {:?}; AQ {:?}",
            self.case().status,
            engine.projection().run_instances().map(|r| (r.id(), r.state())).collect::<Vec<_>>()
        );
    }
}
