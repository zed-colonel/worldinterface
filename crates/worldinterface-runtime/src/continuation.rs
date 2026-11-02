//! Authenticated intake and one stable signal encoding. AQ has no principal filter.
use crate::{
    authority::{caller, Authentication},
    Result,
};
use actionqueue_core::{
    continuation::*,
    ids::{CorrelationId, SignalId},
};
use actionqueue_executor_local::handler::ExecutorHandler;
use worldinterface_domain::{agency::AuthenticatedCaller, continuation::*};
use worldinterface_protocol::{
    canonical_data, ArtifactRef, ContentHash, EffectIntentId, OpaqueRef,
};
use worldinterface_storage::{StoreKind, StoreRoot};

/// Trusted host policy binds stream/generation and current producer authority.
/// The request cannot select AQ namespaces, audiences or actor attribution.
pub trait EventAuthorization {
    fn authorize(
        &self,
        caller: &AuthenticatedCaller,
        kind: EventKind,
        source: &OpaqueRef,
    ) -> Result<EventRoute>;
    fn authorize_read(&self, caller: &AuthenticatedCaller, event: &DurableEvent) -> bool;
    /// Separate run disclosure decision: hosts may grant an operator access to
    /// selected runs without granting event readers every run on that route.
    fn authorize_run_read(
        &self,
        caller: &AuthenticatedCaller,
        run: &worldinterface_storage::flow::FlowRun,
    ) -> bool {
        caller.tenant() == &run.tenant && caller.principal() == &run.principal
    }
}
pub struct EventIntake<'a, A, P> {
    pub root: &'a StoreRoot,
    pub authentication: &'a A,
    pub policy: &'a P,
}
impl<A: Authentication, P: EventAuthorization> EventIntake<'_, A, P> {
    pub fn provider_callback(
        &self,
        session: &A::Session,
        intent: &EffectIntentId,
        revision: u64,
        artifact: ArtifactRef,
    ) -> Result<OpaqueRef> {
        let caller = caller(self.authentication, session)?;
        let state = self.root.effect_snapshot(caller.tenant())?;
        let effect = state.effects.get(intent).ok_or("callback effect unavailable")?;
        let source: OpaqueRef = intent.as_str().to_string().try_into()?;
        let route = self.policy.authorize(&caller, EventKind::ProviderCallback, &source)?;
        if route.kind != EventKind::ProviderCallback
            || route.audience != effect.agency.binding.principal
            || effect.dispatches.is_empty()
        {
            return Err("callback is not bound to a dispatched effect/provider".into());
        }
        self.record(&caller, route, source, revision, artifact)
    }
    pub fn worker_observation(
        &self,
        session: &A::Session,
        observation: worldinterface_worker::ports::Observation,
    ) -> Result<OpaqueRef> {
        let caller = caller(self.authentication, session)?;
        for v in [&observation.provider, &observation.run, &observation.event] {
            worldinterface_worker::nonempty(v)?;
        }
        if observation.sequence == 0 {
            return Err("worker observation sequence must be positive".into());
        }
        // Stable per-provider/run/event identity, independent of observation content.
        let source: OpaqueRef = format!(
            "wi-worker:{}",
            ContentHash::of(&canonical_data(&(
                "wi-worker-observation-1",
                &observation.provider,
                &observation.run,
                &observation.event,
            ))?)
            .as_str()
        )
        .try_into()?;
        let route = self.policy.authorize(&caller, EventKind::WorkerObservation, &source)?;
        if route.kind != EventKind::WorkerObservation {
            return Err("worker route kind mismatch".into());
        }
        if let Some(worker) = self.root.worker_snapshot(
            caller.tenant(),
            &observation.provider.clone().try_into()?,
            &observation.run.clone().try_into()?,
        )? {
            return crate::worker::observation::commit(
                self.root,
                &worker.binding,
                route,
                observation,
            );
        }
        let artifact =
            self.root.publish(caller.tenant().clone(), &canonical_data(&observation)?)?;
        self.record(&caller, route, source, observation.sequence, artifact)
    }
    fn record(
        &self,
        caller: &AuthenticatedCaller,
        route: EventRoute,
        source: OpaqueRef,
        revision: u64,
        artifact: ArtifactRef,
    ) -> Result<OpaqueRef> {
        if route.tenant != *caller.tenant()
            || artifact.scope != *caller.tenant()
            || !matches!(route.kind, EventKind::ProviderCallback | EventKind::WorkerObservation)
        {
            return Err("event intake scope mismatch".into());
        }
        let bytes = self.root.read_artifact(&artifact)?;
        let event =
            DurableEvent::new(route, source, revision, ContentHash::of(&bytes), Some(artifact))?;
        let id = event.id.clone();
        self.root.record_event(event)?;
        Ok(id)
    }
}

pub fn signal_request(event: &DurableEvent) -> Result<AdmitSignalRequest> {
    event.validate()?;
    Ok(AdmitSignalRequest::new(
        SignalId::new(event.id.as_str())?,
        SignalNamespace::new(NAMESPACE)?,
        SignalKind::new(event.route.kind.name())?,
        Some(CorrelationId::new(event.route.correlation_id()?)?),
        None,
        Some(actionqueue_core::bounded::OpaqueRef::new(event.id.as_str())?),
        None,
        None,
        None,
    )?)
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SignalDeliveryStatus {
    pub acknowledged: u32,
    /// Stable event identities and actionable AQ classifications. Remain pending.
    pub pending: Vec<(OpaqueRef, String)>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignalCapacity {
    pub resident_identities: usize,
    pub resident_bytes: usize,
    pub identity_limit: usize,
    pub byte_limit: usize,
    /// Hosts must alert at 80% of either lifetime budget (including zero capacity).
    pub alarm: bool,
    pub capacity_rejections: u64,
}
impl SignalCapacity {
    pub(crate) fn from_statistics(
        s: actionqueue_storage::recovery::signals::SignalStatistics,
        limits: actionqueue_core::limits::SignalLimits,
    ) -> Self {
        let identities = s.retained.saturating_add(s.retired);
        Self {
            resident_identities: identities,
            resident_bytes: s.bytes,
            identity_limit: limits.identities,
            byte_limit: limits.bytes,
            alarm: (identities as u128) * 100 >= (limits.identities as u128) * 80
                || (s.bytes as u128) * 100 >= (limits.bytes as u128) * 80,
            capacity_rejections: s.capacity_rejections,
        }
    }
}
#[derive(Default)]
pub(crate) struct SignalScan {
    fabric: Option<OpaqueRef>,
    flow: Option<OpaqueRef>,
}
pub(crate) fn drain<H: ExecutorHandler + 'static>(
    root: &StoreRoot,
    engine: &mut actionqueue_runtime::engine::BootstrappedEngine<H>,
    scan: &mut SignalScan,
) -> Result<SignalDeliveryStatus> {
    let mut status = SignalDeliveryStatus::default();
    for kind in [StoreKind::Fabric, StoreKind::Flow] {
        let cursor = match kind {
            StoreKind::Fabric => &mut scan.fabric,
            _ => &mut scan.flow,
        };
        let page = root.pending_signals(kind, cursor.as_ref(), 64)?;
        *cursor = if page.len() == 64 { page.last().map(|o| o.event.id.clone()) } else { None };
        for obligation in page {
            let expected = serde_json::to_vec(&signal_request(&obligation.event)?)?;
            let request = if let Some(bytes) = obligation.request {
                if bytes != expected {
                    return Err("outbox request differs from event".into());
                }
                bytes
            } else {
                root.prepare_signal_request(kind, obligation.event.id.clone(), expected.clone())?;
                expected
            };
            let worker = obligation.event.route.kind == EventKind::WorkerObservation;
            if worker {
                worldinterface_storage::worker_boundary("worker-wake-before-publish");
            }
            match engine.admit_signal(serde_json::from_slice(&request)?) {
                Ok(outcome) => {
                    if worker {
                        worldinterface_storage::worker_boundary("worker-wake-published");
                    }
                    worldinterface_storage::continuation_signal_admitted();
                    if worker {
                        worldinterface_storage::worker_boundary("worker-wake-before-ack");
                    }
                    root.acknowledge_signal(
                        kind,
                        obligation.event.id,
                        ContentHash::of(&request),
                        outcome.sequence().get(),
                    )?;
                    if worker {
                        worldinterface_storage::worker_boundary("worker-wake-acknowledged");
                    }
                    status.acknowledged += 1;
                }
                Err(error) => {
                    root.record_signal_rejection(
                        kind,
                        obligation.event.id.clone(),
                        error.code().into(),
                    )?;
                    status.pending.push((obligation.event.id, error.code().into()));
                }
            }
        }
    }
    Ok(status)
}
pub(crate) fn validate_references(
    root: &StoreRoot,
    p: &actionqueue_storage::recovery::reducer::ReplayReducer,
) -> Result<()> {
    for kind in [StoreKind::Fabric, StoreKind::Flow] {
        let mut cursor = None;
        loop {
            let page = root.signal_obligations(kind, cursor.as_ref(), 256)?;
            if page.is_empty() {
                break;
            }
            for o in &page {
                let expected = serde_json::to_vec(&signal_request(&o.event)?)?;
                if o.request.as_ref().is_some_and(|b| b != &expected)
                    || o.request_digest.as_ref().is_some_and(|d| d != &ContentHash::of(&expected))
                {
                    return Err("frozen event request mismatch".into());
                }
                if let Some(sequence) = o.sequence {
                    let signal = p
                        .signals()
                        .by_sequence(actionqueue_core::ids::SignalSequence::new(sequence))
                        .ok_or("acknowledged signal missing from paired AQ store")?;
                    let actual = signal.envelope();
                    let expected = signal_request(&o.event)?.envelope(
                        &SignalIngressContext {
                            control_context: actual.control_context.clone(),
                            ..Default::default()
                        },
                        actual.received_at,
                    );
                    if actual != &expected {
                        return Err("paired AQ event signal changed".into());
                    }
                }
            }
            cursor = page.last().map(|o| o.event.id.clone());
        }
    }
    Ok(())
}
