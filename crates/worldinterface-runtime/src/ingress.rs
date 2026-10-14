//! Production composition for the verified ingress journal and pinned AQ admission API.
pub mod http;
pub mod streaming;
use crate::Result;
use actionqueue_core::{
    admission::{EnsureTaskOutcome, EnsureTaskRequest},
    continuation::{AdmitSignalOutcome, AdmitSignalRequest, SignalKind, SignalNamespace},
    ids::{CorrelationId, SignalId, TaskId},
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::engine::BootstrappedEngine;
use std::{
    collections::BTreeMap,
    io::Read,
    sync::{Arc, Mutex},
};
use worldinterface_domain::ingress::{AcknowledgmentSignal, IngressResult, QueueObligation};
use worldinterface_ingress::{
    transport::{
        http::validate_http, streaming::StreamSession, ProviderCredentials, ValidatedProviderEvent,
    },
    AdmissionIds, AuthenticatedScope, DeliveryToken, Ingress, IssuedDelivery,
};
use worldinterface_protocol::{
    BindingStatus, BindingVersion, ContentHash, DeliveryAcknowledgment, DeliveryId, DeliveryState,
    ObservationId, OpaqueRef, PrincipalId, ReceiptId,
};
use worldinterface_storage::StoreRoot;

/// The caller constructs this service from trusted host configuration. Provider bytes
/// cannot select the tenant, credentials, bindings, or subscriber authority.
pub struct IngressRuntime<'a> {
    root: &'a StoreRoot,
    tenant: OpaqueRef,
    scan_cursor: Mutex<Option<DeliveryId>>,
}
impl<'a> IngressRuntime<'a> {
    pub fn new(root: &'a StoreRoot, tenant: OpaqueRef) -> Self {
        Self { root, tenant, scan_cursor: Mutex::new(None) }
    }
    pub fn accept_http(
        &self,
        credentials: &ProviderCredentials,
        exact_body: &[u8],
        signature: &[u8],
        now: u64,
    ) -> Result<IngressResult> {
        self.accept(validate_http(exact_body, signature, credentials, now)?, now)
    }
    pub fn accept_frame(
        &self,
        session: &StreamSession,
        exact_frame: &[u8],
        signature: &[u8],
        now: u64,
    ) -> Result<IngressResult> {
        self.accept(session.validate_stream(exact_frame, signature, now)?, now)
    }
    fn accept(&self, event: ValidatedProviderEvent, now: u64) -> Result<IngressResult> {
        let profile = event.profile();
        if profile.scope.tenant != self.tenant {
            return Err("configured AQ tenant and provider tenant differ".into());
        }
        let state = self.root.ingress_snapshot(&self.tenant)?;
        let bindings: Vec<BindingVersion> = state
            .bindings
            .values()
            .filter(|b| {
                b.profile == *profile
                    && b.status == worldinterface_protocol::BindingStatus::Active
                    && b.expires_at_ms > now
                    && event.allowed_audiences().contains(&b.subscriber)
            })
            .map(|b| BindingVersion { id: b.id.clone(), revision: b.revision })
            .collect();
        if bindings.is_empty() {
            return Err("no active binding".into());
        }
        let ids = AdmissionIds {
            observation: ObservationId::generate(),
            receipt: ReceiptId::generate(),
            deliveries: bindings.iter().map(|_| DeliveryId::generate()).collect(),
        };
        let queue = ids
            .deliveries
            .iter()
            .map(|id| queue_request(id, 0, None))
            .collect::<Result<Vec<_>>>()?;
        Ingress::new(self.root).admit_provider_event_queued(event, bindings, ids, queue, now)
    }
    /// Replays unchanged requests. A failed local acknowledgment is always treated as
    /// possibly committed in AQ; the next call retries the same persisted bytes.
    /// Retain this runtime across polls so idle scans rotate without journal writes.
    /// A restart resumes after the last durable progress and may repeat one rotation.
    pub fn dispatch_pending(
        &self,
        aq: &mut impl AqIngressPort,
        limit: usize,
        now: u64,
    ) -> Result<usize> {
        if aq.bound_tenant() != &self.tenant {
            return Err("AQ bundle tenant mismatch".into());
        }
        if limit == 0 || limit > 1000 {
            return Err("dispatch bound".into());
        }
        let state = self.root.ingress_snapshot(&self.tenant)?;
        let mut pending: Vec<_> = state
            .queue
            .values()
            .filter(|q| {
                state.deliveries.get(&q.delivery).is_some_and(|d| d.state == DeliveryState::Pending)
            })
            .cloned()
            .collect();
        let cursor = self
            .scan_cursor
            .lock()
            .map_err(|_| "dispatch cursor poisoned")?
            .clone()
            .or(state.dispatch_cursor.clone());
        if let Some(cursor) = &cursor {
            let first_after = pending.partition_point(|q| &q.delivery <= cursor);
            pending.rotate_left(first_after);
        }
        // Acknowledgments are already durable subscriber work and must progress
        // even when an earlier delivery is paused or denied disclosure.
        let acknowledgments: Vec<_> = state
            .acknowledgments_to_signal
            .values()
            .filter(|s| s.sequence.is_none())
            .take(limit)
            .cloned()
            .collect();
        let mut done = 0;
        for signal in acknowledgments {
            if ContentHash::of(&signal.request) != signal.digest {
                return Err("acknowledgment request integrity mismatch".into());
            }
            let request: AdmitSignalRequest = serde_json::from_slice(&signal.request)?;
            let sequence = aq.admit_wake(request)?.sequence;
            self.root.ingress_transact_dispatch(
                &self.tenant,
                now,
                &worldinterface_domain::ingress::IngressCommand::RecordAcknowledgmentSignal {
                    id: signal.delivery,
                    sequence,
                },
            )?;
            done += 1;
        }
        for (scanned, q) in pending.into_iter().enumerate() {
            if done >= limit || scanned >= limit - done {
                break;
            }
            // Idle scans rotate in memory. Durable progress records its cursor
            // atomically below; after a restart at most one rotation is repeated.
            *self.scan_cursor.lock().map_err(|_| "dispatch cursor poisoned")? =
                Some(q.delivery.clone());
            match reconcile_delivery(self.root, &self.tenant, &q.delivery, now)? {
                DeliveryReadiness::Ready => {}
                DeliveryReadiness::Closed => {
                    continue;
                }
                DeliveryReadiness::Unavailable => continue,
            }
            let mut q = q;
            // AQ admission and the WI journal are separate durable authorities. Another
            // dispatcher may replace a terminal generation during either AQ call.
            for _ in 0..4 {
                if ContentHash::of(&q.task_request) != q.task_digest
                    || ContentHash::of(&q.signal_request) != q.signal_digest
                {
                    return Err("queue request integrity mismatch".into());
                }
                let wake: AdmitSignalRequest = serde_json::from_slice(&q.signal_request)?;
                if self.record_retired_wake(aq, &q, &wake, now)? {
                    break;
                }
                q = if q.signal_sequence.is_some() {
                    let task: EnsureTaskRequest = serde_json::from_slice(&q.task_request)?;
                    if !aq.delivery_task_terminal(task.task_spec().id())? {
                        break;
                    }
                    let current = self.root.ingress_snapshot(&self.tenant)?;
                    let subscriber =
                        &current.deliveries.get(&q.delivery).ok_or("unknown delivery")?.subscriber;
                    if !aq.subscriber_registered(subscriber) {
                        break;
                    }
                    if reconcile_delivery(self.root, &self.tenant, &q.delivery, now)?
                        != DeliveryReadiness::Ready
                    {
                        break;
                    }
                    let next = queue_request(
                        &q.delivery,
                        q.generation.checked_add(1).ok_or("recovery generation overflow")?,
                        Some(&q),
                    )?;
                    let recovered = self.root.ingress_transact_dispatch(
                        &self.tenant,
                        now,
                        &worldinterface_domain::ingress::IngressCommand::RecoverQueue {
                            id: q.delivery.clone(),
                            expected_generation: q.generation,
                            replacement: Box::new(next.clone()),
                        },
                    );
                    if matches!(recovered, Ok(IngressResult::QueueRecoveryExhausted)) {
                        break;
                    }
                    if let Err(error) = recovered {
                        if reconcile_delivery(self.root, &self.tenant, &q.delivery, now)?
                            != DeliveryReadiness::Ready
                        {
                            break;
                        }
                        let current = self
                            .root
                            .ingress_snapshot(&self.tenant)?
                            .queue
                            .get(&q.delivery)
                            .cloned()
                            .ok_or("unknown queue obligation")?;
                        if current.generation != q.generation {
                            q = current;
                            continue;
                        }
                        return Err(error);
                    }
                    next
                } else {
                    q
                };
                if reconcile_delivery(self.root, &self.tenant, &q.delivery, now)?
                    != DeliveryReadiness::Ready
                {
                    break;
                }
                if ContentHash::of(&q.task_request) != q.task_digest
                    || ContentHash::of(&q.signal_request) != q.signal_digest
                {
                    return Err("queue request integrity mismatch".into());
                }
                let task: EnsureTaskRequest = serde_json::from_slice(&q.task_request)?;
                let signal: AdmitSignalRequest = serde_json::from_slice(&q.signal_request)?;
                if !q.task_admitted {
                    let _ = aq.ensure_delivery_task(task)?;
                    let recorded = self.root.ingress_transact_dispatch(
                        &self.tenant,
                        now,
                        &worldinterface_domain::ingress::IngressCommand::RecordQueueTask {
                            id: q.delivery.clone(),
                            generation: q.generation,
                            task_digest: q.task_digest.clone(),
                        },
                    )?;
                    if recorded == IngressResult::StaleQueueGeneration {
                        q = self
                            .root
                            .ingress_snapshot(&self.tenant)?
                            .queue
                            .get(&q.delivery)
                            .cloned()
                            .ok_or("unknown queue obligation")?;
                        continue;
                    }
                }
                let sequence = aq.admit_wake(signal)?.sequence;
                // Host retirement may have happened during an interrupted AQ
                // admission, before its local acknowledgment. Inspect AQ's
                // durable retention state rather than trusting AlreadyExists.
                let wake: AdmitSignalRequest = serde_json::from_slice(&q.signal_request)?;
                if self.record_retired_wake(aq, &q, &wake, now)? {
                    break;
                }
                let recorded = self.root.ingress_transact_dispatch(
                    &self.tenant,
                    now,
                    &worldinterface_domain::ingress::IngressCommand::RecordQueueSignal {
                        id: q.delivery.clone(),
                        generation: q.generation,
                        signal_digest: q.signal_digest.clone(),
                        sequence,
                    },
                )?;
                if recorded == IngressResult::StaleQueueGeneration {
                    q = self
                        .root
                        .ingress_snapshot(&self.tenant)?
                        .queue
                        .get(&q.delivery)
                        .cloned()
                        .ok_or("unknown queue obligation")?;
                    continue;
                }
                done += 1;
                break;
            }
        }
        Ok(done)
    }
    fn record_retired_wake(
        &self,
        aq: &mut impl AqIngressPort,
        queue: &QueueObligation,
        wake: &AdmitSignalRequest,
        now: u64,
    ) -> Result<bool> {
        let Some(sequence) = aq.retired_wake_sequence(wake)? else { return Ok(false) };
        self.root.ingress_transact_dispatch(
            &self.tenant,
            now,
            &worldinterface_domain::ingress::IngressCommand::RecordQueueWakeRetired {
                id: queue.delivery.clone(),
                generation: queue.generation,
                signal_digest: queue.signal_digest.clone(),
                sequence,
            },
        )?;
        Ok(true)
    }
    pub fn claim(
        &self,
        subscriber: &AuthenticatedScope,
        id: &DeliveryId,
        now: u64,
    ) -> Result<Option<IssuedDelivery>> {
        if subscriber.tenant != self.tenant {
            return Err("subscriber tenant mismatch".into());
        }
        Ingress::new(self.root).claim_due_delivery(subscriber, id, DeliveryToken::generate(), now)
    }
    pub fn acknowledge(
        &self,
        subscriber: &AuthenticatedScope,
        id: &DeliveryId,
        token: &DeliveryToken,
        now: u64,
    ) -> Result<DeliveryAcknowledgment> {
        if subscriber.tenant != self.tenant {
            return Err("subscriber tenant mismatch".into());
        }
        Ingress::new(self.root).acknowledge_queued(
            subscriber,
            id,
            token,
            acknowledgment_request(id)?,
            now,
        )
    }
}

/// The journal is the authority for expiration. Recheck it at each boundary
/// before creating AQ work because a dispatcher may hold an older snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryReadiness {
    Ready,
    Unavailable,
    Closed,
}

fn reconcile_delivery(
    root: &StoreRoot,
    tenant: &OpaqueRef,
    id: &DeliveryId,
    now: u64,
) -> Result<DeliveryReadiness> {
    let state = root.ingress_snapshot(tenant)?;
    let readiness = delivery_readiness(&state, id, now)?;
    let delivery = state.deliveries.get(id).ok_or("unknown delivery")?;
    let effective_now = now.max(state.last_at_ms);
    if delivery.state == DeliveryState::Pending && effective_now >= delivery.expires_at_ms {
        root.ingress_transact_dispatch(
            tenant,
            effective_now,
            &worldinterface_domain::ingress::IngressCommand::Expire { id: id.clone() },
        )?;
    }
    Ok(readiness)
}

fn delivery_readiness(
    state: &worldinterface_domain::ingress::IngressState,
    id: &DeliveryId,
    now: u64,
) -> Result<DeliveryReadiness> {
    let delivery = state.deliveries.get(id).ok_or("unknown delivery")?;
    if delivery.state != DeliveryState::Pending {
        return Ok(DeliveryReadiness::Closed);
    }
    let effective_now = now.max(state.last_at_ms);
    if effective_now >= delivery.expires_at_ms {
        return Ok(DeliveryReadiness::Closed);
    }
    // Token invalidation forbids every future claim, including the first one.
    // Dispatch must not admit or recover work that can no longer reach a subscriber.
    if delivery.token_invalidated {
        return Ok(DeliveryReadiness::Unavailable);
    }
    let binding = state.bindings.get(&delivery.binding.id).ok_or("missing delivery binding")?;
    Ok(match binding.status {
        _ if binding.expires_at_ms <= effective_now => DeliveryReadiness::Unavailable,
        BindingStatus::Active => {
            let references_available =
                worldinterface_domain::ingress::required_references_disclosable(state, delivery)?;
            let disclosed = worldinterface_domain::ingress::disclose(
                state,
                delivery,
                &delivery.subscriber,
                effective_now,
            )
            .is_ok();
            if !references_available || !disclosed {
                DeliveryReadiness::Unavailable
            } else {
                DeliveryReadiness::Ready
            }
        }
        BindingStatus::Suspended => DeliveryReadiness::Unavailable,
        BindingStatus::Revoked => DeliveryReadiness::Unavailable,
    })
}

fn read_bounded(mut source: impl Read, max_bytes: u32) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    source.by_ref().take(u64::from(max_bytes) + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes as usize {
        return Err("ingress body exceeds configured bound".into());
    }
    Ok(bytes)
}

pub(crate) fn delivery_task_id(id: &DeliveryId, generation: u32) -> Result<TaskId> {
    if generation == 0 {
        Ok(TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str())?))
    } else {
        use sha2::Digest;
        let digest = sha2::Sha256::digest(
            format!("wi-delivery-recovery-v1:{}:{generation}", id.as_str()).as_bytes(),
        );
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x50;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Ok(TaskId::from_uuid(uuid::Uuid::from_bytes(bytes)))
    }
}

fn queue_request(
    id: &DeliveryId,
    generation: u32,
    retained_wake: Option<&QueueObligation>,
) -> Result<QueueObligation> {
    let task_id = delivery_task_id(id, generation)?;
    let mut constraints = TaskConstraints::default();
    // An interrupted physical attempt is counted by AQ. WI's delivery policy
    // remains the bound on actual subscriber attempts.
    constraints.set_max_attempts(100)?;
    let spec = TaskSpec::new(
        task_id,
        TaskPayload::with_content_type(
            format!("delivery:{}", id.as_str()).into_bytes(),
            "application/vnd.worldinterface.delivery+json;version=1",
        ),
        RunPolicy::Once,
        constraints,
        TaskMetadata::default(),
    )?;
    let task = EnsureTaskRequest::for_task(spec, vec![])?;
    let signal_request = if let Some(old) = retained_wake {
        if old.delivery != *id || ContentHash::of(&old.signal_request) != old.signal_digest {
            return Err("queue wake integrity mismatch".into());
        }
        // Signals are non-consuming. Replacement tasks replay the same delivery
        // wake instead of spending another lifetime AQ identity per generation.
        // Preserve persisted bytes, including older task-correlated requests.
        old.signal_request.clone()
    } else {
        serde_json::to_vec(&AdmitSignalRequest::new(
            SignalId::new(format!("wi-delivery-{}", id.as_str()))?,
            SignalNamespace::new("wi_ingress")?,
            SignalKind::new("delivery")?,
            Some(CorrelationId::new(format!("task/{task_id}"))?),
            None,
            Some(actionqueue_core::bounded::OpaqueRef::new(format!("delivery:{}", id.as_str()))?),
            None,
            None,
            None,
        )?)?
    };
    // AQ validates and computes its own CanonicalAdmissionV2 digest. These are
    // stable serde bytes for replay, not a WI replacement admission digest.
    let task_request = serde_json::to_vec(&task)?;
    Ok(QueueObligation {
        version: 1,
        delivery: id.clone(),
        task_digest: ContentHash::of(&task_request),
        signal_digest: ContentHash::of(&signal_request),
        task_request,
        signal_request,
        task_admitted: false,
        signal_sequence: None,
        retired_wake_sequence: None,
        generation,
        prior: vec![],
    })
}

fn acknowledgment_request(id: &DeliveryId) -> Result<AcknowledgmentSignal> {
    let request = AdmitSignalRequest::new(
        SignalId::new(format!("wi-ack-{}", id.as_str()))?,
        SignalNamespace::new("wi_ingress")?,
        SignalKind::new("acknowledgment")?,
        Some(CorrelationId::new(format!("delivery/{}", id.as_str()))?),
        None,
        Some(actionqueue_core::bounded::OpaqueRef::new(format!("delivery:{}", id.as_str()))?),
        None,
        None,
        None,
    )?;
    let request = serde_json::to_vec(&request)?;
    Ok(AcknowledgmentSignal {
        version: 1,
        delivery: id.clone(),
        digest: ContentHash::of(&request),
        request,
        sequence: None,
    })
}

pub trait AqIngressPort {
    fn bound_tenant(&self) -> &OpaqueRef;
    /// Consult AQ's durable retention state. A retired identity remains resident
    /// and duplicate admission succeeds, but it can never match another wait.
    fn retired_wake_sequence(
        &mut self,
        request: &AdmitSignalRequest,
    ) -> std::result::Result<Option<u64>, AqDispatchFailure>;
    /// Recovery needs the active handler's trusted transport registration before
    /// replacing a terminal AQ run. Unknown registration fails closed.
    fn subscriber_registered(&self, _subscriber: &PrincipalId) -> bool {
        false
    }
    fn ensure_delivery_task(
        &mut self,
        request: EnsureTaskRequest,
    ) -> std::result::Result<AdmissionOutcome, AqDispatchFailure>;
    fn admit_wake(
        &mut self,
        request: AdmitSignalRequest,
    ) -> std::result::Result<WakeOutcome, AqDispatchFailure>;
    /// A terminal AQ run cannot be replayed by ensure_task. Only a confirmed
    /// terminal task may cause a new persisted recovery generation.
    fn delivery_task_terminal(
        &mut self,
        _id: TaskId,
    ) -> std::result::Result<bool, AqDispatchFailure> {
        Ok(false)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionOutcome {
    Created,
    AlreadyExists,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WakeOutcome {
    pub sequence: u64,
    pub created: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AqDispatchFailure {
    Conflict,
    Capacity,
    Uncertain,
    Forbidden,
    Invalid,
}
impl std::fmt::Display for AqDispatchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AQ dispatch: {self:?}")
    }
}
impl std::error::Error for AqDispatchFailure {}
fn classify(error: actionqueue_runtime::control::ServiceError) -> AqDispatchFailure {
    match error.code() {
        "conflict" => AqDispatchFailure::Conflict,
        "backpressure" => AqDispatchFailure::Capacity,
        "forbidden" | "not_found" => AqDispatchFailure::Forbidden,
        "invalid_request" => AqDispatchFailure::Invalid,
        _ => AqDispatchFailure::Uncertain,
    }
}
fn validate_delivery_admission(
    request: &EnsureTaskRequest,
    outcome: &EnsureTaskOutcome,
) -> std::result::Result<AdmissionOutcome, AqDispatchFailure> {
    let (key, digest) = match outcome {
        EnsureTaskOutcome::Created { admission_key, digest, .. }
        | EnsureTaskOutcome::AlreadyExists { admission_key, digest, .. } => (admission_key, digest),
    };
    // Check AQ's canonical identity, not the journal's hash of serialized bytes.
    // A mismatched acknowledgment must leave WI pending for identical replay.
    if outcome.sequence() == 0
        || outcome.task_id() != request.task_spec().id()
        || key != request.admission_key()
        || digest != &request.digest().map_err(|_| AqDispatchFailure::Invalid)?
    {
        return Err(AqDispatchFailure::Conflict);
    }
    Ok(if outcome.is_created() {
        AdmissionOutcome::Created
    } else {
        AdmissionOutcome::AlreadyExists
    })
}
/// Binds the unscoped AQ SingleTenant profile to exactly one configured WI tenant.
pub struct BoundAqIngress<'a, H: ExecutorHandler + 'static, C: actionqueue_core::time::clock::Clock>
{
    engine: &'a mut BootstrappedEngine<H, C>,
    tenant: OpaqueRef,
    subscribers: Option<SubscriberRegistry>,
}
impl<'a, H, C> BoundAqIngress<'a, H, C>
where
    H: ExecutorHandler + 'static,
    C: actionqueue_core::time::clock::Clock,
{
    /// Starts with no trusted subscriber registrations. Recovery remains
    /// parked until the host supplies its active registrations below.
    pub fn new(engine: &'a mut BootstrappedEngine<H, C>, tenant: OpaqueRef) -> Self {
        Self { engine, tenant, subscribers: None }
    }
    /// Use the registry obtained from the handler bootstrapped into this engine.
    /// It contains the actual transports, rather than a separate list of names.
    pub fn with_subscriber_registry(mut self, subscribers: SubscriberRegistry) -> Self {
        self.subscribers = Some(subscribers);
        self
    }
}
impl<H, C> AqIngressPort for BoundAqIngress<'_, H, C>
where
    H: ExecutorHandler + 'static,
    C: actionqueue_core::time::clock::Clock,
{
    fn bound_tenant(&self) -> &OpaqueRef {
        &self.tenant
    }
    fn retired_wake_sequence(
        &mut self,
        request: &AdmitSignalRequest,
    ) -> std::result::Result<Option<u64>, AqDispatchFailure> {
        let envelope = request.envelope(&Default::default(), 0);
        match self.engine.get_signal(&envelope.signal_id) {
            Ok(signal) => Ok((!signal.retained).then_some(signal.sequence.get())),
            Err(actionqueue_runtime::inspection::InspectionError::NotFound) => Ok(None),
            Err(_) => Err(AqDispatchFailure::Uncertain),
        }
    }
    fn subscriber_registered(&self, subscriber: &PrincipalId) -> bool {
        self.subscribers
            .as_ref()
            .is_some_and(|r| r.tenant == self.tenant && r.transports.contains_key(subscriber))
    }
    fn ensure_delivery_task(
        &mut self,
        request: EnsureTaskRequest,
    ) -> std::result::Result<AdmissionOutcome, AqDispatchFailure> {
        let outcome = self.engine.ensure_task(request.clone()).map_err(classify)?;
        validate_delivery_admission(&request, &outcome)
    }
    fn admit_wake(
        &mut self,
        request: AdmitSignalRequest,
    ) -> std::result::Result<WakeOutcome, AqDispatchFailure> {
        let outcome: AdmitSignalOutcome = self.engine.admit_signal(request).map_err(classify)?;
        Ok(WakeOutcome {
            sequence: outcome.sequence().get(),
            created: matches!(outcome, AdmitSignalOutcome::Admitted { .. }),
        })
    }
    fn delivery_task_terminal(
        &mut self,
        id: TaskId,
    ) -> std::result::Result<bool, AqDispatchFailure> {
        let runs = self.engine.projection().run_ids_for_task(id);
        if runs.is_empty() {
            return Ok(false);
        }
        for run in runs {
            let state = self.engine.get_run(run).map_err(|_| AqDispatchFailure::Uncertain)?.state;
            if !state.is_terminal() {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// The protected subscriber channel must durably store the delivery by DeliveryId
/// before returning. It receives the token only through IssuedDelivery.
pub trait SubscriberTransport: Send + Sync {
    fn deliver(&self, delivery: &IssuedDelivery) -> Result<()>;
}

/// Immutable host registrations shared by a delivery handler and its dispatcher.
/// Only the handler creates this registry; provider input cannot add names.
#[derive(Clone)]
pub struct SubscriberRegistry {
    tenant: OpaqueRef,
    transports: Arc<BTreeMap<PrincipalId, Arc<dyn SubscriberTransport>>>,
}

/// AQ executes delivery work and owns the acknowledgment wait and retry deadline.
/// This handler reloads current Fabric state on every physical attempt.
pub struct IngressDeliveryHandler {
    bundle: Arc<crate::storage::StoreBundle>,
    tenant: OpaqueRef,
    subscribers: SubscriberRegistry,
}
impl IngressDeliveryHandler {
    pub fn new(
        bundle: Arc<crate::storage::StoreBundle>,
        subscriber: AuthenticatedScope,
        transport: Arc<dyn SubscriberTransport>,
    ) -> Self {
        Self::with_subscribers(bundle, subscriber.tenant, [(subscriber.principal, transport)])
    }
    /// Subscriber registrations come from trusted host configuration, never task input.
    pub fn with_subscribers(
        bundle: Arc<crate::storage::StoreBundle>,
        tenant: OpaqueRef,
        subscribers: impl IntoIterator<Item = (PrincipalId, Arc<dyn SubscriberTransport>)>,
    ) -> Self {
        let subscribers = SubscriberRegistry {
            tenant: tenant.clone(),
            transports: Arc::new(subscribers.into_iter().collect()),
        };
        Self { bundle, tenant, subscribers }
    }
    /// Capture before moving this handler into AQ; pass the clone to recovery.
    pub fn subscriber_registry(&self) -> SubscriberRegistry {
        self.subscribers.clone()
    }
    fn execute_delivery(
        &self,
        ctx: ExecutorContext,
    ) -> Result<actionqueue_core::disposition::AttemptDisposition> {
        use actionqueue_core::{
            continuation::{
                SignalEligibility, SignalFilter, WaitDeadline, WaitMatchPolicy, WaitSpec,
                WaitTimeoutPolicy,
            },
            disposition::AttemptDisposition,
            ids::WaitId,
        };
        let payload = std::str::from_utf8(&ctx.input.payload)?;
        let id: DeliveryId = payload
            .strip_prefix("delivery:")
            .ok_or("invalid delivery task locator")?
            .to_owned()
            .try_into()?;
        let root = self.bundle.wi();
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64;
        let readiness = reconcile_delivery(root, &self.tenant, &id, now)?;
        if readiness == DeliveryReadiness::Closed {
            return Ok(AttemptDisposition::complete(None));
        }
        let state = root.ingress_snapshot(&self.tenant)?;
        let delivery = state.deliveries.get(&id).ok_or("unknown delivery task")?;
        if delivery.state != DeliveryState::Pending {
            return Ok(AttemptDisposition::complete(None));
        }
        let transport = self.subscribers.transports.get(&delivery.subscriber);
        if readiness != DeliveryReadiness::Ready || transport.is_none() {
            // Retire unavailable work without polling or establishing a wait.
            // Dispatch can recover when readiness returns, within the journaled
            // generation budget. Already issued tokens can still be acknowledged.
            return Ok(AttemptDisposition::complete(None));
        }
        let transport = transport.expect("checked registration");
        let subscriber = AuthenticatedScope {
            tenant: self.tenant.clone(),
            principal: delivery.subscriber.clone(),
        };
        let queue = state.queue.get(&id).ok_or("missing queue obligation")?;
        if ContentHash::of(&queue.signal_request) != queue.signal_digest {
            return Err("queue wake integrity mismatch".into());
        }
        let wake: AdmitSignalRequest = serde_json::from_slice(&queue.signal_request)?;
        let wake = wake.envelope(&Default::default(), 0);
        let filter = |kind: &str| -> Result<SignalFilter> {
            Ok(SignalFilter {
                tenant_id: None,
                namespace: SignalNamespace::new("wi_ingress")?,
                kind: SignalKind::new(kind)?,
                correlation_id: if kind == "acknowledgment" {
                    Some(CorrelationId::new(format!("delivery/{}", id.as_str()))?)
                } else {
                    // The immutable wake belongs to the logical delivery, even
                    // when the AQ task has a replacement identity.
                    wake.correlation_id.clone()
                },
                source_ref: Some(actionqueue_core::bounded::OpaqueRef::new(format!(
                    "delivery:{}",
                    id.as_str()
                ))?),
            })
        };
        if ctx.input.resume_context.is_none() {
            // Task admission can commit even when the delivery signal does not.
            // Keep that first wait bounded by WI's durable retention deadline so
            // expiration can finish the AQ run without admitting a stale wake.
            let expiry_second = delivery.expires_at_ms.saturating_add(999) / 1000;
            return Ok(AttemptDisposition::awaiting(
                WaitSpec::new(
                    WaitId::new(),
                    filter("delivery")?,
                    WaitMatchPolicy::FirstMatch,
                    SignalEligibility::AnyRetained,
                    Some(WaitDeadline {
                        at: expiry_second.max(now / 1000 + 1),
                        policy: WaitTimeoutPolicy::ResumeWithTimeout,
                    }),
                )?,
                None,
            ));
        }
        // Recovery can resume an issued delivery before WI's retry time.
        // Reestablish its acknowledgment wait without failing another AQ attempt
        // or issuing a second subscriber token early.
        if delivery.attempts.last().is_none_or(|a| now >= a.next_attempt_ms) {
            let claimed =
                IngressRuntime::new(root, self.tenant.clone()).claim(&subscriber, &id, now);
            let claimed = match claimed {
                Ok(claimed) => claimed,
                Err(error) => match reconcile_delivery(root, &self.tenant, &id, now)? {
                    DeliveryReadiness::Closed => return Ok(AttemptDisposition::complete(None)),
                    DeliveryReadiness::Unavailable => {
                        return Ok(AttemptDisposition::complete(None));
                    }
                    DeliveryReadiness::Ready => return Err(error),
                },
            };
            if let Some(issued) = claimed {
                let succeeded = transport.deliver(&issued).is_ok();
                let finished_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64;
                Ingress::new(root).record_wake(
                    &self.tenant,
                    id.clone(),
                    issued.attempt,
                    succeeded,
                    finished_at,
                )?;
            }
        }
        let finished_at =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64;
        if reconcile_delivery(root, &self.tenant, &id, finished_at)? != DeliveryReadiness::Ready {
            return Ok(AttemptDisposition::complete(None));
        }
        let state = root.ingress_snapshot(&self.tenant)?;
        let delivery = state.deliveries.get(&id).ok_or("missing delivery after claim")?;
        if delivery.state != DeliveryState::Pending {
            return Ok(AttemptDisposition::complete(None));
        }
        let retry_at_ms =
            delivery.attempts.last().map(|a| a.next_attempt_ms).unwrap_or_else(|| {
                now.saturating_add(delivery.policy.acknowledgment_timeout_ms)
                    .saturating_add(delivery.policy.backoff_ms)
            });
        // Fabric records milliseconds; AQ continuation deadlines use Unix seconds.
        let retry_at_seconds = retry_at_ms.min(delivery.expires_at_ms).saturating_add(999) / 1000;
        let now_seconds = finished_at / 1000;
        let wait = WaitSpec::new(
            WaitId::new(),
            filter("acknowledgment")?,
            WaitMatchPolicy::FirstMatch,
            SignalEligibility::AnyRetained,
            Some(WaitDeadline {
                at: retry_at_seconds.max(now_seconds.saturating_add(1)),
                policy: WaitTimeoutPolicy::ResumeWithTimeout,
            }),
        )?;
        Ok(AttemptDisposition::awaiting(wait, None))
    }
}
impl ExecutorHandler for IngressDeliveryHandler {
    fn execute(&self, ctx: ExecutorContext) -> actionqueue_core::disposition::AttemptDisposition {
        self.execute_delivery(ctx).unwrap_or_else(|_| {
            actionqueue_core::disposition::AttemptDisposition::retryable_failure(
                actionqueue_core::bounded::BoundedError::new("ingress delivery unavailable")
                    .expect("static bounded error"),
            )
        })
    }
}

#[cfg(test)]
#[test]
fn delivery_admission_rejects_mismatched_acknowledgments() {
    use actionqueue_core::{admission::AdmissionDigest, ids::AdmissionKey};
    let id = DeliveryId::generate();
    let queue = queue_request(&id, 0, None).unwrap();
    let request: EnsureTaskRequest = serde_json::from_slice(&queue.task_request).unwrap();
    let digest = request.digest().unwrap();
    let other = queue_request(&DeliveryId::generate(), 0, None).unwrap();
    let other: EnsureTaskRequest = serde_json::from_slice(&other.task_request).unwrap();
    for created in [true, false] {
        let valid = if created {
            EnsureTaskOutcome::Created {
                sequence: 1,
                task_id: request.task_spec().id(),
                admission_key: request.admission_key().clone(),
                digest: digest.clone(),
            }
        } else {
            EnsureTaskOutcome::AlreadyExists {
                sequence: 1,
                task_id: request.task_spec().id(),
                admission_key: request.admission_key().clone(),
                digest: digest.clone(),
            }
        };
        assert_eq!(
            validate_delivery_admission(&request, &valid),
            Ok(if created { AdmissionOutcome::Created } else { AdmissionOutcome::AlreadyExists })
        );
        for field in 0..5 {
            let mut invalid = valid.clone();
            let (sequence, task_id, key, returned_digest) = match &mut invalid {
                EnsureTaskOutcome::Created { sequence, task_id, admission_key, digest }
                | EnsureTaskOutcome::AlreadyExists { sequence, task_id, admission_key, digest } => {
                    (sequence, task_id, admission_key, digest)
                }
            };
            match field {
                0 => *sequence = 0,
                1 => *task_id = other.task_spec().id(),
                2 => *key = AdmissionKey::new("wrong-key").unwrap(),
                3 => *returned_digest = other.digest().unwrap(),
                4 => {
                    *returned_digest = AdmissionDigest::versioned(1, digest.hash().clone()).unwrap()
                }
                _ => unreachable!(),
            }
            assert_eq!(
                validate_delivery_admission(&request, &invalid),
                Err(AqDispatchFailure::Conflict),
                "created={created}, field={field}"
            );
        }
    }
}
