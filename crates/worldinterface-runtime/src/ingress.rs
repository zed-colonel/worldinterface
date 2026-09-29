//! Production composition for the verified ingress journal and pinned AQ admission API.
pub mod http;
pub mod streaming;
use crate::Result;
use actionqueue_core::{
    admission::EnsureTaskRequest,
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
use std::{collections::BTreeMap, io::Read, sync::Arc};
use worldinterface_domain::ingress::{AcknowledgmentSignal, IngressResult, QueueObligation};
use worldinterface_ingress::{
    transport::{
        http::validate_http, streaming::StreamSession, ProviderCredentials, ValidatedProviderEvent,
    },
    AdmissionIds, AuthenticatedScope, DeliveryToken, Ingress, IssuedDelivery,
};
use worldinterface_protocol::{
    BindingVersion, ContentHash, DeliveryAcknowledgment, DeliveryId, ObservationId, OpaqueRef,
    PrincipalId, ReceiptId,
};
use worldinterface_storage::StoreRoot;

/// The caller constructs this service from trusted host configuration. Provider bytes
/// cannot select the tenant, credentials, bindings, or subscriber authority.
pub struct IngressRuntime<'a> {
    root: &'a StoreRoot,
    tenant: OpaqueRef,
}
impl<'a> IngressRuntime<'a> {
    pub fn new(root: &'a StoreRoot, tenant: OpaqueRef) -> Self {
        Self { root, tenant }
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
        let queue =
            ids.deliveries.iter().map(|id| queue_request(id, 0)).collect::<Result<Vec<_>>>()?;
        Ingress::new(self.root).admit_provider_event_queued(event, bindings, ids, queue, now)
    }
    /// Replays unchanged requests. A failed local acknowledgment is always treated as
    /// possibly committed in AQ; the next call retries the same persisted bytes.
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
        let pending: Vec<_> = state
            .queue
            .values()
            .filter(|q| {
                q.signal_sequence.is_none()
                    || state
                        .deliveries
                        .get(&q.delivery)
                        .is_some_and(|d| d.state == worldinterface_protocol::DeliveryState::Pending)
            })
            .cloned()
            .collect();
        let mut done = 0;
        for q in pending {
            if done >= limit {
                break;
            }
            let mut q = q;
            // AQ admission and the WI journal are separate durable authorities. Another
            // dispatcher may replace a terminal generation during either AQ call.
            for _ in 0..4 {
                q = if q.signal_sequence.is_some() {
                    let task: EnsureTaskRequest = serde_json::from_slice(&q.task_request)?;
                    if !aq.delivery_task_terminal(task.task_spec().id())? {
                        break;
                    }
                    let next = queue_request(
                        &q.delivery,
                        q.generation.checked_add(1).ok_or("recovery generation overflow")?,
                    )?;
                    let recovered = self.root.ingress_transact_fresh(
                        &self.tenant,
                        now,
                        &worldinterface_domain::ingress::IngressCommand::RecoverQueue {
                            id: q.delivery.clone(),
                            expected_generation: q.generation,
                            replacement: Box::new(next.clone()),
                        },
                    );
                    if let Err(error) = recovered {
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
                if ContentHash::of(&q.task_request) != q.task_digest
                    || ContentHash::of(&q.signal_request) != q.signal_digest
                {
                    return Err("queue request integrity mismatch".into());
                }
                let task: EnsureTaskRequest = serde_json::from_slice(&q.task_request)?;
                let signal: AdmitSignalRequest = serde_json::from_slice(&q.signal_request)?;
                if !q.task_admitted {
                    let _ = aq.ensure_delivery_task(task)?;
                    let recorded = self.root.ingress_transact_fresh(
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
                let recorded = self.root.ingress_transact_fresh(
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
        let acknowledgments: Vec<_> = state
            .acknowledgments_to_signal
            .values()
            .filter(|s| s.sequence.is_none())
            .take(limit.saturating_sub(done))
            .cloned()
            .collect();
        for signal in acknowledgments {
            if ContentHash::of(&signal.request) != signal.digest {
                return Err("acknowledgment request integrity mismatch".into());
            }
            let request: AdmitSignalRequest = serde_json::from_slice(&signal.request)?;
            let sequence = aq.admit_wake(request)?.sequence;
            self.root.ingress_transact_fresh(
                &self.tenant,
                now,
                &worldinterface_domain::ingress::IngressCommand::RecordAcknowledgmentSignal {
                    id: signal.delivery,
                    sequence,
                },
            )?;
            done += 1;
        }
        Ok(done)
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

fn queue_request(id: &DeliveryId, generation: u32) -> Result<QueueObligation> {
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
    let signal = AdmitSignalRequest::new(
        SignalId::new(if generation == 0 {
            format!("wi-delivery-{}", id.as_str())
        } else {
            format!("wi-delivery-{}-{generation}", id.as_str())
        })?,
        SignalNamespace::new("wi_ingress")?,
        SignalKind::new("delivery")?,
        Some(CorrelationId::new(format!("task/{task_id}"))?),
        None,
        Some(actionqueue_core::bounded::OpaqueRef::new(format!("delivery:{}", id.as_str()))?),
        None,
        None,
        None,
    )?;
    // AQ validates and computes its own CanonicalAdmissionV2 digest. These are
    // stable serde bytes for replay, not a WI replacement admission digest.
    let task_request = serde_json::to_vec(&task)?;
    let signal_request = serde_json::to_vec(&signal)?;
    Ok(QueueObligation {
        version: 1,
        delivery: id.clone(),
        task_digest: ContentHash::of(&task_request),
        signal_digest: ContentHash::of(&signal_request),
        task_request,
        signal_request,
        task_admitted: false,
        signal_sequence: None,
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
/// Binds the unscoped AQ SingleTenant profile to exactly one configured WI tenant.
pub struct BoundAqIngress<'a, H: ExecutorHandler + 'static, C: actionqueue_core::time::clock::Clock>
{
    engine: &'a mut BootstrappedEngine<H, C>,
    tenant: OpaqueRef,
}
impl<'a, H, C> BoundAqIngress<'a, H, C>
where
    H: ExecutorHandler + 'static,
    C: actionqueue_core::time::clock::Clock,
{
    pub fn new(engine: &'a mut BootstrappedEngine<H, C>, tenant: OpaqueRef) -> Self {
        Self { engine, tenant }
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
    fn ensure_delivery_task(
        &mut self,
        request: EnsureTaskRequest,
    ) -> std::result::Result<AdmissionOutcome, AqDispatchFailure> {
        Ok(if self.engine.ensure_task(request).map_err(classify)?.is_created() {
            AdmissionOutcome::Created
        } else {
            AdmissionOutcome::AlreadyExists
        })
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

/// AQ executes delivery work and owns the acknowledgment wait and retry deadline.
/// This handler reloads current Fabric state on every physical attempt.
pub struct IngressDeliveryHandler {
    bundle: Arc<crate::storage::StoreBundle>,
    tenant: OpaqueRef,
    subscribers: BTreeMap<PrincipalId, Arc<dyn SubscriberTransport>>,
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
        Self { bundle, tenant, subscribers: subscribers.into_iter().collect() }
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
        let state = root.ingress_snapshot(&self.tenant)?;
        let delivery = state.deliveries.get(&id).ok_or("unknown delivery task")?;
        let transport =
            self.subscribers.get(&delivery.subscriber).ok_or("unregistered subscriber")?;
        let subscriber = AuthenticatedScope {
            tenant: self.tenant.clone(),
            principal: delivery.subscriber.clone(),
        };
        if delivery.state == worldinterface_protocol::DeliveryState::Acknowledged
            || delivery.state == worldinterface_protocol::DeliveryState::Expired
            || delivery.state == worldinterface_protocol::DeliveryState::DeadLetter
        {
            return Ok(AttemptDisposition::complete(None));
        }
        let task_id = delivery_task_id(
            &id,
            state.queue.get(&id).ok_or("missing queue obligation")?.generation,
        )?;
        let filter = |kind: &str| -> Result<SignalFilter> {
            Ok(SignalFilter {
                tenant_id: None,
                namespace: SignalNamespace::new("wi_ingress")?,
                kind: SignalKind::new(kind)?,
                correlation_id: Some(CorrelationId::new(if kind == "acknowledgment" {
                    format!("delivery/{}", id.as_str())
                } else {
                    format!("task/{task_id}")
                })?),
                source_ref: Some(actionqueue_core::bounded::OpaqueRef::new(format!(
                    "delivery:{}",
                    id.as_str()
                ))?),
            })
        };
        if ctx.input.resume_context.is_none() {
            return Ok(AttemptDisposition::awaiting(
                WaitSpec::new(
                    WaitId::new(),
                    filter("delivery")?,
                    WaitMatchPolicy::FirstMatch,
                    SignalEligibility::AnyRetained,
                    None,
                )?,
                None,
            ));
        }
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64;
        // Recovery can resume an issued delivery before WI's retry time.
        // Reestablish its acknowledgment wait without failing another AQ attempt
        // or issuing a second subscriber token early.
        if delivery.attempts.last().is_none_or(|a| now >= a.next_attempt_ms) {
            if let Some(issued) =
                IngressRuntime::new(root, self.tenant.clone()).claim(&subscriber, &id, now)?
            {
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
        let state = root.ingress_snapshot(&self.tenant)?;
        let delivery = state.deliveries.get(&id).ok_or("missing delivery after claim")?;
        if delivery.state == worldinterface_protocol::DeliveryState::Acknowledged
            || delivery.state == worldinterface_protocol::DeliveryState::DeadLetter
        {
            return Ok(AttemptDisposition::complete(None));
        }
        let retry_at_ms =
            delivery.attempts.last().map(|a| a.next_attempt_ms).unwrap_or(now.saturating_add(1000));
        // Fabric records milliseconds; AQ continuation deadlines use Unix seconds.
        let retry_at_seconds = retry_at_ms.saturating_add(999) / 1000;
        let now_seconds = now / 1000;
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
