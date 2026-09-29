//! Pure ingress transactions. Trusted composition supplies authentication, clocks and IDs.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use worldinterface_protocol::*;
type Result<T> = std::result::Result<T, String>;
const MAX_TIME: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressState {
    pub revision: u64,
    pub last_at_ms: u64,
    pub bindings: BTreeMap<IngressBindingId, IngressBinding>,
    pub admissions: BTreeMap<ContentHash, IngressAdmission>,
    pub deliveries: BTreeMap<DeliveryId, ObservationDelivery>,
    #[serde(default)]
    pub queue: BTreeMap<DeliveryId, QueueObligation>,
    #[serde(default)]
    pub acknowledgments_to_signal: BTreeMap<DeliveryId, AcknowledgmentSignal>,
    pub conflicts: BTreeSet<(ContentHash, ContentHash)>,
    pub releases: Vec<(ArtifactRef, OpaqueRef, EvidenceRef)>,
}
/// AQ-neutral, immutable request bytes committed with an observation. The runtime
/// constructs and validates both versioned requests before this journal accepts them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueObligation {
    pub version: u32,
    pub delivery: DeliveryId,
    pub task_request: Vec<u8>,
    pub signal_request: Vec<u8>,
    pub task_digest: ContentHash,
    pub signal_digest: ContentHash,
    pub task_admitted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_sequence: Option<u64>,
    #[serde(default)]
    pub generation: u32,
    #[serde(default)]
    pub prior: Vec<QueueObligation>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcknowledgmentSignal {
    pub version: u32,
    pub delivery: DeliveryId,
    pub request: Vec<u8>,
    pub digest: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum IngressCommand {
    Bind {
        binding: Box<IngressBinding>,
    },
    ChangeBinding {
        id: IngressBindingId,
        expected: u64,
        status: BindingStatus,
    },
    Admit {
        admission: Box<IngressAdmission>,
    },
    AdmitQueued {
        admission: Box<IngressAdmission>,
        queue: Vec<QueueObligation>,
    },
    RecordQueueTask {
        id: DeliveryId,
        generation: u32,
        task_digest: ContentHash,
    },
    RecordQueueSignal {
        id: DeliveryId,
        generation: u32,
        signal_digest: ContentHash,
        sequence: u64,
    },
    RecoverQueue {
        id: DeliveryId,
        expected_generation: u32,
        replacement: Box<QueueObligation>,
    },
    Claim {
        id: DeliveryId,
        subscriber: PrincipalId,
        token_digest: ContentHash,
    },
    Finish {
        id: DeliveryId,
        attempt: u32,
        succeeded: bool,
    },
    Acknowledge {
        id: DeliveryId,
        subscriber: PrincipalId,
        token_digest: ContentHash,
    },
    AcknowledgeQueued {
        id: DeliveryId,
        subscriber: PrincipalId,
        token_digest: ContentHash,
        signal: AcknowledgmentSignal,
    },
    RecordAcknowledgmentSignal {
        id: DeliveryId,
        sequence: u64,
    },
    InvalidateToken {
        id: DeliveryId,
    },
    Expire {
        id: DeliveryId,
    },
    Release {
        artifact: ArtifactRef,
        quarantine: OpaqueRef,
        evidence: EvidenceRef,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngressResult {
    Binding(BindingVersion),
    Admitted {
        observation: ObservationId,
        deliveries: Vec<DeliveryId>,
        duplicate: bool,
        contiguous_next: u64,
    },
    Conflict,
    Attempt(DeliveryAttempt),
    Acknowledged(DeliveryAcknowledgment),
    Updated,
    StaleQueueGeneration,
}
pub fn digest<T: Serialize>(value: &T) -> Result<ContentHash> {
    Ok(ContentHash::of(&canonical_data(value)?))
}
/// Availability namespace is bound to the complete provider/source scope. Knowing a
/// different provider's artifact hash within the same tenant does not grant access.
pub fn provider_artifact_scope(scope: &ProviderScope) -> Result<OpaqueRef> {
    format!("ingress:{}", digest(scope)?.as_str()).try_into()
}
pub fn producer_digest(profile: &ProviderProfile, event: &ProviderEvent) -> Result<ContentHash> {
    let mut normalized = event.clone();
    if let IdentityStrategy::ContentWindow { window_ms } = profile.identity {
        if window_ms == 0 {
            return Err("invalid content window".into());
        }
        normalized.produced_at_ms = event.produced_at_ms / window_ms * window_ms;
        normalized.event_id = None;
        normalized.offset = None;
    }
    digest(&normalized)
}
pub fn provider_identity(
    profile: &ProviderProfile,
    event: &ProviderEvent,
    nonce: &ObservationId,
) -> Result<ContentHash> {
    let value = match &profile.identity {
        IdentityStrategy::StableId => {
            event.event_id.as_ref().ok_or("missing provider id")?.as_str().to_owned()
        }
        IdentityStrategy::StreamPosition => event.offset.ok_or("missing offset")?.to_string(),
        IdentityStrategy::ContentWindow { window_ms } if *window_ms > 0 => format!(
            "{}:{}",
            event.produced_at_ms / window_ms,
            producer_digest(profile, event)?.as_str()
        ),
        IdentityStrategy::NonDeduplicable => nonce.as_str().to_owned(),
        _ => return Err("invalid identity strategy".into()),
    };
    digest(&(&profile.scope, &profile.identity, value))
}
pub fn validate_profile(p: &ProviderProfile) -> Result<()> {
    if p.replay_window_ms == 0
        || p.replay_window_ms > MAX_TIME
        || p.capacity == 0
        || p.capacity > 10000
        || p.max_body_bytes == 0
        || p.max_body_bytes > MAX_RECORD_BYTES as u32
        || p.max_attachments > 64
        || p.first_offset > MAX_TIME
        || matches!(p.identity, IdentityStrategy::ContentWindow { window_ms: 0 })
    {
        return Err("invalid bounded provider profile".into());
    }
    Ok(())
}
pub fn validate_attachments(p: &ProviderProfile, event: &ProviderEvent) -> Result<()> {
    for attachment in &event.attachments {
        match &p.attachment_policy {
            AttachmentPolicy::GovernedReferences if attachment.quarantine.is_none() => {}
            AttachmentPolicy::QuarantineAll { policy }
                if attachment.quarantine.as_ref() == Some(policy) => {}
            _ => return Err("attachment disposition differs from trusted profile".into()),
        }
    }
    Ok(())
}
/// All typed evidence in a provider profile, including nested role attestations.
pub fn profile_evidence(profile: &ObservationProfile) -> Vec<&EvidenceRef> {
    match profile {
        ObservationProfile::Ordinary => vec![],
        ObservationProfile::Commissioning { evidence } => vec![evidence],
        ObservationProfile::RoleStewardship { capacity, evidence } => {
            let mut refs = vec![evidence];
            refs.extend(capacity.attestation_ref.iter());
            refs
        }
    }
}
/// Tenant evidence is governed by the tenant boundary. Provider-scoped attestations
/// must also be declared as attachments, so their trusted quarantine disposition applies.
pub fn validate_event_references(p: &ProviderProfile, event: &ProviderEvent) -> Result<()> {
    let provider_scope = provider_artifact_scope(&p.scope)?;
    if event.content.scope != provider_scope
        || event.attachments.iter().any(|a| a.artifact.scope != provider_scope)
    {
        return Err("artifact scope".into());
    }
    for (index, evidence) in profile_evidence(&event.profile).iter().enumerate() {
        if evidence.artifact.scope != p.scope.tenant
            && !(index > 0
                && evidence.artifact.scope == provider_scope
                && event.attachments.iter().any(|a| a.artifact == evidence.artifact))
        {
            return Err("profile evidence scope or undeclared attestation".into());
        }
    }
    Ok(())
}
/// Apply quarantine to a reference wherever it appears, including aliases in content
/// or nested profiles and quarantines recorded by earlier observations.
pub fn artifact_disclosable(state: &IngressState, artifact: &ArtifactRef) -> bool {
    state.admissions.values().flat_map(|a| &a.event.attachments).all(|a| {
        &a.artifact != artifact
            || a.quarantine
                .as_ref()
                .is_none_or(|q| state.releases.iter().any(|(r, k, _)| r == artifact && k == q))
    })
}
pub fn validate_policy(p: &DeliveryPolicy) -> Result<()> {
    if p.max_attempts == 0
        || p.max_attempts > 100
        || p.acknowledgment_timeout_ms == 0
        || p.retention_ms == 0
        || p.retention_ms > MAX_TIME
        || p.backoff_ms > p.retention_ms
        || p.acknowledgment_timeout_ms > p.retention_ms
        || p.physical_erasure
    {
        return Err("unsupported delivery policy".into());
    }
    Ok(())
}
pub fn disclose(
    state: &IngressState,
    d: &ObservationDelivery,
    subscriber: &PrincipalId,
    now: u64,
) -> Result<()> {
    let b = state.bindings.get(&d.binding.id).ok_or("missing binding")?;
    let a = state
        .admissions
        .values()
        .find(|a| a.observation.id == d.observation)
        .ok_or("missing observation")?;
    if now < state.last_at_ms
        || &d.subscriber != subscriber
        || b.status != BindingStatus::Active
        || now >= b.expires_at_ms
        || now >= d.expires_at_ms
        || d.state == DeliveryState::Expired
        || !a.event.information_use.allowed_audiences.contains(subscriber)
    {
        return Err("disclosure denied".into());
    }
    Ok(())
}
pub fn contiguous(state: &IngressState, p: &ProviderProfile) -> u64 {
    let positions: BTreeSet<_> = state
        .admissions
        .values()
        .filter(|a| a.profile.scope == p.scope)
        .filter_map(|a| a.event.offset)
        .collect();
    let mut next = p.first_offset;
    while positions.contains(&next) {
        next += 1;
    }
    next
}
pub fn apply_ingress(
    state: &IngressState,
    tenant: &OpaqueRef,
    now: u64,
    command: &IngressCommand,
) -> Result<(IngressState, IngressResult)> {
    if now < state.last_at_ms || now > MAX_TIME {
        return Err("invalid or reversed clock".into());
    }
    let mut s = state.clone();
    let result = match command {
        IngressCommand::Bind { binding } => {
            let b = binding.as_ref();
            validate_profile(&b.profile)?;
            validate_policy(&b.policy)?;
            if &b.profile.scope.tenant != tenant
                || b.revision != 1
                || b.status != BindingStatus::Active
                || b.expires_at_ms <= now
                || b.expires_at_ms > MAX_TIME
                || b.authentication_requirement.as_str() != "exact-byte-hmac"
                || b.authorization.artifact.scope != *tenant
                || b.submission.delivery_policy != b.policy.version
                || b.submission.source.namespace != b.profile.scope.source.namespace
                || b.submission.event_scope != b.profile.scope.namespace
            {
                return Err("invalid binding admission".into());
            }
            if let Some(old) = s.bindings.values().find(|old| {
                old.principal == b.principal
                    && old.submission.client_request_id == b.submission.client_request_id
            }) {
                // Idempotency binds all resolved inputs as well as the caller submission.
                let mut original = old.clone();
                original.revision = 1;
                original.status = BindingStatus::Active;
                let mut retry = b.clone();
                retry.id = original.id.clone();
                if original != retry {
                    return Err("conflicting binding request".into());
                }
                return Ok((
                    state.clone(),
                    IngressResult::Binding(BindingVersion {
                        id: old.id.clone(),
                        revision: old.revision,
                    }),
                ));
            }
            if s.bindings.len() >= b.profile.capacity as usize || s.bindings.contains_key(&b.id) {
                return Err("binding capacity or identity conflict".into());
            }
            if s.bindings
                .values()
                .any(|old| old.profile.scope == b.profile.scope && old.profile != b.profile)
            {
                return Err("provider profile change requires a new scope".into());
            }
            s.bindings.insert(b.id.clone(), b.clone());
            IngressResult::Binding(BindingVersion { id: b.id.clone(), revision: 1 })
        }
        IngressCommand::ChangeBinding { id, expected, status } => {
            let b = s.bindings.get_mut(id).ok_or("unknown binding")?;
            if b.revision != *expected || b.status == BindingStatus::Revoked || b.status == *status
            {
                return Err("stale or illegal binding transition".into());
            }
            b.revision = b.revision.checked_add(1).ok_or("revision overflow")?;
            b.status = status.clone();
            IngressResult::Updated
        }
        IngressCommand::Admit { admission } | IngressCommand::AdmitQueued { admission, .. } => {
            let a = admission.as_ref();
            let queue = match command {
                IngressCommand::AdmitQueued { queue, .. } => Some(queue),
                _ => None,
            };
            validate_profile(&a.profile)?;
            validate_attachments(&a.profile, &a.event)?;
            validate_event_references(&a.profile, &a.event)?;
            if &a.event.scope.tenant != tenant
                || a.event.scope != a.profile.scope
                || a.identity != provider_identity(&a.profile, &a.event, &a.observation.id)?
                || a.producer_digest != producer_digest(&a.profile, &a.event)?
            {
                return Err("invalid provider identity".into());
            }
            // Offset occupancy is independent of content identity. A content-window
            // retransmission must not hide conflicting reuse of a committed position.
            let offset_conflict = s.admissions.values().find(|old| {
                a.event.offset.is_some()
                    && old.event.offset == a.event.offset
                    && old.profile.scope == a.profile.scope
                    && old.producer_digest != a.producer_digest
            });
            if let Some(old) = offset_conflict.or_else(|| s.admissions.get(&a.identity)) {
                if old.producer_digest == a.producer_digest {
                    return Ok((
                        state.clone(),
                        IngressResult::Admitted {
                            observation: old.observation.id.clone(),
                            deliveries: old.deliveries.clone(),
                            duplicate: true,
                            contiguous_next: contiguous(state, &a.profile),
                        },
                    ));
                }
                if s.conflicts.len() >= a.profile.capacity as usize {
                    return Err("conflict capacity exhausted".into());
                }
                s.conflicts.insert((old.identity.clone(), a.producer_digest.clone()));
                IngressResult::Conflict
            } else {
                if now < a.event.produced_at_ms
                    || now - a.event.produced_at_ms > a.profile.replay_window_ms
                    || a.admitted_at_ms != now
                    || a.event.offset.is_some_and(|o| o < a.profile.first_offset || o >= MAX_TIME)
                {
                    return Err("outside replay window or offset range".into());
                }
                if s.admissions.len() >= a.profile.capacity as usize
                    || a.bindings.is_empty()
                    || a.bindings.len() != a.deliveries.len()
                    || s.deliveries.len() + a.deliveries.len() > 10000
                {
                    return Err("admission capacity or fanout invalid".into());
                }
                if let Some(queue) = queue {
                    if queue.len() != a.deliveries.len()
                        || queue.iter().zip(&a.deliveries).any(|(q, id)| {
                            q.version != 1
                                || &q.delivery != id
                                || q.task_admitted
                                || q.signal_sequence.is_some()
                                || q.generation != 0
                                || !q.prior.is_empty()
                                || q.task_request.is_empty()
                                || q.signal_request.is_empty()
                                || q.task_request.len() > 16384
                                || q.signal_request.len() > 16384
                                || ContentHash::of(&q.task_request) != q.task_digest
                                || ContentHash::of(&q.signal_request) != q.signal_digest
                        })
                    {
                        return Err("invalid immutable queue obligations".into());
                    }
                }
                let expected: Vec<_> = s
                    .bindings
                    .values()
                    .filter(|b| {
                        b.profile == a.profile
                            && b.status == BindingStatus::Active
                            && b.expires_at_ms > now
                    })
                    .map(|b| BindingVersion { id: b.id.clone(), revision: b.revision })
                    .collect();
                if a.bindings != expected {
                    return Err("stale or incomplete binding capture".into());
                }
                if a.event.attachments.len() > a.profile.max_attachments as usize
                    || a.event.content.scope != provider_artifact_scope(&a.profile.scope)?
                    || a.event.attachments.iter().any(|v| v.artifact.scope != a.event.content.scope)
                {
                    return Err("artifact scope or bound".into());
                }
                let o = &a.observation;
                if o.binding_id != a.bindings[0].id
                    || o.canonical_source != a.event.scope.source
                    || o.content_ref != a.event.content
                    || o.provider_event_id != a.event.event_id
                    || o.deduplication_key.as_str() != a.identity.as_str()
                    || o.asserted_sender != a.event.asserted_sender
                    || o.asserted_audience != a.event.asserted_audience
                    || o.authentication_assurance != a.provenance.assurance
                    || o.information_use.as_ref() != Some(&a.event.information_use)
                    || o.attachment_refs
                        != a.event
                            .attachments
                            .iter()
                            .map(|v| v.artifact.clone())
                            .collect::<Vec<_>>()
                    || s.admissions
                        .values()
                        .any(|old| old.observation.id == o.id || old.receipt.id == a.receipt.id)
                {
                    return Err("observation provenance mismatch".into());
                }
                let ReceiptBody::Ingress(r) = &a.receipt.body else {
                    return Err("wrong receipt direction".into());
                };
                if r.observation_id != o.id
                    || r.binding_id != o.binding_id
                    || r.source != o.canonical_source
                    || r.observation_digest
                        != Envelope::new(Record::BoundaryObservation(o.clone())).digest()?
                    || r.validation.as_str() != "transport-validated-no-semantic-authority"
                {
                    return Err("receipt mismatch".into());
                }
                let first_binding = &s.bindings[&a.bindings[0].id];
                let evidence: Vec<_> =
                    a.bindings.iter().map(|b| s.bindings[&b.id].authorization.clone()).collect();
                let grants = evidence.iter().map(digest).collect::<Result<Vec<_>>>()?;
                if a.receipt.temporal != o.temporal
                    || a.receipt.versions.resolver != first_binding.resolver_version
                    || a.receipt.versions.policy != first_binding.policy.version
                    || a.receipt.versions.grant_versions != grants
                    || a.receipt.evidence != evidence
                    || a.receipt.versions.provider_profile != digest(&a.profile)?
                    || o.temporal.admitted_at.at.epoch_millis()? != now
                    || o.temporal.received_at.at.epoch_millis()? != now
                {
                    return Err("receipt versions or time provenance mismatch".into());
                }
                let previous_next = contiguous(&s, &a.profile);
                let previous_max = s
                    .admissions
                    .values()
                    .filter(|x| x.profile.scope == a.profile.scope)
                    .filter_map(|x| x.event.offset)
                    .max();
                for (b, id) in a.bindings.iter().zip(&a.deliveries) {
                    let binding = &s.bindings[&b.id];
                    if !a.event.information_use.allowed_audiences.contains(&binding.subscriber) {
                        return Err("subscriber outside information-use audience".into());
                    }
                    let expiration = now
                        .checked_add(binding.policy.retention_ms)
                        .filter(|v| *v <= MAX_TIME)
                        .ok_or("expiration overflow")?;
                    let expiration = a
                        .event
                        .information_use
                        .expiration
                        .as_ref()
                        .map(Timestamp::epoch_millis)
                        .transpose()?
                        .map_or(expiration, |t| expiration.min(t));
                    if expiration <= now {
                        return Err("expired information use".into());
                    }
                    if s.deliveries.contains_key(id) {
                        return Err("delivery collision".into());
                    }
                    s.deliveries.insert(
                        id.clone(),
                        ObservationDelivery {
                            id: id.clone(),
                            observation: o.id.clone(),
                            binding: b.clone(),
                            subscriber: binding.subscriber.clone(),
                            policy: binding.policy.clone(),
                            expires_at_ms: expiration,
                            state: DeliveryState::Pending,
                            attempts: vec![],
                            acknowledgment: None,
                            token_invalidated: false,
                            wake_key: format!("ingress-wake:{}", id.as_str()).try_into()?,
                            locator: format!("delivery:{}", id.as_str()).try_into()?,
                        },
                    );
                }
                if let Some(queue) = queue {
                    for q in queue {
                        s.queue.insert(q.delivery.clone(), q.clone());
                    }
                }
                s.admissions.insert(a.identity.clone(), a.clone());
                let next = contiguous(&s, &a.profile);
                let ordering = OrderingEvidence {
                    arrival: s.admissions.len() as u64,
                    offset: a.event.offset,
                    contiguous_next: next,
                    gap: a.event.offset.filter(|o| *o > previous_next).map(|o| (previous_next, o)),
                    out_of_order: a.event.offset.zip(previous_max).is_some_and(|(o, m)| o < m),
                };
                s.admissions.get_mut(&a.identity).unwrap().ordering = ordering;
                IngressResult::Admitted {
                    observation: o.id.clone(),
                    deliveries: a.deliveries.clone(),
                    duplicate: false,
                    contiguous_next: next,
                }
            }
        }
        IngressCommand::RecordQueueTask { id, generation, task_digest } => {
            let q = s.queue.get_mut(id).ok_or("unknown queue obligation")?;
            if q.generation != *generation {
                IngressResult::StaleQueueGeneration
            } else if q.task_digest != *task_digest {
                return Err("queue task identity mismatch".into());
            } else {
                q.task_admitted = true;
                IngressResult::Updated
            }
        }
        IngressCommand::RecordQueueSignal { id, generation, signal_digest, sequence } => {
            let q = s.queue.get_mut(id).ok_or("unknown queue obligation")?;
            if q.generation != *generation {
                IngressResult::StaleQueueGeneration
            } else if q.signal_digest != *signal_digest {
                return Err("queue signal identity mismatch".into());
            } else if !q.task_admitted
                || *sequence == 0
                || q.signal_sequence.is_some_and(|old| old != *sequence)
            {
                return Err("invalid queue signal acknowledgment".into());
            } else {
                q.signal_sequence = Some(*sequence);
                IngressResult::Updated
            }
        }
        IngressCommand::RecoverQueue { id, expected_generation, replacement } => {
            let old = s.queue.get(id).ok_or("unknown queue obligation")?;
            let delivery = s.deliveries.get(id).ok_or("unknown delivery")?;
            if delivery.state != DeliveryState::Pending
                || now >= delivery.expires_at_ms
                || old.generation != *expected_generation
                || old.signal_sequence.is_none()
                || replacement.version != 1
                || replacement.delivery != *id
                || expected_generation.checked_add(1) != Some(replacement.generation)
                || replacement.task_admitted
                || replacement.signal_sequence.is_some()
                || !replacement.prior.is_empty()
                || replacement.task_request.is_empty()
                || replacement.signal_request.is_empty()
                || replacement.task_request.len() > 16384
                || replacement.signal_request.len() > 16384
                || ContentHash::of(&replacement.task_request) != replacement.task_digest
                || ContentHash::of(&replacement.signal_request) != replacement.signal_digest
            {
                return Err("invalid queue recovery".into());
            }
            let mut next = *replacement.clone();
            next.prior = old.prior.clone();
            let mut archived = old.clone();
            archived.prior.clear();
            next.prior.push(archived);
            s.queue.insert(id.clone(), next);
            IngressResult::Updated
        }
        IngressCommand::Claim { id, subscriber, token_digest } => {
            let d = s.deliveries.get(id).ok_or("unknown delivery")?;
            disclose(&s, d, subscriber, now)?;
            if d.state != DeliveryState::Pending
                || d.token_invalidated
                || d.attempts.last().is_some_and(|a| now < a.next_attempt_ms)
            {
                return Err("delivery not due".into());
            }
            let d = s.deliveries.get_mut(id).unwrap();
            if d.attempts.len() >= d.policy.max_attempts as usize {
                d.state = DeliveryState::DeadLetter;
                IngressResult::Updated
            } else {
                if d.attempts.iter().any(|a| a.token_digest == *token_digest) {
                    return Err("token reuse".into());
                }
                let next = now
                    .checked_add(d.policy.acknowledgment_timeout_ms)
                    .and_then(|v| v.checked_add(d.policy.backoff_ms))
                    .filter(|v| *v <= MAX_TIME)
                    .ok_or("retry time overflow")?;
                let attempt = DeliveryAttempt {
                    number: d.attempts.len() as u32 + 1,
                    issued_at_ms: now,
                    next_attempt_ms: next,
                    token_digest: token_digest.clone(),
                    status: AttemptStatus::Issued,
                };
                d.attempts.push(attempt.clone());
                IngressResult::Attempt(attempt)
            }
        }
        IngressCommand::Finish { id, attempt, succeeded } => {
            let d = s.deliveries.get_mut(id).ok_or("unknown delivery")?;
            let a =
                d.attempts.iter_mut().find(|a| a.number == *attempt).ok_or("unknown attempt")?;
            let status =
                if *succeeded { AttemptStatus::WakeSucceeded } else { AttemptStatus::WakeFailed };
            if a.status != AttemptStatus::Issued && a.status != status {
                return Err("attempt result conflict".into());
            }
            a.status = status;
            IngressResult::Updated
        }
        IngressCommand::Acknowledge { id, subscriber, token_digest }
        | IngressCommand::AcknowledgeQueued { id, subscriber, token_digest, .. } => {
            if let IngressCommand::AcknowledgeQueued { signal, .. } = command {
                if signal.version != 1
                    || signal.delivery != *id
                    || signal.request.is_empty()
                    || signal.request.len() > 16384
                    || signal.digest != ContentHash::of(&signal.request)
                    || signal.sequence.is_some()
                {
                    return Err("invalid acknowledgment signal".into());
                }
            }
            let d = s.deliveries.get_mut(id).ok_or("unknown delivery")?;
            if now < state.last_at_ms
                || &d.subscriber != subscriber
                || d.token_invalidated
                || now >= d.expires_at_ms
                || d.state == DeliveryState::Expired
            {
                return Err("acknowledgment denied".into());
            }
            let a = d
                .attempts
                .iter()
                .find(|a| a.token_digest == *token_digest)
                .ok_or("invalid delivery token")?;
            if let Some(ack) = &d.acknowledgment {
                if let IngressCommand::AcknowledgeQueued { signal, .. } = command {
                    if s.acknowledgments_to_signal.get(id) != Some(signal) {
                        // Once dispatched, the recorded sequence is the only mutable field.
                        if s.acknowledgments_to_signal.get(id).is_none_or(|old| {
                            old.request != signal.request || old.digest != signal.digest
                        }) {
                            return Err("acknowledgment signal conflict".into());
                        }
                    }
                }
                return Ok((state.clone(), IngressResult::Acknowledged(ack.clone())));
            }
            let ack = DeliveryAcknowledgment {
                delivery: id.clone(),
                subscriber: subscriber.clone(),
                acknowledged_at_ms: now,
                attempt: a.number,
            };
            d.acknowledgment = Some(ack.clone());
            d.state = DeliveryState::Acknowledged;
            if let IngressCommand::AcknowledgeQueued { signal, .. } = command {
                s.acknowledgments_to_signal.insert(id.clone(), signal.clone());
            }
            IngressResult::Acknowledged(ack)
        }
        IngressCommand::RecordAcknowledgmentSignal { id, sequence } => {
            let signal =
                s.acknowledgments_to_signal.get_mut(id).ok_or("unknown acknowledgment signal")?;
            if *sequence == 0 || signal.sequence.is_some_and(|old| old != *sequence) {
                return Err("invalid acknowledgment signal sequence".into());
            }
            signal.sequence = Some(*sequence);
            IngressResult::Updated
        }
        IngressCommand::InvalidateToken { id } => {
            s.deliveries.get_mut(id).ok_or("unknown delivery")?.token_invalidated = true;
            IngressResult::Updated
        }
        IngressCommand::Expire { id } => {
            let d = s.deliveries.get_mut(id).ok_or("unknown delivery")?;
            if now < d.expires_at_ms {
                return Err("not expired".into());
            }
            if d.state != DeliveryState::Acknowledged {
                d.state = DeliveryState::Expired;
            }
            d.token_invalidated = true;
            IngressResult::Updated
        }
        IngressCommand::Release { artifact, quarantine, evidence } => {
            if evidence.artifact.scope != *tenant
                || !s.admissions.values().any(|a| {
                    a.event.attachments.iter().any(|v| {
                        v.artifact == *artifact && v.quarantine.as_ref() == Some(quarantine)
                    })
                })
            {
                return Err("release does not match quarantine".into());
            }
            let release = (artifact.clone(), quarantine.clone(), evidence.clone());
            if !s.releases.contains(&release) {
                if s.releases.len() >= 10000 {
                    return Err("release capacity".into());
                }
                s.releases.push(release);
            }
            IngressResult::Updated
        }
    };
    if &s != state {
        s.revision = state.revision.checked_add(1).ok_or("revision overflow")?;
        s.last_at_ms = now;
    }
    Ok((s, result))
}
