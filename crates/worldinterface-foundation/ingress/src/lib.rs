//! Authenticated ingress domain facade; the production composition lives in worldinterface-runtime.
pub mod transport;
use worldinterface_domain::ingress::*;
use worldinterface_protocol::*;
use worldinterface_storage::StoreRoot;
pub type Result<T> = worldinterface_storage::Result<T>;
pub(crate) fn error(s: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, s.into()).into()
}

#[derive(Clone)]
pub struct AuthenticatedScope {
    pub tenant: OpaqueRef,
    pub principal: PrincipalId,
}
/// A trusted resolver/authorization port, with no library implementation. Submission
/// assertions are inputs to this decision, never a substitute for it.
pub trait BindingAuthority {
    fn resolve_and_authorize(
        &self,
        scope: &AuthenticatedScope,
        submission: &IngressBindingSubmission,
        now: u64,
    ) -> Result<BindingGrant>;
}
pub struct BindingGrant {
    pub subscriber: PrincipalId,
    pub profile: ProviderProfile,
    pub policy: DeliveryPolicy,
    pub resolver_version: OpaqueRef,
    pub authorization: EvidenceRef,
    pub authentication_requirement: OpaqueRef,
    pub expires_at_ms: u64,
}
/// Secret capability, intentionally neither Debug nor Serialize. Only its delivery-bound
/// digest is journaled. The issuing trusted subscriber channel receives this value once.
pub struct DeliveryToken(String);
impl DeliveryToken {
    pub fn generate() -> Self {
        Self(format!("{}{}", InstanceId::generate().as_str(), InstanceId::generate().as_str()))
    }
    pub fn expose_to_subscriber(&self) -> &str {
        &self.0
    }
    pub fn from_subscriber(value: &str) -> Result<Self> {
        if !value.is_ascii() || value.len() != 72 {
            return Err(error("invalid token length"));
        }
        let _: InstanceId = value[..36].to_string().try_into().map_err(error)?;
        let _: InstanceId = value[36..].to_string().try_into().map_err(error)?;
        Ok(Self(value.to_owned()))
    }
    fn digest(&self, id: &DeliveryId, subscriber: &PrincipalId) -> Result<ContentHash> {
        digest(&(id, subscriber, &self.0)).map_err(error)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryView {
    pub id: DeliveryId,
    pub observation: ObservationId,
    pub state: DeliveryState,
    pub attempt_count: usize,
    pub attempts: Vec<(u32, u64, AttemptStatus)>,
    pub admitting_binding: BindingVersion,
    pub provider_identity: ContentHash,
    pub asserted_sender: OpaqueRef,
    pub asserted_audience: Vec<OpaqueRef>,
    pub acknowledgment: Option<DeliveryAcknowledgment>,
    pub content: ArtifactRef,
    pub attachments: Vec<ArtifactRef>,
    pub receipt: BoundaryReceipt,
    pub ordering: OrderingEvidence,
    pub provenance: TransportProvenance,
    pub profile: ObservationProfile,
    pub information_use: InformationUseEnvelope,
}
pub struct IssuedDelivery {
    pub view: DeliveryView,
    pub token: DeliveryToken,
    pub attempt: u32,
    pub wake_key: OpaqueRef,
    pub locator: OpaqueRef,
}
pub struct AdmissionIds {
    pub observation: ObservationId,
    pub receipt: ReceiptId,
    pub deliveries: Vec<DeliveryId>,
}
pub struct Ingress<'a> {
    root: &'a StoreRoot,
}
impl<'a> Ingress<'a> {
    pub fn new(root: &'a StoreRoot) -> Self {
        Self { root }
    }
    pub fn submit_binding(
        &self,
        scope: &AuthenticatedScope,
        submission: IngressBindingSubmission,
        id: IngressBindingId,
        authority: &impl BindingAuthority,
        now: u64,
    ) -> Result<BindingVersion> {
        let grant = authority.resolve_and_authorize(scope, &submission, now)?;
        if grant.profile.scope.tenant != scope.tenant {
            return Err(error("authority scope mismatch"));
        }
        self.root.read_artifact(&grant.authorization.artifact)?;
        let binding = IngressBinding {
            id,
            revision: 1,
            principal: scope.principal.clone(),
            submission,
            subscriber: grant.subscriber,
            profile: grant.profile,
            policy: grant.policy,
            resolver_version: grant.resolver_version,
            authorization: grant.authorization,
            authentication_requirement: grant.authentication_requirement,
            expires_at_ms: grant.expires_at_ms,
            status: BindingStatus::Active,
        };
        match self.root.ingress_transact(
            &scope.tenant,
            None,
            now,
            &IngressCommand::Bind { binding: Box::new(binding) },
        )? {
            IngressResult::Binding(v) => Ok(v),
            _ => Err(error("unexpected binding result")),
        }
    }
    pub fn change_binding(
        &self,
        scope: &AuthenticatedScope,
        version: &BindingVersion,
        status: BindingStatus,
        now: u64,
    ) -> Result<()> {
        let s = self.root.ingress_snapshot(&scope.tenant)?;
        if s.bindings.get(&version.id).is_none_or(|b| b.principal != scope.principal) {
            return Err(error("binding owner mismatch"));
        }
        self.root.ingress_transact(
            &scope.tenant,
            Some(s.revision),
            now,
            &IngressCommand::ChangeBinding {
                id: version.id.clone(),
                expected: version.revision,
                status,
            },
        )?;
        Ok(())
    }
    pub fn bindings(&self, scope: &AuthenticatedScope) -> Result<Vec<IngressBinding>> {
        Ok(self
            .root
            .ingress_snapshot(&scope.tenant)?
            .bindings
            .into_values()
            .filter(|b| b.principal == scope.principal)
            .collect())
    }
    pub fn admit_provider_event(
        &self,
        event: transport::ValidatedProviderEvent,
        expected: Vec<BindingVersion>,
        ids: AdmissionIds,
        now: u64,
    ) -> Result<IngressResult> {
        self.admit_event(event, expected, ids, None, now)
    }
    pub fn admit_provider_event_queued(
        &self,
        event: transport::ValidatedProviderEvent,
        expected: Vec<BindingVersion>,
        ids: AdmissionIds,
        queue: Vec<QueueObligation>,
        now: u64,
    ) -> Result<IngressResult> {
        self.admit_event(event, expected, ids, Some(queue), now)
    }
    fn admit_event(
        &self,
        event: transport::ValidatedProviderEvent,
        expected: Vec<BindingVersion>,
        ids: AdmissionIds,
        queue: Option<Vec<QueueObligation>>,
        now: u64,
    ) -> Result<IngressResult> {
        let transport::ValidatedProviderEvent { event, profile, provenance } = event;
        self.root.read_artifact(&event.content)?;
        for a in &event.attachments {
            self.root.read_artifact(&a.artifact)?;
        }
        validate_event_references(&profile, &event).map_err(error)?;
        for evidence in profile_evidence(&event.profile) {
            self.root.read_artifact(&evidence.artifact)?;
        }
        let identity = provider_identity(&profile, &event, &ids.observation).map_err(error)?;
        let binding_id = expected.first().ok_or_else(|| error("no binding capture"))?.id.clone();
        let admitted_time = time(now)?;
        let mut occurred_time = time(event.produced_at_ms)?;
        occurred_time.source = event.scope.account.clone();
        occurred_time.assurance = "provider-asserted".to_string().try_into().map_err(error)?;
        let temporal = TemporalProvenance {
            occurred_at: Some(occurred_time),
            observed_at: None,
            received_at: admitted_time.clone(),
            admitted_at: admitted_time,
            dispatched_at: None,
            committed_at: None,
            reconciled_at: None,
            sequence: event.offset.map(|o| o.to_string().try_into().map_err(error)).transpose()?,
        };
        let observation = BoundaryObservation {
            id: ids.observation,
            binding_id: binding_id.clone(),
            source_connector: provenance.transport.clone(),
            canonical_source: event.scope.source.clone(),
            provider_event_id: event.event_id.clone(),
            deduplication_key: identity.as_str().to_owned().try_into().map_err(error)?,
            asserted_sender: event.asserted_sender.clone(),
            asserted_audience: event.asserted_audience.clone(),
            authentication_assurance: provenance.assurance.clone(),
            content_ref: event.content.clone(),
            attachment_refs: event.attachments.iter().map(|a| a.artifact.clone()).collect(),
            temporal: temporal.clone(),
            information_use: Some(event.information_use.clone()),
        };
        let state = self.root.ingress_snapshot(&profile.scope.tenant)?;
        let binding = state.bindings.get(&binding_id).ok_or_else(|| error("unknown binding"))?;
        let mut grants = Vec::new();
        let mut evidence = Vec::new();
        for version in &expected {
            let matched =
                state.bindings.get(&version.id).ok_or_else(|| error("unknown binding"))?;
            grants.push(digest(&matched.authorization).map_err(error)?);
            evidence.push(matched.authorization.clone());
        }
        let profile_hash = digest(&profile).map_err(error)?;
        let receipt = BoundaryReceipt {
            id: ids.receipt,
            profile: profile.version.clone(),
            versions: BoundaryVersions {
                resolver: binding.resolver_version.clone(),
                capability_descriptor: profile_hash.clone(),
                policy: binding.policy.version.clone(),
                grant_versions: grants,
                connector_profile: profile_hash.clone(),
                provider_profile: profile_hash,
            },
            body: ReceiptBody::Ingress(IngressReceipt {
                observation_id: observation.id.clone(),
                binding_id,
                source: event.scope.source.clone(),
                observation_digest: Envelope::new(Record::BoundaryObservation(observation.clone()))
                    .digest()
                    .map_err(error)?,
                validation: "transport-validated-no-semantic-authority"
                    .to_string()
                    .try_into()
                    .map_err(error)?,
            }),
            temporal,
            evidence,
            causal_refs: vec![],
        };
        let tenant = profile.scope.tenant.clone();
        let admission = IngressAdmission {
            identity,
            producer_digest: producer_digest(&profile, &event).map_err(error)?,
            event,
            profile,
            provenance,
            observation,
            receipt,
            bindings: expected,
            deliveries: ids.deliveries,
            ordering: OrderingEvidence {
                arrival: 0,
                offset: None,
                contiguous_next: 0,
                gap: None,
                out_of_order: false,
            },
            admitted_at_ms: now,
        };
        let command = match queue {
            Some(queue) => IngressCommand::AdmitQueued { admission: Box::new(admission), queue },
            None => IngressCommand::Admit { admission: Box::new(admission) },
        };
        self.root.ingress_transact(&tenant, None, now, &command)
    }
    pub fn inspect_delivery(
        &self,
        scope: &AuthenticatedScope,
        id: &DeliveryId,
        now: u64,
    ) -> Result<DeliveryView> {
        let s = self.root.ingress_snapshot(&scope.tenant)?;
        view(&s, scope, id, now)
    }
    pub fn due_deliveries(
        &self,
        scope: &AuthenticatedScope,
        now: u64,
        limit: usize,
    ) -> Result<Vec<DeliveryId>> {
        if limit == 0 || limit > 1000 {
            return Err(error("query bound"));
        }
        let s = self.root.ingress_snapshot(&scope.tenant)?;
        Ok(s.deliveries
            .values()
            .filter(|d| {
                d.state == DeliveryState::Pending
                    && !d.token_invalidated
                    && d.attempts.last().is_none_or(|a| now >= a.next_attempt_ms)
                    && disclose(&s, d, &scope.principal, now).is_ok()
            })
            .take(limit)
            .map(|d| d.id.clone())
            .collect())
    }
    pub fn claim_due_delivery(
        &self,
        scope: &AuthenticatedScope,
        id: &DeliveryId,
        token: DeliveryToken,
        now: u64,
    ) -> Result<Option<IssuedDelivery>> {
        let s = self.root.ingress_snapshot(&scope.tenant)?;
        let mut view = view(&s, scope, id, now)?;
        let token_digest = token.digest(id, &scope.principal)?;
        match self.root.ingress_transact(
            &scope.tenant,
            Some(s.revision),
            now,
            &IngressCommand::Claim {
                id: id.clone(),
                subscriber: scope.principal.clone(),
                token_digest,
            },
        )? {
            IngressResult::Attempt(a) => {
                let d = &s.deliveries[id];
                view.attempt_count += 1;
                view.attempts.push((a.number, a.issued_at_ms, a.status.clone()));
                Ok(Some(IssuedDelivery {
                    view,
                    token,
                    attempt: a.number,
                    wake_key: d.wake_key.clone(),
                    locator: d.locator.clone(),
                }))
            }
            IngressResult::Updated => Ok(None),
            _ => Err(error("unexpected claim result")),
        }
    }
    /// Trusted local dispatcher reports only wake transport outcome; this never acknowledges ingestion.
    pub fn record_wake(
        &self,
        tenant: &OpaqueRef,
        id: DeliveryId,
        attempt: u32,
        succeeded: bool,
        now: u64,
    ) -> Result<()> {
        self.root.ingress_transact(
            tenant,
            None,
            now,
            &IngressCommand::Finish { id, attempt, succeeded },
        )?;
        Ok(())
    }
    pub fn acknowledge(
        &self,
        scope: &AuthenticatedScope,
        id: &DeliveryId,
        token: &DeliveryToken,
        now: u64,
    ) -> Result<DeliveryAcknowledgment> {
        self.acknowledge_with_signal(scope, id, token, None, now)
    }
    pub fn acknowledge_queued(
        &self,
        scope: &AuthenticatedScope,
        id: &DeliveryId,
        token: &DeliveryToken,
        signal: AcknowledgmentSignal,
        now: u64,
    ) -> Result<DeliveryAcknowledgment> {
        self.acknowledge_with_signal(scope, id, token, Some(signal), now)
    }
    fn acknowledge_with_signal(
        &self,
        scope: &AuthenticatedScope,
        id: &DeliveryId,
        token: &DeliveryToken,
        signal: Option<AcknowledgmentSignal>,
        now: u64,
    ) -> Result<DeliveryAcknowledgment> {
        let token_digest = token.digest(id, &scope.principal)?;
        let command = match signal {
            Some(signal) => IngressCommand::AcknowledgeQueued {
                id: id.clone(),
                subscriber: scope.principal.clone(),
                token_digest,
                signal,
            },
            None => IngressCommand::Acknowledge {
                id: id.clone(),
                subscriber: scope.principal.clone(),
                token_digest,
            },
        };
        let result = self.root.ingress_transact(&scope.tenant, None, now, &command)?;
        match result {
            IngressResult::Acknowledged(a) => Ok(a),
            _ => Err(error("unexpected acknowledgment result")),
        }
    }
    /// Owner can invalidate outstanding capabilities without deleting historical evidence.
    pub fn invalidate_token(
        &self,
        scope: &AuthenticatedScope,
        id: &DeliveryId,
        now: u64,
    ) -> Result<()> {
        let s = self.root.ingress_snapshot(&scope.tenant)?;
        let d = s.deliveries.get(id).ok_or_else(|| error("unknown delivery"))?;
        if s.bindings[&d.binding.id].principal != scope.principal {
            return Err(error("binding owner mismatch"));
        }
        self.root.ingress_transact(
            &scope.tenant,
            Some(s.revision),
            now,
            &IngressCommand::InvalidateToken { id: id.clone() },
        )?;
        Ok(())
    }
    pub fn expire_delivery(&self, tenant: &OpaqueRef, id: &DeliveryId, now: u64) -> Result<()> {
        self.root.ingress_transact(
            tenant,
            None,
            now,
            &IngressCommand::Expire { id: id.clone() },
        )?;
        Ok(())
    }
    /// Inactive, trusted policy seam: a release evidence reference must have been independently
    /// authorized by the caller. Provider assertions cannot call this through either transport.
    pub fn release_attachment(
        &self,
        tenant: &OpaqueRef,
        artifact: ArtifactRef,
        quarantine: OpaqueRef,
        evidence: EvidenceRef,
        now: u64,
    ) -> Result<()> {
        self.root.read_artifact(&evidence.artifact)?;
        self.root.ingress_transact(
            tenant,
            None,
            now,
            &IngressCommand::Release { artifact, quarantine, evidence },
        )?;
        Ok(())
    }
}
fn view(
    s: &IngressState,
    scope: &AuthenticatedScope,
    id: &DeliveryId,
    now: u64,
) -> Result<DeliveryView> {
    let d = s.deliveries.get(id).ok_or_else(|| error("unknown delivery"))?;
    disclose(s, d, &scope.principal, now).map_err(error)?;
    let a = s
        .admissions
        .values()
        .find(|a| a.observation.id == d.observation)
        .ok_or_else(|| error("missing observation"))?;
    validate_event_references(&a.profile, &a.event).map_err(error)?;
    // Required fields cannot be redacted without changing the view's meaning.
    if !artifact_disclosable(s, &a.event.content)
        || profile_evidence(&a.event.profile)
            .first()
            .is_some_and(|e| !artifact_disclosable(s, &e.artifact))
        || a.receipt.evidence.iter().any(|e| !artifact_disclosable(s, &e.artifact))
    {
        return Err(error("required delivery reference quarantined"));
    }
    let attachments = a
        .event
        .attachments
        .iter()
        .filter(|v| artifact_disclosable(s, &v.artifact))
        .map(|v| v.artifact.clone())
        .collect();
    let mut profile = a.event.profile.clone();
    if let ObservationProfile::RoleStewardship { capacity, .. } = &mut profile {
        if capacity.attestation_ref.as_ref().is_some_and(|e| !artifact_disclosable(s, &e.artifact))
        {
            capacity.attestation_ref = None;
        }
    }
    Ok(DeliveryView {
        id: d.id.clone(),
        observation: d.observation.clone(),
        state: d.state.clone(),
        attempt_count: d.attempts.len(),
        attempts: d.attempts.iter().map(|v| (v.number, v.issued_at_ms, v.status.clone())).collect(),
        admitting_binding: d.binding.clone(),
        provider_identity: a.identity.clone(),
        asserted_sender: a.event.asserted_sender.clone(),
        asserted_audience: a.event.asserted_audience.clone(),
        acknowledgment: d.acknowledgment.clone(),
        content: a.event.content.clone(),
        attachments,
        receipt: a.receipt.clone(),
        ordering: a.ordering.clone(),
        provenance: a.provenance.clone(),
        profile,
        information_use: a.event.information_use.clone(),
    })
}
fn time(ms: u64) -> Result<TimeEvidence> {
    let t = chrono::DateTime::from_timestamp_millis(i64::try_from(ms)?)
        .ok_or_else(|| error("invalid timestamp"))?;
    Ok(TimeEvidence {
        at: t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true).try_into().map_err(error)?,
        source: "explicit-ingress-clock".to_string().try_into().map_err(error)?,
        assurance: "caller-supplied".to_string().try_into().map_err(error)?,
        uncertainty_nanos: 0,
    })
}
