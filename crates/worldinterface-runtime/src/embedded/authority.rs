use super::*;

pub(super) trait EvaluationGate: Send + Sync {
    fn verify(&self, now: u64) -> Result<()>;
}
impl<S: TrustedWorkerBuild> EvaluationGate for EvaluationVerifier<S> {
    fn verify(&self, now: u64) -> Result<()> {
        EvaluationVerifier::verify(self, now)?;
        Ok(())
    }
}
pub(super) struct ManagedEvidence<V, C> {
    pub bundle: Arc<StoreBundle>,
    pub base: Arc<V>,
    pub clock: SharedClock<C>,
    pub evaluation: Option<Arc<dyn EvaluationGate>>,
}
impl<V, C> Clone for ManagedEvidence<V, C> {
    fn clone(&self) -> Self {
        Self {
            bundle: self.bundle.clone(),
            base: self.base.clone(),
            clock: self.clock.clone(),
            evaluation: self.evaluation.clone(),
        }
    }
}
impl<V: EvidenceAuthority, C: TrustedClock> EvidenceAuthority for ManagedEvidence<V, C> {
    fn current_snapshot(
        &self,
        tenant: &OpaqueRef,
        submission: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        let mut snapshot = self.base.current_snapshot(tenant, submission)?;
        if let Some(recovery) = &snapshot.recovery {
            if !["inspect", "terminate"].contains(&recovery.operation.as_str())
                || snapshot.fixture.is_some()
                || snapshot.qualification.is_some()
            {
                return Err("invalid embedded recovery scope".into());
            }
        } else if snapshot.worker.is_some() {
            self.evaluation
                .as_ref()
                .ok_or("evaluation approval unavailable")?
                .verify(self.clock.now()?.epoch_millis()?)?;
        }
        let principal =
            snapshot.bindings.first().ok_or("managed binding absent")?.principal.clone();
        let caller = AuthenticatedCaller::from_trusted_authentication(tenant.clone(), principal);
        let mut managed = self.bundle.wi().control_plane_snapshot()?;
        let effects = self.bundle.wi().effect_snapshot(tenant)?;
        for effect in effects.effects.values().filter(|e| &e.intent.submission == submission) {
            let operation = ContentHash::of(&canonical_data(&(
                "wi-effect-use-v1",
                tenant,
                &effect.intent.id,
                effect.dispatches.len(),
            ))?);
            if let Some(limits) = managed.effect_reservations.get(&operation).cloned() {
                for limit in limits {
                    if let Some(n) = managed.uses.get_mut(&limit.key) {
                        *n = n.saturating_sub(1);
                    }
                }
            }
        }
        let scope = managed
            .execution_authority(&caller, self.clock.now()?.epoch_millis()?, &mut snapshot)
            .map_err(control_error)?;
        if let Some(worker) = &snapshot.worker {
            if self.bundle.wi().worker_snapshot(tenant, &worker.provider, &worker.run)?.is_some() {
                let reference = ObjectRef {
                    scope,
                    kind: ObjectKind::Worker,
                    id: worker_id(&worker.provider, &worker.run)?,
                };
                if !managed.objects.iter().any(|o| o.reference == reference) {
                    return Err("worker scope unavailable".into());
                }
            }
        }
        Ok(snapshot)
    }
    fn verify(
        &self,
        tenant: &OpaqueRef,
        reference: &EvidenceRef,
        at: &Timestamp,
    ) -> Result<ContentHash> {
        // Managed grant evidence is owned by the attributed control journal.
        let state = self.bundle.wi().control_plane_snapshot()?;
        if state.history.iter().any(|r| matches!(&r.content, cp::Content::Grant(g) if &g.tenant == tenant && &g.evidence == reference)) {
            self.bundle.wi().read_artifact(&reference.artifact)?;
            return Ok(reference.artifact.content.clone());
        }
        self.base.verify(tenant, reference, at)
    }
}
pub(super) struct SharedClock<C>(pub Arc<C>);
impl<C> Clone for SharedClock<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<C: TrustedClock> TrustedClock for SharedClock<C> {
    fn now(&self) -> Result<Timestamp> {
        self.0.now()
    }
}
impl<C: TrustedClock> cp::TrustedClock for SharedClock<C> {
    fn now_ms(&self) -> u64 {
        self.now().and_then(|v| Ok(v.epoch_millis()?)).unwrap_or(u64::MAX)
    }
}
impl<C: TrustedClock + Send + Sync> crate::worker_process::Clock for SharedClock<C> {
    fn unix_ms(&self) -> u64 {
        cp::TrustedClock::now_ms(self)
    }
}
struct ControlAuthentication<'a, A>(&'a A);
impl<A: Authentication> cp::Authentication for ControlAuthentication<'_, A> {
    type Session = A::Session;
    fn authenticate(&self, s: &Self::Session) -> cp::Result<AuthenticatedCaller> {
        crate::authority::caller(self.0, s).map_err(|_| cp::Error::Unavailable)
    }
}
pub(super) fn control<A: Authentication, C: TrustedClock, K: cp::Custody>(
    root: &worldinterface_storage::StoreRoot,
    auth: &A,
    clock: &SharedClock<C>,
    custody: &K,
    session: &A::Session,
    request: &cp::Request,
) -> Result<cp::Outcome> {
    cp::Service::new(
        root.control_plane(),
        ControlAuthentication(auth),
        clock.clone(),
        CustodyRef(custody),
    )
    .execute(session, request)
    .map_err(control_error)
}
struct CustodyRef<'a, K>(&'a K);
impl<K: cp::Custody> cp::Custody for CustodyRef<'_, K> {
    fn deliver(
        &self,
        r: &OpaqueRef,
        m: &CredentialMetadata,
    ) -> std::result::Result<(), CustodyReason> {
        self.0.deliver(r, m)
    }
    fn deliver_attempt(
        &self,
        r: &OpaqueRef,
        m: &CredentialMetadata,
        a: &CredentialDeliveryAttempt,
    ) -> std::result::Result<(), CustodyReason> {
        self.0.deliver_attempt(r, m, a)
    }
    fn revoke(&self, r: &ObjectRef, v: u64, g: u64) -> std::result::Result<(), CustodyReason> {
        self.0.revoke(r, v, g)
    }
}
pub(super) struct Configuration<P, C> {
    pub bundle: Arc<StoreBundle>,
    pub base: Arc<P>,
    pub clock: SharedClock<C>,
}
impl<P, C> Clone for Configuration<P, C> {
    fn clone(&self) -> Self {
        Self { bundle: self.bundle.clone(), base: self.base.clone(), clock: self.clock.clone() }
    }
}
impl<P: ArtifactAuthorization<AuthenticatedContext = AuthenticatedCaller>, C: TrustedClock>
    ArtifactAuthorization for Configuration<P, C>
{
    type AuthenticatedContext = AuthenticatedCaller;
    fn authorize_read(&self, caller: &AuthenticatedCaller, artifact: &ArtifactRef) -> bool {
        if !self.base.authorize_read(caller, artifact) {
            return false;
        }
        (|| -> Result<()> {
            let state = self.bundle.wi().control_plane_snapshot()?;
            let scope = state
                .caller_scope(caller, self.clock.now()?.epoch_millis()?)
                .map_err(control_error)?;
            let reference = artifact_reference(scope, artifact);
            let request = cp::Request {
                id: "embedded-artifact-read".to_string().try_into()?,
                operation: cp::Operation::Get { reference },
            };
            let (_, outcome) =
                cp::apply(&state, caller, &request, self.clock.now()?.epoch_millis()?)
                    .map_err(control_error)?;
            if !matches!(outcome,cp::Outcome::Object(o) if o.artifact.as_ref() == Some(artifact)) {
                return Err("artifact unavailable".into());
            }
            Ok(())
        })()
        .is_ok()
    }
}
impl<P: PlanConfiguration, C: TrustedClock> PlanConfiguration for Configuration<P, C> {
    fn pins(&self, caller: &AuthenticatedCaller) -> Result<worldinterface_storage::flow::FlowPins> {
        self.base.pins(caller)
    }
}
