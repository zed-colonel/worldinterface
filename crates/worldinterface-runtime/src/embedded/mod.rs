//! Embedded application services over real WI repositories and the sole AQ scheduler.
//!
//! Trusted construction supplies adapters and bootstrap authority. Consumers receive
//! only [`EmbeddedApplication`]; all controls and disclosure are authenticated.
//! This is an API boundary inside a trusted Rust process, not native-code isolation.
//!
//! ```compile_fail
//! use worldinterface_runtime::embedded::EmbeddedApplication;
//! # use worldinterface_runtime::{authority::{EvidenceAuthority,TrustedClock},resolution::ResourceObserver,worker::connector::TrustedWorkerBuild};
//! fn escape<A,V,R,C,P,K,S>(app: EmbeddedApplication<'_,A,V,R,C,P,K,S>)
//! where S:TrustedWorkerBuild+'static,V:EvidenceAuthority+Send+Sync+'static,
//! R:ResourceObserver+Send+Sync+'static,C:TrustedClock+Send+Sync+'static {
//!     let engine = app.queue;
//! }
//! ```
mod authority;
mod credential;
mod dispatch;
use crate::{
    artifact::ArtifactAuthorization,
    authority::{Authentication, EvidenceAuthority, TrustedClock},
    flow::{PlanConfiguration, PlanService},
    resolution::ResourceObserver,
    storage::StoreBundle,
    worker::{
        catalog::RecoveryPolicy,
        connector::{TrustedWorkerBuild, WorkerRuntime},
        evaluation::EvaluationVerifier,
    },
    worker_process::HostScope,
    Result,
};
use authority::*;
pub use credential::WorkerCredentialMetadata;
use dispatch::*;
use std::{path::PathBuf, sync::Arc};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::AuthorizationSnapshot,
    control_plane as cp,
    flow::DirectOperationSelection,
    worker::{WorkerCollection, WorkerState},
};
use worldinterface_protocol::{control_plane::*, *};
pub use worldinterface_storage::flow::{FlowOutcome, FlowRun};

fn control_error(e: cp::Error) -> Box<dyn std::error::Error + Send + Sync> {
    format!("control plane {e:?}").into()
}
fn hash<T: serde::Serialize>(v: &T) -> Result<ContentHash> {
    Ok(worldinterface_protocol::collection_digest(v)?)
}
/// Stable exact artifact identity, including the authoritative organization.
pub fn artifact_reference(scope: Scope, artifact: &ArtifactRef) -> ObjectRef {
    ObjectRef {
        scope,
        kind: ObjectKind::Artifact,
        id: artifact.content.as_str().to_string().try_into().expect("bounded digest"),
    }
}
pub fn artifact_import_demand(scope: &Scope, content: &ContentHash) -> Result<ContentHash> {
    hash(&("wi-embedded-import-v1", scope, content))
}
pub fn reconciliation_demand(reference: &ObjectRef) -> Result<ContentHash> {
    hash(&("wi-embedded-reconcile-v1", reference))
}
/// Trusted host construction only; never deserialize these ports from a request.
pub struct EmbeddedPorts<A, V, R, C, P, K> {
    pub authentication: A,
    pub evidence: V,
    pub resources: R,
    pub clock: C,
    pub configuration: P,
    pub custody: K,
    pub credential_recipient: OpaqueRef,
}
/// Recovery needs neither an evaluation approval nor executable files.
pub enum WorkerInstallation<S> {
    Process {
        scope: HostScope,
        workspace: PathBuf,
        fixture: PathBuf,
        helper: PathBuf,
        evaluation: Arc<EvaluationVerifier<S>>,
        slots: u32,
    },
    /// Trusted evaluation-only composition; unqualified external providers are rejected.
    SeamSimulation {
        scope: HostScope,
        provider: Arc<dyn crate::worker::provider::AdmittedProvider>,
        provenance: crate::worker::provider::ProviderProvenance,
        evaluation: Arc<EvaluationVerifier<S>>,
        slots: u32,
    },
    Recovery {
        scope: HostScope,
        workspace: PathBuf,
        policy: RecoveryPolicy,
    },
}
/// Sanitized producer observations. No workspace, process handle, credential or store is disclosed.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerEvidence {
    pub binding: worldinterface_domain::worker::WorkerBinding,
    pub seam_simulation: bool,
    pub controls: Option<worldinterface_worker::controls::ControlEvidenceV1>,
    pub cleanup: Option<WorkerCleanupEvidence>,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCleanupEvidence {
    pub exit_confirmed: bool,
    pub exit_success: Option<bool>,
    pub cleanup_confirmed: bool,
    pub retained_output_bytes: u64,
    pub term_signals: u32,
    pub kill_signals: u32,
    pub descendants_observed: u32,
}
/// Owns private stores, provider, adapters and the exclusive local host lease.
pub struct EmbeddedHost<A, V, R, C, P, K, S> {
    bundle: Arc<StoreBundle>,
    runtime: Arc<WorkerRuntime<S>>,
    authentication: A,
    evidence: ManagedEvidence<V, C>,
    resources: Arc<R>,
    clock: SharedClock<C>,
    configuration: Arc<Configuration<P, C>>,
    custody: K,
    credential_recipient: OpaqueRef,
    _ownership: std::fs::File,
}
/// A running application owns one AQ engine and borrows its trusted host.
///
/// ```compile_fail
/// use worldinterface_runtime::embedded::EmbeddedApplication;
/// # use worldinterface_runtime::{authority::{EvidenceAuthority,TrustedClock},resolution::ResourceObserver,worker::connector::TrustedWorkerBuild};
/// fn bypass<A,V,R,C,P,K,S>(app: EmbeddedApplication<'_,A,V,R,C,P,K,S>)
/// where S:TrustedWorkerBuild+'static,V:EvidenceAuthority+Send+Sync+'static,
/// R:ResourceObserver+Send+Sync+'static,C:TrustedClock+Send+Sync+'static {
///     let stores = app.host.bundle;
/// }
/// ```
pub struct EmbeddedApplication<'host, A, V, R, C, P, K, S>
where
    S: TrustedWorkerBuild + 'static,
    V: EvidenceAuthority + Send + Sync + 'static,
    R: ResourceObserver + Send + Sync + 'static,
    C: TrustedClock + Send + Sync + 'static,
{
    host: &'host EmbeddedHost<A, V, R, C, P, K, S>,
    queue: crate::aq_adapter::QueueEngine<'host, Dispatcher<S, V, ArcObserver<R>, C>>,
}
pub(super) struct ArcObserver<R>(Arc<R>);
impl<R> Clone for ArcObserver<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<R: ResourceObserver> ResourceObserver for ArcObserver<R> {
    fn observe(
        &self,
        t: &OpaqueRef,
        s: &EffectSubmission,
        r: &worldinterface_domain::resource::ResolverSnapshot,
        at: &Timestamp,
    ) -> Result<worldinterface_domain::resource::ResolutionFact> {
        self.0.observe(t, s, r, at)
    }
    fn compare_before_use(
        &self,
        f: &worldinterface_domain::resource::ResolutionFact,
    ) -> Result<worldinterface_domain::resource::TargetDocument> {
        self.0.compare_before_use(f)
    }
}
impl<A, V, R, C, P, K, S> EmbeddedHost<A, V, R, C, P, K, S>
where
    A: Authentication,
    V: EvidenceAuthority + Send + Sync + 'static,
    R: ResourceObserver + Send + Sync + 'static,
    C: TrustedClock + Send + Sync + 'static,
    P: PlanConfiguration + Send + Sync + 'static,
    K: cp::Custody,
    S: TrustedWorkerBuild + 'static,
{
    pub fn install(
        bundle: StoreBundle,
        installation: WorkerInstallation<S>,
        ports: EmbeddedPorts<A, V, R, C, P, K>,
        bootstrap: Option<cp::Bootstrap>,
    ) -> Result<Self> {
        let ownership = bundle.wi().claim_embedded_host()?;
        let bundle = Arc::new(bundle);
        let clock = SharedClock(Arc::new(ports.clock));
        if let Some(bootstrap) = bootstrap {
            bundle.wi().bootstrap_control_plane(bootstrap, clock.now()?.epoch_millis()?)?;
        }
        if bundle.wi().control_plane_snapshot()?.configuration.is_none() {
            return Err("control plane must be bootstrapped".into());
        }
        let evaluation_gate: Option<Arc<dyn EvaluationGate>> = match &installation {
            WorkerInstallation::Process { evaluation, .. }
            | WorkerInstallation::SeamSimulation { evaluation, .. } => Some(evaluation.clone()),
            WorkerInstallation::Recovery { .. } => None,
        };
        let scope = match &installation {
            WorkerInstallation::Process { scope, .. }
            | WorkerInstallation::SeamSimulation { scope, .. }
            | WorkerInstallation::Recovery { scope, .. } => scope.clone(),
        };
        crate::worker::recovery::recover_quarantines(bundle.wi(), &scope)?;
        let runtime = Arc::new(match installation {
            WorkerInstallation::Process {
                scope,
                workspace,
                fixture,
                helper,
                evaluation,
                slots,
            } => WorkerRuntime::install(
                bundle.clone(),
                scope,
                &workspace,
                &fixture,
                &helper,
                evaluation,
                Arc::new(clock.clone()),
                slots,
            )?,
            WorkerInstallation::SeamSimulation {
                scope,
                provider,
                provenance,
                evaluation,
                slots,
            } => WorkerRuntime::install_provider(
                bundle.clone(),
                scope,
                provider,
                provenance,
                evaluation,
                Arc::new(clock.clone()),
                slots,
            )?,
            WorkerInstallation::Recovery { scope, workspace, policy } => {
                WorkerRuntime::install_recovery(
                    bundle.clone(),
                    scope,
                    &workspace,
                    policy,
                    Arc::new(clock.clone()),
                )?
            }
        });
        Ok(Self {
            evidence: ManagedEvidence {
                bundle: bundle.clone(),
                base: Arc::new(ports.evidence),
                evaluation: evaluation_gate,
                clock: clock.clone(),
            },
            configuration: Arc::new(Configuration {
                bundle: bundle.clone(),
                base: Arc::new(ports.configuration),
                clock: clock.clone(),
            }),
            bundle,
            runtime,
            authentication: ports.authentication,
            resources: Arc::new(ports.resources),
            clock,
            custody: ports.custody,
            credential_recipient: ports.credential_recipient,
            _ownership: ownership,
        })
    }
    pub fn application(&self) -> Result<EmbeddedApplication<'_, A, V, R, C, P, K, S>> {
        let port = Arc::new(ProcessDispatcher::new(&self.runtime)?);
        let handler = Dispatcher {
            other: crate::flow_node::FlowAttemptHandler {
                effect: crate::completion::EffectAttemptHandler {
                    bundle: self.bundle.clone(),
                    port: port.clone(),
                    evidence: self.evidence.clone(),
                    clock: self.clock.clone(),
                },
                resolver: ArcObserver(self.resources.clone()),
                artifacts: self.configuration.clone(),
            },
            reconciliation: crate::reconciliation::ReconciliationHandler {
                bundle: self.bundle.clone(),
                connector: port,
                clock: self.clock.clone(),
            },
        };
        let host = actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("wi-embedded-host")?,
            ),
        };
        let application = EmbeddedApplication {
            host: self,
            queue: crate::aq_adapter::QueueEngine::bootstrap(&self.bundle, handler, host)?,
        };
        application.project()?;
        Ok(application)
    }
}
impl<A, V, R, C, P, K, S> EmbeddedApplication<'_, A, V, R, C, P, K, S>
where
    A: Authentication,
    V: EvidenceAuthority + Send + Sync + 'static,
    R: ResourceObserver + Send + Sync + 'static,
    C: TrustedClock + Send + Sync + 'static,
    P: PlanConfiguration + Send + Sync + 'static,
    K: cp::Custody,
    S: TrustedWorkerBuild + 'static,
{
    fn caller(&self, session: &A::Session) -> Result<AuthenticatedCaller> {
        let c = crate::authority::caller(&self.host.authentication, session)?;
        self.host
            .bundle
            .wi()
            .control_plane_snapshot()?
            .caller_scope(&c, self.host.clock.now()?.epoch_millis()?)
            .map_err(control_error)?;
        Ok(c)
    }
    fn plans(
        &self,
    ) -> PlanService<'_, A, ManagedEvidence<V, C>, SharedClock<C>, Configuration<P, C>> {
        PlanService {
            root: self.host.bundle.wi(),
            authentication: &self.host.authentication,
            evidence: &self.host.evidence,
            clock: &self.host.clock,
            configuration: &self.host.configuration,
        }
    }
    pub fn submit(
        &mut self,
        session: &A::Session,
        submission: EffectSubmission,
        selection: DirectOperationSelection,
    ) -> Result<OpaqueRef> {
        self.caller(session)?;
        let h = self.host;
        let id = PlanService {
            root: h.bundle.wi(),
            authentication: &h.authentication,
            evidence: &h.evidence,
            clock: &h.clock,
            configuration: h.configuration.as_ref(),
        }
        .submit_direct(session, submission, selection, &mut self.queue)?;
        self.project()?;
        Ok(id)
    }
    pub fn submit_flow(
        &mut self,
        session: &A::Session,
        definition: &OpaqueRef,
        submission: EffectSubmission,
    ) -> Result<OpaqueRef> {
        self.caller(session)?;
        let h = self.host;
        let id = PlanService {
            root: h.bundle.wi(),
            authentication: &h.authentication,
            evidence: &h.evidence,
            clock: &h.clock,
            configuration: h.configuration.as_ref(),
        }
        .submit_flow(session, definition, submission, &mut self.queue)?;
        self.project()?;
        Ok(id)
    }
    /// Host maintenance: poll WI observations, commit events, drain outbox and tick AQ.
    pub async fn advance(&mut self, session: &A::Session) -> Result<()> {
        self.caller(session)?;
        let root = self.host.bundle.wi();
        let mut after = None;
        loop {
            let page = root.unfinished_workers(
                after.as_ref().map(|(t, p, r): &(String, String, String)| {
                    (t.as_str(), p.as_str(), r.as_str())
                }),
                64,
            )?;
            for s in &page {
                self.host.runtime.maintain(&s.binding)?;
            }
            if page.len() < 64 {
                break;
            }
            let b = &page.last().unwrap().binding;
            after =
                Some((b.tenant.as_str().into(), b.provider.as_str().into(), b.run.as_str().into()));
        }
        self.withdraw_stale_permits()?;
        let _ = self.queue.tick().await?;
        self.project()
    }
    /// Administration, metadata inspection and credential issue share WIP-13's service.
    pub fn control(&self, session: &A::Session, request: &cp::Request) -> Result<cp::Outcome> {
        self.caller(session)?;
        self.withdraw_stale_permits()?;
        control(
            self.host.bundle.wi(),
            &self.host.authentication,
            &self.host.clock,
            &self.host.custody,
            session,
            request,
        )
    }
    fn get(&self, session: &A::Session, reference: ObjectRef) -> Result<ObjectView> {
        match self.control(
            session,
            &cp::Request {
                id: "embedded-read".to_string().try_into()?,
                operation: cp::Operation::Get { reference },
            },
        )? {
            cp::Outcome::Object(o) => Ok(o),
            _ => Err("object unavailable".into()),
        }
    }
    pub fn inspect_flow(
        &self,
        session: &A::Session,
        reference: &ObjectRef,
    ) -> Result<worldinterface_storage::flow::FlowRun> {
        if reference.kind != ObjectKind::Flow {
            return Err("object unavailable".into());
        }
        self.get(session, reference.clone())?;
        self.plans().inspect_run(session, &reference.id)
    }
    pub fn inspect_worker(
        &self,
        session: &A::Session,
        reference: &ObjectRef,
        provider: &OpaqueRef,
        run: &OpaqueRef,
    ) -> Result<WorkerState> {
        if reference.kind != ObjectKind::Worker || reference.id != worker_id(provider, run)? {
            return Err("object unavailable".into());
        }
        self.get(session, reference.clone())?;
        self.host
            .bundle
            .wi()
            .worker_snapshot(&reference.scope.tenant, provider, run)?
            .ok_or_else(|| "object unavailable".into())
    }
    pub fn inspect_worker_evidence(
        &self,
        session: &A::Session,
        reference: &ObjectRef,
        provider: &OpaqueRef,
        run: &OpaqueRef,
    ) -> Result<WorkerEvidence> {
        let state = self.inspect_worker(session, reference, provider, run)?;
        self.host.runtime.inspection_evidence(&state.binding)
    }
    pub fn receipt(
        &self,
        session: &A::Session,
        reference: &ObjectRef,
    ) -> Result<worldinterface_domain::receipt::ReceiptView> {
        if reference.kind != ObjectKind::Receipt {
            return Err("receipt unavailable".into());
        }
        self.get(session, reference.clone())?;
        let caller = self.caller(session)?;
        let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
            caller.tenant().clone(),
            caller.principal().clone(),
            worldinterface_domain::receipt::DisclosureProfile::Summary,
        );
        self.host
            .bundle
            .wi()
            .inspect_receipt(&access, &reference.id.as_str().to_string().try_into()?)?
            .ok_or_else(|| "receipt unavailable".into())
    }
    pub fn read_artifact(&self, session: &A::Session, reference: &ObjectRef) -> Result<Vec<u8>> {
        if reference.kind != ObjectKind::Artifact {
            return Err("object unavailable".into());
        }
        let object = self.get(session, reference.clone())?;
        let artifact = object.artifact.ok_or("artifact unavailable")?;
        let caller = self.caller(session)?;
        crate::artifact::ArtifactResolver::new(
            self.host.bundle.wi(),
            self.host.configuration.as_ref(),
        )
        .read(&caller, &artifact)
    }
    pub fn import_artifact(&self, session: &A::Session, bytes: &[u8]) -> Result<ArtifactRef> {
        let caller = self.caller(session)?;
        let state = self.host.bundle.wi().control_plane_snapshot()?;
        let scope = state
            .caller_scope(&caller, self.host.clock.now()?.epoch_millis()?)
            .map_err(control_error)?;
        state
            .authorize_demand(
                &caller,
                &artifact_import_demand(&scope, &ContentHash::of(bytes))?,
                self.host.clock.now()?.epoch_millis()?,
            )
            .map_err(control_error)?;
        let artifact = self.host.bundle.wi().publish(scope.tenant.clone(), bytes)?;
        self.object(
            artifact_reference(scope, &artifact),
            hash(&artifact)?,
            Some(artifact.clone()),
        )?;
        Ok(artifact)
    }
    pub fn reconcile_effect(&mut self, session: &A::Session, id: &EffectIntentId) -> Result<()> {
        let caller = self.caller(session)?;
        let state = self.host.bundle.wi().control_plane_snapshot()?;
        let scope = state
            .caller_scope(&caller, self.host.clock.now()?.epoch_millis()?)
            .map_err(control_error)?;
        let effects = self.host.bundle.wi().effect_snapshot(caller.tenant())?;
        let effect = effects.effects.get(id).ok_or("effect unavailable")?;
        if effect.agency.binding.principal != *caller.principal()
            || effect.agency.binding.capacity.organization_ref != scope.organization
        {
            return Err("effect unavailable".into());
        }
        let reference = ObjectRef {
            scope,
            kind: ObjectKind::Evidence,
            id: id.as_str().to_string().try_into()?,
        };
        state
            .authorize_demand(
                &caller,
                &reconciliation_demand(&reference)?,
                self.host.clock.now()?.epoch_millis()?,
            )
            .map_err(control_error)?;
        crate::reconciliation::deliver(&mut self.queue, &caller, &self.host.clock, id)?;
        Ok(())
    }
    pub fn reconcile_credential(
        &self,
        session: &A::Session,
        id: &ContentHash,
    ) -> Result<CredentialMetadata> {
        let caller = self.caller(session)?;
        let state = self.host.bundle.wi().control_plane_snapshot()?;
        let m = state.issuances.get(id).ok_or("credential unavailable")?;
        let scope = state
            .caller_scope(&caller, self.host.clock.now()?.epoch_millis()?)
            .map_err(control_error)?;
        if scope != m.binding.scope {
            return Err("credential unavailable".into());
        }
        state
            .authorize_demand(
                &caller,
                &reconciliation_demand(&m.permit)?,
                self.host.clock.now()?.epoch_millis()?,
            )
            .map_err(control_error)?;
        self.host
            .bundle
            .wi()
            .reconcile_control_delivery(id, &self.host.custody)
            .map_err(control_error)
    }
    /// The host selects recipient, authoritative run and deadline; consumers only
    /// name an existing effect and secret reference. No credential bytes cross here.
    pub fn prepare_credential(
        &self,
        session: &A::Session,
        id: &EffectIntentId,
        secret: ObjectRef,
    ) -> Result<CredentialScope> {
        let caller = self.caller(session)?;
        let state = self.host.bundle.wi().effect_snapshot(caller.tenant())?;
        let effect = state.effects.get(id).ok_or("effect unavailable")?;
        if effect.agency.binding.principal != *caller.principal() {
            return Err("effect unavailable".into());
        }
        let facts = effect.authorization_inputs.as_ref().ok_or("effect authority absent")?;
        let worker = facts.worker.as_ref().ok_or("worker absent")?;
        let run = self
            .host
            .bundle
            .wi()
            .worker_snapshot(caller.tenant(), &worker.provider, &worker.run)?
            .ok_or("worker absent")?;
        if run.quarantined || run.cleanup_confirmed || run.stop_requested || !run.running {
            return Err("run withdrawn".into());
        }
        let at = self.host.clock.now()?;
        let verified = crate::authority::verify_snapshot(
            self.host.bundle.wi(),
            &self.host.evidence,
            caller.tenant(),
            &effect.intent.submission,
            &at,
        )?;
        let scope = self
            .host
            .bundle
            .wi()
            .control_plane_snapshot()?
            .caller_scope(&caller, at.epoch_millis()?)
            .map_err(control_error)?;
        let reference = ObjectRef {
            scope,
            kind: ObjectKind::Authorization,
            id: format!("credential:{}", id.as_str()).try_into()?,
        };
        let origin = run
            .operations
            .values()
            .filter(|o| ["start", "provision"].contains(&o.operation.as_str()))
            .filter_map(|o| state.effects.get(&o.intent))
            .min_by_key(|e| e.intent.submitted_at.clone())
            .ok_or("authoritative run origin absent")?;
        let wall = origin
            .authorization_inputs
            .as_ref()
            .and_then(|f| f.worker.as_ref())
            .and_then(|w| w.installation.as_ref())
            .ok_or("authoritative run bounds absent")?
            .wall_ms;
        let deadline = origin
            .intent
            .submitted_at
            .epoch_millis()?
            .checked_add(wall)
            .ok_or("run bounds overflow")?;
        let permit = cp::prepare_permit(
            &self.host.bundle.wi().control_plane_snapshot()?,
            &caller,
            &effect.intent,
            &verified,
            cp::PermitInput {
                reference,
                secret,
                recipient: self.host.credential_recipient.clone(),
                run: worker.run.clone(),
                run_expires_at_ms: deadline,
                class: CredentialClass::Test,
            },
            at.epoch_millis()?,
        )
        .map_err(control_error)?;
        let binding = permit.binding().clone();
        self.host.bundle.wi().register_control_permit(permit)?;
        Ok(binding)
    }
    pub fn worker_credential(
        &self,
        session: &A::Session,
        id: &ContentHash,
    ) -> Result<WorkerCredentialMetadata> {
        let caller = self.caller(session)?;
        self.withdraw_stale_permits()?;
        let state = self.host.bundle.wi().control_plane_snapshot()?;
        let metadata = state.issuances.get(id).ok_or("credential unavailable")?;
        if metadata.binding.principal != *caller.principal() {
            return Err("credential unavailable".into());
        }
        self.get(session, metadata.permit.clone())?;
        let metadata = cp::active_credential(&state, id, self.host.clock.now()?.epoch_millis()?)
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "credential unavailable".into()
            })?;
        WorkerCredentialMetadata::delivered(metadata)
    }
    fn withdraw_stale_permits(&self) -> Result<()> {
        let state = self.host.bundle.wi().control_plane_snapshot()?;
        let at = self.host.clock.now()?;
        for permit in &state.permits {
            let b = permit.binding();
            let current =
                self.host.bundle.wi().worker_snapshot(&b.scope.tenant, &b.provider, &b.run)?;
            let invalid = current.is_none_or(|s| {
                s.quarantined || s.cleanup_confirmed || s.stop_requested || !s.running
            }) || self.host.clock.now()?.epoch_millis()? >= b.expires_at_ms;
            // Refresh external and managed authority for each immutable owner.
            let effects = self.host.bundle.wi().effect_snapshot(&b.scope.tenant)?;
            let valid = effects
                .effects
                .values()
                .filter(|e| {
                    e.authorization.as_ref().is_some_and(|a| a.exact_binding == b.authorization)
                })
                .any(|e| {
                    (|| -> Result<bool> {
                        let verified = crate::authority::verify_snapshot(
                            self.host.bundle.wi(),
                            &self.host.evidence,
                            &b.scope.tenant,
                            &e.intent.submission,
                            &at,
                        )?;
                        let caller = AuthenticatedCaller::from_trusted_authentication(
                            b.scope.tenant.clone(),
                            b.principal.clone(),
                        );
                        let refreshed = cp::prepare_permit(
                            &state,
                            &caller,
                            &e.intent,
                            &verified,
                            cp::PermitInput {
                                reference: permit.reference().clone(),
                                secret: b.secret.clone(),
                                recipient: b.recipient.clone(),
                                run: b.run.clone(),
                                run_expires_at_ms: b.expires_at_ms,
                                class: b.class,
                            },
                            at.epoch_millis()?,
                        )
                        .map_err(control_error)?;
                        Ok(refreshed.binding() == b)
                    })()
                    .unwrap_or(false)
                });
            if invalid || !valid {
                self.host.bundle.wi().withdraw_control_permit(permit.reference().clone())?;
            }
        }
        Ok(())
    }
    fn object(
        &self,
        reference: ObjectRef,
        content: ContentHash,
        artifact: Option<ArtifactRef>,
    ) -> Result<()> {
        let state = self.host.bundle.wi().control_plane_snapshot()?;
        let old = state.objects.iter().find(|o| o.reference == reference);
        if old.is_some_and(|o| o.content == content && o.artifact == artifact) {
            return Ok(());
        }
        self.host.bundle.wi().register_control_object(ObjectView {
            reference,
            revision: old.map_or(1, |o| o.revision + 1),
            content,
            artifact,
        })
    }
    fn project_signals(
        &self,
        scope: &Scope,
        correlation: &OpaqueRef,
        audience: &PrincipalId,
    ) -> Result<()> {
        for kind in
            [worldinterface_storage::StoreKind::Fabric, worldinterface_storage::StoreKind::Flow]
        {
            let mut after = None;
            loop {
                let page = self.host.bundle.wi().signal_obligations(kind, after.as_ref(), 128)?;
                for signal in &page {
                    let route = &signal.event.route;
                    if route.tenant == scope.tenant
                        && &route.audience == audience
                        && &route.correlation == correlation
                    {
                        self.object(
                            ObjectRef {
                                scope: scope.clone(),
                                kind: ObjectKind::Signal,
                                id: signal.event.id.clone(),
                            },
                            hash(signal)?,
                            None,
                        )?;
                    }
                }
                if page.len() < 128 {
                    break;
                }
                after = Some(page.last().unwrap().event.id.clone());
            }
        }
        Ok(())
    }
    fn project(&self) -> Result<()> {
        let root = self.host.bundle.wi();
        for tenant in root.flow_tenants()? {
            for run in root.flow_snapshot(&tenant)?.runs.values() {
                let scope = Scope {
                    tenant: tenant.clone(),
                    organization: run.agency.binding.capacity.organization_ref.clone(),
                };
                self.object(
                    ObjectRef { scope: scope.clone(), kind: ObjectKind::Flow, id: run.id.clone() },
                    hash(run)?,
                    None,
                )?;
                for node in run.results.keys() {
                    self.project_signals(
                        &scope,
                        &worldinterface_domain::continuation::flow_child_ref(&run.id, node)?,
                        &run.principal,
                    )?;
                }
                for a in std::iter::once(&run.submission.input.artifact).chain(run.exports.values())
                {
                    self.object(artifact_reference(scope.clone(), a), hash(a)?, Some(a.clone()))?;
                }
            }
        }
        for tenant in root.effect_tenants()? {
            for effect in root.effect_snapshot(&tenant)?.effects.values() {
                let scope = Scope {
                    tenant: tenant.clone(),
                    organization: effect.agency.binding.capacity.organization_ref.clone(),
                };
                for a in worldinterface_domain::effect::required_artifacts(effect) {
                    self.object(artifact_reference(scope.clone(), a), hash(a)?, Some(a.clone()))?;
                }
                for (id, digest, _) in root.effect_receipts(&tenant, &effect.intent.id)? {
                    self.object(
                        ObjectRef {
                            scope: scope.clone(),
                            kind: ObjectKind::Receipt,
                            id: id.as_str().to_string().try_into()?,
                        },
                        digest,
                        None,
                    )?;
                }
                if let Some(w) =
                    effect.authorization_inputs.as_ref().and_then(|s| s.worker.as_ref())
                {
                    if let Some(worker) = root.worker_snapshot(&tenant, &w.provider, &w.run)? {
                        self.project_signals(
                            &scope,
                            &worker.binding.event_correlation()?,
                            &worker.binding.principal,
                        )?;
                        self.object(
                            ObjectRef {
                                scope: scope.clone(),
                                kind: ObjectKind::Worker,
                                id: worker_id(&w.provider, &w.run)?,
                            },
                            hash(&worker)?,
                            None,
                        )?;
                        if let WorkerCollection::Published(p) = &worker.collection {
                            for a in p.exports.values().chain(std::iter::once(&p.manifest)) {
                                self.object(
                                    artifact_reference(scope.clone(), a),
                                    hash(a)?,
                                    Some(a.clone()),
                                )?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub fn shutdown(self) -> Result<()> {
        self.queue.shutdown()
    }
}
pub fn worker_id(provider: &OpaqueRef, run: &OpaqueRef) -> Result<OpaqueRef> {
    Ok(format!("worker:{}", hash(&(provider, run))?.as_str()).try_into()?)
}

/// Consumer surface usable without importing any store, provider or AQ type.
pub trait Application {
    type Session;
    fn submit(
        &mut self,
        session: &Self::Session,
        submission: EffectSubmission,
        selection: DirectOperationSelection,
    ) -> Result<OpaqueRef>;
    fn advance(&mut self, session: &Self::Session)
        -> impl std::future::Future<Output = Result<()>>;
    fn control(&self, session: &Self::Session, request: &cp::Request) -> Result<cp::Outcome>;
    fn inspect_flow(
        &self,
        session: &Self::Session,
        reference: &ObjectRef,
    ) -> Result<worldinterface_storage::flow::FlowRun>;
    fn inspect_worker(
        &self,
        session: &Self::Session,
        reference: &ObjectRef,
        provider: &OpaqueRef,
        run: &OpaqueRef,
    ) -> Result<WorkerState>;
    fn inspect_worker_evidence(
        &self,
        session: &Self::Session,
        reference: &ObjectRef,
        provider: &OpaqueRef,
        run: &OpaqueRef,
    ) -> Result<WorkerEvidence>;
    fn receipt(
        &self,
        session: &Self::Session,
        reference: &ObjectRef,
    ) -> Result<worldinterface_domain::receipt::ReceiptView>;
    fn read_artifact(&self, session: &Self::Session, reference: &ObjectRef) -> Result<Vec<u8>>;
}
impl<A, V, R, C, P, K, S> Application for EmbeddedApplication<'_, A, V, R, C, P, K, S>
where
    A: Authentication,
    V: EvidenceAuthority + Send + Sync + 'static,
    R: ResourceObserver + Send + Sync + 'static,
    C: TrustedClock + Send + Sync + 'static,
    P: PlanConfiguration + Send + Sync + 'static,
    K: cp::Custody,
    S: TrustedWorkerBuild + 'static,
{
    type Session = A::Session;
    fn submit(
        &mut self,
        s: &A::Session,
        r: EffectSubmission,
        o: DirectOperationSelection,
    ) -> Result<OpaqueRef> {
        Self::submit(self, s, r, o)
    }
    async fn advance(&mut self, s: &A::Session) -> Result<()> {
        Self::advance(self, s).await
    }
    fn control(&self, s: &A::Session, r: &cp::Request) -> Result<cp::Outcome> {
        Self::control(self, s, r)
    }
    fn inspect_flow(
        &self,
        s: &A::Session,
        r: &ObjectRef,
    ) -> Result<worldinterface_storage::flow::FlowRun> {
        Self::inspect_flow(self, s, r)
    }
    fn inspect_worker(
        &self,
        s: &A::Session,
        r: &ObjectRef,
        p: &OpaqueRef,
        w: &OpaqueRef,
    ) -> Result<WorkerState> {
        Self::inspect_worker(self, s, r, p, w)
    }
    fn inspect_worker_evidence(
        &self,
        s: &A::Session,
        r: &ObjectRef,
        p: &OpaqueRef,
        w: &OpaqueRef,
    ) -> Result<WorkerEvidence> {
        Self::inspect_worker_evidence(self, s, r, p, w)
    }
    fn receipt(
        &self,
        s: &A::Session,
        r: &ObjectRef,
    ) -> Result<worldinterface_domain::receipt::ReceiptView> {
        Self::receipt(self, s, r)
    }
    fn read_artifact(&self, s: &A::Session, r: &ObjectRef) -> Result<Vec<u8>> {
        Self::read_artifact(self, s, r)
    }
}
