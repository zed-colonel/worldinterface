//! WI connector: only executor-created invocation envelopes cross the provider boundary.
use super::{
    admission::{derive, AdmittedWorker},
    evaluation::{EvaluationVerifier, TrustedApprovals},
};
use crate::{
    completion::{EvidenceBytes, PortObservation},
    connector::*,
    storage::StoreBundle,
    worker_process::*,
    Result,
};
use actionqueue_executor_local::handler::CancellationToken;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};
use worldinterface_domain::{effect::CommitObservation, worker::*};
use worldinterface_protocol::*;
use worldinterface_worker::mapping::{Operation, WorkerRequestV1};

/// A deterministic acknowledgement can be declared before execution. Lifecycle
/// state is observed at the boundary and belongs in evidence, not the manifest.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerAcknowledgement {
    version: u32,
    request: ContentHash,
}
pub fn acknowledgement(request: &WorkerRequestV1) -> Result<Vec<u8>> {
    Ok(canonical_data(&WorkerAcknowledgement { version: 1, request: request.digest()? })?)
}

fn succeeded(request: &WorkerRequestV1, state: &WorkerState) -> Result<PortObservation> {
    let bytes = canonical_data(state)?;
    Ok(PortObservation {
        commit: CommitObservation::ConfirmedCommitted,
        outcome: ExecutionOutcome::Succeeded,
        outputs: vec![acknowledgement(request)?],
        evidence: vec![EvidenceBytes {
            schema: "wi-worker-lifecycle:2".to_string().try_into()?,
            bytes: bytes.clone(),
        }],
        boundary_record: Some(EvidenceBytes {
            schema: "wi-worker-lifecycle:2".to_string().try_into()?,
            bytes,
        }),
    })
}

pub fn descriptor(operation: Operation) -> Result<ConnectorDescriptor> {
    if !super::admission::OPERATIONS.contains(&operation) {
        return Err("unsupported process operation".into());
    }
    let d = ConnectorDescriptor {
        version: 1,
        connector: "wi:process-worker".to_string().try_into()?,
        implementation: connector_digest(),
        operation: operation.name().to_string().try_into()?,
        effect_class: if operation == Operation::Inspect {
            EffectClass::Read
        } else {
            EffectClass::Control
        },
        resource_namespace: "worker".to_string().try_into()?,
        resource_grammar: "exact-worker:1".to_string().try_into()?,
        input_schema: "wi-worker-effect:1".to_string().try_into()?,
        output_schema: "wi-worker-result:1".to_string().try_into()?,
        isolation_profile: "process-managed-proof-1".to_string().try_into()?,
        secret_refs: vec![],
        idempotency: if operation == Operation::Inspect {
            IdempotencyClass::ReadOnly
        } else {
            IdempotencyClass::ExternallyKeyed
        },
        provider_key: if operation == Operation::Inspect {
            None
        } else {
            Some(ProviderKeyMapping {
                version: 1,
                provider_scope: "wi:process-worker".to_string().try_into()?,
                parameter: "operation_key".to_string().try_into()?,
            })
        },
        retry_after_noncommit: false,
        observes_commit: true,
        reconciliation: ReconciliationPolicy {
            max_observations: 8,
            backoff_millis: 10,
            timeout_millis: 60_000,
            automatic: true,
        },
        cancellation_supported: true,
        evidence_schema: "wi-worker-lifecycle:2".to_string().try_into()?,
        max_evidence_bytes: 64 * 1024,
    };
    worldinterface_worker::mapping::validate_spi_descriptor(operation, &d)?;
    Ok(d)
}
/// Host build provenance is verified separately from repository assertions.
/// Acceptance of a packet authorizes its source/recipe, never arbitrary executable bytes.
pub trait TrustedWorkerBuild: TrustedApprovals {
    fn verify_build(
        &self,
        packet: &ContentHash,
        fixture: &ContentHash,
        helper: &ContentHash,
    ) -> Result<()>;
}
type LiveRuns = BTreeMap<
    (OpaqueRef, OpaqueRef),
    Vec<(WorkerRequestV1, worldinterface_worker::mapping::ResolvedWorkerV1)>,
>;
pub struct WorkerRuntime<S> {
    pub(crate) bundle: Arc<StoreBundle>,
    pub(crate) port: Arc<dyn super::provider::AdmittedProvider>,
    adapter: ContentHash,
    pub(crate) evaluation: Option<Arc<EvaluationVerifier<S>>>,
    recovery_policy: super::catalog::RecoveryPolicy,
    pub(crate) clock: Arc<dyn Clock>,
    executable: ContentHash,
    helper_digest: ContentHash,
    slots: u32,
    seam_simulation: bool,
    scope: HostScope,
    live: Mutex<LiveRuns>,
}
impl<S: TrustedWorkerBuild> WorkerRuntime<S> {
    pub fn install(
        bundle: Arc<StoreBundle>,
        scope: HostScope,
        root: &Path,
        fixture: &Path,
        helper: &Path,
        evaluation: Arc<EvaluationVerifier<S>>,
        clock: Arc<dyn Clock>,
        slots: u32,
    ) -> Result<Self> {
        let verified = evaluation.verify(clock.unix_ms())?;
        let executable = executable_digest(fixture)?;
        let helper_digest = executable_digest(helper)?;
        evaluation.trusted.verify_build(verified.digest(), &executable, &helper_digest)?;
        let mut registry = ExecutableRegistry::default();
        registry.register("evaluation-fixture", fixture, &executable)?;
        let port = ProcessWorkerPort::new(
            scope.clone(),
            root,
            helper,
            &helper_digest,
            registry,
            clock.clone(),
        )?;
        super::recovery::recover_quarantines(bundle.wi(), &scope)?;
        Ok(Self {
            bundle,
            port: Arc::new(port),
            adapter: adapter_digest(),
            recovery_policy: default_recovery_policy(scope.clone())?,
            evaluation: Some(evaluation),
            clock,
            executable,
            helper_digest,
            slots,
            seam_simulation: false,
            scope,
            live: Mutex::new(BTreeMap::new()),
        })
    }
    /// Test/embedded composition for a reviewed asynchronous provider implementation.
    /// Provenance must be independently checked by TrustedWorkerBuild, including seam sources.
    pub fn install_provider(
        bundle: Arc<StoreBundle>,
        scope: HostScope,
        port: Arc<dyn super::provider::AdmittedProvider>,
        provenance: super::provider::ProviderProvenance,
        evaluation: Arc<EvaluationVerifier<S>>,
        clock: Arc<dyn Clock>,
        slots: u32,
    ) -> Result<Self> {
        let verified = evaluation.verify(clock.unix_ms())?;
        if !provenance.seam_simulation {
            return Err("external provider requires separate qualification".into());
        }
        evaluation.trusted.verify_build(
            verified.digest(),
            &provenance.implementation,
            &provenance.helper,
        )?;
        super::recovery::recover_quarantines(bundle.wi(), &scope)?;
        Ok(Self {
            bundle,
            scope: scope.clone(),
            port,
            executable: provenance.implementation,
            helper_digest: provenance.helper,
            adapter: provenance.adapter,
            seam_simulation: true,
            recovery_policy: default_recovery_policy(scope.clone())?,
            evaluation: Some(evaluation),
            clock,
            slots,
            live: Mutex::new(BTreeMap::new()),
        })
    }
    pub fn with_recovery_policy(mut self, policy: super::catalog::RecoveryPolicy) -> Self {
        self.recovery_policy = policy;
        self
    }
    fn recovery_authorization(
        &self,
        binding: &WorkerBinding,
        operation: &str,
    ) -> Result<super::catalog::RecoveryAuthorization> {
        let caller =
            worldinterface_domain::agency::AuthenticatedCaller::from_trusted_authentication(
                self.scope.tenant.clone().try_into()?,
                self.scope.principal.clone().try_into()?,
            );
        super::catalog::authorize_recovery(
            self.bundle.wi(),
            &caller,
            binding,
            operation,
            &descriptor(Operation::Start)?.implementation,
            &self.recovery_policy,
        )
    }
    fn recovery_invocation(
        &self,
        bound: &crate::completion::BoundExecution,
    ) -> Result<Option<(WorkerBinding, WorkerRequestV1)>> {
        let snapshot = self.bundle.wi().effect_snapshot(&bound.tenant)?;
        let effect = snapshot.effects.get(&bound.intent).ok_or("worker effect absent")?;
        let facts = effect.authorization_inputs.as_ref().ok_or("worker authority absent")?;
        let Some(recovery) = &facts.recovery else {
            return Ok(None);
        };
        let authorization = effect.authorization.as_ref().ok_or("worker authorization absent")?;
        if authorization.decision != Decision::Allow
            || worldinterface_protocol::data_digest(authorization)? != bound.authorization
        {
            return Err("worker recovery authorization drift".into());
        }
        let demand = super::admission::WorkerEffect::decode_structure(&bound.input)?;
        let caller =
            worldinterface_domain::agency::AuthenticatedCaller::from_trusted_authentication(
                bound.tenant.clone(),
                effect.agency.binding.principal.clone(),
            );
        let verified = super::catalog::authorize_recovery(
            self.bundle.wi(),
            &caller,
            &recovery.binding,
            demand.worker.operation.name(),
            &descriptor(Operation::Start)?.implementation,
            &self.recovery_policy,
        )?;
        if verified.policy() != &recovery.policy
            || verified.operation() != &recovery.operation
            || recovery.expires_at.epoch_millis()? <= self.clock.unix_ms()
        {
            return Err("recovery policy drift or expiry".into());
        }
        let launch =
            self.bundle.wi().worker_launch(&recovery.binding)?.ok_or("worker launch absent")?;
        let operations: Vec<(WorkerRequestV1, worldinterface_worker::mapping::ResolvedWorkerV1)> =
            serde_json::from_slice(&launch.operations)?;
        let original = operations
            .iter()
            .find(|(q, _)| q == &demand.worker)
            .ok_or("recovery request binding mismatch")?;
        let inputs: Vec<_> = demand
            .inputs
            .iter()
            .map(|r| (format!("input:{}", r.content.as_str()), &r.content, r.size))
            .collect();
        if ContentHash::of(&worldinterface_worker::encode(&inputs)?) != demand.worker.input_digest {
            return Err("recovery input binding mismatch".into());
        }
        if original.1.configuration_digest
            != AuthorizedRun::configuration_digest(&demand.task, &demand.exports)?
            || original.1.controls_digest != AuthorizedRun::controls_digest(&demand.controls)?
            || self
                .state(&recovery.binding)?
                .artifact_policy
                .first()
                .is_some_and(|p| p.inputs != demand.inputs)
        {
            return Err("recovery configuration or scope mismatch".into());
        }
        Ok(Some((recovery.binding.clone(), demand.worker)))
    }
    pub fn connector(self: &Arc<Self>, operation: Operation) -> Result<WorkerConnector<S>> {
        Ok(WorkerConnector { runtime: self.clone(), descriptor: descriptor(operation)? })
    }
    fn state(&self, b: &WorkerBinding) -> Result<WorkerState> {
        super::artifacts::bound_state(self.bundle.wi(), b)
    }
    fn transition(&self, b: &WorkerBinding, fact: WorkerFact) -> Result<()> {
        self.bundle.wi().record_worker_transition(&self.state(b)?, fact)
    }
    fn check_registration(
        &self,
        record: &worldinterface_capability::process_catalog::ProcessRegistrationRecord,
    ) -> Result<()> {
        let worldinterface_capability::process_catalog::ProcessRegistrationRecord::Evaluation {
            binding,
            ..
        } = record
        else {
            return Err("ordinary use disabled".into());
        };
        super::catalog::verify_registration(
            record,
            binding,
            self.evaluation.as_ref().ok_or("admission approval absent")?,
            self.clock.unix_ms(),
        )?;
        Ok(())
    }
    fn invoke(
        &self,
        admitted: AdmittedWorker,
        cancellation: &CancellationToken,
    ) -> Result<PortObservation> {
        let preflight = || -> Result<()> {
            self.check_registration(&admitted.registration)?;
            self.bundle.wi().check_control_execution(
                &admitted.binding.tenant,
                &admitted.binding.principal,
                &admitted.intent,
                admitted.dispatch,
                &admitted.authority,
                self.clock.unix_ms(),
            )?;
            Ok(())
        };
        let root = self.bundle.wi();
        let binding = &admitted.binding;
        let op = admitted.operation;
        let request = admitted
            .authorization
            .operations
            .iter()
            .find(|(r, _)| r.operation == op)
            .ok_or("worker operation absent")?
            .0
            .clone();
        if binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
        {
            return Err("worker host scope mismatch".into());
        }
        let key = (binding.provider.clone(), binding.run.clone());
        if op == Operation::Inspect {
            self.state(binding)?;
            let present =
                self.live.lock().map_err(|_| "worker registry unavailable")?.contains_key(&key);
            if present {
                self.poll(binding)?;
            }
            return succeeded(&request, &self.state(binding)?);
        }
        let provider_operation =
            admitted.provider_operation.clone().ok_or("worker provider operation absent")?;
        let mut live = self.live.lock().map_err(|_| "worker registry unavailable")?;
        let fresh = if matches!(op, Operation::Provision | Operation::Start) {
            if root.worker_snapshot(&binding.tenant, &binding.provider, &binding.run)?.is_none() {
                // The live registry lock serializes this check with reservation/admission.
                self.port.check_capacity()?;
            }
            root.reserve_catalog_start(
                WorkerLaunchRecord {
                    version: 1,
                    binding: binding.clone(),
                    catalog: admitted.catalog.clone(),
                    implementation: descriptor(Operation::Start)?.implementation,
                    connectors: admitted.connectors.clone(),
                    operations: canonical_data(&admitted.authorization.operations)?,
                },
                self.slots,
            )?
        } else {
            self.state(binding)?;
            false
        };
        root.bind_worker_artifact_policy(binding.clone(), admitted.artifact_policy.clone())?;
        let state = self.state(binding)?;
        if let Some(old) = state.operations.get(&provider_operation.key) {
            let mut expected = old.clone();
            expected.acknowledged = false;
            if expected != provider_operation {
                return Err("worker provider key conflict".into());
            }
            if old.acknowledged {
                if op == Operation::CollectArtifacts
                    && !matches!(state.collection, WorkerCollection::Published(_))
                {
                    return Ok(observation(
                        CommitObservation::Unknown,
                        ExecutionOutcome::Uncertain,
                    ));
                }
                return succeeded(&request, &state);
            }
            if op == Operation::CollectArtifacts
                && matches!(
                    state.collection,
                    WorkerCollection::Prepared(_) | WorkerCollection::Published(_)
                )
            {
                drop(live);
                super::artifacts::resume(root, binding)?;
                self.finish_collection(binding)?;
                root.acknowledge_worker_operation(binding.clone(), provider_operation)?;
                return succeeded(&request, &self.state(binding)?);
            }
            super::recovery::quarantine(root, binding)?;
            return Ok(observation(CommitObservation::Unknown, ExecutionOutcome::Uncertain));
        }
        root.begin_worker_operation(binding.clone(), provider_operation.clone())?;
        if !fresh && !live.contains_key(&key) {
            super::recovery::quarantine(root, binding)?;
            if op == Operation::Terminate {
                root.acknowledge_worker_operation(binding.clone(), provider_operation)?;
                return succeeded(&request, &self.state(binding)?);
            }
            return Ok(observation(CommitObservation::Unknown, ExecutionOutcome::Uncertain));
        }
        if fresh {
            let operations = admitted.authorization.operations.clone();
            root.fence_worker_dispatch(binding, &admitted.catalog, || {
                preflight()?;
                self.port.admit(admitted.authorization)
            })?;
            live.insert(key.clone(), operations);
        }
        if cancellation.is_cancelled() || op == Operation::Terminate {
            let current = self.state(binding)?;
            if !current.stop_requested && !current.cleanup_confirmed {
                root.request_worker_stop(&current)?;
            }
        }
        let stopping = self.state(binding)?.stop_requested;
        if stopping {
            self.port.stop(binding.provider.as_str(), binding.run.as_str())?;
        }
        if stopping && matches!(op, Operation::Start | Operation::Provision) {
            return Ok(observation(CommitObservation::Unknown, ExecutionOutcome::Uncertain));
        }
        if cancellation.is_cancelled() && op != Operation::Terminate {
            return Ok(observation(CommitObservation::Unknown, ExecutionOutcome::Uncertain));
        }
        if op == Operation::Start && fresh {
            let provision = live[&key]
                .iter()
                .find(|(r, _)| r.operation == Operation::Provision)
                .unwrap()
                .0
                .clone();
            root.fence_worker_dispatch(binding, &admitted.catalog, || {
                preflight()?;
                self.port.submit(&provision)
            })?;
        }
        let submit = || {
            preflight()?;
            self.port.submit(&request)
        };
        let result = if op == Operation::Terminate {
            root.fence_worker_termination(binding, &admitted.catalog, submit)
        } else {
            root.fence_worker_dispatch(binding, &admitted.catalog, submit)
        };
        if result
            .as_ref()
            .is_ok_and(|a| *a == worldinterface_worker::ports::Acknowledgment::Uncertain)
        {
            return Ok(observation(CommitObservation::Unknown, ExecutionOutcome::Uncertain));
        }
        if result.is_err() {
            if op == Operation::CollectArtifacts {
                super::artifacts::quarantine(
                    root,
                    binding,
                    &provider_operation.key,
                    "provider-collection-rejected",
                    vec![],
                )?;
            }
            super::recovery::quarantine(root, binding)?;
            return Ok(observation(CommitObservation::Unknown, ExecutionOutcome::Uncertain));
        }
        if op == Operation::Start && !self.state(binding)?.accepted {
            self.transition(binding, WorkerFact::StartAccepted)?;
        }
        // Release of the allocation follows verified process-tree cleanup and artifact collection.
        if op == Operation::Release {
            let report = self
                .port
                .evidence(binding.provider.as_str(), binding.run.as_str())?
                .ok_or("cleanup report absent")?;
            if !report.cleanup_confirmed {
                return Err("worker cleanup unconfirmed".into());
            }
            let current = self.state(binding)?;
            if !current.exited && !current.quarantined {
                self.transition(
                    binding,
                    if report.reason == "not launched" {
                        WorkerFact::NoProcess
                    } else {
                        WorkerFact::Quarantined
                    },
                )?;
            }
            if !self.state(binding)?.cleanup_confirmed {
                root.confirm_worker_cleanup(&self.state(binding)?)?;
            }
        }
        drop(live);
        self.poll(binding)?;
        if op == Operation::CollectArtifacts {
            let manifest = self.port.manifest(binding.provider.as_str(), binding.run.as_str());
            let collection = (|| -> Result<_> {
                let manifest = manifest?;
                let policy = self.state(binding)?.artifact_policy[0].clone();
                let mut bytes = BTreeMap::new();
                for e in &policy.exports {
                    let locator = worldinterface_worker::artifacts::ArtifactLocatorV2 {
                        provider: binding.provider.as_str().into(),
                        run: binding.run.as_str().into(),
                        authorization: binding.realization.clone(),
                        reference: e.reference.as_str().into(),
                        digest: e.digest.clone(),
                        size: e.size,
                    };
                    bytes.insert(e.reference.as_str().into(), self.port.read(&locator)?);
                }
                Ok((manifest, bytes))
            })();
            let (manifest, bytes) = match collection {
                Ok(v) => v,
                Err(_) => {
                    super::artifacts::quarantine(
                        root,
                        binding,
                        &provider_operation.key,
                        "provider-collection-unavailable",
                        vec![],
                    )?;
                    return Ok(observation(
                        CommitObservation::Unknown,
                        ExecutionOutcome::Uncertain,
                    ));
                }
            };
            let report = self
                .port
                .evidence(binding.provider.as_str(), binding.run.as_str())?
                .ok_or("collection cleanup evidence absent")?;
            if !report.cleanup_confirmed || !report.exit_confirmed {
                return Err("collection cleanup unconfirmed".into());
            }
            if super::artifacts::prepare(
                root,
                binding,
                &provider_operation.key,
                &manifest,
                &bytes,
                report.exit_success == Some(true),
            )
            .is_err()
            {
                return Ok(observation(CommitObservation::Unknown, ExecutionOutcome::Uncertain));
            }
            super::artifacts::resume(root, binding)?;
            self.finish_collection(binding)?;
        }
        if op == Operation::Release {
            let evidence = self.inspection_evidence(binding)?;
            root.retain_worker_inspection(binding.clone(), serde_json::to_vec(&evidence)?)?;
        }
        root.acknowledge_worker_operation(binding.clone(), provider_operation)?;
        if op == Operation::Release {
            self.port.retire(binding.provider.as_str(), binding.run.as_str())?;
            self.live.lock().map_err(|_| "worker registry unavailable")?.remove(&key);
        }
        succeeded(&request, &self.state(binding)?)
    }
    /// Non-secret diagnostic at the provider's consequential spawn boundary.
    pub fn spawn_attempts(&self) -> Option<u64> {
        self.port.spawn_attempts()
    }
    pub fn read_artifact(&self, binding: &WorkerBinding, reference: &str) -> Result<Vec<u8>> {
        self.evaluation
            .as_ref()
            .ok_or("admission approval absent")?
            .verify(self.clock.unix_ms())?;
        if binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
        {
            return Err("worker artifact scope mismatch".into());
        }
        super::artifacts::read(self.bundle.wi(), binding, reference)
    }
    fn finish_collection(&self, binding: &WorkerBinding) -> Result<()> {
        let state = self.state(binding)?;
        if !state.completed
            && !state.stop_requested
            && !state.quarantined
            && matches!(&state.collection, WorkerCollection::Published(p) if p.prepared.exit_success)
        {
            self.transition(binding, WorkerFact::Completed)?;
        }
        Ok(())
    }

    pub fn recovery_evidence(&self, binding: &WorkerBinding) -> Result<Option<CleanupReport>> {
        self.recovery_authorization(binding, "inspect")?;

        let launch = self.bundle.wi().worker_launch(binding)?.ok_or("worker launch absent")?;
        let operations: Vec<(WorkerRequestV1, worldinterface_worker::mapping::ResolvedWorkerV1)> =
            serde_json::from_slice(&launch.operations)?;
        match self.port.recover_cleanup(
            binding.provider.as_str(),
            binding.run.as_str(),
            &operations,
        ) {
            Ok(report) => Ok(report),
            Err(error) => {
                super::recovery::quarantine(self.bundle.wi(), binding)?;
                Err(error)
            }
        }
    }
    pub(crate) fn inspection_evidence(
        &self,
        binding: &WorkerBinding,
    ) -> Result<crate::embedded::WorkerEvidence> {
        if binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
        {
            return Err("worker evidence scope mismatch".into());
        }
        let state = self.state(binding)?;
        let live = self
            .live
            .lock()
            .map_err(|_| "worker registry unavailable")?
            .contains_key(&(binding.provider.clone(), binding.run.clone()));
        if !live && !state.inspection_evidence.is_empty() {
            let evidence: crate::embedded::WorkerEvidence =
                serde_json::from_slice(&state.inspection_evidence)?;
            if !state.cleanup_confirmed || evidence.binding != *binding {
                return Err("retained worker evidence binding mismatch".into());
            }
            return Ok(evidence);
        }
        let cleanup = if live {
            self.port.evidence(binding.provider.as_str(), binding.run.as_str())?
        } else {
            None
        };
        Ok(crate::embedded::WorkerEvidence {
            binding: binding.clone(),
            seam_simulation: self.seam_simulation,
            controls: if live {
                self.port.controls(binding.provider.as_str(), binding.run.as_str())?
            } else {
                None
            },
            cleanup: cleanup.map(|r| crate::embedded::WorkerCleanupEvidence {
                exit_confirmed: r.exit_confirmed,
                exit_success: r.exit_success,
                cleanup_confirmed: r.cleanup_confirmed,
                retained_output_bytes: r.retained_output_bytes,
                term_signals: r.term_signals,
                kill_signals: r.kill_signals,
                descendants_observed: r.descendants_observed,
            }),
        })
    }
    pub fn cleanup_report(&self, binding: &WorkerBinding) -> Result<Option<CleanupReport>> {
        self.recovery_authorization(binding, "inspect")?;

        self.port.evidence(binding.provider.as_str(), binding.run.as_str())
    }
    /// Host maintenance explicitly polls durable observations; AQ never depends on this process's memory.
    pub(crate) fn maintain(&self, binding: &WorkerBinding) -> Result<()> {
        if binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
        {
            return Ok(());
        }
        if self.poll(binding).is_err() {
            // An unknown historical implementation or damaged private evidence
            // cannot prevent unrelated AQ work from progressing.
            super::recovery::quarantine(self.bundle.wi(), binding)?;
            let _ = self.port.stop(binding.provider.as_str(), binding.run.as_str());
        }
        Ok(())
    }
    pub fn poll(&self, binding: &WorkerBinding) -> Result<()> {
        self.recovery_authorization(binding, "inspect")?;
        if self.state(binding)?.stop_requested {
            let _ = self.port.stop(binding.provider.as_str(), binding.run.as_str());
        }
        if self.evaluation.as_ref().is_none_or(|e| e.verify(self.clock.unix_ms()).is_err())
            || self
                .bundle
                .wi()
                .worker_provider_quarantine(&binding.tenant, &binding.provider)?
                .is_some()
        {
            // Approval invalidation fences this run. Only an attributed incident
            // decision may permanently quarantine the tenant/provider.
            super::recovery::quarantine(self.bundle.wi(), binding)?;
            let _ = self.port.stop(binding.provider.as_str(), binding.run.as_str());
        }
        // A reopened port cannot reconstruct handles or credentials. It reads only
        // the original private cleanup journal, never adopts a numeric process id.
        if !self
            .live
            .lock()
            .map_err(|_| "worker registry unavailable")?
            .contains_key(&(binding.provider.clone(), binding.run.clone()))
        {
            let _ = self.recovery_evidence(binding);
            return Ok(());
        }
        for observed in self.port.observe(binding.provider.as_str(), binding.run.as_str())? {
            let route = worldinterface_domain::continuation::EventRoute {
                tenant: binding.tenant.clone(),
                audience: binding.principal.clone(),
                kind: worldinterface_domain::continuation::EventKind::WorkerObservation,
                correlation: binding.event_correlation()?,
                generation: None,
            };
            super::observation::commit(self.bundle.wi(), binding, route, observed)?;
        }
        Ok(())
    }
}
fn observation(commit: CommitObservation, outcome: ExecutionOutcome) -> PortObservation {
    PortObservation { commit, outcome, outputs: vec![], evidence: vec![], boundary_record: None }
}
pub struct WorkerConnector<S> {
    runtime: Arc<WorkerRuntime<S>>,
    descriptor: ConnectorDescriptor,
}
impl<S> Clone for WorkerConnector<S> {
    fn clone(&self) -> Self {
        Self { runtime: self.runtime.clone(), descriptor: self.descriptor.clone() }
    }
}
impl<S: TrustedWorkerBuild> WorkerConnector<S> {
    fn recovery_state(&self, r: &ReconcileRequest) -> Result<WorkerState> {
        let root = self.runtime.bundle.wi();
        let snapshot = root.effect_snapshot(&r.tenant)?;
        let effect = snapshot.effects.get(&r.intent).ok_or("recovery effect absent")?;
        let w = effect
            .authorization_inputs
            .as_ref()
            .and_then(|f| f.worker.as_ref())
            .ok_or("recovery realization absent")?;
        let state = root
            .worker_snapshot(&r.tenant, &w.provider, &w.run)?
            .ok_or("recovery worker absent")?;
        if r.tenant.as_str() != self.runtime.scope.tenant
            || effect.agency.binding.principal.as_str() != self.runtime.scope.principal
            || effect.resolution.as_ref().map(|x| &x.resolved.resource) != Some(&r.resource)
            || effect.resolution.as_ref().and_then(|x| x.resolved.connector_execution.as_ref())
                != Some(&r.execution)
            || r.execution.binding.descriptor.operation != self.descriptor.operation
        {
            return Err("recovery connector identity mismatch".into());
        }
        self.runtime.recovery_authorization(&state.binding, "reconcile")?;
        let launch = root.worker_launch(&state.binding)?.ok_or("recovery launch absent")?;
        if !launch.connectors.contains(&r.execution.binding) {
            return Err("original recovery implementation mismatch".into());
        }
        Ok(state)
    }
}
impl<S: TrustedWorkerBuild + 'static> Connector for WorkerConnector<S> {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }
    fn validate_artifact(&self, schema: &ContentHash, bytes: &[u8]) -> Result<()> {
        if schema == &ContentHash::of(b"wi-worker-result:1") {
            let result: WorkerAcknowledgement = serde_json::from_value(strict_json(bytes)?)?;
            if result.version != 1 || canonical_data(&result)? != bytes {
                return Err("worker acknowledgement encoding".into());
            }
            return Ok(());
        }
        if schema != &ContentHash::of(b"wi-worker-effect:1") {
            return Err("worker artifact schema mismatch".into());
        }
        super::admission::WorkerEffect::decode_structure(bytes)?;
        Ok(())
    }
    fn resolve(&self, r: ResolveRequest) -> Result<ResolveObservation> {
        Ok(ResolveObservation { resource: r.resource })
    }
    fn prepare(&self, r: &AuthorizedInvocation) -> PrepareOutcome {
        if self.runtime.recovery_invocation(r).is_ok_and(|v| v.is_some()) {
            return PrepareOutcome::Ready;
        }
        let Some(evaluation) = self.runtime.evaluation.as_ref() else {
            return PrepareOutcome::PermanentRejection;
        };
        match derive(
            self.runtime.bundle.wi(),
            r,
            evaluation,
            self.runtime.clock.unix_ms(),
            &self.runtime.executable,
            &self.runtime.helper_digest,
            &self.runtime.adapter,
        ) {
            Ok(a) if a.operation.name() == self.descriptor.operation.as_str() => {
                PrepareOutcome::Ready
            }
            _ => PrepareOutcome::PermanentRejection,
        }
    }
    fn invoke(&self, r: PreparedInvocation, c: &CancellationToken) -> Result<PortObservation> {
        if let Some((binding, request)) = self.runtime.recovery_invocation(&r)? {
            if request.operation.name() != self.descriptor.operation.as_str() {
                return Err("recovery operation mismatch".into());
            }
            if request.operation == Operation::Terminate {
                let state = self.runtime.state(&binding)?;
                if !state.stop_requested && !state.cleanup_confirmed {
                    self.runtime.bundle.wi().request_worker_stop(&state)?;
                }
                let _ = self.runtime.port.stop(binding.provider.as_str(), binding.run.as_str());
            }
            self.runtime.poll(&binding)?;
            return succeeded(&request, &self.runtime.state(&binding)?);
        }
        let admitted = derive(
            self.runtime.bundle.wi(),
            &r,
            self.runtime.evaluation.as_ref().ok_or("admission approval absent")?,
            self.runtime.clock.unix_ms(),
            &self.runtime.executable,
            &self.runtime.helper_digest,
            &self.runtime.adapter,
        )?;
        if admitted.operation.name() != self.descriptor.operation.as_str() {
            return Err("worker operation mismatch".into());
        }
        self.runtime.invoke(admitted, c)
    }
    fn reconcile(&self, r: ReconcileRequest) -> Result<ReconcileOutcome> {
        let mut state = self.recovery_state(&r)?;
        let root = self.runtime.bundle.wi();
        if self
            .runtime
            .evaluation
            .as_ref()
            .is_none_or(|e| e.verify(self.runtime.clock.unix_ms()).is_err())
            || root.worker_provider_quarantine(&r.tenant, &state.binding.provider)?.is_some()
        {
            super::recovery::quarantine(root, &state.binding)?;
            state = self.runtime.state(&state.binding)?;
        }
        self.runtime.recovery_authorization(&state.binding, "reconcile")?;
        if state.quarantined
            && self.descriptor.operation.as_str() == "collect_artifacts"
            && !matches!(state.collection, WorkerCollection::Published(_))
        {
            return Ok(ReconcileOutcome::Unknown(vec![EvidenceBytes {
                schema: "wi-worker-lifecycle:2".to_string().try_into()?,
                bytes: canonical_data(&state)?,
            }]));
        }
        if let Some(key) = &r.execution.provider_key {
            if let Some(operation) = state.operations.get(key).cloned() {
                if !operation.acknowledged
                    && operation.intent == r.intent
                    && operation.operation == self.descriptor.operation
                {
                    if operation.operation.as_str() == "collect_artifacts"
                        && matches!(
                            state.collection,
                            WorkerCollection::Prepared(_) | WorkerCollection::Published(_)
                        )
                    {
                        super::artifacts::resume(root, &state.binding)?;
                        self.runtime.finish_collection(&state.binding)?;
                        root.acknowledge_worker_operation(
                            state.binding.clone(),
                            operation.clone(),
                        )?;
                    } else {
                        let launch =
                            root.worker_launch(&state.binding)?.ok_or("worker launch absent")?;
                        let operations: Vec<(
                            WorkerRequestV1,
                            worldinterface_worker::mapping::ResolvedWorkerV1,
                        )> = serde_json::from_slice(&launch.operations)?;
                        let request = operations
                            .iter()
                            .find(|(q, _)| q.operation.name() == operation.operation.as_str())
                            .map(|(q, _)| q.clone());
                        if let Some(request) = request {
                            if self.runtime.port.acknowledged(&request).unwrap_or(false) {
                                self.runtime.poll(&state.binding)?;
                                root.acknowledge_worker_operation(
                                    state.binding.clone(),
                                    operation.clone(),
                                )?;
                            } else {
                                return Ok(ReconcileOutcome::Unknown(vec![]));
                            }
                        }
                    }
                    state = self.runtime.state(&state.binding)?;
                }
            }
        }
        let evidence = vec![EvidenceBytes {
            schema: "wi-worker-lifecycle:2".to_string().try_into()?,
            bytes: canonical_data(&state)?,
        }];
        if let Some(key) = &r.execution.provider_key {
            if state.operations.get(key).is_some_and(|o| {
                o.intent == r.intent && o.operation == self.descriptor.operation && o.acknowledged
            }) {
                return Ok(ReconcileOutcome::Committed(evidence));
            }
        }
        super::recovery::quarantine(root, &state.binding)?;
        Ok(ReconcileOutcome::Unknown(evidence))
    }
    fn check_recovery(&self, r: &ReconcileRequest) -> Result<()> {
        self.recovery_state(r).map(|_| ())
    }
    fn cancel(&self, r: ReconcileRequest) -> Result<CancelObservation> {
        let state = self.recovery_state(&r)?;
        let root = self.runtime.bundle.wi();
        self.runtime.recovery_authorization(&state.binding, "terminate")?;
        if !state.stop_requested && !state.cleanup_confirmed {
            root.request_worker_stop(&state)?;
        }
        // A stop obligation survives grant/approval expiry; it cannot authorize fresh work.
        let _ = self.runtime.port.stop(state.binding.provider.as_str(), state.binding.run.as_str());
        Ok(CancelObservation::Requested)
    }
}

fn default_recovery_policy(scope: HostScope) -> Result<super::catalog::RecoveryPolicy> {
    let implementation = descriptor(Operation::Start)?.implementation;
    super::catalog::RecoveryPolicy::new(
        scope,
        implementation.clone(),
        vec![implementation],
        ContentHash::of(b"wi-worker-recovery:1"),
    )
}
/// A recovery-only host has no executable registration or admission approval.
pub struct RecoveryOnly;
impl TrustedApprovals for RecoveryOnly {
    fn accepted(&self, _: &super::evaluation::EvaluationManifest, _: &Path) -> Result<bool> {
        Ok(false)
    }
}
impl TrustedWorkerBuild for RecoveryOnly {
    fn verify_build(&self, _: &ContentHash, _: &ContentHash, _: &ContentHash) -> Result<()> {
        Err("recovery cannot admit builds".into())
    }
}
impl<S: TrustedWorkerBuild> WorkerRuntime<S> {
    pub fn install_recovery(
        bundle: Arc<StoreBundle>,
        scope: HostScope,
        root: &Path,
        policy: super::catalog::RecoveryPolicy,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let helper_digest = ContentHash::of(b"no-recovery-launch-helper");
        let port = ProcessWorkerPort::new_recovery(scope.clone(), root, clock.clone())?;
        Ok(Self {
            bundle,
            port: Arc::new(port),
            adapter: adapter_digest(),
            evaluation: None,
            recovery_policy: policy,
            clock,
            executable: ContentHash::of(b"no-executable"),
            helper_digest,
            slots: 0,
            seam_simulation: false,
            scope,
            live: Mutex::new(BTreeMap::new()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use worldinterface_domain::{
        effect::{validate_outcome_publication, OutcomeEvidence},
        Phase,
    };
    use worldinterface_worker::profile::ProfileBinding;

    /// No executable, runtime approval or provider is constructed. Exercise the
    /// domain publication validator used by the executor against all success
    /// responses: fresh completion, replay, inspect and quarantined termination.
    #[test]
    fn worker_success_publication_satisfies_the_executor_contract_without_launch() {
        let binding = WorkerBinding {
            tenant: "tenant:publication".to_string().try_into().unwrap(),
            principal: "12345678-1234-4234-8234-123456789abc".to_string().try_into().unwrap(),
            provider: "local-proof".to_string().try_into().unwrap(),
            run: "run-proof".to_string().try_into().unwrap(),
            request: "start-request".to_string().try_into().unwrap(),
            realization: ContentHash::of(b"publication-only"),
        };
        let initial = WorkerState::start_intent(binding.clone());
        let mut advanced = initial.clone();
        advanced.transition(WorkerFact::StartAccepted).unwrap();
        advanced.transition(WorkerFact::Running).unwrap();
        advanced.revision += 2;
        let mut quarantined = advanced.clone();
        quarantined.transition(WorkerFact::Quarantined).unwrap();
        let mut cleaned = quarantined.clone();
        cleaned.transition(WorkerFact::CleanupConfirmed).unwrap();
        let reference = |bytes: &[u8]| ArtifactRef {
            scope: binding.tenant.clone(),
            content: ContentHash::of(bytes),
            size: bytes.len() as u64,
        };
        let evidence = |e: &EvidenceBytes| EvidenceRef {
            artifact: reference(&e.bytes),
            schema: e.schema.clone(),
        };
        let mut identities = std::collections::BTreeSet::new();
        for op in super::super::admission::OPERATIONS {
            let request = WorkerRequestV1 {
                schema: 1,
                profile: ProfileBinding::process_proof(),
                operation: op,
                provider: binding.provider.as_str().into(),
                run: binding.run.as_str().into(),
                request: format!("operation-{}", op.name()),
                input_digest: ContentHash::of(b"input"),
                controls_digest: ContentHash::of(b"controls"),
            };
            // The manifest is formed before lifecycle state or acknowledgement.
            let declared = reference(&acknowledgement(&request).unwrap());
            assert!(identities.insert(declared.content.clone()));
            for state in [&initial, &advanced, &quarantined, &cleaned] {
                let observed = succeeded(&request, state).unwrap();
                let decoded: WorkerAcknowledgement =
                    serde_json::from_value(strict_json(&observed.outputs[0]).unwrap()).unwrap();
                assert_eq!(decoded.version, 1);
                assert_eq!(decoded.request, request.digest().unwrap());
                let boundary = observed.boundary_record.as_ref().unwrap();
                assert_eq!(boundary.schema.as_str(), "wi-worker-lifecycle:2");
                assert_eq!(serde_json::from_slice::<WorkerState>(&boundary.bytes).unwrap(), *state);
                let mut outcome = OutcomeEvidence {
                    classification: None,
                    reconciliation_admission: None,
                    intent: "12345678-1234-4234-8234-123456789abc".to_string().try_into().unwrap(),
                    attempt: "attempt-1".to_string().try_into().unwrap(),
                    connector_semantics: "worker:1".to_string().try_into().unwrap(),
                    commit: observed.commit,
                    outcome: observed.outcome,
                    evidence: observed.evidence.iter().map(evidence).collect(),
                    required_outputs: vec![declared.clone()],
                    durable_outputs: observed.outputs.iter().map(|b| reference(b)).collect(),
                    boundary_record: Some(evidence(boundary)),
                };
                assert_eq!(
                    validate_outcome_publication(&binding.tenant, &outcome).unwrap(),
                    Phase::Succeeded
                );
                outcome.boundary_record = None;
                assert!(validate_outcome_publication(&binding.tenant, &outcome).is_err());
                outcome.boundary_record = Some(evidence(boundary));
                outcome.required_outputs.clear();
                assert!(validate_outcome_publication(&binding.tenant, &outcome).is_err());
            }
        }
    }
}
