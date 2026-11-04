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
            schema: "wi-worker-lifecycle:1".to_string().try_into()?,
            bytes: bytes.clone(),
        }],
        boundary_record: Some(EvidenceBytes {
            schema: "wi-worker-lifecycle:1".to_string().try_into()?,
            bytes,
        }),
    })
}

pub fn descriptor(operation: Operation) -> Result<ConnectorDescriptor> {
    let d = ConnectorDescriptor {
        version: 1,
        connector: "wi:process-worker".to_string().try_into()?,
        implementation: ContentHash::of(
            [
                include_str!("connector.rs"),
                include_str!("admission.rs"),
                include_str!("evaluation.rs"),
                include_str!("recovery.rs"),
                include_str!("observation.rs"),
                include_str!("artifacts.rs"),
                include_str!("provider.rs"),
            ]
            .concat()
            .as_bytes(),
        ),
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
        evidence_schema: "wi-worker-lifecycle:1".to_string().try_into()?,
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
    pub(crate) evaluation: Arc<EvaluationVerifier<S>>,
    pub(crate) clock: Arc<dyn Clock>,
    executable: ContentHash,
    helper_digest: ContentHash,
    slots: u32,
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
            evaluation,
            clock,
            executable,
            helper_digest,
            slots,
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
            scope,
            port,
            executable: provenance.implementation,
            helper_digest: provenance.helper,
            adapter: provenance.adapter,
            evaluation,
            clock,
            slots,
            live: Mutex::new(BTreeMap::new()),
        })
    }
    pub fn connector(self: &Arc<Self>, operation: Operation) -> Result<WorkerConnector<S>> {
        Ok(WorkerConnector { runtime: self.clone(), descriptor: descriptor(operation)? })
    }
    fn state(&self, b: &WorkerBinding) -> Result<WorkerState> {
        self.bundle
            .wi()
            .worker_snapshot(&b.tenant, &b.provider, &b.run)?
            .ok_or_else(|| "worker absent".into())
    }
    fn transition(&self, b: &WorkerBinding, fact: WorkerFact) -> Result<()> {
        self.bundle.wi().record_worker_transition(&self.state(b)?, fact)
    }
    fn invoke(
        &self,
        admitted: AdmittedWorker,
        cancellation: &CancellationToken,
    ) -> Result<PortObservation> {
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
            if self.state(binding)?.binding != *binding {
                return Err("worker inspect binding mismatch".into());
            }
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
            root.reserve_start(binding.clone(), self.slots)?
        } else {
            if self.state(binding)?.binding != *binding {
                return Err("worker operation binding mismatch".into());
            }
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
                    && (state.quarantined
                        || !matches!(state.collection, WorkerCollection::Published(_)))
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
            self.port.admit(admitted.authorization)?;
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
            self.port.submit(&provision)?;
        }
        let result = self.port.submit(&request);
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
        root.acknowledge_worker_operation(binding.clone(), provider_operation)?;
        succeeded(&request, &self.state(binding)?)
    }
    pub fn read_artifact(&self, binding: &WorkerBinding, reference: &str) -> Result<Vec<u8>> {
        self.evaluation.verify(self.clock.unix_ms())?;
        if binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
            || self.state(binding)?.binding != *binding
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
        self.evaluation.verify(self.clock.unix_ms())?;
        if self.state(binding)?.binding != *binding
            || binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
        {
            return Err("worker recovery scope mismatch".into());
        }
        let live = self.live.lock().map_err(|_| "worker registry unavailable")?;
        let operations = live
            .get(&(binding.provider.clone(), binding.run.clone()))
            .ok_or("worker recovery requires original operation bindings")?;
        self.port.recover_cleanup(binding.provider.as_str(), binding.run.as_str(), operations)
    }
    pub fn cleanup_report(&self, binding: &WorkerBinding) -> Result<Option<CleanupReport>> {
        if binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
            || self.state(binding)?.binding != *binding
        {
            return Err("worker read scope mismatch".into());
        }
        self.port.evidence(binding.provider.as_str(), binding.run.as_str())
    }
    /// Host maintenance explicitly polls durable observations; AQ never depends on this process's memory.
    pub fn poll(&self, binding: &WorkerBinding) -> Result<()> {
        if binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
        {
            return Err("worker polling scope mismatch".into());
        }
        if self.state(binding)?.binding != *binding {
            return Err("worker polling realization mismatch".into());
        }
        if self.state(binding)?.stop_requested {
            self.port.stop(binding.provider.as_str(), binding.run.as_str())?;
        }
        if let Err(error) = self.evaluation.verify(self.clock.unix_ms()) {
            super::recovery::quarantine(self.bundle.wi(), binding)?;
            self.port.stop(binding.provider.as_str(), binding.run.as_str())?;
            return Err(error);
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
        super::admission::WorkerEffect::decode(bytes)?;
        Ok(())
    }
    fn resolve(&self, r: ResolveRequest) -> Result<ResolveObservation> {
        Ok(ResolveObservation { resource: r.resource })
    }
    fn prepare(&self, r: &AuthorizedInvocation) -> PrepareOutcome {
        match derive(
            self.runtime.bundle.wi(),
            r,
            &self.runtime.evaluation,
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
        let admitted = derive(
            self.runtime.bundle.wi(),
            &r,
            &self.runtime.evaluation,
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
        self.runtime.evaluation.verify(self.runtime.clock.unix_ms())?;
        let root = self.runtime.bundle.wi();
        let effect = root
            .effect_snapshot(&r.tenant)?
            .effects
            .get(&r.intent)
            .cloned()
            .ok_or("worker effect absent")?;
        if r.tenant.as_str() != self.runtime.scope.tenant
            || effect.agency.binding.principal.as_str() != self.runtime.scope.principal
        {
            return Err("worker reconciliation scope mismatch".into());
        }
        if effect.resolution.as_ref().map(|x| &x.resolved.resource) != Some(&r.resource)
            || effect.resolution.as_ref().and_then(|x| x.resolved.connector_execution.as_ref())
                != Some(&r.execution)
        {
            return Err("worker reconciliation identity mismatch".into());
        }
        let w = effect
            .authorization_inputs
            .as_ref()
            .and_then(|s| s.worker.as_ref())
            .ok_or("worker realization absent")?;
        let mut state =
            root.worker_snapshot(&r.tenant, &w.provider, &w.run)?.ok_or("worker absent")?;
        if state.quarantined && self.descriptor.operation.as_str() == "collect_artifacts" {
            return Ok(ReconcileOutcome::Unknown(vec![EvidenceBytes {
                schema: "wi-worker-lifecycle:1".to_string().try_into()?,
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
                        let request = self
                            .runtime
                            .live
                            .lock()
                            .map_err(|_| "worker registry unavailable")?
                            .get(&(w.provider.clone(), w.run.clone()))
                            .and_then(|operations| {
                                operations
                                    .iter()
                                    .find(|(q, _)| {
                                        q.operation.name() == operation.operation.as_str()
                                    })
                                    .map(|(q, _)| q.clone())
                            });
                        if let Some(request) = request {
                            if self.runtime.port.acknowledged(&request)? {
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
                    state = root
                        .worker_snapshot(&r.tenant, &w.provider, &w.run)?
                        .ok_or("worker absent")?;
                }
            }
        }
        let evidence = vec![EvidenceBytes {
            schema: "wi-worker-lifecycle:1".to_string().try_into()?,
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
    fn cancel(&self, r: ReconcileRequest) -> Result<CancelObservation> {
        let root = self.runtime.bundle.wi();
        let effect = root
            .effect_snapshot(&r.tenant)?
            .effects
            .get(&r.intent)
            .cloned()
            .ok_or("worker effect absent")?;
        if effect.resolution.as_ref().and_then(|x| x.resolved.connector_execution.as_ref())
            != Some(&r.execution)
            || effect.resolution.as_ref().map(|x| &x.resolved.resource) != Some(&r.resource)
        {
            return Err("worker cancellation identity mismatch".into());
        }
        let w = effect
            .authorization_inputs
            .as_ref()
            .and_then(|s| s.worker.as_ref())
            .ok_or("worker realization absent")?;
        let state = root.worker_snapshot(&r.tenant, &w.provider, &w.run)?.ok_or("worker absent")?;
        if state.binding.tenant.as_str() != self.runtime.scope.tenant
            || state.binding.principal.as_str() != self.runtime.scope.principal
        {
            return Err("worker cleanup scope mismatch".into());
        }
        if !state.stop_requested && !state.cleanup_confirmed {
            root.request_worker_stop(&state)?;
        }
        // A stop obligation survives grant/approval expiry; it cannot authorize fresh work.
        let _ = self.runtime.port.stop(w.provider.as_str(), w.run.as_str());
        Ok(CancelObservation::Requested)
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
                assert_eq!(boundary.schema.as_str(), "wi-worker-lifecycle:1");
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
