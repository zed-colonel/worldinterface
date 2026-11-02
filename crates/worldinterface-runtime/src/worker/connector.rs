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
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};
use worldinterface_domain::{effect::CommitObservation, worker::*};
use worldinterface_protocol::*;
use worldinterface_worker::mapping::{Operation, WorkerRequestV1};

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
    pub(crate) port: ProcessWorkerPort,
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
            port,
            evaluation,
            clock,
            executable,
            helper_digest,
            slots,
            scope,
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
            let mut result =
                observation(CommitObservation::ConfirmedCommitted, ExecutionOutcome::Succeeded);
            result.outputs.push(canonical_data(&self.state(binding)?)?);
            return Ok(result);
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
        let state = self.state(binding)?;
        if let Some(old) = state.operations.get(&provider_operation.key) {
            let mut expected = old.clone();
            expected.acknowledged = false;
            if expected != provider_operation {
                return Err("worker provider key conflict".into());
            }
            if old.acknowledged {
                let mut result =
                    observation(CommitObservation::ConfirmedCommitted, ExecutionOutcome::Succeeded);
                result.outputs.push(canonical_data(&state)?);
                return Ok(result);
            }
            super::recovery::quarantine(root, binding)?;
            return Ok(observation(CommitObservation::Unknown, ExecutionOutcome::Uncertain));
        }
        root.begin_worker_operation(binding.clone(), provider_operation.clone())?;
        if !fresh && !live.contains_key(&key) {
            super::recovery::quarantine(root, binding)?;
            if op == Operation::Terminate {
                root.acknowledge_worker_operation(binding.clone(), provider_operation)?;
                let mut result =
                    observation(CommitObservation::ConfirmedCommitted, ExecutionOutcome::Succeeded);
                result.outputs.push(canonical_data(&self.state(binding)?)?);
                return Ok(result);
            }
            return Ok(observation(CommitObservation::Unknown, ExecutionOutcome::Uncertain));
        }
        if fresh {
            let operations = admitted.authorization.operations.clone();
            self.port.admit(admitted.authorization)?;
            live.insert(key.clone(), operations);
        }
        let request = live[&key]
            .iter()
            .find(|(r, _)| r.operation == op)
            .ok_or("worker operation absent")?
            .0
            .clone();
        if cancellation.is_cancelled() || op == Operation::Terminate {
            let current = self.state(binding)?;
            if !current.stop_requested && !current.cleanup_confirmed {
                root.request_worker_stop(&current)?;
            }
        }
        let stopping = self.state(binding)?.stop_requested;
        if stopping {
            self.port.stop_admitted(binding.provider.as_str(), binding.run.as_str())?;
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
            self.port.submit_admitted(&provision)?;
        }
        let result = self.port.submit_admitted(&request);
        if result.is_err() {
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
        if op == Operation::CollectArtifacts
            && !self.state(binding)?.completed
            && !self.state(binding)?.stop_requested
            && !self.state(binding)?.quarantined
            && self
                .port
                .evidence(binding.provider.as_str(), binding.run.as_str())?
                .is_some_and(|r| r.exit_success == Some(true))
        {
            self.transition(binding, WorkerFact::Completed)?;
        }
        root.acknowledge_worker_operation(binding.clone(), provider_operation)?;
        let mut result =
            observation(CommitObservation::ConfirmedCommitted, ExecutionOutcome::Succeeded);
        result.outputs.push(canonical_data(&self.state(binding)?)?);
        Ok(result)
    }
    pub fn read_artifact(&self, binding: &WorkerBinding, reference: &str) -> Result<Vec<u8>> {
        self.evaluation.verify(self.clock.unix_ms())?;
        if binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
            || self.state(binding)?.binding != *binding
        {
            return Err("worker artifact scope mismatch".into());
        }
        Ok(self.port.artifact_admitted(
            binding.provider.as_str(),
            binding.run.as_str(),
            reference,
        )?)
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
        Ok(self.port.recover_cleanup(
            binding.provider.as_str(),
            binding.run.as_str(),
            operations,
        )?)
    }
    pub fn cleanup_report(&self, binding: &WorkerBinding) -> Result<Option<CleanupReport>> {
        if binding.tenant.as_str() != self.scope.tenant
            || binding.principal.as_str() != self.scope.principal
        {
            return Err("worker read scope mismatch".into());
        }
        Ok(self.port.evidence(binding.provider.as_str(), binding.run.as_str())?)
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
            self.port.stop_admitted(binding.provider.as_str(), binding.run.as_str())?;
        }
        if let Err(error) = self.evaluation.verify(self.clock.unix_ms()) {
            super::recovery::quarantine(self.bundle.wi(), binding)?;
            self.port.stop_admitted(binding.provider.as_str(), binding.run.as_str())?;
            return Err(error);
        }
        for observed in
            self.port.observations_admitted(binding.provider.as_str(), binding.run.as_str())?
        {
            let route = worldinterface_domain::continuation::EventRoute {
                tenant: binding.tenant.clone(),
                audience: binding.principal.clone(),
                kind: worldinterface_domain::continuation::EventKind::WorkerObservation,
                correlation: binding.run.clone(),
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
            let _: WorkerState = serde_json::from_value(strict_json(bytes)?)?;
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
        let state = root.worker_snapshot(&r.tenant, &w.provider, &w.run)?.ok_or("worker absent")?;
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
        let _ = self.runtime.port.stop_admitted(w.provider.as_str(), w.run.as_str());
        Ok(CancelObservation::Requested)
    }
}
