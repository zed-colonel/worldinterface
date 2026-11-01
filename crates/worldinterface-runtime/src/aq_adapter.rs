//! Explicit mappings; opaque attribution never selects authority.
use crate::storage::StoreBundle;
use crate::Result;
use actionqueue_core::control::HostControlContext;
use actionqueue_core::disposition::AttemptDisposition;
use actionqueue_core::{
    admission::{EnsureTaskOutcome, EnsureTaskRequest},
    bounded::{ContentHash, DataScheme, HashAlgorithm, OpaqueRef},
    causal::{CausalContext, CausalOverride, CausationLink},
    data_ref::ExternalDataRef,
    ids::{AttemptId, CorrelationId, RunId, TaskId, TraceId},
    task::task_spec::TaskPayload,
};
use actionqueue_executor_local::handler::ExecutorContext;
use actionqueue_executor_local::handler::ExecutorHandler;
use actionqueue_runtime::{
    config::RuntimeConfig,
    engine::{ActionQueueEngine, BootstrappedEngine},
};
use actionqueue_storage::{recovery::reducer::ReplayReducer, store::StoreSession};
use serde::{Deserialize, Serialize};
use worldinterface_protocol::{canonical_data, strict_json, ArtifactRef};

/// Store identity is obtained from a validated, locked AQ session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueStoreId(uuid::Uuid);
impl QueueStoreId {
    pub fn from_session(session: &StoreSession) -> Self {
        Self(session.manifest().store_id)
    }
    pub fn as_uuid(self) -> uuid::Uuid {
        self.0
    }
}

/// The engine is bootstrapped only from the validated bundle whose WI journal
/// records its admission. A caller cannot pair an engine from another store
/// with this handle or supply an independent store identity.
pub struct QueueEngine<'a, H: ExecutorHandler + 'static> {
    bundle: &'a StoreBundle,
    store: QueueStoreId,
    engine: BootstrappedEngine<H>,
    signal_scan: crate::continuation::SignalScan,
    signal_limits: actionqueue_core::limits::SignalLimits,
    pub signal_delivery_status: crate::continuation::SignalDeliveryStatus,
}
/// Dispatches by the immutable admitted payload shape. The effect handler still
/// verifies the persisted AQ admission and observed run before invoking a port.
pub struct TaskRouter<I, E> {
    pub ingress: I,
    pub effect: E,
}
impl<I: ExecutorHandler, E: ExecutorHandler> ExecutorHandler for TaskRouter<I, E> {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        if TaskInputLocator::decode(&ctx.input.payload).is_ok() {
            self.effect.execute(ctx)
        } else {
            self.ingress.execute(ctx)
        }
    }
}
impl<'a, H: ExecutorHandler + 'static> QueueEngine<'a, H> {
    pub fn bootstrap(
        bundle: &'a StoreBundle,
        handler: H,
        host: HostControlContext,
    ) -> Result<Self> {
        Self::bootstrap_with_signal_limits(bundle, handler, host, Default::default())
    }
    /// Host capacity configuration; Flow binding checks do not allocate signals.
    pub fn bootstrap_with_signal_limits(
        bundle: &'a StoreBundle,
        handler: H,
        host: HostControlContext,
        signal_limits: actionqueue_core::limits::SignalLimits,
    ) -> Result<Self> {
        let session = actionqueue_storage::store::open_store(
            &bundle.aq_path(),
            actionqueue_storage::store::OpenOptions::ReadOnly,
        )?;
        let store = QueueStoreId::from_session(&session);
        drop(session);
        let started = std::time::Instant::now();
        let engine = ActionQueueEngine::new(
            RuntimeConfig {
                data_dir: bundle.aq_path(),
                signal_limits,
                continuation_limits: actionqueue_core::limits::ContinuationLimits {
                    disposition_bytes: 1024 * 1024,
                    ..Default::default()
                },
                ..Default::default()
            },
            handler,
        )
        .bootstrap()?
        .with_host(host);
        crate::storage::recovery_timing("AQ engine bootstrap and scheduling", started);
        if bundle.aq_store_id()? != store.as_uuid() {
            return Err("AQ store changed during engine bootstrap".into());
        }
        Ok(Self {
            bundle,
            store,
            engine,
            signal_scan: Default::default(),
            signal_delivery_status: Default::default(),
            signal_limits,
        })
    }
    pub fn wi(&self) -> &worldinterface_storage::StoreRoot {
        self.bundle.wi()
    }
    pub fn bundle(&self) -> &StoreBundle {
        self.bundle
    }
    pub fn store_id(&self) -> uuid::Uuid {
        self.store.as_uuid()
    }
    pub fn engine(&self) -> &BootstrappedEngine<H> {
        &self.engine
    }
    /// Pollable host alarm: retained and retired records share lifetime capacity.
    pub fn signal_capacity(&self) -> crate::continuation::SignalCapacity {
        crate::continuation::SignalCapacity::from_statistics(
            self.engine.signal_statistics(),
            self.signal_limits,
        )
    }
    /// Submit through the engine bound at bootstrap. The engine itself cannot
    /// be replaced independently of the WI journal and AQ store identity.
    pub fn ensure_task(
        &mut self,
        request: EnsureTaskRequest,
    ) -> std::result::Result<EnsureTaskOutcome, actionqueue_runtime::control::ServiceError> {
        self.engine.ensure_task(request)
    }
    /// Install current trusted host authority without replacing the bound engine.
    pub fn set_host_control_context(&mut self, host: Option<HostControlContext>) {
        self.engine.set_control_context(host);
    }
    pub(crate) fn parts_mut(
        &mut self,
    ) -> (&worldinterface_storage::StoreRoot, &mut BootstrappedEngine<H>, uuid::Uuid) {
        (self.bundle.wi(), &mut self.engine, self.store.as_uuid())
    }
    pub fn drain_signals(&mut self) -> Result<crate::continuation::SignalDeliveryStatus> {
        let status =
            crate::continuation::drain(self.bundle.wi(), &mut self.engine, &mut self.signal_scan)?;
        self.signal_delivery_status = status.clone();
        Ok(status)
    }
    pub fn resume_run(&mut self, run_id: RunId) -> Result<()> {
        self.engine.resume_run(run_id)?;
        Ok(())
    }
    pub fn resolve_wait(
        &mut self,
        run_id: RunId,
        wait_id: actionqueue_core::ids::WaitId,
    ) -> Result<()> {
        self.engine.control(actionqueue_runtime::control::ControlOperation::ResolveWait {
            run_id,
            wait_id,
        })?;
        Ok(())
    }
    /// Host-owned reconciliation surrounds each scheduling tick, including restart.
    pub async fn tick(&mut self) -> Result<actionqueue_runtime::dispatch::TickResult> {
        crate::flow_coordinator::reconcile(
            self.bundle.wi(),
            self.store.as_uuid(),
            &mut self.engine,
        )?;
        self.signal_delivery_status =
            crate::continuation::drain(self.bundle.wi(), &mut self.engine, &mut self.signal_scan)?;
        let result = self.engine.tick().await?;
        crate::flow_coordinator::reconcile(
            self.bundle.wi(),
            self.store.as_uuid(),
            &mut self.engine,
        )?;
        self.signal_delivery_status =
            crate::continuation::drain(self.bundle.wi(), &mut self.engine, &mut self.signal_scan)?;
        Ok(result)
    }
    /// Trusted host cancellation retains AQ's normal child lifecycle semantics.
    pub fn cancel_task(&mut self, task: TaskId) -> Result<()> {
        let root = self.bundle.wi();
        // AQ authorizes the control first. Holding the WI guard across control
        // and the cancel journal write prevents Start in the cross-store gap.
        let guard = root.flow_dispatch_guard()?;
        self.engine.control(actionqueue_runtime::control::ControlOperation::Cancel(
            actionqueue_core::mutation::CancelTarget::Task(task),
        ))?;
        if let Some((tenant, run)) = root.flow_task(&task.to_string())? {
            let state = root.flow_run_snapshot(&tenant, &run)?;
            if state.runs.get(&run).is_some_and(|r| r.outcome.is_none()) {
                root.flow_update_guarded(
                    &guard,
                    &tenant,
                    state.revision,
                    worldinterface_storage::flow::FlowCommand::CancelRequested { run },
                )?;
            }
        }
        Ok(())
    }
    pub fn shutdown(self) -> Result<()> {
        Ok(self.engine.shutdown()?)
    }
}

/// Untrusted provenance claims. Verification checks both store and actual ancestry.
#[derive(Clone, Debug)]
pub struct QueueProvenance {
    pub store: QueueStoreId,
    pub task: TaskId,
    pub run: Option<RunId>,
    pub attempt: Option<AttemptId>,
}
/// A read-only recovered session binds projection and store identity together;
/// callers cannot pair a projection from one store with another store's identity.
pub struct QueueReferences {
    session: StoreSession,
    projection: ReplayReducer,
}
impl QueueReferences {
    pub fn task_terminal(&self, task: &str) -> Result<bool> {
        let task: TaskId = task.parse()?;
        let runs = self.projection.run_ids_for_task(task);
        Ok(!runs.is_empty()
            && runs.iter().all(|id| {
                self.projection.get_run_state(id).is_some_and(|s| {
                    matches!(
                        s,
                        actionqueue_core::run::state::RunState::Completed
                            | actionqueue_core::run::state::RunState::Failed
                            | actionqueue_core::run::state::RunState::Canceled
                    )
                })
            }))
    }
    pub fn run_state(&self, run: &str) -> Result<Option<actionqueue_core::run::state::RunState>> {
        let run: RunId = run.parse()?;
        Ok(self.projection.get_run_state(&run).copied())
    }
    pub fn terminal_failure_code(&self, run: &str) -> Result<Option<String>> {
        let run: RunId = run.parse()?;
        Ok(self
            .projection
            .get_attempt_history(&run)
            .and_then(|history| history.last())
            .and_then(|attempt| attempt.disposition.as_ref())
            .and_then(|record| match record.disposition.outcome() {
                actionqueue_core::disposition::DispositionOutcome::TerminalFailure { error } => {
                    Some(error.code.as_str().to_owned())
                }
                _ => None,
            }))
    }
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let session = actionqueue_storage::store::open_store(
            path,
            actionqueue_storage::store::OpenOptions::ReadOnly,
        )?;
        let recovered = actionqueue_storage::recovery::bootstrap::recover_read_only(
            &session,
            actionqueue_storage::wal::repair::RepairPolicy::Strict,
        )?;
        if std::env::var_os("WI_RECOVERY_TIMINGS").is_some() {
            eprintln!(
                "AQ recovery snapshot={} sequence={} phases={:?}",
                recovered.snapshot_loaded, recovered.snapshot_sequence, recovered.phases
            );
        }
        Ok(Self { session, projection: recovered.projection })
    }
    /// Checks ingress task and signal references through AQ's recovered public models.
    /// External checkpoint artifact closure remains a separate G4 obligation.
    pub fn validate_artifacts(&self, wi: &worldinterface_storage::StoreRoot) -> Result<()> {
        crate::continuation::validate_references(wi, &self.projection)?;
        crate::flow_coordinator::validate_references(
            wi,
            self.store_id().as_uuid(),
            &self.projection,
        )?;
        for tenant in wi.ingress_tenants()? {
            let state = wi.ingress_snapshot(&tenant)?;
            for (id, q) in &state.queue {
                if id != &q.delivery || q.prior.len() != q.generation as usize {
                    return Err("invalid ingress queue lineage".into());
                }
                for (generation, record) in q.prior.iter().chain(std::iter::once(q)).enumerate() {
                    if record.delivery != *id
                        || record.generation as usize != generation
                        || !record.prior.is_empty() && generation < q.prior.len()
                        || record.version != 1
                        || worldinterface_protocol::ContentHash::of(&record.task_request)
                            != record.task_digest
                        || worldinterface_protocol::ContentHash::of(&record.signal_request)
                            != record.signal_digest
                    {
                        return Err("invalid ingress queue obligation".into());
                    }
                    let task: actionqueue_core::admission::EnsureTaskRequest =
                        serde_json::from_slice(&record.task_request)?;
                    let signal: actionqueue_core::continuation::AdmitSignalRequest =
                        serde_json::from_slice(&record.signal_request)?;
                    let expected = crate::ingress::delivery_task_id(id, record.generation)?;
                    if task.task_spec().id() != expected
                        || task.task_spec().tenant_id().is_some()
                        || task.task_spec().task_payload().bytes()
                            != format!("delivery:{}", id.as_str()).as_bytes()
                    {
                        return Err("ingress queue task identity mismatch".into());
                    }
                    if record.task_admitted {
                        let record = self
                            .projection
                            .task_admission(expected)
                            .ok_or("missing AQ ingress task admission")?;
                        if record.digest() != &task.digest()? {
                            return Err("AQ ingress task admission changed".into());
                        }
                    }
                    if let Some(sequence) = record.signal_sequence {
                        let record = self
                            .projection
                            .signals()
                            .by_sequence(actionqueue_core::ids::SignalSequence::new(sequence))
                            .ok_or("missing AQ ingress signal")?;
                        let expected_signal = signal.envelope(
                            &actionqueue_core::continuation::SignalIngressContext::default(),
                            record.envelope().received_at,
                        );
                        if record.envelope().signal_id != expected_signal.signal_id
                            || record.envelope().namespace != expected_signal.namespace
                            || record.envelope().kind != expected_signal.kind
                            || record.envelope().correlation_id != expected_signal.correlation_id
                            || record.envelope().source_ref != expected_signal.source_ref
                        {
                            return Err("AQ ingress signal changed".into());
                        }
                    }
                }
            }
            for (id, q) in &state.acknowledgments_to_signal {
                if id != &q.delivery
                    || q.version != 1
                    || worldinterface_protocol::ContentHash::of(&q.request) != q.digest
                {
                    return Err("invalid ingress acknowledgment obligation".into());
                }
                if let Some(sequence) = q.sequence {
                    let record = self
                        .projection
                        .signals()
                        .by_sequence(actionqueue_core::ids::SignalSequence::new(sequence))
                        .ok_or("missing AQ acknowledgment signal")?;
                    let request: actionqueue_core::continuation::AdmitSignalRequest =
                        serde_json::from_slice(&q.request)?;
                    let expected = request.envelope(
                        &actionqueue_core::continuation::SignalIngressContext::default(),
                        record.envelope().received_at,
                    );
                    if record.envelope().signal_id != expected.signal_id
                        || record.envelope().kind != expected.kind
                        || record.envelope().correlation_id != expected.correlation_id
                    {
                        return Err("AQ acknowledgment signal changed".into());
                    }
                }
            }
        }
        for task in self.projection.tasks() {
            if task.content_type()
                == Some("application/vnd.worldinterface.task-input+json;version=1")
            {
                wi.read_artifact(Self::locator(task.payload())?.artifact())?;
            }
        }
        let mut effect_tasks = std::collections::BTreeSet::new();
        for tenant in wi.effect_tenants()? {
            for effect in wi.effect_snapshot(&tenant)?.effects.values() {
                for case in &effect.reconciliation_cases {
                    if case.queue_store.as_str() != self.store_id().as_uuid().to_string() {
                        return Err("reconciliation queue store mismatch".into());
                    }
                    if let Some(bytes) = &case.admission_request {
                        let expected = crate::reconciliation::request(case)?;
                        if &serde_json::to_vec(&expected)? != bytes {
                            return Err("invalid reconciliation admission bytes".into());
                        }
                        if case.admission_sequence.is_some()
                            && self.projection.task_admission(expected.task_spec().id()).is_none_or(
                                |a| {
                                    a.digest() != &expected.digest().expect("validated request")
                                        || Some(a.sequence()) != case.admission_sequence
                                },
                            )
                        {
                            return Err("missing reconciliation admission".into());
                        }
                    }
                    if let Some(run) = &case.aq_run {
                        let run_id: RunId = run.as_str().parse()?;
                        let expected = crate::reconciliation::request(case)?;
                        if !self
                            .projection
                            .run_ids_for_task(expected.task_spec().id())
                            .contains(&run_id)
                        {
                            return Err("reconciliation run belongs to another task".into());
                        }
                    }
                }
                for dispatch in &effect.dispatches {
                    if let Some(admission) = &dispatch.admission {
                        effect_tasks.insert(admission.task.as_str().to_owned());
                    }
                }
            }
        }
        for run in self.projection.run_instances() {
            self.projection.get_task(&run.task_id()).ok_or("missing AQ run task")?;
            let effect_task = effect_tasks.contains(&run.task_id().to_string());
            let mut completed_locator = false;
            for attempt in self
                .projection
                .get_attempt_history(&run.id())
                .into_iter()
                .flat_map(|history| history.iter())
            {
                if let Some(actionqueue_core::data_ref::DataRef::External(external)) =
                    attempt.output_ref()
                {
                    if external.scheme.as_str() == "wi-artifact-v1" {
                        let (scope, content) = external
                            .locator
                            .expose()
                            .rsplit_once('/')
                            .ok_or("invalid artifact locator")?;
                        let artifact = ArtifactRef {
                            scope: scope.to_string().try_into()?,
                            content: content.to_string().try_into()?,
                            size: external.size_bytes.ok_or("artifact size required")?,
                        };
                        let bytes = wi.read_artifact(&artifact)?;
                        actionqueue_core::data_ref::DataRef::External(external.clone())
                            .verify_bytes(&bytes)?;
                        if effect_task {
                            let locator: crate::completion::CompletionLocator =
                                serde_json::from_value(strict_json(&bytes)?)?;
                            if locator.version != 1
                                || locator.outcome
                                    != worldinterface_protocol::ExecutionOutcome::Succeeded
                            {
                                return Err("invalid effect completion locator".into());
                            }
                            let state = wi.effect_snapshot(&artifact.scope)?;
                            let effect = state
                                .effects
                                .get(&locator.intent)
                                .ok_or("completion without effect")?;
                            if effect
                                .dispatches
                                .last()
                                .and_then(|d| d.admission.as_ref())
                                .is_none_or(|a| a.task.as_str() != run.task_id().to_string())
                                || effect
                                    .outcomes
                                    .last()
                                    .is_none_or(|o| o.outcome != locator.outcome)
                                || !wi
                                    .effect_receipts(&artifact.scope, &locator.intent)?
                                    .iter()
                                    .any(|(id, digest, _)| {
                                        id == &locator.receipt && digest == &locator.receipt_digest
                                    })
                            {
                                return Err("AQ completion disagrees with Fabric receipt".into());
                            }
                            completed_locator = true;
                        }
                    }
                }
            }
            if effect_task
                && run.state() == actionqueue_core::run::state::RunState::Completed
                && !completed_locator
            {
                return Err("AQ completed effect without published Fabric locator".into());
            }
        }
        Ok(())
    }
    fn locator(bytes: &[u8]) -> Result<TaskInputLocator> {
        TaskInputLocator::decode(bytes)
    }
    pub fn store_id(&self) -> QueueStoreId {
        QueueStoreId::from_session(&self.session)
    }
    pub fn causation(&self, claim: &QueueProvenance) -> Result<CausationLink> {
        if claim.store != self.store_id() {
            // The full bounded encoding retains foreign provenance without typed local links.
            let external = format!(
                "aq/{}/{}/{}/{}",
                claim.store.0,
                claim.task,
                claim.run.map(|v| v.to_string()).unwrap_or_default(),
                claim.attempt.map(|v| v.to_string()).unwrap_or_default()
            );
            return Ok(CausationLink::new(None, None, None, Some(OpaqueRef::new(external)?))?);
        }
        if !self.projection.tasks().any(|t| t.id() == claim.task) {
            return Err("unknown local task".into());
        }
        if let Some(run) = claim.run {
            if !self.projection.run_ids_for_task(claim.task).contains(&run) {
                return Err("run does not belong to local task".into());
            }
            if let Some(attempt) = claim.attempt {
                if !self
                    .projection
                    .get_attempt_history(&run)
                    .into_iter()
                    .flat_map(|history| history.iter())
                    .any(|a| a.attempt_id() == attempt)
                {
                    return Err("attempt does not belong to local run".into());
                }
            }
        }
        Ok(CausationLink::new(Some(claim.task), claim.run, claim.attempt, None)?)
    }
}

pub fn causal_context(trace: &str, correlation: &str) -> Result<CausalContext> {
    Ok(CausalContext::new(TraceId::new(trace)?, CorrelationId::new(correlation)?))
}
/// AQ defines exactly three override fields. Unknown fields and excessive values reject.
pub fn child_override(bytes: &[u8]) -> Result<CausalOverride> {
    Ok(serde_json::from_value(strict_json(bytes)?)?)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskInputLocator {
    version: u32,
    artifact: ArtifactRef,
}
impl TaskInputLocator {
    pub fn new(artifact: ArtifactRef) -> Self {
        Self { version: 1, artifact }
    }
    pub fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }
    pub fn payload(&self) -> Result<TaskPayload> {
        let bytes = canonical_data(self)?;
        if bytes.len() > 4096 {
            return Err("task locator exceeds 4096 bytes".into());
        }
        Ok(TaskPayload::with_content_type(
            bytes,
            "application/vnd.worldinterface.task-input+json;version=1",
        ))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 4096 {
            return Err("task locator exceeds 4096 bytes".into());
        }
        let value: Self = serde_json::from_value(strict_json(bytes)?)?;
        if value.version != 1 {
            return Err("unsupported task locator version".into());
        }
        Ok(value)
    }
    /// Separate representation for signal/checkpoint/output APIs accepting DataRef.
    pub fn external(&self) -> Result<ExternalDataRef> {
        Ok(ExternalDataRef {
            scheme: DataScheme::new("wi-artifact-v1")?,
            locator: OpaqueRef::new(format!(
                "{}/{}",
                self.artifact.scope.as_str(),
                self.artifact.content.as_str()
            ))?,
            hash: ContentHash::new(
                HashAlgorithm::Sha256,
                hex::decode(self.artifact.content.as_str())?,
            )?,
            size_bytes: Some(self.artifact.size),
            content_type: None,
        })
    }
}
