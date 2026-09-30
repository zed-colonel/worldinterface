//! Explicit mappings; opaque attribution never selects authority.
use crate::storage::StoreBundle;
use crate::Result;
use actionqueue_core::control::HostControlContext;
use actionqueue_core::{
    admission::{EnsureTaskOutcome, EnsureTaskRequest},
    bounded::{ContentHash, DataScheme, HashAlgorithm, OpaqueRef},
    causal::{CausalContext, CausalOverride, CausationLink},
    data_ref::ExternalDataRef,
    ids::{AttemptId, CorrelationId, RunId, TaskId, TraceId},
    task::task_spec::TaskPayload,
};
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
}
impl<'a, H: ExecutorHandler + 'static> QueueEngine<'a, H> {
    pub fn bootstrap(
        bundle: &'a StoreBundle,
        handler: H,
        host: HostControlContext,
    ) -> Result<Self> {
        let session = actionqueue_storage::store::open_store(
            &bundle.aq_path(),
            actionqueue_storage::store::OpenOptions::ReadOnly,
        )?;
        let store = QueueStoreId::from_session(&session);
        drop(session);
        let engine = ActionQueueEngine::new(
            RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
            handler,
        )
        .bootstrap()?
        .with_host(host);
        if bundle.aq_store_id()? != store.as_uuid() {
            return Err("AQ store changed during engine bootstrap".into());
        }
        Ok(Self { bundle, store, engine })
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
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let session = actionqueue_storage::store::open_store(
            path,
            actionqueue_storage::store::OpenOptions::ReadOnly,
        )?;
        let recovered = actionqueue_storage::recovery::bootstrap::recover_read_only(
            &session,
            actionqueue_storage::wal::repair::RepairPolicy::Strict,
        )?;
        Ok(Self { session, projection: recovered.projection })
    }
    /// Checks ingress task and signal references through AQ's recovered public models.
    /// External checkpoint artifact closure remains a separate G4 obligation.
    pub fn validate_artifacts(&self, wi: &worldinterface_storage::StoreRoot) -> Result<()> {
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
        for run in self.projection.run_instances() {
            for attempt in self.projection.get_attempt_history(&run.id()).unwrap_or(&[]) {
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
                    }
                }
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
                    .unwrap_or(&[])
                    .iter()
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
