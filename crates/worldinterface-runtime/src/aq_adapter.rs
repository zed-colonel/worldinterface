//! Explicit mappings; opaque attribution never selects authority.
use crate::Result;
use actionqueue_core::{
    bounded::{ContentHash, DataScheme, HashAlgorithm, OpaqueRef},
    causal::{CausalContext, CausalOverride, CausationLink},
    data_ref::ExternalDataRef,
    ids::{AttemptId, CorrelationId, RunId, TaskId, TraceId},
    task::task_spec::TaskPayload,
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
    /// Checks all G1 input/output references through AQ's recovered public models.
    /// G3/G4 must extend this closure before introducing signal/checkpoint adapters.
    pub fn validate_artifacts(&self, wi: &worldinterface_storage::StoreRoot) -> Result<()> {
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
