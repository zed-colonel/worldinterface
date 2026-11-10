//! Evaluation is limited to an exact committed packet validated by the trusted host.
//! Review and merge establish durable acceptance; execution grants no integration authority.
use crate::{authority::EvidenceAuthority, worker_process::Task, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path},
};
use worldinterface_domain::authorization::AuthorizationSnapshot;
use worldinterface_protocol::{ContentHash, EffectSubmission, EvidenceRef, OpaqueRef, Timestamp};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationManifest {
    pub version: u32,
    pub profile: String,
    pub environment: ContentHash,
    pub files: BTreeMap<String, ContentHash>,
    pub tasks: BTreeSet<ContentHash>,
    pub cases: BTreeSet<String>,
    pub operations: BTreeSet<String>,
    pub max_wall_ms: u64,
    pub max_output_bytes: u64,
    pub nonsensitive_only: bool,
    pub production_credentials: bool,
    pub ordinary_use: bool,
}
impl EvaluationManifest {
    pub fn digest(&self) -> Result<ContentHash> {
        Ok(worldinterface_protocol::data_digest(self)?)
    }
    pub fn verify_sources(&self, root: &Path) -> Result<()> {
        if self.version != 1
            || self.profile != worldinterface_worker::profile::PROFILE_ID
            || self.files.is_empty()
            || self.tasks.is_empty()
            || self.cases.is_empty()
            || self.operations.is_empty()
            || self.max_wall_ms != 15_000
            || self.max_output_bytes != 65_536
            || !self.nonsensitive_only
            || self.production_credentials
            || self.ordinary_use
        {
            return Err("invalid evaluation scope".into());
        }
        for (name, expected) in &self.files {
            let path = Path::new(name);
            if path.components().any(|c| !matches!(c, Component::Normal(_)))
                || path.components().count() == 0
            {
                return Err("invalid evaluation source path".into());
            }
            let mut current = root.to_path_buf();
            for component in path.components() {
                current.push(component);
                if std::fs::symlink_metadata(&current)?.file_type().is_symlink() {
                    return Err("evaluation source link".into());
                }
            }
            if ContentHash::of(&std::fs::read(&current)?) != *expected {
                return Err("evaluation source drift".into());
            }
        }
        if self.operations.iter().any(|op| {
            !["provision", "start", "inspect", "terminate", "collect_artifacts", "release"]
                .contains(&op.as_str())
        }) {
            return Err("unavailable evaluation operation".into());
        }
        Ok(())
    }
}
/// Authenticated host composition supplies this store independently of request JSON.
/// Implementations must validate the complete current packet against committed bytes.
/// Reviewed merge is a workflow boundary, not an assertion made by runtime tests.
pub trait TrustedApprovals: Send + Sync {
    fn accepted(&self, manifest: &EvaluationManifest, source_root: &Path) -> Result<bool>;
    /// Authenticate the exact catalog binding from host registration provenance.
    /// A request, record file or successful evaluation cannot install itself.
    fn registered(
        &self,
        _record: &worldinterface_capability::process_catalog::ProcessRegistrationRecord,
        _now_ms: u64,
    ) -> Result<bool> {
        Ok(false)
    }
    /// Authenticate producer proof, results, reviewer evidence and operator decision
    /// against independent host provenance. Repository JSON alone is insufficient.
    fn qualified(
        &self,
        _record: &worldinterface_capability::process_catalog::OrdinaryQualificationRecord,
        _now_ms: u64,
    ) -> Result<bool> {
        Ok(false)
    }
}
/// No Deserialize implementation: only successful verification produces this capability.
pub struct VerifiedEvaluation {
    digest: ContentHash,
}
impl VerifiedEvaluation {
    pub fn digest(&self) -> &ContentHash {
        &self.digest
    }
}
pub struct EvaluationVerifier<S> {
    pub manifest: EvaluationManifest,
    pub trusted: S,
    pub source_root: std::path::PathBuf,
}
impl<S: TrustedApprovals> EvaluationVerifier<S> {
    pub fn verify(&self, _now_ms: u64) -> Result<VerifiedEvaluation> {
        self.manifest.verify_sources(&self.source_root)?;
        let digest = self.manifest.digest()?;
        if !self.trusted.accepted(&self.manifest, &self.source_root)? {
            return Err("evaluation packet unavailable".into());
        }
        Ok(VerifiedEvaluation { digest })
    }
    pub fn authorize(
        &self,
        now_ms: u64,
        case: &str,
        operation: &str,
        task: &Task,
        environment: &ContentHash,
    ) -> Result<VerifiedEvaluation> {
        let verified = self.verify(now_ms)?;
        if !self.manifest.cases.contains(case)
            || !self.manifest.operations.contains(operation)
            || !self.manifest.tasks.contains(&worldinterface_protocol::data_digest(task)?)
            || self.manifest.environment != *environment
        {
            return Err("evaluation request outside committed scope".into());
        }
        Ok(verified)
    }
}
/// Wrap the ordinary WI evidence authority so admission and dispatch both reverify packet integrity.
/// Base evidence still authenticates caller, grants, artifacts and exact realization.
#[derive(Clone)]
pub struct EvaluationAuthority<A, S, C> {
    pub base: A,
    pub evaluation: std::sync::Arc<EvaluationVerifier<S>>,
    pub clock: C,
}
impl<A: EvidenceAuthority, S: TrustedApprovals, C: crate::worker_process::Clock> EvidenceAuthority
    for EvaluationAuthority<A, S, C>
{
    fn current_snapshot(
        &self,
        tenant: &OpaqueRef,
        submission: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        let snapshot = self.base.current_snapshot(tenant, submission)?;
        if let Some(recovery) = &snapshot.recovery {
            if !["inspect", "terminate"].contains(&recovery.operation.as_str())
                || snapshot.fixture.is_some()
                || snapshot.qualification.is_some()
            {
                return Err("invalid recovery scope".into());
            }
        } else if let Some(worker) = &snapshot.worker {
            self.evaluation.verify(self.clock.unix_ms())?;
            if snapshot.qualification.is_some()
                || snapshot.fixture.is_none()
                || worker.profile.as_str() != self.evaluation.manifest.profile
                || worker.environment != self.evaluation.manifest.environment
                || !self.evaluation.manifest.operations.contains(worker.operation.as_str())
            {
                return Err("evaluation worker scope mismatch".into());
            }
        }
        Ok(snapshot)
    }
    fn verify(
        &self,
        tenant: &OpaqueRef,
        evidence: &EvidenceRef,
        at: &Timestamp,
    ) -> Result<ContentHash> {
        self.base.verify(tenant, evidence, at)
    }
}
