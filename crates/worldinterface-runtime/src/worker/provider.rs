//! Nonblocking admitted-provider boundary. Async implementations enqueue bounded messages;
//! observation polling is host maintenance, never an executor-local wait.
use crate::{worker_process::*, Result};
use worldinterface_protocol::ContentHash;
use worldinterface_worker::{
    artifacts::ArtifactLocatorV2,
    mapping::{ResolvedWorkerV1, WorkerRequestV1},
    ports::{Acknowledgment, Observation},
};

pub trait AdmittedProvider: Send + Sync {
    fn acknowledged(&self, _request: &WorkerRequestV1) -> Result<bool> {
        Ok(false)
    }
    /// Absent after recovery or when the provider cannot attest applied controls.
    fn controls(
        &self,
        _provider: &str,
        _run: &str,
    ) -> Result<Option<worldinterface_worker::controls::ControlEvidenceV1>> {
        Ok(None)
    }
    fn admit(&self, run: AuthorizedRun) -> Result<()>;
    fn submit(&self, request: &WorkerRequestV1) -> Result<Acknowledgment>;
    fn observe(&self, provider: &str, run: &str) -> Result<Vec<Observation>>;
    fn read(&self, locator: &ArtifactLocatorV2) -> Result<Vec<u8>>;
    fn manifest(&self, provider: &str, run: &str) -> Result<Vec<u8>>;
    fn evidence(&self, provider: &str, run: &str) -> Result<Option<CleanupReport>>;
    fn stop(&self, provider: &str, run: &str) -> Result<()>;
    fn recover_cleanup(
        &self,
        provider: &str,
        run: &str,
        operations: &[(WorkerRequestV1, ResolvedWorkerV1)],
    ) -> Result<Option<CleanupReport>>;
}
/// Provenance comes from trusted host composition, never the worker request.
#[derive(Clone)]
pub struct ProviderProvenance {
    pub implementation: ContentHash,
    pub helper: ContentHash,
    pub adapter: ContentHash,
    pub seam_simulation: bool,
}
impl AdmittedProvider for ProcessWorkerPort {
    fn controls(
        &self,
        provider: &str,
        run: &str,
    ) -> Result<Option<worldinterface_worker::controls::ControlEvidenceV1>> {
        Ok(Some(self.control_evidence(provider, run)?))
    }
    fn admit(&self, run: AuthorizedRun) -> Result<()> {
        Ok(ProcessWorkerPort::admit(self, run)?)
    }
    fn submit(&self, request: &WorkerRequestV1) -> Result<Acknowledgment> {
        Ok(self.submit_admitted(request)?)
    }
    fn observe(&self, provider: &str, run: &str) -> Result<Vec<Observation>> {
        Ok(self.observations_admitted(provider, run)?)
    }
    fn read(&self, locator: &ArtifactLocatorV2) -> Result<Vec<u8>> {
        Ok(self.read_scoped(locator)?)
    }
    fn manifest(&self, provider: &str, run: &str) -> Result<Vec<u8>> {
        Ok(self.collection_manifest(provider, run)?)
    }
    fn evidence(&self, provider: &str, run: &str) -> Result<Option<CleanupReport>> {
        Ok(ProcessWorkerPort::evidence(self, provider, run)?)
    }
    fn stop(&self, provider: &str, run: &str) -> Result<()> {
        Ok(self.stop_admitted(provider, run)?)
    }
    fn recover_cleanup(
        &self,
        provider: &str,
        run: &str,
        operations: &[(WorkerRequestV1, ResolvedWorkerV1)],
    ) -> Result<Option<CleanupReport>> {
        Ok(ProcessWorkerPort::recover_cleanup(self, provider, run, operations)?)
    }
}
