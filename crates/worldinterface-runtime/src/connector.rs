//! Bounded connector SPI. Only the executor can construct invocation envelopes.
use crate::{
    completion::{BoundExecution, EvidenceBytes, PortObservation},
    Result,
};
use actionqueue_executor_local::handler::CancellationToken;
use std::{ops::Deref, sync::Arc};
pub use worldinterface_capability::{validate_connector, ConnectorRegistry};
use worldinterface_domain::effect::CommitObservation;
use worldinterface_protocol::*;

pub struct AuthorizedInvocation {
    bound: BoundExecution,
    execution: ConnectorExecution,
    deadline: Option<Timestamp>,
    budget: EffectBudget,
}
impl AuthorizedInvocation {
    pub(crate) fn new(
        bound: BoundExecution,
        execution: ConnectorExecution,
        deadline: Option<Timestamp>,
        budget: EffectBudget,
    ) -> Self {
        Self { bound, execution, deadline, budget }
    }
    pub fn execution(&self) -> &ConnectorExecution {
        &self.execution
    }
    pub fn deadline(&self) -> Option<&Timestamp> {
        self.deadline.as_ref()
    }
    pub fn budget(&self) -> &EffectBudget {
        &self.budget
    }
}
impl Deref for AuthorizedInvocation {
    type Target = BoundExecution;
    fn deref(&self) -> &Self::Target {
        &self.bound
    }
}
pub struct PreparedInvocation {
    authorized: AuthorizedInvocation,
}
impl PreparedInvocation {
    pub(crate) fn new(authorized: AuthorizedInvocation) -> Self {
        Self { authorized }
    }
    pub fn execution(&self) -> &ConnectorExecution {
        self.authorized.execution()
    }
    pub fn deadline(&self) -> Option<&Timestamp> {
        self.authorized.deadline()
    }
    pub fn budget(&self) -> &EffectBudget {
        self.authorized.budget()
    }
}
impl Deref for PreparedInvocation {
    type Target = BoundExecution;
    fn deref(&self) -> &Self::Target {
        &self.authorized.bound
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrepareOutcome {
    Ready,
    TransientFailure,
    PermanentRejection,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelObservation {
    Unsupported,
    Requested,
    ConfirmedNotCommitted,
}
pub struct ResolveRequest {
    pub selector: ResourceSelector,
    pub resource: CanonicalResourceRef,
}
pub struct ResolveObservation {
    pub resource: CanonicalResourceRef,
}
/// A reconciliation request carries observation authority only, never an invocation envelope.
pub struct ReconcileRequest {
    pub tenant: OpaqueRef,
    pub intent: EffectIntentId,
    pub execution: ConnectorExecution,
    pub resource: CanonicalResourceRef,
}
pub enum ReconcileOutcome {
    Committed(Vec<EvidenceBytes>),
    NotCommitted(Vec<EvidenceBytes>),
    Unknown(Vec<EvidenceBytes>),
    Escalate(Vec<EvidenceBytes>),
}
/// Invoke observations cannot change resource, key, operation or authorization.
pub trait Connector: Send + Sync {
    fn descriptor(&self) -> &ConnectorDescriptor;
    /// Validate the exact persisted binding against the immutable installed
    /// registry. Dispatchers override this without weakening either identity.
    fn check_binding(&self, binding: &ConnectorBinding) -> Result<()> {
        binding.validate()?;
        validate_connector(self.descriptor())?;
        if self.registry_digest()? != binding.registry_digest
            || self.descriptor() != &binding.descriptor
        {
            return Err("installed connector drift".into());
        }
        Ok(())
    }
    /// Host composition may bind a larger immutable snapshot. Singleton registration is the default.
    fn registry_digest(&self) -> Result<ContentHash> {
        Ok(ConnectorRegistry::new(vec![self.descriptor().clone()])?.digest().clone())
    }
    /// Validate an opaque artifact against an installed pinned schema implementation.
    /// Connectors without this capability reject opaque Flow data before invocation.
    fn validate_artifact(&self, _schema: &ContentHash, _bytes: &[u8]) -> Result<()> {
        Err("connector does not validate pinned artifact schemas".into())
    }
    fn resolve(&self, request: ResolveRequest) -> Result<ResolveObservation>;
    /// Must not dispatch the consequential operation or consume its provider key.
    fn prepare(&self, request: &AuthorizedInvocation) -> PrepareOutcome;
    fn invoke(
        &self,
        request: PreparedInvocation,
        cancellation: &CancellationToken,
    ) -> Result<PortObservation>;
    /// Trusted recovery may authenticate an original execution and the current
    /// recovery implementation. This hook is never used for prepare or invoke.
    fn check_recovery(&self, _request: &ReconcileRequest) -> Result<()> {
        Err("recovery binding unavailable".into())
    }
    fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileOutcome>;
    /// Called under cancellation only when invoke reported Unknown or Possible
    /// commit. Definitive commit or noncommit observations skip this phase.
    fn cancel(&self, request: ReconcileRequest) -> Result<CancelObservation>;
}
impl<T: Connector + ?Sized> Connector for Arc<T> {
    fn check_binding(&self, b: &ConnectorBinding) -> Result<()> {
        (**self).check_binding(b)
    }
    fn descriptor(&self) -> &ConnectorDescriptor {
        (**self).descriptor()
    }
    fn registry_digest(&self) -> Result<ContentHash> {
        (**self).registry_digest()
    }
    fn validate_artifact(&self, schema: &ContentHash, bytes: &[u8]) -> Result<()> {
        (**self).validate_artifact(schema, bytes)
    }
    fn resolve(&self, r: ResolveRequest) -> Result<ResolveObservation> {
        (**self).resolve(r)
    }
    fn prepare(&self, r: &AuthorizedInvocation) -> PrepareOutcome {
        (**self).prepare(r)
    }
    fn invoke(&self, r: PreparedInvocation, c: &CancellationToken) -> Result<PortObservation> {
        (**self).invoke(r, c)
    }
    fn check_recovery(&self, r: &ReconcileRequest) -> Result<()> {
        (**self).check_recovery(r)
    }
    fn reconcile(&self, r: ReconcileRequest) -> Result<ReconcileOutcome> {
        (**self).reconcile(r)
    }
    fn cancel(&self, r: ReconcileRequest) -> Result<CancelObservation> {
        (**self).cancel(r)
    }
}
pub use worldinterface_protocol::ExecutorClassification;
impl PortObservation {
    pub fn classification(&self) -> ExecutorClassification {
        match (&self.commit, &self.outcome) {
            (CommitObservation::ProvenImpossible, _) => ExecutorClassification::PreDispatchFailure,
            (_, ExecutionOutcome::TerminalFailure) => ExecutorClassification::TerminalFailure,
            (CommitObservation::ConfirmedNotCommitted, _) => {
                ExecutorClassification::ConfirmedNotCommitted
            }
            (CommitObservation::ConfirmedCommitted, _) => ExecutorClassification::Committed,
            _ => ExecutorClassification::AmbiguousCommit,
        }
    }
}
pub(crate) fn check_installed(
    connector: &impl Connector,
    execution: &ConnectorExecution,
) -> Result<()> {
    connector.check_binding(&execution.binding)
}

/// An implementation can inspect an envelope but cannot substitute its authority.
/// ```compile_fail
/// use worldinterface_runtime::connector::PreparedInvocation;
/// use worldinterface_protocol::ContentHash;
/// fn substitute(mut request: PreparedInvocation) {
///     request.authorization = ContentHash::of(b"forged authority");
/// }
/// ```
/// ```compile_fail
/// use worldinterface_runtime::connector::PreparedInvocation;
/// fn forge() { let request = PreparedInvocation {}; }
/// ```
pub struct InvocationEnvelopeInvariant;
