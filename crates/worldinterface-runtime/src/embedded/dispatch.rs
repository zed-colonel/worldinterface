use super::*;
use crate::connector::*;
use actionqueue_executor_local::handler::CancellationToken;

/// One immutable registry for all supported operations. Selection comes from an
/// executor-created envelope, never a separate consumer-controlled operation tag.
pub(super) struct ProcessDispatcher<S> {
    connectors: Vec<crate::worker::connector::WorkerConnector<S>>,
    registry: ConnectorRegistry,
}
impl<S: TrustedWorkerBuild + 'static> ProcessDispatcher<S> {
    pub fn new(runtime: &Arc<WorkerRuntime<S>>) -> Result<Self> {
        let connectors = crate::worker::admission::OPERATIONS
            .iter()
            .map(|op| runtime.connector(*op))
            .collect::<Result<Vec<_>>>()?;
        let registry =
            ConnectorRegistry::new(connectors.iter().map(|c| c.descriptor().clone()).collect())?;
        Ok(Self { connectors, registry })
    }
    fn select(
        &self,
        b: &ConnectorBinding,
    ) -> Result<&crate::worker::connector::WorkerConnector<S>> {
        self.check_binding(b)?;
        self.connectors
            .iter()
            .find(|c| c.descriptor() == &b.descriptor)
            .ok_or_else(|| "embedded connector absent".into())
    }
}
impl<S: TrustedWorkerBuild + 'static> Connector for ProcessDispatcher<S> {
    fn descriptor(&self) -> &ConnectorDescriptor {
        self.connectors[0].descriptor()
    }
    fn registry_digest(&self) -> Result<ContentHash> {
        Ok(self.registry.digest().clone())
    }
    fn check_binding(&self, b: &ConnectorBinding) -> Result<()> {
        b.validate()?;
        if !self.registry.bindings()?.contains(b) {
            return Err("embedded installed connector drift".into());
        }
        Ok(())
    }
    fn validate_artifact(&self, s: &ContentHash, b: &[u8]) -> Result<()> {
        self.connectors[0].validate_artifact(s, b)
    }
    fn resolve(&self, r: ResolveRequest) -> Result<ResolveObservation> {
        Ok(ResolveObservation { resource: r.resource })
    }
    fn prepare(&self, r: &AuthorizedInvocation) -> PrepareOutcome {
        self.select(&r.execution().binding)
            .map(|c| c.prepare(r))
            .unwrap_or(PrepareOutcome::PermanentRejection)
    }
    fn invoke(
        &self,
        r: PreparedInvocation,
        token: &CancellationToken,
    ) -> Result<crate::completion::PortObservation> {
        self.select(&r.execution().binding)?.invoke(r, token)
    }
    fn check_recovery(&self, r: &ReconcileRequest) -> Result<()> {
        self.select(&r.execution.binding)?.check_recovery(r)
    }
    fn reconcile(&self, r: ReconcileRequest) -> Result<ReconcileOutcome> {
        self.select(&r.execution.binding)?.reconcile(r)
    }
    fn cancel(&self, r: ReconcileRequest) -> Result<CancelObservation> {
        self.select(&r.execution.binding)?.cancel(r)
    }
}

pub(super) type Dispatcher<S, V, R, C> = crate::reconciliation::ReconciliationRouter<
    crate::reconciliation::ReconciliationHandler<Arc<ProcessDispatcher<S>>, SharedClock<C>>,
    crate::flow_node::FlowAttemptHandler<
        Arc<ProcessDispatcher<S>>,
        ManagedEvidence<V, C>,
        R,
        SharedClock<C>,
    >,
>;
