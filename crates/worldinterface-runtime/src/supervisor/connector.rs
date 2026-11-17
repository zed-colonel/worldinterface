use super::Client;
use crate::{
    completion::{EvidenceBytes, PortObservation},
    connector::*,
    Result,
};
use actionqueue_executor_local::handler::CancellationToken;
use std::sync::Arc;
use worldinterface_domain::{
    effect::CommitObservation,
    resource::{Target, TargetDocument},
};
use worldinterface_protocol::*;
use worldinterface_worker::supervisor::Record;
use worldinterface_worker::supervisor::*;

/// Describe one host-supervisor lifecycle operation and its reconciliation bounds.
pub fn descriptor(operation: Operation) -> Result<ConnectorDescriptor> {
    Ok(ConnectorDescriptor {
        version: 1,
        connector: "wi:host-supervisor".to_string().try_into()?,
        implementation: ContentHash::of(b"wi:host-supervisor:1"),
        operation: operation.name().to_string().try_into()?,
        effect_class: EffectClass::Lifecycle,
        resource_namespace: "lifecycle".to_string().try_into()?,
        resource_grammar: "exact-instance:1".to_string().try_into()?,
        input_schema: "wi-supervisor-request:1".to_string().try_into()?,
        output_schema: "wi-supervisor-result:1".to_string().try_into()?,
        isolation_profile: "operator-supervisor:1".to_string().try_into()?,
        secret_refs: vec![],
        idempotency: IdempotencyClass::ExternallyKeyed,
        provider_key: Some(ProviderKeyMapping {
            version: 1,
            provider_scope: "wi:host-supervisor".to_string().try_into()?,
            parameter: "key".to_string().try_into()?,
        }),
        retry_after_noncommit: false,
        observes_commit: true,
        reconciliation: ReconciliationPolicy {
            max_observations: 32,
            backoff_millis: 10,
            timeout_millis: 60000,
            automatic: true,
        },
        cancellation_supported: false,
        evidence_schema: "wi-supervisor-record:1".to_string().try_into()?,
        max_evidence_bytes: MAX_FRAME_BYTES as u64,
    })
}
/// Trusted host composition pins a target, caller and exact operator-approved
/// request. The worker API cannot construct this connector or obtain its channel.
pub struct SupervisorConnector {
    client: Arc<Client>,
    descriptor: ConnectorDescriptor,
    session: Session,
    request: LifecycleRequest,
    resource: CanonicalResourceRef,
}
impl SupervisorConnector {
    pub fn new(
        client: Arc<Client>,
        session: Session,
        request: LifecycleRequest,
        document: TargetDocument,
    ) -> Result<Self> {
        if document.tenant != session.tenant
            || document.target
                != (Target::Lifecycle {
                    instance: request.instance.clone(),
                    target: request.target.clone(),
                    operation: request.operation.name().to_string().try_into()?,
                })
        {
            return Err("supervisor target document mismatch".into());
        }
        let resource = document.reference(request.generation.clone())?;
        let descriptor = descriptor(request.operation)?;
        validate_connector(&descriptor)?;
        Ok(Self { client, descriptor, session, request, resource })
    }
    fn command(
        &self,
        tenant: &OpaqueRef,
        execution: &ConnectorExecution,
        resource: &CanonicalResourceRef,
    ) -> Result<Command> {
        self.check_binding(&execution.binding)?;
        if tenant != &self.session.tenant || resource != &self.resource {
            return Err("supervisor execution scope mismatch".into());
        }
        let key = self
            .descriptor
            .provider_key
            .as_ref()
            .ok_or("missing supervisor key mapping")?
            .derive(tenant, &self.descriptor.operation, &execution.operation_identity)?;
        if execution.provider_key.as_ref() != Some(&key) {
            return Err("supervisor provider key mismatch".into());
        }
        Ok(Command { version: VERSION, key, request: self.request.clone() })
    }
    fn evidence(&self, record: &Record) -> Result<EvidenceBytes> {
        Ok(EvidenceBytes {
            schema: self.descriptor.evidence_schema.clone(),
            bytes: encode(record)?,
        })
    }
    fn validate_record(&self, record: &Record, command: &Command) -> Result<()> {
        if record.version != VERSION
            || record.command != *command
            || record.tenant != self.session.tenant
            || record.principal != self.session.principal
            || record.observation.evidence.len() > command.request.max_evidence_bytes as usize
        {
            return Err("supervisor response binding mismatch".into());
        }
        Ok(())
    }
}
impl Connector for SupervisorConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }
    fn validate_artifact(&self, schema: &ContentHash, bytes: &[u8]) -> Result<()> {
        if schema != &ContentHash::of(b"wi-supervisor-request:1")
            || decode::<LifecycleRequest>(bytes)? != self.request
        {
            return Err("supervisor request mismatch".into());
        }
        Ok(())
    }
    fn resolve(&self, r: ResolveRequest) -> Result<ResolveObservation> {
        if r.resource != self.resource {
            return Err("supervisor resolution mismatch".into());
        }
        Ok(ResolveObservation { resource: r.resource })
    }
    fn prepare(&self, r: &AuthorizedInvocation) -> PrepareOutcome {
        if self.command(&r.tenant, r.execution(), &r.resolved.resource).is_err()
            || decode::<LifecycleRequest>(&r.input).ok().as_ref() != Some(&self.request)
            || r.budget().timeout_millis > MAX_WALL_MS
            || r.deadline()
                .and_then(|d| d.epoch_millis().ok())
                .is_none_or(|d| d > self.request.deadline_ms)
        {
            PrepareOutcome::PermanentRejection
        } else {
            PrepareOutcome::Ready
        }
    }
    fn invoke(&self, r: PreparedInvocation, _: &CancellationToken) -> Result<PortObservation> {
        let command = self.command(&r.tenant, r.execution(), &r.resolved.resource)?;
        let mut evidence = vec![];
        if let Ok(Response::Record(record)) = self.client.request(&Request::Submit(command.clone()))
        {
            self.validate_record(&record, &command)?;
            evidence.push(self.evidence(&record)?);
        }
        // Even a retained terminal response is observed through authenticated
        // reconciliation. Journal insertion never counts as lifecycle commit.
        Ok(PortObservation {
            commit: CommitObservation::Unknown,
            outcome: ExecutionOutcome::Uncertain,
            outputs: vec![],
            evidence,
            boundary_record: None,
        })
    }
    fn reconcile(&self, r: ReconcileRequest) -> Result<ReconcileOutcome> {
        let command = self.command(&r.tenant, &r.execution, &r.resource)?;
        let query = Request::Inspect(Inspect {
            version: VERSION,
            key: command.key.clone(),
            command: command.digest()?,
        });
        let Ok(Response::Record(record)) = self.client.request(&query) else {
            return Ok(ReconcileOutcome::Unknown(vec![]));
        };
        if self.validate_record(&record, &command).is_err() {
            return Ok(ReconcileOutcome::Escalate(vec![]));
        }
        let evidence = vec![self.evidence(&record)?];
        Ok(match record.observation.state {
            State::Completed => ReconcileOutcome::Committed(evidence),
            State::Rejected => ReconcileOutcome::NotCommitted(evidence),
            State::Escalated => ReconcileOutcome::Escalate(evidence),
            _ => ReconcileOutcome::Unknown(evidence),
        })
    }
    fn cancel(&self, _: ReconcileRequest) -> Result<CancelObservation> {
        Ok(CancelObservation::Unsupported)
    }
}
