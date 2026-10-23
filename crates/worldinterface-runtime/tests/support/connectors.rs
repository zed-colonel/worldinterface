#![allow(dead_code)]
use actionqueue_executor_local::handler::CancellationToken;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use worldinterface_domain::{
    authorization::AuthorizationSnapshot, effect::CommitObservation, ingress::digest,
    resource::Target,
};
use worldinterface_protocol::*;
use worldinterface_runtime::{
    authority::{EvidenceAuthority, TrustedClock},
    completion::{EvidenceBytes, PortObservation},
    connector::*,
    Result,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Commit,
    Ambiguous,
    NonCommit,
    Terminal,
    TerminalCommitted,
    TerminalUnknown,
    PrepareReject,
    PrepareTransient,
    Widen,
    Unknown,
    Possible,
    Escalate,
}
#[derive(Clone)]
pub struct DeterministicConnector {
    pub descriptor: ConnectorDescriptor,
    pub db: PathBuf,
    pub mode: Mode,
    pub outputs_override: Option<Vec<Vec<u8>>>,
    pub after_prepare: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_commit: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_invoke: Option<Arc<dyn Fn() + Send + Sync>>,
    pub cancel_observation: Option<CancelObservation>,
    pub cancel_calls: Arc<AtomicU64>,
    pub invocation_calls: Arc<AtomicU64>,
}
impl DeterministicConnector {
    pub fn new(db: PathBuf, class: IdempotencyClass, mode: Mode) -> Self {
        let keyed = class == IdempotencyClass::ExternallyKeyed;
        Self {
            db,
            mode,
            outputs_override: None,
            after_prepare: None,
            after_commit: None,
            after_invoke: None,
            cancel_observation: Some(CancelObservation::Unsupported),
            cancel_calls: Arc::new(AtomicU64::new(0)),
            invocation_calls: Arc::new(AtomicU64::new(0)),
            descriptor: ConnectorDescriptor {
                version: 1,
                connector: "deterministic:1".to_string().try_into().unwrap(),
                implementation: ContentHash::of(b"deterministic:1"),
                operation: "accept".to_string().try_into().unwrap(),
                effect_class: EffectClass::Control,
                resource_namespace: "connector".to_string().try_into().unwrap(),
                resource_grammar: "exact-provider-resource:1".to_string().try_into().unwrap(),
                input_schema: "input:1".to_string().try_into().unwrap(),
                output_schema: "output:1".to_string().try_into().unwrap(),
                isolation_profile: "deterministic-local:1".to_string().try_into().unwrap(),
                secret_refs: vec!["broker-ref:provider".to_string().try_into().unwrap()],
                idempotency: class,
                provider_key: keyed.then(|| ProviderKeyMapping {
                    version: 1,
                    provider_scope: "provider:account-1".to_string().try_into().unwrap(),
                    parameter: "idempotency-key".to_string().try_into().unwrap(),
                }),
                retry_after_noncommit: true,
                observes_commit: true,
                reconciliation: ReconciliationPolicy {
                    automatic: true,
                    max_observations: 3,
                    backoff_millis: 1000,
                    timeout_millis: 120_000,
                },
                cancellation_supported: false,
                evidence_schema: "provider-observation:1".to_string().try_into().unwrap(),
                max_evidence_bytes: 4096,
            },
        }
    }
    fn connection(&self) -> rusqlite::Result<rusqlite::Connection> {
        let c = rusqlite::Connection::open(&self.db)?;
        c.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS commits(identity TEXT PRIMARY KEY, operation TEXT NOT NULL);")?;
        Ok(c)
    }
    pub fn commits(&self) -> usize {
        self.connection()
            .unwrap()
            .query_row("SELECT count(*) FROM commits", [], |r| r.get(0))
            .unwrap()
    }
}
pub fn evidence(bytes: &[u8]) -> EvidenceBytes {
    EvidenceBytes {
        schema: "provider-observation:1".to_string().try_into().unwrap(),
        bytes: bytes.to_vec(),
    }
}
impl Connector for DeterministicConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }
    fn validate_artifact(&self, schema: &ContentHash, bytes: &[u8]) -> Result<()> {
        if schema == &ContentHash::of(b"output:1")
            && self.outputs_override.as_ref().is_some_and(|v| v.iter().any(|b| b == bytes))
        {
            return Ok(());
        }
        if (schema == &ContentHash::of(b"input:1") && bytes == b"input")
            || (schema == &ContentHash::of(b"output:1") && bytes == b"output")
        {
            Ok(())
        } else {
            Err("fixture artifact schema mismatch".into())
        }
    }
    fn resolve(&self, r: ResolveRequest) -> Result<ResolveObservation> {
        let mut resource = r.resource;
        if self.mode == Mode::Widen {
            resource.resource = "other-resource".to_string().try_into()?;
        }
        Ok(ResolveObservation { resource })
    }
    fn prepare(&self, _: &AuthorizedInvocation) -> PrepareOutcome {
        if let Some(f) = &self.after_prepare {
            f();
        }
        match self.mode {
            Mode::PrepareReject => PrepareOutcome::PermanentRejection,
            Mode::PrepareTransient => PrepareOutcome::TransientFailure,
            _ => PrepareOutcome::Ready,
        }
    }
    fn invoke(
        &self,
        r: PreparedInvocation,
        cancellation: &CancellationToken,
    ) -> Result<PortObservation> {
        self.invocation_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(r.input, b"input");
        assert_eq!(r.execution().binding.descriptor, self.descriptor);
        let (commit, outcome) = match self.mode {
            Mode::NonCommit => {
                (CommitObservation::ConfirmedNotCommitted, ExecutionOutcome::FailedBeforeCommit)
            }
            Mode::Terminal => {
                (CommitObservation::ConfirmedNotCommitted, ExecutionOutcome::TerminalFailure)
            }
            Mode::TerminalUnknown => {
                (CommitObservation::Unknown, ExecutionOutcome::TerminalFailure)
            }
            Mode::Unknown | Mode::Escalate => {
                (CommitObservation::Unknown, ExecutionOutcome::Uncertain)
            }
            Mode::Possible => (CommitObservation::Possible, ExecutionOutcome::Uncertain),
            _ => {
                let identity = if self.descriptor.idempotency == IdempotencyClass::NonRepeatable {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    r.execution()
                        .provider_key
                        .as_ref()
                        .unwrap_or(&r.execution().operation_identity)
                        .as_str()
                        .to_owned()
                };
                self.connection()?.execute(
                    "INSERT OR IGNORE INTO commits VALUES(?,?)",
                    [identity, r.execution().operation_identity.as_str().to_owned()],
                )?;
                if let Some(f) = &self.after_commit {
                    f();
                }
                if self.mode == Mode::TerminalCommitted {
                    (CommitObservation::ConfirmedCommitted, ExecutionOutcome::TerminalFailure)
                } else if self.mode == Mode::Ambiguous || cancellation.is_cancelled() {
                    (CommitObservation::Unknown, ExecutionOutcome::Uncertain)
                } else {
                    (CommitObservation::ConfirmedCommitted, ExecutionOutcome::Succeeded)
                }
            }
        };
        if let Some(f) = &self.after_invoke {
            f();
        }
        let success = outcome == ExecutionOutcome::Succeeded;
        Ok(PortObservation {
            commit,
            outcome,
            outputs: if success {
                self.outputs_override.clone().unwrap_or_else(|| vec![b"output".to_vec()])
            } else {
                vec![]
            },
            evidence: vec![evidence(b"provider result")],
            boundary_record: success.then(|| evidence(b"accepted boundary")),
        })
    }
    fn reconcile(&self, r: ReconcileRequest) -> Result<ReconcileOutcome> {
        if self.mode == Mode::Unknown {
            return Ok(ReconcileOutcome::Unknown(vec![evidence(b"still unknown")]));
        }
        if self.mode == Mode::Escalate {
            return Ok(ReconcileOutcome::Escalate(vec![evidence(b"operator required")]));
        }
        let found: bool = if let Some(key) = &r.execution.provider_key {
            self.connection()?.query_row(
                "SELECT EXISTS(SELECT 1 FROM commits WHERE identity=? AND operation=?)",
                [key.as_str(), r.execution.operation_identity.as_str()],
                |row| row.get(0),
            )?
        } else {
            self.connection()?.query_row(
                "SELECT EXISTS(SELECT 1 FROM commits WHERE operation=?)",
                [r.execution.operation_identity.as_str()],
                |row| row.get(0),
            )?
        };
        if found {
            Ok(ReconcileOutcome::Committed(vec![evidence(b"authoritative acceptance query")]))
        } else {
            Ok(ReconcileOutcome::NotCommitted(vec![evidence(b"authoritative absence query")]))
        }
    }
    fn cancel(&self, _: ReconcileRequest) -> Result<CancelObservation> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        self.cancel_observation.ok_or_else(|| "temporary cancellation failure".into())
    }
}
#[derive(Clone)]
pub struct Clock(pub Arc<AtomicU64>);
impl Clock {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
                as u64,
        )))
    }
    pub fn advance(&self, n: u64) {
        self.0.fetch_add(n, Ordering::SeqCst);
    }
}
impl TrustedClock for Clock {
    fn now(&self) -> Result<Timestamp> {
        Ok(Timestamp::from_epoch_millis(self.0.load(Ordering::SeqCst))?)
    }
}
#[derive(Clone)]
pub struct Evidence(pub AuthorizationSnapshot);
impl EvidenceAuthority for Evidence {
    fn current_snapshot(
        &self,
        _: &OpaqueRef,
        _: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        Ok(self.0.clone())
    }
    fn verify(&self, _: &OpaqueRef, e: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        Ok(if e == &self.0.facts[0].evidence {
            self.0.facts[0].attested_document.clone()
        } else {
            ContentHash::of(b"verified")
        })
    }
}
pub fn fixture(d: &ConnectorDescriptor) -> (EffectSubmission, AuthorizationSnapshot) {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/egress/fixture.json"
    ))
    .unwrap();
    let mut submission: EffectSubmission =
        serde_json::from_value(data["submission"].clone()).unwrap();
    let mut s: AuthorizationSnapshot = serde_json::from_value(data["snapshot"].clone()).unwrap();
    let expires = Timestamp::from_epoch_millis(
        Clock::new().now().unwrap().epoch_millis().unwrap() + 86_400_000,
    )
    .unwrap();
    s.policy.expires_at = expires.clone();
    s.policy.retry_after_noncommit = true;
    for b in &mut s.bindings {
        b.expires_at = expires.clone();
    }
    for g in &mut s.grants {
        g.expires_at = expires.clone();
    }
    for f in &mut s.facts {
        f.valid_until = expires.clone();
    }
    submission.resource.namespace = "connector".to_string().try_into().unwrap();
    s.worker = None;
    s.fixture = None;
    s.qualification = None;
    let registry = ConnectorRegistry::new(vec![d.clone()]).unwrap();
    let binding = registry.bindings().unwrap().remove(0);
    s.registry.entries[0].namespace = d.resource_namespace.clone();
    s.registry.entries[0].connector = d.connector.clone();
    s.registry.entries[0].descriptor = binding.descriptor_hash.clone();
    s.registry.entries[0].connector_binding = Some(binding);
    s.facts[0].selector = submission.resource.clone();
    s.facts[0].registry = digest(&s.registry).unwrap();
    s.facts[0].document.target = Target::Connector {
        provider: d.connector.clone(),
        resource: "provider-resource:1".to_string().try_into().unwrap(),
        operation: d.operation.clone(),
    };
    s.facts[0].attested_document = digest(&s.facts[0].document).unwrap();
    s.policy.targets = [s.facts[0].attested_document.clone()].into();
    let demand = digest(&submission).unwrap();
    for b in &mut s.bindings {
        b.principal_authority = [demand.clone()].into();
        b.capacity_authority = [demand.clone()].into();
    }
    s.policy.platform = [demand.clone()].into();
    s.policy.resource = [demand.clone()].into();
    s.policy.information_use = [demand.clone()].into();
    for g in &mut s.grants {
        g.authority = [demand.clone()].into();
        g.max_uses = 10;
    }
    (submission, s)
}
