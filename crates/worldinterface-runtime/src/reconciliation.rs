//! Durable observation tasks. AQ owns wait/lease fencing; WI owns the case budget.
use crate::{
    aq_adapter::QueueEngine, authority::TrustedClock, connector::*, storage::StoreBundle, Result,
};
use actionqueue_core::{
    admission::EnsureTaskRequest,
    bounded::{BoundedCode, BoundedError, BoundedMessage},
    causal::CausalContext,
    continuation::{
        SignalEligibility, SignalFilter, WaitDeadline, WaitMatchPolicy, WaitSpec, WaitTimeoutPolicy,
    },
    disposition::AttemptDisposition,
    ids::{AdmissionKey, CorrelationId, TaskId, TraceId, WaitId},
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        safety::SafetyLevel,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    reconciliation::{CaseAction, CaseStatus, ReconciliationCase},
};
use worldinterface_protocol::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseLocator {
    version: u32,
    tenant: OpaqueRef,
    intent: EffectIntentId,
    case: OpaqueRef,
}
impl CaseLocator {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 4096 {
            return Err("case locator bound".into());
        }
        let value: Self = serde_json::from_value(strict_json(bytes)?)?;
        if value.version != 1 {
            return Err("case locator version".into());
        }
        Ok(value)
    }
}
pub(crate) fn request(case: &ReconciliationCase) -> Result<EnsureTaskRequest> {
    let hash = ContentHash::of(&canonical_data(&(
        "wi-reconciliation-task:1",
        &case.id,
        &case.queue_store,
    ))?);
    let mut uuid = [0u8; 16];
    uuid.copy_from_slice(&hex::decode(hash.as_str())?[..16]);
    let task = TaskId::from_uuid(uuid::Uuid::from_bytes(uuid));
    let locator = CaseLocator {
        version: 1,
        tenant: case.tenant.clone(),
        intent: case.intent.clone(),
        case: case.id.clone(),
    };
    let mut constraints = TaskConstraints::new(3, Some(30), None)?;
    constraints.set_safety_level(SafetyLevel::Idempotent);
    let spec = TaskSpec::new(
        task,
        TaskPayload::with_content_type(
            canonical_data(&locator)?,
            "application/vnd.worldinterface.reconciliation+json;version=1",
        ),
        RunPolicy::Once,
        constraints,
        TaskMetadata::default(),
    )?;
    Ok(EnsureTaskRequest::new(
        AdmissionKey::new(case.id.as_str())?,
        spec,
        vec![],
        CausalContext::new(TraceId::new(case.id.as_str())?, CorrelationId::new(case.id.as_str())?),
        None,
    )?)
}
fn latest(
    root: &worldinterface_storage::StoreRoot,
    caller: &AuthenticatedCaller,
    case: &ReconciliationCase,
) -> Result<ReconciliationCase> {
    root.reconciliation_cases(caller, &case.intent)?
        .into_iter()
        .find(|c| c.id == case.id)
        .ok_or_else(|| "missing case".into())
}
/// Retry the persisted request after any uncertain admission acknowledgment. No replacement generations.
pub fn deliver<H: ExecutorHandler + 'static>(
    queue: &mut QueueEngine<'_, H>,
    caller: &AuthenticatedCaller,
    clock: &impl TrustedClock,
    intent: &EffectIntentId,
) -> Result<()> {
    let cases = queue.wi().reconciliation_cases(caller, intent)?;
    for mut case in cases {
        if case.status.terminal() || (case.admission_sequence.is_some() && case.aq_run.is_some()) {
            continue;
        }
        if case.queue_store.as_str() != queue.store_id().to_string() {
            return Err("reconciliation AQ store drift".into());
        }
        if case.admission_request.is_none() {
            let bytes = serde_json::to_vec(&request(&case)?)?;
            queue.wi().reconciliation_apply(
                caller,
                clock.now()?,
                &case,
                CaseAction::PrepareAdmission { request: bytes },
            )?;
            case = latest(queue.wi(), caller, &case)?;
        }
        let bytes = case.admission_request.as_ref().ok_or("missing reconciliation request")?;
        let prepared: EnsureTaskRequest = serde_json::from_slice(bytes)?;
        if serde_json::to_vec(&request(&case)?)? != *bytes {
            return Err("reconciliation request binding".into());
        }
        let outcome = queue.ensure_task(prepared)?;
        let runs = queue.engine().projection().run_ids_for_task(outcome.task_id());
        if runs.len() != 1 {
            return Err("reconciliation requires one immutable AQ run".into());
        }
        let run = runs[0].to_string().try_into()?;
        queue.wi().reconciliation_apply(
            caller,
            clock.now()?,
            &case,
            CaseAction::Acknowledge { sequence: outcome.sequence(), run: Some(run) },
        )?;
    }
    Ok(())
}
fn diagnostic(bytes: &'static [u8]) -> crate::completion::EvidenceBytes {
    crate::completion::EvidenceBytes {
        schema: "wi-reconciliation-runtime:1".to_string().try_into().expect("static schema"),
        bytes: bytes.to_vec(),
    }
}
pub struct ReconciliationHandler<P, C> {
    pub bundle: Arc<StoreBundle>,
    pub connector: P,
    pub clock: C,
}
impl<P: Connector, C: TrustedClock> ReconciliationHandler<P, C> {
    fn observe(
        &self,
        caller: &AuthenticatedCaller,
        case: &ReconciliationCase,
        result: CaseStatus,
        evidence: Vec<crate::completion::EvidenceBytes>,
    ) -> Result<()> {
        let mut refs = Vec::new();
        if evidence.is_empty()
            || evidence.len() > 2
            || evidence.iter().any(|e| {
                e.bytes.is_empty()
                    || e.bytes.len() as u64 > case.execution.binding.descriptor.max_evidence_bytes
            })
        {
            return Err("reconciliation evidence bound".into());
        }
        for e in evidence {
            let artifact = self.bundle.wi().publish(case.tenant.clone(), &e.bytes)?;
            refs.push(EvidenceRef { artifact, schema: e.schema });
        }
        self.bundle.wi().reconciliation_apply(
            caller,
            self.clock.now()?,
            case,
            CaseAction::Observe { result, evidence: refs },
        )
    }
    fn wait(&self, case: &ReconciliationCase) -> Result<AttemptDisposition> {
        if case.status.terminal() {
            return Ok(AttemptDisposition::complete(None));
        }
        // A private, unmatched filter: only the deadline resumes automatic polling.
        let filter = SignalFilter {
            namespace: actionqueue_core::continuation::SignalNamespace::new(
                "wi.reconciliation.private.v1",
            )?,
            kind: actionqueue_core::continuation::SignalKind::new("deadline-only")?,
            tenant_id: None,
            correlation_id: Some(CorrelationId::new(case.id.as_str())?),
            source_ref: None,
        };
        Ok(AttemptDisposition::awaiting(
            WaitSpec::new(
                WaitId::new(),
                filter,
                WaitMatchPolicy::FirstMatch,
                SignalEligibility::AnyRetained,
                Some(WaitDeadline {
                    at: case
                        .next_eligible_millis
                        .div_ceil(1000)
                        .max(self.clock.now()?.epoch_millis()? / 1000 + 1),
                    policy: WaitTimeoutPolicy::ResumeWithTimeout,
                }),
            )?,
            None,
        ))
    }
    pub fn execute_bound(&self, ctx: &ExecutorContext) -> Result<AttemptDisposition> {
        let locator = CaseLocator::decode(&ctx.input.payload)?;
        let state = self.bundle.wi().effect_snapshot(&locator.tenant)?;
        let effect = state.effects.get(&locator.intent).ok_or("missing reconciliation effect")?;
        let caller = AuthenticatedCaller::from_trusted_authentication(
            locator.tenant,
            effect.agency.binding.principal.clone(),
        );
        let mut case = effect
            .reconciliation_cases
            .iter()
            .find(|c| c.id == locator.case)
            .ok_or("missing case")?
            .clone();
        let expected = request(&case)?;
        if expected.task_spec().task_payload().bytes() != ctx.input.payload
            || case.queue_store.as_str() != self.bundle.aq_store_id()?.to_string()
            || case.admission_request.as_ref() != Some(&serde_json::to_vec(&expected)?)
            || ctx.input.causal_context.as_ref() != Some(expected.causal_context())
            || case.aq_run.as_ref().is_none_or(|run| run.as_str() != ctx.input.run_id.to_string())
        {
            return Err("reconciliation task binding".into());
        }
        if case.status.terminal() {
            return Ok(AttemptDisposition::complete(None));
        }
        let now = self.clock.now()?.epoch_millis()?;
        let policy = &case.execution.binding.descriptor.reconciliation;
        if now >= case.deadline_millis || case.observations >= policy.max_observations {
            self.observe(
                &caller,
                &case,
                CaseStatus::StillUnknown,
                vec![diagnostic(b"automatic reconciliation exhausted")],
            )?;
        } else if !policy.automatic || check_installed(&self.connector, &case.execution).is_err() {
            self.observe(
                &caller,
                &case,
                CaseStatus::Escalated,
                vec![diagnostic(b"manual reconciliation required")],
            )?;
        } else if now < case.next_eligible_millis {
            return self.wait(&case);
        } else if case.status == CaseStatus::Running {
            // A prior observation was interrupted. Spend its durable budget; never infer noncommit.
            self.observe(
                &caller,
                &case,
                CaseStatus::Pending,
                vec![diagnostic(b"observation interrupted")],
            )?;
        } else {
            self.bundle.wi().reconciliation_apply(
                &caller,
                self.clock.now()?,
                &case,
                CaseAction::Start,
            )?;
            case = latest(self.bundle.wi(), &caller, &case)?;
            let observed = self.connector.reconcile(ReconcileRequest {
                tenant: case.tenant.clone(),
                intent: case.intent.clone(),
                execution: case.execution.clone(),
                resource: case.resource.clone(),
            });
            let (status, evidence) = match observed {
                Ok(ReconcileOutcome::Committed(e)) => (CaseStatus::ConfirmedCommitted, e),
                Ok(ReconcileOutcome::NotCommitted(e)) => (CaseStatus::ConfirmedNotCommitted, e),
                Ok(ReconcileOutcome::Unknown(e)) => (CaseStatus::Pending, e),
                Ok(ReconcileOutcome::Escalate(e)) => (CaseStatus::Escalated, e),
                Err(_) => {
                    (CaseStatus::Pending, vec![diagnostic(b"provider observation unavailable")])
                }
            };
            if self.observe(&caller, &case, status, evidence).is_err() {
                let current = latest(self.bundle.wi(), &caller, &case)?;
                if current.revision != case.revision {
                    return self.wait(&current);
                }
                self.observe(
                    &caller,
                    &case,
                    CaseStatus::Pending,
                    vec![diagnostic(b"provider evidence rejected")],
                )?;
            }
        }
        self.wait(&latest(self.bundle.wi(), &caller, &case)?)
    }
}
impl<P: Connector + 'static, C: TrustedClock + Send + Sync + 'static> ExecutorHandler
    for ReconciliationHandler<P, C>
{
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        self.execute_bound(&ctx).unwrap_or_else(|_| {
            AttemptDisposition::terminal_failure(BoundedError {
                code: BoundedCode::new("wi.reconciliation.unavailable").expect("static code"),
                message: BoundedMessage::new("case remains uncertain; inspect durable obligation")
                    .expect("static message"),
            })
        })
    }
}
/// Composition preserves the existing ingress/effect router and adds a separate AQ task.
pub struct ReconciliationRouter<R, H> {
    pub reconciliation: R,
    pub other: H,
}
impl<R: ExecutorHandler, H: ExecutorHandler> ExecutorHandler for ReconciliationRouter<R, H> {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        if CaseLocator::decode(&ctx.input.payload).is_ok() {
            self.reconciliation.execute(ctx)
        } else {
            self.other.execute(ctx)
        }
    }
}

pub(crate) fn admission_bytes(
    effect: &worldinterface_domain::effect::EffectAggregate,
    outcome: &worldinterface_domain::effect::OutcomeEvidence,
    revision: u64,
    at: &Timestamp,
) -> Result<Option<Vec<u8>>> {
    if outcome.outcome != ExecutionOutcome::Uncertain {
        return Ok(None);
    }
    let mut staged = effect.clone();
    staged.outcomes.push(outcome.clone());
    let case = ReconciliationCase::from_uncertain(&staged, revision, at)?;
    case.map(|c| Ok(serde_json::to_vec(&request(&c)?)?)).transpose()
}

pub(crate) fn recover_terminal(
    root: &worldinterface_storage::StoreRoot,
    refs: &crate::aq_adapter::QueueReferences,
) -> Result<()> {
    for tenant in root.effect_tenants()? {
        let state = root.effect_snapshot(&tenant)?;
        for effect in state.effects.values() {
            let caller = AuthenticatedCaller::from_trusted_authentication(
                tenant.clone(),
                effect.agency.binding.principal.clone(),
            );
            for case in &effect.reconciliation_cases {
                if case.status.terminal() {
                    continue;
                }
                let Some(bytes) = &case.admission_request else { continue };
                let prepared: EnsureTaskRequest = serde_json::from_slice(bytes)?;
                if refs.task_terminal(&prepared.task_spec().id().to_string())? {
                    let artifact = root.publish(
                        tenant.clone(),
                        b"AQ observation task ended before automatic reconciliation closed",
                    )?;
                    let at = root
                        .effect_snapshot(&tenant)?
                        .last_at
                        .ok_or("missing reconciliation journal time")?;
                    root.reconciliation_apply(
                        &caller,
                        at,
                        case,
                        CaseAction::Observe {
                            result: CaseStatus::Escalated,
                            evidence: vec![EvidenceRef {
                                artifact,
                                schema: "wi-reconciliation-recovery:1".to_string().try_into()?,
                            }],
                        },
                    )?;
                }
            }
        }
    }
    Ok(())
}
