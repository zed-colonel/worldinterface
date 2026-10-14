//! Effect completion owns the Fabric Start fence and publication before AQ disposition.
//! The execution port is supplied by trusted host composition (WI-06).
use crate::{
    aq_adapter::TaskInputLocator,
    authority::{verify_snapshot, EvidenceAuthority, TrustedClock},
    storage::StoreBundle,
    Result,
};
use actionqueue_core::{
    bounded::{
        BoundedCode, BoundedError, BoundedMessage, ContentHash as AqHash, DataScheme,
        HashAlgorithm, OpaqueRef as AqOpaqueRef,
    },
    data_ref::{DataRef, ExternalDataRef},
    disposition::AttemptDisposition,
};
use actionqueue_executor_local::handler::{CancellationToken, ExecutorContext, ExecutorHandler};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    effect::{CommitObservation, EffectAction, OutcomeEvidence, VerifiedOutcome},
    Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::{
    canonical_data, ArtifactRef, ContentHash, EffectIntentId, EvidenceRef, ExecutionOutcome,
    ExecutionRecord, OpaqueRef, ReceiptId, ResolvedEffect, TemporalProvenance, TimeEvidence,
    Timestamp,
};

#[derive(Clone)]
pub struct BoundExecution {
    pub tenant: OpaqueRef,
    pub intent: EffectIntentId,
    pub run: OpaqueRef,
    pub attempt: OpaqueRef,
    pub input: Vec<u8>,
    pub authorization: ContentHash,
    pub execution_binding: ContentHash,
    pub idempotency: OpaqueRef,
    pub resolved: ResolvedEffect,
}

pub struct EvidenceBytes {
    pub schema: OpaqueRef,
    pub bytes: Vec<u8>,
}

pub struct PortObservation {
    pub commit: CommitObservation,
    pub outcome: ExecutionOutcome,
    pub outputs: Vec<Vec<u8>>,
    pub evidence: Vec<EvidenceBytes>,
    pub boundary_record: Option<EvidenceBytes>,
}

/// Implementations must not infer noncommit from cancellation after invocation.
pub trait EffectExecutionPort: Send + Sync {
    fn invoke(
        &self,
        request: BoundExecution,
        cancellation: &CancellationToken,
    ) -> Result<PortObservation>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionLocator {
    pub version: u32,
    pub intent: EffectIntentId,
    pub receipt: ReceiptId,
    pub receipt_digest: ContentHash,
    pub outcome: ExecutionOutcome,
}

fn terminal(code: &'static str) -> AttemptDisposition {
    AttemptDisposition::terminal_failure(BoundedError {
        code: BoundedCode::new(code).expect("static completion code"),
        message: BoundedMessage::new(code).expect("static completion message"),
    })
}

fn publish(
    root: &worldinterface_storage::StoreRoot,
    tenant: &OpaqueRef,
    identity: String,
    bytes: &[u8],
) -> Result<ArtifactRef> {
    let id: OpaqueRef = identity.try_into()?;
    root.stage_publication(id.clone(), tenant.clone(), bytes)?;
    root.publish_staged(&id, bytes)
}

fn time(at: Timestamp) -> TemporalProvenance {
    let evidence = TimeEvidence {
        at,
        source: "wi-trusted-clock".to_string().try_into().unwrap(),
        assurance: "host-observed".to_string().try_into().unwrap(),
        uncertainty_nanos: 0,
    };
    TemporalProvenance {
        occurred_at: None,
        observed_at: None,
        received_at: evidence.clone(),
        admitted_at: evidence.clone(),
        dispatched_at: Some(evidence.clone()),
        committed_at: Some(evidence),
        reconciled_at: None,
        sequence: None,
    }
}

pub struct EffectAttemptHandler<P, V, C> {
    pub bundle: Arc<StoreBundle>,
    pub port: P,
    pub evidence: V,
    pub clock: C,
}

impl<P: EffectExecutionPort, V: EvidenceAuthority, C: TrustedClock> EffectAttemptHandler<P, V, C> {
    fn bind(
        &self,
        ctx: &ExecutorContext,
    ) -> Result<(AuthenticatedCaller, EffectIntentId, OpaqueRef, Vec<u8>)> {
        let root = self.bundle.wi();
        let locator = TaskInputLocator::decode(&ctx.input.payload)?;
        let run: OpaqueRef = ctx.input.run_id.to_string().try_into()?;
        let store = self.bundle.aq_store_id()?.to_string();
        let mut found = None;
        for tenant in root.effect_tenants()? {
            let state = root.effect_snapshot(&tenant)?;
            for (id, effect) in &state.effects {
                let Some(admission) = effect.dispatches.last().and_then(|d| d.admission.as_ref())
                else {
                    continue;
                };
                if !admission.observed_runs.contains(&run) {
                    continue;
                }
                if found.is_some() {
                    return Err("ambiguous AQ run binding".into());
                }
                let request = crate::admission::load_request(root, admission)?;
                if admission.queue_store.as_str() != store
                    || admission.acknowledged_sequence.is_none()
                    || request.task_spec().id().to_string() != admission.task.as_str()
                    || request.task_spec().task_payload().bytes() != ctx.input.payload
                    || locator.artifact() != &effect.intent.submission.input.artifact
                {
                    return Err("AQ effect binding mismatch".into());
                }
                let input = root.read_artifact(locator.artifact())?;
                let caller = AuthenticatedCaller::from_trusted_authentication(
                    tenant.clone(),
                    effect.agency.binding.principal.clone(),
                );
                found = Some((caller, id.clone(), run.clone(), input));
            }
        }
        found.ok_or_else(|| "unobserved AQ effect run".into())
    }

    fn committed_disposition(
        &self,
        caller: &AuthenticatedCaller,
        id: &EffectIntentId,
    ) -> Result<Option<AttemptDisposition>> {
        let root = self.bundle.wi();
        let view = Egress::new(root).inspect(caller, id)?;
        let Some(outcome) = view.outcome else {
            return Ok(None);
        };
        let state = root.effect_snapshot(caller.tenant())?;
        let effect = state.effects.get(id).ok_or("missing committed effect")?;
        for reference in worldinterface_domain::effect::required_artifacts(effect) {
            root.read_artifact(reference)?;
        }
        let receipts = root.effect_receipts(caller.tenant(), id)?;
        let (receipt, receipt_digest, _) =
            receipts.last().ok_or("terminal outcome without receipt")?.clone();
        let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
            caller.tenant().clone(),
            caller.principal().clone(),
            worldinterface_domain::receipt::DisclosureProfile::Full,
        );
        let receipt_view =
            root.inspect_receipt(&access, &receipt)?.ok_or("committed receipt unavailable")?;
        if receipt_view.digest.as_ref() != Some(&receipt_digest) {
            return Err("committed receipt digest mismatch".into());
        }
        if outcome.outcome != ExecutionOutcome::Succeeded {
            let code = match outcome.outcome {
                ExecutionOutcome::FailedBeforeCommit => "wi.effect.failed_before_commit",
                ExecutionOutcome::CanceledBeforeCommit => "wi.effect.canceled_before_commit",
                ExecutionOutcome::Uncertain => "wi.effect.uncertain",
                ExecutionOutcome::Succeeded => unreachable!(),
            };
            return Ok(Some(terminal(code)));
        }
        let locator = CompletionLocator {
            version: 1,
            intent: id.clone(),
            receipt,
            receipt_digest,
            outcome: outcome.outcome,
        };
        let bytes = canonical_data(&locator)?;
        let artifact =
            publish(root, caller.tenant(), format!("completion-locator:{}", id.as_str()), &bytes)?;
        let external = ExternalDataRef {
            scheme: DataScheme::new("wi-artifact-v1")?,
            locator: AqOpaqueRef::new(format!(
                "{}/{}",
                artifact.scope.as_str(),
                artifact.content.as_str()
            ))?,
            hash: AqHash::new(HashAlgorithm::Sha256, hex::decode(artifact.content.as_str())?)?,
            size_bytes: Some(artifact.size),
            content_type: None,
        };
        Ok(Some(AttemptDisposition::complete(Some(DataRef::External(external)))))
    }

    fn record_outcome(
        &self,
        caller: &AuthenticatedCaller,
        id: &EffectIntentId,
        attempt: OpaqueRef,
        observation: PortObservation,
        identity_class: &'static str,
    ) -> Result<AttemptDisposition> {
        let root = self.bundle.wi();
        let state = root.effect_snapshot(caller.tenant())?;
        let effect = state.effects.get(id).ok_or("missing effect")?;
        if effect.phase != Phase::Running || effect.attempt.as_ref() != Some(&attempt) {
            if let Some(disposition) = self.committed_disposition(caller, id)? {
                return Ok(disposition);
            }
            return Err("stale effect attempt".into());
        }
        let required = effect
            .authorization_inputs
            .as_ref()
            .ok_or("missing authority inputs")?
            .required_outputs
            .clone();
        let semantics = effect
            .resolution
            .as_ref()
            .ok_or("missing resolution")?
            .resolved
            .idempotency_semantics
            .clone();
        let mut outputs = Vec::new();
        for (index, bytes) in observation.outputs.iter().enumerate() {
            outputs.push(publish(
                root,
                caller.tenant(),
                format!(
                    "effect-output:{identity_class}:{}:{}:{}",
                    id.as_str(),
                    attempt.as_str(),
                    index
                ),
                bytes,
            )?);
        }
        let mut evidence = Vec::new();
        for (index, blob) in observation.evidence.iter().enumerate() {
            let artifact = publish(
                root,
                caller.tenant(),
                format!(
                    "effect-evidence:{identity_class}:{}:{}:{}",
                    id.as_str(),
                    attempt.as_str(),
                    index
                ),
                &blob.bytes,
            )?;
            evidence.push(EvidenceRef { artifact, schema: blob.schema.clone() });
        }
        let boundary_record = observation
            .boundary_record
            .map(|blob| {
                publish(
                    root,
                    caller.tenant(),
                    format!(
                        "effect-boundary:{identity_class}:{}:{}",
                        id.as_str(),
                        attempt.as_str()
                    ),
                    &blob.bytes,
                )
                .map(|artifact| EvidenceRef { artifact, schema: blob.schema })
            })
            .transpose()?;
        let at = self.clock.now()?;
        let outcome = OutcomeEvidence {
            intent: id.clone(),
            attempt: attempt.clone(),
            connector_semantics: semantics,
            commit: observation.commit,
            outcome: observation.outcome,
            evidence: evidence.clone(),
            required_outputs: required,
            durable_outputs: outputs,
            boundary_record,
        };
        let command = worldinterface_domain::effect::validate_command(
            &state,
            caller,
            at.clone(),
            EffectAction::Outcome {
                id: id.clone(),
                evidence: VerifiedOutcome::from_trusted_verifier(outcome.clone()),
            },
        )?;
        let execution = ExecutionRecord {
            intent_id: id.clone(),
            attempt,
            outcome: outcome.outcome,
            evidence,
            recorded_at: at.clone(),
        };
        if let Err(error) =
            root.effect_transact_with_receipt(&command, ReceiptId::generate(), &execution, time(at))
        {
            if let Some(disposition) = self.committed_disposition(caller, id)? {
                return Ok(disposition);
            }
            return Err(error);
        }
        self.committed_disposition(caller, id)?.ok_or_else(|| "missing committed outcome".into())
    }

    pub fn execute_bound(&self, ctx: &ExecutorContext) -> Result<AttemptDisposition> {
        let (caller, id, run, input) = self.bind(ctx)?;
        if let Some(disposition) = self.committed_disposition(&caller, &id)? {
            return Ok(disposition);
        }
        let root = self.bundle.wi();
        let view = Egress::new(root).inspect(&caller, &id)?;
        if view.phase == Phase::Running {
            let original = root.effect_snapshot(caller.tenant())?.effects[&id]
                .attempt
                .clone()
                .ok_or("missing original attempt")?;
            return self.record_outcome(
                &caller,
                &id,
                original,
                PortObservation {
                    commit: CommitObservation::Unknown,
                    outcome: ExecutionOutcome::Uncertain,
                    outputs: vec![],
                    evidence: vec![EvidenceBytes {
                        schema: "wi.recovery.uncertainty:1".to_string().try_into()?,
                        bytes: b"Running effect observed on a second AQ delivery".to_vec(),
                    }],
                    boundary_record: None,
                },
                "recovery",
            );
        }
        let at = self.clock.now()?;
        let snapshot =
            verify_snapshot(root, &self.evidence, caller.tenant(), &view.intent.submission, &at)?;
        let attempt: OpaqueRef = ctx.input.attempt_id.to_string().try_into()?;
        Egress::new(root).apply(
            &caller,
            at,
            EffectAction::StartProduction {
                id: id.clone(),
                snapshot,
                run: run.clone(),
                attempt: attempt.clone(),
            },
        )?;
        let observation = if ctx.input.cancellation_context.token().is_cancelled() {
            PortObservation {
                commit: CommitObservation::ProvenImpossible,
                outcome: ExecutionOutcome::CanceledBeforeCommit,
                outputs: vec![],
                evidence: vec![EvidenceBytes {
                    schema: "wi.cancellation:1".to_string().try_into()?,
                    bytes: b"canceled before port invocation".to_vec(),
                }],
                boundary_record: None,
            }
        } else {
            let state = root.effect_snapshot(caller.tenant())?;
            let effect = &state.effects[&id];
            let bound = BoundExecution {
                tenant: caller.tenant().clone(),
                intent: id.clone(),
                run,
                attempt: attempt.clone(),
                input,
                authorization: worldinterface_domain::ingress::digest(
                    effect.authorization.as_ref().ok_or("no authorization")?,
                )?,
                execution_binding: effect
                    .dispatches
                    .last()
                    .and_then(|d| d.admission.as_ref())
                    .ok_or("no production admission")?
                    .execution
                    .clone(),
                idempotency: effect.intent.stable_idempotency_identity.clone(),
                resolved: effect.resolution.as_ref().ok_or("missing resolution")?.resolved.clone(),
            };
            match self.port.invoke(bound, ctx.input.cancellation_context.token()) {
                Ok(observation) => observation,
                Err(_) => PortObservation {
                    commit: CommitObservation::Unknown,
                    outcome: ExecutionOutcome::Uncertain,
                    outputs: vec![],
                    evidence: vec![EvidenceBytes {
                        schema: "wi.port.uncertainty:1".to_string().try_into()?,
                        bytes: b"port did not return commit evidence".to_vec(),
                    }],
                    boundary_record: None,
                },
            }
        };
        self.record_outcome(&caller, &id, attempt, observation, "port")
    }
}

/// Recovery has no connector authority. It can only record uncertainty against
/// the already durable original Start binding.
pub(crate) fn record_uncertain_after_terminal_run(
    root: &worldinterface_storage::StoreRoot,
    tenant: &OpaqueRef,
    id: &EffectIntentId,
) -> Result<()> {
    let state = root.effect_snapshot(tenant)?;
    let effect = state.effects.get(id).ok_or("missing effect")?;
    if effect.phase != Phase::Running || !effect.outcomes.is_empty() {
        return Ok(());
    }
    let attempt = effect.attempt.clone().ok_or("missing original attempt")?;
    let caller = AuthenticatedCaller::from_trusted_authentication(
        tenant.clone(),
        effect.agency.binding.principal.clone(),
    );
    let evidence = EvidenceBytes {
        schema: "wi.recovery.uncertainty:1".to_string().try_into()?,
        bytes: b"AQ run became terminal while Fabric Start had no outcome".to_vec(),
    };
    let artifact = publish(
        root,
        tenant,
        format!("effect-recovery:{}:{}", id.as_str(), attempt.as_str()),
        &evidence.bytes,
    )?;
    let reference = EvidenceRef { artifact, schema: evidence.schema };
    let at = state.last_at.clone().ok_or("missing trusted Start timestamp")?;
    let outcome = OutcomeEvidence {
        intent: id.clone(),
        attempt: attempt.clone(),
        connector_semantics: effect
            .resolution
            .as_ref()
            .ok_or("missing resolution")?
            .resolved
            .idempotency_semantics
            .clone(),
        commit: CommitObservation::Unknown,
        outcome: ExecutionOutcome::Uncertain,
        evidence: vec![reference.clone()],
        required_outputs: effect
            .authorization_inputs
            .as_ref()
            .ok_or("missing authority inputs")?
            .required_outputs
            .clone(),
        durable_outputs: vec![],
        boundary_record: None,
    };
    let command = worldinterface_domain::effect::validate_command(
        &state,
        &caller,
        at.clone(),
        EffectAction::Outcome {
            id: id.clone(),
            evidence: VerifiedOutcome::from_trusted_verifier(outcome),
        },
    )?;
    let execution = ExecutionRecord {
        intent_id: id.clone(),
        attempt,
        outcome: ExecutionOutcome::Uncertain,
        evidence: vec![reference],
        recorded_at: at.clone(),
    };
    root.effect_transact_with_receipt(&command, ReceiptId::generate(), &execution, time(at))?;
    Ok(())
}

impl<
        P: EffectExecutionPort + 'static,
        V: EvidenceAuthority + Send + Sync + 'static,
        C: TrustedClock + Send + Sync + 'static,
    > ExecutorHandler for EffectAttemptHandler<P, V, C>
{
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        self.execute_bound(&ctx).unwrap_or_else(|_| terminal("wi.effect.handler_error"))
    }
}
