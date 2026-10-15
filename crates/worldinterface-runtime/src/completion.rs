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

// SQLite already waits for its busy timeout. Retry transient contention in every
// completion persistence step, including publication and phase lookup. This never
// crosses the provider boundary. Identity-bound publications resume idempotently.
fn retry_storage<T>(mut operation: impl FnMut() -> Result<T>) -> Result<T> {
    for retry in 0..8 {
        match operation() {
            Err(error)
                if retry < 7 && worldinterface_storage::is_effect_contention(error.as_ref()) =>
            {
                continue;
            }
            result => return result,
        }
    }
    unreachable!()
}

fn publish(
    root: &worldinterface_storage::StoreRoot,
    tenant: &OpaqueRef,
    identity: String,
    bytes: &[u8],
) -> Result<ArtifactRef> {
    let id: OpaqueRef = identity.try_into()?;
    retry_storage(|| {
        root.stage_publication(id.clone(), tenant.clone(), bytes)?;
        root.publish_staged(&id, bytes)
    })
}

fn time(
    phases: &worldinterface_storage::EffectExecutionTimes,
    at: Timestamp,
    commit: &CommitObservation,
    inferred: bool,
) -> Result<TemporalProvenance> {
    let observed = |at, inferred| -> Result<TimeEvidence> {
        Ok(TimeEvidence {
            at,
            source: if inferred { "wi-journal-lower-bound" } else { "wi-trusted-clock" }
                .to_string()
                .try_into()?,
            assurance: if inferred { "inferred" } else { "host-observed" }
                .to_string()
                .try_into()?,
            // Canonical JSON uses interoperable integers; this is the capped
            // unknown-uncertainty sentinel, distinguished by inferred assurance.
            uncertainty_nanos: if inferred { 9_007_199_254_740_991 } else { 0 },
        })
    };
    let completion = observed(at, inferred)?;
    Ok(TemporalProvenance {
        occurred_at: None,
        observed_at: Some(completion.clone()),
        received_at: observed(phases.received.clone(), false)?,
        admitted_at: observed(phases.admitted.clone(), false)?,
        dispatched_at: Some(observed(phases.dispatched.clone(), false)?),
        committed_at: if *commit == CommitObservation::ConfirmedCommitted {
            Some(completion.clone())
        } else {
            None
        },
        reconciled_at: None,
        sequence: None,
    })
}

fn uncertainty(schema: &str, reason: &[u8]) -> Result<PortObservation> {
    Ok(PortObservation {
        commit: CommitObservation::Unknown,
        outcome: ExecutionOutcome::Uncertain,
        outputs: vec![],
        evidence: vec![EvidenceBytes {
            schema: schema.to_string().try_into()?,
            bytes: reason.to_vec(),
        }],
        boundary_record: None,
    })
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
        let (tenant, id) = root.effect_run_binding(&run)?.ok_or("unobserved AQ effect run")?;
        let state = root.effect_snapshot(&tenant)?;
        let effect = state.effects.get(&id).ok_or("missing AQ-bound effect")?;
        let admission = effect
            .dispatches
            .last()
            .and_then(|d| d.admission.as_ref())
            .ok_or("missing AQ admission")?;
        let request = crate::admission::load_request(root, admission)?;
        if !admission.observed_runs.contains(&run)
            || admission.queue_store.as_str() != store
            || admission.acknowledged_sequence.is_none()
            || request.task_spec().id().to_string() != admission.task.as_str()
            || request.task_spec().task_payload().bytes() != ctx.input.payload
            || locator.artifact() != &effect.intent.submission.input.artifact
        {
            return Err("AQ effect binding mismatch".into());
        }
        let input = root.read_artifact(locator.artifact())?;
        let caller = AuthenticatedCaller::from_trusted_authentication(
            tenant,
            effect.agency.binding.principal.clone(),
        );
        Ok((caller, id, run, input))
    }

    fn committed_disposition(
        &self,
        caller: &AuthenticatedCaller,
        id: &EffectIntentId,
        run: &OpaqueRef,
    ) -> Result<Option<AttemptDisposition>> {
        let root = self.bundle.wi();
        let view = Egress::new(root).inspect(caller, id)?;
        let Some(outcome) = view.outcome else {
            return Ok(None);
        };
        let state = root.effect_snapshot(caller.tenant())?;
        let effect = state.effects.get(id).ok_or("missing committed effect")?;
        if effect.attempt.as_ref() != Some(&outcome.attempt)
            || matches!(effect.phase, Phase::Authorized | Phase::DispatchPending | Phase::Running)
        {
            return Ok(None);
        }
        let dispatch = effect.dispatches.last().ok_or("missing current dispatch")?;
        if dispatch.admission.as_ref().is_none_or(|a| !a.observed_runs.contains(run)) {
            return Err("committed outcome belongs to another AQ run".into());
        }
        for reference in worldinterface_domain::effect::required_artifacts(effect) {
            root.read_artifact(reference)?;
        }
        let receipts = root.effect_receipts(caller.tenant(), id)?;
        let (receipt, receipt_digest, revision) =
            receipts.last().ok_or("terminal outcome without receipt")?.clone();
        let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
            caller.tenant().clone(),
            caller.principal().clone(),
            worldinterface_domain::receipt::DisclosureProfile::Full,
        );
        let receipt_view =
            root.inspect_receipt(&access, &receipt)?.ok_or("committed receipt unavailable")?;
        let source = root.effect_snapshot_at_revision(caller.tenant(), revision)?;
        let source_effect = source.effects.get(id).ok_or("missing receipt source effect")?;
        let source_dispatch =
            source_effect.dispatches.last().ok_or("missing receipt source dispatch")?;
        let same_admission = match (&dispatch.admission, &source_dispatch.admission) {
            (Some(current), Some(old)) => {
                current.task == old.task
                    && current.queue_store == old.queue_store
                    && current.execution == old.execution
                    && current.canonical_digest == old.canonical_digest
                    && current.admission_key == old.admission_key
                    && old.observed_runs.contains(run)
            }
            _ => false,
        };
        if source_effect.attempt.as_ref() != Some(&outcome.attempt)
            || source_effect.outcomes.last() != Some(&outcome)
            || source_dispatch.request != dispatch.request
            || !same_admission
        {
            return Err("committed receipt belongs to another dispatch or attempt".into());
        }
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
            receipt: receipt.clone(),
            receipt_digest,
            outcome: outcome.outcome,
        };
        let bytes = canonical_data(&locator)?;
        let artifact = publish(
            root,
            caller.tenant(),
            format!("completion-locator:{}:{}", id.as_str(), receipt.as_str()),
            &bytes,
        )?;
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
        run: &OpaqueRef,
        observation: PortObservation,
        identity_class: &'static str,
    ) -> Result<AttemptDisposition> {
        let root = self.bundle.wi();
        // Avoid publishing an already superseded provider observation. The
        // transactional seam rechecks this fence after publication as well.
        let state = retry_storage(|| root.effect_snapshot(caller.tenant()))?;
        let effect = state.effects.get(id).ok_or("missing completion effect")?;
        if effect.phase != Phase::Running
            || effect.attempt.as_ref() != Some(&attempt)
            || effect.outcomes.iter().any(|o| o.attempt == attempt)
            || effect
                .dispatches
                .last()
                .and_then(|d| d.admission.as_ref())
                .is_none_or(|a| !a.observed_runs.contains(run))
        {
            return retry_storage(|| self.committed_disposition(caller, id, run))?
                .ok_or_else(|| "stale effect attempt before publication".into());
        }
        drop(state);
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
        let result = retry_storage(|| {
            root.effect_transact_receipt_with(
                caller.tenant(),
                id,
                &attempt,
                run,
                |state, phases| {
                    let effect = state.effects.get(id).ok_or("missing effect")?;
                    let (at, inferred) = match self.clock.now() {
                        Ok(at) if state.last_at.as_ref().is_none_or(|last| &at >= last) => {
                            (at, false)
                        }
                        _ if identity_class == "fallback" || identity_class == "recovery" => (
                            state.last_at.clone().ok_or("missing journal clock lower bound")?,
                            true,
                        ),
                        _ => return Err("completion clock unavailable or regressed".into()),
                    };
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
                        commit: observation.commit.clone(),
                        outcome: observation.outcome.clone(),
                        evidence: evidence.clone(),
                        required_outputs: effect
                            .authorization_inputs
                            .as_ref()
                            .ok_or("missing authority inputs")?
                            .required_outputs
                            .clone(),
                        durable_outputs: outputs.clone(),
                        boundary_record: boundary_record.clone(),
                    };
                    let command = worldinterface_domain::effect::validate_command(
                        state,
                        caller,
                        at.clone(),
                        EffectAction::Outcome {
                            id: id.clone(),
                            evidence: VerifiedOutcome::from_trusted_verifier(outcome),
                        },
                    )?;
                    let execution = ExecutionRecord {
                        intent_id: id.clone(),
                        attempt: attempt.clone(),
                        outcome: observation.outcome.clone(),
                        evidence: evidence.clone(),
                        recorded_at: at.clone(),
                    };
                    let temporal = time(phases, at, &observation.commit, inferred)?;
                    Ok(Some(worldinterface_storage::EffectReceiptCommand {
                        command,
                        execution,
                        temporal,
                    }))
                },
            )
        });
        // Also recover a lost commit response or a superseding delivery's receipt.
        if let Some(disposition) = retry_storage(|| self.committed_disposition(caller, id, run))? {
            return Ok(disposition);
        }
        result?;
        Err("missing committed outcome or stale effect attempt".into())
    }

    pub fn execute_bound(&self, ctx: &ExecutorContext) -> Result<AttemptDisposition> {
        self.execute_staged(ctx, &mut false)
    }

    fn execute_staged(
        &self,
        ctx: &ExecutorContext,
        started: &mut bool,
    ) -> Result<AttemptDisposition> {
        let (caller, id, run, input) = self.bind(ctx)?;
        if let Some(disposition) = self.committed_disposition(&caller, &id, &run)? {
            return Ok(disposition);
        }
        let root = self.bundle.wi();
        let view = Egress::new(root).inspect(&caller, &id)?;
        if view.phase == Phase::Running {
            *started = true;
            let state = root.effect_snapshot(caller.tenant())?;
            let original = state
                .effects
                .get(&id)
                .ok_or("missing original effect")?
                .attempt
                .clone()
                .ok_or("missing original attempt")?;
            return self.record_outcome(
                &caller,
                &id,
                original,
                &run,
                uncertainty(
                    "wi.recovery.uncertainty:1",
                    b"Running effect observed on a second AQ delivery",
                )?,
                "recovery",
            );
        }
        let attempt: OpaqueRef = ctx.input.attempt_id.to_string().try_into()?;
        let start = (|| -> Result<()> {
            let at = self.clock.now()?;
            let snapshot = verify_snapshot(
                root,
                &self.evidence,
                caller.tenant(),
                &view.intent.submission,
                &at,
            )?;
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
            Ok(())
        })();
        if let Err(error) = start {
            // Failure diagnostics are reference-only and cannot claim an execution outcome.
            let _ =
                root.record_dispatch_fault(caller.tenant(), &id, &run, "wi.effect.pre_start_error");
            return Err(error);
        }
        *started = true;
        let result = (|| -> Result<AttemptDisposition> {
            let state = root.effect_snapshot(caller.tenant())?;
            let effect = state.effects.get(&id).ok_or("missing effect at invocation fence")?;
            // A superseding delivery can have completed uncertainty while this
            // handler was preparing its input. Signal fence loss to the same token.
            if effect.phase != Phase::Running
                || effect.attempt.as_ref() != Some(&attempt)
                || effect.outcomes.last().is_some_and(|o| o.attempt == attempt)
            {
                ctx.input.cancellation_context.cancel();
                return self
                    .committed_disposition(&caller, &id, &run)?
                    .ok_or_else(|| "lost invocation fence".into());
            }
            let bound = BoundExecution {
                tenant: caller.tenant().clone(),
                intent: id.clone(),
                run: run.clone(),
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
            // Re-check after constructing the request, immediately before crossing the port.
            let current = root.effect_snapshot(caller.tenant())?;
            let current = current.effects.get(&id).ok_or("missing invocation effect")?;
            if current.phase != Phase::Running
                || current.attempt.as_ref() != Some(&attempt)
                || current.outcomes.last().is_some_and(|o| o.attempt == attempt)
            {
                ctx.input.cancellation_context.cancel();
                return self
                    .committed_disposition(&caller, &id, &run)?
                    .ok_or_else(|| "lost invocation fence".into());
            }
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
                self.port.invoke(bound, ctx.input.cancellation_context.token()).unwrap_or(
                    uncertainty("wi.port.uncertainty:1", b"port did not return commit evidence")?,
                )
            };
            self.record_outcome(&caller, &id, attempt.clone(), &run, observation, "port")
        })();
        match result {
            Ok(disposition) => Ok(disposition),
            Err(_) => {
                // Invalid/oversized evidence, publication, clock and commit failures
                // after Start are never classified as retry-safe provider noncommit.
                self.record_outcome(
                    &caller,
                    &id,
                    attempt,
                    &run,
                    uncertainty(
                        "wi.completion.uncertainty:1",
                        b"completion could not be durably recorded after Start",
                    )?,
                    "fallback",
                )
            }
        }
    }
}

/// Recovery has no connector authority. It can only record uncertainty against
/// the already durable original Start binding.
pub(crate) fn record_uncertain_after_terminal_run(
    root: &worldinterface_storage::StoreRoot,
    tenant: &OpaqueRef,
    id: &EffectIntentId,
    attempt: &OpaqueRef,
    run: &OpaqueRef,
) -> Result<()> {
    let artifact = publish(
        root,
        tenant,
        format!("effect-recovery:{}:{}", id.as_str(), attempt.as_str()),
        b"AQ run became terminal while Fabric Start had no outcome",
    )?;
    let reference =
        EvidenceRef { artifact, schema: "wi.recovery.uncertainty:1".to_string().try_into()? };
    retry_storage(|| {
        root.effect_transact_receipt_with(tenant, id, attempt, run, |state, phases| {
            let effect = state.effects.get(id).ok_or("missing effect")?;
            let caller = AuthenticatedCaller::from_trusted_authentication(
                tenant.clone(),
                effect.agency.binding.principal.clone(),
            );
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
                state,
                &caller,
                at.clone(),
                EffectAction::Outcome {
                    id: id.clone(),
                    evidence: VerifiedOutcome::from_trusted_verifier(outcome),
                },
            )?;
            let execution = ExecutionRecord {
                intent_id: id.clone(),
                attempt: attempt.clone(),
                outcome: ExecutionOutcome::Uncertain,
                evidence: vec![reference.clone()],
                recorded_at: at.clone(),
            };
            let temporal = time(phases, at, &CommitObservation::Unknown, true)?;
            Ok(Some(worldinterface_storage::EffectReceiptCommand { command, execution, temporal }))
        })
    })?;
    Ok(())
}

impl<
        P: EffectExecutionPort + 'static,
        V: EvidenceAuthority + Send + Sync + 'static,
        C: TrustedClock + Send + Sync + 'static,
    > ExecutorHandler for EffectAttemptHandler<P, V, C>
{
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let mut started = false;
        self.execute_staged(&ctx, &mut started).unwrap_or_else(|_| {
            if started {
                return terminal("wi.effect.uncertain_recording_error");
            }
            // A failed fallback remains possible commit even if storage is unavailable.
            // It must never share a code with a retry-safe pre-Start failure.
            let Ok((caller, id, run, _)) = self.bind(&ctx) else {
                return terminal("wi.effect.phase_unknown");
            };
            let Ok(state) = self.bundle.wi().effect_snapshot(caller.tenant()) else {
                return terminal("wi.effect.phase_unknown");
            };
            let Some(effect) = state.effects.get(&id) else {
                return terminal("wi.effect.phase_unknown");
            };
            if effect.phase != Phase::DispatchPending || effect.attempt.is_some() {
                return terminal("wi.effect.uncertain_recording_error");
            }
            let _ = self.bundle.wi().record_dispatch_fault(
                caller.tenant(),
                &id,
                &run,
                "wi.effect.pre_start_error",
            );
            AttemptDisposition::retryable_failure(BoundedError {
                code: BoundedCode::new("wi.effect.pre_start_error").expect("static code"),
                message: BoundedMessage::new("dispatch refused before Start")
                    .expect("static message"),
            })
        })
    }
}
