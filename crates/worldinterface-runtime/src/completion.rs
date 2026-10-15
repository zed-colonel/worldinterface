//! Effect completion owns the Fabric Start fence and publication before AQ disposition.
//! Every provider invocation crosses a validated, pinned connector registration.
use crate::connector::{
    check_installed, AuthorizedInvocation, Connector, PrepareOutcome, PreparedInvocation,
    ResolveRequest,
};
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
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
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

impl<P: Connector, V: EvidenceAuthority, C: TrustedClock> EffectAttemptHandler<P, V, C> {
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
        let mut selected = None;
        for candidate in receipts.iter().rev() {
            let source = root.effect_receipt_source(caller.tenant(), id, candidate.2)?;
            if source.outcome.attempt == outcome.attempt
                && source.run == *run
                && source.dispatch_index == effect.dispatches.len() - 1
            {
                selected = Some(candidate.clone());
                break;
            }
        }
        let (receipt, receipt_digest, revision) =
            selected.ok_or("terminal outcome without execution receipt")?;
        let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
            caller.tenant().clone(),
            caller.principal().clone(),
            worldinterface_domain::receipt::DisclosureProfile::Full,
        );
        let receipt_view =
            root.inspect_receipt(&access, &receipt)?.ok_or("committed receipt unavailable")?;
        let source = root.effect_receipt_source(caller.tenant(), id, revision)?;
        let request = worldinterface_domain::effect::admission_request(
            caller.tenant(),
            effect,
            &source.snapshot,
            u32::try_from(source.dispatch_index)?,
        )?;
        let same_admission = match &dispatch.admission {
            Some(current) => {
                let old = &source.admission;
                current.version == old.version
                    && current.task == old.task
                    && current.queue_store == old.queue_store
                    && current.execution == old.execution
                    && current.request_artifact == old.request_artifact
                    && current.request_hash == old.request_hash
                    && current.canonical_version == old.canonical_version
                    && current.canonical_digest == old.canonical_digest
                    && current.admission_key == old.admission_key
                    && current.acknowledged_sequence == old.acknowledged_sequence
                    && request.execution == old.execution
                    && request.admission_key == old.admission_key
                    && old.observed_runs.contains(run)
            }
            _ => false,
        };
        if source.outcome != outcome
            || source.run != *run
            || source.dispatch_index != effect.dispatches.len() - 1
            || request != dispatch.request
            || !same_admission
        {
            return Err("committed receipt belongs to another dispatch or attempt".into());
        }
        if receipt_view.digest.as_ref() != Some(&receipt_digest) {
            return Err("committed receipt digest mismatch".into());
        }
        if outcome.outcome != ExecutionOutcome::Succeeded {
            let code = match outcome.outcome {
                ExecutionOutcome::TerminalFailure => "wi.effect.terminal_provider_failure",
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
        let classification = observation.classification();
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
        let phases = retry_storage(|| root.effect_execution_times(caller.tenant(), id))?;
        if phases.run.as_ref() != Some(run) || phases.attempt != attempt {
            return Err("stale Start binding before publication".into());
        }
        if let Some(execution) =
            effect.resolution.as_ref().and_then(|r| r.resolved.connector_execution.as_ref())
        {
            if observation
                .evidence
                .iter()
                .chain(&observation.boundary_record)
                .any(|e| e.bytes.len() as u64 > execution.binding.descriptor.max_evidence_bytes)
            {
                return Err("connector evidence profile bound".into());
            }
        }
        // Bound the complete observation before publishing even its first blob.
        // A rejected provider assertion falls back to one bounded uncertainty
        // observation; no oversized port publications are left behind.
        worldinterface_domain::effect::validate_outcome_bounds(
            observation.outputs.len(),
            observation.evidence.len(),
            observation.outputs.iter().map(|b| b.len() as u64).chain(
                observation
                    .evidence
                    .iter()
                    .chain(&observation.boundary_record)
                    .map(|b| b.bytes.len() as u64),
            ),
        )?;
        // Content-derived references let the domain check structural claims
        // before publication, without validating/digesting the tenant again.
        // The locked transaction still validates the authoritative manifest and
        // attempt after all bytes have actually been published.
        let reference = |bytes: &[u8]| ArtifactRef {
            scope: caller.tenant().clone(),
            content: ContentHash::of(bytes),
            size: bytes.len() as u64,
        };
        let evidence_ref = |blob: &EvidenceBytes| EvidenceRef {
            artifact: reference(&blob.bytes),
            schema: blob.schema.clone(),
        };
        let preview = OutcomeEvidence {
            classification: Some(classification),
            reconciliation_admission: None,
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
            evidence: observation.evidence.iter().map(evidence_ref).collect(),
            required_outputs: effect
                .authorization_inputs
                .as_ref()
                .ok_or("missing authority inputs")?
                .required_outputs
                .clone(),
            durable_outputs: observation.outputs.iter().map(|b| reference(b)).collect(),
            boundary_record: observation.boundary_record.as_ref().map(evidence_ref),
        };
        worldinterface_domain::effect::validate_outcome_publication(caller.tenant(), &preview)?;
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
                    let mut outcome = OutcomeEvidence {
                        classification: Some(classification),
                        reconciliation_admission: None,
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
                    outcome.reconciliation_admission = crate::reconciliation::admission_bytes(
                        effect,
                        &outcome,
                        state.revision + 1,
                        &at,
                    )?;
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
                        classification: Some(classification),
                        connector_execution: effect
                            .resolution
                            .as_ref()
                            .and_then(|r| r.resolved.connector_execution.clone())
                            .map(Box::new),
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
        let state = root.effect_snapshot(caller.tenant())?;
        let effect = state.effects.get(&id).ok_or("missing pre-dispatch effect")?;
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
        let reject = || -> Result<AttemptDisposition> {
            root.record_dispatch_fault(caller.tenant(), &id, &run, "wi.effect.pre_start_error")?;
            Ok(terminal("wi.effect.pre_dispatch_rejected"))
        };
        let Some(execution) = bound.resolved.connector_execution.clone() else {
            return reject();
        };
        if check_installed(&self.port, &execution).is_err() {
            return reject();
        }
        let observed = self.port.resolve(ResolveRequest {
            selector: effect.intent.submission.resource.clone(),
            resource: bound.resolved.resource.clone(),
        })?;
        if observed.resource != bound.resolved.resource {
            return reject();
        }
        let authorized = AuthorizedInvocation::new(
            bound,
            execution,
            effect.intent.submission.deadline.clone(),
            effect.intent.submission.budget.clone(),
        );
        let preparation = self.port.prepare(&authorized);
        if preparation == PrepareOutcome::TransientFailure {
            return Err("transient failure before dispatch".into());
        }
        drop(state);
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
        let mut provider_commit = CommitObservation::Unknown;
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
            let fresh_at = self.clock.now()?;
            let fresh = verify_snapshot(
                root,
                &self.evidence,
                caller.tenant(),
                &current.intent.submission,
                &fresh_at,
            )?;
            let evaluation = worldinterface_domain::authorization::authorize(
                &current.intent,
                &current.agency,
                current.resolution.as_ref().ok_or("no resolution")?,
                &fresh,
                &fresh_at,
            )?;
            let refused = preparation == PrepareOutcome::PermanentRejection
                || evaluation.decision != worldinterface_protocol::Decision::Allow
                || Some(&evaluation) != current.authorization.as_ref()
                || check_installed(&self.port, authorized.execution()).is_err()
                || self
                    .port
                    .resolve(ResolveRequest {
                        selector: current.intent.submission.resource.clone(),
                        resource: authorized.resolved.resource.clone(),
                    })
                    .map_or(true, |r| r.resource != authorized.resolved.resource);
            let final_state = root.effect_snapshot(caller.tenant())?;
            let final_effect =
                final_state.effects.get(&id).ok_or("missing final invocation fence")?;
            if final_effect.phase != Phase::Running
                || final_effect.attempt.as_ref() != Some(&attempt)
                || final_effect.outcomes.iter().any(|o| o.attempt == attempt)
            {
                ctx.input.cancellation_context.cancel();
                return self
                    .committed_disposition(&caller, &id, &run)?
                    .ok_or_else(|| "lost final invocation fence".into());
            }
            let observation = if refused {
                PortObservation {
                    commit: CommitObservation::ProvenImpossible,
                    outcome: ExecutionOutcome::FailedBeforeCommit,
                    outputs: vec![],
                    evidence: vec![EvidenceBytes {
                        schema: "wi.pre-dispatch:1".to_string().try_into()?,
                        bytes: b"dispatch rejected before invocation".to_vec(),
                    }],
                    boundary_record: None,
                }
            } else if ctx.input.cancellation_context.token().is_cancelled() {
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
                self.port
                    .invoke(
                        PreparedInvocation::new(authorized),
                        ctx.input.cancellation_context.token(),
                    )
                    .unwrap_or(uncertainty(
                        "wi.port.uncertainty:1",
                        b"port did not return commit evidence",
                    )?)
            };
            provider_commit = observation.commit.clone();
            let mut observation = observation;
            if observation.outcome == ExecutionOutcome::TerminalFailure
                && !matches!(
                    observation.commit,
                    CommitObservation::ConfirmedCommitted
                        | CommitObservation::ConfirmedNotCommitted
                )
            {
                observation.outcome = ExecutionOutcome::Uncertain;
                observation.commit = CommitObservation::Unknown;
            }
            if observation.commit == CommitObservation::ConfirmedCommitted
                && observation.outcome == ExecutionOutcome::Succeeded
                && (observation.boundary_record.is_none()
                    || observation.outputs.len()
                        != current
                            .authorization_inputs
                            .as_ref()
                            .ok_or("no manifest")?
                            .required_outputs
                            .len())
            {
                let mut incomplete = uncertainty(
                    "wi.completion.uncertainty:1",
                    b"confirmed commit lacks required publication",
                )?;
                incomplete.commit = CommitObservation::ConfirmedCommitted;
                return self.record_outcome(
                    &caller,
                    &id,
                    attempt.clone(),
                    &run,
                    incomplete,
                    "fallback",
                );
            }
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
                    {
                        let mut incomplete = uncertainty(
                            "wi.completion.uncertainty:1",
                            b"completion could not be durably recorded after Start",
                        )?;
                        if provider_commit == CommitObservation::ConfirmedCommitted {
                            incomplete.commit = CommitObservation::ConfirmedCommitted;
                        }
                        incomplete
                    },
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
            let mut outcome = OutcomeEvidence {
                classification: Some(
                    worldinterface_protocol::ExecutorClassification::AmbiguousCommit,
                ),
                reconciliation_admission: None,
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
            outcome.reconciliation_admission =
                crate::reconciliation::admission_bytes(effect, &outcome, state.revision + 1, &at)?;
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
                classification: Some(
                    worldinterface_protocol::ExecutorClassification::AmbiguousCommit,
                ),
                connector_execution: effect
                    .resolution
                    .as_ref()
                    .and_then(|r| r.resolved.connector_execution.clone())
                    .map(Box::new),
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
        P: Connector + 'static,
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
