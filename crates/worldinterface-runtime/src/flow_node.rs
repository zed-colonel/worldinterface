//! One handler for both plan entry points; pure nodes never cross a boundary.
use crate::flow::{is_rejected, rejected};
use crate::{
    artifact::{ArtifactAuthorization, ArtifactResolver},
    authority::{verify_snapshot, EvidenceAuthority, TrustedClock},
    completion::EffectAttemptHandler,
    connector::Connector,
    flow::{transact, FlowPayload},
    flow_coordinator::{load, terminal},
    resolution::{require_current, ResourceObserver},
    Result,
};
use actionqueue_core::{
    admission::EnsureTaskRequest,
    disposition::{AttemptDisposition, DispositionOutcome},
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use serde_json::Value;
use std::collections::BTreeMap;
use worldinterface_domain::{
    agency::{resolve_agency, AuthenticatedCaller},
    effect::{
        admission_request, ChildAdmissionProvenance, EffectAction, EffectOwner, ProductionAdmission,
    },
    flow::*,
    Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::{
    canonical_data, strict_json, ArtifactRef, ContentHash, EffectIntentId, ExecutionOutcome,
    OpaqueRef,
};
use worldinterface_storage::{flow::*, StoreRoot};

pub struct FlowAttemptHandler<P, V, R, C> {
    pub effect: EffectAttemptHandler<P, V, C>,
    pub resolver: R,
    pub artifacts: std::sync::Arc<
        dyn ArtifactAuthorization<AuthenticatedContext = AuthenticatedCaller> + Send + Sync,
    >,
}
pub(crate) fn selected<'a>(
    r: &'a FlowRun,
    branch: &OpaqueRef,
    yes: &'a OutputRef,
    no: &'a OutputRef,
) -> Result<&'a OutputRef> {
    let p = r.restore()?;
    let n = p
        .document()
        .body
        .nodes
        .iter()
        .find(|n| &n.id == branch)
        .ok_or_else(|| rejected("unknown branch"))?;
    let PlanOperation::Branch { when_true, when_false, .. } = &n.operation else {
        return Err(rejected("not branch"));
    };
    let result = r.results.get(branch).ok_or_else(|| rejected("unsettled branch"))?;
    if result.disposition != NodeDisposition::Succeeded {
        return Err(rejected("failed branch"));
    }
    match result.branch.as_ref() {
        Some(v) if v == when_true => Ok(yes),
        Some(v) if v == when_false => Ok(no),
        _ => Err(rejected("invalid branch trace")),
    }
}
fn output(r: &FlowRun, source: &OutputRef) -> Result<(Value, Option<ArtifactRef>)> {
    let v = r
        .results
        .get(&source.node)
        .filter(|r| r.disposition == NodeDisposition::Succeeded)
        .ok_or_else(|| rejected("selected producer has no successful output"))?;
    Ok((
        v.values.get(&source.port).ok_or_else(|| rejected("missing producer value"))?.clone(),
        v.outputs.get(&source.port).cloned(),
    ))
}
pub(crate) fn materialize(
    root: &StoreRoot,
    r: &FlowRun,
    source: &InputSource,
    authorization: &dyn ArtifactAuthorization<AuthenticatedContext = AuthenticatedCaller>,
) -> Result<(Value, Option<ArtifactRef>)> {
    let caller =
        AuthenticatedCaller::from_trusted_authentication(r.tenant.clone(), r.principal.clone());
    let resolver = ArtifactResolver::new(root, authorization);
    let result = match source {
        InputSource::Literal { value } => Ok((value.clone(), None)),
        InputSource::Artifact { reference } => {
            if reference.scope != r.tenant {
                return Err(rejected("foreign input artifact"));
            }
            let b = resolver.read(&caller, reference)?;
            // Opaque bytes remain artifacts and are validated by the connector contract.
            let value = strict_json(&b).unwrap_or(serde_json::to_value(reference)?);
            Ok((value, Some(reference.clone())))
        }
        InputSource::Output { source } => output(r, source),
        InputSource::Exclusive { branch, when_true, when_false } => {
            output(r, selected(r, branch, when_true, when_false)?)
        }
    }?;
    if let Some(reference) = &result.1 {
        resolver.read(&caller, reference)?;
    }
    Ok(result)
}
pub(crate) fn empty_result(disposition: NodeDisposition) -> NodeResult {
    NodeResult {
        disposition,
        values: BTreeMap::new(),
        outputs: BTreeMap::new(),
        branch: None,
        input_hash: ContentHash::of(b""),
        output_hash: ContentHash::of(b""),
        program: None,
        effect: None,
    }
}
pub(crate) fn binding_wait(r: &FlowRun, node: &OpaqueRef) -> Result<AttemptDisposition> {
    crate::flow_coordinator::binding_wait(&r.node_tasks[node])
}

impl<
        P: Connector,
        V: EvidenceAuthority + Send + Sync + 'static,
        R: ResourceObserver + Send + Sync,
        C: TrustedClock + Send + Sync + 'static,
    > FlowAttemptHandler<P, V, R, C>
{
    fn node(
        &self,
        r: &FlowRun,
        node: &OpaqueRef,
        ctx: &ExecutorContext,
    ) -> Result<AttemptDisposition> {
        let root = self.effect.bundle.wi();
        let p = r.restore()?;
        let caller =
            AuthenticatedCaller::from_trusted_authentication(r.tenant.clone(), r.principal.clone());
        let artifact_reader = ArtifactResolver::new(root, self.artifacts.as_ref());
        let n = p
            .document()
            .body
            .nodes
            .iter()
            .find(|n| &n.id == node)
            .ok_or_else(|| rejected("unknown Flow node"))?;
        let Some(binding) = r.bindings.get(node) else {
            return binding_wait(r, node);
        };
        if binding.run.as_str() != ctx.input.run_id.to_string()
            || binding.queue_store.as_str() != self.effect.bundle.aq_store_id()?.to_string()
        {
            return Err(rejected("Flow child run mismatch"));
        }
        let request: EnsureTaskRequest =
            serde_json::from_slice(&root.read_artifact(&binding.request)?).map_err(rejected)?;
        if request.task_spec().task_payload().bytes() != ctx.input.payload
            || request.task_spec().id().to_string() != r.node_tasks[node].as_str()
        {
            return Err(rejected("Flow child payload mismatch"));
        }
        if matches!(n.operation, PlanOperation::Wait { .. } | PlanOperation::Timer { .. }) {
            if let Some(disposition) = crate::flow_wait::accepted(r, ctx)? {
                return Ok(disposition);
            }
        }
        if let Some(result) = r.results.get(node) {
            if result.effect.is_some() && result.disposition == NodeDisposition::Succeeded {
                let state = root.effect_snapshot(&r.tenant)?;
                let effect = state
                    .effects
                    .get(result.effect.as_ref().unwrap())
                    .ok_or_else(|| rejected("missing Flow effect"))?;
                if effect.phase != Phase::Succeeded
                    || effect
                        .outcomes
                        .last()
                        .is_none_or(|o| o.outcome != ExecutionOutcome::Succeeded)
                {
                    return Ok(terminal("wi.flow.result_evidence_mismatch"));
                }
                artifact_reader.read(&caller, &effect.intent.submission.input.artifact)?;
                return Ok(self.effect.execute_context(ctx));
            }
            return Ok(
                if matches!(
                    result.disposition,
                    NodeDisposition::Succeeded | NodeDisposition::Skipped
                ) {
                    AttemptDisposition::complete(None)
                } else {
                    terminal("wi.flow.node_failure")
                },
            );
        }
        if r.outcome.is_some() || r.cancel_requested {
            return Ok(terminal("wi.flow.closed"));
        }
        // All dependencies must have domain results. AQ task success is insufficient.
        for dependency in &n.dependencies {
            let Some(result) = r.results.get(dependency) else {
                return Err(rejected("missing dependency domain result"));
            };
            if !matches!(result.disposition, NodeDisposition::Succeeded | NodeDisposition::Skipped)
            {
                return self.record(r, node, empty_result(result.disposition.clone()));
            }
        }
        for (branch, expected) in &n.guards {
            let result = r.results.get(branch).ok_or_else(|| rejected("unsettled guard"))?;
            if result.disposition != NodeDisposition::Succeeded {
                return self.record(r, node, empty_result(NodeDisposition::Failed));
            }
            if result.branch.as_ref() != Some(expected) {
                return self.record(r, node, empty_result(NodeDisposition::Skipped));
            }
        }
        let mut inputs = BTreeMap::new();
        let mut artifacts = BTreeMap::new();
        for (port, input) in &n.inputs {
            let (value, artifact) = materialize(root, r, &input.source, self.artifacts.as_ref())?;
            if let Some(a) = artifact {
                artifact_reader.read(&caller, &a)?;
                artifacts.insert(port.clone(), a);
            }
            inputs.insert(port.clone(), value);
        }
        // JSON schemas must validate the actual bytes delivered to a connector.
        for (port, artifact) in &artifacts {
            if !matches!(
                p.document().schemas[&n.inputs[port].schema].schema,
                Schema::Artifact { .. }
            ) {
                inputs.insert(
                    port.clone(),
                    strict_json(&artifact_reader.read(&caller, artifact)?)
                        .map_err(|_| rejected("input JSON schema requires JSON bytes"))?,
                );
            }
        }
        match &n.operation {
            PlanOperation::Transform { program } | PlanOperation::Branch { program, .. } => {
                let eval = p.evaluate(node, &inputs)?;
                let mut result = empty_result(NodeDisposition::Succeeded);
                result.input_hash = eval.input_hash;
                result.output_hash = eval.output_hash;
                result.branch = eval.branch_outcome;
                result.program = Some(program.clone());
                if matches!(n.operation, PlanOperation::Transform { .. }) {
                    let port: OpaqueRef = "value".to_string().try_into()?;
                    let artifact = root.publish(r.tenant.clone(), &canonical_data(&eval.value)?)?;
                    result.values.insert(port.clone(), eval.value);
                    result.outputs.insert(port, artifact);
                }
                self.record(r, node, result)
            }
            PlanOperation::Boundary { binding: connector, resource, policy } => {
                let resource = match resource {
                    ResourceBinding::Fixed { resource } => resource.clone(),
                    ResourceBinding::Produced { producers } => serde_json::from_value(
                        materialize(
                            root,
                            r,
                            &InputSource::Output { source: producers[0].clone() },
                            self.artifacts.as_ref(),
                        )?
                        .0,
                    )?,
                    ResourceBinding::Exclusive { branch, when_true, when_false } => {
                        serde_json::from_value(
                            materialize(
                                root,
                                r,
                                &InputSource::Output {
                                    source: selected(r, branch, when_true, when_false)?.clone(),
                                },
                                self.artifacts.as_ref(),
                            )?
                            .0,
                        )?
                    }
                };
                let exact = AuthorityAlternative {
                    connector: connector.descriptor.connector.clone(),
                    operation: connector.descriptor.operation.clone(),
                    effect: connector.descriptor.effect_class.clone(),
                    resource: resource.clone(),
                };
                if ![
                    &n.authority,
                    &p.document().body.authority,
                    &p.document().run_authority,
                    &p.document().submitting_authority,
                ]
                .iter()
                .all(|a| a.alternatives.contains(&exact))
                {
                    return Err(rejected("Flow authority enlargement"));
                }
                if self.effect.port.check_binding(connector).is_err()
                    || connector.registry_digest != p.document().registry_digest
                {
                    return Err(rejected("Flow installed connector mismatch"));
                }
                if n.inputs.len() != 1 || n.outputs.len() != 1 {
                    return Err(rejected("boundary port profile"));
                }
                let (port, input) = n.inputs.first_key_value().unwrap();
                let artifact = if let Some(a) = artifacts.get(port) {
                    a.clone()
                } else {
                    root.publish(r.tenant.clone(), &canonical_data(&inputs[port])?)?
                };
                let bytes = artifact_reader.read(&caller, &artifact)?;
                let schema = &p.document().schemas[&input.schema].schema;
                match schema {
                    Schema::Artifact { max_bytes, schema_hash } if artifact.size <= *max_bytes => {
                        self.effect.port.validate_artifact(schema_hash, &bytes)?
                    }
                    Schema::Artifact { .. } => return Err(rejected("boundary artifact size")),
                    _ if !validate_value(schema, &inputs[port]) => {
                        return Err(rejected("boundary input schema"))
                    }
                    _ => {}
                }
                if let InputSource::Artifact { .. } = input.source {
                    p.verify_input_artifact(node, port, &bytes)?;
                }
                let mut submission = r.submission.clone();
                if !matches!(r.origin, PlanOrigin::Direct) {
                    if p.document().body.nodes.len() > 1 {
                        submission.client_request_id =
                            crate::flow::node_request_id(&r.submission.client_request_id, node)?;
                    }
                    submission.resource = resource;
                    submission.effect_class = connector.descriptor.effect_class.clone();
                    submission.input.artifact = artifact;
                    submission.budget = policy.budget.clone();
                    submission.deadline = policy.deadline.clone();
                    submission.requested_receipt_profile = policy.receipt_profile.clone();
                    submission.expected_outcome = policy.expected_outcome.clone();
                }
                let caller = AuthenticatedCaller::from_trusted_authentication(
                    r.tenant.clone(),
                    r.principal.clone(),
                );
                let state = root.effect_snapshot(&r.tenant)?;
                if let Some(effect) = state
                    .effects
                    .values()
                    .find(|e| e.owner.as_ref().is_some_and(|o| o.flow == r.id && &o.node == node))
                {
                    if !effect.outcomes.is_empty() || effect.phase == Phase::Running {
                        return self.boundary_result(
                            r,
                            node,
                            n,
                            &p,
                            ctx,
                            effect.intent.id.clone(),
                            &bytes,
                        );
                    }
                }
                drop(state);
                let at = self.effect.clock.now()?;
                let snapshot =
                    verify_snapshot(root, &self.effect.evidence, &r.tenant, &submission, &at)?;
                let agency = resolve_agency(&caller, &submission, &snapshot.facts().bindings, &at)?;
                if agency.context != r.agency.context
                    || agency.binding.id != r.agency.binding.id
                    || agency.binding.version != r.agency.binding.version
                {
                    return Err(rejected("Flow admission attribution changed"));
                }
                // Registry changes prevent dispatch even if the descriptor is still installed.
                if !snapshot
                    .facts()
                    .registry
                    .entries
                    .iter()
                    .any(|e| e.connector_binding.as_ref() == Some(connector.as_ref()))
                {
                    return Err(rejected("Flow registry changed"));
                }
                let egress = Egress::new(root);
                let id = egress.apply(
                    &caller,
                    at.clone(),
                    EffectAction::SubmitFlow {
                        id: EffectIntentId::generate(),
                        submission: Box::new(submission.clone()),
                        snapshot: snapshot.clone(),
                        owner: EffectOwner { flow: r.id.clone(), node: node.clone() },
                    },
                )?;
                let owned = root.effect_snapshot(&r.tenant)?;
                if owned.effects.get(&id).and_then(|e| e.owner.as_ref())
                    != Some(&EffectOwner { flow: r.id.clone(), node: node.clone() })
                {
                    return Err(rejected("Flow effect ownership conflict"));
                }
                drop(owned);
                let mut view = egress.inspect(&caller, &id)?;
                if view.phase == Phase::Submitted {
                    let fact = self.resolver.observe(
                        &r.tenant,
                        &submission,
                        &snapshot.facts().registry,
                        &at,
                    )?;
                    if !snapshot.facts().facts.contains(&fact) {
                        return Err(rejected("Flow resource observation mismatch"));
                    }
                    egress.apply(
                        &caller,
                        at.clone(),
                        EffectAction::Resolve { id: id.clone(), snapshot: snapshot.clone() },
                    )?;
                    view = egress.inspect(&caller, &id)?;
                }
                if matches!(
                    view.phase,
                    Phase::Resolving | Phase::AwaitingApproval | Phase::Authorized
                ) {
                    if let Some(resolution) = &view.resolution {
                        require_current(&self.resolver, &resolution.fact)?;
                    }
                    egress.apply(
                        &caller,
                        at.clone(),
                        EffectAction::Authorize { id: id.clone(), snapshot: snapshot.clone() },
                    )?;
                    view = egress.inspect(&caller, &id)?;
                }
                if matches!(view.phase, Phase::Denied | Phase::AwaitingApproval) {
                    let mut result = empty_result(NodeDisposition::Denied);
                    result.effect = Some(id);
                    return self.record(r, node, result);
                }
                if view.phase == Phase::Authorized {
                    let state = root.effect_snapshot(&r.tenant)?;
                    let effect = &state.effects[&id];
                    let boundary_request = admission_request(
                        &r.tenant,
                        effect,
                        snapshot.facts(),
                        effect.dispatches.len() as u32,
                    )?;
                    let execution = boundary_request.execution;
                    let b = root.read_artifact(&binding.request)?;
                    let admission = ProductionAdmission {
                        child: Some(ChildAdmissionProvenance {
                            admission_key: request.admission_key().as_str().to_string(),
                            version: 1,
                            flow: r.id.clone(),
                            node: node.clone(),
                            parent_task: binding.parent_task.clone(),
                            parent_run: binding.parent_run.clone(),
                            producing_attempt: binding.producing_attempt.clone(),
                            disposition_hash: binding.disposition_hash.clone(),
                        }),
                        version: 1,
                        queue_store: r.queue_store.clone(),
                        task: r.node_tasks[node].clone(),
                        admission_key: boundary_request.admission_key,
                        execution,
                        request_artifact: binding.request.clone(),
                        request_hash: ContentHash::of(&b),
                        canonical_version: request.digest()?.canonical_version(),
                        canonical_digest: hex::encode(request.digest()?.hash().bytes())
                            .try_into()?,
                        acknowledged_sequence: None,
                        observed_runs: vec![],
                        discovery_cursor: None,
                    };
                    egress.apply(
                        &caller,
                        self.effect.clock.now()?,
                        EffectAction::PrepareProduction {
                            id: id.clone(),
                            snapshot: snapshot.clone(),
                            admission: Box::new(admission),
                        },
                    )?;
                }
                let view = egress.inspect(&caller, &id)?;
                if view.phase == Phase::DispatchPending {
                    egress.apply(
                        &caller,
                        self.effect.clock.now()?,
                        EffectAction::AcknowledgeProduction {
                            id: id.clone(),
                            sequence: binding.admission_sequence,
                        },
                    )?;
                    egress.apply(
                        &caller,
                        self.effect.clock.now()?,
                        EffectAction::ObserveRun { id: id.clone(), run: binding.run.clone() },
                    )?;
                }
                artifact_reader.read(&caller, &submission.input.artifact)?;
                self.boundary_result(r, node, n, &p, ctx, id, &bytes)
            }
            PlanOperation::Wait { .. } | PlanOperation::Timer { .. } => {
                crate::flow_wait::execute(root, r, n, ctx, self.artifacts.as_ref())
            }
        }
    }
    fn boundary_result(
        &self,
        r: &FlowRun,
        node: &OpaqueRef,
        n: &PlanNode,
        p: &ExecutionPlan,
        ctx: &ExecutorContext,
        id: EffectIntentId,
        bytes: &[u8],
    ) -> Result<AttemptDisposition> {
        let root = self.effect.bundle.wi();
        let caller =
            AuthenticatedCaller::from_trusted_authentication(r.tenant.clone(), r.principal.clone());
        let artifact_reader = ArtifactResolver::new(root, self.artifacts.as_ref());
        let egress = Egress::new(root);
        let disposition = self.effect.execute_context(ctx);
        if matches!(
            disposition.outcome(),
            DispositionOutcome::RetryableFailure { .. }
                | DispositionOutcome::Awaiting
                | DispositionOutcome::Suspended { .. }
        ) {
            return Ok(disposition);
        }
        let view = egress.inspect(&caller, &id)?;
        let status = match view.outcome.as_ref().map(|o| &o.outcome) {
            Some(ExecutionOutcome::Succeeded) => NodeDisposition::Succeeded,
            Some(ExecutionOutcome::Uncertain) => NodeDisposition::Uncertain,
            Some(ExecutionOutcome::CanceledBeforeCommit) => NodeDisposition::Canceled,
            None if view.phase == Phase::Running
                || view.commit == worldinterface_domain::effect::CommitObservation::Unknown =>
            {
                NodeDisposition::Uncertain
            }
            _ => NodeDisposition::Failed,
        };
        let mut result = empty_result(status);
        result.effect = Some(id);
        result.input_hash = ContentHash::of(bytes);
        if result.disposition == NodeDisposition::Succeeded {
            let outcome = view.outcome.ok_or_else(|| rejected("missing boundary outcome"))?;
            if outcome.required_outputs != outcome.durable_outputs
                || outcome.durable_outputs.len() != n.outputs.len()
                || root.effect_receipts(&r.tenant, result.effect.as_ref().unwrap())?.is_empty()
            {
                return Err(rejected("unpublished Flow boundary completion"));
            }
            for ((port, output), artifact) in n.outputs.iter().zip(outcome.durable_outputs) {
                let b = artifact_reader.read(&caller, &artifact)?;
                let value = match &p.document().schemas[&output.schema].schema {
                    Schema::Artifact { max_bytes, schema_hash } if artifact.size <= *max_bytes => {
                        self.effect.port.validate_artifact(schema_hash, &b)?;
                        serde_json::to_value(&artifact)?
                    }
                    Schema::Artifact { .. } => {
                        return Err(rejected("boundary artifact output size"))
                    }
                    schema => {
                        let value = strict_json(&b)?;
                        if !validate_value(schema, &value) {
                            return Err(rejected("boundary output schema"));
                        }
                        value
                    }
                };
                result.values.insert(port.clone(), value);
                result.outputs.insert(port.clone(), artifact);
            }
            result.output_hash = worldinterface_protocol::data_digest(&result.values)?;
        }
        self.record(r, node, result)?;
        Ok(disposition)
    }
    fn record(
        &self,
        r: &FlowRun,
        node: &OpaqueRef,
        result: NodeResult,
    ) -> Result<AttemptDisposition> {
        let good =
            matches!(result.disposition, NodeDisposition::Succeeded | NodeDisposition::Skipped);
        transact(
            self.effect.bundle.wi(),
            &r.tenant,
            FlowCommand::RecordNodeResult { run: r.id.clone(), node: node.clone(), result },
        )?;
        Ok(if good { AttemptDisposition::complete(None) } else { terminal("wi.flow.node_failure") })
    }
    pub fn execute_bound(&self, ctx: &ExecutorContext) -> Result<AttemptDisposition> {
        let payload = FlowPayload::decode(&ctx.input.payload)?;
        let r = load(self.effect.bundle.wi(), &payload)?;
        // Replay is a lookup of an immutable accepted result. New authority is
        // required only when evaluating a context that has not been accepted.
        let bound = payload
            .node
            .as_ref()
            .map_or(r.parent_run.as_ref(), |n| r.bindings.get(n).map(|b| &b.run));
        if bound.is_some_and(|run| run.as_str() == ctx.input.run_id.to_string()) {
            if let Some(disposition) = crate::flow_wait::accepted(&r, ctx)? {
                return Ok(disposition);
            }
        }
        if ctx.input.resume_context.is_some() {
            let at = self.effect.clock.now()?;
            let snapshot = verify_snapshot(
                self.effect.bundle.wi(),
                &self.effect.evidence,
                &r.tenant,
                &r.submission,
                &at,
            )?;
            let caller = AuthenticatedCaller::from_trusted_authentication(
                r.tenant.clone(),
                r.principal.clone(),
            );
            let agency = resolve_agency(&caller, &r.submission, &snapshot.facts().bindings, &at)
                .map_err(rejected)?;
            if agency != r.agency {
                return Err(rejected("Flow agency changed at resumption"));
            }
            ArtifactResolver::new(self.effect.bundle.wi(), self.artifacts.as_ref())
                .read(&caller, &r.submission.input.artifact)?;
        }
        if let Some(node) = payload.node {
            self.node(&r, &node, ctx)
        } else {
            crate::flow_coordinator::coordinate(
                self.effect.bundle.wi(),
                &r,
                ctx,
                self.artifacts.as_ref(),
            )
        }
    }
}
impl<
        P: Connector,
        V: EvidenceAuthority + Send + Sync + 'static,
        R: ResourceObserver + Send + Sync,
        C: TrustedClock + Send + Sync + 'static,
    > ExecutorHandler for FlowAttemptHandler<P, V, R, C>
{
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        self.execute_bound(&ctx).unwrap_or_else(|error| {
            // Unknown host/storage failures are retryable. Only typed contract
            // validation can make an AQ attempt terminal.
            if is_rejected(error.as_ref()) {
                terminal("wi.flow.execution_rejected")
            } else {
                crate::flow_coordinator::retry("wi.flow.pre_start_or_publication_retry")
            }
        })
    }
}

/// Called with the Flow dispatch guard before Start, and again before invocation.
pub(crate) fn require_dispatch_open(
    root: &StoreRoot,
    tenant: &OpaqueRef,
    id: &EffectIntentId,
) -> Result<()> {
    let effects = root.effect_snapshot(tenant)?;
    let effect = effects.effects.get(id).ok_or_else(|| rejected("missing dispatch effect"))?;
    if let Some(owner) = &effect.owner {
        let state = root.flow_run_snapshot(tenant, &owner.flow)?;
        let run = state.runs.get(&owner.flow).ok_or_else(|| rejected("missing owning Flow"))?;
        if run.outcome.is_some()
            || run.cancel_requested
            || state.quarantined.contains_key(&owner.flow)
            || run
                .results
                .get(&owner.node)
                .is_some_and(|r| r.disposition != NodeDisposition::Succeeded)
        {
            return Err(rejected("Flow dispatch closed"));
        }
    }
    Ok(())
}
