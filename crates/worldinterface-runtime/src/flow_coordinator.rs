//! Flat bounded transactional AQ children, reconciled through the live engine.
use crate::{
    flow::{constraints, transact, FlowPayload},
    Result,
};
use actionqueue_core::{
    admission::EnsureTaskRequest,
    bounded::{BoundedCode, BoundedError, BoundedMessage},
    causal::CausalOverride,
    continuation::*,
    disposition::*,
    ids::*,
    task::{
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_executor_local::handler::ExecutorContext;
use worldinterface_protocol::{canonical_data, ContentHash, OpaqueRef};
use worldinterface_storage::{flow::*, StoreRoot};

pub(crate) fn terminal(code: &str) -> AttemptDisposition {
    AttemptDisposition::terminal_failure(BoundedError {
        code: BoundedCode::new(code).expect("static code"),
        message: BoundedMessage::new(code).expect("static message"),
    })
}
pub(crate) fn load(root: &StoreRoot, payload: &FlowPayload) -> Result<FlowRun> {
    let state = root.flow_snapshot(&payload.tenant)?;
    let r = state.runs.get(&payload.run).ok_or("unknown Flow run")?;
    if r.tenant != payload.tenant {
        return Err("foreign Flow run".into());
    }
    r.restore()?;
    Ok(r.clone())
}
pub(crate) fn children(r: &FlowRun) -> Result<Vec<ChildAdmission>> {
    let p = r.restore()?;
    p.document()
        .body
        .nodes
        .iter()
        .map(|n| {
            let spec = TaskSpec::new(
                r.node_tasks[&n.id].as_str().parse()?,
                TaskPayload::new(canonical_data(&FlowPayload::new(r, Some(n.id.clone())))?),
                RunPolicy::Once,
                constraints()?,
                TaskMetadata::default(),
            )?
            .with_parent(r.coordinator_task.as_str().parse()?);
            Ok(ChildAdmission::new(
                AdmissionKey::new(format!("node:{}", r.node_tasks[&n.id].as_str()))?,
                spec,
                n.dependencies
                    .iter()
                    .map(|d| Ok(r.node_tasks[d].as_str().parse()?))
                    .collect::<Result<_>>()?,
                CausalOverride::default(),
            )?)
        })
        .collect()
}
pub(crate) fn coordinate(
    root: &StoreRoot,
    r: &FlowRun,
    ctx: &ExecutorContext,
    artifacts: &dyn crate::artifact::ArtifactAuthorization<
        AuthenticatedContext = worldinterface_domain::agency::AuthenticatedCaller,
    >,
) -> Result<AttemptDisposition> {
    let p = r.restore()?;
    let caller = worldinterface_domain::agency::AuthenticatedCaller::from_trusted_authentication(
        r.tenant.clone(),
        r.principal.clone(),
    );
    let artifact_reader = crate::artifact::ArtifactResolver::new(root, artifacts);
    if r.parent_run.as_ref().map(OpaqueRef::as_str) != Some(ctx.input.run_id.to_string().as_str()) {
        return Err("unbound coordinator run".into());
    }
    if r.outcome.is_some() {
        return Ok(if r.outcome == Some(FlowOutcome::Succeeded) {
            AttemptDisposition::complete(None)
        } else {
            terminal("wi.flow.domain_failure")
        });
    }
    // A wake only requests a domain evaluation. It never proves business success.
    if ctx.input.resume_context.is_some() {
        let outcome = domain_outcome(root, r, FlowOutcome::Failed)?;
        let mut exports = std::collections::BTreeMap::new();
        if outcome == FlowOutcome::Succeeded {
            for (name, input) in &p.document().body.outputs {
                let (value, artifact) =
                    crate::flow_node::materialize(root, r, &input.source, artifacts)?;
                if !matches!(
                    p.document().schemas[&input.schema].schema,
                    worldinterface_domain::flow::Schema::Artifact { .. }
                ) && !worldinterface_domain::flow::validate_value(
                    &p.document().schemas[&input.schema].schema,
                    &value,
                ) {
                    return Err("export schema mismatch".into());
                }
                let artifact = artifact
                    .map_or_else(|| root.publish(r.tenant.clone(), &canonical_data(&value)?), Ok)?;
                artifact_reader.read(&caller, &artifact)?;
                exports.insert(name.clone(), artifact);
            }
        }
        transact(
            root,
            &r.tenant,
            FlowCommand::Finish { run: r.id.clone(), outcome: outcome.clone(), exports },
        )?;
        return Ok(if outcome == FlowOutcome::Succeeded {
            AttemptDisposition::complete(None)
        } else {
            terminal("wi.flow.domain_failure")
        });
    }
    let child_admissions = children(r)?;
    let proposal = serde_json::to_vec(&child_admissions)?;
    transact(
        root,
        &r.tenant,
        FlowCommand::PrepareChildren {
            run: r.id.clone(),
            parent_run: ctx.input.run_id.to_string().try_into()?,
            proposal,
        },
    )?;
    let checkpoint = CheckpointRef {
        checkpoint_id: CheckpointId::new(),
        created_by_attempt: ctx.input.attempt_id,
        data: actionqueue_core::data_ref::DataRef::External(
            crate::aq_adapter::TaskInputLocator::new(
                root.publish(r.tenant.clone(), r.plan_hash.as_str().as_bytes())?,
            )
            .external()?,
        ),
    };
    Ok(AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            child_admissions,
            checkpoint: Some(checkpoint),
            wait: Some(WaitSpec::children(
                r.wait_id.as_str().parse()?,
                r.node_tasks.values().map(|v| Ok(v.as_str().parse()?)).collect::<Result<_>>()?,
                ChildWaitPolicy::AllTerminal,
                None,
            )?),
            ..Default::default()
        },
    )?)
}

/// Verified bindings come from AQ's accepted public projection, not task payload claims.
fn readiness_signal(r: &FlowRun, task: TaskId) -> Result<AdmitSignalRequest> {
    Ok(AdmitSignalRequest::new(
        SignalId::new(format!("wi-flow-ready:{}:{}", r.id.as_str(), task))?,
        SignalNamespace::new("wi.flow")?,
        SignalKind::new("binding-ready")?,
        Some(CorrelationId::new(format!("wi-ready:{task}"))?),
        None,
        None,
        None,
        None,
        None,
    )?)
}
pub(crate) fn reconcile<H: actionqueue_executor_local::handler::ExecutorHandler + 'static>(
    root: &StoreRoot,
    store: uuid::Uuid,
    engine: &mut actionqueue_runtime::engine::BootstrappedEngine<H>,
) -> Result<()> {
    validate_references(root, store, engine.projection())?;
    for tenant in root.flow_tenants()? {
        let state = root.flow_snapshot(&tenant)?;
        for r in state.runs.values() {
            r.restore()?;
            if r.queue_store.as_str() != store.to_string() {
                return Err("Flow queue store mismatch".into());
            }
            let parent: TaskId = r.coordinator_task.as_str().parse()?;
            let expected: EnsureTaskRequest = serde_json::from_slice(&r.coordinator_request)?;
            // A crash after WI admission but before AQ admission retains this outbox.
            if engine.projection().task_admission(parent).is_none() {
                engine.ensure_task(expected.clone())?;
            }
            let projection = engine.projection();
            let admission =
                projection.task_admission(parent).ok_or("missing Flow coordinator admission")?;
            if admission.digest() != &expected.digest()? {
                return Err("Flow coordinator admission changed".into());
            }
            let runs = projection.run_ids_for_task(parent);
            if runs.len() != 1 {
                return Err("Flow coordinator run cardinality".into());
            }
            let parent_run = runs[0];
            transact(
                root,
                &tenant,
                FlowCommand::BindCoordinator {
                    run: r.id.clone(),
                    parent_run: parent_run.to_string().try_into()?,
                },
            )?;
            if projection.get_run_state(&parent_run).is_some_and(|s| s.is_terminal())
                && r.outcome.is_none()
            {
                let fallback = if projection.get_run_state(&parent_run)
                    == Some(&actionqueue_core::run::RunState::Canceled)
                {
                    FlowOutcome::Canceled
                } else {
                    FlowOutcome::Failed
                };
                let outcome = domain_outcome(root, r, fallback)?;
                // AQ completion without a WI publication is never promoted to success.
                let outcome =
                    if outcome == FlowOutcome::Succeeded { FlowOutcome::Failed } else { outcome };
                transact(
                    root,
                    &tenant,
                    FlowCommand::Finish { run: r.id.clone(), outcome, exports: Default::default() },
                )?;
            }
            let Some(proposal) = &r.proposal else { continue };
            let proposed: Vec<ChildAdmission> = serde_json::from_slice(proposal)?;
            if proposed != children(r)? {
                return Err("Flow proposal changed".into());
            }
            let accepted =
                projection.get_attempt_history(&parent_run).unwrap_or(&[]).iter().find(|a| {
                    a.disposition
                        .as_ref()
                        .is_some_and(|d| d.disposition.child_admissions() == proposed)
                });
            let Some(accepted) = accepted else { continue };
            let d = &accepted.disposition.as_ref().unwrap().disposition;
            if !matches!(d.outcome(), DispositionOutcome::Awaiting)
                || d.wait().map(|w| w.wait_id().to_string()).as_deref() != Some(r.wait_id.as_str())
            {
                return Err("Flow child transaction mismatch".into());
            }
            for child in &proposed {
                let task = child.task_spec().id();
                let actual =
                    projection.task_admission(task).ok_or("partial AQ child transaction")?;
                let expected = projection.disposition_child_request(
                    parent_run,
                    accepted.attempt_id(),
                    child,
                )?;
                if actual.digest() != &expected.digest()? {
                    return Err("Flow accepted child request mismatch".into());
                }
                let runs = projection.run_ids_for_task(task);
                if runs.len() != 1 {
                    return Err("Flow child run cardinality".into());
                }
                let payload = FlowPayload::decode(expected.task_spec().task_payload().bytes())?;
                let node = payload.node.ok_or("missing child node")?;
                if r.bindings.contains_key(&node) {
                    continue;
                }
                let binding = {
                    let wake = readiness_signal(r, task)?;
                    ChildBinding {
                        version: 1,
                        queue_store: r.queue_store.clone(),
                        parent_task: r.coordinator_task.clone(),
                        parent_run: parent_run.to_string().try_into()?,
                        producing_attempt: accepted.attempt_id().to_string().try_into()?,
                        disposition_hash: ContentHash::of(&serde_json::to_vec(d)?),
                        admission_sequence: actual.sequence(),
                        request: root
                            .publish(tenant.clone(), &serde_json::to_vec(actual.request())?)?,
                        run: runs[0].to_string().try_into()?,
                        wake_request: serde_json::to_vec(&wake)?,
                    }
                };
                transact(
                    root,
                    &tenant,
                    FlowCommand::BindChild { run: r.id.clone(), node, binding },
                )?;
            }
            // End the projection borrow before delivering pending wake obligations.
            let fresh = root.flow_snapshot(&tenant)?;
            let r = &fresh.runs[&r.id];
            for (node, binding) in &r.bindings {
                if !r.wakes_delivered.contains_key(node) {
                    let request = serde_json::from_slice(&binding.wake_request)?;
                    let sequence = engine.admit_signal(request)?.sequence().get();
                    transact(
                        root,
                        &tenant,
                        FlowCommand::WakeDelivered {
                            run: r.id.clone(),
                            node: node.clone(),
                            sequence,
                        },
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Reload boundary evidence independently of AQ terminal-success projections.
fn domain_outcome(root: &StoreRoot, r: &FlowRun, missing: FlowOutcome) -> Result<FlowOutcome> {
    let mut outcome = FlowOutcome::Succeeded;
    let effects = root.effect_snapshot(&r.tenant)?;
    let mut fold = |next: FlowOutcome| {
        let rank = |o: &FlowOutcome| match o {
            FlowOutcome::Succeeded => 0,
            FlowOutcome::Failed => 1,
            FlowOutcome::Canceled => 2,
            FlowOutcome::Denied => 3,
            FlowOutcome::Uncertain => 4,
        };
        if rank(&next) > rank(&outcome) {
            outcome = next;
        }
    };
    for node in r.node_tasks.keys() {
        match r.results.get(node) {
            Some(v) if v.disposition == NodeDisposition::Skipped => {}
            Some(v) if v.disposition == NodeDisposition::Succeeded => {
                for reference in v.outputs.values() {
                    root.read_artifact(reference)?;
                }
                if let Some(id) = &v.effect {
                    let effect = effects.effects.get(id).ok_or("Flow effect missing")?;
                    let admission = effect
                        .dispatches
                        .last()
                        .and_then(|d| d.admission.as_ref())
                        .ok_or("Flow effect admission missing")?;
                    let binding = r.bindings.get(node).ok_or("Flow effect child unbound")?;
                    if admission.child.as_ref().is_none_or(|c| c.flow != r.id || &c.node != node)
                        || !admission.observed_runs.contains(&binding.run)
                    {
                        return Err("Flow effect belongs to another child".into());
                    }
                    if effect.phase != worldinterface_domain::Phase::Succeeded
                        || effect.outcomes.last().is_none_or(|o| {
                            o.outcome != worldinterface_protocol::ExecutionOutcome::Succeeded
                                || o.required_outputs != o.durable_outputs
                        })
                        || root.effect_receipts(&r.tenant, id)?.is_empty()
                    {
                        fold(FlowOutcome::Failed);
                    }
                }
            }
            Some(v) => fold(match v.disposition {
                NodeDisposition::Uncertain => FlowOutcome::Uncertain,
                NodeDisposition::Denied => FlowOutcome::Denied,
                NodeDisposition::Canceled => FlowOutcome::Canceled,
                _ => FlowOutcome::Failed,
            }),
            None => fold(missing.clone()),
        }
    }
    // A canceled queue run cannot erase an external operation's Start or uncertainty.
    for effect in effects.effects.values() {
        if effect.dispatches.iter().any(|d| {
            d.admission.as_ref().and_then(|a| a.child.as_ref()).is_some_and(|c| c.flow == r.id)
        }) && (effect.phase == worldinterface_domain::Phase::Running
            || effect
                .outcomes
                .last()
                .is_some_and(|o| o.outcome == worldinterface_protocol::ExecutionOutcome::Uncertain))
        {
            fold(FlowOutcome::Uncertain);
        }
    }
    Ok(outcome)
}
/// Same validation is used during bundle reopen and on the active host projection.
pub(crate) fn validate_references(
    root: &StoreRoot,
    store: uuid::Uuid,
    p: &actionqueue_storage::recovery::reducer::ReplayReducer,
) -> Result<()> {
    for tenant in root.flow_tenants()? {
        for r in root.flow_snapshot(&tenant)?.runs.values() {
            r.restore()?;
            if r.queue_store.as_str() != store.to_string() {
                return Err("Flow queue identity changed".into());
            }
            let task = r.coordinator_task.as_str().parse()?;
            let expected: EnsureTaskRequest = serde_json::from_slice(&r.coordinator_request)?;
            if expected.task_spec().id() != task
                || FlowPayload::decode(expected.task_spec().task_payload().bytes())?
                    != FlowPayload::new(r, None)
            {
                return Err("Flow coordinator payload changed".into());
            }
            let admission = p.task_admission(task);
            if admission
                .is_some_and(|a| a.digest() != &expected.digest().expect("validated request"))
                || r.parent_run.is_some() && admission.is_none()
            {
                return Err("Flow coordinator admission changed".into());
            }
            if let Some(run) = &r.parent_run {
                let run_id = run.as_str().parse()?;
                if p.run_ids_for_task(task) != vec![run_id] {
                    return Err("Flow parent run changed".into());
                }
                let proposed = children(r)?;
                if r.proposal.as_ref().is_some_and(|b| {
                    serde_json::from_slice::<Vec<ChildAdmission>>(b).ok().as_ref()
                        != Some(&proposed)
                }) {
                    return Err("Flow proposal changed".into());
                }
                for (node, b) in &r.bindings {
                    let producer = p
                        .get_attempt_history(&run_id)
                        .unwrap_or(&[])
                        .iter()
                        .find(|a| a.attempt_id().to_string() == b.producing_attempt.as_str())
                        .ok_or("missing Flow producer")?;
                    let d = &producer
                        .disposition
                        .as_ref()
                        .ok_or("uncommitted Flow producer")?
                        .disposition;
                    let child = proposed
                        .iter()
                        .find(|c| c.task_spec().id().to_string() == r.node_tasks[node].as_str())
                        .ok_or("foreign Flow node")?;
                    let actual = p
                        .task_admission(child.task_spec().id())
                        .ok_or("missing Flow child admission")?;
                    let expected =
                        p.disposition_child_request(run_id, producer.attempt_id(), child)?;
                    if !matches!(d.outcome(), DispositionOutcome::Awaiting)
                        || d.wait().map(|w| w.wait_id().to_string()).as_deref()
                            != Some(r.wait_id.as_str())
                        || d.checkpoint()
                            .is_none_or(|c| c.created_by_attempt != producer.attempt_id())
                        || b.wake_request
                            != serde_json::to_vec(&readiness_signal(r, child.task_spec().id())?)?
                        || d.child_admissions() != proposed
                        || b.disposition_hash != ContentHash::of(&serde_json::to_vec(d)?)
                        || b.parent_task != r.coordinator_task
                        || &b.parent_run != run
                        || b.queue_store != r.queue_store
                        || actual.digest() != &expected.digest()?
                        || b.admission_sequence != actual.sequence()
                        || root.read_artifact(&b.request)? != serde_json::to_vec(actual.request())?
                        || p.run_ids_for_task(child.task_spec().id())
                            != vec![b.run.as_str().parse()?]
                    {
                        return Err("Flow child binding changed".into());
                    }
                }
            }
        }
    }
    Ok(())
}
