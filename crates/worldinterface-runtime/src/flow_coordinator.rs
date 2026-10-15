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
pub(crate) fn retry(code: &str) -> AttemptDisposition {
    AttemptDisposition::retryable_failure(BoundedError {
        code: BoundedCode::new(code).expect("static code"),
        message: BoundedMessage::new(code).expect("static message"),
    })
}
pub(crate) fn load(root: &StoreRoot, payload: &FlowPayload) -> Result<FlowRun> {
    let state = root.flow_snapshot(&payload.tenant)?;
    if state.quarantined.contains_key(&payload.run) {
        return Err("quarantined Flow run".into());
    }
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
                constraints(Some(match &n.operation {
                    worldinterface_domain::flow::PlanOperation::Boundary { policy, .. } => {
                        if matches!(r.origin, worldinterface_domain::flow::PlanOrigin::Direct) {
                            r.submission.budget.timeout_millis
                        } else {
                            policy.budget.timeout_millis
                        }
                    }
                    _ => 60_000,
                }))?,
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
    if r.cancel_requested {
        return Ok(terminal("wi.flow.canceled"));
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
        let guard = root.flow_dispatch_guard()?;
        let fresh = root.flow_snapshot(&r.tenant)?;
        let r = fresh.runs.get(&r.id).ok_or("missing Flow at Finish")?;
        if let Some(outcome) = &r.outcome {
            return Ok(if *outcome == FlowOutcome::Succeeded {
                AttemptDisposition::complete(None)
            } else {
                terminal("wi.flow.domain_failure")
            });
        }
        if r.cancel_requested {
            return Ok(terminal("wi.flow.closed"));
        }
        let outcome = domain_outcome(root, r, FlowOutcome::Failed)?;
        let mut exports = std::collections::BTreeMap::new();
        if outcome == FlowOutcome::Succeeded {
            for (name, input) in &p.document().body.outputs {
                let (mut value, artifact) =
                    crate::flow_node::materialize(root, r, &input.source, artifacts)?;
                if !matches!(
                    p.document().schemas[&input.schema].schema,
                    worldinterface_domain::flow::Schema::Artifact { .. }
                ) {
                    if let Some(reference) = &artifact {
                        value = worldinterface_protocol::strict_json(
                            &artifact_reader.read(&caller, reference)?,
                        )
                        .map_err(|_| "export JSON schema requires JSON bytes")?;
                    }
                }
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
        root.flow_transact_guarded(
            &guard,
            &r.tenant,
            fresh.revision,
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

pub(crate) fn reconcile<H: actionqueue_executor_local::handler::ExecutorHandler + 'static>(
    root: &StoreRoot,
    store: uuid::Uuid,
    engine: &mut actionqueue_runtime::engine::BootstrappedEngine<H>,
) -> Result<()> {
    for tenant in root.flow_tenants()? {
        let state = root.flow_snapshot(&tenant)?;
        for r in state
            .runs
            .values()
            .filter(|r| r.outcome.is_none() && !state.quarantined.contains_key(&r.id))
        {
            let result = (|| -> Result<()> {
                validate_run_references(root, store, engine.projection(), r)?;
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
                let admission = projection
                    .task_admission(parent)
                    .ok_or("missing Flow coordinator admission")?;
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
                let child_tasks = r
                    .node_tasks
                    .values()
                    .map(|t| t.as_str().parse())
                    .collect::<std::result::Result<Vec<TaskId>, _>>()?;
                if projection.get_run_state(&parent_run).is_some_and(|s| s.is_terminal())
                    && r.outcome.is_none()
                    && child_tasks.iter().all(|task| {
                        projection
                            .run_ids_for_task(*task)
                            .iter()
                            .all(|id| projection.get_run_state(id).is_some_and(|s| s.is_terminal()))
                    })
                {
                    let fallback = if projection.get_run_state(&parent_run)
                        == Some(&actionqueue_core::run::RunState::Canceled)
                    {
                        FlowOutcome::Canceled
                    } else {
                        FlowOutcome::Failed
                    };
                    let guard = root.flow_dispatch_guard()?;
                    let fresh = root.flow_snapshot(&tenant)?;
                    let r = fresh.runs.get(&r.id).ok_or("missing Flow at Finish")?;
                    if r.outcome.is_some() {
                        return Ok(());
                    }
                    let outcome = domain_outcome(root, r, fallback)?;
                    // AQ completion without a WI publication is never promoted to success.
                    let outcome = if outcome == FlowOutcome::Succeeded {
                        // A terminal coordinator may have been canceled after an
                        // owned effect committed, before its Finish publication.
                        let effects = root.effect_snapshot(&tenant)?;
                        if effects.effects.values().any(|e| {
                            e.owner.as_ref().is_some_and(|o| o.flow == r.id)
                                && e.phase == worldinterface_domain::Phase::Succeeded
                        }) {
                            FlowOutcome::Uncertain
                        } else {
                            FlowOutcome::Failed
                        }
                    } else {
                        outcome
                    };
                    root.flow_transact_guarded(
                        &guard,
                        &tenant,
                        fresh.revision,
                        FlowCommand::Finish {
                            run: r.id.clone(),
                            outcome,
                            exports: Default::default(),
                        },
                    )?;
                    return Ok(());
                }
                let Some(proposal) = &r.proposal else { return Ok(()) };
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
                let Some(accepted) = accepted else { return Ok(()) };
                let d = &accepted.disposition.as_ref().unwrap().disposition;
                if !matches!(d.outcome(), DispositionOutcome::Awaiting)
                    || d.wait().map(|w| w.wait_id().to_string()).as_deref()
                        != Some(r.wait_id.as_str())
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
                    let binding = ChildBinding {
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
                        wake_request: vec![],
                    };
                    transact(
                        root,
                        &tenant,
                        FlowCommand::BindChild { run: r.id.clone(), node, binding },
                    )?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                if root.flow_snapshot(&tenant)?.runs.get(&r.id).is_some_and(|r| r.outcome.is_some())
                {
                    continue;
                }
                if [
                    "Flow revision conflict",
                    "Flow journal contention",
                    "database is locked",
                    "database is busy",
                ]
                .iter()
                .any(|s| error.to_string().contains(s))
                {
                    continue;
                }
                // Run damage cannot stop scheduling other tenants. Journal/store
                // errors while persisting quarantine still fail globally.
                transact(
                    root,
                    &tenant,
                    FlowCommand::QuarantineRun {
                        run: r.id.clone(),
                        diagnostic: error.to_string().chars().take(1024).collect(),
                    },
                )?;
                if let Ok(task) = r.coordinator_task.as_str().parse() {
                    let _ = engine.control(actionqueue_runtime::control::ControlOperation::Cancel(
                        actionqueue_core::mutation::CancelTarget::Task(task),
                    ));
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
        if effect.owner.as_ref().is_some_and(|o| o.flow == r.id)
            && (effect.phase == worldinterface_domain::Phase::Running
                || effect.outcomes.last().is_some_and(|o| {
                    o.outcome == worldinterface_protocol::ExecutionOutcome::Uncertain
                }))
        {
            fold(FlowOutcome::Uncertain);
        }
        if let Some(owner) = effect.owner.as_ref().filter(|o| o.flow == r.id) {
            if effect.phase == worldinterface_domain::Phase::Succeeded
                && r.results.get(&owner.node).is_none_or(|v| {
                    v.disposition != NodeDisposition::Succeeded
                        || v.effect.as_ref() != Some(&effect.intent.id)
                })
            {
                fold(FlowOutcome::Uncertain);
            }
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
            validate_run_references(root, store, p, r)?;
        }
    }
    Ok(())
}
fn validate_run_references(
    root: &StoreRoot,
    store: uuid::Uuid,
    p: &actionqueue_storage::recovery::reducer::ReplayReducer,
    r: &FlowRun,
) -> Result<()> {
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
    let expected_digest = expected.digest()?;
    if admission.is_some_and(|a| a.digest() != &expected_digest)
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
            serde_json::from_slice::<Vec<ChildAdmission>>(b).ok().as_ref() != Some(&proposed)
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
            let d = &producer.disposition.as_ref().ok_or("uncommitted Flow producer")?.disposition;
            let task = r.node_tasks.get(node).ok_or("foreign Flow binding node")?;
            let child = proposed
                .iter()
                .find(|c| c.task_spec().id().to_string() == task.as_str())
                .ok_or("foreign Flow node")?;
            let actual =
                p.task_admission(child.task_spec().id()).ok_or("missing Flow child admission")?;
            let expected = p.disposition_child_request(run_id, producer.attempt_id(), child)?;
            if !matches!(d.outcome(), DispositionOutcome::Awaiting)
                || d.wait().map(|w| w.wait_id().to_string()).as_deref() != Some(r.wait_id.as_str())
                || d.checkpoint().is_none_or(|c| c.created_by_attempt != producer.attempt_id())
                || d.child_admissions() != proposed
                || b.disposition_hash != ContentHash::of(&serde_json::to_vec(d)?)
                || b.parent_task != r.coordinator_task
                || &b.parent_run != run
                || b.queue_store != r.queue_store
                || actual.digest() != &expected.digest()?
                || b.admission_sequence != actual.sequence()
                || root.read_artifact(&b.request)? != serde_json::to_vec(actual.request())?
                || p.run_ids_for_task(child.task_spec().id()) != vec![b.run.as_str().parse()?]
            {
                return Err("Flow child binding changed".into());
            }
        }
    }
    Ok(())
}
