//! Flat bounded transactional AQ children, reconciled through the live engine.
use crate::flow::{is_rejected, rejected};
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

// Counts and timings are disposable diagnostics, never scheduling authority.
#[derive(Clone, Debug, Default)]
pub struct ReconcileWork {
    pub rows: usize,
    pub pages: usize,
    pub task_lookups: usize,
    pub plans: usize,
    pub artifacts: usize,
    pub plan_time: std::time::Duration,
    pub artifact_time: std::time::Duration,
}
pub(crate) fn read_artifact(
    root: &StoreRoot,
    a: &worldinterface_protocol::ArtifactRef,
    work: &mut ReconcileWork,
) -> Result<Vec<u8>> {
    let t = std::time::Instant::now();
    let result = root.read_artifact(a);
    work.artifacts += 1;
    work.artifact_time += t.elapsed();
    result
}
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
    let state = root.flow_run_snapshot(&payload.tenant, &payload.run)?;
    if state.quarantined.contains_key(&payload.run) {
        return Err(rejected("quarantined Flow run"));
    }
    let r = state.runs.get(&payload.run).ok_or_else(|| rejected("unknown Flow run"))?;
    if r.tenant != payload.tenant {
        return Err(rejected("foreign Flow run"));
    }
    r.restore()?;
    Ok(r.clone())
}
pub(crate) fn children(r: &FlowRun) -> Result<Vec<ChildAdmission>> {
    children_with_plan(r, &r.restore()?)
}
fn children_with_plan(
    r: &FlowRun,
    p: &worldinterface_domain::flow::ExecutionPlan,
) -> Result<Vec<ChildAdmission>> {
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
    if r.parent_run.is_none() {
        return binding_wait(&r.coordinator_task);
    }
    if r.parent_run.as_ref().map(OpaqueRef::as_str) != Some(ctx.input.run_id.to_string().as_str()) {
        return Err(rejected("unbound coordinator run"));
    }
    if r.cancel_requested {
        return Ok(terminal("wi.flow.canceled"));
    }
    if let Some(disposition) = crate::flow_wait::accepted(r, ctx)? {
        return Ok(disposition);
    }
    if r.outcome.is_some() {
        return Ok(if r.outcome == Some(FlowOutcome::Succeeded) {
            AttemptDisposition::complete(None)
        } else {
            terminal("wi.flow.domain_failure")
        });
    }
    // A wake only requests a domain evaluation. It never proves business success.
    let binding_wake = ctx.input.resume_context.as_ref().is_some_and(|c| {
        // The coordinator's only deadline is its private binding wait; its
        // child wait has none. Replaying after PrepareChildren must recognize
        // this wake too, even when the proposal is already durable.
        matches!(&c.wake, WakeReason::Deadline { deadline_at: 0, .. })
    });
    if let Some(context) = ctx.input.resume_context.as_ref().filter(|_| !binding_wake) {
        let all_terminal =
            ctx.children.as_ref().is_some_and(|children| children.all_children_terminal());
        let finish = match &context.wake {
            WakeReason::Children { wait_id, .. } => {
                if wait_id.to_string() != r.wait_id.as_str()
                    && !r.accepted_resumes.values().any(|a| {
                        serde_json::from_slice::<AttemptDisposition>(&a.disposition)
                            .ok()
                            .and_then(|d| d.wait().map(|w| w.wait_id()))
                            == Some(*wait_id)
                    })
                {
                    return Err(rejected("coordinator child wait mismatch"));
                }
                true
            }
            WakeReason::ControlResolution { .. } | WakeReason::AdministrativeResume { .. } => {
                all_terminal
            }
            WakeReason::Signal { .. } | WakeReason::Deadline { .. } => {
                return Err(rejected("unexpected coordinator wake"))
            }
        };
        if !finish {
            let wait = WaitSpec::children(
                crate::flow_wait::stable_wait_id(&(&r.id, context.context_id.0))?,
                r.node_tasks.values().map(|v| Ok(v.as_str().parse()?)).collect::<Result<_>>()?,
                ChildWaitPolicy::AllTerminal,
                None,
            )?;
            let disposition = AttemptDisposition::awaiting(wait, None);
            let acceptance = crate::flow_wait::acceptance(r, ctx, &disposition, None, None)?;
            transact(
                root,
                &r.tenant,
                FlowCommand::AcceptResume {
                    run: r.id.clone(),
                    node: None,
                    acceptance,
                    result: None,
                    next_wait: None,
                    finish: None,
                },
            )?;
            return Ok(disposition);
        }
        let guard = root.flow_dispatch_guard()?;
        let fresh = root.flow_run_snapshot(&r.tenant, &r.id)?;
        let r = fresh.runs.get(&r.id).ok_or_else(|| rejected("missing Flow at Finish"))?;
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
                        .map_err(|_| rejected("export JSON schema requires JSON bytes"))?;
                    }
                }
                if !matches!(
                    p.document().schemas[&input.schema].schema,
                    worldinterface_domain::flow::Schema::Artifact { .. }
                ) && !worldinterface_domain::flow::validate_value(
                    &p.document().schemas[&input.schema].schema,
                    &value,
                ) {
                    return Err(rejected("export schema mismatch"));
                }
                let artifact = artifact
                    .map_or_else(|| root.publish(r.tenant.clone(), &canonical_data(&value)?), Ok)?;
                artifact_reader.read(&caller, &artifact)?;
                exports.insert(name.clone(), artifact);
            }
        }
        let disposition = if outcome == FlowOutcome::Succeeded {
            AttemptDisposition::complete(None)
        } else {
            terminal("wi.flow.domain_failure")
        };
        let acceptance = crate::flow_wait::acceptance(r, ctx, &disposition, None, None)?;
        root.flow_update_guarded(
            &guard,
            &r.tenant,
            fresh.revision,
            FlowCommand::AcceptResume {
                run: r.id.clone(),
                node: None,
                acceptance,
                result: None,
                next_wait: None,
                finish: Some((outcome, exports)),
            },
        )?;
        return Ok(disposition);
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
        data: actionqueue_core::data_ref::DataRef::External(crate::aq_adapter::artifact_data_ref(
            &root.publish(r.tenant.clone(), r.plan_hash.as_str().as_bytes())?,
        )?),
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

pub(crate) fn binding_wait(task: &OpaqueRef) -> Result<AttemptDisposition> {
    Ok(AttemptDisposition::awaiting(
        WaitSpec::new(
            WaitId::new(),
            SignalFilter {
                tenant_id: None,
                namespace: SignalNamespace::new("wi.flow")?,
                kind: SignalKind::new("binding-ready")?,
                correlation_id: Some(CorrelationId::new(format!("wi-ready:{}", task.as_str()))?),
                source_ref: None,
            },
            WaitMatchPolicy::FirstMatch,
            SignalEligibility::AnyRetained,
            Some(WaitDeadline { at: 0, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
        )?,
        None,
    ))
}

/// Shared across both reconciliation calls in a tick. No retained cursor owns
/// wake authority: restart still validates every reference, then starts afresh.
pub const RECONCILE_ROWS_PER_TICK: usize = 8;
#[derive(Default)]
pub(crate) struct ReconcileCursor {
    position: Option<(u64, u64)>,
    pass_position: Option<(u64, u64)>,
    through: i64,
    aq_sequence: u64,
    changed: std::collections::VecDeque<TaskId>,
    queued: std::collections::BTreeSet<TaskId>,
    after: i64,
    retry: bool,
    work: ReconcileWork,
}
impl ReconcileCursor {
    pub(crate) fn new(aq_sequence: u64) -> Self {
        Self { aq_sequence, ..Self::default() }
    }
}
pub(crate) fn begin_tick(cursor: &mut ReconcileCursor) {
    cursor.work = ReconcileWork::default();
}
pub(crate) fn tick_work(cursor: &ReconcileCursor) -> ReconcileWork {
    cursor.work.clone()
}
pub(crate) fn reconcile<H: actionqueue_executor_local::handler::ExecutorHandler + 'static>(
    root: &StoreRoot,
    store: uuid::Uuid,
    engine: &mut actionqueue_runtime::engine::BootstrappedEngine<H>,
    cursor: &mut ReconcileCursor,
    remaining: &mut usize,
) -> Result<()> {
    let result = reconcile_page(root, store, engine, cursor, remaining);
    if result.is_err() {
        cursor.pass_position = None;
        cursor.position = None;
        cursor.after = 0;
        cursor.retry = false;
    }
    result
}
fn reconcile_page<H: actionqueue_executor_local::handler::ExecutorHandler + 'static>(
    root: &StoreRoot,
    store: uuid::Uuid,
    engine: &mut actionqueue_runtime::engine::BootstrappedEngine<H>,
    cursor: &mut ReconcileCursor,
    remaining: &mut usize,
) -> Result<()> {
    // Changes are hints only. Every selected row still passes full validation.
    // The AQ index enumerates changed tasks, never unchanged waits or history.
    for task in engine.projection().changed_tasks_since(cursor.aq_sequence) {
        if cursor.queued.insert(task) {
            cursor.changed.push_back(task);
        }
    }
    cursor.aq_sequence = engine.projection().latest_sequence();
    let position = (root.flow_journal_position()?, cursor.aq_sequence);
    let phase_limit = (*remaining).min(RECONCILE_ROWS_PER_TICK / 2);
    if phase_limit == 0 {
        return Ok(());
    }
    let mut entries = Vec::new();
    let mut selected = std::collections::BTreeSet::new();
    // Reserve one row for the finite recovery pass, even under continuous traffic.
    // Limit task-to-Flow lookups too; many children may name the same Flow.
    for _ in 0..64 {
        if entries.len() >= phase_limit - 1 {
            break;
        }
        let Some(task) = cursor.changed.pop_front() else {
            break;
        };
        cursor.queued.remove(&task);
        cursor.work.task_lookups += 1;
        if let Some((tenant, run)) = root.flow_task(&task.to_string())? {
            if selected.insert((tenant.clone(), run.clone())) {
                if let Some(entry) = root.flow_pending_run(&tenant, &run)? {
                    entries.push(entry);
                }
            }
        }
    }
    let background_limit = phase_limit - entries.len();
    let mut page_len = 0;
    let scanning = cursor.position != Some(position);
    if scanning {
        cursor.position = None;
        if cursor.pass_position.is_none() {
            cursor.pass_position = Some(position);
            cursor.through = root.flow_scan_bounds()?.1;
            cursor.after = 0;
        }
        let page = root.flow_pending_global_page(cursor.after, cursor.through, background_limit)?;
        cursor.work.pages += 1;
        page_len = page.len();
        if let Some(last) = page.last() {
            cursor.after = last.position;
        }
        // Count all fetched rows, including a row also selected by the priority lane.
        cursor.work.rows += page.len();
        entries.extend(page);
    }
    cursor.work.rows += entries.len() - page_len;
    *remaining -= entries.len();
    for entry in &entries {
        let r = &entry.run;
        let tenant = &r.tenant;
        let quarantine = &entry.quarantine;
        if quarantine.is_some() {
            cancel_quarantined(engine, r)?;
            continue;
        }
        let result = (|| -> Result<()> {
            // Includes complete plan/hash and queue identity validation.
            let plan =
                validate_run_references(root, store, engine.projection(), r, &mut cursor.work)?;
            let parent: TaskId = r.coordinator_task.as_str().parse()?;
            let expected: EnsureTaskRequest =
                serde_json::from_slice(&r.coordinator_request).map_err(rejected)?;
            // A crash after WI admission but before AQ admission retains this outbox.
            if engine.projection().task_admission(parent).is_none() {
                engine.ensure_task(expected.clone())?;
            }
            let projection = engine.projection();
            let admission = projection
                .task_admission(parent)
                .ok_or_else(|| rejected("missing Flow coordinator admission"))?;
            if admission.digest() != &expected.digest()? {
                return Err(rejected("Flow coordinator admission changed"));
            }
            let runs = projection.run_ids_for_task(parent);
            if runs.len() != 1 {
                return Err(rejected("Flow coordinator run cardinality"));
            }
            let parent_run = runs[0];
            if r.parent_run.is_none() {
                transact(
                    root,
                    tenant,
                    FlowCommand::BindCoordinator {
                        run: r.id.clone(),
                        parent_run: parent_run.to_string().try_into()?,
                    },
                )?;
            }
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
                let fallback = if r.cancel_requested
                    || projection.get_run_state(&parent_run)
                        == Some(&actionqueue_core::run::RunState::Canceled)
                {
                    FlowOutcome::Canceled
                } else {
                    FlowOutcome::Failed
                };
                let guard = root.flow_dispatch_guard()?;
                let fresh = root.flow_run_snapshot(tenant, &r.id)?;
                let r = fresh.runs.get(&r.id).ok_or_else(|| rejected("missing Flow at Finish"))?;
                if r.outcome.is_some() {
                    return Ok(());
                }
                let outcome = domain_outcome(root, r, fallback)?;
                // AQ completion without a WI publication is never promoted to success.
                let outcome = if outcome == FlowOutcome::Succeeded {
                    // A terminal coordinator may have been canceled after an
                    // owned effect committed, before its Finish publication.
                    let effects = root.effect_owner_snapshot(tenant, &r.id, None)?;
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
                root.flow_update_guarded(
                    &guard,
                    tenant,
                    fresh.revision,
                    FlowCommand::Finish { run: r.id.clone(), outcome, exports: Default::default() },
                )?;
                return Ok(());
            }
            if r.bindings.len() == r.node_tasks.len() {
                // References were validated above. Fully bound runs need
                // only cursor preparation for newly eligible nodes, without
                // another admission transaction or per-run database reload.
                crate::flow_wait::prepare_eligible_with_plan(
                    root,
                    r,
                    &plan,
                    projection.signals().last_sequence().get(),
                )?;
                return Ok(());
            }
            let Some(proposal) = &r.proposal else { return Ok(()) };
            let proposed: Vec<ChildAdmission> =
                serde_json::from_slice(proposal).map_err(rejected)?;
            if proposed != children_with_plan(r, &plan)? {
                return Err(rejected("Flow proposal changed"));
            }
            let accepted = projection
                .get_attempt_history(&parent_run)
                .into_iter()
                .flat_map(|history| history.iter())
                .find(|a| {
                    a.disposition
                        .as_ref()
                        .is_some_and(|d| d.disposition.child_admissions() == proposed)
                });
            let Some(accepted) = accepted else { return Ok(()) };
            let d = &accepted.disposition.as_ref().unwrap().disposition;
            if !matches!(d.outcome(), DispositionOutcome::Awaiting)
                || d.wait().map(|w| w.wait_id().to_string()).as_deref() != Some(r.wait_id.as_str())
            {
                return Err(rejected("Flow child transaction mismatch"));
            }
            for child in &proposed {
                let task = child.task_spec().id();
                let actual = projection
                    .task_admission(task)
                    .ok_or_else(|| rejected("partial AQ child transaction"))?;
                let expected = projection.disposition_child_request(
                    parent_run,
                    accepted.attempt_id(),
                    child,
                )?;
                if actual.digest() != &expected.digest()? {
                    return Err(rejected("Flow accepted child request mismatch"));
                }
                let runs = projection.run_ids_for_task(task);
                if runs.len() != 1 {
                    return Err(rejected("Flow child run cardinality"));
                }
                let payload = FlowPayload::decode(expected.task_spec().task_payload().bytes())?;
                let node = payload.node.ok_or_else(|| rejected("missing child node"))?;
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
                    tenant,
                    FlowCommand::BindChild { run: r.id.clone(), node, binding },
                )?;
            }
            let fresh = root.flow_run_snapshot(tenant, &r.id)?;
            if let Some(run) = fresh.runs.get(&r.id) {
                crate::flow_wait::prepare_eligible(
                    root,
                    run,
                    projection.signals().last_sequence().get(),
                )?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            if root
                .flow_run_snapshot(tenant, &r.id)?
                .runs
                .get(&r.id)
                .is_some_and(|r| r.outcome.is_some())
            {
                continue;
            }
            if !is_rejected(error.as_ref()) {
                // I/O, AQ service, capacity and contention failures retry on
                // later ticks. Their diagnostic text never establishes damage.
                cursor.retry = true;
                continue;
            }
            // Run damage cannot stop scheduling other tenants. Journal/store
            // errors while persisting quarantine still fail globally.
            transact(
                root,
                tenant,
                FlowCommand::QuarantineRun {
                    run: r.id.clone(),
                    diagnostic: bounded_diagnostic(&error.to_string()),
                },
            )?;
            cancel_quarantined(engine, r)?;
        }
    }
    if scanning && page_len < background_limit {
        let completed = cursor.pass_position.take();
        if !cursor.retry {
            cursor.position = completed;
        }
        cursor.retry = false;
    }
    Ok(())
}

// Failed controls surface to the host; the next tick retries quarantine's
// cancellation obligation rather than silently abandoning it.
fn cancel_quarantined<H: actionqueue_executor_local::handler::ExecutorHandler + 'static>(
    engine: &mut actionqueue_runtime::engine::BootstrappedEngine<H>,
    r: &FlowRun,
) -> Result<()> {
    let task = r.coordinator_task.as_str().parse()?;
    if engine
        .projection()
        .run_ids_for_task(task)
        .iter()
        .any(|id| engine.projection().get_run_state(id).is_some_and(|s| !s.is_terminal()))
    {
        engine.control(actionqueue_runtime::control::ControlOperation::Cancel(
            actionqueue_core::mutation::CancelTarget::Task(task),
        ))?;
    }
    Ok(())
}

/// Reload boundary evidence independently of AQ terminal-success projections.
fn domain_outcome(root: &StoreRoot, r: &FlowRun, missing: FlowOutcome) -> Result<FlowOutcome> {
    let mut outcome =
        if r.cancel_requested { FlowOutcome::Canceled } else { FlowOutcome::Succeeded };
    let effects = root.effect_owner_snapshot(&r.tenant, &r.id, None)?;
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
                    let effect =
                        effects.effects.get(id).ok_or_else(|| rejected("Flow effect missing"))?;
                    let admission = effect
                        .dispatches
                        .last()
                        .and_then(|d| d.admission.as_ref())
                        .ok_or_else(|| rejected("Flow effect admission missing"))?;
                    let binding = r
                        .bindings
                        .get(node)
                        .ok_or_else(|| rejected("Flow effect child unbound"))?;
                    if admission.child.as_ref().is_none_or(|c| c.flow != r.id || &c.node != node)
                        || !admission.observed_runs.contains(&binding.run)
                    {
                        return Err(rejected("Flow effect belongs to another child"));
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
            validate_run_references(root, store, p, r, &mut ReconcileWork::default())?;
        }
    }
    Ok(())
}
fn validate_run_references(
    root: &StoreRoot,
    store: uuid::Uuid,
    p: &actionqueue_storage::recovery::reducer::ReplayReducer,
    r: &FlowRun,
    work: &mut ReconcileWork,
) -> Result<worldinterface_domain::flow::ExecutionPlan> {
    let started = std::time::Instant::now();
    let plan = r.restore();
    work.plans += 1;
    work.plan_time += started.elapsed();
    let plan = plan?;
    for (node, w) in &r.waits {
        let n = plan
            .document()
            .body
            .nodes
            .iter()
            .find(|n| &n.id == node)
            .ok_or_else(|| rejected("checkpoint node missing"))?;
        crate::flow_wait::validate_measured(root, r, n, w, work)?;
        let checkpoint: CheckpointRef = serde_json::from_slice(&w.checkpoint).map_err(rejected)?;
        let wait: WaitSpec = serde_json::from_slice(&w.wait).map_err(rejected)?;
        if p.waits().get(wait.wait_id()).is_some_and(|actual| {
            actual.spec != wait
                || actual.run_id.to_string() != w.run.as_str()
                || actual.checkpoint.as_ref() != w.queue_checkpoint.then_some(&checkpoint)
        }) {
            return Err(rejected("published AQ wait/checkpoint changed"));
        }

        if p.checkpoint(checkpoint.checkpoint_id)
            .is_some_and(|saved| saved.checkpoint != checkpoint)
        {
            return Err(rejected("published AQ checkpoint changed"));
        }
    }
    for a in r.accepted_resumes.values() {
        let run: RunId = a.run.as_str().parse()?;
        let matching = p
            .get_attempt_history(&run)
            .into_iter()
            .flat_map(|v| v.iter())
            .filter_map(|attempt| p.attempt_resume(run, attempt.attempt_id()))
            .any(|context| {
                context.context_id.0 == a.context_id
                    && serde_json::to_vec(&context)
                        .is_ok_and(|b| ContentHash::of(&b) == a.context_digest)
            });
        if a.queue_store != r.queue_store || !matching {
            return Err(rejected("accepted resume has no immutable AQ context"));
        }
    }
    if r.queue_store.as_str() != store.to_string() {
        return Err(rejected("Flow queue identity changed"));
    }
    let task = r.coordinator_task.as_str().parse()?;
    let expected: EnsureTaskRequest =
        serde_json::from_slice(&r.coordinator_request).map_err(rejected)?;
    if expected.task_spec().id() != task
        || FlowPayload::decode(expected.task_spec().task_payload().bytes())?
            != FlowPayload::new(r, None)
    {
        return Err(rejected("Flow coordinator payload changed"));
    }
    let admission = p.task_admission(task);
    let expected_digest = expected.digest()?;
    if admission.is_some_and(|a| a.digest() != &expected_digest)
        || r.parent_run.is_some() && admission.is_none()
    {
        return Err(rejected("Flow coordinator admission changed"));
    }
    if let Some(run) = &r.parent_run {
        let run_id = run.as_str().parse()?;
        if p.run_ids_for_task(task) != vec![run_id] {
            return Err(rejected("Flow parent run changed"));
        }
        let proposed = children_with_plan(r, &plan)?;
        if r.proposal.as_ref().is_some_and(|b| {
            serde_json::from_slice::<Vec<ChildAdmission>>(b).ok().as_ref() != Some(&proposed)
        }) {
            return Err(rejected("Flow proposal changed"));
        }
        for (node, b) in &r.bindings {
            let producer = p
                .get_attempt_history(&run_id)
                .into_iter()
                .flat_map(|history| history.iter())
                .find(|a| a.attempt_id().to_string() == b.producing_attempt.as_str())
                .ok_or_else(|| rejected("missing Flow producer"))?;
            let d = &producer
                .disposition
                .as_ref()
                .ok_or_else(|| rejected("uncommitted Flow producer"))?
                .disposition;
            let task =
                r.node_tasks.get(node).ok_or_else(|| rejected("foreign Flow binding node"))?;
            let child = proposed
                .iter()
                .find(|c| c.task_spec().id().to_string() == task.as_str())
                .ok_or_else(|| rejected("foreign Flow node"))?;
            let actual = p
                .task_admission(child.task_spec().id())
                .ok_or_else(|| rejected("missing Flow child admission"))?;
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
                || read_artifact(root, &b.request, work)? != serde_json::to_vec(actual.request())?
                || p.run_ids_for_task(child.task_spec().id()) != vec![b.run.as_str().parse()?]
            {
                return Err(rejected("Flow child binding changed"));
            }
        }
    }
    Ok(plan)
}

fn bounded_diagnostic(message: &str) -> String {
    message
        .chars()
        .scan(0, |bytes, c| {
            *bytes += c.len_utf8();
            (*bytes <= 1024).then_some(c)
        })
        .collect()
}
#[cfg(test)]
#[test]
fn quarantine_diagnostic_bounds_utf8_bytes_without_splitting_characters() {
    let message = "界".repeat(1024);
    let diagnostic = bounded_diagnostic(&message);
    assert_eq!(diagnostic.len(), 1023);
    assert!(message.starts_with(&diagnostic));
    assert_eq!(bounded_diagnostic("short"), "short");
}
