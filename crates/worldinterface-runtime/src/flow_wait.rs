//! Child continuations: physical attempts close while AQ owns the wait.
use crate::{
    flow::{rejected, transact},
    flow_coordinator::terminal,
    Result,
};
use actionqueue_core::{continuation::*, disposition::AttemptDisposition, ids::*};
use actionqueue_executor_local::handler::ExecutorContext;
use worldinterface_domain::{continuation::*, flow::*};
use worldinterface_protocol::{ContentHash, OpaqueRef};
use worldinterface_storage::{flow::*, StoreRoot};

pub(crate) fn stable_wait_id(value: &impl serde::Serialize) -> Result<WaitId> {
    let h = worldinterface_protocol::data_digest(&("wi-wait-1", value))?;
    let b = hex::decode(h.as_str())?;
    Ok(uuid::Uuid::from_bytes(b[..16].try_into()?).to_string().parse()?)
}
fn route(r: &FlowRun, signal: &SignalWait) -> Result<EventRoute> {
    if signal.tenant != r.tenant || signal.namespace.as_str() != NAMESPACE {
        return Err(rejected("foreign or unknown WI signal route"));
    }
    fn generation(r: &FlowRun, p: &SignalProgress) -> Result<Option<OpaqueRef>> {
        match p {
            SignalProgress::FreshGeneration { generation } => Ok(Some(generation.clone())),
            SignalProgress::AfterHandled { wait } => Ok(r
                .waits
                .get(wait)
                .ok_or_else(|| rejected("missing predecessor checkpoint"))?
                .route
                .as_ref()
                .ok_or_else(|| rejected("timer has no signal generation"))?
                .generation
                .clone()),
            _ => Ok(None),
        }
    }
    Ok(EventRoute {
        tenant: r.tenant.clone(),
        audience: r.principal.clone(),
        kind: EventKind::parse(signal.signal_kind.as_str()).map_err(rejected)?,
        correlation: signal.correlation.clone(),
        generation: generation(r, &signal.progress)?,
    })
}
/// Live engine projection supplies the actual signal high-water mark before dispatch.
pub(crate) fn prepare_eligible(root: &StoreRoot, r: &FlowRun, high_water: u64) -> Result<()> {
    for n in &r.restore()?.document().body.nodes {
        if matches!(n.operation, PlanOperation::Wait { .. } | PlanOperation::Timer { .. })
            && r.bindings.contains_key(&n.id)
            && !r.results.contains_key(&n.id)
            && !r.wait_cursors.contains_key(&n.id)
            && n.dependencies.iter().all(|d| r.results.contains_key(d))
        {
            let source_store = match &n.operation {
                PlanOperation::Wait { signal }
                    if signal.signal_kind.as_str() == EventKind::FlowChild.name() =>
                {
                    worldinterface_storage::StoreKind::Flow
                }
                _ => worldinterface_storage::StoreKind::Fabric,
            };
            transact(
                root,
                &r.tenant,
                FlowCommand::SetWaitCursor {
                    run: r.id.clone(),
                    node: n.id.clone(),
                    sequence: high_water,
                    event_position: root.event_high_water(source_store)?,
                },
            )?;
        }
    }
    Ok(())
}
fn wait_spec(r: &FlowRun, n: &PlanNode, wait_id: WaitId) -> Result<(WaitSpec, Option<EventRoute>)> {
    let (filter, after, deadline, route) = match &n.operation {
        PlanOperation::Wait { signal } => {
            let route = route(r, signal)?;
            let mut after = match &signal.progress {
                SignalProgress::AfterSequence { sequence } => Some(*sequence),
                SignalProgress::AfterHandled { wait } => Some(
                    r.accepted_resumes
                        .values()
                        .filter(|a| r.bindings.get(wait).is_some_and(|b| b.run == a.run))
                        .filter_map(|a| a.sequence)
                        .max()
                        .ok_or_else(|| rejected("predecessor has no accepted signal cursor"))?,
                ),
                _ => None,
            };
            if !signal.accept_early {
                let cursor = r
                    .wait_cursors
                    .get(&n.id)
                    .ok_or_else(|| rejected("unprepared eligibility cursor"))?;
                after = Some(after.unwrap_or(0).max(cursor.sequence));
            }
            if let Some(handled) = r
                .accepted_resumes
                .values()
                .filter(|a| r.bindings.get(&n.id).is_some_and(|b| b.run == a.run))
                .filter(|a| {
                    serde_json::from_slice::<AttemptDisposition>(&a.disposition)
                        .is_ok_and(|d| d.wait().is_some())
                })
                .filter_map(|a| a.sequence)
                .max()
            {
                after = Some(after.unwrap_or(0).max(handled));
            }
            (
                SignalFilter {
                    tenant_id: None,
                    namespace: SignalNamespace::new(NAMESPACE)?,
                    kind: SignalKind::new(route.kind.name())?,
                    correlation_id: Some(CorrelationId::new(route.correlation_id()?)?),
                    source_ref: None,
                },
                after,
                signal.deadline.as_ref(),
                Some(route),
            )
        }
        PlanOperation::Timer { deadline } => (
            SignalFilter {
                tenant_id: None,
                namespace: SignalNamespace::new("wi.timer.private.v1")?,
                kind: SignalKind::new("deadline")?,
                correlation_id: Some(CorrelationId::new(format!(
                    "wi-timer:{}",
                    r.node_tasks[&n.id].as_str()
                ))?),
                source_ref: None,
            },
            None,
            Some(deadline),
            None,
        ),
        _ => return Err(rejected("not a continuation node")),
    };
    let deadline = deadline
        .map(|d| -> Result<_> {
            Ok(WaitDeadline {
                at: d.inclusive_at.epoch_millis()? / 1000,
                policy: WaitTimeoutPolicy::ResumeWithTimeout,
            })
        })
        .transpose()?;
    Ok((
        WaitSpec::new(
            wait_id,
            filter,
            WaitMatchPolicy::FirstMatch,
            after.map_or(SignalEligibility::AnyRetained, |v| {
                SignalEligibility::After(SignalSequence::new(v))
            }),
            deadline,
        )?,
        route,
    ))
}
pub(crate) fn accepted(r: &FlowRun, ctx: &ExecutorContext) -> Result<Option<AttemptDisposition>> {
    let Some(context) = &ctx.input.resume_context else { return Ok(None) };
    let digest = ContentHash::of(&serde_json::to_vec(context)?);
    for a in r.accepted_resumes.values() {
        if a.queue_store == r.queue_store
            && a.run.as_str() == ctx.input.run_id.to_string()
            && a.context_id == context.context_id.0
        {
            if a.context_digest != digest {
                return Err(rejected("conflicting resume context digest"));
            }
            return Ok(Some(serde_json::from_slice(&a.disposition).map_err(rejected)?));
        }
    }
    Ok(None)
}
pub(crate) fn acceptance(
    r: &FlowRun,
    ctx: &ExecutorContext,
    disposition: &AttemptDisposition,
    sequence: Option<u64>,
    event: Option<OpaqueRef>,
) -> Result<ResumeAcceptance> {
    let context =
        ctx.input.resume_context.as_ref().ok_or_else(|| rejected("missing resume context"))?;
    Ok(ResumeAcceptance {
        queue_store: r.queue_store.clone(),
        run: ctx.input.run_id.to_string().try_into()?,
        context_id: context.context_id.0,
        context_digest: ContentHash::of(&serde_json::to_vec(context)?),
        sequence,
        event,
        disposition: serde_json::to_vec(disposition)?,
    })
}
pub(crate) fn execute(
    root: &StoreRoot,
    r: &FlowRun,
    n: &PlanNode,
    ctx: &ExecutorContext,
    artifacts: &dyn crate::artifact::ArtifactAuthorization<
        AuthenticatedContext = worldinterface_domain::agency::AuthenticatedCaller,
    >,
) -> Result<AttemptDisposition> {
    if let Some(result) = accepted(r, ctx)? {
        return Ok(result);
    }
    if !r.wait_cursors.contains_key(&n.id) {
        return crate::flow_node::binding_wait(r, &n.id);
    }
    let mut prepared = if let Some(w) = r.waits.get(&n.id) {
        w.clone()
    } else {
        let (wait, route) =
            wait_spec(r, n, stable_wait_id(&(&r.queue_store, &ctx.input.run_id, &n.id))?)?;
        let artifact =
            root.publish(r.tenant.clone(), &serde_json::to_vec(&(&r.plan_hash, &n.id, &wait))?)?;
        let checkpoint = CheckpointRef {
            checkpoint_id: CheckpointId::new(),
            created_by_attempt: ctx.input.attempt_id,
            data: actionqueue_core::data_ref::DataRef::External(
                crate::aq_adapter::artifact_data_ref(&artifact)?,
            ),
        };
        let w = WaitCheckpoint {
            node: n.id.clone(),
            queue_store: r.queue_store.clone(),
            run: ctx.input.run_id.to_string().try_into()?,
            plan_hash: r.plan_hash.clone(),
            route,
            after_sequence: match wait.eligible_from() {
                Some(SignalEligibility::After(v)) => Some(v.get()),
                _ => None,
            },
            artifact,
            wait: serde_json::to_vec(&wait)?,
            checkpoint: serde_json::to_vec(&checkpoint)?,
            queue_checkpoint: true,
        };
        transact(root, &r.tenant, FlowCommand::PrepareWait { run: r.id.clone(), wait: w.clone() })?;
        w
    };
    let wait: WaitSpec = serde_json::from_slice(&prepared.wait).map_err(rejected)?;
    let mut checkpoint: CheckpointRef =
        serde_json::from_slice(&prepared.checkpoint).map_err(rejected)?;
    // A crash after WI preparation can precede the first AQ checkpoint commit.
    // The trusted input then has no checkpoint. Rebind only this unpublished
    // producer attempt, retaining the exact checkpoint data and wait identity.
    if prepared.queue_checkpoint
        && checkpoint.created_by_attempt != ctx.input.attempt_id
        && ctx.input.resume_context.as_ref().is_none_or(|c| c.checkpoint.is_none())
    {
        let previous = prepared.checkpoint.clone();
        checkpoint.created_by_attempt = ctx.input.attempt_id;
        prepared.checkpoint = serde_json::to_vec(&checkpoint)?;
        transact(
            root,
            &r.tenant,
            FlowCommand::RebindCheckpoint {
                run: r.id.clone(),
                node: n.id.clone(),
                expected: previous,
                checkpoint: prepared.checkpoint.clone(),
            },
        )?;
    }
    validate(root, r, n, &prepared)?;
    let Some(context) = &ctx.input.resume_context else {
        return Ok(AttemptDisposition::awaiting(wait, Some(checkpoint)));
    };
    // Binding-readiness deadlines precede the real wait. They cannot decide the node.
    if matches!(context.wake, WakeReason::Deadline { deadline_at: 0, .. })
        && context.checkpoint.is_none()
        && context.wait_id() != Some(wait.wait_id())
    {
        return Ok(AttemptDisposition::awaiting(wait, Some(checkpoint)));
    }
    if context.wait_id().is_some_and(|id| id != wait.wait_id()) {
        return Err(rejected("resume wait mismatch"));
    }
    if context.checkpoint.as_ref() != prepared.queue_checkpoint.then_some(&checkpoint) {
        return Err(rejected("resume checkpoint changed"));
    }
    let (good, sequence, event, next_wait) = match &context.wake {
        WakeReason::Signal { signal_sequence, envelope, .. } => {
            let id: OpaqueRef = envelope
                .source_ref
                .as_ref()
                .ok_or_else(|| rejected("signal has no WI record"))?
                .expose()
                .to_string()
                .try_into()?;
            let kind = if prepared.route.as_ref().is_some_and(|r| r.kind == EventKind::FlowChild) {
                worldinterface_storage::StoreKind::Flow
            } else {
                worldinterface_storage::StoreKind::Fabric
            };
            let obligation = root.signal_obligation(kind, &id)?;
            if root.authoritative_event(kind, &id)? != obligation.event {
                return Err(rejected("event projection differs from source"));
            }
            if prepared.route.as_ref() != Some(&obligation.event.route) {
                return Err(rejected("signal source scope mismatch"));
            }
            let request = crate::continuation::signal_request(&obligation.event)?;
            let request_bytes = serde_json::to_vec(&request)?;
            let digest = ContentHash::of(&request_bytes);
            let expected = request.envelope(
                &SignalIngressContext {
                    control_context: envelope.control_context.clone(),
                    ..Default::default()
                },
                envelope.received_at,
            );
            if **envelope != expected
                || obligation.request.as_ref() != Some(&request_bytes)
                || obligation.request_digest.as_ref() != Some(&digest)
                || obligation.sequence.is_some_and(|v| v != signal_sequence.get())
                || prepared.after_sequence.is_some_and(|v| signal_sequence.get() <= v)
            {
                return Err(rejected("signal does not bind frozen source record"));
            }
            // AQ can dispatch an admitted wake before a bounded outbox scan
            // reaches its WI acknowledgment after restart. Repair from the
            // trusted envelope and frozen request, independently of scan order.
            root.acknowledge_signal(kind, id.clone(), digest, signal_sequence.get())?;
            let stale = matches!(&n.operation, PlanOperation::Wait { signal } if !signal.accept_early)
                && root.event_position(kind, &id)? <= r.wait_cursors[&n.id].event_position;
            if stale {
                let mut next = prepared.clone();
                let advanced = WaitSpec::new(
                    stable_wait_id(&(&prepared.run, context.context_id.0))?,
                    wait.filter().unwrap().clone(),
                    WaitMatchPolicy::FirstMatch,
                    SignalEligibility::After(*signal_sequence),
                    wait.deadline().cloned(),
                )?;
                next.wait = serde_json::to_vec(&advanced)?;
                next.after_sequence = Some(signal_sequence.get());
                next.queue_checkpoint = false;
                (None, Some(signal_sequence.get()), Some(id), Some(next))
            } else {
                if let Some(a) = &obligation.event.artifact {
                    let caller =
                    worldinterface_domain::agency::AuthenticatedCaller::from_trusted_authentication(
                        r.tenant.clone(),
                        r.principal.clone(),
                    );
                    crate::artifact::ArtifactResolver::new(root, artifacts).read(&caller, a)?;
                }
                (Some(true), Some(signal_sequence.get()), Some(id), None)
            }
        }
        WakeReason::Deadline { deadline_at, .. } => {
            if wait.deadline().is_none_or(|d| d.at != *deadline_at) {
                return Err(rejected("deadline mismatch"));
            }
            let d = match &n.operation {
                PlanOperation::Wait { signal } => signal.deadline.as_ref().unwrap(),
                PlanOperation::Timer { deadline } => deadline,
                _ => unreachable!(),
            };
            (Some(d.action == TimeoutAction::Continue), None, None, None)
        }
        WakeReason::ControlResolution { .. } | WakeReason::AdministrativeResume { .. } => {
            let (wait, _) =
                wait_spec(r, n, stable_wait_id(&(&prepared.run, context.context_id.0))?)?;
            let mut next = prepared.clone();
            next.wait = serde_json::to_vec(&wait)?;
            next.queue_checkpoint = false;
            (None, None, None, Some(next))
        }
        WakeReason::Children { .. } => {
            return Err(rejected("child wake cannot resolve signal wait"))
        }
    };
    let result = good.map(|good| {
        crate::flow_node::empty_result(if good {
            NodeDisposition::Succeeded
        } else {
            NodeDisposition::Failed
        })
    });
    let disposition = if let Some(good) = good {
        if good {
            AttemptDisposition::complete(None)
        } else {
            terminal("wi.flow.wait_timeout")
        }
    } else {
        AttemptDisposition::awaiting(
            serde_json::from_slice(&next_wait.as_ref().unwrap().wait)?,
            None,
        )
    };
    let a = acceptance(r, ctx, &disposition, sequence, event)?;
    transact(
        root,
        &r.tenant,
        FlowCommand::AcceptResume {
            run: r.id.clone(),
            node: Some(n.id.clone()),
            acceptance: a,
            result,
            next_wait: next_wait.map(Box::new),
            finish: None,
        },
    )?;
    Ok(disposition)
}
pub(crate) fn validate(
    root: &StoreRoot,
    r: &FlowRun,
    n: &PlanNode,
    w: &WaitCheckpoint,
) -> Result<()> {
    let wait: WaitSpec = serde_json::from_slice(&w.wait).map_err(rejected)?;
    let checkpoint: CheckpointRef = serde_json::from_slice(&w.checkpoint).map_err(rejected)?;
    let (expected, route) = wait_spec(r, n, wait.wait_id())?;
    if wait != expected
        || route != w.route
        || w.plan_hash != r.plan_hash
        || w.queue_store != r.queue_store
        || r.bindings.get(&n.id).is_none_or(|b| b.run != w.run)
    {
        return Err(rejected("altered wait checkpoint"));
    }
    let actionqueue_core::data_ref::DataRef::External(external) = &checkpoint.data else {
        return Err(rejected("checkpoint artifact required"));
    };
    let artifact = crate::aq_adapter::data_ref_artifact(external).map_err(|error| {
        if error.is::<crate::aq_adapter::ArtifactLocatorError>() {
            rejected(error)
        } else {
            error
        }
    })?;
    if artifact != w.artifact || artifact.scope != r.tenant {
        return Err(rejected("foreign checkpoint artifact"));
    }
    let b = root.read_artifact(&artifact)?;
    checkpoint.data.verify_bytes(&b).map_err(rejected)?;
    // Control reestablishment changes only the wait identity, preserving its checkpoint evidence.
    let original: (ContentHash, OpaqueRef, WaitSpec) =
        serde_json::from_slice(&b).map_err(rejected)?;
    if original.0 != r.plan_hash
        || original.1 != n.id
        || original.2.filter() != wait.filter()
        || original.2.match_policy() != wait.match_policy()
        || original.2.deadline() != wait.deadline()
    {
        return Err(rejected("checkpoint content mismatch"));
    }
    Ok(())
}
