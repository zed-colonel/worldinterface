//! Authenticated plan admission. Direct and Flow enter the same coordinator.
use crate::{
    aq_adapter::QueueEngine,
    artifact::{ArtifactAuthorization, ArtifactResolver},
    authority::{caller, verify_snapshot, Authentication, EvidenceAuthority, TrustedClock},
    Result,
};
use actionqueue_core::{
    admission::EnsureTaskRequest,
    causal::CausalContext,
    ids::*,
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        safety::SafetyLevel,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_executor_local::handler::ExecutorHandler;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use worldinterface_domain::{
    agency::{resolve_agency, AuthenticatedCaller},
    flow::*,
};
use worldinterface_protocol::{
    canonical_data, strict_json, ClientRequestId, ContentHash, EffectSubmission, OpaqueRef,
};
use worldinterface_storage::{flow::*, StoreRoot};

/// A deterministic execution contract or persisted-reference rejection.
#[derive(Debug)]
pub(crate) struct FlowRejected(String);
impl std::fmt::Display for FlowRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for FlowRejected {}
pub(crate) fn rejected(
    message: impl std::fmt::Display,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(FlowRejected(message.to_string()))
}
pub(crate) fn is_rejected(error: &(dyn std::error::Error + 'static)) -> bool {
    error.is::<FlowRejected>() || error.is::<FlowIntegrity>() || error.is::<CompileError>()
}

/// Stable bounded identity for an exact materialized node demand. Hosts can
/// derive it before admission when issuing exact-demand credentials.
pub fn node_request_id(request: &ClientRequestId, node: &OpaqueRef) -> Result<ClientRequestId> {
    let hash = ContentHash::of(&canonical_data(&("wi-flow-node-request-1", request, node))?);
    Ok(format!("wi-flow-node:{}", hash.as_str()).try_into()?)
}

/// Trusted host configuration, scoped to the authenticated caller, never request data.
pub trait PlanConfiguration:
    ArtifactAuthorization<AuthenticatedContext = AuthenticatedCaller>
{
    fn pins(&self, caller: &AuthenticatedCaller) -> Result<FlowPins>;
}
pub struct PlanService<'a, A, V, C, P> {
    pub root: &'a StoreRoot,
    pub authentication: &'a A,
    pub evidence: &'a V,
    pub clock: &'a C,
    pub configuration: &'a P,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FlowPayload {
    pub profile: String,
    pub tenant: OpaqueRef,
    pub run: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<OpaqueRef>,
}
impl FlowPayload {
    pub fn new(r: &FlowRun, node: Option<OpaqueRef>) -> Self {
        Self { profile: "wi-flow-1".into(), tenant: r.tenant.clone(), run: r.id.clone(), node }
    }
    pub fn decode(b: &[u8]) -> Result<Self> {
        if b.len() > 4096 {
            return Err(rejected("Flow locator too large"));
        }
        let p: Self =
            serde_json::from_value(strict_json(b).map_err(rejected)?).map_err(rejected)?;
        if p.profile != "wi-flow-1" || canonical_data(&p)? != b {
            return Err(rejected("invalid Flow locator"));
        }
        Ok(p)
    }
}
pub(crate) fn constraints(timeout_millis: Option<u64>) -> Result<TaskConstraints> {
    let timeout = timeout_millis.map_or(60, |ms| ms.div_ceil(1000).max(1));
    let mut c = TaskConstraints::new(3, Some(timeout), None)?;
    c.set_safety_level(SafetyLevel::Idempotent);
    Ok(c)
}
pub(crate) fn transact(
    root: &StoreRoot,
    tenant: &OpaqueRef,
    command: FlowCommand,
) -> Result<FlowState> {
    for _ in 0..16 {
        let s = root.flow_catalog(tenant)?;
        match root.flow_update(tenant, s.revision, command.clone()) {
            Err(e) if e.is::<FlowRevisionConflict>() => continue,
            result => return result,
        }
    }
    Err("Flow journal contention".into())
}
impl<A: Authentication, V: EvidenceAuthority, C: TrustedClock, P: PlanConfiguration>
    PlanService<'_, A, V, C, P>
{
    pub fn activate_flow_version(
        &self,
        session: &A::Session,
        definition: FlowDefinition,
        previous: Option<u64>,
    ) -> Result<()> {
        let c = caller(self.authentication, session)?;
        let pins = self.configuration.pins(&c)?;
        let (version, _) = compile_flow_version(&definition, &pins.context()?)?;
        transact(
            self.root,
            c.tenant(),
            FlowCommand::InsertVersion { definition: version.definition().clone() },
        )?;
        transact(
            self.root,
            c.tenant(),
            FlowCommand::ActivateVersion {
                id: definition.id,
                version: definition.version,
                previous,
            },
        )?;
        Ok(())
    }
    pub fn submit_direct<H: ExecutorHandler + 'static>(
        &self,
        session: &A::Session,
        submission: EffectSubmission,
        selection: DirectOperationSelection,
        queue: &mut QueueEngine<'_, H>,
    ) -> Result<OpaqueRef> {
        let c = caller(self.authentication, session)?;
        let pins = self.configuration.pins(&c)?;
        let p = compile_direct(&submission, &selection, &pins.context()?)?;
        self.submit(c, submission, pins, p, queue)
    }
    /// The template retains the submitting agency and request policy; it grants no node authority.
    pub fn submit_flow<H: ExecutorHandler + 'static>(
        &self,
        session: &A::Session,
        definition: &OpaqueRef,
        submission: EffectSubmission,
        queue: &mut QueueEngine<'_, H>,
    ) -> Result<OpaqueRef> {
        let c = caller(self.authentication, session)?;
        let pins = self.configuration.pins(&c)?;
        let s = self.root.flow_catalog(c.tenant())?;
        let version = s.active.get(definition).ok_or("inactive Flow")?;
        let d = &s.versions[definition][version];
        let p = compile_flow(d, &pins.context()?)?;
        self.submit(c, submission, pins, p, queue)
    }
    fn submit<H: ExecutorHandler + 'static>(
        &self,
        c: AuthenticatedCaller,
        submission: EffectSubmission,
        pins: FlowPins,
        plan: ExecutionPlan,
        queue: &mut QueueEngine<'_, H>,
    ) -> Result<OpaqueRef> {
        if !std::ptr::eq(self.root, queue.wi()) {
            return Err("Flow queue bundle mismatch".into());
        }
        if plan.document().body.nodes.iter().any(|n| {
            matches!(n.operation, PlanOperation::Wait { .. } | PlanOperation::Timer { .. })
        }) {
            return Err("unsupported WIP-07 execution profile: wait/timer requires WIP-08".into());
        }
        let at = self.clock.now()?;
        let snapshot = verify_snapshot(self.root, self.evidence, c.tenant(), &submission, &at)?;
        let agency = resolve_agency(&c, &submission, &snapshot.facts().bindings, &at)?;
        if submission.input.artifact.scope != *c.tenant() {
            return Err("foreign Flow submission artifact".into());
        }
        let artifacts = ArtifactResolver::new(self.root, self.configuration);
        artifacts.read(&c, &submission.input.artifact)?;
        for n in &plan.document().body.nodes {
            for (port, input) in &n.inputs {
                if let InputSource::Artifact { reference } = &input.source {
                    if reference.scope != *c.tenant() {
                        return Err("foreign plan artifact".into());
                    }
                    let b = artifacts.read(&c, reference)?;
                    plan.verify_input_artifact(&n.id, port, &b)?;
                }
            }
        }
        // Dedup includes immutable plan and full attribution, never regenerated from a plan.
        if let Some(r) =
            self.root.flow_request(c.tenant(), c.principal(), &submission.client_request_id)?
        {
            if r.submission != submission
                || r.plan_hash != *plan.hash()
                || r.agency != agency
                || r.pins != pins
                || r.queue_store.as_str() != queue.store_id().to_string()
            {
                return Err("Flow request conflict".into());
            }
            let request = serde_json::from_slice(&r.coordinator_request)?;
            queue.ensure_task(request)?;
            return Ok(r.id.clone());
        }
        let task = TaskId::new();
        let mut r = FlowRun {
            id: uuid::Uuid::new_v4().to_string().try_into()?,
            tenant: c.tenant().clone(),
            principal: c.principal().clone(),
            agency,
            submission,
            plan: plan.canonical_bytes().to_vec(),
            plan_hash: plan.hash().clone(),
            admission_hash: worldinterface_protocol::ContentHash::of(b"pending"),
            pins,
            origin: plan.document().origin.clone(),
            queue_store: queue.store_id().to_string().try_into()?,
            coordinator_request: vec![],
            coordinator_task: task.to_string().try_into()?,
            node_tasks: plan
                .document()
                .body
                .nodes
                .iter()
                .map(|n| Ok((n.id.clone(), TaskId::new().to_string().try_into()?)))
                .collect::<Result<_>>()?,
            wait_id: WaitId::new().to_string().try_into()?,
            proposal: None,
            cancel_requested: false,
            parent_run: None,
            bindings: BTreeMap::new(),
            wakes_delivered: BTreeMap::new(),
            results: BTreeMap::new(),
            outcome: None,
            exports: BTreeMap::new(),
        };
        let spec = TaskSpec::new(
            task,
            TaskPayload::new(canonical_data(&FlowPayload::new(&r, None))?),
            RunPolicy::Once,
            constraints(None)?,
            TaskMetadata::default(),
        )?;
        let causal = CausalContext::new(
            TraceId::new(format!("wi/flow/{}", r.id.as_str()))?,
            CorrelationId::new(format!("wi/flow/{}", r.id.as_str()))?,
        )
        .with_submitting_principal_ref(actionqueue_core::bounded::OpaqueRef::new(
            c.principal().as_str(),
        )?);
        let request = EnsureTaskRequest::new(
            AdmissionKey::new(format!("wi-flow:{}", r.id.as_str()))?,
            spec,
            vec![],
            causal,
            None,
        )?;
        r.coordinator_request = serde_json::to_vec(&request)?;
        r.admission_hash = r.admission_digest()?;
        if let Err(error) =
            transact(self.root, c.tenant(), FlowCommand::AdmitRun { run: Box::new(r.clone()) })
        {
            // A concurrent identical submit reloads the winning immutable admission.
            let winner = self
                .root
                .flow_request(c.tenant(), c.principal(), &r.submission.client_request_id)?
                .filter(|v| {
                    v.principal == r.principal
                        && v.submission == r.submission
                        && v.plan_hash == r.plan_hash
                        && v.pins == r.pins
                        && v.agency == r.agency
                        && v.queue_store == r.queue_store
                })
                .ok_or(error)?;
            queue.ensure_task(serde_json::from_slice(&winner.coordinator_request)?)?;
            return Ok(winner.id.clone());
        }
        queue.ensure_task(request)?;
        Ok(r.id)
    }
    pub fn inspect_run(&self, session: &A::Session, run: &OpaqueRef) -> Result<FlowRun> {
        let c = caller(self.authentication, session)?;
        let s = self.root.flow_run_snapshot(c.tenant(), run)?;
        let r =
            s.runs.get(run).filter(|r| r.principal == *c.principal()).ok_or("unknown Flow run")?;
        let plan = r.restore()?;
        let artifacts = ArtifactResolver::new(self.root, self.configuration);
        for reference in plan
            .document()
            .body
            .nodes
            .iter()
            .flat_map(|n| n.inputs.values())
            .filter_map(|i| match &i.source {
                InputSource::Artifact { reference } => Some(reference),
                _ => None,
            })
            .chain(r.results.values().flat_map(|v| v.outputs.values()))
            .chain(r.exports.values())
        {
            artifacts.read(&c, reference)?;
        }
        Ok(r.clone())
    }
}
