//! Durable AQ admission. Only this module handles concrete queue request types.
use crate::{aq_adapter::TaskInputLocator, Result};
use actionqueue_core::{
    admission::{EnsureTaskOutcome, EnsureTaskRequest},
    bounded::OpaqueRef as AqOpaqueRef,
    causal::CausalContext,
    ids::{AdmissionKey, CorrelationId, TaskId, TraceId},
    task::{
        constraints::TaskConstraints, metadata::TaskMetadata, run_policy::RunPolicy,
        safety::SafetyLevel, task_spec::TaskSpec,
    },
};
use actionqueue_executor_local::handler::ExecutorHandler;
use actionqueue_runtime::engine::BootstrappedEngine;
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::VerifiedSnapshot,
    effect::{EffectAction, EffectView, ProductionAdmission},
};
use worldinterface_egress::Egress;
use worldinterface_protocol::{ContentHash, EffectIntentId, Timestamp};
use worldinterface_storage::StoreRoot;

fn invalid(message: &'static str) -> crate::Result<()> {
    Err(message.into())
}
fn aq_digest(request: &EnsureTaskRequest) -> Result<ContentHash> {
    Ok(hex::encode(request.digest()?.hash().bytes()).try_into()?)
}
fn encode_request(request: &EnsureTaskRequest) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(request)?;
    if bytes.len() > 65536 {
        return Err("AQ request artifact exceeds bound".into());
    }
    Ok(bytes)
}
fn decode_request(bytes: &[u8]) -> Result<EnsureTaskRequest> {
    if bytes.len() > 65536 {
        return Err("AQ request artifact exceeds bound".into());
    }
    Ok(serde_json::from_slice(bytes)?)
}
/// Prepare exactly one complete request. The journal transaction also reserves authorization use.
/// A losing concurrent preparer reloads the winner's immutable artifact.
pub fn prepare(
    root: &StoreRoot,
    caller: &AuthenticatedCaller,
    at: Timestamp,
    id: &EffectIntentId,
    snapshot: VerifiedSnapshot,
    queue_store: uuid::Uuid,
) -> Result<ProductionAdmission> {
    let egress = Egress::new(root);
    let view = egress.inspect(caller, id)?;
    if let Some(existing) = view.dispatch.as_ref().and_then(|d| d.admission.as_ref()) {
        if existing.queue_store.as_str() != queue_store.to_string() {
            return Err("queue store changed for prepared admission".into());
        }
        return Ok(existing.clone());
    }
    let index = root.effect_snapshot(caller.tenant())?.effects[id].dispatches.len();
    let task = TaskId::new();
    let identity = worldinterface_domain::ingress::digest(&(caller.tenant(), id, index as u32))
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
    let key = AdmissionKey::new(format!("wi-effect:{}", identity.as_str()))?;
    let payload = TaskInputLocator::new(view.intent.submission.input.artifact.clone()).payload()?;
    // Effect retries require WI provider noncommit evidence; AQ must not independently retry.
    let timeout = view.intent.submission.budget.timeout_millis.div_ceil(1000).max(1);
    let mut constraints = TaskConstraints::new(1, Some(timeout), None)?;
    constraints.set_safety_level(SafetyLevel::Transactional);
    let spec = TaskSpec::new(task, payload, RunPolicy::Once, constraints, TaskMetadata::default())?;
    let causal = CausalContext::new(
        TraceId::new(format!("wi/effect/{}", id.as_str()))?,
        CorrelationId::new(format!("wi/effect/{}", id.as_str()))?,
    )
    .with_authorization_ref(AqOpaqueRef::new(
        view.authorization.as_ref().ok_or("no authorization")?.exact_binding.as_str(),
    )?)
    .with_submitting_principal_ref(AqOpaqueRef::new(caller.principal().as_str())?);
    let request = EnsureTaskRequest::new(key, spec, vec![], causal, None)?;
    let bytes = encode_request(&request)?;
    let artifact = root.publish(caller.tenant().clone(), &bytes)?;
    let execution = {
        let state = root.effect_snapshot(caller.tenant())?;
        let e = &state.effects[id];
        worldinterface_domain::ingress::digest(&(
            &e.intent,
            &e.resolution,
            e.authorization.as_ref().ok_or("no authorization")?,
            snapshot.facts(),
            index as u32,
        ))
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?
    };
    let admission = ProductionAdmission {
        version: 1,
        queue_store: queue_store.to_string().try_into()?,
        task: task.to_string().try_into()?,
        admission_key: request.admission_key().as_str().to_owned().try_into()?,
        execution,
        request_artifact: artifact.clone(),
        request_hash: ContentHash::of(&bytes),
        canonical_version: request.digest()?.canonical_version(),
        canonical_digest: aq_digest(&request)?,
        acknowledged_sequence: None,
        observed_runs: vec![],
    };
    match egress.apply(
        caller,
        at,
        EffectAction::PrepareProduction {
            id: id.clone(),
            snapshot,
            admission: Box::new(admission.clone()),
        },
    ) {
        Ok(_) => Ok(admission),
        Err(e)
            if e.to_string().contains("admission binding conflict")
                || e.to_string().contains("dispatch preparation phase") =>
        {
            let existing =
                egress.inspect(caller, id)?.dispatch.and_then(|d| d.admission).ok_or(e)?;
            if existing.queue_store.as_str() != queue_store.to_string() {
                return Err("queue store changed for prepared admission".into());
            }
            Ok(existing)
        }
        Err(e) => Err(e),
    }
}
pub fn load_request(
    root: &StoreRoot,
    admission: &ProductionAdmission,
) -> Result<EnsureTaskRequest> {
    let bytes = root.read_artifact(&admission.request_artifact)?;
    if ContentHash::of(&bytes) != admission.request_hash {
        invalid("AQ request artifact integrity")?;
    }
    let request = decode_request(&bytes)?;
    if encode_request(&request)? != bytes
        || request.task_spec().id().to_string() != admission.task.as_str()
        || request.admission_key().as_str() != admission.admission_key.as_str()
        || request.digest()?.canonical_version() != admission.canonical_version
        || aq_digest(&request)? != admission.canonical_digest
    {
        invalid("AQ request binding mismatch")?;
    }
    Ok(request)
}

/// The host installs a current tenant-scoped control context on the engine before this call.
/// An error or uncertain acknowledgment leaves the request pending for identical retry.
pub fn deliver<H: ExecutorHandler + 'static>(
    root: &StoreRoot,
    caller: &AuthenticatedCaller,
    at: Timestamp,
    id: &EffectIntentId,
    fresh: VerifiedSnapshot,
    queue_store: uuid::Uuid,
    engine: &mut BootstrappedEngine<H>,
) -> Result<EnsureTaskOutcome> {
    let egress = Egress::new(root);
    // Reauthorization is mandatory even on duplicate AQ admission after a lost acknowledgment.
    egress.apply(
        caller,
        at.clone(),
        EffectAction::Authorize { id: id.clone(), snapshot: fresh },
    )?;
    let view: EffectView = egress.inspect(caller, id)?;
    let admission = view.dispatch.and_then(|d| d.admission).ok_or("unprepared admission")?;
    if admission.queue_store.as_str() != queue_store.to_string()
        || admission.request_artifact.scope != *caller.tenant()
    {
        return Err("queue or tenant binding mismatch".into());
    }
    if admission.acknowledged_sequence.is_some() {
        return Err("admission already acknowledged".into());
    }
    let request = load_request(root, &admission)?;
    let outcome = engine.ensure_task(request.clone())?;
    let (sequence, task_id, key, digest) = match &outcome {
        EnsureTaskOutcome::Created { sequence, task_id, admission_key, digest }
        | EnsureTaskOutcome::AlreadyExists { sequence, task_id, admission_key, digest } => {
            (*sequence, task_id, admission_key, digest)
        }
    };
    if sequence == 0
        || task_id.to_string() != admission.task.as_str()
        || key.as_str() != admission.admission_key.as_str()
        || digest.canonical_version() != admission.canonical_version
        || hex::encode(digest.hash().bytes()) != admission.canonical_digest.as_str()
    {
        return Err("AQ acknowledgment conflict".into());
    }
    egress.apply(caller, at, EffectAction::AcknowledgeProduction { id: id.clone(), sequence })?;
    Ok(outcome)
}
