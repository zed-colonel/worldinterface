//! Exact admitted execution becomes a process run; request bytes cannot mint run authority.
use super::evaluation::{EvaluationVerifier, TrustedApprovals};
use crate::{completion::BoundExecution, worker_process::*, Result};
use serde::{Deserialize, Serialize};
use worldinterface_domain::worker::{WorkerBinding, WorkerOperation};
use worldinterface_protocol::*;
use worldinterface_storage::StoreRoot;
use worldinterface_worker::{
    controls::ControlEvidenceV1,
    credential::CredentialBindingV1,
    mapping::{Operation, ResolvedWorkerV1, WorkerRequestV1},
};

pub const OPERATIONS: [Operation; 6] = [
    Operation::Provision,
    Operation::Start,
    Operation::Inspect,
    Operation::Terminate,
    Operation::CollectArtifacts,
    Operation::Release,
];
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerEffect {
    pub version: u32,
    pub case: String,
    pub worker: WorkerRequestV1,
    pub task: Task,
    pub controls: ControlEvidenceV1,
    pub inputs: Vec<ArtifactRef>,
    pub exports: Vec<Export>,
}
pub struct AdmittedWorker {
    pub catalog: ContentHash,
    pub registration: worldinterface_capability::process_catalog::ProcessRegistrationRecord,
    pub artifact_policy: worldinterface_domain::worker::WorkerArtifactPolicy,
    pub authorization: AuthorizedRun,
    pub binding: WorkerBinding,
    pub operation: Operation,
    pub provider_operation: Option<WorkerOperation>,
}
impl WorkerEffect {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 64 * 1024 {
            return Err("worker effect bound".into());
        }
        let effect: Self = serde_json::from_value(strict_json(bytes)?)?;
        if effect.version != 1 || canonical_data(&effect)? != bytes {
            return Err("worker effect encoding".into());
        }
        effect.worker.validate()?;
        effect.controls.validate()?;
        Ok(effect)
    }
}
pub fn derive<S: TrustedApprovals>(
    root: &StoreRoot,
    bound: &BoundExecution,
    evaluation: &EvaluationVerifier<S>,
    now_ms: u64,
    executable: &ContentHash,
    helper: &ContentHash,
    adapter: &ContentHash,
) -> Result<AdmittedWorker> {
    let demand = WorkerEffect::decode(&bound.input)?;
    let request = &demand.worker;
    let state = root.effect_snapshot(&bound.tenant)?;
    let effect = state.effects.get(&bound.intent).ok_or("worker effect absent")?;
    let authorization = effect.authorization.as_ref().ok_or("worker effect unauthorized")?;
    if authorization.decision != Decision::Allow
        || ContentHash::of(&canonical_data(authorization)?) != bound.authorization
    {
        return Err("worker authorization drift".into());
    }
    let facts = effect.authorization_inputs.as_ref().ok_or("worker authority absent")?;
    let w = facts.worker.as_ref().ok_or("worker realization absent")?;
    if facts.qualification.is_some()
        || facts.fixture.is_none()
        || w.provider.as_str() != request.provider
        || w.run.as_str() != request.run
        || w.operation.as_str() != request.operation.name()
        || w.input != effect.intent.submission.input.artifact
        || w.aq_version != dependencies_digest()
        || w.manifest.executable.content != *executable
        || w.manifest.profile_definition.content.as_str() != request.profile.definition_sha256
        || w.exports != ContentHash::of(&canonical_data(&demand.exports)?)
        || w.manifest.configuration.content
            != AuthorizedRun::configuration_digest(&demand.task, &demand.exports)?
        || w.manifest.adapter.content != *adapter
        || w.manifest.dependencies.iter().map(|d| d.content.clone()).collect::<Vec<_>>()
            != vec![dependencies_digest()]
        || request.controls_digest != AuthorizedRun::controls_digest(&demand.controls)?
        || demand.controls.provider != request.provider
        || demand.controls.run != request.run
        || !demand.controls.observed.is_empty()
    {
        return Err("worker realization drift".into());
    }
    let verified = evaluation.authorize(
        now_ms,
        &demand.case,
        request.operation.name(),
        &demand.task,
        &w.environment,
    )?;
    if &w.wi_version != verified.digest() {
        return Err("worker WI source binding drift".into());
    }
    let installation = w.installation.as_ref().ok_or("process control installation absent")?;
    if installation.wall_ms > evaluation.manifest.max_wall_ms
        || installation.output_bytes > evaluation.manifest.max_output_bytes
    {
        return Err("evaluation control bounds exceeded".into());
    }
    for (kind, limit) in [
        (worldinterface_worker::controls::ControlKind::WallTimeMs, installation.wall_ms),
        (worldinterface_worker::controls::ControlKind::OutputBytes, installation.output_bytes),
    ] {
        if !demand.controls.applied.iter().any(|c| {
            c.kind == kind
                && c.state == worldinterface_worker::controls::ControlState::Supported(limit)
        }) {
            return Err("process control installation drift".into());
        }
    }
    if demand.inputs.len() > 1 || demand.exports.len() > 4 {
        return Err("worker artifact scope bound".into());
    }
    let mut inputs = Vec::new();
    for reference in &demand.inputs {
        if reference.scope != bound.tenant || reference.size > 1024 * 1024 {
            return Err("worker input scope".into());
        }
        inputs.push(Input {
            reference: format!("input:{}", reference.content.as_str()),
            digest: reference.content.clone(),
            bytes: root.read_artifact(reference)?,
        });
    }
    if request.input_digest != AuthorizedRun::input_digest(&inputs)? {
        return Err("worker input digest drift".into());
    }
    let current = worldinterface_capability::process_catalog::ProcessCatalogBinding {
        version: 1,
        profile: w.profile.clone(),
        profile_definition: w.manifest.profile_definition.content.clone(),
        descriptor: ContentHash::of(&canonical_data(
            &OPERATIONS
                .iter()
                .map(|op| super::connector::descriptor(*op))
                .collect::<Result<Vec<_>>>()?,
        )?),
        adapter: adapter.clone(),
        executable: executable.clone(),
        helper: helper.clone(),
        dependencies: vec![dependencies_digest()],
        configuration: w.manifest.configuration.content.clone(),
        // Exact-demand sets vary by operation and are reauthorized by the domain.
        // The policy revision and governing rules must remain stable across a run.
        policy: ContentHash::of(&canonical_data(&facts.policy)?),
        controls: request.controls_digest.clone(),
        aq: w.aq_version.clone(),
        wi: w.wi_version.clone(),
        environment: w.environment.clone(),
        fixture: verified.digest().clone(),
        inputs: request.input_digest.clone(),
        exports: w.exports.clone(),
    };
    let record =
        worldinterface_capability::process_catalog::ProcessRegistrationRecord::Evaluation {
            binding: current.clone(),
            approval: verified.digest().clone(),
        };
    let registration = super::catalog::verify_registration(&record, &current, evaluation, now_ms)?;
    // A run spans several exact-demand policies. Each complete policy is
    // independently registered above; only its stable governing rules enter
    // the shared run identity and quarantine fence.
    let mut run_catalog = registration.binding().clone();
    run_catalog.policy = super::catalog::policy_digest(&facts.policy)?;
    let catalog = run_catalog.digest()?;
    let principal = effect.agency.binding.principal.clone();
    let run_digest = ContentHash::of(&canonical_data(&(
        "wi-worker-run-2",
        &catalog,
        verified.digest(),
        &bound.tenant,
        &principal,
        &request.provider,
        &request.run,
        (&w.manifest, &w.aq_version, &w.wi_version, executable, helper),
        &request.input_digest,
        &request.controls_digest,
        &demand.task,
        &demand.exports,
    ))?);
    let operations: Vec<_> = OPERATIONS
        .iter()
        .map(|op| {
            let mut r = request.clone();
            r.operation = *op;
            r.request = format!("{}:{}:{}", request.provider, request.run, op.name());
            let resolved = ResolvedWorkerV1 {
                schema: 1,
                request_digest: r.digest()?,
                provider: r.provider.clone(),
                run: r.run.clone(),
                adapter_digest: adapter.clone(),
                executable_digest: executable.clone(),
                dependencies_digest: dependencies_digest(),
                configuration_digest: AuthorizedRun::configuration_digest(
                    &demand.task,
                    &demand.exports,
                )?,
                authorization_digest: run_digest.clone(),
                controls_digest: r.controls_digest.clone(),
            };
            Ok((r, resolved))
        })
        .collect::<Result<_>>()?;
    if !operations.iter().any(|(r, _)| r == request) {
        return Err("unstable worker operation identity".into());
    }
    let binding = WorkerBinding {
        tenant: bound.tenant.clone(),
        principal: principal.clone(),
        provider: request.provider.clone().try_into()?,
        run: request.run.clone().try_into()?,
        request: format!("{}:{}:start", request.provider, request.run).try_into()?,
        realization: run_digest.clone(),
    };
    let credential = CredentialBindingV1 {
        schema: 1,
        reference: format!("worker-credential:{}", run_digest.as_str()),
        issuer: "worldinterface".into(),
        audience: "process-managed-proof-1".into(),
        principal: principal.as_str().to_string(),
        provider: request.provider.clone(),
        run: request.run.clone(),
        operations: OPERATIONS.to_vec(),
        artifact_scope: demand.exports.iter().map(|e| e.reference.clone()).collect(),
        authorization_digest: run_digest,
        expires_unix_ms: now_ms.checked_add(120_000).ok_or("credential expiry overflow")?,
        revocation_generation: 0,
    };
    let provider_operation = if request.operation == Operation::Inspect {
        None
    } else {
        let execution = bound
            .resolved
            .connector_execution
            .as_ref()
            .ok_or("worker connector execution absent")?;
        Some(WorkerOperation {
            key: execution.provider_key.clone().ok_or("worker provider key absent")?,
            intent: bound.intent.clone(),
            operation: request.operation.name().to_string().try_into()?,
            canonical_request: ContentHash::of(&bound.input),
            acknowledged: false,
        })
    };
    if !matches!(request.operation, Operation::Inspect | Operation::Terminate) {
        super::catalog::authorize_admission(root, &binding, &registration)?;
    }
    Ok(AdmittedWorker {
        catalog,
        registration: record,
        artifact_policy: super::artifacts::policy(
            demand.inputs.clone(),
            &demand.exports,
            installation.output_bytes,
        )?,
        operation: request.operation,
        provider_operation,
        binding,
        authorization: AuthorizedRun {
            scope: HostScope {
                tenant: bound.tenant.as_str().to_string(),
                principal: principal.as_str().to_string(),
            },
            executable: "evaluation-fixture".into(),
            operations,
            task: demand.task,
            inputs,
            exports: demand.exports,
            controls: demand.controls,
            credential,
        },
    })
}
