//! Runtime catalog capabilities. Records and inspection alone cannot authorize work.
use super::evaluation::{EvaluationVerifier, TrustedApprovals};
use crate::{worker_process::HostScope, Result};
use worldinterface_capability::process_catalog::*;
use worldinterface_domain::{agency::AuthenticatedCaller, worker::*};
use worldinterface_protocol::*;
use worldinterface_storage::StoreRoot;

pub use worldinterface_capability::process_catalog;

pub struct VerifiedRegistration {
    binding: ProcessCatalogBinding,
}
impl VerifiedRegistration {
    pub fn binding(&self) -> &ProcessCatalogBinding {
        &self.binding
    }
}
/// Inspection describes support; it never returns an admission capability.
pub fn inspect_profile() -> Result<ProcessProfileInspection> {
    Ok(ProcessProfileInspection {
        profile: "process-managed-proof-1".to_string().try_into()?,
        operations: PROCESS_OPERATIONS
            .iter()
            .map(|s| s.to_string().try_into())
            .collect::<std::result::Result<_, _>>()?,
        mandatory_controls: ["wall_time_ms", "output_bytes"]
            .iter()
            .map(|s| s.to_string().try_into())
            .collect::<std::result::Result<_, _>>()?,
        registration: "unverified".to_string().try_into()?,
        denial_reasons: vec!["ordinary-use-disabled".to_string().try_into()?],
    })
}
/// Read-only inspection of current registration and durable provider disposition.
/// The verified capability is discarded; this result cannot reserve or dispatch.
pub fn inspect_registration<S: TrustedApprovals>(
    root: &StoreRoot,
    worker: &WorkerBinding,
    record: Option<&ProcessRegistrationRecord>,
    current: &ProcessCatalogBinding,
    evaluation: &EvaluationVerifier<S>,
    now: u64,
) -> Result<ProcessProfileInspection> {
    let mut inspection = inspect_profile()?;
    let (state, denial) =
        if root.worker_provider_quarantine(&worker.tenant, &worker.provider)?.is_some() {
            ("quarantined", Some("provider-quarantined"))
        } else if let Some(record) = record {
            if verify_registration(record, current, evaluation, now).is_ok() {
                ("evaluation-only", None)
            } else {
                ("invalidated", Some("registration-invalid"))
            }
        } else {
            ("unregistered", Some("registration-absent"))
        };
    inspection.registration = state.to_string().try_into()?;
    if let Some(reason) = denial {
        inspection.denial_reasons.push(reason.to_string().try_into()?);
    }
    Ok(inspection)
}
pub fn verify_registration<S: TrustedApprovals>(
    record: &ProcessRegistrationRecord,
    current: &ProcessCatalogBinding,
    evaluation: &EvaluationVerifier<S>,
    now: u64,
) -> Result<VerifiedRegistration> {
    current.validate()?;
    match record {
        ProcessRegistrationRecord::Evaluation { binding, approval } => {
            let verified = evaluation.verify(now)?;
            if !evaluation.trusted.registered(record, now)?
                || binding != current
                || approval != verified.digest()
                || binding.fixture != *approval
                || binding.profile.as_str() != evaluation.manifest.profile
                || binding.environment != evaluation.manifest.environment
            {
                return Err("catalog registration drift".into());
            }
        }
        ProcessRegistrationRecord::Ordinary { qualification: q } => {
            q.validate()?;
            if q.binding != *current
                || q.revoked
                || now < q.valid_from_ms
                || now >= q.expires_ms
                || !evaluation.trusted.qualified(q, now)?
            {
                return Err("qualification unavailable or inapplicable".into());
            }
            // No switch exists for enabling ordinary process use in this plan.
            return Err("ordinary process use disabled".into());
        }
    }
    Ok(VerifiedRegistration { binding: current.clone() })
}
pub fn authorize_admission(
    root: &StoreRoot,
    binding: &WorkerBinding,
    registration: &VerifiedRegistration,
) -> Result<ContentHash> {
    if root.worker_provider_quarantine(&binding.tenant, &binding.provider)?.is_some() {
        return Err("provider quarantined".into());
    }
    Ok(registration.binding.digest()?)
}
/// Trusted host composition supplies current recovery policy independently of old
/// admission approval. No Deserialize path, no launch or export authority.
pub struct RecoveryPolicy {
    scope: HostScope,
    current: ContentHash,
    originals: Vec<ContentHash>,
    digest: ContentHash,
}
impl RecoveryPolicy {
    pub fn new(
        scope: HostScope,
        current: ContentHash,
        originals: Vec<ContentHash>,
        digest: ContentHash,
    ) -> Result<Self> {
        if originals.is_empty() || originals.len() > 32 {
            return Err("recovery policy bound".into());
        }
        Ok(Self { scope, current, originals, digest })
    }
    pub fn digest(&self) -> &ContentHash {
        &self.digest
    }
}
pub struct RecoveryAuthorization {
    binding: WorkerBinding,
    operation: OpaqueRef,
    policy: ContentHash,
}
impl RecoveryAuthorization {
    pub fn binding(&self) -> &WorkerBinding {
        &self.binding
    }
    pub fn operation(&self) -> &OpaqueRef {
        &self.operation
    }
    pub fn policy(&self) -> &ContentHash {
        &self.policy
    }
}
pub fn authorize_recovery(
    root: &StoreRoot,
    caller: &AuthenticatedCaller,
    binding: &WorkerBinding,
    operation: &str,
    current: &ContentHash,
    policy: &RecoveryPolicy,
) -> Result<RecoveryAuthorization> {
    if !["inspect", "terminate", "reconcile"].contains(&operation)
        || binding.tenant.as_str() != policy.scope.tenant
        || binding.principal.as_str() != policy.scope.principal
        || caller.tenant() != &binding.tenant
        || caller.principal() != &binding.principal
        || current != &policy.current
    {
        return Err("recovery authority mismatch".into());
    }
    let state = root
        .worker_snapshot(&binding.tenant, &binding.provider, &binding.run)?
        .ok_or("recovery worker absent")?;
    let launch = root.worker_launch(binding)?.ok_or("recovery launch absent")?;
    if state.binding != *binding || !policy.originals.contains(&launch.implementation) {
        return Err("recovery execution binding mismatch".into());
    }
    Ok(RecoveryAuthorization {
        binding: binding.clone(),
        operation: operation.to_string().try_into()?,
        policy: policy.digest.clone(),
    })
}

/// Demand-specific authority sets are checked by the domain on every effect.
/// Stable governing policy rules are also bound by catalog registration.
pub fn policy_digest(p: &worldinterface_domain::authorization::Policy) -> Result<ContentHash> {
    Ok(ContentHash::of(&canonical_data(&(
        &p.revision,
        &p.authorizer,
        &p.grant_issuers,
        &p.approvers,
        p.approval_required,
        p.separation_of_duties,
        p.retry_after_noncommit,
    ))?))
}
