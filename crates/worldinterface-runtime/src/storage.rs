//! Offline composition, without a second AQ projection or WAL reader.
use crate::Result;
use actionqueue_storage::store::{self, OpenOptions, StoreManifest};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};
use worldinterface_protocol::{canonical_data, strict_json, InstanceId};
use worldinterface_storage::{publish_directory, StoreProfile, StoreRoot};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleManifest {
    version: u32,
    wi: InstanceId,
    aq: uuid::Uuid,
}

fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
fn sync(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
fn destination_parent(destination: &Path) -> &Path {
    destination.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."))
}
fn require_independent_destination(source: &Path, destination: &Path) -> Result<()> {
    // Resolve aliases before any store recovery or staging can change the source.
    if destination_parent(destination).canonicalize()?.starts_with(source.canonicalize()?) {
        return Err("destination inside source".into());
    }
    Ok(())
}
fn validate(path: &Path) -> Result<BundleManifest> {
    for e in fs::read_dir(path)? {
        let e = e?;
        if e.file_type()?.is_symlink()
            || !["manifest.json", "bundle.lock", "wi", "aq"].iter().any(|n| e.file_name() == *n)
        {
            return Err("unexpected bundle member".into());
        }
    }
    let m: BundleManifest =
        serde_json::from_value(strict_json(&fs::read(path.join("manifest.json"))?)?)?;
    if m.version != 1 {
        return Err("unsupported bundle".into());
    }
    let aq = StoreManifest::read(&path.join("aq"))?;
    if aq.store_id != m.aq || !aq.features.is_empty() {
        return Err("AQ bundle identity/profile mismatch".into());
    }
    Ok(m)
}
fn publish(staging: tempfile::TempDir, dest: &Path) -> Result<()> {
    if dest.exists() {
        return Err("destination exists".into());
    }
    sync(staging.path())?;
    #[cfg(test)]
    crate::crash::checkpoint("before-rename");
    publish_directory(staging.path(), dest)?;
    #[cfg(test)]
    crate::crash::checkpoint("after-rename");
    sync(destination_parent(dest))
}
fn staging(dest: &Path) -> Result<tempfile::TempDir> {
    if dest.exists() {
        return Err("destination exists".into());
    }
    Ok(tempfile::Builder::new().prefix(".wi-bundle-").tempdir_in(destination_parent(dest))?)
}
/// Owns a WI root and the shared bundle lock. AQ runtimes opened with `aq_path`
/// retain their own exclusive writer lock. Offline maintenance acquires all locks
/// before copying anything, including against direct users of foundation/AQ APIs.
pub struct StoreBundle {
    wi: StoreRoot,
    path: std::path::PathBuf,
    convergence_faults: Vec<ConvergenceFault>,
    _lock: File,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConvergenceFault {
    pub tenant: worldinterface_protocol::OpaqueRef,
    pub intent: worldinterface_protocol::EffectIntentId,
    pub run: worldinterface_protocol::OpaqueRef,
    pub fabric: worldinterface_protocol::ExecutionOutcome,
    pub queue: actionqueue_core::run::state::RunState,
}
impl StoreBundle {
    pub fn initialize(path: &Path, profile: StoreProfile) -> Result<Self> {
        let stage = staging(path)?;
        let wi = StoreRoot::create_empty(stage.path().join("wi"), profile)?;
        let aq = store::open_store(
            &stage.path().join("aq"),
            OpenOptions::Initialize { features: vec![] },
        )?;
        let manifest = BundleManifest {
            version: 1,
            wi: wi.profile().instance.clone(),
            aq: aq.manifest().store_id,
        };
        drop(aq);
        drop(wi);
        write(&stage.path().join("bundle.lock"), b"")?;
        write(&stage.path().join("manifest.json"), &canonical_data(&manifest)?)?;
        drop(Self::open(stage.path())?);
        publish(stage, path)?;
        Self::open(path)
    }
    pub fn open(path: &Path) -> Result<Self> {
        let manifest = validate(path)?;
        let lock = File::open(path.join("bundle.lock"))?;
        FileExt::try_lock_shared(&lock)?;
        // Preflight AQ read-only before WI recovery can mutate a valid journal.
        let references = crate::aq_adapter::QueueReferences::open(&path.join("aq"))?;
        let wi = StoreRoot::open_for_instance(path.join("wi"), &manifest.wi, false)?;
        if wi.profile().instance != manifest.wi {
            return Err("WI bundle identity mismatch".into());
        }
        references.validate_artifacts(&wi)?;
        // AQ may have spent its interruption budget after Fabric Start. Its
        // terminal run cannot prove noncommit, so preserve Uncertain and the
        // original attempt without re-admission or a second port call.
        for tenant in wi.effect_tenants()? {
            let state = wi.effect_snapshot(&tenant)?;
            for (id, effect) in &state.effects {
                if effect.phase == worldinterface_domain::Phase::DispatchPending
                    && effect.attempt.is_none()
                {
                    if let Some(admission) =
                        effect.dispatches.last().and_then(|d| d.admission.as_ref())
                    {
                        for run in &admission.observed_runs {
                            let code = match references.run_state(run.as_str())? {
                                Some(actionqueue_core::run::state::RunState::Failed) => {
                                    "wi.effect.pre_start_exhausted"
                                }
                                Some(actionqueue_core::run::state::RunState::Canceled) => {
                                    "wi.effect.pre_start_canceled"
                                }
                                _ => continue,
                            };
                            wi.record_dispatch_fault(&tenant, id, run, code)?;
                        }
                    }
                }
                if effect.phase != worldinterface_domain::Phase::Running
                    || effect.outcomes.iter().any(|o| Some(&o.attempt) == effect.attempt.as_ref())
                {
                    continue;
                }
                let Some(admission) = effect.dispatches.last().and_then(|d| d.admission.as_ref())
                else {
                    continue;
                };
                let mut terminal_run = None;
                for run in &admission.observed_runs {
                    if matches!(
                        references.run_state(run.as_str())?,
                        Some(
                            actionqueue_core::run::state::RunState::Completed
                                | actionqueue_core::run::state::RunState::Failed
                                | actionqueue_core::run::state::RunState::Canceled
                        )
                    ) {
                        terminal_run = Some(run);
                    }
                }
                if let (Some(run), Some(attempt)) = (terminal_run, effect.attempt.as_ref()) {
                    crate::completion::record_uncertain_after_terminal_run(
                        &wi, &tenant, id, attempt, run,
                    )?;
                }
            }
        }
        let mut convergence_faults = Vec::new();
        for tenant in wi.effect_tenants()? {
            let state = wi.effect_snapshot(&tenant)?;
            for (id, effect) in &state.effects {
                let Some(outcome) = effect.outcomes.last() else {
                    continue;
                };
                if effect.attempt.as_ref() != Some(&outcome.attempt)
                    || matches!(
                        effect.phase,
                        worldinterface_domain::Phase::Authorized
                            | worldinterface_domain::Phase::DispatchPending
                            | worldinterface_domain::Phase::Running
                    )
                {
                    continue;
                }
                let Some(admission) = effect.dispatches.last().and_then(|d| d.admission.as_ref())
                else {
                    continue;
                };
                for run in &admission.observed_runs {
                    let Some(queue) = references.run_state(run.as_str())? else {
                        continue;
                    };
                    let expected_code = match outcome.outcome {
                        worldinterface_protocol::ExecutionOutcome::FailedBeforeCommit => {
                            Some("wi.effect.failed_before_commit")
                        }
                        worldinterface_protocol::ExecutionOutcome::CanceledBeforeCommit => {
                            Some("wi.effect.canceled_before_commit")
                        }
                        worldinterface_protocol::ExecutionOutcome::Uncertain => {
                            Some("wi.effect.uncertain")
                        }
                        worldinterface_protocol::ExecutionOutcome::Succeeded => None,
                    };
                    if matches!(
                        queue,
                        actionqueue_core::run::state::RunState::Completed
                            | actionqueue_core::run::state::RunState::Failed
                            | actionqueue_core::run::state::RunState::Canceled
                    ) && match expected_code {
                        None => queue != actionqueue_core::run::state::RunState::Completed,
                        Some(code) => {
                            let actual = references.terminal_failure_code(run.as_str())?;
                            let expected_uncertainty = outcome.outcome
                                == worldinterface_protocol::ExecutionOutcome::Uncertain
                                && matches!(
                                    actual.as_deref(),
                                    Some(
                                        "wi.effect.uncertain_recording_error"
                                            | "wi.effect.phase_unknown"
                                    )
                                );
                            queue != actionqueue_core::run::state::RunState::Failed
                                || (actual.as_deref() != Some(code) && !expected_uncertainty)
                        }
                    } {
                        let queue_label = match queue {
                            actionqueue_core::run::state::RunState::Completed => "completed",
                            actionqueue_core::run::state::RunState::Failed => "failed",
                            actionqueue_core::run::state::RunState::Canceled => "canceled",
                            _ => unreachable!(),
                        };
                        wi.record_convergence_fault(
                            &tenant,
                            id,
                            run,
                            &outcome.outcome,
                            queue_label,
                        )?;
                        convergence_faults.push(ConvergenceFault {
                            tenant: tenant.clone(),
                            intent: id.clone(),
                            run: run.clone(),
                            fabric: outcome.outcome.clone(),
                            queue,
                        });
                    }
                }
            }
        }
        Ok(Self { wi, path: path.to_path_buf(), convergence_faults, _lock: lock })
    }
    pub fn wi(&self) -> &StoreRoot {
        &self.wi
    }
    pub fn convergence_faults(&self) -> &[ConvergenceFault] {
        &self.convergence_faults
    }
    pub fn aq_path(&self) -> std::path::PathBuf {
        self.path.join("aq")
    }
    /// The retained bundle lock pins the validated AQ store identity.
    pub fn aq_store_id(&self) -> Result<uuid::Uuid> {
        Ok(validate(&self.path)?.aq)
    }
    pub fn backup(source: &Path, destination: &Path) -> Result<()> {
        require_independent_destination(source, destination)?;
        let manifest = validate(source)?;
        let lock = File::open(source.join("bundle.lock"))?;
        FileExt::try_lock_exclusive(&lock)?;
        // A retained AQ reader prevents writers even while backup_store obtains
        // its additional shared session. Retain WI maintenance across both copies.
        let aq = store::open_store(&source.join("aq"), OpenOptions::ReadOnly)?;
        let wi = StoreRoot::open_for_instance(source.join("wi"), &manifest.wi, true)?;
        if wi.profile().instance != manifest.wi {
            return Err("WI bundle identity mismatch".into());
        }
        let stage = staging(destination)?;
        wi.backup_to(stage.path().join("wi"))?;
        store::backup_store(aq.root(), &stage.path().join("aq"))?;
        write(&stage.path().join("bundle.lock"), b"")?;
        write(&stage.path().join("manifest.json"), &canonical_data(&manifest)?)?;
        drop(Self::open(stage.path())?);
        publish(stage, destination)
    }
    pub fn restore(backup: &Path, destination: &Path) -> Result<()> {
        require_independent_destination(backup, destination)?;
        let manifest = validate(backup)?;
        let lock = File::open(backup.join("bundle.lock"))?;
        FileExt::try_lock_exclusive(&lock)?;
        let aq = store::open_store(&backup.join("aq"), OpenOptions::ReadOnly)?;
        let wi = StoreRoot::open_for_instance(backup.join("wi"), &manifest.wi, true)?;
        if wi.profile().instance != manifest.wi {
            return Err("WI backup identity mismatch".into());
        }
        let stage = staging(destination)?;
        wi.backup_to(stage.path().join("wi"))?;
        store::restore_store(aq.root(), &stage.path().join("aq"))?;
        write(&stage.path().join("bundle.lock"), b"")?;
        write(&stage.path().join("manifest.json"), &canonical_data(&manifest)?)?;
        drop(Self::open(stage.path())?);
        publish(stage, destination)
    }
}
