//! Interrupted launches are quarantined; absence of acknowledgement never permits replacement.
use crate::Result;
use worldinterface_domain::worker::{WorkerBinding, WorkerCollection, WorkerFact};
use worldinterface_storage::StoreRoot;
pub fn quarantine(root: &StoreRoot, binding: &WorkerBinding) -> Result<()> {
    let state = root
        .worker_snapshot(&binding.tenant, &binding.provider, &binding.run)?
        .ok_or("worker absent")?;
    if state.binding != *binding {
        return Err("worker recovery binding drift".into());
    }
    if !state.quarantined && !state.cleanup_confirmed {
        if !state.artifact_policy.is_empty() {
            let operation = state
                .operations
                .values()
                .find(|o| o.operation.as_str() == "collect_artifacts")
                .map(|o| o.key.clone())
                .unwrap_or("wi:recovery".to_string().try_into()?);
            super::artifacts::quarantine(
                root,
                binding,
                &operation,
                "ambiguous-worker-restart",
                vec![],
            )?;
        } else {
            root.record_worker_transition(&state, WorkerFact::Quarantined)?;
        }
    }
    Ok(())
}

/// Recover bounded pages without adopting a PID or permitting replacement.
pub fn recover_quarantines(
    root: &StoreRoot,
    scope: &crate::worker_process::HostScope,
) -> Result<()> {
    let mut after: Option<(String, String, String)> = None;
    loop {
        let page = root.unfinished_workers(
            after.as_ref().map(|(t, p, r)| (t.as_str(), p.as_str(), r.as_str())),
            64,
        )?;
        for state in &page {
            let b = &state.binding;
            if b.tenant.as_str() == scope.tenant && b.principal.as_str() == scope.principal {
                match &state.collection {
                    WorkerCollection::Prepared(_) => {
                        let _ = super::artifacts::resume(root, b);
                    }
                    WorkerCollection::Published(_) => {}
                    _ => quarantine(root, b)?,
                }
            }
        }
        if page.len() < 64 {
            break;
        }
        let b = &page.last().unwrap().binding;
        after = Some((b.tenant.as_str().into(), b.provider.as_str().into(), b.run.as_str().into()));
    }
    Ok(())
}
