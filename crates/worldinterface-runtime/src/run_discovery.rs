//! Recoverable run mapping from the active AQ engine's authorized inspector.
use crate::Result;
use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_executor_local::handler::ExecutorHandler;
use actionqueue_runtime::{
    engine::BootstrappedEngine,
    inspection::{InspectionError, Query},
};
use worldinterface_domain::{agency::AuthenticatedCaller, effect::EffectAction};
use worldinterface_egress::Egress;
use worldinterface_protocol::{EffectIntentId, Timestamp};
use worldinterface_storage::StoreRoot;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiscoveryProgress {
    pub observed: usize,
    pub complete: bool,
}

fn scan(
    task: TaskId,
    pages_limit: usize,
    mut fetch: impl FnMut(
        Option<String>,
    ) -> std::result::Result<
        (Vec<(TaskId, RunId)>, Option<String>),
        InspectionError,
    >,
    mut record: impl FnMut(RunId) -> Result<()>,
) -> Result<DiscoveryProgress> {
    let mut cursor = None;
    let mut count = 0;
    let mut restarts = 0;
    for _ in 0..pages_limit {
        let (items, next) = match fetch(cursor.clone()) {
            Ok(page) => page,
            Err(InspectionError::StaleCursor) if restarts < 2 => {
                cursor = None;
                restarts += 1;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        for (owner, run) in items {
            if owner == task {
                record(run)?;
                count += 1;
            }
        }
        cursor = next;
        if cursor.is_none() {
            return Ok(DiscoveryProgress { observed: count, complete: true });
        }
    }
    Ok(DiscoveryProgress { observed: count, complete: false })
}

pub fn discover<H: ExecutorHandler + 'static>(
    root: &StoreRoot,
    caller: &AuthenticatedCaller,
    at: Timestamp,
    id: &EffectIntentId,
    queue_store: uuid::Uuid,
    engine: &BootstrappedEngine<H>,
    pages_limit: usize,
) -> Result<DiscoveryProgress> {
    if pages_limit == 0 || pages_limit > 128 {
        return Err("invalid discovery page bound".into());
    }
    let egress = Egress::new(root);
    let admission =
        egress.inspect(caller, id)?.dispatch.and_then(|d| d.admission).ok_or("no admission")?;
    if admission.queue_store.as_str() != queue_store.to_string() {
        return Err("queue store changed during discovery".into());
    }
    if admission.acknowledged_sequence.is_none() {
        return Err("admission not acknowledged".into());
    }
    let task = admission.task.as_str().parse::<TaskId>()?;
    scan(
        task,
        pages_limit,
        |cursor| {
            let inspector = engine.inspector()?;
            let page =
                inspector.list_runs(&Query { cursor, limit: Some(32), ..Default::default() })?;
            Ok((page.items.into_iter().map(|r| (r.task_id, r.run_id)).collect(), page.next_cursor))
        },
        |run| {
            egress.apply(
                caller,
                at.clone(),
                EffectAction::ObserveRun { id: id.clone(), run: run.to_string().try_into()? },
            )?;
            Ok(())
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_cursor_restarts_and_partial_page_is_not_complete() {
        let task = TaskId::new();
        let foreign = TaskId::new();
        let r1 = RunId::new();
        let r2 = RunId::new();
        let mut calls = 0;
        let mut seen = std::collections::BTreeSet::new();
        let result = scan(
            task,
            3,
            |cursor| {
                calls += 1;
                match calls {
                    1 => {
                        assert!(cursor.is_none());
                        Ok((vec![(task, r1), (foreign, RunId::new())], Some("old".into())))
                    }
                    2 => {
                        assert_eq!(cursor.as_deref(), Some("old"));
                        Err(InspectionError::StaleCursor)
                    }
                    3 => {
                        assert!(cursor.is_none());
                        Ok((vec![(task, r1)], Some("new".into())))
                    }
                    _ => panic!("page bound"),
                }
            },
            |run| {
                seen.insert(run);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(result, DiscoveryProgress { observed: 2, complete: false });
        assert_eq!(seen.len(), 1);
        let result = scan(
            task,
            2,
            |cursor| {
                if cursor.is_none() {
                    Ok((vec![(task, r1)], Some("next".into())))
                } else {
                    Ok((vec![(task, r2)], None))
                }
            },
            |run| {
                seen.insert(run);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(result, DiscoveryProgress { observed: 2, complete: true });
        assert_eq!(seen.len(), 2);
    }
}
