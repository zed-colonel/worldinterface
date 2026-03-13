//! Background tick loop for driving AQ execution and coordinator resumption.

use std::collections::HashMap;
use std::sync::Arc;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::RunState;
use actionqueue_core::time::clock::SystemClock;
use actionqueue_runtime::engine::BootstrappedEngine;
use tokio::sync::watch;
use worldinterface_contextstore::SqliteContextStore;
use worldinterface_coordinator::FlowHandler;
use worldinterface_core::id::FlowRunId;
use worldinterface_core::metrics::MetricsRecorder;

use crate::host::EngineSlot;
use crate::status::find_latest_run;

pub(crate) type Engine = BootstrappedEngine<FlowHandler<SqliteContextStore>, SystemClock>;

/// Run the background tick loop.
///
/// This task drives AQ execution by calling `engine.tick()` at the configured
/// interval and unconditionally resuming all suspended Coordinators after each tick.
pub(crate) async fn tick_loop(
    engine: EngineSlot,
    coordinator_map: Arc<std::sync::Mutex<HashMap<FlowRunId, TaskId>>>,
    store: Arc<SqliteContextStore>,
    tick_interval: std::time::Duration,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<dyn MetricsRecorder>,
) {
    let mut interval = tokio::time::interval(tick_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticks_since_gc: u64 = 0;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(e) = do_tick(&engine, &coordinator_map, &store, &mut ticks_since_gc, &*metrics).await {
                    tracing::error!(error = %e, "tick loop error");
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!("tick loop received shutdown signal");
                    break;
                }
            }
        }
    }
}

/// Execute a single tick: drive AQ dispatch, then resume suspended coordinators.
async fn do_tick(
    engine: &EngineSlot,
    coordinator_map: &Arc<std::sync::Mutex<HashMap<FlowRunId, TaskId>>>,
    store: &SqliteContextStore,
    ticks_since_gc: &mut u64,
    metrics: &dyn MetricsRecorder,
) -> Result<(), crate::error::HostError> {
    let mut guard = engine.lock().await;
    let engine = match guard.as_mut() {
        Some(e) => e,
        None => return Ok(()), // Engine was taken during shutdown
    };

    // 1. Drive AQ dispatch
    let _tick_result = engine.tick().await?;

    // 2. Resume all suspended coordinators
    resume_suspended_coordinators(engine, coordinator_map)?;

    // 3. Periodically prune terminal flows from coordinator map (every ~100 ticks)
    *ticks_since_gc += 1;
    if *ticks_since_gc >= 100 {
        *ticks_since_gc = 0;
        prune_terminal_flows(engine, coordinator_map, store, metrics);
    }

    Ok(())
}

/// Remove terminal flows from the coordinator map and persist the pruned map.
///
/// A flow is terminal when its Coordinator's latest run is in a terminal state
/// (Completed, Failed, or Canceled). These entries are no longer needed for
/// resume logic and can be safely removed.
fn prune_terminal_flows(
    engine: &Engine,
    coordinator_map: &std::sync::Mutex<HashMap<FlowRunId, TaskId>>,
    store: &SqliteContextStore,
    metrics: &dyn MetricsRecorder,
) {
    let mut map = coordinator_map.lock().unwrap();
    let before = map.len();

    map.retain(|_flow_run_id, &mut coordinator_task_id| {
        match find_latest_run(engine.projection(), coordinator_task_id) {
            Some(run) => {
                if run.state().is_terminal() {
                    // Record flow terminal metrics
                    match run.state() {
                        RunState::Completed => metrics.record_flow_completed(),
                        RunState::Failed => metrics.record_flow_failed(),
                        _ => {}
                    }
                    false // prune
                } else {
                    true // keep
                }
            }
            // No run yet — keep (still pending)
            None => true,
        }
    });

    let pruned = before - map.len();
    if pruned > 0 {
        drop(map); // Release lock before persisting
        if let Err(e) = crate::helpers::persist_coordinator_map(store, coordinator_map) {
            tracing::warn!(error = %e, "failed to persist pruned coordinator map");
        } else {
            tracing::debug!(pruned, "pruned terminal flows from coordinator map");
        }
    }
}

/// Resume all suspended Coordinator runs.
///
/// This is unconditional — we don't track whether children have progressed.
/// The Coordinator is stateless and re-derives everything from durable state.
/// If nothing changed, it submits no new steps and suspends again.
fn resume_suspended_coordinators(
    engine: &mut Engine,
    coordinator_map: &std::sync::Mutex<HashMap<FlowRunId, TaskId>>,
) -> Result<(), crate::error::HostError> {
    let map = coordinator_map.lock().unwrap();

    for (_flow_run_id, &coordinator_task_id) in map.iter() {
        let run = match find_latest_run(engine.projection(), coordinator_task_id) {
            Some(r) => r,
            None => continue,
        };

        if run.state() != RunState::Suspended {
            continue;
        }

        engine.resume_run(run.id())?;
    }

    Ok(())
}
