#![cfg(target_os = "linux")]
mod support {
    pub mod connectors;
    pub mod continuation_fixture;
    pub mod flow_fixture;
    pub mod spi_harness;
    pub mod worker_fixture;
}
use support::worker_fixture::*;
use worldinterface_runtime::worker_process::Task;
use worldinterface_storage::StoreKind;
use worldinterface_worker::mapping::Operation;
#[test]
fn worker_admission_retry_keeps_exact_agency_facts_without_launch() {
    use worldinterface_domain::agency::{resolve_agency, AuthenticatedCaller};
    use worldinterface_protocol::{canonical_data, Timestamp};
    let created = Timestamp::from_epoch_millis(1_000_000).unwrap();
    for op in worldinterface_runtime::worker::admission::OPERATIONS {
        let descriptor = worldinterface_runtime::worker::connector::descriptor(op).unwrap();
        let (first, facts) = admission_fixture(&descriptor, &created);
        std::thread::sleep(std::time::Duration::from_millis(2));
        let (retry, repeated) = admission_fixture(&descriptor, &created);
        assert_eq!(canonical_data(&first).unwrap(), canonical_data(&retry).unwrap());
        assert_eq!(canonical_data(&facts).unwrap(), canonical_data(&repeated).unwrap());
        let caller = AuthenticatedCaller::from_trusted_authentication(
            facts.bindings[0].tenant.clone(),
            facts.bindings[0].principal.clone(),
        );
        assert_eq!(
            resolve_agency(&caller, &first, &facts.bindings, &created).unwrap(),
            resolve_agency(&caller, &retry, &repeated.bindings, &created).unwrap()
        );
    }
}
#[tokio::test]
async fn wp_017_admitted_worker_intent_precedes_bounded_process_and_wakes_are_durable() {
    let mut h = Harness::new(Task::Probe, "WP-017", 1500, 65536);
    h.exports = vec![worldinterface_runtime::worker_process::Export {
        reference: "artifact-probe".into(),
        name: "probe.json".into(),
        digest: worldinterface_protocol::ContentHash::of(b"{\"clean\":true}"),
        size: 14,
    }];
    h.operation(Operation::Provision).await;
    assert!(!h.state().accepted && h.state().capacity_reserved);
    h.operation(Operation::Start).await;
    assert!(h.state().operations.values().any(|o| o.operation.as_str() == "start"
        && o.key.as_str().starts_with("wi-key-1:")
        && o.acknowledged));
    let slots = h.state().capacity_reserved;
    h.operation(Operation::Inspect).await;
    assert_eq!(h.state().capacity_reserved, slots);
    assert!(h.state().accepted);
    assert!(h.state().capacity_reserved);
    let report = h.finish().await;
    assert!(report.exit_confirmed && report.cleanup_confirmed && report.helper_launch_clean);
    assert!(report.retained_output_bytes <= 65536);
    let b = h.binding();
    assert_eq!(b.tenant, h.auth.0);
    assert_eq!(b.principal, h.auth.1);
    assert!(h.state().exited);
    assert!(h.state().capacity_reserved);
    // The real executor may already have published and acknowledged these wakes.
    // Durability includes those retained obligations, not just pending delivery.
    let signals = h.bundle.wi().signal_obligations(StoreKind::Fabric, None, 100).unwrap();
    assert!(signals.iter().any(|s| s.event.route.kind
        == worldinterface_domain::continuation::EventKind::WorkerObservation
        && s.event.route.correlation == b.event_correlation().unwrap()));
    for s in &signals {
        assert_eq!(
            h.bundle.wi().authoritative_event(StoreKind::Fabric, &s.event.id).unwrap(),
            s.event
        );
    }
    h.operation(Operation::CollectArtifacts).await;
    h.operation(Operation::Release).await;
    assert!(h.state().cleanup_confirmed);
    assert!(!h.state().capacity_reserved);
}
#[tokio::test]
async fn wp_018_duplicate_admission_and_start_preserve_one_immutable_run() {
    let h = Harness::new(Task::Probe, "WP-018", 1500, 65536);
    h.operation(Operation::Start).await;
    let binding = h.binding();
    h.operation(Operation::Start).await;
    assert_eq!(binding, h.binding());
    let report = h.finish().await;
    assert!(report.cleanup_confirmed);
    let journals = std::fs::read_dir(h.temp.path().join("process"))
        .unwrap()
        .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with(".wi-owner-"))
        .count();
    assert_eq!(journals, 1);
    assert_eq!(h.runtime.spawn_attempts(), Some(1));
    let mut changed = binding;
    changed.realization = worldinterface_protocol::ContentHash::of(b"changed canonical content");
    assert!(h.bundle.wi().reserve_start(changed, 1).is_err());
}
#[tokio::test]
async fn one_host_releases_capacity_for_more_than_64_sequential_runs() {
    async fn operation(h: &Harness, op: Operation) {
        let mut snapshot = h.demand(op).1;
        // The default 100-use grant runs out after 50 provision/release pairs,
        // before this test can reach the provider's former 64-run lifetime cap.
        for grant in &mut snapshot.grants {
            grant.max_uses = 256;
        }
        assert_eq!(
            h.operation_snapshot(op, snapshot).await,
            worldinterface_storage::flow::FlowOutcome::Succeeded,
            "run {} operation {op:?}",
            h.run
        );
    }
    let mut h = Harness::new(Task::Probe, "WP-018", 1500, 65536);
    for n in 0..66 {
        h.run = format!("sequential-{n}");
        h.controls.run = h.run.clone();
        operation(&h, Operation::Provision).await;
        if n == 65 {
            operation(&h, Operation::Start).await;
            assert!(h.finish().await.cleanup_confirmed);
        }
        operation(&h, Operation::Release).await;
        assert!(h.state().cleanup_confirmed);
        assert!(!h.state().capacity_reserved);
        // Durable acknowledgement makes release idempotent even after port retirement.
        operation(&h, Operation::Release).await;
    }
    assert_eq!(h.runtime.spawn_attempts(), Some(1));
}
#[tokio::test]
async fn wp_021_stop_obligation_precedes_bounded_descendant_cleanup() {
    let h = Harness::new(
        Task::Tree { early_exit: false, change_session: true, resist_term: true },
        "WP-021",
        3000,
        65536,
    );
    h.operation(Operation::Start).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !h.state().running {
        h.runtime.poll(&h.binding()).unwrap();
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    h.operation(Operation::Terminate).await;
    assert!(h.state().stop_requested);
    assert!(h.state().capacity_reserved);
    let report = h.finish().await;
    assert!(report.exit_confirmed && report.cleanup_confirmed);
    assert!(report.descendants_observed > 0);
    assert!(report.kill_signals > 0);
    h.operation(Operation::Release).await;
    assert!(!h.state().capacity_reserved);
}
#[tokio::test]
async fn wp_022_wall_and_output_limits_are_enforced_after_real_admission() {
    for (task, wall, output, reason) in [
        (Task::Sleep { millis: 5000 }, 80, 65536, "wall-time bound"),
        (Task::Flood { bytes: 1048576 }, 1500, 1024, "output bound"),
    ] {
        let h = Harness::new(task, "WP-022", wall, output);
        h.operation(Operation::Start).await;
        let report = h.finish().await;
        assert_eq!(report.reason, reason);
        assert!(report.cleanup_confirmed);
        assert!(report.retained_output_bytes <= output);
        assert!(h.state().capacity_reserved);
    }
}
#[tokio::test]
async fn wp_023_helper_and_fixture_exclude_deliberate_host_descriptors_and_environment() {
    use std::os::fd::AsRawFd;
    let secret = tempfile::tempfile().unwrap();
    let flags = unsafe { libc::fcntl(secret.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(secret.as_raw_fd(), libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
        0
    );
    let mut h = Harness::new(Task::Probe, "WP-023", 1500, 65536);
    h.exports = vec![worldinterface_runtime::worker_process::Export {
        reference: "artifact-probe".into(),
        name: "probe.json".into(),
        digest: worldinterface_protocol::ContentHash::of(br#"{"clean":true}"#),
        size: 14,
    }];
    h.operation(Operation::Start).await;
    let report = h.finish().await;
    assert!(report.helper_launch_clean && report.cleanup_confirmed);
    assert!(!h.state().quarantined);
    h.operation(Operation::CollectArtifacts).await;
    assert_eq!(
        h.runtime.read_artifact(&h.binding(), "artifact-probe").unwrap(),
        br#"{"clean":true}"#
    );
    // This asserts descriptor/environment exclusion, not same-user filesystem isolation.
}
#[tokio::test]
async fn wp_026_unavailable_operations_are_not_registered_or_decoded_as_available_effects() {
    for op in [Operation::Command, Operation::Checkpoint, Operation::Suspend, Operation::Resume] {
        assert!(worldinterface_runtime::worker::connector::descriptor(op).is_err());
    }
    let h = Harness::new(Task::Probe, "WP-026", 1500, 65536);
    let (submission, snapshot, config) = h.demand(Operation::Start);
    let evidence = Evidence {
        base: HostEvidence(snapshot.clone()),
        evaluation: h.evaluation.clone(),
        clock: HostClock,
    };
    let resolver = support::flow_fixture::Resolver(snapshot.facts[0].clone());
    let mut q = worldinterface_runtime::aq_adapter::QueueEngine::bootstrap(
        &h.bundle,
        support::spi_harness::Noop,
        support::spi_harness::host(),
    )
    .unwrap();
    let service = worldinterface_runtime::egress::EffectService {
        root: h.bundle.wi(),
        authentication: &h.auth,
        evidence: &evidence,
        resolver: &resolver,
        clock: &HostClock,
    };
    for op in [Operation::Command, Operation::Checkpoint, Operation::Suspend, Operation::Resume] {
        assert!(service
            .submit_direct(
                &(),
                submission.clone(),
                worldinterface_domain::flow::DirectOperationSelection {
                    connector: worldinterface_runtime::worker::connector::descriptor(
                        Operation::Start
                    )
                    .unwrap()
                    .connector,
                    operation: op.name().to_string().try_into().unwrap()
                },
                &config,
                &mut q
            )
            .is_err());
    }
    q.shutdown().unwrap();
    assert!(std::fs::read_dir(h.temp.path().join("process")).unwrap().next().is_none());
    assert!(h.bundle.wi().unfinished_workers(None, 10).unwrap().is_empty());
}
#[tokio::test]
async fn wp_024_worker_state_and_event_are_authoritative_before_aq_wake() {
    use worldinterface_domain::continuation::{EventKind, EventRoute};
    use worldinterface_worker::ports::{Observation, ObservationKind};
    let h = Harness::new(Task::Probe, "WP-024", 1500, 65536);
    h.operation(Operation::Start).await;
    h.finish().await;
    let b = h.binding();
    // Start's AQ ticks can already have delivered every process observation.
    // Commit a distinct late exit through the same observation boundary, with
    // no intervening queue tick, to exercise commit-before-wake deterministically.
    assert!(h.state().exited);
    let event = worldinterface_runtime::worker::observation::commit(
        h.bundle.wi(),
        &b,
        EventRoute {
            tenant: b.tenant.clone(),
            audience: b.principal.clone(),
            kind: EventKind::WorkerObservation,
            correlation: b.event_correlation().unwrap(),
            generation: None,
        },
        Observation {
            provider: b.provider.as_str().into(),
            run: b.run.as_str().into(),
            event: "wp-024-final-exit".into(),
            sequence: h.state().provider_sequence + 1,
            kind: ObservationKind::Exited,
        },
    )
    .unwrap();
    assert!(h.bundle.wi().signal_obligation(StoreKind::Fabric, &event).unwrap().sequence.is_none());
    let mut q = worldinterface_runtime::aq_adapter::QueueEngine::bootstrap(
        &h.bundle,
        support::spi_harness::Noop,
        support::spi_harness::host(),
    )
    .unwrap();
    let obligations = h.bundle.wi().pending_signals(StoreKind::Fabric, None, 100).unwrap();
    assert!(!obligations.is_empty());
    let before = h.state();
    q.drain_signals().unwrap();
    assert_eq!(h.state(), before);
    for o in obligations {
        let committed = h.bundle.wi().signal_obligation(StoreKind::Fabric, &o.event.id).unwrap();
        assert!(committed.sequence.is_some());
        assert_eq!(
            h.bundle.wi().authoritative_event(StoreKind::Fabric, &o.event.id).unwrap(),
            o.event
        );
    }
    assert_eq!(q.drain_signals().unwrap().acknowledged, 0);
    q.shutdown().unwrap();
    assert_eq!(h.binding(), b);
}
#[tokio::test]
async fn wp_025_reordered_duplicate_late_and_cancel_racing_events_cannot_revive_work() {
    use worldinterface_domain::continuation::{EventKind, EventRoute};
    use worldinterface_worker::ports::{Observation, ObservationKind};
    let h = Harness::new(Task::Probe, "WP-025", 1500, 65536);
    h.operation(Operation::Start).await;
    h.finish().await;
    let b = h.binding();
    let seq = h.state().provider_sequence;
    let route = EventRoute {
        tenant: b.tenant.clone(),
        audience: b.principal.clone(),
        kind: EventKind::WorkerObservation,
        correlation: b.event_correlation().unwrap(),
        generation: None,
    };
    let late = Observation {
        provider: b.provider.as_str().into(),
        run: b.run.as_str().into(),
        event: "late-progress".into(),
        sequence: seq + 2,
        kind: ObservationKind::Running,
    };
    let id = worldinterface_runtime::worker::observation::commit(
        h.bundle.wi(),
        &b,
        route.clone(),
        late.clone(),
    )
    .unwrap();
    assert_eq!(h.state().provider_sequence, seq);
    assert!(!h.state().running);
    let gap = Observation {
        provider: b.provider.as_str().into(),
        run: b.run.as_str().into(),
        event: "cancel-race".into(),
        sequence: seq + 1,
        kind: ObservationKind::Quarantined,
    };
    worldinterface_runtime::worker::observation::commit(h.bundle.wi(), &b, route.clone(), gap)
        .unwrap();
    assert_eq!(h.state().provider_sequence, seq + 2);
    assert!(!h.state().running);
    assert!(h.state().exited && h.state().stop_requested);
    assert_eq!(
        worldinterface_runtime::worker::observation::commit(
            h.bundle.wi(),
            &b,
            route.clone(),
            late.clone()
        )
        .unwrap(),
        id
    );
    let mut conflict = late.clone();
    conflict.kind = ObservationKind::Released;
    assert!(worldinterface_runtime::worker::observation::commit(
        h.bundle.wi(),
        &b,
        route.clone(),
        conflict
    )
    .is_err());
    let root =
        worldinterface_storage::StoreRoot::open_existing(h.temp.path().join("bundle/wi")).unwrap();
    assert_eq!(
        worldinterface_runtime::worker::observation::commit(&root, &b, route, late).unwrap(),
        id
    );
    assert!(!root.worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap().running);
}
#[tokio::test]
async fn wp_020_mismatched_and_damaged_private_ownership_never_adopts_an_unrelated_run() {
    let h = Harness::new(Task::Probe, "WP-020", 1500, 65536);
    h.operation(Operation::Start).await;
    h.finish().await;
    let other = Harness::new(Task::Sleep { millis: 5000 }, "WP-020", 3000, 65536);
    other.operation(Operation::Start).await;
    fn journal(h: &Harness) -> std::path::PathBuf {
        std::fs::read_dir(h.temp.path().join("process"))
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(".wi-owner-"))
            .unwrap()
            .path()
    }
    let path = journal(&h);
    let original = std::fs::read(&path).unwrap();
    let unrelated = std::fs::read(journal(&other)).unwrap();
    std::fs::write(&path, &unrelated).unwrap();
    // Per-run launch credentials and immutable realizations make otherwise well-formed evidence foreign.
    assert!(h.runtime.recovery_evidence(&h.binding()).is_err());
    assert!(h.state().quarantined && h.state().stop_requested && h.state().capacity_reserved);
    h.bundle.wi().repository(worldinterface_storage::StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert!(h.state().quarantined && h.state().stop_requested);
    assert!(other.runtime.cleanup_report(&other.binding()).unwrap().is_none());
    std::fs::write(&path, &original[..original.len() / 2]).unwrap();
    assert!(h.runtime.recovery_evidence(&h.binding()).is_err());
    std::fs::write(&path, &original).unwrap();
    assert!(h.runtime.recovery_evidence(&h.binding()).unwrap().unwrap().cleanup_confirmed);
    other.operation(Operation::Terminate).await;
    assert!(other.finish().await.cleanup_confirmed);
}
#[tokio::test]
async fn wp_019_helper_death_retains_capacity_without_a_cleanup_or_success_claim() {
    use std::{
        os::fd::{AsRawFd, FromRawFd},
        time::{Duration, Instant},
    };
    let h = Harness::new(Task::Sleep { millis: 5000 }, "WP-019", 3000, 65536);
    h.operation(Operation::Start).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !h.state().running {
        h.runtime.poll(&h.binding()).unwrap();
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let journal = std::fs::read_dir(h.temp.path().join("process"))
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with(".wi-owner-"))
        .unwrap()
        .path();
    let identity = std::fs::read_to_string(journal)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|v| v["entry"]["stage"] == "owned")
        .unwrap()["entry"]["identity"]
        .clone();
    let pid = identity["pid"].as_u64().unwrap() as u32;
    let birth = identity["birth"].as_u64().unwrap();
    let process_birth = || {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        stat.rsplit_once(')').unwrap().1.split_whitespace().nth(19).unwrap().parse::<u64>().unwrap()
    };
    assert_eq!(process_birth(), birth);
    assert_eq!(
        identity["boot"].as_str().unwrap(),
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap()
    );
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    assert!(fd >= 0);
    let handle = unsafe { std::fs::File::from_raw_fd(fd as i32) };
    assert_eq!(process_birth(), birth);
    assert_eq!(
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                handle.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        },
        0
    );
    let report = h.finish().await;
    assert!(!report.cleanup_confirmed);
    assert!(h.state().quarantined && h.state().capacity_reserved);
    assert!(!h.state().completed);
    // The reviewed fixture has its own finite task; do not remove its workspace until it can exit.
    tokio::time::sleep(Duration::from_secs(6)).await;
}
#[tokio::test]
async fn wp_025_real_worker_events_preserve_aq_deadline_cancel_and_restart_winners() {
    use worldinterface_domain::flow::{Deadline, PlanOperation, SignalProgress, TimeoutAction};
    use worldinterface_storage::flow::FlowOutcome;
    for order in ["signal-deadline", "deadline-signal", "cancel-signal"] {
        let h = Harness::new(Task::Sleep { millis: 5000 }, "WP-025", 1500, 65536);
        h.operation(Operation::Start).await;
        let mut f = support::flow_fixture::Fixture::new();
        f.bundle = h.bundle.clone();
        assert_eq!(f.auth.0, h.auth.0);
        assert_eq!(f.auth.1, h.auth.1);
        f.bundle.wi().publish(f.auth.0.clone(), b"input").unwrap();
        let evidence = support::connectors::Evidence(f.snapshot.clone());
        let service = f.service(&evidence);
        let deadline = (order != "cancel-signal").then(|| Deadline {
            inclusive_at: "2000-01-01T00:00:00.000000000Z".to_string().try_into().unwrap(),
            action: TimeoutAction::Fail,
        });
        let mut definition = support::continuation_fixture::definition(
            &f,
            order == "signal-deadline",
            SignalProgress::Once {},
            deadline,
        );
        let PlanOperation::Wait { signal } = &mut definition.body.nodes[0].operation else {
            unreachable!()
        };
        signal.correlation = h.binding().event_correlation().unwrap();
        service.activate_flow_version(&(), definition.clone(), None).unwrap();
        if order == "signal-deadline" {
            h.finish().await;
        }
        let mut q = worldinterface_runtime::aq_adapter::QueueEngine::bootstrap(
            &f.bundle,
            f.handler(),
            support::spi_harness::host(),
        )
        .unwrap();
        let id = service.submit_flow(&(), &definition.id, f.submission.clone(), &mut q).unwrap();
        if order == "cancel-signal" {
            support::continuation_fixture::until_wait(&f, &mut q, &id).await;
            q.cancel_task(f.run(&id).coordinator_task.as_str().parse().unwrap()).unwrap();
            h.finish().await;
        }
        f.drive(&mut q, &id).await;
        let expected = match order {
            "signal-deadline" => FlowOutcome::Succeeded,
            "deadline-signal" => FlowOutcome::Failed,
            _ => FlowOutcome::Canceled,
        };
        assert_eq!(f.run(&id).outcome, Some(expected));
        let settled = f.run(&id);
        h.finish().await;
        q.drain_signals().unwrap();
        assert_eq!(f.run(&id), settled);
        assert!(h.state().exited && !h.state().running && h.state().capacity_reserved);
        q.shutdown().unwrap();
        let reopened =
            worldinterface_runtime::storage::StoreBundle::open(&h.temp.path().join("bundle"))
                .unwrap();
        assert_eq!(reopened.wi().flow_run_snapshot(&f.auth.0, &id).unwrap().runs[&id], settled);
    }
}
