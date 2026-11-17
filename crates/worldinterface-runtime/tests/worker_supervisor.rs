mod support {
    pub mod child;
    pub mod connectors;
    pub mod spi_harness;
    pub mod supervisor_fixture;
    pub mod supervisor_harness;
}
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    os::{
        fd::{FromRawFd, OwnedFd},
        unix::{net::UnixStream, process::ExitStatusExt},
    },
    path::{Path, PathBuf},
    process::{Command as Process, Stdio},
    sync::Arc,
};
use support::{
    child::KillOnDrop, connectors::*, supervisor_fixture::*, supervisor_harness::Harness,
};
use worldinterface_domain::{
    authorization::AuthorizationSnapshot, ingress::digest, resource::Target, Phase,
};
use worldinterface_protocol::*;
use worldinterface_runtime::{storage::StoreBundle, supervisor::*};
use worldinterface_worker::supervisor::{self as protocol, *};

#[test]
fn supervisor_child() {
    if let Some(root) = std::env::var_os("WI_SUPERVISOR_ROOT") {
        server_child(Path::new(&root));
    }
}
#[derive(Serialize, Deserialize)]
struct Metadata {
    snapshot: AuthorizationSnapshot,
    id: EffectIntentId,
    run: String,
}
#[test]
fn wi_supervisor_child() {
    let Some(root) = std::env::var_os("WI_SUPERVISOR_WI") else { return };
    let root = PathBuf::from(root);
    let config: Config =
        serde_json::from_slice(&std::fs::read(root.join("config.json")).unwrap()).unwrap();
    let stream = unsafe { UnixStream::from_raw_fd(SUPERVISOR_FD) };
    let snapshot: AuthorizationSnapshot =
        serde_json::from_slice(&std::fs::read(root.join("snapshot.json")).unwrap()).unwrap();
    let connector = Arc::new(
        SupervisorConnector::new(
            Arc::new(Client::from_provisioned(stream)),
            config.policy().session,
            config.request,
            snapshot.facts[0].document.clone(),
        )
        .unwrap(),
    );
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let stage = std::env::var("WI_SUPERVISOR_STAGE").unwrap();
    let invoke = stage.starts_with("invoke");
    let h = if invoke {
        let submission =
            serde_json::from_slice(&std::fs::read(root.join("submission.json")).unwrap()).unwrap();
        let h = Harness::new(&root, connector, submission, snapshot);
        std::fs::write(
            root.join("metadata.json"),
            serde_json::to_vec(&Metadata {
                snapshot: h.snapshot.clone(),
                id: h.id.clone(),
                run: h.run.to_string(),
            })
            .unwrap(),
        )
        .unwrap();
        h
    } else {
        let metadata: Metadata =
            serde_json::from_slice(&std::fs::read(root.join("metadata.json")).unwrap()).unwrap();
        let bundle = Arc::new(StoreBundle::open(&root.join("bundle")).unwrap());
        // Restore fixture time from durable WI history rather than counting
        // process startup/storage latency against the command's virtual budget.
        let at = bundle
            .wi()
            .effect_snapshot(&metadata.snapshot.bindings[0].tenant)
            .unwrap()
            .last_at
            .unwrap();
        let clock = Clock(Arc::new(std::sync::atomic::AtomicU64::new(at.epoch_millis().unwrap())));
        Harness {
            bundle,
            connector,
            snapshot: metadata.snapshot,
            id: metadata.id,
            run: metadata.run.parse().unwrap(),
            clock,
        }
    };
    if let Some(point) =
        stage.strip_prefix("invoke-cut-").or_else(|| stage.strip_prefix("reconcile-cut-"))
    {
        std::env::set_var("WI_CRASH_POINT", point);
        std::env::set_var("WI_CRASH_READY", root.join("wi-crash-ready"));
    }
    if invoke {
        runtime.block_on(h.execute());
        assert_eq!(h.effect().phase, Phase::Uncertain);
        assert!(h.effect().outcomes.iter().all(|o| o.outcome != ExecutionOutcome::Succeeded));
    } else {
        runtime.block_on(h.execute());
        let original = h.bundle.wi().effect_receipts(h.caller().tenant(), &h.id).unwrap();
        runtime.block_on(h.reconcile());
        assert_eq!(
            h.effect().phase,
            Phase::Reconciled,
            "case status {:?}, observations {}, evidence count {}",
            h.case().status,
            h.case().observations,
            h.case().evidence.len()
        );
        assert_eq!(
            h.case().status,
            worldinterface_domain::reconciliation::CaseStatus::ConfirmedCommitted
        );
        assert_eq!(h.bundle.wi().effect_receipts(h.caller().tenant(), &h.id).unwrap(), original);
        let case = h.case();
        runtime.block_on(h.reconcile());
        assert_eq!(h.case(), case);
        let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
            h.caller().tenant().clone(),
            h.caller().principal().clone(),
            worldinterface_domain::receipt::DisclosureProfile::Full,
        );
        let envelopes: Vec<_> = original
            .iter()
            .map(|(id, _, _)| {
                h.bundle.wi().inspect_receipt(&access, id).unwrap().unwrap().canonical.unwrap()
            })
            .collect();
        durable_write(&root.join("wi-evidence.json"), &serde_json::to_vec(&serde_json::json!({
            "intent":h.effect().intent,"original_receipts":original,"receipt_envelopes":envelopes,
            "reconciled_receipts":h.bundle.wi().effect_receipts(h.caller().tenant(), &h.id).unwrap(),
            "case":case,"case_after_retry":h.case(),"phase":h.effect().phase
        })).unwrap());
    }
}
fn prepare(root: &Path, operation: Operation) -> Config {
    let d = descriptor(operation).unwrap();
    let (mut submission, mut s) = fixture(&d);
    let request = LifecycleRequest {
        version: 1,
        audience: r("operator:supervisor"),
        authority_generation: r("authority:1"),
        policy_revision: r("policy:supervisor:1"),
        instance: r("instance:fixture"),
        generation: r("generation:1"),
        operation,
        target: ContentHash::of(b"approved-target"),
        image: ContentHash::of(b"approved-image"),
        deadline_ms: now() + 15000,
        max_evidence_bytes: 1024,
    };
    let bytes = protocol::encode(&request).unwrap();
    submission.effect_class = EffectClass::Lifecycle;
    // The generic egress example allows only a one-second AQ attempt, which
    // can cancel during durable WI preparation before Submit is written. Use
    // the supervisor's existing host-enforced wall budget; transport frames
    // still have their independent one-second bound and WI children twelve.
    submission.budget.timeout_millis = MAX_WALL_MS;
    submission.resource.namespace = r("lifecycle");
    submission.deadline = Some(Timestamp::from_epoch_millis(request.deadline_ms).unwrap());
    submission.input.artifact.content = ContentHash::of(&bytes);
    submission.input.artifact.size = bytes.len() as u64;
    s.facts[0].selector = submission.resource.clone();
    s.facts[0].document.target = Target::Lifecycle {
        instance: request.instance.clone(),
        target: request.target.clone(),
        operation: r(operation.name()),
    };
    s.facts[0].observed_version = request.generation.clone();
    s.facts[0].attested_document = digest(&s.facts[0].document).unwrap();
    s.policy.targets = [s.facts[0].attested_document.clone()].into();
    s.required_outputs.clear();
    let demand = digest(&submission).unwrap();
    for b in &mut s.bindings {
        b.principal_authority = [demand.clone()].into();
        b.capacity_authority = [demand.clone()].into();
    }
    s.policy.platform = [demand.clone()].into();
    s.policy.resource = [demand.clone()].into();
    s.policy.information_use = [demand.clone()].into();
    for g in &mut s.grants {
        g.authority = [demand.clone()].into();
    }
    let config = Config {
        request,
        tenant: s.bindings[0].tenant.clone(),
        principal: r(s.bindings[0].principal.as_str()),
    };
    for (name, bytes) in [
        ("config.json", serde_json::to_vec(&config).unwrap()),
        ("submission.json", serde_json::to_vec(&submission).unwrap()),
        ("snapshot.json", serde_json::to_vec(&s).unwrap()),
        ("request.json", bytes),
    ] {
        std::fs::write(root.join(name), bytes).unwrap();
    }
    config
}
fn wi(root: &Path, client: Arc<Client>, stage: &str) {
    let stream = Arc::try_unwrap(client).ok().unwrap().into_stream().unwrap();
    let mut command = Process::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "wi_supervisor_child", "--nocapture"])
        .env_clear()
        .env("WI_SUPERVISOR_WI", root)
        .env("WI_SUPERVISOR_STAGE", stage)
        .env("TMPDIR", std::env::var_os("TMPDIR").unwrap())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    transfer(&mut command, &stream);
    let mut child = KillOnDrop(command.spawn().unwrap());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(12);
    loop {
        if stage.contains("-cut-") && root.join("wi-crash-ready").exists() {
            child.kill().unwrap();
            child.wait().unwrap();
            std::fs::remove_file(root.join("wi-crash-ready")).unwrap();
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "WI stage {stage}: {status}");
            return;
        }
        assert!(std::time::Instant::now() < deadline, "WI fixture exceeded wall bound");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
fn assert_guest_isolated(pid: u32, readiness: &mut UnixStream) {
    // /bin/cat must echo from guest code after exec before procfs is sampled.
    // spawn() alone can expose the parent's environment, transient EACCES, or
    // an empty environment before the guest's environment has been installed.
    readiness.set_read_timeout(Some(std::time::Duration::from_secs(1))).unwrap();
    readiness.set_write_timeout(Some(std::time::Duration::from_secs(1))).unwrap();
    let expected = b"guest ready\n";
    readiness.write_all(expected).unwrap();
    let mut echoed = [0; 12];
    readiness.read_exact(&mut echoed).unwrap();
    assert_eq!(&echoed, expected);

    // No supervisor descriptors or host credentials are inherited by the target.
    let env = std::fs::read(format!("/proc/{pid}/environ")).unwrap();
    assert!(env.is_empty(), "guest inherited environment after readiness");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    loop {
        let descriptors: Vec<_> = std::fs::read_dir(format!("/proc/{pid}/fd"))
            .unwrap()
            .map(|e| e.unwrap().path().read_link())
            .collect();
        if descriptors.len() <= 3 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "unexpected guest descriptors: {descriptors:?}"
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

#[derive(Default)]
struct Scenario<'a> {
    lost_ack: bool,
    ambiguous: bool,
    failed_guest_first: bool,
    cut: Option<&'a str>,
}
fn lifecycle(case: &str, operation: Operation, scenario: Scenario<'_>) {
    let Scenario { lost_ack, ambiguous, failed_guest_first, cut } = scenario;
    live_packet();
    let temp = tempfile::tempdir().unwrap();
    let wi_root = temp.path();
    let config = prepare(wi_root, operation);
    let scenario = cut.unwrap_or("restart");
    let operator_root = retained_operator_root(case, scenario);
    let root = operator_root.as_path();
    assert!(!root.starts_with(wi_root) && !wi_root.starts_with(root));
    durable_write(&root.join("config.json"), &std::fs::read(wi_root.join("config.json")).unwrap());
    let (mut readiness, stdio) = UnixStream::pair().unwrap();
    let mut guest = KillOnDrop(
        Process::new("/bin/cat")
            // Process ASCII readiness through stdio without cat's splice pipes.
            .arg("-v")
            .env_clear()
            .stdin(Stdio::from(OwnedFd::from(stdio.try_clone().unwrap())))
            .stdout(Stdio::from(OwnedFd::from(stdio)))
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    assert_guest_isolated(guest.id(), &mut readiness);
    let mut guest_exit = None;
    if failed_guest_first {
        guest.kill().unwrap();
        guest_exit = Some(guest.wait().unwrap());
    }
    if lost_ack {
        std::fs::write(root.join("drop-ack"), b"1").unwrap();
        std::fs::write(root.join("delay-inspect"), b"1").unwrap();
    }
    if ambiguous {
        std::fs::write(root.join("interrupt-dispatch"), b"1").unwrap();
        std::fs::write(root.join("delay-abort"), b"1").unwrap();
    }
    let (client, mut supervisor) = channel(root, &config);
    let initial_supervisor = supervisor.id();
    let initial = cut
        .filter(|p| p.starts_with("fabric-"))
        .map(|p| format!("invoke-cut-{p}"))
        .unwrap_or_else(|| "invoke".into());
    wi(wi_root, client, &initial);
    wait_for_activation(root);
    assert_eq!(std::fs::read(root.join("activations")).unwrap(), b"1");
    if ambiguous {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let status = loop {
            if let Some(status) = supervisor.try_wait().unwrap() {
                break status;
            }
            assert!(std::time::Instant::now() < deadline, "supervisor did not abort");
            std::thread::sleep(std::time::Duration::from_millis(5));
        };
        assert_eq!(status.signal(), Some(libc::SIGABRT), "real abnormal death required");
        assert!(!status.core_dumped(), "intentional crash produced a core dump");
        assert_eq!(
            std::fs::read(root.join("abort-no-core")).unwrap(),
            b"dumpable=0\nrlimit_core=0:0\n"
        );
    }
    if !failed_guest_first {
        guest.kill().unwrap();
        guest_exit = Some(guest.wait().unwrap());
    }
    let guest_stopped = guest_exit.is_some_and(|status| status.signal() == Some(libc::SIGKILL));
    assert!(guest_stopped, "guest death was not reaped");
    drop(supervisor);
    let _ = std::fs::remove_file(root.join("drop-ack"));
    let _ = std::fs::remove_file(root.join("interrupt-dispatch"));
    // Completion belongs to the operator backend, and needs no guest callback.
    complete_backend(root);
    let (client, supervisor) = channel(root, &config);
    if let Some(point) = cut.filter(|p| p.starts_with("reconciliation-")) {
        wi(wi_root, client, &format!("reconcile-cut-{point}"));
        drop(supervisor);
        let (client, supervisor) = channel(root, &config);
        wi(wi_root, client, "reconcile");
        drop(supervisor);
    } else {
        wi(wi_root, client, "reconcile");
        drop(supervisor);
    }
    assert_eq!(std::fs::read(root.join("activations")).unwrap(), b"1");
    // Read WI evidence only after the WI process has exited. Then destroy its
    // temporary state before one final authenticated inspect of retained results.
    let wi_evidence: serde_json::Value =
        serde_json::from_slice(&std::fs::read(wi_root.join("wi-evidence.json")).unwrap()).unwrap();
    let wi_path = wi_root.to_path_buf();
    drop(temp);
    let wi_state_removed = !wi_path.exists();
    assert!(wi_state_removed, "temporary WI state survived removal");
    let backend: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("backend-record.json")).unwrap()).unwrap();
    let command: protocol::Command = serde_json::from_value(backend["command"].clone()).unwrap();
    let (client, supervisor) = channel(root, &config);
    let supervisor_restarted = supervisor.id() != initial_supervisor;
    assert!(supervisor_restarted, "inspect reused the original supervisor process");
    let Response::Record(inspected) = client
        .request(&Request::Inspect(Inspect {
            version: 1,
            key: command.key.clone(),
            command: command.digest().unwrap(),
        }))
        .unwrap()
    else {
        panic!("authenticated inspect unavailable")
    };
    assert_eq!(inspected.command, command);
    assert_eq!(inspected.observation.state, State::Completed);
    drop(client);
    drop(supervisor);
    let base = root.parent().unwrap();
    let mut records = std::collections::BTreeMap::new();
    let mut names = vec![
        root.join("backend-command"),
        root.join("config.json"),
        root.join("backend-record.json"),
        root.join("backend-result.json"),
        root.join("activations"),
    ];
    names.extend(
        std::fs::read_dir(root.join("journal"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "json")),
    );
    assert_eq!(names.len(), 6);
    for path in names {
        records.insert(
            path.strip_prefix(base).unwrap().to_str().unwrap().to_owned(),
            ContentHash::of(&std::fs::read(path).unwrap()),
        );
    }
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let packet: serde_json::Value = serde_json::from_slice(
        &std::fs::read(source.join("conformance/wi-fabric-2/worker/evaluation-manifest.json"))
            .unwrap(),
    )
    .unwrap();
    let observation = serde_json::json!({"version":1,"kind":"wi-supervisor-observation-v1", "case":case,"scenario":scenario,
        "command":command,"command_digest":command.digest().unwrap(),
        "authorization":{"tenant":config.tenant,"principal":config.principal,"audience":config.request.audience,"authority_generation":config.request.authority_generation,"policy_revision":config.request.policy_revision},
        "backend_command":backend,"backend_result":serde_json::from_slice::<serde_json::Value>(&std::fs::read(root.join("backend-result.json")).unwrap()).unwrap(),
        "inspect_record":inspected,"wi":wi_evidence,"activation_count":1,
        "guest_stopped":guest_stopped,"wi_state_removed":wi_state_removed,"supervisor_restarted":supervisor_restarted,"records":records,
        "build":{"packet":ContentHash::of(&canonical_collection(&packet).unwrap()),
            "profile_definition":ContentHash::of(&std::fs::read(source.join("contracts/sources/wi-fabric-2-worker-execution-profile.yaml")).unwrap()),
            "fixture_source":ContentHash::of(&std::fs::read(source.join("crates/worldinterface-runtime/tests/support/supervisor_fixture.rs")).unwrap())}
    });
    let bytes = serde_json::to_vec(&observation).unwrap();
    assert!(bytes.len() <= 65536, "supervisor observation bound");
    let name = format!("supervisor-{case}-{scenario}.json");
    durable_write(&base.join(&name), &bytes);
    eprintln!("WI-SUPERVISOR-OBSERVATION {name} {}", ContentHash::of(&bytes).as_str());
}
#[test]
fn wp_035_authenticated_supervisor_binds_exact_lifecycle_operation() {
    let temp = tempfile::tempdir().unwrap();
    prepare(temp.path(), Operation::Stop);
    let mut snapshot: AuthorizationSnapshot =
        serde_json::from_slice(&std::fs::read(temp.path().join("snapshot.json")).unwrap()).unwrap();
    let submission =
        serde_json::from_slice(&std::fs::read(temp.path().join("submission.json")).unwrap())
            .unwrap();
    let caller = worldinterface_domain::agency::AuthenticatedCaller::from_trusted_authentication(
        snapshot.bindings[0].tenant.clone(),
        snapshot.bindings[0].principal.clone(),
    );
    let bundle = StoreBundle::initialize(
        &temp.path().join("bundle"),
        worldinterface_storage::StoreProfile {
            instance: InstanceId::generate(),
            created_at: Timestamp::from_epoch_millis(now()).unwrap(),
            platform: false,
        },
    )
    .unwrap();
    let store = bundle.wi();
    store
        .publish(caller.tenant().clone(), &std::fs::read(temp.path().join("request.json")).unwrap())
        .unwrap();
    let id = support::spi_harness::submit(store, &Clock::new(), submission, &snapshot);
    if let Target::Lifecycle { operation, .. } = &mut snapshot.facts[0].document.target {
        *operation = r("recover");
    }
    snapshot.facts[0].attested_document = digest(&snapshot.facts[0].document).unwrap();
    let intent = store.effect_snapshot(caller.tenant()).unwrap().effects[&id].intent.clone();
    let error = worldinterface_domain::resource::resolve_resource(
        &intent,
        caller.tenant(),
        &snapshot.registry,
        &snapshot.facts,
        &Timestamp::from_epoch_millis(now()).unwrap(),
    )
    .unwrap_err();
    assert!(error.contains("lifecycle target operation mismatch"), "{error}");
    drop(bundle);
    lifecycle("WP-035", Operation::Stop, Scenario::default());
}
#[test]
fn wp_036_acceptance_is_uncertain_until_external_transition_completes() {
    lifecycle("WP-036", Operation::Stop, Scenario::default());
}
#[test]
fn wp_037_external_result_survives_stopped_guest_and_wi_restart() {
    lifecycle("WP-037", Operation::Replace, Scenario::default());
}
#[test]
fn wp_038_lost_ack_and_callback_converge_through_durable_inspect() {
    for cut in [
        "fabric-receipt-history",
        "fabric-receipt-committed",
        "reconciliation-transition-committed",
    ] {
        lifecycle(
            "WP-038",
            Operation::Replace,
            Scenario { lost_ack: true, ambiguous: true, cut: Some(cut), ..Scenario::default() },
        );
    }
}
#[test]
fn wp_039_operator_recovery_is_independent_of_failed_guest() {
    lifecycle(
        "WP-039",
        Operation::Recover,
        Scenario { failed_guest_first: true, ..Scenario::default() },
    );
}
