mod support {
    pub mod child;
    pub mod connectors;
    pub mod supervisor_fixture;
    pub mod supervisor_harness;
}
use serde::{Deserialize, Serialize};
use std::{
    os::{fd::FromRawFd, unix::net::UnixStream},
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
    let stream = unsafe { UnixStream::from_raw_fd(198) };
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
    let h = if stage == "invoke" {
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
        Harness {
            bundle: Arc::new(StoreBundle::open(&root.join("bundle")).unwrap()),
            connector,
            snapshot: metadata.snapshot,
            id: metadata.id,
            run: metadata.run.parse().unwrap(),
            clock: Clock::new(),
        }
    };
    if stage == "invoke" {
        runtime.block_on(h.execute());
        assert_eq!(h.effect().phase, Phase::Uncertain);
        assert!(h.effect().outcomes.iter().all(|o| o.outcome != ExecutionOutcome::Succeeded));
    } else {
        let original = h.bundle.wi().effect_receipts(h.caller().tenant(), &h.id).unwrap();
        runtime.block_on(h.reconcile());
        assert_eq!(h.effect().phase, Phase::Reconciled);
        assert_eq!(
            h.case().status,
            worldinterface_domain::reconciliation::CaseStatus::ConfirmedCommitted
        );
        assert_eq!(h.bundle.wi().effect_receipts(h.caller().tenant(), &h.id).unwrap(), original);
        let case = h.case();
        runtime.block_on(h.reconcile());
        assert_eq!(h.case(), case);
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
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "WI stage {stage}: {status}");
            return;
        }
        assert!(std::time::Instant::now() < deadline, "WI fixture exceeded wall bound");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
fn lifecycle(operation: Operation, lost_ack: bool, ambiguous: bool, failed_guest_first: bool) {
    live_packet();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let config = prepare(root, operation);
    let mut guest = KillOnDrop(
        Process::new("/bin/sleep")
            .arg("15")
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    // No supervisor descriptors or host credentials are inherited by the target.
    let env = std::fs::read(format!("/proc/{}/environ", guest.id())).unwrap();
    assert!(env.is_empty());
    let descriptors: Vec<_> =
        std::fs::read_dir(format!("/proc/{}/fd", guest.id())).unwrap().collect();
    assert!(descriptors.len() <= 3);
    if failed_guest_first {
        guest.kill().unwrap();
        guest.wait().unwrap();
    }
    if lost_ack {
        std::fs::write(root.join("drop-ack"), b"1").unwrap();
    }
    if ambiguous {
        std::fs::write(root.join("interrupt-dispatch"), b"1").unwrap();
    }
    let (client, supervisor) = channel(root, &config);
    wi(root, client, "invoke");
    wait(&root.join("backend-command"));
    assert_eq!(std::fs::read(root.join("activations")).unwrap(), b"1");
    if !failed_guest_first {
        guest.kill().unwrap();
        guest.wait().unwrap();
    }
    drop(supervisor);
    let _ = std::fs::remove_file(root.join("drop-ack"));
    let _ = std::fs::remove_file(root.join("interrupt-dispatch"));
    // Completion belongs to the operator backend, and needs no guest callback.
    std::fs::write(root.join("complete"), b"stopped/recovered outside guest").unwrap();
    let (client, supervisor) = channel(root, &config);
    wi(root, client, "reconcile");
    assert_eq!(std::fs::read(root.join("activations")).unwrap(), b"1");
    drop(supervisor);
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
    let verified = || {
        worldinterface_domain::authorization::VerifiedSnapshot::from_trusted_verifier(
            snapshot.clone(),
        )
        .unwrap()
    };
    let id = worldinterface_egress::Egress::new(store)
        .apply(
            &caller,
            Timestamp::from_epoch_millis(now()).unwrap(),
            worldinterface_domain::effect::EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot: verified(),
                link: None,
            },
        )
        .unwrap();
    if let Target::Lifecycle { operation, .. } = &mut snapshot.facts[0].document.target {
        *operation = r("recover");
    }
    snapshot.facts[0].attested_document = digest(&snapshot.facts[0].document).unwrap();
    let error = worldinterface_egress::Egress::new(store)
        .apply(
            &caller,
            Timestamp::from_epoch_millis(now()).unwrap(),
            worldinterface_domain::effect::EffectAction::Resolve {
                id,
                snapshot:
                    worldinterface_domain::authorization::VerifiedSnapshot::from_trusted_verifier(
                        snapshot,
                    )
                    .unwrap(),
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("lifecycle target operation mismatch"), "{error}");
}
#[test]
fn wp_036_acceptance_is_uncertain_until_external_transition_completes() {
    lifecycle(Operation::Stop, false, false, false);
}
#[test]
fn wp_037_external_result_survives_stopped_guest_and_wi_restart() {
    lifecycle(Operation::Replace, false, false, false);
}
#[test]
fn wp_038_lost_ack_and_callback_converge_through_durable_inspect() {
    lifecycle(Operation::Replace, true, true, false);
}
#[test]
fn wp_039_operator_recovery_is_independent_of_failed_guest() {
    lifecycle(Operation::Recover, false, false, true);
}
