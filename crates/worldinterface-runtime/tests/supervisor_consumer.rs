//! Self-contained public server/handler consumer; also shipped in the archive.
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use worldinterface_protocol::{ContentHash, OpaqueRef};
use worldinterface_runtime::supervisor::*;
use worldinterface_worker::supervisor::*;
fn r(s: &str) -> OpaqueRef {
    s.to_owned().try_into().unwrap()
}
fn config() -> (Command, ChannelPolicy) {
    let request = LifecycleRequest {
        version: 1,
        audience: r("supervisor"),
        authority_generation: r("auth:1"),
        policy_revision: r("policy:1"),
        instance: r("instance"),
        generation: r("gen:1"),
        operation: Operation::Stop,
        target: ContentHash::of(b"target"),
        image: ContentHash::of(b"image"),
        deadline_ms: 15000,
        max_evidence_bytes: 1024,
    };
    let session = Session {
        tenant: r("tenant"),
        principal: r("operator"),
        audience: r("supervisor"),
        authority_generation: r("auth:1"),
        policy_revision: r("policy:1"),
    };
    (
        Command { version: 1, key: r("key"), request: request.clone() },
        ChannelPolicy {
            session: session.clone(),
            grants: vec![Grant { session, request }],
            revoked: Arc::new(AtomicBool::new(false)),
        },
    )
}
struct Consumer {
    calls: AtomicUsize,
    complete: AtomicBool,
}
impl SupervisorHandler for Consumer {
    fn supports_authoritative_inspect(&self) -> bool {
        true
    }
    fn dispatch(&self, _: AuthorizedSupervisorCommand) -> Result<Observation, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Observation { state: State::Running, evidence: vec![] })
    }
    fn inspect(&self, _: AuthorizedSupervisorInspect) -> Result<Observation, String> {
        Ok(Observation {
            state: if self.complete.load(Ordering::SeqCst) {
                State::Completed
            } else {
                State::Running
            },
            evidence: b"external result".to_vec(),
        })
    }
}
fn inspect(c: &Command) -> Request {
    Request::Inspect(Inspect { version: 1, key: c.key.clone(), command: c.digest().unwrap() })
}
fn terminal(
    server: &Server,
    p: &ChannelPolicy,
    c: &Command,
) -> worldinterface_worker::supervisor::Record {
    for _ in 0..300 {
        if let Ok(Response::Record(record)) = server.request(p, inspect(c), 1) {
            if record.observation.state.terminal() {
                return *record;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    panic!("supervisor result did not settle")
}
#[test]
fn public_server_durable_acceptance_replay_revocation_and_terminal_immutability() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("supervisor");
    let (command, policy) = config();
    let handler =
        Arc::new(Consumer { calls: AtomicUsize::new(0), complete: AtomicBool::new(false) });
    let server = Arc::new(Server::open(&root, handler.clone()).unwrap());
    let accepted = server.request(&policy, Request::Submit(command.clone()), 1).unwrap();
    assert!(matches!(accepted, Response::Record(ref r) if r.observation.state == State::Accepted));
    assert!(Server::open(&root, handler.clone()).is_err(), "durable dispatch owner fenced");
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                let _ = server.request(&policy, Request::Submit(command.clone()), 1);
            });
        }
    });
    let mut conflict = command.clone();
    conflict.request.image = ContentHash::of(b"other");
    assert!(server.request(&policy, Request::Submit(conflict), 1).is_err());
    handler.complete.store(true, Ordering::SeqCst);
    let result = terminal(&server, &policy, &command);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    handler.complete.store(false, Ordering::SeqCst);
    assert_eq!(
        server.request(&policy, inspect(&command), 99999).unwrap(),
        Response::Record(Box::new(result.clone()))
    );
    let mut wrong = policy.clone();
    wrong.session.tenant = r("other");
    assert!(server.request(&wrong, inspect(&command), 1).is_err());
    wrong = policy.clone();
    wrong.session.principal = r("worker-credential");
    assert!(server.request(&wrong, inspect(&command), 1).is_err());
    let mut stale = command.clone();
    stale.request.generation = r("stale");
    assert!(server.request(&policy, Request::Submit(stale), 1).is_err());
    policy.revoke();
    assert!(server.request(&policy, inspect(&command), 1).is_err());
    drop(server);
    let (_, policy) = config();
    let server = Server::open(&root, handler.clone()).unwrap();
    assert_eq!(
        server.request(&policy, inspect(&command), 99999).unwrap(),
        Response::Record(Box::new(result))
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    drop(server);
    let record = std::fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "json"))
        .unwrap();
    std::fs::write(record, b"corrupt").unwrap();
    assert!(Server::open(&root, handler).is_err());
}
#[test]
fn private_transport_rejects_malformed_and_oversized_frames_outside_guest() {
    use std::io::Write;
    let temp = tempfile::tempdir().unwrap();
    let (_, policy) = config();
    let server = Server::open(
        &temp.path().join("supervisor"),
        Arc::new(Consumer { calls: AtomicUsize::new(0), complete: AtomicBool::new(false) }),
    )
    .unwrap();
    let (client, mut channel) = PrivateChannel::provision(policy.clone()).unwrap();
    let mut stream = client.into_stream().unwrap();
    stream.write_all(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes()).unwrap();
    assert!(channel.serve_one(&server, 1).is_err());
    let (client, mut channel) = PrivateChannel::provision(policy).unwrap();
    let mut stream = client.into_stream().unwrap();
    let bytes = br#"{"kind":"submit","body":{"credentials":"forged"}}"#;
    stream.write_all(&(bytes.len() as u32).to_be_bytes()).unwrap();
    stream.write_all(bytes).unwrap();
    channel.serve_one(&server, 1).unwrap();
}

#[test]
fn connector_rejects_stale_authenticated_response_and_never_treats_acceptance_as_commit() {
    use worldinterface_domain::resource::{Target, TargetDocument};
    use worldinterface_protocol::{ConnectorExecution, PrincipalId};
    use worldinterface_runtime::connector::{
        Connector, ConnectorRegistry, ReconcileOutcome, ReconcileRequest,
    };
    let (mut command, policy) = config();
    let document = TargetDocument {
        version: 1,
        tenant: policy.session.tenant.clone(),
        owner: PrincipalId::generate(),
        target: Target::Lifecycle {
            instance: command.request.instance.clone(),
            target: command.request.target.clone(),
            operation: r("stop"),
        },
    };
    let resource = document.reference(command.request.generation.clone()).unwrap();
    let descriptor = descriptor(Operation::Stop).unwrap();
    let execution = ConnectorExecution {
        binding: ConnectorRegistry::new(vec![descriptor.clone()])
            .unwrap()
            .bindings()
            .unwrap()
            .remove(0),
        operation_identity: r("operation"),
        provider_key: Some(
            descriptor
                .provider_key
                .as_ref()
                .unwrap()
                .derive(&policy.session.tenant, &descriptor.operation, &r("operation"))
                .unwrap(),
        ),
    };
    command.key = r("different-command-key");
    let (client, channel) = PrivateChannel::provision(policy.clone()).unwrap();
    let (mut stream, _) = channel.into_parts();
    let stale = Response::Record(Box::new(worldinterface_worker::supervisor::Record {
        version: 1,
        command: command.clone(),
        tenant: policy.session.tenant.clone(),
        principal: policy.session.principal.clone(),
        observation: Observation { state: State::Completed, evidence: vec![] },
    }));
    let reply = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let mut size = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let mut bytes = vec![0; u32::from_be_bytes(size) as usize];
        stream.read_exact(&mut bytes).unwrap();
        let bytes = encode(&stale).unwrap();
        stream.write_all(&(bytes.len() as u32).to_be_bytes()).unwrap();
        stream.write_all(&bytes).unwrap();
    });
    let connector = SupervisorConnector::new(
        Arc::new(client),
        policy.session.clone(),
        command.request,
        document,
    )
    .unwrap();
    assert!(matches!(
        connector
            .reconcile(ReconcileRequest {
                tenant: policy.session.tenant,
                intent: worldinterface_protocol::EffectIntentId::generate(),
                execution,
                resource
            })
            .unwrap(),
        ReconcileOutcome::Escalate(_)
    ));
    reply.join().unwrap();
}

#[test]
fn supervisor_generation_deadline_and_unsupported_recovery_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let (command, policy) = config();
    let handler =
        Arc::new(Consumer { calls: AtomicUsize::new(0), complete: AtomicBool::new(false) });
    let server = Server::open(&temp.path().join("supervisor"), handler.clone()).unwrap();
    server.request(&policy, Request::Submit(command.clone()), 1).unwrap();
    let mut other = command.clone();
    other.key = r("different-key");
    assert!(server.request(&policy, Request::Submit(other), 1).is_err());
    // A finite deadline is enforced by the server even if backend work is pending.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if let Ok(Response::Record(record)) = server.request(&policy, inspect(&command), 15001) {
            if record.observation.state == State::Escalated {
                break;
            }
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    struct Unsupported;
    impl SupervisorHandler for Unsupported {
        fn supports_authoritative_inspect(&self) -> bool {
            false
        }
        fn dispatch(&self, _: AuthorizedSupervisorCommand) -> Result<Observation, String> {
            panic!()
        }
        fn inspect(&self, _: AuthorizedSupervisorInspect) -> Result<Observation, String> {
            panic!()
        }
    }
    assert!(Server::open(&temp.path().join("unsupported"), Arc::new(Unsupported)).is_err());
}

#[test]
fn interrupted_result_persistence_requires_reopen_and_authoritative_inspect() {
    use std::{
        path::PathBuf,
        time::{Duration, Instant},
    };
    struct Backend {
        root: PathBuf,
        ready: AtomicBool,
        release: AtomicBool,
    }
    impl SupervisorHandler for Backend {
        fn supports_authoritative_inspect(&self) -> bool {
            true
        }
        fn dispatch(&self, _: AuthorizedSupervisorCommand) -> Result<Observation, String> {
            std::fs::write(self.root.join("dispatched"), b"once").unwrap();
            Ok(Observation { state: State::Running, evidence: vec![] })
        }
        fn inspect(&self, _: AuthorizedSupervisorInspect) -> Result<Observation, String> {
            self.ready.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(Observation {
                state: State::Completed,
                evidence: b"retained backend result".to_vec(),
            })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("journal");
    let (command, policy) = config();
    let handler = Arc::new(Backend {
        root: temp.path().to_owned(),
        ready: AtomicBool::new(false),
        release: AtomicBool::new(false),
    });
    let server = Server::open(&root, handler.clone()).unwrap();
    server.request(&policy, Request::Submit(command.clone()), 1).unwrap();
    let limit = Instant::now() + Duration::from_secs(3);
    while !handler.ready.load(Ordering::SeqCst) {
        assert!(Instant::now() < limit);
        let _ = server.request(&policy, inspect(&command), 1);
        std::thread::sleep(Duration::from_millis(2));
    }
    // Simulate storage loss after the backend completed but before journal write.
    let saved = temp.path().join("saved");
    std::fs::rename(&root, &saved).unwrap();
    std::fs::write(&root, b"unavailable mount").unwrap();
    handler.release.store(true, Ordering::SeqCst);
    let limit = Instant::now() + Duration::from_secs(3);
    loop {
        if server
            .request(&policy, inspect(&command), 1)
            .err()
            .is_some_and(|e| e.to_string().contains("requires recovery"))
        {
            break;
        }
        assert!(Instant::now() < limit);
        std::thread::sleep(Duration::from_millis(2));
    }
    std::fs::remove_file(&root).unwrap();
    std::fs::rename(saved, &root).unwrap();
    drop(server);
    // A crash before atomic rename leaves private pending bytes, never authority.
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut partial = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(root.join(".pending-interrupted"))
        .unwrap();
    partial.write_all(b"incomplete result").unwrap();
    drop(partial);
    let server = Server::open(&root, handler).unwrap();
    assert_eq!(terminal(&server, &policy, &command).observation.state, State::Completed);
    assert_eq!(std::fs::read(temp.path().join("dispatched")).unwrap(), b"once");
    assert!(!root.join(".pending-interrupted").exists());
}

#[test]
fn authenticated_inspect_recovers_retained_backend_completion_after_dispatch_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("supervisor");
    let (command, policy) = config();
    let handler =
        Arc::new(Consumer { calls: AtomicUsize::new(0), complete: AtomicBool::new(false) });
    let server = Server::open(&root, handler.clone()).unwrap();
    server.request(&policy, Request::Submit(command.clone()), 1).unwrap();
    drop(server);
    // Reopening waits for the original dispatch owner to durably finish its call.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let server = loop {
        if let Ok(server) = Server::open(&root, handler.clone()) {
            break server;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(2));
    };
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    handler.complete.store(true, Ordering::SeqCst);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        if let Ok(Response::Record(record)) = server.request(&policy, inspect(&command), 20000) {
            if record.observation.state.terminal() {
                assert_eq!(record.observation.state, State::Completed);
                break;
            }
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}
