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
impl Consumer {
    fn new() -> Self {
        Self { calls: AtomicUsize::new(0), complete: AtomicBool::new(false) }
    }
}
fn reopen(root: &std::path::Path, handler: Arc<dyn SupervisorHandler>) -> Server {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok(server) = Server::open(root, handler.clone()) {
            return server;
        }
        assert!(std::time::Instant::now() < deadline, "supervisor owner did not release");
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
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
    now_ms: u64,
) -> worldinterface_worker::supervisor::Record {
    for _ in 0..300 {
        if let Ok(Response::Record(record)) = server.request(p, inspect(c), now_ms) {
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
    let handler = Arc::new(Consumer::new());
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
    let result = terminal(&server, &policy, &command, 1);
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
    let server = Server::open(&temp.path().join("supervisor"), Arc::new(Consumer::new())).unwrap();
    let (client, mut channel) = PrivateChannel::provision(policy.clone()).unwrap();
    let mut stream = client.into_stream().unwrap();
    stream.write_all(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes()).unwrap();
    assert!(channel.serve_one(&server, || 1).is_err());
    let (client, mut channel) = PrivateChannel::provision(policy).unwrap();
    let mut stream = client.into_stream().unwrap();
    let bytes = br#"{"kind":"submit","body":{"credentials":"forged"}}"#;
    stream.write_all(&(bytes.len() as u32).to_be_bytes()).unwrap();
    stream.write_all(bytes).unwrap();
    channel.serve_one(&server, || 1).unwrap();
}

#[test]
fn private_transport_samples_authorization_clock_after_receiving_submit() {
    use std::{sync::atomic::AtomicU64, time::Duration};
    for expired in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let (mut command, mut policy) = config();
        if !expired {
            command.request.deadline_ms = 35000;
            policy.grants[0].request = command.request.clone();
        }
        let handler = Arc::new(Consumer::new());
        let server = Server::open(&temp.path().join("supervisor"), handler.clone()).unwrap();
        let (client, mut channel) = PrivateChannel::provision(policy).unwrap();
        let clock = Arc::new(AtomicU64::new(1));
        let now = clock.clone();
        let (sampled, samples) = std::sync::mpsc::channel();
        let (idle, polls) = std::sync::mpsc::channel();
        let serving = std::thread::spawn(move || {
            let served = AtomicBool::new(false);
            // Empty idle polls are legal, including when the client thread is
            // delayed by host load. Stop only after a frame samples the clock.
            while !served.load(Ordering::SeqCst) {
                channel
                    .serve_one(&server, || {
                        served.store(true, Ordering::SeqCst);
                        sampled.send(()).unwrap();
                        now.load(Ordering::SeqCst)
                    })
                    .unwrap();
                if !served.load(Ordering::SeqCst) {
                    idle.send(()).unwrap();
                }
            }
        });
        // The server is idle. A clock sample here would predate the request.
        assert!(matches!(
            samples.recv_timeout(Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        // Exercise a completed idle poll deterministically before submitting.
        polls.recv_timeout(Duration::from_secs(5)).unwrap();
        clock.store(20000, Ordering::SeqCst);
        let response = client.request(&Request::Submit(command)).unwrap();
        if expired {
            assert_eq!(response, Response::Unavailable);
            assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
        } else {
            assert!(matches!(response, Response::Record(ref record)
                if record.observation.state == State::Accepted));
        }
        samples.recv_timeout(Duration::from_secs(3)).unwrap();
        serving.join().unwrap();
    }
}

#[test]
fn private_transport_gives_a_late_submit_its_own_frame_budget() {
    use std::{
        io::{Read, Write},
        time::Duration,
    };
    for idle_ms in [100, 990] {
        let temp = tempfile::tempdir().unwrap();
        let (command, policy) = config();
        let server =
            Server::open(&temp.path().join("supervisor"), Arc::new(Consumer::new())).unwrap();
        let (client, mut channel) = PrivateChannel::provision(policy).unwrap();
        let mut stream = client.into_stream().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let request = encode(&Request::Submit(command.clone())).unwrap();
        let header = (request.len() as u32).to_be_bytes();
        let (ready, waiting) = std::sync::mpsc::channel();
        let serve = std::thread::spawn(move || {
            ready.send(()).unwrap();
            // A delayed test thread may miss an idle poll entirely. Retrying
            // an empty poll is legal; consuming a partial frame is not.
            loop {
                channel.serve_one(&server, || 1).unwrap();
                if matches!(
                    server.request(&config().1, inspect(&command), 1),
                    Ok(Response::Record(_))
                ) {
                    return;
                }
            }
        });
        waiting.recv().unwrap();
        std::thread::sleep(Duration::from_millis(idle_ms));
        stream.write_all(&header[..1]).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        stream.write_all(&header[1..]).unwrap();
        stream.write_all(&request).unwrap();
        let mut size = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let len = u32::from_be_bytes(size) as usize;
        assert!(len <= MAX_FRAME_BYTES);
        let mut response = vec![0; len];
        stream.read_exact(&mut response).unwrap();
        assert!(matches!(decode::<Response>(&response).unwrap(), Response::Record(ref r)
            if r.observation.state == State::Accepted));
        serve.join().unwrap();
    }
}

#[test]
fn private_transport_partial_progress_does_not_renew_frame_deadline() {
    use std::{
        io::{Read, Write},
        time::{Duration, Instant},
    };
    let temp = tempfile::tempdir().unwrap();
    let (_, policy) = config();
    let server = Server::open(&temp.path().join("supervisor"), Arc::new(Consumer::new())).unwrap();
    let (client, mut channel) = PrivateChannel::provision(policy).unwrap();
    let mut stream = client.into_stream().unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    // Send a partial header, then make progress without completing the frame.
    stream.write_all(&[0]).unwrap();
    let serve = std::thread::spawn(move || {
        let started = Instant::now();
        assert!(channel.serve_one(&server, || 1).is_err());
        assert!(started.elapsed() < Duration::from_secs(3));
        // Keep the channel object alive to prove shutdown, rather than drop,
        // closes the poisoned endpoint and prevents interpreting a suffix.
        assert!(channel.serve_one(&server, || 1).is_err());
        channel
    });
    std::thread::sleep(Duration::from_millis(600));
    stream.write_all(&[0]).unwrap();
    assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    let _channel = serve.join().unwrap();
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
    let handler = Arc::new(Consumer::new());
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
    assert_eq!(terminal(&server, &policy, &command, 1).observation.state, State::Completed);
    assert_eq!(std::fs::read(temp.path().join("dispatched")).unwrap(), b"once");
    assert!(!root.join(".pending-interrupted").exists());
}

#[test]
fn authenticated_inspect_recovers_retained_backend_completion_after_dispatch_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("supervisor");
    let (command, policy) = config();
    let handler = Arc::new(Consumer::new());
    let server = Server::open(&root, handler.clone()).unwrap();
    server.request(&policy, Request::Submit(command.clone()), 1).unwrap();
    drop(server);
    // Reopening waits for the original dispatch owner to durably finish its call.
    let server = reopen(&root, handler.clone());
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    handler.complete.store(true, Ordering::SeqCst);
    assert_eq!(terminal(&server, &policy, &command, 20000).observation.state, State::Completed);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn transient_inspect_error_after_deadline_preserves_recovery_after_restart() {
    use std::time::{Duration, Instant};
    struct Backend {
        dispatches: AtomicUsize,
        available: AtomicBool,
    }
    impl SupervisorHandler for Backend {
        fn supports_authoritative_inspect(&self) -> bool {
            true
        }
        fn dispatch(&self, _: AuthorizedSupervisorCommand) -> Result<Observation, String> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            Ok(Observation { state: State::Running, evidence: vec![] })
        }
        fn inspect(&self, _: AuthorizedSupervisorInspect) -> Result<Observation, String> {
            if !self.available.load(Ordering::SeqCst) {
                return Err("temporarily unavailable".into());
            }
            Ok(Observation { state: State::Completed, evidence: b"retained result".to_vec() })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("supervisor");
    let (command, policy) = config();
    let handler =
        Arc::new(Backend { dispatches: AtomicUsize::new(0), available: AtomicBool::new(false) });
    let server = Server::open(&root, handler.clone()).unwrap();
    server.request(&policy, Request::Submit(command.clone()), 1).unwrap();
    drop(server);
    let server = reopen(&root, handler.clone());
    let limit = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(Response::Record(record)) = server.request(&policy, inspect(&command), 20000) {
            assert!(!record.observation.state.terminal(), "failed inspect must remain recoverable");
            if record.observation.state == State::Ambiguous {
                break;
            }
        }
        assert!(Instant::now() < limit);
        std::thread::sleep(Duration::from_millis(2));
    }
    handler.available.store(true, Ordering::SeqCst);
    let record = terminal(&server, &policy, &command, 20000);
    assert_eq!(record.observation.state, State::Completed);
    assert_eq!(record.observation.evidence, b"retained result");
    assert_eq!(handler.dispatches.load(Ordering::SeqCst), 1);
}
