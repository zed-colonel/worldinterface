//! Linux process-managed-proof-1 mechanics for trusted, reviewed nonsensitive fixtures.
//! Host composition types are not wire envelopes. No process identity or host path is
//! serialized, exposed through WorkerPort, or included in ordinary diagnostics.
mod admission;
mod artifact;
mod bootstrap;
mod fixture;
mod launch;
mod ownership;
mod supervision;
pub use admission::{
    executable_digest, AuthorizedRun, Clock, ExecutableRegistry, Export, HostScope, Input,
    SystemClock, Task,
};
use artifact::Workspace;
use bootstrap::{Launch, Message};
pub use fixture::fixture_main;
use launch::Executable;
use ownership::{Context, Owned};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
pub use supervision::{helper_main, CleanupReport};
use worldinterface_protocol::ContentHash;
use worldinterface_worker::{
    mapping::{Operation, ResolvedWorkerV1, WorkerRequestV1},
    ports::{Acknowledgment, Observation, ObservationKind, PortFuture, WorkerPort},
    Result,
};
fn io<T>(r: std::io::Result<T>) -> Result<T> {
    r.map_err(|_| "private process I/O failed".into())
}
/// Recipe identity. WIP-10 binds the reviewed source/build recipe as admission provenance.
pub fn adapter_digest() -> ContentHash {
    ContentHash::of(
        [
            include_str!("mod.rs"),
            include_str!("admission.rs"),
            include_str!("artifact.rs"),
            include_str!("bootstrap.rs"),
            include_str!("fixture.rs"),
            include_str!("launch.rs"),
            include_str!("ownership.rs"),
            include_str!("supervision.rs"),
            include_str!("assets/sample-rust-lib.txt"),
            include_str!("assets/sample-rust-Cargo.txt"),
        ]
        .concat()
        .as_bytes(),
    )
}
pub fn dependencies_digest() -> ContentHash {
    ContentHash::of(include_bytes!("../../Cargo.lock"))
}
type RunRegistry = BTreeMap<(String, String), Arc<Mutex<Run>>>;
struct Run {
    authorization: Arc<AuthorizedRun>,
    workspace: Option<Workspace>,
    started: bool,
    collected: bool,
    released: bool,
    quarantined: bool,
    observations: Vec<Observation>,
    artifacts: BTreeMap<String, Vec<u8>>,
    report: Option<CleanupReport>,
    stop: Arc<AtomicBool>,
    monitor: Option<JoinHandle<()>>,
}
// Own the direct child before the next fallible operation, including thread creation.
// On a failed thread spawn Rust drops its closure, which drops this guard and reaps.
struct HelperGuard {
    reaped: bool,
    child: std::process::Child,
    owned: Option<Owned>,
    context: Context,
    socket: std::os::unix::net::UnixStream,
}
impl HelperGuard {
    fn poll_exit(&mut self) -> Result<bool> {
        if self.reaped {
            return Ok(true);
        }
        self.reaped =
            self.owned.as_ref().ok_or("stable child ownership unavailable")?.reap(&self.context)?;
        Ok(self.reaped)
    }
}
impl Drop for HelperGuard {
    fn drop(&mut self) {
        if self.poll_exit().unwrap_or(false) {
            return;
        }
        let _ = bootstrap::send(&mut self.socket, &Message::new(&self.context.nonce, "stop"));
        let _ = self.socket.shutdown(std::net::Shutdown::Write);
        let until = Instant::now() + Duration::from_secs(4);
        while Instant::now() < until {
            if self.poll_exit().unwrap_or(false) {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        if let Some(owned) = &self.owned {
            let _ = owned.signal(&self.context, libc::SIGKILL);
        }
        let until = Instant::now() + Duration::from_millis(500);
        while Instant::now() < until {
            if self.poll_exit().unwrap_or(false) {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Run {
    fn event(&mut self, kind: ObservationKind) {
        if kind == ObservationKind::Quarantined {
            self.quarantined = true;
        }
        if self.observations.len() >= 12 {
            return;
        }
        let sequence = self.observations.len() as u64 + 1;
        self.observations.push(Observation {
            provider: self.authorization.controls.provider.clone(),
            run: self.authorization.controls.run.clone(),
            event: format!("process-event-{sequence}"),
            sequence,
            kind,
        });
    }
}
/// One port per authenticated tenant/principal. Trusted setup may accept paths;
/// the consumer-facing WorkerPort never does.
pub struct ProcessWorkerPort {
    scope: HostScope,
    root: PathBuf,
    helper: Arc<Executable>,
    registry: ExecutableRegistry,
    clock: Arc<dyn Clock>,
    generation: AtomicU64,
    runs: Mutex<RunRegistry>,
}
impl ProcessWorkerPort {
    pub fn new(
        scope: HostScope,
        root: &Path,
        helper: &Path,
        helper_digest: &ContentHash,
        registry: ExecutableRegistry,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        ownership::probe()?;
        worldinterface_worker::nonempty(&scope.tenant)?;
        worldinterface_worker::nonempty(&scope.principal)?;
        let metadata = io(std::fs::symlink_metadata(root))?;
        use std::os::unix::fs::MetadataExt;
        if !metadata.is_dir()
            || metadata.mode() & 0o077 != 0
            || metadata.uid() != unsafe { libc::geteuid() }
        {
            return Err("workspace root must be private and owned".into());
        }
        Ok(Self {
            scope,
            root: root.into(),
            helper: Arc::new(Executable::review(helper, helper_digest)?),
            registry,
            clock,
            generation: AtomicU64::new(0),
            runs: Mutex::new(BTreeMap::new()),
        })
    }
    /// Supply an already authorized realization, including exact requests for each allowed operation.
    pub fn admit(&self, authorization: AuthorizedRun) -> Result<()> {
        let executable =
            self.registry.entries.get(&authorization.executable).ok_or("unreviewed executable")?;
        authorization.validate(
            &self.scope,
            executable,
            self.clock.unix_ms(),
            self.generation.load(Ordering::SeqCst),
        )?;
        let key = (authorization.controls.provider.clone(), authorization.controls.run.clone());
        let mut runs = self.runs.lock().map_err(|_| "process registry unavailable")?;
        if runs.len() >= 64 || runs.contains_key(&key) {
            return Err("run admission conflict or bound".into());
        }
        for run in runs.values() {
            let run = run.lock().map_err(|_| "run unavailable")?;
            if run.authorization.operations.iter().any(|(old, _)| {
                authorization.operations.iter().any(|(new, _)| old.request == new.request)
            }) || run
                .authorization
                .exports
                .iter()
                .any(|old| authorization.exports.iter().any(|new| old.reference == new.reference))
            {
                return Err("request or artifact identity conflict".into());
            }
        }
        runs.insert(
            key,
            Arc::new(Mutex::new(Run {
                authorization: Arc::new(authorization),
                workspace: None,
                started: false,
                collected: false,
                released: false,
                quarantined: false,
                observations: Vec::new(),
                artifacts: BTreeMap::new(),
                report: None,
                stop: Arc::new(AtomicBool::new(false)),
                monitor: None,
            })),
        );
        Ok(())
    }
    pub fn revoke(&self, generation: u64) -> Result<()> {
        let previous = self.generation.fetch_max(generation, Ordering::SeqCst);
        if generation > previous {
            for run in self.runs.lock().map_err(|_| "registry unavailable")?.values() {
                run.lock().map_err(|_| "run unavailable")?.stop.store(true, Ordering::SeqCst);
            }
        }
        Ok(())
    }
    fn run(&self, provider: &str, run: &str) -> Result<Arc<Mutex<Run>>> {
        self.runs
            .lock()
            .map_err(|_| "registry unavailable")?
            .get(&(provider.into(), run.into()))
            .cloned()
            .ok_or_else(|| "run access denied".into())
    }
    /// Bounded non-secret evidence for host integration. No private launch facts are returned.
    pub fn evidence(&self, provider: &str, run: &str) -> Result<Option<CleanupReport>> {
        let shared = self.run(provider, run)?;
        let run = shared.lock().map_err(|_| "run unavailable")?;
        self.authorize_read(&run, Operation::Inspect, None)?;
        Ok(run.report.clone())
    }
    pub fn realization(&self, request: &WorkerRequestV1) -> Result<ResolvedWorkerV1> {
        let shared = self.run(&request.provider, &request.run)?;
        let run = shared.lock().map_err(|_| "run unavailable")?;
        self.authorize_read(&run, Operation::Inspect, None)?;
        run.authorization
            .operations
            .iter()
            .find(|(r, _)| r == request)
            .map(|(_, resolved)| resolved.clone())
            .ok_or_else(|| "request access denied".into())
    }
    pub fn control_evidence(
        &self,
        provider: &str,
        run: &str,
    ) -> Result<worldinterface_worker::controls::ControlEvidenceV1> {
        let shared = self.run(provider, run)?;
        let run = shared.lock().map_err(|_| "run unavailable")?;
        self.authorize_read(&run, Operation::Inspect, None)?;
        let mut evidence = run.authorization.controls.clone();
        evidence.advertised = evidence.applied.clone();
        for kind in [
            worldinterface_worker::controls::ControlKind::Cpu,
            worldinterface_worker::controls::ControlKind::Memory,
            worldinterface_worker::controls::ControlKind::ProcessCount,
            worldinterface_worker::controls::ControlKind::Network,
            worldinterface_worker::controls::ControlKind::Filesystem,
            worldinterface_worker::controls::ControlKind::IndependentVm,
        ] {
            if !evidence.advertised.iter().any(|c| c.kind == kind) {
                evidence.advertised.push(worldinterface_worker::controls::Control {
                    kind,
                    mandatory: false,
                    state: worldinterface_worker::controls::ControlState::Unsupported,
                });
            }
        }
        // Admission declarations cannot manufacture observations. Only a completed supervisor can.
        evidence.observed = if run.report.as_ref().is_some_and(|r| r.cleanup_confirmed) {
            evidence.applied.clone()
        } else {
            Vec::new()
        };
        Ok(evidence)
    }
    fn authorize_read(&self, run: &Run, op: Operation, artifact: Option<&str>) -> Result<()> {
        if run.authorization.scope != self.scope {
            return Err("scope access denied".into());
        }
        run.authorization.check_credential(
            op,
            artifact,
            self.clock.unix_ms(),
            self.generation.load(Ordering::SeqCst),
        )
    }
    fn submit_sync(&self, request: &WorkerRequestV1) -> Result<Acknowledgment> {
        // This branch precedes validation, locking, lookup, workspace allocation and credential issue.
        if !request.operation.available() {
            return Ok(Acknowledgment::Unavailable);
        }
        request.validate()?;
        let shared = self.run(&request.provider, &request.run)?;
        let mut run = shared.lock().map_err(|_| "run unavailable")?;
        let auth = run.authorization.clone();
        let executable =
            self.registry.entries.get(&auth.executable).ok_or("unreviewed executable")?;
        let (wall, output) = auth.validate(
            &self.scope,
            executable,
            self.clock.unix_ms(),
            self.generation.load(Ordering::SeqCst),
        )?;
        if !auth.operations.iter().any(|(r, _)| r == request) {
            return Err("exact operation not admitted".into());
        }
        if run.quarantined
            && !matches!(request.operation, Operation::Inspect | Operation::Terminate)
        {
            return Err("run quarantined".into());
        }
        if run.released {
            return if request.operation == Operation::Release {
                Ok(Acknowledgment::Accepted)
            } else {
                Err("run released".into())
            };
        }
        match request.operation {
            Operation::Provision => {
                if run.workspace.is_none() {
                    run.workspace = Some(Workspace::allocate(&self.root, &auth)?);
                    run.event(ObservationKind::Provisioned);
                }
            }
            Operation::Start => {
                if run.started {
                    return Ok(Acknowledgment::Accepted);
                }
                if run.stop.load(Ordering::SeqCst) {
                    return Err("run already stopped".into());
                }
                let workspace = run.workspace.as_ref().ok_or("run not provisioned")?;
                let nonce = ownership::random()?;
                let token = ownership::random()?;
                let context = Context {
                    provider: request.provider.clone(),
                    run: request.run.clone(),
                    nonce: nonce.clone(),
                    allocation: workspace.allocation.clone(),
                };
                let (socket, child_socket) = io(std::os::unix::net::UnixStream::pair())?;
                let child =
                    launch::helper(&self.helper, executable, &child_socket, workspace.dir.path())?;
                drop(child_socket);
                let mut helper = HelperGuard { reaped: false, child, owned: None, context, socket };
                match Owned::acquire(helper.child.id(), std::process::id(), &helper.context) {
                    Ok(p) => helper.owned = Some(p),
                    Err(_) => {
                        // No numeric-PID fallback. Helper has not received authority to launch work.
                        run.started = true;
                        run.report = Some(CleanupReport {
                            reason: "helper ownership unavailable".into(),
                            findings: vec!["helper exit or cleanup unconfirmed".into()],
                            ..CleanupReport::default()
                        });
                        run.event(ObservationKind::Quarantined);
                        return Err("helper ownership unavailable".into());
                    }
                }
                let launch = Launch {
                    nonce,
                    allocation: helper.context.allocation.clone(),
                    token,
                    credential: auth.credential.clone(),
                    task: auth.task.clone(),
                    wall_ms: wall,
                    output_bytes: output,
                };
                run.started = true; // Never retry an ambiguous consequential spawn.
                let stop = run.stop.clone();
                let target = shared.clone();
                match std::thread::Builder::new()
                    .name("wi-process-monitor".into())
                    .spawn(move || monitor(&mut helper, launch, stop, target))
                {
                    Ok(monitor) => run.monitor = Some(monitor),
                    Err(_) => {
                        run.report = Some(CleanupReport {
                            reason: "supervisor unavailable".into(),
                            findings: vec!["launch supervision failed".into()],
                            ..CleanupReport::default()
                        });
                        run.event(ObservationKind::Quarantined);
                        return Err("supervisor unavailable".into());
                    }
                }
            }
            Operation::Inspect => {}
            Operation::Terminate => {
                run.stop.store(true, Ordering::SeqCst);
            }
            Operation::CollectArtifacts => {
                if run.collected {
                    return Ok(Acknowledgment::Accepted);
                }
                if !run.report.as_ref().is_some_and(|r| r.exit_confirmed && r.cleanup_confirmed) {
                    return Err("cleanup not confirmed".into());
                }
                let remaining = output
                    .saturating_sub(run.report.as_ref().map_or(0, |r| r.retained_output_bytes));
                match run.workspace.as_ref().ok_or("workspace absent")?.collect(&auth, remaining) {
                    Ok(artifacts) => {
                        run.artifacts = artifacts.into_iter().collect();
                        run.collected = true;
                        run.event(ObservationKind::ArtifactPublished);
                    }
                    Err(e) => {
                        run.event(ObservationKind::Quarantined);
                        return Err(e);
                    }
                }
            }
            Operation::Release => {
                if run.quarantined {
                    return Err("quarantined allocation retained".into());
                }
                if run.started && !run.report.as_ref().is_some_and(|r| r.cleanup_confirmed) {
                    return Err("cleanup incomplete; allocation quarantined".into());
                }
                if !auth.exports.is_empty() && !run.collected {
                    return Err("required exports not collected".into());
                }
                if let Some(workspace) = run.workspace.take() {
                    io(workspace.dir.close())?;
                }
                run.released = true;
                run.event(ObservationKind::Released);
            }
            _ => unreachable!(),
        }
        Ok(Acknowledgment::Accepted)
    }
}
impl WorkerPort for ProcessWorkerPort {
    fn submit<'a>(&'a self, request: &'a WorkerRequestV1) -> PortFuture<'a, Acknowledgment> {
        Box::pin(async move { self.submit_sync(request) })
    }
    fn observe<'a>(&'a self, provider: &'a str, run: &'a str) -> PortFuture<'a, Vec<Observation>> {
        Box::pin(async move {
            let shared = self.run(provider, run)?;
            let run = shared.lock().map_err(|_| "run unavailable")?;
            self.authorize_read(&run, Operation::Inspect, None)?;
            Ok(run.observations.clone())
        })
    }
    fn read_artifact<'a>(&'a self, reference: &'a str) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let runs = self.runs.lock().map_err(|_| "registry unavailable")?;
            for shared in runs.values() {
                let run = shared.lock().map_err(|_| "run unavailable")?;
                if let Some(bytes) = run.artifacts.get(reference) {
                    self.authorize_read(&run, Operation::CollectArtifacts, Some(reference))?;
                    return Ok(bytes.clone());
                }
            }
            Err("artifact access denied".into())
        })
    }
}
fn monitor(
    helper: &mut HelperGuard,
    launch: Launch,
    stop: Arc<AtomicBool>,
    target: Arc<Mutex<Run>>,
) {
    let HelperGuard { child: _, reaped: already_reaped, owned, context, socket } = helper;
    let Some(owned) = owned.as_ref() else {
        return;
    };
    let mut report =
        CleanupReport { reason: "helper protocol failure".into(), ..CleanupReport::default() };
    let result = (|| -> Result<()> {
        bootstrap::send(socket, &launch)?;
        let ready: Message = bootstrap::receive(socket)?;
        if ready.nonce != context.nonce || ready.kind != "ready" {
            return Err("helper bootstrap refused".into());
        }
        owned.validate(context)?;
        bootstrap::send(socket, &Message::new(&context.nonce, "go"))?;
        let until = Instant::now() + Duration::from_millis(launch.wall_ms) + Duration::from_secs(6);
        let mut stop_at = None;
        let mut decoder = bootstrap::Decoder::default();
        let mut running = false;
        while Instant::now() < until {
            if stop.load(Ordering::SeqCst) && stop_at.is_none() {
                bootstrap::send(socket, &Message::new(&context.nonce, "stop"))?;
                stop_at = Some(Instant::now());
            }
            if stop_at.is_some_and(|t: Instant| t.elapsed() > Duration::from_secs(4)) {
                return Err("helper stop deadline".into());
            }
            match decoder.poll::<Message>(socket)? {
                Some(m) if m.nonce != context.nonce => return Err("helper ownership drift".into()),
                Some(m) if m.kind == "running" && !running => {
                    running = true;
                    if let Ok(mut run) = target.lock() {
                        run.event(ObservationKind::Running);
                    }
                }
                Some(m) if m.kind == "report" => {
                    report = m.report.ok_or("missing cleanup report")?;
                    return Ok(());
                }
                Some(_) => return Err("helper protocol state".into()),
                None => {}
            }
            std::thread::sleep(Duration::from_millis(3));
        }
        Err("helper wall deadline".into())
    })();
    if result.is_err() {
        let _ = bootstrap::send(socket, &Message::new(&context.nonce, "stop"));
        let _ = socket.shutdown(std::net::Shutdown::Write);
    }
    // Let the subreaper finish cleanup before escalating against the helper itself.
    let until = Instant::now() + Duration::from_secs(4);
    let mut reaped = false;
    while Instant::now() < until {
        match owned.reap(context) {
            Ok(true) => {
                *already_reaped = true;
                reaped = true;
                break;
            }
            Ok(false) => {}
            Err(_) => break,
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    if !reaped {
        let _ = owned.signal(context, libc::SIGKILL);
        let until = Instant::now() + Duration::from_millis(500);
        while Instant::now() < until {
            if owned.reap(context).unwrap_or(false) {
                *already_reaped = true;
                reaped = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    if result.is_err() || !reaped {
        report.cleanup_confirmed = false;
        report.findings.push("helper cleanup unconfirmed".into());
    }
    if let Ok(mut run) = target.lock() {
        if report.exit_confirmed {
            run.event(ObservationKind::Exited);
        }
        if !report.cleanup_confirmed {
            run.event(ObservationKind::Quarantined);
        }
        run.report = Some(report);
    }
}
impl Drop for ProcessWorkerPort {
    fn drop(&mut self) {
        if let Ok(runs) = self.runs.get_mut() {
            let mut monitors = Vec::new();
            for shared in runs.values() {
                if let Ok(mut run) = shared.lock() {
                    run.stop.store(true, Ordering::SeqCst);
                    if let Some(monitor) = run.monitor.take() {
                        monitors.push(monitor);
                    }
                }
            }
            for monitor in monitors {
                let _ = monitor.join();
            }
            for shared in runs.values() {
                if let Ok(mut run) = shared.lock() {
                    if run.quarantined
                        || (run.started
                            && !run.report.as_ref().is_some_and(|r| r.cleanup_confirmed))
                    {
                        if let Some(workspace) = run.workspace.take() {
                            workspace.preserve();
                        }
                    }
                }
            }
        }
    }
}
