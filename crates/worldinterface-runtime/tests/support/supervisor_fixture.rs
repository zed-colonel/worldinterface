#![allow(dead_code)]
use super::child::KillOnDrop;
use serde::{Deserialize, Serialize};
use std::{
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{net::UnixStream, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Command as Process, Stdio},
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};
use worldinterface_protocol::{ContentHash, OpaqueRef};
use worldinterface_runtime::supervisor::*;
use worldinterface_worker::supervisor::*;
pub fn r(s: &str) -> OpaqueRef {
    s.to_owned().try_into().unwrap()
}
pub fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}
#[derive(Serialize, Deserialize)]
pub struct Config {
    pub request: LifecycleRequest,
    pub tenant: OpaqueRef,
    pub principal: OpaqueRef,
}
impl Config {
    pub fn policy(&self) -> ChannelPolicy {
        let session = Session {
            tenant: self.tenant.clone(),
            principal: self.principal.clone(),
            audience: self.request.audience.clone(),
            authority_generation: self.request.authority_generation.clone(),
            policy_revision: self.request.policy_revision.clone(),
        };
        ChannelPolicy {
            grants: vec![Grant { session: session.clone(), request: self.request.clone() }],
            session,
            revoked: Arc::new(AtomicBool::new(false)),
        }
    }
}
pub struct Fixture {
    pub root: PathBuf,
}
impl SupervisorHandler for Fixture {
    fn supports_authoritative_inspect(&self) -> bool {
        true
    }
    fn dispatch(&self, command: AuthorizedSupervisorCommand) -> Result<Observation, String> {
        // The consumer's durable stable-key backend lives outside WI and guest.
        let path = self.root.join("backend-command");
        let digest = command.command().digest()?;
        if path.exists() {
            if std::fs::read(&path).unwrap() != digest.as_str().as_bytes() {
                return Err("backend conflict".into());
            }
        } else {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|e| e.to_string())?;
            file.write_all(digest.as_str().as_bytes()).map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            std::fs::File::open(&self.root).unwrap().sync_all().unwrap();
            std::fs::write(self.root.join("activations"), b"1").unwrap();
        }
        if self.root.join("interrupt-dispatch").exists() {
            abort_without_core_dump(&self.root);
        }
        Ok(Observation { state: State::Running, evidence: b"durably dispatched".to_vec() })
    }
    fn inspect(&self, command: AuthorizedSupervisorInspect) -> Result<Observation, String> {
        if self.root.join("delay-inspect").exists() {
            // Deliberately outlast many AQ ticks. Host latency must not burn
            // virtual seconds and exhaust reconciliation before this finishes.
            std::thread::sleep(Duration::from_millis(250));
        }
        let path = self.root.join("backend-command");
        if !path.exists() {
            return Ok(Observation {
                state: State::Rejected,
                evidence: b"authoritatively absent".to_vec(),
            });
        }
        if std::fs::read(path).unwrap() != command.command().digest()?.as_str().as_bytes() {
            return Ok(Observation { state: State::Escalated, evidence: b"conflict".to_vec() });
        }
        let complete = self.root.join("complete").exists();
        Ok(Observation {
            state: if complete { State::Completed } else { State::Running },
            evidence: if complete { b"external lifecycle completed".to_vec() } else { vec![] },
        })
    }
}
/// Suppress workstation crash dumps only at the intentional crash cut. Earlier
/// procfs/environment/descriptor probes must retain their ordinary visibility.
pub fn abort_without_core_dump(root: &Path) -> ! {
    if root.join("delay-abort").exists() {
        // Give the lost-ack main thread time to return if its crash wait is
        // missing. Dumpability is changed only after this deliberate race cut.
        std::thread::sleep(Duration::from_millis(250));
    }
    let disabled = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_CORE, &disabled) }, 0);
    assert_eq!(unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) }, 0);
    let mut limit = disabled;
    assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut limit) }, 0);
    assert_eq!((limit.rlim_cur, limit.rlim_max), (0, 0));
    assert_eq!(unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) }, 0);
    std::fs::write(root.join("abort-no-core"), b"dumpable=0\nrlimit_core=0:0\n").unwrap();
    std::process::abort();
}
pub fn live_packet() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let environment: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.join("conformance/wi-fabric-2/worker/evaluation-environment.json"))
            .unwrap(),
    )
    .unwrap();
    for (path, expected) in environment["support_binary_sha256"].as_object().unwrap() {
        assert_eq!(
            ContentHash::of(&std::fs::read(path).unwrap()).as_str(),
            expected.as_str().unwrap(),
            "supervisor support binary drift"
        );
    }
    assert!(Process::new("timeout")
        .args(["30s", "python3", "-B", "scripts/wi_worker_evaluation.py", "--require-committed"])
        .current_dir(root)
        .status()
        .unwrap()
        .success());
}
pub fn transfer(command: &mut Process, stream: &UnixStream) {
    let fd = stream.as_raw_fd();
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(fd, 198) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}
pub fn channel(root: &Path, config: &Config) -> (Arc<Client>, KillOnDrop) {
    let (client, channel) = PrivateChannel::provision(config.policy()).unwrap();
    let (stream, _) = channel.into_parts();
    let mut command = Process::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "supervisor_child", "--nocapture"])
        .env_clear()
        .env("WI_SUPERVISOR_ROOT", root)
        .env("TMPDIR", std::env::var_os("TMPDIR").unwrap())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    transfer(&mut command, &stream);
    let child = KillOnDrop(command.spawn().unwrap());
    (Arc::new(client), child)
}
pub fn server_child(root: &Path) {
    let config: Config =
        serde_json::from_slice(&std::fs::read(root.join("config.json")).unwrap()).unwrap();
    let stream = unsafe { UnixStream::from_raw_fd(198) };
    let mut channel = PrivateChannel::from_provisioned(stream, config.policy());
    let server =
        Server::open(&root.join("journal"), Arc::new(Fixture { root: root.to_owned() })).unwrap();
    if root.join("drop-ack").exists() {
        use std::io::Read;
        let (mut stream, policy) = channel.into_parts();
        let mut size = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let len = u32::from_be_bytes(size) as usize;
        assert!(len <= MAX_FRAME_BYTES);
        let mut bytes = vec![0; len];
        stream.read_exact(&mut bytes).unwrap();
        server.request(&policy, decode(&bytes).unwrap(), now()).unwrap();
        wait_for_activation(root);
        if root.join("interrupt-dispatch").exists() {
            // Activation is published just before the dispatch thread's abort
            // cut. Do not let the test main thread exit normally in that gap.
            std::thread::sleep(Duration::from_secs(3));
            panic!("interrupted dispatch did not abort the supervisor");
        }
        return; // Intentionally lose every acknowledgement and callback.
    }
    loop {
        if channel.serve_one(&server, now()).is_err() {
            return;
        }
    }
}
pub fn wait_for_activation(root: &Path) {
    // backend-command is created before its contents are synced and before the
    // activation record is written. Do not exit/kill the supervisor in that gap.
    let path = root.join("activations");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match std::fs::read(&path) {
            Ok(bytes) if bytes == b"1" => return,
            Ok(bytes) => assert!(bytes.is_empty(), "unexpected activation evidence: {bytes:?}"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("activation evidence read failed: {error}"),
        }
        assert!(Instant::now() < deadline, "missing fixture evidence {}", path.display());
        std::thread::sleep(Duration::from_millis(5));
    }
}
