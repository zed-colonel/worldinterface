//! Per-run subreaper. The embedding application's child policy is never changed.
use super::{
    bootstrap::{self, Decoder, Launch, Message},
    io, launch,
    ownership::{self, Context, Owned},
    Result,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::net::UnixStream,
    },
    process::Child,
    time::{Duration, Instant},
};
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupReport {
    pub helper_launch_clean: bool,
    pub exit_confirmed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_success: Option<bool>,
    pub cleanup_confirmed: bool,
    pub term_signals: u32,
    pub kill_signals: u32,
    pub descendants_observed: u32,
    pub retained_output_bytes: u64,
    pub reason: String,
    pub findings: Vec<String>,
}
struct Tree {
    child: Child,
    context: Context,
    members: BTreeMap<u32, Owned>,
    report: CleanupReport,
    finished: bool,
    logs: Vec<u8>,
}
impl Tree {
    fn discover(&mut self) {
        // Descendants reaped by their own parents never reach our waitpid loop.
        // Release their stable handles before accounting for the live-member cap.
        self.members.retain(|_, member| !member.exited());
        let root = std::process::id();
        let mut pending = vec![root];
        let mut visited = std::collections::BTreeSet::new();
        while let Some(parent) = pending.pop() {
            if !visited.insert(parent) {
                continue;
            }
            if visited.len() > 256 {
                self.finding("descendant discovery bound");
                break;
            }
            let children = if parent == root {
                ownership::children(root)
            } else if let Some(p) = self.members.get(&parent) {
                p.children(&self.context)
            } else {
                continue;
            };
            let children = match children {
                Ok(c) => c,
                Err(_) => {
                    self.finding("descendant discovery incomplete");
                    continue;
                }
            };
            for pid in children {
                if !self.members.contains_key(&pid) {
                    if self.members.len() >= 256 {
                        self.finding("descendant handle bound");
                        continue;
                    }
                    match Owned::acquire_if_present(pid, parent, &self.context) {
                        Ok(Some(member)) => {
                            // Parent identity is pinned and revalidated after acquiring the child.
                            if parent != root
                                && self
                                    .members
                                    .get(&parent)
                                    .is_none_or(|p| p.validate(&self.context).is_err())
                            {
                                self.finding("descendant relationship uncertain");
                                continue;
                            }
                            self.members.insert(pid, member);
                            self.report.descendants_observed += 1;
                        }
                        Ok(None) => continue,
                        Err(_) => {
                            self.finding("descendant ownership unavailable");
                            continue;
                        }
                    }
                }
                pending.push(pid);
            }
        }
    }
    fn finding(&mut self, s: &str) {
        if self.report.findings.len() < 8 && !self.report.findings.iter().any(|f| f == s) {
            self.report.findings.push(s.into());
        }
    }
    fn reap(&mut self) -> bool {
        loop {
            let mut status = 0;
            // Only this dedicated helper's children are reaped. Never wait on embedding-host children.
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if pid > 0 {
                if pid as u32 == self.child.id() {
                    self.report.exit_confirmed = true;
                    self.report.exit_success =
                        Some(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
                }
                self.members.remove(&(pid as u32));
            } else if pid == 0 {
                return false;
            } else {
                return std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD);
            }
        }
    }
    fn signal(&mut self, signal: i32) {
        let mut failed = false;
        for member in self.members.values() {
            if member.exited() {
                continue;
            }
            if member.signal(&self.context, signal).is_err() {
                failed = true;
            } else if signal == libc::SIGTERM {
                self.report.term_signals += 1;
            } else {
                self.report.kill_signals += 1;
            }
        }
        if failed {
            self.finding("ownership-validated signal refused");
        }
    }
    fn drain(&mut self, bound: u64) -> bool {
        let mut overflow = false;
        for fd in [
            self.child.stdout.as_ref().map(AsRawFd::as_raw_fd),
            self.child.stderr.as_ref().map(AsRawFd::as_raw_fd),
        ]
        .into_iter()
        .flatten()
        {
            let mut buf = [0u8; 4096];
            // Each pipe gets one read per turn; inherited open pipes cannot block cleanup.
            let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
            if n > 0 {
                let available = (bound as usize).saturating_sub(self.logs.len());
                self.logs.extend_from_slice(&buf[..(n as usize).min(available)]);
                if n as usize > available {
                    overflow = true;
                }
            } else if n < 0
                && std::io::Error::last_os_error().kind() != std::io::ErrorKind::WouldBlock
            {
                self.finding("output read failed");
            }
        }
        self.report.retained_output_bytes = self.logs.len() as u64;
        overflow
    }
    fn cleanup(&mut self, fixture: &mut UnixStream, nonce: &str, bound: u64) {
        let until = Instant::now() + Duration::from_millis(1000);
        let _ = bootstrap::send(fixture, &Message::new(nonce, "stop"));
        let escalation = Instant::now() + Duration::from_millis(200);
        while Instant::now() < until {
            self.discover();
            self.drain(bound);
            self.signal(if Instant::now() < escalation { libc::SIGTERM } else { libc::SIGKILL });
            if self.reap() {
                self.report.cleanup_confirmed =
                    self.report.exit_confirmed && self.report.findings.is_empty();
                self.finished = true;
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        self.finding("cleanup deadline exhausted");
        self.finished = true;
    }
}
impl Drop for Tree {
    fn drop(&mut self) {
        if !self.finished {
            // Every post-spawn error uses the same bounded, validated tree cleanup.
            let until = Instant::now() + Duration::from_millis(1000);
            while Instant::now() < until {
                self.discover();
                self.signal(libc::SIGKILL);
                if self.reap() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
}
fn validate_launch(l: &Launch) -> Result<()> {
    l.task.validate()?;
    if l.nonce.len() != 64
        || l.allocation.len() != 64
        || l.token.len() != 64
        || l.wall_ms == 0
        || l.wall_ms > 60_000
        || l.output_bytes == 0
        || l.output_bytes > 4 * 1024 * 1024
    {
        return Err("invalid launch binding".into());
    }
    let c = &l.credential;
    c.permits(
        &worldinterface_worker::credential::CredentialExpectation {
            issuer: "worldinterface",
            audience: "process-managed-proof-1",
            principal: &c.principal,
            provider: &c.provider,
            run: &c.run,
            authorization_digest: &c.authorization_digest,
        },
        worldinterface_worker::mapping::Operation::Start,
        None,
        super::admission::Clock::unix_ms(&super::admission::SystemClock),
        c.revocation_generation,
    )
}
/// Entry point for the reviewed per-run reaper binary, not a consumer API.
#[doc(hidden)]
pub fn helper_main() -> Result<()> {
    let helper_launch_clean = launch::environment_and_descriptors_clean(5)?;
    // SAFETY: invoked in the standalone single-threaded helper, never the embedding process.
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } < 0 {
        return Err("subreaper unavailable".into());
    }
    ownership::probe()?;
    let mut host = unsafe { UnixStream::from_raw_fd(3) };
    let launch: Launch = bootstrap::receive(&mut host)?;
    validate_launch(&launch)?;
    let journal = super::journal::Journal::from_helper(
        unsafe { std::fs::File::from_raw_fd(5) },
        &launch.nonce,
    )?;
    let fixture_launch = launch.fixture_bootstrap()?;
    bootstrap::send(&mut host, &Message::new(&launch.nonce, "ready"))?;
    let go: Message = bootstrap::receive(&mut host)?;
    if go.nonce != launch.nonce || go.kind != "go" {
        return Err("launch ownership handshake refused".into());
    }
    let (mut fixture, child_socket) = io(UnixStream::pair())?;
    let allocation = launch.allocation.clone();
    let child = launch::fixture(&child_socket, &launch.task)?;
    drop(child_socket);
    // Tree guard exists before any fallible registration or bootstrap work.
    let mut tree = Tree {
        child,
        context: Context {
            provider: launch.credential.provider.clone(),
            run: launch.credential.run.clone(),
            nonce: launch.nonce.clone(),
            allocation,
        },
        members: BTreeMap::new(),
        report: CleanupReport { helper_launch_clean, ..CleanupReport::default() },
        finished: false,
        logs: Vec::new(),
    };
    let startup = (|| -> Result<()> {
        bootstrap::send(&mut host, &Message::new(&launch.nonce, "fixture_spawned"))?;
        let record: Message = bootstrap::receive(&mut host)?;
        if record.nonce != launch.nonce || record.kind != "record_fixture" {
            return Err("fixture recording permission refused".into());
        }
        let owned = Owned::acquire(tree.child.id(), std::process::id(), &tree.context)?;
        journal.append("fixture_owned", Some(&owned), None)?;
        tree.members.insert(owned.pid(), owned);
        bootstrap::send(&mut host, &Message::new(&launch.nonce, "fixture_owned"))?;
        let permission: Message = bootstrap::receive(&mut host)?;
        if permission.nonce != launch.nonce || permission.kind != "launch_fixture" {
            return Err("fixture launch permission refused".into());
        }
        launch::nonblocking(tree.child.stdout.as_ref().ok_or("stdout missing")?.as_raw_fd())?;
        launch::nonblocking(tree.child.stderr.as_ref().ok_or("stderr missing")?.as_raw_fd())?;
        bootstrap::send(&mut fixture, &fixture_launch)?;
        let ready: Message = bootstrap::receive(&mut fixture)?;
        bootstrap::verify_ready(
            &fixture_launch,
            &ready,
            &worldinterface_worker::credential::CredentialExpectation {
                issuer: "worldinterface",
                audience: "process-managed-proof-1",
                principal: &launch.credential.principal,
                provider: &tree.context.provider,
                run: &tree.context.run,
                authorization_digest: &launch.credential.authorization_digest,
            },
            super::admission::Clock::unix_ms(&super::admission::SystemClock),
            launch.credential.revocation_generation,
        )?;
        bootstrap::send(&mut fixture, &Message::new(&fixture_launch.handshake, "go"))?;
        bootstrap::send(&mut host, &Message::new(&launch.nonce, "running"))
    })();
    let started = Instant::now();
    let mut decoder = Decoder::default();
    if startup.is_err() {
        tree.report.reason = "bootstrap refused".into();
        tree.finding("bootstrap incomplete");
    } else {
        loop {
            tree.discover();
            let overflow = tree.drain(launch.output_bytes);
            if tree.reap() || tree.report.exit_confirmed {
                tree.report.reason = "exited".into();
                break;
            }
            if overflow {
                tree.report.reason = "output bound".into();
                break;
            }
            if started.elapsed() >= Duration::from_millis(launch.wall_ms) {
                tree.report.reason = "wall-time bound".into();
                break;
            }
            if super::admission::Clock::unix_ms(&super::admission::SystemClock)
                >= launch.credential.expires_unix_ms
            {
                tree.report.reason = "credential expired".into();
                break;
            }
            match decoder.poll::<Message>(&mut host) {
                Ok(Some(m)) if m.nonce == launch.nonce && m.kind == "stop" => {
                    tree.report.reason = "stop requested".into();
                    break;
                }
                Ok(Some(_)) | Err(_) => {
                    tree.report.reason = "host protocol closed or refused".into();
                    break;
                }
                _ => {}
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    tree.cleanup(&mut fixture, &fixture_launch.handshake, launch.output_bytes);
    journal.append("cleanup", None, Some(&tree.report))?;
    bootstrap::send(
        &mut host,
        &Message { nonce: launch.nonce, kind: "report".into(), report: Some(tree.report.clone()) },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Guard(Tree);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.0.child.kill();
            let _ = self.0.child.wait();
        }
    }
    #[test]
    fn cleanup_uncertainty_is_reported_without_signalling_mismatched_ownership() {
        const CHILD_CASE: &str = "WI_TEST_PRIVATE_CLEANUP_CASE";
        if std::env::var_os(CHILD_CASE).is_some() {
            // Isolated test process: no embedding-host children can enter the helper's discovery/reaping.
            let context = Context {
                provider: "p".into(),
                run: "r".into(),
                nonce: ownership::random().unwrap(),
                allocation: ownership::random().unwrap(),
            };
            let child = std::process::Command::new("/bin/sleep")
                .arg("30")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let mut tree = Guard(Tree {
                child,
                context,
                members: BTreeMap::new(),
                report: CleanupReport::default(),
                finished: false,
                logs: Vec::new(),
            });
            let owned =
                Owned::acquire(tree.0.child.id(), std::process::id(), &tree.0.context).unwrap();
            tree.0.members.insert(owned.pid(), owned);
            launch::nonblocking(tree.0.child.stdout.as_ref().unwrap().as_raw_fd()).unwrap();
            launch::nonblocking(tree.0.child.stderr.as_ref().unwrap().as_raw_fd()).unwrap();
            tree.0.context.nonce.push('x'); // Test-only stale ownership injection; target is still alive.
            let (mut socket, _peer) = UnixStream::pair().unwrap();
            tree.0.cleanup(&mut socket, "wrong", 100);
            assert!(!tree.0.report.cleanup_confirmed);
            assert!(!tree.0.report.exit_confirmed);
            assert!(tree.0.child.try_wait().unwrap().is_none());
            assert!(tree.0.report.findings.iter().any(|f| f.contains("signal refused")));
            return;
        }
        let child=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","worker_process::supervision::tests::cleanup_uncertainty_is_reported_without_signalling_mismatched_ownership","--test-threads=1"]).env(CHILD_CASE,"1").stdout(std::process::Stdio::null()).spawn().unwrap();
        struct ChildGuard(std::process::Child);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut child = ChildGuard(child);
        let until = Instant::now() + Duration::from_secs(5);
        while Instant::now() < until {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("bounded test subprocess deadline");
    }
}
