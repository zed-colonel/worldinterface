//! Stable handles and private identity. Never use numeric kill or process-group signalling.
use super::{io, Result};
use std::{
    fs::{self, File},
    io::Read,
    os::fd::{AsRawFd, FromRawFd},
};

pub(super) fn random() -> Result<String> {
    let mut bytes = [0u8; 32];
    io(io(fs::File::open("/dev/urandom"))?.read_exact(&mut bytes))?;
    Ok(hex::encode(bytes))
}
fn boot() -> Result<String> {
    io(fs::read_to_string("/proc/sys/kernel/random/boot_id"))
}
fn stat(pid: u32) -> Result<(u32, u64)> {
    stat_if_present(pid)?.ok_or_else(|| "process departed".into())
}
fn stat_if_present(pid: u32) -> Result<Option<(u32, u64)>> {
    stat_from_read(fs::read_to_string(format!("/proc/{pid}/stat")))
}
fn stat_from_read(read: std::io::Result<String>) -> Result<Option<(u32, u64)>> {
    let s = match read {
        Ok(s) => s,
        // Reaping between open and read returns ESRCH rather than ENOENT.
        // Both establish departure; other I/O and live identity failures remain errors.
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                || e.raw_os_error() == Some(libc::ESRCH) =>
        {
            return Ok(None);
        }
        Err(e) => return io(Err(e)),
    };
    let fields: Vec<_> =
        s.rsplit_once(')').ok_or("process identity missing")?.1.split_whitespace().collect();
    Ok(Some((
        fields
            .get(1)
            .ok_or("process parent missing")?
            .parse()
            .map_err(|_| "process parent invalid")?,
        fields
            .get(19)
            .ok_or("process birth missing")?
            .parse()
            .map_err(|_| "process birth invalid")?,
    )))
}
pub(super) struct Context {
    pub provider: String,
    pub run: String,
    pub nonce: String,
    pub allocation: String,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Identity {
    pid: u32,
    boot: String,
    birth: u64,
}
pub(super) struct Owned {
    pid: u32,
    boot: String,
    birth: u64,
    nonce: String,
    provider: String,
    run: String,
    allocation: String,
    handle: File,
}
pub(super) enum Acquisition {
    Acquired(Owned),
    Departed,
    Rediscover,
}
impl Owned {
    pub fn identity(&self) -> Identity {
        Identity { pid: self.pid, boot: self.boot.clone(), birth: self.birth }
    }
    pub fn acquire(pid: u32, parent: u32, context: &Context) -> Result<Self> {
        match Self::acquire_descendant(pid, parent, None, context)? {
            Acquisition::Acquired(record) => Ok(record),
            Acquisition::Departed => Err("process departed".into()),
            Acquisition::Rediscover => Err("descendant relationship changed".into()),
        }
    }
    pub fn acquire_descendant(
        pid: u32,
        parent: u32,
        subreaper: Option<u32>,
        context: &Context,
    ) -> Result<Acquisition> {
        Self::acquire_with_barrier(pid, parent, subreaper, context, || {})
    }
    fn acquire_with_barrier(
        pid: u32,
        parent: u32,
        subreaper: Option<u32>,
        context: &Context,
        after_handle_acquired: impl FnOnce(),
    ) -> Result<Acquisition> {
        let Some((_, birth)) = stat_if_present(pid)? else {
            return Ok(Acquisition::Departed);
        };
        // SAFETY: pidfd_open has no pointer arguments; the returned descriptor is owned once.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                return Ok(Acquisition::Departed);
            }
            return Err("stable process handle unavailable".into());
        }
        let handle = unsafe { File::from_raw_fd(fd as i32) };
        let record = Self {
            pid,
            boot: boot()?,
            birth,
            nonce: context.nonce.clone(),
            provider: context.provider.clone(),
            run: context.run.clone(),
            allocation: context.allocation.clone(),
            handle,
        };
        // Tests force parent exit or child reaping after the handle is acquired.
        after_handle_acquired();
        let validated = record.validate(context);
        if record.exited() {
            return Ok(Acquisition::Departed);
        }
        validated?; // A live birth/handle/context mismatch always fails closed.
        let relationship = stat_if_present(pid)?;
        if record.exited() {
            return Ok(Acquisition::Departed);
        }
        let Some((observed_parent, observed_birth)) = relationship else {
            return Ok(Acquisition::Departed);
        };
        if observed_birth != record.birth {
            return Err("process ownership mismatch".into());
        }
        if observed_parent == parent {
            return Ok(Acquisition::Acquired(record));
        }
        if subreaper == Some(observed_parent) {
            // Do not adopt through a stale ancestry edge. The next root scan
            // must acquire and validate a fresh stable handle to this identity.
            return Ok(Acquisition::Rediscover);
        }
        Err("unproven descendant".into())
    }
    pub fn validate(&self, c: &Context) -> Result<()> {
        if self.boot != boot()?
            || self.nonce != c.nonce
            || self.provider != c.provider
            || self.run != c.run
            || self.allocation != c.allocation
            || self.birth != stat(self.pid)?.1
        {
            return Err("process ownership mismatch".into());
        }
        // Verify that the descriptor itself is bound to this live identity, including fault-injected records.
        let info =
            io(fs::read_to_string(format!("/proc/self/fdinfo/{}", self.handle.as_raw_fd())))?;
        let target =
            info.lines().find_map(|l| l.strip_prefix("Pid:\t")).and_then(|p| p.parse::<u32>().ok());
        if target != Some(self.pid) {
            return Err("process handle mismatch".into());
        }
        Ok(())
    }
    pub fn signal(&self, c: &Context, signal: i32) -> Result<()> {
        self.validate(c)?;
        // SAFETY: signal targets the stable descriptor. Null siginfo uses kernel credentials.
        let r = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.handle.as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if r < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            return Err("process signal failed".into());
        }
        Ok(())
    }
    pub fn exited(&self) -> bool {
        let mut fd = libc::pollfd { fd: self.handle.as_raw_fd(), events: libc::POLLIN, revents: 0 };
        // SAFETY: one initialized pollfd; zero timeout.
        unsafe { libc::poll(&mut fd, 1, 0) > 0 }
    }
    pub fn reap(&self, c: &Context) -> Result<bool> {
        self.validate(c)?;
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // Stable direct-child wait: even a host SIGCHLD handler cannot cause us
        // to wait on an unrelated child that later reuses the numeric PID.
        if unsafe {
            libc::waitid(
                libc::P_PIDFD,
                self.handle.as_raw_fd() as u32,
                &mut info,
                libc::WEXITED | libc::WNOHANG,
            )
        } < 0
        {
            return Err("stable child status unavailable".into());
        }
        Ok(unsafe { info.si_pid() != 0 })
    }
    pub fn pid(&self) -> u32 {
        self.pid
    }
    pub fn children(&self, c: &Context) -> Result<Vec<u32>> {
        if self.exited() {
            return Ok(Vec::new());
        }
        let result = (|| {
            self.validate(c)?;
            let children = children(self.pid)?;
            self.validate(c)?;
            Ok(children)
        })();
        // Reaping between /proc reads is normal. Orphans will be rediscovered
        // under the subreaper; a live identity failure still quarantines cleanup.
        if result.is_err() && self.exited() {
            return Ok(Vec::new());
        }
        result
    }
}
pub(super) fn children(pid: u32) -> Result<Vec<u32>> {
    let s = io(fs::read_to_string(format!("/proc/{pid}/task/{pid}/children")))?;
    if s.len() > 8192 {
        return Err("descendant discovery bound".into());
    }
    s.split_whitespace().map(|s| s.parse().map_err(|_| "invalid descendant".into())).collect()
}
pub(super) fn probe() -> Result<()> {
    let mut subreaper = 0i32;
    // Read-only probe; setting subreaper is reserved for the dedicated helper.
    if unsafe { libc::prctl(libc::PR_GET_CHILD_SUBREAPER, &mut subreaper, 0, 0, 0) } < 0 {
        return Err("subreaper unavailable".into());
    }
    let own = std::process::id();
    let parent = stat(own)?.0;
    let c = Context {
        provider: "probe".into(),
        run: "probe".into(),
        nonce: random()?,
        allocation: random()?,
    };
    let record = Owned::acquire(own, parent, &c)?;
    record.signal(&c, 0)?;
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let r = unsafe {
        libc::waitid(
            libc::P_PIDFD,
            record.handle.as_raw_fd() as u32,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if r >= 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ECHILD) {
        return Err("stable child wait unavailable".into());
    }
    // Probe without modifying existing descriptors. Kernel rejects unsupported flags even for an empty high range.
    if unsafe {
        libc::syscall(libc::SYS_close_range, u32::MAX, u32::MAX, libc::CLOSE_RANGE_CLOEXEC)
    } < 0
    {
        return Err("descriptor hygiene unavailable".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, BufReader, Write},
        process::{Command, Stdio},
    };
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    #[test]
    fn reaped_proc_stat_descriptor_is_departure_but_other_read_errors_fail_closed() {
        let mut child = ChildGuard(
            Command::new("/bin/cat").stdin(Stdio::piped()).stdout(Stdio::null()).spawn().unwrap(),
        );
        let mut descriptor = File::open(format!("/proc/{}/stat", child.0.id())).unwrap();
        assert_eq!(stat_if_present(child.0.id()).unwrap().unwrap().0, std::process::id());
        // Hold the proc descriptor across reaping to exercise the real kernel
        // error, without relying on scheduler timing during read_to_string.
        drop(child.0.stdin.take());
        assert!(child.0.wait().unwrap().success());
        let mut contents = String::new();
        let read = descriptor.read_to_string(&mut contents).map(|_| contents);
        assert_eq!(read.as_ref().unwrap_err().raw_os_error(), Some(libc::ESRCH));
        assert!(stat_from_read(read).unwrap().is_none());
        assert!(stat_from_read(Err(std::io::Error::from_raw_os_error(libc::ENOENT)))
            .unwrap()
            .is_none());
        for errno in [libc::EACCES, libc::EIO, libc::EMFILE] {
            assert!(stat_from_read(Err(std::io::Error::from_raw_os_error(errno))).is_err());
        }
        assert!(stat_from_read(Ok("malformed identity".into())).is_err());
    }
    #[test]
    fn acquisition_reparenting_and_departure_are_rediscovered_without_relaxing_identity() {
        const CHILD: &str = "WI_TEST_ACQUISITION_BARRIER";
        if std::env::var_os(CHILD).is_none() {
            let mut process = ChildGuard(Command::new(std::env::current_exe().unwrap()).args(["--exact", "worker_process::ownership::tests::acquisition_reparenting_and_departure_are_rediscovered_without_relaxing_identity", "--test-threads=1"]).env(CHILD, "1").spawn().unwrap());
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            loop {
                if let Some(status) = process.0.try_wait().unwrap() {
                    assert!(status.success());
                    return;
                }
                assert!(std::time::Instant::now() < deadline, "acquisition regression deadline");
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
        assert_eq!(unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) }, 0);
        let c = Context {
            provider: "p".into(),
            run: "r".into(),
            nonce: random().unwrap(),
            allocation: random().unwrap(),
        };
        let root = std::process::id();
        for during in [false, true] {
            let mut parent = ChildGuard(
                Command::new("/bin/sh")
                    .args(["-c", "sleep 30 & echo $!; read value"])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap(),
            );
            let mut line = String::new();
            BufReader::new(parent.0.stdout.as_mut().unwrap()).read_line(&mut line).unwrap();
            let pid: u32 = line.trim().parse().unwrap();
            let parent_pid = parent.0.id();
            let mut depart = || {
                drop(parent.0.stdin.take());
                let _ = parent.0.wait().unwrap();
                assert_eq!(stat(pid).unwrap().0, root);
            };
            if !during {
                depart();
            }
            let acquired = Owned::acquire_with_barrier(pid, parent_pid, Some(root), &c, || {
                if during {
                    depart();
                }
            })
            .unwrap();
            assert!(matches!(acquired, Acquisition::Rediscover));
            let owned = Owned::acquire(pid, root, &c).unwrap();
            assert!(Owned::acquire_descendant(pid, root + 1, None, &c).is_err());
            owned.signal(&c, libc::SIGKILL).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while !owned.exited() {
                assert!(std::time::Instant::now() < deadline);
                std::thread::yield_now();
            }
            assert!(owned.reap(&c).unwrap());
        }
        let mut child = ChildGuard(
            Command::new("/bin/cat").stdin(Stdio::piped()).stdout(Stdio::null()).spawn().unwrap(),
        );
        let result = Owned::acquire_with_barrier(child.0.id(), root, Some(root), &c, || {
            drop(child.0.stdin.take());
            child.0.wait().unwrap();
        })
        .unwrap();
        assert!(matches!(result, Acquisition::Departed));
    }
    #[test]
    fn recycled_pid_simulation_never_adopts_or_signals_unrelated_child() {
        // Deterministic substitution of a live unrelated process, not forced kernel PID wraparound.
        let mut child = ChildGuard(
            Command::new("/bin/cat").stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap(),
        );
        let mut other = ChildGuard(
            Command::new("/bin/cat").stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap(),
        );
        let c = Context {
            provider: "p".into(),
            run: "r".into(),
            nonce: random().unwrap(),
            allocation: random().unwrap(),
        };
        for field in ["boot", "birth", "nonce", "provider", "run", "allocation", "handle"] {
            let mut owned = Owned::acquire(child.0.id(), std::process::id(), &c).unwrap();
            match field {
                "boot" => owned.boot.push('x'),
                "birth" => owned.birth += 1,
                "nonce" => owned.nonce.push('x'),
                "provider" => owned.provider.push('x'),
                "run" => owned.run.push('x'),
                "allocation" => owned.allocation.push('x'),
                _ => {
                    let other_owned = Owned::acquire(other.0.id(), std::process::id(), &c).unwrap();
                    owned.handle = other_owned.handle.try_clone().unwrap();
                    // PID, boot and birth remain valid: only the descriptor target differs.
                    assert_eq!(owned.validate(&c).unwrap_err(), "process handle mismatch");
                }
            }
            assert!(owned.validate(&c).is_err());
            assert!(owned.signal(&c, libc::SIGKILL).is_err());
            assert!(owned.reap(&c).is_err());
            for child in [&mut child, &mut other] {
                child.0.stdin.as_mut().unwrap().write_all(b"alive\n").unwrap();
                let mut pipe = libc::pollfd {
                    fd: child.0.stdout.as_ref().unwrap().as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                assert!(
                    unsafe { libc::poll(&mut pipe, 1, 2000) } > 0,
                    "unrelated child stopped responding"
                );
                let mut response = String::new();
                BufReader::new(child.0.stdout.as_mut().unwrap()).read_line(&mut response).unwrap();
                assert_eq!(response, "alive\n");
            }
        }
    }
}
