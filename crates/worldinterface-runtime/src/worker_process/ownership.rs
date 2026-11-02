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
    let s = match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
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
impl Owned {
    pub fn acquire(pid: u32, parent: u32, context: &Context) -> Result<Self> {
        Self::acquire_if_present(pid, parent, context)?.ok_or_else(|| "process departed".into())
    }
    pub fn acquire_if_present(pid: u32, parent: u32, context: &Context) -> Result<Option<Self>> {
        let Some((observed_parent, birth)) = stat_if_present(pid)? else {
            return Ok(None);
        };
        if observed_parent != parent {
            return Err("unproven descendant".into());
        }
        // SAFETY: pidfd_open has no pointer arguments; the returned descriptor is owned once.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                return Ok(None);
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
        let validated: Result<()> = (|| {
            record.validate(context)?;
            if stat(pid)?.0 != parent {
                return Err("descendant relationship changed".into());
            }
            Ok(())
        })();
        // A parent may reap its child during registration. Only stable-handle exit
        // evidence makes a failed validation benign; live mismatches remain errors.
        if record.exited() {
            return Ok(None);
        }
        validated?;
        Ok(Some(record))
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
