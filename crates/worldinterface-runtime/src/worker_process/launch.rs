//! The child-side pre_exec closure only invokes async-signal-safe Linux syscalls.
use super::{io, Result};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::{fs::OpenOptionsExt, net::UnixStream, process::CommandExt},
    },
    path::Path,
    process::{Child, Command, Stdio},
};
use worldinterface_protocol::ContentHash;
pub(super) struct Executable {
    file: File,
    pub digest: ContentHash,
}
impl Executable {
    pub fn review(path: &Path, expected: &ContentHash) -> Result<Self> {
        let bytes = read_reviewed(path)?;
        let digest = ContentHash::of(&bytes);
        if bytes.len() > 64 * 1024 * 1024 || !bytes.starts_with(b"\x7fELF") || &digest != expected {
            return Err("reviewed executable digest mismatch".into());
        }
        // SAFETY: static nul-terminated name; owned fd, sealed before sharing with another process.
        let fd = unsafe {
            libc::memfd_create(c"wi-reviewed".as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        if fd < 0 {
            return Err("immutable executable unavailable".into());
        }
        let mut file = unsafe { File::from_raw_fd(fd) };
        io(file.write_all(&bytes))?;
        io(file.seek(SeekFrom::Start(0)))?;
        if unsafe {
            libc::fcntl(
                fd,
                libc::F_ADD_SEALS,
                libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL,
            )
        } < 0
        {
            return Err("executable sealing failed".into());
        }
        Ok(Self { file, digest })
    }
    fn high_fd(&self) -> Result<File> {
        duplicate(self.file.as_raw_fd())
    }
}
pub(super) fn read_reviewed(path: &Path) -> Result<Vec<u8>> {
    let mut source = io(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path))?;
    let meta = io(source.metadata())?;
    if !meta.is_file() || meta.len() > 64 * 1024 * 1024 {
        return Err("invalid reviewed executable".into());
    }
    let mut bytes = Vec::new();
    io((&mut source).take(64 * 1024 * 1024 + 1).read_to_end(&mut bytes))?;
    if bytes.len() > 64 * 1024 * 1024 || !bytes.starts_with(b"\x7fELF") {
        return Err("invalid reviewed executable bytes".into());
    }
    Ok(bytes)
}
fn duplicate(fd: RawFd) -> Result<File> {
    let result = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 10) };
    if result < 0 {
        return Err("descriptor duplication failed".into());
    }
    Ok(unsafe { File::from_raw_fd(result) })
}
/// Mark the entire range CLOEXEC, including Rust's private exec-error pipe. Only explicit
/// capabilities survive successful exec; keeping that error pipe open until exec preserves spawn errors.
unsafe fn descriptors(bootstrap: RawFd, executable: Option<RawFd>) -> std::io::Result<()> {
    if libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, libc::CLOSE_RANGE_CLOEXEC) < 0
        || libc::dup2(bootstrap, 3) < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    if let Some(fd) = executable {
        if libc::dup2(fd, 4) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}
pub(super) fn helper(
    executable: &Executable,
    fixture: &Executable,
    socket: &UnixStream,
    workspace: &Path,
    journal: &File,
) -> Result<Child> {
    let exe = executable.high_fd()?;
    let fixture = fixture.high_fd()?;
    let socket = duplicate(socket.as_raw_fd())?;
    let mut cmd = Command::new(format!("/proc/self/fd/{}", exe.as_raw_fd()));
    cmd.env_clear()
        .env("WI_PROFILE", "process-managed-proof-1")
        .current_dir(workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let journal = duplicate(journal.as_raw_fd())?;
    let (s, f, j) = (socket.as_raw_fd(), fixture.as_raw_fd(), journal.as_raw_fd());
    // SAFETY: closure contains only close_range/dup2 and errno handling; it allocates no memory.
    unsafe {
        cmd.pre_exec(move || {
            descriptors(s, Some(f))?;
            if libc::dup2(j, 5) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    io(cmd.spawn())
}
pub(super) fn fixture(socket: &UnixStream, task: &super::admission::Task) -> Result<Child> {
    let socket = duplicate(socket.as_raw_fd())?;
    // fd 4 was supplied by the parent from a sealed, reviewed executable.
    let exe = duplicate(4)?;
    let mut cmd = Command::new(format!("/proc/self/fd/{}", exe.as_raw_fd()));
    cmd.args(task.args())
        .env_clear()
        .env("WI_PROFILE", "process-managed-proof-1")
        .current_dir("work")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let s = socket.as_raw_fd();
    unsafe {
        cmd.pre_exec(move || descriptors(s, None));
    }
    io(cmd.spawn())
}
pub(super) fn nonblocking(fd: RawFd) -> Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err("nonblocking descriptor failed".into());
    }
    Ok(())
}
/// Probe each launch layer before it opens any additional runtime descriptors.
pub(super) fn environment_and_descriptors_clean(last_allowed: RawFd) -> Result<bool> {
    let env = std::env::vars_os().collect::<Vec<_>>();
    // Close the directory iterator before checking its transient descriptor.
    let descriptors = io(std::fs::read_dir("/proc/self/fd"))?
        .map(|entry| {
            io(entry)?
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<RawFd>().ok())
                .ok_or_else(|| "invalid descriptor entry".to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    let extra = descriptors
        .into_iter()
        .any(|fd| fd > last_allowed && unsafe { libc::fcntl(fd, libc::F_GETFD) } >= 0);
    Ok(env.len() == 1
        && env[0].0 == "WI_PROFILE"
        && env[0].1 == "process-managed-proof-1"
        && !extra)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reviewed_bytes_are_sealed_and_symlink_executables_are_rejected() {
        let mut source = tempfile::NamedTempFile::new().unwrap();
        let bytes = b"\x7fELFreviewed-test-bytes";
        source.write_all(bytes).unwrap();
        let expected = ContentHash::of(bytes);
        let mut executable = Executable::review(source.path(), &expected).unwrap();
        assert!(executable.file.write_all(b"replacement").is_err());
        executable.file.seek(SeekFrom::Start(0)).unwrap();
        let mut observed = Vec::new();
        executable.file.read_to_end(&mut observed).unwrap();
        assert_eq!(observed, bytes);
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(source.path(), &link).unwrap();
        assert!(Executable::review(&link, &expected).is_err());
    }
}
