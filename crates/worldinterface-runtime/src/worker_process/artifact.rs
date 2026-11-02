use super::{
    admission::{AuthorizedRun, Task},
    io, Result,
};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::Path,
};
pub(super) struct Workspace {
    pub dir: tempfile::TempDir,
    exports: File,
    pub allocation: String,
}
impl Workspace {
    pub fn allocate(root: &Path, a: &AuthorizedRun) -> Result<Self> {
        let dir = io(tempfile::Builder::new()
            .prefix("wi-")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(root))?;
        io(fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)))?;
        for name in ["input", "work", "export"] {
            io(fs::create_dir(dir.path().join(name)))?;
            io(fs::set_permissions(dir.path().join(name), fs::Permissions::from_mode(0o700)))?;
        }
        if let Some(input) = a.inputs.first() {
            io(fs::write(dir.path().join("input/bytes"), &input.bytes))?;
        }
        if a.task == Task::Coding {
            let copy = dir.path().join("work/sample-rust/src");
            io(fs::create_dir_all(&copy))?;
            io(fs::write(copy.join("lib.rs"), include_bytes!("assets/sample-rust-lib.txt")))?;
            io(fs::write(
                dir.path().join("work/sample-rust/Cargo.toml"),
                include_bytes!("assets/sample-rust-Cargo.txt"),
            ))?;
        }
        let exports = io(OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(dir.path().join("export")))?;
        Ok(Self { dir, exports, allocation: super::ownership::random()? })
    }
    pub fn collect(&self, a: &AuthorizedRun, bound: u64) -> Result<Vec<(String, Vec<u8>)>> {
        let mut collected = Vec::new();
        let mut total = 0;
        // Open against the retained export directory capability; names are single validated components.
        for export in &a.exports {
            let name =
                std::ffi::CString::new(export.name.as_bytes()).map_err(|_| "invalid export")?;
            let fd = unsafe {
                libc::openat(
                    self.exports.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err("export unavailable or unsafe".into());
            }
            let mut file = unsafe { File::from_raw_fd(fd) };
            let before = io(file.metadata())?;
            if !before.is_file()
                || before.nlink() != 1
                || before.len() != export.size
                || before.len() > bound
            {
                return Err("unsafe export metadata".into());
            }
            let mut bytes = Vec::new();
            io((&mut file).take(bound + 1).read_to_end(&mut bytes))?;
            total += bytes.len() as u64;
            let after = io(file.metadata())?;
            if total > bound
                || before.len() != after.len()
                || before.mtime_nsec() != after.mtime_nsec()
                || before.mtime() != after.mtime()
                || bytes.len() as u64 != export.size
                || worldinterface_protocol::ContentHash::of(&bytes) != export.digest
            {
                return Err("export integrity or bound".into());
            }
            collected.push((export.reference.clone(), bytes));
        }
        Ok(collected)
    }
    pub fn preserve(self) {
        let _ = self.dir.keep();
    }
}
