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
        let retained = io(self.exports.metadata())?;
        let linked = io(fs::symlink_metadata(self.dir.path().join("export")))?;
        if !linked.is_dir() || retained.dev() != linked.dev() || retained.ino() != linked.ino() {
            return Err("export directory replaced".into());
        }
        let mut collected = Vec::new();
        let mut total = 0;
        let actual: std::collections::BTreeSet<_> =
            io(fs::read_dir(format!("/proc/self/fd/{}", self.exports.as_raw_fd())))?
                // One excess entry is enough to reject; never enumerate an unbounded tree.
                .take(a.exports.len() + 1)
                .map(|entry| entry.map(|e| e.file_name()))
                .collect::<std::io::Result<_>>()
                .map_err(|_| "export enumeration failed")?;
        let expected: std::collections::BTreeSet<_> =
            a.exports.iter().map(|e| std::ffi::OsString::from(&e.name)).collect();
        if actual != expected {
            return Err("unexpected or missing export entry".into());
        }
        for export in &a.exports {
            let bytes = read_export(&self.exports, export, bound, || {})?;
            total += bytes.len() as u64;
            if total > bound {
                return Err("aggregate export bound".into());
            }
            collected.push((export.reference.clone(), bytes));
        }
        let after: std::collections::BTreeSet<_> =
            io(fs::read_dir(format!("/proc/self/fd/{}", self.exports.as_raw_fd())))?
                // One excess entry is enough to reject; never enumerate an unbounded tree.
                .take(a.exports.len() + 1)
                .map(|entry| entry.map(|e| e.file_name()))
                .collect::<std::io::Result<_>>()
                .map_err(|_| "export enumeration failed")?;
        let linked = io(fs::symlink_metadata(self.dir.path().join("export")))?;
        if after != expected
            || !linked.is_dir()
            || retained.dev() != linked.dev()
            || retained.ino() != linked.ino()
        {
            return Err("export directory changed".into());
        }
        Ok(collected)
    }
    pub fn preserve(self) {
        let _ = self.dir.keep();
    }
}

fn unchanged(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    a.dev() == b.dev()
        && a.ino() == b.ino()
        && a.uid() == b.uid()
        && a.gid() == b.gid()
        && a.mode() == b.mode()
        && a.nlink() == b.nlink()
        && a.len() == b.len()
        && a.mtime() == b.mtime()
        && a.mtime_nsec() == b.mtime_nsec()
        && a.ctime() == b.ctime()
        && a.ctime_nsec() == b.ctime_nsec()
}
fn read_export(
    directory: &File,
    export: &super::Export,
    bound: u64,
    mut after_open: impl FnMut(),
) -> Result<Vec<u8>> {
    let name = std::ffi::CString::new(export.name.as_bytes()).map_err(|_| "invalid export")?;
    let open = |flags| -> Result<File> {
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err("export unavailable or unsafe".into());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    };
    let mut file = open(libc::O_RDONLY | libc::O_NONBLOCK)?;
    let before = io(file.metadata())?;
    let parent = io(directory.metadata())?;
    if !before.is_file()
        || before.nlink() != 1
        || before.uid() != unsafe { libc::geteuid() }
        || before.dev() != parent.dev()
        || before.len() != export.size
        || before.len() > bound
    {
        return Err("unsafe export metadata".into());
    }
    after_open();
    let mut bytes = Vec::new();
    io((&mut file).take(bound + 1).read_to_end(&mut bytes))?;
    let after = io(file.metadata())?;
    let entry = io(open(libc::O_PATH)?.metadata())?;
    if !unchanged(&before, &after)
        || !unchanged(&after, &entry)
        || !entry.is_file()
        || bytes.len() as u64 != export.size
        || worldinterface_protocol::ContentHash::of(&bytes) != export.digest
    {
        return Err("export replacement, integrity or bound".into());
    }
    Ok(bytes)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn export_links_special_files_and_replacement_races_reject() {
        let d = tempfile::tempdir().unwrap();
        let directory = File::open(d.path()).unwrap();
        let path = d.path().join("out");
        let export = super::super::Export {
            reference: "out".into(),
            name: "out".into(),
            digest: worldinterface_protocol::ContentHash::of(b"safe"),
            size: 4,
        };
        fs::write(&path, b"safe").unwrap();
        assert_eq!(read_export(&directory, &export, 4, || {}).unwrap(), b"safe");
        assert!(read_export(&directory, &export, 4, || {
            fs::rename(&path, d.path().join("old")).unwrap();
            fs::write(&path, b"safe").unwrap();
        })
        .is_err());
        assert!(read_export(&directory, &export, 4, || {
            fs::write(&path, b"evil").unwrap();
        })
        .is_err());
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(d.path().join("old"), &path).unwrap();
        assert!(read_export(&directory, &export, 4, || {}).is_err());
        fs::remove_file(&path).unwrap();
        fs::hard_link(d.path().join("old"), &path).unwrap();
        assert!(read_export(&directory, &export, 4, || {}).is_err());
        fs::remove_file(&path).unwrap();
        let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        assert!(read_export(&directory, &export, 4, || {}).is_err());
    }
}
