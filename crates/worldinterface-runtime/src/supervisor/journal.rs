use crate::Result;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
use worldinterface_protocol::{ContentHash, OpaqueRef};
use worldinterface_worker::supervisor::*;
const MAX_RECORDS: usize = 256;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stored {
    checksum: ContentHash,
    record: Record,
}
pub(super) struct Journal {
    root: PathBuf,
    _owner: File,
    records: BTreeMap<OpaqueRef, Record>,
    failed: bool,
}
fn private_file(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path)?;
    if !m.is_file()
        || m.mode() & 0o077 != 0
        || m.uid() != unsafe { libc::geteuid() }
        || m.nlink() != 1
    {
        return Err("supervisor journal file is not private".into());
    }
    Ok(())
}
impl Journal {
    pub fn open(root: &Path) -> Result<Self> {
        // All ancestor symlinks are refused, including a symlink to a guest root.
        let absolute = std::path::absolute(root)?;
        let root = absolute.as_path();
        let mut ancestor = PathBuf::new();
        for component in absolute.components() {
            ancestor.push(component);
            if ancestor.exists() && fs::symlink_metadata(&ancestor)?.file_type().is_symlink() {
                return Err("supervisor root symlink".into());
            }
        }
        if !root.exists() {
            fs::DirBuilder::new().mode(0o700).create(root)?;
            File::open(root.parent().ok_or("supervisor root parent absent")?)?.sync_all()?;
        }
        let meta = fs::symlink_metadata(root)?;
        if !meta.is_dir() || meta.mode() & 0o077 != 0 || meta.uid() != unsafe { libc::geteuid() } {
            return Err("supervisor root is not operator-private".into());
        }
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root.join("owner"))?;
        private_file(&root.join("owner"))?;
        owner.try_lock_exclusive()?;
        let mut records = BTreeMap::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if entry.file_name() == "owner" {
                continue;
            }
            // Interrupted atomic writes are never published. They contain no
            // dispatch authority, and are bounded to one per interrupted write.
            if entry.file_name().to_string_lossy().starts_with(".pending-") {
                private_file(&entry.path())?;
                fs::remove_file(entry.path())?;
                continue;
            }
            private_file(&entry.path())?;
            let mut bytes = Vec::new();
            File::open(entry.path())?.take((MAX_FRAME_BYTES + 1) as u64).read_to_end(&mut bytes)?;
            let stored: Stored = decode(&bytes)?;
            stored.record.command.validate()?;
            if stored.record.version != VERSION
                || stored.checksum != ContentHash::of(&encode(&stored.record)?)
                || stored.record.observation.evidence.len()
                    > stored.record.command.request.max_evidence_bytes as usize
                || entry.file_name().to_str()
                    != Some(&format!(
                        "{}.json",
                        ContentHash::of(stored.record.command.key.as_str().as_bytes()).as_str()
                    ))
                || records.insert(stored.record.command.key.clone(), stored.record).is_some()
                || records.len() > MAX_RECORDS
            {
                return Err("corrupt supervisor journal".into());
            }
        }
        Ok(Self { root: root.to_owned(), _owner: owner, records, failed: false })
    }
    pub fn healthy(&self) -> Result<()> {
        if self.failed {
            return Err("supervisor journal requires recovery after I/O failure".into());
        }
        Ok(())
    }
    pub fn generation_claimed(&self, command: &Command, tenant: &OpaqueRef) -> bool {
        self.records.values().any(|r| {
            &r.tenant == tenant
                && r.command.request.instance == command.request.instance
                && r.command.request.generation == command.request.generation
        })
    }
    pub fn get(&self, key: &OpaqueRef) -> Option<&Record> {
        self.records.get(key)
    }
    pub fn persist(&mut self, record: &Record) -> Result<()> {
        if let Some(old) = self.records.get(&record.command.key) {
            if old.command != record.command
                || old.tenant != record.tenant
                || old.principal != record.principal
                || (old.observation.state.terminal() && old != record)
            {
                return Err("immutable supervisor binding or terminal result".into());
            }
        } else if self.records.len() >= MAX_RECORDS {
            return Err("supervisor journal capacity".into());
        }
        self.healthy()?;
        let result = self.write_record(record);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn write_record(&mut self, record: &Record) -> Result<()> {
        let stored = Stored { checksum: ContentHash::of(&encode(record)?), record: record.clone() };
        let bytes = encode(&stored)?;
        let mut pending = tempfile::Builder::new().prefix(".pending-").tempfile_in(&self.root)?;
        pending.write_all(&bytes)?;
        pending.as_file().sync_all()?;
        let path = self.root.join(format!(
            "{}.json",
            ContentHash::of(record.command.key.as_str().as_bytes()).as_str()
        ));
        pending.persist(path).map_err(|e| e.error)?;
        File::open(&self.root)?.sync_all()?;
        self.records.insert(record.command.key.clone(), record.clone());
        Ok(())
    }
}
