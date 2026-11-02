//! Adapter-private append-only launch evidence, outside the fixture workspace.
//! This is process recovery evidence, never an AQ continuation or same-UID sandbox.
use super::{io, ownership::Owned, AuthorizedRun, CleanupReport, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};
use worldinterface_protocol::{canonical_data, strict_json, ContentHash};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    binding: ContentHash,
    nonce: String,
    stage: String,
    identity: Option<super::ownership::Identity>,
    report: Option<CleanupReport>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    previous: ContentHash,
    entry: Entry,
    digest: ContentHash,
}
pub(super) struct Journal {
    pub file: File,
    binding: ContentHash,
    nonce: String,
}
fn key(provider: &str, run: &str) -> Result<String> {
    Ok(format!(
        ".wi-owner-{}",
        ContentHash::of(&worldinterface_worker::encode(&(provider, run))?).as_str()
    ))
}
fn read(file: &mut File) -> Result<Vec<Frame>> {
    let meta = io(file.metadata())?;
    if !meta.is_file()
        || meta.len() > 64 * 1024
        || meta.mode() & 0o077 != 0
        || meta.uid() != unsafe { libc::geteuid() }
    {
        return Err("private ownership evidence invalid".into());
    }
    let mut bytes = Vec::new();
    io(file.take(64 * 1024 + 1).read_to_end(&mut bytes))?;
    if bytes.last() != Some(&b'\n') {
        return Err("private ownership evidence truncated".into());
    }
    let mut frames = Vec::new();
    let mut previous = ContentHash::of(b"wi-process-journal-1");
    for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let frame: Frame =
            serde_json::from_value(strict_json(line).map_err(|_| "ownership JSON invalid")?)
                .map_err(|_| "ownership frame invalid")?;
        if frame.previous != previous
            || frame.digest
                != ContentHash::of(
                    &canonical_data(&(&frame.previous, &frame.entry))
                        .map_err(|_| "ownership canonical invalid")?,
                )
            || canonical_data(&frame).map_err(|_| "ownership canonical invalid")? != line
        {
            return Err("private ownership digest conflict".into());
        }
        previous = frame.digest.clone();
        frames.push(frame);
    }
    Ok(frames)
}
impl Journal {
    pub fn create(root: &Path, auth: &AuthorizedRun, nonce: &str) -> Result<Self> {
        let path = root.join(key(&auth.controls.provider, &auth.controls.run)?);
        let file = io(OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path))?;
        let binding = ContentHash::of(&worldinterface_worker::encode(&auth.operations)?);
        let journal = Self { file, binding, nonce: nonce.into() };
        journal.append("intent", None, None)?;
        io(File::open(root))?.sync_all().map_err(|_| "ownership directory sync failed")?;
        Ok(journal)
    }
    pub fn from_helper(file: File, nonce: &str) -> Result<Self> {
        use std::os::fd::AsRawFd;
        // Reopen the explicitly passed capability with an independent lock description.
        let reopened = io(OpenOptions::new()
            .read(true)
            .append(true)
            .custom_flags(libc::O_CLOEXEC)
            .open(format!("/proc/self/fd/{}", file.as_raw_fd())))?;
        drop(file);
        let file = reopened;
        io(FileExt::lock_shared(&file))?;
        let mut copy = io(file.try_clone())?;
        use std::io::{Seek, SeekFrom};
        io(copy.seek(SeekFrom::Start(0)))?;
        let frames = read(&mut copy)?;
        let first = frames.first().ok_or("missing ownership intent")?;
        if first.entry.nonce != nonce || first.entry.stage != "intent" {
            return Err("ownership launch binding mismatch".into());
        }
        io(FileExt::unlock(&file))?;
        Ok(Self { file, binding: first.entry.binding.clone(), nonce: nonce.into() })
    }
    pub fn append(
        &self,
        stage: &str,
        owned: Option<&Owned>,
        report: Option<&CleanupReport>,
    ) -> Result<()> {
        io(self.file.lock_exclusive())?;
        struct Unlock<'a>(&'a File);
        impl Drop for Unlock<'_> {
            fn drop(&mut self) {
                let _ = FileExt::unlock(self.0);
            }
        }
        let _unlock = Unlock(&self.file);
        let mut copy = io(self.file.try_clone())?;
        use std::io::{Seek, SeekFrom};
        io(copy.seek(SeekFrom::Start(0)))?;
        let previous = if io(copy.metadata())?.len() == 0 {
            ContentHash::of(b"wi-process-journal-1")
        } else {
            read(&mut copy)?.last().ok_or("ownership history absent")?.digest.clone()
        };
        let entry = Entry {
            binding: self.binding.clone(),
            nonce: self.nonce.clone(),
            stage: stage.into(),
            identity: owned.map(Owned::identity),
            report: report.cloned(),
        };
        let digest = ContentHash::of(
            &canonical_data(&(&previous, &entry)).map_err(|_| "ownership encoding failed")?,
        );
        let mut bytes = canonical_data(&Frame { previous, entry, digest })
            .map_err(|_| "ownership encoding failed")?;
        bytes.push(b'\n');
        if io(self.file.metadata())?.len() + bytes.len() as u64 > 64 * 1024 {
            return Err("ownership journal bound".into());
        }
        io((&self.file).write_all(&bytes))?;
        io(self.file.sync_all())
    }
}
/// Absence, corruption, mismatch or missing cleanup leaves the run quarantined.
/// No process is ever adopted or signalled from this evidence by PID alone.
pub(super) fn recovered_report(
    root: &Path,
    provider: &str,
    run: &str,
    binding: &ContentHash,
) -> Result<Option<CleanupReport>> {
    let path = root.join(key(provider, run)?);
    let mut file = io(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path))?;
    io(FileExt::lock_shared(&file))?;
    let frames = read(&mut file)?;
    let first = frames.first().ok_or("ownership intent missing")?;
    if first.entry.stage != "intent"
        || first.entry.binding != *binding
        || frames.iter().any(|f| f.entry.binding != *binding || f.entry.nonce != first.entry.nonce)
    {
        return Err("ownership realization mismatch".into());
    }
    Ok(frames
        .iter()
        .rev()
        .find(|f| f.entry.stage == "cleanup")
        .and_then(|f| f.entry.report.clone())
        .filter(|r| r.cleanup_confirmed))
}
