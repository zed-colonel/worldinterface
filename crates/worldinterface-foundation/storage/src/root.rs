use crate::{invalid, repository::SCHEMA, Result};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use worldinterface_protocol::{
    canonical_data, strict_json, InstanceId, Timestamp, CANONICAL_PROFILE, CONTRACT,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreKind {
    Fabric,
    Flow,
    Context,
    Platform,
}
impl StoreKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Fabric => "fabric",
            Self::Flow => "flow",
            Self::Context => "context",
            Self::Platform => "platform",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreProfile {
    pub instance: InstanceId,
    pub created_at: Timestamp,
    pub platform: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    contract: String,
    schema: u32,
    canonical_profile: String,
    feature_profile: String,
    profile: StoreProfile,
    stores: Vec<StoreKind>,
    complete: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreManifest {
    root: Manifest,
    kind: StoreKind,
}

pub struct StoreRoot {
    pub(crate) path: PathBuf,
    manifest: Manifest,
    _lock: File,
    pub(crate) exclusive: bool,
}
fn kinds(platform: bool) -> Vec<StoreKind> {
    let mut v = vec![StoreKind::Fabric, StoreKind::Flow, StoreKind::Context];
    if platform {
        v.push(StoreKind::Platform);
    }
    v
}
fn parent_dir(path: &Path) -> Option<&Path> {
    // A single relative component has an empty parent, which denotes the current directory.
    path.parent().map(|parent| if parent.as_os_str().is_empty() { Path::new(".") } else { parent })
}
/// Atomically publishes a completed directory without replacing any destination.
///
/// The staging directory must share the destination filesystem. Unsupported
/// no-replace operations fail closed; there is no check-then-rename fallback.
/// Callers must sync the staged contents before this call and the parent after it.
pub fn publish_directory(staging: &Path, destination: &Path) -> Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staging,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    Ok(())
}
pub(crate) fn sync_dir(p: &Path) -> Result<()> {
    File::open(p)?.sync_all()?;
    Ok(())
}
pub(crate) fn write_new(p: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new().write(true).create_new(true).open(p)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
fn read_manifest<T: serde::de::DeserializeOwned>(p: &Path) -> Result<T> {
    let v = strict_json(&fs::read(p)?).map_err(invalid)?;
    Ok(serde_json::from_value(v)?)
}
fn reject_links(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(invalid("symlinks are not store members"));
    }
    if path.is_dir() {
        for e in fs::read_dir(path)? {
            reject_links(&e?.path())?;
        }
    }
    Ok(())
}
fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    for e in fs::read_dir(src)? {
        let e = e?;
        let target = dst.join(e.file_name());
        if e.file_type()?.is_dir() {
            fs::create_dir(&target)?;
            copy_dir(&e.path(), &target)?;
        } else if e.file_type()?.is_file() {
            write_new(&target, &fs::read(e.path())?)?;
        } else {
            return Err(invalid("non-regular store member"));
        }
    }
    sync_dir(dst)
}
/// Inspect the SQLite file format before giving SQLite a descriptor for the
/// original store. Even a read-only SQLite open can create WAL/SHM sidecars
/// when the database header selects WAL mode.
fn require_rollback_mode(path: &Path) -> Result<()> {
    let mut header = [0u8; 20];
    File::open(path)?.read_exact(&mut header)?;
    if &header[..16] != b"SQLite format 3\0" || header[18] != 1 || header[19] != 1 {
        return Err(invalid("unsupported SQLite database header or journal mode"));
    }
    Ok(())
}
impl StoreRoot {
    pub fn create_empty(path: impl AsRef<Path>, profile: StoreProfile) -> Result<Self> {
        Self::initialize(path.as_ref(), profile, None)
    }
    fn initialize(
        path: &Path,
        profile: StoreProfile,
        interrupt_after: Option<usize>,
    ) -> Result<Self> {
        if path.exists() {
            if fs::symlink_metadata(path)?.file_type().is_symlink()
                || !path.is_dir()
                || fs::read_dir(path)?.next().is_some()
            {
                return Err(invalid("fresh store requires empty directory"));
            }
        } else {
            fs::create_dir(path)?;
        }
        // A create-new marker elects one initializer. Incomplete roots are never resumed implicitly.
        write_new(&path.join("initializing"), b"WI-FABRIC-2")?;
        let manifest = Manifest {
            contract: CONTRACT.into(),
            schema: 1,
            canonical_profile: CANONICAL_PROFILE.into(),
            feature_profile: "local-control-plane-14".into(),
            stores: kinds(profile.platform),
            profile,
            complete: true,
        };
        write_new(&path.join("lock"), b"")?;
        write_new(&path.join("flow-dispatch.lock"), b"")?;
        fs::create_dir(path.join("artifacts"))?;
        for (i, kind) in manifest.stores.iter().enumerate() {
            let identity = canonical_data(&StoreManifest { root: manifest.clone(), kind: *kind })
                .map_err(invalid)?;
            let conn = Connection::open(path.join(format!("{}.sqlite", kind.name())))?;
            conn.execute_batch(SCHEMA)?;
            conn.execute_batch(crate::ingress::SCHEMA)?;
            conn.execute_batch(crate::effect::SCHEMA)?;
            conn.execute_batch(crate::flow::SCHEMA)?;
            conn.execute_batch(crate::continuation::SCHEMA)?;
            conn.execute_batch(crate::worker::SCHEMA)?;
            conn.execute_batch(crate::control_plane::SCHEMA)?;
            conn.execute("INSERT INTO store_identity VALUES(1,?1)", [&identity])?;
            drop(conn);
            File::open(path.join(format!("{}.sqlite", kind.name())))?.sync_all()?;
            write_new(&path.join(format!("{}.json", kind.name())), &identity)?;
            #[cfg(test)]
            crate::crash::checkpoint("initialization-store");
            #[cfg(all(feature = "crash-points", not(test)))]
            crate::crashpoint::checkpoint("initialization-store");
            if interrupt_after == Some(i) {
                return Err(invalid("injected initialization interruption"));
            }
        }
        sync_dir(path)?;
        fs::remove_file(path.join("initializing"))?;
        // Root manifest is the completion marker and is published only after all stores.
        write_new(&path.join("manifest.pending"), &canonical_data(&manifest).map_err(invalid)?)?;
        #[cfg(test)]
        crate::crash::checkpoint("initialization-publish");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("initialization-publish");
        fs::rename(path.join("manifest.pending"), path.join("manifest.json"))?;
        sync_dir(path)?;
        if let Some(parent) = parent_dir(path) {
            sync_dir(parent)?;
        }
        Self::open_existing(path)
    }
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(path.as_ref(), false)
    }
    /// Holds exclusive maintenance ownership until dropped. No repository writer can coexist.
    pub fn open_maintenance(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(path.as_ref(), true)
    }
    /// Enforces a containing bundle's identity before any original recovery writes.
    pub fn open_for_instance(
        path: impl AsRef<Path>,
        instance: &InstanceId,
        maintenance: bool,
    ) -> Result<Self> {
        Self::open_checked(path.as_ref(), maintenance, Some(instance))
    }
    fn open(path: &Path, exclusive: bool) -> Result<Self> {
        Self::open_checked(path, exclusive, None)
    }
    fn open_checked(path: &Path, exclusive: bool, instance: Option<&InstanceId>) -> Result<Self> {
        reject_links(path)?;
        let manifest: Manifest = read_manifest(&path.join("manifest.json"))?;
        if instance.is_some_and(|id| id != &manifest.profile.instance)
            || manifest.contract != CONTRACT
            || manifest.schema != 1
            || manifest.canonical_profile != CANONICAL_PROFILE
            || manifest.feature_profile != "local-control-plane-14"
            || !manifest.complete
            || manifest.stores != kinds(manifest.profile.platform)
            || path.join("initializing").exists()
        {
            return Err(invalid("unsupported or incomplete manifest"));
        }
        // Validate EVERY manifest and file before opening ANY SQLite database.
        for kind in &manifest.stores {
            let store: StoreManifest = read_manifest(&path.join(format!("{}.json", kind.name())))?;
            if store.root != manifest
                || store.kind != *kind
                || !path.join(format!("{}.sqlite", kind.name())).is_file()
            {
                return Err(invalid("inconsistent store manifest"));
            }
        }
        let allowed: Vec<_> = [
            "manifest.json".to_string(),
            "lock".into(),
            "flow-dispatch.lock".into(),
            "artifacts".into(),
        ]
        .into_iter()
        .chain(manifest.stores.iter().flat_map(|k| {
            [
                format!("{}.json", k.name()),
                format!("{}.sqlite", k.name()),
                format!("{}.sqlite-journal", k.name()),
            ]
        }))
        .collect();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if !allowed.iter().any(|n| entry.file_name() == n.as_str()) {
                return Err(invalid("unexpected or unfinished store member"));
            }
        }
        if !path.join("artifacts").is_dir()
            || !path.join("lock").is_file()
            || !path.join("flow-dispatch.lock").is_file()
        {
            return Err(invalid("incomplete artifact or lock store"));
        }
        let journals: Vec<_> = manifest
            .stores
            .iter()
            .filter_map(|k| {
                let p = path.join(format!("{}.sqlite-journal", k.name()));
                p.exists().then_some(p)
            })
            .collect();
        let lock = File::open(path.join("lock"))?;
        if exclusive || !journals.is_empty() {
            FileExt::try_lock_exclusive(&lock)?;
        } else {
            FileExt::try_lock_shared(&lock)?;
        }
        let root = Self { path: path.to_path_buf(), manifest, _lock: lock, exclusive };
        // Check every source header before SQLite opens any source database,
        // including on maintenance/backup paths. Journal recovery still runs
        // on a private copy first, so a valid hot journal remains recoverable.
        for kind in &root.manifest.stores {
            require_rollback_mode(&root.db_path(*kind))?;
        }
        if !journals.is_empty() {
            // SQLite alone interprets/replays journals. First recover an isolated copy,
            // then validate the entire root (including embedded identities/artifacts).
            // No original SQLite descriptor is opened for mutation before this succeeds.
            let private = tempfile::tempdir()?;
            copy_dir(path, private.path())?;
            for journal in &journals {
                let bytes = fs::read(journal)?;
                // SQLite initially creates an empty journal, then writes a header
                // whose magic stays zero until the rollback pages are durable.
                // A killed creation/header write is a valid non-hot state. Reject
                // foreign magic (and truncated hot headers), but let SQLite decide
                // whether accepted journals need replay in the private copy.
                let prefix = &bytes[..bytes.len().min(8)];
                let zero_magic = prefix.iter().all(|b| *b == 0);
                let hot_magic = prefix == [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];
                if !(zero_magic || hot_magic && bytes.len() >= 512) {
                    return Err(invalid("malformed rollback journal"));
                }
            }
            for kind in &root.manifest.stores {
                let db = private.path().join(format!("{}.sqlite", kind.name()));
                let c = Connection::open_with_flags(&db, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
                c.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get::<_, i64>(0))?;
                drop(c);
                // An uncommitted journal with a zero header is non-hot. SQLite ignores
                // it; removing it in the private copy permits ordinary validation.
                let journal = db.with_extension("sqlite-journal");
                if journal.exists() {
                    fs::remove_file(journal)?;
                }
            }
            drop(Self::open(private.path(), true)?);
            for kind in &root.manifest.stores {
                let c = Connection::open_with_flags(
                    root.db_path(*kind),
                    OpenFlags::SQLITE_OPEN_READ_WRITE,
                )?;
                c.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get::<_, i64>(0))?;
                drop(c);
            }
            for journal in journals {
                if journal.exists() {
                    fs::remove_file(journal)?;
                }
            }
            sync_dir(path)?;
            if !exclusive {
                FileExt::lock_shared(&root._lock)?;
            }
        }
        let databases_started = std::time::Instant::now();
        for kind in &root.manifest.stores {
            let c =
                Connection::open_with_flags(root.db_path(*kind), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            let version: u32 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
            let mode: String = c.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
            let integrity: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
            if version != 1 || mode != "delete" || integrity != "ok" {
                return Err(invalid("unsupported or corrupt database"));
            }
            root.validate_identity(&c, *kind)?;
            crate::repository::validate_database(&c)?;
        }
        if std::env::var_os("WI_RECOVERY_TIMINGS").is_some() {
            eprintln!("WI databases replay/comparison: {:?}", databases_started.elapsed());
        }
        let artifacts_started = std::time::Instant::now();
        root.validate_artifacts()?;
        root.validate_effect_artifacts()?;
        root.validate_receipt_artifacts()?;
        root.validate_flow_artifacts()?;
        root.validate_continuation_artifacts()?;
        if std::env::var_os("WI_RECOVERY_TIMINGS").is_some() {
            eprintln!("WI plans and artifact closure: {:?}", artifacts_started.elapsed());
        }
        crate::proof::validate(&root)?;
        Ok(root)
    }
    pub fn profile(&self) -> &StoreProfile {
        &self.manifest.profile
    }
    pub(crate) fn db_path(&self, kind: StoreKind) -> PathBuf {
        self.path.join(format!("{}.sqlite", kind.name()))
    }
    fn validate_identity(&self, c: &Connection, kind: StoreKind) -> Result<()> {
        let mut query = c.prepare("SELECT singleton,manifest FROM store_identity")?;
        let identities = query
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = canonical_data(&StoreManifest { root: self.manifest.clone(), kind })
            .map_err(invalid)?;
        if identities != vec![(1, expected)] {
            return Err(invalid("database identity differs from store manifest"));
        }
        Ok(())
    }
    pub(crate) fn connect(&self, kind: StoreKind) -> Result<Connection> {
        if self.exclusive {
            return Err(invalid("maintenance handles cannot write"));
        }
        if !self.manifest.stores.contains(&kind) {
            return Err(invalid("store is not enabled"));
        }
        let c = Connection::open_with_flags(self.db_path(kind), OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        c.busy_timeout(std::time::Duration::from_secs(5))?;
        self.validate_identity(&c, kind)?;
        c.execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
        #[cfg(test)]
        crate::crash::prepare(&c);
        Ok(c)
    }
    pub fn backup_offline(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
        let source = Self::open(source.as_ref(), true)?;
        source.backup_to(destination)
    }
    /// Copies while retaining the caller's exclusive maintenance lock.
    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<()> {
        if !self.exclusive {
            return Err(invalid("backup requires exclusive maintenance ownership"));
        }
        let source = self;
        let dest = destination.as_ref();
        if dest.exists() {
            return Err(invalid("backup destination exists"));
        }
        let parent = parent_dir(dest).ok_or_else(|| invalid("backup parent required"))?;
        if parent.canonicalize()?.starts_with(source.path.canonicalize()?) {
            return Err(invalid("backup destination must be outside the source root"));
        }
        let staging = parent.join(format!(".wi-backup-{}", InstanceId::generate().as_str()));
        fs::create_dir(&staging)?;
        let result = (|| {
            copy_dir(&source.path, &staging)?;
            drop(Self::open(&staging, true)?);
            // Publish the complete directory without a visible reservation window.
            #[cfg(test)]
            crate::crash::checkpoint("backup-publish");
            #[cfg(all(feature = "crash-points", not(test)))]
            crate::crashpoint::checkpoint("backup-publish");
            publish_directory(&staging, dest)?;
            #[cfg(test)]
            crate::crash::checkpoint("backup-renamed");
            #[cfg(all(feature = "crash-points", not(test)))]
            crate::crashpoint::checkpoint("backup-renamed");
            sync_dir(parent)
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&staging);
        }
        result
    }
    pub fn restore_offline(backup: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
        Self::backup_offline(backup, destination)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publication_never_replaces_any_existing_destination() {
        for kind in ["empty-directory", "directory", "file", "symlink"] {
            let t = tempfile::tempdir().unwrap();
            let stage = t.path().join("stage");
            fs::create_dir(&stage).unwrap();
            fs::write(stage.join("complete"), b"data").unwrap();
            let dest = t.path().join("destination");
            match kind {
                "empty-directory" => fs::create_dir(&dest).unwrap(),
                "directory" => {
                    fs::create_dir(&dest).unwrap();
                    fs::write(dest.join("unrelated"), b"keep").unwrap();
                }
                "file" => fs::write(&dest, b"keep").unwrap(),
                "symlink" => std::os::unix::fs::symlink(t.path().join("absent"), &dest).unwrap(),
                _ => unreachable!(),
            }
            use std::os::unix::fs::MetadataExt;
            let inode = fs::symlink_metadata(&dest).unwrap().ino();
            assert!(publish_directory(&stage, &dest).is_err(), "{kind}");
            assert_eq!(fs::symlink_metadata(&dest).unwrap().ino(), inode, "{kind}");
            assert_eq!(fs::read(stage.join("complete")).unwrap(), b"data");
            if kind == "directory" {
                assert_eq!(fs::read(dest.join("unrelated")).unwrap(), b"keep");
            }
        }
    }
    #[test]
    fn interrupted_initialization_never_opens() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("root");
        let profile = StoreProfile {
            instance: InstanceId::generate(),
            created_at: "2026-09-19T00:00:00.000000000Z".to_string().try_into().unwrap(),
            platform: true,
        };
        assert!(StoreRoot::initialize(&p, profile, Some(1)).is_err());
        assert!(StoreRoot::open_existing(&p).is_err());
    }
}
