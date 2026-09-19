use crate::{invalid, repository::SCHEMA, Result};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
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
}
fn kinds(platform: bool) -> Vec<StoreKind> {
    let mut v = vec![StoreKind::Fabric, StoreKind::Flow, StoreKind::Context];
    if platform {
        v.push(StoreKind::Platform);
    }
    v
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
            feature_profile: "local-domain-1".into(),
            stores: kinds(profile.platform),
            profile,
            complete: true,
        };
        write_new(&path.join("lock"), b"")?;
        fs::create_dir(path.join("artifacts"))?;
        for (i, kind) in manifest.stores.iter().enumerate() {
            let identity = canonical_data(&StoreManifest { root: manifest.clone(), kind: *kind })
                .map_err(invalid)?;
            let conn = Connection::open(path.join(format!("{}.sqlite", kind.name())))?;
            conn.execute_batch(SCHEMA)?;
            conn.execute("INSERT INTO store_identity VALUES(1,?1)", [&identity])?;
            drop(conn);
            File::open(path.join(format!("{}.sqlite", kind.name())))?.sync_all()?;
            write_new(&path.join(format!("{}.json", kind.name())), &identity)?;
            if interrupt_after == Some(i) {
                return Err(invalid("injected initialization interruption"));
            }
        }
        sync_dir(path)?;
        fs::remove_file(path.join("initializing"))?;
        // Root manifest is the completion marker and is published only after all stores.
        write_new(&path.join("manifest.pending"), &canonical_data(&manifest).map_err(invalid)?)?;
        fs::rename(path.join("manifest.pending"), path.join("manifest.json"))?;
        sync_dir(path)?;
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
        Self::open_existing(path)
    }
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(path.as_ref(), false)
    }
    fn open(path: &Path, exclusive: bool) -> Result<Self> {
        reject_links(path)?;
        let manifest: Manifest = read_manifest(&path.join("manifest.json"))?;
        if manifest.contract != CONTRACT
            || manifest.schema != 1
            || manifest.canonical_profile != CANONICAL_PROFILE
            || manifest.feature_profile != "local-domain-1"
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
        let allowed: Vec<_> = ["manifest.json".to_string(), "lock".into(), "artifacts".into()]
            .into_iter()
            .chain(
                manifest
                    .stores
                    .iter()
                    .flat_map(|k| [format!("{}.json", k.name()), format!("{}.sqlite", k.name())]),
            )
            .collect();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if !allowed.iter().any(|n| entry.file_name() == n.as_str()) {
                return Err(invalid("unexpected or unfinished store member"));
            }
        }
        if !path.join("artifacts").is_dir() || !path.join("lock").is_file() {
            return Err(invalid("incomplete artifact or lock store"));
        }
        let lock = File::open(path.join("lock"))?;
        if exclusive {
            FileExt::try_lock_exclusive(&lock)?;
        } else {
            FileExt::try_lock_shared(&lock)?;
        }
        let root = Self { path: path.to_path_buf(), manifest, _lock: lock };
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
        root.validate_artifacts()?;
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
        if !self.manifest.stores.contains(&kind) {
            return Err(invalid("store is not enabled"));
        }
        let c = Connection::open_with_flags(self.db_path(kind), OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        c.busy_timeout(std::time::Duration::from_secs(5))?;
        self.validate_identity(&c, kind)?;
        c.execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
        Ok(c)
    }
    pub fn backup_offline(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
        let source = Self::open(source.as_ref(), true)?;
        let dest = destination.as_ref();
        if dest.exists() {
            return Err(invalid("backup destination exists"));
        }
        let parent = dest.parent().ok_or_else(|| invalid("backup parent required"))?;
        if parent.canonicalize()?.starts_with(source.path.canonicalize()?) {
            return Err(invalid("backup destination must be outside the source root"));
        }
        let staging = parent.join(format!(".wi-backup-{}", InstanceId::generate().as_str()));
        fs::create_dir(&staging)?;
        fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
            for e in fs::read_dir(src)? {
                let e = e?;
                let target = dst.join(e.file_name());
                if e.file_type()?.is_dir() {
                    fs::create_dir(&target)?;
                    copy_dir(&e.path(), &target)?;
                } else {
                    write_new(&target, &fs::read(e.path())?)?;
                }
            }
            sync_dir(dst)
        }
        let result = (|| {
            copy_dir(&source.path, &staging)?;
            drop(Self::open(&staging, true)?);
            // create_dir is a no-clobber reservation; rename is only into our empty directory.
            fs::create_dir(dest)?;
            fs::rename(&staging, dest)?;
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
