//! Immutable exact-byte content and scoped availability. A reference is not a grant.
#[cfg(test)]
use crate::root::{sync_dir, write_new};
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::fs;
#[cfg(test)]
use worldinterface_protocol::InstanceId;
use worldinterface_protocol::{ArtifactRef, ContentHash, OpaqueRef};
const MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
impl StoreRoot {
    pub fn publish(&self, scope: OpaqueRef, bytes: &[u8]) -> Result<ArtifactRef> {
        let content = ContentHash::of(bytes);
        let identity: OpaqueRef = format!(
            "content:{}:{}",
            ContentHash::of(scope.as_str().as_bytes()).as_str(),
            content.as_str()
        )
        .try_into()
        .map_err(invalid)?;
        self.stage_publication(identity.clone(), scope, bytes)?;
        self.publish_staged(&identity, bytes)
    }
    #[cfg(test)]
    fn publish_inner(
        &self,
        scope: OpaqueRef,
        bytes: &[u8],
        interrupt: bool,
    ) -> Result<ArtifactRef> {
        if self.exclusive {
            return Err(invalid("maintenance handles cannot publish"));
        }
        if bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(invalid("artifact exceeds local profile bound"));
        }
        let content = ContentHash::of(bytes);
        let dir = self.path.join("artifacts");
        let final_path = dir.join(content.as_str());
        let pending = dir.join(format!(".pending-{}", InstanceId::generate().as_str()));
        write_new(&pending, bytes)?;
        match fs::hard_link(&pending, &final_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::read(&final_path)? != bytes {
                    fs::remove_file(&pending)?;
                    return Err(invalid("existing artifact corrupt"));
                }
            }
            Err(e) => {
                let _ = fs::remove_file(&pending);
                return Err(e.into());
            }
        }
        fs::remove_file(&pending)?;
        sync_dir(&dir)?;
        #[cfg(test)]
        crate::crash::checkpoint("artifact-durable");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("artifact-durable");
        if interrupt {
            return Err(invalid("injected after durable bytes before advertisement"));
        }
        let c = self.connect(StoreKind::Context)?;
        c.execute(
            "INSERT INTO artifacts VALUES(?,?,?) ON CONFLICT(scope,content) DO NOTHING",
            params![scope.as_str(), content.as_str(), bytes.len() as u64],
        )?;
        let reference = ArtifactRef { scope, content, size: bytes.len() as u64 };
        self.read_artifact(&reference)?;
        Ok(reference)
    }
    pub fn read_artifact(&self, reference: &ArtifactRef) -> Result<Vec<u8>> {
        let c = Connection::open_with_flags(
            self.db_path(StoreKind::Context),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let size: Option<u64> = c
            .query_row(
                "SELECT size FROM artifacts WHERE scope=? AND content=?",
                params![reference.scope.as_str(), reference.content.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        if size != Some(reference.size) {
            return Err(invalid("artifact not available in scope"));
        }
        let bytes = fs::read(self.path.join("artifacts").join(reference.content.as_str()))?;
        if bytes.len() as u64 != reference.size || ContentHash::of(&bytes) != reference.content {
            return Err(invalid("artifact integrity mismatch"));
        }
        Ok(bytes)
    }
    pub(crate) fn validate_artifacts(&self) -> Result<()> {
        let c = Connection::open_with_flags(
            self.db_path(StoreKind::Context),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let mut q = c.prepare("SELECT scope,content,size FROM artifacts")?;
        let rows = q.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, u64>(2)?))
        })?;
        for row in rows {
            let (scope, content, size) = row?;
            let _: OpaqueRef = scope.try_into().map_err(invalid)?;
            let digest: ContentHash = content.try_into().map_err(invalid)?;
            let bytes = fs::read(self.path.join("artifacts").join(digest.as_str()))?;
            if bytes.len() as u64 != size
                || size > MAX_ARTIFACT_BYTES as u64
                || ContentHash::of(&bytes) != digest
            {
                return Err(invalid("referenced artifact missing or corrupt"));
            }
        }
        let mut staged = c.prepare("SELECT scope,content,size,state FROM publication_index")?;
        let rows = staged.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, u64>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            let (scope, content, size, state) = row?;
            let digest: ContentHash = content.try_into().map_err(invalid)?;
            if state == "durable" || state == "available" {
                let bytes = fs::read(self.path.join("artifacts").join(digest.as_str()))?;
                if bytes.len() as u64 != size || ContentHash::of(&bytes) != digest {
                    return Err(invalid("publication obligation missing content"));
                }
            }
            if state == "available" {
                let reference = ArtifactRef {
                    scope: scope.try_into().map_err(invalid)?,
                    content: digest,
                    size,
                };
                self.read_artifact(&reference)?;
            }
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreProfile;
    #[test]
    fn interrupted_publication_leaves_only_unreferenced_bytes() {
        let t = tempfile::tempdir().unwrap();
        let root = StoreRoot::create_empty(
            t.path().join("root"),
            StoreProfile {
                instance: InstanceId::generate(),
                created_at: "2026-09-19T00:00:00.000000000Z".to_string().try_into().unwrap(),
                platform: false,
            },
        )
        .unwrap();
        let scope: OpaqueRef = "tenant".to_string().try_into().unwrap();
        assert!(root.publish_inner(scope.clone(), b"bytes", true).is_err());
        let reference =
            ArtifactRef { scope: scope.clone(), content: ContentHash::of(b"bytes"), size: 5 };
        assert!(root.read_artifact(&reference).is_err());
        let reference = root.publish(scope, b"bytes").unwrap();
        assert_eq!(root.read_artifact(&reference).unwrap(), b"bytes");
    }
}
