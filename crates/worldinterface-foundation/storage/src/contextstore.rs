//! Immutable exact-byte content and scoped availability. A reference is not a grant.
use crate::{
    invalid,
    root::{sync_dir, write_new},
    Result, StoreKind, StoreRoot,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::fs;
use worldinterface_protocol::{ArtifactRef, ContentHash, InstanceId, OpaqueRef};
const MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
impl StoreRoot {
    pub fn publish(&self, scope: OpaqueRef, bytes: &[u8]) -> Result<ArtifactRef> {
        self.publish_inner(scope, bytes, false)
    }
    fn publish_inner(
        &self,
        scope: OpaqueRef,
        bytes: &[u8],
        interrupt: bool,
    ) -> Result<ArtifactRef> {
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
        let c = self.connect(StoreKind::Context)?;
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
