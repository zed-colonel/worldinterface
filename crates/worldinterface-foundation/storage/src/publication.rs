//! Context-owned, resumable immutable publication obligations.
use crate::{
    invalid,
    root::{sync_dir, write_new},
    Result, StoreKind, StoreRoot,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::fs;
use worldinterface_domain::publication::{Publication, PublicationState};
use worldinterface_protocol::{
    canonical_data, strict_json, ArtifactRef, ContentHash, InstanceId, OpaqueRef,
};

fn load(c: &Connection, identity: &OpaqueRef) -> Result<Option<Publication>> {
    c.query_row(
        "SELECT scope,content,size,state FROM publication_index WHERE identity=?",
        [identity.as_str()],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, u64>(2)?,
                r.get::<_, String>(3)?,
            ))
        },
    )
    .optional()?
    .map(|(scope, content, size, state)| {
        Ok(Publication {
            identity: identity.clone(),
            artifact: ArtifactRef {
                scope: scope.try_into().map_err(invalid)?,
                content: content.try_into().map_err(invalid)?,
                size,
            },
            state: match state.as_str() {
                "identified" => PublicationState::Identified,
                "durable" => PublicationState::Durable,
                "available" => PublicationState::Available,
                _ => return Err(invalid("invalid publication state")),
            },
        })
    })
    .transpose()
}
fn append(c: &Connection, publication: &Publication) -> Result<()> {
    check_transition(c, publication)?;
    let bytes = canonical_data(publication).map_err(invalid)?;
    c.execute(
        "INSERT INTO publication_history(identity,entry,digest) VALUES(?,?,?)",
        params![publication.identity.as_str(), bytes, ContentHash::of(&bytes).as_str()],
    )?;
    update_index(c, publication)
}
fn check_transition(c: &Connection, publication: &Publication) -> Result<()> {
    let previous = load(c, &publication.identity)?;
    match previous {
        Some(ref old) if old == publication => {
            return Err(invalid("duplicate publication journal transition"))
        }
        Some(ref old) if old.advance(publication.state.clone()).as_ref() != Ok(publication) => {
            return Err(invalid("publication identity or transition conflict"))
        }
        None if publication.state != PublicationState::Identified => {
            return Err(invalid("missing publication identity"))
        }
        _ => {}
    }
    Ok(())
}
fn update_index(c: &Connection, publication: &Publication) -> Result<()> {
    let state = match publication.state {
        PublicationState::Identified => "identified",
        PublicationState::Durable => "durable",
        PublicationState::Available => "available",
    };
    c.execute("INSERT INTO publication_index VALUES(?,?,?,?,?) ON CONFLICT(identity) DO UPDATE SET state=excluded.state", params![publication.identity.as_str(), publication.artifact.scope.as_str(), publication.artifact.content.as_str(), publication.artifact.size, state])?;
    Ok(())
}
impl StoreRoot {
    pub fn stage_publication(
        &self,
        identity: OpaqueRef,
        scope: OpaqueRef,
        bytes: &[u8],
    ) -> Result<Publication> {
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(invalid("artifact exceeds local profile bound"));
        }
        let publication = Publication {
            identity,
            artifact: ArtifactRef {
                scope,
                content: ContentHash::of(bytes),
                size: bytes.len() as u64,
            },
            state: PublicationState::Identified,
        };
        let mut c = self.connect(StoreKind::Context)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(old) = load(&tx, &publication.identity)? {
            if old.artifact != publication.artifact {
                return Err(invalid("conflicting publication identity"));
            }
            return Ok(old);
        }
        append(&tx, &publication)?;
        tx.commit()?;
        #[cfg(test)]
        crate::crash::checkpoint("publication-identified");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("publication-identified");
        Ok(publication)
    }
    pub fn publication(&self, identity: &OpaqueRef) -> Result<Option<Publication>> {
        load(&self.connect(StoreKind::Context)?, identity)
    }
    pub fn publish_staged(&self, identity: &OpaqueRef, bytes: &[u8]) -> Result<ArtifactRef> {
        let current =
            self.publication(identity)?.ok_or_else(|| invalid("unknown publication identity"))?;
        if current.artifact.size != bytes.len() as u64
            || current.artifact.content != ContentHash::of(bytes)
        {
            return Err(invalid("publication bytes differ from identity"));
        }
        if current.state == PublicationState::Available {
            self.read_artifact(&current.artifact)?;
            return Ok(current.artifact);
        }
        let dir = self.path.join("artifacts");
        let final_path = dir.join(current.artifact.content.as_str());
        let pending = dir.join(format!(".pending-{}", InstanceId::generate().as_str()));
        write_new(&pending, bytes)?;
        match fs::hard_link(&pending, &final_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::read(&final_path)? != bytes {
                    fs::remove_file(&pending)?;
                    return Err(invalid("existing content corrupt"));
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
        #[cfg(test)]
        crate::crash::checkpoint("publication-durable");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("publication-durable");
        let durable = if current.state == PublicationState::Identified {
            let next = current.advance(PublicationState::Durable).map_err(invalid)?;
            let mut c = self.connect(StoreKind::Context)?;
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            append(&tx, &next)?;
            tx.commit()?;
            next
        } else {
            current
        };
        #[cfg(test)]
        crate::crash::checkpoint("publication-availability");
        #[cfg(all(feature = "crash-points", not(test)))]
        crate::crashpoint::checkpoint("publication-availability");
        let available = durable.advance(PublicationState::Available).map_err(invalid)?;
        let mut c = self.connect(StoreKind::Context)?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        append(&tx, &available)?;
        tx.execute(
            "INSERT INTO artifacts VALUES(?,?,?) ON CONFLICT(scope,content) DO NOTHING",
            params![
                available.artifact.scope.as_str(),
                available.artifact.content.as_str(),
                available.artifact.size
            ],
        )?;
        tx.commit()?;
        self.read_artifact(&available.artifact)?;
        Ok(available.artifact)
    }
}
pub(crate) fn replay(source: &Connection, target: &Connection) -> Result<()> {
    let mut q =
        source.prepare("SELECT identity,entry,digest FROM publication_history ORDER BY seq")?;
    let rows = q.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, String>(2)?))
    })?;
    for row in rows {
        let (identity, bytes, digest) = row?;
        if ContentHash::of(&bytes).as_str() != digest {
            return Err(invalid("publication journal digest"));
        }
        let publication: Publication =
            serde_json::from_value(strict_json(&bytes).map_err(invalid)?)?;
        if canonical_data(&publication).map_err(invalid)? != bytes
            || publication.identity.as_str() != identity
        {
            return Err(invalid("publication journal encoding"));
        }
        check_transition(target, &publication)?;
        update_index(target, &publication)?;
    }
    Ok(())
}
