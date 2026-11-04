//! Private immutable collection snapshots are never inserted into the artifact index.
use crate::{invalid, Result, StoreKind, StoreRoot};
use rusqlite::{params, OptionalExtension};
use worldinterface_domain::{continuation::*, worker::*};
use worldinterface_protocol::*;

pub(crate) fn validate_policy(p: &WorkerArtifactPolicy) -> Result<()> {
    if p.version != 2 || p.inputs.len() > 1 || p.exports.len() > 4 || p.max_bytes > 4 * 1024 * 1024
    {
        return Err(invalid("worker artifact policy bound"));
    }
    let mut refs = std::collections::BTreeSet::new();
    let mut names = std::collections::BTreeSet::new();
    let mut total = 0u64;
    for e in &p.exports {
        if e.reference.as_str() == "wi:manifest"
            || !refs.insert(&e.reference)
            || !names.insert(&e.name)
            || e.name.is_empty()
            || e.name.len() > 64
            || e.name == "."
            || e.name == ".."
            || !e.name.bytes().all(|c| c.is_ascii_alphanumeric() || b".-_".contains(&c))
            || e.size > p.max_bytes
            || e.verifiers.is_empty()
            || e.verifiers.len() > 4
            || e.verifiers.iter().collect::<std::collections::BTreeSet<_>>().len()
                != e.verifiers.len()
        {
            return Err(invalid("unsafe worker export policy"));
        }
        total = total.checked_add(e.size).ok_or_else(|| invalid("export size overflow"))?;
    }
    if total > p.max_bytes || p.inputs.iter().any(|i| i.size > 1024 * 1024) {
        return Err(invalid("worker artifact policy byte bound"));
    }
    Ok(())
}
fn validate_prepared(s: &WorkerState, p: &WorkerPreparedCollection) -> Result<()> {
    let policy = s.artifact_policy.first().ok_or_else(|| invalid("artifact policy absent"))?;
    if p.version != 2
        || p.policy != ContentHash::of(&canonical_data(policy).map_err(invalid)?)
        || s.operations
            .get(&p.operation)
            .is_none_or(|o| o.operation.as_str() != "collect_artifacts")
        || !s.exited
        || p.findings.len() > 16
    {
        return Err(invalid("collection operation or policy mismatch"));
    }
    let expected: Vec<_> = policy
        .exports
        .iter()
        .flat_map(|e| {
            e.verifiers.iter().map(move |v| WorkerVerifierFinding {
                verifier: v.clone(),
                reference: e.reference.clone(),
                digest: e.digest.clone(),
                passed: true,
            })
        })
        .collect();
    if p.findings != expected {
        return Err(invalid("collection verification incomplete"));
    }
    Ok(())
}
pub(crate) fn advance(
    s: &mut WorkerState,
    next: &WorkerCollection,
    events: &[DurableEvent],
) -> Result<()> {
    match next {
        WorkerCollection::Prepared(p)
            if matches!(s.collection, WorkerCollection::Absent) && !s.quarantined =>
        {
            validate_prepared(s, p)?;
            if !events.is_empty() {
                return Err(invalid("preparation cannot advertise exports"));
            }
        }
        WorkerCollection::Published(p)
            if matches!(&s.collection, WorkerCollection::Prepared(old) if old == &p.prepared)
                && !s.quarantined
                && !s.stop_requested =>
        {
            validate_prepared(s, &p.prepared)?;
            let policy = &s.artifact_policy[0];
            if p.exports.len() != policy.exports.len()
                || p.manifest.scope != s.binding.tenant
                || p.manifest.size > 65536
                || events.len() != 1
                || policy.exports.iter().any(|e| {
                    !p.exports.get(&e.reference).is_some_and(|a| {
                        a.scope == s.binding.tenant && a.content == e.digest && a.size == e.size
                    })
                })
            {
                return Err(invalid("published collection scope mismatch"));
            }
            let event = &events[0];
            if event.route.tenant != s.binding.tenant
                || event.route.audience != s.binding.principal
                || event.route.kind != EventKind::WorkerObservation
                || event.route.generation.is_some()
                || event.route.correlation != s.binding.event_correlation().map_err(invalid)?
                || event.revision != s.revision + 1
                || event.digest != p.manifest.content
                || event.artifact.as_ref() != Some(&p.manifest)
            {
                return Err(invalid("collection event binding mismatch"));
            }
        }
        WorkerCollection::Quarantined(r) => {
            let policy =
                s.artifact_policy.first().ok_or_else(|| invalid("artifact policy absent"))?;
            if r.version != 2
                || r.binding != s.binding
                || r.codes.is_empty()
                || r.codes.len() > 16
                || r.findings.len() > 16
                || r.manifests.len() > 1
                || r.policy != ContentHash::of(&canonical_data(policy).map_err(invalid)?)
                || !events.is_empty()
            {
                return Err(invalid("quarantine report binding or bound"));
            }
            s.transition(WorkerFact::Quarantined).map_err(invalid)?;
        }
        _ => return Err(invalid("invalid collection transition")),
    }
    s.collection = next.clone();
    Ok(())
}
fn snapshot_id(b: &WorkerBinding, digest: &ContentHash) -> Result<String> {
    Ok(ContentHash::of(&canonical_data(&("wi-worker-snapshot-2", b, digest)).map_err(invalid)?)
        .as_str()
        .into())
}
impl StoreRoot {
    /// Trusted collector only: call after the complete batch has passed validation.
    pub fn retain_worker_snapshot(&self, b: &WorkerBinding, bytes: &[u8]) -> Result<ContentHash> {
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(invalid("snapshot bound"));
        }
        let digest = ContentHash::of(bytes);
        let id = snapshot_id(b, &digest)?;
        let c = self.connect(StoreKind::Fabric)?;
        c.execute(
            "INSERT OR IGNORE INTO worker_snapshots VALUES(?,?,?)",
            params![id, digest.as_str(), bytes],
        )?;
        if self.read_worker_snapshot(b, &digest)? != bytes {
            return Err(invalid("snapshot conflict"));
        }
        crate::worker_boundary("worker-snapshot-retained");
        Ok(digest)
    }
    pub fn read_worker_snapshot(&self, b: &WorkerBinding, digest: &ContentHash) -> Result<Vec<u8>> {
        let c = self.connect(StoreKind::Fabric)?;
        let bytes: Vec<u8> = c
            .query_row(
                "SELECT bytes FROM worker_snapshots WHERE identity=? AND content=?",
                params![snapshot_id(b, digest)?, digest.as_str()],
                |r| r.get(0),
            )
            .optional()?
            .ok_or_else(|| invalid("snapshot missing"))?;
        if bytes.len() > 4 * 1024 * 1024 || ContentHash::of(&bytes) != *digest {
            return Err(invalid("snapshot corrupt"));
        }
        Ok(bytes)
    }
}
