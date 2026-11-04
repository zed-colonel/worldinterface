//! WI validates the complete batch, retains private bytes, then publishes from that snapshot.
use crate::{worker_process::Export, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use worldinterface_domain::{continuation::*, worker::*};
use worldinterface_protocol::*;
use worldinterface_storage::{worker_boundary, StoreRoot};
use worldinterface_worker::artifacts::{verify_content, ExportManifestV1};

pub fn policy(
    inputs: Vec<ArtifactRef>,
    exports: &[Export],
    max_bytes: u64,
) -> Result<WorkerArtifactPolicy> {
    Ok(WorkerArtifactPolicy {
        version: 2,
        inputs,
        max_bytes,
        exports: exports
            .iter()
            .map(|e| {
                let mut verifiers = vec!["wi:integrity:1".to_string().try_into()?];
                if e.name.ends_with(".json") {
                    verifiers.push("wi:json:1".to_string().try_into()?);
                }
                if e.name.ends_with(".patch") {
                    verifiers.push("wi:utf8:1".to_string().try_into()?);
                }
                Ok(WorkerExportPolicy {
                    reference: e.reference.clone().try_into()?,
                    name: e.name.clone(),
                    digest: e.digest.clone(),
                    size: e.size,
                    verifiers,
                })
            })
            .collect::<Result<_>>()?,
    })
}
fn state(root: &StoreRoot, b: &WorkerBinding) -> Result<WorkerState> {
    let s = root.worker_snapshot(&b.tenant, &b.provider, &b.run)?.ok_or("worker absent")?;
    if s.binding != *b {
        return Err("collection binding mismatch".into());
    }
    Ok(s)
}
pub fn quarantine(
    root: &StoreRoot,
    b: &WorkerBinding,
    operation: &OpaqueRef,
    code: &str,
    findings: Vec<WorkerVerifierFinding>,
) -> Result<()> {
    let manifests = match &state(root, b)?.collection {
        WorkerCollection::Prepared(p) => vec![p.manifest.clone()],
        WorkerCollection::Published(p) => vec![p.prepared.manifest.clone()],
        _ => vec![],
    };
    quarantine_manifest(root, b, operation, code, findings, manifests)
}
fn quarantine_manifest(
    root: &StoreRoot,
    b: &WorkerBinding,
    operation: &OpaqueRef,
    code: &str,
    findings: Vec<WorkerVerifierFinding>,
    manifests: Vec<ContentHash>,
) -> Result<()> {
    let s = state(root, b)?;
    let p = s.artifact_policy.first().ok_or("policy absent")?;
    let report = WorkerQuarantineReport {
        version: 2,
        binding: b.clone(),
        operation: operation.clone(),
        policy: ContentHash::of(&canonical_data(p)?),
        codes: vec![code.to_string().try_into()?],
        manifests,
        findings,
    };
    root.record_worker_collection(b.clone(), WorkerCollection::Quarantined(report), vec![])?;
    Ok(())
}
/// All parsing/verification happens before the first snapshot or normal publication.
pub fn prepare(
    root: &StoreRoot,
    b: &WorkerBinding,
    operation: &OpaqueRef,
    manifest_bytes: &[u8],
    bytes: &BTreeMap<String, Vec<u8>>,
    exit_success: bool,
) -> Result<()> {
    let s = state(root, b)?;
    if !matches!(s.collection, WorkerCollection::Absent) {
        return Err("collection already decided".into());
    }
    let p = s.artifact_policy.first().ok_or("policy absent")?;
    let mut findings = Vec::new();
    let mut rejection = "manifest-decode";
    let validation = (|| -> Result<()> {
        let manifest: ExportManifestV1 = serde_json::from_value(strict_json(manifest_bytes)?)?;
        let scope: Vec<_> = p.exports.iter().map(|e| e.reference.as_str().into()).collect();
        rejection = "manifest-binding-or-path";
        manifest.validate(b.provider.as_str(), b.run.as_str(), b.principal.as_str(), &scope)?;
        rejection = "collection-export-set";
        if manifest.exports.len() != p.exports.len()
            || bytes.len() != p.exports.len()
            || manifest.exports.len() > 4
        {
            return Err("collection set mismatch".into());
        }
        let mut total = 0u64;
        for e in &p.exports {
            let declared = manifest
                .exports
                .iter()
                .find(|d| d.reference == e.reference.as_str())
                .ok_or("required export missing")?;
            let content = bytes.get(e.reference.as_str()).ok_or("export bytes missing")?;
            rejection = "export-policy-drift";
            if declared.relative_name != e.name
                || declared.digest != e.digest
                || declared.size != e.size
            {
                return Err("export policy drift".into());
            }
            if declared
                .required_verifiers
                .iter()
                .chain(&declared.passed_verifiers)
                .any(|v| !e.verifiers.iter().any(|trusted| trusted.as_str() == v))
            {
                return Err("unknown provider verifier claim".into());
            }
            rejection = "export-content-integrity";
            verify_content(declared, content)?;
            total = total.checked_add(content.len() as u64).ok_or("collection overflow")?;
            if total > p.max_bytes || total > 4 * 1024 * 1024 {
                return Err("collection bound".into());
            }
            rejection = "trusted-verifier-failed";
            for verifier in &e.verifiers {
                let passed = match verifier.as_str() {
                    "wi:integrity:1" => {
                        ContentHash::of(content) == e.digest && content.len() as u64 == e.size
                    }
                    "wi:json:1" => strict_json(content).is_ok_and(|v| v.is_object()),
                    "wi:utf8:1" => std::str::from_utf8(content).is_ok(),
                    _ => false,
                };
                findings.push(WorkerVerifierFinding {
                    verifier: verifier.clone(),
                    reference: e.reference.clone(),
                    digest: e.digest.clone(),
                    passed,
                });
                if !passed {
                    return Err("trusted verifier rejected".into());
                }
            }
        }
        Ok(())
    })();
    if validation.is_err() {
        quarantine_manifest(
            root,
            b,
            operation,
            rejection,
            findings,
            vec![ContentHash::of(manifest_bytes)],
        )?;
        return Err("collection quarantined".into());
    }
    let manifest = root.retain_worker_snapshot(b, manifest_bytes)?;
    for content in bytes.values() {
        root.retain_worker_snapshot(b, content)?;
    }
    let prepared = WorkerPreparedCollection {
        version: 2,
        operation: operation.clone(),
        policy: ContentHash::of(&canonical_data(p)?),
        manifest,
        findings,
        exit_success,
    };
    root.record_worker_collection(b.clone(), WorkerCollection::Prepared(prepared), vec![])?;
    Ok(())
}
fn identity(b: &WorkerBinding, p: &WorkerPreparedCollection, reference: &str) -> Result<OpaqueRef> {
    Ok(p.publication_identity(b, reference)?)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedManifest {
    version: u32,
    binding: WorkerBinding,
    prepared: WorkerPreparedCollection,
    exports: BTreeMap<OpaqueRef, ArtifactRef>,
}
/// Storage failures leave Prepared intact. Snapshot failures quarantine without recollection.
pub fn resume(root: &StoreRoot, b: &WorkerBinding) -> Result<()> {
    let s = state(root, b)?;
    let prepared = match &s.collection {
        WorkerCollection::Published(_) => return Ok(()),
        WorkerCollection::Prepared(p) => p.clone(),
        _ => return Err("collection not recoverable".into()),
    };
    if s.quarantined
        || s.stop_requested
        || root.worker_provider_quarantine(&b.tenant, &b.provider)?.is_some()
    {
        quarantine(
            root,
            b,
            &prepared.operation,
            "collection-cancelled",
            prepared.findings.clone(),
        )?;
        return Err("collection cancelled".into());
    }
    let policy = &s.artifact_policy[0];
    // Recheck every private byte before resuming any publication.
    let snapshots = (|| -> Result<_> {
        root.read_worker_snapshot(b, &prepared.manifest)?;
        let mut result = Vec::new();
        for e in &policy.exports {
            let bytes = root.read_worker_snapshot(b, &e.digest)?;
            if bytes.len() as u64 != e.size {
                return Err("snapshot size drift".into());
            }
            result.push((e.reference.clone(), bytes));
        }
        Ok(result)
    })();
    let snapshots = match snapshots {
        Ok(v) => v,
        Err(_) => {
            quarantine(
                root,
                b,
                &prepared.operation,
                "snapshot-unavailable",
                prepared.findings.clone(),
            )?;
            return Err("snapshot quarantined".into());
        }
    };
    let (exports, manifest, id) = root.fence_worker_publication(b, || {
        let mut exports = BTreeMap::new();
        for (reference, bytes) in snapshots {
            let id = identity(b, &prepared, reference.as_str())?;
            root.stage_publication(id.clone(), b.tenant.clone(), &bytes)?;
            exports.insert(reference, root.publish_staged(&id, &bytes)?);
        }
        let bytes = canonical_data(&PublishedManifest {
            version: 2,
            binding: b.clone(),
            prepared: prepared.clone(),
            exports: exports.clone(),
        })?;
        let id = identity(b, &prepared, "wi:manifest")?;
        root.stage_publication(id.clone(), b.tenant.clone(), &bytes)?;
        let manifest = root.publish_staged(&id, &bytes)?;
        worker_boundary("worker-manifest-published");
        Ok((exports, manifest, id))
    })?;
    let current = state(root, b)?;
    let event = DurableEvent::new(
        EventRoute {
            tenant: b.tenant.clone(),
            audience: b.principal.clone(),
            kind: EventKind::WorkerObservation,
            correlation: b.event_correlation()?,
            generation: None,
        },
        id,
        current.revision + 1,
        manifest.content.clone(),
        Some(manifest.clone()),
    )?;
    root.record_worker_collection(
        b.clone(),
        WorkerCollection::Published(WorkerPublishedCollection { prepared, exports, manifest }),
        vec![event],
    )?;
    Ok(())
}
pub fn read(root: &StoreRoot, b: &WorkerBinding, reference: &str) -> Result<Vec<u8>> {
    let s = state(root, b)?;
    // Export disposition is independent of later allocation/cleanup quarantine.
    if let WorkerCollection::Published(p) = s.collection {
        root.read_artifact(
            p.exports.get(&reference.to_string().try_into()?).ok_or("export access denied")?,
        )
    } else {
        Err("collection unpublished".into())
    }
}
