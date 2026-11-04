//! Non-live collection/security checks. No provider, approval or executable is constructed.
#[path = "support/worker_collection.rs"]
mod fixture;
use fixture::Collection;
use worldinterface_domain::worker::*;
use worldinterface_protocol::*;
use worldinterface_runtime::worker::artifacts;
use worldinterface_storage::{StoreKind, StoreRoot};
#[test]
fn wp_028_invalid_batches_quarantine_before_any_publication_and_survive_reopen() {
    for attack in 0..16 {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("store");
        let mut c = Collection::new(&path);
        let mut raw = None;
        match attack {
            0 => c.manifest.owner = PrincipalId::generate().as_str().into(),
            1 => c.manifest.run = "other".into(),
            2 => c.manifest.provider = "other".into(),
            3 => {
                c.manifest.exports.pop();
            }
            4 => c.manifest.exports.push(c.manifest.exports[0].clone()),
            5 => c.manifest.exports[0].relative_name = "../out".into(),
            6 => c.manifest.exports[0].relative_name = c.manifest.exports[1].relative_name.clone(),
            7 => c.manifest.exports[0].digest = ContentHash::of(b"wrong"),
            8 => c.manifest.exports[0].size += 1,
            9 => {
                c.bytes.insert("report".into(), b"invalid-json".to_vec());
            }
            10 => raw = Some(b"{\"schema\":1,\"schema\":1}".to_vec()),
            11 => raw = Some(vec![b' '; 65537]),
            12 => {
                c.bytes.insert("unexpected".into(), vec![]);
            }
            13 => c.manifest.exports[0].passed_verifiers = vec!["made-up".into(), "made-up".into()],
            14 => c.manifest.exports[0].relative_name = "/absolute".into(),
            _ => c.manifest.exports[0].reference = "not-admitted".into(),
        }
        let bytes = raw.unwrap_or_else(|| canonical_data(&c.manifest).unwrap());
        assert!(
            artifacts::prepare(&c.root, &c.binding, &c.operation, &bytes, &c.bytes, true).is_err(),
            "attack {attack}"
        );
        assert!(matches!(c.state().collection, WorkerCollection::Quarantined(_)));
        for e in &c.state().artifact_policy[0].exports {
            assert!(c
                .root
                .read_artifact(&ArtifactRef {
                    scope: c.binding.tenant.clone(),
                    content: e.digest.clone(),
                    size: e.size
                })
                .is_err());
        }
        c.root.confirm_worker_cleanup(&c.state()).unwrap();
        let binding = c.binding.clone();
        c.root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        drop(c);
        let root = StoreRoot::open_existing(&path).unwrap();
        let s = root
            .worker_snapshot(&binding.tenant, &binding.provider, &binding.run)
            .unwrap()
            .unwrap();
        assert!(s.quarantined && !s.completed && !s.capacity_reserved);
        assert!(artifacts::read(&root, &binding, "blob").is_err());
        assert!(matches!(s.collection, WorkerCollection::Quarantined(_)));
    }
}
#[test]
fn wp_027_publication_requires_exact_binding_and_precedes_completion_across_reopen() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("store");
    let c = Collection::new(&path);
    assert!(c.root.record_worker_transition(&c.state(), WorkerFact::Completed).is_err());
    c.prepare().unwrap();
    assert!(artifacts::read(&c.root, &c.binding, "blob").is_err());
    artifacts::resume(&c.root, &c.binding).unwrap();
    let published = c.state();
    assert!(!published.completed);
    c.root.record_worker_transition(&published, WorkerFact::Completed).unwrap();
    c.root.confirm_worker_cleanup(&c.state()).unwrap();
    for scope in 0..4 {
        let mut b = c.binding.clone();
        match scope {
            0 => b.tenant = "other".to_string().try_into().unwrap(),
            1 => b.principal = PrincipalId::generate(),
            2 => b.run = "other".to_string().try_into().unwrap(),
            _ => b.realization = ContentHash::of(b"other"),
        }
        assert!(artifacts::read(&c.root, &b, "blob").is_err());
    }
    let binding = c.binding.clone();
    let exact = c.state();
    let signals = c.root.signal_obligations(StoreKind::Fabric, None, 100).unwrap();
    assert_eq!(signals.len(), 1);
    c.root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(c.state(), exact);
    drop(c);
    let root = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(artifacts::read(&root, &binding, "blob").unwrap(), b"safe");
    let other = Collection::add(root, "other", binding.principal.clone());
    other.prepare().unwrap();
    artifacts::resume(&other.root, &other.binding).unwrap();
    assert_eq!(artifacts::read(&other.root, &other.binding, "blob").unwrap(), b"safe");
    let first = match exact.collection {
        WorkerCollection::Published(p) => p,
        _ => unreachable!(),
    };
    let second = match other.state().collection {
        WorkerCollection::Published(p) => p,
        _ => unreachable!(),
    };
    assert_ne!(first.manifest, second.manifest);
}
#[test]
fn wp_028_cancelled_prepared_collection_cannot_publish_and_cleanup_keeps_report() {
    let t = tempfile::tempdir().unwrap();
    let c = Collection::new(&t.path().join("store"));
    c.prepare().unwrap();
    c.root.request_worker_stop(&c.state()).unwrap();
    assert!(artifacts::resume(&c.root, &c.binding).is_err());
    c.root.confirm_worker_cleanup(&c.state()).unwrap();
    assert!(matches!(c.state().collection, WorkerCollection::Quarantined(_)));
    assert!(!c.state().completed && !c.state().capacity_reserved);
}
#[test]
fn wp_028_unknown_and_failed_trusted_verifiers_ignore_provider_pass_claims() {
    for unknown in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let c = Collection::new(&t.path().join("store"));
        // A separate run is admitted with immutable verifier policy before execution.
        let mut b = c.binding.clone();
        b.run = "verify".to_string().try_into().unwrap();
        b.request = "start-verify".to_string().try_into().unwrap();
        c.root.reserve_start(b.clone(), 4).unwrap();
        let mut p = c.state().artifact_policy[0].clone();
        let content = b"invalid-json".to_vec();
        let mut bytes = c.bytes.clone();
        let mut manifest = c.manifest.clone();
        manifest.run = "verify".into();
        if unknown {
            p.exports[0].verifiers = vec!["unknown:1".to_string().try_into().unwrap()];
        } else {
            let e = p.exports.iter_mut().find(|e| e.name.ends_with(".json")).unwrap();
            e.digest = ContentHash::of(&content);
            e.size = content.len() as u64;
            let d =
                manifest.exports.iter_mut().find(|e| e.relative_name.ends_with(".json")).unwrap();
            d.digest = e.digest.clone();
            d.size = e.size;
            d.passed_verifiers = vec!["wi:json:1".into()];
            bytes.insert("report".into(), content);
        }
        c.root.bind_worker_artifact_policy(b.clone(), p).unwrap();
        let s = c.root.worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap();
        c.root.record_worker_transition(&s, WorkerFact::Exited).unwrap();
        let operation = WorkerOperation {
            key: "collect-verify".to_string().try_into().unwrap(),
            intent: EffectIntentId::generate(),
            operation: "collect_artifacts".to_string().try_into().unwrap(),
            canonical_request: ContentHash::of(b"verify"),
            acknowledged: false,
        };
        c.root.begin_worker_operation(b.clone(), operation.clone()).unwrap();
        assert!(artifacts::prepare(
            &c.root,
            &b,
            &operation.key,
            &canonical_data(&manifest).unwrap(),
            &bytes,
            true
        )
        .is_err());
        let s = c.root.worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap();
        let WorkerCollection::Quarantined(r) = s.collection else { panic!("quarantine absent") };
        assert!(r.findings.iter().any(|f| !f.passed));
    }
}

#[test]
fn wp_027_empty_exports_still_require_a_validated_published_collection() {
    let t = tempfile::tempdir().unwrap();
    let c = Collection::new(&t.path().join("store"));
    let mut b = c.binding.clone();
    b.run = "empty".to_string().try_into().unwrap();
    b.request = "start-empty".to_string().try_into().unwrap();
    c.root.reserve_start(b.clone(), 4).unwrap();
    c.root
        .bind_worker_artifact_policy(b.clone(), artifacts::policy(vec![], &[], 65536).unwrap())
        .unwrap();
    let snapshot = || c.root.worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap();
    c.root.record_worker_transition(&snapshot(), WorkerFact::Exited).unwrap();
    assert!(c.root.record_worker_transition(&snapshot(), WorkerFact::Completed).is_err());
    let op = WorkerOperation {
        key: "empty-collection".to_string().try_into().unwrap(),
        intent: EffectIntentId::generate(),
        operation: "collect_artifacts".to_string().try_into().unwrap(),
        canonical_request: ContentHash::of(b"empty"),
        acknowledged: false,
    };
    c.root.begin_worker_operation(b.clone(), op.clone()).unwrap();
    let manifest = worldinterface_worker::artifacts::ExportManifestV1 {
        schema: 1,
        provider: b.provider.as_str().into(),
        run: b.run.as_str().into(),
        owner: b.principal.as_str().into(),
        exports: vec![],
    };
    artifacts::prepare(
        &c.root,
        &b,
        &op.key,
        &canonical_data(&manifest).unwrap(),
        &std::collections::BTreeMap::new(),
        true,
    )
    .unwrap();
    assert!(c.root.record_worker_transition(&snapshot(), WorkerFact::Completed).is_err());
    artifacts::resume(&c.root, &b).unwrap();
    c.root.record_worker_transition(&snapshot(), WorkerFact::Completed).unwrap();
    assert!(snapshot().completed);
}
