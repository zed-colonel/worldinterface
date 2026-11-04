#![allow(dead_code)]
use std::{collections::BTreeMap, path::Path};
use worldinterface_domain::worker::*;
use worldinterface_protocol::*;
use worldinterface_runtime::{worker::artifacts, worker_process::Export};
use worldinterface_storage::{StoreProfile, StoreRoot};
use worldinterface_worker::artifacts::*;
pub struct Collection {
    pub root: StoreRoot,
    pub binding: WorkerBinding,
    pub operation: OpaqueRef,
    pub manifest: ExportManifestV1,
    pub bytes: BTreeMap<String, Vec<u8>>,
}
impl Collection {
    pub fn new(path: &Path) -> Self {
        let root = StoreRoot::create_empty(
            path,
            StoreProfile {
                instance: InstanceId::generate(),
                created_at: Timestamp::from_epoch_millis(1000).unwrap(),
                platform: false,
            },
        )
        .unwrap();
        Self::add(root, "run", PrincipalId::generate())
    }
    pub fn add(root: StoreRoot, run: &str, principal: PrincipalId) -> Self {
        let binding = WorkerBinding {
            tenant: "tenant".to_string().try_into().unwrap(),
            principal,
            provider: "provider".to_string().try_into().unwrap(),
            run: run.to_string().try_into().unwrap(),
            request: format!("start-{run}").try_into().unwrap(),
            realization: ContentHash::of(run.as_bytes()),
        };
        root.reserve_start(binding.clone(), 4).unwrap();
        let bytes: BTreeMap<String, Vec<u8>> =
            [("blob".into(), b"safe".to_vec()), ("report".into(), b"{\"ok\":true}".to_vec())]
                .into();
        let exports: Vec<_> = bytes
            .iter()
            .map(|(r, b)| Export {
                reference: r.clone(),
                name: if r == "report" { "report.json".into() } else { "out.bin".into() },
                digest: ContentHash::of(b),
                size: b.len() as u64,
            })
            .collect();
        let policy = artifacts::policy(vec![], &exports, 65536).unwrap();
        root.bind_worker_artifact_policy(binding.clone(), policy).unwrap();
        root.record_worker_transition(
            &root
                .worker_snapshot(&binding.tenant, &binding.provider, &binding.run)
                .unwrap()
                .unwrap(),
            WorkerFact::Exited,
        )
        .unwrap();
        let operation: OpaqueRef = format!("collect-{run}").try_into().unwrap();
        root.begin_worker_operation(
            binding.clone(),
            WorkerOperation {
                key: operation.clone(),
                intent: EffectIntentId::generate(),
                operation: "collect_artifacts".to_string().try_into().unwrap(),
                canonical_request: ContentHash::of(b"collect"),
                acknowledged: false,
            },
        )
        .unwrap();
        let manifest = ExportManifestV1 {
            schema: 1,
            provider: binding.provider.as_str().into(),
            run: run.into(),
            owner: binding.principal.as_str().into(),
            exports: exports
                .iter()
                .map(|e| ExportV1 {
                    reference: e.reference.clone(),
                    relative_name: e.name.clone(),
                    digest: e.digest.clone(),
                    size: e.size,
                    required_verifiers: vec![],
                    passed_verifiers: vec![],
                })
                .collect(),
        };
        Self { root, binding, operation, manifest, bytes }
    }
    pub fn state(&self) -> WorkerState {
        self.root
            .worker_snapshot(&self.binding.tenant, &self.binding.provider, &self.binding.run)
            .unwrap()
            .unwrap()
    }
    pub fn prepare(&self) -> worldinterface_runtime::Result<()> {
        artifacts::prepare(
            &self.root,
            &self.binding,
            &self.operation,
            &canonical_data(&self.manifest)?,
            &self.bytes,
            true,
        )
    }
}
