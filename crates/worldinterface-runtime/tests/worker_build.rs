//! Build/provenance regressions: these tests never launch workers or helpers.
#![cfg(target_os = "linux")]
use worldinterface_protocol::{ContentHash, InstanceId, Timestamp};
use worldinterface_runtime::{storage::StoreBundle, worker_process::ExecutableRegistry};
use worldinterface_storage::StoreProfile;

#[path = "support/worker_build.rs"]
mod build;

fn artifacts() -> [(std::path::PathBuf, Vec<u8>); 2] {
    let built = build::current();
    [
        (built.fixture.clone(), built.fixture_bytes.clone()),
        (built.helper.clone(), built.helper_bytes.clone()),
    ]
}

#[test]
fn reviewed_build_artifacts_are_bounded_and_publishable_without_launch() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("bundle"),
        StoreProfile {
            instance: InstanceId::generate(),
            created_at: Timestamp::from_epoch_millis(1000).unwrap(),
            platform: false,
        },
    )
    .unwrap();
    for (path, bytes) in artifacts() {
        assert!(bytes.starts_with(b"\x7fELF"));
        assert!(bytes.len() as u64 <= worldinterface_protocol::plan::MAX_BOUNDARY_ARTIFACT_BYTES);
        assert_eq!(std::fs::read(path).unwrap(), bytes, "compiled output drift");
        let artifact = bundle
            .wi()
            .publish("tenant:build-regression".to_string().try_into().unwrap(), &bytes)
            .unwrap();
        assert_eq!(artifact.content, ContentHash::of(&bytes));
        assert_eq!(artifact.size, bytes.len() as u64);
    }
}

#[test]
fn trusted_build_digests_reject_replaced_paths_without_launch() {
    let temp = tempfile::tempdir().unwrap();
    for (i, (_, bytes)) in artifacts().into_iter().enumerate() {
        let path = temp.path().join(format!("executable-{i}"));
        std::fs::write(&path, &bytes).unwrap();
        let expected = ContentHash::of(&bytes);
        let mut registry = ExecutableRegistry::default();
        registry.register("reviewed", &path, &expected).unwrap();
        let mut replacement = bytes.to_vec();
        *replacement.last_mut().unwrap() ^= 1;
        std::fs::write(&path, replacement).unwrap();
        assert!(ExecutableRegistry::default().register("replaced", &path, &expected).is_err());
    }
}
