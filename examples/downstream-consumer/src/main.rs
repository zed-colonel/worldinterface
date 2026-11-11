use worldinterface_protocol::InstanceId;
use worldinterface_runtime::storage::StoreBundle;
use worldinterface_storage::{StoreProfile, StoreRoot};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("bundle");
    let profile = StoreProfile {
        instance: InstanceId::generate(),
        created_at: "2026-11-09T00:00:00.000000000Z".to_string().try_into()?,
        platform: false,
    };
    drop(StoreBundle::initialize(&path, profile)?);
    let root = StoreRoot::open_existing(path.join("wi"))?;
    let scope = "consumer-tenant".to_string().try_into()?;
    let artifact = root.publish(scope, b"registry consumer artifact")?;
    assert_eq!(root.read_artifact(&artifact)?, b"registry consumer artifact");
    drop(root);
    let root = StoreRoot::open_existing(path.join("wi"))?;
    assert_eq!(root.read_artifact(&artifact)?, b"registry consumer artifact");
    println!("{}", serde_json::json!({"store_reopened": true, "artifact_roundtrip": true}));
    Ok(())
}
