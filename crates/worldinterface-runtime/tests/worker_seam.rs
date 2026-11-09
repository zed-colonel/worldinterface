#![cfg(target_os = "linux")]
mod support {
    pub mod connectors;
    pub mod flow_fixture;
    pub mod spi_harness;
    pub mod worker_fixture;
}
use support::worker_fixture::*;
use worldinterface_domain::worker::WorkerCollection;
use worldinterface_protocol::*;
use worldinterface_runtime::worker_process::*;
use worldinterface_worker::mapping::Operation;
#[tokio::test]
async fn wp_031_seam_simulation_delayed_reordered_observations_and_uncertain_ack_use_real_wi_aq() {
    let mut h = Harness::new(Task::Transform { xor: 7 }, "WP-031", 1500, 65536);
    h.inputs = vec![h.bundle.wi().publish(h.auth.0.clone(), b"hello").unwrap()];
    let bytes: Vec<_> = b"hello".iter().map(|b| b ^ 7).collect();
    h.exports = vec![Export {
        reference: "result".into(),
        name: "output.bin".into(),
        digest: ContentHash::of(&bytes),
        size: bytes.len() as u64,
    }];
    let (h, provider) = h.with_seam();
    assert_eq!(seam::LABEL, "seam simulation");
    h.operation(Operation::Start).await;
    h.reconcile_start().await;
    assert!(h.finish().await.cleanup_confirmed);
    assert_eq!(provider.launches(), 1);
    h.runtime.poll(&h.binding()).unwrap();
    let before = h.state();
    h.runtime.poll(&h.binding()).unwrap();
    assert_eq!(h.state(), before);
    assert!(h.runtime.read_artifact(&h.binding(), "result").is_err());
    h.operation(Operation::CollectArtifacts).await;
    assert!(matches!(h.state().collection, WorkerCollection::Published(_)));
    assert!(h.state().completed);
    h.operation(Operation::Release).await;
    assert_eq!(h.runtime.read_artifact(&h.binding(), "result").unwrap(), bytes);
    assert_eq!(provider.launches(), 1);
    let binding = h.binding();
    let path = h.temp.path().join("bundle");
    let Harness { temp: _retained_temp, bundle, runtime, .. } = h;
    drop(runtime);
    drop(bundle);
    let bundle = worldinterface_runtime::storage::StoreBundle::open(&path).unwrap();
    assert_eq!(
        worldinterface_runtime::worker::artifacts::read(bundle.wi(), &binding, "result").unwrap(),
        bytes
    );
}
