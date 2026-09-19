use worldinterface_capability::ResourceScope;
use worldinterface_protocol::CanonicalResourceRef;
#[test]
fn attenuation_requires_exact_resource_versions_and_operations() {
    let r = |v: &str| CanonicalResourceRef {
        namespace: "worker".to_string().try_into().unwrap(),
        resource: "resource".to_string().try_into().unwrap(),
        version: v.to_string().try_into().unwrap(),
    };
    let parent = ResourceScope {
        resources: vec![r("v1")],
        operations: vec!["read".to_string().try_into().unwrap()],
    };
    assert!(parent.attenuates(&parent));
    let child = ResourceScope { resources: vec![r("v2")], ..parent.clone() };
    assert!(!child.attenuates(&parent));
    let child = ResourceScope {
        operations: vec!["write".to_string().try_into().unwrap()],
        ..parent.clone()
    };
    assert!(!child.attenuates(&parent));
}
