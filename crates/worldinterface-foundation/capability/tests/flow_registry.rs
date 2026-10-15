use serde_json::Value;
use worldinterface_capability::{validate_connector, ConnectorRegistry};
use worldinterface_protocol::*;

#[test]
fn flow_shared_registry_validation_preserves_wip05_bytes_and_bindings() {
    let context: Value =
        serde_json::from_str(include_str!("../../../../conformance/wi-fabric-2/flow/context.json"))
            .unwrap();
    let descriptors: Vec<ConnectorDescriptor> =
        serde_json::from_value(context["descriptors"].clone()).unwrap();
    let golden: Value = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/flow/plan.canonical.json"
    ))
    .unwrap();
    let registry = ConnectorRegistry::new(descriptors.clone()).unwrap();
    assert_eq!(registry.digest().as_str(), golden["plan"]["registry_digest"].as_str().unwrap());
    assert_eq!(registry.bindings().unwrap()[0].descriptor, descriptors[0]);
    let mut shared = descriptors.clone();
    assert_eq!(&validate_registry_snapshot(&mut shared).unwrap(), registry.digest());
    let mut invalid = descriptors[0].clone();
    invalid.secret_refs = vec!["secret".to_string().try_into().unwrap(); 2];
    assert_eq!(
        validate_connector(&invalid),
        worldinterface_protocol::validate_connector_descriptor(&invalid)
    );
    assert!(validate_connector(&invalid).is_err());
    let mut second = descriptors[0].clone();
    second.operation = "second".to_string().try_into().unwrap();
    assert_eq!(
        ConnectorRegistry::new(vec![descriptors[0].clone(), second.clone()]).unwrap().digest(),
        ConnectorRegistry::new(vec![second, descriptors[0].clone()]).unwrap().digest()
    );
    assert!(ConnectorRegistry::new(vec![descriptors[0].clone(); 2]).is_err());
}
