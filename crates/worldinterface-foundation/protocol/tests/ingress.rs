use serde_json::Value;
use worldinterface_protocol::*;
#[test]
fn ingress_records_match_independent_golden_hashes_and_strict_envelopes() {
    let vectors: Vec<Value> = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/ingress/canonical-vectors.json"
    ))
    .unwrap();
    for v in vectors {
        let bytes = serde_json::to_vec(&v["envelope"]).unwrap();
        let e = Envelope::parse(&bytes).unwrap();
        assert_eq!(e.canonical_bytes().unwrap(), v["canonical"].as_str().unwrap().as_bytes());
        assert_eq!(e.digest().unwrap().as_str(), v["digest"].as_str().unwrap());
        let mut changed = v["envelope"].clone();
        changed["record"]["body"]["authority"] = Value::Bool(true);
        assert!(Envelope::parse(&serde_json::to_vec(&changed).unwrap()).is_err());
        let mut changed = v["envelope"].clone();
        changed["schema"] = 2.into();
        assert!(Envelope::parse(&serde_json::to_vec(&changed).unwrap()).is_err());
    }
}
#[test]
fn collection_bound_does_not_weaken_public_record_bound() {
    let large = "x".repeat(MAX_RECORD_BYTES);
    assert!(canonical_data(&large).is_err());
    let bytes = canonical_collection(&large).unwrap();
    assert!(strict_json(&bytes).is_err());
    assert_eq!(strict_collection(&bytes).unwrap(), Value::String(large));
    assert!(canonical_collection(&"x".repeat(16 * 1024 * 1024)).is_err());
}
