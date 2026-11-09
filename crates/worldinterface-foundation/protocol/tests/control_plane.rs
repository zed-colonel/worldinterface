use serde_json::json;
use worldinterface_protocol::{control_plane::*, *};
#[test]
fn control_wire_is_closed_has_no_default_profile_and_no_identity_or_secret_input() {
    for name in ["daemon", "platform", "default", ""] {
        assert!(serde_json::from_value::<Profile>(json!(name)).is_err());
    }
    let q = json!({"kind":"receipt","limit":2});
    let parsed: Query = serde_json::from_value(q.clone()).unwrap();
    canonical_data(&parsed).unwrap();
    for field in [
        "principal",
        "tenant",
        "actor",
        "organization",
        "roles",
        "token",
        "secret",
        "authorization_header",
    ] {
        let mut v = q.clone();
        v[field] = json!("forged");
        assert!(serde_json::from_value::<Query>(v).is_err(), "{field}");
    }
    let scope = json!({"scope":{"tenant":"t"},"kind":"artifact","id":"a"});
    let parsed: ObjectRef = serde_json::from_value(scope.clone()).unwrap();
    canonical_data(&parsed).unwrap();
    let mut scope = scope;
    scope["scope"]["principal"] = json!(PrincipalId::generate());
    assert!(serde_json::from_value::<ObjectRef>(scope).is_err());
    assert!(serde_json::from_value::<CredentialScope>(json!({"audience":"*"})).is_err());
    assert!(serde_json::from_value::<CustodyReason>(json!({"error":"token=secret"})).is_err());
}
