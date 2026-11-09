use serde_json::{json, Value};
use worldinterface_protocol::*;
#[test]
fn receipt_import_requires_exact_canonical_bytes_and_supported_profile() {
    let vector = vectors()
        .into_iter()
        .find(|v| v["envelope"]["record"]["kind"] == "boundary_receipt")
        .unwrap();
    let mut value = vector["envelope"].clone();
    value["record"]["body"]["profile"] = serde_json::json!("boundary:1");
    let receipt: Envelope = Envelope::parse(&serde_json::to_vec(&value).unwrap()).unwrap();
    let canonical = receipt.canonical_bytes().unwrap();
    assert_eq!(
        Envelope::parse_receipt(&canonical).unwrap().digest().unwrap(),
        receipt.digest().unwrap()
    );
    let pretty = serde_json::to_vec_pretty(&value).unwrap();
    assert!(Envelope::parse(&pretty).is_ok());
    assert!(Envelope::parse_receipt(&pretty).is_err());
    value["record"]["body"]["profile"] = serde_json::json!("unknown:1");
    let unknown = Envelope::parse(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(Envelope::parse_receipt(&unknown.canonical_bytes().unwrap()).is_err());
}
fn vectors() -> Vec<Value> {
    serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../conformance/wi-fabric-2/domain/canonical-vectors.json"
        ))
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn all_record_hash_inputs_match_independent_golden_bytes() {
    let all = vectors();
    assert_eq!(all.len(), 22);
    for v in all {
        let e = Envelope::parse(&serde_json::to_vec(&v["envelope"]).unwrap()).unwrap();
        assert_eq!(
            e.canonical_bytes().unwrap(),
            v["canonical"].as_str().unwrap().as_bytes(),
            "{}",
            v["name"]
        );
        assert_eq!(e.digest().unwrap().as_str(), v["digest"].as_str().unwrap());
        assert!(!v["canonical"].as_str().unwrap().contains("flow_id"));
    }
}
fn mutations(v: &Value) -> Vec<Value> {
    match v {
        Value::Object(o) => o
            .iter()
            .flat_map(|(k, child)| {
                let mut results = vec![];
                let mut missing = v.clone();
                missing.as_object_mut().unwrap().remove(k);
                results.push(missing);
                for m in mutations(child) {
                    let mut changed = v.clone();
                    changed[k] = m;
                    results.push(changed);
                }
                results
            })
            .collect(),
        Value::Array(a) => {
            let mut out = vec![json!([])];
            for (i, child) in a.iter().enumerate() {
                for m in mutations(child) {
                    let mut c = v.clone();
                    c[i] = m;
                    out.push(c);
                }
            }
            out
        }
        Value::String(s) => {
            let changed = if s.len() == 64 || s.len() == 36 {
                format!("{}{}", if s.starts_with('a') { "b" } else { "a" }, &s[1..])
            } else {
                format!("{s}-changed")
            };
            vec![json!(changed)]
        }
        Value::Number(n) => vec![json!(n.as_u64().unwrap() + 1)],
        _ => vec![json!(false)],
    }
}
#[test]
fn every_supplied_field_is_hash_sensitive_or_rejected() {
    for v in vectors() {
        let e = &v["envelope"];
        for changed in mutations(e) {
            if changed == *e {
                continue;
            }
            if let Ok(parsed) = Envelope::parse(&serde_json::to_vec(&changed).unwrap()) {
                assert_ne!(
                    parsed.digest().unwrap().as_str(),
                    v["digest"].as_str().unwrap(),
                    "{}",
                    v["name"]
                );
            }
        }
    }
}
#[test]
fn strict_input_rejects_duplicates_unknown_versions_nulls_and_numbers() {
    for raw in [
        r#"{"a":1,"a":2}"#,
        r#"{"a":{"x":1,"x":2}}"#,
        r#"1.0"#,
        r#"1e0"#,
        r#"null"#,
        r#"9007199254740992"#,
        r#"[1,null]"#,
    ] {
        assert!(strict_json(raw.as_bytes()).is_err(), "{raw}");
    }
    let e = vectors()[0]["envelope"].clone();
    for path in ["schema", "contract", "canonical_profile"] {
        let mut c = e.clone();
        c[path] = json!(999);
        assert!(Envelope::parse(&serde_json::to_vec(&c).unwrap()).is_err());
    }
    for level in [0, 1, 2] {
        let mut c = e.clone();
        let target = match level {
            0 => &mut c,
            1 => &mut c["record"],
            _ => &mut c["record"]["body"],
        };
        target["extra"] = json!(true);
        assert!(Envelope::parse(&serde_json::to_vec(&c).unwrap()).is_err());
    }
}
#[test]
fn encoding_preserves_unicode_arrays_and_has_exact_escape_rules() {
    assert_eq!(
        canonical_data(&json!({"z":[2,1],"é":"e\u{301}\n\t\"\\\u{0001}/"})).unwrap(),
        "{\"z\":[2,1],\"é\":\"e\u{301}\\n\\t\\\"\\\\\\u0001/\"}".as_bytes()
    );
    assert_eq!(
        canonical_data(&json!({"n":-9007199254740991i64})).unwrap(),
        b"{\"n\":-9007199254740991}"
    );
    assert_ne!(ContentHash::of(b"a"), ContentHash::of(b"a\n"));
}
#[test]
fn canonical_identifiers_timestamps_and_reference_bounds() {
    for s in [
        "2026-09-19T23:00:00Z",
        "2026-09-19T23:00:00.000000000+00:00",
        "2026-09-19T23:00:60.000000000Z",
        "2026-02-30T23:00:00.000000000Z",
    ] {
        assert!(Timestamp::try_from(s.to_string()).is_err());
    }
    for s in ["", "/host/private", ".. /foo", "pid=123", "x\n"] {
        assert!(OpaqueRef::try_from(s.to_string()).is_err());
    }
    assert!(OpaqueRef::try_from("x".repeat(257)).is_err());
    assert!(EntityId::try_from("00000000-0000-0000-0000-000000000000".to_string()).is_err());
    assert!(EntityId::try_from(EntityId::generate().as_str().to_string()).is_ok());
}
#[test]
fn wp002_process_image_and_enclosing_evidence_are_distinct() {
    let v = vectors().into_iter().find(|v| v["name"] == "control_evidence").unwrap();
    let mut e = v["envelope"].clone();
    assert!(Envelope::parse(&serde_json::to_vec(&e).unwrap()).is_ok());
    for bad in [json!("unknown"), json!({"known":"a".repeat(64)})] {
        e["record"]["body"]["image"] = bad;
        assert!(Envelope::parse(&serde_json::to_vec(&e).unwrap()).is_err());
    }
    e["record"]["body"]["provider_kind"] = json!("image");
    assert!(Envelope::parse(&serde_json::to_vec(&e).unwrap()).is_ok());
    e["record"]["body"]["image"] = json!("not_applicable");
    assert!(Envelope::parse(&serde_json::to_vec(&e).unwrap()).is_err());
    e = v["envelope"].clone();
    e["record"]["body"].as_object_mut().unwrap().remove("enclosing_trust");
    assert!(Envelope::parse(&serde_json::to_vec(&e).unwrap()).is_err());
}

fn wp002_fixture() -> Value {
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../conformance/wi-fabric-2/domain/wp002-process.json"
        ))
        .unwrap(),
    )
    .unwrap();
    fixture
}

fn wp002_version_bindings(fixture: &Value) -> std::result::Result<(), &'static str> {
    let parse = |name: &str| Envelope::parse(&serde_json::to_vec(&fixture[name]).unwrap()).unwrap();
    let Record::ImmutableArtifactManifest(m) = parse("manifest").record else {
        panic!("manifest fixture")
    };
    let Record::ResolvedEffect(r) = parse("resolved").record else { panic!("resolved fixture") };
    let Record::BoundaryReceipt(b) = parse("receipt").record else { panic!("receipt fixture") };
    if b.versions.provider_profile != m.profile_definition.content {
        return Err("provider profile binding mismatch");
    }
    if b.versions.resolver != r.resolver_version {
        return Err("resolver binding mismatch");
    }
    Ok(())
}

#[test]
fn wp002_rejects_mismatched_profile_and_resolver_bindings() {
    for (field, value, error) in [
        (
            "provider_profile",
            ContentHash::of(b"other profile").as_str().to_string(),
            "provider profile binding mismatch",
        ),
        ("resolver", "other-resolver".to_string(), "resolver binding mismatch"),
    ] {
        let mut fixture = wp002_fixture();
        fixture["receipt"]["record"]["body"]["versions"][field] = value.into();
        assert_eq!(wp002_version_bindings(&fixture), Err(error));
    }
}

#[test]
fn wp002_existing_seams_bind_process_configuration_and_control_artifacts() {
    let fixture = wp002_fixture();
    assert_eq!(wp002_version_bindings(&fixture), Ok(()));
    let envelope =
        |name: &str| Envelope::parse(&serde_json::to_vec(&fixture[name]).unwrap()).unwrap();
    let manifest = envelope("manifest");
    let controls = envelope("controls");
    let resolved = envelope("resolved");
    let receipt = envelope("receipt");
    let submission = envelope("submission");
    let Record::ControlEvidence(c) = &controls.record else { panic!("control fixture") };
    let Record::ResolvedEffect(r) = &resolved.record else { panic!("resolved fixture") };
    let Record::BoundaryReceipt(b) = &receipt.record else { panic!("receipt fixture") };
    let Record::EffectSubmission(s) = &submission.record else { panic!("submission fixture") };
    let bytes = manifest.canonical_bytes().unwrap();
    assert_eq!(c.realization.content, ContentHash::of(&bytes));
    assert_eq!(c.realization.size, bytes.len() as u64);
    assert_eq!(r.normalized_parameters.artifact, c.realization);
    assert_eq!(r.resource.resource, c.profile);
    assert_eq!(s.resource.selector, c.profile);
    assert_eq!(s.input.artifact.content, ContentHash::of(&canonical_data(&c.requested).unwrap()));
    assert_eq!(
        b.evidence[0].artifact.content,
        ContentHash::of(&controls.canonical_bytes().unwrap())
    );
    let ReceiptBody::Egress(e) = &b.body else { panic!("egress fixture") };
    assert_eq!(e.resolved_effect_digest, resolved.digest().unwrap());
    assert_eq!(c.image, ImageApplicability::NotApplicable);
    assert!(c.enclosing_instance.is_some());
    // Enclosing evidence has no field that grants worker isolation or executable authority.
    let mut changed = manifest.clone();
    let Record::ImmutableArtifactManifest(m) = &mut changed.record else {
        panic!("manifest fixture")
    };
    m.configuration.content = ContentHash::of(b"changed configuration");
    assert_ne!(ContentHash::of(&changed.canonical_bytes().unwrap()), c.realization.content);
}
