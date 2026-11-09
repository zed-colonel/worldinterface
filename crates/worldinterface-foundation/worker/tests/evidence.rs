use worldinterface_worker::{controls::*, evidence::*, profile::ProfileBinding};

fn fixture() -> WorkerEvidenceReportV1 {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/egress/fixture.json"
    ))
    .unwrap();
    let worker = &v["snapshot"]["worker"];
    let binding: ExactWorkerBindingV1 = serde_json::from_value(serde_json::json!({
        "profile": ProfileBinding::process_proof(),
        "adapter": worker["manifest"]["adapter"],
        "executable": worker["manifest"]["executable"],
        "dependencies": worker["manifest"]["dependencies"],
        "configuration": worker["manifest"]["configuration"],
        "inputs": [v["submission"]["input"]["artifact"]],
        "principal": v["snapshot"]["bindings"][0]["principal"],
        "run": worker["run"],
        "resource": worker["resource"],
        "authorization": worker["evidence"],
        "grant": v["snapshot"]["grants"][0]["evidence"],
        "artifact_scope": "tenant:fixture",
        "aq_revision": worker["aq_version"],
        "wi_revision": worker["wi_version"]
    }))
    .unwrap();
    WorkerEvidenceReportV1 {
        schema: 1,
        controls: ControlEvidenceV1 {
            schema: 1,
            provider: "fixture-provider".into(),
            run: binding.run.as_str().into(),
            requested: vec![Control {
                kind: ControlKind::WallTimeMs,
                mandatory: true,
                state: ControlState::Supported(100),
            }],
            advertised: vec![],
            applied: vec![],
            observed: vec![],
        },
        binding,
        missing: vec![ControlKind::WallTimeMs],
        enclosing: None,
        fixture_only: true,
    }
}

#[test]
fn wp015_host_provenance_cannot_certify_worker_enforcement() {
    let mut report = fixture();
    let source = report.binding.authorization.clone();
    report.enclosing = Some(EnclosingInstanceEvidenceV1 {
        source: "host-operator".to_string().try_into().unwrap(),
        trust: "fixture".to_string().try_into().unwrap(),
        status: VerificationStatus::Verified,
        evidence: source,
    });
    assert!(report.validate().is_ok());
    assert!(!report.fixture_eligible().unwrap());
    assert_eq!(report.mandatory_gaps().unwrap().len(), 1);
    assert!(!report.binding.profile.independent_worker_vm);
}

#[test]
fn wp016_missing_control_is_recorded_and_never_promoted() {
    let mut report = fixture();
    assert!(report.validate().is_ok());
    assert!(!report.fixture_eligible().unwrap());
    report.controls.applied.push(Control {
        kind: ControlKind::WallTimeMs,
        mandatory: true,
        state: ControlState::Supported(50),
    });
    assert!(report.validate().is_err());
    report.missing.clear();
    report.controls.applied.push(Control {
        kind: ControlKind::OutputBytes,
        mandatory: true,
        state: ControlState::Supported(100),
    });
    assert!(report.fixture_eligible().unwrap());
    report.fixture_only = false;
    assert!(!report.fixture_eligible().unwrap());
}
