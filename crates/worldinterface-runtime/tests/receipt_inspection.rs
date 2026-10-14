mod support {
    pub mod effect_fixture;
}
use std::sync::Arc;
use support::effect_fixture::*;
use worldinterface_domain::{authorization::AuthorizationSnapshot, receipt::DisclosureProfile};
use worldinterface_protocol::{
    receipt::{ProofBundleKind, ProofBundleV1, SigningProofV1},
    *,
};
use worldinterface_runtime::{
    authority::Authentication,
    inspection::{ReceiptDisclosure, ReceiptInspector},
    storage::StoreBundle,
    Result,
};
use worldinterface_storage::{StoreKind, StoreProfile};

struct Auth;
type Session = (OpaqueRef, PrincipalId);
impl Authentication for Auth {
    type Session = Session;
    fn authenticate(&self, session: &Session) -> Result<Session> {
        Ok(session.clone())
    }
}
struct Disclosure {
    profile: DisclosureProfile,
    restricted: Option<ReceiptId>,
    evidence: Option<EvidenceRef>,
}
impl ReceiptDisclosure<Auth> for Disclosure {
    fn profile(
        &self,
        _: &Session,
        _: &OpaqueRef,
        receipt: &ReceiptId,
    ) -> Result<DisclosureProfile> {
        Ok(if self.restricted.as_ref() == Some(receipt) {
            DisclosureProfile::Redacted
        } else {
            self.profile
        })
    }
    fn effect_profile(
        &self,
        _: &Session,
        _: &OpaqueRef,
        _: &EffectIntentId,
    ) -> Result<DisclosureProfile> {
        Ok(self.profile)
    }
    fn bundle_allowed(&self, _: &Session, _: &OpaqueRef, _: &OpaqueRef) -> Result<bool> {
        Ok(true)
    }
    fn evidence_allowed(
        &self,
        _: &Session,
        _: &OpaqueRef,
        reference: &EvidenceRef,
    ) -> Result<bool> {
        Ok(self.evidence.as_ref() == Some(reference))
    }
}
struct Case {
    temp: tempfile::TempDir,
    bundle: Arc<StoreBundle>,
    snapshot: AuthorizationSnapshot,
    id: EffectIntentId,
    receipt: ReceiptId,
    digest: ContentHash,
}
impl Case {
    fn new(evidence: Vec<(OpaqueRef, Vec<u8>)>) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let bundle = Arc::new(
            StoreBundle::initialize(
                &temp.path().join("bundle"),
                StoreProfile {
                    instance: InstanceId::generate(),
                    created_at: at(),
                    platform: false,
                },
            )
            .unwrap(),
        );
        let (submission, snapshot) = fixture();
        let id = authorize(&bundle, submission, &snapshot);
        complete(bundle.clone(), &id, &snapshot, evidence);
        let (receipt, digest, _) =
            bundle.wi().effect_receipts(caller(&snapshot).tenant(), &id).unwrap()[0].clone();
        Self { temp, bundle, snapshot, id, receipt, digest }
    }
    fn session(&self) -> Session {
        let c = caller(&self.snapshot);
        (c.tenant().clone(), c.principal().clone())
    }
}
fn full() -> Disclosure {
    Disclosure { profile: DisclosureProfile::Full, restricted: None, evidence: None }
}

#[test]
fn canonical_receipt_survives_replay_rebuild_backup_restore() {
    let case = Case::new(vec![]);
    let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        case.session().0,
        case.session().1,
        DisclosureProfile::Full,
    );
    let original = case.bundle.wi().inspect_receipt(&access, &case.receipt).unwrap().unwrap();
    let bytes = original.canonical.as_ref().unwrap().canonical_bytes().unwrap();
    assert_eq!(original.digest.as_ref(), Some(&case.digest));
    assert_eq!(Envelope::parse_receipt(&bytes).unwrap().digest().unwrap(), case.digest);
    case.bundle.wi().repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(
        case.bundle.wi().inspect_receipt(&access, &case.receipt).unwrap().unwrap(),
        original
    );
    let path = case.temp.path().join("bundle");
    drop(case.bundle);
    drop(StoreBundle::open(&path).unwrap());
    StoreBundle::backup(&path, &case.temp.path().join("backup")).unwrap();
    StoreBundle::restore(&case.temp.path().join("backup"), &case.temp.path().join("restored"))
        .unwrap();
    let restored = StoreBundle::open(&case.temp.path().join("restored")).unwrap();
    assert_eq!(restored.wi().inspect_receipt(&access, &case.receipt).unwrap().unwrap(), original);
    assert_eq!(restored.wi().effect_receipts(access.tenant(), &case.id).unwrap().len(), 1);
}

#[test]
fn authenticated_redaction_and_cross_tenant_principal_denials() {
    let case = Case::new(vec![(
        "private-assertion:1".to_owned().try_into().unwrap(),
        b"private fixture bytes".to_vec(),
    )]);
    let auth = Auth;
    let full = full();
    let inspector =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &full };
    let original = inspector.receipt(&case.session(), &case.receipt).unwrap().unwrap();
    let redacted =
        Disclosure { profile: DisclosureProfile::Redacted, restricted: None, evidence: None };
    let redactor =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &redacted };
    let view = redactor.receipt(&case.session(), &case.receipt).unwrap().unwrap();
    assert!(view.canonical.is_none());
    assert_eq!(view.digest, original.digest);
    assert_eq!(view.causal_refs, original.causal_refs);
    assert_eq!(view.evidence_hashes, original.evidence_hashes);
    assert!(!String::from_utf8(canonical_data(&view).unwrap())
        .unwrap()
        .contains("private fixture bytes"));
    let foreign = ("foreign-tenant".to_owned().try_into().unwrap(), case.session().1);
    assert!(inspector.receipt(&foreign, &case.receipt).unwrap().is_none());
    let stranger = (case.session().0, PrincipalId::generate());
    assert!(inspector.receipt(&stranger, &case.receipt).is_err());
    assert!(inspector.effect_chain(&stranger, &case.id).is_err());
    assert!(inspector.worker_controls(&stranger, &case.id).is_err());
    assert!(redactor.worker_evidence(&case.session(), &case.id).is_err());
}

#[test]
fn proof_bundles_bind_receipts_and_authorize_every_member() {
    let case = Case::new(vec![]);
    let tenant = case.session().0;
    let (mut submission, mut snapshot) = fixture();
    submission.client_request_id = "second-request".to_owned().try_into().unwrap();
    let demand = worldinterface_domain::ingress::digest(&submission).unwrap();
    let binding = &mut snapshot.bindings[0];
    for authority in [
        &mut binding.principal_authority,
        &mut binding.capacity_authority,
        &mut snapshot.policy.platform,
        &mut snapshot.policy.resource,
        &mut snapshot.policy.information_use,
        &mut snapshot.grants[0].authority,
    ] {
        authority.insert(demand.clone());
    }
    let second = authorize(&case.bundle, submission, &snapshot);
    complete(case.bundle.clone(), &second, &snapshot, vec![]);
    let (second_receipt, second_digest, _) =
        case.bundle.wi().effect_receipts(&tenant, &second).unwrap()[0].clone();
    let id: OpaqueRef = "bundle:review".to_owned().try_into().unwrap();
    let bundle = ProofBundleV1 {
        version: 1,
        kind: ProofBundleKind::Trial,
        candidate: CandidateId::generate(),
        records: vec![case.digest.clone(), second_digest],
        evidence: vec![],
    };
    let digest = case.bundle.wi().append_proof_bundle(&tenant, &id, &bundle).unwrap();
    assert_eq!(case.bundle.wi().append_proof_bundle(&tenant, &id, &bundle).unwrap(), digest);
    let auth = Auth;
    let disclosure = full();
    let inspector =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &disclosure };
    assert_eq!(
        inspector.proof_bundle(&case.session(), &id, &case.receipt).unwrap(),
        Some(bundle.clone())
    );
    let restricted = Disclosure {
        profile: DisclosureProfile::Full,
        restricted: Some(second_receipt.clone()),
        evidence: None,
    };
    let restricted_inspector =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &restricted };
    assert!(restricted_inspector.proof_bundle(&case.session(), &id, &case.receipt).is_err());
    let single_id: OpaqueRef = "bundle:second".to_owned().try_into().unwrap();
    let mut single = bundle.clone();
    single.records.remove(0);
    case.bundle.wi().append_proof_bundle(&tenant, &single_id, &single).unwrap();
    assert!(inspector.proof_bundle(&case.session(), &single_id, &case.receipt).is_err());
    assert!(inspector
        .proof_bundle(&(tenant.clone(), PrincipalId::generate()), &id, &case.receipt)
        .is_err());
    assert!(inspector
        .proof_bundle(
            &("foreign".to_owned().try_into().unwrap(), case.session().1),
            &id,
            &case.receipt
        )
        .is_err());
    let redacted = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        tenant.clone(),
        case.session().1,
        DisclosureProfile::Redacted,
    );
    assert!(case.bundle.wi().inspect_proof_bundle(&redacted, &id).is_err());
    assert!(case.bundle.wi().append_proof_bundle(&tenant, &id, &single).is_err());
    single.records = vec![ContentHash::of(b"absent")];
    assert!(case.bundle.wi().append_proof_bundle(&tenant, &single_id, &single).is_err());
    assert_eq!(
        inspector.proof_bundle(&case.session(), &id, &case.receipt).unwrap(),
        Some(bundle.clone())
    );
    // A Full receipt decision and bundle membership do not disclose additional
    // evidence references. The authorizer must approve the exact reference.
    let extra = EvidenceRef {
        artifact: case.bundle.wi().publish(tenant.clone(), b"restricted bundle fixture").unwrap(),
        schema: "extra-evidence:1".to_owned().try_into().unwrap(),
    };
    let extra_id: OpaqueRef = "bundle:extra-evidence".to_owned().try_into().unwrap();
    let mut with_extra = bundle;
    with_extra.evidence.push(extra.clone());
    case.bundle.wi().append_proof_bundle(&tenant, &extra_id, &with_extra).unwrap();
    assert!(inspector.proof_bundle(&case.session(), &extra_id, &case.receipt).is_err());
    let allowed =
        Disclosure { profile: DisclosureProfile::Full, restricted: None, evidence: Some(extra) };
    let allowed_inspector =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &allowed };
    assert_eq!(
        allowed_inspector.proof_bundle(&case.session(), &extra_id, &case.receipt).unwrap(),
        Some(with_extra)
    );
}

#[test]
fn detached_proofs_are_restricted_provenance_and_leave_receipts_immutable() {
    let case = Case::new(vec![]);
    let tenant = case.session().0;
    let key_state = EvidenceRef {
        artifact: case.bundle.wi().publish(tenant.clone(), b"key-state fixture").unwrap(),
        schema: "key-state:1".to_owned().try_into().unwrap(),
    };
    let proof = SigningProofV1 {
        version: 1,
        receipt_digest: case.digest.clone(),
        suite: "ed25519:1".to_owned().try_into().unwrap(),
        purpose: "provenance".to_owned().try_into().unwrap(),
        signed_at: at(),
        key_state,
        signature: case.bundle.wi().publish(tenant.clone(), b"detached provider fixture").unwrap(),
    };
    let auth = Auth;
    let disclosure = full();
    let inspector =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &disclosure };
    let before = inspector.receipt(&case.session(), &case.receipt).unwrap();
    case.bundle.wi().append_receipt_proof(&tenant, &case.receipt, &proof).unwrap();
    assert_eq!(inspector.receipt_proofs(&case.session(), &case.receipt).unwrap()[0].1, proof);
    assert_eq!(inspector.receipt(&case.session(), &case.receipt).unwrap(), before);
    let restricted =
        Disclosure { profile: DisclosureProfile::Redacted, restricted: None, evidence: None };
    assert!(ReceiptInspector {
        root: case.bundle.wi(),
        authentication: &auth,
        disclosure: &restricted
    }
    .receipt_proofs(&case.session(), &case.receipt)
    .is_err());
    assert!(inspector
        .receipt_proofs(&(tenant.clone(), PrincipalId::generate()), &case.receipt)
        .is_err());
    let mut orphan = proof.clone();
    orphan.receipt_digest = ContentHash::of(b"orphan");
    assert!(case.bundle.wi().append_receipt_proof(&tenant, &case.receipt, &orphan).is_err());
    assert!(case
        .bundle
        .wi()
        .append_receipt_proof(&tenant, &ReceiptId::generate(), &proof)
        .is_err());
}

fn worker_report() -> worldinterface_worker::evidence::WorkerEvidenceReportV1 {
    use worldinterface_worker::{controls::*, evidence::*, profile::ProfileBinding};
    let (submission, snapshot) = fixture();
    let worker = snapshot.worker.unwrap();
    WorkerEvidenceReportV1 {
        schema: 1,
        binding: ExactWorkerBindingV1 {
            profile: ProfileBinding::process_proof(),
            adapter: worker.manifest.adapter,
            executable: worker.manifest.executable,
            dependencies: worker.manifest.dependencies,
            configuration: worker.manifest.configuration,
            inputs: vec![submission.input.artifact],
            principal: snapshot.bindings[0].principal.clone(),
            run: worker.run,
            resource: worker.resource,
            authorization: worker.evidence.clone(),
            grant: snapshot.grants[0].evidence.clone(),
            artifact_scope: snapshot.bindings[0].tenant.clone(),
            aq_revision: worker.aq_version,
            wi_revision: worker.wi_version,
        },
        controls: ControlEvidenceV1 {
            schema: 1,
            provider: "fixture-provider".into(),
            run: "fixture-run".into(),
            requested: vec![Control {
                kind: ControlKind::WallTimeMs,
                mandatory: true,
                state: ControlState::Supported(100),
            }],
            advertised: vec![Control {
                kind: ControlKind::WallTimeMs,
                mandatory: true,
                state: ControlState::Unknown,
            }],
            applied: vec![],
            observed: vec![],
        },
        missing: vec![ControlKind::WallTimeMs],
        enclosing: Some(EnclosingInstanceEvidenceV1 {
            source: "host-operator".to_owned().try_into().unwrap(),
            trust: "fixture".to_owned().try_into().unwrap(),
            status: VerificationStatus::Verified,
            evidence: worker.evidence,
        }),
        fixture_only: true,
    }
}

#[test]
fn wp015_runtime_receipt_separates_enclosing_provenance_from_worker_enforcement() {
    let mut report = worker_report();
    report.controls.run = report.binding.run.as_str().to_owned();
    report.validate().unwrap();
    let case = Case::new(vec![(
        "wi.worker-evidence:1".to_owned().try_into().unwrap(),
        canonical_data(&report).unwrap(),
    )]);
    let auth = Auth;
    let disclosure = full();
    let inspector =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &disclosure };
    let retrieved = inspector.worker_evidence(&case.session(), &case.id).unwrap().unwrap();
    assert_eq!(retrieved, report);
    assert!(!retrieved.fixture_eligible().unwrap());
    assert!(!retrieved.binding.profile.independent_worker_vm);
    let controls = inspector.worker_controls(&case.session(), &case.id).unwrap();
    assert_eq!(controls.enclosing_instance, report.enclosing.map(|e| e.evidence));
    assert!(controls.worker_enforcement_evidence.is_empty());
    assert!(controls.missing.iter().any(|name| name.as_str() == "wall_time_ms"));
    assert_eq!(controls.producer_report, Some(retrieved));
    let receipts = inspector.effect_chain(&case.session(), &case.id).unwrap().receipts;
    assert_eq!(receipts, vec![(case.receipt, case.digest, receipts[0].2)]);
}

#[test]
fn wp016_runtime_receipt_retains_requested_advertised_applied_observed_missing_unknown() {
    use worldinterface_worker::controls::*;
    let mut report = worker_report();
    report.controls.run = report.binding.run.as_str().to_owned();
    report.controls.requested.push(Control {
        kind: ControlKind::OutputBytes,
        mandatory: true,
        state: ControlState::Supported(100),
    });
    report.controls.advertised.push(Control {
        kind: ControlKind::OutputBytes,
        mandatory: true,
        state: ControlState::Supported(100),
    });
    report.controls.applied.push(Control {
        kind: ControlKind::OutputBytes,
        mandatory: true,
        state: ControlState::Supported(100),
    });
    report.controls.observed.push(Control {
        kind: ControlKind::OutputBytes,
        mandatory: true,
        state: ControlState::Supported(50),
    });
    report.validate().unwrap();
    let case = Case::new(vec![(
        "wi.worker-evidence:1".to_owned().try_into().unwrap(),
        canonical_data(&report).unwrap(),
    )]);
    let auth = Auth;
    let disclosure = full();
    let inspector =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &disclosure };
    let observed = inspector.worker_evidence(&case.session(), &case.id).unwrap().unwrap();
    assert_eq!(observed.controls, report.controls);
    assert_eq!(observed.missing, report.missing);
    assert_eq!(observed.mandatory_gaps().unwrap().len(), 1);
    assert!(!observed.fixture_eligible().unwrap());
    assert!(matches!(observed.controls.advertised[0].state, ControlState::Unknown));
    let bytes = inspector
        .receipt(&case.session(), &case.receipt)
        .unwrap()
        .unwrap()
        .canonical
        .unwrap()
        .canonical_bytes()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(Envelope::parse_receipt(&canonical_data(&json).unwrap()).is_ok());
    for prohibited in [
        "winning_arm",
        "binding_constraint",
        "saturation",
        "task_quality",
        "continuity",
        "acceptance",
    ] {
        assert!(!String::from_utf8(bytes.clone()).unwrap().contains(prohibited));
        let mut forged = json.clone();
        forged["record"]["body"][prohibited] = serde_json::json!(true);
        assert!(Envelope::parse_receipt(&canonical_data(&forged).unwrap()).is_err());
    }
}

#[test]
fn runtime_campaign_inspection_preserves_authority_and_material_confounds() {
    use worldinterface_domain::developmental::{CampaignArmV1, Comparison};
    let case = Case::new(vec![]);
    let auth = Auth;
    let disclosure = full();
    let inspector =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &disclosure };
    let state = case.bundle.wi().effect_snapshot(&case.session().0).unwrap();
    let receipt = inspector.receipt(&case.session(), &case.receipt).unwrap();
    let arm = |name: &str| CampaignArmV1 {
        version: 1,
        campaign: "campaign:fixture".to_owned().try_into().unwrap(),
        arm: name.to_owned().try_into().unwrap(),
        declared_interventions: vec!["environment".to_owned().try_into().unwrap()],
    };
    let comparison = inspector
        .compare(&case.session(), &case.id, &case.id, Some(arm("left")), Some(arm("right")))
        .unwrap();
    let baseline = inspector.compare(&case.session(), &case.id, &case.id, None, None).unwrap();
    assert_eq!(comparison.fields, baseline.fields);
    assert_eq!(comparison.confounds, baseline.confounds);
    assert_eq!(case.bundle.wi().effect_snapshot(&case.session().0).unwrap(), state);
    assert_eq!(inspector.receipt(&case.session(), &case.receipt).unwrap(), receipt);
    let original = inspector.developmental(&case.session(), &case.id, None).unwrap();
    let mut changed = original.clone();
    changed.resource.as_mut().unwrap().resource = "changed-resource".to_owned().try_into().unwrap();
    changed.policy = Some("changed-policy".to_owned().try_into().unwrap());
    changed.connector = Some("changed-connector".to_owned().try_into().unwrap());
    changed.environment = Some(ContentHash::of(b"changed-environment"));
    changed.outputs.clear();
    changed.submitted_at = "2026-09-21T00:00:01.000000000Z".to_owned().try_into().unwrap();
    changed.uncertainty = true;
    let differences =
        worldinterface_domain::developmental::compare_arms(&original, &changed).unwrap();
    for field in
        ["resource", "policy", "connector", "environment", "outputs", "timing", "uncertainty"]
    {
        assert!(differences
            .fields
            .iter()
            .any(|(name, value)| name.as_str() == field && *value == Comparison::Difference));
        assert!(differences.confounds.iter().any(|name| name.as_str() == field));
    }
}

#[test]
fn worker_report_identity_mismatch_is_not_disclosed_as_bound_evidence() {
    let mut report = worker_report();
    report.controls.run = report.binding.run.as_str().to_owned();
    report.binding.principal = PrincipalId::generate();
    let case = Case::new(vec![(
        "wi.worker-evidence:1".to_owned().try_into().unwrap(),
        canonical_data(&report).unwrap(),
    )]);
    let auth = Auth;
    let disclosure = full();
    let inspector =
        ReceiptInspector { root: case.bundle.wi(), authentication: &auth, disclosure: &disclosure };
    assert!(inspector.worker_evidence(&case.session(), &case.id).is_err());
    assert!(inspector.worker_controls(&case.session(), &case.id).is_err());
}
