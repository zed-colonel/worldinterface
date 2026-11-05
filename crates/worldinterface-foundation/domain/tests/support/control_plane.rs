use std::collections::{BTreeMap, BTreeSet};
use worldinterface_domain::{
    agency::*, authorization::*, control_plane as cp, ingress::digest, resource::*,
};
use worldinterface_protocol::{control_plane::*, *};
pub fn o(s: &str) -> OpaqueRef {
    s.to_string().try_into().unwrap()
}
pub fn id(s: &str) -> ClientRequestId {
    s.to_string().try_into().unwrap()
}
pub fn now() -> u64 {
    time().epoch_millis().unwrap()
}
pub fn time() -> Timestamp {
    "2026-09-21T00:00:00.000000000Z".to_string().try_into().unwrap()
}
pub fn reference(scope: &Scope, kind: ObjectKind, name: &str) -> ObjectRef {
    ObjectRef { scope: scope.clone(), kind, id: o(name) }
}
pub fn request(name: &str, operation: cp::Operation) -> cp::Request {
    cp::Request { id: id(name), operation }
}
pub fn set_authority(snapshot: &mut AuthorizationSnapshot, authority: &Authority) {
    snapshot.bindings[0].principal_authority = authority.clone();
    snapshot.bindings[0].capacity_authority = authority.clone();
    snapshot.policy.platform = authority.clone();
    snapshot.policy.resource = authority.clone();
    snapshot.policy.information_use = authority.clone();
    snapshot.grants[0].authority = authority.clone();
}
pub fn bundle(snapshot: &AuthorizationSnapshot, profile: Profile) -> cp::Bootstrap {
    let b = &snapshot.bindings[0];
    let scope =
        Scope { tenant: b.tenant.clone(), organization: b.capacity.organization_ref.clone() };
    cp::Bootstrap {
        configuration: cp::Configuration::select(Some(profile), b.tenant.clone(), 120_000).unwrap(),
        operator: b.principal.clone(),
        request: id("bootstrap"),
        objects: vec![
            (
                reference(&scope, ObjectKind::Binding, b.id.as_str()),
                cp::Content::Binding(b.clone()),
            ),
            (
                reference(&scope, ObjectKind::Policy, "policy"),
                cp::Content::Policy(snapshot.policy.clone()),
            ),
            (
                reference(&scope, ObjectKind::Grant, snapshot.grants[0].id.as_str()),
                cp::Content::Grant(snapshot.grants[0].clone()),
            ),
            (
                reference(&scope, ObjectKind::Secret, "secret"),
                cp::Content::Secret(cp::Secret {
                    custody: o("custody:safe"),
                    version: 1,
                    generation: 0,
                    expires_at_ms: now() + 300_000,
                    revoked: false,
                }),
            ),
        ],
    }
}
pub fn fixture(worker: bool) -> (EffectSubmission, AuthorizationSnapshot) {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../conformance/wi-fabric-2/egress/fixture.json"
    ))
    .unwrap();
    let mut submission: EffectSubmission = serde_json::from_value(v["submission"].clone()).unwrap();
    let mut snapshot: AuthorizationSnapshot =
        serde_json::from_value(v["snapshot"].clone()).unwrap();
    snapshot.grants[0].max_uses = 100;
    snapshot.policy.grant_issuers.insert(snapshot.bindings[0].accountable.clone());
    if !worker {
        submission.resource.namespace = o("connector");
        snapshot.registry.entries[0].namespace = o("connector");
        let fact = &mut snapshot.facts[0];
        fact.selector = submission.resource.clone();
        fact.document.target = Target::Connector {
            provider: o("provider:fixture"),
            resource: o("connector:fixture"),
            operation: o("read"),
        };
        fact.registry = digest(&snapshot.registry).unwrap();
        fact.attested_document = digest(&fact.document).unwrap();
        snapshot.policy.targets = BTreeSet::from([fact.attested_document.clone()]);
        snapshot.worker = None;
        snapshot.fixture = None;
    }
    set_authority(&mut snapshot, &BTreeSet::from([digest(&submission).unwrap()]));
    (submission, snapshot)
}
pub fn caller(s: &AuthorizationSnapshot) -> AuthenticatedCaller {
    AuthenticatedCaller::from_trusted_authentication(
        s.bindings[0].tenant.clone(),
        s.bindings[0].principal.clone(),
    )
}
pub fn intent(submission: &EffectSubmission, snapshot: &AuthorizationSnapshot) -> EffectIntent {
    let agency =
        resolve_agency(&caller(snapshot), submission, &snapshot.bindings, &time()).unwrap();
    EffectIntent {
        id: EffectIntentId::generate(),
        submission: submission.clone(),
        agency: agency.context,
        stable_idempotency_identity: o("stable"),
        submitted_at: time(),
    }
}
pub fn issue_fixture(
    worker: bool,
    profile: Profile,
) -> (cp::State, cp::Permit, cp::Request, AuthorizationSnapshot, EffectIntent) {
    issue_fixture_class(worker, profile, CredentialClass::Test)
}

pub fn issue_fixture_class(
    worker: bool,
    profile: Profile,
    class: CredentialClass,
) -> (cp::State, cp::Permit, cp::Request, AuthorizationSnapshot, EffectIntent) {
    let (submission, mut snapshot) = fixture(worker);
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let resource =
        snapshot.facts[0].document.reference(snapshot.facts[0].observed_version.clone()).unwrap();
    let credential = CredentialRequest {
        request: id("issue"),
        permit: reference(&scope, ObjectKind::Authorization, "permit"),
        binding: CredentialScope {
            scope: scope.clone(),
            principal: snapshot.bindings[0].principal.clone(),
            audience: if worker { Audience::Worker } else { Audience::Connector },
            recipient: o("recipient:trusted"),
            provider: o("provider:fixture"),
            operation: o(if worker { "start" } else { "read" }),
            resource,
            run: o("run:fixture"),
            authorization: ContentHash::of(b"temporary"),
            revisions: BTreeMap::new(),
            class,
            secret: reference(&scope, ObjectKind::Secret, "secret"),
            secret_version: 1,
            generation: 0,
            expires_at_ms: now() + 120_000,
        },
    };
    let mut authority = snapshot.policy.platform.clone();
    authority.insert(cp::issue_demand(&credential).unwrap());
    set_authority(&mut snapshot, &authority);
    let state = cp::bootstrap(&cp::State::default(), &bundle(&snapshot, profile), now()).unwrap();
    let intent = intent(&submission, &snapshot);
    let permit = cp::prepare_permit(
        &state,
        &caller(&snapshot),
        &intent,
        &VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        cp::PermitInput {
            reference: credential.permit.clone(),
            secret: credential.binding.secret.clone(),
            recipient: credential.binding.recipient.clone(),
            run: credential.binding.run.clone(),
            run_expires_at_ms: now() + 180_000,
            class,
        },
        now(),
    )
    .unwrap();
    let mut credential = credential;
    credential.binding = permit.binding().clone();
    credential.binding.expires_at_ms = now() + 120_000;
    let state = cp::register_permit(&state, &permit).unwrap();
    (
        state,
        permit,
        request("issue", cp::Operation::Issue { request: Box::new(credential) }),
        snapshot,
        intent,
    )
}
