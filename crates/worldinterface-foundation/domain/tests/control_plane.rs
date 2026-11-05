#[path = "support/control_plane.rs"]
mod support;
use std::collections::BTreeSet;
use support::*;
use worldinterface_domain::{agency::*, authorization::*, control_plane as cp, ingress::digest};
use worldinterface_protocol::{control_plane::*, *};

#[test]
fn control_profiles_are_explicit_and_local_development_is_restricted() {
    assert_eq!(cp::Configuration::select(None, o("tenant"), 120_000), Err(cp::Error::Profile));
    for duration in [0, 120_001, u64::MAX] {
        assert!(cp::Configuration::select(Some(Profile::Embedded), o("tenant"), duration).is_err());
    }
    let (state, _, request, snapshot, _) = issue_fixture(false, Profile::LocalDevelopment);
    assert!(state.configuration.as_ref().unwrap().status.non_production);
    assert!(cp::apply(&state, &caller(&snapshot), &request, now()).is_ok());
    let mut bad = request.clone();
    if let cp::Operation::Issue { request } = &mut bad.operation {
        request.binding.class = CredentialClass::Production;
    }
    assert!(cp::apply(&state, &caller(&snapshot), &bad, now()).is_err());
    let mut bootstrap = bundle(&snapshot, Profile::LocalDevelopment);
    bootstrap.objects[0].0.scope.tenant = o("new-tenant");
    assert!(cp::bootstrap(&cp::State::default(), &bootstrap, now()).is_err());
    assert!(cp::bootstrap(&state, &bundle(&snapshot, Profile::Embedded), now()).is_err());
    for profile in [Profile::Embedded, Profile::LocalDevelopment] {
        let (s, _, req, snapshot, _) =
            issue_fixture_class(false, profile, CredentialClass::Production);
        assert_eq!(
            cp::apply(&s, &caller(&snapshot), &req, now()).is_ok(),
            profile == Profile::Embedded
        );
    }
}

#[test]
fn control_caller_claims_and_invalid_server_bindings_cannot_select_identity() {
    let (_, snapshot) = fixture(false);
    let b = &snapshot.bindings[0];
    let c = caller(&snapshot);
    for mutation in 0..8 {
        let mut bs = vec![b.clone()];
        let mut claims = IdentityClaims::default();
        match mutation {
            0 => claims.accountable = Some(PrincipalId::generate()),
            1 => claims.actor = Some(ActorId::generate()),
            2 => bs.push(b.clone()),
            3 => bs[0].revoked = true,
            4 => bs[0].expires_at = time(),
            5 => bs[0].tenant = o("foreign"),
            6 => {
                bs[0].capacity_authority.insert(ContentHash::of(b"widen"));
            }
            _ => {
                let mut cap = b.capacity.clone();
                cap.organization_ref = Some(PrincipalId::generate());
                claims.capacity = Some(cap);
            }
        };
        assert!(resolve_binding(&c, &claims, &bs, &time()).is_err(), "{mutation}");
    }
    for mutation in 0..4 {
        let mut b = b.clone();
        let mut d = Delegation {
            id: o("d"),
            version: 1,
            issuer: b.accountable.clone(),
            subject: b.principal.clone(),
            tenant: b.tenant.clone(),
            authority: b.capacity_authority.clone(),
            expires_at: b.expires_at.clone(),
            revoked: false,
            evidence: snapshot.grants[0].evidence.clone(),
        };
        match mutation {
            0 => d.revoked = true,
            1 => d.expires_at = time(),
            2 => {
                d.authority.insert(ContentHash::of(b"widen"));
            }
            _ => {}
        }
        b.delegations.push(d); // self endpoint is a cycle in the fourth case
        assert!(resolve_binding(&c, &IdentityClaims::default(), &[b], &time()).is_err());
    }
}

#[test]
fn control_every_object_family_is_scoped_before_reads_counts_and_pagination() {
    let (_, mut snapshot) = fixture(false);
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let kinds = [
        ObjectKind::Principal,
        ObjectKind::Binding,
        ObjectKind::Policy,
        ObjectKind::Grant,
        ObjectKind::Resource,
        ObjectKind::Flow,
        ObjectKind::Worker,
        ObjectKind::Signal,
        ObjectKind::Receipt,
        ObjectKind::Evidence,
        ObjectKind::Artifact,
        ObjectKind::Audit,
        ObjectKind::Secret,
        ObjectKind::Authorization,
    ];
    let mut authority = BTreeSet::new();
    for kind in kinds {
        for name in ["a", "b", "c"] {
            authority.insert(cp::read_demand(&reference(&scope, kind, name)).unwrap());
        }
        authority.insert(
            cp::query_demand(&scope, &Query { kind, tenant_filter: None, limit: 1, cursor: None })
                .unwrap(),
        );
    }
    set_authority(&mut snapshot, &authority);
    let mut state =
        cp::bootstrap(&cp::State::default(), &bundle(&snapshot, Profile::Embedded), now()).unwrap();
    for kind in kinds {
        // Metadata catalog is trusted-host input; simulate all families without exposing bytes.
        for name in ["a", "b", "c", "hidden"] {
            state.objects.push(ObjectView {
                reference: reference(&scope, kind, name),
                revision: 1,
                content: ContentHash::of(b"object"),
                artifact: None,
            });
        }
        let mut foreign = reference(&scope, kind, "foreign");
        foreign.scope.tenant = o("other");
        state.objects.push(ObjectView {
            reference: foreign.clone(),
            revision: 1,
            content: ContentHash::of(b"foreign"),
            artifact: None,
        });
        let denied = request("foreign", cp::Operation::Get { reference: foreign });
        assert_eq!(
            cp::apply(&state, &caller(&snapshot), &denied, now()),
            Err(cp::Error::Unavailable)
        );
        let missing = request(
            "missing",
            cp::Operation::Get { reference: reference(&scope, kind, "missing") },
        );
        assert_eq!(
            cp::apply(&state, &caller(&snapshot), &missing, now()),
            Err(cp::Error::Unavailable)
        );
        let query = Query { kind, tenant_filter: None, limit: 1, cursor: None };
        let (_, out) = cp::apply(
            &state,
            &caller(&snapshot),
            &request("page", cp::Operation::Query { query: query.clone() }),
            now(),
        )
        .unwrap();
        let cp::Outcome::Page(first) = out else { panic!() };
        assert_eq!(first.visible_count, 3);
        assert_eq!(first.objects[0].reference.id, o("a"));
        let mut next = query.clone();
        next.cursor = first.next;
        let (_, out) = cp::apply(
            &state,
            &caller(&snapshot),
            &request("next", cp::Operation::Query { query: next.clone() }),
            now(),
        )
        .unwrap();
        let cp::Outcome::Page(second) = out else { panic!() };
        assert_eq!(second.objects[0].reference.id, o("b"));
        next.limit = 2;
        assert!(cp::apply(
            &state,
            &caller(&snapshot),
            &request("tampered", cp::Operation::Query { query: next }),
            now()
        )
        .is_err());
        let mut query = query;
        query.tenant_filter = Some(o("other"));
        assert!(cp::apply(
            &state,
            &caller(&snapshot),
            &request("filter", cp::Operation::Query { query }),
            now()
        )
        .is_err());
        let wrong = AuthenticatedCaller::from_trusted_authentication(
            o("other"),
            snapshot.bindings[0].principal.clone(),
        );
        assert!(cp::apply(&state, &wrong, &missing, now()).is_err());
        let mut foreign_org = reference(&scope, kind, "a");
        foreign_org.scope.organization = Some(PrincipalId::generate());
        assert!(cp::apply(
            &state,
            &caller(&snapshot),
            &request("org", cp::Operation::Get { reference: foreign_org }),
            now()
        )
        .is_err());
    }
}

#[test]
fn control_membership_and_roles_grant_no_ambient_authority() {
    let (_, mut snapshot) = fixture(false);
    snapshot.bindings[0].capacity.organization_ref = Some(PrincipalId::generate());
    set_authority(&mut snapshot, &BTreeSet::new());
    let state =
        cp::bootstrap(&cp::State::default(), &bundle(&snapshot, Profile::Embedded), now()).unwrap();
    let r = state.objects[0].reference.clone();
    assert!(cp::apply(
        &state,
        &caller(&snapshot),
        &request("read", cp::Operation::Get { reference: r }),
        now()
    )
    .is_err());
}

#[test]
fn control_administration_is_attributed_idempotent_and_cannot_widen() {
    let (_, mut snapshot) = fixture(false);
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let mut grant = snapshot.grants[0].clone();
    grant.version = 2;
    grant.revoked = true;
    let cmd = cp::AdminCommand {
        request: id("revoke"),
        reference: reference(&scope, ObjectKind::Grant, grant.id.as_str()),
        expected_revision: 1,
        content: cp::Content::Grant(grant),
        claims: IdentityClaims::default(),
        approvals: vec![],
    };
    let mut authority = snapshot.policy.platform.clone();
    authority.insert(cp::admin_demand(&cmd).unwrap());
    // Revocation carries the original authority ceiling, which the actor holds.
    set_authority(&mut snapshot, &authority);
    let state =
        cp::bootstrap(&cp::State::default(), &bundle(&snapshot, Profile::Embedded), now()).unwrap();
    let req = request("revoke", cp::Operation::Admin { command: Box::new(cmd.clone()) });
    let (new, out) = cp::apply(&state, &caller(&snapshot), &req, now()).unwrap();
    let cp::Outcome::Revision(r) = out else { panic!() };
    assert_eq!(r.revision, 2);
    assert!(r.previous.is_some());
    assert_eq!(r.attribution.requester, snapshot.bindings[0].principal);
    assert_eq!(r.attribution.actor, snapshot.bindings[0].actor);
    assert!(!r.attribution.bootstrap);
    // Identical retry against the original authorized revision is stable; after
    // revocation, renewed access is denied by the current authority intersection.
    assert_eq!(
        cp::apply(&state, &caller(&snapshot), &req, now()).unwrap().1,
        cp::Outcome::Revision(r)
    );
    assert_eq!(cp::apply(&new, &caller(&snapshot), &req, now()).unwrap().0, new);
    let mut bad = cmd;
    bad.content = cp::Content::Policy(snapshot.policy.clone());
    assert!(cp::apply(
        &state,
        &caller(&snapshot),
        &request("revoke", cp::Operation::Admin { command: Box::new(bad) }),
        now()
    )
    .is_err());
}

#[test]
fn control_worker_and_connector_credentials_reject_each_scope_mismatch() {
    for worker in [false, true] {
        let (state, _, req, snapshot, _) = issue_fixture(worker, Profile::Embedded);
        assert!(cp::apply(&state, &caller(&snapshot), &req, now()).is_ok());
        let cp::Operation::Issue { request: original } = &req.operation else { panic!() };
        for mutation in 0..19 {
            let mut value = serde_json::to_value(original).unwrap();
            let b = &mut value["binding"];
            match mutation {
                0 => b["scope"]["tenant"] = serde_json::json!("other"),
                1 => b["scope"]["organization"] = serde_json::json!(PrincipalId::generate()),
                2 => b["principal"] = serde_json::json!(PrincipalId::generate()),
                3 => b["audience"] = serde_json::json!(if worker { "connector" } else { "worker" }),
                4 => b["provider"] = serde_json::json!("other"),
                5 => b["operation"] = serde_json::json!("other"),
                6 => b["resource"]["resource"] = serde_json::json!("other"),
                7 => b["resource"]["version"] = serde_json::json!("other"),
                8 => b["run"] = serde_json::json!("other"),
                9 => b["authorization"] = serde_json::json!(ContentHash::of(b"stale")),
                10 => b["class"] = serde_json::json!("production"),
                11 => b["expires_at_ms"] = serde_json::json!(now()),
                12 => b["expires_at_ms"] = serde_json::json!(now() + 120_001),
                13 => b["generation"] = serde_json::json!(1),
                14 => b["secret_version"] = serde_json::json!(2),
                15 => b["recipient"] = serde_json::json!("other"),
                16 => b["revisions"] = serde_json::json!({}),
                17 => b["secret"]["id"] = serde_json::json!("other"),
                _ => b["resource"]["namespace"] = serde_json::json!("other"),
            }
            let bad: CredentialRequest = serde_json::from_value(value).unwrap();
            assert!(
                cp::apply(
                    &state,
                    &caller(&snapshot),
                    &request("issue", cp::Operation::Issue { request: Box::new(bad) }),
                    now()
                )
                .is_err(),
                "worker={worker}, mutation={mutation}"
            );
        }
        let mut shorter = original.clone();
        shorter.binding.expires_at_ms = now() + 1;
        assert!(cp::apply(
            &state,
            &caller(&snapshot),
            &request("issue", cp::Operation::Issue { request: shorter }),
            now()
        )
        .is_ok());
    }
}

#[test]
fn control_permits_require_current_exact_authorization_and_bound_expiries() {
    let (state, permit, issue, snapshot, intent) = issue_fixture(false, Profile::Embedded);
    let input = || cp::PermitInput {
        reference: reference(&permit.binding().scope, ObjectKind::Authorization, "another"),
        secret: permit.binding().secret.clone(),
        recipient: o("recipient:trusted"),
        run: o("run:fixture"),
        run_expires_at_ms: now() + 50,
        class: CredentialClass::Test,
    };
    let p = cp::prepare_permit(
        &state,
        &caller(&snapshot),
        &intent,
        &VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        input(),
        now(),
    )
    .unwrap();
    assert_eq!(p.binding().expires_at_ms, now() + 50);
    for mutation in 0..5 {
        let mut s = snapshot.clone();
        match mutation {
            0 => s.grants[0].revoked = true,
            1 => s.policy.expires_at = time(),
            2 => s.bindings[0].version += 1,
            3 => s.facts[0].attested_document = ContentHash::of(b"drift"),
            _ => s.policy.platform.clear(),
        };
        assert!(cp::prepare_permit(
            &state,
            &caller(&snapshot),
            &intent,
            &VerifiedSnapshot::from_trusted_verifier(s).unwrap(),
            input(),
            now()
        )
        .is_err());
    }
    // A newly applicable grant changes the complete authority snapshot even if
    // its authority hashes equal the original grant's hashes.
    let mut changed = state.clone();
    let mut revision = changed
        .history
        .iter()
        .find(|r| matches!(r.content, cp::Content::Grant(_)))
        .unwrap()
        .clone();
    revision.reference.id = o("grant:added");
    if let cp::Content::Grant(g) = &mut revision.content {
        g.id = o("grant:added");
    }
    revision.content_digest = digest(&revision.content).unwrap();
    changed.history.push(revision);
    assert!(cp::prepare_permit(
        &changed,
        &caller(&snapshot),
        &intent,
        &VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        input(),
        now()
    )
    .is_err());
    assert!(cp::apply(&changed, &caller(&snapshot), &issue, now()).is_err());
    let (pending, out) = cp::apply(&state, &caller(&snapshot), &issue, now()).unwrap();
    let cp::Outcome::Credential(m) = out else { panic!() };
    let mut pending = pending;
    pending.history.push(changed.history.last().unwrap().clone());
    assert!(cp::delivery_reference(&pending, &m.issuance, now()).is_err());
    let digest = digest(permit.binding()).unwrap();
    assert_ne!(digest, ContentHash::of(b"token"));
}

#[test]
fn control_administration_requires_exact_approvals_and_known_scoped_principals() {
    let (_, mut snapshot) = fixture(false);
    snapshot.policy.approval_required = true;
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let mut command = cp::AdminCommand {
        request: id("secret-admin"),
        reference: reference(&scope, ObjectKind::Secret, "secret"),
        expected_revision: 1,
        content: cp::Content::Secret(cp::Secret {
            custody: o("custody:rotated"),
            version: 2,
            generation: 1,
            expires_at_ms: now() + 200_000,
            revoked: false,
        }),
        claims: IdentityClaims::default(),
        approvals: vec![],
    };
    let mut authority = snapshot.policy.platform.clone();
    authority.insert(cp::admin_demand(&command).unwrap());
    set_authority(&mut snapshot, &authority);
    let state =
        cp::bootstrap(&cp::State::default(), &bundle(&snapshot, Profile::Embedded), now()).unwrap();
    assert!(cp::apply(
        &state,
        &caller(&snapshot),
        &request("secret-admin", cp::Operation::Admin { command: Box::new(command.clone()) }),
        now()
    )
    .is_err());
    command.approvals.push(Approval {
        id: o("approval"),
        version: 1,
        approver: snapshot.policy.authorizer.clone(),
        exact_binding: cp::administration_approval_binding(
            &snapshot.policy,
            &snapshot.bindings[0],
            &command,
        )
        .unwrap(),
        expires_at: snapshot.policy.expires_at.clone(),
        revoked: false,
        max_uses: 1,
        evidence: snapshot.grants[0].evidence.clone(),
    });
    // Host authorization does not waive required approvals or separation of duties.
    let host = AuthenticatedCaller::from_trusted_authentication(
        scope.tenant.clone(),
        snapshot.policy.authorizer.clone(),
    );
    let attested = cp::authorize_administration(
        &state,
        &host,
        &caller(&snapshot),
        &command,
        now() + 1000,
        now(),
    )
    .unwrap();
    assert!(cp::apply(
        &attested,
        &caller(&snapshot),
        &request("secret-admin", cp::Operation::Admin { command: Box::new(command.clone()) }),
        now()
    )
    .is_err());
    // The authorizer is excluded when separation of duties applies.
    assert!(cp::apply(
        &state,
        &caller(&snapshot),
        &request("secret-admin", cp::Operation::Admin { command: Box::new(command.clone()) }),
        now()
    )
    .is_err());
    snapshot.policy.separation_of_duties = false;
    command.approvals[0].exact_binding =
        cp::administration_approval_binding(&snapshot.policy, &snapshot.bindings[0], &command)
            .unwrap();
    let state =
        cp::bootstrap(&cp::State::default(), &bundle(&snapshot, Profile::Embedded), now()).unwrap();
    let req = request("secret-admin", cp::Operation::Admin { command: Box::new(command.clone()) });
    let (new, out) = cp::apply(&state, &caller(&snapshot), &req, now()).unwrap();
    assert_eq!(new.uses.len(), 2);
    assert_eq!(cp::apply(&new, &caller(&snapshot), &req, now()).unwrap().1, out);
    for n in 0..3 {
        let mut bad = command.clone();
        match n {
            0 => bad.approvals[0].exact_binding = ContentHash::of(b"other"),
            1 => bad.approvals[0].revoked = true,
            _ => bad.approvals[0].expires_at = time(),
        };
        assert!(cp::apply(
            &state,
            &caller(&snapshot),
            &request("secret-admin", cp::Operation::Admin { command: Box::new(bad) }),
            now()
        )
        .is_err());
    }
    let mut unknown = snapshot.grants[0].clone();
    unknown.id = o("new-grant");
    unknown.issuer = snapshot.bindings[0].accountable.clone();
    unknown.subject = PrincipalId::generate();
    let mut cmd = cp::AdminCommand {
        request: id("unknown-subject"),
        reference: reference(&scope, ObjectKind::Grant, "new-grant"),
        expected_revision: 0,
        content: cp::Content::Grant(unknown),
        claims: IdentityClaims::default(),
        approvals: vec![],
    };
    snapshot.policy.approval_required = false;
    let mut authority = snapshot.policy.platform.clone();
    authority.insert(cp::admin_demand(&cmd).unwrap());
    set_authority(&mut snapshot, &authority);
    let state =
        cp::bootstrap(&cp::State::default(), &bundle(&snapshot, Profile::Embedded), now()).unwrap();
    assert!(cp::apply(
        &state,
        &caller(&snapshot),
        &request("unknown-subject", cp::Operation::Admin { command: Box::new(cmd.clone()) }),
        now()
    )
    .is_err());
    cmd.reference.scope.tenant = o("foreign");
    assert!(cp::apply(
        &state,
        &caller(&snapshot),
        &request("unknown-subject", cp::Operation::Admin { command: Box::new(cmd) }),
        now()
    )
    .is_err());
}

fn host_admin(
    state: &cp::State,
    snapshot: &AuthorizationSnapshot,
    command: cp::AdminCommand,
) -> cp::State {
    let authorizer = AuthenticatedCaller::from_trusted_authentication(
        snapshot.bindings[0].tenant.clone(),
        snapshot.policy.authorizer.clone(),
    );
    let attested = cp::authorize_administration(
        state,
        &authorizer,
        &caller(snapshot),
        &command,
        now() + 1000,
        now(),
    )
    .unwrap();
    let req = cp::Request {
        id: command.request.clone(),
        operation: cp::Operation::Admin { command: Box::new(command) },
    };
    let (next, outcome) = cp::apply(&attested, &caller(snapshot), &req, now()).unwrap();
    let cp::Outcome::Revision(r) = outcome else { panic!() };
    assert!(r.attribution.authorization.is_some());
    assert_eq!(r.attribution.authorizer, snapshot.policy.authorizer);
    assert!(!r.attribution.bootstrap);
    next
}

#[test]
fn control_exact_host_authorizations_extend_post_bootstrap_authority() {
    let (_, mut snapshot) = fixture(false);
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let query = Query { kind: ObjectKind::Receipt, tenant_filter: None, limit: 100, cursor: None };
    set_authority(&mut snapshot, &BTreeSet::from([cp::query_demand(&scope, &query).unwrap()]));
    let mut state =
        cp::bootstrap(&cp::State::default(), &bundle(&snapshot, Profile::Embedded), now()).unwrap();
    let object = ObjectView {
        reference: reference(&scope, ObjectKind::Receipt, "post-bootstrap"),
        revision: 1,
        content: ContentHash::of(b"receipt"),
        artifact: None,
    };
    state = cp::register_object(&state, &object).unwrap();
    let get = request("get", cp::Operation::Get { reference: object.reference.clone() });
    let list = request("list", cp::Operation::Query { query });
    assert_eq!(cp::apply(&state, &caller(&snapshot), &get, now()), Err(cp::Error::Unavailable));
    let cp::Outcome::Page(page) = cp::apply(&state, &caller(&snapshot), &list, now()).unwrap().1
    else {
        panic!()
    };
    assert_eq!(page.visible_count, 0);
    let mut authority = snapshot.policy.platform.clone();
    authority.insert(cp::read_demand(&object.reference).unwrap());
    snapshot.policy.platform = authority.clone();
    snapshot.policy.resource = authority.clone();
    snapshot.policy.information_use = authority.clone();
    let command = cp::AdminCommand {
        request: id("new-policy"),
        reference: reference(&scope, ObjectKind::Policy, "policy"),
        expected_revision: 1,
        content: cp::Content::Policy(snapshot.policy.clone()),
        claims: IdentityClaims::default(),
        approvals: vec![],
    };
    let req = request("new-policy", cp::Operation::Admin { command: Box::new(command.clone()) });
    assert!(cp::apply(&state, &caller(&snapshot), &req, now()).is_err());
    let authorizer = AuthenticatedCaller::from_trusted_authentication(
        scope.tenant.clone(),
        snapshot.policy.authorizer.clone(),
    );
    for bad in [
        caller(&snapshot),
        AuthenticatedCaller::from_trusted_authentication(
            o("foreign"),
            snapshot.policy.authorizer.clone(),
        ),
    ] {
        if bad.tenant() != authorizer.tenant() || bad.principal() != authorizer.principal() {
            assert!(cp::authorize_administration(
                &state,
                &bad,
                &caller(&snapshot),
                &command,
                now() + 1000,
                now()
            )
            .is_err());
        }
    }
    let wrong = AuthenticatedCaller::from_trusted_authentication(
        scope.tenant.clone(),
        PrincipalId::generate(),
    );
    assert!(cp::authorize_administration(
        &state,
        &wrong,
        &caller(&snapshot),
        &command,
        now() + 1000,
        now()
    )
    .is_err());
    let attested = cp::authorize_administration(
        &state,
        &authorizer,
        &caller(&snapshot),
        &command,
        now() + 1000,
        now(),
    )
    .unwrap();
    assert!(cp::apply(&attested, &caller(&snapshot), &req, now() + 1000).is_err());
    let mut tampered = command.clone();
    if let cp::Content::Policy(p) = &mut tampered.content {
        p.platform.insert(ContentHash::of(b"unattested"));
    }
    assert!(cp::apply(
        &attested,
        &caller(&snapshot),
        &request("new-policy", cp::Operation::Admin { command: Box::new(tampered) }),
        now()
    )
    .is_err());
    let mut changed_binding = snapshot.bindings[0].clone();
    changed_binding.version = 2;
    let drifted = host_admin(
        &attested,
        &snapshot,
        cp::AdminCommand {
            request: id("binding-drift"),
            reference: reference(&scope, ObjectKind::Binding, changed_binding.id.as_str()),
            expected_revision: 1,
            content: cp::Content::Binding(changed_binding),
            claims: IdentityClaims::default(),
            approvals: vec![],
        },
    );
    assert!(cp::apply(&drifted, &caller(&snapshot), &req, now()).is_err());
    state = host_admin(&state, &snapshot, command);
    assert!(cp::apply(&state, &caller(&snapshot), &get, now()).is_err());
    snapshot.bindings[0].version = 2;
    snapshot.bindings[0].principal_authority = authority.clone();
    snapshot.bindings[0].capacity_authority = authority.clone();
    state = host_admin(
        &state,
        &snapshot,
        cp::AdminCommand {
            request: id("new-binding"),
            reference: reference(&scope, ObjectKind::Binding, snapshot.bindings[0].id.as_str()),
            expected_revision: 1,
            content: cp::Content::Binding(snapshot.bindings[0].clone()),
            claims: IdentityClaims::default(),
            approvals: vec![],
        },
    );
    assert!(cp::apply(&state, &caller(&snapshot), &get, now()).is_err());
    snapshot.grants[0].version = 2;
    snapshot.grants[0].authority = authority;
    state = host_admin(
        &state,
        &snapshot,
        cp::AdminCommand {
            request: id("new-grant"),
            reference: reference(&scope, ObjectKind::Grant, snapshot.grants[0].id.as_str()),
            expected_revision: 1,
            content: cp::Content::Grant(snapshot.grants[0].clone()),
            claims: IdentityClaims::default(),
            approvals: vec![],
        },
    );
    assert!(cp::apply(&state, &caller(&snapshot), &get, now()).is_ok());
    let cp::Outcome::Page(page) = cp::apply(&state, &caller(&snapshot), &list, now()).unwrap().1
    else {
        panic!()
    };
    assert_eq!(page.objects, vec![object]);
    assert_eq!(page.visible_count, 1);
    let mut foreign = req.clone();
    if let cp::Operation::Admin { command } = &mut foreign.operation {
        command.reference.scope.tenant = o("foreign");
    }
    assert!(cp::apply(&state, &caller(&snapshot), &foreign, now()).is_err());
}

#[test]
fn control_inactive_grants_do_not_poison_current_grants_and_host_can_recover() {
    let (_, mut snapshot) = fixture(false);
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let object = reference(&scope, ObjectKind::Principal, snapshot.bindings[0].principal.as_str());
    set_authority(&mut snapshot, &BTreeSet::from([cp::read_demand(&object).unwrap()]));
    let original =
        cp::bootstrap(&cp::State::default(), &bundle(&snapshot, Profile::Embedded), now()).unwrap();
    for inactive in 0..3 {
        let mut state = original.clone();
        let mut second = snapshot.grants[0].clone();
        second.id = o("independent-current-grant");
        state = host_admin(
            &state,
            &snapshot,
            cp::AdminCommand {
                request: id("add-current-grant"),
                reference: reference(&scope, ObjectKind::Grant, second.id.as_str()),
                expected_revision: 0,
                content: cp::Content::Grant(second),
                claims: IdentityClaims::default(),
                approvals: vec![],
            },
        );
        let key = digest(&(
            "grant",
            &snapshot.grants[0].tenant,
            &snapshot.grants[0].issuer,
            &snapshot.grants[0].id,
        ))
        .unwrap();
        if inactive == 2 {
            state.uses.insert(key, snapshot.grants[0].max_uses);
        } else {
            let mut first = snapshot.grants[0].clone();
            first.version = 2;
            first.revoked = inactive == 0;
            if inactive == 1 {
                first.expires_at = Timestamp::from_epoch_millis(now() + 1).unwrap();
            }
            state = host_admin(
                &state,
                &snapshot,
                cp::AdminCommand {
                    request: id("disable-first"),
                    reference: reference(&scope, ObjectKind::Grant, first.id.as_str()),
                    expected_revision: 1,
                    content: cp::Content::Grant(first),
                    claims: IdentityClaims::default(),
                    approvals: vec![],
                },
            );
        }
        let before = state.clone();
        let at = now() + u64::from(inactive == 1);
        for n in 0..10 {
            state = cp::apply(
                &state,
                &caller(&snapshot),
                &request(&format!("read-{n}"), cp::Operation::Get { reference: object.clone() }),
                at,
            )
            .unwrap()
            .0;
        }
        assert_eq!(state, before);
    }
    // Even exhausting the sole grant cannot prevent the trusted authorizer from
    // recording a new exact grant. Stable identity does not reset the old budget.
    let mut state = original;
    let key = digest(&(
        "grant",
        &snapshot.grants[0].tenant,
        &snapshot.grants[0].issuer,
        &snapshot.grants[0].id,
    ))
    .unwrap();
    state.uses.insert(key.clone(), snapshot.grants[0].max_uses);
    let mut replacement = snapshot.grants[0].clone();
    replacement.id = o("recovery-grant");
    state = host_admin(
        &state,
        &snapshot,
        cp::AdminCommand {
            request: id("recover"),
            reference: reference(&scope, ObjectKind::Grant, replacement.id.as_str()),
            expected_revision: 0,
            content: cp::Content::Grant(replacement),
            claims: IdentityClaims::default(),
            approvals: vec![],
        },
    );
    assert!(cp::apply(
        &state,
        &caller(&snapshot),
        &request("recovered", cp::Operation::Get { reference: object }),
        now()
    )
    .is_ok());
    assert_eq!(state.uses[&key], snapshot.grants[0].max_uses);
}

#[test]
fn control_binding_resolution_is_per_principal_beyond_256_global_bindings() {
    let (_, mut snapshot) = fixture(false);
    let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
    let object = reference(&scope, ObjectKind::Principal, snapshot.bindings[0].principal.as_str());
    set_authority(&mut snapshot, &BTreeSet::from([cp::read_demand(&object).unwrap()]));
    let mut state =
        cp::bootstrap(&cp::State::default(), &bundle(&snapshot, Profile::Embedded), now()).unwrap();
    for n in 0..260 {
        let mut b = snapshot.bindings[0].clone();
        b.id = o(&format!("other-binding-{n}"));
        b.principal = PrincipalId::generate();
        b.accountable = b.principal.clone();
        b.capacity.principal_ref = b.principal.clone();
        state = cp::register_object(
            &state,
            &ObjectView {
                reference: reference(&scope, ObjectKind::Principal, b.principal.as_str()),
                revision: 1,
                content: ContentHash::of(b"principal"),
                artifact: None,
            },
        )
        .unwrap();
        state = host_admin(
            &state,
            &snapshot,
            cp::AdminCommand {
                request: id(&format!("add-binding-{n}")),
                reference: reference(&scope, ObjectKind::Binding, b.id.as_str()),
                expected_revision: 0,
                content: cp::Content::Binding(b),
                claims: IdentityClaims::default(),
                approvals: vec![],
            },
        );
    }
    assert!(cp::apply(
        &state,
        &caller(&snapshot),
        &request("read", cp::Operation::Get { reference: object }),
        now()
    )
    .is_ok());
    let mut b = snapshot.bindings[0].clone();
    b.version = 2;
    b.revoked = true;
    state = host_admin(
        &state,
        &snapshot,
        cp::AdminCommand {
            request: id("revoke-self"),
            reference: reference(&scope, ObjectKind::Binding, b.id.as_str()),
            expected_revision: 1,
            content: cp::Content::Binding(b),
            claims: IdentityClaims::default(),
            approvals: vec![],
        },
    );
    assert!(cp::apply(
        &state,
        &caller(&snapshot),
        &request("denied", cp::Operation::Get { reference: state.objects[0].reference.clone() }),
        now()
    )
    .is_err());
}

#[test]
fn control_last_reserved_grant_use_delivers_and_retries_but_cannot_issue_again() {
    let (mut state, _, req, snapshot, _) = issue_fixture(false, Profile::Embedded);
    let grant = &snapshot.grants[0];
    let key = digest(&("grant", &grant.tenant, &grant.issuer, &grant.id)).unwrap();
    state.uses.insert(key.clone(), grant.max_uses - 1);
    let (pending, out) = cp::apply(&state, &caller(&snapshot), &req, now()).unwrap();
    assert_eq!(pending.uses[&key], grant.max_uses);
    let cp::Outcome::Credential(m) = out else { panic!() };
    assert!(cp::delivery_reference(&pending, &m.issuance, now()).is_ok());
    assert_eq!(cp::apply(&pending, &caller(&snapshot), &req, now()).unwrap().0, pending);
    let mut another = req;
    another.id = id("another-issue");
    if let cp::Operation::Issue { request } = &mut another.operation {
        request.request = another.id.clone();
    }
    assert!(cp::apply(&pending, &caller(&snapshot), &another, now()).is_err());
}

#[test]
fn control_fresh_permits_can_use_current_grants_after_another_grant_retires() {
    for retired in 0..3 {
        let (mut state, _, _, snapshot, intent) = issue_fixture(false, Profile::Embedded);
        let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
        let mut inactive = snapshot.grants[0].clone();
        inactive.id = o("retired-grant");
        inactive.revoked = retired == 0;
        if retired == 1 {
            inactive.expires_at = Timestamp::from_epoch_millis(now() + 1).unwrap();
        }
        state = host_admin(
            &state,
            &snapshot,
            cp::AdminCommand {
                request: id("other-grant"),
                reference: reference(&scope, ObjectKind::Grant, inactive.id.as_str()),
                expected_revision: 0,
                content: cp::Content::Grant(inactive.clone()),
                claims: IdentityClaims::default(),
                approvals: vec![],
            },
        );
        if retired == 2 {
            state.uses.insert(
                digest(&("grant", &inactive.tenant, &inactive.issuer, &inactive.id)).unwrap(),
                inactive.max_uses,
            );
        }
        let at = now() + u64::from(retired == 1);
        let permit = cp::prepare_permit(
            &state,
            &caller(&snapshot),
            &intent,
            &VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
            cp::PermitInput {
                reference: reference(&scope, ObjectKind::Authorization, "fresh"),
                secret: reference(&scope, ObjectKind::Secret, "secret"),
                recipient: o("recipient:trusted"),
                run: o("run:fixture"),
                run_expires_at_ms: now() + 180_000,
                class: CredentialClass::Test,
            },
            at,
        )
        .unwrap();
        let mut binding = permit.binding().clone();
        binding.expires_at_ms = now() + 120_000;
        let credential = request(
            "fresh",
            cp::Operation::Issue {
                request: Box::new(CredentialRequest {
                    request: id("fresh"),
                    permit: reference(&scope, ObjectKind::Authorization, "fresh"),
                    binding,
                }),
            },
        );
        state = cp::register_permit(&state, &permit).unwrap();
        let (pending, out) = cp::apply(&state, &caller(&snapshot), &credential, at).unwrap();
        let cp::Outcome::Credential(m) = out else { panic!() };
        assert!(cp::delivery_reference(&pending, &m.issuance, at).is_ok());
    }
}
