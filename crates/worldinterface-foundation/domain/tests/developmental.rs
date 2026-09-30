use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::{AuthorizationSnapshot, VerifiedSnapshot},
    developmental::{compare_arms, inspect, CampaignArmV1, Comparison},
    effect::{validate_command, EffectAction, EffectState},
};
use worldinterface_protocol::*;

fn fixture() -> (EffectSubmission, AuthorizationSnapshot) {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/egress/fixture.json"
    ))
    .unwrap();
    (
        serde_json::from_value(v["submission"].clone()).unwrap(),
        serde_json::from_value(v["snapshot"].clone()).unwrap(),
    )
}
#[test]
fn campaign_labels_are_attribution_and_material_differences_remain_visible() {
    let (submission, snapshot) = fixture();
    let tenant = snapshot.bindings[0].tenant.clone();
    let caller = AuthenticatedCaller::from_trusted_authentication(
        tenant.clone(),
        snapshot.bindings[0].principal.clone(),
    );
    let verified = VerifiedSnapshot::from_trusted_verifier(snapshot).unwrap();
    let at: Timestamp = "2026-09-21T00:00:00.000000000Z".to_string().try_into().unwrap();
    let id = EffectIntentId::generate();
    let mut state = EffectState::default();
    for action in [
        EffectAction::Submit {
            id: id.clone(),
            submission: Box::new(submission),
            snapshot: verified.clone(),
            link: None,
        },
        EffectAction::Resolve { id: id.clone(), snapshot: verified.clone() },
        EffectAction::Authorize { id: id.clone(), snapshot: verified },
    ] {
        let command = validate_command(&state, &caller, at.clone(), action).unwrap();
        let (next, _) =
            worldinterface_domain::effect::replay_command(&state, &tenant, command.bytes())
                .unwrap();
        state = next;
    }
    let source = &state.effects[&id];
    let arm = |name: &str| CampaignArmV1 {
        version: 1,
        campaign: "campaign:1".to_string().try_into().unwrap(),
        arm: name.to_string().try_into().unwrap(),
        declared_interventions: vec!["declared-change".to_string().try_into().unwrap()],
    };
    let a = inspect(source, Some(arm("a"))).unwrap();
    let mut b = inspect(source, Some(arm("b"))).unwrap();
    let same = compare_arms(&a, &b).unwrap();
    assert!(same.confounds.iter().any(|c| c.as_str() == "outputs")); // no outcome: unknown
    assert!(same.confounds.iter().any(|c| c.as_str() == "outcome"));
    assert!(same.fields.iter().all(|(name, state)| name.as_str() == "outputs"
        || name.as_str() == "outcome"
        || name.as_str() == "attempt"
        || name.as_str() == "dispatch_key"
        || *state == Comparison::Match
        || name.as_str() == "information_use"));
    b.policy = Some("changed-policy".to_string().try_into().unwrap());
    b.environment = None;
    let comparison = compare_arms(&a, &b).unwrap();
    assert!(comparison
        .fields
        .contains(&("policy".to_string().try_into().unwrap(), Comparison::Difference)));
    assert!(comparison
        .fields
        .contains(&("environment".to_string().try_into().unwrap(), Comparison::Unknown)));
    assert_eq!(comparison.declared_interventions.len(), 2);
    assert_eq!(a.agency, b.agency);
}
