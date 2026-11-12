use worldinterface_protocol::{ContentHash, OpaqueRef};
use worldinterface_worker::supervisor::*;
fn r(s: &str) -> OpaqueRef {
    s.to_string().try_into().unwrap()
}
fn command() -> Command {
    Command {
        version: VERSION,
        key: r("provider-key"),
        request: LifecycleRequest {
            version: VERSION,
            audience: r("supervisor"),
            authority_generation: r("authority:1"),
            policy_revision: r("policy:1"),
            instance: r("instance:1"),
            generation: r("generation:1"),
            operation: Operation::Stop,
            target: ContentHash::of(b"target"),
            image: ContentHash::of(b"image"),
            deadline_ms: 1000,
            max_evidence_bytes: 1024,
        },
    }
}
#[test]
fn strict_supervisor_protocol_and_exact_server_authorization() {
    let c = command();
    let session = Session {
        tenant: r("tenant"),
        principal: r("operator"),
        audience: r("supervisor"),
        authority_generation: r("authority:1"),
        policy_revision: r("policy:1"),
    };
    let grants = vec![Grant { session: session.clone(), request: c.request.clone() }];
    assert!(authorize(c.clone(), &session, &grants, 1, true).is_ok());
    assert!(authorize(c.clone(), &session, &grants, 1000, true).is_err());
    assert!(authorize(c.clone(), &session, &grants, 1000, false).is_ok());
    for field in
        ["tenant", "principal", "mount", "device", "command", "credentials", "engine_options"]
    {
        let mut value = serde_json::to_value(&c).unwrap();
        value[field] = "injected".into();
        assert!(decode::<Command>(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    assert!(decode::<Command>(br#"{"version":1,"version":1}"#).is_err());
    assert!(decode::<Command>(&vec![b' '; MAX_FRAME_BYTES + 1]).is_err());
    for field in [
        "audience",
        "authority_generation",
        "policy_revision",
        "instance",
        "generation",
        "target",
        "image",
        "operation",
        "deadline_ms",
        "max_evidence_bytes",
    ] {
        let mut value = serde_json::to_value(&c).unwrap();
        value["request"][field] = match field {
            "target" | "image" => serde_json::to_value(ContentHash::of(b"wrong")).unwrap(),
            "operation" => "recover".into(),
            "deadline_ms" => 1001.into(),
            "max_evidence_bytes" => 2048.into(),
            _ => "wrong".into(),
        };
        let changed: Command = serde_json::from_value(value).unwrap();
        assert!(authorize(changed, &session, &grants, 1, true).is_err(), "{field}");
    }
    let mut wrong = session.clone();
    wrong.tenant = r("other");
    assert!(authorize(c.clone(), &wrong, &grants, 1, true).is_err());
    wrong = session.clone();
    wrong.principal = r("worker");
    assert!(authorize(c, &wrong, &grants, 1, true).is_err());
}
/// A downstream implementation uses public API only; no repository test helpers.
struct Consumer;
impl SupervisorHandler for Consumer {
    fn supports_authoritative_inspect(&self) -> bool {
        true
    }
    fn dispatch(&self, c: AuthorizedSupervisorCommand) -> Result<Observation, String> {
        assert_eq!(c.command().request.operation, Operation::Stop);
        Ok(Observation { state: State::Running, evidence: vec![] })
    }
    fn inspect(&self, c: AuthorizedSupervisorInspect) -> Result<Observation, String> {
        assert_eq!(c.command().key, r("provider-key"));
        Ok(Observation { state: State::Completed, evidence: b"stopped".to_vec() })
    }
}
#[test]
fn public_consumer_handler_distinguishes_acceptance_and_completion() {
    let c = command();
    let session = Session {
        tenant: r("tenant"),
        principal: r("operator"),
        audience: r("supervisor"),
        authority_generation: r("authority:1"),
        policy_revision: r("policy:1"),
    };
    let grants = vec![Grant { session: session.clone(), request: c.request.clone() }];
    let auth = authorize(c, &session, &grants, 1, true).unwrap();
    assert_eq!(Consumer.dispatch(auth.clone()).unwrap().state, State::Running);
    assert_eq!(Consumer.inspect(authorize_inspect(auth)).unwrap().state, State::Completed);
}
