use actionqueue_core::{
    bounded::OpaqueRef as AqOpaqueRef,
    causal::ControlMutationContext,
    control::{ControlScope, HostControlContext},
    disposition::AttemptDisposition,
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use std::cell::Cell;
use worldinterface_domain::{
    authorization::AuthorizationSnapshot,
    resource::{ResolutionFact, ResolverSnapshot, Target, TargetDocument},
    Phase,
};
use worldinterface_protocol::{
    ContentHash, EffectSubmission, EvidenceRef, InstanceId, OpaqueRef, PrincipalId, Timestamp,
};
use worldinterface_runtime::{
    aq_adapter::QueueEngine,
    authority::{Authentication, EvidenceAuthority, TrustedClock},
    egress::EffectService,
    resolution::ResourceObserver,
    storage::StoreBundle,
    Result,
};
use worldinterface_storage::{StoreProfile, StoreRoot};

fn at() -> Timestamp {
    "2026-09-21T00:00:00.000000000Z".to_owned().try_into().unwrap()
}
fn fixture() -> (EffectSubmission, AuthorizationSnapshot) {
    let data: serde_json::Value =
        serde_json::from_str(include_str!("../../../conformance/wi-fabric-2/egress/fixture.json"))
            .unwrap();
    (
        serde_json::from_value(data["submission"].clone()).unwrap(),
        serde_json::from_value(data["snapshot"].clone()).unwrap(),
    )
}
fn publish(root: &StoreRoot) {
    for name in [
        "input",
        "executable",
        "dependency",
        "configuration",
        "profile-definition",
        "adapter",
        "resolution",
        "grant",
        "fixture-review",
        "worker",
    ] {
        root.publish("tenant:fixture".to_owned().try_into().unwrap(), name.as_bytes()).unwrap();
    }
}
struct Auth {
    principal: PrincipalId,
}
impl Authentication for Auth {
    type Session = &'static str;
    fn authenticate(&self, session: &Self::Session) -> Result<(OpaqueRef, PrincipalId)> {
        match *session {
            "good" => Ok(("tenant:fixture".to_owned().try_into()?, self.principal.clone())),
            "cross" => Ok(("tenant:other".to_owned().try_into()?, self.principal.clone())),
            _ => Err("invalid session".into()),
        }
    }
}
struct Clock {
    expired: Cell<bool>,
}
impl TrustedClock for Clock {
    fn now(&self) -> Result<Timestamp> {
        if self.expired.get() {
            Ok("2026-09-23T00:00:00.000000000Z".to_owned().try_into()?)
        } else {
            Ok(at())
        }
    }
}
struct Evidence {
    snapshot: AuthorizationSnapshot,
    revoked: Cell<bool>,
    invalid: Cell<bool>,
}
impl EvidenceAuthority for Evidence {
    fn current_snapshot(
        &self,
        _: &OpaqueRef,
        _: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        Ok(self.snapshot.clone())
    }
    fn verify(&self, _: &OpaqueRef, reference: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        if self.revoked.get() {
            return Err("revoked authority evidence".into());
        }
        if reference == &self.snapshot.facts[0].evidence {
            if self.invalid.get() {
                Ok(ContentHash::of(b"wrong document"))
            } else {
                Ok(self.snapshot.facts[0].attested_document.clone())
            }
        } else {
            Ok(ContentHash::of(b"verified"))
        }
    }
}
struct Observer<'a> {
    root: &'a StoreRoot,
    fact: ResolutionFact,
    changed: Cell<bool>,
    observed_after_intent: Cell<bool>,
}
impl ResourceObserver for Observer<'_> {
    fn observe(
        &self,
        tenant: &OpaqueRef,
        _: &EffectSubmission,
        _: &ResolverSnapshot,
        _: &Timestamp,
    ) -> Result<ResolutionFact> {
        self.observed_after_intent.set(!self.root.effect_snapshot(tenant)?.effects.is_empty());
        if self.changed.get() {
            Err("ambiguous live target".into())
        } else {
            Ok(self.fact.clone())
        }
    }
    fn compare_before_use(&self, _: &ResolutionFact) -> Result<TargetDocument> {
        let mut document = self.fact.document.clone();
        if self.changed.get() {
            match &mut document.target {
                Target::Filesystem { symlink_chain, .. } => {
                    *symlink_chain = ContentHash::of(b"replaced link")
                }
                Target::Network { addresses, redirects, .. } => {
                    addresses.insert("169.254.169.254".to_owned().try_into()?);
                    redirects.push("redirect:metadata".to_owned().try_into()?);
                }
                Target::Communication { audience, .. } => {
                    audience.insert(PrincipalId::generate());
                }
                _ => document.version += 1,
            }
        }
        Ok(document)
    }
}
struct Complete;
impl ExecutorHandler for Complete {
    fn execute(&self, _: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(AqOpaqueRef::new("trusted-host").unwrap()),
    }
}

#[test]
fn production_facade_authenticates_verifies_and_rechecks_live_target() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("a"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    let other = StoreBundle::initialize(
        &temp.path().join("b"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    publish(bundle.wi());
    let (submission, snapshot) = fixture();
    let auth = Auth { principal: snapshot.bindings[0].principal.clone() };
    let evidence = Evidence {
        snapshot: snapshot.clone(),
        revoked: Cell::new(false),
        invalid: Cell::new(false),
    };
    let observer = Observer {
        root: bundle.wi(),
        fact: snapshot.facts[0].clone(),
        changed: Cell::new(false),
        observed_after_intent: Cell::new(false),
    };
    let clock = Clock { expired: Cell::new(false) };
    let service = EffectService {
        root: bundle.wi(),
        authentication: &auth,
        evidence: &evidence,
        resolver: &observer,
        clock: &clock,
    };
    assert!(service.submit(&"forged", submission.clone()).is_err());
    assert!(bundle.wi().effect_snapshot(&snapshot.bindings[0].tenant).unwrap().effects.is_empty());
    evidence.invalid.set(true);
    assert!(service.submit(&"good", submission.clone()).is_err());
    evidence.invalid.set(false);
    evidence.revoked.set(true);
    assert!(service.submit(&"good", submission.clone()).is_err());
    evidence.revoked.set(false);
    let id = service.submit(&"good", submission.clone()).unwrap();
    assert!(service.advance(&"cross", &id).is_err());
    assert!(service.explain(&"forged", &id).is_err());
    let mut conflict = submission.clone();
    conflict.budget.timeout_millis += 1;
    assert!(service.submit(&"good", conflict).is_err());
    assert_eq!(service.submit(&"good", submission.clone()).unwrap(), id);
    let mut wrong_queue = QueueEngine::bootstrap(&other, Complete, host()).unwrap();
    assert!(service.dispatch_pending(&"good", &mut wrong_queue, 1).is_err());
    assert_eq!(wrong_queue.engine().projection().task_count(), 0);
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    assert_eq!(service.advance(&"good", &id).unwrap().phase, Phase::Resolving);
    assert!(observer.observed_after_intent.get());
    assert_eq!(service.advance(&"good", &id).unwrap().phase, Phase::Authorized);
    observer.changed.set(true);
    assert_eq!(service.dispatch_pending(&"good", &mut queue, 1).unwrap().acknowledged, 0);
    assert_eq!(queue.engine().projection().task_count(), 0);
    observer.changed.set(false);
    clock.expired.set(true);
    assert_eq!(service.dispatch_pending(&"good", &mut queue, 1).unwrap().acknowledged, 0);
    assert_eq!(queue.engine().projection().task_count(), 0);
    clock.expired.set(false);
    assert_eq!(service.dispatch_pending(&"good", &mut queue, 1).unwrap().acknowledged, 1);
    assert_eq!(queue.engine().projection().task_count(), 1);
    queue.shutdown().unwrap();
    wrong_queue.shutdown().unwrap();
}

#[test]
fn live_ambiguity_after_intent_never_reaches_aq() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("bundle"),
        StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
    )
    .unwrap();
    publish(bundle.wi());
    let (mut submission, snapshot) = fixture();
    submission.purpose = Some("self-initiated:fixture".to_owned().try_into().unwrap());
    let auth = Auth { principal: snapshot.bindings[0].principal.clone() };
    let evidence = Evidence {
        snapshot: snapshot.clone(),
        revoked: Cell::new(false),
        invalid: Cell::new(false),
    };
    let observer = Observer {
        root: bundle.wi(),
        fact: snapshot.facts[0].clone(),
        changed: Cell::new(true),
        observed_after_intent: Cell::new(false),
    };
    let clock = Clock { expired: Cell::new(false) };
    let service = EffectService {
        root: bundle.wi(),
        authentication: &auth,
        evidence: &evidence,
        resolver: &observer,
        clock: &clock,
    };
    let id = service.submit(&"good", submission).unwrap();
    let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
    assert_ne!(service.advance(&"good", &id).unwrap().phase, Phase::Authorized);
    assert!(observer.observed_after_intent.get());
    assert_eq!(service.dispatch_pending(&"good", &mut queue, 1).unwrap().attempted, 0);
    assert_eq!(queue.engine().projection().task_count(), 0);
    queue.shutdown().unwrap();
}

#[test]
fn production_facade_rejects_resource_boundaries_before_admission() {
    use std::collections::BTreeSet;
    for case in [
        "path_traversal",
        "symlink_replacement",
        "private_dns",
        "redirect",
        "dns_rebinding",
        "audience",
        "registry_ambiguity",
        "tenant_ambiguity",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let bundle = StoreBundle::initialize(
            &temp.path().join("bundle"),
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap();
        publish(bundle.wi());
        let (mut submission, mut snapshot) = fixture();
        snapshot.worker = None;
        snapshot.fixture = None;
        let namespace = match case {
            "path_traversal" | "symlink_replacement" => "filesystem",
            "private_dns" | "redirect" | "dns_rebinding" => "network",
            "audience" => "communication",
            _ => "worker",
        };
        submission.resource.namespace = namespace.to_owned().try_into().unwrap();
        snapshot.registry.entries[0].namespace = namespace.to_owned().try_into().unwrap();
        snapshot.facts[0].selector = submission.resource.clone();
        snapshot.facts[0].document.target = match case {
            "path_traversal" | "symlink_replacement" => Target::Filesystem {
                root: "root:fixture".to_owned().try_into().unwrap(),
                components: vec![if case == "path_traversal" { ".." } else { "safe" }.into()],
                symlink_chain: ContentHash::of(b"original link"),
            },
            "private_dns" | "redirect" | "dns_rebinding" => Target::Network {
                scheme: "https".to_owned().try_into().unwrap(),
                host: "example.org".to_owned().try_into().unwrap(),
                port: 443,
                addresses: BTreeSet::from([if case == "private_dns" {
                    "169.254.169.254"
                } else {
                    "93.184.215.14"
                }
                .to_owned()
                .try_into()
                .unwrap()]),
                redirects: if case == "redirect" {
                    vec!["redirect:metadata".to_owned().try_into().unwrap()]
                } else {
                    vec![]
                },
                path: "/".into(),
            },
            "audience" => Target::Communication {
                account: "account:fixture".to_owned().try_into().unwrap(),
                sender: snapshot.bindings[0].principal.clone(),
                channel: "channel:fixture".to_owned().try_into().unwrap(),
                thread: None,
                audience: BTreeSet::new(),
                visibility: "private".to_owned().try_into().unwrap(),
            },
            _ => snapshot.facts[0].document.target.clone(),
        };
        if case == "tenant_ambiguity" {
            snapshot.facts[0].document.tenant = "tenant:other".to_owned().try_into().unwrap();
        }
        if case == "registry_ambiguity" {
            snapshot.registry.entries.push(snapshot.registry.entries[0].clone());
        }
        snapshot.facts[0].registry =
            worldinterface_domain::ingress::digest(&snapshot.registry).unwrap();
        snapshot.facts[0].attested_document =
            worldinterface_domain::ingress::digest(&snapshot.facts[0].document).unwrap();
        snapshot.policy.targets = BTreeSet::from([snapshot.facts[0].attested_document.clone()]);
        let auth = Auth { principal: snapshot.bindings[0].principal.clone() };
        let evidence = Evidence {
            snapshot: snapshot.clone(),
            revoked: Cell::new(false),
            invalid: Cell::new(false),
        };
        let observer = Observer {
            root: bundle.wi(),
            fact: snapshot.facts[0].clone(),
            changed: Cell::new(false),
            observed_after_intent: Cell::new(false),
        };
        let clock = Clock { expired: Cell::new(false) };
        let service = EffectService {
            root: bundle.wi(),
            authentication: &auth,
            evidence: &evidence,
            resolver: &observer,
            clock: &clock,
        };
        let id = service.submit(&"good", submission).unwrap();
        let mut queue = QueueEngine::bootstrap(&bundle, Complete, host()).unwrap();
        let first = service.advance(&"good", &id).unwrap();
        if case == "symlink_replacement" || case == "dns_rebinding" {
            assert_eq!(first.phase, Phase::Resolving, "{case}");
            observer.changed.set(true);
            assert!(service.advance(&"good", &id).is_err(), "{case}");
        } else {
            assert_eq!(first.phase, Phase::Failed, "{case}");
            assert_ne!(service.advance(&"good", &id).unwrap().phase, Phase::Authorized, "{case}");
        }
        assert_eq!(
            service.dispatch_pending(&"good", &mut queue, 1).unwrap().acknowledged,
            0,
            "{case}"
        );
        assert_eq!(queue.engine().projection().task_count(), 0, "{case}");
        queue.shutdown().unwrap();
    }
}
