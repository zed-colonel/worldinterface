#![allow(dead_code)]
//! Trusted provisioning is deliberately separate from embedded_consumer.rs.
use super::{
    flow_fixture::Config,
    worker_fixture::{self, Approval, Harness, HostClock},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
};
use worldinterface_domain::{
    agency::AuthenticatedCaller, authorization::*, control_plane as cp, flow::*,
};
use worldinterface_protocol::{control_plane::*, *};
use worldinterface_runtime::{
    authority::*,
    embedded::*,
    resolution::*,
    worker::{connector::descriptor, evaluation::EvaluationVerifier},
    worker_process::*,
    Result,
};
use worldinterface_storage::StoreRoot;
#[path = "../../../worldinterface-foundation/domain/tests/support/control_plane.rs"]
mod cp_setup;
#[derive(Clone)]
pub struct Auth {
    pub tenant: OpaqueRef,
    pub principal: PrincipalId,
}
#[derive(Clone)]
pub enum Session {
    Valid,
    ForeignTenant,
    ForeignPrincipal,
}
impl Authentication for Auth {
    type Session = Session;
    fn authenticate(&self, s: &Session) -> Result<(OpaqueRef, PrincipalId)> {
        Ok(match s {
            Session::Valid => (self.tenant.clone(), self.principal.clone()),
            Session::ForeignTenant => {
                ("other-tenant".to_string().try_into()?, self.principal.clone())
            }
            Session::ForeignPrincipal => (self.tenant.clone(), PrincipalId::generate()),
        })
    }
}
#[derive(Clone)]
pub struct Evidence(pub Arc<Mutex<Vec<(EffectSubmission, AuthorizationSnapshot)>>>);
impl EvidenceAuthority for Evidence {
    fn current_snapshot(
        &self,
        t: &OpaqueRef,
        s: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|(a, b)| a.input == s.input && b.bindings[0].tenant == *t)
            .map(|(_, b)| b.clone())
            .ok_or_else(|| "unregistered embedded effect".into())
    }
    fn verify(&self, t: &OpaqueRef, e: &EvidenceRef, at: &Timestamp) -> Result<ContentHash> {
        let mut verified = None;
        for (_, s) in self.0.lock().unwrap().iter() {
            if let Ok(v) = worker_fixture::HostEvidence(s.clone()).verify(t, e, at) {
                if verified.as_ref().is_some_and(|previous| previous != &v) {
                    return Err("ambiguous embedded evidence".into());
                }
                verified = Some(v);
            }
        }
        verified.ok_or_else(|| "unregistered embedded evidence".into())
    }
}
fn attest_resolutions(root: &StoreRoot, snapshot: &mut AuthorizationSnapshot) {
    for fact in &mut snapshot.facts {
        // A singleton harness can use a placeholder attestation. A shared host
        // needs an immutable reference for each exact operation's target.
        fact.evidence.artifact = root
            .publish(fact.document.tenant.clone(), &canonical_data(&fact.document).unwrap())
            .unwrap();
        fact.attested_document = fact.evidence.artifact.content.clone();
    }
}
#[derive(Clone)]
pub struct Resources(pub Evidence);
impl ResourceObserver for Resources {
    fn observe(
        &self,
        t: &OpaqueRef,
        s: &EffectSubmission,
        _: &worldinterface_domain::resource::ResolverSnapshot,
        _: &Timestamp,
    ) -> Result<worldinterface_domain::resource::ResolutionFact> {
        Ok(self.0.current_snapshot(t, s)?.facts[0].clone())
    }
    fn compare_before_use(
        &self,
        f: &worldinterface_domain::resource::ResolutionFact,
    ) -> Result<worldinterface_domain::resource::TargetDocument> {
        Ok(f.document.clone())
    }
}
pub struct Vault;
impl cp::Custody for Vault {
    fn deliver(
        &self,
        _: &OpaqueRef,
        _: &CredentialMetadata,
    ) -> std::result::Result<(), CustodyReason> {
        Ok(())
    }
    fn revoke(&self, _: &ObjectRef, _: u64, _: u64) -> std::result::Result<(), CustodyReason> {
        Ok(())
    }
}
pub type Host = EmbeddedHost<Auth, Evidence, Resources, HostClock, Config, Vault, Approval>;
pub struct Setup {
    pub host: Host,
    pub demands: Vec<(EffectSubmission, DirectOperationSelection)>,
    pub worker: ObjectRef,
    pub scope: Scope,
    pub provider: OpaqueRef,
    pub run: OpaqueRef,
    pub expected: Vec<(ObjectRef, Vec<u8>)>,
    pub root: std::path::PathBuf,
    pub evaluation: Arc<EvaluationVerifier<Approval>>,
    pub evidence: Evidence,
    pub config: Config,
    pub auth: Auth,
    pub temp: tempfile::TempDir,
}
impl Setup {
    pub fn live(coding: bool) -> Self {
        Self::live_case(coding, "WP-029")
    }
    pub fn live_case(coding: bool, case: &str) -> Self {
        let temp = std::env::var_os("WI_EMBEDDED_CRASH_PARENT")
            .map(|p| tempfile::tempdir_in(p).unwrap())
            .unwrap_or_else(|| tempfile::tempdir().unwrap());
        let mut h = Harness::at(
            temp,
            if coding { Task::Coding } else { Task::Transform { xor: 7 } },
            case,
            1500,
            65536,
        );
        let expected: Vec<(&str, &str, Vec<u8>)> = if coding {
            vec![("patch","change.patch",b"--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -10,10 +10,10 @@\n     Some((key.trim(), value.trim()))\n }\n \n-/// Calculate the averge of a slice of numbers.\n+/// Calculate the average of a slice of numbers.\n ///\n /// Returns 0.0 for an empty slice.\n-pub fn calculate_averge(numbers: &[f64]) -> f64 {\n+pub fn calculate_average(numbers: &[f64]) -> f64 {\n     if numbers.is_empty() {\n         return 0.0;\n     }\n".to_vec()),("report","report.json",b"{\"changed\":[\"src/lib.rs\"],\"task\":\"rename-average\",\"schema\":1}".to_vec())]
        } else {
            h.inputs = vec![h.bundle.wi().publish(h.auth.0.clone(), b"hello").unwrap()];
            vec![("result", "output.bin", b"hello".iter().map(|b| b ^ 7).collect())]
        };
        h.exports = expected
            .iter()
            .map(|(r, n, b)| Export {
                reference: (*r).into(),
                name: (*n).into(),
                digest: ContentHash::of(b),
                size: b.len() as u64,
            })
            .collect();
        let mut records = worldinterface_runtime::worker::admission::OPERATIONS
            .iter()
            .map(|op| h.demand(*op))
            .collect::<Vec<_>>();
        let scope = Scope { tenant: h.auth.0.clone(), organization: None };
        let provider: OpaqueRef = "local-proof".to_string().try_into().unwrap();
        let run: OpaqueRef = h.run.clone().try_into().unwrap();
        let worker = ObjectRef {
            scope: scope.clone(),
            kind: ObjectKind::Worker,
            id: worker_id(&provider, &run).unwrap(),
        };
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(_, _, b)| {
                let a = ArtifactRef {
                    scope: scope.tenant.clone(),
                    content: ContentHash::of(&b),
                    size: b.len() as u64,
                };
                (artifact_reference(scope.clone(), &a), b)
            })
            .collect();
        let mut permissions = BTreeSet::new();
        let mut targets = BTreeSet::new();
        for (submission, s, _) in &records {
            permissions.extend(s.bindings[0].principal_authority.clone());
            targets.extend(s.policy.targets.clone());
            permissions.insert(
                cp::read_demand(&artifact_reference(scope.clone(), &submission.input.artifact))
                    .unwrap(),
            );
            for a in &s.required_outputs {
                permissions.insert(cp::read_demand(&artifact_reference(scope.clone(), a)).unwrap());
            }
        }
        permissions.insert(cp::read_demand(&worker).unwrap());
        for (r, _) in &expected {
            permissions.insert(cp::read_demand(r).unwrap());
        }
        for (_, s, _) in &mut records {
            cp_setup::set_authority(s, &permissions);
            s.policy.targets = targets.clone();
            s.policy.authorizer = s.bindings[0].principal.clone();
            s.policy.grant_issuers.insert(s.bindings[0].accountable.clone());
            h.evaluation.trusted.approve_policy(&s.policy);
        }
        let mut pins = records[0].2 .0.clone();
        pins.descriptors = worldinterface_runtime::worker::admission::OPERATIONS
            .iter()
            .map(|op| descriptor(*op).unwrap())
            .collect();
        let alternatives = records
            .iter()
            .flat_map(|(_, _, c)| c.0.run_authority.alternatives.clone())
            .collect::<Vec<_>>();
        pins.run_authority.alternatives = alternatives.clone();
        pins.submitting_authority.alternatives = alternatives;
        let config = Config(pins);
        let evidence = Evidence(Arc::new(Mutex::new(
            records.iter().map(|(a, b, _)| (a.clone(), b.clone())).collect(),
        )));
        let registry =
            worldinterface_runtime::connector::ConnectorRegistry::new(config.0.descriptors.clone())
                .unwrap();
        for (_, snapshot, _) in &mut records {
            let op = snapshot.worker.as_ref().unwrap().operation.clone();
            let binding = registry
                .bindings()
                .unwrap()
                .into_iter()
                .find(|b| b.descriptor.operation == op)
                .unwrap();
            snapshot.registry.entries[0].connector_binding = Some(binding);
            snapshot.facts[0].registry =
                worldinterface_domain::ingress::digest(&snapshot.registry).unwrap();
            attest_resolutions(h.bundle.wi(), snapshot);
        }
        let mut boot = cp_setup::bundle(&records[0].1, Profile::Embedded);
        boot.objects.retain(|(_, c)| !matches!(c, cp::Content::Secret(_)));
        *evidence.0.lock().unwrap() =
            records.iter().map(|(a, b, _)| (a.clone(), b.clone())).collect();
        let demands = records
            .into_iter()
            .zip(worldinterface_runtime::worker::admission::OPERATIONS)
            .map(|((s, _, _), op)| {
                (
                    s,
                    DirectOperationSelection {
                        connector: descriptor(op).unwrap().connector,
                        operation: op.name().to_string().try_into().unwrap(),
                    },
                )
            })
            .collect();
        let auth = Auth { tenant: h.auth.0.clone(), principal: h.auth.1.clone() };
        let root = h.root.clone();
        let evaluation = h.evaluation.clone();
        let built = super::worker_fixture::embedded_build();
        drop(h.runtime);
        let bundle = Arc::try_unwrap(h.bundle).ok().expect("exclusive test composition");
        bundle
            .wi()
            .bootstrap_control_plane(boot, HostClock.now().unwrap().epoch_millis().unwrap())
            .unwrap();
        for (submission, snapshot) in evidence.0.lock().unwrap().iter() {
            for a in
                std::iter::once(&submission.input.artifact).chain(snapshot.required_outputs.iter())
            {
                if !bundle
                    .wi()
                    .control_plane_snapshot()
                    .unwrap()
                    .objects
                    .iter()
                    .any(|o| o.reference == artifact_reference(scope.clone(), a))
                {
                    bundle
                        .wi()
                        .register_control_object(ObjectView {
                            reference: artifact_reference(scope.clone(), a),
                            revision: 1,
                            content: ContentHash::of(&canonical_data(a).unwrap()),
                            artifact: Some(a.clone()),
                        })
                        .unwrap();
                }
            }
        }
        let host = Host::install(
            bundle,
            WorkerInstallation::Process {
                scope: HostScope {
                    tenant: auth.tenant.as_str().into(),
                    principal: auth.principal.as_str().into(),
                },
                workspace: root.join("process"),
                fixture: built.0,
                helper: built.1,
                evaluation: evaluation.clone(),
                slots: 1,
            },
            EmbeddedPorts {
                authentication: auth.clone(),
                evidence: evidence.clone(),
                resources: Resources(evidence.clone()),
                clock: HostClock,
                configuration: config.clone(),
                custody: Vault,
                credential_recipient: "reviewed-recipient".to_string().try_into().unwrap(),
            },
            None,
        )
        .unwrap();
        Self {
            host,
            demands,
            worker,
            scope,
            provider,
            run,
            expected,
            root,
            evaluation,
            evidence,
            config,
            auth,
            temp: h.temp,
        }
    }
    /// After execution, separately authenticated trusted provisioning grants exact
    /// metadata disclosure. It never executes a consumer operation or reads its bytes.
    pub fn disclose(&self) {
        let root = StoreRoot::open_existing(self.root.join("bundle/wi")).unwrap();
        let state = root.control_plane_snapshot().unwrap();
        let caller = AuthenticatedCaller::from_trusted_authentication(
            self.auth.tenant.clone(),
            self.auth.principal.clone(),
        );
        let permissions = state
            .objects
            .iter()
            .map(|o| cp::read_demand(&o.reference).unwrap())
            .collect::<BTreeSet<_>>();
        let mut revisions = state
            .history
            .iter()
            .rev()
            .filter(|r| {
                matches!(
                    r.content,
                    cp::Content::Policy(_) | cp::Content::Binding(_) | cp::Content::Grant(_)
                )
            })
            .fold(BTreeMap::new(), |mut a, r| {
                a.entry(r.reference.clone()).or_insert(r.clone());
                a
            });
        for r in revisions.values_mut() {
            let mut content = r.content.clone();
            match &mut content {
                cp::Content::Policy(p) => {
                    p.platform.extend(permissions.clone());
                    p.resource.extend(permissions.clone());
                    p.information_use.extend(permissions.clone());
                    self.evaluation.trusted.approve_policy(p);
                }
                cp::Content::Binding(b) => {
                    b.version += 1;
                    b.principal_authority.extend(permissions.clone());
                    b.capacity_authority.extend(permissions.clone());
                }
                cp::Content::Grant(g) => {
                    g.version += 1;
                    g.revoked = false;
                    g.authority.extend(permissions.clone());
                }
                _ => unreachable!(),
            }
            let at = HostClock.now().unwrap().epoch_millis().unwrap();
            let command = cp::AdminCommand {
                request: format!("disclose-{}-{}", r.reference.id.as_str(), r.revision + 1)
                    .try_into()
                    .unwrap(),
                reference: r.reference.clone(),
                expected_revision: r.revision,
                content,
                claims: Default::default(),
                approvals: vec![],
            };
            root.authorize_control_administration(
                &caller,
                &caller,
                command.clone(),
                at + 120000,
                at,
            )
            .unwrap();
            use cp::Repository;
            root.control_plane()
                .execute(
                    &caller,
                    &cp::Request {
                        id: command.request.clone(),
                        operation: cp::Operation::Admin { command: Box::new(command) },
                    },
                    at,
                )
                .unwrap();
        }
    }
}

pub struct RecoverySetup {
    pub host: Host,
    pub temp: tempfile::TempDir,
    pub artifact: ObjectRef,
    pub auth: Auth,
    pub snapshot: AuthorizationSnapshot,
}
impl RecoverySetup {
    pub fn new() -> Self {
        use worldinterface_runtime::storage::StoreBundle;
        let temp = tempfile::tempdir().unwrap();
        let clock = HostClock;
        let bundle = StoreBundle::initialize(
            &temp.path().join("bundle"),
            worldinterface_storage::StoreProfile {
                instance: InstanceId::generate(),
                created_at: clock.now().unwrap(),
                platform: false,
            },
        )
        .unwrap();
        let (_, mut snapshot) = worker_fixture::admission_fixture(
            &descriptor(worldinterface_worker::mapping::Operation::Start).unwrap(),
            &clock.now().unwrap(),
        );
        let scope = Scope { tenant: snapshot.bindings[0].tenant.clone(), organization: None };
        let bytes = b"scoped artifact";
        let a = bundle.wi().publish(scope.tenant.clone(), bytes).unwrap();
        let artifact = artifact_reference(scope.clone(), &a);
        let authority = BTreeSet::from([cp::read_demand(&artifact).unwrap()]);
        cp_setup::set_authority(&mut snapshot, &authority);
        snapshot.policy.authorizer = snapshot.bindings[0].principal.clone();
        snapshot.policy.grant_issuers.insert(snapshot.bindings[0].accountable.clone());
        let mut boot = cp_setup::bundle(&snapshot, Profile::Embedded);
        boot.objects.retain(|(_, c)| !matches!(c, cp::Content::Secret(_)));
        bundle
            .wi()
            .bootstrap_control_plane(boot, clock.now().unwrap().epoch_millis().unwrap())
            .unwrap();
        bundle
            .wi()
            .register_control_object(ObjectView {
                reference: artifact.clone(),
                revision: 1,
                content: ContentHash::of(bytes),
                artifact: Some(a),
            })
            .unwrap();
        let auth = Auth { tenant: scope.tenant, principal: snapshot.bindings[0].principal.clone() };
        let evidence = Evidence(Arc::new(Mutex::new(vec![])));
        let config = Config(worldinterface_storage::flow::FlowPins {
            descriptors: worldinterface_runtime::worker::admission::OPERATIONS
                .iter()
                .map(|op| descriptor(*op).unwrap())
                .collect(),
            schemas: BTreeMap::new(),
            submitting_authority: AuthorityUpperBound { alternatives: vec![] },
            run_authority: AuthorityUpperBound { alternatives: vec![] },
        });
        let workspace = temp.path().join("recovery-only");
        std::fs::create_dir(&workspace).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o700)).unwrap();
        let scope = HostScope {
            tenant: auth.tenant.as_str().into(),
            principal: auth.principal.as_str().into(),
        };
        let implementation =
            descriptor(worldinterface_worker::mapping::Operation::Start).unwrap().implementation;
        let policy = worldinterface_runtime::worker::catalog::RecoveryPolicy::new(
            scope.clone(),
            implementation.clone(),
            vec![implementation],
            ContentHash::of(b"recovery-only"),
        )
        .unwrap();
        let host = Host::install(
            bundle,
            WorkerInstallation::Recovery { scope, workspace, policy },
            EmbeddedPorts {
                authentication: auth.clone(),
                evidence: evidence.clone(),
                resources: Resources(evidence),
                clock,
                configuration: config,
                custody: Vault,
                credential_recipient: "recipient".to_string().try_into().unwrap(),
            },
            None,
        )
        .unwrap();
        Self { host, temp, artifact, auth, snapshot }
    }
    pub fn revoke(&self) {
        let root = StoreRoot::open_existing(self.temp.path().join("bundle/wi")).unwrap();
        let state = root.control_plane_snapshot().unwrap();
        let r = state
            .history
            .iter()
            .rev()
            .find(|r| matches!(r.content, cp::Content::Grant(_)))
            .unwrap();
        let cp::Content::Grant(mut g) = r.content.clone() else { unreachable!() };
        g.version += 1;
        g.revoked = true;
        let caller = AuthenticatedCaller::from_trusted_authentication(
            self.auth.tenant.clone(),
            self.auth.principal.clone(),
        );
        let at = HostClock.now().unwrap().epoch_millis().unwrap();
        let command = cp::AdminCommand {
            request: "revoke".to_string().try_into().unwrap(),
            reference: r.reference.clone(),
            expected_revision: r.revision,
            content: cp::Content::Grant(g),
            claims: Default::default(),
            approvals: vec![],
        };
        root.authorize_control_administration(&caller, &caller, command.clone(), at + 120000, at)
            .unwrap();
        use cp::Repository;
        root.control_plane()
            .execute(
                &caller,
                &cp::Request {
                    id: command.request.clone(),
                    operation: cp::Operation::Admin { command: Box::new(command) },
                },
                at,
            )
            .unwrap();
    }
    pub fn writer_exclusion(&self) {
        let root = StoreRoot::open_existing(self.temp.path().join("bundle/wi")).unwrap();
        assert!(root.claim_embedded_host().is_err());
        assert!(worldinterface_runtime::storage::StoreBundle::backup(
            &self.temp.path().join("bundle"),
            &self.temp.path().join("busy")
        )
        .is_err());
    }
}

impl Setup {
    pub fn restored(self) -> Self {
        let Self {
            host,
            demands,
            worker,
            scope,
            provider,
            run,
            expected,
            root,
            evaluation,
            evidence,
            config,
            auth,
            temp,
        } = self;
        drop(host);
        use worldinterface_runtime::storage::StoreBundle;
        StoreBundle::backup(&root.join("bundle"), &root.join("backup")).unwrap();
        StoreBundle::restore(&root.join("backup"), &root.join("restored-bundle")).unwrap();
        // No restored artifact read may depend on this original private workspace.
        std::fs::remove_dir_all(root.join("process")).unwrap();
        let workspace = root.join("restored-process");
        std::fs::create_dir(&workspace).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o700)).unwrap();
        let host = recovery_host(
            StoreBundle::open(&root.join("restored-bundle")).unwrap(),
            &workspace,
            auth.clone(),
            evidence.clone(),
            config.clone(),
        );
        Self {
            host,
            demands,
            worker,
            scope,
            provider,
            run,
            expected,
            root,
            evaluation,
            evidence,
            config,
            auth,
            temp,
        }
    }
}
fn recovery_host(
    bundle: worldinterface_runtime::storage::StoreBundle,
    workspace: &Path,
    auth: Auth,
    evidence: Evidence,
    configuration: Config,
) -> Host {
    let scope = HostScope {
        tenant: auth.tenant.as_str().into(),
        principal: auth.principal.as_str().into(),
    };
    let implementation =
        descriptor(worldinterface_worker::mapping::Operation::Start).unwrap().implementation;
    let policy = worldinterface_runtime::worker::catalog::RecoveryPolicy::new(
        scope.clone(),
        implementation.clone(),
        vec![implementation],
        ContentHash::of(b"wi-worker-recovery:1"),
    )
    .unwrap();
    Host::install(
        bundle,
        WorkerInstallation::Recovery { scope, workspace: workspace.into(), policy },
        EmbeddedPorts {
            authentication: auth,
            evidence: evidence.clone(),
            resources: Resources(evidence),
            clock: HostClock,
            configuration,
            custody: Vault,
            credential_recipient: "recipient".to_string().try_into().unwrap(),
        },
        None,
    )
    .unwrap()
}
pub async fn crash_child() {
    if std::env::var_os("WI_EMBEDDED_CRASH_PARENT").is_none() {
        return;
    }
    let s = Setup::live(false);
    let parent = std::path::PathBuf::from(std::env::var_os("WI_EMBEDDED_CRASH_PARENT").unwrap());
    std::fs::write(parent.join("location"), s.root.to_str().unwrap()).unwrap();
    let mut app = s.host.application().unwrap();
    let (submission, selection) = s.demands[1].clone();
    super::embedded_consumer::operation(
        &mut app,
        &Session::Valid,
        submission,
        selection,
        &s.worker,
        &s.provider,
        &s.run,
    )
    .await;
    panic!("crash cut not reached")
}
pub fn crash_recovery() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    // Compilation has its own timeout and must precede the crash-cut deadline.
    // This launches no fixture and grants no evaluation approval.
    worker_fixture::prebuild();
    let parent = tempfile::tempdir().unwrap();
    let ready = parent.path().join("ready");
    let log = std::fs::File::create(parent.path().join("child.log")).unwrap();
    let mut child = super::child::KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "embedded_worker_crash_child", "--nocapture"])
            .env("WI_EMBEDDED_CRASH_PARENT", parent.path())
            .env("WI_CRASH_POINT", "worker-helper-before-spawn")
            .env("WI_CRASH_READY", &ready)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ready.is_file() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "child exited {status}: {}",
                std::fs::read_to_string(parent.path().join("child.log")).unwrap()
            );
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let directory =
        std::path::PathBuf::from(std::fs::read_to_string(parent.path().join("location")).unwrap());
    let bundle =
        worldinterface_runtime::storage::StoreBundle::open(&directory.join("bundle")).unwrap();
    let tenant = cp_setup::o("tenant:fixture");
    let state = bundle.wi().effect_snapshot(&tenant).unwrap();
    let effect = state.effects.values().next().unwrap();
    let auth = Auth { tenant: tenant.clone(), principal: effect.agency.binding.principal.clone() };
    let evidence = Evidence(Arc::new(Mutex::new(vec![(
        effect.intent.submission.clone(),
        effect.authorization_inputs.clone().unwrap(),
    )])));
    let config = Config(
        bundle.wi().flow_snapshot(&tenant).unwrap().runs.values().next().unwrap().pins.clone(),
    );
    let host = recovery_host(bundle, &directory.join("process"), auth, evidence, config);
    let app = host.application().unwrap();
    let provider = cp_setup::o("local-proof");
    let run = cp_setup::o("run-proof");
    let reference = ObjectRef {
        scope: Scope { tenant, organization: None },
        kind: ObjectKind::Worker,
        id: worker_id(&provider, &run).unwrap(),
    };
    let worker = app.inspect_worker(&Session::Valid, &reference, &provider, &run).unwrap();
    assert!(worker.quarantined && worker.capacity_reserved && !worker.cleanup_confirmed);
    app.shutdown().unwrap();
}
impl Setup {
    pub fn receipts(&self) -> Vec<ObjectRef> {
        StoreRoot::open_existing(self.root.join("bundle/wi"))
            .unwrap()
            .control_plane_snapshot()
            .unwrap()
            .objects
            .into_iter()
            .filter(|o| o.reference.kind == ObjectKind::Receipt)
            .map(|o| o.reference)
            .collect()
    }
}
impl Setup {
    pub fn quarantine(&self) {
        let root = StoreRoot::open_existing(self.root.join("bundle/wi")).unwrap();
        root.quarantine_worker_provider(quarantine_record(
            &self.auth,
            &self.provider,
            &self.config.0.descriptors,
        ))
        .unwrap();
    }
    pub fn revoke(&self) {
        let root = StoreRoot::open_existing(self.root.join("bundle/wi")).unwrap();
        let state = root.control_plane_snapshot().unwrap();
        let revision = state
            .history
            .iter()
            .rev()
            .find(|r| matches!(r.content, cp::Content::Grant(_)))
            .unwrap();
        let cp::Content::Grant(mut g) = revision.content.clone() else { unreachable!() };
        g.version += 1;
        g.revoked = true;
        let caller = AuthenticatedCaller::from_trusted_authentication(
            self.auth.tenant.clone(),
            self.auth.principal.clone(),
        );
        let at = HostClock.now().unwrap().epoch_millis().unwrap();
        let command = cp::AdminCommand {
            request: "withdraw-execution".to_string().try_into().unwrap(),
            reference: revision.reference.clone(),
            expected_revision: revision.revision,
            content: cp::Content::Grant(g),
            claims: Default::default(),
            approvals: vec![],
        };
        root.authorize_control_administration(&caller, &caller, command.clone(), at + 120000, at)
            .unwrap();
        use cp::Repository;
        root.control_plane()
            .execute(
                &caller,
                &cp::Request {
                    id: command.request.clone(),
                    operation: cp::Operation::Admin { command: Box::new(command) },
                },
                at,
            )
            .unwrap();
    }
}
impl RecoverySetup {
    pub fn persist_unprojected_flow(&self) -> ObjectRef {
        let fixture = super::flow_fixture::Fixture::new();
        let bundle =
            worldinterface_runtime::storage::StoreBundle::open(&self.temp.path().join("bundle"))
                .unwrap();
        for bytes in [b"input".as_slice(), b"resolution", b"grant"] {
            bundle.wi().publish(self.auth.tenant.clone(), bytes).unwrap();
        }
        let mut submission = fixture.submission.clone();
        let input = fixture.bundle.wi().read_artifact(&submission.input.artifact).unwrap();
        submission.input.artifact = bundle.wi().publish(self.auth.tenant.clone(), &input).unwrap();
        let evidence = super::connectors::Evidence(fixture.snapshot.clone());
        let mut queue = worldinterface_runtime::aq_adapter::QueueEngine::bootstrap(
            &bundle,
            super::spi_harness::Noop,
            super::spi_harness::host(),
        )
        .unwrap();
        let service = worldinterface_runtime::flow::PlanService {
            root: bundle.wi(),
            authentication: &fixture.auth,
            evidence: &evidence,
            clock: &fixture.clock,
            configuration: &fixture.config,
        };
        let id = service.submit_direct(&(), submission, fixture.selection(), &mut queue).unwrap();
        queue.shutdown().unwrap();
        let reference = ObjectRef {
            scope: Scope { tenant: self.auth.tenant.clone(), organization: None },
            kind: ObjectKind::Flow,
            id,
        };
        assert!(!bundle
            .wi()
            .control_plane_snapshot()
            .unwrap()
            .objects
            .iter()
            .any(|o| o.reference == reference));
        reference
    }
    pub fn assert_flow_projected(&self, reference: &ObjectRef) {
        let root = StoreRoot::open_existing(self.temp.path().join("bundle/wi")).unwrap();
        assert!(root
            .control_plane_snapshot()
            .unwrap()
            .objects
            .iter()
            .any(|o| &o.reference == reference));
    }
}

fn quarantine_record(
    auth: &Auth,
    provider: &OpaqueRef,
    descriptors: &[ConnectorDescriptor],
) -> worldinterface_domain::worker::WorkerProviderQuarantine {
    worldinterface_domain::worker::WorkerProviderQuarantine {
        version: 1,
        tenant: auth.tenant.clone(),
        provider: provider.clone(),
        revision: 1,
        affected: vec![ContentHash::of(&canonical_data(&descriptors).unwrap())],
        reason: "reviewed-incident".to_string().try_into().unwrap(),
        evidence: ContentHash::of(b"incident"),
        attributed_to: auth.principal.clone(),
    }
}
#[test]
fn embedded_trusted_quarantine_setup_records_the_exact_descriptor_snapshot() {
    let setup = RecoverySetup::new();
    let provider = cp_setup::o("local-proof");
    let descriptors = worldinterface_runtime::worker::admission::OPERATIONS
        .iter()
        .map(|op| descriptor(*op).unwrap())
        .collect::<Vec<_>>();
    let record = quarantine_record(&setup.auth, &provider, &descriptors);
    let root = StoreRoot::open_existing(setup.temp.path().join("bundle/wi")).unwrap();
    root.quarantine_worker_provider(record.clone()).unwrap();
    assert_eq!(
        root.worker_provider_quarantine(&setup.auth.tenant, &provider).unwrap(),
        Some(record)
    );
}

#[test]
fn embedded_resolution_evidence_attests_each_operation_independently() {
    let temp = tempfile::tempdir().unwrap();
    let root = StoreRoot::create_empty(
        temp.path().join("wi"),
        worldinterface_storage::StoreProfile {
            instance: InstanceId::generate(),
            created_at: HostClock.now().unwrap(),
            platform: false,
        },
    )
    .unwrap();
    let records = worldinterface_runtime::worker::admission::OPERATIONS
        .iter()
        .map(|operation| {
            let (mut submission, mut snapshot) =
                super::connectors::fixture(&descriptor(*operation).unwrap());
            submission.input.artifact = root
                .publish(snapshot.bindings[0].tenant.clone(), operation.name().as_bytes())
                .unwrap();
            for bytes in [b"resolution".as_slice(), b"grant"] {
                root.publish(snapshot.bindings[0].tenant.clone(), bytes).unwrap();
            }
            attest_resolutions(&root, &mut snapshot);
            (submission, snapshot)
        })
        .collect::<Vec<_>>();
    let evidence = Evidence(Arc::new(Mutex::new(records.clone())));
    let mut references = BTreeSet::new();
    for (submission, snapshot) in &records {
        let fact = &snapshot.facts[0];
        assert!(references.insert(fact.evidence.artifact.content.clone()));
        assert_eq!(
            root.read_artifact(&fact.evidence.artifact).unwrap(),
            canonical_data(&fact.document).unwrap()
        );
        verify_snapshot(
            &root,
            &evidence,
            &snapshot.bindings[0].tenant,
            submission,
            &HostClock.now().unwrap(),
        )
        .unwrap();
    }
    // An attestation for another operation must never authorize this target.
    evidence.0.lock().unwrap()[1].1.facts[0].evidence = records[0].1.facts[0].evidence.clone();
    let error = evidence
        .verify(
            &records[0].1.bindings[0].tenant,
            &records[0].1.facts[0].evidence,
            &HostClock.now().unwrap(),
        )
        .unwrap_err();
    assert_eq!(error.to_string(), "ambiguous embedded evidence");
}
