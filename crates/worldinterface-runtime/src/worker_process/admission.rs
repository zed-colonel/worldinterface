//! Host-only composition. None of these values may be constructed from worker requests.
use super::{launch::Executable, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use worldinterface_protocol::ContentHash;
use worldinterface_worker::{
    controls::{ControlEvidenceV1, ControlKind, ControlState},
    credential::{CredentialBindingV1, CredentialExpectation},
    mapping::{Operation, ResolvedWorkerV1, WorkerRequestV1},
};

/// Typed reviewed fixture arguments; no arbitrary program, flag, environment or path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "task", rename_all = "snake_case")]
pub enum Task {
    Coding,
    Transform { xor: u8 },
    Probe,
    Sleep { millis: u64 },
    Flood { bytes: u64 },
    Tree { early_exit: bool, change_session: bool, resist_term: bool },
    Churn { children: u16 },
}
impl Task {
    pub(super) fn validate(&self) -> Result<()> {
        match self {
            Self::Sleep { millis } if *millis > 60_000 => {
                Err("typed duration exceeds bound".into())
            }
            Self::Flood { bytes } if *bytes > 16 * 1024 * 1024 => {
                Err("typed output exceeds bound".into())
            }
            Self::Churn { children } if *children > 1024 => {
                Err("typed child count exceeds bound".into())
            }
            _ => Ok(()),
        }
    }
    pub(super) fn args(&self) -> Vec<String> {
        match self {
            Self::Coding => vec!["coding".into()],
            Self::Transform { xor } => vec!["transform".into(), xor.to_string()],
            Self::Probe => vec!["probe".into()],
            Self::Churn { children } => vec!["churn".into(), children.to_string()],
            Self::Sleep { millis } => vec!["sleep".into(), millis.to_string()],
            Self::Flood { bytes } => vec!["flood".into(), bytes.to_string()],
            Self::Tree { early_exit, change_session, resist_term } => vec![
                "tree".into(),
                u8::from(*early_exit).to_string(),
                u8::from(*change_session).to_string(),
                u8::from(*resist_term).to_string(),
            ],
        }
    }
}
/// Scope comes from the authenticated embedding host, never from an opaque reference.
#[derive(Clone, PartialEq, Eq)]
pub struct HostScope {
    pub tenant: String,
    pub principal: String,
}
/// Trusted clock permits deterministic expiry tests without worker-controlled time.
pub trait Clock: Send + Sync {
    fn unix_ms(&self) -> u64;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn unix_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}
/// Each input is already authorized by the host for this run. Bytes are nonsensitive.
#[derive(Clone)]
pub struct Input {
    pub reference: String,
    pub digest: ContentHash,
    pub bytes: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Export {
    pub reference: String,
    pub name: String,
    pub digest: ContentHash,
    pub size: u64,
}
/// Trusted admission result. Registry membership alone cannot create this authority.
/// WIP-10 supplies it from WI admission; this adapter does not mint operator approvals.
pub struct AuthorizedRun {
    pub scope: HostScope,
    pub executable: String,
    pub operations: Vec<(WorkerRequestV1, ResolvedWorkerV1)>,
    pub task: Task,
    pub inputs: Vec<Input>,
    pub exports: Vec<Export>,
    pub controls: ControlEvidenceV1,
    pub credential: CredentialBindingV1,
}
impl AuthorizedRun {
    pub fn input_digest(inputs: &[Input]) -> Result<ContentHash> {
        let bindings: Vec<_> =
            inputs.iter().map(|i| (&i.reference, &i.digest, i.bytes.len())).collect();
        Ok(ContentHash::of(&worldinterface_worker::encode(&bindings)?))
    }
    pub fn configuration_digest(task: &Task, exports: &[Export]) -> Result<ContentHash> {
        Ok(ContentHash::of(&worldinterface_worker::encode(&(task, exports))?))
    }
    pub fn controls_digest(controls: &ControlEvidenceV1) -> Result<ContentHash> {
        Ok(ContentHash::of(&worldinterface_worker::encode(controls)?))
    }
    pub(super) fn validate(
        &self,
        scope: &HostScope,
        executable: &Executable,
        now: u64,
        generation: u64,
    ) -> Result<(u64, u64)> {
        if &self.scope != scope || scope.principal == "root" {
            return Err("host scope mismatch".into());
        }
        worldinterface_worker::nonempty(&scope.tenant)?;
        worldinterface_worker::nonempty(&scope.principal)?;
        self.task.validate()?;
        self.controls.validate()?;
        if !self.controls.observed.is_empty()
            || self.credential.expires_unix_ms.saturating_sub(now) > 120_000
        {
            return Err("unobserved controls or credential lifetime invalid".into());
        }
        if self.operations.is_empty()
            || self.operations.len() > 6
            || self.inputs.len() > 1
            || self.exports.len() > 4
        {
            return Err("admission bound".into());
        }
        let input = Self::input_digest(&self.inputs)?;
        let config = Self::configuration_digest(&self.task, &self.exports)?;
        let controls = Self::controls_digest(&self.controls)?;
        let mut seen = std::collections::BTreeSet::new();
        let mut requests = std::collections::BTreeSet::new();
        for (r, resolved) in &self.operations {
            r.validate()?;
            resolved.validate_for(r)?;
            if !requests.insert(&r.request)
                || !seen.insert(r.operation.name())
                || r.provider != self.credential.provider
                || r.run != self.credential.run
                || r.provider != self.controls.provider
                || r.run != self.controls.run
                || r.input_digest != input
                || r.controls_digest != controls
                || resolved.executable_digest != executable.digest
                || resolved.configuration_digest != config
                || resolved.authorization_digest != self.credential.authorization_digest
                || resolved.adapter_digest != super::adapter_digest()
                || resolved.dependencies_digest != super::dependencies_digest()
            {
                return Err("admitted realization drift".into());
            }
            self.check_credential(r.operation, None, now, generation)?;
        }
        if self
            .credential
            .operations
            .iter()
            .any(|op| !self.operations.iter().any(|(r, _)| r.operation == *op))
            || self
                .credential
                .artifact_scope
                .iter()
                .any(|reference| !self.exports.iter().any(|e| &e.reference == reference))
        {
            return Err("credential widens admitted authority".into());
        }
        let mut refs = std::collections::BTreeSet::new();
        let mut names = std::collections::BTreeSet::new();
        for i in &self.inputs {
            worldinterface_worker::nonempty(&i.reference)?;
            if i.bytes.len() > 1024 * 1024 || i.digest != ContentHash::of(&i.bytes) {
                return Err("input integrity or bound".into());
            }
        }
        let mut total = 0;
        for e in &self.exports {
            worldinterface_worker::nonempty(&e.reference)?;
            if e.size > 4 * 1024 * 1024
                || !names.insert(&e.name)
                || e.name.is_empty()
                || e.name.len() > 64
                || !e.name.bytes().all(|c| c.is_ascii_alphanumeric() || b".-_".contains(&c))
                || e.name == "."
                || e.name == ".."
                || !refs.insert(&e.reference)
                || !self.credential.artifact_scope.contains(&e.reference)
            {
                return Err("export scope invalid".into());
            }
            total += e.size;
        }
        let limit = |kind| {
            self.controls
                .applied
                .iter()
                .find_map(|c| {
                    if c.kind == kind {
                        if let ControlState::Supported(n) = c.state {
                            Some(n)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .ok_or_else(|| "finite control missing".to_string())
        };
        let wall = limit(ControlKind::WallTimeMs)?;
        let output = limit(ControlKind::OutputBytes)?;
        if wall > 60_000 || output > 4 * 1024 * 1024 || total > output {
            return Err("adapter bound exceeded".into());
        }
        Ok((wall, output))
    }
    pub(super) fn check_credential(
        &self,
        op: Operation,
        artifact: Option<&str>,
        now: u64,
        generation: u64,
    ) -> Result<()> {
        self.credential.permits(
            &CredentialExpectation {
                issuer: "worldinterface",
                audience: "process-managed-proof-1",
                principal: &self.scope.principal,
                provider: &self.controls.provider,
                run: &self.controls.run,
                authorization_digest: &self
                    .operations
                    .first()
                    .ok_or("missing admission")?
                    .1
                    .authorization_digest,
            },
            op,
            artifact,
            now,
            generation,
        )
    }
}
/// Reviewed executable capabilities contain sealed verified bytes, never a late pathname lookup.
#[derive(Default)]
pub struct ExecutableRegistry {
    pub(super) entries: BTreeMap<String, Arc<Executable>>,
}
impl ExecutableRegistry {
    pub fn register(&mut self, key: &str, path: &Path, expected: &ContentHash) -> Result<()> {
        worldinterface_worker::nonempty(key)?;
        if self.entries.contains_key(key) {
            return Err("duplicate executable".into());
        }
        self.entries.insert(key.into(), Arc::new(Executable::review(path, expected)?));
        Ok(())
    }
}
/// Read a built executable only in trusted host composition. The returned digest is observed, not asserted.
pub fn executable_digest(path: &Path) -> Result<ContentHash> {
    Ok(ContentHash::of(&super::launch::read_reviewed(path)?))
}
