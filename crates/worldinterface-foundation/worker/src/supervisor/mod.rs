//! Operator-owned instance lifecycle protocol, independent of WorkerPort.
//!
//! A channel is provisioned by the host, never authenticated by request fields.
//! Consumers implement the handler in their supervisor, outside the target's
//! lifetime. Dispatch must use the stable key, and inspect must be authoritative
//! after an ambiguous dispatch. Inspect never grants authority to activate.
use crate::Result;
use serde::{Deserialize, Serialize};
use worldinterface_protocol::{ContentHash, OpaqueRef};

pub const VERSION: u32 = 1;
pub const MAX_FRAME_BYTES: usize = 65536;
pub const MAX_WALL_MS: u64 = 15000;
pub const MAX_EVIDENCE_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Stop,
    Replace,
    Recover,
}
impl Operation {
    pub fn name(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Replace => "replace",
            Self::Recover => "recover",
        }
    }
}

/// Only opaque references and pinned digests cross this boundary. No engine
/// options, paths, credentials, command text or guest-supplied identity exist.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleRequest {
    pub version: u32,
    pub audience: OpaqueRef,
    pub authority_generation: OpaqueRef,
    pub policy_revision: OpaqueRef,
    pub instance: OpaqueRef,
    pub generation: OpaqueRef,
    pub operation: Operation,
    pub target: ContentHash,
    pub image: ContentHash,
    pub deadline_ms: u64,
    pub max_evidence_bytes: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub version: u32,
    pub key: OpaqueRef,
    pub request: LifecycleRequest,
}
impl Command {
    pub fn validate(&self) -> Result<()> {
        if self.version != VERSION
            || self.request.version != VERSION
            || self.request.max_evidence_bytes == 0
            || self.request.max_evidence_bytes as usize > MAX_EVIDENCE_BYTES
        {
            return Err("unsupported supervisor command or evidence bound".into());
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<ContentHash> {
        self.validate()?;
        Ok(ContentHash::of(&encode(self)?))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inspect {
    pub version: u32,
    pub key: OpaqueRef,
    pub command: ContentHash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Submit(Command),
    Inspect(Inspect),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Accepted,
    Running,
    Completed,
    Rejected,
    Ambiguous,
    Escalated,
}
impl State {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Rejected | Self::Escalated)
    }
}
/// Rejected means authoritatively never dispatched; partial transitions escalate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub state: State,
    pub evidence: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub version: u32,
    pub command: Command,
    pub tenant: OpaqueRef,
    pub principal: OpaqueRef,
    pub observation: Observation,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "snake_case", deny_unknown_fields)]
pub enum Response {
    Record(Record),
    Unavailable,
}

pub fn decode<T: serde::de::DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err("supervisor frame exceeds bound".into());
    }
    crate::decode(bytes)
}
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = crate::encode(value)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err("supervisor frame exceeds bound".into());
    }
    Ok(bytes)
}
/// Server-owned authentication provenance. Never deserialize this from a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    pub tenant: OpaqueRef,
    pub principal: OpaqueRef,
    pub audience: OpaqueRef,
    pub authority_generation: OpaqueRef,
    pub policy_revision: OpaqueRef,
}
/// Operator policy binds the exact instance, generation, operation and approved
/// digests. Session validity/revocation is checked by the server on every request.
#[derive(Clone, Debug)]
pub struct Grant {
    pub session: Session,
    pub request: LifecycleRequest,
}

/// Only trusted server composition can authorize; neither this envelope nor
/// its inspect counterpart implements Deserialize or has public fields.
/// ```compile_fail
/// use worldinterface_worker::supervisor::AuthorizedSupervisorCommand;
/// let forged = AuthorizedSupervisorCommand {};
/// ```
#[derive(Clone, Debug)]
pub struct AuthorizedSupervisorCommand {
    command: Command,
    session: Session,
}
impl AuthorizedSupervisorCommand {
    pub fn command(&self) -> &Command {
        &self.command
    }
    pub fn session(&self) -> &Session {
        &self.session
    }
}
#[derive(Clone, Debug)]
pub struct AuthorizedSupervisorInspect {
    command: AuthorizedSupervisorCommand,
}
impl AuthorizedSupervisorInspect {
    pub fn command(&self) -> &Command {
        self.command.command()
    }
    pub fn session(&self) -> &Session {
        self.command.session()
    }
}
/// Trusted server API, not an authentication mechanism. The caller supplies
/// session and grants from its operator-owned channel table, never the wire.
pub fn authorize(
    command: Command,
    session: &Session,
    grants: &[Grant],
    now_ms: u64,
    dispatch: bool,
) -> Result<AuthorizedSupervisorCommand> {
    command.validate()?;
    let r = &command.request;
    if grants.len() > 32
        || r.audience != session.audience
        || r.authority_generation != session.authority_generation
        || r.policy_revision != session.policy_revision
        || !grants.iter().any(|g| g.session == *session && g.request == *r)
        || (dispatch && (r.deadline_ms <= now_ms || r.deadline_ms - now_ms > MAX_WALL_MS))
    {
        return Err("supervisor authority, generation, policy or deadline mismatch".into());
    }
    Ok(AuthorizedSupervisorCommand { command, session: session.clone() })
}
pub fn authorize_inspect(command: AuthorizedSupervisorCommand) -> AuthorizedSupervisorInspect {
    AuthorizedSupervisorInspect { command }
}
/// The backend must be able to inspect a stable command key across process loss.
/// Unsupported semantics are refused before any command is accepted. Calls must
/// initiate bounded work and return promptly; a running transition returns Running.
/// The server admits at most one outstanding handler call and polls at a finite
/// deadline; a stuck consumer cannot create an unbounded queue or more dispatches.
pub trait SupervisorHandler: Send + Sync + 'static {
    fn supports_authoritative_inspect(&self) -> bool;
    fn dispatch(&self, command: AuthorizedSupervisorCommand) -> Result<Observation>;
    fn inspect(&self, command: AuthorizedSupervisorInspect) -> Result<Observation>;
}
