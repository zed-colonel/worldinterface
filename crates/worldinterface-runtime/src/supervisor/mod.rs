//! Operator-owned supervisor journal and authenticated private-channel server.
//! This module contains no engine client. Host composition must place the private
//! root outside both StoreBundle and the target's filesystem and lifetime.
mod connector;
mod journal;
mod transport;
use crate::Result;
pub use connector::{descriptor, SupervisorConnector};
use journal::Journal;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
pub use transport::{Client, PrivateChannel};
use worldinterface_worker::supervisor::*;

pub struct Server {
    journal: Arc<Mutex<Journal>>,
    handler: Arc<dyn SupervisorHandler>,
    busy: Arc<AtomicBool>,
}
/// A provisioned channel has one server-owned session. Revocation is shared with
/// the operator and checked on inspect as well as submit. Guests get no channel.
#[derive(Clone)]
pub struct ChannelPolicy {
    pub session: Session,
    pub grants: Vec<Grant>,
    pub revoked: Arc<AtomicBool>,
}
impl ChannelPolicy {
    pub fn revoke(&self) {
        self.revoked.store(true, Ordering::SeqCst);
    }
    fn authorize(
        &self,
        c: Command,
        now: u64,
        dispatch: bool,
    ) -> Result<AuthorizedSupervisorCommand> {
        if self.revoked.load(Ordering::SeqCst) {
            return Err("revoked supervisor channel".into());
        }
        Ok(authorize(c, &self.session, &self.grants, now, dispatch)?)
    }
}
impl Server {
    pub fn open(root: &Path, handler: Arc<dyn SupervisorHandler>) -> Result<Self> {
        if !handler.supports_authoritative_inspect() {
            return Err("backend cannot reconcile dispatch".into());
        }
        Ok(Self {
            journal: Arc::new(Mutex::new(Journal::open(root)?)),
            handler,
            busy: Arc::new(AtomicBool::new(false)),
        })
    }
    /// No caller identity is taken from Request. A trusted transport supplies the
    /// policy bound to the provisioned channel. Concurrent callers never queue.
    pub fn request(
        &self,
        policy: &ChannelPolicy,
        request: Request,
        now_ms: u64,
    ) -> Result<Response> {
        let mut journal = self.journal.try_lock().map_err(|_| "supervisor journal busy")?;
        journal.healthy()?;
        let (record, fresh) = match request {
            Request::Submit(command) => {
                let authorized = policy.authorize(command.clone(), now_ms, false)?;
                if let Some(old) = journal.get(&command.key) {
                    if old.command != command
                        || old.tenant != authorized.session().tenant
                        || old.principal != authorized.session().principal
                    {
                        return Err("supervisor key conflict".into());
                    }
                    (old.clone(), false)
                } else {
                    if self.busy.load(Ordering::SeqCst)
                        || journal.generation_claimed(&command, &policy.session.tenant)
                    {
                        return Err("supervisor capacity or generation already claimed".into());
                    }
                    policy.authorize(command.clone(), now_ms, true)?;
                    let record = Record {
                        version: VERSION,
                        command,
                        tenant: authorized.session().tenant.clone(),
                        principal: authorized.session().principal.clone(),
                        observation: Observation { state: State::Accepted, evidence: vec![] },
                    };
                    journal.persist(&record)?;
                    (record, true)
                }
            }
            Request::Inspect(query) => {
                if query.version != VERSION {
                    return Err("unsupported inspect version".into());
                }
                let old = journal.get(&query.key).ok_or("unknown supervisor command")?;
                policy.authorize(old.command.clone(), now_ms, false)?;
                if old.tenant != policy.session.tenant
                    || old.principal != policy.session.principal
                    || old.command.digest()? != query.command
                {
                    return Err("inspect binding mismatch".into());
                }
                (old.clone(), false)
            }
        };
        if !record.observation.state.terminal() {
            self.drive(policy, &record, now_ms, fresh)?;
        }
        drop(journal);
        // Acceptance is returned even if a fast backend has already completed.
        Ok(Response::Record(Box::new(record)))
    }
    fn drive(&self, policy: &ChannelPolicy, record: &Record, now: u64, fresh: bool) -> Result<()> {
        if self.busy.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Ok(());
        }
        let auth = match policy.authorize(record.command.clone(), now, fresh) {
            Ok(auth) => auth,
            Err(e) => {
                self.busy.store(false, Ordering::SeqCst);
                return Err(e);
            }
        };
        let journal = self.journal.clone();
        let handler = self.handler.clone();
        let busy = self.busy.clone();
        let mut next = record.clone();
        let revoked = policy.revoked.clone();
        let admitted = std::time::Instant::now();
        // A single outstanding trusted backend call holds the durable owner lock.
        // A new Server cannot open this journal until that call has exited. After
        // process loss, existing records can ONLY inspect, never dispatch again.
        let spawn = std::thread::Builder::new().name("wi-supervisor".into()).spawn(move || {
            struct Release(Arc<AtomicBool>);
            impl Drop for Release {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::SeqCst);
                }
            }
            let _release = Release(busy);
            let result = (|| -> Result<()> {
                if fresh {
                    next.observation.state = State::Running;
                    journal.lock().map_err(|_| "supervisor journal poisoned")?.persist(&next)?;
                }
                if revoked.load(Ordering::SeqCst) {
                    return Ok(());
                }
                if fresh
                    && now.saturating_add(admitted.elapsed().as_millis() as u64)
                        >= next.command.request.deadline_ms
                {
                    next.observation = Observation { state: State::Escalated, evidence: vec![] };
                    journal.lock().map_err(|_| "supervisor journal poisoned")?.persist(&next)?;
                    return Ok(());
                }
                let started = std::time::Instant::now();
                let observed = if fresh {
                    handler.dispatch(auth)
                } else {
                    handler.inspect(authorize_inspect(auth))
                };
                let mut observation =
                    observed.unwrap_or(Observation { state: State::Ambiguous, evidence: vec![] });
                if observation.evidence.len() > next.command.request.max_evidence_bytes as usize
                    || started.elapsed().as_millis() > MAX_WALL_MS as u128
                    // Inspect is read-only authority. A result retained by the
                    // backend remains recoverable after the dispatch deadline.
                    // Completion asserts the consumer enforced execution bounds.
                    || ((fresh || !observation.state.terminal())
                        && now.saturating_add(admitted.elapsed().as_millis() as u64)
                            >= next.command.request.deadline_ms)
                    || observation.state == State::Accepted
                {
                    observation = Observation { state: State::Escalated, evidence: vec![] };
                }
                next.observation = observation;
                journal.lock().map_err(|_| "supervisor journal poisoned")?.persist(&next)
            })();
            // Persistence failure retains the prior nonterminal record, which
            // forces authoritative inspect on the next authenticated request.
            let _ = result;
        });
        if let Err(error) = spawn {
            self.busy.store(false, Ordering::SeqCst);
            return Err(error.into());
        }
        Ok(())
    }
}
