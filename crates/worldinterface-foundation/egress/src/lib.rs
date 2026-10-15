//! Guarded effect journal facade. The production AQ adapter and trusted host
//! authority composition live in `worldinterface-runtime`.
use worldinterface_domain::{
    agency::AuthenticatedCaller, authorization::VerifiedSnapshot, effect::*, ingress::digest,
    ports::TestEffectAdmissionPort, Phase,
};
use worldinterface_protocol::*;
use worldinterface_storage::{Result, StoreRoot};
fn invalid(s: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, s.into()).into()
}
pub struct Egress<'a> {
    root: &'a StoreRoot,
}
impl<'a> Egress<'a> {
    pub fn new(root: &'a StoreRoot) -> Self {
        Self { root }
    }
    pub fn apply(
        &self,
        caller: &AuthenticatedCaller,
        at: Timestamp,
        action: EffectAction,
    ) -> Result<EffectIntentId> {
        // Optimistic retries revalidate against the newly committed authority/use state.
        for _ in 0..32 {
            let state = self.root.effect_snapshot(caller.tenant())?;
            let command =
                validate_command(&state, caller, at.clone(), action.clone()).map_err(invalid)?;
            match self.root.effect_transact(&command) {
                Ok(id) => return Ok(id),
                Err(e) if worldinterface_storage::is_effect_contention(e.as_ref()) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(invalid("effect contention"))
    }
    pub fn inspect(&self, caller: &AuthenticatedCaller, id: &EffectIntentId) -> Result<EffectView> {
        let state = self.root.effect_snapshot(caller.tenant())?;
        let e = state
            .effects
            .get(id)
            .filter(|e| &e.agency.binding.principal == caller.principal())
            .ok_or_else(|| invalid("effect unavailable"))?;
        inspect_effect(e).map_err(invalid)
    }
    /// Test-only delivery: revalidate authority even when recovering a lost acknowledgment.
    /// Acknowledged queue admission is never interpreted as provider execution or success.
    pub fn deliver_test(
        &self,
        caller: &AuthenticatedCaller,
        at: Timestamp,
        id: &EffectIntentId,
        snapshot: VerifiedSnapshot,
        port: &mut impl TestEffectAdmissionPort,
    ) -> Result<bool> {
        self.apply(caller, at.clone(), EffectAction::Prepare { id: id.clone(), snapshot })?;
        let view = self.inspect(caller, id)?;
        if view.phase != Phase::DispatchPending {
            return Err(invalid("not dispatch ready"));
        }
        let dispatch = view.dispatch.ok_or_else(|| invalid("no prepared request"))?;
        if dispatch.acknowledgment.is_some() {
            return Ok(true);
        }
        let Some(ack) = port.ensure_effect(&dispatch.request).map_err(invalid)? else {
            return Ok(false);
        };
        if ack.request_digest != digest(&dispatch.request).map_err(invalid)? {
            return Err(invalid("acknowledgment digest mismatch"));
        }
        self.apply(caller, at, EffectAction::Acknowledge { id: id.clone(), acknowledgment: ack })?;
        Ok(true)
    }
}
