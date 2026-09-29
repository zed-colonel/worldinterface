//! Authenticated production effect facade. Host composition owns credentials and evidence stores.
use crate::{
    authority::{caller, verify_snapshot, Authentication, EvidenceAuthority, TrustedClock},
    resolution::ResourceObserver,
    Result,
};
use actionqueue_executor_local::handler::ExecutorHandler;
use actionqueue_runtime::engine::BootstrappedEngine;
use worldinterface_domain::{
    authorization::AuthorizationEvaluation,
    effect::{EffectAction, EffectView},
};
use worldinterface_egress::Egress;
use worldinterface_protocol::{EffectIntentId, EffectSubmission};
use worldinterface_storage::StoreRoot;

pub struct EffectService<'a, A, V, R, C> {
    pub root: &'a StoreRoot,
    pub authentication: &'a A,
    pub evidence: &'a V,
    pub resolver: &'a R,
    pub clock: &'a C,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DispatchReport {
    pub attempted: usize,
    pub acknowledged: usize,
    pub failed: usize,
}
impl<A: Authentication, V: EvidenceAuthority, R: ResourceObserver, C: TrustedClock>
    EffectService<'_, A, V, R, C>
{
    pub fn submit(
        &self,
        session: &A::Session,
        submission: EffectSubmission,
    ) -> Result<EffectIntentId> {
        let at = self.clock.now()?;
        let caller = caller(self.authentication, session)?;
        let snapshot =
            verify_snapshot(self.root, self.evidence, caller.tenant(), &submission, &at)?;
        Egress::new(self.root).apply(
            &caller,
            at,
            EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot,
                link: None,
            },
        )
    }
    pub fn advance(&self, session: &A::Session, id: &EffectIntentId) -> Result<EffectView> {
        let at = self.clock.now()?;
        let caller = caller(self.authentication, session)?;
        let egress = Egress::new(self.root);
        let view = egress.inspect(&caller, id)?;
        let snapshot = verify_snapshot(
            self.root,
            self.evidence,
            caller.tenant(),
            &view.intent.submission,
            &at,
        )?;
        match view.phase {
            worldinterface_domain::Phase::Submitted => {
                let observed = self.resolver.observe(
                    caller.tenant(),
                    &view.intent.submission,
                    &snapshot.facts().registry,
                    &at,
                );
                let effective = match observed {
                    Ok(fact) if snapshot.facts().facts.contains(&fact) => snapshot,
                    _ => {
                        let mut failed = snapshot.facts().clone();
                        failed.facts.clear();
                        worldinterface_domain::authorization::VerifiedSnapshot::from_trusted_verifier(failed)?
                    }
                };
                egress.apply(
                    &caller,
                    at,
                    EffectAction::Resolve { id: id.clone(), snapshot: effective },
                )?;
            }
            worldinterface_domain::Phase::Resolving
            | worldinterface_domain::Phase::AwaitingApproval
            | worldinterface_domain::Phase::Authorized => {
                if let Some(record) = &view.resolution {
                    crate::resolution::require_current(self.resolver, &record.fact)?;
                }
                egress.apply(&caller, at, EffectAction::Authorize { id: id.clone(), snapshot })?;
            }
            _ => {}
        }
        egress.inspect(&caller, id)
    }
    pub fn explain(
        &self,
        session: &A::Session,
        id: &EffectIntentId,
    ) -> Result<AuthorizationEvaluation> {
        let caller = caller(self.authentication, session)?;
        Ok(Egress::new(self.root)
            .inspect(&caller, id)?
            .authorization
            .ok_or("authorization unavailable")?)
    }
    /// The host must install a current tenant-scoped AQ control context on `engine`.
    pub fn dispatch_pending<H: ExecutorHandler + 'static>(
        &self,
        session: &A::Session,
        queue_store: uuid::Uuid,
        engine: &mut BootstrappedEngine<H>,
        limit: usize,
    ) -> Result<DispatchReport> {
        let at = self.clock.now()?;
        if limit == 0 || limit > 100 {
            return Err("invalid dispatch bound".into());
        }
        let principal = caller(self.authentication, session)?;
        let state = self.root.effect_snapshot(principal.tenant())?;
        let mut report = DispatchReport::default();
        for (id, effect) in state.effects.iter() {
            if report.attempted == limit {
                break;
            }
            if effect.agency.binding.principal != *principal.principal()
                || !matches!(
                    effect.phase,
                    worldinterface_domain::Phase::Authorized
                        | worldinterface_domain::Phase::DispatchPending
                )
                || effect
                    .dispatches
                    .last()
                    .and_then(|d| d.admission.as_ref())
                    .is_some_and(|a| a.acknowledged_sequence.is_some())
            {
                continue;
            }
            report.attempted += 1;
            let result = (|| -> Result<()> {
                let snapshot = verify_snapshot(
                    self.root,
                    self.evidence,
                    principal.tenant(),
                    &effect.intent.submission,
                    &at,
                )?;
                if let Some(resolution) = &effect.resolution {
                    crate::resolution::require_current(self.resolver, &resolution.fact)?;
                }
                if effect.phase == worldinterface_domain::Phase::Authorized {
                    crate::admission::prepare(
                        self.root,
                        &principal,
                        at.clone(),
                        id,
                        snapshot.clone(),
                        queue_store,
                    )?;
                }
                crate::admission::deliver(
                    self.root,
                    &principal,
                    at.clone(),
                    id,
                    snapshot,
                    queue_store,
                    engine,
                )?;
                Ok(())
            })();
            if result.is_ok() {
                report.acknowledged += 1;
            } else {
                report.failed += 1;
            }
        }
        Ok(report)
    }
    pub fn discover_runs<H: ExecutorHandler + 'static>(
        &self,
        session: &A::Session,
        id: &EffectIntentId,
        queue_store: uuid::Uuid,
        engine: &BootstrappedEngine<H>,
        page_bound: usize,
    ) -> Result<crate::run_discovery::DiscoveryProgress> {
        let at = self.clock.now()?;
        let principal = caller(self.authentication, session)?;
        crate::run_discovery::discover(
            self.root,
            &principal,
            at,
            id,
            queue_store,
            engine,
            page_bound,
        )
    }
}
