//! Cases, receipts and the admission obligation are projections of the effect journal.
//! No separate mutable database can get ahead of the original uncertainty transaction.
use crate::{Result, StoreRoot};
#[cfg(all(feature = "crash-points", not(test)))]
use worldinterface_domain::reconciliation::CaseAction;
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    effect::{validate_command, EffectAction},
    reconciliation::{ReconciliationCase, VerifiedCaseAction},
};
use worldinterface_protocol::*;
impl StoreRoot {
    pub fn reconciliation_cases(
        &self,
        caller: &AuthenticatedCaller,
        intent: &EffectIntentId,
    ) -> Result<Vec<ReconciliationCase>> {
        let s = self.effect_snapshot(caller.tenant())?;
        let e = s.effects.get(intent).ok_or("effect unavailable")?;
        if &e.agency.binding.principal != caller.principal() {
            return Err("effect unavailable".into());
        }
        Ok(e.reconciliation_cases.clone())
    }
    pub fn reconciliation_apply(
        &self,
        caller: &AuthenticatedCaller,
        at: Timestamp,
        case: &ReconciliationCase,
        action: VerifiedCaseAction,
    ) -> Result<()> {
        #[cfg(all(feature = "crash-points", not(test)))]
        if matches!(action.action(), CaseAction::Observe { .. }) {
            crate::crashpoint::checkpoint("reconciliation-evidence-published");
        }
        // Retry only journal/SQLite contention. The original case revision stays
        // fixed: an unrelated effect may advance the tenant journal, but a newer
        // observation of this case must never be overwritten.
        for retry in 0..32 {
            let result = (|| -> Result<()> {
                let state = self.effect_snapshot(caller.tenant())?;
                // This is journal ordering time, not a fabricated provider time.
                let at = state
                    .last_at
                    .as_ref()
                    .map_or_else(|| at.clone(), |last| at.clone().max(last.clone()));
                let command = validate_command(
                    &state,
                    caller,
                    at,
                    EffectAction::ReconciliationCase {
                        id: case.intent.clone(),
                        case: case.id.clone(),
                        expected: case.revision,
                        action: action.clone(),
                    },
                )?;
                #[cfg(all(feature = "crash-points", not(test)))]
                if retry == 0 {
                    match action.action() {
                        CaseAction::Start => {
                            crate::crashpoint::checkpoint("reconciliation-start-validated")
                        }
                        CaseAction::Observe { .. } => {
                            crate::crashpoint::checkpoint("reconciliation-observe-validated")
                        }
                        _ => {}
                    }
                }
                self.effect_transact(&command)?;
                Ok(())
            })();
            match result {
                Err(error) if retry < 31 && crate::is_effect_contention(error.as_ref()) => continue,
                result => result?,
            }
            break;
        }
        #[cfg(all(feature = "crash-points", not(test)))]
        if matches!(action.action(), CaseAction::Observe { .. }) {
            crate::crashpoint::checkpoint("reconciliation-transition-committed");
        }
        Ok(())
    }
}
