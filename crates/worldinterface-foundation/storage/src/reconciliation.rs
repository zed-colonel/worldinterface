//! Cases, receipts and the admission obligation are projections of the effect journal.
//! No separate mutable database can get ahead of the original uncertainty transaction.
use crate::{Result, StoreRoot};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    effect::{validate_command, EffectAction},
    reconciliation::{CaseAction, ReconciliationCase},
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
        action: CaseAction,
    ) -> Result<()> {
        #[cfg(all(feature = "crash-points", not(test)))]
        if matches!(&action, CaseAction::Observe { .. }) {
            crate::crashpoint::checkpoint("reconciliation-evidence-published");
        }
        let state = self.effect_snapshot(caller.tenant())?;
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
        self.effect_transact(&command)?;
        #[cfg(all(feature = "crash-points", not(test)))]
        if matches!(&action, CaseAction::Observe { .. }) {
            crate::crashpoint::checkpoint("reconciliation-transition-committed");
        }
        Ok(())
    }
}
